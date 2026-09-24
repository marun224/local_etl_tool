//! MongoDB: a collection read in batches through a cursor, all of it or only
//! what is new since the last successful run, and written to by insert or by
//! upsert on key fields.
//!
//! **Reading.** `find` with the node's `filter`, `projection` and `sort`, one
//! cursor, `batch_size` documents a round trip. A document becomes a row: its
//! top-level fields are the columns, and a value that is not plain JSON is made
//! plain: an `ObjectId` its hex text, a date a UTC timestamp, a `Decimal128`
//! its text, nested documents and arrays JSON (with the same rules inside).
//! A collection that does not exist is an error, not an empty read: MongoDB
//! itself answers a `find` on a missing collection with nothing.
//!
//! **Only what is new** (Settled decision 81): with `incremental_field`, the
//! read adds `field > <the last run's highest value>` to the filter and sorts
//! by the field, and the highest value read becomes the checkpoint, saved only
//! if the whole run succeeds. The value is kept as Extended JSON so a date
//! stays a date and an `ObjectId` an `ObjectId`. A document without the field
//! is never read this way, and a field that can go down skips documents; both
//! are the data's to guarantee, and `connectors.md` says so.
//!
//! **Writing.** `insert` is `insertMany`, unordered, a thousand at a time;
//! `upsert` is the `update` command, a thousand replacements with `upsert` in
//! one round trip, matched on `key_fields`, so a re-run replaces rather than
//! duplicates. It works on every server version, where the driver's own
//! `bulkWrite` needs MongoDB 8.
//!
//! The driver is `mongodb`'s blocking API; TLS is its own `rustls` on `ring`,
//! which trusts `ca_cert` alone if given and the bundled public roots if not,
//! the same rule as `tls.rs`. Every wait is bounded by `timeout_ms`.

use crate::http::{base64_bytes, positive, text};
use crate::kafka::timestamp_text;
use ::mongodb::bson::{doc, Bson, Document};
use ::mongodb::options::{ClientOptions, Tls, TlsOptions};
use ::mongodb::sync::{Client, Collection, Database};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordReader, RecordWriter, Sink, Source,
    Summary,
};
use serde_json::{json, Map, Value as JsonValue};
use std::time::Duration;

#[cfg(test)]
mod tests;

/// `src.db.mongodb`.
pub struct MongoSource;

/// `snk.db.mongodb`.
pub struct MongoSink;

/// Documents to a write call.
const WRITE_BATCH: usize = 1000;

// ---------------------------------------------------------------------------
// The server
// ---------------------------------------------------------------------------

/// The properties both components have, after their own identifying ones.
fn connection_properties() -> Vec<PropertySpec> {
    vec![
        PropertySpec::text("uri").required().help(
            "mongodb://host[:port][,host...]/?options, or mongodb+srv://cluster.example.net. \
             A user and password may be in it, but username and password keep the password out.",
        ),
        PropertySpec::text("username").help("Unset: the URI's."),
        PropertySpec::text("password")
            .help("Use ${SECRET:name} rather than the value itself. Unset: the URI's."),
        PropertySpec::text("auth_source")
            .help("The database the user is defined in. Unset: the URI's, else admin."),
        PropertySpec::text("database").required(),
        PropertySpec::text("collection").required(),
        PropertySpec::path("ca_cert").help(
            "A PEM file of the certificate authority for a server with a private certificate; \
             turns TLS on. Unset, the URI's tls setting and the bundled public roots.",
        ),
        PropertySpec::integer("timeout_ms")
            .default(JsonValue::from(30_000))
            .help("How long to wait for a server, and for any one operation."),
    ]
}

/// Where to connect: the parsed options, and what the report may say.
pub(crate) struct Server {
    options: ClientOptions,
    database: String,
    collection: String,
    /// `hosts, database 'd'`: never the password.
    place: String,
}

impl Server {
    pub(crate) fn from(properties: &JsonValue, context: &Context) -> Result<Self, ConnectorError> {
        let uri = required(properties, "uri")?;
        if !(uri.starts_with("mongodb://") || uri.starts_with("mongodb+srv://")) {
            return Err(ConnectorError::property(
                "uri",
                "must start with mongodb:// or mongodb+srv://",
            ));
        }
        let database = required(properties, "database")?.to_string();
        let collection = required(properties, "collection")?.to_string();
        let timeout = Duration::from_millis(positive(properties, "timeout_ms", 30_000)?);

        // Parsing a mongodb+srv:// URI asks DNS, so it waits for the run.
        // Not the driver's words on failure: they can quote the URI.
        let mut options = ClientOptions::parse(uri).run().map_err(|error| {
            ConnectorError::property(
                "uri",
                format!("is not a MongoDB connection string ({})", error.kind_text()),
            )
        })?;
        let username = text(properties, "username").map(str::to_string);
        let password = text(properties, "password").map(str::to_string);
        let auth_source = text(properties, "auth_source").map(str::to_string);
        if username.is_some() || password.is_some() || auth_source.is_some() {
            let mut credential = options.credential.clone().unwrap_or_default();
            if username.is_some() {
                credential.username = username;
            }
            if password.is_some() {
                credential.password = password;
            }
            if auth_source.is_some() {
                credential.source = auth_source;
            }
            options.credential = Some(credential);
        }
        if let Some(ca) = text(properties, "ca_cert") {
            options.tls = Some(Tls::Enabled(
                TlsOptions::builder()
                    .ca_file_path(Some(context.resolve(ca)))
                    .build(),
            ));
        }
        options.server_selection_timeout = Some(timeout);
        options.connect_timeout = Some(timeout);
        options.app_name = Some(concat!("etl/", env!("CARGO_PKG_VERSION")).to_string());

        let hosts = options
            .hosts
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let place = format!("{hosts}, database '{database}'");
        Ok(Server {
            options,
            database,
            collection,
            place,
        })
    }

    fn connect(&self) -> Result<(Database, Collection<Document>), ConnectorError> {
        let client = Client::with_options(self.options.clone())
            .map_err(|error| self.failed("connecting", error))?;
        let database = client.database(&self.database);
        let collection = database.collection::<Document>(&self.collection);
        Ok((database, collection))
    }

    /// An error from the driver, with where and what, and never the URI.
    fn failed(&self, what: &str, error: ::mongodb::error::Error) -> ConnectorError {
        ConnectorError::Data(format!(
            "MongoDB at {}, {what}: {}",
            self.place,
            error.kind_text()
        ))
    }

    /// The collection must exist: MongoDB answers a read of a missing one with
    /// nothing at all, which would look like an empty collection.
    fn existing(&self, database: &Database) -> Result<(), ConnectorError> {
        let names = database
            .list_collection_names()
            .filter(doc! { "name": &self.collection })
            .run()
            .map_err(|error| self.failed("listing collections", error))?;
        if names.is_empty() {
            return Err(ConnectorError::Data(format!(
                "MongoDB at {}: there is no collection '{}'",
                self.place, self.collection
            )));
        }
        Ok(())
    }
}

/// The driver's message without its own framing: the kind, and the server's
/// words, which are what say what went wrong.
trait KindText {
    fn kind_text(&self) -> String;
}

impl KindText for ::mongodb::error::Error {
    fn kind_text(&self) -> String {
        use ::mongodb::error::ErrorKind;
        match self.kind.as_ref() {
            ErrorKind::Command(command) => format!("{} ({})", command.message, command.code_name),
            ErrorKind::Authentication { message, .. } => {
                format!("authentication failed: {message}")
            }
            ErrorKind::ServerSelection { message, .. } => {
                format!("no server answered in time: {message}")
            }
            _ => self.kind.to_string(),
        }
    }
}

fn required<'a>(properties: &'a JsonValue, key: &str) -> Result<&'a str, ConnectorError> {
    text(properties, key)
        .map(str::trim)
        .ok_or_else(|| ConnectorError::property(key, "is required"))
}

/// A JSON document from a property: an object, or its text (Extended JSON).
fn document_property(
    properties: &JsonValue,
    key: &str,
) -> Result<Option<Document>, ConnectorError> {
    let value = match properties.get(key) {
        None | Some(JsonValue::Null) => return Ok(None),
        Some(JsonValue::String(text)) if text.trim().is_empty() => return Ok(None),
        Some(JsonValue::String(text)) => serde_json::from_str(text)
            .map_err(|error| ConnectorError::property(key, format!("is not JSON: {error}")))?,
        Some(other) => other.clone(),
    };
    match Bson::try_from(value) {
        Ok(Bson::Document(document)) => Ok(Some(document)),
        Ok(_) => Err(ConnectorError::property(key, "must be a JSON object")),
        Err(error) => Err(ConnectorError::property(
            key,
            format!("is not Extended JSON MongoDB reads: {error}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/// A BSON value as plain JSON, for a row.
pub(crate) fn plain(value: &Bson) -> JsonValue {
    match value {
        Bson::Double(number) => json!(number),
        Bson::String(text) => json!(text),
        Bson::Array(items) => JsonValue::Array(items.iter().map(plain).collect()),
        Bson::Document(document) => JsonValue::Object(
            document
                .iter()
                .map(|(key, value)| (key.clone(), plain(value)))
                .collect::<Map<_, _>>(),
        ),
        Bson::Boolean(flag) => json!(flag),
        Bson::Null | Bson::Undefined | Bson::MinKey | Bson::MaxKey => JsonValue::Null,
        Bson::Int32(number) => json!(number),
        Bson::Int64(number) => json!(number),
        Bson::ObjectId(id) => json!(id.to_hex()),
        Bson::DateTime(at) => json!(timestamp_text(at.timestamp_millis())),
        Bson::Decimal128(decimal) => json!(decimal.to_string()),
        Bson::Binary(binary) => json!(base64_bytes(&binary.bytes)),
        Bson::Timestamp(stamp) => json!(timestamp_text(i64::from(stamp.time) * 1000)),
        Bson::RegularExpression(regex) => json!(format!("/{}/{}", regex.pattern, regex.options)),
        Bson::JavaScriptCode(code) => json!(code),
        Bson::JavaScriptCodeWithScope(code) => json!(code.code),
        Bson::Symbol(symbol) => json!(symbol),
        Bson::DbPointer(_) => JsonValue::Null,
    }
}

/// One document as a row: its top-level fields, made plain.
pub(crate) fn row(document: &Document) -> Record {
    document
        .iter()
        .map(|(key, value)| (key.clone(), plain(value)))
        .collect()
}

// ---------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------

impl Source for MongoSource {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties();
        properties.extend([
            PropertySpec::code("filter").help(
                "A query document in Extended JSON, e.g. {\"status\": \"paid\"} or \
                 {\"at\": {\"$gte\": {\"$date\": \"2026-01-01T00:00:00Z\"}}}. Unset: every document.",
            ),
            PropertySpec::code("projection")
                .help("Which fields, e.g. {\"customer\": 1, \"amount\": 1}. Unset: all."),
            PropertySpec::code("sort").help(
                "An order, e.g. {\"at\": 1}. With incremental_field the order is that field's.",
            ),
            PropertySpec::text("incremental_field").help(
                "Read only documents whose value here is above the last successful run's \
                 highest, e.g. updatedAt or _id. It must only ever go up.",
            ),
            PropertySpec::code("start").help(
                "With incremental_field, where the first run starts, in Extended JSON, e.g. \
                 {\"$date\": \"2026-01-01T00:00:00Z\"}. Unset: from the beginning.",
            ),
            PropertySpec::integer("batch_size")
                .default(JsonValue::from(1000))
                .help("Documents a round trip."),
            PropertySpec::integer("max_records")
                .help("The most one run reads. Unset: all that match."),
            columns_property(),
        ]);
        ComponentSpec::new("src.db.mongodb", "MongoDB collection")
            .description(
                "Read documents from a MongoDB collection, with a filter, all of them or only \
                 those new since the last successful run. Each top-level field is a column.",
            )
            .icon("database")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        SourceSettings::check(properties)
    }

    fn read(
        &self,
        properties: &JsonValue,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = SourceSettings::from(properties, context)?;
        read(&settings, out, context.checkpoint.as_ref())
    }
}

pub(crate) struct SourceSettings {
    pub(crate) server: Server,
    pub(crate) filter: Document,
    pub(crate) projection: Option<Document>,
    pub(crate) sort: Option<Document>,
    pub(crate) incremental: Option<Incremental>,
    pub(crate) batch_size: u32,
    pub(crate) max_records: Option<u64>,
}

/// Which field, and where the first run starts.
pub(crate) struct Incremental {
    pub(crate) field: String,
    pub(crate) start: Option<Bson>,
}

impl SourceSettings {
    /// Everything that can be refused without asking DNS or a server.
    fn check(properties: &JsonValue) -> Result<(), ConnectorError> {
        let uri = required(properties, "uri")?;
        if !(uri.starts_with("mongodb://") || uri.starts_with("mongodb+srv://")) {
            return Err(ConnectorError::property(
                "uri",
                "must start with mongodb:// or mongodb+srv://",
            ));
        }
        required(properties, "database")?;
        required(properties, "collection")?;
        Self::parts(properties).map(|_| ())
    }

    #[allow(clippy::type_complexity)]
    fn parts(
        properties: &JsonValue,
    ) -> Result<
        (
            Document,
            Option<Document>,
            Option<Document>,
            Option<Incremental>,
        ),
        ConnectorError,
    > {
        let filter = document_property(properties, "filter")?.unwrap_or_default();
        let projection = document_property(properties, "projection")?;
        let sort = document_property(properties, "sort")?;
        let incremental = match text(properties, "incremental_field").map(str::trim) {
            None => {
                if properties
                    .get("start")
                    .is_some_and(|start| !start.is_null())
                {
                    return Err(ConnectorError::property(
                        "start",
                        "goes with incremental_field, which is not set",
                    ));
                }
                None
            }
            Some(field) => {
                if filter.contains_key(field) {
                    return Err(ConnectorError::property(
                        "filter",
                        format!(
                            "also filters on '{field}', the incremental_field; the run adds \
                             that condition itself"
                        ),
                    ));
                }
                let start = match properties.get("start") {
                    None | Some(JsonValue::Null) => None,
                    Some(JsonValue::String(text)) if text.trim().is_empty() => None,
                    Some(value) => {
                        let value = match value {
                            // Extended JSON as text, or a plain string value.
                            JsonValue::String(text) => {
                                serde_json::from_str(text).unwrap_or_else(|_| json!(text))
                            }
                            other => other.clone(),
                        };
                        Some(Bson::try_from(value).map_err(|error| {
                            ConnectorError::property(
                                "start",
                                format!("is not Extended JSON MongoDB reads: {error}"),
                            )
                        })?)
                    }
                };
                Some(Incremental {
                    field: field.to_string(),
                    start,
                })
            }
        };
        Ok((filter, projection, sort, incremental))
    }

    pub(crate) fn from(properties: &JsonValue, context: &Context) -> Result<Self, ConnectorError> {
        Self::check(properties)?;
        let (filter, projection, sort, incremental) = Self::parts(properties)?;
        let batch_size = positive(properties, "batch_size", 1000)?.min(u64::from(u32::MAX)) as u32;
        let max_records = match properties.get("max_records") {
            None | Some(JsonValue::Null) => None,
            Some(_) => Some(positive(properties, "max_records", 1)?),
        };
        Ok(SourceSettings {
            server: Server::from(properties, context)?,
            filter,
            projection,
            sort,
            incremental,
            batch_size,
            max_records,
        })
    }
}

/// The checkpoint: where the last run got to, and what it was reading, so a
/// node whose collection or field changed starts over.
fn checkpoint(settings: &SourceSettings, field: &str, highest: &Bson) -> JsonValue {
    json!({
        "collection": format!("{}.{}", settings.server.database, settings.server.collection),
        "field": field,
        "value": highest.clone().into_canonical_extjson(),
    })
}

/// Where this run starts: the saved value if it belongs to this read, else
/// `start`. And a note when a saved one was set aside.
fn resume_from(
    settings: &SourceSettings,
    incremental: &Incremental,
    saved: Option<&JsonValue>,
) -> Result<(Option<Bson>, Option<String>), ConnectorError> {
    let collection = format!(
        "{}.{}",
        settings.server.database, settings.server.collection
    );
    match saved {
        Some(saved)
            if saved["collection"] == json!(collection)
                && saved["field"] == json!(incremental.field) =>
        {
            let value = Bson::try_from(saved["value"].clone()).map_err(|error| {
                ConnectorError::Data(format!(
                    "the saved position for {collection} is not Extended JSON ({error}); \
                     `etl state forget` it to start over"
                ))
            })?;
            Ok((Some(value), None))
        }
        Some(saved) => Ok((
            incremental.start.clone(),
            Some(format!(
                "; the saved position was for {} by '{}', so this read started over",
                saved["collection"].as_str().unwrap_or("?"),
                saved["field"].as_str().unwrap_or("?")
            )),
        )),
        None => Ok((incremental.start.clone(), None)),
    }
}

/// Read every matching document, or those new since `saved`, as rows.
pub(crate) fn read(
    settings: &SourceSettings,
    out: &mut dyn RecordWriter,
    saved: Option<&JsonValue>,
) -> Result<Summary, ConnectorError> {
    let server = &settings.server;
    let (database, collection) = server.connect()?;
    server.existing(&database)?;

    let mut filter = settings.filter.clone();
    let mut sort = settings.sort.clone();
    let mut note = None;
    let mut from = None;
    if let Some(incremental) = &settings.incremental {
        let (after, set_aside) = resume_from(settings, incremental, saved)?;
        if let Some(after) = &after {
            filter.insert(incremental.field.clone(), doc! { "$gt": after.clone() });
        } else {
            // From the beginning: only documents that have the field at all.
            filter.insert(incremental.field.clone(), doc! { "$exists": true });
        }
        sort = Some(doc! { incremental.field.clone(): 1 });
        note = set_aside;
        from = after;
    }

    let mut find = collection.find(filter).batch_size(settings.batch_size);
    if let Some(projection) = &settings.projection {
        find = find.projection(projection.clone());
    }
    if let Some(sort) = sort {
        find = find.sort(sort);
    }
    if let Some(limit) = settings.max_records {
        find = find.limit(limit as i64);
    }
    let cursor = find.run().map_err(|error| server.failed("find", error))?;

    let mut count = 0u64;
    let mut highest: Option<Bson> = None;
    for document in cursor {
        let document = document.map_err(|error| server.failed("reading", error))?;
        if let Some(incremental) = &settings.incremental {
            // Sorted by the field, so the last one read is the highest.
            highest = document.get(&incremental.field).cloned();
        }
        out.write(row(&document))?;
        count += 1;
    }

    let mut detail = format!(
        "{count} document(s) from collection '{}' at {}",
        server.collection, server.place
    );
    let mut summary_checkpoint = None;
    if let Some(incremental) = &settings.incremental {
        match (&highest, &from) {
            (Some(highest), _) => {
                detail.push_str(&format!(
                    "; read up to {} = {}",
                    incremental.field,
                    plain(highest)
                ));
                summary_checkpoint = Some(checkpoint(settings, &incremental.field, highest));
            }
            (None, Some(from)) => detail.push_str(&format!(
                "; nothing new after {} = {}",
                incremental.field,
                plain(from)
            )),
            (None, None) => detail.push_str("; nothing to read yet"),
        }
        if settings.max_records == Some(count) {
            detail.push_str(", stopped at max_records, with more for the next run");
        }
    } else if settings.max_records == Some(count) {
        detail.push_str("; stopped at max_records");
    }
    if let Some(note) = note {
        detail.push_str(&note);
    }
    let mut summary = Summary::new(count, detail);
    summary.checkpoint = summary_checkpoint;
    Ok(summary)
}

// ---------------------------------------------------------------------------
// The sink
// ---------------------------------------------------------------------------

impl Sink for MongoSink {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties();
        properties.extend([
            PropertySpec::enumerated("mode", &["insert", "upsert"])
                .default(JsonValue::String("insert".into()))
                .help(
                    "insert adds every row as a new document; upsert replaces the document whose \
                     key_fields match, or adds it, so a re-run adds nothing twice.",
                ),
            PropertySpec::string_list("key_fields")
                .help("With upsert: the fields that identify a document, e.g. order_id."),
        ]);
        ComponentSpec::new("snk.db.mongodb", "MongoDB collection")
            .description(
                "Write rows to a MongoDB collection, one document each: inserted, or upserted on \
                 key fields.",
            )
            .icon("database")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        let uri = required(properties, "uri")?;
        if !(uri.starts_with("mongodb://") || uri.starts_with("mongodb+srv://")) {
            return Err(ConnectorError::property(
                "uri",
                "must start with mongodb:// or mongodb+srv://",
            ));
        }
        required(properties, "database")?;
        required(properties, "collection")?;
        Mode::from(properties).map(|_| ())
    }

    fn write(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        self.check(properties)?;
        let server = Server::from(properties, context)?;
        write(&server, &Mode::from(properties)?, input)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Mode {
    Insert,
    Upsert(Vec<String>),
}

impl Mode {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let keys: Vec<String> = match properties.get("key_fields") {
            None | Some(JsonValue::Null) => Vec::new(),
            Some(JsonValue::Array(items)) => items
                .iter()
                .map(|item| {
                    item.as_str()
                        .map(|key| key.trim().to_string())
                        .filter(|key| !key.is_empty())
                        .ok_or_else(|| {
                            ConnectorError::property("key_fields", "must be a list of field names")
                        })
                })
                .collect::<Result<_, _>>()?,
            Some(_) => {
                return Err(ConnectorError::property(
                    "key_fields",
                    "must be a list of field names",
                ))
            }
        };
        match text(properties, "mode").unwrap_or("insert") {
            "insert" if keys.is_empty() => Ok(Mode::Insert),
            "insert" => Err(ConnectorError::property(
                "key_fields",
                "is for mode upsert; insert adds every row",
            )),
            "upsert" if keys.is_empty() => Err(ConnectorError::property(
                "key_fields",
                "is required for mode upsert: it says which document a row replaces",
            )),
            "upsert" => Ok(Mode::Upsert(keys)),
            other => Err(ConnectorError::property(
                "mode",
                format!("'{other}' is not one of insert, upsert"),
            )),
        }
    }
}

/// A row as a document. JSON becomes BSON as Extended JSON says, so a column
/// holding `{"$date": "..."}` is stored as a date; everything else as itself.
pub(crate) fn document(row: u64, record: &Record) -> Result<Document, ConnectorError> {
    match Bson::try_from(JsonValue::Object(record.clone())) {
        Ok(Bson::Document(document)) => Ok(document),
        Ok(_) => unreachable!("an object is a document"),
        Err(error) => Err(ConnectorError::Data(format!(
            "row {row} is not a document MongoDB takes: {error}"
        ))),
    }
}

/// What has landed so far, for the summary and a failure's message.
#[derive(Default)]
struct Written {
    inserted: u64,
    matched: u64,
    upserted: u64,
    calls: u64,
}

impl Written {
    fn failed(&self, server: &Server, error: impl std::fmt::Display) -> ConnectorError {
        ConnectorError::Data(format!(
            "{error}. {} document(s) had been written to collection '{}' before this, and stay",
            self.inserted + self.matched + self.upserted,
            server.collection
        ))
    }
}

pub(crate) fn write(
    server: &Server,
    mode: &Mode,
    input: &mut dyn RecordReader,
) -> Result<Summary, ConnectorError> {
    let (database, collection) = server.connect()?;
    let mut written = Written::default();
    let mut batch: Vec<(u64, Document)> = Vec::new();
    let mut row = 0u64;

    let flush = |batch: Vec<(u64, Document)>, written: &mut Written| match mode {
        Mode::Insert => insert(server, &collection, batch, written),
        Mode::Upsert(keys) => upsert(server, &database, keys, batch, written),
    };
    while let Some(record) = input.read()? {
        row += 1;
        let document = document(row, &record).map_err(|error| written.failed(server, error))?;
        batch.push((row, document));
        if batch.len() == WRITE_BATCH {
            flush(std::mem::take(&mut batch), &mut written)?;
        }
    }
    if !batch.is_empty() {
        flush(batch, &mut written)?;
    }

    let detail = match mode {
        Mode::Insert => format!(
            "{} document(s) inserted into collection '{}' at {} in {} call(s)",
            written.inserted, server.collection, server.place, written.calls
        ),
        Mode::Upsert(keys) => format!(
            "{} document(s) replaced and {} added in collection '{}' at {}, matched on {} in {} \
             call(s)",
            written.matched,
            written.upserted,
            server.collection,
            server.place,
            keys.join(", "),
            written.calls
        ),
    };
    Ok(Summary::new(
        written.inserted + written.matched + written.upserted,
        detail,
    ))
}

fn insert(
    server: &Server,
    collection: &Collection<Document>,
    batch: Vec<(u64, Document)>,
    written: &mut Written,
) -> Result<(), ConnectorError> {
    let rows: Vec<u64> = batch.iter().map(|(row, _)| *row).collect();
    let count = batch.len() as u64;
    let result = collection
        .insert_many(batch.into_iter().map(|(_, document)| document))
        .ordered(false)
        .run();
    written.calls += 1;
    match result {
        Ok(_) => {
            written.inserted += count;
            Ok(())
        }
        Err(error) => {
            use ::mongodb::error::ErrorKind;
            if let ErrorKind::InsertMany(failure) = error.kind.as_ref() {
                if let Some(errors) = &failure.write_errors {
                    // Unordered: everything else in the batch was inserted.
                    written.inserted += count - errors.len() as u64;
                    let first = &errors[0];
                    return Err(written.failed(
                        server,
                        format!(
                            "row {} was refused ({} more in its batch): {}",
                            rows[first.index],
                            errors.len() - 1,
                            first.message
                        ),
                    ));
                }
            }
            Err(written.failed(server, server.failed("insertMany", error)))
        }
    }
}

fn upsert(
    server: &Server,
    database: &Database,
    keys: &[String],
    batch: Vec<(u64, Document)>,
    written: &mut Written,
) -> Result<(), ConnectorError> {
    let mut updates = Vec::with_capacity(batch.len());
    let rows: Vec<u64> = batch.iter().map(|(row, _)| *row).collect();
    for (row, document) in &batch {
        let mut matching = Document::new();
        for key in keys {
            match document.get(key) {
                None | Some(Bson::Null) => {
                    return Err(written.failed(
                        server,
                        format!("row {row} has no value for key field '{key}'"),
                    ))
                }
                Some(value) => {
                    matching.insert(key.clone(), value.clone());
                }
            }
        }
        updates.push(doc! { "q": matching, "u": document.clone(), "upsert": true });
    }
    let answer = database
        .run_command(doc! {
            "update": &server.collection, "updates": updates, "ordered": false,
        })
        .run()
        .map_err(|error| written.failed(server, server.failed("update", error)))?;
    written.calls += 1;

    let count = |key: &str| -> u64 {
        match answer.get(key) {
            Some(Bson::Int32(n)) => *n as u64,
            Some(Bson::Int64(n)) => *n as u64,
            _ => 0,
        }
    };
    let upserted = answer
        .get_array("upserted")
        .map_or(0, |upserted| upserted.len() as u64);
    written.upserted += upserted;
    written.matched += count("n").saturating_sub(upserted);
    if let Ok(errors) = answer.get_array("writeErrors") {
        if let Some(Bson::Document(first)) = errors.first() {
            let index = first.get_i32("index").unwrap_or(0) as usize;
            return Err(written.failed(
                server,
                format!(
                    "row {} was refused ({} more in its batch): {}",
                    rows.get(index).copied().unwrap_or(0),
                    errors.len() - 1,
                    first.get_str("errmsg").unwrap_or("?")
                ),
            ));
        }
    }
    Ok(())
}
