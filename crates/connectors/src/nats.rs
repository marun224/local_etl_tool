//! NATS JetStream, both ways, in **bounded micro-batches**.
//!
//! Only JetStream: core NATS keeps nothing, so there is nothing to read back
//! in a batch. `src.stream.nats` reads a stream (optionally only the subjects a
//! filter matches) the way `src.stream.kafka` reads a topic:
//!
//! 1. When the run starts it records the stream's last sequence. That is the
//!    end of this batch.
//! 2. It reads from the sequence the last **successful** run saved, through an
//!    ephemeral ordered consumer, up to that end or `max_records`. Nothing is
//!    left on the server: no durable consumer (Settled decision 38).
//! 3. It returns the next sequence to read as its checkpoint, which the engine
//!    saves only if the whole run succeeds.
//!
//! **Gaps are errors** as for Kafka: a saved position older than the stream's
//! first sequence means the stream's limits discarded messages this pipeline
//! never read. Messages deleted from the *middle* of a stream are ordinary in
//! JetStream (`max_msgs_per_subject`, a delete by hand) and are simply not
//! there to read.
//!
//! `snk.stream.nats` publishes each row as JSON to a subject and waits for
//! JetStream to acknowledge each batch. With `msg_id_column`, each message
//! carries `Nats-Msg-Id`, and JetStream drops a re-sent one within the
//! stream's duplicate window.
//!
//! The client is `async-nats`, on a single-threaded `tokio` runtime that exists
//! for one read or write (Settled decision 43), with TLS from [`crate::tls`]
//! rather than the operating system's store (Settled decision 42).

use crate::http::{positive, text};
use crate::kafka::{key_text, timestamp_text, value_columns, Format, Start};
use async_nats::jetstream::consumer::pull::OrderedConfig;
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::{ConnectOptions, HeaderMap};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordReader, RecordWriter, Sink, Source,
    Summary,
};
use futures_util::StreamExt;
use serde_json::{json, Map, Value as JsonValue};
use std::future::{Future, IntoFuture};
use std::time::Duration;

#[cfg(test)]
mod tests;

/// `src.stream.nats`.
pub struct NatsSource;

/// `snk.stream.nats`.
pub struct NatsSink;

/// The columns every row carries, whatever `value_format` says.
pub(crate) const METADATA_COLUMNS: [&str; 5] =
    ["_stream", "_subject", "_sequence", "_timestamp", "_headers"];

/// How much longer than `timeout_ms` a call may take before it counts as
/// stalled, so a failure the client reports itself arrives first.
const STALL_SLACK: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// The connection both directions share
// ---------------------------------------------------------------------------

fn with_connection(own: Vec<PropertySpec>) -> Vec<PropertySpec> {
    let mut properties = vec![
        PropertySpec::text("url").required().help(
            "The server, e.g. nats://localhost:4222; comma-separated for a cluster. \
             tls://... also turns TLS on.",
        ),
        PropertySpec::enumerated("auth", &["none", "user_password", "token", "creds"])
            .default(JsonValue::String("none".into()))
            .help(
                "How to sign in. creds is a .creds file (a JWT and an NKey seed), which is how \
                 hosted NATS such as Synadia Cloud works.",
            ),
        PropertySpec::text("username").help("For user_password."),
        PropertySpec::text("password")
            .help("For user_password. Use ${SECRET:name} rather than the value itself."),
        PropertySpec::text("token")
            .help("For token. Use ${SECRET:name} rather than the value itself."),
        PropertySpec::path("creds_file").help(
            "For creds: the .creds file, relative to the workspace. It holds a secret key, so \
             keep it out of version control.",
        ),
        PropertySpec::boolean("tls")
            .default(JsonValue::Bool(false))
            .help("Require TLS. A tls:// url turns it on too."),
        PropertySpec::path("ca_cert").help(
            "For TLS: a PEM file of the certificate authority to trust, for a server with a \
             private CA. Unset trusts the usual public authorities.",
        ),
        PropertySpec::integer("timeout_ms")
            .default(JsonValue::from(30_000))
            .help("How long any one step may take: connecting, one fetch, one batch's acks."),
    ];
    properties.extend(own);
    properties
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Auth {
    None,
    UserPassword(String, String),
    Token(String),
    Creds(String),
}

impl Auth {
    fn name(&self) -> &'static str {
        match self {
            Auth::None => "none",
            Auth::UserPassword(..) => "user_password",
            Auth::Token(_) => "token",
            Auth::Creds(_) => "creds",
        }
    }
}

#[derive(Debug)]
pub(crate) struct Connection {
    pub(crate) urls: Vec<String>,
    pub(crate) auth: Auth,
    pub(crate) tls: bool,
    pub(crate) ca_cert: Option<String>,
    pub(crate) timeout: Duration,
}

impl Connection {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let written = text(properties, "url").ok_or_else(|| {
            ConnectorError::property("url", "is required, e.g. nats://localhost:4222")
        })?;
        let urls: Vec<String> = written
            .split(',')
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(str::to_string)
            .collect();
        if urls.is_empty() {
            return Err(ConnectorError::property(
                "url",
                "is required, e.g. nats://localhost:4222",
            ));
        }
        let mut tls_scheme = false;
        for url in &urls {
            if let Some((scheme, _)) = url.split_once("://") {
                match scheme {
                    "nats" => {}
                    "tls" => tls_scheme = true,
                    other => {
                        return Err(ConnectorError::property(
                            "url",
                            format!("'{url}' has scheme '{other}'; use nats:// or tls://"),
                        ))
                    }
                }
            }
        }

        let username = text(properties, "username");
        let password = text(properties, "password");
        let token = text(properties, "token");
        let creds = text(properties, "creds_file");
        let needed = |value: Option<&str>, name: &str, auth: &str| {
            value
                .map(str::to_string)
                .ok_or_else(|| ConnectorError::property(name, format!("is required for {auth}")))
        };

        let auth = match text(properties, "auth").unwrap_or("none") {
            "none" => Auth::None,
            "user_password" => Auth::UserPassword(
                needed(username, "username", "user_password")?,
                needed(password, "password", "user_password")?,
            ),
            "token" => Auth::Token(needed(token, "token", "token")?),
            "creds" => Auth::Creds(needed(creds, "creds_file", "creds")?),
            other => {
                return Err(ConnectorError::property(
                    "auth",
                    format!("'{other}' is not one of none, user_password, token, creds"),
                ))
            }
        };

        // A credential that would be ignored is a mistake worth catching now.
        let set: Vec<&str> = [
            ("username", username, matches!(auth, Auth::UserPassword(..))),
            ("password", password, matches!(auth, Auth::UserPassword(..))),
            ("token", token, matches!(auth, Auth::Token(_))),
            ("creds_file", creds, matches!(auth, Auth::Creds(_))),
        ]
        .into_iter()
        .filter(|(_, value, used)| value.is_some() && !used)
        .map(|(name, _, _)| name)
        .collect();
        if let Some(name) = set.first() {
            return Err(ConnectorError::property(
                "auth",
                format!("is '{}', which does not use {name}", auth.name()),
            ));
        }

        let tls = tls_scheme || properties.get("tls").and_then(JsonValue::as_bool) == Some(true);
        let ca_cert = text(properties, "ca_cert").map(str::to_string);
        if ca_cert.is_some() && !tls {
            return Err(ConnectorError::property(
                "ca_cert",
                "is for TLS, which is off; set tls or use a tls:// url",
            ));
        }

        Ok(Connection {
            urls,
            auth,
            tls,
            ca_cert,
            timeout: Duration::from_millis(positive(properties, "timeout_ms", 30_000)?),
        })
    }

    /// How this connection is described in an error. Never a secret.
    pub(crate) fn describe(&self) -> String {
        let how = match &self.auth {
            Auth::None => String::new(),
            Auth::UserPassword(user, _) => format!(", user_password as '{user}'"),
            Auth::Token(_) => ", token".to_string(),
            Auth::Creds(file) => format!(", creds from {file}"),
        };
        format!(
            "{} ({}{how})",
            self.urls.join(","),
            if self.tls { "tls" } else { "plaintext" }
        )
    }

    pub(crate) async fn connect(
        &self,
        context: &Context,
    ) -> Result<async_nats::Client, ConnectorError> {
        let mut options = ConnectOptions::new()
            .name("etl")
            .connection_timeout(self.timeout)
            .request_timeout(Some(self.timeout));

        if self.tls {
            options = options
                .tls_client_config(crate::tls::client_config(self.ca_cert.as_deref(), context)?)
                .require_tls(true);
        }
        options = match &self.auth {
            Auth::None => options,
            Auth::UserPassword(user, password) => {
                options.user_and_password(user.clone(), password.clone())
            }
            Auth::Token(token) => options.token(token.clone()),
            Auth::Creds(written) => {
                let path = context.resolve(written);
                options.credentials_file(&path).await.map_err(|error| {
                    ConnectorError::property("creds_file", format!("{}: {error}", path.display()))
                })?
            }
        };

        within(
            self.timeout,
            || format!("connecting to {}", self.describe()),
            options.connect(self.urls.clone()),
        )
        .await
    }
}

fn runtime() -> Result<tokio::runtime::Runtime, ConnectorError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| ConnectorError::Data(format!("could not start the NATS client: {error}")))
}

/// One call to the server, given `limit` (plus [`STALL_SLACK`]).
async fn within<T, E: std::fmt::Display>(
    limit: Duration,
    what: impl Fn() -> String,
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, ConnectorError> {
    match tokio::time::timeout(limit + STALL_SLACK, work).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(ConnectorError::Data(format!("{}: {error}", what()))),
        Err(_) => Err(ConnectorError::Data(format!(
            "{}: no answer within {} ms (timeout_ms) and {} s more",
            what(),
            limit.as_millis(),
            STALL_SLACK.as_secs()
        ))),
    }
}

// ---------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------

impl Source for NatsSource {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("src.stream.nats", "NATS JetStream")
            .description(
                "Read a JetStream stream in bounded batches: each run reads what arrived since \
                 the last successful one, up to max_records.",
            )
            .icon("radio")
            .properties(with_connection(vec![
                PropertySpec::text("stream").required(),
                PropertySpec::text("filter_subject").help(
                    "Only messages whose subject matches, e.g. orders.eu.>. Unset reads the \
                     whole stream.",
                ),
                PropertySpec::enumerated("start", &["earliest", "latest"])
                    .default(JsonValue::String("earliest".into()))
                    .help(
                        "Where a first run starts: earliest reads what the stream still holds, \
                         latest only what arrives from now on.",
                    ),
                PropertySpec::integer("max_records")
                    .default(JsonValue::from(100_000))
                    .help(
                        "The most one run reads. Reaching it is a normal stop: the position is \
                         saved there, and the next run carries on.",
                    ),
                PropertySpec::enumerated("value_format", &["json", "text", "bytes"])
                    .default(JsonValue::String("json".into()))
                    .help(
                        "json makes each message's fields into columns; text gives a value \
                         column; bytes gives value as base64. Every row also has _stream, \
                         _subject, _sequence, _timestamp and _headers.",
                    ),
                columns_property(),
            ]))
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        SourceSettings::from(properties).map(|_| ())
    }

    fn read(
        &self,
        properties: &JsonValue,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = SourceSettings::from(properties)?;
        let saved = context
            .checkpoint
            .as_ref()
            .map(Position::from_json)
            .transpose()?;
        runtime()?.block_on(read_batch(&settings, saved, out, context))
    }
}

#[derive(Debug)]
pub(crate) struct SourceSettings {
    pub(crate) connection: Connection,
    pub(crate) stream: String,
    pub(crate) filter: String,
    pub(crate) start: Start,
    pub(crate) max_records: u64,
    pub(crate) format: Format,
}

impl SourceSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let stream = text(properties, "stream")
            .ok_or_else(|| ConnectorError::property("stream", "is required"))?
            .trim()
            .to_string();
        let start = match text(properties, "start").unwrap_or("earliest") {
            "earliest" => Start::Earliest,
            "latest" => Start::Latest,
            other => {
                return Err(ConnectorError::property(
                    "start",
                    format!("'{other}' is not one of earliest, latest"),
                ))
            }
        };
        let format = match text(properties, "value_format").unwrap_or("json") {
            "json" => Format::Json,
            "text" => Format::Text,
            "bytes" => Format::Bytes,
            other => {
                return Err(ConnectorError::property(
                    "value_format",
                    format!("'{other}' is not one of json, text, bytes"),
                ))
            }
        };
        Ok(SourceSettings {
            connection: Connection::from(properties)?,
            stream,
            filter: text(properties, "filter_subject")
                .unwrap_or("")
                .trim()
                .to_string(),
            start,
            max_records: positive(properties, "max_records", 100_000)?,
            format,
        })
    }
}

/// Where a read got to: the next stream sequence to read, and what it applies
/// to, so a node pointed at another stream or filter is not handed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Position {
    pub(crate) stream: String,
    pub(crate) filter: String,
    pub(crate) next: u64,
}

impl Position {
    pub(crate) fn to_json(&self) -> JsonValue {
        json!({ "stream": self.stream, "filter": self.filter, "next": self.next })
    }

    pub(crate) fn from_json(value: &JsonValue) -> Result<Self, ConnectorError> {
        let unusable = || {
            ConnectorError::Data(format!(
                "the saved position is not one this connector wrote ({value}); `etl state forget` \
                 this node to start it over"
            ))
        };
        Ok(Position {
            stream: value
                .get("stream")
                .and_then(JsonValue::as_str)
                .ok_or_else(unusable)?
                .to_string(),
            filter: value
                .get("filter")
                .and_then(JsonValue::as_str)
                .ok_or_else(unusable)?
                .to_string(),
            next: value
                .get("next")
                .and_then(JsonValue::as_u64)
                .filter(|next| *next >= 1)
                .ok_or_else(unusable)?,
        })
    }
}

/// Where this run starts, and anything worth saying about it. `first` and
/// `last` are the stream's sequences when the run starts; an empty stream
/// reports `last` as the last sequence it ever held and `first` one past it.
///
/// Pure, so the rules that decide whether messages are skipped or read twice
/// are tested without a server.
pub(crate) fn plan_start(
    stream: &str,
    filter: &str,
    first: u64,
    last: u64,
    saved: Option<&Position>,
    start: Start,
) -> Result<(u64, Vec<String>), ConnectorError> {
    let mut notes = Vec::new();
    let saved = match saved {
        Some(position) if position.stream != stream || position.filter != filter => {
            notes.push(format!(
                "the saved position was for stream '{}'{}, so this starts from {}",
                position.stream,
                if position.filter.is_empty() {
                    String::new()
                } else {
                    format!(" filtered to '{}'", position.filter)
                },
                start_name(start)
            ));
            None
        }
        other => other,
    };

    let oldest = first.max(1);
    let from = match saved {
        Some(position) if position.next < oldest => {
            return Err(ConnectorError::Data(format!(
                "stream '{stream}': sequences {} to {} were discarded before this pipeline read \
                 them ({} message(s) lost, most likely to the stream's limits on age, count or \
                 size). Nothing was read. To carry on from what the stream still holds, `etl \
                 state forget` this node, which restarts it from `start`",
                position.next,
                oldest - 1,
                oldest - position.next
            )));
        }
        Some(position) if position.next > last + 1 => {
            return Err(ConnectorError::Data(format!(
                "stream '{stream}': the saved position {} is past the end of the stream ({last}), \
                 so the stream was probably deleted and made again. Nothing was read. `etl state \
                 forget` this node to start it over",
                position.next
            )));
        }
        Some(position) => position.next,
        None => match start {
            Start::Earliest => oldest,
            Start::Latest => last + 1,
        },
    };
    Ok((from, notes))
}

fn start_name(start: Start) -> &'static str {
    match start {
        Start::Earliest => "earliest",
        Start::Latest => "latest",
    }
}

/// One JetStream message as a row.
pub(crate) fn row(
    stream: &str,
    subject: &str,
    sequence: u64,
    timestamp: &str,
    headers: Option<&HeaderMap>,
    value: &[u8],
    format: Format,
) -> Result<Record, ConnectorError> {
    // NATS has no tombstone: an empty payload is an empty value, and for
    // `json` that is a row of only the underscore columns.
    let value = if value.is_empty() && format == Format::Json {
        None
    } else {
        Some(value)
    };
    let mut row = value_columns(
        format,
        value,
        &format!("sequence {sequence}"),
        &METADATA_COLUMNS,
    )?;

    row.insert("_stream".to_string(), JsonValue::String(stream.to_string()));
    row.insert(
        "_subject".to_string(),
        JsonValue::String(subject.to_string()),
    );
    row.insert("_sequence".to_string(), JsonValue::from(sequence));
    row.insert(
        "_timestamp".to_string(),
        JsonValue::String(timestamp.to_string()),
    );
    row.insert("_headers".to_string(), headers_json(headers));
    Ok(row)
}

/// Headers as a JSON object of name to value, a repeated name as a list, and
/// null when there are none. Values that are not UTF-8 are base64, as keys are.
pub(crate) fn headers_json(headers: Option<&HeaderMap>) -> JsonValue {
    let Some(headers) = headers.filter(|headers| !headers.is_empty()) else {
        return JsonValue::Null;
    };
    let mut object: Map<String, JsonValue> = Map::new();
    let mut names: Vec<_> = headers.iter().collect();
    names.sort_by_key(|(name, _)| name.to_string());
    for (name, values) in names {
        let values: Vec<JsonValue> = values
            .iter()
            .map(|value| key_text(Some(value.as_str().as_bytes())))
            .collect();
        let value = if values.len() == 1 {
            values.into_iter().next().unwrap_or(JsonValue::Null)
        } else {
            JsonValue::Array(values)
        };
        object.insert(name.to_string(), value);
    }
    JsonValue::Object(object)
}

async fn read_batch(
    settings: &SourceSettings,
    saved: Option<Position>,
    out: &mut dyn RecordWriter,
    context: &Context,
) -> Result<Summary, ConnectorError> {
    let limit = settings.connection.timeout;
    let name = settings.stream.as_str();
    let client = settings.connection.connect(context).await?;
    let jetstream = async_nats::jetstream::new(client);

    let mut stream = within(
        limit,
        || format!("finding stream '{name}'"),
        jetstream.get_stream(name),
    )
    .await?;
    let state = within(limit, || format!("reading stream '{name}'"), stream.info())
        .await?
        .state
        .clone();
    let (first, last) = (state.first_sequence, state.last_sequence);

    let (from, mut notes) = plan_start(
        name,
        &settings.filter,
        first,
        last,
        saved.as_ref(),
        settings.start,
    )?;

    let mut read = 0u64;
    let mut next = from;
    let mut left = 0u64;

    if from <= last {
        let consumer = within(
            limit,
            || format!("reading stream '{name}' from sequence {from}"),
            stream.create_consumer(OrderedConfig {
                deliver_policy: DeliverPolicy::ByStartSequence {
                    start_sequence: from,
                },
                filter_subject: settings.filter.clone(),
                ..Default::default()
            }),
        )
        .await?;
        let pending = consumer.cached_info().num_pending;

        if pending > 0 {
            let mut messages = within(
                limit,
                || format!("reading stream '{name}' from sequence {from}"),
                consumer.messages(),
            )
            .await?;

            while read < settings.max_records {
                let message = within(
                    limit,
                    || format!("reading stream '{name}' after sequence {}", next - 1),
                    async {
                        match messages.next().await {
                            Some(message) => message.map_err(|error| error.to_string()),
                            None => Err("the server ended the read early".to_string()),
                        }
                    },
                )
                .await?;
                let info = message.info().map_err(|error| {
                    ConnectorError::Data(format!(
                        "stream '{name}': a message without its sequence: {error}"
                    ))
                })?;
                let (sequence, remaining) = (info.stream_sequence, info.pending);
                if sequence > last {
                    // Arrived after the run started: the next run's.
                    break;
                }
                let millis = (info.published.unix_timestamp_nanos() / 1_000_000) as i64;

                out.write(row(
                    name,
                    message.subject.as_str(),
                    sequence,
                    &timestamp_text(millis),
                    message.headers.as_ref(),
                    &message.payload,
                    settings.format,
                )?)?;
                read += 1;
                next = sequence + 1;
                left = remaining;
                if remaining == 0 {
                    break;
                }
            }
        }

        // Everything the filter matched up to the recorded end was read, so the
        // position moves past the end even if the last messages matched nothing.
        if read < settings.max_records || left == 0 {
            next = next.max(last + 1);
            left = 0;
        }
    }

    let mut detail = format!(
        "{read} message(s) from stream '{name}'{}",
        if settings.filter.is_empty() {
            String::new()
        } else {
            format!(" matching '{}'", settings.filter)
        }
    );
    if left > 0 {
        detail.push_str(&format!(
            "; stopped at max_records ({}) with about {left} more for the next run",
            settings.max_records
        ));
    }
    for note in notes.drain(..) {
        detail.push_str("; ");
        detail.push_str(&note);
    }

    let position = Position {
        stream: name.to_string(),
        filter: settings.filter.clone(),
        next,
    };
    Ok(Summary {
        checkpoint: Some(position.to_json()),
        ..Summary::new(read, detail)
    })
}

// ---------------------------------------------------------------------------
// The sink
// ---------------------------------------------------------------------------

impl Sink for NatsSink {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("snk.stream.nats", "NATS JetStream")
            .description(
                "Publish rows to a JetStream subject, one JSON message each, in batches that \
                 JetStream acknowledges.",
            )
            .icon("radio")
            .properties(with_connection(vec![
                PropertySpec::text("subject")
                    .required()
                    .help("Where to publish. A JetStream stream must already capture it."),
                PropertySpec::integer("batch_size")
                    .default(JsonValue::from(500))
                    .help("Rows per batch. Each batch is acknowledged before the next is sent."),
                PropertySpec::text("msg_id_column").help(
                    "A column whose value becomes each message's Nats-Msg-Id, so JetStream drops \
                     a message it has already stored within the stream's duplicate window.",
                ),
            ]))
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        SinkSettings::from(properties).map(|_| ())
    }

    fn write(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = SinkSettings::from(properties)?;
        runtime()?.block_on(write_batches(&settings, input, context))
    }
}

#[derive(Debug)]
pub(crate) struct SinkSettings {
    pub(crate) connection: Connection,
    pub(crate) subject: String,
    pub(crate) batch_size: usize,
    pub(crate) msg_id_column: Option<String>,
}

impl SinkSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let subject = text(properties, "subject")
            .ok_or_else(|| ConnectorError::property("subject", "is required"))?
            .trim()
            .to_string();
        if subject.contains(['*', '>', ' ']) {
            return Err(ConnectorError::property(
                "subject",
                format!("'{subject}' has a wildcard or a space; publish to one exact subject"),
            ));
        }
        Ok(SinkSettings {
            connection: Connection::from(properties)?,
            subject,
            batch_size: positive(properties, "batch_size", 500)? as usize,
            msg_id_column: text(properties, "msg_id_column").map(str::to_string),
        })
    }
}

/// The message ID for one row: its column's value as text, or none for a null.
pub(crate) fn msg_id(row: &Record, column: &str) -> Result<Option<String>, ConnectorError> {
    match row.get(column) {
        None => Err(ConnectorError::property(
            "msg_id_column",
            format!("'{column}' is not a column of the rows"),
        )),
        Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(text)) => Ok(Some(text.clone())),
        Some(other) => Ok(Some(other.to_string())),
    }
}

async fn write_batches(
    settings: &SinkSettings,
    input: &mut dyn RecordReader,
    context: &Context,
) -> Result<Summary, ConnectorError> {
    let limit = settings.connection.timeout;
    let subject = settings.subject.as_str();
    let client = settings.connection.connect(context).await?;
    let jetstream = async_nats::jetstream::new(client);

    let mut sent_batches = 0u64;
    let mut sent = 0u64;
    let mut duplicates = 0u64;
    let mut exhausted = false;
    let mut batch: Vec<Record> = Vec::with_capacity(settings.batch_size);

    while !exhausted {
        match input.read()? {
            Some(row) => batch.push(row),
            None => exhausted = true,
        }
        if batch.is_empty() || !(batch.len() == settings.batch_size || exhausted) {
            continue;
        }

        let rows = std::mem::take(&mut batch);
        let count = rows.len() as u64;
        let what = sent_batches + 1;
        let failed = |error: ConnectorError| {
            ConnectorError::Data(format!(
                "batch {what} failed after {sent_batches} batch(es) ({sent} message(s)) were \
                 delivered, and part of batch {what} may have landed too: {error}"
            ))
        };

        // Publish the whole batch, then wait for every acknowledgement: one
        // round trip's latency per batch rather than per message.
        let mut acks = Vec::with_capacity(rows.len());
        for row in rows {
            let payload = serde_json::to_vec(&row)
                .map_err(|error| failed(ConnectorError::Data(error.to_string())))?;
            let mut headers = HeaderMap::new();
            if let Some(column) = &settings.msg_id_column {
                if let Some(id) = msg_id(&row, column).map_err(failed)? {
                    headers.insert("Nats-Msg-Id", id.as_str());
                }
            }
            let ack = within(
                limit,
                || format!("publishing to '{subject}'"),
                jetstream.publish_with_headers(subject.to_string(), headers, payload.into()),
            )
            .await
            .map_err(failed)?;
            acks.push(ack);
        }
        for ack in acks {
            let ack = within(
                limit,
                || format!("waiting for JetStream to acknowledge '{subject}'"),
                ack.into_future(),
            )
            .await
            .map_err(failed)?;
            if ack.duplicate {
                duplicates += 1;
            }
        }

        sent_batches += 1;
        sent += count;
    }

    let mut detail = if sent_batches == 0 {
        format!("0 messages; nothing published to '{subject}'")
    } else {
        format!("{sent} message(s) in {sent_batches} batch(es) to '{subject}'")
    };
    if duplicates > 0 {
        detail.push_str(&format!(
            "; JetStream dropped {duplicates} as duplicates of messages it already held"
        ));
    }
    Ok(Summary::new(sent, detail))
}
