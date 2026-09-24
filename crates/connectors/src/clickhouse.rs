//! ClickHouse: a table or a query read over the HTTP interface, streamed a
//! row a line, all of it or only what is new since the last successful run;
//! and a table written to by `INSERT ... FORMAT JSONEachRow`, in batches.
//!
//! The HTTP interface rather than the `clickhouse` crate, which asks for a
//! newer Rust than the project's. **Reading** asks for
//! `JSONCompactEachRowWithNamesAndTypes`: a line of column names, a line of
//! types, then one JSON array a row, read line by line so memory stays flat
//! however large the result. 64-bit and wider integers come quoted (a double
//! would round a 128-bit one) and are made numbers by their type where they
//! fit; decimals come as their exact text. Every request runs with the session
//! time zone UTC.
//!
//! **An error part-way through a result arrives after a `200`**: ClickHouse
//! writes it as a last row holding its message (found in the probe). The
//! reader recognises it and fails, rather than taking it for data.
//!
//! **Only what is new**: `incremental_column` wraps the read as
//! `SELECT * FROM (<read>) WHERE col > {etl_after:<type>} ORDER BY col`, the
//! last run's highest value a **query parameter**, never pasted into the SQL.

use crate::http::{base64, positive, snippet, text, Client, Extra, Judged, Method, Settings};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, RecordReader, RecordWriter, Sink, Source, Summary,
};
use serde_json::{json, Map, Value as JsonValue};
use std::io::BufRead;
use std::time::Duration;

#[cfg(test)]
mod tests;

/// `src.db.clickhouse`.
pub struct ClickhouseSource;

/// `snk.db.clickhouse`.
pub struct ClickhouseSink;

/// Rows an `INSERT` carries at most, and bytes.
const INSERT_ROWS: usize = 100_000;
const INSERT_BYTES: usize = 16 * 1024 * 1024;

/// Settings every request carries.
const SESSION: [(&str, &str); 4] = [
    ("session_timezone", "UTC"),
    ("output_format_json_quote_64bit_integers", "1"),
    ("output_format_json_quote_decimals", "1"),
    ("date_time_input_format", "best_effort"),
];

// ---------------------------------------------------------------------------
// The server
// ---------------------------------------------------------------------------

fn connection_properties() -> Vec<PropertySpec> {
    vec![
        PropertySpec::text("url")
            .required()
            .help("The HTTP interface, e.g. http://clickhouse.local:8123 or https://...:8443."),
        PropertySpec::text("username").help("Unset: default."),
        PropertySpec::text("password").help("Use ${SECRET:name} rather than the value itself."),
        PropertySpec::text("database").help("Unset: the user's default database."),
        PropertySpec::integer("timeout_ms")
            .default(JsonValue::from(300_000))
            .help("How long to wait for a server's first answer to a request."),
    ]
}

/// Where requests go, and as whom.
#[derive(Clone)]
pub(crate) struct Server {
    pub(crate) url: String,
    authorization: String,
    user: String,
    pub(crate) database: Option<String>,
    timeout: Duration,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("url", &self.url)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

impl Server {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let url = text(properties, "url")
            .map(|url| url.trim().trim_end_matches('/').to_string())
            .ok_or_else(|| ConnectorError::property("url", "is required"))?;
        if crate::aws::host_of(&url).is_none() || url.contains('?') {
            return Err(ConnectorError::property(
                "url",
                format!("'{url}' is not http://host[:port] or https://host[:port]"),
            ));
        }
        let user = text(properties, "username")
            .unwrap_or("default")
            .trim()
            .to_string();
        let password = text(properties, "password").unwrap_or("");
        Ok(Server {
            url,
            authorization: format!("Basic {}", base64(&format!("{user}:{password}"))),
            user,
            database: text(properties, "database").map(|d| d.trim().to_string()),
            timeout: Duration::from_millis(positive(properties, "timeout_ms", 300_000)?),
        })
    }

    /// `url, as user`, for messages. Never the password.
    pub(crate) fn place(&self) -> String {
        format!("{} as {}", self.url, self.user)
    }

    /// The query string every request carries, and `extra`.
    fn query(&self, extra: &[(String, String)]) -> Vec<(String, String)> {
        let mut query: Vec<(String, String)> = SESSION
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        if let Some(database) = &self.database {
            query.push(("database".into(), database.clone()));
        }
        query.extend(extra.iter().cloned());
        query
    }
}

/// ClickHouse's own message, `Code: N. DB::Exception: ... (NAME) (version ...)`,
/// without the version.
fn clickhouse_error(text: &str) -> String {
    let text = text.trim();
    let text = text.split(" (version ").next().unwrap_or(text);
    snippet(text)
}

/// A table name as SQL: `table` or `database.table`, backticked.
pub(crate) fn table_name(name: &str) -> Result<String, ConnectorError> {
    let parts: Vec<&str> = name.split('.').map(str::trim).collect();
    if parts.len() > 2 || parts.iter().any(|part| part.is_empty()) {
        return Err(ConnectorError::property(
            "table",
            format!("'{name}' is not table or database.table"),
        ));
    }
    Ok(parts
        .iter()
        .map(|part| identifier(part))
        .collect::<Vec<_>>()
        .join("."))
}

pub(crate) fn identifier(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/// The type inside `Nullable(...)` and `LowCardinality(...)`.
pub(crate) fn bare_type(kind: &str) -> &str {
    let mut kind = kind.trim();
    loop {
        let inner = ["Nullable(", "LowCardinality("]
            .iter()
            .find_map(|wrapper| kind.strip_prefix(wrapper).and_then(|k| k.strip_suffix(')')));
        match inner {
            Some(inner) => kind = inner.trim(),
            None => return kind,
        }
    }
}

/// A value as a row holds it: a quoted wide integer made a number where it
/// fits; everything else as ClickHouse wrote it.
pub(crate) fn value(kind: &str, value: JsonValue) -> JsonValue {
    let kind = bare_type(kind);
    let wide = matches!(
        kind,
        "Int64" | "UInt64" | "Int128" | "UInt128" | "Int256" | "UInt256"
    );
    match value {
        JsonValue::String(text) if wide => {
            if let Ok(number) = text.parse::<i64>() {
                json!(number)
            } else if let Ok(number) = text.parse::<u64>() {
                json!(number)
            } else {
                JsonValue::String(text)
            }
        }
        other => other,
    }
}

/// Whether a line of results is ClickHouse reporting an error mid-stream: a
/// one-element row holding its exception text.
fn exception_row(cells: &[JsonValue]) -> Option<&str> {
    match cells {
        [JsonValue::String(text)]
            if text.starts_with("Code: ") && text.contains("DB::Exception") =>
        {
            Some(text)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------

impl Source for ClickhouseSource {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties();
        properties.extend([
            PropertySpec::text("table").help("table or database.table. Or give query."),
            PropertySpec::code("query").help("SQL to read, instead of table."),
            PropertySpec::text("incremental_column").help(
                "Read only rows whose value here is above the last successful run's highest. It \
                 must only ever go up.",
            ),
            PropertySpec::text("start").help(
                "With incremental_column, where the first run starts, as a SQL literal, e.g. \
                 toDateTime64('2026-01-01 00:00:00', 6) or 1000. Unset: from the beginning.",
            ),
            PropertySpec::integer("max_records").help("The most one run reads. Unset: every row."),
            columns_property(),
        ]);
        ComponentSpec::new("src.db.clickhouse", "ClickHouse table")
            .description(
                "Read a ClickHouse table or query over the HTTP interface, streamed, all of it or \
                 only rows new since the last successful run.",
            )
            .icon("database")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        Server::from(properties)?;
        SourceSettings::from(properties).map(|_| ())
    }

    fn read(
        &self,
        properties: &JsonValue,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let server = Server::from(properties)?;
        let settings = SourceSettings::from(properties)?;
        read(&server, &settings, out, context.checkpoint.as_ref())
    }
}

#[derive(Debug)]
pub(crate) struct SourceSettings {
    pub(crate) what: String,
    pub(crate) sql: String,
    pub(crate) incremental: Option<(String, Option<String>)>,
    pub(crate) max_records: Option<u64>,
}

impl SourceSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let (what, sql) = match (text(properties, "table"), text(properties, "query")) {
            (Some(_), Some(_)) => {
                return Err(ConnectorError::property(
                    "query",
                    "and table both say what to read; give one",
                ))
            }
            (Some(table), None) => {
                let table = table_name(table.trim())?;
                (format!("table {table}"), format!("SELECT * FROM {table}"))
            }
            (None, Some(query)) => {
                let query = query.trim().trim_end_matches(';').to_string();
                if query.to_ascii_uppercase().contains(" FORMAT ") {
                    return Err(ConnectorError::property(
                        "query",
                        "names its own FORMAT; the connector chooses the format it reads",
                    ));
                }
                (
                    format!("query {}", crate::bigquery::fingerprint(&query)),
                    query,
                )
            }
            (None, None) => return Err(ConnectorError::property("table", "or query is required")),
        };
        let incremental = match text(properties, "incremental_column").map(str::trim) {
            None => {
                if text(properties, "start").is_some() {
                    return Err(ConnectorError::property(
                        "start",
                        "goes with incremental_column, which is not set",
                    ));
                }
                None
            }
            Some(column) => Some((
                column.to_string(),
                text(properties, "start").map(|start| start.trim().to_string()),
            )),
        };
        let max_records = match properties.get("max_records") {
            None | Some(JsonValue::Null) => None,
            Some(_) => Some(positive(properties, "max_records", 1)?),
        };
        Ok(SourceSettings {
            what,
            sql,
            incremental,
            max_records,
        })
    }
}

/// The SQL this run sends, its parameters, and a note when a saved position
/// was set aside.
pub(crate) fn statement(
    settings: &SourceSettings,
    saved: Option<&JsonValue>,
) -> (String, Vec<(String, String)>, Option<String>) {
    let limit = settings
        .max_records
        .map_or(String::new(), |cap| format!(" LIMIT {cap}"));
    let format = " FORMAT JSONCompactEachRowWithNamesAndTypes";
    let Some((column, start)) = &settings.incremental else {
        return (format!("{}{limit}{format}", settings.sql), Vec::new(), None);
    };
    let name = identifier(column);
    let inner = &settings.sql;
    let mine = saved
        .filter(|saved| saved["read"] == json!(settings.what) && saved["column"] == json!(column));
    let note = match (saved, mine) {
        (Some(saved), None) => Some(format!(
            "; the saved position was for {} by '{}', so this read started over",
            saved["read"].as_str().unwrap_or("?"),
            saved["column"].as_str().unwrap_or("?")
        )),
        _ => None,
    };
    match (mine, start) {
        (Some(saved), _) => (
            format!(
                "SELECT * FROM ({inner}) WHERE {name} > {{etl_after:{}}} ORDER BY {name}{limit}{format}",
                saved["type"].as_str().unwrap_or("String")
            ),
            vec![(
                "param_etl_after".to_string(),
                saved["value"].as_str().unwrap_or_default().to_string(),
            )],
            note,
        ),
        (None, Some(start)) => (
            format!("SELECT * FROM ({inner}) WHERE {name} > ({start}) ORDER BY {name}{limit}{format}"),
            Vec::new(),
            note,
        ),
        (None, None) => (
            format!("SELECT * FROM ({inner}) ORDER BY {name}{limit}{format}"),
            Vec::new(),
            note,
        ),
    }
}

/// Run the read and stream its rows.
pub(crate) fn read(
    server: &Server,
    settings: &SourceSettings,
    out: &mut dyn RecordWriter,
    saved: Option<&JsonValue>,
) -> Result<Summary, ConnectorError> {
    let (sql, parameters, note) = statement(settings, saved);
    let failed = |error: String| {
        ConnectorError::Data(format!("{} at {}: {error}", settings.what, server.place()))
    };

    let agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_connect(Some(server.timeout))
        .timeout_recv_response(Some(server.timeout))
        .user_agent(concat!("etl/", env!("CARGO_PKG_VERSION")))
        .build()
        .new_agent();
    let mut request = agent
        .post(&format!("{}/", server.url))
        .header("Authorization", &server.authorization);
    for (key, value) in server.query(&parameters) {
        request = request.query(&key, &value);
    }
    let mut response = request
        .send(sql.as_bytes())
        .map_err(|error| failed(format!("could not reach the server: {error}")))?;
    let status = response.status().as_u16();
    if status != 200 {
        let body = response.body_mut().read_to_string().unwrap_or_default();
        // An error answer can still start with the names and types lines.
        let message = body
            .lines()
            .rev()
            .find(|line| line.contains("DB::Exception"))
            .map(|line| {
                line.trim_matches(|c| c == '[' || c == ']' || c == '"')
                    .to_string()
            })
            .unwrap_or(body);
        return Err(failed(format!(
            "HTTP {status}: {}",
            clickhouse_error(&message)
        )));
    }

    let reader = std::io::BufReader::new(response.body_mut().as_reader());
    let mut lines = reader.lines();
    let mut header = |what: &str| -> Result<Vec<String>, ConnectorError> {
        let line = lines
            .next()
            .transpose()
            .map_err(|error| failed(error.to_string()))?
            .ok_or_else(|| failed(format!("the answer has no {what} line")))?;
        serde_json::from_str(&line)
            .map_err(|_| failed(format!("the {what} line is not JSON: {}", snippet(&line))))
    };
    let names = header("names")?;
    let kinds = header("types")?;

    let position = match &settings.incremental {
        None => None,
        Some((column, _)) => Some(names.iter().position(|name| name == column).ok_or_else(
            || {
                ConnectorError::property(
                    "incremental_column",
                    format!("'{column}' is not a column of what is read"),
                )
            },
        )?),
    };

    let mut count = 0u64;
    let mut highest: Option<JsonValue> = None;
    for line in lines {
        let line = line.map_err(|error| failed(format!("reading the result: {error}")))?;
        if line.trim().is_empty() {
            continue;
        }
        let cells: Vec<JsonValue> = serde_json::from_str(&line)
            .map_err(|_| failed(format!("a row is not JSON: {}", snippet(&line))))?;
        if cells.len() != names.len() || names.len() == 1 {
            if let Some(exception) = exception_row(&cells) {
                return Err(failed(clickhouse_error(exception)));
            }
        }
        if cells.len() != names.len() {
            return Err(failed(format!(
                "a row has {} values for {} columns",
                cells.len(),
                names.len()
            )));
        }
        let mut row = Map::new();
        for ((name, kind), cell) in names.iter().zip(kinds.iter()).zip(cells) {
            row.insert(name.clone(), value(kind, cell));
        }
        if let Some(index) = position {
            highest = row.get(&names[index]).cloned();
        }
        out.write(row)?;
        count += 1;
    }

    let mut detail = format!(
        "{count} row(s) from {} at {}",
        settings.what,
        server.place()
    );
    let mut checkpoint = None;
    if let (Some((column, _)), Some(index)) = (&settings.incremental, position) {
        match highest {
            Some(value) if !value.is_null() => {
                let text = match &value {
                    JsonValue::String(text) => text.clone(),
                    other => other.to_string(),
                };
                detail.push_str(&format!("; read up to {column} = {text}"));
                checkpoint = Some(json!({
                    "read": settings.what, "column": column,
                    "type": bare_type(&kinds[index]), "value": text,
                }));
            }
            _ => detail.push_str(&format!("; nothing new by {column}")),
        }
    }
    if settings.max_records == Some(count) {
        detail.push_str("; stopped at max_records, with more for the next run");
    }
    if let Some(note) = note {
        detail.push_str(&note);
    }
    let mut summary = Summary::new(count, detail);
    summary.checkpoint = checkpoint;
    Ok(summary)
}

// ---------------------------------------------------------------------------
// The sink
// ---------------------------------------------------------------------------

impl Sink for ClickhouseSink {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties();
        properties.extend([
            PropertySpec::text("table")
                .required()
                .help("table or database.table. It must exist."),
            PropertySpec::enumerated("mode", &["append", "truncate"])
                .default(JsonValue::String("append".into()))
                .help("append adds the rows; truncate empties the table first."),
        ]);
        ComponentSpec::new("snk.db.clickhouse", "ClickHouse table")
            .description(
                "Write rows to an existing ClickHouse table over the HTTP interface, as \
                 JSONEachRow, 100,000 rows or 16 MB an INSERT.",
            )
            .icon("database")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        Server::from(properties)?;
        SinkSettings::from(properties).map(|_| ())
    }

    fn write(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        _context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let server = Server::from(properties)?;
        let settings = SinkSettings::from(properties)?;
        insert(&server, &settings, input, INSERT_ROWS, INSERT_BYTES)
    }
}

#[derive(Debug)]
pub(crate) struct SinkSettings {
    pub(crate) table: String,
    pub(crate) truncate: bool,
}

impl SinkSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        if text(properties, "query").is_some() {
            return Err(ConnectorError::property(
                "query",
                "is for reading; a sink writes to table",
            ));
        }
        let table = table_name(
            text(properties, "table")
                .ok_or_else(|| ConnectorError::property("table", "is required"))?
                .trim(),
        )?;
        let truncate = match text(properties, "mode").unwrap_or("append") {
            "append" => false,
            "truncate" => true,
            other => {
                return Err(ConnectorError::property(
                    "mode",
                    format!("'{other}' is not one of append, truncate"),
                ))
            }
        };
        Ok(SinkSettings { table, truncate })
    }
}

/// One statement with a body: an INSERT's rows, or nothing.
fn execute(
    client: &mut Client,
    server: &Server,
    sql: &str,
    rows: &[u8],
    token: Option<String>,
) -> Result<(), ConnectorError> {
    let mut extra_query = vec![("query".to_string(), sql.to_string())];
    if let Some(token) = token {
        extra_query.push(("insert_deduplication_token".to_string(), token));
    }
    let query = server.query(&extra_query);
    let authorization = server.authorization.clone();
    let headers = || vec![("Authorization".to_string(), authorization.clone())];
    let extra = Extra {
        headers: &headers,
        content_type: "application/x-ndjson",
        throttled: &|_, _| false,
    };
    client
        .send_with(
            &format!("{}/", server.url),
            &query,
            Some(rows),
            Some(&extra),
            Judged::Accept,
        )
        .map(|_| ())
        .map_err(|error| ConnectorError::Data(clickhouse_error(&error.to_string())))
}

/// Every row of `input` into the table, an `INSERT` per batch.
pub(crate) fn insert(
    server: &Server,
    settings: &SinkSettings,
    input: &mut dyn RecordReader,
    batch_rows: usize,
    batch_bytes: usize,
) -> Result<Summary, ConnectorError> {
    let table = &settings.table;
    let mut client = Client::new(Settings::signed(
        format!("{}/", server.url),
        Method::Post,
        server.timeout,
        3,
    ));
    let mut inserted = 0u64;
    let mut statements = 0u64;
    let failed = |inserted: u64, error: ConnectorError| {
        ConnectorError::Data(format!(
            "{error}. {inserted} row(s) had been inserted into {table} before this, and stay"
        ))
    };
    if settings.truncate {
        execute(
            &mut client,
            server,
            &format!("TRUNCATE TABLE {table}"),
            b"",
            None,
        )
        .map_err(|error| failed(0, error))?;
    }

    let sql = format!("INSERT INTO {table} FORMAT JSONEachRow");
    // A token per batch: a batch sent twice is kept once where the table
    // deduplicates inserts (Replicated*, or non_replicated_deduplication_window).
    let run = crate::bigquery::fingerprint(&format!("{table}{:?}", std::time::SystemTime::now()));
    let mut batch: Vec<u8> = Vec::new();
    let mut in_batch = 0usize;
    while let Some(record) = input.read()? {
        let mut line = serde_json::to_vec(&record)
            .map_err(|error| failed(inserted, ConnectorError::Data(error.to_string())))?;
        line.push(b'\n');
        if in_batch > 0 && (in_batch == batch_rows || batch.len() + line.len() > batch_bytes) {
            execute(
                &mut client,
                server,
                &sql,
                &batch,
                Some(format!("{run}-{statements}")),
            )
            .map_err(|error| failed(inserted, error))?;
            inserted += in_batch as u64;
            statements += 1;
            batch.clear();
            in_batch = 0;
        }
        batch.extend_from_slice(&line);
        in_batch += 1;
    }
    if in_batch > 0 {
        execute(
            &mut client,
            server,
            &sql,
            &batch,
            Some(format!("{run}-{statements}")),
        )
        .map_err(|error| failed(inserted, error))?;
        inserted += in_batch as u64;
        statements += 1;
    }

    let detail = format!(
        "{inserted} row(s) {} {table} at {} in {statements} INSERT(s)",
        if settings.truncate {
            "replaced the rows of"
        } else {
            "appended to"
        },
        server.place()
    );
    Ok(Summary::new(inserted, detail))
}
