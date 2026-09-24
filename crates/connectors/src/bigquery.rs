//! Google BigQuery: a table or a query read through a query job, page by
//! page, all of it or only what is new since the last successful run; and a
//! table written to by load jobs.
//!
//! **Reading** (Settled decision 75) is `jobs.query`, polled while the job
//! runs, then `getQueryResults` page by page. Rows come back as BigQuery's
//! own JSON (`{"f": [{"v": ...}]}`, every value text) and are typed by the
//! result's schema: INT64 a number, NUMERIC exact text, TIMESTAMP a UTC
//! timestamp to the microsecond, RECORD an object, REPEATED an array.
//!
//! **Only what is new**: with `incremental_column`, the query is wrapped as
//! `SELECT * FROM (<query>) WHERE col > @etl_after ORDER BY col`, the last
//! run's highest value passed as a **typed query parameter**, never pasted into
//! the SQL; the highest value read becomes the checkpoint, saved only if the
//! whole run succeeds. The first run starts at `start`, a GoogleSQL literal the
//! node's author writes (`TIMESTAMP '2026-01-01'`), or at the beginning.
//!
//! **Writing** is a **load job** per batch of rows as newline-delimited JSON,
//! sent as a multipart upload and polled until done: free, where streaming
//! inserts are billed. The table must exist. `truncate` replaces the table
//! with the first load job and appends the rest, so a failure part-way leaves
//! the rows loaded until then, which the error says.
//!
//! Signing in is [`crate::gcp`]'s, as for Pub/Sub: a service account, gcloud's
//! login, or nothing at all for a plain-`http://` endpoint (the emulator).

use crate::aws::{host_of, Sources};
use crate::gcp::{self, Credentials, Tokens};
use crate::http::{base64_bytes, positive, snippet, text, Client, Extra, Judged, Method, Settings};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordReader, RecordWriter, Sink, Source,
    Summary,
};
use serde_json::{json, Map, Value as JsonValue};
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// `src.warehouse.bigquery`.
pub struct BigquerySource;

/// `snk.warehouse.bigquery`.
pub struct BigquerySink;

/// The OAuth scope every call needs.
const SCOPE: &str = "https://www.googleapis.com/auth/bigquery";

/// Where BigQuery is when nothing says otherwise.
const DEFAULT_ENDPOINT: &str = "https://bigquery.googleapis.com";

/// Rows a page of results.
const PAGE_ROWS: u64 = 10_000;

/// How long one poll asks the server to wait for a job, in milliseconds.
const POLL_WAIT_MS: u64 = 10_000;

/// The most bytes of rows one load job carries: a multipart upload is meant
/// for bodies small enough to send again whole.
pub(crate) const LOAD_BYTES: usize = 4_000_000;

/// The parameter an incremental read compares with.
const AFTER: &str = "etl_after";

// ---------------------------------------------------------------------------
// The API
// ---------------------------------------------------------------------------

fn connection_properties() -> Vec<PropertySpec> {
    vec![
        PropertySpec::text("project")
            .required()
            .help("The Google Cloud project that runs the jobs, and owns the table unless dataset says otherwise."),
        PropertySpec::text("dataset").help("With table: the dataset, or project.dataset for another project's."),
        PropertySpec::text("table").help("The table's name within the dataset."),
        PropertySpec::text("location").help(
            "Where the dataset lives, e.g. EU or europe-west2. Unset: BigQuery works it out.",
        ),
        PropertySpec::text("credentials_file").help(
            "A service account's JSON key file, or gcloud's login file. Unset: \
             GOOGLE_APPLICATION_CREDENTIALS, then gcloud's application-default login.",
        ),
        PropertySpec::text("endpoint").help(
            "Only for the emulator or a private endpoint. A plain http:// endpoint signs nothing. \
             Unset: https://bigquery.googleapis.com.",
        ),
        PropertySpec::integer("timeout_ms")
            .default(JsonValue::from(120_000))
            .help("How long one request may take."),
        PropertySpec::integer("retries")
            .default(JsonValue::from(5))
            .help("Extra attempts after a 429, a 5xx or a network failure."),
    ]
}

fn required<'a>(properties: &'a JsonValue, key: &str) -> Result<&'a str, ConnectorError> {
    text(properties, key)
        .map(str::trim)
        .ok_or_else(|| ConnectorError::property(key, "is required"))
}

fn endpoint_of(properties: &JsonValue) -> Result<String, ConnectorError> {
    let endpoint = text(properties, "endpoint")
        .map(|endpoint| endpoint.trim().trim_end_matches('/').to_string())
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
    if host_of(&endpoint).is_none() {
        return Err(ConnectorError::property(
            "endpoint",
            format!("'{endpoint}' is not http://host[:port] or https://host[:port]"),
        ));
    }
    Ok(endpoint)
}

/// A table named by the properties: `project.dataset.table`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Table {
    pub(crate) project: String,
    pub(crate) dataset: String,
    pub(crate) table: String,
}

impl Table {
    fn from(properties: &JsonValue) -> Result<Option<Self>, ConnectorError> {
        let project = required(properties, "project")?;
        match (text(properties, "dataset"), text(properties, "table")) {
            (None, None) => Ok(None),
            (Some(_), None) => Err(ConnectorError::property(
                "table",
                "is required with dataset",
            )),
            (None, Some(_)) => Err(ConnectorError::property(
                "dataset",
                "is required with table",
            )),
            (Some(dataset), Some(table)) => {
                let (project, dataset) = match dataset.trim().split_once('.') {
                    Some((owner, dataset)) => (owner.to_string(), dataset.to_string()),
                    None => (project.to_string(), dataset.trim().to_string()),
                };
                let table = table.trim().to_string();
                for (property, part) in [("dataset", &dataset), ("table", &table)] {
                    if part.is_empty() || part.contains(['`', '.', ' ']) {
                        return Err(ConnectorError::property(
                            property,
                            format!("'{part}' is not a {property} name"),
                        ));
                    }
                }
                Ok(Some(Table {
                    project,
                    dataset,
                    table,
                }))
            }
        }
    }

    /// `` `project.dataset.table` `` for SQL.
    fn quoted(&self) -> String {
        format!("`{}.{}.{}`", self.project, self.dataset, self.table)
    }

    fn name(&self) -> String {
        format!("{}.{}.{}", self.project, self.dataset, self.table)
    }
}

/// A signed-in client for BigQuery's REST API.
pub(crate) struct Api {
    post: Client,
    get: Client,
    endpoint: String,
    tokens: Tokens,
    project: String,
    location: Option<String>,
}

impl Api {
    pub(crate) fn connect(
        properties: &JsonValue,
        sources: &Sources,
    ) -> Result<Self, ConnectorError> {
        let endpoint = endpoint_of(properties)?;
        let credentials = if endpoint.starts_with("http://") {
            Credentials::nobody()
        } else {
            gcp::credentials(properties, sources)?
        };
        let timeout = Duration::from_millis(positive(properties, "timeout_ms", 120_000)?);
        let retries = properties
            .get("retries")
            .and_then(JsonValue::as_u64)
            .unwrap_or(5) as u32;
        Ok(Api {
            post: Client::new(Settings::signed(
                endpoint.clone(),
                Method::Post,
                timeout,
                retries,
            )),
            get: Client::new(Settings::signed(
                endpoint.clone(),
                Method::Get,
                timeout,
                retries,
            )),
            tokens: Tokens::new(credentials, SCOPE, timeout, retries),
            project: required(properties, "project")?.to_string(),
            location: text(properties, "location").map(|l| l.trim().to_string()),
            endpoint,
        })
    }

    pub(crate) fn signed_in_as(&self) -> &str {
        self.tokens.source()
    }

    fn base(&self) -> String {
        format!("{}/bigquery/v2/projects/{}", self.endpoint, self.project)
    }

    /// One call. `body` of `None` is a GET; otherwise a POST of JSON, or of
    /// `raw` bytes with their content type (an upload).
    fn call(
        &mut self,
        what: &str,
        url: &str,
        query: &[(String, String)],
        body: Option<(&[u8], &str)>,
    ) -> Result<JsonValue, ConnectorError> {
        let authorization = self.tokens.authorization()?;
        let headers = || {
            authorization
                .iter()
                .map(|value| ("Authorization".to_string(), value.clone()))
                .collect()
        };
        let content_type = body.map_or("application/json", |(_, kind)| kind);
        let extra = Extra {
            headers: &headers,
            content_type,
            throttled: &|_, _| false,
        };
        let client = if body.is_some() {
            &mut self.post
        } else {
            &mut self.get
        };
        let reply = client
            .send_with(
                url,
                query,
                body.map(|(bytes, _)| bytes),
                Some(&extra),
                Judged::Accept,
            )
            .map_err(|error| {
                ConnectorError::Data(format!(
                    "BigQuery {what}: {}",
                    google_error(&error.to_string())
                ))
            })?;
        if reply.body.trim().is_empty() {
            return Ok(JsonValue::Object(Map::new()));
        }
        serde_json::from_str(&reply.body).map_err(|error| {
            ConnectorError::Data(format!("BigQuery {what}: the answer is not JSON: {error}"))
        })
    }

    fn location_query(&self) -> Vec<(String, String)> {
        self.location
            .iter()
            .map(|location| ("location".to_string(), location.clone()))
            .collect()
    }
}

/// Google's error JSON, `{"error": {"message": ...}}`, down to its message,
/// where the HTTP layer quoted the whole body.
fn google_error(text: &str) -> String {
    if let Some(start) = text.find('{') {
        if let Ok(body) = serde_json::from_str::<JsonValue>(&text[start..]) {
            if let Some(message) = body["error"]["message"].as_str() {
                return format!("{}{message}", &text[..start]);
            }
        }
    }
    text.to_string()
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/// A field of a result's schema.
#[derive(Debug, Clone)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) repeated: bool,
    pub(crate) fields: Vec<Field>,
}

impl Field {
    pub(crate) fn list(schema: &JsonValue) -> Vec<Field> {
        schema["fields"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|field| Field {
                name: field["name"].as_str().unwrap_or_default().to_string(),
                kind: field["type"]
                    .as_str()
                    .unwrap_or("STRING")
                    .to_ascii_uppercase(),
                repeated: field["mode"].as_str() == Some("REPEATED"),
                fields: Field::list(field),
            })
            .collect()
    }
}

/// One row of results, typed by the schema.
pub(crate) fn row(fields: &[Field], cells: &JsonValue) -> Result<Record, ConnectorError> {
    let cells = cells["f"].as_array().cloned().unwrap_or_default();
    let mut row = Map::new();
    for (field, cell) in fields.iter().zip(cells.iter()) {
        row.insert(field.name.clone(), cell_value(field, &cell["v"])?);
    }
    Ok(row)
}

fn cell_value(field: &Field, value: &JsonValue) -> Result<JsonValue, ConnectorError> {
    if field.repeated {
        let items = value.as_array().cloned().unwrap_or_default();
        let single = Field {
            repeated: false,
            ..field.clone()
        };
        return items
            .iter()
            .map(|item| cell_value(&single, &item["v"]))
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array);
    }
    if value.is_null() {
        return Ok(JsonValue::Null);
    }
    if matches!(field.kind.as_str(), "RECORD" | "STRUCT") {
        return row(&field.fields, value).map(JsonValue::Object);
    }
    let text = value.as_str().unwrap_or_default();
    let bad = || {
        ConnectorError::Data(format!(
            "BigQuery gave '{text}' for {} column '{}'",
            field.kind, field.name
        ))
    };
    Ok(match field.kind.as_str() {
        "INTEGER" | "INT64" => match text.parse::<i64>() {
            Ok(number) => json!(number),
            Err(_) => return Err(bad()),
        },
        "FLOAT" | "FLOAT64" => match text.parse::<f64>() {
            Ok(number) if number.is_finite() => json!(number),
            // NaN and the infinities are not JSON: kept as BigQuery's text.
            Ok(_) => json!(text),
            Err(_) => return Err(bad()),
        },
        "BOOLEAN" | "BOOL" => json!(text.eq_ignore_ascii_case("true")),
        "TIMESTAMP" => json!(micros_text(timestamp_micros(text).ok_or_else(bad)?)),
        "DATETIME" => json!(text.replacen('T', " ", 1)),
        // NUMERIC, BIGNUMERIC, DATE, TIME, STRING, BYTES (base64 already),
        // JSON, GEOGRAPHY, INTERVAL: BigQuery's text is the exact value.
        _ => json!(text),
    })
}

/// A TIMESTAMP cell in microseconds since 1970. BigQuery writes it as whole
/// microseconds (`formatOptions.useInt64Timestamp`), or as seconds with a
/// fraction, or in scientific notation (`1.790244000123456E9`). Done in
/// text, exactly: a double cannot hold today's microseconds.
pub(crate) fn timestamp_micros(text: &str) -> Option<i64> {
    let text = text.trim();
    let (mantissa, exponent) = match text.split_once(['E', 'e']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None if text.contains('.') => (text, 0),
        // A plain integer is already microseconds, as asked for.
        None => return text.parse().ok(),
    };
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.trim_start_matches(['-', '+']);
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // The decimal point sits after `whole.len() + exponent` digits; seconds
    // to microseconds moves it six further.
    let point = whole.len() as i32 + exponent + 6;
    let micros_digits = if point <= 0 {
        "0".to_string()
    } else if point as usize >= digits.len() {
        format!("{digits}{}", "0".repeat(point as usize - digits.len()))
    } else {
        digits[..point as usize].to_string()
    };
    let micros: i64 = micros_digits.parse().ok()?;
    Some(if negative { -micros } else { micros })
}

/// Microseconds since 1970 as `YYYY-MM-DD HH:MM:SS.ffffff`, UTC.
pub(crate) fn micros_text(micros: i64) -> String {
    const DAY: i64 = 86_400_000_000;
    let (year, month, day) = etl_state::time::civil_from_days(micros.div_euclid(DAY));
    let within = micros.rem_euclid(DAY);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{:06}",
        within / 3_600_000_000,
        within / 60_000_000 % 60,
        within / 1_000_000 % 60,
        within % 1_000_000
    )
}

// ---------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------

impl Source for BigquerySource {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties();
        properties.extend([
            PropertySpec::code("query").help(
                "GoogleSQL to read, instead of dataset and table. Standard SQL, not legacy.",
            ),
            PropertySpec::text("incremental_column").help(
                "Read only rows whose value here is above the last successful run's highest. It \
                 must only ever go up: a load time, an increasing id.",
            ),
            PropertySpec::text("start").help(
                "With incremental_column, where the first run starts, as a GoogleSQL literal, e.g. \
                 TIMESTAMP '2026-01-01 00:00:00' or 1000. Unset: from the beginning.",
            ),
            PropertySpec::integer("max_records")
                .help("The most one run reads. Unset: every row."),
            columns_property(),
        ]);
        ComponentSpec::new("src.warehouse.bigquery", "BigQuery table")
            .description(
                "Read a BigQuery table or query through a query job, all of it or only rows new \
                 since the last successful run.",
            )
            .icon("warehouse")
            .properties(properties)
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
        let mut api = Api::connect(properties, &Sources::process())?;
        read(&mut api, &settings, out, context.checkpoint.as_ref())
    }
}

#[derive(Debug)]
pub(crate) struct SourceSettings {
    /// What is read: a table's name, or the query, for the report and the
    /// checkpoint.
    pub(crate) what: String,
    pub(crate) sql: String,
    pub(crate) incremental: Option<(String, Option<String>)>,
    pub(crate) max_records: Option<u64>,
}

impl SourceSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        required(properties, "project")?;
        endpoint_of(properties)?;
        let table = Table::from(properties)?;
        let query = text(properties, "query").map(str::trim);
        let (what, sql) = match (table, query) {
            (Some(_), Some(_)) => {
                return Err(ConnectorError::property(
                    "query",
                    "and dataset and table both say what to read; give one",
                ))
            }
            (Some(table), None) => (
                format!("table {}", table.name()),
                format!("SELECT * FROM {}", table.quoted()),
            ),
            (None, Some(query)) => (
                format!("query {}", fingerprint(query)),
                query.trim_end_matches(';').to_string(),
            ),
            (None, None) => {
                return Err(ConnectorError::property(
                    "table",
                    "or query is required: dataset and table, or GoogleSQL",
                ))
            }
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
            Some(column) => {
                if column.contains('`') {
                    return Err(ConnectorError::property(
                        "incremental_column",
                        "is a column name, without backticks",
                    ));
                }
                Some((
                    column.to_string(),
                    text(properties, "start").map(|start| start.trim().to_string()),
                ))
            }
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

/// A short, stable name for a query: its SHA-256, first sixteen hex digits.
fn fingerprint(query: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, query.as_bytes());
    digest.as_ref()[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The GoogleSQL types an incremental column may have, and so a parameter.
const COMPARABLE: [&str; 10] = [
    "INTEGER",
    "INT64",
    "NUMERIC",
    "BIGNUMERIC",
    "FLOAT",
    "FLOAT64",
    "TIMESTAMP",
    "DATETIME",
    "DATE",
    "STRING",
];

/// The SQL this run sends, and its parameters, from the settings and what the
/// last run saved. And a note when a saved position was set aside.
pub(crate) fn statement(
    settings: &SourceSettings,
    saved: Option<&JsonValue>,
) -> (String, Vec<JsonValue>, Option<String>) {
    let Some((column, start)) = &settings.incremental else {
        return (settings.sql.clone(), Vec::new(), None);
    };
    let quoted = format!("`{column}`");
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
            format!("SELECT * FROM ({inner}) WHERE {quoted} > @{AFTER} ORDER BY {quoted}"),
            vec![json!({
                "name": AFTER,
                "parameterType": { "type": saved["type"] },
                "parameterValue": { "value": saved["value"] },
            })],
            note,
        ),
        (None, Some(start)) => (
            format!("SELECT * FROM ({inner}) WHERE {quoted} > ({start}) ORDER BY {quoted}"),
            Vec::new(),
            note,
        ),
        (None, None) => (
            format!("SELECT * FROM ({inner}) WHERE {quoted} IS NOT NULL ORDER BY {quoted}"),
            Vec::new(),
            note,
        ),
    }
}

/// A value as a query parameter takes it: TIMESTAMP as text BigQuery reads.
fn parameter_value(kind: &str, raw: &str) -> Option<String> {
    Some(match kind {
        "TIMESTAMP" => format!("{} UTC", micros_text(timestamp_micros(raw)?)),
        _ => raw.to_string(),
    })
}

/// Run the query, and every page of its results, as rows.
pub(crate) fn read(
    api: &mut Api,
    settings: &SourceSettings,
    out: &mut dyn RecordWriter,
    saved: Option<&JsonValue>,
) -> Result<Summary, ConnectorError> {
    let (sql, parameters, note) = statement(settings, saved);
    let mut request = json!({
        "query": sql,
        "useLegacySql": false,
        "maxResults": settings.max_records.map_or(PAGE_ROWS, |cap| cap.min(PAGE_ROWS)),
        "timeoutMs": POLL_WAIT_MS,
        "formatOptions": { "useInt64Timestamp": true },
    });
    if !parameters.is_empty() {
        request["parameterMode"] = json!("NAMED");
        request["queryParameters"] = json!(parameters);
    }
    if let Some(location) = &api.location {
        request["location"] = json!(location);
    }
    let base = api.base();
    let body =
        serde_json::to_vec(&request).map_err(|error| ConnectorError::Data(error.to_string()))?;
    let mut page = api
        .call(
            "query",
            &format!("{base}/queries"),
            &[],
            Some((&body, "application/json")),
        )
        .map_err(|error| {
            ConnectorError::Data(format!("{} ({}): {error}", settings.what, api.project))
        })?;
    let job = page["jobReference"]["jobId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if let Some(location) = page["jobReference"]["location"].as_str() {
        api.location.get_or_insert(location.to_string());
    }

    // Still running: ask again, each time waiting on the server's side.
    while page["jobComplete"] == json!(false) {
        let mut query = api.location_query();
        query.push(("timeoutMs".into(), POLL_WAIT_MS.to_string()));
        query.push(("maxResults".into(), PAGE_ROWS.to_string()));
        query.push(("formatOptions.useInt64Timestamp".into(), "true".into()));
        page = api.call(
            "getQueryResults",
            &format!("{base}/queries/{job}"),
            &query,
            None,
        )?;
    }
    if let Some(error) = page["errors"].as_array().and_then(|errors| errors.first()) {
        return Err(ConnectorError::Data(format!(
            "BigQuery job {job}: {}",
            error["message"].as_str().unwrap_or("failed")
        )));
    }

    let fields = Field::list(&page["schema"]);
    let incremental = settings.incremental.as_ref().map(|(column, _)| {
        let field = fields.iter().find(|field| &field.name == column).cloned();
        (column, field)
    });
    if let Some((column, field)) = &incremental {
        match field {
            None => {
                return Err(ConnectorError::property(
                    "incremental_column",
                    format!("'{column}' is not a column of what is read"),
                ))
            }
            Some(field) if field.repeated || !COMPARABLE.contains(&field.kind.as_str()) => {
                return Err(ConnectorError::property(
                    "incremental_column",
                    format!(
                        "'{column}' is {}, which cannot be compared to go on from",
                        field.kind
                    ),
                ))
            }
            Some(_) => {}
        }
    }

    let mut count = 0u64;
    let mut highest: Option<String> = None;
    let bytes = page["totalBytesProcessed"].as_str().map(str::to_string);
    'pages: loop {
        for cells in page["rows"].as_array().into_iter().flatten() {
            if settings.max_records == Some(count) {
                break 'pages;
            }
            if let Some((column, _)) = &incremental {
                let position = fields
                    .iter()
                    .position(|field| &field.name == *column)
                    .unwrap();
                highest = cells["f"][position]["v"].as_str().map(str::to_string);
            }
            out.write(row(&fields, cells)?)?;
            count += 1;
        }
        let Some(token) = page["pageToken"].as_str().map(str::to_string) else {
            break;
        };
        if settings.max_records == Some(count) {
            break;
        }
        let mut query = api.location_query();
        query.push(("pageToken".into(), token));
        query.push(("maxResults".into(), PAGE_ROWS.to_string()));
        query.push(("formatOptions.useInt64Timestamp".into(), "true".into()));
        page = api.call(
            "getQueryResults",
            &format!("{base}/queries/{job}"),
            &query,
            None,
        )?;
    }

    let mut detail = format!(
        "{count} row(s) from {} by job {job} ({})",
        settings.what,
        api.signed_in_as()
    );
    if let Some(bytes) = bytes {
        detail.push_str(&format!(", {bytes} bytes processed"));
    }
    let mut checkpoint = None;
    if let Some((column, Some(field))) = &incremental {
        match &highest {
            Some(raw) => {
                let value = parameter_value(&field.kind, raw).ok_or_else(|| {
                    ConnectorError::Data(format!("BigQuery gave '{raw}' for '{column}'"))
                })?;
                detail.push_str(&format!("; read up to {column} = {value}"));
                checkpoint = Some(json!({
                    "read": settings.what, "column": column,
                    "type": field.kind, "value": value,
                }));
            }
            None => detail.push_str(&format!("; nothing new by {column}")),
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

impl Sink for BigquerySink {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties();
        properties.push(
            PropertySpec::enumerated("mode", &["append", "truncate"])
                .default(JsonValue::String("append".into()))
                .help(
                    "append adds the rows; truncate replaces the table's rows with them. The \
                     table must exist.",
                ),
        );
        ComponentSpec::new("snk.warehouse.bigquery", "BigQuery table")
            .description(
                "Write rows to an existing BigQuery table through load jobs, as \
                 newline-delimited JSON: free, where streaming inserts are billed.",
            )
            .icon("warehouse")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        SinkSettings::from(properties).map(|_| ())
    }

    fn write(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        _context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = SinkSettings::from(properties)?;
        let mut api = Api::connect(properties, &Sources::process())?;
        load(&mut api, &settings, input, LOAD_BYTES)
    }
}

#[derive(Debug)]
pub(crate) struct SinkSettings {
    pub(crate) table: Table,
    pub(crate) truncate: bool,
}

impl SinkSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        endpoint_of(properties)?;
        if text(properties, "query").is_some() {
            return Err(ConnectorError::property(
                "query",
                "is for reading; a sink writes to dataset and table",
            ));
        }
        let table = Table::from(properties)?.ok_or_else(|| {
            ConnectorError::property("table", "is required, with dataset: where the rows go")
        })?;
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

/// What has been loaded so far, for the summary and a failure's message.
#[derive(Default)]
struct Loaded {
    rows: u64,
    jobs: Vec<String>,
}

impl Loaded {
    fn failed(&self, table: &Table, error: impl std::fmt::Display) -> ConnectorError {
        ConnectorError::Data(format!(
            "{error}. {} row(s) had been loaded into {} by {} job(s) before this, and stay",
            self.rows,
            table.name(),
            self.jobs.len()
        ))
    }
}

/// Every row of `input` into the table, a load job per `limit` bytes.
pub(crate) fn load(
    api: &mut Api,
    settings: &SinkSettings,
    input: &mut dyn RecordReader,
    limit: usize,
) -> Result<Summary, ConnectorError> {
    let mut loaded = Loaded::default();
    let mut batch: Vec<u8> = Vec::new();
    let mut batch_rows = 0u64;
    let mut row = 0u64;
    while let Some(record) = input.read()? {
        row += 1;
        let mut line = serde_json::to_vec(&record)
            .map_err(|error| loaded.failed(&settings.table, format!("row {row}: {error}")))?;
        line.push(b'\n');
        if !batch.is_empty() && batch.len() + line.len() > limit {
            let first = loaded.jobs.is_empty();
            run_load(
                api,
                settings,
                std::mem::take(&mut batch),
                batch_rows,
                first,
                &mut loaded,
            )?;
            batch_rows = 0;
        }
        batch.extend_from_slice(&line);
        batch_rows += 1;
    }
    // Truncating with nothing to load still empties the table.
    if !batch.is_empty() || (settings.truncate && loaded.jobs.is_empty()) {
        let first = loaded.jobs.is_empty();
        run_load(api, settings, batch, batch_rows, first, &mut loaded)?;
    }

    let detail = format!(
        "{} row(s) {} {} by {} load job(s) ({})",
        loaded.rows,
        if settings.truncate {
            "replaced the rows of"
        } else {
            "appended to"
        },
        settings.table.name(),
        loaded.jobs.len(),
        api.signed_in_as()
    );
    Ok(Summary::new(loaded.rows, detail))
}

/// One load job: the upload, then polled until BigQuery says it is done.
fn run_load(
    api: &mut Api,
    settings: &SinkSettings,
    rows: Vec<u8>,
    count: u64,
    first: bool,
    loaded: &mut Loaded,
) -> Result<(), ConnectorError> {
    let table = &settings.table;
    let disposition = if settings.truncate && first {
        "WRITE_TRUNCATE"
    } else {
        "WRITE_APPEND"
    };
    let mut configuration = json!({ "configuration": { "load": {
        "destinationTable": {
            "projectId": table.project, "datasetId": table.dataset, "tableId": table.table,
        },
        "sourceFormat": "NEWLINE_DELIMITED_JSON",
        "writeDisposition": disposition,
        "createDisposition": "CREATE_NEVER",
    }}});
    if let Some(location) = &api.location {
        configuration["jobReference"] = json!({ "location": location });
    }
    let boundary = format!(
        "etl-{}",
        base64_bytes(&count.to_be_bytes()).trim_end_matches('=')
    );
    let mut body = format!(
        "--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{configuration}\r\n\
         --{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n"
    )
    .into_bytes();
    body.extend_from_slice(&rows);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let url = format!(
        "{}/upload/bigquery/v2/projects/{}/jobs",
        api.endpoint, api.project
    );
    let content_type = format!("multipart/related; boundary={boundary}");
    let mut job = api
        .call(
            "load",
            &url,
            &[("uploadType".to_string(), "multipart".to_string())],
            Some((&body, &content_type)),
        )
        .map_err(|error| loaded.failed(table, error))?;
    let id = job["jobReference"]["jobId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if let Some(location) = job["jobReference"]["location"].as_str() {
        api.location.get_or_insert(location.to_string());
    }

    let started = Instant::now();
    let mut wait = Duration::from_millis(250);
    while job["status"]["state"].as_str() != Some("DONE") {
        std::thread::sleep(wait);
        wait = (wait * 2).min(Duration::from_secs(5));
        let base = api.base();
        let query = api.location_query();
        job = api
            .call("jobs.get", &format!("{base}/jobs/{id}"), &query, None)
            .map_err(|error| loaded.failed(table, error))?;
        if started.elapsed() > Duration::from_secs(3600) {
            return Err(loaded.failed(table, format!("load job {id} was not done after an hour")));
        }
    }
    if let Some(error) = job["status"]["errorResult"].as_object() {
        let detail = job["status"]["errors"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|error| error["message"].as_str())
            .take(3)
            .collect::<Vec<_>>()
            .join("; ");
        return Err(loaded.failed(
            table,
            format!(
                "BigQuery load job {id} failed: {}{}",
                error
                    .get("message")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("?"),
                if detail.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", snippet(&detail))
                }
            ),
        ));
    }
    loaded.rows += job["statistics"]["load"]["outputRows"]
        .as_str()
        .and_then(|rows| rows.parse().ok())
        .unwrap_or(count);
    loaded.jobs.push(id);
    Ok(())
}
