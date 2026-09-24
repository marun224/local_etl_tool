//! Snowflake: a table or a query read through the SQL API, partition by
//! partition, all of it or only what is new since the last successful run;
//! and a table written to with batched, bound `INSERT`s.
//!
//! **Signing in is a key pair** (Settled decision 77): a JWT signed RS256 with
//! the user's private key, 10k's signing from [`crate::gcp`], naming the account,
//! the user and the **SHA-256 fingerprint of the public key** Snowflake holds
//! for the user. The fingerprint is proved against `openssl`'s, computed the
//! way Snowflake's own documentation computes it.
//!
//! **No Snowflake runs here**: there is no emulator, and no account is used
//! (decision 78), so everything is proved against the local fixture and
//! recorded as not yet checked against real Snowflake.
//!
//! **Reading**: `POST /api/v2/statements`; a `202` means still running, and
//! the statement is polled until it answers `200`; then every result
//! **partition** after the first is fetched. Values come back as text and are
//! typed by the result's `rowType`: NUMBER with scale 0 a number, with scale
//! its exact text, TIMESTAMP_* a UTC timestamp to the microsecond, DATE and
//! TIME their text, VARIANT, OBJECT and ARRAY parsed JSON.
//!
//! **Only what is new**: `incremental_column` wraps the read as
//! `SELECT * FROM (<read>) WHERE col > CAST(? AS <type>) ORDER BY col`, the
//! last run's highest value a **bind variable**, never pasted into the SQL. The
//! session's time zone is UTC for every statement, so a timestamp means the
//! same both ways.
//!
//! **Writing**: `INSERT INTO t (cols) VALUES (?, ...)` with each column bound
//! to an array of values, a thousand rows a statement. The SQL API offers no
//! `PUT` or `COPY`, so this is the way in.

use crate::bigquery::micros_text;
use crate::gcp::{jwt, rsa_key};
use crate::http::{positive, snippet, text, Client, Extra, Judged, Method, Settings};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordReader, RecordWriter, Sink, Source,
    Summary,
};
use ring::digest;
use ring::signature::RsaKeyPair;
use serde_json::{json, Map, Value as JsonValue};
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// `src.warehouse.snowflake`.
pub struct SnowflakeSource;

/// `snk.warehouse.snowflake`.
pub struct SnowflakeSink;

/// Rows an `INSERT` binds at most.
const INSERT_ROWS: usize = 1000;

/// How long a signed-in JWT is good for. Snowflake allows at most an hour.
const JWT_SECONDS: i64 = 3540;

// ---------------------------------------------------------------------------
// Signing in
// ---------------------------------------------------------------------------

/// DER: a tag, a length, and the content.
fn der(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    match content.len() {
        n if n < 0x80 => out.push(n as u8),
        n if n < 0x100 => out.extend([0x81, n as u8]),
        n => out.extend([0x82, (n >> 8) as u8, n as u8]),
    }
    out.extend_from_slice(content);
    out
}

/// `SHA256:<base64>` of the public key's SubjectPublicKeyInfo, as Snowflake
/// shows it in `DESC USER` and as `openssl rsa -pubout -outform DER | openssl
/// dgst -sha256 -binary | base64` prints it.
pub(crate) fn fingerprint(key: &RsaKeyPair) -> String {
    // ring gives the PKCS#1 RSAPublicKey; the SPKI wraps it with the
    // rsaEncryption algorithm in a BIT STRING.
    let algorithm = der(
        0x30,
        &[
            der(
                0x06,
                &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01],
            ),
            vec![0x05, 0x00],
        ]
        .concat(),
    );
    let bits = der(0x03, &[&[0x00][..], key.public().as_ref()].concat());
    let spki = der(0x30, &[algorithm, bits].concat());
    let hash = digest::digest(&digest::SHA256, &spki);
    format!("SHA256:{}", crate::http::base64_bytes(hash.as_ref()))
}

/// The account as a JWT names it: upper case, and a locator without its
/// region (`xy12345.eu-west-1` is `XY12345`).
pub(crate) fn jwt_account(account: &str) -> String {
    account
        .split('.')
        .next()
        .unwrap_or(account)
        .to_ascii_uppercase()
}

/// Who signs in, and with what.
pub(crate) struct Login {
    account: String,
    user: String,
    key: RsaKeyPair,
    fingerprint: String,
}

impl Login {
    fn from(properties: &JsonValue, context: &Context) -> Result<Self, ConnectorError> {
        let account = required(properties, "account")?.to_string();
        let user = required(properties, "user")?.to_string();
        let pem = match (
            text(properties, "private_key_file"),
            text(properties, "private_key"),
        ) {
            (Some(_), Some(_)) => {
                return Err(ConnectorError::property(
                    "private_key",
                    "and private_key_file both give the key; give one",
                ))
            }
            (Some(file), None) => {
                let path = context.resolve(file.trim());
                std::fs::read_to_string(&path).map_err(|error| {
                    ConnectorError::property(
                        "private_key_file",
                        format!("cannot read {}: {error}", path.display()),
                    )
                })?
            }
            (None, Some(pem)) => pem.to_string(),
            (None, None) => {
                return Err(ConnectorError::property(
                    "private_key_file",
                    "or private_key is required: Snowflake's SQL API signs in with a key pair",
                ))
            }
        };
        if pem.contains("ENCRYPTED PRIVATE KEY") {
            return Err(ConnectorError::property(
                "private_key_file",
                "holds an encrypted key, which is not read yet; `openssl pkcs8 -topk8 -nocrypt` \
                 writes it unencrypted, to be kept as a secret",
            ));
        }
        let key =
            rsa_key(&pem).map_err(|error| ConnectorError::property("private_key_file", error))?;
        let fingerprint = fingerprint(&key);
        Ok(Login {
            account,
            user,
            key,
            fingerprint,
        })
    }

    /// A fresh JWT: `iss` names the key by its fingerprint, `sub` the user.
    fn token(&self) -> Result<String, ConnectorError> {
        let qualified = format!(
            "{}.{}",
            jwt_account(&self.account),
            self.user.to_ascii_uppercase()
        );
        let now = etl_state::time::now_unix();
        jwt(
            &self.key,
            &json!({ "alg": "RS256", "typ": "JWT" }),
            &json!({
                "iss": format!("{qualified}.{}", self.fingerprint),
                "sub": qualified,
                "iat": now,
                "exp": now + JWT_SECONDS,
            }),
        )
        .map_err(ConnectorError::Data)
    }
}

// ---------------------------------------------------------------------------
// The API
// ---------------------------------------------------------------------------

fn connection_properties() -> Vec<PropertySpec> {
    vec![
        PropertySpec::text("account").required().help(
            "The account identifier: orgname-accountname, or a locator such as xy12345.eu-west-1.",
        ),
        PropertySpec::text("user").required().help("The user the key pair belongs to."),
        PropertySpec::path("private_key_file")
            .help("The user's private key, PKCS#8 PEM, unencrypted. Or give private_key."),
        PropertySpec::text("private_key")
            .help("The private key's PEM text. Use ${SECRET:name} rather than the value itself."),
        PropertySpec::text("role").help("Unset: the user's default role."),
        PropertySpec::text("warehouse").help("Unset: the user's default warehouse."),
        PropertySpec::text("database"),
        PropertySpec::text("schema"),
        PropertySpec::text("endpoint").help(
            "Only for a private link or a test server. Unset: https://<account>.snowflakecomputing.com.",
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
    let endpoint = match text(properties, "endpoint") {
        Some(endpoint) => endpoint.trim().trim_end_matches('/').to_string(),
        None => format!(
            "https://{}.snowflakecomputing.com",
            required(properties, "account")?.to_ascii_lowercase()
        ),
    };
    if crate::aws::host_of(&endpoint).is_none() {
        return Err(ConnectorError::property(
            "endpoint",
            format!("'{endpoint}' is not http://host[:port] or https://host[:port]"),
        ));
    }
    Ok(endpoint)
}

/// An identifier as SQL: bare when Snowflake would read it bare (and so
/// upper-case it, as it stored it), quoted otherwise.
pub(crate) fn identifier(name: &str) -> String {
    let bare = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
    if bare {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// A signed-in client for the SQL API.
pub(crate) struct Api {
    post: Client,
    get: Client,
    endpoint: String,
    login: Login,
    /// Role, warehouse, database and schema for every statement.
    context: Map<String, JsonValue>,
    /// Where the connection is, for messages. Never the key.
    pub(crate) place: String,
}

impl Api {
    pub(crate) fn connect(
        properties: &JsonValue,
        context: &Context,
    ) -> Result<Self, ConnectorError> {
        let endpoint = endpoint_of(properties)?;
        let login = Login::from(properties, context)?;
        let timeout = Duration::from_millis(positive(properties, "timeout_ms", 120_000)?);
        let retries = properties
            .get("retries")
            .and_then(JsonValue::as_u64)
            .unwrap_or(5) as u32;
        let mut session = Map::new();
        for key in ["role", "warehouse", "database", "schema"] {
            if let Some(value) = text(properties, key) {
                session.insert(key.to_string(), json!(value.trim()));
            }
        }
        let place = format!(
            "account {} as {}",
            login.account,
            login.user.to_ascii_uppercase()
        );
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
            endpoint,
            login,
            context: session,
            place,
        })
    }

    fn send(
        &mut self,
        what: &str,
        url: &str,
        body: Option<&[u8]>,
    ) -> Result<(u16, JsonValue), ConnectorError> {
        let token = self.login.token()?;
        let headers = || {
            vec![
                ("Authorization".to_string(), format!("Bearer {token}")),
                (
                    "X-Snowflake-Authorization-Token-Type".to_string(),
                    "KEYPAIR_JWT".to_string(),
                ),
            ]
        };
        let extra = Extra {
            headers: &headers,
            content_type: "application/json",
            throttled: &|_, _| false,
        };
        let client = if body.is_some() {
            &mut self.post
        } else {
            &mut self.get
        };
        // A 202 is a success to HTTP and "still running" to Snowflake; the
        // body says which, so the status is read from it.
        let reply = client
            .send_with(url, &[], body, Some(&extra), Judged::Accept)
            .map_err(|error| {
                ConnectorError::Data(format!(
                    "Snowflake {what}: {}",
                    snowflake_error(&error.to_string())
                ))
            })?;
        let answer: JsonValue = if reply.body.trim().is_empty() {
            JsonValue::Object(Map::new())
        } else {
            serde_json::from_str(&reply.body).map_err(|error| {
                ConnectorError::Data(format!(
                    "Snowflake {what}: the answer is not JSON ({error}): {}",
                    snippet(&reply.body)
                ))
            })?
        };
        let running = answer["code"].as_str() == Some("333334")
            || (answer["statementStatusUrl"].is_string() && answer["resultSetMetaData"].is_null());
        Ok((if running { 202 } else { 200 }, answer))
    }

    /// Run a statement to the end: submitted, then polled while it runs.
    pub(crate) fn execute(
        &mut self,
        sql: &str,
        bindings: Option<JsonValue>,
    ) -> Result<JsonValue, ConnectorError> {
        let mut request = json!({
            "statement": sql,
            "timeout": 0,
            "parameters": { "TIMEZONE": "UTC", "MULTI_STATEMENT_COUNT": "1" },
        });
        for (key, value) in &self.context {
            request[key] = value.clone();
        }
        if let Some(bindings) = bindings {
            request["bindings"] = bindings;
        }
        let body = serde_json::to_vec(&request)
            .map_err(|error| ConnectorError::Data(error.to_string()))?;
        let url = format!(
            "{}/api/v2/statements?async=false&requestId={}",
            self.endpoint,
            request_id()
        );
        let (mut status, mut answer) = self.send("statement", &url, Some(&body))?;
        let started = Instant::now();
        let mut wait = Duration::from_millis(250);
        while status == 202 {
            let handle = answer["statementHandle"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if started.elapsed() > Duration::from_secs(24 * 3600) {
                return Err(ConnectorError::Data(format!(
                    "Snowflake statement {handle} was still running after a day"
                )));
            }
            std::thread::sleep(wait);
            wait = (wait * 2).min(Duration::from_secs(5));
            let url = format!("{}/api/v2/statements/{handle}", self.endpoint);
            (status, answer) = self.send("statement status", &url, None)?;
        }
        Ok(answer)
    }

    /// One result partition's rows, after the first.
    fn partition(&mut self, handle: &str, index: usize) -> Result<Vec<JsonValue>, ConnectorError> {
        let url = format!(
            "{}/api/v2/statements/{handle}?partition={index}",
            self.endpoint
        );
        let (_, answer) = self.send("result partition", &url, None)?;
        Ok(answer["data"].as_array().cloned().unwrap_or_default())
    }
}

/// A request ID of 128 random bits, so a retried submission is the same
/// statement to Snowflake rather than a second one.
fn request_id() -> String {
    use ring::rand::SecureRandom;
    let mut bytes = [0u8; 16];
    let _ = ring::rand::SystemRandom::new().fill(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

/// Snowflake's error JSON, `{"code": "...", "message": "..."}`, down to its
/// code and message, where the HTTP layer quoted the whole body.
fn snowflake_error(text: &str) -> String {
    if let Some(start) = text.find('{') {
        if let Ok(body) = serde_json::from_str::<JsonValue>(&text[start..]) {
            if let Some(message) = body["message"].as_str() {
                let code = body["code"].as_str().unwrap_or("?");
                return format!("{}{message} (code {code})", &text[..start]);
            }
        }
    }
    text.to_string()
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/// A column of a result's `rowType`.
#[derive(Debug, Clone)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) scale: i64,
}

impl Column {
    pub(crate) fn list(meta: &JsonValue) -> Vec<Column> {
        meta["rowType"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|column| Column {
                name: column["name"].as_str().unwrap_or_default().to_string(),
                kind: column["type"]
                    .as_str()
                    .unwrap_or("text")
                    .to_ascii_lowercase(),
                scale: column["scale"].as_i64().unwrap_or(0),
            })
            .collect()
    }

    /// The type a value of this column is cast to when bound as text, or
    /// `None` when it cannot be compared to go on from.
    fn cast(&self) -> Option<String> {
        Some(match self.kind.as_str() {
            "fixed" => format!("NUMBER(38, {})", self.scale),
            "real" => "FLOAT".to_string(),
            "text" => "VARCHAR".to_string(),
            "date" => "DATE".to_string(),
            "time" => "TIME".to_string(),
            "timestamp_ntz" => "TIMESTAMP_NTZ".to_string(),
            "timestamp_ltz" => "TIMESTAMP_LTZ".to_string(),
            "timestamp_tz" => "TIMESTAMP_TZ".to_string(),
            _ => return None,
        })
    }
}

/// Seconds with a fraction (`1790244000.123456789`) as microseconds, in text.
fn seconds_micros(text: &str) -> Option<i64> {
    let text = text.trim();
    let negative = text.starts_with('-');
    let (whole, fraction) = text
        .trim_start_matches('-')
        .split_once('.')
        .unwrap_or((text.trim_start_matches('-'), ""));
    let micros_part = format!("{:0<6}", &fraction[..fraction.len().min(6)]);
    let micros = whole.parse::<i64>().ok()? * 1_000_000 + micros_part.parse::<i64>().ok()?;
    Some(if negative { -micros } else { micros })
}

/// One row of results, typed by the columns.
pub(crate) fn row(columns: &[Column], cells: &JsonValue) -> Result<Record, ConnectorError> {
    let cells = cells.as_array().cloned().unwrap_or_default();
    let mut row = Map::new();
    for (column, cell) in columns.iter().zip(cells.iter()) {
        row.insert(column.name.clone(), value(column, cell)?);
    }
    Ok(row)
}

fn value(column: &Column, cell: &JsonValue) -> Result<JsonValue, ConnectorError> {
    let Some(text) = cell.as_str() else {
        return Ok(JsonValue::Null);
    };
    let bad = || {
        ConnectorError::Data(format!(
            "Snowflake gave '{text}' for {} column '{}'",
            column.kind.to_ascii_uppercase(),
            column.name
        ))
    };
    Ok(match column.kind.as_str() {
        "fixed" if column.scale == 0 => match text.parse::<i64>() {
            Ok(number) => json!(number),
            // NUMBER(38) holds more than an i64: its exact text.
            Err(_) => json!(text),
        },
        "real" => match text.parse::<f64>() {
            Ok(number) if number.is_finite() => json!(number),
            _ => json!(text),
        },
        "boolean" => json!(text.eq_ignore_ascii_case("true")),
        "date" => {
            let days: i64 = text.parse().map_err(|_| bad())?;
            let (year, month, day) = etl_state::time::civil_from_days(days);
            json!(format!("{year:04}-{month:02}-{day:02}"))
        }
        "time" => {
            let micros = seconds_micros(text).ok_or_else(bad)?;
            json!(micros_text(micros)[11..].to_string())
        }
        // Seconds since 1970 in UTC; TIMESTAMP_TZ adds its offset after a
        // space, which a UTC timestamp does not need.
        "timestamp_ntz" | "timestamp_ltz" | "timestamp_tz" => {
            let seconds = text.split_whitespace().next().unwrap_or_default();
            json!(micros_text(seconds_micros(seconds).ok_or_else(bad)?))
        }
        "variant" | "object" | "array" => serde_json::from_str(text).map_err(|_| bad())?,
        // fixed with scale (exact text), text, binary (hex), geography.
        _ => json!(text),
    })
}

// ---------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------

impl Source for SnowflakeSource {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties();
        properties.extend([
            PropertySpec::text("table").help(
                "The table to read: name, schema.name or database.schema.name. Or give query.",
            ),
            PropertySpec::code("query").help("SQL to read, instead of table."),
            PropertySpec::text("incremental_column").help(
                "Read only rows whose value here is above the last successful run's highest. It \
                 must only ever go up.",
            ),
            PropertySpec::text("start").help(
                "With incremental_column, where the first run starts, as a SQL literal, e.g. \
                 '2026-01-01'::TIMESTAMP_NTZ or 1000. Unset: from the beginning.",
            ),
            PropertySpec::integer("max_records").help("The most one run reads. Unset: every row."),
            columns_property(),
        ]);
        ComponentSpec::new("src.warehouse.snowflake", "Snowflake table")
            .description(
                "Read a Snowflake table or query through the SQL API, all of it or only rows new \
                 since the last successful run.",
            )
            .icon("warehouse")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        check_connection(properties)?;
        SourceSettings::from(properties).map(|_| ())
    }

    fn read(
        &self,
        properties: &JsonValue,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = SourceSettings::from(properties)?;
        let mut api = Api::connect(properties, context)?;
        read(&mut api, &settings, out, context.checkpoint.as_ref())
    }
}

/// What can be refused before any request.
fn check_connection(properties: &JsonValue) -> Result<(), ConnectorError> {
    required(properties, "account")?;
    required(properties, "user")?;
    endpoint_of(properties)?;
    match (
        text(properties, "private_key_file"),
        text(properties, "private_key"),
    ) {
        (None, None) => Err(ConnectorError::property(
            "private_key_file",
            "or private_key is required: Snowflake's SQL API signs in with a key pair",
        )),
        (Some(_), Some(_)) => Err(ConnectorError::property(
            "private_key",
            "and private_key_file both give the key; give one",
        )),
        _ => Ok(()),
    }
}

/// A table name as written, if it is one: up to three parts, no statement
/// separators or quotes smuggled in.
fn table_name(name: &str) -> Result<String, ConnectorError> {
    let parts: Vec<&str> = name.split('.').map(str::trim).collect();
    if parts.len() > 3 || parts.iter().any(|part| part.is_empty()) {
        return Err(ConnectorError::property(
            "table",
            format!("'{name}' is not name, schema.name or database.schema.name"),
        ));
    }
    Ok(parts
        .iter()
        .map(|part| identifier(part))
        .collect::<Vec<_>>()
        .join("."))
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

/// The SQL this run sends, its bindings, and a note when a saved position
/// was set aside.
pub(crate) fn statement(
    settings: &SourceSettings,
    saved: Option<&JsonValue>,
) -> (String, Option<JsonValue>, Option<String>) {
    let Some((column, start)) = &settings.incremental else {
        return (settings.sql.clone(), None, None);
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
                "SELECT * FROM ({inner}) WHERE {name} > CAST(? AS {}) ORDER BY {name}",
                saved["type"].as_str().unwrap_or("VARCHAR")
            ),
            Some(json!({ "1": { "type": "TEXT", "value": saved["value"] } })),
            note,
        ),
        (None, Some(start)) => (
            format!("SELECT * FROM ({inner}) WHERE {name} > ({start}) ORDER BY {name}"),
            None,
            note,
        ),
        (None, None) => (
            format!("SELECT * FROM ({inner}) WHERE {name} IS NOT NULL ORDER BY {name}"),
            None,
            note,
        ),
    }
}

/// Run the read, and every partition of its result, as rows.
pub(crate) fn read(
    api: &mut Api,
    settings: &SourceSettings,
    out: &mut dyn RecordWriter,
    saved: Option<&JsonValue>,
) -> Result<Summary, ConnectorError> {
    let (sql, bindings, note) = statement(settings, saved);
    let first = api.execute(&sql, bindings).map_err(|error| {
        ConnectorError::Data(format!("{} at {}: {error}", settings.what, api.place))
    })?;
    let handle = first["statementHandle"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let meta = &first["resultSetMetaData"];
    let columns = Column::list(meta);
    let partitions = meta["partitionInfo"].as_array().map_or(1, Vec::len);

    let incremental = match &settings.incremental {
        None => None,
        Some((column, _)) => {
            let found = columns
                .iter()
                .position(|c| &c.name == column)
                .or_else(|| {
                    columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(column))
                })
                .ok_or_else(|| {
                    ConnectorError::property(
                        "incremental_column",
                        format!("'{column}' is not a column of what is read"),
                    )
                })?;
            let cast = columns[found].cast().ok_or_else(|| {
                ConnectorError::property(
                    "incremental_column",
                    format!(
                        "'{column}' is {}, which cannot be compared to go on from",
                        columns[found].kind.to_ascii_uppercase()
                    ),
                )
            })?;
            Some((column, found, cast))
        }
    };

    let mut count = 0u64;
    let mut highest: Option<JsonValue> = None;
    let mut rows = first["data"].as_array().cloned().unwrap_or_default();
    let mut partition = 0;
    'partitions: loop {
        for cells in &rows {
            if settings.max_records == Some(count) {
                break 'partitions;
            }
            let record = row(&columns, cells)?;
            if let Some((_, index, _)) = &incremental {
                highest = record.get(&columns[*index].name).cloned();
            }
            out.write(record)?;
            count += 1;
        }
        partition += 1;
        if partition >= partitions || settings.max_records == Some(count) {
            break;
        }
        rows = api.partition(&handle, partition)?;
    }

    let mut detail = format!(
        "{count} row(s) from {} at {} by statement {handle}",
        settings.what, api.place
    );
    let mut checkpoint = None;
    if let Some((column, _, cast)) = &incremental {
        match highest {
            Some(value) if !value.is_null() => {
                let text = match &value {
                    JsonValue::String(text) => text.clone(),
                    other => other.to_string(),
                };
                detail.push_str(&format!("; read up to {column} = {text}"));
                checkpoint = Some(json!({
                    "read": settings.what, "column": column, "type": cast, "value": text,
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

impl Sink for SnowflakeSink {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties();
        properties.extend([
            PropertySpec::text("table").required().help(
                "The table to write: name, schema.name or database.schema.name. It must exist.",
            ),
            PropertySpec::enumerated("mode", &["append", "truncate"])
                .default(JsonValue::String("append".into()))
                .help("append adds the rows; truncate empties the table first."),
        ]);
        ComponentSpec::new("snk.warehouse.snowflake", "Snowflake table")
            .description(
                "Write rows to an existing Snowflake table through the SQL API, a thousand bound \
                 rows to an INSERT.",
            )
            .icon("warehouse")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        check_connection(properties)?;
        SinkSettings::from(properties).map(|_| ())
    }

    fn write(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = SinkSettings::from(properties)?;
        let mut api = Api::connect(properties, context)?;
        insert(&mut api, &settings, input)
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
        let table = table_name(required(properties, "table")?)?;
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

/// A value bound as text: Snowflake converts it to the column's type.
fn bound(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Null => JsonValue::Null,
        JsonValue::String(text) => json!(text),
        other => json!(other.to_string()),
    }
}

/// Every row of `input` into the table, a bound `INSERT` per batch.
pub(crate) fn insert(
    api: &mut Api,
    settings: &SinkSettings,
    input: &mut dyn RecordReader,
) -> Result<Summary, ConnectorError> {
    let table = &settings.table;
    let mut inserted = 0u64;
    let mut statements = 0u64;
    let failed = |inserted: u64, error: ConnectorError| {
        ConnectorError::Data(format!(
            "{error}. {inserted} row(s) had been inserted into {table} before this, and stay"
        ))
    };
    if settings.truncate {
        api.execute(&format!("TRUNCATE TABLE {table}"), None)
            .map_err(|error| failed(0, error))?;
    }

    let mut columns: Option<Vec<String>> = None;
    let mut batch: Vec<Record> = Vec::new();
    let mut row = 0u64;
    let mut flush = |api: &mut Api, batch: Vec<Record>, columns: &[String], inserted: &mut u64| {
        let names = columns
            .iter()
            .map(|c| identifier(c))
            .collect::<Vec<_>>()
            .join(", ");
        let marks = vec!["?"; columns.len()].join(", ");
        let mut bindings = Map::new();
        for (index, column) in columns.iter().enumerate() {
            let values: Vec<JsonValue> = batch
                .iter()
                .map(|record| bound(record.get(column).unwrap_or(&JsonValue::Null)))
                .collect();
            bindings.insert(
                (index + 1).to_string(),
                json!({ "type": "TEXT", "value": values }),
            );
        }
        let answer = api
            .execute(
                &format!("INSERT INTO {table} ({names}) VALUES ({marks})"),
                Some(JsonValue::Object(bindings)),
            )
            .map_err(|error| failed(*inserted, error))?;
        *inserted += answer["data"][0][0]
            .as_str()
            .and_then(|count| count.parse::<u64>().ok())
            .unwrap_or(batch.len() as u64);
        statements += 1;
        Ok::<(), ConnectorError>(())
    };

    while let Some(record) = input.read()? {
        row += 1;
        let known = columns.get_or_insert_with(|| record.keys().cloned().collect());
        if let Some(extra) = record.keys().find(|key| !known.contains(key)) {
            return Err(failed(
                inserted,
                ConnectorError::Data(format!(
                    "row {row} has a column '{extra}' the first row did not"
                )),
            ));
        }
        batch.push(record);
        if batch.len() == INSERT_ROWS {
            let names = columns.clone().unwrap_or_default();
            flush(api, std::mem::take(&mut batch), &names, &mut inserted)?;
        }
    }
    if !batch.is_empty() {
        let names = columns.clone().unwrap_or_default();
        flush(api, batch, &names, &mut inserted)?;
    }

    let detail = format!(
        "{inserted} row(s) {} {table} at {} in {statements} INSERT(s)",
        if settings.truncate {
            "replaced the rows of"
        } else {
            "appended to"
        },
        api.place
    );
    Ok(Summary::new(inserted, detail))
}
