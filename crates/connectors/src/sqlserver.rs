//! SQL Server: a table or a query read over TDS, streamed, all of it or only
//! what is new since the last successful run; and a table written to with
//! batched, parameterised `INSERT`s: appended, replacing its rows, or merged on
//! key columns through a staging table.
//!
//! **Through `tiberius`**, the TDS client, with its default features off: not
//! `native-tls`, which links OpenSSL on Linux and would break the single
//! binary, and not Windows authentication. Sign-in is a SQL Server login. The
//! client is asynchronous, so each connection has a small `tokio` runtime of
//! its own and **every call has a deadline**, `timeout_ms`.
//!
//! **No SQL Server runs here** (Settled decision 87): its image needs 2 GB,
//! over the 1 GB a test container is given, so everything is proved against
//! the local TDS fixture in `sqlserver/fixture.rs` and recorded as not yet
//! checked against real SQL Server.
//!
//! **TLS is `tiberius`'s own** (rustls 0.21), which cannot be handed a
//! configuration from [`crate::tls`]. It trusts one certificate authority
//! (`ca_cert`), anything (`trust_server_certificate`), or else the machine's
//! store, which the connector never lets it use (decision 42): encrypting needs
//! one of the first two, and with `encryption: none` a server that insists on
//! TLS is refused rather than trusted through the store.
//!
//! **Reading**: rows are typed by what SQL Server sends. Integers and floats
//! are numbers; `decimal` and `numeric` are their exact text, or a number when
//! the scale is 0 and it fits; `date`, `time`, `datetime`, `datetime2` and
//! `datetimeoffset` are text to the microsecond, `datetimeoffset` in UTC;
//! `uniqueidentifier` is upper-case text as SQL Server shows it; binary is hex.
//! A column `tiberius` cannot read (`geography`, `hierarchyid`, `sql_variant`)
//! is an error naming the way round it, a `CAST` in a query.
//!
//! **Only what is new**: `incremental_column` wraps the read as
//! `SELECT * FROM (<read>) AS [etl_read] WHERE col > CAST(@P1 AS <type>) ORDER
//! BY col`, the last run's highest value a **parameter**, never pasted into the
//! SQL. The value saved is the column's own text at its full precision (seven
//! digits of a `datetime2`), so no row is skipped for being between two
//! microseconds.
//!
//! **Writing**: values are bound as text (`nvarchar`) and SQL Server converts
//! them to each column's type, one path for every type, as Snowflake's are. A
//! statement carries at most 1,000 rows and 2,000 parameters. `merge` stages
//! every row in a temporary table and applies one `MERGE` at the end, so the
//! table changes all at once or not at all, and the last of two rows with the
//! same key wins.

use crate::http::{positive, text};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, RecordReader, RecordWriter, Sink, Source, Summary,
};
use futures_util::TryStreamExt;
use serde_json::{json, Map, Value as JsonValue};
use std::borrow::Cow;
use std::future::Future;
use std::path::Path;
use std::time::Duration;
use tiberius::time::Time;
use tiberius::{
    AuthMethod, Client, ColumnData, ColumnType, Config, EncryptionLevel, QueryItem, ToSql,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

#[cfg(test)]
mod fixture;
#[cfg(test)]
mod tests;

/// `src.db.sqlserver`.
pub struct SqlserverSource;

/// `snk.db.sqlserver`.
pub struct SqlserverSink;

/// Parameters one statement binds at most. SQL Server's limit is 2,100.
const MAX_PARAMETERS: usize = 2000;

/// Rows one `INSERT ... VALUES` holds at most: SQL Server's own limit.
const MAX_ROWS: usize = 1000;

/// The temporary table `merge` stages rows in, and its row-order column.
const STAGE: &str = "#etl_stage";
const STAGE_ROW: &str = "[etl_row]";

/// Days from 0001-01-01 (`date`, `datetime2`) and from 1900-01-01
/// (`datetime`, `smalldatetime`) to 1970-01-01.
const DAYS_FROM_0001: i64 = 719_162;
const DAYS_FROM_1900: i64 = 25_567;
const DAY_MICROS: i64 = 86_400_000_000;

// ---------------------------------------------------------------------------
// The server
// ---------------------------------------------------------------------------

fn connection_properties() -> Vec<PropertySpec> {
    vec![
        PropertySpec::text("host").required().help(
            "The server's name or address. A named instance is reached by its port: SQL \
             Browser is not asked.",
        ),
        PropertySpec::integer("port").default(JsonValue::from(1433)),
        PropertySpec::text("database").help("Unset: the login's default database."),
        PropertySpec::text("username")
            .required()
            .help("A SQL Server login. Windows and Entra ID sign-in are not offered."),
        PropertySpec::text("password").help("Use ${SECRET:name} rather than the value itself."),
        PropertySpec::enumerated("encryption", &["required", "login_only", "none"])
            .default(json!("required"))
            .help(
                "required encrypts everything; login_only only the sign-in (SQL Server's \
                 Encrypt=false); none nothing, for a server without TLS.",
            ),
        PropertySpec::path("ca_cert").help(
            "The certificate authority that signed the server's certificate: one certificate, \
             in a .pem, .crt or .der file. Encrypting needs this or trust_server_certificate.",
        ),
        PropertySpec::boolean("trust_server_certificate")
            .default(json!(false))
            .help("Encrypt without checking the server's certificate. For a test server only."),
        PropertySpec::integer("timeout_ms")
            .default(JsonValue::from(300_000))
            .help(
                "How long to wait for any one answer: connecting, signing in, a statement, the \
                 next rows.",
            ),
    ]
}

/// Which certificates an encrypted connection accepts.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Trust {
    /// Those `ca_cert` signed, as written.
    Ca(String),
    /// Any: `trust_server_certificate`.
    All,
    /// None: `encryption: none`, so a server that insists on TLS is refused.
    Refuse,
}

/// Where the connection goes, and as whom.
#[derive(Clone)]
pub(crate) struct Server {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) database: Option<String>,
    pub(crate) user: String,
    password: String,
    pub(crate) encryption: EncryptionLevel,
    pub(crate) trust: Trust,
    pub(crate) timeout: Duration,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

/// What `tiberius` reads the file named by `trust_cert_ca` as, which it
/// cannot be, so a server insisting on TLS is refused (see [`Trust::Refuse`]).
const REFUSE_TLS: &str = "<etl: encryption is none, so no certificate is trusted>";

impl Server {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let host = text(properties, "host")
            .map(str::trim)
            .ok_or_else(|| ConnectorError::property("host", "is required"))?;
        if host.contains('\\') {
            return Err(ConnectorError::property(
                "host",
                format!(
                    "'{host}' names an instance; give the instance's port instead, as SQL \
                     Browser is not asked"
                ),
            ));
        }
        if host.contains(char::is_whitespace) || host.contains(':') && !host.starts_with('[') {
            return Err(ConnectorError::property(
                "host",
                format!(
                    "'{host}' is not a host name or address: the port goes in port, and an IPv6 \
                     address in brackets"
                ),
            ));
        }
        let port = positive(properties, "port", 1433)?;
        let port = u16::try_from(port)
            .map_err(|_| ConnectorError::property("port", format!("{port} is not a TCP port")))?;
        let user = text(properties, "username")
            .map(|user| user.trim().to_string())
            .ok_or_else(|| ConnectorError::property("username", "is required"))?;

        let encryption = match text(properties, "encryption").unwrap_or("required") {
            "required" => EncryptionLevel::Required,
            "login_only" => EncryptionLevel::Off,
            "none" => EncryptionLevel::NotSupported,
            other => {
                return Err(ConnectorError::property(
                    "encryption",
                    format!("'{other}' is not one of required, login_only, none"),
                ))
            }
        };
        let ca_cert = text(properties, "ca_cert").map(|path| path.trim().to_string());
        let trust_all = properties
            .get("trust_server_certificate")
            .and_then(JsonValue::as_bool)
            == Some(true);
        let trust =
            match (encryption, ca_cert, trust_all) {
                (_, Some(_), true) => {
                    return Err(ConnectorError::property(
                        "ca_cert",
                        "and trust_server_certificate both say what to trust; give one",
                    ))
                }
                (EncryptionLevel::NotSupported, Some(_), _)
                | (EncryptionLevel::NotSupported, _, true) => {
                    return Err(ConnectorError::property(
                        "encryption",
                        "is none, so there is no certificate to check; unset ca_cert and \
                     trust_server_certificate, or choose required",
                    ))
                }
                (EncryptionLevel::NotSupported, None, false) => Trust::Refuse,
                (_, Some(path), false) => Trust::Ca(path),
                (_, None, true) => Trust::All,
                (_, None, false) => return Err(ConnectorError::property(
                    "ca_cert",
                    "or trust_server_certificate is needed to encrypt: the server's certificate \
                     is checked against ca_cert, never against this machine's store",
                )),
            };

        Ok(Server {
            host: host.to_string(),
            port,
            database: text(properties, "database").map(|d| d.trim().to_string()),
            user,
            password: text(properties, "password").unwrap_or("").to_string(),
            encryption,
            trust,
            timeout: Duration::from_millis(positive(properties, "timeout_ms", 300_000)?),
        })
    }

    /// `host:port/database as user`, for messages. Never the password.
    pub(crate) fn place(&self) -> String {
        match &self.database {
            Some(database) => format!("{}:{}/{database} as {}", self.host, self.port, self.user),
            None => format!("{}:{} as {}", self.host, self.port, self.user),
        }
    }

    /// The client's configuration, `ca_cert` resolved against the workspace.
    fn config(&self, context: &Context) -> Result<Config, ConnectorError> {
        let mut config = Config::new();
        config.host(&self.host);
        config.port(self.port);
        if let Some(database) = &self.database {
            config.database(database);
        }
        config.authentication(AuthMethod::sql_server(&self.user, &self.password));
        config.application_name(concat!("etl ", env!("CARGO_PKG_VERSION")));
        config.encryption(self.encryption);
        match &self.trust {
            Trust::Ca(written) => {
                let path = context.resolve(written);
                one_certificate(&path)?;
                config.trust_cert_ca(path.display());
            }
            Trust::All => config.trust_cert(),
            Trust::Refuse => config.trust_cert_ca(REFUSE_TLS),
        }
        Ok(config)
    }
}

/// `tiberius` reads exactly one certificate from `ca_cert`, and only from a
/// file named `.pem`, `.crt` or `.der`: said before connecting, in our words.
fn one_certificate(path: &Path) -> Result<(), ConnectorError> {
    let bad = |reason: String| {
        ConnectorError::property("ca_cert", format!("{}: {reason}", path.display()))
    };
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    let bytes = std::fs::read(path).map_err(|error| bad(error.to_string()))?;
    match extension.as_deref() {
        Some("pem") | Some("crt") => {
            let count = String::from_utf8_lossy(&bytes)
                .matches("-----BEGIN CERTIFICATE-----")
                .count();
            if count != 1 {
                return Err(bad(format!(
                    "holds {count} certificates; SQL Server's client takes exactly one, the \
                     authority that signed the server's certificate"
                )));
            }
        }
        Some("der") => {}
        _ => {
            return Err(bad(
                "is not a .pem, .crt or .der file, the kinds SQL Server's client reads".into(),
            ))
        }
    }
    Ok(())
}

/// An identifier as SQL: bracketed, a `]` inside doubled.
pub(crate) fn identifier(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// A table name as written, `name`, `schema.name` or `database.schema.name`,
/// each part bare or already bracketed.
pub(crate) fn table_name(name: &str) -> Result<String, ConnectorError> {
    let parts: Vec<&str> = name.split('.').map(str::trim).collect();
    if parts.len() > 3 || parts.iter().any(|part| part.is_empty()) {
        return Err(ConnectorError::property(
            "table",
            format!("'{name}' is not name, schema.name or database.schema.name"),
        ));
    }
    Ok(parts
        .iter()
        .map(
            |part| match part.strip_prefix('[').and_then(|p| p.strip_suffix(']')) {
                Some(inner) => identifier(&inner.replace("]]", "]")),
                None => identifier(part),
            },
        )
        .collect::<Vec<_>>()
        .join("."))
}

// ---------------------------------------------------------------------------
// The connection
// ---------------------------------------------------------------------------

type Wire = Compat<TcpStream>;

/// Why a call came to nothing.
#[derive(Debug)]
pub(crate) enum Failure {
    /// The server, the network or the client said no.
    Said(tiberius::error::Error),
    /// Nothing within the deadline, in milliseconds.
    Silent(u128),
    /// The client gave up on something it cannot read.
    Panicked(String),
}

impl Failure {
    /// SQL Server's error number, when it gave one.
    pub(crate) fn code(&self) -> Option<u32> {
        match self {
            Failure::Said(tiberius::error::Error::Server(token)) => Some(token.code()),
            _ => None,
        }
    }

    /// What went wrong, in SQL Server's words where it gave any.
    pub(crate) fn message(&self) -> String {
        match self {
            Failure::Said(tiberius::error::Error::Server(token)) => format!(
                "SQL Server error {}: {}",
                token.code(),
                token.message().trim()
            ),
            Failure::Said(tiberius::error::Error::Io { message, .. }) => message.clone(),
            Failure::Said(tiberius::error::Error::Tls(message)) => format!("TLS: {message}"),
            Failure::Said(other) => other.to_string(),
            Failure::Silent(ms) => format!("no answer within {ms} ms"),
            Failure::Panicked(what) => format!(
                "the client could not read what the server sent ({what}). A column of a type \
                 it does not know, such as geography, hierarchyid or sql_variant, is read by \
                 CASTing it in a query"
            ),
        }
    }
}

async fn within<T>(
    limit: Duration,
    work: impl Future<Output = tiberius::Result<T>>,
) -> Result<T, Failure> {
    match tokio::time::timeout(limit, work).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(Failure::Said(error)),
        Err(_) => Err(Failure::Silent(limit.as_millis())),
    }
}

/// Run `work` to the end on `runtime`. `tiberius` panics on a column type or a
/// token it does not know rather than returning an error; that becomes one.
fn run<T, E: From<Failure>>(
    runtime: &tokio::runtime::Runtime,
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runtime.block_on(work))) {
        Ok(result) => result,
        Err(panic) => Err(E::from(Failure::Panicked(
            panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "no detail".to_string()),
        ))),
    }
}

/// Why a read stopped early: the server, or something the connector refuses.
enum Stop {
    Failed(Failure),
    Refused(ConnectorError),
}

impl From<Failure> for Stop {
    fn from(failure: Failure) -> Self {
        Stop::Failed(failure)
    }
}

/// An open, signed-in connection and the runtime it lives on.
pub(crate) struct Link {
    runtime: tokio::runtime::Runtime,
    client: Client<Wire>,
    timeout: Duration,
    pub(crate) place: String,
}

impl Link {
    pub(crate) fn open(server: &Server, context: &Context) -> Result<Link, ConnectorError> {
        let config = server.config(context)?;
        let place = server.place();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| ConnectorError::Data(format!("SQL Server: {error}")))?;
        let client = run(&runtime, connect(config, server.timeout)).map_err(|failure| {
            let refused = server.trust == Trust::Refuse
                && failure
                    .message()
                    .contains("Could not read provided CA certificate");
            ConnectorError::Data(if refused {
                format!(
                    "SQL Server at {place} insists on encryption, and encryption is none: choose \
                     required, with ca_cert or trust_server_certificate"
                )
            } else {
                format!("SQL Server at {place}: {}", failure.message())
            })
        })?;
        Ok(Link {
            runtime,
            client,
            timeout: server.timeout,
            place,
        })
    }

    /// A statement with parameters, through `sp_executesql`: the rows it
    /// changed.
    pub(crate) fn execute(&mut self, sql: &str, values: &[Text]) -> Result<u64, Failure> {
        let Link {
            runtime,
            client,
            timeout,
            ..
        } = self;
        let limit = *timeout;
        run(runtime, async move {
            let params: Vec<&dyn ToSql> = values.iter().map(|value| value as &dyn ToSql).collect();
            let done = within(limit, client.execute(sql, &params)).await?;
            Ok(done.total())
        })
    }

    /// A batch without parameters, sent as SQL text rather than through
    /// `sp_executesql`: what it creates outlives it, as a temporary table must.
    pub(crate) fn batch(&mut self, sql: &str) -> Result<(), Failure> {
        let Link {
            runtime,
            client,
            timeout,
            ..
        } = self;
        let limit = *timeout;
        run(runtime, async move {
            let stream = within(limit, client.simple_query(sql)).await?;
            within(limit, stream.into_results()).await?;
            Ok(())
        })
    }
}

/// Connect and sign in. Azure SQL's gateway may send the client on to another
/// server, which is followed once.
async fn connect(mut config: Config, limit: Duration) -> Result<Client<Wire>, Failure> {
    let mut hops = 0;
    loop {
        let address = config.get_addr();
        let tcp = within(limit, async {
            TcpStream::connect(&address)
                .await
                .map_err(tiberius::error::Error::from)
        })
        .await?;
        let _ = tcp.set_nodelay(true);
        match within(limit, Client::connect(config.clone(), tcp.compat_write())).await {
            Err(Failure::Said(tiberius::error::Error::Routing { host, port })) if hops == 0 => {
                hops += 1;
                config.host(host);
                config.port(port);
            }
            other => return other,
        }
    }
}

/// A value bound as text, or a NULL.
pub(crate) struct Text(pub(crate) Option<String>);

impl ToSql for Text {
    fn to_sql(&self) -> ColumnData<'_> {
        ColumnData::String(self.0.as_deref().map(Cow::Borrowed))
    }
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

fn date_text(days_since_1970: i64) -> String {
    let (year, month, day) = etl_state::time::civil_from_days(days_since_1970);
    format!("{year:04}-{month:02}-{day:02}")
}

/// A `time` as nanoseconds since midnight.
fn nanos(time: Time) -> i64 {
    time.increments() as i64 * 10i64.pow(9 - u32::from(time.scale().min(9)))
}

/// `hh:mm:ss`, and `digits` of a second's fraction, from nanoseconds.
fn clock(nanos: i64, digits: u8) -> String {
    let seconds = nanos / 1_000_000_000;
    let mut text = format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    );
    if digits > 0 {
        let fraction = nanos % 1_000_000_000 / 10i64.pow(9 - u32::from(digits.min(9)));
        text.push_str(&format!(".{fraction:0width$}", width = usize::from(digits)));
    }
    text
}

/// A `datetime`'s 300ths of a second as microseconds and as milliseconds,
/// each rounded as SQL Server rounds it for display.
fn ticks_micros(ticks: u32) -> i64 {
    (i64::from(ticks) * 10_000 + 1) / 3
}
fn ticks_millis(ticks: u32) -> i64 {
    (i64::from(ticks) * 10 + 1) / 3
}

/// A decimal's exact text from its unscaled value.
pub(crate) fn decimal_text(value: i128, scale: u8) -> String {
    let digits = value.unsigned_abs().to_string();
    let sign = if value < 0 { "-" } else { "" };
    let scale = usize::from(scale);
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = padded.split_at(padded.len() - scale);
    format!("{sign}{whole}.{fraction}")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A value as a row holds it.
pub(crate) fn value(data: &ColumnData<'_>) -> JsonValue {
    match data {
        ColumnData::U8(Some(v)) => json!(v),
        ColumnData::I16(Some(v)) => json!(v),
        ColumnData::I32(Some(v)) => json!(v),
        ColumnData::I64(Some(v)) => json!(v),
        // Through its shortest text, so a `real` 1.1 stays 1.1.
        ColumnData::F32(Some(v)) => v
            .to_string()
            .parse::<f64>()
            .map_or(JsonValue::Null, |v| json!(v)),
        ColumnData::F64(Some(v)) => json!(v),
        ColumnData::Bit(Some(v)) => json!(v),
        ColumnData::String(Some(v)) => json!(v),
        ColumnData::Guid(Some(v)) => json!(v.to_string().to_uppercase()),
        ColumnData::Binary(Some(v)) => json!(hex(v)),
        ColumnData::Numeric(Some(v)) => match (v.scale(), i64::try_from(v.value())) {
            (0, Ok(whole)) => json!(whole),
            _ => json!(decimal_text(v.value(), v.scale())),
        },
        ColumnData::Xml(Some(v)) => json!(v.as_ref().clone().into_string()),
        ColumnData::DateTime(Some(v)) => json!(crate::bigquery::micros_text(
            (i64::from(v.days()) - DAYS_FROM_1900) * DAY_MICROS
                + ticks_micros(v.seconds_fragments())
        )),
        ColumnData::SmallDateTime(Some(v)) => json!(crate::bigquery::micros_text(
            (i64::from(v.days()) - DAYS_FROM_1900) * DAY_MICROS
                + i64::from(v.seconds_fragments()) * 60_000_000
        )),
        ColumnData::Time(Some(v)) => json!(clock(nanos(*v), 6)),
        ColumnData::Date(Some(v)) => json!(date_text(i64::from(v.days()) - DAYS_FROM_0001)),
        ColumnData::DateTime2(Some(v)) => json!(crate::bigquery::micros_text(
            (i64::from(v.date().days()) - DAYS_FROM_0001) * DAY_MICROS + nanos(v.time()) / 1000
        )),
        // The date and time of a `datetimeoffset` come in UTC; its offset is
        // only how it was written.
        ColumnData::DateTimeOffset(Some(v)) => {
            let utc = v.datetime2();
            json!(crate::bigquery::micros_text(
                (i64::from(utc.date().days()) - DAYS_FROM_0001) * DAY_MICROS
                    + nanos(utc.time()) / 1000
            ))
        }
        _ => JsonValue::Null,
    }
}

/// A value as text SQL Server reads back exactly with [`cast`]'s type: an
/// incremental read's saved position.
pub(crate) fn exact(data: &ColumnData<'_>) -> Option<String> {
    Some(match data {
        ColumnData::DateTime2(Some(v)) => format!(
            "{} {}",
            date_text(i64::from(v.date().days()) - DAYS_FROM_0001),
            clock(nanos(v.time()), v.time().scale())
        ),
        ColumnData::DateTimeOffset(Some(v)) => {
            let utc = v.datetime2();
            format!(
                "{} {} +00:00",
                date_text(i64::from(utc.date().days()) - DAYS_FROM_0001),
                clock(nanos(utc.time()), utc.time().scale())
            )
        }
        ColumnData::Time(Some(v)) => clock(nanos(*v), v.scale()),
        ColumnData::DateTime(Some(v)) => format!(
            "{} {}",
            date_text(i64::from(v.days()) - DAYS_FROM_1900),
            clock(ticks_millis(v.seconds_fragments()) * 1_000_000, 3)
        ),
        ColumnData::SmallDateTime(Some(v)) => format!(
            "{} {}",
            date_text(i64::from(v.days()) - DAYS_FROM_1900),
            clock(i64::from(v.seconds_fragments()) * 60_000_000_000, 0)
        ),
        ColumnData::Numeric(Some(v)) => decimal_text(v.value(), v.scale()),
        other => match value(other) {
            JsonValue::Null => return None,
            JsonValue::String(text) => text,
            number => number.to_string(),
        },
    })
}

/// The type a saved position is cast to, or `None` when a column of this kind
/// cannot be compared to go on from.
pub(crate) fn cast(data: &ColumnData<'_>) -> Option<String> {
    Some(match data {
        ColumnData::U8(_) | ColumnData::I16(_) | ColumnData::I32(_) | ColumnData::I64(_) => {
            "bigint".into()
        }
        ColumnData::F32(_) | ColumnData::F64(_) => "float".into(),
        ColumnData::Numeric(Some(v)) => format!("decimal(38, {})", v.scale()),
        ColumnData::String(_) => "nvarchar(max)".into(),
        ColumnData::Guid(_) => "uniqueidentifier".into(),
        ColumnData::Date(_) => "date".into(),
        ColumnData::Time(Some(v)) => format!("time({})", v.scale()),
        ColumnData::DateTime(_) => "datetime".into(),
        ColumnData::SmallDateTime(_) => "smalldatetime".into(),
        ColumnData::DateTime2(Some(v)) => format!("datetime2({})", v.time().scale()),
        ColumnData::DateTimeOffset(Some(v)) => {
            format!("datetimeoffset({})", v.datetime2().time().scale())
        }
        _ => return None,
    })
}

/// Whether a column of this type can be an `incremental_column`.
fn comparable(kind: ColumnType) -> bool {
    !matches!(
        kind,
        ColumnType::Null
            | ColumnType::Bit
            | ColumnType::Bitn
            | ColumnType::BigVarBin
            | ColumnType::BigBinary
            | ColumnType::Image
            | ColumnType::Text
            | ColumnType::NText
            | ColumnType::Xml
            | ColumnType::Udt
            | ColumnType::SSVariant
    )
}

/// The names of a result's columns, each one a field of a row: none empty,
/// none twice.
fn column_names(columns: &[tiberius::Column]) -> Result<Vec<String>, Stop> {
    let mut names: Vec<String> = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let name = column.name().to_string();
        if name.is_empty() {
            return Err(Stop::Refused(ConnectorError::property(
                "query",
                format!("column {} has no name; give it one with AS", index + 1),
            )));
        }
        if names.contains(&name) {
            return Err(Stop::Refused(ConnectorError::property(
                "query",
                format!(
                    "gives two columns named '{name}'; give one another name with AS, as a row \
                     holds each name once"
                ),
            )));
        }
        names.push(name);
    }
    Ok(names)
}

/// Where `column` is among `names`: as written, or else ignoring case, as SQL
/// Server's usual collations compare names.
fn incremental_position(names: &[String], column: &str) -> Result<usize, Stop> {
    names
        .iter()
        .position(|name| name == column)
        .or_else(|| {
            names
                .iter()
                .position(|name| name.eq_ignore_ascii_case(column))
        })
        .ok_or_else(|| {
            Stop::Refused(ConnectorError::property(
                "incremental_column",
                format!("'{column}' is not a column of what is read"),
            ))
        })
}

// ---------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------

impl Source for SqlserverSource {
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
                 '2026-01-01' or 1000. Unset: from the beginning.",
            ),
            PropertySpec::integer("max_records").help("The most one run reads. Unset: every row."),
            columns_property(),
        ]);
        ComponentSpec::new("src.db.sqlserver", "SQL Server table")
            .description(
                "Read a SQL Server or Azure SQL table or query over TDS, streamed, all of it or \
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
        let mut link = Link::open(&server, context)?;
        read(&mut link, &settings, out, context.checkpoint.as_ref())
    }
}

#[derive(Debug)]
pub(crate) struct SourceSettings {
    pub(crate) what: String,
    /// The table, when `table` says what to read.
    pub(crate) table: Option<String>,
    /// What is read, as a statement that could be a subquery.
    pub(crate) sql: String,
    pub(crate) incremental: Option<(String, Option<String>)>,
    pub(crate) max_records: Option<u64>,
}

impl SourceSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let (what, table, sql) = match (text(properties, "table"), text(properties, "query")) {
            (Some(_), Some(_)) => {
                return Err(ConnectorError::property(
                    "query",
                    "and table both say what to read; give one",
                ))
            }
            (Some(table), None) => {
                let table = table_name(table.trim())?;
                (
                    format!("table {table}"),
                    Some(table.clone()),
                    format!("SELECT * FROM {table}"),
                )
            }
            (None, Some(query)) => {
                let query = query.trim().trim_end_matches(';').trim().to_string();
                (
                    format!("query {}", crate::bigquery::fingerprint(&query)),
                    None,
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
            table,
            sql,
            incremental,
            max_records,
        })
    }
}

/// The SQL this run sends, its parameter, and a note when a saved position
/// was set aside.
pub(crate) fn statement(
    settings: &SourceSettings,
    saved: Option<&JsonValue>,
) -> (String, Option<String>, Option<String>) {
    let top = settings
        .max_records
        .map_or(String::new(), |cap| format!("TOP ({cap}) "));
    let Some((column, start)) = &settings.incremental else {
        // A query is sent as written; `max_records` then stops reading.
        return match &settings.table {
            Some(table) => (format!("SELECT {top}* FROM {table}"), None, None),
            None => (settings.sql.clone(), None, None),
        };
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
    let from = format!("SELECT {top}* FROM ({inner}) AS [etl_read]");
    match (mine, start) {
        (Some(saved), _) => (
            format!(
                "{from} WHERE {name} > CAST(@P1 AS {}) ORDER BY {name}",
                saved["type"].as_str().unwrap_or("nvarchar(max)")
            ),
            Some(saved["value"].as_str().unwrap_or_default().to_string()),
            note,
        ),
        (None, Some(start)) => (
            format!("{from} WHERE {name} > ({start}) ORDER BY {name}"),
            None,
            note,
        ),
        (None, None) => (
            format!("{from} WHERE {name} IS NOT NULL ORDER BY {name}"),
            None,
            note,
        ),
    }
}

/// Run the read and stream its rows.
pub(crate) fn read(
    link: &mut Link,
    settings: &SourceSettings,
    out: &mut dyn RecordWriter,
    saved: Option<&JsonValue>,
) -> Result<Summary, ConnectorError> {
    let (sql, bound, note) = statement(settings, saved);
    let place = link.place.clone();
    let failed = |failure: Failure| {
        let mut message = failure.message();
        if failure.code() == Some(1033) && settings.incremental.is_some() {
            message.push_str(
                ". With incremental_column the query is read as a subquery, where SQL Server \
                 allows ORDER BY only with TOP or OFFSET: leave the ORDER BY out",
            );
        }
        ConnectorError::Data(format!("{} at {place}: {message}", settings.what))
    };
    let what_is_read = if settings.table.is_some() {
        "table"
    } else {
        "query"
    };

    let Link {
        runtime,
        client,
        timeout,
        ..
    } = link;
    let limit = *timeout;
    let outcome = run(runtime, async {
        let parameter = bound.map(|value| Text(Some(value)));
        let params: Vec<&dyn ToSql> = parameter.iter().map(|p| p as &dyn ToSql).collect();
        let mut stream = within(limit, client.query(sql.as_str(), &params)).await?;

        let mut names: Vec<String> = Vec::new();
        let mut position: Option<usize> = None;
        let mut count = 0u64;
        let mut highest: Option<ColumnData<'static>> = None;
        while settings.max_records != Some(count) {
            let Some(item) = within(limit, stream.try_next()).await? else {
                break;
            };
            match item {
                QueryItem::Metadata(meta) => {
                    if meta.result_index() > 0 {
                        return Err(Stop::Refused(ConnectorError::property(
                            what_is_read,
                            "gives more than one result set; the connector reads one, so end \
                             with the SELECT whose rows are wanted",
                        )));
                    }
                    names = column_names(meta.columns())?;
                    if let Some((column, _)) = &settings.incremental {
                        let found = incremental_position(&names, column)?;
                        let kind = meta.columns()[found].column_type();
                        if !comparable(kind) {
                            return Err(Stop::Refused(ConnectorError::property(
                                "incremental_column",
                                format!(
                                    "'{column}' is {kind:?}, which cannot be compared to go on \
                                     from"
                                ),
                            )));
                        }
                        position = Some(found);
                    }
                }
                QueryItem::Row(row) => {
                    let mut record = Map::new();
                    for (index, (_, data)) in row.cells().enumerate() {
                        record.insert(names[index].clone(), value(data));
                        if position == Some(index) {
                            highest = Some(data.clone());
                        }
                    }
                    out.write(record).map_err(Stop::Refused)?;
                    count += 1;
                }
            }
        }
        Ok::<_, Stop>((count, highest))
    });
    let (count, highest) = match outcome {
        Ok(read) => read,
        Err(Stop::Failed(failure)) => return Err(failed(failure)),
        Err(Stop::Refused(error)) => return Err(error),
    };

    let mut detail = format!("{count} row(s) from {} at {place}", settings.what);
    let mut checkpoint = None;
    if let Some((column, _)) = &settings.incremental {
        match highest
            .as_ref()
            .and_then(|data| Some((exact(data)?, cast(data)?)))
        {
            Some((text, kind)) => {
                detail.push_str(&format!("; read up to {column} = {text}"));
                checkpoint = Some(json!({
                    "read": settings.what, "column": column, "type": kind, "value": text,
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

impl Sink for SqlserverSink {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties();
        properties.extend([
            PropertySpec::text("table").required().help(
                "The table to write: name, schema.name or database.schema.name. It must exist.",
            ),
            PropertySpec::enumerated("mode", &["append", "truncate", "merge"])
                .default(JsonValue::String("append".into()))
                .help(
                    "append adds the rows; truncate empties the table first; merge updates the \
                     rows whose key_columns match and inserts the rest.",
                ),
            PropertySpec::string_list("key_columns")
                .help("With merge: the columns that pick out a row, as the table's key does."),
        ]);
        ComponentSpec::new("snk.db.sqlserver", "SQL Server table")
            .description(
                "Write rows to an existing SQL Server or Azure SQL table over TDS: appended, \
                 replacing its rows, or merged on key columns.",
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
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let server = Server::from(properties)?;
        let settings = SinkSettings::from(properties)?;
        let mut link = Link::open(&server, context)?;
        insert(&mut link, &settings, input)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum Mode {
    Append,
    Truncate,
    /// On these columns, as written.
    Merge(Vec<String>),
}

#[derive(Debug)]
pub(crate) struct SinkSettings {
    pub(crate) table: String,
    pub(crate) mode: Mode,
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
        let keys: Vec<String> = match properties.get("key_columns") {
            None | Some(JsonValue::Null) => Vec::new(),
            Some(JsonValue::Array(items)) => items
                .iter()
                .map(|item| match item.as_str().map(str::trim) {
                    Some(name) if !name.is_empty() => Ok(name.to_string()),
                    _ => Err(ConnectorError::property(
                        "key_columns",
                        "must be a list of column names",
                    )),
                })
                .collect::<Result<_, _>>()?,
            Some(_) => {
                return Err(ConnectorError::property(
                    "key_columns",
                    "must be a list of column names",
                ))
            }
        };
        let mode = match (
            text(properties, "mode").unwrap_or("append"),
            keys.is_empty(),
        ) {
            ("merge", false) => Mode::Merge(keys),
            ("merge", true) => {
                return Err(ConnectorError::property(
                    "key_columns",
                    "is required with mode merge: the columns that pick out a row",
                ))
            }
            ("append" | "truncate", false) => {
                return Err(ConnectorError::property(
                    "key_columns",
                    "goes with mode merge; append and truncate do not match rows",
                ))
            }
            ("append", true) => Mode::Append,
            ("truncate", true) => Mode::Truncate,
            (other, _) => {
                return Err(ConnectorError::property(
                    "mode",
                    format!("'{other}' is not one of append, truncate, merge"),
                ))
            }
        };
        Ok(SinkSettings { table, mode })
    }
}

/// A value bound as text: SQL Server converts it to the column's type.
fn bound(value: &JsonValue) -> Text {
    Text(match value {
        JsonValue::Null => None,
        JsonValue::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    })
}

/// The `MERGE` that applies the staged rows, the last row of each key winning.
pub(crate) fn merge_statement(table: &str, columns: &[String], keys: &[String]) -> String {
    let names = columns
        .iter()
        .map(|c| identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    let partition = keys
        .iter()
        .map(|k| identifier(k))
        .collect::<Vec<_>>()
        .join(", ");
    let on = keys
        .iter()
        .map(|k| format!("[t].{0} = [s].{0}", identifier(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let updates = columns
        .iter()
        .filter(|c| !keys.contains(c))
        .map(|c| format!("[t].{0} = [s].{0}", identifier(c)))
        .collect::<Vec<_>>();
    let values = columns
        .iter()
        .map(|c| format!("[s].{}", identifier(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let matched = if updates.is_empty() {
        String::new()
    } else {
        format!(" WHEN MATCHED THEN UPDATE SET {}", updates.join(", "))
    };
    format!(
        "MERGE {table} WITH (HOLDLOCK) AS [t] USING (SELECT {names} FROM (SELECT *, \
         ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {STAGE_ROW} DESC) AS [etl_rank] \
         FROM {STAGE}) AS [r] WHERE [etl_rank] = 1) AS [s] ON {on}{matched} WHEN NOT MATCHED BY \
         TARGET THEN INSERT ({names}) VALUES ({values});"
    )
}

/// Every row of `input` into the table: an `INSERT` per batch, straight in, or
/// into the staging table and then one `MERGE`.
pub(crate) fn insert(
    link: &mut Link,
    settings: &SinkSettings,
    input: &mut dyn RecordReader,
) -> Result<Summary, ConnectorError> {
    let table = &settings.table;
    let place = link.place.clone();
    let keys = match &settings.mode {
        Mode::Merge(keys) => Some(keys.as_slice()),
        _ => None,
    };
    let mut inserted = 0u64;
    let mut statements = 0u64;
    let failed = |inserted: u64, error: String| {
        ConnectorError::Data(match keys {
            None => format!(
                "{error}. {inserted} row(s) had been inserted into {table} before this, and stay"
            ),
            Some(_) => format!(
                "{error}. Nothing was merged into {table}: {inserted} row(s) had been staged, and \
                 go with the connection"
            ),
        })
    };
    let said = |what: &str, failure: Failure| format!("{what} at {place}: {}", failure.message());

    if settings.mode == Mode::Truncate {
        link.execute(&format!("TRUNCATE TABLE {table}"), &[])
            .map_err(|failure| failed(0, said(&format!("TRUNCATE TABLE {table}"), failure)))?;
    }

    let mut columns: Option<Vec<String>> = None;
    let mut per_statement = MAX_ROWS;
    let mut batch: Vec<Vec<Text>> = Vec::new();
    let mut row = 0u64;
    let target = if keys.is_some() {
        STAGE
    } else {
        table.as_str()
    };

    let flush = |link: &mut Link, batch: &mut Vec<Vec<Text>>, columns: &[String]| {
        let mut names: Vec<String> = columns.iter().map(|c| identifier(c)).collect();
        if keys.is_some() {
            names.push(STAGE_ROW.to_string());
        }
        let width = names.len();
        let rows = (0..batch.len())
            .map(|r| {
                let marks = (1..=width)
                    .map(|c| format!("@P{}", r * width + c))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({marks})")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("INSERT INTO {target} ({}) VALUES {rows}", names.join(", "));
        let values: Vec<Text> = batch.drain(..).flatten().collect();
        link.execute(&sql, &values)
            .map_err(|failure| said(&format!("INSERT INTO {target}"), failure))
    };

    while let Some(record) = input.read()? {
        row += 1;
        if columns.is_none() {
            let names: Vec<String> = record.keys().cloned().collect();
            let width = names.len() + usize::from(keys.is_some());
            if width > MAX_PARAMETERS {
                return Err(failed(
                    inserted,
                    format!(
                        "rows have {} columns, more than SQL Server's 2,100 parameters to a \
                         statement allow",
                        names.len()
                    ),
                ));
            }
            per_statement = (MAX_PARAMETERS / width).clamp(1, MAX_ROWS);
            if let Some(keys) = keys {
                if let Some(missing) = keys.iter().find(|key| !names.contains(key)) {
                    return Err(ConnectorError::property(
                        "key_columns",
                        format!("'{missing}' is not a column of the rows written"),
                    ));
                }
                // Only the columns written, and without IDENTITY: a SELECT
                // INTO over a UNION does not copy it.
                let listed = names
                    .iter()
                    .map(|c| identifier(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let create = format!(
                    "SELECT CAST(NULL AS bigint) AS {STAGE_ROW}, {listed} INTO {STAGE} FROM \
                     {table} WHERE 1 = 0 UNION ALL SELECT CAST(NULL AS bigint), {listed} FROM \
                     {table} WHERE 1 = 0"
                );
                link.batch(&create)
                    .map_err(|failure| failed(0, said(&format!("staging for {table}"), failure)))?;
            }
            columns = Some(names);
        }
        let known = columns.as_ref().expect("set above");
        if let Some(extra) = record.keys().find(|key| !known.contains(key)) {
            return Err(failed(
                inserted,
                format!("row {row} has a column '{extra}' the first row did not"),
            ));
        }
        let mut values: Vec<Text> = known
            .iter()
            .map(|column| bound(record.get(column).unwrap_or(&JsonValue::Null)))
            .collect();
        if keys.is_some() {
            values.push(Text(Some(row.to_string())));
        }
        batch.push(values);
        if batch.len() == per_statement {
            let size = batch.len() as u64;
            flush(link, &mut batch, known).map_err(|error| failed(inserted, error))?;
            inserted += size;
            statements += 1;
        }
    }
    if let Some(known) = &columns {
        if !batch.is_empty() {
            let size = batch.len() as u64;
            flush(link, &mut batch, known).map_err(|error| failed(inserted, error))?;
            inserted += size;
            statements += 1;
        }
    }

    let detail = match (keys, &columns) {
        (Some(keys), Some(known)) => {
            let changed = link
                .execute(&merge_statement(table, known, keys), &[])
                .map_err(|failure| {
                    ConnectorError::Data(format!(
                        "{}. Nothing was merged into {table}: MERGE is one statement",
                        said(&format!("MERGE INTO {table}"), failure)
                    ))
                })?;
            let _ = link.batch(&format!("DROP TABLE {STAGE}"));
            format!(
                "{inserted} row(s) merged into {table} at {place} on {}: staged in {statements} \
                 INSERT(s), then {changed} row(s) inserted or updated by one MERGE",
                keys.join(", ")
            )
        }
        (Some(_), None) => format!("0 row(s): nothing to merge into {table} at {place}"),
        (None, _) => format!(
            "{inserted} row(s) {} {table} at {place} in {statements} INSERT(s)",
            if settings.mode == Mode::Truncate {
                "replaced the rows of"
            } else {
                "appended to"
            },
        ),
    };
    Ok(Summary::new(inserted, detail))
}
