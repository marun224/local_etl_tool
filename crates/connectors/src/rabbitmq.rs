//! RabbitMQ: a queue read with `basic.get` in **bounded batches** and **held
//! until the run's outcome is known**, and an exchange published to with
//! publisher confirms. AMQP 0-9-1, classic and quorum queues alike.
//!
//! RabbitMQ holds a message it handed out for as long as the channel that
//! received it stays open, and hands it out again when that channel closes
//! unacknowledged (Settled decision 60). So the [`Receipt`] owns the
//! connection and the channel, and settles with one message:
//!
//! - **acknowledged** after the run fully succeeded and its sinks delivered:
//!   `basic.ack` with `multiple`, up to the last delivery;
//! - **released** on every other path, a preview included: `basic.nack` with
//!   `multiple` and `requeue`. Closing the connection gives them back too, so a
//!   release that cannot be sent still releases.
//!
//! There is no lease to keep: `lapin` drives the connection, heartbeats
//! included, on a thread of its own while the run goes on. The broker's own
//! limit is `consumer_timeout` (30 minutes by default), after which it closes
//! the channel and the messages go back; the acknowledgement then fails and
//! the run warns.
//!
//! The client is `lapin`, on a small `tokio` runtime the receipt owns. Its TLS
//! is ours, [`crate::tls`], handed to it through its own connect function, so
//! RabbitMQ trusts what Kafka and NATS trust. **Every call has a deadline**:
//! `lapin` never answers a connect to a vhost that does not exist, although
//! the broker has refused it (found in the probe), so a call that outlasts
//! `timeout_ms` fails, saying so.

use crate::http::{positive, text};
use crate::kafka::{timestamp_text, value_columns, Format};
use async_rs::traits::*;
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Receipt, Record, RecordReader, RecordWriter, Sink,
    Source, Summary,
};
use lapin::options::{
    BasicAckOptions, BasicGetOptions, BasicNackOptions, BasicPublishOptions, ConfirmSelectOptions,
};
use lapin::tcp::RustlsConnector;
use lapin::types::{AMQPValue, FieldTable};
use lapin::uri::{AMQPScheme, AMQPUri};
use lapin::{AsyncTcpStream, BasicProperties, Channel, Confirmation, Connection};
use serde_json::{json, Map, Value as JsonValue};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// `src.queue.rabbitmq`.
pub struct RabbitmqSource;

/// `snk.queue.rabbitmq`.
pub struct RabbitmqSink;

/// The columns every row carries, whatever `value_format` says.
pub(crate) const METADATA_COLUMNS: [&str; 7] = [
    "_queue",
    "_exchange",
    "_routing_key",
    "_redelivered",
    "_message_id",
    "_timestamp",
    "_headers",
];

/// How many messages are published before their confirms are awaited.
const CONFIRM_BATCH: usize = 1000;

// ---------------------------------------------------------------------------
// The broker
// ---------------------------------------------------------------------------

/// The properties every RabbitMQ component has, after its own identifying ones.
fn connection_properties() -> Vec<PropertySpec> {
    vec![
        PropertySpec::text("url").required().help(
            "amqp://host[:port][/vhost], or amqps:// for TLS. A user and password may be in it, \
             but username and password keep the password out of the URL.",
        ),
        PropertySpec::text("username").help("Unset: the URL's, else guest."),
        PropertySpec::text("password")
            .help("Use ${SECRET:name} rather than the value itself. Unset: the URL's."),
        PropertySpec::text("vhost").help("The virtual host. Unset: the URL's, else /."),
        PropertySpec::path("ca_cert").help(
            "amqps only: a PEM file of the certificate authority for a broker with a private \
             certificate. Unset, the bundled public roots.",
        ),
        PropertySpec::integer("timeout_ms")
            .default(JsonValue::from(30_000))
            .help("How long one call to the broker may take, connecting included."),
    ]
}

/// Where to connect, and how. Its `Debug` leaves the password out.
#[derive(Clone)]
pub(crate) struct Server {
    pub(crate) uri: AMQPUri,
    pub(crate) ca_cert: Option<String>,
    pub(crate) timeout: Duration,
}

impl Server {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let url = text(properties, "url")
            .map(str::trim)
            .ok_or_else(|| ConnectorError::property("url", "is required"))?;
        if !(url.starts_with("amqp://") || url.starts_with("amqps://")) {
            return Err(ConnectorError::property(
                "url",
                "must start with amqp:// or amqps://",
            ));
        }
        // Not the parser's own words: they can quote the URL, password and all.
        let mut uri: AMQPUri = url.parse().map_err(|_| {
            ConnectorError::property(
                "url",
                "is not an AMQP URL: amqp://[user[:password]@]host[:port][/vhost]",
            )
        })?;
        if let Some(username) = text(properties, "username") {
            uri.authority.userinfo.username = username.to_string();
        }
        if let Some(password) = text(properties, "password") {
            uri.authority.userinfo.password = password.to_string();
        }
        if let Some(vhost) = text(properties, "vhost") {
            uri.vhost = vhost.to_string();
        }
        let ca_cert = text(properties, "ca_cert").map(str::to_string);
        if ca_cert.is_some() && uri.scheme != AMQPScheme::AMQPS {
            return Err(ConnectorError::property(
                "ca_cert",
                "is for TLS; the url starts with amqp://, not amqps://",
            ));
        }
        Ok(Server {
            uri,
            ca_cert,
            timeout: Duration::from_millis(positive(properties, "timeout_ms", 30_000)?),
        })
    }

    /// `host:port, vhost '/'`, for messages. Never the password.
    pub(crate) fn place(&self) -> String {
        format!(
            "{}:{}, vhost '{}'",
            self.uri.authority.host, self.uri.authority.port, self.uri.vhost
        )
    }
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("place", &self.place())
            .field("user", &self.uri.authority.userinfo.username)
            .field("ca_cert", &self.ca_cert)
            .finish_non_exhaustive()
    }
}

type Runtime = async_rs::Runtime<async_rs::Tokio>;

/// An open connection and one channel on it, and the runtime they live on.
pub(crate) struct Link {
    runtime: Runtime,
    connection: Connection,
    pub(crate) channel: Channel,
    timeout: Duration,
}

impl Link {
    pub(crate) fn open(server: &Server, context: &Context) -> Result<Link, ConnectorError> {
        let tls = match server.uri.scheme {
            AMQPScheme::AMQPS => Some(Arc::new(RustlsConnector::from(crate::tls::client_config(
                server.ca_cert.as_deref(),
                context,
            )?))),
            AMQPScheme::AMQP => None,
        };
        // Its own threads: one worker for the heartbeat and the connect, and
        // `lapin`'s I/O thread, so the connection stays alive between calls.
        let runtime = async_rs::Runtime::tokio_with_runtime(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|error| ConnectorError::Data(format!("RabbitMQ: {error}")))?,
        );

        let uri = server.uri.clone();
        let connect = Connection::connector(
            uri,
            runtime.clone(),
            async move |uri, runtime| {
                let addresses =
                    runtime.to_socket_addrs((uri.authority.host.clone(), uri.authority.port));
                let stream = AsyncTcpStream::connect(&runtime, addresses).await?;
                Ok(match &tls {
                    Some(tls) => stream.into_rustls(tls, &uri.authority.host).await?,
                    None => stream,
                })
            },
            lapin::ConnectionProperties::default(),
        );
        let place = server.place();
        let connection = within(&runtime, server.timeout, connect).map_err(|failure| {
            ConnectorError::Data(match failure {
                Failure::Refused(error) => format!("RabbitMQ at {place}: {error}"),
                Failure::Silent(ms) => format!(
                    "RabbitMQ at {place} gave no answer within {ms} ms while connecting. It \
                     gives none at all when the vhost does not exist, so check the vhost first"
                ),
            })
        })?;
        let channel = within(&runtime, server.timeout, connection.create_channel())
            .map_err(|failure| failure.said("opening a channel"))?;
        Ok(Link {
            runtime,
            connection,
            channel,
            timeout: server.timeout,
        })
    }

    /// One call, with the deadline.
    pub(crate) fn run<T>(
        &self,
        what: &str,
        work: impl Future<Output = lapin::Result<T>>,
    ) -> Result<T, ConnectorError> {
        within(&self.runtime, self.timeout, work).map_err(|failure| failure.said(what))
    }

    /// Close the channel, then the connection, as far as the broker answers.
    /// Whatever is still held unacknowledged goes back to its queue.
    pub(crate) fn close(self) {
        let _ = self.run("channel.close", self.channel.close(200, "done".into()));
        let _ = self.run(
            "connection.close",
            self.connection.close(200, "done".into()),
        );
    }
}

/// Why a call came to nothing.
enum Failure {
    /// The broker, or the network, said no.
    Refused(lapin::Error),
    /// Nothing within the deadline, in milliseconds.
    Silent(u128),
}

impl Failure {
    fn said(self, what: &str) -> ConnectorError {
        ConnectorError::Data(match self {
            Failure::Refused(error) => format!("RabbitMQ {what}: {error}"),
            Failure::Silent(ms) => format!("RabbitMQ {what}: no answer within {ms} ms"),
        })
    }
}

fn within<T>(
    runtime: &Runtime,
    limit: Duration,
    work: impl Future<Output = lapin::Result<T>>,
) -> Result<T, Failure> {
    runtime.block_on(async {
        match tokio::time::timeout(limit, work).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(Failure::Refused(error)),
            Err(_) => Err(Failure::Silent(limit.as_millis())),
        }
    })
}

// ---------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------

impl Source for RabbitmqSource {
    fn spec(&self) -> ComponentSpec {
        let mut properties = vec![PropertySpec::text("queue")
            .required()
            .help("The queue to read. A classic or a quorum queue; it must already exist.")];
        properties.extend(connection_properties());
        properties.extend([
            PropertySpec::integer("max_records")
                .default(JsonValue::from(10_000))
                .help(
                    "The most one run receives. Every message received is held until the run \
                     ends, so this is lower than for a stream.",
                ),
            PropertySpec::integer("max_wait_ms")
                .default(JsonValue::from(30_000))
                .help("The longest one run spends receiving."),
            PropertySpec::enumerated("value_format", &["json", "text", "bytes"])
                .default(JsonValue::String("json".into()))
                .help(
                    "json makes each message body's fields into columns; text gives a value \
                     column; bytes gives value as base64. Every row also has _queue, _exchange, \
                     _routing_key, _redelivered, _message_id, _timestamp and _headers.",
                ),
            columns_property(),
        ]);
        ComponentSpec::new("src.queue.rabbitmq", "RabbitMQ queue")
            .description(
                "Receive messages from a RabbitMQ queue in bounded batches. They are held until \
                 the run ends: acknowledged if it succeeded, given back if not.",
            )
            .icon("inbox")
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
        // Outside a run nothing can say it succeeded, so give everything back.
        let (summary, receipt) = self.read_held(properties, out, context)?;
        if let Some(receipt) = receipt {
            receipt.release()?;
        }
        Ok(summary)
    }

    fn read_held(
        &self,
        properties: &JsonValue,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<(Summary, Option<Box<dyn Receipt>>), ConnectorError> {
        let settings = SourceSettings::from(properties)?;
        let (summary, receipt) = receive(&settings, out, context)?;
        Ok((summary, Some(Box::new(receipt))))
    }
}

#[derive(Debug)]
pub(crate) struct SourceSettings {
    pub(crate) server: Server,
    pub(crate) queue: String,
    pub(crate) max_records: u64,
    pub(crate) max_wait: Duration,
    pub(crate) format: Format,
}

impl SourceSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let queue = text(properties, "queue")
            .map(str::trim)
            .ok_or_else(|| ConnectorError::property("queue", "is required"))?
            .to_string();
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
            server: Server::from(properties)?,
            queue,
            max_records: positive(properties, "max_records", 10_000)?,
            max_wait: Duration::from_millis(positive(properties, "max_wait_ms", 30_000)?),
            format,
        })
    }
}

/// Why receiving stopped, for the report.
enum Stop {
    Empty,
    Cap,
    Waited,
}

/// Receive up to `max_records`, writing each message as a row, and return what
/// is held. The receipt exists before the first message arrives, so anything
/// that fails part-way drops it, which gives the messages back.
pub(crate) fn receive(
    settings: &SourceSettings,
    out: &mut dyn RecordWriter,
    context: &Context,
) -> Result<(Summary, RabbitmqReceipt), ConnectorError> {
    let link = Link::open(&settings.server, context)?;
    let queue = settings.queue.clone();
    let mut held = RabbitmqReceipt {
        link: Some(link),
        queue: queue.clone(),
        last: None,
        count: 0,
    };

    let started = Instant::now();
    let stop = loop {
        if held.count >= settings.max_records {
            break Stop::Cap;
        }
        if started.elapsed() >= settings.max_wait {
            break Stop::Waited;
        }
        let link = held.link.as_ref().expect("held until settled");
        let got = link
            .run(
                "basic.get",
                link.channel
                    .basic_get(queue.as_str().into(), BasicGetOptions { no_ack: false }),
            )
            .map_err(|error| ConnectorError::Data(format!("queue '{queue}': {error}")))?;
        let Some(message) = got else {
            break Stop::Empty;
        };
        // Held first: a row that will not decode is still given back.
        held.last = Some(message.delivery.delivery_tag);
        held.count += 1;
        out.write(row(&queue, &message.delivery, settings.format)?)?;
    };

    let count = held.count;
    let mut detail = format!(
        "{count} message(s) from queue '{queue}' at {}, held until the run ends",
        settings.server.place()
    );
    detail.push_str(&match stop {
        Stop::Empty => "; the queue answered empty".to_string(),
        Stop::Cap => format!(
            "; stopped at max_records ({}), with more for the next run",
            settings.max_records
        ),
        Stop::Waited => format!(
            "; stopped after max_wait_ms ({})",
            settings.max_wait.as_millis()
        ),
    });
    Ok((Summary::new(count, detail), held))
}

/// One delivery as a row.
pub(crate) fn row(
    queue: &str,
    delivery: &lapin::message::Delivery,
    format: Format,
) -> Result<Record, ConnectorError> {
    let properties = &delivery.properties;
    let id = properties
        .message_id()
        .as_ref()
        .map(|id| id.as_str().to_string());
    let at = match &id {
        Some(id) => format!("message {id}"),
        None => format!("delivery {}", delivery.delivery_tag),
    };
    let value = if delivery.data.is_empty() && format == Format::Json {
        None
    } else {
        Some(delivery.data.as_slice())
    };
    let mut row = value_columns(format, value, &at, &METADATA_COLUMNS)?;

    let text_or_null = |text: &str| {
        if text.is_empty() {
            JsonValue::Null
        } else {
            json!(text)
        }
    };
    row.insert("_queue".to_string(), json!(queue));
    row.insert(
        "_exchange".to_string(),
        text_or_null(delivery.exchange.as_str()),
    );
    row.insert(
        "_routing_key".to_string(),
        text_or_null(delivery.routing_key.as_str()),
    );
    row.insert("_redelivered".to_string(), json!(delivery.redelivered));
    row.insert(
        "_message_id".to_string(),
        id.map_or(JsonValue::Null, JsonValue::from),
    );
    row.insert(
        "_timestamp".to_string(),
        properties.timestamp().map_or(JsonValue::Null, |seconds| {
            json!(timestamp_text(seconds as i64 * 1000))
        }),
    );
    row.insert(
        "_headers".to_string(),
        properties.headers().as_ref().map_or(json!({}), table_json),
    );
    Ok(row)
}

/// AMQP headers as JSON: numbers and booleans as themselves, strings and
/// bytes as text, tables and arrays nested, a timestamp as its seconds.
pub(crate) fn table_json(table: &FieldTable) -> JsonValue {
    JsonValue::Object(
        table
            .inner()
            .iter()
            .map(|(name, value)| (name.as_str().to_string(), value_json(value)))
            .collect::<Map<_, _>>(),
    )
}

fn value_json(value: &AMQPValue) -> JsonValue {
    match value {
        AMQPValue::Boolean(value) => json!(value),
        AMQPValue::ShortShortInt(value) => json!(value),
        AMQPValue::ShortShortUInt(value) => json!(value),
        AMQPValue::ShortInt(value) => json!(value),
        AMQPValue::ShortUInt(value) => json!(value),
        AMQPValue::LongInt(value) => json!(value),
        AMQPValue::LongUInt(value) => json!(value),
        AMQPValue::LongLongInt(value) => json!(value),
        AMQPValue::Float(value) => json!(value),
        AMQPValue::Double(value) => json!(value),
        AMQPValue::DecimalValue(decimal) => {
            json!(f64::from(decimal.value) / 10f64.powi(i32::from(decimal.scale)))
        }
        AMQPValue::ShortString(text) => json!(text.as_str()),
        AMQPValue::LongString(text) => json!(String::from_utf8_lossy(text.as_bytes())),
        AMQPValue::FieldArray(array) => {
            JsonValue::Array(array.as_slice().iter().map(value_json).collect())
        }
        AMQPValue::Timestamp(seconds) => json!(seconds),
        AMQPValue::FieldTable(table) => table_json(table),
        AMQPValue::ByteArray(bytes) => json!(String::from_utf8_lossy(bytes.as_slice())),
        AMQPValue::Void => JsonValue::Null,
    }
}

// ---------------------------------------------------------------------------
// The receipt
// ---------------------------------------------------------------------------

/// What one run received and is holding: the connection and channel it came
/// on, and the last delivery tag, which settles everything up to it at once.
pub(crate) struct RabbitmqReceipt {
    link: Option<Link>,
    queue: String,
    last: Option<u64>,
    count: u64,
}

impl RabbitmqReceipt {
    fn settle(&mut self, acknowledge: bool) -> Result<String, ConnectorError> {
        let Some(link) = self.link.take() else {
            return Ok(format!("nothing held from queue '{}'", self.queue));
        };
        let (queue, count) = (&self.queue, self.count);
        let Some(last) = self.last else {
            link.close();
            return Ok(format!("nothing held from queue '{queue}'"));
        };

        let outcome = if acknowledge {
            link.run(
                "basic.ack",
                link.channel
                    .basic_ack(last, BasicAckOptions { multiple: true }),
            )
        } else {
            link.run(
                "basic.nack",
                link.channel.basic_nack(
                    last,
                    BasicNackOptions {
                        multiple: true,
                        requeue: true,
                    },
                ),
            )
        };
        // Closing waits for the broker's close-ok, which comes after it has
        // taken the acknowledgement; and whatever is still unacknowledged
        // goes back to the queue.
        link.close();

        match (outcome, acknowledge) {
            (Ok(()), true) => Ok(format!(
                "{count} message(s) acknowledged on queue '{queue}'"
            )),
            (Ok(()), false) => Ok(format!(
                "{count} message(s) released back to queue '{queue}'"
            )),
            (Err(error), true) => Err(ConnectorError::Data(format!(
                "{count} message(s) from queue '{queue}' could not be acknowledged, so the \
                 broker will deliver them again: {error}"
            ))),
            // Closing the connection gave them back all the same.
            (Err(error), false) => Ok(format!(
                "{count} message(s) released back to queue '{queue}' by closing the connection \
                 ({error})"
            )),
        }
    }
}

impl Receipt for RabbitmqReceipt {
    fn acknowledge(mut self: Box<Self>) -> Result<String, ConnectorError> {
        self.settle(true)
    }

    fn release(mut self: Box<Self>) -> Result<String, ConnectorError> {
        self.settle(false)
    }
}

impl Drop for RabbitmqReceipt {
    fn drop(&mut self) {
        if self.link.is_some() {
            let _ = self.settle(false);
        }
    }
}

// ---------------------------------------------------------------------------
// The sink
// ---------------------------------------------------------------------------

impl Sink for RabbitmqSink {
    fn spec(&self) -> ComponentSpec {
        let mut properties = vec![PropertySpec::text("exchange").help(
            "The exchange to publish to. Unset: the default exchange, which routes by queue name.",
        )];
        properties.extend(connection_properties());
        properties.extend([
            PropertySpec::text("routing_key").help(
                "Every message's routing key; for the default exchange, the queue's name. Or \
                 give routing_key_column.",
            ),
            PropertySpec::text("routing_key_column")
                .help("The column whose value is each message's routing key."),
            PropertySpec::boolean("persistent")
                .default(JsonValue::Bool(true))
                .help("Mark each message persistent, so a durable queue keeps it over a restart."),
        ]);
        ComponentSpec::new("snk.queue.rabbitmq", "RabbitMQ exchange")
            .description(
                "Publish rows to a RabbitMQ exchange, one JSON message each, with publisher \
                 confirms. A message no queue receives fails the run.",
            )
            .icon("inbox")
            .properties(properties)
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
        let link = Link::open(&settings.server, context)?;
        let result = publish(&link, &settings, input);
        link.close();
        result
    }
}

#[derive(Debug)]
pub(crate) struct SinkSettings {
    pub(crate) server: Server,
    pub(crate) exchange: String,
    pub(crate) routing: Routing,
    pub(crate) persistent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Routing {
    Fixed(String),
    Column(String),
}

impl SinkSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let routing = match (
            text(properties, "routing_key"),
            text(properties, "routing_key_column"),
        ) {
            (Some(_), Some(_)) => {
                return Err(ConnectorError::property(
                    "routing_key",
                    "and routing_key_column both say where messages go; give one",
                ))
            }
            (Some(key), None) => Routing::Fixed(key.to_string()),
            (None, Some(column)) => Routing::Column(column.to_string()),
            (None, None) => Routing::Fixed(String::new()),
        };
        let exchange = text(properties, "exchange")
            .unwrap_or("")
            .trim()
            .to_string();
        if exchange.is_empty() && routing == Routing::Fixed(String::new()) {
            return Err(ConnectorError::property(
                "routing_key",
                "is required with the default exchange: it names the queue",
            ));
        }
        Ok(SinkSettings {
            server: Server::from(properties)?,
            exchange,
            routing,
            persistent: properties
                .get("persistent")
                .and_then(JsonValue::as_bool)
                .unwrap_or(true),
        })
    }

    fn describe(&self) -> String {
        let exchange = if self.exchange.is_empty() {
            "the default exchange".to_string()
        } else {
            format!("exchange '{}'", self.exchange)
        };
        match &self.routing {
            Routing::Fixed(key) => format!("{exchange}, routing key '{key}'"),
            Routing::Column(column) => format!("{exchange}, routing keys from '{column}'"),
        }
    }
}

/// A row's routing key.
fn routing_key(row: u64, record: &Record, routing: &Routing) -> Result<String, ConnectorError> {
    match routing {
        Routing::Fixed(key) => Ok(key.clone()),
        Routing::Column(column) => match record.get(column) {
            None => Err(ConnectorError::property(
                "routing_key_column",
                format!("'{column}' is not a column of the rows"),
            )),
            Some(JsonValue::Null) => Err(ConnectorError::Data(format!(
                "row {row}: '{column}' is null, and every message needs a routing key"
            ))),
            Some(JsonValue::String(key)) => Ok(key.clone()),
            Some(other) => Ok(other.to_string()),
        },
    }
}

/// Every row of `input` to the exchange, confirms awaited every
/// [`CONFIRM_BATCH`] messages and at the end.
pub(crate) fn publish(
    link: &Link,
    settings: &SinkSettings,
    input: &mut dyn RecordReader,
) -> Result<Summary, ConnectorError> {
    link.run(
        "confirm.select",
        link.channel.confirm_select(ConfirmSelectOptions::default()),
    )?;
    let mut properties = BasicProperties::default().with_content_type("application/json".into());
    if settings.persistent {
        properties = properties.with_delivery_mode(2);
    }

    let place = settings.describe();
    let mut confirmed = 0u64;
    let mut batches = 0u64;
    let mut pending = Vec::new();
    let mut row = 0u64;
    let failed = |confirmed: u64, error: ConnectorError| {
        ConnectorError::Data(format!(
            "{error}. {confirmed} message(s) to {place} had been confirmed before this, and stay \
             there"
        ))
    };

    while let Some(record) = input.read()? {
        row += 1;
        let key = routing_key(row, &record, &settings.routing).map_err(|error| match error {
            // A setting that cannot work says so plainly, before anything is sent.
            ConnectorError::Property { .. } if confirmed == 0 && pending.is_empty() => error,
            other => failed(confirmed, other),
        })?;
        let body =
            serde_json::to_vec(&record).map_err(|error| ConnectorError::Data(error.to_string()))?;
        let confirm = link
            .run(
                "basic.publish",
                link.channel.basic_publish(
                    settings.exchange.as_str().into(),
                    key.as_str().into(),
                    BasicPublishOptions {
                        mandatory: true,
                        ..Default::default()
                    },
                    &body,
                    properties.clone(),
                ),
            )
            .map_err(|error| failed(confirmed, error))?;
        pending.push((row, key, confirm));
        if pending.len() == CONFIRM_BATCH {
            await_confirms(link, std::mem::take(&mut pending), &mut confirmed)
                .map_err(|error| failed(confirmed, error))?;
            batches += 1;
        }
    }
    if !pending.is_empty() {
        await_confirms(link, pending, &mut confirmed).map_err(|error| failed(confirmed, error))?;
        batches += 1;
    }

    let detail = if confirmed == 0 {
        format!("0 messages; nothing published to {place}")
    } else {
        format!(
            "{confirmed} message(s) to {place} at {}, confirmed in {batches} batch(es)",
            settings.server.place()
        )
    };
    Ok(Summary::new(confirmed, detail))
}

/// Wait for each message's confirm, in order. A message the broker could not
/// route (`mandatory`) or refused fails, naming the row. Each confirm is
/// counted into `confirmed` as it comes, so a failure can say how many landed.
fn await_confirms(
    link: &Link,
    pending: Vec<(u64, String, lapin::PublisherConfirm)>,
    confirmed: &mut u64,
) -> Result<(), ConnectorError> {
    for (row, key, confirm) in pending {
        match link.run("publisher confirm", confirm)? {
            Confirmation::Ack(None) | Confirmation::NotRequested => *confirmed += 1,
            Confirmation::Ack(Some(returned)) => {
                return Err(ConnectorError::Data(format!(
                    "row {row} reached no queue: RabbitMQ returned it ({} {}) for routing key \
                     '{key}'",
                    returned.reply_code, returned.reply_text
                )))
            }
            Confirmation::Nack(_) => {
                return Err(ConnectorError::Data(format!(
                    "RabbitMQ refused row {row} (a negative publisher confirm)"
                )))
            }
        }
    }
    Ok(())
}
