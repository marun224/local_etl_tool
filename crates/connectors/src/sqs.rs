//! Amazon SQS: standard and FIFO queues, received in **bounded batches** and
//! **held until the run's outcome is known**, and sent to.
//!
//! A queue keeps no position a consumer can come back to. It hands a message
//! out, hides it for a *visibility timeout*, and deletes it only when told to;
//! told nothing, it shows the message again. So this source does not save
//! where it got to (Kafka's, NATS's and Kinesis's model). It receives, holds
//! what it received, and hands the engine a [`Receipt`] (Settled decision 58):
//!
//! - **acknowledged** after the run fully succeeded and its sinks delivered:
//!   `DeleteMessageBatch`, and the messages are gone;
//! - **released** on every other path, a preview included:
//!   `ChangeMessageVisibilityBatch` to 0, and they come back at once.
//!
//! While the run goes on, a **lease keeper** thread extends the hold every
//! half-period, so a run longer than `visibility_seconds` does not see its
//! messages handed to someone else (decision 60).
//!
//! A batch ends at `max_records`, when a receive with a one-second wait comes
//! back empty, or at `max_wait_ms` (decision 61). SQS speaks AWS's JSON
//! protocol, signed as Kinesis is, through [`crate::aws::JsonApi`]: no `tokio`.
//!
//! `snk.queue.sqs` sends each row as one JSON message, ten to a
//! `SendMessageBatch`.

use crate::aws::{self, JsonApi, Sources};
use crate::http::{positive, text};
use crate::kafka::{timestamp_text, value_columns, Format};
use crate::lease::Keeper;
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Receipt, Record, RecordReader, RecordWriter, Sink,
    Source, Summary,
};
use serde_json::{json, Map, Value as JsonValue};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// `src.queue.sqs`.
pub struct SqsSource;

/// `snk.queue.sqs`.
pub struct SqsSink;

/// How SQS speaks AWS's JSON protocol.
pub(crate) static SQS: aws::Protocol = aws::Protocol {
    name: "SQS",
    service: "sqs",
    target_prefix: "AmazonSQS",
    content_type: "application/x-amz-json-1.0",
    throttled,
};

/// A "slow down": `ThrottlingException`, `RequestThrottled`, `KmsThrottled`.
fn throttled(status: u16, body: &str) -> bool {
    status == 400 && body.contains("Throttl")
}

/// The columns every row carries, whatever `value_format` says.
pub(crate) const METADATA_COLUMNS: [&str; 6] = [
    "_queue",
    "_message_id",
    "_sent_timestamp",
    "_receive_count",
    "_group_id",
    "_attributes",
];

/// The most one `ReceiveMessage` returns, and one batch call takes.
const BATCH_LIMIT: usize = 10;

/// The largest message SQS takes, and the most one `SendMessageBatch` carries.
const MESSAGE_BYTES: usize = 1024 * 1024;

/// How long a receive waits for a message before saying the queue is empty.
/// Long polling asks every server the queue lives on, so an empty answer after
/// a wait means empty, where a short poll can miss messages that are there.
const WAIT_SECONDS: u64 = 1;

/// The longest hold SQS allows: twelve hours.
const MAX_VISIBILITY: u64 = 43_200;

/// The first wait before sending refused messages again, doubling each time.
const RESEND_BACKOFF: Duration = Duration::from_millis(200);

// ---------------------------------------------------------------------------
// The queue
// ---------------------------------------------------------------------------

/// The properties every SQS component has, first, then `own`.
fn with_connection(own: Vec<PropertySpec>) -> Vec<PropertySpec> {
    let mut properties = vec![
        PropertySpec::text("queue_url").help(
            "The queue's URL, e.g. https://sqs.eu-west-1.amazonaws.com/123456789012/orders. \
             Or give queue instead.",
        ),
        PropertySpec::text("queue").help(
            "The queue's name, looked up with GetQueueUrl. A FIFO queue's name ends in .fifo.",
        ),
        PropertySpec::text("queue_owner")
            .help("With queue: the AWS account that owns it, when it is not yours."),
    ];
    properties.extend(aws::connection_properties("sqs"));
    properties.extend(own);
    properties
}

/// Which queue, as the properties say it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    Url(String),
    Named { name: String, owner: Option<String> },
}

impl Target {
    fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let owner = text(properties, "queue_owner").map(str::to_string);
        match (text(properties, "queue_url"), text(properties, "queue")) {
            (Some(url), None) => {
                if owner.is_some() {
                    return Err(ConnectorError::property(
                        "queue_owner",
                        "goes with queue; a queue_url already names the account",
                    ));
                }
                if !(url.starts_with("https://") || url.starts_with("http://")) {
                    return Err(ConnectorError::property(
                        "queue_url",
                        format!("'{url}' is not an http:// or https:// URL"),
                    ));
                }
                Ok(Target::Url(url.to_string()))
            }
            (None, Some(name)) => Ok(Target::Named {
                name: name.to_string(),
                owner,
            }),
            (Some(_), Some(_)) => Err(ConnectorError::property(
                "queue_url",
                "and queue name the same thing; give one",
            )),
            (None, None) => Err(ConnectorError::property(
                "queue_url",
                "or queue is required",
            )),
        }
    }

    /// Whether it is a FIFO queue, which SQS says by the name's `.fifo`.
    fn fifo(&self) -> bool {
        match self {
            Target::Url(url) => url.ends_with(".fifo"),
            Target::Named { name, .. } => name.ends_with(".fifo"),
        }
    }
}

/// A queue, found: its URL for every call, its name for rows and messages.
#[derive(Debug, Clone)]
pub(crate) struct Queue {
    pub(crate) url: String,
    pub(crate) name: String,
}

pub(crate) fn resolve(api: &mut JsonApi, target: &Target) -> Result<Queue, ConnectorError> {
    let url = match target {
        Target::Url(url) => url.clone(),
        Target::Named { name, owner } => {
            let mut request = json!({ "QueueName": name });
            if let Some(owner) = owner {
                request["QueueOwnerAWSAccountId"] = json!(owner);
            }
            // SQS's own words ("The specified queue does not exist.") do not
            // say which queue, so this does.
            let answer = api
                .call("GetQueueUrl", &request)
                .map_err(|error| ConnectorError::Data(format!("queue '{name}': {error}")))?;
            answer["QueueUrl"]
                .as_str()
                .ok_or_else(|| {
                    ConnectorError::Data(format!("SQS GetQueueUrl: no QueueUrl for '{name}'"))
                })?
                .to_string()
        }
    };
    let name = url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_string();
    Ok(Queue { url, name })
}

// ---------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------

impl Source for SqsSource {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("src.queue.sqs", "SQS queue")
            .description(
                "Receive messages from an Amazon SQS queue in bounded batches. They are held \
                 until the run ends: deleted if it succeeded, given back if not.",
            )
            .icon("inbox")
            .properties(with_connection(vec![
                PropertySpec::integer("max_records")
                    .default(JsonValue::from(10_000))
                    .help(
                        "The most one run receives. Every message received is held until the \
                         run ends, so this is lower than for a stream.",
                    ),
                PropertySpec::integer("max_wait_ms")
                    .default(JsonValue::from(30_000))
                    .help("The longest one run spends receiving."),
                PropertySpec::integer("visibility_seconds")
                    .default(JsonValue::from(300))
                    .help(
                        "How long SQS hides a received message. The hold is extended every half \
                         of this while the run goes on, up to SQS's twelve hours.",
                    ),
                PropertySpec::enumerated("value_format", &["json", "text", "bytes"])
                    .default(JsonValue::String("json".into()))
                    .help(
                        "json makes each message body's fields into columns; text gives a value \
                         column; bytes gives value as base64. Every row also has _queue, \
                         _message_id, _sent_timestamp, _receive_count, _group_id and _attributes.",
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
        _context: &Context,
    ) -> Result<(Summary, Option<Box<dyn Receipt>>), ConnectorError> {
        let settings = SourceSettings::from(properties)?;
        let api = JsonApi::connect(properties, &Sources::process(), &SQS)?;
        let (summary, receipt) = receive(api, &settings, out)?;
        Ok((summary, Some(Box::new(receipt))))
    }
}

#[derive(Debug)]
pub(crate) struct SourceSettings {
    pub(crate) target: Target,
    pub(crate) max_records: u64,
    pub(crate) max_wait: Duration,
    pub(crate) visibility_seconds: u64,
    pub(crate) format: Format,
}

impl SourceSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let target = Target::from(properties)?;
        aws::check_connection(properties)?;
        let visibility_seconds = positive(properties, "visibility_seconds", 300)?;
        if visibility_seconds > MAX_VISIBILITY {
            return Err(ConnectorError::property(
                "visibility_seconds",
                format!("{visibility_seconds} is more than SQS's {MAX_VISIBILITY} (twelve hours)"),
            ));
        }
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
            target,
            max_records: positive(properties, "max_records", 10_000)?,
            max_wait: Duration::from_millis(positive(properties, "max_wait_ms", 30_000)?),
            visibility_seconds,
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
    api: JsonApi,
    settings: &SourceSettings,
    out: &mut dyn RecordWriter,
) -> Result<(Summary, SqsReceipt), ConnectorError> {
    let mut api = api;
    let queue = resolve(&mut api, &settings.target)?;
    let credentials = api.credentials_source().to_string();
    let lease_api = api.duplicate();
    let mut held = SqsReceipt::new(api, &queue, settings.visibility_seconds, lease_api);

    let started = Instant::now();
    let stop = loop {
        let got = held.count() as u64;
        if got >= settings.max_records {
            break Stop::Cap;
        }
        if started.elapsed() >= settings.max_wait {
            break Stop::Waited;
        }
        let want = (settings.max_records - got).min(BATCH_LIMIT as u64);
        let answer = held.api.call(
            "ReceiveMessage",
            &json!({
                "QueueUrl": queue.url,
                "MaxNumberOfMessages": want,
                "WaitTimeSeconds": WAIT_SECONDS,
                "VisibilityTimeout": settings.visibility_seconds,
                "MessageSystemAttributeNames": ["All"],
                "AttributeNames": ["All"],
                "MessageAttributeNames": ["All"],
            }),
        )?;
        let messages = answer["Messages"].as_array().cloned().unwrap_or_default();
        if messages.is_empty() {
            break Stop::Empty;
        }
        for message in &messages {
            let handle = message["ReceiptHandle"].as_str().ok_or_else(|| {
                ConnectorError::Data("SQS ReceiveMessage: a message without a ReceiptHandle".into())
            })?;
            // Held first: a row that will not decode is still given back.
            held.hold(handle);
            out.write(row(&queue.name, message, settings.format)?)?;
        }
    };

    let count = held.count();
    let mut detail = format!(
        "{count} message(s) from queue '{}' (credentials from {credentials}), held until the run \
         ends",
        queue.name
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
    Ok((Summary::new(count as u64, detail), held))
}

/// One message as a row.
pub(crate) fn row(
    queue: &str,
    message: &JsonValue,
    format: Format,
) -> Result<Record, ConnectorError> {
    let id = message["MessageId"].as_str().unwrap_or_default();
    let body = message["Body"].as_str().unwrap_or_default();
    let value = if body.is_empty() && format == Format::Json {
        None
    } else {
        Some(body.as_bytes())
    };
    let mut row = value_columns(format, value, &format!("message {id}"), &METADATA_COLUMNS)?;

    let attributes = &message["Attributes"];
    let number = |key: &str| {
        attributes[key]
            .as_str()
            .and_then(|text| text.parse::<i64>().ok())
    };

    row.insert("_queue".to_string(), json!(queue));
    row.insert("_message_id".to_string(), json!(id));
    row.insert(
        "_sent_timestamp".to_string(),
        number("SentTimestamp").map_or(JsonValue::Null, |millis| json!(timestamp_text(millis))),
    );
    row.insert(
        "_receive_count".to_string(),
        number("ApproximateReceiveCount").map_or(JsonValue::Null, |count| json!(count)),
    );
    row.insert(
        "_group_id".to_string(),
        attributes["MessageGroupId"]
            .as_str()
            .map_or(JsonValue::Null, |group| json!(group)),
    );
    row.insert(
        "_attributes".to_string(),
        message_attributes(&message["MessageAttributes"]),
    );
    Ok(row)
}

/// Message attributes as `{name: value}`: the string value, or the binary one
/// as base64, as SQS sends it.
fn message_attributes(attributes: &JsonValue) -> JsonValue {
    let mut out = Map::new();
    for (name, attribute) in attributes.as_object().into_iter().flatten() {
        let value = attribute["StringValue"]
            .as_str()
            .or_else(|| attribute["BinaryValue"].as_str())
            .map_or(JsonValue::Null, |value| json!(value));
        out.insert(name.clone(), value);
    }
    JsonValue::Object(out)
}

// ---------------------------------------------------------------------------
// The receipt, and the lease keeper
// ---------------------------------------------------------------------------

/// What one run received and is holding.
pub(crate) struct SqsReceipt {
    api: JsonApi,
    queue: Queue,
    handles: Arc<Mutex<Vec<String>>>,
    keeper: Option<Keeper>,
    settled: bool,
}

impl SqsReceipt {
    fn new(api: JsonApi, queue: &Queue, visibility_seconds: u64, lease_api: JsonApi) -> Self {
        let handles = Arc::new(Mutex::new(Vec::new()));
        let keeper = keeper(
            lease_api,
            queue.url.clone(),
            handles.clone(),
            visibility_seconds,
        );
        SqsReceipt {
            api,
            queue: queue.clone(),
            handles,
            keeper: Some(keeper),
            settled: false,
        }
    }

    fn hold(&mut self, handle: &str) {
        self.handles.lock().unwrap().push(handle.to_string());
    }

    fn count(&self) -> usize {
        self.handles.lock().unwrap().len()
    }

    /// Delete (`acknowledge`) or show again (release) everything held.
    fn settle(&mut self, acknowledge: bool) -> Result<String, ConnectorError> {
        self.settled = true;
        let trouble = self.keeper.take().and_then(Keeper::stop);
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        let (operation, verb) = if acknowledge {
            ("DeleteMessageBatch", "deleted from")
        } else {
            ("ChangeMessageVisibilityBatch", "released back to")
        };
        let name = &self.queue.name;
        if handles.is_empty() {
            return Ok(format!("nothing held from queue '{name}'"));
        }

        let mut done = 0usize;
        let mut refused: Vec<String> = Vec::new();
        for chunk in handles.chunks(BATCH_LIMIT) {
            let answer = self
                .api
                .call(
                    operation,
                    &batch_entries(&self.queue.url, chunk, acknowledge),
                )
                .map_err(|error| {
                    ConnectorError::Data(format!(
                        "{done} of {} message(s) were {verb} queue '{name}' before: {error}",
                        handles.len()
                    ))
                })?;
            let failed = answer["Failed"].as_array().cloned().unwrap_or_default();
            done += chunk.len() - failed.len();
            refused.extend(failed.iter().map(|entry| {
                format!(
                    "{}: {}",
                    entry["Code"].as_str().unwrap_or("?"),
                    entry["Message"].as_str().unwrap_or_default()
                )
            }));
        }

        let mut line = format!("{done} message(s) {verb} queue '{name}'");
        if let Some(trouble) = trouble {
            line.push_str(&format!(
                "; the hold could not always be extended, so some may have been received \
                 elsewhere meanwhile: {trouble}"
            ));
        }
        if refused.is_empty() {
            Ok(line)
        } else {
            Err(ConnectorError::Data(format!(
                "{line}, and {} could not be; the first said {}",
                refused.len(),
                refused[0]
            )))
        }
    }
}

/// A batch call's entries: each handle, with visibility 0 when releasing.
fn batch_entries(queue_url: &str, handles: &[String], acknowledge: bool) -> JsonValue {
    let entries: Vec<JsonValue> = handles
        .iter()
        .enumerate()
        .map(|(index, handle)| {
            let mut entry = json!({ "Id": index.to_string(), "ReceiptHandle": handle });
            if !acknowledge {
                entry["VisibilityTimeout"] = json!(0);
            }
            entry
        })
        .collect();
    json!({ "QueueUrl": queue_url, "Entries": entries })
}

impl Receipt for SqsReceipt {
    fn acknowledge(mut self: Box<Self>) -> Result<String, ConnectorError> {
        self.settle(true)
    }

    fn release(mut self: Box<Self>) -> Result<String, ConnectorError> {
        self.settle(false)
    }
}

impl Drop for SqsReceipt {
    fn drop(&mut self) {
        if !self.settled {
            let _ = self.settle(false);
        }
    }
}

/// Extend the hold on everything received, every half of `visibility_seconds`,
/// until the receipt is settled.
fn keeper(
    mut api: JsonApi,
    queue_url: String,
    handles: Arc<Mutex<Vec<String>>>,
    visibility_seconds: u64,
) -> Keeper {
    Keeper::start(Duration::from_millis(visibility_seconds * 500), move || {
        let held = handles.lock().unwrap().clone();
        let mut trouble = None;
        for chunk in held.chunks(BATCH_LIMIT) {
            let mut request = batch_entries(&queue_url, chunk, false);
            for entry in request["Entries"].as_array_mut().into_iter().flatten() {
                entry["VisibilityTimeout"] = json!(visibility_seconds);
            }
            let failed = match api.call("ChangeMessageVisibilityBatch", &request) {
                Ok(answer) => answer["Failed"]
                    .as_array()
                    .and_then(|failed| failed.first())
                    .map(|entry| {
                        format!(
                            "{}: {}",
                            entry["Code"].as_str().unwrap_or("?"),
                            entry["Message"].as_str().unwrap_or_default()
                        )
                    }),
                Err(error) => Some(error.to_string()),
            };
            if let Some(failed) = failed {
                trouble.get_or_insert(failed);
            }
        }
        trouble
    })
}

// ---------------------------------------------------------------------------
// The sink
// ---------------------------------------------------------------------------

impl Sink for SqsSink {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("snk.queue.sqs", "SQS queue")
            .description(
                "Send rows to an Amazon SQS queue, one JSON message each, ten to a \
                 SendMessageBatch call. Messages SQS refuses on its own side are sent again.",
            )
            .icon("inbox")
            .properties(with_connection(vec![
                PropertySpec::text("message_group_id_column").help(
                    "The column whose value is each message's group. Required for a FIFO \
                     queue, which keeps each group in order.",
                ),
                PropertySpec::text("deduplication_id_column").help(
                    "FIFO only: the column whose value is each message's deduplication ID. \
                     Unset, the queue must have content-based deduplication on.",
                ),
                PropertySpec::integer("delay_seconds")
                    .help("Standard queues only: hide each message this long, up to 900."),
            ]))
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
        let mut api = JsonApi::connect(properties, &Sources::process(), &SQS)?;
        send_messages(&mut api, &settings, input, RESEND_BACKOFF)
    }
}

#[derive(Debug)]
pub(crate) struct SinkSettings {
    pub(crate) target: Target,
    pub(crate) group_column: Option<String>,
    pub(crate) deduplication_column: Option<String>,
    pub(crate) delay_seconds: Option<u64>,
    pub(crate) retries: u32,
}

impl SinkSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let target = Target::from(properties)?;
        aws::check_connection(properties)?;
        let group_column = text(properties, "message_group_id_column").map(str::to_string);
        let deduplication_column = text(properties, "deduplication_id_column").map(str::to_string);
        if target.fifo() && group_column.is_none() {
            return Err(ConnectorError::property(
                "message_group_id_column",
                "is required for a FIFO queue: every message there belongs to a group",
            ));
        }
        if !target.fifo() && deduplication_column.is_some() {
            return Err(ConnectorError::property(
                "deduplication_id_column",
                "is for FIFO queues, whose names end in .fifo",
            ));
        }
        let delay_seconds = properties.get("delay_seconds").and_then(JsonValue::as_u64);
        if let Some(delay) = delay_seconds {
            if delay > 900 {
                return Err(ConnectorError::property(
                    "delay_seconds",
                    format!("{delay} is more than SQS's 900"),
                ));
            }
            if target.fifo() {
                return Err(ConnectorError::property(
                    "delay_seconds",
                    "cannot be set per message on a FIFO queue; set it on the queue",
                ));
            }
        }
        Ok(SinkSettings {
            target,
            group_column,
            deduplication_column,
            delay_seconds,
            retries: properties
                .get("retries")
                .and_then(JsonValue::as_u64)
                .unwrap_or(5) as u32,
        })
    }
}

/// One row, ready to send.
#[derive(Debug)]
pub(crate) struct Outgoing {
    /// Counted from 1, for messages; also the entry's `Id` in its batch.
    pub(crate) row: u64,
    pub(crate) body: String,
    pub(crate) group: Option<String>,
    pub(crate) deduplication: Option<String>,
}

/// A column's value as text, for a group or deduplication ID: required.
fn column_text(
    row: u64,
    record: &Record,
    property: &str,
    column: &str,
) -> Result<String, ConnectorError> {
    match record.get(column) {
        None => Err(ConnectorError::property(
            property,
            format!("'{column}' is not a column of the rows"),
        )),
        Some(JsonValue::Null) => Err(ConnectorError::Data(format!(
            "row {row}: '{column}' is null, and SQS needs a value for every message"
        ))),
        Some(JsonValue::String(text)) => Ok(text.clone()),
        Some(other) => Ok(other.to_string()),
    }
}

pub(crate) fn outgoing(
    row: u64,
    record: &Record,
    settings: &SinkSettings,
) -> Result<Outgoing, ConnectorError> {
    let body =
        serde_json::to_string(record).map_err(|error| ConnectorError::Data(error.to_string()))?;
    if body.len() > MESSAGE_BYTES {
        return Err(ConnectorError::Data(format!(
            "row {row} is {} bytes as JSON, and SQS takes at most {MESSAGE_BYTES} (1 MiB) in one \
             message",
            body.len()
        )));
    }
    let group = settings
        .group_column
        .as_deref()
        .map(|column| column_text(row, record, "message_group_id_column", column))
        .transpose()?;
    let deduplication = settings
        .deduplication_column
        .as_deref()
        .map(|column| column_text(row, record, "deduplication_id_column", column))
        .transpose()?;
    Ok(Outgoing {
        row,
        body,
        group,
        deduplication,
    })
}

/// What has been sent so far, for the summary and for a failure's message.
#[derive(Default)]
struct Sent {
    messages: u64,
    calls: u64,
    resent: u64,
}

impl Sent {
    fn failed(&self, queue: &str, error: impl std::fmt::Display) -> ConnectorError {
        ConnectorError::Data(format!(
            "{error}. {} message(s) had been sent to queue '{queue}' before this, and stay there",
            self.messages
        ))
    }
}

/// Every row of `input` to the queue, a `SendMessageBatch` at a time.
pub(crate) fn send_messages(
    api: &mut JsonApi,
    settings: &SinkSettings,
    input: &mut dyn RecordReader,
    backoff: Duration,
) -> Result<Summary, ConnectorError> {
    let queue = resolve(api, &settings.target)?;
    let mut sent = Sent::default();
    let mut batch: Vec<Outgoing> = Vec::new();
    let mut batch_bytes = 0usize;
    let mut row = 0u64;

    while let Some(record) = input.read()? {
        row += 1;
        let message = outgoing(row, &record, settings).map_err(|error| match error {
            // A setting that cannot work says so plainly, before anything is sent.
            ConnectorError::Property { .. } if sent.messages == 0 => error,
            other => sent.failed(&queue.name, other),
        })?;
        if batch.len() == BATCH_LIMIT || batch_bytes + message.body.len() > MESSAGE_BYTES {
            send(
                api,
                settings,
                &queue,
                std::mem::take(&mut batch),
                backoff,
                &mut sent,
            )?;
            batch_bytes = 0;
        }
        batch_bytes += message.body.len();
        batch.push(message);
    }
    if !batch.is_empty() {
        send(api, settings, &queue, batch, backoff, &mut sent)?;
    }

    let detail = if sent.messages == 0 {
        format!("0 messages; nothing sent to queue '{}'", queue.name)
    } else {
        let mut detail = format!(
            "{} message(s) in {} call(s) to queue '{}' (credentials from {})",
            sent.messages,
            sent.calls,
            queue.name,
            api.credentials_source()
        );
        if sent.resent > 0 {
            detail.push_str(&format!(
                "; {} sent again after SQS refused them on its own side",
                sent.resent
            ));
        }
        detail
    };
    Ok(Summary::new(sent.messages, detail))
}

/// One `SendMessageBatch`, and again for the messages it refused through no
/// fault of ours (`SenderFault` false), up to `retries` more times. A refusal
/// that is ours to fix fails at once.
fn send(
    api: &mut JsonApi,
    settings: &SinkSettings,
    queue: &Queue,
    messages: Vec<Outgoing>,
    backoff: Duration,
    sent: &mut Sent,
) -> Result<(), ConnectorError> {
    let mut pending = messages;
    let mut attempt = 0u32;

    loop {
        let entries: Vec<JsonValue> = pending
            .iter()
            .map(|message| {
                let mut entry =
                    json!({ "Id": message.row.to_string(), "MessageBody": message.body });
                if let Some(group) = &message.group {
                    entry["MessageGroupId"] = json!(group);
                }
                if let Some(id) = &message.deduplication {
                    entry["MessageDeduplicationId"] = json!(id);
                }
                if let Some(delay) = settings.delay_seconds {
                    entry["DelaySeconds"] = json!(delay);
                }
                entry
            })
            .collect();
        let answer = api
            .call(
                "SendMessageBatch",
                &json!({ "QueueUrl": queue.url, "Entries": entries }),
            )
            .map_err(|error| sent.failed(&queue.name, error))?;
        sent.calls += 1;

        let failed = answer["Failed"].as_array().cloned().unwrap_or_default();
        sent.messages += (pending.len() - failed.len()) as u64;

        let mut again = Vec::new();
        let mut last = String::new();
        for message in pending {
            let id = message.row.to_string();
            let Some(entry) = failed
                .iter()
                .find(|f| f["Id"].as_str() == Some(id.as_str()))
            else {
                continue;
            };
            let said = format!(
                "{}: {}",
                entry["Code"].as_str().unwrap_or("?"),
                entry["Message"].as_str().unwrap_or_default()
            );
            if entry["SenderFault"].as_bool().unwrap_or(true) {
                return Err(sent.failed(
                    &queue.name,
                    format!("SQS refused row {}: {said}", message.row),
                ));
            }
            last = said;
            again.push(message);
        }

        if again.is_empty() {
            return Ok(());
        }
        if attempt == settings.retries {
            return Err(sent.failed(
                &queue.name,
                format!(
                    "SQS still refused {} message(s), the first row {}, after {} resend(s); the \
                     last refusal said {last}",
                    again.len(),
                    again[0].row,
                    settings.retries
                ),
            ));
        }
        std::thread::sleep(backoff.saturating_mul(1 << attempt.min(6)));
        attempt += 1;
        sent.resent += again.len() as u64;
        pending = again;
    }
}
