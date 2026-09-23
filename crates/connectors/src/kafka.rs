//! Kafka, read in **bounded micro-batches**. This is not continuous streaming,
//! and nothing here pretends to be.
//!
//! Each run of `src.stream.kafka` is one batch:
//!
//! 1. It records each partition's latest offset -- the high watermark -- as
//!    the run starts. That is the end of this batch: whatever arrives while it
//!    reads is the next run's.
//! 2. It reads from where the last **successful** run stopped, handed back by
//!    the engine as the checkpoint, up to those ends or `max_records`, whichever
//!    comes first. Partitions take turns, so a large backlog in one cannot
//!    starve the others.
//! 3. It returns where it got to as a new checkpoint. The engine saves it only
//!    if the whole run succeeds, so a failed run re-reads the same records.
//!
//! Positions live in this project's state file, not in a Kafka consumer group
//! (Settled decision 27), so Kafka's own tools do not see this pipeline as a
//! consumer, and `etl state forget` is how to replay.
//!
//! **Gaps are errors.** If retention deleted records this pipeline never read,
//! the read fails and says how many. Carrying on quietly would be a partial
//! load that looks complete.
//!
//! The client is `rskafka`, which is async: a single-threaded `tokio` runtime
//! is built for each read and dropped after it, and nothing outside this module
//! sees it (Settled decision 26). Its retries have no deadline by default and
//! back off up to 500 s, so both are set here, and every call to the broker is
//! also given `timeout_ms`, because a connection that stalls has no timeout of
//! its own.
//!
//! `snk.stream.kafka` (Phase 10f) sends rows the other way, one JSON object
//! per record, partitioned by key the way Java producers partition, so a key
//! lands where other producers put it. Both directions share one set of
//! connection properties: plaintext, TLS, and SASL PLAIN or SCRAM, over either.

use crate::http::{base64_bytes, kind, positive, text};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordReader, RecordWriter, Sink, Source,
    Summary,
};
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};
use rskafka::record::Record as KafkaRecord;
use rskafka::BackoffConfig;
use serde_json::{json, Map, Value as JsonValue};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
mod tests;

/// `src.stream.kafka`.
pub struct KafkaSource;

/// `snk.stream.kafka`.
pub struct KafkaSink;

/// The columns every row carries, whatever `value_format` says.
pub(crate) const METADATA_COLUMNS: [&str; 5] =
    ["_topic", "_partition", "_offset", "_timestamp", "_key"];

/// How many bytes one fetch asks for. A record bigger than this still arrives
/// (Kafka returns at least one whole batch); this is how much to ask for, not
/// a cap on what comes back.
const FETCH_BYTES: i32 = 1 << 20;

/// How long a fetch may wait for data. There is always data below the high
/// watermark recorded at the start, so this only matters at the very end.
const FETCH_WAIT_MS: i32 = 100;

// ---------------------------------------------------------------------------
// The spec
// ---------------------------------------------------------------------------

impl Source for KafkaSource {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("src.stream.kafka", "Kafka topic")
            .description(
                "Read a Kafka topic in bounded batches: each run reads what arrived since the \
                 last successful one, up to max_records.",
            )
            .icon("radio")
            .properties(with_connection(vec![
                PropertySpec::text("topic").required(),
                PropertySpec::enumerated("start", &["earliest", "latest"])
                    .default(JsonValue::String("earliest".into()))
                    .help(
                        "Where a partition with no saved position starts: earliest reads what \
                         the topic still holds, latest only what arrives from now on.",
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
                        "json makes each value's fields into columns; text gives a value \
                         column; bytes gives value as base64. Every row also has _topic, \
                         _partition, _offset, _timestamp and _key.",
                    ),
                columns_property(),
            ]))
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        Settings::from(properties).map(|_| ())
    }

    fn read(
        &self,
        properties: &JsonValue,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = Settings::from(properties)?;
        let saved = context
            .checkpoint
            .as_ref()
            .map(Position::from_json)
            .transpose()?;

        runtime()?.block_on(read_batch(&settings, saved, out, context))
    }
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct Settings {
    pub(crate) connection: Connection,
    pub(crate) topic: String,
    pub(crate) start: Start,
    pub(crate) max_records: u64,
    pub(crate) format: Format,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Start {
    Earliest,
    Latest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Format {
    Json,
    Text,
    Bytes,
}

impl Settings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let connection = Connection::from(properties)?;
        let topic = topic(properties)?;

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

        Ok(Settings {
            connection,
            topic,
            start,
            max_records: positive(properties, "max_records", 100_000)?,
            format,
        })
    }
}

fn brokers(properties: &JsonValue) -> Result<Vec<String>, ConnectorError> {
    let written = text(properties, "brokers")
        .ok_or_else(|| ConnectorError::property("brokers", "is required, e.g. localhost:9092"))?;

    let brokers: Vec<String> = written
        .split(',')
        .map(str::trim)
        .filter(|broker| !broker.is_empty())
        .map(str::to_string)
        .collect();

    for broker in &brokers {
        let usable = broker
            .rsplit_once(':')
            .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
        if !usable {
            return Err(ConnectorError::property(
                "brokers",
                format!("'{broker}' is not host:port, e.g. localhost:9092"),
            ));
        }
    }
    if brokers.is_empty() {
        return Err(ConnectorError::property(
            "brokers",
            "is required, e.g. localhost:9092",
        ));
    }
    Ok(brokers)
}

// ---------------------------------------------------------------------------
// The position a run hands to the next
// ---------------------------------------------------------------------------

/// Where a read got to: the next offset to read in each partition, and the
/// topic it applies to, so a node pointed at another topic is not handed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Position {
    pub(crate) topic: String,
    pub(crate) next: BTreeMap<i32, i64>,
}

impl Position {
    pub(crate) fn to_json(&self) -> JsonValue {
        let offsets: Map<String, JsonValue> = self
            .next
            .iter()
            .map(|(partition, offset)| (partition.to_string(), JsonValue::from(*offset)))
            .collect();
        json!({ "topic": self.topic, "offsets": offsets })
    }

    pub(crate) fn from_json(value: &JsonValue) -> Result<Self, ConnectorError> {
        let unusable = || {
            ConnectorError::Data(format!(
                "the saved position is not one this connector wrote ({value}); `etl state forget` \
                 this node to start it over"
            ))
        };

        let topic = value
            .get("topic")
            .and_then(JsonValue::as_str)
            .ok_or_else(unusable)?
            .to_string();
        let offsets = value
            .get("offsets")
            .and_then(JsonValue::as_object)
            .ok_or_else(unusable)?;

        let mut next = BTreeMap::new();
        for (partition, offset) in offsets {
            let partition: i32 = partition.parse().map_err(|_| unusable())?;
            let offset = offset.as_i64().filter(|o| *o >= 0).ok_or_else(unusable)?;
            next.insert(partition, offset);
        }

        Ok(Position { topic, next })
    }
}

/// One partition's share of this run: read `[from, to)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Span {
    pub(crate) partition: i32,
    pub(crate) from: i64,
    /// The high watermark when the run started: the end of this batch.
    pub(crate) to: i64,
}

/// A partition as the broker describes it when the run starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Bounds {
    pub(crate) partition: i32,
    /// The oldest offset still held.
    pub(crate) earliest: i64,
    /// The high watermark: one past the newest record.
    pub(crate) latest: i64,
}

/// Where each partition starts this run, and anything worth saying about it.
///
/// Pure, so the rules that decide whether records are skipped or read twice
/// are tested without a broker.
pub(crate) fn plan_spans(
    topic: &str,
    bounds: &[Bounds],
    saved: Option<&Position>,
    start: Start,
) -> Result<(Vec<Span>, Vec<String>), ConnectorError> {
    let mut notes = Vec::new();

    let saved = match saved {
        Some(position) if position.topic != topic => {
            notes.push(format!(
                "the saved position was for topic '{}', so '{topic}' starts from {}",
                position.topic,
                start_name(start)
            ));
            None
        }
        other => other,
    };

    let mut spans = Vec::with_capacity(bounds.len());
    for bound in bounds {
        let from = match saved.and_then(|position| position.next.get(&bound.partition)) {
            Some(&next) if next < bound.earliest => {
                return Err(ConnectorError::Data(format!(
                    "partition {}: offsets {next} to {} were deleted before this pipeline read \
                     them ({} record(s) lost, most likely to the topic's retention). Nothing \
                     was read. To carry on from what the topic still holds, `etl state forget` \
                     this node, which restarts every partition from `start`",
                    bound.partition,
                    bound.earliest - 1,
                    bound.earliest - next
                )));
            }
            Some(&next) if next > bound.latest => {
                return Err(ConnectorError::Data(format!(
                    "partition {}: the saved position {next} is past the end of the partition \
                     ({}), so the topic was probably deleted and made again. Nothing was read. \
                     `etl state forget` this node to start it over",
                    bound.partition, bound.latest
                )));
            }
            Some(&next) => next,
            None => {
                if saved.is_some() {
                    notes.push(format!(
                        "partition {} is new since the last run and starts from {}",
                        bound.partition,
                        start_name(start)
                    ));
                }
                match start {
                    Start::Earliest => bound.earliest,
                    Start::Latest => bound.latest,
                }
            }
        };

        spans.push(Span {
            partition: bound.partition,
            from,
            to: bound.latest,
        });
    }

    Ok((spans, notes))
}

fn start_name(start: Start) -> &'static str {
    match start {
        Start::Earliest => "earliest",
        Start::Latest => "latest",
    }
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// One Kafka record as a row.
pub(crate) fn row(
    topic: &str,
    partition: i32,
    offset: i64,
    timestamp: &str,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    format: Format,
) -> Result<Record, ConnectorError> {
    let at = format!("partition {partition} offset {offset}");
    let mut row = value_columns(format, value, &at, &METADATA_COLUMNS)?;

    row.insert("_topic".to_string(), JsonValue::String(topic.to_string()));
    row.insert("_partition".to_string(), JsonValue::from(partition));
    row.insert("_offset".to_string(), JsonValue::from(offset));
    row.insert(
        "_timestamp".to_string(),
        JsonValue::String(timestamp.to_string()),
    );
    row.insert("_key".to_string(), key_text(key));

    Ok(row)
}

/// A key, or any bytes a person will read: text when it is UTF-8, which is
/// nearly always; base64 otherwise, rather than a lossy conversion that would
/// quietly change it. Null when there is none.
pub(crate) fn key_text(key: Option<&[u8]>) -> JsonValue {
    key.map_or(JsonValue::Null, |bytes| match std::str::from_utf8(bytes) {
        Ok(text) => JsonValue::String(text.to_string()),
        Err(_) => JsonValue::String(base64_bytes(bytes)),
    })
}

/// A streamed message's value as columns, for any broker: `json` spreads an
/// object into columns, `text` and `bytes` give one `value` column. `at` names
/// the message in an error ("partition 2 offset 41", "sequence 7"), and
/// `reserved` are the underscore columns the connector adds, which a JSON
/// field may not shadow.
pub(crate) fn value_columns(
    format: Format,
    value: Option<&[u8]>,
    at: &str,
    reserved: &[&str],
) -> Result<Record, ConnectorError> {
    let at = || at.to_string();
    let mut row = Map::new();

    match (format, value) {
        // A tombstone: a key with no value. Kept, as a row of only the
        // underscore columns, because a deletion is information.
        (Format::Json, None) => {}
        (Format::Json, Some(bytes)) => {
            let parsed: JsonValue = serde_json::from_slice(bytes).map_err(|error| {
                // Invisible, and the parser's own words do not lead anywhere
                // near it. Windows tools add one; found producing test data
                // from PowerShell.
                let hint = if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
                    "; it starts with a UTF-8 byte-order mark, which JSON does not allow, so \
                     the producer should write UTF-8 without one"
                } else {
                    "; use value_format text or bytes"
                };
                ConnectorError::Data(format!("{}: the value is not JSON ({error}){hint}", at()))
            })?;
            let JsonValue::Object(fields) = parsed else {
                return Err(ConnectorError::Data(format!(
                    "{}: the value is {}, not a JSON object; use value_format text to read it \
                     as one column",
                    at(),
                    kind(&parsed)
                )));
            };
            for name in fields.keys() {
                if reserved.contains(&name.as_str()) {
                    return Err(ConnectorError::Data(format!(
                        "{}: the value has a field '{name}', which is a column this connector \
                         adds; use value_format text and unpack it downstream",
                        at()
                    )));
                }
            }
            row = fields;
        }
        (Format::Text, value) => {
            let text = match value {
                None => JsonValue::Null,
                Some(bytes) => {
                    JsonValue::String(String::from_utf8(bytes.to_vec()).map_err(|_| {
                        ConnectorError::Data(format!(
                            "{}: the value is not UTF-8 text; use value_format bytes",
                            at()
                        ))
                    })?)
                }
            };
            row.insert("value".to_string(), text);
        }
        (Format::Bytes, value) => {
            let encoded = value.map_or(JsonValue::Null, |bytes| {
                JsonValue::String(base64_bytes(bytes))
            });
            row.insert("value".to_string(), encoded);
        }
    }

    Ok(row)
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

async fn read_batch(
    settings: &Settings,
    saved: Option<Position>,
    out: &mut dyn RecordWriter,
    context: &Context,
) -> Result<Summary, ConnectorError> {
    let limit = settings.connection.timeout;
    let brokers = settings.connection.brokers.join(",");
    let topic = settings.topic.as_str();

    let client = settings.connection.connect(context).await?;

    let topics = within(
        limit,
        || format!("listing the topics on {brokers}"),
        async { client.list_topics().await },
    )
    .await?;
    let partitions = topics
        .into_iter()
        .find(|found| found.name == topic)
        .map(|found| found.partitions)
        .ok_or_else(|| {
            ConnectorError::Data(format!(
                "there is no topic '{topic}' on {brokers}; this connector does not create one"
            ))
        })?;

    let mut clients: BTreeMap<i32, PartitionClient> = BTreeMap::new();
    let mut bounds = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let what = || format!("partition {partition} of '{topic}'");
        let partition_client = within(limit, what, async {
            client
                .partition_client(topic, partition, UnknownTopicHandling::Error)
                .await
        })
        .await?;
        let earliest = within(limit, what, partition_client.get_offset(OffsetAt::Earliest)).await?;
        let latest = within(limit, what, partition_client.get_offset(OffsetAt::Latest)).await?;

        bounds.push(Bounds {
            partition,
            earliest,
            latest,
        });
        clients.insert(partition, partition_client);
    }

    let (spans, mut notes) = plan_spans(topic, &bounds, saved.as_ref(), settings.start)?;

    let mut next: BTreeMap<i32, i64> = spans.iter().map(|s| (s.partition, s.from)).collect();
    let mut budget = settings.max_records;
    let mut read = 0u64;

    // Partitions take turns, one fetch each, so a backlog in one cannot starve
    // the rest when `max_records` cuts the batch short.
    let mut turn: Vec<Span> = spans.iter().copied().filter(|s| s.from < s.to).collect();
    while budget > 0 && !turn.is_empty() {
        let mut again = Vec::with_capacity(turn.len());

        for span in turn {
            if budget == 0 {
                break;
            }
            let offset = next[&span.partition];
            let partition_client = &clients[&span.partition];

            let (records, _) = within(
                limit,
                || {
                    format!(
                        "reading partition {} of '{topic}' at offset {offset}",
                        span.partition
                    )
                },
                partition_client.fetch_records(offset, 1..FETCH_BYTES, FETCH_WAIT_MS),
            )
            .await?;

            let mut ordered: Vec<_> = records
                .into_iter()
                .filter(|record| record.offset >= offset && record.offset < span.to)
                .collect();
            ordered.sort_by_key(|record| record.offset);

            let mut moved = false;
            for record in ordered {
                if budget == 0 {
                    break;
                }
                let timestamp = timestamp_text(record.record.timestamp.timestamp_millis());
                out.write(row(
                    topic,
                    span.partition,
                    record.offset,
                    &timestamp,
                    record.record.key.as_deref(),
                    record.record.value.as_deref(),
                    settings.format,
                )?)?;
                budget -= 1;
                read += 1;
                next.insert(span.partition, record.offset + 1);
                moved = true;
            }

            if !moved && budget > 0 {
                // Below the recorded end but nothing to read: transaction
                // markers, or records compacted away. Said, and stepped over,
                // rather than asked for again forever.
                notes.push(format!(
                    "partition {}: offsets {offset} to {} held nothing readable (transaction \
                     markers, or compacted away) and were stepped over",
                    span.partition,
                    span.to - 1
                ));
                next.insert(span.partition, span.to);
            }

            if next[&span.partition] < span.to {
                again.push(span);
            }
        }

        turn = again;
    }

    let left: i64 = spans.iter().map(|s| s.to - next[&s.partition]).sum();
    let mut detail = format!(
        "{read} record(s) from {} partition(s) of '{topic}'",
        spans.len()
    );
    if left > 0 {
        detail.push_str(&format!(
            "; stopped at max_records ({}) with about {left} more for the next run",
            settings.max_records
        ));
    }
    for note in notes {
        detail.push_str("; ");
        detail.push_str(&note);
    }

    let position = Position {
        topic: topic.to_string(),
        next,
    };

    Ok(Summary {
        checkpoint: Some(position.to_json()),
        ..Summary::new(read, detail)
    })
}

/// A record's timestamp, UTC, as DuckDB reads a `TIMESTAMP`: millisecond
/// precision, which is all Kafka keeps. By hand, with the same civil-date
/// arithmetic the scheduler uses, because the `chrono` that `rskafka` brings
/// is built without its formatting.
pub(crate) fn timestamp_text(millis: i64) -> String {
    const DAY: i64 = 86_400_000;
    let (year, month, day) = etl_state::time::civil_from_days(millis.div_euclid(DAY));
    let within = millis.rem_euclid(DAY);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{:03}",
        within / 3_600_000,
        within / 60_000 % 60,
        within / 1000 % 60,
        within % 1000
    )
}

/// Retries that give up. `rskafka`'s default has no deadline and backs off to
/// 500 s, which would turn a mistyped broker address into a run that hangs.
fn backoff(deadline: Duration) -> BackoffConfig {
    BackoffConfig {
        init_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(2),
        base: 2.0,
        deadline: Some(deadline),
    }
}

/// How much longer than `rskafka`'s own retry deadline a call is given before
/// it counts as stalled. The library's deadline is `timeout_ms`; when retries
/// run out it fails with the reason ("authentication failed"), and this slack
/// lets that reason arrive. A timeout the same length as the deadline won the
/// race, and a wrong password read as "no answer" (found in 10f).
const STALL_SLACK: Duration = Duration::from_secs(5);

/// A backoff that gives up after the first failure, for finding out why.
fn no_retries() -> BackoffConfig {
    BackoffConfig {
        deadline: Some(Duration::ZERO),
        ..backoff(Duration::ZERO)
    }
}

/// One call to the broker, given `limit` (plus [`STALL_SLACK`]) to finish, its
/// error said in terms of what was being attempted.
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
// The connection both directions share
// ---------------------------------------------------------------------------

/// The properties every Kafka component has, first, then `own`.
fn with_connection(own: Vec<PropertySpec>) -> Vec<PropertySpec> {
    let mut properties = vec![
        PropertySpec::text("brokers")
            .required()
            .help("Bootstrap brokers as host:port, comma-separated."),
        PropertySpec::enumerated(
            "security",
            &["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"],
        )
        .default(JsonValue::String("plaintext".into()))
        .help(
            "ssl encrypts; sasl_* signs in with username and password; sasl_ssl does both, \
             which is what hosted Kafka (Confluent Cloud, MSK, Aiven) expects.",
        ),
        PropertySpec::enumerated(
            "sasl_mechanism",
            &["plain", "scram-sha-256", "scram-sha-512"],
        )
        .default(JsonValue::String("plain".into()))
        .help("For sasl_*: how the password is checked."),
        PropertySpec::text("username").help("For sasl_*."),
        PropertySpec::text("password")
            .help("For sasl_*. Use ${SECRET:name} rather than the value itself."),
        PropertySpec::path("ca_cert").help(
            "For ssl and sasl_ssl: a PEM file of the certificate authority to trust, for a \
             cluster with a private CA. Unset trusts the usual public authorities.",
        ),
        PropertySpec::integer("timeout_ms")
            .default(JsonValue::from(30_000))
            .help(
                "How long any one step may take, retries included: connecting, listing, one \
                 fetch or one send.",
            ),
    ];
    properties.extend(own);
    properties
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Security {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl Security {
    fn tls(self) -> bool {
        matches!(self, Security::Ssl | Security::SaslSsl)
    }

    fn sasl(self) -> bool {
        matches!(self, Security::SaslPlaintext | Security::SaslSsl)
    }

    fn name(self) -> &'static str {
        match self {
            Security::Plaintext => "plaintext",
            Security::Ssl => "ssl",
            Security::SaslPlaintext => "sasl_plaintext",
            Security::SaslSsl => "sasl_ssl",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl Mechanism {
    fn name(self) -> &'static str {
        match self {
            Mechanism::Plain => "PLAIN",
            Mechanism::ScramSha256 => "SCRAM-SHA-256",
            Mechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// Where the brokers are and how to reach them.
#[derive(Debug)]
pub(crate) struct Connection {
    pub(crate) brokers: Vec<String>,
    pub(crate) security: Security,
    /// Present exactly when `security` is `sasl_*`.
    pub(crate) sasl: Option<(Mechanism, String, String)>,
    /// As written; resolved against the workspace when connecting.
    pub(crate) ca_cert: Option<String>,
    pub(crate) timeout: Duration,
}

impl Connection {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let brokers = brokers(properties)?;

        let security = match text(properties, "security").unwrap_or("plaintext") {
            "plaintext" => Security::Plaintext,
            "ssl" => Security::Ssl,
            "sasl_plaintext" => Security::SaslPlaintext,
            "sasl_ssl" => Security::SaslSsl,
            other => {
                return Err(ConnectorError::property(
                    "security",
                    format!("'{other}' is not one of plaintext, ssl, sasl_plaintext, sasl_ssl"),
                ))
            }
        };

        let username = text(properties, "username");
        let password = text(properties, "password");
        let sasl = if security.sasl() {
            let mechanism = match text(properties, "sasl_mechanism").unwrap_or("plain") {
                "plain" => Mechanism::Plain,
                "scram-sha-256" => Mechanism::ScramSha256,
                "scram-sha-512" => Mechanism::ScramSha512,
                other => {
                    return Err(ConnectorError::property(
                        "sasl_mechanism",
                        format!("'{other}' is not one of plain, scram-sha-256, scram-sha-512"),
                    ))
                }
            };
            let username = username.ok_or_else(|| {
                ConnectorError::property("username", format!("is required for {}", security.name()))
            })?;
            let password = password.ok_or_else(|| {
                ConnectorError::property("password", format!("is required for {}", security.name()))
            })?;
            Some((mechanism, username.to_string(), password.to_string()))
        } else {
            // A credential that would be ignored is a mistake worth catching
            // now: the run would otherwise fail at the broker, saying less.
            if username.is_some() || password.is_some() {
                return Err(ConnectorError::property(
                    "security",
                    format!(
                        "is '{}', which signs in with nothing, but a username or password is \
                         set; use sasl_plaintext or sasl_ssl",
                        security.name()
                    ),
                ));
            }
            None
        };

        let ca_cert = text(properties, "ca_cert").map(str::to_string);
        if ca_cert.is_some() && !security.tls() {
            return Err(ConnectorError::property(
                "ca_cert",
                format!(
                    "is for ssl and sasl_ssl, and security is '{}'",
                    security.name()
                ),
            ));
        }

        Ok(Connection {
            brokers,
            security,
            sasl,
            ca_cert,
            timeout: Duration::from_millis(positive(properties, "timeout_ms", 30_000)?),
        })
    }

    /// How this connection is described in an error: the brokers, and how they
    /// were reached. Never the password.
    fn describe(&self) -> String {
        let brokers = self.brokers.join(",");
        match &self.sasl {
            Some((mechanism, username, _)) => format!(
                "{brokers} ({}, {} as '{username}')",
                self.security.name(),
                mechanism.name()
            ),
            None => format!("{brokers} ({})", self.security.name()),
        }
    }

    pub(crate) async fn connect(&self, context: &Context) -> Result<Client, ConnectorError> {
        let what = || format!("connecting to {}", self.describe());
        let retrying = self.builder(context, backoff(self.timeout))?.build();

        match tokio::time::timeout(self.timeout + STALL_SLACK, retrying).await {
            Ok(Ok(client)) => Ok(client),
            Ok(Err(error)) => Err(ConnectorError::Data(format!("{}: {error}", what()))),
            Err(_) => {
                // `rskafka` retries a failed sign-in like any failed connection,
                // and its deadline counts only the waits between attempts, so a
                // wrong password or an untrusted certificate outlasts any
                // timeout and says nothing. One attempt with no retries finds
                // out why; a broker that is merely slow times out again.
                let once = self.builder(context, no_retries())?.build();
                match tokio::time::timeout(self.timeout, once).await {
                    Ok(Err(reason)) => Err(ConnectorError::Data(format!(
                        "{}: it kept failing, and one more attempt says: {reason}",
                        what()
                    ))),
                    _ => Err(ConnectorError::Data(format!(
                        "{}: no answer within {} ms (timeout_ms) and {} s more",
                        what(),
                        self.timeout.as_millis(),
                        STALL_SLACK.as_secs()
                    ))),
                }
            }
        }
    }

    fn builder(
        &self,
        context: &Context,
        backoff: BackoffConfig,
    ) -> Result<ClientBuilder, ConnectorError> {
        let mut builder = ClientBuilder::new(self.brokers.clone())
            .client_id("etl")
            .backoff_config(backoff);

        if self.security.tls() {
            builder = builder.tls_config(self.tls(context)?);
        }
        if let Some((mechanism, username, password)) = &self.sasl {
            let credentials = Credentials::new(username.clone(), password.clone());
            builder = builder.sasl_config(match mechanism {
                Mechanism::Plain => SaslConfig::Plain(credentials),
                Mechanism::ScramSha256 => SaslConfig::ScramSha256(credentials),
                Mechanism::ScramSha512 => SaslConfig::ScramSha512(credentials),
            });
        }
        Ok(builder)
    }

    /// The TLS settings, shared with every connector that opens its own
    /// connection: see [`crate::tls`].
    fn tls(&self, context: &Context) -> Result<Arc<rustls::ClientConfig>, ConnectorError> {
        crate::tls::client_config(self.ca_cert.as_deref(), context).map(Arc::new)
    }
}

fn topic(properties: &JsonValue) -> Result<String, ConnectorError> {
    Ok(text(properties, "topic")
        .ok_or_else(|| ConnectorError::property("topic", "is required"))?
        .trim()
        .to_string())
}

/// A runtime for one read or one write, dropped with it (Settled decision 26).
fn runtime() -> Result<tokio::runtime::Runtime, ConnectorError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| ConnectorError::Data(format!("could not start the Kafka client: {error}")))
}

// ---------------------------------------------------------------------------
// The sink
// ---------------------------------------------------------------------------

impl Sink for KafkaSink {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("snk.stream.kafka", "Kafka topic")
            .description(
                "Send rows to a Kafka topic, one JSON object per record, in batches, keyed and \
                 partitioned the way Java producers do.",
            )
            .icon("radio")
            .properties(with_connection(vec![
                PropertySpec::text("topic").required(),
                PropertySpec::text("key_column").help(
                    "The column whose value is each record's key. Records with the same key \
                     go to the same partition, as they would from a Java producer. Unset, or \
                     null in a row, sends that row without a key.",
                ),
                PropertySpec::integer("batch_size")
                    .default(JsonValue::from(500))
                    .help("Rows per batch. Each batch is acknowledged before the next is sent."),
                PropertySpec::enumerated("compression", &["none", "gzip", "snappy", "lz4", "zstd"])
                    .default(JsonValue::String("none".into()))
                    .help("How each batch is compressed on the wire and in the topic."),
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
    pub(crate) topic: String,
    pub(crate) key_column: Option<String>,
    pub(crate) batch_size: usize,
    pub(crate) compression: Compression,
}

impl SinkSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let compression = match text(properties, "compression").unwrap_or("none") {
            "none" => Compression::NoCompression,
            "gzip" => Compression::Gzip,
            "snappy" => Compression::Snappy,
            "lz4" => Compression::Lz4,
            "zstd" => Compression::Zstd,
            other => {
                return Err(ConnectorError::property(
                    "compression",
                    format!("'{other}' is not one of none, gzip, snappy, lz4, zstd"),
                ))
            }
        };

        Ok(SinkSettings {
            connection: Connection::from(properties)?,
            topic: topic(properties)?,
            key_column: text(properties, "key_column").map(str::to_string),
            batch_size: positive(properties, "batch_size", 500)? as usize,
            compression,
        })
    }
}

async fn write_batches(
    settings: &SinkSettings,
    input: &mut dyn RecordReader,
    context: &Context,
) -> Result<Summary, ConnectorError> {
    let limit = settings.connection.timeout;
    let topic = settings.topic.as_str();
    let client = settings.connection.connect(context).await?;

    let topics = within(
        limit,
        || format!("listing the topics of {topic}'s cluster"),
        async { client.list_topics().await },
    )
    .await?;
    let partitions: Vec<i32> = topics
        .into_iter()
        .find(|found| found.name == topic)
        .map(|found| found.partitions.into_iter().collect())
        .ok_or_else(|| {
            ConnectorError::Data(format!(
                "there is no topic '{topic}' on {}; this connector does not create one",
                settings.connection.brokers.join(",")
            ))
        })?;

    let mut clients: BTreeMap<i32, PartitionClient> = BTreeMap::new();
    for &partition in &partitions {
        let partition_client = within(
            limit,
            || format!("partition {partition} of '{topic}'"),
            client.partition_client(topic, partition, UnknownTopicHandling::Error),
        )
        .await?;
        clients.insert(partition, partition_client);
    }

    let mut sent_batches = 0u64;
    let mut sent_records = 0u64;
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

        // Keyless rows share one partition per batch, taking turns across
        // batches, so they spread without each row costing a request.
        let keyless = partitions[(sent_batches as usize) % partitions.len()];
        let by_partition = assign(rows, settings.key_column.as_deref(), &partitions, keyless)
            .map_err(|error| delivered_so_far(what, sent_batches, sent_records, error))?;

        for (partition, records) in by_partition {
            within(
                limit,
                || format!("sending to partition {partition} of '{topic}'"),
                clients[&partition].produce(records, settings.compression),
            )
            .await
            .map_err(|error| delivered_so_far(what, sent_batches, sent_records, error))?;
        }

        sent_batches += 1;
        sent_records += count;
    }

    let detail = if sent_batches == 0 {
        format!("0 records; nothing sent to '{topic}'")
    } else {
        format!("{sent_records} record(s) in {sent_batches} batch(es) to '{topic}'")
    };
    Ok(Summary::new(sent_records, detail))
}

/// At-least-once, per batch: what went before has been acknowledged, and part
/// of the failing batch may have landed too, since it goes to each partition
/// separately. Saying so is what makes a partial failure recoverable.
fn delivered_so_far(
    batch: u64,
    batches: u64,
    records: u64,
    error: ConnectorError,
) -> ConnectorError {
    ConnectorError::Data(format!(
        "batch {batch} failed after {batches} batch(es) ({records} record(s)) were delivered, \
         and part of batch {batch} may have landed too: {error}"
    ))
}

/// Split one batch of rows into records by partition.
pub(crate) fn assign(
    rows: Vec<Record>,
    key_column: Option<&str>,
    partitions: &[i32],
    keyless: i32,
) -> Result<BTreeMap<i32, Vec<KafkaRecord>>, ConnectorError> {
    let now = now_utc();
    let mut by_partition: BTreeMap<i32, Vec<KafkaRecord>> = BTreeMap::new();

    for row in rows {
        let key = match key_column {
            None => None,
            Some(column) => match row.get(column) {
                None => {
                    return Err(ConnectorError::property(
                        "key_column",
                        format!("'{column}' is not a column of the rows"),
                    ))
                }
                Some(value) => key_bytes(value),
            },
        };
        let partition = match &key {
            Some(bytes) => partition_for(bytes, partitions),
            None => keyless,
        };
        let value =
            serde_json::to_vec(&row).map_err(|error| ConnectorError::Data(error.to_string()))?;

        by_partition
            .entry(partition)
            .or_default()
            .push(KafkaRecord {
                key,
                value: Some(value),
                headers: BTreeMap::new(),
                timestamp: now,
            });
    }

    Ok(by_partition)
}

/// A key's bytes: text as UTF-8, a number or boolean as it is written, and
/// anything nested as its JSON. Null is no key.
pub(crate) fn key_bytes(value: &JsonValue) -> Option<Vec<u8>> {
    match value {
        JsonValue::Null => None,
        JsonValue::String(text) => Some(text.clone().into_bytes()),
        other => Some(other.to_string().into_bytes()),
    }
}

/// The partition a Java producer's default partitioner picks for this key:
/// murmur2 of the key bytes, made positive, modulo the partition count.
pub(crate) fn partition_for(key: &[u8], partitions: &[i32]) -> i32 {
    let positive = murmur2(key) & 0x7fff_ffff;
    partitions[(positive as usize) % partitions.len()]
}

/// Kafka's murmur2, bit for bit as `org.apache.kafka.common.utils.Utils`
/// computes it, including its seed and its fall-through tail.
pub(crate) fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: i32 = 0x5bd1_e995;
    const R: u32 = 24;

    let length = data.len();
    let mut h: i32 = (SEED as i32) ^ (length as i32);

    for chunk in data.chunks_exact(4) {
        let mut k = i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(M);
        k ^= ((k as u32) >> R) as i32;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    let tail = &data[length & !3..];
    if tail.len() == 3 {
        h ^= i32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        h ^= i32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        h ^= i32::from(tail[0]);
        h = h.wrapping_mul(M);
    }

    h ^= ((h as u32) >> 13) as i32;
    h = h.wrapping_mul(M);
    h ^= ((h as u32) >> 15) as i32;
    h
}

fn now_utc() -> rskafka::chrono::DateTime<rskafka::chrono::Utc> {
    use rskafka::chrono::TimeZone;
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as i64);
    rskafka::chrono::Utc
        .timestamp_millis_opt(millis)
        .single()
        .unwrap_or_default()
}
