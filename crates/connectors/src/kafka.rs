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
//! 10e is plaintext only. The sink, TLS and SASL are Phase 10f's.

use crate::http::{base64_bytes, kind, positive, text};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordWriter, Source, Summary,
};
use rskafka::client::partition::{OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::BackoffConfig;
use serde_json::{json, Map, Value as JsonValue};
use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

#[cfg(test)]
mod tests;

/// `src.stream.kafka`.
pub struct KafkaSource;

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
            .properties(vec![
                PropertySpec::text("brokers")
                    .required()
                    .help("Bootstrap brokers as host:port, comma-separated."),
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
                PropertySpec::enumerated("security", &["plaintext"])
                    .default(JsonValue::String("plaintext".into()))
                    .help("How to reach the brokers. TLS and SASL arrive in Phase 10f."),
                PropertySpec::integer("timeout_ms")
                    .default(JsonValue::from(30_000))
                    .help(
                        "How long any one step may take, retries included: connecting, \
                         listing, or one fetch.",
                    ),
                columns_property(),
            ])
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

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                ConnectorError::Data(format!("could not start the Kafka client: {error}"))
            })?;

        runtime.block_on(read_batch(&settings, saved, out))
    }
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct Settings {
    pub(crate) brokers: Vec<String>,
    pub(crate) topic: String,
    pub(crate) start: Start,
    pub(crate) max_records: u64,
    pub(crate) format: Format,
    pub(crate) timeout: Duration,
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
        let brokers = brokers(properties)?;
        let topic = text(properties, "topic")
            .ok_or_else(|| ConnectorError::property("topic", "is required"))?
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

        match text(properties, "security").unwrap_or("plaintext") {
            "plaintext" => {}
            other => {
                return Err(ConnectorError::property(
                    "security",
                    format!("'{other}' is not supported yet; this build speaks plaintext only"),
                ))
            }
        }

        Ok(Settings {
            brokers,
            topic,
            start,
            max_records: positive(properties, "max_records", 100_000)?,
            format,
            timeout: Duration::from_millis(positive(properties, "timeout_ms", 30_000)?),
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
    let at = || format!("partition {partition} offset {offset}");
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
                if METADATA_COLUMNS.contains(&name.as_str()) {
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

    row.insert("_topic".to_string(), JsonValue::String(topic.to_string()));
    row.insert("_partition".to_string(), JsonValue::from(partition));
    row.insert("_offset".to_string(), JsonValue::from(offset));
    row.insert(
        "_timestamp".to_string(),
        JsonValue::String(timestamp.to_string()),
    );
    // Text when the key is UTF-8, which is nearly always; base64 otherwise,
    // rather than a lossy conversion that would quietly change it.
    let key = key.map_or(JsonValue::Null, |bytes| match std::str::from_utf8(bytes) {
        Ok(text) => JsonValue::String(text.to_string()),
        Err(_) => JsonValue::String(base64_bytes(bytes)),
    });
    row.insert("_key".to_string(), key);

    Ok(row)
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

async fn read_batch(
    settings: &Settings,
    saved: Option<Position>,
    out: &mut dyn RecordWriter,
) -> Result<Summary, ConnectorError> {
    let limit = settings.timeout;
    let brokers = settings.brokers.join(",");
    let topic = settings.topic.as_str();

    let client = within(limit, || format!("connecting to {brokers}"), async {
        ClientBuilder::new(settings.brokers.clone())
            .client_id("etl")
            .backoff_config(backoff(limit))
            .build()
            .await
    })
    .await?;

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

/// One call to the broker, given `limit` to finish, its error said in terms of
/// what was being attempted.
async fn within<T, E: std::fmt::Display>(
    limit: Duration,
    what: impl Fn() -> String,
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, ConnectorError> {
    match tokio::time::timeout(limit, work).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(ConnectorError::Data(format!("{}: {error}", what()))),
        Err(_) => Err(ConnectorError::Data(format!(
            "{}: no answer within {} ms (timeout_ms)",
            what(),
            limit.as_millis()
        ))),
    }
}
