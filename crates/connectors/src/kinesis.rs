//! Amazon Kinesis Data Streams, read in **bounded micro-batches**, and written
//! with `PutRecords` (see *The sink*).
//!
//! Kinesis is a JSON-over-HTTPS API, so this goes through the same blocking
//! `ureq` layer as REST and GraphQL: no `tokio`. Every request is signed with
//! SigV4 ([`crate::aws`]), afresh on each attempt so a retry carries its own
//! time.
//!
//! How it differs from Kafka and NATS, and what it does about it:
//!
//! - **A position is a sequence number per shard**, and sequence numbers are
//!   128-bit and leave gaps. They are kept as text.
//! - **A batch reads each shard until Kinesis says it is caught up**
//!   (`MillisBehindLatest` 0), the shard ends, or `max_records` is reached,
//!   shards taking turns. There is no cheap way to ask a shard for its newest
//!   sequence number, so this is "up to now", not a snapshot taken at the start
//!   (Settled decision 50).
//! - **Shards split and merge.** A child shard is read only after its parents
//!   are finished, so a partition key's records stay in order across resharding
//!   (decision 51). A parent that has aged out of the stream counts as finished.
//! - **Expiry cannot be counted.** When the last record a run read is no longer
//!   held, records after it *may* have expired unread. With `on_expired: fail`
//!   (the default) the read fails and says so; `continue` carries on from the
//!   oldest record held (decision 52).
//!
//! Nothing is registered with Kinesis: no consumer, no enhanced fan-out. The
//! position lives in this project's state file, as for Kafka and NATS.

use crate::aws::{self, Sources};
use crate::http::{base64_bytes, base64_decode, positive, text};
use crate::kafka::{key_text, timestamp_text, value_columns, Format, Start};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordReader, RecordWriter, Sink, Source,
    Summary,
};
use serde_json::{json, Map, Value as JsonValue};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// `src.stream.kinesis`.
pub struct KinesisSource;

/// The columns every row carries, whatever `value_format` says.
pub(crate) const METADATA_COLUMNS: [&str; 5] = [
    "_stream",
    "_shard",
    "_sequence",
    "_timestamp",
    "_partition_key",
];

/// Kinesis allows five `GetRecords` a second per shard.
const PER_SHARD_INTERVAL: Duration = Duration::from_millis(200);

/// The most one `GetRecords` returns.
const GET_RECORDS_LIMIT: u64 = 10_000;

// ---------------------------------------------------------------------------
// The connection: a signed Kinesis API client
// ---------------------------------------------------------------------------

/// How Kinesis speaks AWS's JSON protocol.
pub(crate) static KINESIS: aws::Protocol = aws::Protocol {
    name: "Kinesis",
    service: "kinesis",
    target_prefix: "Kinesis_20131202",
    content_type: "application/x-amz-json-1.1",
    throttled,
};

/// A "slow down". Kinesis says `LimitExceededException` for two different
/// things: a call rate ("Rate exceeded"), which passes, and an account's shard
/// limit, which does not. Only the first is retried; the second fails at once
/// with Kinesis's own words (found in 10h's tests).
fn throttled(status: u16, body: &str) -> bool {
    status == 400
        && (body.contains("ProvisionedThroughputExceededException")
            || body.contains("ThrottlingException")
            || (body.contains("LimitExceededException")
                && body.to_ascii_lowercase().contains("rate exceeded")))
}

/// The properties every Kinesis component has, first, then `own`.
pub(crate) fn with_connection(own: Vec<PropertySpec>) -> Vec<PropertySpec> {
    let mut properties = vec![PropertySpec::text("stream").required()];
    properties.extend(aws::connection_properties("kinesis"));
    properties.extend(own);
    properties
}

use crate::aws::check_connection;
#[cfg(test)]
use crate::aws::{host_of, Credentials};

/// A signed Kinesis API client.
pub(crate) struct Api(aws::JsonApi);

impl Api {
    pub(crate) fn connect(
        properties: &JsonValue,
        sources: &Sources,
    ) -> Result<Self, ConnectorError> {
        aws::JsonApi::connect(properties, sources, &KINESIS).map(Api)
    }

    /// Where the credentials came from, for the report. Never the secret.
    pub(crate) fn credentials_source(&self) -> &str {
        self.0.credentials_source()
    }

    /// One Kinesis API call: `target` is the operation, e.g. `ListShards`.
    pub(crate) fn call(
        &mut self,
        target: &str,
        body: &JsonValue,
    ) -> Result<JsonValue, ConnectorError> {
        self.0.call(target, body)
    }

    /// Every shard of `stream`, following `NextToken`.
    pub(crate) fn shards(&mut self, stream: &str) -> Result<Vec<Shard>, ConnectorError> {
        let mut shards = Vec::new();
        let mut request = json!({ "StreamName": stream });
        loop {
            let answer = self.call("ListShards", &request)?;
            for shard in answer["Shards"].as_array().into_iter().flatten() {
                shards.push(Shard {
                    id: shard["ShardId"].as_str().unwrap_or_default().to_string(),
                    parents: ["ParentShardId", "AdjacentParentShardId"]
                        .iter()
                        .filter_map(|key| shard[key].as_str().map(str::to_string))
                        .collect(),
                });
            }
            match answer["NextToken"].as_str() {
                Some(token) => request = json!({ "NextToken": token }),
                None => break,
            }
        }
        Ok(shards)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Shard {
    pub(crate) id: String,
    pub(crate) parents: Vec<String>,
}

// ---------------------------------------------------------------------------
// The position
// ---------------------------------------------------------------------------

/// Where one shard's reading got to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShardPosition {
    /// Read up to and including this sequence number.
    After(String),
    /// A closed shard read to its end: its children may be read.
    Done,
    /// Nothing read yet from a `latest` start: records arriving from this
    /// time on, in milliseconds since 1970, are still to read.
    Since(i64),
    /// Nothing read yet from a shard read from its beginning: the next run
    /// starts at its beginning again. Saved for a shard `max_records` left
    /// unread, so it is not mistaken for one to start at "now".
    Start,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Position {
    pub(crate) stream: String,
    pub(crate) shards: BTreeMap<String, ShardPosition>,
}

impl Position {
    pub(crate) fn to_json(&self) -> JsonValue {
        let shards: Map<String, JsonValue> = self
            .shards
            .iter()
            .map(|(id, position)| {
                let value = match position {
                    ShardPosition::After(sequence) => json!({ "after": sequence }),
                    ShardPosition::Done => json!({ "done": true }),
                    ShardPosition::Since(millis) => json!({ "since": millis }),
                    ShardPosition::Start => json!({ "start": true }),
                };
                (id.clone(), value)
            })
            .collect();
        json!({ "stream": self.stream, "shards": shards })
    }

    pub(crate) fn from_json(value: &JsonValue) -> Result<Self, ConnectorError> {
        let unusable = || {
            ConnectorError::Data(format!(
                "the saved position is not one this connector wrote ({value}); `etl state forget` \
                 this node to start it over"
            ))
        };
        let stream = value["stream"].as_str().ok_or_else(unusable)?.to_string();
        let mut shards = BTreeMap::new();
        for (id, position) in value["shards"].as_object().ok_or_else(unusable)? {
            let position = if let Some(sequence) = position["after"].as_str() {
                if sequence.is_empty() || !sequence.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(unusable());
                }
                ShardPosition::After(sequence.to_string())
            } else if position["done"].as_bool() == Some(true) {
                ShardPosition::Done
            } else if let Some(millis) = position["since"].as_i64() {
                ShardPosition::Since(millis)
            } else if position["start"].as_bool() == Some(true) {
                ShardPosition::Start
            } else {
                return Err(unusable());
            };
            shards.insert(id.clone(), position);
        }
        Ok(Position { stream, shards })
    }
}

/// Which shards may be read now: not done, and every parent finished. A parent
/// the stream no longer lists has aged out, and counts as finished.
pub(crate) fn readable(
    shards: &[Shard],
    positions: &BTreeMap<String, ShardPosition>,
) -> Vec<String> {
    let listed: BTreeSet<&str> = shards.iter().map(|shard| shard.id.as_str()).collect();
    let finished =
        |id: &str| !listed.contains(id) || positions.get(id) == Some(&ShardPosition::Done);
    shards
        .iter()
        .filter(|shard| !finished(&shard.id))
        .filter(|shard| shard.parents.iter().all(|parent| finished(parent)))
        .map(|shard| shard.id.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------

impl Source for KinesisSource {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("src.stream.kinesis", "Kinesis stream")
            .description(
                "Read an Amazon Kinesis data stream in bounded batches: each run reads what \
                 arrived since the last successful one, up to max_records.",
            )
            .icon("radio")
            .properties(with_connection(vec![
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
                        "json makes each record's fields into columns; text gives a value \
                         column; bytes gives value as base64. Every row also has _stream, \
                         _shard, _sequence, _timestamp and _partition_key.",
                    ),
                PropertySpec::enumerated("on_expired", &["fail", "continue"])
                    .default(JsonValue::String("fail".into()))
                    .help(
                        "When the last record read has expired, records after it may have too. \
                         fail stops and says so; continue reads on from the oldest record held.",
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
        let mut api = Api::connect(properties, &Sources::process())?;
        read_batch(&mut api, &settings, saved, out)
    }
}

#[derive(Debug)]
pub(crate) struct SourceSettings {
    pub(crate) stream: String,
    pub(crate) start: Start,
    pub(crate) max_records: u64,
    pub(crate) format: Format,
    pub(crate) continue_on_expiry: bool,
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
        let continue_on_expiry = match text(properties, "on_expired").unwrap_or("fail") {
            "fail" => false,
            "continue" => true,
            other => {
                return Err(ConnectorError::property(
                    "on_expired",
                    format!("'{other}' is not one of fail, continue"),
                ))
            }
        };
        check_connection(properties)?;
        Ok(SourceSettings {
            stream,
            start,
            max_records: positive(properties, "max_records", 100_000)?,
            format,
            continue_on_expiry,
        })
    }
}

/// One shard being read in this run.
struct Reading {
    id: String,
    iterator: Option<String>,
    last_call: Option<Instant>,
}

/// How a shard's reading began, which decides what to save for it if this run
/// reads nothing from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Opened {
    /// From a saved sequence or time: its position stands.
    Continuing,
    /// From the shard's beginning: save [`ShardPosition::Start`].
    FromStart,
    /// From "now", on a `latest` first run: save when "now" was.
    FromNow,
}

pub(crate) fn read_batch(
    api: &mut Api,
    settings: &SourceSettings,
    saved: Option<Position>,
    out: &mut dyn RecordWriter,
) -> Result<Summary, ConnectorError> {
    let stream = settings.stream.as_str();
    // To the millisecond: to the second, records put earlier in the same second
    // would count as "from now on" and be read by a `latest` start (found in
    // 10h's tests).
    let started_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as i64);
    let mut notes: Vec<String> = Vec::new();

    let first_run = saved.is_none();
    let mut positions: BTreeMap<String, ShardPosition> = match saved {
        Some(position) if position.stream != stream => {
            notes.push(format!(
                "the saved position was for stream '{}', so this starts from {}",
                position.stream,
                start_name(settings.start)
            ));
            BTreeMap::new()
        }
        Some(position) => position.shards,
        None => BTreeMap::new(),
    };
    // A position from another stream is a first run for this one.
    let first_run = first_run || positions.is_empty();

    let shards = api.shards(stream)?;
    let mut read = 0u64;
    let mut stopped_by_cap = false;

    // Lineage: read what is readable; a closed shard finished in this run can
    // make its children readable, so go round again until nothing new opens.
    let mut attempted: BTreeSet<String> = BTreeSet::new();
    let mut opened: BTreeMap<String, Opened> = BTreeMap::new();
    loop {
        let mut turn: Vec<Reading> = Vec::new();
        for id in readable(&shards, &positions) {
            if attempted.insert(id.clone()) {
                let (iterator, how) = open_iterator(
                    api,
                    stream,
                    &id,
                    positions.get(&id),
                    settings,
                    first_run,
                    &mut notes,
                )?;
                opened.insert(id.clone(), how);
                turn.push(Reading {
                    id,
                    iterator,
                    last_call: None,
                });
            }
        }
        if turn.is_empty() {
            break;
        }

        // Shards take turns, one GetRecords each, so none is starved when
        // `max_records` cuts the batch short.
        while !turn.is_empty() {
            let mut still = Vec::with_capacity(turn.len());
            for mut shard in turn {
                if read >= settings.max_records {
                    stopped_by_cap = true;
                    break;
                }
                let Some(iterator) = shard.iterator.take() else {
                    positions.insert(shard.id.clone(), ShardPosition::Done);
                    continue;
                };
                if let Some(last) = shard.last_call {
                    let since = last.elapsed();
                    if since < PER_SHARD_INTERVAL {
                        std::thread::sleep(PER_SHARD_INTERVAL - since);
                    }
                }
                shard.last_call = Some(Instant::now());

                let limit = (settings.max_records - read).min(GET_RECORDS_LIMIT);
                let answer = api.call(
                    "GetRecords",
                    &json!({ "ShardIterator": iterator, "Limit": limit }),
                )?;
                let records = answer["Records"].as_array().cloned().unwrap_or_default();
                for record in &records {
                    let sequence = record["SequenceNumber"].as_str().unwrap_or_default();
                    out.write(row(stream, &shard.id, record, settings.format)?)?;
                    read += 1;
                    positions.insert(shard.id.clone(), ShardPosition::After(sequence.to_string()));
                }

                let behind = answer["MillisBehindLatest"].as_u64();
                match answer["NextShardIterator"].as_str() {
                    // A closed shard read to its end: its children may open.
                    None => {
                        positions.insert(shard.id.clone(), ShardPosition::Done);
                    }
                    // Caught up with an open shard: this run is done with it.
                    Some(_) if behind == Some(0) && records.len() < limit as usize => {}
                    Some(next) => {
                        shard.iterator = Some(next.to_string());
                        still.push(shard);
                    }
                }
            }
            if stopped_by_cap {
                break;
            }
            turn = still;
        }
        if stopped_by_cap {
            break;
        }
    }

    // A shard this run opened but read nothing from still has to say where the
    // next run begins. Which depends on how it was opened: a `latest` first
    // run's "now", or the shard's beginning. Getting this wrong loses records:
    // an earlier draft saved "now" for every such shard, so a shard that
    // `max_records` left unread was skipped the next run (found in 10h's tests).
    for (id, how) in &opened {
        if positions.contains_key(id) && *how == Opened::Continuing {
            continue;
        }
        match how {
            Opened::FromNow => {
                positions
                    .entry(id.clone())
                    .or_insert(ShardPosition::Since(started_ms));
            }
            Opened::FromStart => {
                positions.entry(id.clone()).or_insert(ShardPosition::Start);
            }
            Opened::Continuing => {}
        }
    }

    let mut detail = format!(
        "{read} record(s) from {} shard(s) of '{stream}' (credentials from {})",
        shards.len(),
        api.credentials_source()
    );
    if stopped_by_cap {
        detail.push_str(&format!(
            "; stopped at max_records ({}), with more for the next run",
            settings.max_records
        ));
    }
    for note in notes {
        detail.push_str("; ");
        detail.push_str(&note);
    }

    let position = Position {
        stream: stream.to_string(),
        shards: positions,
    };
    Ok(Summary {
        checkpoint: Some(position.to_json()),
        ..Summary::new(read, detail)
    })
}

/// A shard iterator for where this shard's reading should begin, checking on
/// the way that the last record read is still held.
fn open_iterator(
    api: &mut Api,
    stream: &str,
    shard: &str,
    position: Option<&ShardPosition>,
    settings: &SourceSettings,
    first_run: bool,
    notes: &mut Vec<String>,
) -> Result<(Option<String>, Opened), ConnectorError> {
    let request = |kind: &str, extra: JsonValue| {
        let mut body = json!({ "StreamName": stream, "ShardId": shard, "ShardIteratorType": kind });
        if let (Some(body), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
            body.extend(extra.clone());
        }
        body
    };
    let iterator = |api: &mut Api, body: JsonValue| -> Result<Option<String>, ConnectorError> {
        let answer = api.call("GetShardIterator", &body)?;
        Ok(answer["ShardIterator"].as_str().map(str::to_string))
    };

    match position {
        Some(ShardPosition::Done) => Ok((None, Opened::Continuing)),
        Some(ShardPosition::Start) => Ok((
            iterator(api, request("TRIM_HORIZON", json!({})))?,
            Opened::FromStart,
        )),
        Some(ShardPosition::Since(millis)) => Ok((
            iterator(
                api,
                request(
                    "AT_TIMESTAMP",
                    json!({ "Timestamp": *millis as f64 / 1000.0 }),
                ),
            )?,
            Opened::Continuing,
        )),
        Some(ShardPosition::After(sequence)) => {
            if still_held(api, stream, shard, sequence)? {
                return Ok((
                    iterator(
                        api,
                        request(
                            "AFTER_SEQUENCE_NUMBER",
                            json!({ "StartingSequenceNumber": sequence }),
                        ),
                    )?,
                    Opened::Continuing,
                ));
            }
            if !settings.continue_on_expiry {
                return Err(ConnectorError::Data(format!(
                    "stream '{stream}' shard {shard}: the last record read (sequence {sequence}) \
                     is no longer held, so records after it may have expired before this \
                     pipeline read them. Kinesis cannot say how many. Nothing was read. If the \
                     stream had no new records for longer than its retention, nothing was lost. \
                     To carry on from the oldest record still held, set on_expired to continue, \
                     or `etl state forget` this node"
                )));
            }
            notes.push(format!(
                "shard {shard}: the last record read had expired, so reading carried on from the \
                 oldest record held (on_expired: continue); some records may have been lost"
            ));
            Ok((
                iterator(api, request("TRIM_HORIZON", json!({})))?,
                Opened::FromStart,
            ))
        }
        None => {
            // A shard with no position of its own: a first run follows `start`;
            // a later run has met a shard that is new since (a child of a split
            // or merge), and reads all of it.
            if first_run && settings.start == Start::Latest {
                return Ok((
                    iterator(api, request("LATEST", json!({})))?,
                    Opened::FromNow,
                ));
            }
            if !first_run {
                notes.push(format!(
                    "shard {shard} is new since the last run and is read from its start"
                ));
            }
            Ok((
                iterator(api, request("TRIM_HORIZON", json!({})))?,
                Opened::FromStart,
            ))
        }
    }
}

/// Whether the record at `sequence` is still in the shard: the check that
/// tells a run whether records after it may have expired.
fn still_held(
    api: &mut Api,
    stream: &str,
    shard: &str,
    sequence: &str,
) -> Result<bool, ConnectorError> {
    let answer = api.call(
        "GetShardIterator",
        &json!({
            "StreamName": stream,
            "ShardId": shard,
            "ShardIteratorType": "AT_SEQUENCE_NUMBER",
            "StartingSequenceNumber": sequence,
        }),
    );
    let iterator = match answer {
        Ok(answer) => answer["ShardIterator"].as_str().map(str::to_string),
        // A sequence number the shard no longer holds: AWS answers
        // InvalidArgumentException, kinesis-mock ResourceNotFoundException
        // naming the sequence number. Both are exactly "not held". A missing
        // stream is also ResourceNotFoundException, but does not mention one.
        Err(error)
            if {
                let text = error.to_string();
                text.contains("InvalidArgumentException")
                    || (text.contains("ResourceNotFoundException")
                        && text.contains("SequenceNumber"))
            } =>
        {
            return Ok(false)
        }
        Err(error) => return Err(error),
    };
    let Some(iterator) = iterator else {
        return Ok(false);
    };
    let first = api.call(
        "GetRecords",
        &json!({ "ShardIterator": iterator, "Limit": 1 }),
    )?;
    Ok(first["Records"][0]["SequenceNumber"].as_str() == Some(sequence))
}

fn start_name(start: Start) -> &'static str {
    match start {
        Start::Earliest => "earliest",
        Start::Latest => "latest",
    }
}

/// One Kinesis record as a row.
pub(crate) fn row(
    stream: &str,
    shard: &str,
    record: &JsonValue,
    format: Format,
) -> Result<Record, ConnectorError> {
    let sequence = record["SequenceNumber"].as_str().unwrap_or_default();
    let data = record["Data"].as_str().unwrap_or_default();
    let bytes = base64_decode(data).ok_or_else(|| {
        ConnectorError::Data(format!(
            "stream '{stream}' shard {shard} sequence {sequence}: the record's data is not base64"
        ))
    })?;
    let value = if bytes.is_empty() && format == Format::Json {
        None
    } else {
        Some(bytes.as_slice())
    };
    let mut row = value_columns(
        format,
        value,
        &format!("shard {shard} sequence {sequence}"),
        &METADATA_COLUMNS,
    )?;

    // Arrival time comes as seconds since 1970, with a fraction.
    let millis = record["ApproximateArrivalTimestamp"]
        .as_f64()
        .map_or(0, |seconds| (seconds * 1000.0).round() as i64);

    row.insert("_stream".to_string(), JsonValue::String(stream.to_string()));
    row.insert("_shard".to_string(), JsonValue::String(shard.to_string()));
    row.insert(
        "_sequence".to_string(),
        JsonValue::String(sequence.to_string()),
    );
    row.insert(
        "_timestamp".to_string(),
        JsonValue::String(timestamp_text(millis)),
    );
    row.insert(
        "_partition_key".to_string(),
        key_text(record["PartitionKey"].as_str().map(str::as_bytes)),
    );
    Ok(row)
}

// ---------------------------------------------------------------------------
// The sink
// ---------------------------------------------------------------------------

/// `snk.stream.kinesis`.
pub struct KinesisSink;

/// The most one `PutRecords` takes: 500 records, 5 MiB with their keys.
const PUT_RECORDS_LIMIT: u64 = 500;
const PUT_RECORDS_BYTES: usize = 5 * 1024 * 1024;

/// The most one record may be: 1 MiB of data and key together.
const RECORD_BYTES: usize = 1024 * 1024;

/// A partition key is 1 to 256 characters.
const KEY_CHARACTERS: usize = 256;

/// The first wait before sending refused records again, doubling each time.
const RESEND_BACKOFF: Duration = Duration::from_millis(200);

impl Sink for KinesisSink {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("snk.stream.kinesis", "Kinesis stream")
            .description(
                "Put rows into an Amazon Kinesis data stream, one JSON record each, up to 500 \
                 to a PutRecords call. Records Kinesis refuses for throughput are sent again.",
            )
            .icon("radio")
            .properties(with_connection(vec![
                PropertySpec::text("partition_key_column").help(
                    "The column whose value is each record's partition key. Records with the \
                     same key go to the same shard, in order. Unset, rows are spread across \
                     shards by their row number.",
                ),
                PropertySpec::integer("batch_size")
                    .default(JsonValue::from(PUT_RECORDS_LIMIT))
                    .help(
                        "Records per PutRecords call, at most 500. A call is also kept under \
                         Kinesis's 5 MiB.",
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
        _context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = SinkSettings::from(properties)?;
        let mut api = Api::connect(properties, &Sources::process())?;
        write_records(&mut api, &settings, input, RESEND_BACKOFF)
    }
}

#[derive(Debug)]
pub(crate) struct SinkSettings {
    pub(crate) stream: String,
    pub(crate) key_column: Option<String>,
    pub(crate) batch_size: u64,
    pub(crate) retries: u32,
}

impl SinkSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let stream = text(properties, "stream")
            .ok_or_else(|| ConnectorError::property("stream", "is required"))?
            .trim()
            .to_string();
        check_connection(properties)?;
        let batch_size = positive(properties, "batch_size", PUT_RECORDS_LIMIT)?;
        if batch_size > PUT_RECORDS_LIMIT {
            return Err(ConnectorError::property(
                "batch_size",
                format!("{batch_size} is more than 500, the most one PutRecords call takes"),
            ));
        }
        Ok(SinkSettings {
            stream,
            key_column: text(properties, "partition_key_column").map(str::to_string),
            batch_size,
            retries: properties
                .get("retries")
                .and_then(JsonValue::as_u64)
                .unwrap_or(5) as u32,
        })
    }
}

/// One row, ready to put.
#[derive(Debug)]
pub(crate) struct Entry {
    /// Counted from 1, for messages.
    pub(crate) row: u64,
    /// Base64, as `PutRecords` takes it.
    pub(crate) data: String,
    pub(crate) key: String,
    /// Data and key, before base64: what Kinesis's limits count.
    pub(crate) size: usize,
}

/// Row `row` (counted from 1) as a record: the row as JSON, and its key.
pub(crate) fn entry(
    row: u64,
    record: &Record,
    key_column: Option<&str>,
) -> Result<Entry, ConnectorError> {
    let key = match key_column {
        None => row.to_string(),
        Some(column) => match record.get(column) {
            None => {
                return Err(ConnectorError::property(
                    "partition_key_column",
                    format!("'{column}' is not a column of the rows"),
                ))
            }
            Some(JsonValue::Null) => {
                return Err(ConnectorError::Data(format!(
                    "row {row}: '{column}' is null, and Kinesis needs a partition key for every \
                     record"
                )))
            }
            Some(JsonValue::String(text)) => text.clone(),
            Some(other) => other.to_string(),
        },
    };
    let characters = key.chars().count();
    if characters == 0 || characters > KEY_CHARACTERS {
        return Err(ConnectorError::Data(format!(
            "row {row}: a partition key must be 1 to 256 characters, and this one is {characters}"
        )));
    }

    let data =
        serde_json::to_vec(record).map_err(|error| ConnectorError::Data(error.to_string()))?;
    let size = data.len() + key.len();
    if size > RECORD_BYTES {
        return Err(ConnectorError::Data(format!(
            "row {row} is {size} bytes with its partition key, and Kinesis takes at most \
             {RECORD_BYTES} (1 MiB) in one record"
        )));
    }
    Ok(Entry {
        row,
        data: base64_bytes(&data),
        key,
        size,
    })
}

/// What has been put so far, for the summary and for a failure's message.
#[derive(Default)]
struct Delivered {
    records: u64,
    calls: u64,
    resent: u64,
    shards: BTreeSet<String>,
}

impl Delivered {
    fn failed(&self, stream: &str, error: impl std::fmt::Display) -> ConnectorError {
        ConnectorError::Data(format!(
            "{error}. {} record(s) had been put into '{stream}' before this, and stay there",
            self.records
        ))
    }
}

/// Every row of `input` into the stream, a `PutRecords` call at a time.
pub(crate) fn write_records(
    api: &mut Api,
    settings: &SinkSettings,
    input: &mut dyn RecordReader,
    backoff: Duration,
) -> Result<Summary, ConnectorError> {
    let stream = settings.stream.as_str();
    let mut delivered = Delivered::default();
    let mut batch: Vec<Entry> = Vec::new();
    let mut batch_bytes = 0usize;
    let mut row = 0u64;

    while let Some(record) = input.read()? {
        row += 1;
        let entry = entry(row, &record, settings.key_column.as_deref()).map_err(|error| {
            match error {
                // A setting that cannot work says so plainly, before anything is put.
                ConnectorError::Property { .. } if delivered.records == 0 => error,
                other => delivered.failed(stream, other),
            }
        })?;
        let full = batch.len() as u64 == settings.batch_size
            || batch_bytes + entry.size > PUT_RECORDS_BYTES;
        if full {
            put(
                api,
                settings,
                std::mem::take(&mut batch),
                backoff,
                &mut delivered,
            )?;
            batch_bytes = 0;
        }
        batch_bytes += entry.size;
        batch.push(entry);
    }
    if !batch.is_empty() {
        put(api, settings, batch, backoff, &mut delivered)?;
    }

    let detail = if delivered.records == 0 {
        format!("0 records; nothing put into '{stream}'")
    } else {
        let mut detail = format!(
            "{} record(s) in {} call(s) into '{stream}', landing on {} shard(s) (credentials \
             from {})",
            delivered.records,
            delivered.calls,
            delivered.shards.len(),
            api.credentials_source()
        );
        if delivered.resent > 0 {
            detail.push_str(&format!(
                "; {} sent again after Kinesis refused them for throughput",
                delivered.resent
            ));
        }
        detail
    };
    Ok(Summary::new(delivered.records, detail))
}

/// What a refused record's `ErrorCode` means: worth sending again, or not.
pub(crate) fn resendable(code: &str) -> bool {
    matches!(
        code,
        "ProvisionedThroughputExceededException" | "InternalFailure" | "KMSThrottlingException"
    )
}

/// One `PutRecords`, and again for the records it refused, up to `retries`
/// more times. A call can put some of its records and refuse others; only the
/// refused ones are sent again, so they land after the call's others.
fn put(
    api: &mut Api,
    settings: &SinkSettings,
    entries: Vec<Entry>,
    backoff: Duration,
    delivered: &mut Delivered,
) -> Result<(), ConnectorError> {
    let stream = settings.stream.as_str();
    let mut pending = entries;
    let mut attempt = 0u32;

    loop {
        let body = json!({
            "StreamName": stream,
            "Records": pending
                .iter()
                .map(|entry| json!({ "Data": entry.data, "PartitionKey": entry.key }))
                .collect::<Vec<_>>(),
        });
        let answer = api
            .call("PutRecords", &body)
            .map_err(|error| delivered.failed(stream, error))?;
        delivered.calls += 1;

        let results = answer["Records"].as_array().cloned().unwrap_or_default();
        if results.len() != pending.len() {
            return Err(delivered.failed(
                stream,
                format!(
                    "Kinesis PutRecords answered for {} record(s) of {} sent, so which landed \
                     is unknown",
                    results.len(),
                    pending.len()
                ),
            ));
        }

        let mut refused = Vec::new();
        let mut last_refusal = String::new();
        for (entry, result) in pending.into_iter().zip(&results) {
            match result["ErrorCode"].as_str() {
                None => {
                    delivered.records += 1;
                    if let Some(shard) = result["ShardId"].as_str() {
                        delivered.shards.insert(shard.to_string());
                    }
                }
                Some(code) => {
                    let message = result["ErrorMessage"].as_str().unwrap_or_default();
                    if !resendable(code) {
                        return Err(delivered.failed(
                            stream,
                            format!("Kinesis refused row {}: {code}: {message}", entry.row),
                        ));
                    }
                    last_refusal = format!("{code}: {message}");
                    refused.push(entry);
                }
            }
        }

        if refused.is_empty() {
            return Ok(());
        }
        if attempt == settings.retries {
            return Err(delivered.failed(
                stream,
                format!(
                    "Kinesis still refused {} record(s), the first row {}, after {} resend(s); \
                     the last refusal said {last_refusal}",
                    refused.len(),
                    refused[0].row,
                    settings.retries
                ),
            ));
        }
        std::thread::sleep(backoff.saturating_mul(1 << attempt.min(6)));
        attempt += 1;
        delivered.resent += refused.len() as u64;
        pending = refused;
    }
}
