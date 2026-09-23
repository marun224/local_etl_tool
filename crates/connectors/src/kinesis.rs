//! Amazon Kinesis Data Streams, read in **bounded micro-batches**.
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

use crate::aws::{self, Credentials, Sources};
use crate::http::{base64_decode, positive, text, Client, Extra, Judged, Settings};
use crate::kafka::{key_text, timestamp_text, value_columns, Format, Start};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordWriter, Source, Summary,
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

/// The properties every Kinesis component has, first, then `own`.
pub(crate) fn with_connection(own: Vec<PropertySpec>) -> Vec<PropertySpec> {
    let mut properties =
        vec![
        PropertySpec::text("stream").required(),
        PropertySpec::text("region").help(
            "The AWS region, e.g. eu-west-1. Unset, AWS_REGION, AWS_DEFAULT_REGION or the \
             profile's region.",
        ),
        PropertySpec::text("profile").help(
            "A named profile in ~/.aws/credentials and ~/.aws/config. Unset, AWS_PROFILE or \
             default.",
        ),
        PropertySpec::text("access_key_id").help(
            "Only to override the environment and profiles. Use ${SECRET:name} rather than the \
             value itself.",
        ),
        PropertySpec::text("secret_access_key")
            .help("With access_key_id. Use ${SECRET:name} rather than the value itself."),
        PropertySpec::text("session_token").help("For temporary credentials."),
        PropertySpec::text("endpoint").help(
            "Only for a VPC endpoint or a Kinesis-compatible test server. Unset, \
             https://kinesis.<region>.amazonaws.com.",
        ),
        PropertySpec::integer("timeout_ms")
            .default(JsonValue::from(30_000))
            .help("How long one request may take."),
        PropertySpec::integer("retries").default(JsonValue::from(5)).help(
            "Extra attempts after throttling, a 5xx or a network failure. Kinesis throttles \
             often, so this is higher than for REST.",
        ),
    ];
    properties.extend(own);
    properties
}

/// A signed Kinesis API client.
pub(crate) struct Api {
    client: Client,
    host: String,
    region: String,
    credentials: Credentials,
}

impl Api {
    pub(crate) fn connect(
        properties: &JsonValue,
        sources: &Sources,
    ) -> Result<Self, ConnectorError> {
        let region = aws::region(properties, sources)?;
        let credentials = aws::credentials(properties, sources)?;
        let endpoint = text(properties, "endpoint")
            .map(|endpoint| endpoint.trim_end_matches('/').to_string())
            .unwrap_or_else(|| format!("https://kinesis.{region}.amazonaws.com"));
        let host = host_of(&endpoint).ok_or_else(|| {
            ConnectorError::property(
                "endpoint",
                format!("'{endpoint}' is not http://host[:port] or https://host[:port]"),
            )
        })?;

        let timeout = Duration::from_millis(positive(properties, "timeout_ms", 30_000)?);
        let retries = properties
            .get("retries")
            .and_then(JsonValue::as_u64)
            .unwrap_or(5) as u32;

        Ok(Api {
            client: Client::new(Settings::signed_post(
                format!("{endpoint}/"),
                timeout,
                retries,
            )),
            host,
            region,
            credentials,
        })
    }

    /// Where the credentials came from, for the report. Never the secret.
    pub(crate) fn credentials_source(&self) -> &str {
        &self.credentials.source
    }

    /// One Kinesis API call: `target` is the operation, e.g. `ListShards`.
    pub(crate) fn call(
        &mut self,
        target: &str,
        body: &JsonValue,
    ) -> Result<JsonValue, ConnectorError> {
        let bytes =
            serde_json::to_vec(body).map_err(|error| ConnectorError::Data(error.to_string()))?;
        let amz_target = format!("Kinesis_20131202.{target}");
        let (host, region, credentials) = (&self.host, &self.region, &self.credentials);

        let headers = || {
            let unsigned = vec![
                ("Host".to_string(), host.clone()),
                (
                    "Content-Type".to_string(),
                    "application/x-amz-json-1.1".to_string(),
                ),
                ("X-Amz-Target".to_string(), amz_target.clone()),
            ];
            let amz_date = aws::amz_date_now();
            let signed = aws::sign(
                &aws::Unsigned {
                    method: "POST",
                    target: "/",
                    headers: &unsigned,
                    body: &bytes,
                },
                &aws::Signer {
                    credentials,
                    region,
                    service: "kinesis",
                    amz_date: &amz_date,
                    normalize: true,
                    sign_body: false,
                    omit_session_token: false,
                },
            );
            // Host and Content-Type are sent by the client itself; the
            // signature covers the values it sends.
            let mut headers = vec![("X-Amz-Target".to_string(), amz_target.clone())];
            headers.extend(signed.headers);
            headers
        };
        // A "slow down". Kinesis says `LimitExceededException` for two
        // different things: a call rate ("Rate exceeded"), which passes, and an
        // account's shard limit, which does not. Only the first is retried; the
        // second fails at once with Kinesis's own words (found in 10h's tests).
        let throttled = |status: u16, body: &str| {
            status == 400
                && (body.contains("ProvisionedThroughputExceededException")
                    || body.contains("ThrottlingException")
                    || (body.contains("LimitExceededException")
                        && body.to_ascii_lowercase().contains("rate exceeded")))
        };

        let url = self.client.settings.url.clone();
        let extra = Extra {
            headers: &headers,
            content_type: "application/x-amz-json-1.1",
            throttled: &throttled,
        };
        let reply = self
            .client
            .send_with(&url, &[], Some(&bytes), Some(&extra), Judged::Accept)
            .map_err(|error| ConnectorError::Data(format!("Kinesis {target}: {error}")))?;

        if reply.body.trim().is_empty() {
            return Ok(JsonValue::Object(Map::new()));
        }
        serde_json::from_str(&reply.body).map_err(|error| {
            ConnectorError::Data(format!("Kinesis {target}: the answer is not JSON: {error}"))
        })
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

/// `host[:port]` of an endpoint, as the client will send it in `Host`: the
/// default port for the scheme is left out, as HTTP clients leave it out.
pub(crate) fn host_of(endpoint: &str) -> Option<String> {
    let (scheme, rest) = endpoint.split_once("://")?;
    let authority = rest.split('/').next()?;
    if authority.is_empty() {
        return None;
    }
    let default_port = match scheme {
        "https" => ":443",
        "http" => ":80",
        _ => return None,
    };
    Some(authority.trim_end_matches(default_port).to_string())
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
        let partial_keys = text(properties, "access_key_id").is_some()
            != text(properties, "secret_access_key").is_some();
        if partial_keys {
            return Err(ConnectorError::property(
                "access_key_id",
                "and secret_access_key are set together or not at all",
            ));
        }
        if let Some(endpoint) = text(properties, "endpoint") {
            if host_of(endpoint.trim_end_matches('/')).is_none() {
                return Err(ConnectorError::property(
                    "endpoint",
                    format!("'{endpoint}' is not http://host[:port] or https://host[:port]"),
                ));
            }
        }
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
