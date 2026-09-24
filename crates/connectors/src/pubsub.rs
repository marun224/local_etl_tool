//! Google Cloud Pub/Sub: a subscription pulled in **bounded batches** and
//! **held until the run's outcome is known**, and a topic published to.
//!
//! Pub/Sub is a queue in the sense that matters here (Settled decision 58): a
//! subscription hands a message out, waits an *ack deadline* for it to be
//! acknowledged, and hands it out again if it is not. So, as for SQS, this
//! source keeps no position; it pulls, holds, and hands the engine a
//! [`Receipt`]:
//!
//! - **acknowledged** after the run fully succeeded and its sinks delivered:
//!   `:acknowledge`, and the messages are gone from the subscription;
//! - **released** on every other path, a preview included:
//!   `:modifyAckDeadline` to 0, and they are handed out again at once.
//!
//! A pull holds messages for the *subscription's* deadline, 10 seconds unless
//! it was set longer, so each pull's messages are extended at once to
//! `ack_deadline_seconds`, and a **lease keeper** extends everything held every
//! half of that while the run goes on (decision 60).
//!
//! A batch ends at `max_records`, when a pull comes back empty, or at
//! `max_wait_ms` (decision 61). Pub/Sub's REST API (`v1`), through the shared
//! `ureq` layer: no `tokio`. Signing in is [`crate::gcp`]'s.
//!
//! `snk.queue.pubsub` publishes each row as one JSON message, up to 1,000 and
//! under 10 MB to a `:publish` call.

use crate::aws::{host_of, Sources};
use crate::gcp::{self, Credentials, Tokens};
use crate::http::{base64_bytes, base64_decode, positive, text, Client, Extra, Judged, Settings};
use crate::kafka::{value_columns, Format};
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

/// `src.queue.pubsub`.
pub struct PubsubSource;

/// `snk.queue.pubsub`.
pub struct PubsubSink;

/// The columns every row carries, whatever `value_format` says.
pub(crate) const METADATA_COLUMNS: [&str; 6] = [
    "_subscription",
    "_message_id",
    "_publish_time",
    "_ordering_key",
    "_attributes",
    "_delivery_attempt",
];

/// The OAuth scope every call needs.
const SCOPE: &str = "https://www.googleapis.com/auth/pubsub";

/// Where Pub/Sub is when nothing says otherwise.
const DEFAULT_ENDPOINT: &str = "https://pubsub.googleapis.com";

/// The most one pull asks for, and one publish carries.
const MESSAGES_PER_CALL: usize = 1000;

/// The largest request Pub/Sub takes: a publish, whose data is base64.
const PUBLISH_BYTES: usize = 10_000_000;

/// The most acknowledgement IDs one `:acknowledge` or `:modifyAckDeadline`
/// carries, and the most bytes of them: Pub/Sub refuses a request over 512 KB,
/// and an ID's length is not promised.
const IDS_PER_CALL: usize = 1000;
const ID_BYTES_PER_CALL: usize = 400_000;

/// The longest ack deadline Pub/Sub allows: ten minutes.
const MAX_ACK_DEADLINE: u64 = 600;

// ---------------------------------------------------------------------------
// The API
// ---------------------------------------------------------------------------

/// The properties every Pub/Sub component has, after its own identifying ones.
fn connection_properties() -> Vec<PropertySpec> {
    vec![
        PropertySpec::text("project").help(
            "The Google Cloud project ID. Not needed when the subscription or topic is given as a \
             full path, projects/<project>/....",
        ),
        PropertySpec::text("credentials_file").help(
            "A service account's JSON key file, or gcloud's login file. Unset: \
             GOOGLE_APPLICATION_CREDENTIALS, then gcloud's application-default login.",
        ),
        PropertySpec::text("endpoint").help(
            "Only for a regional endpoint or the emulator. A plain http:// endpoint is an \
             emulator, and nothing is signed. Unset: PUBSUB_EMULATOR_HOST if set, else \
             https://pubsub.googleapis.com.",
        ),
        PropertySpec::integer("timeout_ms")
            .default(JsonValue::from(30_000))
            .help("How long one request may take."),
        PropertySpec::integer("retries")
            .default(JsonValue::from(5))
            .help("Extra attempts after a 429, a 5xx or a network failure."),
    ]
}

/// A subscription or topic's full name, `projects/<p>/<kind>/<name>`, from
/// `property` and `project`.
pub(crate) fn resource(
    properties: &JsonValue,
    property: &str,
    kind: &str,
) -> Result<String, ConnectorError> {
    let name = text(properties, property)
        .map(str::trim)
        .ok_or_else(|| ConnectorError::property(property, "is required"))?;
    let project = text(properties, "project").map(str::trim);
    let full = if name.starts_with("projects/") {
        let parts: Vec<&str> = name.split('/').collect();
        if parts.len() != 4 || parts[1].is_empty() || parts[2] != kind || parts[3].is_empty() {
            return Err(ConnectorError::property(
                property,
                format!("'{name}' is not projects/<project>/{kind}/<name>"),
            ));
        }
        if let Some(project) = project.filter(|project| *project != parts[1]) {
            return Err(ConnectorError::property(
                "project",
                format!(
                    "is '{project}', but {property} names project '{}'",
                    parts[1]
                ),
            ));
        }
        name.to_string()
    } else {
        if name.contains('/') {
            return Err(ConnectorError::property(
                property,
                format!("'{name}' is neither a name nor projects/<project>/{kind}/<name>"),
            ));
        }
        let project = project.ok_or_else(|| {
            ConnectorError::property(
                "project",
                format!("is required unless {property} is a full projects/... path"),
            )
        })?;
        format!("projects/{project}/{kind}/{name}")
    };
    Ok(full)
}

/// The last part of a full name, for rows and messages.
fn short(resource: &str) -> &str {
    resource.rsplit('/').next().unwrap_or(resource)
}

/// Where requests go: `endpoint`, the emulator's variable, or Google.
fn endpoint(properties: &JsonValue, sources: &Sources) -> Result<String, ConnectorError> {
    let endpoint = match text(properties, "endpoint") {
        Some(endpoint) => endpoint.trim().trim_end_matches('/').to_string(),
        None => match sources.get("PUBSUB_EMULATOR_HOST") {
            Some(host) => format!("http://{}", host.trim().trim_end_matches('/')),
            None => DEFAULT_ENDPOINT.to_string(),
        },
    };
    if host_of(&endpoint).is_none() {
        return Err(ConnectorError::property(
            "endpoint",
            format!("'{endpoint}' is not http://host[:port] or https://host[:port]"),
        ));
    }
    Ok(endpoint)
}

/// What can be refused before any request.
fn check_connection(properties: &JsonValue) -> Result<(), ConnectorError> {
    if let Some(endpoint) = text(properties, "endpoint") {
        if host_of(endpoint.trim().trim_end_matches('/')).is_none() {
            return Err(ConnectorError::property(
                "endpoint",
                format!("'{endpoint}' is not http://host[:port] or https://host[:port]"),
            ));
        }
    }
    positive(properties, "timeout_ms", 30_000)?;
    Ok(())
}

/// A signed-in client for Pub/Sub's REST API.
pub(crate) struct Api {
    client: Client,
    endpoint: String,
    tokens: Tokens,
    timeout: Duration,
    retries: u32,
}

impl Api {
    pub(crate) fn connect(
        properties: &JsonValue,
        sources: &Sources,
    ) -> Result<Self, ConnectorError> {
        let endpoint = endpoint(properties, sources)?;
        // Plain http is an emulator; a token is never sent over it.
        let credentials = if endpoint.starts_with("http://") {
            Credentials::nobody()
        } else {
            gcp::credentials(properties, sources)?
        };
        let timeout = Duration::from_millis(positive(properties, "timeout_ms", 30_000)?);
        let retries = properties
            .get("retries")
            .and_then(JsonValue::as_u64)
            .unwrap_or(5) as u32;
        Ok(Api {
            client: Client::new(Settings::signed_post(endpoint.clone(), timeout, retries)),
            tokens: Tokens::new(credentials, SCOPE, timeout, retries),
            endpoint,
            timeout,
            retries,
        })
    }

    /// Another client to the same place, sharing the token cache, for a
    /// thread of its own: the lease keeper.
    pub(crate) fn duplicate(&self) -> Api {
        Api {
            client: Client::new(Settings::signed_post(
                self.endpoint.clone(),
                self.timeout,
                self.retries,
            )),
            endpoint: self.endpoint.clone(),
            tokens: self.tokens.clone(),
            timeout: self.timeout,
            retries: self.retries,
        }
    }

    /// How the calls sign in, for the report. Never the secret.
    pub(crate) fn signed_in_as(&self) -> &str {
        self.tokens.source()
    }

    /// `POST /v1/<resource>:<verb>`.
    pub(crate) fn call(
        &mut self,
        resource: &str,
        verb: &str,
        body: &JsonValue,
    ) -> Result<JsonValue, ConnectorError> {
        let authorization = self.tokens.authorization()?;
        let bytes =
            serde_json::to_vec(body).map_err(|error| ConnectorError::Data(error.to_string()))?;
        let url = format!("{}/v1/{resource}:{verb}", self.endpoint);
        let headers = || {
            authorization
                .iter()
                .map(|value| ("Authorization".to_string(), value.clone()))
                .collect()
        };
        let extra = Extra {
            headers: &headers,
            content_type: "application/json",
            // Pub/Sub says "slow down" with a 429, which is retried already.
            throttled: &|_, _| false,
        };
        let reply = self
            .client
            .send_with(&url, &[], Some(&bytes), Some(&extra), Judged::Accept)
            .map_err(|error| ConnectorError::Data(format!("Pub/Sub {verb}: {error}")))?;
        if reply.body.trim().is_empty() {
            return Ok(JsonValue::Object(Map::new()));
        }
        serde_json::from_str(&reply.body).map_err(|error| {
            ConnectorError::Data(format!("Pub/Sub {verb}: the answer is not JSON: {error}"))
        })
    }
}

/// `ids` in calls Pub/Sub will take: at most [`IDS_PER_CALL`], and under
/// [`ID_BYTES_PER_CALL`].
fn id_chunks(ids: &[String]) -> Vec<&[String]> {
    let mut chunks = Vec::new();
    let (mut start, mut bytes) = (0, 0);
    for (index, id) in ids.iter().enumerate() {
        let size = id.len() + 3;
        if index > start && (index - start == IDS_PER_CALL || bytes + size > ID_BYTES_PER_CALL) {
            chunks.push(&ids[start..index]);
            (start, bytes) = (index, 0);
        }
        bytes += size;
    }
    if start < ids.len() {
        chunks.push(&ids[start..]);
    }
    chunks
}

/// Set the deadline of every one of `ids`: 0 releases, more extends.
fn modify_deadline(
    api: &mut Api,
    subscription: &str,
    ids: &[String],
    seconds: u64,
) -> Result<usize, ConnectorError> {
    let mut done = 0;
    for chunk in id_chunks(ids) {
        api.call(
            subscription,
            "modifyAckDeadline",
            &json!({ "ackIds": chunk, "ackDeadlineSeconds": seconds }),
        )
        .map_err(|error| {
            ConnectorError::Data(format!("{error} ({done} of {} done before)", ids.len()))
        })?;
        done += chunk.len();
    }
    Ok(done)
}

// ---------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------

impl Source for PubsubSource {
    fn spec(&self) -> ComponentSpec {
        let mut properties = vec![PropertySpec::text("subscription").required().help(
            "The subscription to pull from: its name, or projects/<project>/subscriptions/<name>.",
        )];
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
                .help("The longest one run spends pulling."),
            PropertySpec::integer("ack_deadline_seconds")
                .default(JsonValue::from(60))
                .help(
                    "How long each message is held for at a time. The hold is extended every half \
                     of this while the run goes on. At most 600.",
                ),
            PropertySpec::enumerated("value_format", &["json", "text", "bytes"])
                .default(JsonValue::String("json".into()))
                .help(
                    "json makes each message's fields into columns; text gives a value column; \
                     bytes gives value as base64. Every row also has _subscription, _message_id, \
                     _publish_time, _ordering_key, _attributes and _delivery_attempt.",
                ),
            columns_property(),
        ]);
        ComponentSpec::new("src.queue.pubsub", "Pub/Sub subscription")
            .description(
                "Pull messages from a Google Cloud Pub/Sub subscription in bounded batches. They \
                 are held until the run ends: acknowledged if it succeeded, given back if not.",
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
        _context: &Context,
    ) -> Result<(Summary, Option<Box<dyn Receipt>>), ConnectorError> {
        let settings = SourceSettings::from(properties)?;
        let api = Api::connect(properties, &Sources::process())?;
        let (summary, receipt) = receive(api, &settings, out)?;
        Ok((summary, Some(Box::new(receipt))))
    }
}

#[derive(Debug)]
pub(crate) struct SourceSettings {
    pub(crate) subscription: String,
    pub(crate) max_records: u64,
    pub(crate) max_wait: Duration,
    pub(crate) ack_deadline_seconds: u64,
    pub(crate) format: Format,
}

impl SourceSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let subscription = resource(properties, "subscription", "subscriptions")?;
        check_connection(properties)?;
        let ack_deadline_seconds = positive(properties, "ack_deadline_seconds", 60)?;
        if ack_deadline_seconds > MAX_ACK_DEADLINE {
            return Err(ConnectorError::property(
                "ack_deadline_seconds",
                format!("{ack_deadline_seconds} is more than Pub/Sub's {MAX_ACK_DEADLINE}"),
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
            subscription,
            max_records: positive(properties, "max_records", 10_000)?,
            max_wait: Duration::from_millis(positive(properties, "max_wait_ms", 30_000)?),
            ack_deadline_seconds,
            format,
        })
    }
}

/// Why pulling stopped, for the report.
enum Stop {
    Empty,
    Cap,
    Waited,
}

/// Pull up to `max_records`, writing each message as a row, and return what
/// is held. The receipt exists before the first pull, so anything that fails
/// part-way drops it, which gives the messages back.
pub(crate) fn receive(
    api: Api,
    settings: &SourceSettings,
    out: &mut dyn RecordWriter,
) -> Result<(Summary, PubsubReceipt), ConnectorError> {
    let signed_in = api.signed_in_as().to_string();
    let lease_api = api.duplicate();
    let subscription = settings.subscription.clone();
    let mut held = PubsubReceipt::new(api, &subscription, settings.ack_deadline_seconds, lease_api);
    let name = short(&subscription).to_string();

    let started = Instant::now();
    let stop = loop {
        let got = held.count() as u64;
        if got >= settings.max_records {
            break Stop::Cap;
        }
        if started.elapsed() >= settings.max_wait {
            break Stop::Waited;
        }
        let want = (settings.max_records - got).min(MESSAGES_PER_CALL as u64);
        let answer = held
            .api
            .call(
                &subscription,
                "pull",
                &json!({ "maxMessages": want, "returnImmediately": true }),
            )
            .map_err(|error| {
                ConnectorError::Data(format!("subscription '{subscription}': {error}"))
            })?;
        let messages = answer["receivedMessages"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        if messages.is_empty() {
            break Stop::Empty;
        }

        // Held first, and extended from the subscription's own deadline to
        // ours, before a row is written: one that will not decode is still
        // given back.
        let mut ids = Vec::with_capacity(messages.len());
        for message in &messages {
            let id = message["ackId"].as_str().ok_or_else(|| {
                ConnectorError::Data("Pub/Sub pull: a message without an ackId".into())
            })?;
            ids.push(id.to_string());
        }
        held.hold(&ids);
        modify_deadline(
            &mut held.api,
            &subscription,
            &ids,
            settings.ack_deadline_seconds,
        )?;
        for message in &messages {
            out.write(row(&name, message, settings.format)?)?;
        }
    };

    let count = held.count();
    let mut detail = format!(
        "{count} message(s) from subscription '{name}' ({signed_in}), held until the run ends"
    );
    detail.push_str(&match stop {
        Stop::Empty => "; the subscription answered empty".to_string(),
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

/// One received message as a row.
pub(crate) fn row(
    subscription: &str,
    received: &JsonValue,
    format: Format,
) -> Result<Record, ConnectorError> {
    let message = &received["message"];
    let id = message["messageId"].as_str().unwrap_or_default();
    let data = match message["data"].as_str().filter(|data| !data.is_empty()) {
        Some(data) => Some(base64_decode(data).ok_or_else(|| {
            ConnectorError::Data(format!("message {id}: its data is not base64"))
        })?),
        None => None,
    };
    let value = match (&data, format) {
        (None, Format::Json) => None,
        (None, _) => Some(&[][..]),
        (Some(bytes), _) => Some(bytes.as_slice()),
    };
    let mut row = value_columns(format, value, &format!("message {id}"), &METADATA_COLUMNS)?;

    let text_or_null = |value: &JsonValue| {
        value
            .as_str()
            .filter(|text| !text.is_empty())
            .map_or(JsonValue::Null, |text| json!(text))
    };
    row.insert("_subscription".to_string(), json!(subscription));
    row.insert("_message_id".to_string(), json!(id));
    row.insert(
        "_publish_time".to_string(),
        message["publishTime"]
            .as_str()
            .map_or(JsonValue::Null, |time| json!(publish_time(time))),
    );
    row.insert(
        "_ordering_key".to_string(),
        text_or_null(&message["orderingKey"]),
    );
    row.insert(
        "_attributes".to_string(),
        match &message["attributes"] {
            JsonValue::Object(attributes) => JsonValue::Object(attributes.clone()),
            _ => json!({}),
        },
    );
    // Only a subscription with a dead-letter policy counts deliveries.
    row.insert(
        "_delivery_attempt".to_string(),
        received["deliveryAttempt"]
            .as_i64()
            .map_or(JsonValue::Null, |attempt| json!(attempt)),
    );
    Ok(row)
}

/// RFC 3339 in UTC, `2026-09-24T10:00:00.123456789Z`, as DuckDB reads a
/// timestamp: `2026-09-24 10:00:00.123456`. Microseconds are all a TIMESTAMP
/// keeps.
pub(crate) fn publish_time(text: &str) -> String {
    let Some(utc) = text.strip_suffix('Z') else {
        return text.to_string();
    };
    let utc = utc.replacen('T', " ", 1);
    match utc.split_once('.') {
        Some((seconds, fraction)) => {
            format!("{seconds}.{}", &fraction[..fraction.len().min(6)])
        }
        None => utc,
    }
}

// ---------------------------------------------------------------------------
// The receipt, and the lease keeper
// ---------------------------------------------------------------------------

/// What one run pulled and is holding.
pub(crate) struct PubsubReceipt {
    api: Api,
    subscription: String,
    ids: Arc<Mutex<Vec<String>>>,
    keeper: Option<Keeper>,
    settled: bool,
}

impl PubsubReceipt {
    fn new(api: Api, subscription: &str, deadline_seconds: u64, mut lease_api: Api) -> Self {
        let ids = Arc::new(Mutex::new(Vec::<String>::new()));
        let held = ids.clone();
        let resource = subscription.to_string();
        let keeper = Keeper::start(Duration::from_millis(deadline_seconds * 500), move || {
            let ids = held.lock().unwrap().clone();
            modify_deadline(&mut lease_api, &resource, &ids, deadline_seconds)
                .err()
                .map(|error| error.to_string())
        });
        PubsubReceipt {
            api,
            subscription: subscription.to_string(),
            ids,
            keeper: Some(keeper),
            settled: false,
        }
    }

    fn hold(&mut self, ids: &[String]) {
        self.ids.lock().unwrap().extend_from_slice(ids);
    }

    fn count(&self) -> usize {
        self.ids.lock().unwrap().len()
    }

    /// Acknowledge, or release, everything held.
    fn settle(&mut self, acknowledge: bool) -> Result<String, ConnectorError> {
        self.settled = true;
        let trouble = self.keeper.take().and_then(Keeper::stop);
        let ids = std::mem::take(&mut *self.ids.lock().unwrap());
        let name = short(&self.subscription).to_string();
        let verb = if acknowledge {
            "acknowledged on"
        } else {
            "released back to"
        };
        if ids.is_empty() {
            return Ok(format!("nothing held from subscription '{name}'"));
        }

        let mut done = 0usize;
        for chunk in id_chunks(&ids) {
            let result = if acknowledge {
                self.api
                    .call(
                        &self.subscription,
                        "acknowledge",
                        &json!({ "ackIds": chunk }),
                    )
                    .map(|_| ())
            } else {
                modify_deadline(&mut self.api, &self.subscription, chunk, 0).map(|_| ())
            };
            result.map_err(|error| {
                ConnectorError::Data(format!(
                    "{done} of {} message(s) were {verb} subscription '{name}' before: {error}",
                    ids.len()
                ))
            })?;
            done += chunk.len();
        }

        let mut line = format!("{done} message(s) {verb} subscription '{name}'");
        if let Some(trouble) = trouble {
            line.push_str(&format!(
                "; the hold could not always be extended, so some may have been delivered \
                 elsewhere meanwhile: {trouble}"
            ));
        }
        Ok(line)
    }
}

impl Receipt for PubsubReceipt {
    fn acknowledge(mut self: Box<Self>) -> Result<String, ConnectorError> {
        self.settle(true)
    }

    fn release(mut self: Box<Self>) -> Result<String, ConnectorError> {
        self.settle(false)
    }
}

impl Drop for PubsubReceipt {
    fn drop(&mut self) {
        if !self.settled {
            let _ = self.settle(false);
        }
    }
}

// ---------------------------------------------------------------------------
// The sink
// ---------------------------------------------------------------------------

impl Sink for PubsubSink {
    fn spec(&self) -> ComponentSpec {
        let mut properties = vec![PropertySpec::text("topic")
            .required()
            .help("The topic to publish to: its name, or projects/<project>/topics/<name>.")];
        properties.extend(connection_properties());
        properties.extend([
            PropertySpec::text("ordering_key_column").help(
                "The column whose value is each message's ordering key. A subscription with \
                 message ordering on delivers each key's messages in order. Null: no key.",
            ),
            PropertySpec::text("attributes_column").help(
                "A column holding an object, whose entries become each message's attributes, \
                 as text.",
            ),
        ]);
        ComponentSpec::new("snk.queue.pubsub", "Pub/Sub topic")
            .description(
                "Publish rows to a Google Cloud Pub/Sub topic, one JSON message each, up to 1,000 \
                 to a call.",
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
        _context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = SinkSettings::from(properties)?;
        let mut api = Api::connect(properties, &Sources::process())?;
        publish(&mut api, &settings, input)
    }
}

#[derive(Debug)]
pub(crate) struct SinkSettings {
    pub(crate) topic: String,
    pub(crate) ordering_column: Option<String>,
    pub(crate) attributes_column: Option<String>,
}

impl SinkSettings {
    pub(crate) fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let topic = resource(properties, "topic", "topics")?;
        check_connection(properties)?;
        Ok(SinkSettings {
            topic,
            ordering_column: text(properties, "ordering_key_column").map(str::to_string),
            attributes_column: text(properties, "attributes_column").map(str::to_string),
        })
    }
}

/// One row as a message: its JSON as base64 `data`, a key and attributes if
/// the settings name columns for them, and roughly what it adds to a request.
pub(crate) fn message(
    row: u64,
    record: &Record,
    settings: &SinkSettings,
) -> Result<(JsonValue, usize), ConnectorError> {
    let body =
        serde_json::to_vec(record).map_err(|error| ConnectorError::Data(error.to_string()))?;
    let mut message = json!({ "data": base64_bytes(&body) });

    let column = |property: &str, column: &str| {
        record.get(column).ok_or_else(|| {
            ConnectorError::property(property, format!("'{column}' is not a column of the rows"))
        })
    };
    if let Some(name) = &settings.ordering_column {
        match column("ordering_key_column", name)? {
            JsonValue::Null => {}
            JsonValue::String(key) => message["orderingKey"] = json!(key),
            other => message["orderingKey"] = json!(other.to_string()),
        }
    }
    if let Some(name) = &settings.attributes_column {
        match column("attributes_column", name)? {
            JsonValue::Null => {}
            JsonValue::Object(entries) => {
                let mut attributes = Map::new();
                for (key, value) in entries {
                    let value = match value {
                        JsonValue::Null => continue,
                        JsonValue::String(text) => text.clone(),
                        other => other.to_string(),
                    };
                    attributes.insert(key.clone(), json!(value));
                }
                message["attributes"] = JsonValue::Object(attributes);
            }
            other => {
                return Err(ConnectorError::Data(format!(
                    "row {row}: '{name}' is {}, and attributes_column needs an object",
                    crate::http::kind(other)
                )))
            }
        }
    }

    let size = message.to_string().len();
    if size > PUBLISH_BYTES {
        return Err(ConnectorError::Data(format!(
            "row {row} is {} bytes as JSON, {size} once encoded, and one Pub/Sub publish takes at \
             most {PUBLISH_BYTES} (10 MB)",
            body.len()
        )));
    }
    Ok((message, size))
}

/// What has been published so far, for the summary and a failure's message.
#[derive(Default)]
struct Published {
    messages: u64,
    calls: u64,
}

impl Published {
    fn failed(&self, topic: &str, error: impl std::fmt::Display) -> ConnectorError {
        ConnectorError::Data(format!(
            "{error}. {} message(s) had been published to topic '{topic}' before this, and stay \
             there",
            self.messages
        ))
    }
}

/// Every row of `input` to the topic, a `:publish` at a time.
pub(crate) fn publish(
    api: &mut Api,
    settings: &SinkSettings,
    input: &mut dyn RecordReader,
) -> Result<Summary, ConnectorError> {
    let name = short(&settings.topic).to_string();
    let mut published = Published::default();
    let mut batch: Vec<JsonValue> = Vec::new();
    let mut batch_bytes = 0usize;
    let mut row = 0u64;

    while let Some(record) = input.read()? {
        row += 1;
        let (message, size) = message(row, &record, settings).map_err(|error| match error {
            // A setting that cannot work says so plainly, before anything is sent.
            ConnectorError::Property { .. } if published.messages == 0 => error,
            other => published.failed(&name, other),
        })?;
        if batch.len() == MESSAGES_PER_CALL || batch_bytes + size > PUBLISH_BYTES {
            send(
                api,
                &settings.topic,
                std::mem::take(&mut batch),
                &mut published,
            )
            .map_err(|error| published.failed(&name, error))?;
            batch_bytes = 0;
        }
        batch_bytes += size;
        batch.push(message);
    }
    if !batch.is_empty() {
        send(api, &settings.topic, batch, &mut published)
            .map_err(|error| published.failed(&name, error))?;
    }

    let detail = if published.messages == 0 {
        format!("0 messages; nothing published to topic '{name}'")
    } else {
        format!(
            "{} message(s) in {} call(s) to topic '{name}' ({})",
            published.messages,
            published.calls,
            api.signed_in_as()
        )
    };
    Ok(Summary::new(published.messages, detail))
}

/// One `:publish`. Pub/Sub takes a call whole or not at all; a retried call
/// that had in fact landed publishes its messages twice (at-least-once).
fn send(
    api: &mut Api,
    topic: &str,
    messages: Vec<JsonValue>,
    published: &mut Published,
) -> Result<(), ConnectorError> {
    let count = messages.len();
    let answer = api
        .call(topic, "publish", &json!({ "messages": messages }))
        .map_err(|error| ConnectorError::Data(format!("topic '{}': {error}", short(topic))))?;
    let ids = answer["messageIds"].as_array().map_or(0, Vec::len);
    if ids != count {
        return Err(ConnectorError::Data(format!(
            "Pub/Sub publish: {count} message(s) sent and {ids} message ID(s) back"
        )));
    }
    published.messages += count as u64;
    published.calls += 1;
    Ok(())
}
