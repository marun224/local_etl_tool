//! SaaS REST, both ways.
//!
//! `src.saas.rest` reads an HTTP JSON API page by page; `snk.saas.rest` sends
//! rows to one in batches. Both share one HTTP layer, [`Client`], which owns the
//! parts that are easy to get wrong and expensive to get wrong at 3 am:
//!
//! - **Retries** on 429 and 5xx and on transport failures, with a doubling
//!   backoff, honouring a `Retry-After` given in seconds. **Never** on any other
//!   4xx: a 401 retried three times is still a 401, and a 400 is a request that
//!   will not get better by being sent again.
//! - **A page cap**, `max_pages`, which is an **error** when reached rather than a
//!   quiet stop. A pagination rule that never terminates would otherwise read
//!   forever, and one that stopped silently at the cap would look like a
//!   complete load.
//! - **Rate limiting** by a minimum interval between requests, and a timeout on
//!   each one.
//!
//! HTTP is `ureq`: blocking, so no async runtime (Settled decision 10), with
//! `rustls`, `ring` (Settled decision 16) and bundled `webpki-roots`
//! certificates, so a built artifact carries its trust store the way it carries
//! its engine. Proxies come from the usual environment variables.
//!
//! **Delivery semantics** are written down in `docs/connectors.md`. In short:
//! the source is a snapshot per run and not transactional -- pages can shift
//! while being read -- and the sink is at-least-once per batch.

use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordReader, RecordWriter, Sink, Source,
    Summary,
};
use serde_json::{Map, Value as JsonValue};
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// `src.saas.rest`.
pub struct RestSource;

/// `snk.saas.rest`.
pub struct RestSink;

/// A server asking for longer than this is refused rather than obeyed: a run
/// that sleeps for an hour because a header said so is a run that looks hung.
pub(crate) const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);

/// How much of a failing response's body goes into the error.
const BODY_SNIPPET: usize = 300;

/// The most a single response body may hold. Generous; an API page larger
/// than this is a pagination setting to fix, not a page to read.
const MAX_BODY: u64 = 256 * 1024 * 1024;

// ---------------------------------------------------------------------------
// The specs
// ---------------------------------------------------------------------------

/// Properties both directions share: where, who, and how patiently.
fn connection_properties(methods: &[&str], default_method: &str) -> Vec<PropertySpec> {
    vec![
        PropertySpec::text("url")
            .required()
            .help("The endpoint, e.g. https://api.example.com/v1/orders."),
        PropertySpec::enumerated("method", methods)
            .default(JsonValue::String(default_method.into())),
        PropertySpec::map("headers").help("Extra request headers, name to value."),
        PropertySpec::enumerated("auth", &["none", "bearer", "basic", "header"])
            .default(JsonValue::String("none".into()))
            .help(
                "bearer sends Authorization: Bearer <token>; header sends <auth_header>: <token>; \
                 basic uses username and password.",
            ),
        PropertySpec::text("token")
            .help("For bearer or header auth. Use ${SECRET:name} rather than the value itself."),
        PropertySpec::text("auth_header")
            .default(JsonValue::String("X-API-Key".into()))
            .help("The header name for header auth."),
        PropertySpec::text("username").help("For basic auth."),
        PropertySpec::text("password")
            .help("For basic auth. Use ${SECRET:name} rather than the value itself."),
        PropertySpec::integer("timeout_ms")
            .default(JsonValue::from(30_000))
            .help("How long one request may take."),
        PropertySpec::integer("retries")
            .default(JsonValue::from(3))
            .help("Extra attempts after a 429, a 5xx or a network failure. Never for other 4xx."),
        PropertySpec::integer("retry_backoff_ms")
            .default(JsonValue::from(500))
            .help("The first wait before a retry, doubling each time. Retry-After wins."),
        PropertySpec::integer("min_interval_ms")
            .default(JsonValue::from(0))
            .help("The least time between two requests, for APIs with a rate limit."),
    ]
}

impl Source for RestSource {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties(&["GET", "POST"], "GET");
        properties.extend([
            PropertySpec::map("query").help("Query parameters sent with every request."),
            PropertySpec::text("body")
                .help("A JSON request body, for APIs that take a search as a POST."),
            PropertySpec::text("records").default(JsonValue::String(String::new())).help(
                "Where the rows are in each response, as a JSON pointer such as /data. Empty \
                 means the response is itself the array.",
            ),
            PropertySpec::enumerated(
                "pagination",
                &["none", "page", "offset", "cursor", "link"],
            )
            .default(JsonValue::String("none".into()))
            .help(
                "page and offset count up until a short page; cursor follows cursor_path; link \
                 follows the Link: rel=\"next\" header.",
            ),
            PropertySpec::text("page_param")
                .default(JsonValue::String("page".into()))
                .help("For page pagination: the query parameter holding the page number."),
            PropertySpec::integer("page_start")
                .default(JsonValue::from(1))
                .help("For page pagination: the first page's number."),
            PropertySpec::text("offset_param")
                .default(JsonValue::String("offset".into()))
                .help("For offset pagination: the query parameter holding the offset."),
            PropertySpec::text("size_param").help(
                "The query parameter holding the page size, e.g. per_page or limit. Offset \
                 pagination defaults it to limit.",
            ),
            PropertySpec::integer("page_size").help(
                "Rows per page. Sent as size_param, and a page shorter than this is the last. \
                 Offset pagination defaults it to 100.",
            ),
            PropertySpec::text("cursor_path").help(
                "For cursor pagination: where the next cursor is in each response, as a JSON \
                 pointer such as /meta/next_cursor. Missing, null or empty ends the read.",
            ),
            PropertySpec::text("cursor_param")
                .default(JsonValue::String("cursor".into()))
                .help("For cursor pagination: the query parameter the cursor is sent in."),
            PropertySpec::integer("max_pages").default(JsonValue::from(1000)).help(
                "A safety cap. Reaching it is an error, not a quiet stop, so a load is never \
                 silently partial.",
            ),
            columns_property(),
        ]);

        ComponentSpec::new("src.saas.rest", "REST API")
            .description("Read JSON records from an HTTP API, following its pagination.")
            .icon("globe")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        Settings::from(properties, "GET")?;
        Pagination::from(properties)?;
        if let Some(body) = text(properties, "body") {
            serde_json::from_str::<JsonValue>(body).map_err(|error| {
                ConnectorError::property("body", format!("is not JSON: {error}"))
            })?;
        }
        Ok(())
    }

    fn read(
        &self,
        properties: &JsonValue,
        out: &mut dyn RecordWriter,
        _context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = Settings::from(properties, "GET")?;
        let mut pagination = Pagination::from(properties)?;
        let records_at = text(properties, "records").unwrap_or("");
        let max_pages = positive(properties, "max_pages", 1000)?;
        let base_query = pairs(properties, "query")?;
        let body = text(properties, "body").map(str::to_string);

        let mut client = Client::new(settings);
        let mut url = client.settings.url.clone();
        let mut pages = 0u64;
        let mut records = 0u64;

        loop {
            if pages == max_pages {
                return Err(ConnectorError::Data(format!(
                    "reached max_pages ({max_pages}) with more still to read; raise max_pages if \
                     the API really has that many pages, or check the pagination settings"
                )));
            }

            let mut query = base_query.clone();
            query.extend(pagination.query());

            let reply = client.send(&url, &query, body.as_deref().map(str::as_bytes))?;
            pages += 1;

            let document: JsonValue = serde_json::from_str(&reply.body).map_err(|error| {
                ConnectorError::Data(format!(
                    "page {pages} is not JSON ({error}): {}",
                    snippet(&reply.body)
                ))
            })?;

            let items = rows_at(&document, records_at, pages)?;
            let count = items.len() as u64;

            for (index, item) in items.iter().enumerate() {
                match item {
                    JsonValue::Object(record) => out.write(record.clone())?,
                    other => {
                        return Err(ConnectorError::Data(format!(
                            "row {} on page {pages} is {}, not an object; point `records` at \
                             the array of objects",
                            index + 1,
                            kind(other)
                        )))
                    }
                }
            }
            records += count;

            match pagination.advance(count, &document, &reply, &url)? {
                Some(next) => url = next,
                None => break,
            }
        }

        Ok(Summary {
            records,
            detail: format!(
                "{records} record(s) from {pages} page(s) of {}",
                client.settings.url
            ),
        })
    }
}

impl Sink for RestSink {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties(&["POST", "PUT", "PATCH"], "POST");
        properties.extend([
            PropertySpec::integer("batch_size")
                .default(JsonValue::from(100))
                .help(
                    "Rows per request. 1 sends each row as a JSON object; more sends a JSON array.",
                ),
            PropertySpec::text("wrap").help(
                "Nest each request body under this key, e.g. records sends {\"records\": [...]}.",
            ),
        ]);

        ComponentSpec::new("snk.saas.rest", "REST API")
            .description("Send rows to an HTTP API as JSON, in batches.")
            .icon("globe")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        sink_settings(properties)?;
        positive(properties, "batch_size", 100)?;
        Ok(())
    }

    fn write(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        _context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = sink_settings(properties)?;
        let batch_size = positive(properties, "batch_size", 100)? as usize;
        let wrap = text(properties, "wrap").map(str::to_string);

        let mut client = Client::new(settings);
        let url = client.settings.url.clone();

        let mut batch: Vec<Record> = Vec::with_capacity(batch_size);
        let mut sent_batches = 0u64;
        let mut sent_records = 0u64;
        let mut exhausted = false;

        while !exhausted {
            match input.read()? {
                Some(record) => batch.push(record),
                None => exhausted = true,
            }

            let full = batch.len() == batch_size;
            if batch.is_empty() || !(full || exhausted) {
                continue;
            }

            let body = batch_body(std::mem::take(&mut batch), batch_size);
            let count = match &body {
                Batch::One(_) => 1,
                Batch::Many(rows) => rows.len() as u64,
            };
            let bytes = serde_json::to_vec(&body.into_json(wrap.as_deref()))
                .map_err(|error| ConnectorError::Data(error.to_string()))?;

            client.send(&url, &[], Some(&bytes)).map_err(|error| {
                // At-least-once, per batch: what went before has been accepted,
                // and saying so is what makes a partial failure recoverable.
                ConnectorError::Data(format!(
                    "batch {} failed after {sent_batches} batch(es) ({sent_records} record(s)) \
                     were delivered: {error}",
                    sent_batches + 1
                ))
            })?;

            sent_batches += 1;
            sent_records += count;
        }

        let detail = if sent_batches == 0 {
            format!("0 records; nothing sent to {url}")
        } else {
            format!("{sent_records} record(s) in {sent_batches} request(s) to {url}")
        };

        Ok(Summary {
            records: sent_records,
            detail,
        })
    }
}

/// A sink's settings: POST unless told otherwise, and never GET, whose request
/// has no body to carry the rows in.
fn sink_settings(properties: &JsonValue) -> Result<Settings, ConnectorError> {
    let settings = Settings::from(properties, "POST")?;
    if settings.method == Method::Get {
        return Err(ConnectorError::property(
            "method",
            "GET cannot send rows; use POST, PUT or PATCH",
        ));
    }
    Ok(settings)
}

/// One request's worth of rows.
enum Batch {
    One(Record),
    Many(Vec<Record>),
}

impl Batch {
    fn into_json(self, wrap: Option<&str>) -> JsonValue {
        let body = match self {
            Batch::One(record) => JsonValue::Object(record),
            Batch::Many(rows) => {
                JsonValue::Array(rows.into_iter().map(JsonValue::Object).collect())
            }
        };
        match wrap {
            Some(key) => {
                let mut outer = Map::new();
                outer.insert(key.to_string(), body);
                JsonValue::Object(outer)
            }
            None => body,
        }
    }
}

/// A batch size of one sends a bare object, which is what an API taking one
/// record per call expects. Anything larger always sends an array, even for a
/// last batch of one, so the shape of a request never depends on the row count.
fn batch_body(mut rows: Vec<Record>, batch_size: usize) -> Batch {
    if batch_size == 1 && rows.len() == 1 {
        Batch::One(rows.remove(0))
    } else {
        Batch::Many(rows)
    }
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

pub(crate) struct Settings {
    pub(crate) url: String,
    method: Method,
    headers: Vec<(String, String)>,
    auth: Auth,
    timeout: Duration,
    retries: u32,
    backoff: Duration,
    min_interval: Duration,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Method {
    Get,
    Post,
    Put,
    Patch,
}

enum Auth {
    None,
    /// A header name and its value, whichever kind of auth produced it.
    Header(String, String),
}

impl Settings {
    /// `default_method` is the direction's: GET to read, POST to write. It is
    /// passed in rather than assumed, because a sink that fell back to GET would
    /// send its rows nowhere -- a GET has no body -- and report success. The
    /// first draft did exactly that when called without the spec's defaults.
    fn from(properties: &JsonValue, default_method: &str) -> Result<Self, ConnectorError> {
        let url = text(properties, "url")
            .ok_or_else(|| ConnectorError::property("url", "is required"))?
            .to_string();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(ConnectorError::property(
                "url",
                "must start with http:// or https://",
            ));
        }

        let method = match text(properties, "method").unwrap_or(default_method) {
            "GET" => Method::Get,
            "POST" => Method::Post,
            "PUT" => Method::Put,
            "PATCH" => Method::Patch,
            other => {
                return Err(ConnectorError::property(
                    "method",
                    format!("'{other}' is not supported"),
                ))
            }
        };

        let auth = match text(properties, "auth").unwrap_or("none") {
            "none" => Auth::None,
            "bearer" => Auth::Header(
                "Authorization".to_string(),
                format!("Bearer {}", needed(properties, "token", "bearer")?),
            ),
            "header" => Auth::Header(
                text(properties, "auth_header")
                    .unwrap_or("X-API-Key")
                    .to_string(),
                needed(properties, "token", "header")?.to_string(),
            ),
            "basic" => {
                let username = needed(properties, "username", "basic")?;
                let password = text(properties, "password").unwrap_or("");
                Auth::Header(
                    "Authorization".to_string(),
                    format!("Basic {}", base64(&format!("{username}:{password}"))),
                )
            }
            other => {
                return Err(ConnectorError::property(
                    "auth",
                    format!("'{other}' is not one of none, bearer, basic, header"),
                ))
            }
        };

        Ok(Settings {
            url,
            method,
            headers: pairs(properties, "headers")?,
            auth,
            timeout: Duration::from_millis(positive(properties, "timeout_ms", 30_000)?),
            retries: whole(properties, "retries", 3)? as u32,
            backoff: Duration::from_millis(whole(properties, "retry_backoff_ms", 500)?),
            min_interval: Duration::from_millis(whole(properties, "min_interval_ms", 0)?),
        })
    }
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum Pagination {
    None,
    Page {
        param: String,
        number: i64,
        size: Option<(Option<String>, u64)>,
    },
    Offset {
        param: String,
        offset: u64,
        size_param: String,
        size: u64,
    },
    Cursor {
        path: String,
        param: String,
        current: Option<String>,
    },
    Link,
}

impl Pagination {
    fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let size = match properties.get("page_size") {
            None | Some(JsonValue::Null) => None,
            Some(_) => Some(positive(properties, "page_size", 100)?),
        };

        Ok(match text(properties, "pagination").unwrap_or("none") {
            "none" => Pagination::None,
            "page" => Pagination::Page {
                param: text(properties, "page_param").unwrap_or("page").to_string(),
                number: properties
                    .get("page_start")
                    .and_then(JsonValue::as_i64)
                    .unwrap_or(1),
                size: size.map(|size| (text(properties, "size_param").map(str::to_string), size)),
            },
            "offset" => Pagination::Offset {
                param: text(properties, "offset_param")
                    .unwrap_or("offset")
                    .to_string(),
                offset: 0,
                size_param: text(properties, "size_param")
                    .unwrap_or("limit")
                    .to_string(),
                size: size.unwrap_or(100),
            },
            "cursor" => Pagination::Cursor {
                path: text(properties, "cursor_path")
                    .ok_or_else(|| {
                        ConnectorError::property(
                            "cursor_path",
                            "is required for cursor pagination: say where each response holds \
                             the next cursor, e.g. /meta/next_cursor",
                        )
                    })?
                    .to_string(),
                param: text(properties, "cursor_param")
                    .unwrap_or("cursor")
                    .to_string(),
                current: None,
            },
            "link" => Pagination::Link,
            other => {
                return Err(ConnectorError::property(
                    "pagination",
                    format!("'{other}' is not one of none, page, offset, cursor, link"),
                ))
            }
        })
    }

    /// The query parameters for the page about to be requested.
    fn query(&self) -> Vec<(String, String)> {
        match self {
            Pagination::None | Pagination::Link => Vec::new(),
            Pagination::Page {
                param,
                number,
                size,
            } => {
                let mut query = vec![(param.clone(), number.to_string())];
                if let Some((Some(size_param), size)) = size {
                    query.push((size_param.clone(), size.to_string()));
                }
                query
            }
            Pagination::Offset {
                param,
                offset,
                size_param,
                size,
            } => vec![
                (param.clone(), offset.to_string()),
                (size_param.clone(), size.to_string()),
            ],
            Pagination::Cursor { param, current, .. } => current
                .as_ref()
                .map(|cursor| vec![(param.clone(), cursor.clone())])
                .unwrap_or_default(),
        }
    }

    /// Move to the next page, or `None` when that was the last. Returns the URL
    /// to request next, which only link pagination changes.
    fn advance(
        &mut self,
        count: u64,
        document: &JsonValue,
        reply: &Reply,
        url: &str,
    ) -> Result<Option<String>, ConnectorError> {
        match self {
            Pagination::None => Ok(None),

            Pagination::Page { number, size, .. } => {
                let short = match size {
                    Some((_, size)) => count < *size,
                    None => count == 0,
                };
                if short {
                    return Ok(None);
                }
                *number += 1;
                Ok(Some(url.to_string()))
            }

            Pagination::Offset { offset, size, .. } => {
                if count < *size {
                    return Ok(None);
                }
                *offset += count;
                Ok(Some(url.to_string()))
            }

            Pagination::Cursor { path, current, .. } => {
                let next = match document.pointer(path) {
                    Some(JsonValue::String(text)) if !text.is_empty() => text.clone(),
                    Some(JsonValue::Number(number)) => number.to_string(),
                    _ => return Ok(None),
                };
                // The same cursor twice is a loop, and max_pages would only
                // catch it a thousand requests later.
                if current.as_deref() == Some(next.as_str()) {
                    return Err(ConnectorError::Data(format!(
                        "the API returned the cursor '{next}' twice in a row, which would loop \
                         forever"
                    )));
                }
                *current = Some(next);
                Ok(Some(url.to_string()))
            }

            Pagination::Link => Ok(reply
                .link_next
                .as_deref()
                .map(|next| resolve_link(url, next))),
        }
    }
}

/// The URL a `rel="next"` link names, if the header has one.
pub(crate) fn next_link(header: &str) -> Option<String> {
    header.split(',').find_map(|part| {
        let (target, parameters) = part.trim().split_once('>')?;
        let target = target.trim().strip_prefix('<')?;

        let is_next = parameters.split(';').any(|parameter| {
            let Some((name, value)) = parameter.trim().split_once('=') else {
                return false;
            };
            name.trim().eq_ignore_ascii_case("rel")
                && value
                    .trim()
                    .trim_matches('"')
                    .split_whitespace()
                    .any(|rel| rel.eq_ignore_ascii_case("next"))
        });

        is_next.then(|| target.to_string())
    })
}

/// A link target against the URL it came from. Absolute stays absolute; a
/// path starting with `/` keeps the scheme and host. Anything else is taken as
/// given, because resolving relative paths properly is a URL library's job
/// and no API seen so far needs it.
pub(crate) fn resolve_link(current: &str, target: &str) -> String {
    if target.starts_with("http://") || target.starts_with("https://") {
        return target.to_string();
    }
    if let Some(path) = target.strip_prefix('/') {
        if let Some(scheme_end) = current.find("://") {
            let authority_start = scheme_end + 3;
            let authority_end = current[authority_start..]
                .find('/')
                .map(|index| authority_start + index)
                .unwrap_or(current.len());
            return format!("{}/{path}", &current[..authority_end]);
        }
    }
    target.to_string()
}

fn rows_at<'a>(
    document: &'a JsonValue,
    pointer: &str,
    page: u64,
) -> Result<&'a Vec<JsonValue>, ConnectorError> {
    match document.pointer(pointer) {
        Some(JsonValue::Array(items)) => Ok(items),
        Some(other) => Err(ConnectorError::Data(format!(
            "page {page}: `records` ('{pointer}') points at {}, not an array",
            kind(other)
        ))),
        None => Err(ConnectorError::Data(format!(
            "page {page}: nothing at `records` ('{pointer}') in the response"
        ))),
    }
}

// ---------------------------------------------------------------------------
// The HTTP layer
// ---------------------------------------------------------------------------

/// What came back from a request that succeeded.
pub(crate) struct Reply {
    pub(crate) body: String,
    pub(crate) link_next: Option<String>,
}

pub(crate) struct Client {
    agent: ureq::Agent,
    pub(crate) settings: Settings,
    last_request: Option<Instant>,
}

/// Whether a failed attempt is worth another.
enum Attempt {
    /// A 429, a 5xx, or the network: try again, after this long if the server
    /// said how long.
    Retry {
        reason: String,
        after: Option<Duration>,
    },
    /// Anything else: stop.
    Fatal(ConnectorError),
}

impl Client {
    pub(crate) fn new(settings: Settings) -> Self {
        let agent = ureq::Agent::config_builder()
            // Statuses are decided here, not by the library: a 429 has to be
            // readable to be retried, and a 401's body is the explanation.
            .http_status_as_error(false)
            .timeout_global(Some(settings.timeout))
            .user_agent(concat!("etl/", env!("CARGO_PKG_VERSION")))
            .build()
            .new_agent();

        Client {
            agent,
            settings,
            last_request: None,
        }
    }

    /// Send one request, retrying what is worth retrying.
    pub(crate) fn send(
        &mut self,
        url: &str,
        query: &[(String, String)],
        body: Option<&[u8]>,
    ) -> Result<Reply, ConnectorError> {
        let mut wait = self.settings.backoff;
        let mut attempt = 0u32;

        loop {
            self.pace();

            let outcome = self.once(url, query, body);
            attempt += 1;

            let (reason, after) = match outcome {
                Ok(reply) => return Ok(reply),
                Err(Attempt::Fatal(error)) => return Err(error),
                Err(Attempt::Retry { reason, after }) => (reason, after),
            };

            if attempt > self.settings.retries {
                let tries = attempt;
                return Err(ConnectorError::Data(format!(
                    "{reason}, after {tries} attempt(s)"
                )));
            }

            let pause = match after {
                Some(after) if after > MAX_RETRY_AFTER => {
                    return Err(ConnectorError::Data(format!(
                        "{reason}, and the server asked to wait {}s before retrying, which is more \
                         than the {}s this will wait",
                        after.as_secs(),
                        MAX_RETRY_AFTER.as_secs()
                    )))
                }
                Some(after) => after,
                None => wait,
            };

            std::thread::sleep(pause);
            wait = wait.saturating_mul(2);
        }
    }

    /// Hold back until `min_interval` has passed since the last request.
    fn pace(&mut self) {
        if let Some(last) = self.last_request {
            let since = last.elapsed();
            if since < self.settings.min_interval {
                std::thread::sleep(self.settings.min_interval - since);
            }
        }
        self.last_request = Some(Instant::now());
    }

    fn once(
        &self,
        url: &str,
        query: &[(String, String)],
        body: Option<&[u8]>,
    ) -> Result<Reply, Attempt> {
        let settings = &self.settings;

        macro_rules! prepared {
            ($builder:expr) => {{
                let mut builder = $builder.header("Accept", "application/json");
                for (name, value) in &settings.headers {
                    builder = builder.header(name.as_str(), value.as_str());
                }
                if let Auth::Header(name, value) = &settings.auth {
                    builder = builder.header(name.as_str(), value.as_str());
                }
                for (name, value) in query {
                    builder = builder.query(name.as_str(), value.as_str());
                }
                builder
            }};
        }

        let sent = match (settings.method, body) {
            (Method::Get, _) => prepared!(self.agent.get(url)).call(),
            (method, body) => {
                let builder = match method {
                    Method::Post => self.agent.post(url),
                    Method::Put => self.agent.put(url),
                    _ => self.agent.patch(url),
                };
                let builder = prepared!(builder).header("Content-Type", "application/json");
                match body {
                    Some(bytes) => builder.send(bytes),
                    None => builder.send_empty(),
                }
            }
        };

        let mut response = match sent {
            Ok(response) => response,
            Err(error) => return Err(transport(error, url)),
        };

        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        let link_next = response
            .headers()
            .get("link")
            .and_then(|value| value.to_str().ok())
            .and_then(next_link);

        let text = response
            .body_mut()
            .with_config()
            .limit(MAX_BODY)
            .read_to_string()
            .map_err(|error| Attempt::Retry {
                reason: format!("reading the response from {url} failed: {error}"),
                after: None,
            })?;

        match status {
            200..=299 => Ok(Reply {
                body: text,
                link_next,
            }),
            429 | 500..=599 => Err(Attempt::Retry {
                reason: format!("HTTP {status} from {url}: {}", snippet(&text)),
                after: retry_after,
            }),
            _ => Err(Attempt::Fatal(ConnectorError::Data(format!(
                "HTTP {status} from {url}: {}",
                snippet(&text)
            )))),
        }
    }
}

/// A failure below HTTP. Most are worth another try; a URL that will not parse
/// never is.
fn transport(error: ureq::Error, url: &str) -> Attempt {
    match error {
        ureq::Error::BadUri(reason) => Attempt::Fatal(ConnectorError::property(
            "url",
            format!("'{url}' is not a usable URL: {reason}"),
        )),
        ureq::Error::Http(reason) => Attempt::Fatal(ConnectorError::property(
            "headers",
            format!("could not build the request: {reason}"),
        )),
        other => Attempt::Retry {
            reason: format!("could not reach {url}: {other}"),
            after: None,
        },
    }
}

// ---------------------------------------------------------------------------
// Small things
// ---------------------------------------------------------------------------

fn snippet(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "(empty body)".to_string();
    }
    match trimmed.char_indices().nth(BODY_SNIPPET) {
        Some((cut, _)) => format!("{}…", &trimmed[..cut]),
        None => trimmed.to_string(),
    }
}

fn kind(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "a boolean",
        JsonValue::Number(_) => "a number",
        JsonValue::String(_) => "a string",
        JsonValue::Array(_) => "an array",
        JsonValue::Object(_) => "an object",
    }
}

fn text<'a>(properties: &'a JsonValue, key: &str) -> Option<&'a str> {
    properties
        .get(key)
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn needed<'a>(properties: &'a JsonValue, key: &str, auth: &str) -> Result<&'a str, ConnectorError> {
    text(properties, key)
        .ok_or_else(|| ConnectorError::property(key, format!("is required for {auth} auth")))
}

fn pairs(properties: &JsonValue, key: &str) -> Result<Vec<(String, String)>, ConnectorError> {
    match properties.get(key) {
        None | Some(JsonValue::Null) => Ok(Vec::new()),
        Some(JsonValue::Object(map)) => map
            .iter()
            .map(|(name, value)| match value {
                JsonValue::String(text) => Ok((name.clone(), text.clone())),
                _ => Err(ConnectorError::property(
                    key,
                    format!("the value for '{name}' must be text"),
                )),
            })
            .collect(),
        Some(_) => Err(ConnectorError::property(key, "must be name/value pairs")),
    }
}

fn whole(properties: &JsonValue, key: &str, default: u64) -> Result<u64, ConnectorError> {
    match properties.get(key) {
        None | Some(JsonValue::Null) => Ok(default),
        Some(value) => value
            .as_u64()
            .ok_or_else(|| ConnectorError::property(key, "must be zero or more")),
    }
}

fn positive(properties: &JsonValue, key: &str, default: u64) -> Result<u64, ConnectorError> {
    match whole(properties, key, default)? {
        0 => Err(ConnectorError::property(key, "must be at least 1")),
        value => Ok(value),
    }
}

/// Standard base64 with padding, for basic auth. Twenty lines rather than a
/// dependency, the same call as the hex in `etl-secrets`.
pub(crate) fn base64(input: &str) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let bytes = input.as_bytes();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);

    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);

        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }

    out
}
