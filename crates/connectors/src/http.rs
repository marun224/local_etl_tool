//! The HTTP layer the web connectors share: REST and GraphQL.
//!
//! [`Client`] owns the parts that are easy to get wrong and expensive to get
//! wrong at 3 am:
//!
//! - **Retries** on 429 and 5xx and on transport failures, with a doubling
//!   backoff, honouring a `Retry-After` given in seconds. **Never** on any other
//!   4xx: a 401 retried three times is still a 401, and a 400 is a request that
//!   will not get better by being sent again. A connector can also ask for a
//!   retry on a 2xx, through [`Client::send_judged`]: GraphQL reports throttling
//!   inside a 200, and it deserves the same patience as a 429.
//! - **Rate limiting** by a minimum interval between requests, and a timeout on
//!   each one.
//!
//! HTTP is `ureq`: blocking, so no async runtime (Settled decision 10), with
//! `rustls`, `ring` (Settled decision 16) and bundled `webpki-roots`
//! certificates, so a built artifact carries its trust store the way it carries
//! its engine. Proxies come from the usual environment variables.

use etl_metadata::PropertySpec;
use etl_plugin_sdk::ConnectorError;
use serde_json::Value as JsonValue;
use std::time::{Duration, Instant};

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

/// Properties every web connector shares: where, who, and how patiently.
///
/// `method` is the choice of methods and the default, for a connector that
/// offers one; `None` leaves the property out, for a protocol that fixes it
/// (GraphQL always POSTs).
pub(crate) fn connection_properties(method: Option<(&[&str], &str)>) -> Vec<PropertySpec> {
    let mut properties = vec![PropertySpec::text("url")
        .required()
        .help("The endpoint, e.g. https://api.example.com/v1/orders.")];
    if let Some((methods, default_method)) = method {
        properties.push(
            PropertySpec::enumerated("method", methods)
                .default(JsonValue::String(default_method.into())),
        );
    }
    properties.extend([
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
    ]);
    properties
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

pub(crate) struct Settings {
    pub(crate) url: String,
    pub(crate) method: Method,
    headers: Vec<(String, String)>,
    auth: Auth,
    timeout: Duration,
    retries: u32,
    backoff: Duration,
    min_interval: Duration,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Method {
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
    pub(crate) fn from(
        properties: &JsonValue,
        default_method: &str,
    ) -> Result<Self, ConnectorError> {
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
        Self::with_method(properties, method)
    }

    /// For a protocol whose method is fixed: always POST, and a `method`
    /// property, which the spec does not offer, is ignored.
    pub(crate) fn posting(properties: &JsonValue) -> Result<Self, ConnectorError> {
        Self::with_method(properties, Method::Post)
    }

    fn with_method(properties: &JsonValue, method: Method) -> Result<Self, ConnectorError> {
        let url = text(properties, "url")
            .ok_or_else(|| ConnectorError::property("url", "is required"))?
            .to_string();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(ConnectorError::property(
                "url",
                "must start with http:// or https://",
            ));
        }

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

// ---------------------------------------------------------------------------
// The HTTP layer
// ---------------------------------------------------------------------------

/// What came back from a request that succeeded.
pub(crate) struct Reply {
    pub(crate) body: String,
    pub(crate) link_next: Option<String>,
    /// A `Retry-After` in seconds, if the server sent one with its success.
    /// Only a [`Judged::Retry`] has a use for it.
    pub(crate) retry_after: Option<Duration>,
}

pub(crate) struct Client {
    agent: ureq::Agent,
    pub(crate) settings: Settings,
    last_request: Option<Instant>,
}

/// What a connector makes of a reply the server called a success.
pub(crate) enum Judged<T> {
    /// It is what it looks like.
    Accept(T),
    /// It is really a "slow down", the way GraphQL reports throttling inside a
    /// 200: retry it as a 429 would be.
    Retry(String),
    /// It is really a failure, and not one that another attempt would fix.
    Fail(ConnectorError),
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
        self.send_judged(url, query, body, Judged::Accept)
    }

    /// [`send`](Self::send), with the connector's own say over a 2xx: accept
    /// it as whatever `judge` makes of it, retry it as if it were a 429, or
    /// fail. The retry shares the one loop, backoff and `retries` budget, and a
    /// `Retry-After` sent with the 2xx is honoured the same way.
    pub(crate) fn send_judged<T>(
        &mut self,
        url: &str,
        query: &[(String, String)],
        body: Option<&[u8]>,
        mut judge: impl FnMut(Reply) -> Judged<T>,
    ) -> Result<T, ConnectorError> {
        let mut wait = self.settings.backoff;
        let mut attempt = 0u32;

        loop {
            self.pace();

            let outcome = self.once(url, query, body).and_then(|reply| {
                let after = reply.retry_after;
                match judge(reply) {
                    Judged::Accept(value) => Ok(value),
                    Judged::Retry(reason) => Err(Attempt::Retry { reason, after }),
                    Judged::Fail(error) => Err(Attempt::Fatal(error)),
                }
            });
            attempt += 1;

            let (reason, after) = match outcome {
                Ok(value) => return Ok(value),
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
                retry_after,
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

/// Reaching `max_pages` with more to read. An error, not a quiet stop: a
/// pagination rule that never ends would otherwise read forever, and one that
/// stopped silently at the cap would look like a complete load.
pub(crate) fn page_cap_reached(max_pages: u64) -> ConnectorError {
    ConnectorError::Data(format!(
        "reached max_pages ({max_pages}) with more still to read; raise max_pages if the API \
         really has that many pages, or check the pagination settings"
    ))
}

/// The array of rows at `pointer` in page `page`'s response.
pub(crate) fn rows_at<'a>(
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

pub(crate) fn snippet(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "(empty body)".to_string();
    }
    match trimmed.char_indices().nth(BODY_SNIPPET) {
        Some((cut, _)) => format!("{}…", &trimmed[..cut]),
        None => trimmed.to_string(),
    }
}

pub(crate) fn kind(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "a boolean",
        JsonValue::Number(_) => "a number",
        JsonValue::String(_) => "a string",
        JsonValue::Array(_) => "an array",
        JsonValue::Object(_) => "an object",
    }
}

pub(crate) fn text<'a>(properties: &'a JsonValue, key: &str) -> Option<&'a str> {
    properties
        .get(key)
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
}

pub(crate) fn needed<'a>(
    properties: &'a JsonValue,
    key: &str,
    auth: &str,
) -> Result<&'a str, ConnectorError> {
    text(properties, key)
        .ok_or_else(|| ConnectorError::property(key, format!("is required for {auth} auth")))
}

pub(crate) fn pairs(
    properties: &JsonValue,
    key: &str,
) -> Result<Vec<(String, String)>, ConnectorError> {
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

pub(crate) fn whole(
    properties: &JsonValue,
    key: &str,
    default: u64,
) -> Result<u64, ConnectorError> {
    match properties.get(key) {
        None | Some(JsonValue::Null) => Ok(default),
        Some(value) => value
            .as_u64()
            .ok_or_else(|| ConnectorError::property(key, "must be zero or more")),
    }
}

pub(crate) fn positive(
    properties: &JsonValue,
    key: &str,
    default: u64,
) -> Result<u64, ConnectorError> {
    match whole(properties, key, default)? {
        0 => Err(ConnectorError::property(key, "must be at least 1")),
        value => Ok(value),
    }
}

/// Standard base64 with padding, for basic auth. Twenty lines rather than a
/// dependency, the same call as the hex in `etl-secrets`.
pub(crate) fn base64(input: &str) -> String {
    base64_bytes(input.as_bytes())
}

/// [`base64`], for bytes that are not text: a Kafka value in `bytes` format.
pub(crate) fn base64_bytes(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

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
