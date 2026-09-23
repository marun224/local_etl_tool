//! SaaS GraphQL, both ways.
//!
//! `src.saas.graphql` runs a query and follows its pages; `snk.saas.graphql`
//! runs a mutation once per batch of rows. Both POST `{"query", "variables"}`
//! through the HTTP layer REST uses ([`crate::http`]), so auth, retries on 429
//! and 5xx, pacing and timeouts behave exactly as REST's do.
//!
//! What GraphQL adds is that **an HTTP 200 can be a failure.** A server
//! answers a bad query, a missing field or a permission problem with a 200 and
//! an `errors` array, sometimes beside partial `data`. So:
//!
//! - **Any error fails**, even with `data` present. A page with a hole in it
//!   that loaded as if whole is the partial load `max_pages` exists to prevent.
//! - **Except throttling.** Shopify and GitHub say "slow down" inside a 200,
//!   naming it in `extensions.code` or `type`. When every error is one of
//!   `retry_codes`, the request is retried as a 429 would be, through the same
//!   loop and the same `retries` budget.
//!
//! **Delivery semantics** are written down in `docs/connectors.md`: the source
//! is a snapshot per run and not transactional, and the sink is at-least-once
//! per batch.

use crate::http::{
    connection_properties, kind, page_cap_reached, positive, rows_at, snippet, text, Client,
    Judged, Reply, Settings,
};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordReader, RecordWriter, Sink, Source,
    Summary,
};
use serde_json::{json, Map, Value as JsonValue};

#[cfg(test)]
mod tests;

/// `src.saas.graphql`.
pub struct GraphqlSource;

/// `snk.saas.graphql`.
pub struct GraphqlSink;

/// The throttling codes retried unless the node says otherwise: Shopify's
/// `extensions.code` and GitHub's `type`.
const DEFAULT_RETRY_CODES: [&str; 2] = ["THROTTLED", "RATE_LIMITED"];

/// How many of a response's errors are quoted before the rest are counted.
const ERRORS_QUOTED: usize = 3;

// ---------------------------------------------------------------------------
// The specs
// ---------------------------------------------------------------------------

fn variables_property() -> PropertySpec {
    PropertySpec::code("variables").help(
        "A JSON object of variables sent with every request, e.g. {\"status\": \"shipped\"}. \
         ${...} parameters work inside it.",
    )
}

fn retry_codes_property() -> PropertySpec {
    PropertySpec::string_list("retry_codes")
        .default(json!(DEFAULT_RETRY_CODES))
        .help(
            "Error codes that mean \"slow down\" rather than \"wrong\": when every error in a \
             response has one of these as its extensions.code or type, the request is retried \
             like a 429. Empty retries none.",
        )
}

impl Source for GraphqlSource {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties(None);
        properties.extend([
            PropertySpec::code("query").required().help(
                "The GraphQL query. For relay pagination it declares $after (or cursor_variable) \
                 and asks for pageInfo { hasNextPage endCursor }.",
            ),
            variables_property(),
            PropertySpec::text("records").required().help(
                "Where the rows are in each response, as a JSON pointer such as \
                 /data/orders/nodes. Prefer nodes to edges where the API has both: edges gives \
                 each row a node column instead of its fields.",
            ),
            PropertySpec::enumerated("pagination", &["none", "relay", "offset"])
                .default(JsonValue::String("none".into()))
                .help(
                    "relay follows pageInfo.endCursor until hasNextPage is false; offset counts \
                     up in page_size steps until a short page.",
                ),
            PropertySpec::text("cursor_variable")
                .default(JsonValue::String("after".into()))
                .help("For relay pagination: the variable the cursor is sent in, null at first."),
            PropertySpec::text("page_info").help(
                "For relay pagination: where pageInfo is, as a JSON pointer. Unset means beside \
                 the records: /data/orders/pageInfo for records at /data/orders/nodes.",
            ),
            PropertySpec::text("offset_variable")
                .default(JsonValue::String("offset".into()))
                .help("For offset pagination: the variable holding the offset."),
            PropertySpec::text("limit_variable")
                .default(JsonValue::String("limit".into()))
                .help("For offset pagination: the variable holding page_size."),
            PropertySpec::integer("page_size")
                .default(JsonValue::from(100))
                .help("For offset pagination: rows per page. A shorter page is the last."),
            PropertySpec::integer("max_pages")
                .default(JsonValue::from(1000))
                .help(
                    "A safety cap. Reaching it is an error, not a quiet stop, so a load is never \
                 silently partial.",
                ),
            retry_codes_property(),
            columns_property(),
        ]);

        ComponentSpec::new("src.saas.graphql", "GraphQL API")
            .description("Read records from a GraphQL API, following relay or offset pages.")
            .icon("globe")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        SourceSettings::from(properties).map(|_| ())
    }

    fn read(
        &self,
        properties: &JsonValue,
        out: &mut dyn RecordWriter,
        _context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let SourceSettings {
            http,
            query,
            variables,
            records_at,
            mut paging,
            max_pages,
            retry_codes,
        } = SourceSettings::from(properties)?;

        let mut client = Client::new(http);
        let url = client.settings.url.clone();
        let mut pages = 0u64;
        let mut records = 0u64;

        loop {
            if pages == max_pages {
                return Err(page_cap_reached(max_pages));
            }

            let mut sent = variables.clone();
            paging.bind(&mut sent);
            let body = request(&query, sent)?;

            let page = format!("page {}", pages + 1);
            let document = client.send_judged(&url, &[], Some(&body), |reply| {
                judge(reply, &page, &retry_codes)
            })?;
            pages += 1;

            let items = rows_at(&document, &records_at, pages)?;
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

            if !paging.advance(count, &document, pages)? {
                break;
            }
        }

        Ok(Summary::new(
            records,
            format!("{records} record(s) from {pages} page(s) of {url}"),
        ))
    }
}

impl Sink for GraphqlSink {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties(None);
        properties.extend([
            PropertySpec::code("mutation").required().help(
                "The GraphQL mutation, run once per batch. It declares the rows variable, e.g. \
                 mutation ($rows: [OrderInput!]!) { addOrders(input: $rows) { count } }.",
            ),
            PropertySpec::text("rows_variable")
                .default(JsonValue::String("rows".into()))
                .help("The variable each batch is sent in, always as a list."),
            variables_property(),
            PropertySpec::integer("batch_size")
                .default(JsonValue::from(100))
                .help("Rows per request."),
            retry_codes_property(),
        ]);

        ComponentSpec::new("snk.saas.graphql", "GraphQL API")
            .description("Send rows to a GraphQL API through a mutation, in batches.")
            .icon("globe")
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
        let SinkSettings {
            http,
            mutation,
            rows_variable,
            variables,
            batch_size,
            retry_codes,
        } = SinkSettings::from(properties)?;

        let mut client = Client::new(http);
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

            // Always a list, even a last batch of one: the variable is typed
            // as a list in the mutation, and the shape of a request must not
            // depend on how many rows happened to be left.
            let rows = std::mem::take(&mut batch);
            let count = rows.len() as u64;
            let mut sent = variables.clone();
            sent.insert(
                rows_variable.clone(),
                JsonValue::Array(rows.into_iter().map(JsonValue::Object).collect()),
            );
            let body = request(&mutation, sent)?;

            let what = format!("batch {}", sent_batches + 1);
            client
                .send_judged(&url, &[], Some(&body), |reply| {
                    judge(reply, &what, &retry_codes)
                })
                .map_err(|error| {
                    // At-least-once, per batch: what went before has been
                    // accepted, and saying so is what makes a partial failure
                    // recoverable.
                    ConnectorError::Data(format!(
                        "{what} failed after {sent_batches} batch(es) ({sent_records} record(s)) \
                         were delivered: {error}"
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

        Ok(Summary::new(sent_records, detail))
    }
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

/// Everything a read needs, checked. `check` builds one and throws it away, so
/// validation and the run can never disagree about what is acceptable.
struct SourceSettings {
    http: Settings,
    query: String,
    variables: Map<String, JsonValue>,
    records_at: String,
    paging: Paging,
    max_pages: u64,
    retry_codes: Vec<String>,
}

impl SourceSettings {
    fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let http = Settings::posting(properties)?;
        let query = document(properties, "query")?;
        let variables = variables(properties)?;

        let records_at = text(properties, "records")
            .ok_or_else(|| {
                ConnectorError::property(
                    "records",
                    "is required: say where the rows are, e.g. /data/orders/nodes",
                )
            })?
            .to_string();
        if !records_at.starts_with('/') {
            return Err(ConnectorError::property(
                "records",
                format!("'{records_at}' is not a JSON pointer; it starts with /, e.g. /data/orders/nodes"),
            ));
        }

        let paging = Paging::from(properties, &records_at)?;
        for name in paging.variables() {
            if !declares(&query, name) {
                return Err(ConnectorError::property(
                    "query",
                    format!(
                        "{} pagination sends ${name}, but the query does not declare it; add \
                         ${name} to the operation's variables",
                        paging.name()
                    ),
                ));
            }
            if variables.contains_key(name) {
                return Err(ConnectorError::property(
                    "variables",
                    format!(
                        "sets '{name}', which {} pagination sends itself",
                        paging.name()
                    ),
                ));
            }
        }

        Ok(SourceSettings {
            http,
            query,
            variables,
            records_at,
            paging,
            max_pages: positive(properties, "max_pages", 1000)?,
            retry_codes: retry_codes(properties)?,
        })
    }
}

struct SinkSettings {
    http: Settings,
    mutation: String,
    rows_variable: String,
    variables: Map<String, JsonValue>,
    batch_size: usize,
    retry_codes: Vec<String>,
}

impl SinkSettings {
    fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let http = Settings::posting(properties)?;
        let mutation = document(properties, "mutation")?;
        let variables = variables(properties)?;
        let rows_variable = variable_name(properties, "rows_variable", "rows")?;

        if !declares(&mutation, &rows_variable) {
            return Err(ConnectorError::property(
                "mutation",
                format!(
                    "the rows are sent as ${rows_variable}, but the mutation does not declare it; \
                     add ${rows_variable} to the operation's variables, or set rows_variable"
                ),
            ));
        }
        if variables.contains_key(&rows_variable) {
            return Err(ConnectorError::property(
                "variables",
                format!("sets '{rows_variable}', which is where the rows go"),
            ));
        }

        Ok(SinkSettings {
            http,
            mutation,
            rows_variable,
            variables,
            batch_size: positive(properties, "batch_size", 100)? as usize,
            retry_codes: retry_codes(properties)?,
        })
    }
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum Paging {
    None,
    Relay {
        variable: String,
        page_info: String,
        cursor: Option<String>,
    },
    Offset {
        offset_variable: String,
        limit_variable: String,
        size: u64,
        offset: u64,
    },
}

impl Paging {
    fn from(properties: &JsonValue, records_at: &str) -> Result<Self, ConnectorError> {
        Ok(match text(properties, "pagination").unwrap_or("none") {
            "none" => Paging::None,
            "relay" => Paging::Relay {
                variable: variable_name(properties, "cursor_variable", "after")?,
                page_info: match text(properties, "page_info") {
                    Some(pointer) if pointer.starts_with('/') => pointer.to_string(),
                    Some(other) => {
                        return Err(ConnectorError::property(
                            "page_info",
                            format!("'{other}' is not a JSON pointer; it starts with /"),
                        ))
                    }
                    None => beside(records_at),
                },
                cursor: None,
            },
            "offset" => Paging::Offset {
                offset_variable: variable_name(properties, "offset_variable", "offset")?,
                limit_variable: variable_name(properties, "limit_variable", "limit")?,
                size: positive(properties, "page_size", 100)?,
                offset: 0,
            },
            other => {
                return Err(ConnectorError::property(
                    "pagination",
                    format!("'{other}' is not one of none, relay, offset"),
                ))
            }
        })
    }

    fn name(&self) -> &'static str {
        match self {
            Paging::None => "no",
            Paging::Relay { .. } => "relay",
            Paging::Offset { .. } => "offset",
        }
    }

    /// The variables this style sends, which the query has to declare.
    fn variables(&self) -> Vec<&str> {
        match self {
            Paging::None => Vec::new(),
            Paging::Relay { variable, .. } => vec![variable],
            Paging::Offset {
                offset_variable,
                limit_variable,
                ..
            } => vec![offset_variable, limit_variable],
        }
    }

    /// Set this page's variables.
    fn bind(&self, variables: &mut Map<String, JsonValue>) {
        match self {
            Paging::None => {}
            Paging::Relay {
                variable, cursor, ..
            } => {
                // Null on the first page, which is what a Relay server takes to
                // mean "from the start".
                let value = cursor.clone().map_or(JsonValue::Null, JsonValue::String);
                variables.insert(variable.clone(), value);
            }
            Paging::Offset {
                offset_variable,
                limit_variable,
                size,
                offset,
            } => {
                variables.insert(offset_variable.clone(), JsonValue::from(*offset));
                variables.insert(limit_variable.clone(), JsonValue::from(*size));
            }
        }
    }

    /// Move to the next page; `false` when that was the last.
    fn advance(
        &mut self,
        count: u64,
        document: &JsonValue,
        page: u64,
    ) -> Result<bool, ConnectorError> {
        match self {
            Paging::None => Ok(false),

            Paging::Offset { size, offset, .. } => {
                if count < *size {
                    return Ok(false);
                }
                *offset += count;
                Ok(true)
            }

            Paging::Relay {
                page_info, cursor, ..
            } => {
                let Some(info) = document.pointer(page_info).and_then(JsonValue::as_object) else {
                    return Err(ConnectorError::Data(format!(
                        "page {page}: no pageInfo at '{page_info}'; ask for pageInfo {{ \
                         hasNextPage endCursor }} in the query, or set page_info to where it is"
                    )));
                };

                let Some(has_next) = info.get("hasNextPage").and_then(JsonValue::as_bool) else {
                    return Err(ConnectorError::Data(format!(
                        "page {page}: pageInfo at '{page_info}' has no hasNextPage; ask for it \
                         in the query"
                    )));
                };
                if !has_next {
                    return Ok(false);
                }

                let next = match info.get("endCursor") {
                    Some(JsonValue::String(text)) if !text.is_empty() => text.clone(),
                    _ => {
                        return Err(ConnectorError::Data(format!(
                            "page {page}: hasNextPage is true but endCursor is missing; ask \
                             for endCursor in the query's pageInfo"
                        )))
                    }
                };

                // The same cursor twice is a loop, and max_pages would only
                // catch it a thousand requests later.
                if cursor.as_deref() == Some(next.as_str()) {
                    return Err(ConnectorError::Data(format!(
                        "the API returned the cursor '{next}' twice in a row, which would loop \
                         forever"
                    )));
                }
                *cursor = Some(next);
                Ok(true)
            }
        }
    }
}

/// Where `pageInfo` is when nobody said: beside the records, which is where a
/// Relay connection keeps it for both `nodes` and `edges`.
fn beside(records_at: &str) -> String {
    let parent = records_at.rsplit_once('/').map_or("", |(parent, _)| parent);
    format!("{parent}/pageInfo")
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// What a 2xx really was. `what` names the request for the messages: "page 3"
/// or "batch 3".
fn judge(reply: Reply, what: &str, retry_codes: &[String]) -> Judged<JsonValue> {
    let document: JsonValue = match serde_json::from_str(&reply.body) {
        Ok(document) => document,
        Err(error) => {
            return Judged::Fail(ConnectorError::Data(format!(
                "{what}: the response is not JSON ({error}): {}",
                snippet(&reply.body)
            )))
        }
    };

    let errors = match document.get("errors") {
        None | Some(JsonValue::Null) => &[][..],
        Some(JsonValue::Array(errors)) => errors.as_slice(),
        Some(other) => {
            return Judged::Fail(ConnectorError::Data(format!(
                "{what}: `errors` in the response is {}, not a list",
                kind(other)
            )))
        }
    };

    if !errors.is_empty() {
        let codes: Vec<&str> = errors.iter().filter_map(error_code).collect();
        let throttled = codes.len() == errors.len()
            && codes
                .iter()
                .all(|code| retry_codes.iter().any(|retry| retry == code));
        if throttled {
            let mut codes = codes;
            codes.dedup();
            return Judged::Retry(format!(
                "{what}: the API is throttling ({})",
                codes.join(", ")
            ));
        }
        return Judged::Fail(ConnectorError::Data(format!(
            "{what}: the API answered with {} error(s): {}",
            errors.len(),
            describe(errors)
        )));
    }

    match document.get("data") {
        None | Some(JsonValue::Null) => Judged::Fail(ConnectorError::Data(format!(
            "{what}: the response has neither data nor errors: {}",
            snippet(&reply.body)
        ))),
        Some(_) => Judged::Accept(document),
    }
}

/// An error's code, from `extensions.code` (the spec's convention, and
/// Shopify's) or `type` (GitHub's).
fn error_code(error: &JsonValue) -> Option<&str> {
    error
        .pointer("/extensions/code")
        .and_then(JsonValue::as_str)
        .or_else(|| error.get("type").and_then(JsonValue::as_str))
}

/// The first few errors, each with its code and the path it is about.
fn describe(errors: &[JsonValue]) -> String {
    let mut quoted: Vec<String> = errors
        .iter()
        .take(ERRORS_QUOTED)
        .map(|error| {
            let message = error
                .get("message")
                .and_then(JsonValue::as_str)
                .unwrap_or("(no message)");
            let mut line = format!("\"{message}\"");
            if let Some(code) = error_code(error) {
                line.push_str(&format!(" [{code}]"));
            }
            if let Some(path) = error.get("path").and_then(JsonValue::as_array) {
                let path: Vec<String> = path
                    .iter()
                    .map(|part| match part {
                        JsonValue::String(text) => text.clone(),
                        other => other.to_string(),
                    })
                    .collect();
                line.push_str(&format!(" at {}", path.join(".")));
            }
            line
        })
        .collect();
    if errors.len() > ERRORS_QUOTED {
        quoted.push(format!("and {} more", errors.len() - ERRORS_QUOTED));
    }
    quoted.join("; ")
}

// ---------------------------------------------------------------------------
// Small things
// ---------------------------------------------------------------------------

fn request(document: &str, variables: Map<String, JsonValue>) -> Result<Vec<u8>, ConnectorError> {
    serde_json::to_vec(&json!({ "query": document, "variables": variables }))
        .map_err(|error| ConnectorError::Data(error.to_string()))
}

/// A query or mutation, which has to be there.
fn document(properties: &JsonValue, key: &str) -> Result<String, ConnectorError> {
    text(properties, key)
        .map(str::to_string)
        .ok_or_else(|| ConnectorError::property(key, "is required"))
}

fn variables(properties: &JsonValue) -> Result<Map<String, JsonValue>, ConnectorError> {
    let Some(written) = text(properties, "variables") else {
        return Ok(Map::new());
    };
    match serde_json::from_str::<JsonValue>(written) {
        Ok(JsonValue::Object(map)) => Ok(map),
        Ok(other) => Err(ConnectorError::property(
            "variables",
            format!(
                "is {}, not a JSON object such as {{\"status\": \"shipped\"}}",
                kind(&other)
            ),
        )),
        Err(error) => Err(ConnectorError::property(
            "variables",
            format!("is not JSON: {error}"),
        )),
    }
}

/// A variable's name, as a property: GraphQL's name rules, without the `$`.
fn variable_name(
    properties: &JsonValue,
    key: &str,
    default: &str,
) -> Result<String, ConnectorError> {
    let name = text(properties, key).unwrap_or(default).trim();
    let name = name.strip_prefix('$').unwrap_or(name);
    let valid = name
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && name.chars().all(is_name_char);
    if !valid {
        return Err(ConnectorError::property(
            key,
            format!("'{name}' is not a GraphQL variable name"),
        ));
    }
    Ok(name.to_string())
}

/// Whether `document` mentions `$name` as a whole name. Loose on purpose: it
/// catches the query that never mentions the variable, and cannot reject a
/// valid one. The server is the authority on the rest.
fn declares(document: &str, name: &str) -> bool {
    let needle = format!("${name}");
    document.match_indices(&needle).any(|(at, _)| {
        document[at + needle.len()..]
            .chars()
            .next()
            .is_none_or(|next| !is_name_char(next))
    })
}

fn is_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn retry_codes(properties: &JsonValue) -> Result<Vec<String>, ConnectorError> {
    match properties.get("retry_codes") {
        None | Some(JsonValue::Null) => Ok(DEFAULT_RETRY_CODES.map(String::from).to_vec()),
        Some(JsonValue::Array(codes)) => codes
            .iter()
            .map(|code| {
                code.as_str().map(str::to_string).ok_or_else(|| {
                    ConnectorError::property("retry_codes", "must be a list of codes")
                })
            })
            .collect(),
        Some(_) => Err(ConnectorError::property(
            "retry_codes",
            "must be a list of codes",
        )),
    }
}
