//! SaaS REST, both ways.
//!
//! `src.saas.rest` reads an HTTP JSON API page by page; `snk.saas.rest` sends
//! rows to one in batches. Retries, pacing, timeouts and auth belong to the
//! HTTP layer both share with GraphQL, in [`crate::http`]. What is REST's own:
//!
//! - **Pagination**, in five styles.
//! - **A page cap**, `max_pages`, which is an **error** when reached rather than a
//!   quiet stop. A pagination rule that never terminates would otherwise read
//!   forever, and one that stopped silently at the cap would look like a
//!   complete load.
//! - **Batching** for the sink.
//!
//! **Delivery semantics** are written down in `docs/connectors.md`. In short:
//! the source is a snapshot per run and not transactional -- pages can shift
//! while being read -- and the sink is at-least-once per batch.

use crate::http::{
    connection_properties, kind, page_cap_reached, pairs, positive, rows_at, snippet, text, Client,
    Method, Reply, Settings,
};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordReader, RecordWriter, Sink, Source,
    Summary,
};
use serde_json::{Map, Value as JsonValue};

// What the tests reach through `use super::*`, from before the HTTP layer moved.
#[cfg(test)]
use crate::http::{base64, next_link};
#[cfg(test)]
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// `src.saas.rest`.
pub struct RestSource;

/// `snk.saas.rest`.
pub struct RestSink;

impl Source for RestSource {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties(Some((&["GET", "POST"], "GET")));
        properties.extend([
            PropertySpec::map("query").help("Query parameters sent with every request."),
            PropertySpec::code("body")
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
                return Err(page_cap_reached(max_pages));
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

        Ok(Summary::new(
            records,
            format!(
                "{records} record(s) from {pages} page(s) of {}",
                client.settings.url
            ),
        ))
    }
}

impl Sink for RestSink {
    fn spec(&self) -> ComponentSpec {
        let mut properties = connection_properties(Some((&["POST", "PUT", "PATCH"], "POST")));
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

        Ok(Summary::new(sent_records, detail))
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
