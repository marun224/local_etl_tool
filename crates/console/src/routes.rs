//! What each request means, as a pure function.
//!
//! Nothing here touches a socket or knows what `tiny_http` is. A request is a
//! method, a path, a query and a token; a response is a status, a content type
//! and a body. That is what lets every route — including every way of being
//! refused — be tested without binding a port.
//!
//! # The routes
//!
//! | Method | Path | Role |
//! |---|---|---|
//! | `GET` | `/` | the page; a `?token=` here is how the printed link works |
//! | `GET` | `/api/health` | none |
//! | `GET` | `/api/pipelines` | viewer |
//! | `GET` | `/api/pipelines/{name}/lineage` | viewer |
//! | `GET` | `/api/runs` | viewer |
//! | `GET` | `/api/runs/{id}` | viewer |
//! | `GET` | `/api/schedules` | viewer |
//! | `POST` | `/api/runs` | **operator** |
//!
//! Every authenticated response carries `X-Etl-Role`, which is how the page
//! knows whether to draw a Run button.
//!
//! # Why `/api/health` needs no token
//!
//! So that a process manager can tell whether the console is up without being
//! given a credential. It says only that: no workspace path, no pipeline
//! names, no version. Anything that would help somebody decide whether this
//! host is worth attacking belongs behind the token.

use crate::auth::{bearer_of, Role, Tokens};
use crate::workspace::{Failure, Workspace};
use serde_json::json;

/// How many runs a listing returns when the request does not say.
const DEFAULT_LIMIT: usize = 50;

/// The most a listing will return however large a `limit` is asked for.
///
/// A bound rather than a preference: `?limit=100000000` on a workspace with a
/// year of history would serialise the lot into memory to answer one request.
const MAX_LIMIT: usize = 1_000;

/// What arrived, with the transport stripped off.
#[derive(Debug, Clone)]
pub struct Incoming<'a> {
    pub method: &'a str,
    /// The path, already split from the query and percent-decoded.
    pub path: &'a str,
    /// The raw query string, without the `?`.
    pub query: &'a str,
    /// The `Authorization` header, verbatim.
    pub authorization: Option<&'a str>,
}

/// What to send back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outgoing {
    pub status: u16,
    pub content_type: &'static str,
    pub body: String,
    /// Extra headers, as (name, value).
    pub headers: Vec<(&'static str, String)>,
}

impl Outgoing {
    pub fn json(status: u16, body: serde_json::Value) -> Self {
        Outgoing {
            status,
            content_type: "application/json; charset=utf-8",
            body: serde_json::to_string(&body).unwrap_or_else(|_| {
                // Cannot happen for a value already built as JSON, and a panic
                // in a request handler would take the whole console down.
                r#"{"error":"the response could not be serialised"}"#.to_string()
            }),
            headers: Vec::new(),
        }
    }

    pub fn html(status: u16, body: String) -> Self {
        Outgoing {
            status,
            content_type: "text/html; charset=utf-8",
            body,
            headers: Vec::new(),
        }
    }

    fn error(status: u16, message: impl Into<String>) -> Self {
        Outgoing::json(status, json!({ "error": message.into() }))
    }
}

/// Answer one request.
pub fn handle(request: &Incoming, tokens: &Tokens, workspace: &dyn Workspace) -> Outgoing {
    // Every response gets these, whatever it is, including the refusals.
    let mut response = route(request, tokens, workspace);
    harden(&mut response);
    response
}

fn route(request: &Incoming, tokens: &Tokens, workspace: &dyn Workspace) -> Outgoing {
    // The page, and the one place a token may travel in a URL.
    if request.path == "/" || request.path == "/index.html" {
        if request.method != "GET" {
            return Outgoing::error(405, "the console page is a GET");
        }

        return Outgoing::html(200, crate::ui::page(&workspace.label()));
    }

    if request.path == "/api/health" {
        // No token, and nothing in the answer worth having. See the module
        // docs for why this one is open.
        return match request.method {
            "GET" => Outgoing::json(200, json!({ "status": "ok" })),
            _ => Outgoing::error(405, "health is a GET"),
        };
    }

    if !request.path.starts_with("/api/") {
        return Outgoing::error(404, "no such path");
    }

    // Everything past here needs a token, in a header. A `?token=` is
    // deliberately not read: it is how the printed link opens the page, not a
    // credential for the API, and accepting it here would make a URL in a
    // chat log a working key — and let another site's form post one for you.
    let Some(role) = request
        .authorization
        .and_then(bearer_of)
        .and_then(|token| tokens.role_for(token))
    else {
        return unauthorized();
    };

    match (request.method, request.path) {
        ("GET", "/api/pipelines") => with_role(role, Role::Viewer, || {
            workspace
                .pipelines()
                .map(|found| json!({ "pipelines": found }))
        }),

        ("GET", "/api/schedules") => with_role(role, Role::Viewer, || {
            workspace
                .schedules()
                .map(|found| json!({ "schedules": found }))
        }),

        ("GET", "/api/runs") => with_role(role, Role::Viewer, || {
            let pipeline = parameter(request.query, "pipeline");
            let limit = match parameter(request.query, "limit") {
                Some(text) => text
                    .parse::<usize>()
                    .map_err(|_| Failure::invalid(format!("'{text}' is not a number of runs")))?
                    .clamp(1, MAX_LIMIT),
                None => DEFAULT_LIMIT,
            };

            workspace
                .runs(pipeline.as_deref(), limit)
                .map(|found| json!({ "runs": found }))
        }),

        ("POST", "/api/runs") => with_role(role, Role::Operator, || {
            let Some(name) = parameter(request.query, "pipeline") else {
                return Err(Failure::invalid("which pipeline? pass ?pipeline=<name>"));
            };

            workspace
                .start(&name)
                .map(|record| json!({ "run": record }))
        }),

        ("GET", path) if path.starts_with("/api/runs/") => with_role(role, Role::Viewer, || {
            let id = path.trim_start_matches("/api/runs/");

            if id.is_empty() {
                return Err(Failure::not_found("no run id"));
            }

            workspace.run(id).map(|record| json!({ "run": record }))
        }),

        ("GET", path) if path.starts_with("/api/pipelines/") => {
            let rest = path.trim_start_matches("/api/pipelines/");

            let Some(name) = rest.strip_suffix("/lineage") else {
                return Outgoing::error(404, "no such path");
            };

            // The name is handed to the workspace, which resolves it against
            // the pipelines it knows about. It is never joined onto a path
            // here, which is what keeps `..` from reaching the filesystem.
            with_role(role, Role::Viewer, || workspace.lineage(name))
        }

        ("GET", _) => Outgoing::error(404, "no such path"),

        _ => Outgoing::error(405, "that path does not take this method"),
    }
}

/// Run a handler if the role is enough, and turn a `Failure` into a response.
fn with_role(
    role: Role,
    required: Role,
    handler: impl FnOnce() -> Result<serde_json::Value, Failure>,
) -> Outgoing {
    let mut response = if role.allows(required) {
        match handler() {
            Ok(body) => Outgoing::json(200, body),
            Err(failure) => Outgoing::error(failure.status, failure.message),
        }
    } else {
        // 403 rather than 401: the token was good, the role was not, and
        // saying "unauthorized" would send somebody looking for a better
        // token when what they need is a different one.
        Outgoing::error(
            403,
            format!(
                "a {} may not do this; this needs {}",
                role.name(),
                required.name()
            ),
        )
    };

    // The page needs to know which role it has, so it can avoid offering a
    // button the server is going to refuse. Stated on every authenticated
    // response — including the refusals, which is where knowing your own role
    // is most useful — rather than by a route whose only job is to answer it,
    // and rather than by the page probing with a request it expects to fail
    // and reading the role out of the error text, which works until somebody
    // rewords the error.
    response
        .headers
        .push(("X-Etl-Role", role.name().to_string()));

    response
}

fn unauthorized() -> Outgoing {
    let mut response = Outgoing::error(401, "a bearer token is required");

    // Says what kind of credential, without hinting whether one was close.
    response
        .headers
        .push(("WWW-Authenticate", "Bearer".to_string()));

    response
}

/// A request that was refused before it was routed, for being too large.
///
/// Built here rather than in the server so it carries the same hardening
/// headers every other response does.
pub fn too_large() -> Outgoing {
    let mut response = Outgoing::error(413, "that request body is too large");
    harden(&mut response);
    response
}

/// Percent-decode a path.
///
/// The path only, never the query: the query still has to be split on `&` and
/// `=`, and decoding it first would let an escaped `&` inside a value invent a
/// parameter that was never sent.
pub fn decode_path(path: &str) -> String {
    percent_decode(path)
}

/// One query parameter, percent-decoded.
///
/// Hand-rolled because it is a dozen lines and the alternative is a URL crate
/// for one function — the same call this project made for the topological sort
/// and the cron grammar. It is not a general URL parser: it reads `a=b&c=d`,
/// takes the first occurrence of a name, and decodes `%XX` and `+`.
pub fn parameter(query: &str, name: &str) -> Option<String> {
    for pair in query.split('&') {
        let (key, value) = match pair.split_once('=') {
            Some(split) => split,
            // A bare `?foo` is a parameter with an empty value.
            None => (pair, ""),
        };

        if percent_decode(key) == name {
            let value = percent_decode(value);

            return if value.is_empty() { None } else { Some(value) };
        }
    }

    None
}

/// Decode `%XX` escapes and `+` as a space.
///
/// Invalid escapes are left as written rather than dropped: a name containing
/// a literal `%` should come back containing it, and silently deleting bytes
/// is how a check gets bypassed by something that was not what it was read as.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(b' ');
                index += 1;
            }

            b'%' if index + 2 < bytes.len() => {
                let high = (bytes[index + 1] as char).to_digit(16);
                let low = (bytes[index + 2] as char).to_digit(16);

                match (high, low) {
                    (Some(high), Some(low)) => {
                        out.push((high * 16 + low) as u8);
                        index += 3;
                    }
                    // Not a valid escape: keep the `%` as written and carry
                    // on from the next byte.
                    _ => {
                        out.push(bytes[index]);
                        index += 1;
                    }
                }
            }

            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }

    // A percent escape can produce a byte sequence that is not UTF-8. Lossy
    // rather than an error: the result is compared against known names and
    // will simply fail to match, which is the right outcome for input that
    // was never a name in the first place.
    String::from_utf8_lossy(&out).into_owned()
}

/// Headers every response carries.
///
/// This console renders its own page from its own strings and loads nothing
/// from anywhere else, so the strictest policy is also the accurate one.
fn harden(response: &mut Outgoing) {
    let headers = [
        // No third-party anything, no frames, no inline event handlers.
        (
            "Content-Security-Policy",
            "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; \
             connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
        ),
        ("X-Content-Type-Options", "nosniff"),
        ("Referrer-Policy", "no-referrer"),
        // The page URL carries a token. Without this, following a link out of
        // the console would hand it to whatever was linked.
        ("Cache-Control", "no-store"),
    ];

    for (name, value) in headers {
        response.headers.push((name, value.to_string()));
    }
}

#[cfg(test)]
mod tests;
