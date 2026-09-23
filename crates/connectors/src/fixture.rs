//! A local HTTP server for the web connectors' tests: no network, no
//! container. It records every request and answers with whatever the test's
//! handler says.

use etl_plugin_sdk::{Record, Records};
use serde_json::Value as JsonValue;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

/// One request the fixture received.
#[derive(Clone, Debug)]
pub(crate) struct Seen {
    pub(crate) method: String,
    pub(crate) url: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: String,
}

impl Seen {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub(crate) fn query(&self, name: &str) -> Option<String> {
        let (_, query) = self.url.split_once('?')?;
        query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == name).then(|| value.to_string())
        })
    }

    pub(crate) fn path(&self) -> &str {
        self.url.split('?').next().unwrap_or("")
    }
}

/// What the fixture answers.
pub(crate) struct Answer {
    pub(crate) status: u16,
    pub(crate) body: String,
    pub(crate) headers: Vec<(String, String)>,
}

pub(crate) fn ok(body: JsonValue) -> Answer {
    Answer {
        status: 200,
        body: body.to_string(),
        headers: Vec::new(),
    }
}

pub(crate) fn status(code: u16, body: &str) -> Answer {
    Answer {
        status: code,
        body: body.to_string(),
        headers: Vec::new(),
    }
}

impl Answer {
    pub(crate) fn with(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }
}

pub(crate) struct Fixture {
    base: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    server: Arc<tiny_http::Server>,
}

impl Fixture {
    pub(crate) fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    pub(crate) fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.unblock();
    }
}

/// Serve `handler`, which is told how many requests came before this one.
pub(crate) fn serve<F>(handler: F) -> Fixture
where
    F: Fn(usize, &Seen) -> Answer + Send + 'static,
{
    let server = Arc::new(tiny_http::Server::http("127.0.0.1:0").expect("a port"));
    let port = server.server_addr().to_ip().expect("an IP address").port();
    let seen = Arc::new(Mutex::new(Vec::new()));

    let (listening, log) = (Arc::clone(&server), Arc::clone(&seen));
    std::thread::spawn(move || {
        for mut request in listening.incoming_requests() {
            let mut body = String::new();
            let _ = request.as_reader().read_to_string(&mut body);

            let received = Seen {
                method: request.method().to_string(),
                url: request.url().to_string(),
                headers: request
                    .headers()
                    .iter()
                    .map(|h| (h.field.to_string(), h.value.to_string()))
                    .collect(),
                body,
            };

            let index = {
                let mut all = log.lock().unwrap();
                all.push(received.clone());
                all.len() - 1
            };

            let answer = handler(index, &received);
            let mut response =
                tiny_http::Response::from_string(answer.body).with_status_code(answer.status);
            for (name, value) in answer.headers {
                response.add_header(
                    tiny_http::Header::from_bytes(name.as_bytes(), value.as_bytes()).unwrap(),
                );
            }
            let _ = request.respond(response);
        }
    });

    Fixture {
        base: format!("http://127.0.0.1:{port}"),
        seen,
        server,
    }
}

/// Rows for a sink to read, from JSON objects.
pub(crate) fn records(values: Vec<JsonValue>) -> Records<std::vec::IntoIter<Record>> {
    Records(
        values
            .into_iter()
            .map(|v| v.as_object().unwrap().clone())
            .collect::<Vec<_>>()
            .into_iter(),
    )
}
