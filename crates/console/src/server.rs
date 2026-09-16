//! The only part that knows what a socket is.
//!
//! Everything interesting happens in [`crate::routes`], as a pure function.
//! This translates: a `tiny_http` request into an [`Incoming`], an
//! [`Outgoing`] back into a response, and a pool of threads so that one person
//! starting a twenty-second run does not stop everybody else from reading.
//!
//! # Threads
//!
//! `tiny_http`'s server is shareable, so the pool is a handful of threads each
//! blocking on `recv`. The first threading in this project, and it is
//! deliberately the least interesting kind: no channels, no shared mutable
//! state, nothing to lock. Each thread handles a whole request and forgets it.
//!
//! What is *not* guarded here is two operators starting the same pipeline at
//! once. That is the same exposure a hand-run `etl run` has beside a running
//! scheduler, documented in `etl_scheduler::lock`, and the workspace decides —
//! the CLI's `Workspace::start` serialises runs, so the console never has two
//! going at once whatever the threads are doing.

use crate::auth::Tokens;
use crate::routes::{handle, Incoming};
use crate::workspace::Workspace;
use crate::ServeOptions;
use std::sync::Arc;
use tiny_http::{Header, Response, Server};

/// The largest request body this will read.
///
/// Nothing here takes a body at all — a run is started by a `POST` with a
/// query string — so this exists only so that a request claiming a gigabyte
/// of content is refused rather than read. Off the socket, before routing.
const MAX_BODY: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("could not listen on {address}: {message}")]
    Listen { address: String, message: String },
}

/// A console that is running.
pub struct Serving {
    server: Arc<Server>,
    tokens: Tokens,
    options: ServeOptions,
}

impl Serving {
    /// The address actually bound, which differs from the one asked for when
    /// port 0 was given — the way a test gets a free port without racing.
    pub fn address(&self) -> std::net::SocketAddr {
        self.server
            .server_addr()
            .to_ip()
            .unwrap_or(self.options.address)
    }

    pub fn tokens(&self) -> &Tokens {
        &self.tokens
    }

    /// The link to open, for a given token.
    pub fn link(&self, token: &str) -> String {
        let mut options = self.options.clone();
        options.address = self.address();
        options.link(token)
    }

    /// Serve until the process ends.
    ///
    /// There is no stop: a console is stopped with Ctrl-C, and a shutdown path
    /// that nothing calls is a path nothing tests. The threads are detached
    /// and the process exiting is what closes the socket.
    pub fn run(self, workspace: Arc<dyn Workspace>) {
        let mut threads = Vec::new();

        for _ in 0..self.options.threads.max(1) {
            let server = Arc::clone(&self.server);
            let workspace = Arc::clone(&workspace);
            let tokens = self.tokens.clone();

            threads.push(std::thread::spawn(move || loop {
                let Ok(request) = server.recv() else {
                    // The server has gone. Nothing useful to say from a worker
                    // thread on the way out.
                    return;
                };

                answer(request, &tokens, workspace.as_ref());
            }));
        }

        for thread in threads {
            let _ = thread.join();
        }
    }
}

/// Bind the port. Separate from [`Serving::run`] so a caller can print the
/// link — and a test can learn the port — before anything is served.
pub fn serve(options: ServeOptions, tokens: Tokens) -> Result<Serving, ServeError> {
    let server = Server::http(options.address).map_err(|error| ServeError::Listen {
        address: options.address.to_string(),
        message: error.to_string(),
    })?;

    Ok(Serving {
        server: Arc::new(server),
        tokens,
        options,
    })
}

/// Translate one request, answer it, and send the response.
fn answer(mut request: tiny_http::Request, tokens: &Tokens, workspace: &dyn Workspace) {
    // Read and discard any body before answering. Not doing so leaves it in
    // the socket, where `tiny_http` would read it as the start of the next
    // request on a kept-alive connection.
    let oversized = drain(&mut request);

    let method = request.method().as_str().to_string();
    let target = request.url().to_string();

    let authorization = request
        .headers()
        .iter()
        .find(|header| header.field.equiv("Authorization"))
        .map(|header| header.value.as_str().to_string());

    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (target.as_str(), ""),
    };

    // The path is decoded here and the query is not: the query still has to be
    // split on `&` and `=`, and decoding it first would let an escaped `&` in
    // a value invent a parameter.
    let path = crate::routes::decode_path(path);

    let outgoing = if oversized {
        crate::routes::too_large()
    } else {
        handle(
            &Incoming {
                method: &method,
                path: &path,
                query,
                authorization: authorization.as_deref(),
            },
            tokens,
            workspace,
        )
    };

    let mut response = Response::from_string(outgoing.body).with_status_code(outgoing.status);

    if let Ok(header) = Header::from_bytes(&b"Content-Type"[..], outgoing.content_type.as_bytes()) {
        response = response.with_header(header);
    }

    for (name, value) in &outgoing.headers {
        if let Ok(header) = Header::from_bytes(name.as_bytes(), value.as_bytes()) {
            response = response.with_header(header);
        }
    }

    // A client that hung up mid-response is not an error worth reporting: it
    // happens every time somebody closes a tab.
    let _ = request.respond(response);
}

/// Read the body out of the socket, and say whether it was too big.
fn drain(request: &mut tiny_http::Request) -> bool {
    use std::io::Read;

    let declared = request.body_length().unwrap_or(0);

    if declared > MAX_BODY {
        return true;
    }

    let mut sink = Vec::new();

    // Bounded by one more than the limit, so a request that lied about its
    // length in the header is still caught by what it actually sent.
    let read = request
        .as_reader()
        .take(MAX_BODY as u64 + 1)
        .read_to_end(&mut sink)
        .unwrap_or(0);

    read > MAX_BODY
}
