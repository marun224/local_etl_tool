//! A small web console over a workspace's pipelines, runs and schedules.
//!
//! The last slice of Phase 8, and a view over what 8b and 8c produce rather
//! than anything new underneath: it lists pipelines, shows run history, shows
//! what is scheduled and when it next fires, and lets an operator start a run.
//!
//! # Shape
//!
//! Four layers, each testable without the one below it:
//!
//! * [`auth`] — two roles, constant-time token checks, tokens minted per
//!   process unless the environment supplies stable ones.
//! * [`routes`] — every route as a pure function from a method, a path, a
//!   query and a token to a status and a body. No socket, no `tiny_http`.
//! * [`ui`] — the page, as one string. No build step, so a headless runner can
//!   serve its own console.
//! * [`server`] — `tiny_http`, and the only part that knows what a socket is.
//!
//! [`Workspace`] is the seam: this crate does not depend on the engine and
//! cannot compile SQL, exactly as the scheduler does not. The CLI implements
//! it, where the engine and the resolver already live.
//!
//! # `tiny_http`, and why
//!
//! Settled decision 8. Small and blocking, no async runtime; routing for eight
//! endpoints is less code than wiring a framework, and `axum` would bring
//! tokio, tower and hyper into a workspace that had four external crates.
//! Hand-rolling HTTP was considered and rejected — unlike the topological sort
//! and the cron grammar, this parses untrusted input off a socket and checks
//! credentials, which is a different risk class.
//!
//! Five crates arrive with it: `tiny_http`, `ascii`, `chunked_transfer`,
//! `httpdate`, `log`.
//!
//! # What it is not
//!
//! Not a public service. It binds loopback unless told otherwise, speaks no
//! TLS, and has no accounts — two shared tokens, which is the right weight for
//! a console somebody runs next to their work and wrong for anything on an
//! open network. Binding elsewhere says so, loudly.

pub mod auth;
pub mod routes;
pub mod server;
pub mod ui;
pub mod workspace;

pub use auth::{Role, Source, Tokens};
pub use routes::{Incoming, Outgoing};
pub use server::{serve, Serving};
pub use workspace::{Failure, PipelineSummary, ScheduleSummary, Workspace};

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// The port `etl serve` uses when nobody says otherwise.
pub const DEFAULT_PORT: u16 = 8087;

/// How to serve.
#[derive(Debug, Clone)]
pub struct ServeOptions {
    /// What to bind. Loopback by default — see [`ServeOptions::is_loopback`].
    pub address: SocketAddr,

    /// How many requests can be handled at once.
    ///
    /// A run is served synchronously on its own thread, so this is also how
    /// many things can be happening while one is going. Small: a console has a
    /// handful of readers, and every thread is a thread that can be waiting on
    /// DuckDB.
    pub threads: usize,
}

impl Default for ServeOptions {
    fn default() -> Self {
        ServeOptions {
            // 127.0.0.1, never 0.0.0.0. A console with no TLS and a bearer
            // token is right for a machine somebody is working on and wrong
            // for a network, so reaching the network has to be asked for.
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_PORT),
            threads: 4,
        }
    }
}

impl ServeOptions {
    /// Whether this only accepts connections from this machine.
    pub fn is_loopback(&self) -> bool {
        self.address.ip().is_loopback()
    }

    /// What to warn about before starting, if anything.
    ///
    /// Returned rather than printed so the caller decides where it goes and
    /// the test can read it.
    pub fn warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();

        if !self.is_loopback() {
            warnings.push(format!(
                "binding {} makes this console reachable from the network. It speaks no TLS, \
                 so its tokens cross that network in clear and anyone who reads one becomes \
                 whoever it belongs to. Put it behind a reverse proxy that terminates TLS, or \
                 bind loopback and use an SSH tunnel.",
                self.address
            ));
        }

        warnings
    }

    /// The link to open, with a token in it.
    ///
    /// The one place a token belongs in a URL: the page takes it out of the
    /// address bar on load and sends it as a header from then on.
    pub fn link(&self, token: &str) -> String {
        // An unspecified bind address (0.0.0.0) is not somewhere a browser can
        // go, so the printed link points at loopback, which is where the
        // person reading it actually is.
        let host = if self.address.ip().is_unspecified() {
            format!("127.0.0.1:{}", self.address.port())
        } else {
            self.address.to_string()
        };

        format!("http://{host}/?token={token}")
    }
}

#[cfg(test)]
mod tests;
