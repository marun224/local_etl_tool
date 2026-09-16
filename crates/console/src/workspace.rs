//! What the console needs from a workspace, and nothing more.
//!
//! The same shape 8c used for the scheduler, for the same reason: this crate
//! does not depend on the engine, does not know what a pipeline is, and cannot
//! compile SQL. It knows how to speak HTTP, check a token and render a page.
//! Everything behind that is a trait the CLI implements, where the engine, the
//! parameter resolver and the secret store already live.
//!
//! That keeps every route testable against a fake workspace with no DuckDB
//! anywhere near it, and it keeps running a pipeline in the one place that
//! already knows how — which is the whole of Settled decision 5's reasoning,
//! applied a third time.

use serde::Serialize;

/// Why a request could not be answered.
///
/// Carries the status because only the workspace knows whether a name it could
/// not find is a missing file (404) or a pipeline that will not compile (422).
/// The console turns it into a response and never inspects the message.
#[derive(Debug, Clone)]
pub struct Failure {
    pub status: u16,
    pub message: String,
}

impl Failure {
    pub fn not_found(message: impl Into<String>) -> Self {
        Failure {
            status: 404,
            message: message.into(),
        }
    }

    /// The request was understood and is wrong — a pipeline that will not
    /// compile, a limit that is not a number.
    pub fn invalid(message: impl Into<String>) -> Self {
        Failure {
            status: 422,
            message: message.into(),
        }
    }

    /// Something else holds the thing being asked for.
    pub fn conflict(message: impl Into<String>) -> Self {
        Failure {
            status: 409,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Failure {
            status: 500,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

/// One pipeline in the workspace.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineSummary {
    /// What to call it, and what every other route takes as its name. The
    /// document's own `name` when it has one, otherwise the file stem — the
    /// same rule `state::key_for` uses, so this agrees with where run records
    /// were written.
    pub name: String,

    /// Relative to the workspace, for display. Never used to open anything:
    /// a route resolves a name against this list rather than joining it onto
    /// a path, which is what keeps `../` out of the filesystem.
    pub path: String,

    /// How many nodes it has, or `None` if it would not compile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stages: Option<usize>,

    /// Why it will not compile, if it will not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub problem: Option<String>,

    /// The most recent run's outcome, if it has ever run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_outcome: Option<String>,

    /// When that run started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run: Option<String>,
}

/// One schedule, as the console shows it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleSummary {
    pub name: String,
    pub pipeline: String,
    /// The trigger in words: `every 1h`, `cron 0 3 * * *`, `watch data/inbox`.
    pub trigger: String,
    pub enabled: bool,

    /// When it next fires, UTC. Absent for a watch, which is not due at a
    /// time, and for a cron expression that can never match.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run: Option<String>,
}

/// Everything the console can ask of a workspace.
///
/// `Send + Sync` because requests are served from several threads, so that a
/// run started by one caller does not stop everybody else from reading.
pub trait Workspace: Send + Sync {
    /// Where this workspace is, for the page title.
    fn label(&self) -> String;

    fn pipelines(&self) -> Result<Vec<PipelineSummary>, Failure>;

    /// Node-level lineage for one pipeline, as `etl lineage --json` prints it.
    fn lineage(&self, name: &str) -> Result<serde_json::Value, Failure>;

    /// Run history, newest first.
    fn runs(
        &self,
        pipeline: Option<&str>,
        limit: usize,
    ) -> Result<Vec<etl_state::RunRecord>, Failure>;

    fn run(&self, id: &str) -> Result<etl_state::RunRecord, Failure>;

    fn schedules(&self) -> Result<Vec<ScheduleSummary>, Failure>;

    /// Start a run and wait for it. Operator only.
    ///
    /// Synchronous, deliberately: a console that returned a job id would need
    /// a job store, a status route and a way to reap the results, and the
    /// thing it would buy — not holding a connection open — matters far less
    /// on a local console than the machinery costs. The caller sees the same
    /// record `etl run --json` prints, which is the same record that went into
    /// history.
    fn start(&self, name: &str) -> Result<etl_state::RunRecord, Failure>;
}
