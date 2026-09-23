//! Running a plan against the DuckDB CLI.
//!
//! The engine shells out rather than linking DuckDB. That keeps the binary
//! small and the engine swappable, at the cost of one process per run and a
//! dependency on an external binary — so locating that binary, and reporting
//! clearly when it is missing, is part of the job here.
//!
//! The whole plan goes to DuckDB as **one script**. Temp views live in a
//! session and each invocation is a fresh process, so stage-at-a-time
//! execution would discard every view between stages. One script also means a
//! failure aborts the remainder, which is the behaviour we want.
//!
//! Reading the results relies on a specific CLI behaviour: `-json` prints one
//! JSON array **per statement that returns rows**, concatenated, not one array
//! for the batch. Stdout is therefore parsed as a stream of values, and the
//! number of values that arrived before a failure is what identifies the stage
//! that failed.

use crate::plan::{Control, Plan, Stage};
use crate::session::{Session, SessionError};
use crate::sql::quote_path;
use etl_metadata::ControlKind;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use thiserror::Error;

/// The environment variable that overrides binary discovery.
pub const DUCKDB_BIN_ENV: &str = "ETL_DUCKDB_BIN";

/// The environment variable that overrides extension-directory discovery.
pub const DUCKDB_EXTENSIONS_ENV: &str = "ETL_DUCKDB_EXTENSIONS";

/// The version this project is pinned to and tested against.
pub const PINNED_DUCKDB_VERSION: &str = "v1.5.5";

#[derive(Debug, Error)]
pub enum ExecError {
    #[error(
        "no DuckDB binary found. Looked at: {}. Run scripts/fetch-duckdb.ps1 to install the \
         pinned {PINNED_DUCKDB_VERSION}, or set {DUCKDB_BIN_ENV} to a binary.",
        .looked.join(", ")
    )]
    DuckdbNotFound { looked: Vec<String> },

    #[error("could not run the DuckDB binary at {path}: {source}")]
    Spawn {
        path: String,
        #[source]
        source: io::Error,
    },

    #[error("stage '{label}' ({node_id}) failed: {message}")]
    StageFailed {
        node_id: String,
        label: String,
        message: String,
    },

    #[error("the run failed: {message}")]
    RunFailed { message: String },

    #[error(
        "this pipeline needs the DuckDB extension(s) {extensions}, which could not be loaded. \
         Install them into the DuckDB extension directory, or choose components that do not need \
         them. DuckDB said: {message}"
    )]
    ExtensionLoadFailed { extensions: String, message: String },

    #[error("could not prepare the output directory {path}: {source}")]
    OutputDirectory {
        path: String,
        #[source]
        source: io::Error,
    },

    #[error("stage '{label}' would overwrite {path}, and its write mode is error_if_exists")]
    OutputExists { label: String, path: String },

    #[error("could not read DuckDB's output: {0}")]
    BadOutput(String),

    #[error("there is no node '{node_id}' in this pipeline")]
    NoSuchNode { node_id: String },

    #[error(
        "'{node_id}' is a sink, and a sink has no rows to look at. Preview the node feeding it \
         instead."
    )]
    CannotPreview { node_id: String },

    #[error(transparent)]
    Session(#[from] SessionError),
}

/// How to run a plan.
pub struct RunOptions {
    /// An explicit binary, skipping discovery.
    pub duckdb_bin: Option<PathBuf>,
    /// Where relative paths in the pipeline resolve from, and where discovery
    /// starts looking for a vendored binary.
    pub working_dir: Option<PathBuf>,
    /// Collect per-stage row counts. See [`Plan::script`] for what this costs.
    pub counts: bool,
    /// Where vendored DuckDB extensions live. Skips discovery when set.
    pub extension_dir: Option<PathBuf>,
    /// Plaintext secret values to mask out of anything this run reports.
    ///
    /// The real values still reach DuckDB — it needs the actual password — but
    /// they are masked in the report's copy of the script and in any error,
    /// because DuckDB echoes a connection string back in its own messages.
    /// [`crate::Resolved::secret_values`] is where these come from.
    pub redact: Vec<String>,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            duckdb_bin: None,
            working_dir: None,
            counts: true,
            extension_dir: None,
            redact: Vec::new(),
        }
    }
}

/// What one stage did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageOutcome {
    pub node_id: String,
    pub label: String,
    pub component_id: String,
    /// Rows the stage produced, when counts were collected. For a quality node
    /// this is the accepted side only; see [`StageOutcome::rejected`].
    pub rows: Option<u64>,
    /// Rows a quality node sent to its dead-letter output. `None` for every
    /// stage that does not split, which is how a node with nothing to reject
    /// stays distinguishable from a node that cannot reject at all.
    pub rejected: Option<u64>,
    /// Why this stage produced nothing, when it did not run at all. Only the
    /// driven path can report this: on the one-script path a failure ends the
    /// run, so there is never a stage that was reached and skipped.
    pub skipped: Option<SkipReason>,
    /// How long this stage took, when that number means what it appears to
    /// mean.
    ///
    /// `None` far more often than not, and deliberately. Two things have to
    /// hold before a duration here is honest:
    ///
    /// * **The stage was sent on its own.** Only the driven path does that. The
    ///   one-script path hands DuckDB the whole plan in one invocation, so
    ///   there is no per-stage boundary to measure and every stage reports
    ///   `None`.
    /// * **The stage did its work when it ran.** A lazy `CREATE VIEW` returns
    ///   in microseconds and the work it describes happens later, at the sink
    ///   that pulls it. Timing it truthfully produces `0 ms` beside the
    ///   transform that cost the most, which is worse than saying nothing. See
    ///   [`Stage::work_happens_here`].
    ///
    /// A blank timing is a smaller lie than a zero, so the rule is to report
    /// nothing rather than something indefensible.
    pub elapsed: Option<Duration>,
}

/// Why a stage did not run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// It failed, and `continue_on_failure` let the run go on.
    Failed,
    /// Something it reads never got created, because that stage failed.
    /// Running it anyway would fail with a message about a missing table,
    /// which would name the wrong node.
    UpstreamFailed { node_id: String },
    /// A `ctl.branch` upstream did not take this way. Not a failure — the
    /// pipeline said this might not run, and it did not.
    NotTaken { node_id: String },
}

impl SkipReason {
    pub fn describe(&self) -> String {
        match self {
            SkipReason::Failed => "failed".to_string(),
            SkipReason::UpstreamFailed { node_id } => format!("skipped: {node_id} failed"),
            SkipReason::NotTaken { node_id } => format!("not taken: {node_id}"),
        }
    }

    /// Whether this stage's absence means the run went wrong. A branch not
    /// taken is the pipeline working as written.
    pub fn is_failure(&self) -> bool {
        !matches!(self, SkipReason::NotTaken { .. })
    }
}

impl StageOutcome {
    /// Every row the stage saw, accepted and rejected together.
    ///
    /// For a quality node the two sides partition the input exactly, so this is
    /// the upstream row count — which is the number worth showing beside a
    /// rejection rate.
    pub fn rows_in(&self) -> Option<u64> {
        match (self.rows, self.rejected) {
            (Some(rows), Some(rejected)) => Some(rows + rejected),
            (rows, None) => rows,
            (None, rejected) => rejected,
        }
    }
}

/// What a whole run did.
#[derive(Debug, Clone)]
pub struct RunReport {
    pub stages: Vec<StageOutcome>,
    /// Wall-clock time for the whole run.
    ///
    /// Always present, and always the number to trust: it is the one timing
    /// that needs no caveat about which stage did the work. Per-stage
    /// durations ride on [`StageOutcome::elapsed`], where most of them are
    /// `None` for the reasons recorded there.
    pub elapsed: Duration,
    pub duckdb_bin: PathBuf,
    /// What was sent to DuckDB, with any secret values masked. Identical to
    /// the real script unless the pipeline used `${SECRET:...}`.
    pub script: String,
    /// How many `disk`-materialised spill files were written and cleared up.
    pub spilled: usize,
    /// What the control nodes had to say: waits held, branches taken or not,
    /// logs printed. Empty for a plan without them.
    pub notes: Vec<String>,
    /// The new high-water mark each incremental source reached.
    ///
    /// **Advisory until the run is known to have succeeded.** These are what
    /// the probes read, nothing more; persisting them is the caller's decision
    /// and must not be taken while [`RunReport::failed`] is true. A run that
    /// wrote some of its output and then failed has state that is *behind* its
    /// output, which is the recoverable direction — running it again redoes the
    /// window. Saving these anyway would skip whatever was in flight, and
    /// nothing afterwards could tell you it happened.
    pub watermarks: Vec<Watermark>,
    /// Stages that failed while `continue_on_failure` kept the run going.
    ///
    /// **A report holding any of these is a failed run.** Every other way a
    /// stage can fail returns an error instead; these come back inside a report
    /// precisely so the rest of it survives to be read.
    pub failures: Vec<StageFailure>,
}

impl RunReport {
    /// Whether this run failed despite reaching the end.
    pub fn failed(&self) -> bool {
        !self.failures.is_empty()
    }

    pub fn total_rows_written(&self) -> Option<u64> {
        self.stages.iter().filter_map(|s| s.rows).next_back()
    }
}

/// Rows from one node, for looking at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Preview {
    pub node_id: String,
    /// Column names, in the order the query returned them. Taken from the first
    /// row, so an empty result has no columns — which is honest: a preview
    /// reads data, and with no data there is nothing to read a shape from.
    pub columns: Vec<String>,
    /// Rows as JSON objects, at most the requested limit.
    pub rows: Vec<JsonValue>,
    /// Whether more rows exist than were returned.
    pub truncated: bool,
}

/// The most rows a preview will return however many are asked for.
///
/// A preview goes to a canvas, where it is a table someone glances at. Ten
/// thousand rows down an IPC channel to render a grid nobody scrolls is a way
/// to make the app feel broken.
pub const PREVIEW_LIMIT_MAX: usize = 500;

/// Read the rows one node produces, without running the rest of the pipeline.
///
/// Only the stages that node depends on are run, and sinks are dropped from
/// them: previewing is a read, and a read that wrote someone's output file
/// would be a trap. Nothing is written, and any `disk` spill is cleared up the
/// same way a run clears it.
pub fn preview(
    plan: &Plan,
    node_id: &str,
    limit: usize,
    options: &RunOptions,
) -> Result<Preview, ExecError> {
    let binary = locate_duckdb(options)?;

    let stages = plan.upto(node_id).ok_or_else(|| ExecError::NoSuchNode {
        node_id: node_id.to_string(),
    })?;

    if !stages.iter().any(|stage| stage.node_id == node_id) {
        // The node is in the plan but was dropped from the preview, which can
        // only mean it is a sink. Say so rather than returning nothing.
        return Err(ExecError::CannotPreview {
            node_id: node_id.to_string(),
        });
    }

    let limit = limit.clamp(1, PREVIEW_LIMIT_MAX);

    let mut script = String::new();

    if let Some(directory) = locate_extension_dir(options) {
        if !plan.extensions().is_empty() {
            script.push_str(&format!(
                "SET extension_directory={};\n",
                quote_path(&directory.to_string_lossy())
            ));
        }
    }

    for extension in plan.extensions() {
        script.push_str(&format!("LOAD {extension};\n"));
    }

    for stage in &stages {
        script.push_str(&stage.sql);
        script.push('\n');
    }

    // One more than asked for, so "there is more" is answered by the same query
    // rather than by a second count over the same work.
    script.push_str(&format!(
        "SELECT * FROM {} LIMIT {};\n",
        crate::sql::quote_identifier(node_id),
        limit + 1
    ));

    let mut command = Command::new(&binary);
    command.arg("-json").arg("-c").arg(&script);

    if let Some(directory) = &options.working_dir {
        command.current_dir(directory);
    }

    let output = command.output().map_err(|source| ExecError::Spawn {
        path: binary.display().to_string(),
        source,
    })?;

    // Spills belong to the stages that were run, so they are cleared here too.
    let _ = clear_spills(plan, options);

    if !output.status.success() {
        return Err(ExecError::RunFailed {
            message: redact(
                String::from_utf8_lossy(&output.stderr).trim(),
                &options.redact,
            ),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // The preview is the last result on stdout: the stages before it are views,
    // which return nothing.
    let last = serde_json::Deserializer::from_str(&stdout)
        .into_iter::<JsonValue>()
        .filter_map(Result::ok)
        .last()
        .unwrap_or(JsonValue::Array(Vec::new()));

    let mut rows = match last {
        JsonValue::Array(rows) => rows,
        _ => Vec::new(),
    };

    let truncated = rows.len() > limit;
    rows.truncate(limit);

    let columns = rows
        .first()
        .and_then(JsonValue::as_object)
        .map(|row| row.keys().cloned().collect())
        .unwrap_or_default();

    Ok(Preview {
        node_id: node_id.to_string(),
        columns,
        rows,
        truncated,
    })
}

/// Compile-free execution: run an already-compiled plan.
///
/// Two transports, one plan. Most plans go to DuckDB as a single script, which
/// is fastest and is what every component was written against. A plan holding a
/// control node or a per-stage policy needs its stages addressable one at a
/// time, and takes the session path instead.
pub fn run(plan: &Plan, options: &RunOptions) -> Result<RunReport, ExecError> {
    let binary = locate_duckdb(options)?;

    if plan.needs_session() {
        return run_driven(plan, options, binary);
    }

    run_one_script(plan, options, binary)
}

/// The original path: the whole plan as one `-c` invocation.
fn run_one_script(
    plan: &Plan,
    options: &RunOptions,
    binary: PathBuf,
) -> Result<RunReport, ExecError> {
    let script = with_extension_directory(plan, options);

    prepare_sinks(plan, options)?;
    prepare_spills(plan, options)?;

    let mut command = Command::new(&binary);
    command.arg("-json").arg("-c").arg(&script);

    if let Some(directory) = &options.working_dir {
        command.current_dir(directory);
    }

    let started = Instant::now();
    let output = command.output().map_err(|source| ExecError::Spawn {
        path: binary.display().to_string(),
        source,
    })?;
    let elapsed = started.elapsed();

    // Whatever happened, the spill files are scratch space and should not be
    // left behind. Cleared before any early return below, so a failed run does
    // not litter either.
    let spilled = clear_spills(plan, options);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut counts = parse_counts(&stdout)?;

    // The prelude's probe emits a count that belongs to no stage. Nothing
    // arriving at all means the prelude failed before any stage ran, which is
    // otherwise indistinguishable from the first stage failing.
    if plan.has_prelude_probe(options.counts) {
        if counts.is_empty() {
            return Err(ExecError::ExtensionLoadFailed {
                extensions: plan.extensions().join(", "),
                message: redact(
                    String::from_utf8_lossy(&output.stderr).trim(),
                    &options.redact,
                ),
            });
        }
        counts.remove(0);
    }

    if !output.status.success() {
        let message = redact(
            String::from_utf8_lossy(&output.stderr).trim(),
            &options.redact,
        );
        return Err(attribute_failure(plan, options.counts, &counts, message));
    }

    Ok(RunReport {
        stages: outcomes(plan, options.counts, &counts),
        watermarks: read_watermarks(plan, &parse_watermarks(&stdout)),
        elapsed,
        duckdb_bin: binary,
        // The report is read by people and written to logs, so it carries the
        // masked script. The unmasked one went to DuckDB and nowhere else.
        script: redact(&script, &options.redact),
        spilled,
        notes: Vec::new(),
        failures: Vec::new(),
    })
}

/// Point DuckDB at the project's own extension directory before anything is
/// loaded.
///
/// Extensions are vendored under `tools/duckdb/extensions/` rather than
/// installed into the user's DuckDB home, for the same reason the CLI itself is
/// vendored: the versions the project runs against are the versions it was
/// tested against, and nothing outside the project changes. It is also the
/// air-gapped path Phase 9 needs, exercised from the start.
///
/// The statement is prepended here rather than emitted by [`Plan::script`]
/// because it says where *this machine* keeps its extensions, which is not a
/// property of the pipeline. It returns no rows, so it does not disturb the
/// count attribution.
fn with_extension_directory(plan: &Plan, options: &RunOptions) -> String {
    let script = plan.script(options.counts);

    if plan.extensions().is_empty() {
        return script;
    }

    match locate_extension_dir(options) {
        // Nothing found: fall through to DuckDB's own default rather than
        // failing here, so a machine with its extensions installed the ordinary
        // way still runs.
        None => script,

        Some(directory) => format!(
            "SET extension_directory={};\n\n{}",
            quote_path(&directory.to_string_lossy()),
            script
        ),
    }
}

/// Find the vendored extension directory.
///
/// Order mirrors [`locate_duckdb`]: an explicit path, then the environment,
/// then a search upward from the working directory so this works from a crate
/// subdirectory during tests.
pub fn locate_extension_dir(options: &RunOptions) -> Option<PathBuf> {
    if let Some(explicit) = &options.extension_dir {
        return Some(explicit.clone());
    }

    if let Some(from_env) = std::env::var_os(DUCKDB_EXTENSIONS_ENV) {
        return Some(PathBuf::from(from_env));
    }

    let start = options
        .working_dir
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));

    start
        .ancestors()
        .map(|directory| directory.join("tools").join("duckdb").join("extensions"))
        .find(|candidate| candidate.is_dir())
}

/// Find a DuckDB binary.
///
/// Order: an explicit path, then `ETL_DUCKDB_BIN`, then the vendored copy
/// (searching upward from the working directory, so this works from a crate
/// subdirectory during tests), then whatever is on `PATH`.
pub fn locate_duckdb(options: &RunOptions) -> Result<PathBuf, ExecError> {
    let mut looked = Vec::new();

    if let Some(explicit) = &options.duckdb_bin {
        if explicit.is_file() {
            return Ok(explicit.clone());
        }
        looked.push(explicit.display().to_string());
    }

    if let Some(from_env) = std::env::var_os(DUCKDB_BIN_ENV) {
        let candidate = PathBuf::from(from_env);
        if candidate.is_file() {
            return Ok(candidate);
        }
        looked.push(format!("{DUCKDB_BIN_ENV}={}", candidate.display()));
    }

    let start = options
        .working_dir
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));

    if let Some(vendored) = find_vendored(&start) {
        return Ok(vendored);
    }
    looked.push(format!("{}/**/tools/duckdb/", start.display()));

    // Last resort: let the OS resolve it. Anything unresolvable surfaces as a
    // spawn error naming this, which is clear enough.
    if on_path("duckdb") {
        return Ok(PathBuf::from("duckdb"));
    }
    looked.push("duckdb on PATH".to_string());

    Err(ExecError::DuckdbNotFound { looked })
}

fn find_vendored(start: &Path) -> Option<PathBuf> {
    let name = if cfg!(windows) {
        "duckdb.exe"
    } else {
        "duckdb"
    };

    start
        .ancestors()
        .map(|directory| directory.join("tools").join("duckdb").join(name))
        .find(|candidate| candidate.is_file())
}

fn on_path(binary: &str) -> bool {
    Command::new(binary)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Create output directories, and refuse to clobber a file the pipeline said
/// not to clobber — before DuckDB runs, so a refusal leaves nothing written.
fn prepare_sinks(plan: &Plan, options: &RunOptions) -> Result<(), ExecError> {
    for stage in plan.sinks() {
        let Some(path) = &stage.sink_path else {
            continue;
        };

        let resolved = resolve_against(path, options);

        if stage.sink_mode.as_deref() == Some("error_if_exists") && resolved.exists() {
            return Err(ExecError::OutputExists {
                label: stage.label.clone(),
                path: resolved.display().to_string(),
            });
        }

        if let Some(parent) = resolved.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|source| ExecError::OutputDirectory {
                    path: parent.display().to_string(),
                    source,
                })?;
            }
        }
    }

    Ok(())
}

/// Mask every secret value out of text that a person will see.
///
/// A plain string replacement, because the values are known exactly. It is
/// applied to the reported script and to DuckDB's own error output, which is
/// the path that actually leaks: a failed `ATTACH` quotes the whole connection
/// string back, password and all.
fn redact(text: &str, secrets: &[String]) -> String {
    let mut out = text.to_string();

    for secret in secrets {
        // An empty secret would match at every position and mask nothing.
        if !secret.is_empty() {
            out = out.replace(secret.as_str(), crate::params::REDACTED);
        }
    }

    out
}

/// Make room for the spill files a `disk`-materialised stage writes.
///
/// DuckDB's `COPY ... TO` will not create the directory for itself, and the
/// failure it gives when the directory is missing says nothing about
/// materialisation.
fn prepare_spills(plan: &Plan, options: &RunOptions) -> Result<(), ExecError> {
    for spill in plan.spills() {
        let resolved = resolve_against(spill, options);

        if let Some(parent) = resolved.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|source| ExecError::OutputDirectory {
                    path: parent.display().to_string(),
                    source,
                })?;
            }
        }
    }

    Ok(())
}

/// Delete the spill files, and report how many there were.
///
/// Best-effort: a spill that cannot be removed is scratch space in a temporary
/// directory, and failing a run that otherwise succeeded over it would be the
/// wrong trade. It is the reason `spilled` is a count rather than a promise.
fn clear_spills(plan: &Plan, options: &RunOptions) -> usize {
    plan.spills()
        .into_iter()
        .filter(|spill| std::fs::remove_file(resolve_against(spill, options)).is_ok())
        .count()
}

/// A path from the plan, against the run's working directory.
fn resolve_against(path: &str, options: &RunOptions) -> PathBuf {
    match &options.working_dir {
        Some(directory) => directory.join(path),
        None => PathBuf::from(path),
    }
}

/// Read the concatenated JSON arrays DuckDB prints, one per statement that
/// returned rows, and pull the count out of each.
fn parse_counts(stdout: &str) -> Result<Vec<u64>, ExecError> {
    let mut counts = Vec::new();

    for value in serde_json::Deserializer::from_str(stdout).into_iter::<JsonValue>() {
        let value = match value {
            Ok(value) => value,
            // A truncated trailing value is what a killed process looks like.
            // Everything parsed so far still tells us how far the run got.
            Err(error) if error.is_eof() => break,
            Err(error) => return Err(ExecError::BadOutput(error.to_string())),
        };

        let count = value
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("n"))
            .and_then(JsonValue::as_u64);

        if let Some(count) = count {
            counts.push(count);
        }
    }

    Ok(counts)
}

/// Read the watermark probes out of the same stdout the counts came from.
///
/// The two never collide: a count probe emits `n` and a watermark probe emits
/// `w`, and each parser ignores what it does not recognise. That is why adding
/// watermarks needed no change to count attribution, which is delicate enough
/// that threading a second value through it would have been the risky way to
/// do this.
fn parse_watermarks(stdout: &str) -> Vec<Option<String>> {
    let mut found = Vec::new();

    for value in serde_json::Deserializer::from_str(stdout).into_iter::<JsonValue>() {
        let Ok(value) = value else { break };

        let Some(cell) = value
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("w"))
        else {
            continue;
        };

        found.push(match cell {
            // `max` over a relation that loaded nothing. Not a value to store.
            JsonValue::Null => None,
            JsonValue::String(text) => Some(text.clone()),
            // A number, a date, whatever else DuckDB chose to print. Rendered
            // without JSON quoting, because what goes back into the next run's
            // predicate is the literal, not its JSON spelling.
            other => Some(other.to_string()),
        });
    }

    found
}

/// Pair the values read back with the sources that asked for them.
///
/// Positional, because the probes are emitted in plan order and read in the
/// order they arrive. A short list means the run stopped early; those sources
/// simply report nothing, which the caller reads as "do not move it".
fn read_watermarks(plan: &Plan, values: &[Option<String>]) -> Vec<Watermark> {
    plan.watermark_probes()
        .zip(values)
        .map(|((stage, state), value)| Watermark {
            node_id: stage.node_id.clone(),
            column: state.column.clone(),
            value: value.clone(),
        })
        .collect()
}

/// Turn a non-zero exit into an error that names the stage responsible.
///
/// With counts on, each count probe emits exactly one array, so the number that
/// arrived is the number of probes that finished — and the stage owning the
/// next one is the culprit. Probes, not stages: a quality node emits two, so
/// counting stages would name the wrong node for everything after the first
/// one. Without counts there is nothing to count, so the error stays
/// unattributed rather than guessing.
fn attribute_failure(
    plan: &Plan,
    counts_enabled: bool,
    counts: &[u64],
    message: String,
) -> ExecError {
    if !counts_enabled {
        return ExecError::RunFailed { message };
    }

    let probes: Vec<&Stage> = plan.count_probes().map(|(stage, _)| stage).collect();

    match probes.get(counts.len()) {
        Some(stage) => ExecError::StageFailed {
            node_id: stage.node_id.clone(),
            label: stage.label.clone(),
            message,
        },
        None => ExecError::RunFailed { message },
    }
}

fn outcomes(plan: &Plan, counts_enabled: bool, counts: &[u64]) -> Vec<StageOutcome> {
    // Keyed by node **and port**, because a quality node contributes two
    // numbers and they must not overwrite one another.
    let mut by_output = std::collections::HashMap::new();

    if counts_enabled {
        for ((stage, probe), count) in plan.count_probes().zip(counts) {
            by_output.insert((stage.node_id.clone(), probe.is_rejected()), *count);
        }
    }

    plan.stages
        .iter()
        .map(|stage| {
            let count = |rejected: bool| by_output.get(&(stage.node_id.clone(), rejected)).copied();

            StageOutcome {
                node_id: stage.node_id.clone(),
                label: stage.label.clone(),
                component_id: stage.component_id.clone(),
                rows: count(false),
                rejected: stage.splits.then(|| count(true)).flatten(),
                skipped: None,
                // One invocation, one timing. There is no per-stage boundary
                // on this path to put a number either side of.
                elapsed: None,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The driven path
//
// One DuckDB process held open, stages sent to it one at a time. Used only for
// plans that need it: see `Plan::needs_session` and
// `docs/DECISION_execution_model.md`.
//
// Everything here is about *transport*. The plan, the SQL, and the count probes
// are the same ones the one-script path uses — if the two ever disagree about
// anything else, the dual path has stopped paying for itself.
// ---------------------------------------------------------------------------

/// A high-water mark one incremental source reached in this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watermark {
    pub node_id: String,
    pub column: String,
    /// The value read back, as DuckDB printed it.
    ///
    /// Absent when the source loaded nothing, which is the ordinary state of an
    /// incremental pipeline with nothing new to do. The distinction matters:
    /// `None` means leave the stored watermark exactly where it was, whereas a
    /// value means move it.
    pub value: Option<String>,
}

/// A stage that failed while the run was told to carry on past it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageFailure {
    pub node_id: String,
    pub label: String,
    pub message: String,
}

/// Run a plan through a persistent session.
fn run_driven(plan: &Plan, options: &RunOptions, binary: PathBuf) -> Result<RunReport, ExecError> {
    let extensions = plan.extensions();
    let extension_dir = locate_extension_dir(options);

    prepare_sinks(plan, options)?;
    prepare_spills(plan, options)?;

    let mut session = Session::open(
        &binary,
        options.working_dir.as_deref(),
        extension_dir.as_deref(),
        &extensions,
    )
    .map_err(|source| session_error(source, &extensions, options))?;

    let started = Instant::now();

    let mut outcomes: Vec<StageOutcome> = Vec::with_capacity(plan.stages.len());
    let mut failures: Vec<StageFailure> = Vec::new();
    // Stages that cannot run because something they read never got created.
    let mut unusable: HashSet<String> = HashSet::new();
    // Stages downstream of a branch that went the other way. Separate from
    // `unusable` because this is the pipeline working, not failing.
    let mut untaken: HashSet<String> = HashSet::new();
    let mut notes: Vec<String> = Vec::new();
    let mut watermarks: Vec<Watermark> = Vec::new();
    let mut script = String::new();

    for stage in &plan.stages {
        script.push_str(&format!("-- {} ({})\n", stage.node_id, stage.component_id));
        script.push_str(&stage.sql);
        script.push_str("\n\n");

        // A stage reading a relation that was never created would fail with a
        // DuckDB message about a missing table, which says nothing about the
        // stage that actually broke. Skipping is the honest report.
        if let Some(missing) = blocked_by(stage, &unusable) {
            unusable.insert(stage.node_id.clone());
            outcomes.push(skipped_outcome(
                stage,
                SkipReason::UpstreamFailed { node_id: missing },
            ));
            continue;
        }

        if let Some(branch) = blocked_by(stage, &untaken) {
            untaken.insert(stage.node_id.clone());
            outcomes.push(skipped_outcome(
                stage,
                SkipReason::NotTaken { node_id: branch },
            ));
            continue;
        }

        // The clock starts here rather than at `run_stage`, because a control
        // node does its work in `act` — a `ctl.wait` timed from below this
        // block reports the microseconds after the wait rather than the wait.
        // Both skip paths above return before this point, and a stage that
        // never ran reports no duration at all.
        let stage_started = Instant::now();

        // A control node acts before its rows are allowed past: it may hold,
        // report, stop the run, or decide that what follows does not run.
        if let Some(control) = &stage.control {
            match act(&mut session, stage, control, options)? {
                Ok(Action::Continue) => {}

                Ok(Action::Note(note)) => notes.push(note),

                Ok(Action::DoNotTake(note)) => {
                    notes.push(note);
                    untaken.insert(stage.node_id.clone());
                }

                Err(message) => {
                    unusable.insert(stage.node_id.clone());
                    outcomes.push(skipped_outcome(stage, SkipReason::Failed));
                    failures.push(StageFailure {
                        node_id: stage.node_id.clone(),
                        label: stage.label.clone(),
                        message: message.clone(),
                    });

                    if !stage.policy.continue_on_failure {
                        return Err(ExecError::StageFailed {
                            node_id: stage.node_id.clone(),
                            label: stage.label.clone(),
                            message: redact(&message, &options.redact),
                        });
                    }
                    continue;
                }
            }
        }

        // `run_stage` retries internally, so measuring around it reports what
        // the stage actually cost rather than what its last attempt did.
        let ran = run_stage(&mut session, stage, options)?;
        let took = stage_started.elapsed();

        match ran {
            Ok(readings) => {
                outcomes.push(StageOutcome {
                    elapsed: timing(stage, took),
                    ..stage_outcome(stage, &readings.counts)
                });

                // Recorded only for a stage that actually ran. A source that
                // was skipped has no new mark, and must keep the old one.
                if let Some(state) = &stage.incremental {
                    watermarks.push(Watermark {
                        node_id: stage.node_id.clone(),
                        column: state.column.clone(),
                        value: readings.watermark,
                    });
                }
            }

            Err(message) => {
                unusable.insert(stage.node_id.clone());

                outcomes.push(StageOutcome {
                    node_id: stage.node_id.clone(),
                    label: stage.label.clone(),
                    component_id: stage.component_id.clone(),
                    rows: None,
                    rejected: None,
                    skipped: Some(SkipReason::Failed),
                    elapsed: timing(stage, took),
                });

                failures.push(StageFailure {
                    node_id: stage.node_id.clone(),
                    label: stage.label.clone(),
                    message: message.clone(),
                });

                // Without `continue_on_failure` this is the end of the run, and
                // it ends the same way the one-script path ends: at the first
                // failure, naming the stage.
                if !stage.policy.continue_on_failure {
                    return Err(ExecError::StageFailed {
                        node_id: stage.node_id.clone(),
                        label: stage.label.clone(),
                        message: redact(&message, &options.redact),
                    });
                }
            }
        }
    }

    let elapsed = started.elapsed();
    let spilled = clear_spills(plan, options);
    let _ = session.close();

    // A run that carried on past a failure still failed, but the report is the
    // reason anyone asked it to carry on: it says which stages ran, which were
    // skipped, and why. Returning an error here would throw that away and leave
    // `continue_on_failure` with nothing to show for itself. The failures ride
    // along instead, and the caller decides the exit code.
    let failures = failures
        .into_iter()
        .map(|failure| StageFailure {
            message: redact(&failure.message, &options.redact),
            ..failure
        })
        .collect();

    Ok(RunReport {
        stages: outcomes,
        watermarks,
        elapsed,
        duckdb_bin: binary,
        script: redact(&script, &options.redact),
        spilled,
        notes,
        failures,
    })
}

/// A measured duration, kept only if the stage it came from earned one.
///
/// The measurement is always taken — it costs two `Instant`s — and discarded
/// here rather than at the call site, so the rule about which stages may
/// report a timing lives in exactly one place.
fn timing(stage: &Stage, took: Duration) -> Option<Duration> {
    stage.work_happens_here().then_some(took)
}

/// A stage that was reached but not run.
fn skipped_outcome(stage: &Stage, reason: SkipReason) -> StageOutcome {
    StageOutcome {
        node_id: stage.node_id.clone(),
        label: stage.label.clone(),
        component_id: stage.component_id.clone(),
        rows: None,
        rejected: None,
        skipped: Some(reason),
        elapsed: None,
    }
}

/// What a control node decided.
enum Action {
    /// Carry on, saying nothing.
    Continue,
    /// Carry on, with something worth telling the user.
    Note(String),
    /// Carry on, but nothing downstream of this runs.
    DoNotTake(String),
}

/// Do what a control node says, before its rows are let past.
#[allow(clippy::type_complexity)]
fn act(
    session: &mut Session,
    stage: &Stage,
    control: &Control,
    options: &RunOptions,
) -> Result<Result<Action, String>, ExecError> {
    // The probe is evaluated first for every kind that has one, so a failing
    // probe is a failing stage before any decision rests on its answer.
    let answered = match &control.probe {
        None => None,
        Some(probe) => {
            let answer = session.execute(probe).map_err(ExecError::Session)?;

            match answer.values.first().and_then(first_number) {
                Some(number) => Some(number),
                None => {
                    let said = answer.stderr.trim().to_string();

                    // Keep both halves. DuckDB's message is usually the more
                    // specific — it names the missing column and what was
                    // available — but the node's own message is why the person
                    // put the check there, and dropping it loses the intent.
                    let detail = if said.is_empty() {
                        format!("{} produced no answer", stage.label)
                    } else {
                        said
                    };

                    return Ok(Err(match control.message.as_deref() {
                        Some(note) if !note.is_empty() => format!("{note}. {detail}"),
                        _ => detail,
                    }));
                }
            }
        }
    };

    let said = || control.message.clone().unwrap_or_default();

    let action = match control.kind {
        ControlKind::Wait => {
            let ms = control.wait_ms.unwrap_or(0);
            std::thread::sleep(Duration::from_millis(ms));

            match control.message.as_deref() {
                Some(note) => Action::Note(format!("{} — waited {ms}ms: {note}", stage.label)),
                None => Action::Note(format!("{} — waited {ms}ms", stage.label)),
            }
        }

        // The count is reported by the ordinary probes; this is only the note
        // that goes with it.
        ControlKind::Log => match control.message.as_deref() {
            Some(note) => Action::Note(format!("{} — {note}", stage.label)),
            None => Action::Continue,
        },

        // No probe means fail on arrival; a probe means fail only if some row
        // matched it.
        ControlKind::Fail => match answered {
            None => return Ok(Err(said())),
            Some(0) => Action::Continue,
            Some(matched) => return Ok(Err(format!("{} ({matched} row(s) matched)", said()))),
        },

        ControlKind::Branch => match answered {
            Some(0) | None => {
                let note = match control.message.as_deref() {
                    Some(note) => format!("{} — not taken: {note}", stage.label),
                    None => format!("{} — not taken, nothing downstream ran", stage.label),
                };
                Action::DoNotTake(note)
            }
            Some(matched) => Action::Note(format!("{} — taken ({matched} row(s))", stage.label)),
        },

        // Nothing is done with the answer. Having read the other input is the
        // whole effect.
        ControlKind::Sequence => Action::Continue,

        // The probe returns a boolean, which arrives as 1 or 0.
        ControlKind::Assert => match answered {
            Some(1) => Action::Continue,
            _ => return Ok(Err(said())),
        },
    };

    let _ = options;
    Ok(Ok(action))
}

/// The first value of the first row, as a number. Booleans arrive as 1 and 0.
fn first_number(value: &JsonValue) -> Option<u64> {
    let row = value.as_array()?.first()?.as_object()?;
    let first = row.values().next()?;

    match first {
        JsonValue::Bool(true) => Some(1),
        JsonValue::Bool(false) => Some(0),
        other => other.as_u64(),
    }
}

/// The upstream this stage cannot run without, if that upstream is unusable.
fn blocked_by(stage: &Stage, unusable: &HashSet<String>) -> Option<String> {
    stage
        .inputs
        .iter()
        .find(|input| unusable.contains(&input.node_id))
        .map(|input| input.node_id.clone())
}

/// Run one stage, with its retries, returning either its counts or the message
/// explaining why it did not produce them.
///
/// The outer `Result` is for the session itself coming apart — a timeout or a
/// lost pipe, which no retry policy can help with. The inner one is the stage
/// failing, which is what a policy is about.
#[allow(clippy::type_complexity)]
fn run_stage(
    session: &mut Session,
    stage: &Stage,
    options: &RunOptions,
) -> Result<Result<Readings, String>, ExecError> {
    let mut attempt = 0;

    loop {
        let outcome = attempt_stage(session, stage, options)?;

        match outcome {
            Ok(readings) => return Ok(Ok(readings)),

            Err(message) => {
                if attempt >= stage.policy.retry_attempts {
                    return Ok(Err(message));
                }

                attempt += 1;
                std::thread::sleep(stage.policy.backoff_for(attempt));
            }
        }
    }
}

/// One attempt at one stage.
fn attempt_stage(
    session: &mut Session,
    stage: &Stage,
    options: &RunOptions,
) -> Result<Result<Readings, String>, ExecError> {
    // A memory ceiling is set around the statement and put back afterwards, so
    // one greedy stage cannot quietly change the budget for the rest of the run.
    if let Some(limit) = stage.policy.memory_limit_mb {
        let set = format!("SET memory_limit='{limit}MB';");
        session.execute(&set).map_err(ExecError::Session)?;
    }

    let answer = session.execute(&stage.sql).map_err(ExecError::Session)?;

    // A `CREATE VIEW` prints nothing whether it worked or not, so the statement
    // itself cannot say. The count probes that follow are the verdict: a probe
    // against a relation that was never created fails and returns nothing.
    //
    // With counts turned off there is no verdict to read, so stderr is all
    // there is. That is sound only because the session frames stderr per
    // statement; before it did, a late message made this report a failed stage
    // as a success and blame the next one.
    // Whatever the statement itself said. When a `CREATE VIEW` fails, this is
    // the real cause; the count probe that follows then fails too, complaining
    // that the view does not exist. Reporting the probe's message would name
    // the symptom and hide the reason.
    let said = answer.stderr.trim().to_string();

    let result = if options.counts {
        collect_counts(session, stage, options).map_err(|from_probe| {
            if said.is_empty() {
                from_probe
            } else {
                said.clone()
            }
        })
    } else if answer.has_message() {
        Err(said.clone())
    } else {
        Ok(Vec::new())
    };

    let result = result.and_then(|counts| {
        collect_watermark(session, stage).map(|watermark| Readings { counts, watermark })
    });

    if stage.policy.memory_limit_mb.is_some() {
        session
            .execute("RESET memory_limit;")
            .map_err(ExecError::Session)?;
    }

    Ok(result)
}

/// Run the stage's count probes.
///
/// These are the same probes the one-script path emits, and they double as the
/// success signal: a probe against a relation that was never created fails and
/// returns nothing, so a missing count is how a failed `CREATE VIEW` — which
/// prints nothing either way — becomes visible.
/// What one stage's probes reported.
struct Readings {
    counts: Vec<u64>,
    /// The watermark this stage reached, for an incremental source. `None`
    /// both when the stage is not incremental and when it loaded nothing —
    /// the caller tells them apart by whether the stage declared one.
    watermark: Option<String>,
}

/// Read this run's new high-water mark, for a stage that has one.
///
/// Inside the stage attempt rather than beside it, so that a retry covers the
/// probe too and a probe that fails is a stage that failed — which is what it
/// is. A watermark column that does not exist is a configuration error, and
/// this is the point at which it surfaces, before anything downstream writes.
fn collect_watermark(session: &mut Session, stage: &Stage) -> Result<Option<String>, String> {
    let Some(state) = &stage.incremental else {
        return Ok(None);
    };

    let answer = session
        .execute(&state.probe)
        .map_err(|error| error.to_string())?;

    match answer.values.first().and_then(|row| row.get("w")) {
        Some(JsonValue::Null) | None => {
            // `max` over nothing loaded. Ordinary, and not a value to store.
            let said = answer.stderr.trim();
            if said.is_empty() {
                Ok(None)
            } else {
                Err(said.to_string())
            }
        }
        Some(JsonValue::String(text)) => Ok(Some(text.clone())),
        Some(other) => Ok(Some(other.to_string())),
    }
}

fn collect_counts(
    session: &mut Session,
    stage: &Stage,
    options: &RunOptions,
) -> Result<Vec<u64>, String> {
    if !options.counts {
        return Ok(Vec::new());
    }

    let mut counts = Vec::with_capacity(stage.counts.len());

    for probe in &stage.counts {
        let answer = match session.execute(&probe.sql) {
            Ok(answer) => answer,
            Err(error) => return Err(error.to_string()),
        };

        match answer.values.first().and_then(count_in) {
            Some(count) => counts.push(count),
            None => {
                // The session reads stderr up to this statement's own marker,
                // so the explanation is already here if DuckDB gave one.
                let said = answer.stderr.trim().to_string();

                return Err(if said.is_empty() {
                    format!("{} produced no rows to count", stage.label)
                } else {
                    said
                });
            }
        }
    }

    Ok(counts)
}

/// The `n` out of a count probe's result.
fn count_in(value: &JsonValue) -> Option<u64> {
    value
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("n"))
        .and_then(JsonValue::as_u64)
}

/// Pair a stage's counts with its ports, the way the one-script path does.
fn stage_outcome(stage: &Stage, counts: &[u64]) -> StageOutcome {
    let at = |wanted_rejected: bool| {
        stage
            .counts
            .iter()
            .position(|probe| probe.is_rejected() == wanted_rejected)
            .and_then(|index| counts.get(index).copied())
    };

    StageOutcome {
        node_id: stage.node_id.clone(),
        label: stage.label.clone(),
        component_id: stage.component_id.clone(),
        rows: at(false),
        rejected: stage.splits.then(|| at(true)).flatten(),
        skipped: None,
        // The caller times the stage and fills this in; this function knows
        // only how to read counts.
        elapsed: None,
    }
}

/// Turn a session that would not open into the message the one-script path
/// would have given for the same cause.
fn session_error(source: SessionError, extensions: &[&str], options: &RunOptions) -> ExecError {
    // A prelude that fails is almost always a missing extension, and saying so
    // is more use than repeating DuckDB's own wording.
    if !extensions.is_empty() {
        if let SessionError::BadOutput(message) = &source {
            return ExecError::ExtensionLoadFailed {
                extensions: extensions.join(", "),
                message: redact(message, &options.redact),
            };
        }
    }

    ExecError::Session(source)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concatenated_arrays_are_read_as_separate_results() {
        // The shape DuckDB actually prints for a multi-statement script.
        let stdout = "[{\"n\":12}]\n[{\"n\":7}]\n[{\"n\":6}]\n";

        assert_eq!(parse_counts(stdout).unwrap(), [12, 7, 6]);
    }

    #[test]
    fn results_without_a_count_column_are_ignored() {
        let stdout = "[{\"a\":1}]\n[{\"n\":5}]\n";

        assert_eq!(parse_counts(stdout).unwrap(), [5]);
    }

    #[test]
    fn empty_output_is_not_an_error() {
        assert!(parse_counts("").unwrap().is_empty());
    }

    #[test]
    fn a_truncated_tail_keeps_what_parsed() {
        let stdout = "[{\"n\":3}]\n[{\"n\":";

        assert_eq!(parse_counts(stdout).unwrap(), [3]);
    }

    #[test]
    fn a_missing_binary_lists_where_it_looked() {
        let options = RunOptions {
            duckdb_bin: Some(PathBuf::from("/nonexistent/duckdb")),
            working_dir: Some(PathBuf::from("/nonexistent")),
            counts: true,
            extension_dir: None,
            redact: Vec::new(),
        };

        // Only meaningful when there is no duckdb on PATH; when there is one,
        // discovery correctly succeeds and there is nothing to assert.
        if let Err(error) = locate_duckdb(&options) {
            let message = error.to_string();
            assert!(message.contains("/nonexistent/duckdb"), "{message}");
            assert!(message.contains("fetch-duckdb"), "{message}");
        }
    }
}
