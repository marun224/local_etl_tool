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

use crate::plan::{Plan, Stage};
use crate::sql::quote_path;
use serde_json::Value as JsonValue;
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
    /// Wall-clock time for the DuckDB process.
    ///
    /// Deliberately one number rather than one per stage: in a plan of lazy
    /// views, every transform would report roughly zero and the sink would
    /// report the entire pipeline's work. Per-stage timings only become
    /// meaningful with materialisation, which arrives in Phase 5.
    pub elapsed: Duration,
    pub duckdb_bin: PathBuf,
    /// What was sent to DuckDB, with any secret values masked. Identical to
    /// the real script unless the pipeline used `${SECRET:...}`.
    pub script: String,
    /// How many `disk`-materialised spill files were written and cleared up.
    pub spilled: usize,
}

impl RunReport {
    pub fn total_rows_written(&self) -> Option<u64> {
        self.stages.iter().filter_map(|s| s.rows).next_back()
    }
}

/// Compile-free execution: run an already-compiled plan.
pub fn run(plan: &Plan, options: &RunOptions) -> Result<RunReport, ExecError> {
    let binary = locate_duckdb(options)?;
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
        elapsed,
        duckdb_bin: binary,
        // The report is read by people and written to logs, so it carries the
        // masked script. The unmasked one went to DuckDB and nowhere else.
        script: redact(&script, &options.redact),
        spilled,
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
            }
        })
        .collect()
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
