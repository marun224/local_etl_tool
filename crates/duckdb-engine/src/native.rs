//! The executor's half of a native component.
//!
//! A component written in Rust exchanges records with DuckDB through a JSON
//! Lines file under [`NATIVE_DIR`](crate::plan::NATIVE_DIR). This module owns
//! that file from both ends:
//!
//! - **Before DuckDB starts**, every native source in the stages about to run
//!   reads its data and writes it to its staging file. A source has no
//!   upstream, so it can always go first. That is what leaves both transports
//!   and `preview` otherwise as they were: they call [`stage_sources`] the way
//!   they already call `prepare_spills`, and the stage itself is a plain view.
//! - **After DuckDB finishes**, and only if the run succeeded, every native sink
//!   delivers the file DuckDB wrote for it. A failed run delivers nothing, so a
//!   pipeline cannot half-deliver -- the same rule watermarks follow.
//!
//! Staging files are scratch. [`Staging`] deletes them when it goes out of
//! scope, which covers every early return without each one remembering to.

use crate::exec::{redact, resolve_against, Checkpoint, ExecError, RunOptions};
use crate::plan::{Direction, NativeStep, Stage};
use etl_plugin_sdk::{Connector, ConnectorError, Context, Record, RecordReader, RecordWriter};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

#[cfg(test)]
mod tests;

/// The staging files a run has made, deleted when it is dropped.
///
/// Best-effort, as spill cleanup is: a file that will not delete is scratch in
/// a temporary directory, and failing a run over it would be the wrong trade.
#[derive(Default)]
pub(crate) struct Staging {
    files: Vec<PathBuf>,
}

impl Staging {
    fn track(&mut self, path: PathBuf) {
        self.files.push(path);
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        for file in &self.files {
            let _ = std::fs::remove_file(file);
        }
    }
}

/// What staging the sources produced: a report line each, and where each
/// source that keeps a position got to.
#[derive(Debug, Default)]
pub(crate) struct Staged {
    pub(crate) notes: Vec<String>,
    /// Advisory until the run succeeds, like a watermark: the caller keeps
    /// these only for a run that fully succeeded.
    pub(crate) checkpoints: Vec<Checkpoint>,
}

/// Run every native source among `stages`, writing each one's staging file.
pub(crate) fn stage_sources<'a>(
    stages: impl IntoIterator<Item = &'a Stage>,
    options: &RunOptions,
    staging: &mut Staging,
) -> Result<Staged, ExecError> {
    stage_sources_using(stages, options, staging, etl_connectors::find)
}

/// [`stage_sources`], with the registry lookup passed in, so the bridge can
/// be tested with a connector that exists only in a test.
pub(crate) fn stage_sources_using<'a>(
    stages: impl IntoIterator<Item = &'a Stage>,
    options: &RunOptions,
    staging: &mut Staging,
    find: impl Fn(&str) -> Option<Connector>,
) -> Result<Staged, ExecError> {
    let mut staged = Staged::default();

    for stage in stages {
        let Some(step) = native(stage, Direction::Ingest) else {
            continue;
        };
        let Some(Connector::Source(source)) = find(&stage.component_id) else {
            return Err(unregistered(stage));
        };

        let path = prepare(step, options, staging)?;
        let file = File::create(&path)
            .map_err(|error| failed(stage, ConnectorError::io(&path, error), options))?;

        let mut writer = JsonlWriter {
            out: BufWriter::new(file),
        };
        let summary = source
            .read(
                &step.properties,
                &mut writer,
                &context(options, step.checkpoint.clone()),
            )
            .map_err(|error| failed(stage, error, options))?;
        writer
            .out
            .flush()
            .map_err(|error| failed(stage, ConnectorError::io(&path, error), options))?;

        staged.notes.push(format!(
            "{}: {}",
            stage.label,
            redact(&summary.detail, &options.redact)
        ));
        if let Some(value) = summary.checkpoint {
            staged.checkpoints.push(Checkpoint {
                node_id: stage.node_id.clone(),
                component_id: stage.component_id.clone(),
                value,
            });
        }
    }

    Ok(staged)
}

/// Make room for the files DuckDB will write for native sinks, and track them
/// for cleanup. Called before DuckDB starts, because `COPY ... TO` will not
/// create a directory for itself.
pub(crate) fn prepare_sinks<'a>(
    stages: impl IntoIterator<Item = &'a Stage>,
    options: &RunOptions,
    staging: &mut Staging,
) -> Result<(), ExecError> {
    for stage in stages {
        if let Some(step) = native(stage, Direction::Egress) {
            prepare(step, options, staging)?;
        }
    }
    Ok(())
}

/// Deliver what DuckDB wrote for each native sink. Call only after a run that
/// succeeded.
///
/// `not_taken` holds the stages a branch decided against: their `COPY` never
/// ran, so there is nothing to deliver and nothing wrong with that.
pub(crate) fn deliver_sinks<'a>(
    stages: impl IntoIterator<Item = &'a Stage>,
    not_taken: &HashSet<String>,
    options: &RunOptions,
) -> Result<Vec<String>, ExecError> {
    let mut notes = Vec::new();

    for stage in stages {
        let Some(step) = native(stage, Direction::Egress) else {
            continue;
        };
        if not_taken.contains(&stage.node_id) {
            continue;
        }
        let Some(Connector::Sink(sink)) = etl_connectors::find(&stage.component_id) else {
            return Err(unregistered(stage));
        };

        let path = resolve_against(&step.staging, options);
        let file = File::open(&path)
            .map_err(|error| failed(stage, ConnectorError::io(&path, error), options))?;

        let mut reader = JsonlReader {
            lines: BufReader::new(file),
            line: 0,
        };
        let summary = sink
            .write(&step.properties, &mut reader, &context(options, None))
            .map_err(|error| failed(stage, error, options))?;

        notes.push(format!("{}: {}", stage.label, summary.detail));
    }

    Ok(notes)
}

/// What to say about the native sinks of a run that failed.
pub(crate) fn withheld<'a>(stages: impl IntoIterator<Item = &'a Stage>) -> Vec<String> {
    stages
        .into_iter()
        .filter(|stage| native(stage, Direction::Egress).is_some())
        .map(|stage| format!("{}: nothing delivered, because the run failed", stage.label))
        .collect()
}

fn native(stage: &Stage, direction: Direction) -> Option<&NativeStep> {
    stage
        .native
        .as_ref()
        .filter(|step| step.direction == direction)
}

/// Create the staging directory and register the file for cleanup.
fn prepare(
    step: &NativeStep,
    options: &RunOptions,
    staging: &mut Staging,
) -> Result<PathBuf, ExecError> {
    let path = resolve_against(&step.staging, options);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ExecError::OutputDirectory {
            path: parent.display().to_string(),
            source,
        })?;
    }

    staging.track(path.clone());
    Ok(path)
}

fn context(options: &RunOptions, checkpoint: Option<serde_json::Value>) -> Context {
    Context {
        working_dir: options.working_dir.clone(),
        checkpoint,
    }
}

/// A connector's failure, as a stage failure, with any secret masked: a
/// connector quotes paths and URLs back the way DuckDB quotes connection
/// strings.
fn failed(stage: &Stage, error: ConnectorError, options: &RunOptions) -> ExecError {
    ExecError::StageFailed {
        node_id: stage.node_id.clone(),
        label: stage.label.clone(),
        message: redact(&error.to_string(), &options.redact),
    }
}

fn unregistered(stage: &Stage) -> ExecError {
    ExecError::StageFailed {
        node_id: stage.node_id.clone(),
        label: stage.label.clone(),
        message: format!(
            "'{}' is planned as native but no connector is registered for it",
            stage.component_id
        ),
    }
}

// ---------------------------------------------------------------------------
// The staging format
// ---------------------------------------------------------------------------

/// One JSON object per line, which is what `read_json(format =
/// 'newline_delimited')` reads and `COPY ... (FORMAT json)` writes.
struct JsonlWriter<W: Write> {
    out: W,
}

impl<W: Write> RecordWriter for JsonlWriter<W> {
    fn write(&mut self, record: Record) -> Result<(), ConnectorError> {
        serde_json::to_writer(&mut self.out, &record)
            .map_err(|error| ConnectorError::Staging(error.to_string()))?;
        self.out
            .write_all(b"\n")
            .map_err(|error| ConnectorError::Staging(error.to_string()))
    }
}

struct JsonlReader<R: BufRead> {
    lines: R,
    line: u64,
}

impl<R: BufRead> RecordReader for JsonlReader<R> {
    fn read(&mut self) -> Result<Option<Record>, ConnectorError> {
        let mut text = String::new();

        loop {
            text.clear();
            let read = self
                .lines
                .read_line(&mut text)
                .map_err(|error| ConnectorError::Staging(error.to_string()))?;

            if read == 0 {
                return Ok(None);
            }
            self.line += 1;

            if !text.trim().is_empty() {
                break;
            }
        }

        serde_json::from_str::<Record>(text.trim_end())
            .map(Some)
            .map_err(|error| {
                // DuckDB writes a non-finite double as a bare `Infinity` or `NaN`,
                // which is not JSON. Worth naming, because nothing else about the
                // message would lead anyone there.
                let hint = if text.contains("Infinity") || text.contains("NaN") {
                    ". DuckDB writes a non-finite double (e.g. 1/0) as a bare Infinity or NaN, \
                 which is not JSON; filter or cast it before this sink"
                } else {
                    ""
                };
                ConnectorError::Staging(format!(
                    "line {} is not a JSON object: {error}{hint}",
                    self.line
                ))
            })
    }
}
