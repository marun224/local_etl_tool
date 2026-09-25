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
//!
//! A source that **holds** its messages until the run's outcome is known (a
//! queue) hands back a [`Receipt`]. [`Receipts`] keeps them, and the run
//! settles them once: acknowledged after it fully succeeded and its sinks
//! delivered, released on every other path. Its `Drop` releases whatever is
//! still held, so an early return cannot leave messages waiting out a timeout.

use crate::exec::{redact, resolve_against, Checkpoint, ExecError, RunOptions};
use crate::plan::{Direction, NativeStep, Stage};
use crate::session::Session;
use etl_plugin_sdk::{
    Connector, ConnectorError, Context, Receipt, Record, RecordReader, RecordWriter,
};
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

/// What staging the sources produced: a report line each, where each source
/// that keeps a position got to, and what the sources that hold are holding.
#[derive(Debug, Default)]
pub(crate) struct Staged {
    pub(crate) notes: Vec<String>,
    /// Advisory until the run succeeds, like a watermark: the caller keeps
    /// these only for a run that fully succeeded.
    pub(crate) checkpoints: Vec<Checkpoint>,
    /// Released when dropped; acknowledged only by a run that succeeded.
    pub(crate) receipts: Receipts,
}

/// Messages the native sources are holding, one receipt per source node.
///
/// Settled once: [`Receipts::acknowledge`] after a run that fully succeeded
/// and delivered, [`Receipts::release`] otherwise. Dropping it unsettled
/// releases, best-effort, which is what an early return or a preview wants.
#[derive(Default)]
pub(crate) struct Receipts {
    held: Vec<(String, Box<dyn Receipt>)>,
    redact: Vec<String>,
}

/// What settling said: lines for the report, and warnings for what could not
/// be acknowledged.
#[derive(Debug, Default)]
pub(crate) struct Settled {
    pub(crate) notes: Vec<String>,
    pub(crate) warnings: Vec<String>,
}

impl std::fmt::Debug for Receipts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.held.iter().map(|(label, _)| label))
            .finish()
    }
}

impl Receipts {
    fn hold(&mut self, label: &str, receipt: Box<dyn Receipt>, options: &RunOptions) {
        self.redact = options.redact.clone();
        self.held.push((label.to_string(), receipt));
    }

    /// The run fully succeeded and its sinks delivered. A receipt that cannot
    /// be acknowledged is a **warning**, not a failure: the output is already
    /// delivered, and the messages will only come again (Settled decision 59).
    pub(crate) fn acknowledge(mut self) -> Settled {
        let mut settled = Settled::default();
        for (label, receipt) in std::mem::take(&mut self.held) {
            match receipt.acknowledge() {
                Ok(line) => settled.notes.push(format!("{label}: {line}")),
                Err(error) => settled.warnings.push(format!(
                    "{label}: the messages read could not be acknowledged and will be \
                     delivered again: {}",
                    redact(&error.to_string(), &self.redact)
                )),
            }
        }
        settled
    }

    /// Anything but a full success: give every message back.
    pub(crate) fn release(mut self) -> Settled {
        let mut settled = Settled::default();
        for (label, receipt) in std::mem::take(&mut self.held) {
            match receipt.release() {
                Ok(line) => settled.notes.push(format!("{label}: {line}")),
                // The messages come back when their hold runs out instead.
                Err(error) => settled.warnings.push(format!(
                    "{label}: the messages read could not be released, so they come back only \
                     when their hold runs out: {}",
                    redact(&error.to_string(), &self.redact)
                )),
            }
        }
        settled
    }
}

impl Drop for Receipts {
    fn drop(&mut self) {
        for (_, receipt) in std::mem::take(&mut self.held) {
            let _ = receipt.release();
        }
    }
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
        let (summary, receipt) = source
            .read_held(
                &step.properties,
                &mut writer,
                &context(options, step.checkpoint.clone()),
            )
            .map_err(|error| failed(stage, error, options))?;
        if let Some(receipt) = receipt {
            // Held before anything else can fail, so the guard releases it.
            staged.receipts.hold(&stage.label, receipt, options);
        }
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

/// Run a native transform's half of its stage, in the session its view will
/// then be created in: the feed numbers the input rows and writes the columns
/// the transform reads, the transform writes what it adds, and the stage's own
/// SQL joins that back.
///
/// `Ok(Ok(line))` is a line for the report; `Ok(Err(message))` is the stage
/// failing, which the caller treats as any stage failure (retries aside: a
/// transform that calls out makes its own); `Err` is the session broken.
pub(crate) fn run_transform(
    session: &mut Session,
    stage: &Stage,
    options: &RunOptions,
    staging: &mut Staging,
) -> Result<Result<String, String>, ExecError> {
    run_transform_using(session, stage, options, staging, etl_connectors::find)
}

/// [`run_transform`], with the registry lookup passed in, so the bridge can
/// be tested with a transform that exists only in a test.
pub(crate) fn run_transform_using(
    session: &mut Session,
    stage: &Stage,
    options: &RunOptions,
    staging: &mut Staging,
    find: impl Fn(&str) -> Option<Connector>,
) -> Result<Result<String, String>, ExecError> {
    let Some(step) = native(stage, Direction::Transform) else {
        return Ok(Ok(String::new()));
    };
    let Some(Connector::Transform(transform)) = find(&stage.component_id) else {
        return Err(unregistered(stage));
    };
    let (Some(input), Some(feed)) = (&step.input, &step.feed) else {
        return Err(unregistered(stage));
    };

    let input = track(input, options, staging)?;
    let output = prepare(step, options, staging)?;

    let answer = session.execute(feed).map_err(ExecError::Session)?;
    if answer.has_message() {
        return Ok(Err(redact(answer.stderr.trim(), &options.redact)));
    }

    let outcome = (|| {
        let reader = File::open(&input).map_err(|error| ConnectorError::io(&input, error))?;
        let writer = File::create(&output).map_err(|error| ConnectorError::io(&output, error))?;
        let mut reader = JsonlReader {
            lines: BufReader::new(reader),
            line: 0,
        };
        let mut writer = JsonlWriter {
            out: BufWriter::new(writer),
        };
        let summary = transform.transform(
            &step.properties,
            &mut reader,
            &mut writer,
            &context(options, None),
        )?;
        writer
            .out
            .flush()
            .map_err(|error| ConnectorError::io(&output, error))?;
        Ok::<_, ConnectorError>(summary)
    })();

    Ok(match outcome {
        Ok(summary) => Ok(format!(
            "{}: {}",
            stage.label,
            redact(&summary.detail, &options.redact)
        )),
        Err(error) => Err(redact(&error.to_string(), &options.redact)),
    })
}

/// Make room for a staging file that is not the step's own, and track it.
fn track(path: &str, options: &RunOptions, staging: &mut Staging) -> Result<PathBuf, ExecError> {
    let path = resolve_against(path, options);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ExecError::OutputDirectory {
            path: parent.display().to_string(),
            source,
        })?;
    }
    staging.track(path.clone());
    Ok(path)
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
