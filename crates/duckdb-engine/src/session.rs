//! A DuckDB process held open for the length of a run.
//!
//! The ordinary path hands a whole plan to DuckDB as one `-c` invocation and
//! reads what comes back. That is simple, fast, and cannot express control
//! flow: a `foreach` does not know how many times its body runs until the run
//! is under way, and a stage cannot be retried if it was never addressable on
//! its own. This module is the other path — one process, stdin held open,
//! statements sent and answered one at a time.
//!
//! See `docs/DECISION_execution_model.md` for why this rather than linking
//! DuckDB or splitting the plan into segments. The short version: a round trip
//! on a warm session costs 0.54 ms against 41.5 ms to spawn a process, and
//! keeping the subprocess leaves the vendored binary and the vendored
//! extensions exactly as Phases 4 and 9 need them.
//!
//! **A plan earns a session.** Plans without control flow or per-stage policy
//! keep the one-script path they were written against. Only the transport
//! differs between the two — the plan, the SQL, and the count probes are the
//! same.
//!
//! ## The protocol
//!
//! DuckDB's `-json` output is a stream of JSON arrays, one per statement that
//! returns rows, and an array may span several lines. There is no framing, so
//! this adds one: every statement is followed by a `SELECT` of a unique marker,
//! and the driver reads until it sees that marker. Whatever arrived before it
//! belongs to the statement.
//!
//! The marker carries a sequence number. If a marker from an earlier statement
//! ever shows up, the session is out of step with itself, and that is a bug to
//! fail loudly on rather than a number to report — a desynced session would
//! otherwise attribute one stage's rows to another.
//!
//! **Failure is read from stdout, not stderr.** A statement that fails emits no
//! JSON array, so "the rows I expected did not arrive" is the verdict, and
//! stderr only supplies the message.
//!
//! **stderr is framed too.** The two pipes have no ordering between them, so a
//! failed statement's marker can come back on stdout before its message has
//! arrived on stderr. Collecting "whatever stderr holds by now" therefore
//! attributed a late message to the *next* statement, which reported a success
//! as a failure and the failure as nothing. CI's first Linux run caught it in a
//! test; the engine had the same exposure. So every statement is also followed
//! by `SELECT error('<error marker>')`, which DuckDB prints to stderr *after* the
//! statement's own message, and the driver reads stderr up to that marker the
//! way it reads stdout up to the other. Attribution is exact, and there is no
//! grace period to wait out: a quarter-second sleep used to stand in for this.

use crate::sql::quote_path;
use serde_json::Value as JsonValue;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;
use thiserror::Error;

/// How long a single statement may take before the session is declared lost.
///
/// Generous, because a statement here can be an entire table scan. It exists
/// for the case the one-script path never had: with a pipe held open, a DuckDB
/// that never answers is a hang in our process rather than a child that exits.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("could not start the DuckDB binary at {path}: {source}")]
    Spawn {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("lost contact with DuckDB while running: {sql}")]
    Closed { sql: String },

    #[error("DuckDB did not answer within {}s. The statement was: {sql}", .timeout.as_secs())]
    Timeout { timeout: Duration, sql: String },

    #[error("could not send a statement to DuckDB: {source}")]
    Write {
        #[source]
        source: std::io::Error,
    },

    #[error("DuckDB's output could not be read as JSON: {0}")]
    BadOutput(String),

    #[error(
        "the session lost its place: expected marker {expected} but saw {seen}. This is a bug in \
         the session protocol, not in the pipeline."
    )]
    OutOfStep { expected: u64, seen: u64 },
}

/// What one statement did.
#[derive(Debug, Clone)]
pub struct Answer {
    /// The JSON arrays the statement printed, one per result-producing
    /// statement inside it.
    pub values: Vec<JsonValue>,
    /// What DuckDB wrote to stderr for this statement, and only this one — it
    /// is read up to the statement's error marker, so it is complete when the
    /// answer is returned and holds nothing that belongs to a neighbour.
    pub stderr: String,
}

impl Answer {
    /// Whether DuckDB complained. The message, not the verdict — see the module
    /// note on why the verdict comes from the values instead.
    pub fn has_message(&self) -> bool {
        !self.stderr.trim().is_empty()
    }
}

/// A DuckDB process with its stdin held open.
pub struct Session {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    errors: Receiver<String>,
    sequence: u64,
    timeout: Duration,
}

impl Session {
    /// Start a DuckDB process and put it in a state where statements can be
    /// sent to it.
    ///
    /// `prelude` is run before anything else and its output discarded — the
    /// extension directory and the `LOAD` list, which are properties of the
    /// machine and the plan rather than of any one stage.
    pub fn open(
        binary: &Path,
        working_dir: Option<&Path>,
        extension_dir: Option<&Path>,
        extensions: &[&str],
    ) -> Result<Self, SessionError> {
        let mut command = Command::new(binary);

        command
            .arg("-json")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(directory) = working_dir {
            command.current_dir(directory);
        }

        let mut child = command.spawn().map_err(|source| SessionError::Spawn {
            path: binary.display().to_string(),
            source,
        })?;

        let stdin = child.stdin.take().expect("stdin was piped");
        let stdout = child.stdout.take().expect("stdout was piped");
        let stderr_pipe = child.stderr.take().expect("stderr was piped");

        // Both streams are pumped by their own thread. Reading them inline
        // would deadlock the moment DuckDB filled a pipe buffer we were not
        // draining, which on a wide result set is immediately.
        let (sender, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });

        let (error_sender, errors) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr_pipe).lines().map_while(Result::ok) {
                if error_sender.send(line).is_err() {
                    break;
                }
            }
        });

        let mut session = Session {
            child,
            stdin: Some(stdin),
            lines,
            errors,
            sequence: 0,
            timeout: DEFAULT_TIMEOUT,
        };

        session.run_prelude(extension_dir, extensions)?;

        Ok(session)
    }

    /// Override the per-statement timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Point DuckDB at the vendored extensions and load what the plan needs.
    ///
    /// `.bail off` is set explicitly rather than relied on as the default. It
    /// matters more here than the default suggests: with `.bail on`, a failing
    /// statement terminates the whole session rather than just that statement,
    /// which would make `continue_on_failure` impossible and turn one bad stage
    /// into a lost run. Fail-fast is the driver's decision, taken per stage.
    fn run_prelude(
        &mut self,
        extension_dir: Option<&Path>,
        extensions: &[&str],
    ) -> Result<(), SessionError> {
        self.send_raw(".bail off\n")?;

        if let Some(directory) = extension_dir {
            let statement = format!(
                "SET extension_directory={};\n",
                quote_path(&directory.to_string_lossy())
            );
            self.send_raw(&statement)?;
        }

        for extension in extensions {
            self.send_raw(&format!("LOAD {extension};\n"))?;
        }

        // One round trip to prove the prelude landed. A failed LOAD does not
        // stop the `SELECT 1` behind it, so rows alone prove nothing -- that
        // was the only check here until the stderr framing existed, and it
        // never once caught a missing extension. The LOAD's message is on
        // stderr ahead of this statement's error marker, so it is now
        // attributed here, and anything said at all means the prelude failed.
        let answer = self.execute("SELECT 1 AS ok;")?;

        if answer.values.is_empty() || answer.has_message() {
            return Err(SessionError::BadOutput(format!(
                "the session prelude failed. DuckDB said: {}",
                answer.stderr.trim()
            )));
        }

        Ok(())
    }

    fn send_raw(&mut self, text: &str) -> Result<(), SessionError> {
        let stdin = self.stdin.as_mut().ok_or_else(|| SessionError::Closed {
            sql: text.to_string(),
        })?;

        stdin
            .write_all(text.as_bytes())
            .and_then(|()| stdin.flush())
            .map_err(|source| SessionError::Write { source })
    }

    /// Send one statement and read everything it produced.
    ///
    /// Returns whatever arrived, which is the caller's evidence about what
    /// happened: a statement that failed produces no JSON array, so an empty
    /// `values` where output was expected is the failure signal.
    pub fn execute(&mut self, sql: &str) -> Result<Answer, SessionError> {
        self.sequence += 1;
        let sequence = self.sequence;

        // Anything sent raw since the last statement -- the prelude's LOADs --
        // is answered inside this one, which is where its messages belong.
        let statement = format!(
            "{sql}\nSELECT '{}' AS __etl_mark;\nSELECT error('{}');\n",
            marker_for(sequence),
            error_marker_for(sequence)
        );
        self.send_raw(&statement)?;

        match read_answer(&self.lines, &self.errors, sequence, self.timeout) {
            Ok((stdout, stderr)) => Ok(Answer {
                values: parse_values(&stdout)?,
                stderr,
            }),
            Err(Wait::TimedOut) => {
                // The session cannot be trusted to be at a statement boundary
                // any more, so it is killed rather than reused.
                let _ = self.child.kill();
                Err(SessionError::Timeout {
                    timeout: self.timeout,
                    sql: sql.to_string(),
                })
            }
            Err(Wait::Closed) => Err(SessionError::Closed {
                sql: sql.to_string(),
            }),
            Err(Wait::OutOfStep(seen)) => Err(SessionError::OutOfStep {
                expected: sequence,
                seen,
            }),
        }
    }

    /// Close stdin and wait for the process to finish.
    pub fn close(mut self) -> Result<(), SessionError> {
        self.shutdown();
        Ok(())
    }

    fn shutdown(&mut self) {
        drop(self.stdin.take());

        // The child exits on its own once stdin closes. Kill only if it does
        // not, so an ordinary close stays orderly.
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            _ => {
                let _ = self.child.wait();
            }
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.shutdown();
    }
}

const MARK: &str = "__etl_mark_";
const ERROR_MARK: &str = "__etl_errmark_";

/// The marker text for a statement.
///
/// Distinctive on purpose: it is compared against every line DuckDB prints, so
/// it has to be something no plausible data value contains.
fn marker_for(sequence: u64) -> String {
    format!("{MARK}{sequence}__")
}

/// The stderr counterpart: raised with `error()`, which DuckDB reports as
/// `Invalid Input Error: __etl_errmark_<n>__` after anything the statement
/// itself said. Neither prefix contains the other, so the two cannot be
/// mistaken for each other.
fn error_marker_for(sequence: u64) -> String {
    format!("{ERROR_MARK}{sequence}__")
}

/// The sequence number in a stdout line, if that line is a marker.
fn marker_sequence(line: &str) -> Option<u64> {
    sequence_after(line, MARK)
}

/// The sequence number in a stderr line, if that line is an error marker.
fn error_marker_sequence(line: &str) -> Option<u64> {
    sequence_after(line, ERROR_MARK)
}

fn sequence_after(line: &str, prefix: &str) -> Option<u64> {
    let start = line.find(prefix)? + prefix.len();
    let rest = &line[start..];
    let end = rest.find("__")?;

    rest[..end].parse().ok()
}

/// Why a statement's answer could not be read.
#[derive(Debug, PartialEq)]
enum Wait {
    TimedOut,
    Closed,
    OutOfStep(u64),
}

/// Read one statement's stdout and stderr, each up to its own marker.
///
/// Separate from the process so the attribution can be tested with a message
/// that arrives *late* -- after the stdout marker, which is exactly what a busy
/// machine does and what the first Linux CI run caught.
fn read_answer(
    lines: &Receiver<String>,
    errors: &Receiver<String>,
    sequence: u64,
    timeout: Duration,
) -> Result<(String, String), Wait> {
    let stdout = read_until(lines, marker_sequence, sequence, timeout)?;
    let stderr = read_until(errors, error_marker_sequence, sequence, timeout)?;

    Ok((stdout.join("\n") + "\n", stderr.join("\n")))
}

/// Lines from `source` up to the marker for `sequence`, which is dropped.
///
/// The timeout is per line, as it always was: a statement is allowed to take
/// that long before its first output, not to finish within it.
fn read_until(
    source: &Receiver<String>,
    marker: fn(&str) -> Option<u64>,
    sequence: u64,
    timeout: Duration,
) -> Result<Vec<String>, Wait> {
    let mut collected = Vec::new();

    loop {
        let line = match source.recv_timeout(timeout) {
            Ok(line) => line,
            Err(RecvTimeoutError::Timeout) => return Err(Wait::TimedOut),
            Err(RecvTimeoutError::Disconnected) => return Err(Wait::Closed),
        };

        if let Some(seen) = marker(&line) {
            if seen != sequence {
                return Err(Wait::OutOfStep(seen));
            }
            return Ok(collected);
        }

        collected.push(line.trim_end_matches('\r').to_string());
    }
}

/// Read the concatenated JSON arrays a statement printed.
fn parse_values(text: &str) -> Result<Vec<JsonValue>, SessionError> {
    let mut values = Vec::new();

    for value in serde_json::Deserializer::from_str(text).into_iter::<JsonValue>() {
        match value {
            Ok(value) => values.push(value),
            Err(error) if error.is_eof() => break,
            Err(error) => return Err(SessionError::BadOutput(error.to_string())),
        }
    }

    Ok(values)
}

/// Where a session's DuckDB binary and extensions were found, so a caller can
/// report it the way the one-script path does.
#[derive(Debug, Clone)]
pub struct SessionPaths {
    pub binary: PathBuf,
    pub extension_dir: Option<PathBuf>,
}

#[cfg(test)]
mod tests;
