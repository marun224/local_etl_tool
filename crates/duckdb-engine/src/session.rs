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
//! stderr only supplies the message. The two streams have no ordering guarantee
//! between them, so deciding *whether* something failed by watching stderr
//! would be a race; deciding *what to say about it* is not.

use crate::sql::quote_path;
use serde_json::Value as JsonValue;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

/// How long a single statement may take before the session is declared lost.
///
/// Generous, because a statement here can be an entire table scan. It exists
/// for the case the one-script path never had: with a pipe held open, a DuckDB
/// that never answers is a hang in our process rather than a child that exits.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(600);

/// How long to let stderr catch up once a statement is known to have failed.
///
/// stdout and stderr are separate pipes with no ordering between them, so the
/// message explaining a failure can arrive just after the marker that revealed
/// it. **Waited only when a caller asks for a message**, never on the way
/// through: a `CREATE VIEW` returns no rows whether it worked or not, so
/// pausing on "no rows" would put this delay on every ordinary stage. It cost
/// four seconds a run before it was moved behind [`Session::message`].
pub const STDERR_GRACE: Duration = Duration::from_millis(250);

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
    /// Anything DuckDB wrote to stderr while running it.
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
    stderr: Arc<Mutex<Vec<String>>>,
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

        let stderr = Arc::new(Mutex::new(Vec::new()));
        let collected = Arc::clone(&stderr);
        std::thread::spawn(move || {
            for line in BufReader::new(stderr_pipe).lines().map_while(Result::ok) {
                if let Ok(mut held) = collected.lock() {
                    held.push(line);
                }
            }
        });

        let mut session = Session {
            child,
            stdin: Some(stdin),
            lines,
            stderr,
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

        // One round trip to prove the prelude landed. A failed LOAD prints to
        // stderr and produces no rows, exactly like any other failure, so this
        // is the same check the stages use rather than a special case.
        let answer = self.execute("SELECT 1 AS ok;")?;

        if answer.values.is_empty() {
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
        let marker = marker_for(sequence);

        self.drain_stderr();

        let statement = format!("{sql}\nSELECT '{marker}' AS __etl_mark;\n");
        self.send_raw(&statement)?;

        let mut collected = String::new();

        loop {
            let line = match self.lines.recv_timeout(self.timeout) {
                Ok(line) => line,
                Err(RecvTimeoutError::Timeout) => {
                    // The session cannot be trusted to be at a statement
                    // boundary any more, so it is killed rather than reused.
                    let _ = self.child.kill();
                    return Err(SessionError::Timeout {
                        timeout: self.timeout,
                        sql: sql.to_string(),
                    });
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(SessionError::Closed {
                        sql: sql.to_string(),
                    })
                }
            };

            if let Some(seen) = marker_sequence(&line) {
                if seen != sequence {
                    return Err(SessionError::OutOfStep {
                        expected: sequence,
                        seen,
                    });
                }
                break;
            }

            collected.push_str(line.trim_end_matches('\r'));
            collected.push('\n');
        }

        let values = parse_values(&collected)?;

        // Whatever stderr already holds, without waiting for more. A caller
        // that has decided something failed asks for the rest with
        // [`Session::message`].
        let stderr = self.drain_stderr();

        Ok(Answer { values, stderr })
    }

    /// Take whatever stderr holds right now.
    fn drain_stderr(&mut self) -> String {
        match self.stderr.lock() {
            Ok(mut held) => held.drain(..).collect::<Vec<_>>().join("\n"),
            Err(_) => String::new(),
        }
    }

    /// The explanation for a failure the caller has already detected.
    ///
    /// Waits [`STDERR_GRACE`] for the message to arrive, because it may still
    /// be in flight when the marker that revealed the failure came back. Only
    /// call this having decided something went wrong — on the happy path it is
    /// a quarter of a second of nothing.
    pub fn message(&mut self) -> String {
        std::thread::sleep(STDERR_GRACE);
        self.drain_stderr()
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

/// The marker text for a statement.
///
/// Distinctive on purpose: it is compared against every line DuckDB prints, so
/// it has to be something no plausible data value contains.
fn marker_for(sequence: u64) -> String {
    format!("__etl_mark_{sequence}__")
}

/// The sequence number in a line, if that line is a marker.
fn marker_sequence(line: &str) -> Option<u64> {
    let start = line.find("__etl_mark_")? + "__etl_mark_".len();
    let rest = &line[start..];
    let end = rest.find("__")?;

    rest[..end].parse().ok()
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
