//! One scheduler per workspace.
//!
//! `crates/state` says plainly that it has no locking and that single-writer
//! is the assumption. The scheduler is the first thing that could break that,
//! so it does not: it takes this lock at startup and runs pipelines one at a
//! time, which keeps the assumption *true* rather than making it safe to
//! violate.
//!
//! # What this does and does not guard
//!
//! It guards a second `etl schedule start` against the same workspace. It
//! does **not** guard a person running `etl run` by hand while a scheduler is
//! going, and that is deliberate: the alternative is a lock that someone
//! running one command has to understand and wait on, which would make the
//! common case worse to protect against an uncommon one. Running the same
//! pipeline both ways at the same moment races on its watermark, and the
//! loser is a re-read rather than lost data, because state advances only on a
//! run that fully succeeded.
//!
//! # Why the lock is a held handle rather than a file that exists
//!
//! A scheduler is a foreground process and Ctrl-C is how you stop one. A lock
//! that is "the file exists" would therefore be left behind on almost every
//! stop, and every restart would need `--force` — a guard people would learn
//! to bypass by reflex, which is no guard at all.
//!
//! So on Windows the lock file is **held open with no sharing**, and the
//! operating system releases it when the process ends *however* it ends,
//! including a kill. Nothing has to be cleaned up and there is no staleness
//! to reason about. Elsewhere, std offers no equivalent without a dependency
//! this workspace has not taken, so the lock falls back to a file that must
//! exist and `--force` to break one left behind. Both paths are exercised by
//! the same tests; the difference is only what happens after a crash.
//!
//! Because the held file cannot be read by anybody else, who holds it is
//! written to a **separate, readable** status file. That one is only ever
//! used to build a message, so a stale copy costs nothing: the lock itself is
//! the authority on whether the workspace is taken.

use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Where the lock lives, relative to the workspace root.
pub const LOCK_PATH: &str = ".etl/scheduler.lock";

/// Where the readable description of the holder lives.
pub const STATUS_PATH: &str = ".etl/scheduler.status";

#[derive(Debug, Error)]
pub enum LockError {
    #[error("another scheduler holds this workspace: {holder}{advice}")]
    Held { holder: String, advice: String },

    #[error("could not take the scheduler lock at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// What is written into the status file, for a person to read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Holder {
    pub pid: u32,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub host: String,

    /// UTC, `YYYY-MM-DDTHH:MM:SSZ`.
    pub since: String,
}

impl std::fmt::Display for Holder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "pid {}", self.pid)?;

        if !self.host.is_empty() {
            write!(formatter, " on {}", self.host)?;
        }

        write!(formatter, ", since {}", self.since)
    }
}

/// A held lock. Released when dropped, and by the operating system if this
/// process is killed before it gets the chance.
#[derive(Debug)]
pub struct WorkspaceLock {
    path: PathBuf,
    status: PathBuf,

    /// The open handle *is* the lock on Windows. Never read; closing it is
    /// what releases the workspace.
    ///
    /// An `Option` so `Drop` can close it *before* trying to remove the file:
    /// struct fields are dropped after the `Drop` body runs, and on Windows a
    /// file held with no sharing cannot be deleted, not even by the process
    /// holding it.
    held: Option<File>,
}

impl WorkspaceLock {
    /// Take the lock, or say who has it.
    pub fn acquire(workspace: &Path, force: bool) -> Result<Self, LockError> {
        let path = workspace.join(LOCK_PATH);
        let status = workspace.join(STATUS_PATH);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| LockError::Io {
                path: path.clone(),
                source,
            })?;
        }

        if force {
            // Missing is fine, and so is a live lock on Windows refusing to be
            // deleted — the open below is what actually decides. Forcing a
            // lock nobody holds is a no-op rather than an error, so `--force`
            // is safe to leave in a script.
            let _ = fs::remove_file(&path);
        }

        match open_exclusive(&path) {
            Ok(held) => {
                let holder = Holder {
                    pid: std::process::id(),
                    host: hostname(),
                    since: etl_state::now_utc(),
                };

                // Best effort: the lock is taken either way, and a status file
                // that could not be written costs a better error message for
                // the *next* person rather than this person's run.
                if let Ok(text) = serde_json::to_string_pretty(&holder) {
                    let _ = fs::write(&status, format!("{text}\n"));
                }

                Ok(WorkspaceLock {
                    path,
                    status,
                    held: Some(held),
                })
            }

            Err(error) if is_taken(&error) => Err(LockError::Held {
                holder: read_holder(&status),
                advice: advice_for(&path),
            }),

            Err(source) => Err(LockError::Io { path, source }),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Give up the lock now rather than at the end of the scope.
    pub fn release(self) {
        // `Drop` does the work; this exists so a caller can say when.
    }
}

impl Drop for WorkspaceLock {
    fn drop(&mut self) {
        // Closed first, and explicitly: on Windows a file held with no
        // sharing cannot be deleted, not even by us, and a field dropped at
        // the end of this function would be too late.
        self.held.take();

        // Failures here are not worth reporting — the process is on its way
        // out, and the lock is already released by the close above. Tidying
        // the files is so the next person does not find litter.
        let _ = fs::remove_file(&self.status);
        let _ = fs::remove_file(&self.path);
    }
}

/// Open the lock file so that a second opener is refused.
///
/// On Windows that is a share mode of zero, which the operating system
/// enforces and releases on process exit however the process exits. Elsewhere
/// it is an exclusive create, which a crash leaves behind — see the module
/// docs.
#[cfg(windows)]
fn open_exclusive(path: &Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        // No sharing at all: no other process may open this for reading,
        // writing or deletion while we hold it.
        .share_mode(0)
        .open(path)?;

    // Written for anybody looking at the file directly. The status file is
    // the one meant to be read, because this one cannot be while it is held.
    let _ = file.write_all(b"held\n");

    Ok(file)
}

#[cfg(not(windows))]
fn open_exclusive(path: &Path) -> std::io::Result<File> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;

    let _ = file.write_all(b"held\n");

    Ok(file)
}

/// Whether this error means somebody else has it, rather than that something
/// went wrong.
fn is_taken(error: &std::io::Error) -> bool {
    // `AlreadyExists` is the `create_new` path. A sharing violation arrives as
    // `PermissionDenied`, and on Windows as raw error 32, which older
    // toolchains do not map to a named kind.
    matches!(
        error.kind(),
        std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
    ) || error.raw_os_error() == Some(32)
}

/// What to tell somebody who has just been refused.
///
/// Different on each platform because the situations genuinely differ: a
/// Windows lock cannot be stale, so suggesting `--force` there would be
/// telling people to reach for a hammer that will not help.
fn advice_for(path: &Path) -> String {
    if cfg!(windows) {
        // The operating system holds this one. If it is refused, a scheduler
        // really is running.
        String::new()
    } else {
        format!(
            "\nIf that process is gone — after a crash or a power cut — take the lock with \
             `etl schedule start --force`, or delete {}",
            path.display()
        )
    }
}

/// Who the status file says holds the lock, or a plain description if it
/// cannot be read. Never an error: this only ever builds a message.
fn read_holder(status: &Path) -> String {
    match fs::read_to_string(status) {
        Ok(text) => match serde_json::from_str::<Holder>(&text) {
            Ok(holder) => holder.to_string(),
            Err(_) => "an unreadable status file".to_string(),
        },
        Err(_) => "another process".to_string(),
    }
}

/// The machine's name, best effort.
///
/// Only ever shown to a person deciding whether a lock is theirs, so an empty
/// answer costs a little context and nothing else. Read from the environment
/// rather than a syscall, which keeps this dependency-free.
fn hostname() -> String {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
