//! What the workspace remembers.
//!
//! Two things, kept apart because they answer different questions and deserve
//! different care:
//!
//! * **Watermarks** ([`Store`]) — the highest value each incremental source has
//!   already loaded, so the next run can ask for only what came after it. This
//!   changes what the next run *does*, so it is written atomically and a
//!   corrupt file is a hard error.
//! * **Run history** ([`runs`]) — a record of what happened, appended one line
//!   per run. This is hindsight: it changes nothing, so it is append-only and
//!   a corrupt line costs that record rather than stopping the world.
//!
//! Two properties are the whole point of this crate, and both are about not
//! losing rows:
//!
//! * **State advances only on a run that fully succeeded.** A run that failed
//!   partway has written some of its output and none of its state, and that is
//!   the recoverable arrangement: running it again re-reads the same window and
//!   redoes the work. The other order — advance first, then fail — silently
//!   skips whatever was in flight, and nothing afterwards can tell you it
//!   happened. So the store is written once, at the end, by a caller that has
//!   the whole report in hand.
//! * **The write is atomic.** A crash during the write must leave the previous
//!   watermark intact rather than a half-written file, because a state file
//!   that will not parse is indistinguishable from a pipeline that has never
//!   run — and that reloads everything from the beginning.
//!
//! **What this does not do.** There is no locking, so two runs of the same
//! pipeline at once will race and the second to finish wins. Single-writer is
//! the assumption until a scheduler exists to break it, and 8c is where that
//! gets faced rather than assumed away.

pub mod runs;

pub use runs::{History, Outcome, RunRecord, StageRecord, WatermarkRecord};

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// The format version this crate writes.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

/// Where state lives, relative to the workspace root.
pub const STATE_DIR: &str = ".etl/state";

#[derive(Debug, Error)]
pub enum StateError {
    #[error("could not read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("could not write {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("{path} is not valid state: {source}")]
    Malformed {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error(
        "state for '{key}' was written by format version {found}, and this build understands \
         {understood}"
    )]
    TooNew {
        key: String,
        found: u32,
        understood: u32,
    },

    #[error("'{key}' is not a usable state key: {reason}")]
    BadKey { key: String, reason: String },
}

/// The highest value already loaded from one incremental source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    /// The value itself, kept as the text DuckDB printed.
    ///
    /// Text rather than a typed value on purpose: a watermark column may be a
    /// timestamp, a date, an integer id or a string, and this crate has no
    /// business deciding which. It goes back into SQL as a quoted literal and
    /// DuckDB compares it against the column, which is the authority on what
    /// the type is. The alternative — parsing it here — would mean this crate
    /// re-implementing DuckDB's type rules and being subtly wrong at the edges.
    pub value: String,

    /// The column it was read from.
    ///
    /// Stored so that changing the column in the document is *detected* rather
    /// than silently comparing next run's `order_id` against last run's
    /// `order_ts`. See [`Watermark::matches_column`].
    pub column: String,

    /// When it was recorded, UTC, `YYYY-MM-DDTHH:MM:SSZ`.
    pub at: String,

    /// Anything a newer version wrote, preserved across a load and save.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl Watermark {
    /// Whether this watermark can be compared against `column`.
    ///
    /// A mismatch is not an error here — the caller decides, because the right
    /// answer is to start over from nothing rather than to fail, and only the
    /// caller can say that out loud.
    pub fn matches_column(&self, column: &str) -> bool {
        self.column == column
    }
}

/// Everything one pipeline remembers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineState {
    #[serde(default, rename = "formatVersion")]
    pub format_version: u32,

    /// Watermarks by node id.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub watermarks: BTreeMap<String, Watermark>,

    /// Anything a newer version wrote.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl PipelineState {
    /// What this node has already loaded, if anything.
    pub fn watermark(&self, node_id: &str) -> Option<&Watermark> {
        self.watermarks.get(node_id)
    }

    /// Record a new high-water mark for a node.
    ///
    /// Takes the column as well as the value so that a stored watermark always
    /// knows what it measured.
    pub fn advance(&mut self, node_id: &str, column: &str, value: impl Into<String>) {
        self.watermarks.insert(
            node_id.to_string(),
            Watermark {
                value: value.into(),
                column: column.to_string(),
                at: now_utc(),
                extra: BTreeMap::new(),
            },
        );
    }

    /// Forget one node's watermark, so the next run reloads from the start.
    pub fn forget(&mut self, node_id: &str) -> bool {
        self.watermarks.remove(node_id).is_some()
    }
}

/// The state files under a workspace.
#[derive(Debug, Clone)]
pub struct Store {
    directory: PathBuf,
}

impl Store {
    /// The store inside `workspace`.
    pub fn at(workspace: impl AsRef<Path>) -> Self {
        Store {
            directory: workspace.as_ref().join(STATE_DIR),
        }
    }

    /// Where one pipeline's state lives.
    pub fn path_for(&self, key: &str) -> PathBuf {
        self.directory.join(format!("{key}.json"))
    }

    /// Read one pipeline's state.
    ///
    /// A pipeline that has never run has no file, and that is not an error: it
    /// is a pipeline with no watermarks, which is exactly what "load
    /// everything" means. A file that exists but will not parse *is* an error,
    /// because the alternative is to treat a corrupted watermark as "never run"
    /// and quietly reload the entire source.
    pub fn load(&self, key: &str) -> Result<PipelineState, StateError> {
        check_key(key)?;
        let path = self.path_for(key);

        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(PipelineState {
                    format_version: CURRENT_FORMAT_VERSION,
                    ..PipelineState::default()
                })
            }
            Err(source) => return Err(StateError::Read { path, source }),
        };

        let state: PipelineState =
            serde_json::from_str(&text).map_err(|source| StateError::Malformed {
                path: path.clone(),
                source,
            })?;

        // A document from the future is refused rather than read optimistically:
        // guessing at a watermark's meaning is how rows go missing.
        if state.format_version > CURRENT_FORMAT_VERSION {
            return Err(StateError::TooNew {
                key: key.to_string(),
                found: state.format_version,
                understood: CURRENT_FORMAT_VERSION,
            });
        }

        Ok(state)
    }

    /// Write one pipeline's state, replacing whatever was there.
    ///
    /// Written to a neighbouring temporary file and renamed over the target, so
    /// that a crash mid-write leaves the previous state intact. A rename within
    /// a directory is atomic on both platforms this runs on; writing in place
    /// is not, and the failure it produces — a truncated file that will not
    /// parse — reads as "never run" and reloads the world.
    pub fn save(&self, key: &str, state: &PipelineState) -> Result<(), StateError> {
        check_key(key)?;
        let path = self.path_for(key);

        std::fs::create_dir_all(&self.directory).map_err(|source| StateError::Write {
            path: self.directory.clone(),
            source,
        })?;

        let mut writing = state.clone();
        writing.format_version = CURRENT_FORMAT_VERSION;

        let text = serde_json::to_string_pretty(&writing).expect("state serialises");

        let temporary = path.with_extension("json.tmp");
        std::fs::write(&temporary, format!("{text}\n")).map_err(|source| StateError::Write {
            path: temporary.clone(),
            source,
        })?;

        // `rename` replaces an existing file on both Windows and Unix. On
        // failure the temporary is cleared up, so a full disk does not leave
        // litter that looks like state.
        if let Err(source) = std::fs::rename(&temporary, &path) {
            let _ = std::fs::remove_file(&temporary);
            return Err(StateError::Write { path, source });
        }

        Ok(())
    }

    /// Every pipeline key with state, in order.
    pub fn keys(&self) -> Result<Vec<String>, StateError> {
        let entries = match std::fs::read_dir(&self.directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(StateError::Read {
                    path: self.directory.clone(),
                    source,
                })
            }
        };

        let mut keys: Vec<String> = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .filter_map(|path| {
                path.file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(str::to_string)
            })
            .collect();

        keys.sort();
        Ok(keys)
    }
}

/// The state key for a pipeline.
///
/// A document's `name` when it has one, otherwise the file's stem. Names are
/// preferred because a pipeline that gets renamed on disk should keep its
/// history, and a file stem is the only thing available when it has no name.
pub fn key_for(name: Option<&str>, path: &Path) -> String {
    let from_name = name.map(str::trim).filter(|name| !name.is_empty());

    let key = match from_name {
        Some(name) => name.to_string(),
        None => path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("pipeline")
            .to_string(),
    };

    sanitise(&key)
}

/// Make a key safe to use as a filename.
///
/// A pipeline's name is a user's words — it can hold slashes, colons and
/// anything else a filesystem refuses. Everything outside a conservative set
/// becomes `_`, which can collide, and collisions are acceptable here in a way
/// that path traversal is not: two similarly-named pipelines sharing a
/// watermark file is a confusing bug, whereas a name of `../../etc/passwd`
/// writing outside the workspace is a hole.
fn sanitise(key: &str) -> String {
    let cleaned: String = key
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => character,
            _ => '_',
        })
        .collect();

    // A key of all dots would name the directory itself.
    if cleaned.chars().all(|character| character == '.') {
        return "pipeline".to_string();
    }

    cleaned
}

/// Refuse a key that would escape the state directory.
///
/// `key_for` already sanitises, but `Store` is public and a caller can hand it
/// anything. Checking here means the guarantee holds at the boundary that
/// actually touches the disk rather than at the one that happens to be used.
pub(crate) fn check_key(key: &str) -> Result<(), StateError> {
    let bad = |reason: &str| {
        Err(StateError::BadKey {
            key: key.to_string(),
            reason: reason.to_string(),
        })
    };

    if key.is_empty() {
        return bad("it is empty");
    }

    if key.contains(['/', '\\']) {
        return bad("it contains a path separator");
    }

    if key.chars().all(|character| character == '.') {
        return bad("it names a directory rather than a file");
    }

    Ok(())
}

/// Now, UTC, as `YYYY-MM-DDTHH:MM:SSZ`.
///
/// Hand-rolled for the same reason the engine's `${date}` is: this workspace
/// has taken a date crate nowhere, the conversion is short and well known, and
/// it is tested against timestamps whose answers are not in doubt. A timezone
/// database would be a different argument — this is UTC only, which needs none.
pub fn now_utc() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or(0);

    from_unix_seconds(seconds)
}

/// A Unix timestamp as `YYYY-MM-DDTHH:MM:SSZ`.
fn from_unix_seconds(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let within_day = seconds.rem_euclid(86_400);

    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (
        within_day / 3_600,
        (within_day % 3_600) / 60,
        within_day % 60,
    );

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Days since 1970-01-01 to a calendar date. Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    // Shift the epoch to 0000-03-01, so leap days land at the end of the cycle.
    let shifted = days + 719_468;

    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;

    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153; // [0, 11], March-based

    let day = day_of_year - (153 * month_position + 2) / 5 + 1;
    let month = if month_position < 10 {
        month_position + 3
    } else {
        month_position - 9
    };

    (year + i64::from(month <= 2), month, day)
}

#[cfg(test)]
mod tests;
