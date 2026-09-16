//! What happened, run by run.
//!
//! A record per run, appended to one file per pipeline. It is the structured
//! log and the history at the same time, deliberately: the thing `--json`
//! prints and the thing that gets stored are the same shape, so a CI job
//! parsing stdout and a person running `etl runs show` are looking at the same
//! record rather than two views that drift.
//!
//! **Append-only, and not pruned behind your back.** One JSON object per line,
//! opened for append, so a run adds to history without rewriting it — which
//! means a crash mid-write can cost the record being written and nothing that
//! came before. Nothing here deletes old records; `prune` exists and is
//! something a person runs. A record is a few hundred bytes, so a pipeline
//! running hourly for a year costs a few megabytes, and silently discarding
//! the history of a pipeline that turns out to have been wrong for a month is
//! a worse failure than a large file.
//!
//! **A record is written whether the run succeeded or not.** History that only
//! remembers successes cannot answer the question anybody actually has.

use crate::{check_key, now_utc, StateError};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// The format version this module writes.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

/// Where run history lives, relative to the workspace root.
pub const RUNS_DIR: &str = ".etl/runs";

/// How a run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Succeeded,
    /// Ran to the end, or stopped partway, having failed. The distinction
    /// between those two lives in the stages, which say which ones were
    /// skipped and why.
    Failed,
}

impl Outcome {
    pub fn name(self) -> &'static str {
        match self {
            Outcome::Succeeded => "succeeded",
            Outcome::Failed => "failed",
        }
    }
}

/// What one stage did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StageRecord {
    pub node_id: String,
    pub label: String,
    pub component_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected: Option<u64>,
    /// Why the stage did not run, if it did not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped: Option<String>,
    /// How long it took, where that number means what it looks like. Absent
    /// for most stages — see `StageOutcome::elapsed` in the engine, which is
    /// where the rule lives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u128>,
}

/// A watermark a run moved, as it was at the time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatermarkRecord {
    pub node_id: String,
    pub column: String,
    /// The mark reached, or absent when the source loaded nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// One run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRecord {
    #[serde(default, rename = "formatVersion")]
    pub format_version: u32,
    /// Sortable, and unique within a workspace: the start time to the second,
    /// plus a suffix, because two runs can start in the same second.
    pub id: String,
    /// Which pipeline, as the state key spells it.
    pub pipeline: String,
    /// The document this ran, as it was given on the command line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// When it started, UTC.
    pub started: String,
    pub elapsed_ms: u128,
    pub outcome: Outcome,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stages: Vec<StageRecord>,
    /// What the control nodes said.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    /// What went wrong, for a run that failed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<String>,
    /// What each incremental source reached.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub watermarks: Vec<WatermarkRecord>,
    /// Anything a newer version wrote.
    #[serde(
        flatten,
        default,
        skip_serializing_if = "std::collections::BTreeMap::is_empty"
    )]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

impl RunRecord {
    /// The total rows the last stage reported, which for a pipeline ending in
    /// a sink is what it wrote.
    pub fn rows_written(&self) -> Option<u64> {
        self.stages
            .iter()
            .filter_map(|stage| stage.rows)
            .next_back()
    }
}

/// A run id from a start time.
///
/// Sortable as text, which is what makes "the most recent runs" a tail rather
/// than a sort. The suffix disambiguates two runs that started inside the same
/// second — rare, but a scheduler firing several pipelines at once makes it
/// ordinary rather than rare.
pub fn new_id(started: &str, suffix: u64) -> String {
    let compact: String = started
        .chars()
        .filter(|character| character.is_ascii_digit())
        .collect();

    format!("{compact}-{:04x}", suffix % 0x1_0000)
}

/// The run history under a workspace.
#[derive(Debug, Clone)]
pub struct History {
    directory: PathBuf,
}

impl History {
    pub fn at(workspace: impl AsRef<Path>) -> Self {
        History {
            directory: workspace.as_ref().join(RUNS_DIR),
        }
    }

    pub fn path_for(&self, key: &str) -> PathBuf {
        self.directory.join(format!("{key}.jsonl"))
    }

    /// Add one run to a pipeline's history.
    ///
    /// Appends rather than rewrites, so an earlier record cannot be lost by a
    /// later write. A record that will not serialise is a bug rather than a
    /// runtime condition, so it panics rather than quietly dropping history.
    pub fn append(&self, key: &str, record: &RunRecord) -> Result<(), StateError> {
        check_key(key)?;
        let path = self.path_for(key);

        std::fs::create_dir_all(&self.directory).map_err(|source| StateError::Write {
            path: self.directory.clone(),
            source,
        })?;

        let mut writing = record.clone();
        writing.format_version = CURRENT_FORMAT_VERSION;

        let mut line = serde_json::to_string(&writing).expect("a run record serialises");
        line.push('\n');

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|source| StateError::Write {
                path: path.clone(),
                source,
            })?;

        file.write_all(line.as_bytes())
            .map_err(|source| StateError::Write { path, source })
    }

    /// Every run of one pipeline, oldest first.
    ///
    /// A line that will not parse is **skipped rather than fatal**, which is
    /// the opposite of the call made for watermark state — and deliberately so.
    /// A corrupt watermark silently changes what the next run loads, so it must
    /// stop everything; a corrupt history line costs one record of hindsight,
    /// and refusing to show the other nine hundred because of it would be the
    /// wrong trade.
    pub fn read(&self, key: &str) -> Result<Vec<RunRecord>, StateError> {
        check_key(key)?;
        let path = self.path_for(key);

        let file = match std::fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => return Err(StateError::Read { path, source }),
        };

        let mut records = Vec::new();

        for line in BufReader::new(file).lines() {
            let line = line.map_err(|source| StateError::Read {
                path: path.clone(),
                source,
            })?;

            if line.trim().is_empty() {
                continue;
            }

            if let Ok(record) = serde_json::from_str::<RunRecord>(&line) {
                records.push(record);
            }
        }

        Ok(records)
    }

    /// The most recent `limit` runs of one pipeline, newest first.
    pub fn recent(&self, key: &str, limit: usize) -> Result<Vec<RunRecord>, StateError> {
        let mut records = self.read(key)?;
        records.reverse();
        records.truncate(limit);
        Ok(records)
    }

    /// One run by id, searched across every pipeline when `key` is not given.
    pub fn find(&self, key: Option<&str>, id: &str) -> Result<Option<RunRecord>, StateError> {
        let keys = match key {
            Some(key) => vec![key.to_string()],
            None => self.keys()?,
        };

        for key in keys {
            if let Some(found) = self.read(&key)?.into_iter().find(|record| record.id == id) {
                return Ok(Some(found));
            }
        }

        Ok(None)
    }

    /// Keep the most recent `keep` runs of one pipeline and drop the rest.
    ///
    /// Rewrites the file, and so is the one operation here that can lose
    /// history. It exists only to be run deliberately; nothing calls it on a
    /// person's behalf.
    pub fn prune(&self, key: &str, keep: usize) -> Result<usize, StateError> {
        let records = self.read(key)?;
        if records.len() <= keep {
            return Ok(0);
        }

        let dropped = records.len() - keep;
        let kept = &records[dropped..];

        let mut text = String::new();
        for record in kept {
            text.push_str(&serde_json::to_string(record).expect("a run record serialises"));
            text.push('\n');
        }

        // Through a temporary and a rename, for the same reason the watermark
        // store is: a crash partway must not leave a half-file that reads as
        // "this pipeline has never run".
        let path = self.path_for(key);
        let temporary = path.with_extension("jsonl.tmp");

        std::fs::write(&temporary, text).map_err(|source| StateError::Write {
            path: temporary.clone(),
            source,
        })?;

        if let Err(source) = std::fs::rename(&temporary, &path) {
            let _ = std::fs::remove_file(&temporary);
            return Err(StateError::Write { path, source });
        }

        Ok(dropped)
    }

    /// Every pipeline with history, in order.
    pub fn keys(&self) -> Result<Vec<String>, StateError> {
        let entries = match std::fs::read_dir(&self.directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
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
            .filter(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
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

/// A run id for right now.
pub fn id_for_now(suffix: u64) -> String {
    new_id(&now_utc(), suffix)
}

#[cfg(test)]
mod tests;
