//! When a workspace runs its pipelines.
//!
//! A schedule says *when*, and nothing else. It does not know how to compile a
//! pipeline or how to talk to DuckDB, and this crate does not depend on the
//! engine — the loop in [`run`] is handed a closure that runs one pipeline and
//! reports what happened. That keeps every timing rule in here testable
//! against a fake clock with no database anywhere near it, and it keeps
//! execution in the one place that already does it, which is the CLI.
//!
//! # Where a schedule lives
//!
//! In its own file — `.etl/schedules.json` by default — rather than inside a
//! pipeline document. A schedule is a property of *this workspace*, not of the
//! pipeline: the same file is a five-minute job on a developer's laptop and a
//! nightly one in production, and baking one cadence into the document would
//! force those to be two documents. It is the same arrangement contexts
//! already use, for the same reason.
//!
//! Because `.etl/` is git-ignored, a schedule file that other people can read
//! needs to live somewhere else and be pointed at — `samples/schedules.json`
//! is the committed example, and `--schedules` is the flag.
//!
//! # Three triggers
//!
//! * `{"every": "1h"}` — an [interval](every), counted from the **last
//!   recorded run** rather than from when the scheduler started, so restarting
//!   does not restart the clock.
//! * `{"cron": "0 3 * * *"}` — a [cron expression](cron), always UTC. A `tz`
//!   field is refused rather than approximated.
//! * `{"watch": "data/inbox"}` — a [path polled for change](watch), firing
//!   once it has been stable across two polls.
//!
//! # What it will not do
//!
//! **It does not catch up.** A run that overruns its next tick means that tick
//! is missed, counted, and reported — not queued. For a watermarked pipeline
//! each run already reads everything new since the last mark, so five catch-up
//! runs do exactly what one does, and a backlog that can grow without bound is
//! a worse failure than a gap somebody can see in the log.
//!
//! **It does not run two things at once.** See [`lock`].

pub mod cron;
pub mod every;
pub mod lock;
pub mod run;
pub mod watch;

pub use cron::{Cron, CronError};
pub use every::{Interval, IntervalError};
pub use lock::{LockError, WorkspaceLock};
pub use run::{Clock, Entry, Outcome, Scheduler, Summary, SystemClock, Tick};
pub use watch::{Poll, Watch, WatchState};

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// The format version this crate writes.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

/// Where schedules live, relative to the workspace root.
pub const SCHEDULES_PATH: &str = ".etl/schedules.json";

#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("could not read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{path} is not a schedule file: {source}")]
    Malformed {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error(
        "{path} was written by format version {found}, and this build understands {understood}"
    )]
    TooNew {
        path: PathBuf,
        found: u32,
        understood: u32,
    },

    #[error("the schedule at position {position} has no name")]
    Unnamed { position: usize },

    #[error("two schedules are both called '{name}'; names are how runs are reported and reset")]
    DuplicateName { name: String },

    #[error("schedule '{name}' names no pipeline to run")]
    NoPipeline { name: String },

    #[error("schedule '{name}': {source}")]
    BadTrigger {
        name: String,
        #[source]
        source: TriggerError,
    },
}

#[derive(Debug, Error)]
pub enum TriggerError {
    #[error("no trigger: give it one of \"every\", \"cron\" or \"watch\"")]
    Missing,

    #[error("more than one trigger ({found}); a schedule fires one way")]
    Several { found: String },

    #[error(
        "\"tz\": \"{zone}\" is refused. Schedules here are UTC and this build carries no \
         timezone database — one goes stale silently, and a schedule that quietly runs an \
         hour off is worse than one that will not start. Write the expression in UTC."
    )]
    TimezoneRefused { zone: String },

    #[error("\"pollSeconds\" only means something with \"watch\"")]
    PollWithoutWatch,

    #[error(transparent)]
    Interval(#[from] IntervalError),

    #[error(transparent)]
    Cron(#[from] CronError),
}

/// How a schedule decides it is time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trigger {
    /// Every so often, counted from the last run.
    Every(Interval),
    /// On a cron expression, in UTC.
    Cron(Cron),
    /// When a path changes and settles.
    Watch(Watch),
}

impl Trigger {
    /// A one-line description, for `etl schedule list`.
    pub fn describe(&self) -> String {
        match self {
            Trigger::Every(interval) => format!("every {interval}"),
            Trigger::Cron(cron) => format!("cron {cron}"),
            Trigger::Watch(watch) => {
                format!("watch {}", watch.path().display())
            }
        }
    }
}

/// The wire form of a trigger.
///
/// Deserialised as a plain struct with every field optional, rather than as an
/// untagged enum, because an untagged enum that does not match says "data did
/// not match any variant" — which is true and useless. This way the error can
/// name what was found and what was expected, and it is also the only way to
/// *see* a `tz` field in order to refuse it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTrigger {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    every: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    cron: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    watch: Option<PathBuf>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    poll_seconds: Option<u64>,

    /// Captured only so it can be refused by name. See
    /// [`TriggerError::TimezoneRefused`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tz: Option<String>,

    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    extra: BTreeMap<String, serde_json::Value>,
}

impl TryFrom<RawTrigger> for Trigger {
    type Error = TriggerError;

    fn try_from(raw: RawTrigger) -> Result<Self, Self::Error> {
        // Refused before anything else, so `{"cron": "0 3 * * *", "tz": "IST"}`
        // reports the timezone rather than parsing happily and running in UTC.
        if let Some(zone) = raw.tz {
            return Err(TriggerError::TimezoneRefused { zone });
        }

        let mut named = Vec::new();
        if raw.every.is_some() {
            named.push("every");
        }
        if raw.cron.is_some() {
            named.push("cron");
        }
        if raw.watch.is_some() {
            named.push("watch");
        }

        match named.len() {
            0 => return Err(TriggerError::Missing),
            1 => {}
            _ => {
                return Err(TriggerError::Several {
                    found: named.join(", "),
                })
            }
        }

        if raw.poll_seconds.is_some() && raw.watch.is_none() {
            return Err(TriggerError::PollWithoutWatch);
        }

        if let Some(text) = raw.every {
            return Ok(Trigger::Every(Interval::parse(&text)?));
        }

        if let Some(text) = raw.cron {
            return Ok(Trigger::Cron(Cron::parse(&text)?));
        }

        let path = raw.watch.expect("one trigger was named, and it is watch");

        Ok(Trigger::Watch(Watch::new(path, raw.poll_seconds)))
    }
}

impl From<&Trigger> for RawTrigger {
    fn from(trigger: &Trigger) -> Self {
        match trigger {
            Trigger::Every(interval) => RawTrigger {
                every: Some(interval.to_string()),
                ..RawTrigger::default()
            },
            Trigger::Cron(cron) => RawTrigger {
                cron: Some(cron.to_string()),
                ..RawTrigger::default()
            },
            Trigger::Watch(watch) => RawTrigger {
                watch: Some(watch.path().to_path_buf()),
                poll_seconds: Some(watch.poll_seconds()),
                ..RawTrigger::default()
            },
        }
    }
}

/// One pipeline, and when to run it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Schedule {
    /// What to call it. Unique within the file: it is how a run is reported
    /// and how `etl schedule list` names a row.
    pub name: String,

    /// The pipeline document to run, relative to the workspace.
    pub pipeline: PathBuf,

    /// When.
    #[serde(with = "trigger_serde")]
    pub trigger: Trigger,

    /// Off without deleting it. A disabled schedule is still listed, because
    /// a schedule that has quietly vanished is harder to debug than one that
    /// says it is off.
    #[serde(default = "yes", skip_serializing_if = "is_yes")]
    pub enabled: bool,

    /// Run in this context rather than the workspace's active one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,

    /// Parameters bound for this schedule's runs, as `--param` would.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,

    /// Anything a newer version wrote, preserved across a load and save.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl Schedule {
    /// The pipeline path, resolved against a workspace root.
    pub fn pipeline_in(&self, workspace: &Path) -> PathBuf {
        if self.pipeline.is_absolute() {
            self.pipeline.clone()
        } else {
            workspace.join(&self.pipeline)
        }
    }
}

fn yes() -> bool {
    true
}

fn is_yes(value: &bool) -> bool {
    *value
}

/// `Trigger` through its raw form, so the error messages survive serde.
mod trigger_serde {
    use super::{RawTrigger, Trigger};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(trigger: &Trigger, serializer: S) -> Result<S::Ok, S::Error> {
        RawTrigger::from(trigger).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Trigger, D::Error> {
        let raw = RawTrigger::deserialize(deserializer)?;

        Trigger::try_from(raw).map_err(serde::de::Error::custom)
    }
}

/// A workspace's schedules.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleFile {
    #[serde(default)]
    pub format_version: u32,

    #[serde(default)]
    pub schedules: Vec<Schedule>,

    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl Default for ScheduleFile {
    fn default() -> Self {
        ScheduleFile {
            format_version: CURRENT_FORMAT_VERSION,
            schedules: Vec::new(),
            extra: BTreeMap::new(),
        }
    }
}

impl ScheduleFile {
    /// Where a workspace's schedules live by default.
    pub fn path_in(workspace: &Path) -> PathBuf {
        workspace.join(SCHEDULES_PATH)
    }

    /// Read a schedule file.
    ///
    /// A file that is not there is an **empty set of schedules**, not an
    /// error — a workspace that has never scheduled anything is the normal
    /// state, and `etl schedule list` should say "none" rather than fail.
    /// This is the same call `Contexts::load` makes for the same reason.
    pub fn load(path: &Path) -> Result<Self, ScheduleError> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ScheduleFile::default())
            }
            Err(source) => {
                return Err(ScheduleError::Read {
                    path: path.to_path_buf(),
                    source,
                })
            }
        };

        let file: ScheduleFile =
            serde_json::from_str(&text).map_err(|source| ScheduleError::Malformed {
                path: path.to_path_buf(),
                source,
            })?;

        if file.format_version > CURRENT_FORMAT_VERSION {
            return Err(ScheduleError::TooNew {
                path: path.to_path_buf(),
                found: file.format_version,
                understood: CURRENT_FORMAT_VERSION,
            });
        }

        file.check()?;

        Ok(file)
    }

    /// Everything that can be said about a schedule file without running it.
    ///
    /// Trigger syntax is already checked by deserialisation — this is the
    /// part that spans fields: names present, names unique, a pipeline named.
    pub fn check(&self) -> Result<(), ScheduleError> {
        let mut seen: BTreeMap<&str, ()> = BTreeMap::new();

        for (position, schedule) in self.schedules.iter().enumerate() {
            if schedule.name.trim().is_empty() {
                return Err(ScheduleError::Unnamed { position });
            }

            if seen.insert(schedule.name.as_str(), ()).is_some() {
                return Err(ScheduleError::DuplicateName {
                    name: schedule.name.clone(),
                });
            }

            if schedule.pipeline.as_os_str().is_empty() {
                return Err(ScheduleError::NoPipeline {
                    name: schedule.name.clone(),
                });
            }
        }

        Ok(())
    }

    /// The schedules that are switched on.
    pub fn enabled(&self) -> impl Iterator<Item = &Schedule> {
        self.schedules.iter().filter(|schedule| schedule.enabled)
    }
}

#[cfg(test)]
mod tests;
