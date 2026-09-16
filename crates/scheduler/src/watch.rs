//! `"watch": "data/inbox"` — a path polled for change.
//!
//! **Polling, not native events**, deliberately. The only thing OS-level file
//! events buy is latency, and for "a file landed, run the pipeline" a ten
//! second poll is indistinguishable from instant. Native events are also
//! genuinely unreliable on network shares and virtual filesystems, which is
//! exactly where a watched inbox most often lives — so the dependency would
//! buy speed in the easy case and nothing in the hard one.
//!
//! # Stable across two polls
//!
//! A change does **not** fire on the poll that notices it. It fires on the
//! next poll that sees the same stamp again. That is what keeps a 2 GB CSV
//! still being copied into the inbox from being read half-written: while the
//! copy is running its mtime keeps moving, so the stamp keeps changing, so it
//! never settles. With the default ten second poll this costs between ten and
//! twenty seconds of latency and buys never reading a partial file.
//!
//! # The first poll never fires
//!
//! Starting the scheduler establishes a baseline rather than firing. An inbox
//! that has had files sitting in it since yesterday would otherwise be
//! reprocessed every time the scheduler restarted, which is the kind of thing
//! that turns a restart into a duplicate load.
//!
//! # What counts as a change
//!
//! For a file, its own mtime. For a directory, the newest mtime among its
//! immediate entries **and** the directory's own — the entries catch a file
//! being rewritten in place, and the directory catches one being added or
//! deleted.
//!
//! It is **not recursive**: a watch on a tree of a hundred thousand files
//! would turn every poll into a full walk, and the shape that wants that is a
//! watch per inbox rather than one at the root.
//!
//! **Only the immediate entries are guaranteed.** A subdirectory is an entry,
//! so its own mtime is part of the stamp — but whether *that* moves when
//! something changes inside it is up to the filesystem, and NTFS in
//! particular defers directory timestamp updates. Measured on Windows during
//! 8c: creating a file one level down did not register, and editing one
//! sometimes did. So nothing here promises either way, and the rule is to
//! watch the directory whose files actually matter. What *is* solid is the
//! top level, and it is solid because it does not depend on the directory's
//! clock at all — a new file is an entry with its own fresh mtime, and a
//! removed one changes the summed length.
//!
//! A path that does not exist is not an error. An inbox is often created by
//! whatever drops the first file into it, and a scheduler that refused to
//! start until it existed would be wrong about the common case.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

/// How often a watch is polled, when the schedule does not say.
pub const DEFAULT_POLL_SECONDS: u64 = 10;

/// What a watched path looks like right now.
///
/// `None` means the path does not exist. That is a real state rather than an
/// error, and it differs from every `Some`, so a directory being deleted or
/// created is itself a change.
type Stamp = Option<(u64, u32, u64)>;

/// A path polled for change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watch {
    path: PathBuf,
    poll_seconds: u64,
}

impl Watch {
    pub fn new(path: impl Into<PathBuf>, poll_seconds: Option<u64>) -> Self {
        Watch {
            path: path.into(),
            // A zero poll would spin. Anything the file says is taken as
            // written otherwise, including a one second poll on a local disk.
            poll_seconds: poll_seconds
                .filter(|seconds| *seconds > 0)
                .unwrap_or(DEFAULT_POLL_SECONDS),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn poll_seconds(&self) -> u64 {
        self.poll_seconds
    }

    /// The path this watch resolves to under a workspace root.
    ///
    /// Relative paths resolve from the workspace, the same rule the rest of
    /// the product uses, so a schedule file is portable between checkouts.
    pub fn resolve(&self, workspace: &Path) -> PathBuf {
        if self.path.is_absolute() {
            self.path.clone()
        } else {
            workspace.join(&self.path)
        }
    }
}

/// What a watch has seen, between polls.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchState {
    /// The stamp at the last poll.
    seen: Stamp,
    /// Whether a baseline has been taken at all.
    started: bool,
    /// Whether the last poll saw a change that has not yet settled.
    unsettled: bool,
}

/// What a poll concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Poll {
    /// Nothing has changed.
    Quiet,
    /// Something changed, and is not stable yet. Not a fire.
    Changing,
    /// A change has been stable across two polls. Fire.
    Settled,
    /// The baseline was taken. Never a fire; see the module docs.
    Baseline,
}

impl WatchState {
    /// Look at the path and say what it means.
    pub fn poll(&mut self, path: &Path) -> Poll {
        let stamp = stamp_of(path);

        if !self.started {
            self.started = true;
            self.seen = stamp;
            return Poll::Baseline;
        }

        if stamp != self.seen {
            // Moved since last time. Record it and wait for it to stop.
            self.seen = stamp;
            self.unsettled = true;
            return Poll::Changing;
        }

        if self.unsettled {
            // Same as last poll, and last poll was a change: it has settled.
            self.unsettled = false;
            return Poll::Settled;
        }

        Poll::Quiet
    }

    /// Whether a change is in flight, for reporting.
    pub fn is_changing(&self) -> bool {
        self.unsettled
    }
}

/// The newest mtime at a path, as (seconds, nanoseconds, size).
///
/// Size rides along because a file can be rewritten within the same mtime
/// tick — some filesystems only keep whole seconds — and a length that
/// changed is a change even when the clock did not move. It is not a
/// checksum and does not pretend to be: a same-length rewrite inside one
/// mtime tick is not detected, and the honest answer for content-addressed
/// change detection is a hash, which is not what a ten second poll over a
/// network share should be doing.
fn stamp_of(path: &Path) -> Stamp {
    let metadata = fs::metadata(path).ok()?;

    let newest = |meta: &fs::Metadata| -> (u64, u32, u64) {
        let modified = meta
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok());

        match modified {
            Some(since) => (since.as_secs(), since.subsec_nanos(), meta.len()),
            // A filesystem with no mtime at all: fall back to length, so a
            // growing file is still seen to change.
            None => (0, 0, meta.len()),
        }
    };

    if !metadata.is_dir() {
        return Some(newest(&metadata));
    }

    // The directory's own stamp catches an entry being added or removed.
    let mut best = newest(&metadata);
    let mut total_len = best.2;

    let Ok(entries) = fs::read_dir(path) else {
        return Some(best);
    };

    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };

        let stamp = newest(&meta);
        total_len = total_len.wrapping_add(stamp.2);

        if (stamp.0, stamp.1) > (best.0, best.1) {
            best = stamp;
        }
    }

    // The summed length across the directory, so a file being replaced by a
    // shorter one of the same mtime still reads as a change.
    Some((best.0, best.1, total_len))
}

#[cfg(test)]
mod tests;
