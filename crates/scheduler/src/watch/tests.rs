//! File-watching, against a real filesystem.
//!
//! These write real files under a temporary directory rather than faking the
//! filesystem, because what is being tested *is* what the filesystem reports.

use super::*;
use std::fs;
use std::path::PathBuf;

/// A directory of our own under the system temp, removed on drop.
struct Scratch {
    root: PathBuf,
}

impl Scratch {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "etl-watch-{name}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|since| since.as_nanos())
                .unwrap_or(0)
        ));

        fs::create_dir_all(&root).expect("makes a scratch directory");

        Scratch { root }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn write(&self, name: &str, contents: &str) {
        fs::write(self.path(name), contents).expect("writes");
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn the_first_poll_takes_a_baseline_and_does_not_fire() {
    // Otherwise every restart reprocesses an inbox that has been sitting
    // there since yesterday.
    let scratch = Scratch::new("baseline");
    scratch.write("already-here.csv", "old");

    let mut state = WatchState::default();

    assert_eq!(state.poll(&scratch.root), Poll::Baseline);
    assert_eq!(state.poll(&scratch.root), Poll::Quiet);
}

#[test]
fn a_quiet_path_stays_quiet() {
    let scratch = Scratch::new("quiet");
    let mut state = WatchState::default();

    state.poll(&scratch.root);

    for _ in 0..5 {
        assert_eq!(state.poll(&scratch.root), Poll::Quiet);
    }
}

#[test]
fn a_new_file_settles_on_the_second_poll_after_it_appears() {
    // The whole rule: notice on one poll, fire on the next. This is what
    // keeps a half-copied file from being read.
    let scratch = Scratch::new("new-file");
    let mut state = WatchState::default();

    assert_eq!(state.poll(&scratch.root), Poll::Baseline);

    scratch.write("landed.csv", "a,b\n1,2\n");

    assert_eq!(state.poll(&scratch.root), Poll::Changing);
    assert_eq!(state.poll(&scratch.root), Poll::Settled);
    // And then it is quiet again rather than firing forever.
    assert_eq!(state.poll(&scratch.root), Poll::Quiet);
}

#[test]
fn a_file_that_keeps_changing_never_settles() {
    // A large file still being written: its stamp moves on every poll, so it
    // is never read half-finished.
    let scratch = Scratch::new("still-writing");
    let mut state = WatchState::default();

    state.poll(&scratch.root);

    let mut contents = String::new();

    for _ in 0..5 {
        contents.push_str("more rows\n");
        scratch.write("growing.csv", &contents);

        assert_eq!(
            state.poll(&scratch.root),
            Poll::Changing,
            "a file still being written must not settle"
        );
    }

    // It settles once the writing stops.
    assert_eq!(state.poll(&scratch.root), Poll::Settled);
}

#[test]
fn a_deleted_file_is_a_change() {
    let scratch = Scratch::new("deleted");
    scratch.write("going.csv", "here");

    let mut state = WatchState::default();
    state.poll(&scratch.root);

    fs::remove_file(scratch.path("going.csv")).expect("removes");

    assert_eq!(state.poll(&scratch.root), Poll::Changing);
    assert_eq!(state.poll(&scratch.root), Poll::Settled);
}

#[test]
fn a_shorter_file_of_the_same_age_is_still_a_change() {
    // The case mtime alone misses on a filesystem that keeps whole seconds.
    // Length is why the stamp carries a size.
    let scratch = Scratch::new("shorter");
    scratch.write("data.csv", "a long line of content here");

    let mut state = WatchState::default();
    state.poll(&scratch.root);

    scratch.write("data.csv", "short");

    assert_eq!(state.poll(&scratch.root), Poll::Changing);
}

#[test]
fn watching_a_single_file_works_as_well_as_a_directory() {
    let scratch = Scratch::new("single");
    scratch.write("one.csv", "first");

    let target = scratch.path("one.csv");
    let mut state = WatchState::default();

    assert_eq!(state.poll(&target), Poll::Baseline);

    scratch.write("one.csv", "second and longer");

    assert_eq!(state.poll(&target), Poll::Changing);
    assert_eq!(state.poll(&target), Poll::Settled);
}

#[test]
fn a_path_that_does_not_exist_is_quiet_rather_than_an_error() {
    // An inbox is often created by whatever drops the first file into it.
    let scratch = Scratch::new("absent");
    let missing = scratch.path("not-here-yet");

    let mut state = WatchState::default();

    assert_eq!(state.poll(&missing), Poll::Baseline);
    assert_eq!(state.poll(&missing), Poll::Quiet);
}

#[test]
fn a_path_appearing_is_a_change() {
    let scratch = Scratch::new("appears");
    let coming = scratch.path("inbox");

    let mut state = WatchState::default();
    state.poll(&coming);

    fs::create_dir(&coming).expect("makes it");

    assert_eq!(state.poll(&coming), Poll::Changing);
    assert_eq!(state.poll(&coming), Poll::Settled);
}

#[test]
fn only_immediate_entries_are_watched_reliably() {
    // A watch sees its immediate entries. What happens *below* a
    // subdirectory is filesystem-dependent — it rides on the subdirectory's
    // own mtime, and NTFS defers directory timestamp updates, so the same
    // edit fires on one platform and not on another. Rather than assert a
    // direction this cannot guarantee, the contract is: watch the directory
    // whose files you care about. This test pins that contract.
    let scratch = Scratch::new("subdir-direct");
    let nested = scratch.path("sub");
    fs::create_dir(&nested).expect("makes it");

    let mut state = WatchState::default();
    assert_eq!(state.poll(&nested), Poll::Baseline);

    fs::write(nested.join("deep.csv"), "content").expect("writes");

    assert_eq!(state.poll(&nested), Poll::Changing);
    assert_eq!(state.poll(&nested), Poll::Settled);
}

#[test]
fn a_change_is_seen_through_the_entries_rather_than_the_directory_clock() {
    // Why the top-level cases are solid even where directory mtimes are not:
    // a new file is an entry with its own fresh mtime, and a deleted one
    // changes the summed length. Neither depends on the directory's clock.
    let scratch = Scratch::new("entry-driven");
    scratch.write("a.csv", "one");

    let mut state = WatchState::default();
    state.poll(&scratch.root);

    // Replace the contents without adding or removing an entry, which is the
    // case a directory mtime alone would miss entirely.
    scratch.write("a.csv", "one, considerably longer than before");

    assert_eq!(state.poll(&scratch.root), Poll::Changing);
    assert_eq!(state.poll(&scratch.root), Poll::Settled);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[test]
fn the_poll_interval_defaults_and_a_zero_is_refused() {
    assert_eq!(
        Watch::new("inbox", None).poll_seconds(),
        DEFAULT_POLL_SECONDS
    );
    assert_eq!(Watch::new("inbox", Some(2)).poll_seconds(), 2);
    // A zero poll would spin the loop.
    assert_eq!(
        Watch::new("inbox", Some(0)).poll_seconds(),
        DEFAULT_POLL_SECONDS
    );
}

#[test]
fn relative_paths_resolve_from_the_workspace() {
    let workspace = Path::new("/work/space");

    assert_eq!(
        Watch::new("data/inbox", None).resolve(workspace),
        workspace.join("data/inbox")
    );
}

#[test]
fn an_absolute_path_is_left_alone() {
    let workspace = Path::new("/work/space");
    let absolute = if cfg!(windows) {
        PathBuf::from(r"C:\inbox")
    } else {
        PathBuf::from("/inbox")
    };

    assert_eq!(
        Watch::new(absolute.clone(), None).resolve(workspace),
        absolute
    );
}
