//! The workspace lock, against a real filesystem.

use super::*;

struct Scratch {
    root: PathBuf,
}

impl Scratch {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "etl-lock-{name}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_nanos())
                .unwrap_or(0)
        ));

        fs::create_dir_all(&root).expect("makes a scratch directory");

        Scratch { root }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn a_lock_can_be_taken_in_a_workspace_with_no_etl_directory_yet() {
    // A fresh clone has no `.etl/`, and the first thing anyone might run is
    // the scheduler.
    let scratch = Scratch::new("fresh");
    let lock = WorkspaceLock::acquire(&scratch.root, false).expect("takes the lock");

    assert!(lock.path().exists());
}

#[test]
fn a_second_lock_is_refused_and_names_who_holds_it() {
    let scratch = Scratch::new("second");
    let _first = WorkspaceLock::acquire(&scratch.root, false).expect("takes the lock");

    let error = WorkspaceLock::acquire(&scratch.root, false).expect_err("refused");

    let message = error.to_string();
    assert!(
        message.contains(&format!("pid {}", std::process::id())),
        "{message}"
    );
}

#[test]
fn the_advice_matches_what_the_platform_can_actually_do() {
    // On Windows the operating system holds the lock, so a refusal means a
    // scheduler really is running and `--force` would be a hammer that does
    // not help. Elsewhere a crash can leave the file behind, so the message
    // has to say how to get out of it — that one is read by somebody at 3am.
    let scratch = Scratch::new("advice");
    let _first = WorkspaceLock::acquire(&scratch.root, false).expect("takes the lock");

    let message = WorkspaceLock::acquire(&scratch.root, false)
        .expect_err("refused")
        .to_string();

    if cfg!(windows) {
        assert!(!message.contains("--force"), "{message}");
    } else {
        assert!(message.contains("--force"), "{message}");
    }
}

#[test]
fn releasing_frees_it_for_the_next_one() {
    let scratch = Scratch::new("release");

    let lock = WorkspaceLock::acquire(&scratch.root, false).expect("takes the lock");
    let path = lock.path().to_path_buf();
    lock.release();

    assert!(!path.exists(), "the lock file outlived the lock");

    WorkspaceLock::acquire(&scratch.root, false).expect("takes it again");
}

#[test]
fn dropping_frees_it_even_without_release() {
    let scratch = Scratch::new("drop");

    {
        let _lock = WorkspaceLock::acquire(&scratch.root, false).expect("takes the lock");
    }

    WorkspaceLock::acquire(&scratch.root, false).expect("takes it again");
}

#[test]
fn a_lock_file_left_behind_by_a_crash_does_not_wedge_the_workspace() {
    // The case Ctrl-C produces on every stop. On Windows the file is only a
    // lock while it is *held*, so one left on disk is simply taken again --
    // which is the whole reason the lock is a handle rather than an
    // existence check. Elsewhere it needs `--force`, and this pins which
    // platform does which rather than leaving it to be discovered.
    let scratch = Scratch::new("left-behind");

    fs::create_dir_all(scratch.root.join(".etl")).expect("makes .etl");
    fs::write(
        scratch.root.join(LOCK_PATH),
        "held
",
    )
    .expect("writes a leftover lock");

    if cfg!(windows) {
        WorkspaceLock::acquire(&scratch.root, false)
            .expect("a leftover file is not a held lock on Windows");
    } else {
        assert!(
            WorkspaceLock::acquire(&scratch.root, false).is_err(),
            "a leftover lock must be refused rather than silently stolen"
        );

        WorkspaceLock::acquire(&scratch.root, true).expect("force takes it");
    }
}

#[test]
fn force_on_a_free_workspace_is_not_an_error() {
    // So `--force` is safe to leave in a script that runs on a clean machine.
    let scratch = Scratch::new("force-free");

    WorkspaceLock::acquire(&scratch.root, true).expect("takes it");
}

#[test]
fn an_unreadable_status_file_still_reports_a_held_lock() {
    // The status file is only ever used to build a message, so a corrupt one
    // must not turn a held lock into a free one.
    let scratch = Scratch::new("corrupt-status");
    let _first = WorkspaceLock::acquire(&scratch.root, false).expect("takes the lock");

    fs::write(scratch.root.join(STATUS_PATH), "not json at all").expect("writes");

    let error = WorkspaceLock::acquire(&scratch.root, false).expect_err("still held");

    assert!(error.to_string().contains("unreadable"), "{error}");
}

#[test]
fn the_status_file_says_who_when_and_where() {
    let scratch = Scratch::new("contents");
    let _lock = WorkspaceLock::acquire(&scratch.root, false).expect("takes the lock");

    let text = fs::read_to_string(scratch.root.join(STATUS_PATH)).expect("reads");
    let holder: Holder = serde_json::from_str(&text).expect("parses");

    assert_eq!(holder.pid, std::process::id());
    assert!(holder.since.ends_with('Z'), "{}", holder.since);
}

#[test]
fn releasing_clears_the_status_file_too() {
    // A status file left behind would name a scheduler that is not running,
    // which is exactly the confusion the lock is meant to prevent.
    let scratch = Scratch::new("status-cleanup");
    let status = scratch.root.join(STATUS_PATH);

    let lock = WorkspaceLock::acquire(&scratch.root, false).expect("takes the lock");
    assert!(status.exists());

    lock.release();
    assert!(!status.exists());
}
