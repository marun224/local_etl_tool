//! What the store promises, asserted against a real directory.
//!
//! The interesting cases here are all failures: a missing file, a corrupted
//! one, a file from a future version, a key that tries to leave the directory.
//! Each has a plausible wrong answer that loses rows or writes outside the
//! workspace, so each is pinned.

use super::*;

/// A store in its own temporary directory, cleaned up by the OS.
fn store(name: &str) -> (Store, PathBuf) {
    let root = std::env::temp_dir().join(format!("etl-state-tests/{name}"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("temp directory");

    (Store::at(&root), root)
}

// ---------------------------------------------------------------------------
// Reading and writing
// ---------------------------------------------------------------------------

#[test]
fn a_pipeline_that_has_never_run_has_no_watermarks_rather_than_an_error() {
    let (store, _root) = store("never-run");

    // "Never run" and "load everything" are the same statement, and a missing
    // file is how it is spelled.
    let state = store
        .load("orders")
        .expect("a missing file is not an error");

    assert!(state.watermarks.is_empty());
    assert_eq!(state.watermark("source"), None);
}

#[test]
fn a_watermark_survives_a_save_and_a_load() {
    let (store, _root) = store("round-trip");

    let mut state = PipelineState::default();
    state.advance("orders", "order_ts", "2026-03-01 12:00:00");
    store.save("daily", &state).expect("saves");

    let read = store.load("daily").expect("loads");
    let watermark = read.watermark("orders").expect("the watermark is there");

    assert_eq!(watermark.value, "2026-03-01 12:00:00");
    assert_eq!(watermark.column, "order_ts");
    assert_eq!(read.format_version, CURRENT_FORMAT_VERSION);
}

#[test]
fn saving_replaces_rather_than_merges() {
    let (store, _root) = store("replace");

    let mut first = PipelineState::default();
    first.advance("orders", "order_ts", "2026-03-01");
    first.advance("events", "seen_at", "2026-03-01");
    store.save("p", &first).expect("saves");

    let mut second = PipelineState::default();
    second.advance("orders", "order_ts", "2026-03-02");
    store.save("p", &second).expect("saves");

    // The caller owns the whole state; a store that merged would make
    // forgetting a watermark impossible.
    let read = store.load("p").expect("loads");
    assert_eq!(read.watermarks.len(), 1);
    assert_eq!(
        read.watermark("orders").map(|w| w.value.as_str()),
        Some("2026-03-02")
    );
}

#[test]
fn forgetting_a_node_reloads_it_from_the_start() {
    let mut state = PipelineState::default();
    state.advance("orders", "order_ts", "2026-03-01");

    assert!(state.forget("orders"));
    assert_eq!(state.watermark("orders"), None);
    // Forgetting something that was never there is not a failure.
    assert!(!state.forget("orders"));
}

#[test]
fn a_write_leaves_no_temporary_file_behind() {
    let (store, root) = store("no-litter");

    let mut state = PipelineState::default();
    state.advance("orders", "id", "7");
    store.save("p", &state).expect("saves");

    let left: Vec<String> = std::fs::read_dir(root.join(STATE_DIR))
        .expect("the directory exists")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();

    assert_eq!(left, ["p.json"], "a .tmp left behind is state nobody reads");
}

// ---------------------------------------------------------------------------
// Refusing what cannot be trusted
// ---------------------------------------------------------------------------

#[test]
fn a_corrupted_state_file_is_an_error_rather_than_a_fresh_start() {
    let (store, root) = store("corrupt");

    std::fs::create_dir_all(root.join(STATE_DIR)).expect("directory");
    std::fs::write(store.path_for("p"), "{ this is not json").expect("writes");

    // Treating it as "never run" would quietly reload the entire source, which
    // is the expensive, silent, wrong answer.
    let error = store.load("p").expect_err("must refuse");
    assert!(matches!(error, StateError::Malformed { .. }), "{error}");
}

#[test]
fn state_from_a_newer_version_is_refused_by_name() {
    let (store, root) = store("too-new");

    std::fs::create_dir_all(root.join(STATE_DIR)).expect("directory");
    std::fs::write(
        store.path_for("p"),
        r#"{"formatVersion": 99, "watermarks": {}}"#,
    )
    .expect("writes");

    let error = store.load("p").expect_err("must refuse");
    assert!(
        matches!(error, StateError::TooNew { found: 99, .. }),
        "{error}"
    );
}

#[test]
fn unrecognised_keys_survive_a_load_and_a_save() {
    let (store, root) = store("forwards");

    std::fs::create_dir_all(root.join(STATE_DIR)).expect("directory");
    std::fs::write(
        store.path_for("p"),
        r#"{"formatVersion": 1, "watermarks": {}, "somethingNewer": {"a": 1}}"#,
    )
    .expect("writes");

    let state = store.load("p").expect("loads");
    store.save("p", &state).expect("saves");

    let text = std::fs::read_to_string(store.path_for("p")).expect("reads");
    assert!(
        text.contains("somethingNewer"),
        "state written by a newer version must survive an older one: {text}"
    );
}

#[test]
fn a_key_cannot_escape_the_state_directory() {
    let (store, _root) = store("traversal");
    let state = PipelineState::default();

    for key in ["../escape", "..\\escape", "a/b", "", ".."] {
        assert!(
            store.save(key, &state).is_err(),
            "'{key}' must be refused at the boundary that touches the disk"
        );
        assert!(store.load(key).is_err(), "'{key}' must be refused on read");
    }
}

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

#[test]
fn a_named_pipeline_keeps_its_history_when_the_file_is_renamed() {
    let named = key_for(Some("daily orders"), Path::new("whatever.json"));
    let moved = key_for(Some("daily orders"), Path::new("some/other/place.json"));

    assert_eq!(named, moved, "the name is the identity, not the path");
}

#[test]
fn an_unnamed_pipeline_falls_back_to_its_file_stem() {
    assert_eq!(key_for(None, Path::new("a/b/orders.json")), "orders");
    assert_eq!(key_for(Some("  "), Path::new("orders.json")), "orders");
}

#[test]
fn a_name_that_would_escape_the_directory_is_flattened() {
    let key = key_for(Some("../../etc/passwd"), Path::new("p.json"));

    // What matters is that it cannot leave the directory, not that the dots
    // are gone: `.._.._etc_passwd` is an ordinary filename with no separators
    // in it, and flattening further would collide more names for no gain.
    assert!(!key.contains(['/', '\\']), "{key}");
    assert!(check_key(&key).is_ok(), "{key}");
    assert_eq!(key, ".._.._etc_passwd");
}

#[test]
fn a_name_of_nothing_but_dots_does_not_name_the_directory() {
    assert_eq!(key_for(Some("..."), Path::new("p.json")), "pipeline");
}

#[test]
fn keys_lists_what_has_state_and_nothing_else() {
    let (store, root) = store("listing");

    let mut state = PipelineState::default();
    state.advance("n", "c", "1");

    store.save("beta", &state).expect("saves");
    store.save("alpha", &state).expect("saves");
    std::fs::write(root.join(STATE_DIR).join("notes.txt"), "ignore me").expect("writes");

    assert_eq!(store.keys().expect("lists"), ["alpha", "beta"]);
}

#[test]
fn listing_a_workspace_that_has_never_run_anything_is_empty() {
    let (store, _root) = store("empty-listing");
    assert!(store.keys().expect("lists").is_empty());
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

#[test]
fn timestamps_are_utc_and_sortable() {
    // Answers that are not in doubt, which is the whole justification for
    // hand-rolling this rather than taking a date crate.
    assert_eq!(from_unix_seconds(0), "1970-01-01T00:00:00Z");
    assert_eq!(from_unix_seconds(86_399), "1970-01-01T23:59:59Z");
    assert_eq!(from_unix_seconds(86_400), "1970-01-02T00:00:00Z");
    // A leap day, which is the case a wrong implementation gets wrong.
    assert_eq!(from_unix_seconds(1_709_164_800), "2024-02-29T00:00:00Z");
    assert_eq!(from_unix_seconds(1_774_000_000), "2026-03-20T09:46:40Z");
}

#[test]
fn a_recorded_watermark_carries_when_it_was_taken() {
    let mut state = PipelineState::default();
    state.advance("orders", "order_ts", "2026-03-01");

    let at = &state.watermark("orders").expect("there").at;

    assert!(at.ends_with('Z'), "{at}");
    assert_eq!(at.len(), "1970-01-01T00:00:00Z".len(), "{at}");
    assert!(
        at.as_str() > "2020-01-01T00:00:00Z",
        "the clock is not at the epoch: {at}"
    );
}

#[test]
fn a_watermark_knows_which_column_it_measured() {
    let mut state = PipelineState::default();
    state.advance("orders", "order_ts", "2026-03-01");

    let watermark = state.watermark("orders").expect("there");

    assert!(watermark.matches_column("order_ts"));
    // Comparing next run's `order_id` against last run's `order_ts` is the
    // silent, wrong thing this exists to make detectable.
    assert!(!watermark.matches_column("order_id"));
}
