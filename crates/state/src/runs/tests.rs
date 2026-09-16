//! History is append-only, survives a bad line, and never loses a run to a
//! write it did not ask for.

use super::*;

fn history(name: &str) -> (History, PathBuf) {
    let root = std::env::temp_dir().join(format!("etl-runs-tests/{name}"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("temp directory");

    (History::at(&root), root)
}

fn record(id: &str, outcome: Outcome) -> RunRecord {
    RunRecord {
        format_version: CURRENT_FORMAT_VERSION,
        id: id.to_string(),
        pipeline: "orders".to_string(),
        path: Some("samples/pipelines/orders.json".to_string()),
        started: "2026-09-16T08:00:00Z".to_string(),
        elapsed_ms: 220,
        outcome,
        stages: vec![StageRecord {
            node_id: "orders".to_string(),
            label: "Orders".to_string(),
            component_id: "src.file.csv".to_string(),
            rows: Some(12),
            ..StageRecord::default()
        }],
        notes: Vec::new(),
        failures: Vec::new(),
        watermarks: Vec::new(),
        extra: Default::default(),
    }
}

#[test]
fn a_pipeline_that_has_never_run_has_no_history_rather_than_an_error() {
    let (history, _root) = history("empty");
    assert!(history.read("orders").expect("reads").is_empty());
    assert!(history.keys().expect("lists").is_empty());
}

#[test]
fn appending_adds_to_history_without_rewriting_it() {
    let (history, _root) = history("append");

    for index in 0..3 {
        let id = format!("2026091{index}T080000Z-0001");
        history
            .append("orders", &record(&id, Outcome::Succeeded))
            .expect("appends");
    }

    let found = history.read("orders").expect("reads");
    assert_eq!(found.len(), 3);
    // Oldest first, which is the order they were written.
    assert!(found[0].id < found[2].id);
}

#[test]
fn a_failed_run_is_recorded_too() {
    let (history, _root) = history("failures");

    let mut failed = record("20260916T080000Z-0001", Outcome::Failed);
    failed.failures = vec!["Load (load): no such file".to_string()];
    history.append("orders", &failed).expect("appends");

    let found = history.read("orders").expect("reads");

    // History that only remembers successes cannot answer the question anyone
    // actually has.
    assert_eq!(found[0].outcome, Outcome::Failed);
    assert_eq!(found[0].failures.len(), 1);
}

#[test]
fn a_corrupt_line_costs_that_record_and_nothing_else() {
    let (history, root) = history("corrupt");

    history
        .append(
            "orders",
            &record("20260916T080000Z-0001", Outcome::Succeeded),
        )
        .expect("appends");

    // A torn write, which is what a crash mid-append leaves.
    let path = root.join(RUNS_DIR).join("orders.jsonl");
    let mut text = std::fs::read_to_string(&path).expect("reads");
    text.push_str("{\"id\": \"truncated\", \"pipel\n");
    std::fs::write(&path, text).expect("writes");

    history
        .append(
            "orders",
            &record("20260916T090000Z-0002", Outcome::Succeeded),
        )
        .expect("appends after the damage");

    let found = history.read("orders").expect("reads");

    // The opposite call from watermark state, and deliberately: a corrupt
    // watermark changes what the next run loads and must stop everything; a
    // corrupt history line costs one record of hindsight.
    assert_eq!(found.len(), 2);
    assert_eq!(found[0].id, "20260916T080000Z-0001");
    assert_eq!(found[1].id, "20260916T090000Z-0002");
}

#[test]
fn recent_is_newest_first_and_bounded() {
    let (history, _root) = history("recent");

    for index in 0..5 {
        let id = format!("2026091{index}T080000Z-0001");
        history
            .append("orders", &record(&id, Outcome::Succeeded))
            .expect("appends");
    }

    let found = history.recent("orders", 2).expect("reads");

    assert_eq!(found.len(), 2);
    assert_eq!(found[0].id, "20260914T080000Z-0001", "newest first");
    assert_eq!(found[1].id, "20260913T080000Z-0001");
}

#[test]
fn a_run_can_be_found_by_id_without_naming_its_pipeline() {
    let (history, _root) = history("find");

    history
        .append(
            "alpha",
            &record("20260916T080000Z-000a", Outcome::Succeeded),
        )
        .expect("appends");
    history
        .append("beta", &record("20260916T090000Z-000b", Outcome::Failed))
        .expect("appends");

    let found = history
        .find(None, "20260916T090000Z-000b")
        .expect("searches")
        .expect("is there");

    assert_eq!(found.outcome, Outcome::Failed);
    assert!(history.find(None, "nope").expect("searches").is_none());
}

#[test]
fn pruning_keeps_the_most_recent_and_is_never_automatic() {
    let (history, _root) = history("prune");

    for index in 0..5 {
        let id = format!("2026091{index}T080000Z-0001");
        history
            .append("orders", &record(&id, Outcome::Succeeded))
            .expect("appends");
    }

    let dropped = history.prune("orders", 2).expect("prunes");
    assert_eq!(dropped, 3);

    let found = history.read("orders").expect("reads");
    assert_eq!(found.len(), 2);
    assert_eq!(found[1].id, "20260914T080000Z-0001", "the newest survives");

    // Pruning to more than there is does nothing at all.
    assert_eq!(history.prune("orders", 50).expect("prunes"), 0);
}

#[test]
fn unrecognised_keys_survive_a_read_and_a_write() {
    let (history, root) = history("forwards");

    std::fs::create_dir_all(root.join(RUNS_DIR)).expect("directory");
    std::fs::write(
        root.join(RUNS_DIR).join("orders.jsonl"),
        "{\"formatVersion\":1,\"id\":\"a\",\"pipeline\":\"orders\",\"started\":\"x\",\
         \"elapsedMs\":1,\"outcome\":\"succeeded\",\"somethingNewer\":7}\n",
    )
    .expect("writes");

    let found = history.read("orders").expect("reads");
    assert_eq!(found.len(), 1);

    history.prune("orders", 1).expect("prunes nothing");
    let text = std::fs::read_to_string(root.join(RUNS_DIR).join("orders.jsonl")).expect("reads");
    assert!(text.contains("somethingNewer"), "{text}");
}

#[test]
fn a_key_cannot_escape_the_history_directory() {
    let (history, _root) = history("traversal");
    let one = record("a", Outcome::Succeeded);

    for key in ["../escape", "a/b", "", ".."] {
        assert!(history.append(key, &one).is_err(), "{key} must be refused");
        assert!(history.read(key).is_err(), "{key} must be refused on read");
    }
}

#[test]
fn run_ids_sort_in_the_order_the_runs_happened() {
    let earlier = new_id("2026-09-16T08:00:00Z", 1);
    let later = new_id("2026-09-16T08:00:01Z", 1);

    assert!(earlier < later, "{earlier} then {later}");
    assert_eq!(earlier, "20260916080000-0001");

    // Two runs in the same second are still distinct, which a scheduler
    // firing several pipelines at once makes ordinary rather than rare.
    assert_ne!(
        new_id("2026-09-16T08:00:00Z", 1),
        new_id("2026-09-16T08:00:00Z", 2)
    );
}
