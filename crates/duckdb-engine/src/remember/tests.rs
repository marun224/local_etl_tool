//! The two rules every caller shares: what stored state still applies, and
//! that a failed run changes none of it.

use super::*;
use crate::{Checkpoint, StageFailure, Watermark};
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;

fn document(json: &str) -> PipelineDoc {
    PipelineDoc::from_json(json).expect("parses")
}

/// A stream source and an incremental CSV source, side by side.
fn two_sources() -> PipelineDoc {
    document(
        r#"{ "formatVersion": 1, "nodes": [
            { "id": "stream", "position": {"x":0,"y":0},
              "data": { "label": "S", "componentId": "src.stream.kafka", "properties": {} } },
            { "id": "table", "position": {"x":0,"y":0},
              "data": { "label": "T", "componentId": "src.file.csv", "properties": {},
                        "incremental": { "column": "order_ts" } } }
          ], "edges": [] }"#,
    )
}

fn report() -> RunReport {
    RunReport {
        stages: Vec::new(),
        elapsed: Duration::ZERO,
        duckdb_bin: PathBuf::from("duckdb"),
        script: String::new(),
        spilled: 0,
        notes: Vec::new(),
        watermarks: vec![Watermark {
            node_id: "table".into(),
            column: "order_ts".into(),
            value: Some("2026-06-01".into()),
        }],
        checkpoints: vec![Checkpoint {
            node_id: "stream".into(),
            component_id: "src.stream.kafka".into(),
            value: json!({ "offsets": { "0": 10 } }),
        }],
        failures: Vec::new(),
    }
}

#[test]
fn stored_state_that_fits_is_handed_to_compile() {
    let mut stored = PipelineState::default();
    stored.advance("table", "order_ts", "2026-03-01");
    stored.record_checkpoint("stream", "src.stream.kafka", json!(7));

    let remembering = compile_options(&two_sources(), &stored);

    assert!(
        remembering.warnings.is_empty(),
        "{:?}",
        remembering.warnings
    );
    assert_eq!(remembering.options.watermarks["table"], "2026-03-01");
    assert_eq!(remembering.options.checkpoints["stream"], json!(7));
}

#[test]
fn a_position_from_another_component_is_set_aside_and_said_so() {
    let mut stored = PipelineState::default();
    stored.record_checkpoint("stream", "src.saas.rest", json!(7));
    stored.advance("table", "order_id", "99");

    let remembering = compile_options(&two_sources(), &stored);

    assert!(remembering.options.checkpoints.is_empty());
    assert!(remembering.options.watermarks.is_empty());
    assert_eq!(remembering.warnings.len(), 2);
    assert!(
        remembering.warnings[0].contains("'order_ts' but its watermark was taken from 'order_id'")
            || remembering.warnings[1]
                .contains("'order_ts' but its watermark was taken from 'order_id'"),
        "{:?}",
        remembering.warnings
    );
    assert!(
        remembering
            .warnings
            .iter()
            .any(|w| w.contains("saved position came from 'src.saas.rest'")),
        "{:?}",
        remembering.warnings
    );
}

#[test]
fn a_successful_run_saves_watermarks_and_positions() {
    let mut stored = PipelineState::default();

    let remembered = remember(&report(), &mut stored);

    assert!(remembered.changed());
    assert_eq!(remembered.positions, ["stream"]);
    assert_eq!(stored.watermark("table").unwrap().value, "2026-06-01");
    let checkpoint = stored.checkpoint("stream").unwrap();
    assert_eq!(checkpoint.component, "src.stream.kafka");
    assert_eq!(checkpoint.value, json!({ "offsets": { "0": 10 } }));
}

#[test]
fn a_failed_run_changes_nothing_even_with_positions_in_its_report() {
    // The engine empties a failed report's checkpoints itself; this proves the
    // shared rule does not depend on that.
    let mut failed = report();
    failed.failures.push(StageFailure {
        node_id: "t".into(),
        label: "T".into(),
        message: "boom".into(),
    });
    let mut stored = PipelineState::default();
    stored.record_checkpoint(
        "stream",
        "src.stream.kafka",
        json!({ "offsets": { "0": 3 } }),
    );
    let before = stored.clone();

    let remembered = remember(&failed, &mut stored);

    assert!(!remembered.changed());
    assert_eq!(stored, before);
}

#[test]
fn a_source_that_loaded_nothing_keeps_its_mark() {
    let mut quiet = report();
    quiet.watermarks[0].value = None;
    quiet.checkpoints.clear();
    let mut stored = PipelineState::default();
    stored.advance("table", "order_ts", "2026-03-01");

    let remembered = remember(&quiet, &mut stored);

    assert!(!remembered.changed());
    assert_eq!(remembered.nothing_new, ["table"]);
    assert_eq!(stored.watermark("table").unwrap().value, "2026-03-01");
}
