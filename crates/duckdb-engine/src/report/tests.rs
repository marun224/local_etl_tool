//! The judgement calls in a run's output, pinned.
//!
//! Each of these is a case where the obvious formatting says something untrue:
//! a dash that means two different things, a zero that hides a passing check, a
//! timing that reads as "instant" for the stage that cost the most.

use super::*;
use crate::{SkipReason, StageFailure, StageOutcome};
use std::path::PathBuf;
use std::time::Duration;

fn stage(label: &str) -> StageOutcome {
    StageOutcome {
        node_id: label.to_lowercase().replace(' ', "_"),
        label: label.to_string(),
        component_id: "xf.filter".to_string(),
        rows: Some(12),
        rejected: None,
        skipped: None,
        elapsed: None,
    }
}

fn report(stages: Vec<StageOutcome>) -> RunReport {
    RunReport {
        stages,
        elapsed: Duration::from_millis(140),
        duckdb_bin: PathBuf::from("duckdb"),
        script: String::new(),
        spilled: 0,
        notes: Vec::new(),
        warnings: Vec::new(),
        watermarks: Vec::new(),
        checkpoints: Vec::new(),
        failures: Vec::new(),
    }
}

#[test]
fn a_stage_shows_its_rows_and_its_component() {
    let lines = report_lines(&report(vec![stage("Orders CSV")]));

    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("Orders CSV"), "{}", lines[0]);
    assert!(lines[0].contains("12 rows"), "{}", lines[0]);
    assert!(lines[0].contains("xf.filter"), "{}", lines[0]);
}

#[test]
fn a_stage_with_no_counts_shows_a_dash_and_a_skipped_one_says_why() {
    let mut uncounted = stage("Uncounted");
    uncounted.rows = None;

    let mut not_taken = stage("Branch body");
    not_taken.rows = None;
    not_taken.skipped = Some(SkipReason::NotTaken {
        node_id: "any_large".to_string(),
    });

    let lines = report_lines(&report(vec![uncounted, not_taken]));

    // The two would otherwise read identically, and they mean very different
    // things: one ran without being counted, the other never ran.
    assert!(lines[0].contains('-'), "{}", lines[0]);
    assert!(lines[1].contains("not taken: any_large"), "{}", lines[1]);
}

#[test]
fn a_quality_node_shows_zero_rejects_rather_than_hiding_them() {
    let mut passing = stage("Status is known");
    passing.component_id = "qa.accepted_values".to_string();
    passing.rejected = Some(0);

    let lines = report_lines(&report(vec![passing]));

    // Zero rejects is the result somebody ran the check to see. Hiding it would
    // make a passing check look like a node that did nothing at all.
    assert!(lines[0].contains("0 rejected"), "{}", lines[0]);
}

#[test]
fn a_stage_that_cannot_reject_shows_no_reject_column() {
    let lines = report_lines(&report(vec![stage("Orders from 2026")]));

    // `None` and `Some(0)` are different statements: cannot reject, versus
    // rejected nothing.
    assert!(!lines[0].contains("rejected"), "{}", lines[0]);
}

#[test]
fn only_a_stage_with_an_honest_timing_shows_one() {
    let untimed = stage("Lazy view");

    let mut timed = stage("Write parquet");
    timed.component_id = "snk.file.parquet".to_string();
    timed.elapsed = Some(Duration::from_millis(37));

    let lines = report_lines(&report(vec![untimed, timed]));

    // A lazy `CREATE VIEW` returns in microseconds and the work happens later,
    // at the sink that pulls it. `0 ms` beside the transform that cost the most
    // is worse than a blank.
    assert!(!lines[0].contains("ms"), "{}", lines[0]);
    assert!(lines[1].contains("37ms"), "{}", lines[1]);
}

#[test]
fn labels_line_up_in_a_column_whatever_their_length() {
    let lines = report_lines(&report(vec![stage("Short"), stage("A much longer label")]));

    // The row counts are what somebody scans down, so they have to be at the
    // same offset on every line.
    let first = lines[0].find("12 rows").expect("rows");
    let second = lines[1].find("12 rows").expect("rows");

    assert_eq!(first, second);
}

#[test]
fn failures_and_notes_follow_the_stages_in_that_order() {
    let mut with_both = report(vec![stage("Orders CSV")]);
    with_both.failures = vec![StageFailure {
        node_id: "write".to_string(),
        label: "Write parquet".to_string(),
        message: "no such directory".to_string(),
    }];
    with_both.notes = vec!["branch taken: any_large".to_string()];

    let lines = report_lines(&with_both);

    assert_eq!(lines.len(), 3);
    assert!(
        lines[1].starts_with("  ! Write parquet (write)"),
        "{}",
        lines[1]
    );
    assert!(lines[2].starts_with("  · branch taken"), "{}", lines[2]);
}

#[test]
fn warnings_come_last_and_marked_so_they_are_not_missed() {
    let mut warned = report(vec![stage("Orders CSV")]);
    warned.notes = vec!["Orders: 3 message(s) acknowledged".to_string()];
    warned.warnings = vec!["Refunds: could not be acknowledged".to_string()];

    let lines = report_lines(&warned);

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[2], "  ⚠ Refunds: could not be acknowledged");
}

#[test]
fn a_report_with_no_stages_produces_no_lines_rather_than_a_blank_one() {
    assert!(report_lines(&report(Vec::new())).is_empty());
}
