//! What the CLI does beyond parsing arguments and printing.
//!
//! Most of this crate is a shell: read a file, call the engine, print what came
//! back. That part is covered where the behaviour lives, and asserting it again
//! here would only pin the wording of a message.
//!
//! Four things are not a shell, and this file is about those:
//!
//! * [`ConsoleWorkspace`] — the seam `etl-console` is built around. Every route
//!   in that crate is tested against a fake; this is the real one, and the only
//!   place a name off a socket becomes a path on disk.
//! * [`collect_pipelines`] — what the console considers to be a pipeline,
//!   decided by scanning a folder rather than by reading a manifest.
//! * [`watermarks_for`] and [`Settings::for_schedule`] — two precedence rules
//!   that decide what a run reads. Getting either wrong loses rows or reloads
//!   them, and neither failure announces itself.
//! * [`record_of`] — the report a run hands to history, and to the console.
//!
//! None of these need DuckDB: compiling is pure, and every test here stops at
//! the point where a process would be spawned.

use super::*;
use etl_console::Workspace as _;
use etl_duckdb_engine::{StageFailure, StageOutcome, Watermark};
use etl_metadata::PipelineDoc;

// ---------------------------------------------------------------------------
// Scaffolding
// ---------------------------------------------------------------------------

/// An empty workspace of its own, cleaned up by the OS.
///
/// The same shape `etl-state`'s tests use: a named directory under the system
/// temp, removed before it is made, so a failed run leaves nothing that a later
/// one has to reason about.
fn workspace(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("etl-cli-tests/{name}"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("temp directory");

    root
}

fn settings_for(root: &Path) -> Settings {
    Settings {
        workspace: Some(root.to_path_buf()),
        ..Default::default()
    }
}

fn console_for(root: &Path) -> ConsoleWorkspace {
    ConsoleWorkspace {
        settings: settings_for(root),
        duckdb: None,
        counts: false,
        running: std::sync::Mutex::new(()),
    }
}

/// Write a file, making its parents first.
fn write(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("parent directory");
    }

    std::fs::write(path, text).expect("write");
}

/// A document that compiles: one source, no sink.
///
/// No sink is a warning rather than an error, which keeps this the smallest
/// thing that reaches a plan without needing edges or handles.
fn one_source(name: &str) -> String {
    format!(
        r#"{{
  "formatVersion": 1,
  "name": "{name}",
  "nodes": [
    {{
      "id": "read",
      "type": "source",
      "position": {{ "x": 0, "y": 0 }},
      "data": {{
        "label": "Orders",
        "componentId": "src.file.csv",
        "properties": {{ "path": "data/orders.csv", "header": true }}
      }}
    }}
  ],
  "edges": []
}}"#
    )
}

/// A document that parses and will not compile: a node naming no component.
fn broken(name: &str) -> String {
    format!(
        r#"{{
  "formatVersion": 1,
  "name": "{name}",
  "nodes": [
    {{
      "id": "read",
      "type": "source",
      "position": {{ "x": 0, "y": 0 }},
      "data": {{ "label": "Orders" }}
    }}
  ],
  "edges": []
}}"#
    )
}

fn names_found(root: &Path) -> Vec<String> {
    console_for(root)
        .documents()
        .into_iter()
        .map(|(name, _, _)| name)
        .collect()
}

// ---------------------------------------------------------------------------
// What counts as a pipeline
// ---------------------------------------------------------------------------

#[test]
fn a_pipeline_in_the_workspace_root_is_found() {
    let root = workspace("scan-root");
    write(&root.join("orders.json"), &one_source("orders"));

    assert_eq!(names_found(&root), vec!["orders"]);
}

#[test]
fn a_document_is_named_by_its_name_not_its_filename() {
    let root = workspace("scan-name");
    // The two disagree on purpose: the key has to follow the document, because
    // that is where this pipeline's run history was written.
    write(&root.join("on-disk.json"), &one_source("in-document"));

    let found = console_for(&root).documents();

    assert_eq!(found[0].0, "in-document");
    assert_eq!(found[0].1, root.join("on-disk.json"));
}

#[test]
fn a_document_with_no_name_falls_back_to_its_filename() {
    let root = workspace("scan-unnamed");
    write(
        &root.join("unnamed.json"),
        r#"{"nodes":[{"id":"read","type":"source","position":{"x":0,"y":0},
           "data":{"label":"Orders","componentId":"src.file.csv"}}],"edges":[]}"#,
    );

    assert_eq!(names_found(&root), vec!["unnamed"]);
}

#[test]
fn json_that_is_not_a_pipeline_is_not_one() {
    let root = workspace("scan-not-pipelines");
    write(&root.join("orders.json"), &one_source("orders"));
    // The two files a workspace always has beside its pipelines. Telling them
    // apart by shape rather than by a list of excluded names is the point: the
    // list would need a line every time somebody invented a new one.
    write(&root.join("contexts.json"), r#"{"contexts":{"dev":{}}}"#);
    write(&root.join("schedules.json"), r#"{"schedules":[]}"#);
    write(
        &root.join("notes.json"),
        r#"["nothing","to","do","with","it"]"#,
    );

    assert_eq!(names_found(&root), vec!["orders"]);
}

#[test]
fn a_document_with_no_nodes_is_not_a_pipeline() {
    let root = workspace("scan-empty");
    write(&root.join("empty.json"), r#"{"nodes":[],"edges":[]}"#);

    assert!(names_found(&root).is_empty());
}

#[test]
fn state_and_build_directories_are_skipped() {
    let root = workspace("scan-skipped");
    write(&root.join("orders.json"), &one_source("orders"));

    // `.etl/` is the one that matters: it holds run history as JSON lines, and
    // reading it on every page refresh would cost the console a file read per
    // run ever recorded.
    write(&root.join(".etl/runs/orders.json"), &one_source("history"));
    write(&root.join("target/debug/thing.json"), &one_source("built"));
    write(
        &root.join("node_modules/pkg/fixture.json"),
        &one_source("vendored"),
    );
    write(&root.join(".hidden/secret.json"), &one_source("hidden"));

    assert_eq!(names_found(&root), vec!["orders"]);
}

#[test]
fn the_scan_stops_at_four_levels_down() {
    let root = workspace("scan-depth");
    write(&root.join("a/b/c/d/deep.json"), &one_source("deep"));
    write(&root.join("a/b/c/d/e/deeper.json"), &one_source("deeper"));

    // Bounded rather than unbounded, so a workspace that happens to sit above a
    // large tree does not turn a page refresh into a full-disk walk.
    assert_eq!(names_found(&root), vec!["deep"]);
}

#[test]
fn documents_come_back_in_name_order() {
    let root = workspace("scan-order");
    // Written in an order the filesystem might well hand back.
    write(&root.join("zebra.json"), &one_source("zebra"));
    write(&root.join("apple.json"), &one_source("apple"));
    write(&root.join("nested/mango.json"), &one_source("mango"));

    assert_eq!(names_found(&root), vec!["apple", "mango", "zebra"]);
}

// ---------------------------------------------------------------------------
// A name off the network becoming a path
// ---------------------------------------------------------------------------

#[test]
fn a_name_the_workspace_does_not_hold_is_not_found() {
    let root = workspace("locate-unknown");
    write(&root.join("orders.json"), &one_source("orders"));

    let failure = console_for(&root)
        .locate("customers")
        .expect_err("no such pipeline");

    assert_eq!(failure.status, 404);
}

#[test]
fn a_name_that_climbs_out_of_the_workspace_is_a_404_rather_than_a_read() {
    let root = workspace("locate-traversal");
    write(&root.join("orders.json"), &one_source("orders"));

    // `locate` resolves by looking the name up in the workspace's own list, so
    // there is no join for any of these to escape through. What is being pinned
    // is that they are *not found*, rather than found-and-then-refused: a
    // refusal is a check somebody can later decide to relax.
    let console = console_for(&root);

    for name in [
        "../../../etc/passwd",
        "..\\..\\..\\Windows\\win.ini",
        "/etc/passwd",
        "C:\\Windows\\win.ini",
        "orders.json",
    ] {
        match console.locate(name) {
            Ok(path) => panic!("'{name}' located {}", path.display()),
            Err(failure) => assert_eq!(failure.status, 404, "for '{name}'"),
        }
    }
}

#[test]
fn a_name_that_is_in_the_workspace_resolves_to_its_file() {
    let root = workspace("locate-found");
    write(&root.join("nested/orders.json"), &one_source("orders"));

    let path = console_for(&root).locate("orders").expect("found");

    assert_eq!(path, root.join("nested/orders.json"));
}

// ---------------------------------------------------------------------------
// What the console lists
// ---------------------------------------------------------------------------

#[test]
fn the_label_is_the_workspace_directory() {
    let root = workspace("label");

    assert_eq!(console_for(&root).label(), "label");
}

#[test]
fn a_pipeline_that_will_not_compile_is_listed_with_its_problem() {
    let root = workspace("listing-problem");
    write(&root.join("good.json"), &one_source("good"));
    write(&root.join("bad.json"), &broken("bad"));

    let summaries = console_for(&root).pipelines().expect("lists");

    assert_eq!(summaries.len(), 2);

    let bad = &summaries[0];
    assert_eq!(bad.name, "bad");
    assert_eq!(bad.stages, None);
    // Which pipeline will not run is the thing worth knowing before 3am, so a
    // broken one is a row that says so rather than a failed request.
    assert!(
        bad.problem.is_some(),
        "a broken pipeline carries its reason"
    );

    let good = &summaries[1];
    assert_eq!(good.name, "good");
    assert_eq!(good.stages, Some(1));
    assert_eq!(good.problem, None);
}

#[test]
fn a_listed_path_is_relative_to_the_workspace_and_uses_forward_slashes() {
    let root = workspace("listing-path");
    write(&root.join("nested/orders.json"), &one_source("orders"));

    let summaries = console_for(&root).pipelines().expect("lists");

    // The page shows this, and a console showing a backslash path that the same
    // product writes with forward slashes everywhere else reads as a bug.
    assert_eq!(summaries[0].path, "nested/orders.json");
}

#[test]
fn a_listing_carries_the_last_run_when_there_is_one() {
    let root = workspace("listing-history");
    write(&root.join("orders.json"), &one_source("orders"));

    state::History::at(&root)
        .append(
            "orders",
            &failed_record(
                "r1".to_string(),
                "orders",
                Path::new("orders.json"),
                "2026-09-16T10:00:00Z".to_string(),
                "it did not work".to_string(),
            ),
        )
        .expect("appends");

    let summaries = console_for(&root).pipelines().expect("lists");

    assert_eq!(summaries[0].last_outcome.as_deref(), Some("failed"));
    assert_eq!(
        summaries[0].last_run.as_deref(),
        Some("2026-09-16T10:00:00Z")
    );
}

#[test]
fn a_pipeline_that_has_never_run_has_no_last_run() {
    let root = workspace("listing-no-history");
    write(&root.join("orders.json"), &one_source("orders"));

    let summaries = console_for(&root).pipelines().expect("lists");

    assert_eq!(summaries[0].last_outcome, None);
    assert_eq!(summaries[0].last_run, None);
}

#[test]
fn runs_come_back_newest_first_across_every_pipeline_and_stop_at_the_limit() {
    let root = workspace("runs-order");
    write(&root.join("orders.json"), &one_source("orders"));
    write(&root.join("customers.json"), &one_source("customers"));

    let history = state::History::at(&root);

    for (key, started) in [
        ("orders", "2026-09-16T10:00:00Z"),
        ("customers", "2026-09-16T11:00:00Z"),
        ("orders", "2026-09-16T12:00:00Z"),
    ] {
        history
            .append(
                key,
                &failed_record(
                    format!("{key}-{started}"),
                    key,
                    Path::new("p.json"),
                    started.to_string(),
                    "no".to_string(),
                ),
            )
            .expect("appends");
    }

    let recent = console_for(&root).runs(None, 2).expect("reads");

    // Sorted as strings, which is chronological because `now_utc` writes a
    // fixed-width UTC timestamp. That is the property being leaned on here.
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0].started, "2026-09-16T12:00:00Z");
    assert_eq!(recent[1].started, "2026-09-16T11:00:00Z");
}

#[test]
fn runs_for_one_pipeline_leave_the_others_out() {
    let root = workspace("runs-filtered");
    write(&root.join("orders.json"), &one_source("orders"));
    write(&root.join("customers.json"), &one_source("customers"));

    let history = state::History::at(&root);

    for key in ["orders", "customers"] {
        history
            .append(
                key,
                &failed_record(
                    key.to_string(),
                    key,
                    Path::new("p.json"),
                    "2026-09-16T10:00:00Z".to_string(),
                    "no".to_string(),
                ),
            )
            .expect("appends");
    }

    let recent = console_for(&root).runs(Some("orders"), 10).expect("reads");

    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].pipeline, "orders");
}

#[test]
fn runs_for_a_pipeline_that_is_not_here_is_a_404() {
    let root = workspace("runs-unknown");

    let failure = console_for(&root)
        .runs(Some("nothing"), 10)
        .expect_err("no such pipeline");

    assert_eq!(failure.status, 404);
}

#[test]
fn a_run_id_that_was_never_recorded_is_a_404() {
    let root = workspace("run-unknown-id");

    let failure = console_for(&root).run("nope").expect_err("no such run");

    assert_eq!(failure.status, 404);
}

#[test]
fn lineage_for_a_pipeline_that_will_not_compile_is_a_422_not_a_404() {
    let root = workspace("lineage-broken");
    write(&root.join("bad.json"), &broken("bad"));

    let failure = console_for(&root)
        .lineage("bad")
        .expect_err("will not compile");

    // The difference matters to whoever is reading the console: 404 means look
    // for the file, 422 means look at the pipeline.
    assert_eq!(failure.status, 422);
}

#[test]
fn lineage_for_a_pipeline_that_compiles_is_a_document() {
    let root = workspace("lineage-good");
    write(&root.join("orders.json"), &one_source("orders"));

    let found = console_for(&root).lineage("orders").expect("compiles");

    assert!(found.is_object() || found.is_array(), "got {found}");
}

// ---------------------------------------------------------------------------
// relative_to
// ---------------------------------------------------------------------------

#[test]
fn a_path_outside_the_workspace_is_shown_whole() {
    let displayed = relative_to(
        Path::new("/work/space"),
        Path::new("/elsewhere/orders.json"),
    );

    // Nothing to strip, so nothing is stripped. Silently showing a bare
    // filename would make two different files look like the same one.
    assert!(
        displayed.ends_with("elsewhere/orders.json"),
        "got {displayed}"
    );
}

// ---------------------------------------------------------------------------
// Which watermarks still apply
// ---------------------------------------------------------------------------

/// A document with one incremental source watching `column`.
fn watching(column: &str) -> PipelineDoc {
    let text = format!(
        r#"{{
  "nodes": [
    {{
      "id": "read",
      "type": "source",
      "position": {{ "x": 0, "y": 0 }},
      "data": {{
        "label": "Orders",
        "componentId": "src.file.csv",
        "properties": {{ "path": "orders.csv" }},
        "incremental": {{ "column": "{column}" }}
      }}
    }}
  ],
  "edges": []
}}"#
    );

    PipelineDoc::from_json(&text).expect("a document")
}

#[test]
fn a_stored_watermark_on_the_same_column_is_used() {
    let document = watching("order_ts");

    let mut stored = state::PipelineState::default();
    stored.advance("read", "order_ts", "2026-03-01 12:00:00");

    let marks = watermarks_for(&document, &stored);

    assert_eq!(
        marks.get("read").map(String::as_str),
        Some("2026-03-01 12:00:00")
    );
}

#[test]
fn changing_the_watched_column_starts_over_rather_than_comparing_the_wrong_one() {
    // The node now watches `updated_at`; the stored mark was taken from
    // `order_ts`. Carrying the old value across would produce a predicate that
    // means something nobody wrote, and would skip rows without saying so.
    let document = watching("updated_at");

    let mut stored = state::PipelineState::default();
    stored.advance("read", "order_ts", "2026-03-01 12:00:00");

    let marks = watermarks_for(&document, &stored);

    assert!(
        marks.is_empty(),
        "reloading everything is the safe direction"
    );
}

#[test]
fn a_node_that_is_not_incremental_contributes_no_watermark() {
    let document = PipelineDoc::from_json(&one_source("orders")).expect("a document");

    let mut stored = state::PipelineState::default();
    // State left behind by a version of this pipeline that *was* incremental.
    stored.advance("read", "order_ts", "2026-03-01");

    assert!(watermarks_for(&document, &stored).is_empty());
}

#[test]
fn an_incremental_node_with_nothing_stored_yet_contributes_nothing() {
    let document = watching("order_ts");

    let marks = watermarks_for(&document, &state::PipelineState::default());

    // "Never run" and "read everything" are the same statement, and an absent
    // entry is how the compiler is told so.
    assert!(marks.is_empty());
}

// ---------------------------------------------------------------------------
// Settings, and what a scheduled run inherits
// ---------------------------------------------------------------------------

fn schedule_from(json: &str) -> sched::Schedule {
    serde_json::from_str(json).expect("a schedule")
}

#[test]
fn a_command_line_param_is_bound_after_the_schedules_own() {
    let schedule = schedule_from(
        r#"{"name":"nightly","pipeline":"p.json","trigger":{"every":"15m"},
            "params":{"since":"2026-01-01"}}"#,
    );

    let settings = Settings {
        params: vec!["since=2026-06-01".to_string()],
        ..Default::default()
    }
    .for_schedule(&schedule);

    // The schedule's binding is still present and still first. Precedence is
    // last-wins once the resolver binds them, which the next test pins against
    // the resolver itself rather than against this ordering.
    assert_eq!(
        settings.params,
        vec![
            "since=2026-01-01".to_string(),
            "since=2026-06-01".to_string()
        ]
    );
}

#[test]
fn that_precedence_holds_through_the_resolver_and_not_only_in_the_list() {
    let root = workspace("param-precedence");

    let settings = Settings {
        params: vec![
            "since=2026-01-01".to_string(),
            "since=2026-06-01".to_string(),
        ],
        ..settings_for(&root)
    };

    let document = PipelineDoc::from_json(
        r#"{
  "parameters": { "since": { "type": "string" } },
  "nodes": [
    { "id": "read", "type": "source", "position": { "x": 0, "y": 0 },
      "data": { "label": "Orders", "componentId": "src.file.csv",
                "properties": { "path": "orders-${since}.csv" } } }
  ],
  "edges": []
}"#,
    )
    .expect("a document");

    let resolver = settings.resolver().expect("a resolver");
    let resolved = params::resolve(&document, &resolver).expect("resolves");

    let properties = resolved.document.nodes[0]
        .data
        .properties
        .as_ref()
        .expect("properties");

    assert_eq!(properties["path"], "orders-2026-06-01.csv");
}

#[test]
fn a_schedule_supplies_its_context_unless_one_was_asked_for() {
    let schedule = schedule_from(
        r#"{"name":"nightly","pipeline":"p.json","trigger":{"every":"15m"},"context":"prod"}"#,
    );

    let inherited = Settings::default().for_schedule(&schedule);
    assert_eq!(inherited.context.as_deref(), Some("prod"));

    let overridden = Settings {
        context: Some("dev".to_string()),
        ..Default::default()
    }
    .for_schedule(&schedule);

    // An explicit `--context` on `schedule start` is somebody saying what they
    // want now, over what the file said earlier.
    assert_eq!(overridden.context.as_deref(), Some("dev"));
}

#[test]
fn a_schedule_without_a_context_leaves_the_workspaces_active_one_alone() {
    let schedule = schedule_from(r#"{"name":"n","pipeline":"p.json","trigger":{"every":"15m"}}"#);

    assert_eq!(Settings::default().for_schedule(&schedule).context, None);
}

#[test]
fn schedules_are_read_from_the_workspace_unless_a_file_was_given() {
    let root = workspace("schedules-path");

    let default = settings_for(&root).schedules_path();
    assert_eq!(default, sched::ScheduleFile::path_in(&root));
    assert!(default.starts_with(&root));

    let explicit = Settings {
        schedules: Some(PathBuf::from("elsewhere/schedules.json")),
        ..settings_for(&root)
    }
    .schedules_path();

    assert_eq!(explicit, PathBuf::from("elsewhere/schedules.json"));
}

// ---------------------------------------------------------------------------
// The record a run leaves behind
// ---------------------------------------------------------------------------

fn stage(node_id: &str, rows: Option<u64>) -> StageOutcome {
    StageOutcome {
        node_id: node_id.to_string(),
        label: format!("{node_id} label"),
        component_id: "src.file.csv".to_string(),
        rows,
        rejected: None,
        skipped: None,
        elapsed: None,
    }
}

fn report_of(stages: Vec<StageOutcome>, failures: Vec<StageFailure>) -> RunReport {
    RunReport {
        stages,
        elapsed: std::time::Duration::from_millis(140),
        duckdb_bin: PathBuf::from("duckdb"),
        script: "SELECT 1".to_string(),
        spilled: 0,
        notes: vec!["a note".to_string()],
        warnings: Vec::new(),
        watermarks: Vec::new(),
        checkpoints: Vec::new(),
        failures,
    }
}

fn record_from(report: &RunReport) -> state::RunRecord {
    record_of(
        "r1".to_string(),
        "orders",
        Path::new("orders.json"),
        "2026-09-16T10:00:00Z".to_string(),
        report,
    )
}

#[test]
fn a_report_with_no_failures_is_recorded_as_succeeded() {
    let record = record_from(&report_of(vec![stage("read", Some(12))], Vec::new()));

    assert_eq!(record.outcome, state::Outcome::Succeeded);
    assert_eq!(record.elapsed_ms, 140);
    assert_eq!(record.stages.len(), 1);
    assert_eq!(record.stages[0].rows, Some(12));
    assert_eq!(record.notes, vec!["a note".to_string()]);
    assert!(record.failures.is_empty());
}

#[test]
fn a_warning_is_kept_and_the_run_still_succeeded() {
    let mut report = report_of(vec![stage("read", Some(12))], Vec::new());
    report.warnings = vec!["Orders: 3 message(s) will be delivered again".to_string()];
    let record = record_from(&report);

    assert_eq!(record.outcome, state::Outcome::Succeeded);
    assert_eq!(record.warnings, report.warnings);
    let json = serde_json::to_value(&record).unwrap();
    assert_eq!(
        json["warnings"][0],
        "Orders: 3 message(s) will be delivered again"
    );
}

#[test]
fn a_report_carrying_a_failure_is_recorded_as_failed_even_though_it_finished() {
    // `continueOnFailure` is the whole reason a report can come back at all
    // from a run that went wrong. It must not be read as success because the
    // call returned `Ok`.
    let record = record_from(&report_of(
        vec![stage("read", Some(12))],
        vec![StageFailure {
            node_id: "write".to_string(),
            label: "Write parquet".to_string(),
            message: "no such directory".to_string(),
        }],
    ));

    assert_eq!(record.outcome, state::Outcome::Failed);
    assert_eq!(
        record.failures,
        vec!["Write parquet (write): no such directory".to_string()]
    );
}

#[test]
fn a_records_watermarks_carry_the_absent_ones_too() {
    let mut report = report_of(vec![stage("read", Some(0))], Vec::new());
    report.watermarks = vec![
        Watermark {
            node_id: "read".to_string(),
            column: "order_ts".to_string(),
            value: None,
        },
        Watermark {
            node_id: "events".to_string(),
            column: "seen_at".to_string(),
            value: Some("2026-03-01".to_string()),
        },
    ];

    let record = record_from(&report);

    // `None` means "the source had nothing new", which is the ordinary outcome
    // of an incremental pipeline and is worth being able to read back.
    assert_eq!(record.watermarks.len(), 2);
    assert_eq!(record.watermarks[0].value, None);
    assert_eq!(record.watermarks[1].value.as_deref(), Some("2026-03-01"));
}

#[test]
fn a_run_that_never_produced_a_report_is_still_recorded() {
    let record = failed_record(
        "r1".to_string(),
        "orders",
        Path::new("orders.json"),
        "2026-09-16T10:00:00Z".to_string(),
        "the pipeline would not compile".to_string(),
    );

    assert_eq!(record.outcome, state::Outcome::Failed);
    assert_eq!(record.elapsed_ms, 0);
    assert!(record.stages.is_empty());
    assert_eq!(
        record.failures,
        vec!["the pipeline would not compile".to_string()]
    );
}

// ---------------------------------------------------------------------------
// Saving watermarks
// ---------------------------------------------------------------------------

#[test]
fn a_source_that_loaded_nothing_keeps_the_mark_it_had() {
    let root = workspace("watermark-nothing-new");
    let settings = settings_for(&root);

    let store = state::Store::at(&root);
    let mut stored = state::PipelineState::default();
    stored.advance("read", "order_ts", "2026-03-01");
    store.save("orders", &stored).expect("saves");

    let mut report = report_of(Vec::new(), Vec::new());
    report.watermarks = vec![Watermark {
        node_id: "read".to_string(),
        column: "order_ts".to_string(),
        value: None,
    }];

    save_state(&settings, "orders", &report, true).expect("saves");

    let after = store.load("orders").expect("loads");

    assert_eq!(
        after.watermark("read").expect("still there").value,
        "2026-03-01"
    );
}

#[test]
fn a_source_that_loaded_something_moves_its_mark() {
    let root = workspace("watermark-advance");
    let settings = settings_for(&root);

    let mut report = report_of(Vec::new(), Vec::new());
    report.watermarks = vec![Watermark {
        node_id: "read".to_string(),
        column: "order_ts".to_string(),
        value: Some("2026-06-01".to_string()),
    }];

    save_state(&settings, "orders", &report, true).expect("saves");

    let after = state::Store::at(&root).load("orders").expect("loads");
    let mark = after.watermark("read").expect("recorded");

    assert_eq!(mark.value, "2026-06-01");
    assert_eq!(mark.column, "order_ts");
}

#[test]
fn a_pipeline_with_no_watermarks_writes_no_state_file() {
    let root = workspace("watermark-none");
    let settings = settings_for(&root);

    save_state(
        &settings,
        "orders",
        &report_of(Vec::new(), Vec::new()),
        true,
    )
    .expect("nothing to do");

    // Not merely empty — absent. A state file for a pipeline that has no
    // watermarks is a file somebody later has to explain.
    assert!(!state::Store::at(&root).path_for("orders").exists());
}

// ---------------------------------------------------------------------------
// What `etl build` refuses
// ---------------------------------------------------------------------------

#[test]
fn a_document_with_no_incremental_source_has_nothing_to_refuse() {
    let document = PipelineDoc::from_json(&one_source("orders")).expect("a document");

    assert!(incremental_nodes(&document).is_empty());
}

#[test]
fn every_incremental_node_is_named_so_the_build_note_can_say_which() {
    let document = PipelineDoc::from_json(
        r#"{
  "nodes": [
    { "id": "read_orders", "type": "source", "position": { "x": 0, "y": 0 },
      "data": { "label": "Orders", "componentId": "src.file.csv",
                "incremental": { "column": "order_ts" } } },
    { "id": "read_events", "type": "source", "position": { "x": 0, "y": 0 },
      "data": { "label": "Events", "componentId": "src.file.csv",
                "incremental": { "column": "seen_at" } } },
    { "id": "read_static", "type": "source", "position": { "x": 0, "y": 0 },
      "data": { "label": "Customers", "componentId": "src.file.csv" } }
  ],
  "edges": []
}"#,
    )
    .expect("a document");

    // Named rather than counted: "two nodes load incrementally" leaves somebody
    // hunting a canvas for which two.
    assert_eq!(
        incremental_nodes(&document),
        vec!["read_orders", "read_events"]
    );
}

#[test]
fn a_piped_secret_loses_what_the_piping_added_and_nothing_else() {
    assert_eq!(stdin_secret("etl-secret\r\n"), "etl-secret");
    assert_eq!(stdin_secret("\u{feff}etl-secret\r\n"), "etl-secret");
    // Spaces are the user's: a password may start or end with one.
    assert_eq!(stdin_secret(" pass word \n"), " pass word ");
}

#[test]
fn a_stream_source_is_named_so_the_build_note_can_say_which() {
    let document = PipelineDoc::from_json(
        r#"{
  "nodes": [
    { "id": "topic", "type": "source", "position": { "x": 0, "y": 0 },
      "data": { "label": "Orders", "componentId": "src.stream.kafka" } },
    { "id": "file", "type": "source", "position": { "x": 0, "y": 0 },
      "data": { "label": "Customers", "componentId": "src.file.csv" } }
  ],
  "edges": []
}"#,
    )
    .expect("a document");

    assert_eq!(stream_nodes(&document), vec!["topic"]);
}

// ---------------------------------------------------------------------------
// Which platform an artifact is built for
// ---------------------------------------------------------------------------

#[test]
fn this_machine_is_its_own_target() {
    let host = Target::host();

    assert!(host.is_host);
    // Named the way DuckDB names platforms, because the extension directory
    // layout is DuckDB's and keys on exactly these strings.
    assert_eq!(host.platform, Target::named(&host.platform).platform);
    assert!(Target::named(&host.platform).is_host);
}

#[test]
fn naming_another_platform_is_not_the_host() {
    let other = if Target::host().platform == "linux_amd64" {
        "windows_amd64"
    } else {
        "linux_amd64"
    };

    assert!(!Target::named(other).is_host);
}

#[test]
fn only_a_windows_target_gets_an_exe_suffix() {
    assert_eq!(Target::named("windows_amd64").exe_suffix(), ".exe");
    assert_eq!(Target::named("windows_arm64").exe_suffix(), ".exe");
    assert_eq!(Target::named("linux_amd64").exe_suffix(), "");
    assert_eq!(Target::named("osx_arm64").exe_suffix(), "");
}

#[test]
fn a_cross_targets_runner_and_engine_are_kept_out_of_the_hosts_path() {
    let root = Path::new("/work");
    let target = Target::named("linux_amd64");

    assert_eq!(
        target.runner_path(root),
        Path::new("/work/tools/runners/linux_amd64/etl-runner")
    );

    // Under `targets/` rather than beside the host's copy: the executor finds
    // its engine by searching upward for `tools/duckdb/`, and a Linux binary
    // sitting where it looks would be found and then fail to run.
    assert_eq!(
        target.engine_path(root),
        Path::new("/work/tools/duckdb/targets/linux_amd64/duckdb")
    );
}

#[test]
fn a_windows_targets_paths_carry_the_suffix() {
    let root = Path::new("/work");
    let target = Target::named("windows_amd64");

    assert!(target.runner_path(root).ends_with("etl-runner.exe"));
    assert!(target.engine_path(root).ends_with("duckdb.exe"));
}

#[test]
fn the_host_platform_is_spelled_the_way_duckdb_spells_it() {
    let platform = host_platform();

    // Two parts, an OS and an architecture, both in DuckDB's vocabulary rather
    // than Rust's: `osx` not `macos`, `amd64` not `x86_64`.
    let (os, arch) = platform.split_once('_').expect("os_arch");

    assert!(
        ["windows", "linux", "osx"].contains(&os),
        "unexpected os: {os}"
    );
    assert!(
        ["amd64", "arm64"].contains(&arch),
        "unexpected arch: {arch}"
    );
    assert_ne!(os, "macos", "DuckDB calls it osx");
}

#[test]
fn the_toolchain_is_looked_for_in_the_workspace_first_and_beside_etl_last() {
    let root = workspace("toolchain-roots");
    let roots = toolchain_roots(&settings_for(&root));

    // The workspace leads, because that is where somebody keeping their own
    // vendored copy would put it.
    assert_eq!(roots[0], root);
    // No duplicates: the list is walked and every entry costs a stat.
    let mut unique = roots.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), roots.len(), "duplicate roots: {roots:?}");
}
