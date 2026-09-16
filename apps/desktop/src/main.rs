//! The desktop shell.
//!
//! Tauri gives the canvas a window and a way to call Rust. This crate is the
//! *only* place those two meet: it holds the IPC commands and nothing else. No
//! SQL is generated here, no DuckDB is spawned here, and no component is
//! described here — all of that stays in the engine, which the CLI already uses
//! and which has 326 tests against it.
//!
//! That division is the point. The GUI and the CLI must stay interchangeable on
//! the same document, and the way to guarantee it is for both to be thin
//! callers of one engine rather than two implementations that agree for now.
//!
//! **Everything crossing IPC is plain JSON**, shaped by the types in this file,
//! so the TypeScript side has one place to mirror. Engine errors are flattened
//! to strings with the node id kept separate, because the canvas wants to put a
//! message on a box rather than parse it.

// Release builds open no console window. A dev build keeps it, because that is
// where a panic is legible.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use etl_duckdb_engine::{
    compile, preview, registry, resolve, run, Contexts, EngineError, Plan, Resolver, RunOptions,
};
use etl_metadata::PipelineDoc;
use etl_secrets::SecretStore;
use serde::Serialize;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// What crosses the wire
// ---------------------------------------------------------------------------

/// An error the canvas can act on.
///
/// `node_id` is separate rather than embedded in the message so a node can be
/// highlighted without the frontend having to read English.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IpcError {
    message: String,
    node_id: Option<String>,
    /// Which step failed: `read`, `resolve`, `compile` or `run`. Lets the UI
    /// say "this document is not valid" rather than "something went wrong".
    stage: &'static str,
}

impl IpcError {
    fn new(stage: &'static str, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            node_id: None,
            stage,
        }
    }

    fn engine(error: EngineError) -> Self {
        Self {
            node_id: error.node_id().map(str::to_string),
            message: error.to_string(),
            stage: "compile",
        }
    }
}

type IpcResult<T> = Result<T, IpcError>;

/// One lowered stage, as the canvas shows it.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StageView {
    node_id: String,
    component_id: String,
    label: String,
    kind: String,
    sql: String,
    from: Option<String>,
    splits: bool,
    needs_session: bool,
}

/// A compiled plan, as the canvas shows it.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlanView {
    stages: Vec<StageView>,
    warnings: Vec<String>,
    extensions: Vec<String>,
    /// The whole script, which the Plan tab shows. Secrets are already masked:
    /// resolution happens before this and hands back the values to hide.
    script: String,
    needs_session: bool,
    session_reasons: Vec<String>,
}

/// What `validate` answers. Deliberately not an error when invalid: an invalid
/// document is the normal state of a pipeline being built, and the canvas wants
/// to render the problem, not catch an exception on every keystroke.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Validation {
    valid: bool,
    error: Option<IpcError>,
    stage_count: usize,
    sink_count: usize,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StageResult {
    node_id: String,
    label: String,
    component_id: String,
    rows: Option<u64>,
    rejected: Option<u64>,
    skipped: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RunResult {
    stages: Vec<StageResult>,
    elapsed_ms: u128,
    notes: Vec<String>,
    failures: Vec<String>,
    failed: bool,
    script: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PreviewResult {
    node_id: String,
    columns: Vec<String>,
    rows: Vec<serde_json::Value>,
    truncated: bool,
}

// ---------------------------------------------------------------------------
// Shared setup
// ---------------------------------------------------------------------------

/// How a command was asked to interpret the document: where relative paths
/// resolve from, which context is active, and what the parameters are bound to.
///
/// The same three knobs the CLI takes, named the same way, because the two have
/// to mean the same thing on the same file.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Settings {
    workspace: Option<String>,
    context: Option<String>,
    #[serde(default)]
    params: Vec<(String, String)>,
}

impl Settings {
    fn workspace_dir(&self) -> PathBuf {
        self.workspace
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    }
}

/// Read, resolve, compile — the same order the CLI uses.
///
/// Resolution is separate and first, so an unresolved `${...}` is reported as
/// that rather than as a confusing compile error about a path that looks fine.
fn prepare(document: &str, settings: &Settings) -> IpcResult<(Plan, Vec<String>)> {
    let doc = PipelineDoc::from_json(document).map_err(|error| {
        IpcError::new("read", format!("this is not a pipeline document: {error}"))
    })?;

    let workspace = settings.workspace_dir();

    let mut resolver = Resolver::new(&workspace);
    for (name, value) in &settings.params {
        resolver = resolver.bind(name, value);
    }

    // Opened only when the workspace has a key, exactly as the CLI does it: a
    // pipeline with no `${SECRET:...}` must not need one, and opening the
    // canvas must never mint a key nobody asked for.
    if SecretStore::has_key(&workspace) {
        let store = SecretStore::open_existing(&workspace)
            .map_err(|error| IpcError::new("resolve", error.to_string()))?;

        resolver = resolver.secrets(store);
    }

    let contexts = Contexts::load_from_workspace(&workspace)
        .map_err(|error| IpcError::new("resolve", error.to_string()))?;

    let resolver = contexts
        .apply(resolver, settings.context.as_deref())
        .map_err(|error| IpcError::new("resolve", error.to_string()))?;

    let resolved =
        resolve(&doc, &resolver).map_err(|error| IpcError::new("resolve", error.to_string()))?;

    let plan = compile(&resolved.document).map_err(IpcError::engine)?;

    Ok((plan, resolved.secret_values()))
}

fn run_options(settings: &Settings, redact: Vec<String>) -> RunOptions {
    RunOptions {
        working_dir: Some(settings.workspace_dir()),
        redact,
        ..Default::default()
    }
}

fn plan_view(plan: &Plan) -> PlanView {
    PlanView {
        stages: plan
            .stages
            .iter()
            .map(|stage| StageView {
                node_id: stage.node_id.clone(),
                component_id: stage.component_id.clone(),
                label: stage.label.clone(),
                kind: format!("{:?}", stage.kind).to_lowercase(),
                sql: stage.sql.clone(),
                from: stage.from.clone(),
                splits: stage.splits,
                needs_session: stage.needs_session(),
            })
            .collect(),
        warnings: plan.warnings.iter().map(describe_warning).collect(),
        extensions: plan.extensions().iter().map(|e| e.to_string()).collect(),
        script: plan.script(true),
        needs_session: plan.needs_session(),
        session_reasons: plan
            .session_reasons()
            .iter()
            .map(|s| s.to_string())
            .collect(),
    }
}

/// Warnings have no `Display`, because the engine deliberately keeps them as
/// data the canvas can act on. This is the one place they become English.
fn describe_warning(warning: &etl_duckdb_engine::Warning) -> String {
    use etl_duckdb_engine::Warning as W;

    match warning {
        W::DisabledSkipped { id } => format!("'{id}' is switched off"),
        W::DroppedDownstreamOfDisabled { id, disabled } => {
            format!("'{id}' was dropped because '{disabled}' is switched off")
        }
        W::Orphan { id } => format!("'{id}' is not wired to anything"),
        W::UnknownProperty { id, property } => {
            format!("'{id}' sets '{property}', which its component does not define")
        }
        W::UnknownMaterialize { id, value } => {
            format!("'{id}' asks for materialize '{value}', which is not a mode; using auto")
        }
        W::NoSink => "nothing in this pipeline writes anything".to_string(),
    }
}

// ---------------------------------------------------------------------------
// The commands
// ---------------------------------------------------------------------------

/// Every component the engine knows, as the manifest the palette and the
/// property panels are generated from.
///
/// The frontend holds no component list of its own. With ~400 components to
/// reach, a second copy would be a second thing to keep right.
#[tauri::command]
fn list_components() -> serde_json::Value {
    registry().manifest()
}

/// Compile without running. Touches no files and spawns nothing.
#[tauri::command]
fn compile_pipeline(document: String, settings: Settings) -> IpcResult<PlanView> {
    let (plan, _) = prepare(&document, &settings)?;
    Ok(plan_view(&plan))
}

/// Check a document and describe what is wrong, without treating "wrong" as an
/// exception. A pipeline under construction is invalid most of the time.
#[tauri::command]
fn validate_pipeline(document: String, settings: Settings) -> Validation {
    match prepare(&document, &settings) {
        Ok((plan, _)) => Validation {
            valid: true,
            error: None,
            stage_count: plan.stages.len(),
            sink_count: plan.sinks().count(),
            warnings: plan.warnings.iter().map(describe_warning).collect(),
        },
        Err(error) => Validation {
            valid: false,
            error: Some(error),
            stage_count: 0,
            sink_count: 0,
            warnings: Vec::new(),
        },
    }
}

/// Run the pipeline for real.
#[tauri::command]
fn run_pipeline(document: String, settings: Settings) -> IpcResult<RunResult> {
    let (plan, redact) = prepare(&document, &settings)?;
    let options = run_options(&settings, redact);

    let report = run(&plan, &options).map_err(|error| IpcError::new("run", error.to_string()))?;

    Ok(RunResult {
        stages: report
            .stages
            .iter()
            .map(|stage| StageResult {
                node_id: stage.node_id.clone(),
                label: stage.label.clone(),
                component_id: stage.component_id.clone(),
                rows: stage.rows,
                rejected: stage.rejected,
                skipped: stage.skipped.as_ref().map(|r| r.describe()),
            })
            .collect(),
        elapsed_ms: report.elapsed.as_millis(),
        notes: report.notes.clone(),
        failures: report
            .failures
            .iter()
            .map(|f| format!("{} ({}): {}", f.label, f.node_id, f.message))
            .collect(),
        failed: report.failed(),
        script: report.script.clone(),
    })
}

/// Read a pipeline document off disk.
///
/// The picking is done by the dialog plugin; the reading is done here. That
/// split is deliberate — granting a filesystem plugin a path scope would put
/// disk access behind a permission list, whereas this way every touch of the
/// disk is a function in this repo that can be read.
#[tauri::command]
fn read_pipeline(path: String) -> IpcResult<String> {
    std::fs::read_to_string(&path)
        .map_err(|error| IpcError::new("read", format!("could not read {path}: {error}")))
}

/// Write a pipeline document to disk.
#[tauri::command]
fn write_pipeline(path: String, document: String) -> IpcResult<()> {
    // Parse before writing. A document that will not load is not one worth
    // putting over a file someone already has.
    PipelineDoc::from_json(&document).map_err(|error| {
        IpcError::new(
            "read",
            format!("refusing to save something that will not load: {error}"),
        )
    })?;

    if let Some(parent) = Path::new(&path).parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|error| {
                IpcError::new(
                    "read",
                    format!("could not make {}: {error}", parent.display()),
                )
            })?;
        }
    }

    std::fs::write(&path, document)
        .map_err(|error| IpcError::new("read", format!("could not write {path}: {error}")))
}

/// Read the rows one node produces, without running the rest of the pipeline
/// and without writing anything.
#[tauri::command]
fn preview_node(
    document: String,
    node_id: String,
    limit: usize,
    settings: Settings,
) -> IpcResult<PreviewResult> {
    let (plan, redact) = prepare(&document, &settings)?;
    let options = run_options(&settings, redact);

    let rows = preview(&plan, &node_id, limit, &options)
        .map_err(|error| IpcError::new("run", error.to_string()))?;

    Ok(PreviewResult {
        node_id: rows.node_id,
        columns: rows.columns,
        rows: rows.rows,
        truncated: rows.truncated,
    })
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .invoke_handler(tauri::generate_handler![
            list_components,
            compile_pipeline,
            validate_pipeline,
            run_pipeline,
            preview_node,
            read_pipeline,
            write_pipeline,
        ])
        .run(tauri::generate_context!())
        .expect("the desktop shell failed to start");
}

// ---------------------------------------------------------------------------
// The IPC surface, exercised without a window
// ---------------------------------------------------------------------------
//
// `#[tauri::command]` leaves the function callable as an ordinary one, so the
// whole surface can be tested against the real engine without opening a window
// or driving a webview. That matters more here than it looks: everything below
// is the contract the canvas is written against in 7b, and a GUI-only test
// would mean it is only checked by someone clicking.
//
// These need the vendored DuckDB for the two that execute, and skip without it.

#[cfg(test)]
mod tests {
    use super::*;

    /// The repo root, so the sample paths in the fixtures resolve.
    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("apps/desktop sits two levels under the root")
            .to_path_buf()
    }

    fn settings() -> Settings {
        Settings {
            workspace: Some(repo_root().to_string_lossy().to_string()),
            ..Default::default()
        }
    }

    fn have_duckdb() -> bool {
        etl_duckdb_engine::exec::locate_duckdb(&RunOptions {
            working_dir: Some(repo_root()),
            ..Default::default()
        })
        .is_ok()
    }

    /// A source feeding a filter. No sink, so running it writes nothing.
    const SAMPLE: &str = r#"{
      "formatVersion": 1,
      "nodes": [
        {"id": "orders", "type": "source", "position": {"x": 0, "y": 0},
         "data": {"label": "Orders", "componentId": "src.file.csv",
                  "properties": {"path": "samples/data/orders.csv"}}},
        {"id": "large", "type": "transform", "position": {"x": 200, "y": 0},
         "data": {"label": "Large", "componentId": "xf.filter",
                  "properties": {"predicate": "amount > 100"}}}
      ],
      "edges": [
        {"id": "e1", "source": "orders", "target": "large",
         "sourceHandle": "main", "targetHandle": "in"}
      ]
    }"#;

    #[test]
    fn the_manifest_is_the_engines_registry_and_not_a_copy() {
        let manifest = list_components();
        let components = manifest["components"].as_array().expect("an array");

        assert_eq!(
            components.len(),
            registry().len(),
            "the canvas must see exactly what the engine has"
        );

        // Every component carries what a property panel is generated from.
        for component in components {
            assert!(component["id"].is_string());
            assert!(component["label"].is_string());
            assert!(component["properties"].is_array());
        }
    }

    #[test]
    fn a_good_document_compiles_to_stages_and_a_script() {
        let plan = compile_pipeline(SAMPLE.to_string(), settings()).expect("compiles");

        assert_eq!(plan.stages.len(), 2);
        assert_eq!(plan.stages[0].node_id, "orders");
        assert_eq!(plan.stages[1].node_id, "large");

        assert!(plan.script.contains("CREATE OR REPLACE TEMP VIEW"));
        assert!(!plan.needs_session, "nothing here asks for one");

        // The Plan tab shows this, so it has to be the real thing.
        assert!(plan.stages[1].sql.contains("amount > 100"));
    }

    #[test]
    fn an_invalid_document_is_an_answer_rather_than_an_exception() {
        // A pipeline under construction is invalid most of the time, and the
        // canvas re-validates on every edit. If that threw, every keystroke
        // between two valid states would be an error to catch.
        let broken = SAMPLE.replace("xf.filter", "xf.nonexistent");
        let result = validate_pipeline(broken, settings());

        assert!(!result.valid);

        let error = result.error.expect("an invalid document says why");
        assert_eq!(error.stage, "compile");
        assert_eq!(
            error.node_id.as_deref(),
            Some("large"),
            "the canvas needs the node id to highlight the right box"
        );
    }

    #[test]
    fn a_valid_document_validates_and_counts_its_stages() {
        let result = validate_pipeline(SAMPLE.to_string(), settings());

        assert!(result.valid);
        assert_eq!(result.stage_count, 2);
        assert_eq!(result.sink_count, 0);

        // No sink is worth saying, and is a warning rather than an error.
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.contains("writes anything")),
            "{:?}",
            result.warnings
        );
    }

    #[test]
    fn malformed_json_fails_at_read_and_says_so() {
        let result = validate_pipeline("{ not json".to_string(), settings());

        assert!(!result.valid);
        assert_eq!(result.error.expect("says why").stage, "read");
    }

    #[test]
    fn running_reports_per_stage_rows() {
        if !have_duckdb() {
            return;
        }

        let result = run_pipeline(SAMPLE.to_string(), settings()).expect("runs");

        assert!(!result.failed);
        assert_eq!(result.stages.len(), 2);
        assert_eq!(result.stages[0].rows, Some(12));
        assert_eq!(result.stages[1].rows, Some(6));
    }

    #[test]
    fn previewing_reads_one_node_without_running_the_rest() {
        if !have_duckdb() {
            return;
        }

        let preview =
            preview_node(SAMPLE.to_string(), "large".to_string(), 3, settings()).expect("previews");

        assert_eq!(preview.node_id, "large");
        assert_eq!(preview.rows.len(), 3, "the limit is honoured");
        assert!(preview.truncated, "and it says there are more");

        assert!(
            preview.columns.contains(&"order_id".to_string()),
            "columns come back for the grid: {:?}",
            preview.columns
        );
    }

    #[test]
    fn previewing_a_node_that_is_not_there_says_which() {
        if !have_duckdb() {
            return;
        }

        let error = preview_node(SAMPLE.to_string(), "typo".to_string(), 10, settings())
            .expect_err("there is no such node");

        assert!(error.message.contains("typo"), "{}", error.message);
    }

    #[test]
    fn an_unresolved_parameter_is_reported_as_one() {
        // Resolution runs before compilation precisely so this reads as a
        // missing parameter rather than as a puzzling error about a path.
        let parameterised = SAMPLE.replace("samples/data/orders.csv", "${nowhere}/orders.csv");
        let result = validate_pipeline(parameterised, settings());

        assert!(!result.valid);

        let error = result.error.expect("says why");
        assert_eq!(error.stage, "resolve");
        assert!(error.message.contains("nowhere"), "{}", error.message);
    }
}
