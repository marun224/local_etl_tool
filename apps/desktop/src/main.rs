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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tauri::Manager;

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
    /// How long this stage took, when the engine was willing to say. Absent
    /// for most stages on most runs — see `StageOutcome::elapsed`, which is
    /// where the rule lives. The canvas renders nothing at all for `None`
    /// rather than a zero or a dash, because a dash in a column of numbers
    /// still reads as a measurement.
    elapsed_ms: Option<u128>,
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

/// What the assistant wrote, and how.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AssistResult {
    /// The draft, as a document's text.
    document: String,
    /// The components the model was allowed to use.
    offered: Vec<String>,
    /// The sampling seed, so *Try again* can ask for a different one.
    seed: u64,
    elapsed_ms: u128,
    /// The canvas's usual answer about the draft. An invalid draft is still
    /// returned (decision 103): on the canvas it can be fixed by hand.
    validation: Validation,
}

// ---------------------------------------------------------------------------
// Shared setup
// ---------------------------------------------------------------------------

/// How a command was asked to interpret the document: where relative paths
/// resolve from, which context is active, and what the parameters are bound to.
///
/// The same three knobs the CLI takes, named the same way, because the two have
/// to mean the same thing on the same file.
#[derive(Debug, Default, Clone, serde::Deserialize)]
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
        W::IncrementalIgnored { id, component_id } => {
            format!("'{id}' asks to load incrementally, but '{component_id}' is not a source")
        }
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
// The assistant
// ---------------------------------------------------------------------------

/// What `assist_pipeline` ends with when *Cancel* stopped it.
const CANCELLED: &str = "Cancelled.";

/// The local model, started on the first request and kept while the app is
/// open (decision 100), so a later request skips loading it and reading the
/// same prompt again. One request at a time.
#[derive(Default)]
struct Assistant {
    server: Mutex<Option<Arc<etl_assistant::Server>>>,
    busy: AtomicBool,
    cancelled: AtomicBool,
}

/// Clears the busy flag however a request ends.
struct Idle<'a>(&'a AtomicBool);

impl Drop for Idle<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Where `llama-server` and the model are: as `etl assist` finds them, under
/// the first of `starts` that has them, or named by the environment.
fn locate_tools(starts: &[PathBuf]) -> IpcResult<(PathBuf, PathBuf)> {
    let find = |locate: fn(Option<&Path>, &Path) -> Result<PathBuf, String>| {
        let mut first = None;
        for start in starts {
            match locate(None, start) {
                Ok(found) => return Ok(found),
                Err(message) => {
                    first.get_or_insert(message);
                }
            }
        }
        Err(IpcError::new(
            "assist",
            first.unwrap_or_else(|| "nowhere to look for the model".to_string()),
        ))
    };
    Ok((
        find(etl_assistant::locate_server)?,
        find(etl_assistant::locate_model)?,
    ))
}

impl Assistant {
    fn ask(
        &self,
        request: &str,
        seed: Option<u64>,
        settings: &Settings,
        tools: &(PathBuf, PathBuf),
    ) -> IpcResult<AssistResult> {
        if self.busy.swap(true, Ordering::SeqCst) {
            return Err(IpcError::new(
                "assist",
                "the assistant is already writing a pipeline",
            ));
        }
        let _idle = Idle(&self.busy);
        self.cancelled.store(false, Ordering::SeqCst);
        let started = Instant::now();

        let server = self.server(tools)?;
        let specs: Vec<etl_metadata::ComponentSpec> = registry().specs().cloned().collect();
        let seed = seed.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos() as u64)
                .unwrap_or_default()
        });
        let draft = etl_assistant::draft(&server, request, &specs, seed)
            .map_err(|message| self.failure(message))?;

        let document = serde_json::to_string_pretty(&draft.document)
            .map_err(|error| IpcError::new("assist", error.to_string()))?;
        let validation = validate_pipeline(document.clone(), settings.clone());
        Ok(AssistResult {
            document,
            offered: draft.offered,
            seed,
            elapsed_ms: started.elapsed().as_millis(),
            validation,
        })
    }

    /// The running server, or a new one once it has loaded the model.
    fn server(&self, tools: &(PathBuf, PathBuf)) -> IpcResult<Arc<etl_assistant::Server>> {
        let mut slot = self.server.lock().unwrap_or_else(|held| held.into_inner());
        if let Some(running) = slot.as_ref().filter(|running| running.is_alive()) {
            return Ok(Arc::clone(running));
        }

        let log = std::env::temp_dir().join(format!(
            "etl-desktop-llama-server-{}.log",
            std::process::id()
        ));
        let spawned = Arc::new(
            etl_assistant::Server::spawn(&tools.0, &tools.1, &log)
                .map_err(|message| IpcError::new("assist", message))?,
        );
        // Held before it has loaded, so Cancel can stop it while it loads.
        *slot = Some(Arc::clone(&spawned));
        drop(slot);

        spawned
            .wait_until_loaded()
            .map_err(|message| self.failure(message))?;
        if self.cancelled.load(Ordering::SeqCst) {
            spawned.stop();
            return Err(IpcError::new("cancelled", CANCELLED));
        }
        Ok(spawned)
    }

    fn failure(&self, message: String) -> IpcError {
        if self.cancelled.load(Ordering::SeqCst) || message == etl_assistant::STOPPED {
            IpcError::new("cancelled", CANCELLED)
        } else {
            IpcError::new("assist", message)
        }
    }

    /// Stop the request under way, if there is one (decision 104). The model
    /// goes with it; the next request starts it again.
    fn cancel(&self) {
        if !self.busy.load(Ordering::SeqCst) {
            return;
        }
        self.cancelled.store(true, Ordering::SeqCst);
        self.shut_down();
    }

    /// Stop the server. On exit, since a child process outlives its parent on
    /// Windows unless it is told to stop.
    fn shut_down(&self) {
        let running = self
            .server
            .lock()
            .unwrap_or_else(|held| held.into_inner())
            .take();
        if let Some(running) = running {
            running.stop();
        }
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
                elapsed_ms: stage.elapsed.map(|elapsed| elapsed.as_millis()),
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

/// Ask the local model for a pipeline doing `request`.
///
/// `async`, and the work on a blocking thread: Tauri runs a synchronous
/// command on the main thread, and a minute of model time there would freeze
/// the window.
#[tauri::command]
async fn assist_pipeline(
    request: String,
    seed: Option<u64>,
    settings: Settings,
    assistant: tauri::State<'_, Arc<Assistant>>,
) -> IpcResult<AssistResult> {
    let assistant = Arc::clone(&assistant);
    let mut starts = vec![settings.workspace_dir()];
    if let Some(beside) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf))
    {
        starts.push(beside);
    }

    tauri::async_runtime::spawn_blocking(move || {
        let tools = locate_tools(&starts)?;
        assistant.ask(&request, seed, &settings, &tools)
    })
    .await
    .map_err(|error| IpcError::new("assist", error.to_string()))?
}

/// Stop the request under way, if any.
#[tauri::command]
fn cancel_assist(assistant: tauri::State<'_, Arc<Assistant>>) {
    assistant.cancel();
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(Arc::new(Assistant::default()))
        .invoke_handler(tauri::generate_handler![
            list_components,
            compile_pipeline,
            validate_pipeline,
            run_pipeline,
            preview_node,
            read_pipeline,
            write_pipeline,
            assist_pipeline,
            cancel_assist,
        ])
        .build(tauri::generate_context!())
        .expect("the desktop shell failed to start")
        .run(|app, event| {
            if let tauri::RunEvent::Exit = event {
                app.state::<Arc<Assistant>>().shut_down();
            }
        });
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
    fn a_run_carries_a_timing_only_where_the_engine_gave_one() {
        if !have_duckdb() {
            return;
        }

        // The rule itself lives in the engine and is tested there. What this
        // checks is the mapping across the wire, in both directions: `None`
        // must arrive as `null` rather than becoming a zero on the way, and a
        // real duration must arrive as a number. The canvas draws whatever
        // arrives, and a zero beside a lazy view credits the wrong stage.
        //
        // The sample earns no session, so nothing in it can be timed.
        let plain = run_pipeline(SAMPLE.to_string(), settings()).expect("runs");
        assert!(
            plain.stages.iter().all(|stage| stage.elapsed_ms.is_none()),
            "one invocation cannot be attributed to individual stages"
        );

        // A control node moves the same shape of pipeline onto the driven
        // path, where the stage that does its work when it runs is timed.
        let waiting = SAMPLE.replace(
            r#""componentId": "xf.filter",
                  "properties": {"predicate": "amount > 100"}"#,
            r#""componentId": "ctl.wait",
                  "properties": {"ms": 20}"#,
        );

        let driven = run_pipeline(waiting, settings()).expect("runs");
        assert!(
            driven.stages[1].elapsed_ms.is_some_and(|ms| ms >= 20),
            "a wait reports what it held for, and it crossed the wire as a number"
        );
        assert_eq!(
            driven.stages[0].elapsed_ms, None,
            "the lazy source above it still says nothing"
        );
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
    fn a_missing_model_is_named_with_the_script_that_fetches_it() {
        let nowhere =
            std::env::temp_dir().join(format!("etl-desktop-no-model-{}", std::process::id()));
        std::fs::create_dir_all(&nowhere).unwrap();

        let error = locate_tools(&[nowhere]).expect_err("nothing is there");

        assert_eq!(error.stage, "assist");
        assert!(
            error.message.contains("fetch-model.ps1"),
            "{}",
            error.message
        );
    }

    #[test]
    fn one_request_at_a_time() {
        let assistant = Assistant::default();
        assistant.busy.store(true, Ordering::SeqCst);
        let tools = (PathBuf::from("unused"), PathBuf::from("unused"));

        let error = assistant
            .ask("csv to parquet", Some(1), &settings(), &tools)
            .expect_err("one is already under way");

        assert!(error.message.contains("already"), "{}", error.message);
        // Refusing did not clear the flag the other request holds.
        assert!(assistant.busy.load(Ordering::SeqCst));
    }

    /// The vendored model, or `None` to skip.
    fn model() -> Option<(PathBuf, PathBuf)> {
        locate_tools(&[repo_root()]).ok()
    }

    #[test]
    fn a_request_becomes_a_valid_draft_and_the_model_stays_up() {
        let Some(tools) = model() else {
            eprintln!("skipped: no model in tools/ (scripts/fetch-model.ps1)");
            return;
        };
        let assistant = Assistant::default();

        let answer = assistant
            .ask(
                "read this Postgres table, dedupe, write Parquet",
                Some(1),
                &settings(),
                &tools,
            )
            .expect("answers");

        assert!(answer.validation.valid, "{:?}", answer.validation.error);
        assert!(PipelineDoc::from_json(&answer.document).is_ok());
        assert!(
            answer.offered.contains(&"xf.dedup".to_string()),
            "{:?}",
            answer.offered
        );
        assert_eq!(answer.seed, 1);
        assert!(
            !assistant.busy.load(Ordering::SeqCst),
            "free for the next one"
        );

        // Kept for the next request (decision 100), then stopped.
        let kept = assistant.server.lock().unwrap().clone().expect("kept");
        assert!(kept.is_alive());
        assistant.shut_down();
        assert!(!kept.is_alive());
    }

    #[test]
    fn cancel_ends_a_running_request_and_the_model_with_it() {
        let Some(tools) = model() else {
            eprintln!("skipped: no model in tools/ (scripts/fetch-model.ps1)");
            return;
        };
        let assistant = Arc::new(Assistant::default());

        let asking = {
            let assistant = Arc::clone(&assistant);
            std::thread::spawn(move || {
                assistant.ask(
                    "read this Postgres table, dedupe, write Parquet",
                    Some(2),
                    &settings(),
                    &tools,
                )
            })
        };
        // Long enough to be loading the model or writing, not yet done.
        std::thread::sleep(std::time::Duration::from_secs(4));
        assistant.cancel();

        let error = asking.join().unwrap().expect_err("cancelled");
        assert_eq!(error.stage, "cancelled");
        assert_eq!(error.message, CANCELLED);
        assert!(
            assistant.server.lock().unwrap().is_none(),
            "the model went too"
        );
        assert!(!assistant.busy.load(Ordering::SeqCst));
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
