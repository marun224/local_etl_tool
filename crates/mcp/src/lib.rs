//! An MCP server over a workspace (Phase 11a): an agent such as Claude Code
//! finds components, writes a pipeline, checks it, runs it, reads what
//! happened and builds an executable, through the same engine `etl` uses.
//!
//! **Over stdin and stdout only** (Settled decision 92): the agent starts
//! `etl mcp` as a subprocess and nothing listens on a port. Stdout therefore
//! belongs to the protocol; nothing here or below it may print there.
//!
//! **This crate knows MCP and nothing about the engine**, the same seam
//! `etl-console` has: everything a tool answers with comes through
//! [`Workspace`], which `etl` implements where the engine, the resolver and
//! the secret store already are.
//!
//! **Every tool the plan lists** (decision 93), the agent's own permission
//! prompts being the gate. **Secrets leave by name only**: no tool returns a
//! value, the workspace redacts what it hands back, and a pipeline that bakes a
//! secret into an executable is refused here, where no one would see the
//! warning `etl build` prints.
//!
//! **Paths are the workspace's**: every path a tool takes is read relative to
//! the workspace and refused if it leads outside it.
//!
//! **Failures are results, not protocol errors**: an invalid pipeline or a
//! failed run is a result the agent reads and acts on, marked as an error.

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig};
use rmcp::{tool, tool_handler, tool_router, ErrorData, ServerHandler, ServiceExt};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// What a pipeline is run, checked or planned with: parameter bindings, and a
/// context other than the workspace's active one.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Bindings {
    pub params: BTreeMap<String, String>,
    pub context: Option<String>,
}

/// Everything a tool needs from the workspace. Each method is blocking; the
/// server calls them off its async threads.
///
/// Every `Err` is a message a person or an agent can act on, already redacted.
pub trait Workspace: Send + Sync + 'static {
    /// The workspace's root, absolute.
    fn root(&self) -> PathBuf;

    /// The component manifest, as `etl components --manifest` prints it.
    fn manifest(&self) -> JsonValue;

    /// The JSON Schema of a pipeline document (`etl_metadata::schema`).
    fn schema(&self) -> JsonValue;

    /// The pipelines in the workspace: name, path, whether each compiles.
    fn pipelines(&self) -> Result<JsonValue, String>;

    /// Check a document, given as JSON text: `{"valid": ..., ...}`.
    fn validate(&self, document: &str, bindings: &Bindings) -> Result<JsonValue, String>;

    /// The plan and the SQL each stage runs, secrets masked.
    fn plan(&self, pipeline: &Path, bindings: &Bindings) -> Result<JsonValue, String>;

    /// Node-level lineage, as `etl lineage --json` prints it.
    fn lineage(&self, pipeline: &Path, bindings: &Bindings) -> Result<JsonValue, String>;

    /// Run a pipeline and wait for it: the record written to history.
    fn run(&self, pipeline: &Path, bindings: &Bindings) -> Result<JsonValue, String>;

    /// Run history, newest first, for one pipeline key or all.
    fn runs(&self, pipeline: Option<&str>, limit: usize) -> Result<JsonValue, String>;

    /// Everything recorded about one run.
    fn run_record(&self, id: &str) -> Result<JsonValue, String>;

    /// Bake a pipeline into an executable at `out`, for `target` (this
    /// machine's platform when `None`). A pipeline that resolves a secret is
    /// refused.
    fn build(
        &self,
        pipeline: &Path,
        target: Option<&str>,
        out: &Path,
        bindings: &Bindings,
    ) -> Result<JsonValue, String>;

    /// Contexts and secrets, by name: never a secret's value.
    fn connections(&self) -> Result<JsonValue, String>;
}

/// `written`, read relative to `root`, if it stays inside it.
///
/// Lexical, because the file need not exist yet: `..` is followed and refused
/// once it climbs above the root. An absolute path is accepted only if it is
/// under the root already.
pub fn inside(root: &Path, written: &str) -> Result<PathBuf, String> {
    let written = written.trim();
    if written.is_empty() {
        return Err("the path is empty".into());
    }
    let root = std::path::absolute(root).map_err(|error| error.to_string())?;
    let candidate = Path::new(written);
    let joined = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        root.join(candidate)
    };

    let mut normal = PathBuf::new();
    for part in joined.components() {
        match part {
            Component::ParentDir => {
                if !normal.pop() {
                    return Err(format!("'{written}' leads outside the workspace"));
                }
            }
            Component::CurDir => {}
            other => normal.push(other.as_os_str()),
        }
    }
    if normal.starts_with(&root) && normal != root {
        Ok(normal)
    } else {
        Err(format!(
            "'{written}' is outside the workspace ({}); give a path inside it",
            root.display()
        ))
    }
}

fn outcome(value: Result<JsonValue, String>) -> CallToolResult {
    match value {
        Ok(value) => {
            // A check or a run that failed is still an answer, marked as one.
            let failed = value.get("valid") == Some(&json!(false))
                || value.get("outcome") == Some(&json!("failed"));
            let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
            if failed {
                CallToolResult::error(vec![ContentBlock::text(text)])
            } else {
                CallToolResult::success(vec![ContentBlock::text(text)])
            }
        }
        Err(message) => CallToolResult::error(vec![ContentBlock::text(message)]),
    }
}

// ---------------------------------------------------------------------------
// The tools' arguments
// ---------------------------------------------------------------------------

/// Parameters and context for a pipeline.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct With {
    /// Parameter bindings, name to value, as `etl run --param name=value`.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    /// A context to use instead of the workspace's active one.
    #[serde(default)]
    pub context: Option<String>,
}

impl With {
    fn bindings(&self) -> Bindings {
        Bindings {
            params: self.params.clone(),
            context: self.context.clone(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListComponents {
    /// Only this namespace: src, xf, snk, qa, ctl or code.
    #[serde(default)]
    pub namespace: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetComponent {
    /// The component's id, e.g. `src.file.csv`.
    pub id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct Validate {
    /// The pipeline document itself. Give this or `path`.
    #[serde(default)]
    pub document: Option<JsonValue>,
    /// A pipeline file, relative to the workspace. Give this or `document`.
    #[serde(default)]
    pub path: Option<String>,
    #[serde(flatten)]
    pub with: With,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct Create {
    /// Where to write it, relative to the workspace, ending `.json`.
    pub path: String,
    /// The pipeline document.
    pub document: JsonValue,
    /// Replace a file already there. Otherwise one there is left alone.
    #[serde(default)]
    pub overwrite: bool,
    #[serde(flatten)]
    pub with: With,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct Pipeline {
    /// The pipeline file, relative to the workspace.
    pub path: String,
    #[serde(flatten)]
    pub with: With,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListRuns {
    /// Only this pipeline's runs: its key, as list_pipelines or list_runs names it.
    #[serde(default)]
    pub pipeline: Option<String>,
    /// How many, newest first. Default 10.
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetRun {
    /// The run's id, as run_pipeline or list_runs gives it.
    pub id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct Build {
    /// The pipeline file, relative to the workspace.
    pub path: String,
    /// Where to write the executable, relative to the workspace.
    pub out: String,
    /// Another platform, as DuckDB names them: linux_amd64, windows_amd64,
    /// osx_arm64. Default: this machine's.
    #[serde(default)]
    pub target: Option<String>,
    #[serde(flatten)]
    pub with: With,
}

// ---------------------------------------------------------------------------
// The server
// ---------------------------------------------------------------------------

const INSTRUCTIONS: &str = "\
Pipelines for a local-first ETL engine on DuckDB. A pipeline is a JSON document of nodes \
(each running one component) wired by edges. Start with list_components and get_component to \
choose components, and get_schema for the exact document shape. Write with create_pipeline, \
which checks the document before writing it; check one without writing with validate_pipeline. \
run_pipeline runs one and waits; get_run_log shows what a run did. Paths are relative to the \
workspace and cannot leave it. Refer to a credential as ${SECRET:name} (list_connections names \
them); never put a password in a document.";

/// The MCP server over one workspace.
#[derive(Clone)]
pub struct Server<W: Workspace> {
    workspace: Arc<W>,
    tool_router: ToolRouter<Self>,
}

impl<W: Workspace> Server<W> {
    pub fn new(workspace: W) -> Self {
        Self {
            workspace: Arc::new(workspace),
            tool_router: Self::tool_router(),
        }
    }

    /// Run `work` against the workspace on a blocking thread.
    async fn blocking(
        &self,
        work: impl FnOnce(&W) -> Result<JsonValue, String> + Send + 'static,
    ) -> Result<CallToolResult, ErrorData> {
        let workspace = Arc::clone(&self.workspace);
        let value = tokio::task::spawn_blocking(move || work(&workspace))
            .await
            .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
        Ok(outcome(value))
    }

    fn path(&self, written: &str) -> Result<PathBuf, String> {
        inside(&self.workspace.root(), written)
    }
}

#[tool_router]
impl<W: Workspace> Server<W> {
    #[tool(
        description = "The components a node can run: id, label, namespace and required \
                          properties. Namespaces: src (sources), xf (transforms), snk (sinks), \
                          qa (checks), ctl (control), code."
    )]
    async fn list_components(
        &self,
        Parameters(args): Parameters<ListComponents>,
    ) -> Result<CallToolResult, ErrorData> {
        let manifest = self.workspace.manifest();
        let wanted = args
            .namespace
            .map(|n| n.trim().trim_end_matches('.').to_string());
        let listed: Vec<JsonValue> = manifest["components"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|component| match &wanted {
                Some(prefix) => component["id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with(&format!("{prefix}."))),
                None => true,
            })
            .map(|component| {
                let required: Vec<&JsonValue> = component["properties"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|p| p["required"] == json!(true))
                    .map(|p| &p["name"])
                    .collect();
                json!({
                    "id": component["id"], "label": component["label"],
                    "namespace": component["namespace"],
                    "description": component["description"], "required": required,
                })
            })
            .collect();
        Ok(outcome(Ok(json!({ "components": listed }))))
    }

    #[tool(
        description = "One component in full: its properties with types, defaults and help, \
                          and its input and output handles."
    )]
    async fn get_component(
        &self,
        Parameters(args): Parameters<GetComponent>,
    ) -> Result<CallToolResult, ErrorData> {
        let manifest = self.workspace.manifest();
        let found = manifest["components"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|component| component["id"] == json!(args.id.trim()))
            .cloned();
        Ok(outcome(found.ok_or_else(|| {
            format!(
                "no component '{}'; list_components names them",
                args.id.trim()
            )
        })))
    }

    #[tool(
        description = "The JSON Schema every pipeline document must match: each node tied to \
                          one component, with that component's properties typed."
    )]
    async fn get_schema(&self) -> Result<CallToolResult, ErrorData> {
        Ok(outcome(Ok(self.workspace.schema())))
    }

    #[tool(
        description = "The pipelines in the workspace: name (the key runs are kept under), \
                          path, stages, and why one does not compile."
    )]
    async fn list_pipelines(&self) -> Result<CallToolResult, ErrorData> {
        self.blocking(|workspace| workspace.pipelines()).await
    }

    #[tool(
        description = "Check a pipeline without running it or writing anything: the document \
                          itself, or a file in the workspace. Says whether it is valid, and why \
                          not."
    )]
    async fn validate_pipeline(
        &self,
        Parameters(args): Parameters<Validate>,
    ) -> Result<CallToolResult, ErrorData> {
        let bindings = args.with.bindings();
        let text = match (args.document, args.path) {
            (Some(document), None) => Ok(document.to_string()),
            (None, Some(path)) => self.path(&path).and_then(|path| {
                std::fs::read_to_string(&path).map_err(|e| format!("{path:?}: {e}"))
            }),
            _ => Err("give either document or path".to_string()),
        };
        match text {
            Ok(text) => {
                self.blocking(move |workspace| workspace.validate(&text, &bindings))
                    .await
            }
            Err(message) => Ok(outcome(Err(message))),
        }
    }

    #[tool(
        description = "Write a pipeline document into the workspace, after checking it. An \
                          invalid document is not written. A file already there is left alone \
                          unless overwrite is true."
    )]
    async fn create_pipeline(
        &self,
        Parameters(args): Parameters<Create>,
    ) -> Result<CallToolResult, ErrorData> {
        let path = match self.path(&args.path) {
            Ok(path) if path.extension().is_some_and(|e| e == "json") => path,
            Ok(_) => return Ok(outcome(Err("a pipeline's path ends in .json".into()))),
            Err(message) => return Ok(outcome(Err(message))),
        };
        let bindings = args.with.bindings();
        let overwrite = args.overwrite;
        let document = args.document;
        self.blocking(move |workspace| {
            if path.exists() && !overwrite {
                return Err(format!(
                    "{} is already there; pass overwrite: true to replace it",
                    path.display()
                ));
            }
            let checked = workspace.validate(&document.to_string(), &bindings)?;
            if checked["valid"] != json!(true) {
                return Ok(checked);
            }
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            let text = serde_json::to_string_pretty(&document).map_err(|e| e.to_string())?;
            std::fs::write(&path, text + "\n").map_err(|e| format!("{}: {e}", path.display()))?;
            let mut written = checked;
            written["written"] = json!(path.display().to_string());
            Ok(written)
        })
        .await
    }

    #[tool(
        description = "Run a pipeline in the workspace and wait for it. Returns the run's \
                          record: outcome, rows per stage, failures. Writes what the pipeline \
                          writes."
    )]
    async fn run_pipeline(
        &self,
        Parameters(args): Parameters<Pipeline>,
    ) -> Result<CallToolResult, ErrorData> {
        let bindings = args.with.bindings();
        match self.path(&args.path) {
            Ok(path) => {
                self.blocking(move |workspace| workspace.run(&path, &bindings))
                    .await
            }
            Err(message) => Ok(outcome(Err(message))),
        }
    }

    #[tool(
        description = "The plan for a pipeline: its stages in order and the SQL each runs, \
                          secrets masked. Runs nothing."
    )]
    async fn plan_pipeline(
        &self,
        Parameters(args): Parameters<Pipeline>,
    ) -> Result<CallToolResult, ErrorData> {
        let bindings = args.with.bindings();
        match self.path(&args.path) {
            Ok(path) => {
                self.blocking(move |workspace| workspace.plan(&path, &bindings))
                    .await
            }
            Err(message) => Ok(outcome(Err(message))),
        }
    }

    #[tool(description = "Where a pipeline's data comes from and where it goes, node by node.")]
    async fn get_lineage(
        &self,
        Parameters(args): Parameters<Pipeline>,
    ) -> Result<CallToolResult, ErrorData> {
        let bindings = args.with.bindings();
        match self.path(&args.path) {
            Ok(path) => {
                self.blocking(move |workspace| workspace.lineage(&path, &bindings))
                    .await
            }
            Err(message) => Ok(outcome(Err(message))),
        }
    }

    #[tool(description = "Recent runs, newest first: id, pipeline, outcome, when, how long.")]
    async fn list_runs(
        &self,
        Parameters(args): Parameters<ListRuns>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = args.limit.unwrap_or(10).clamp(1, 500);
        let pipeline = args.pipeline;
        self.blocking(move |workspace| workspace.runs(pipeline.as_deref(), limit))
            .await
    }

    #[tool(
        description = "Everything recorded about one run: each stage's rows and time, \
                          watermarks, notes, warnings and failures."
    )]
    async fn get_run_log(
        &self,
        Parameters(args): Parameters<GetRun>,
    ) -> Result<CallToolResult, ErrorData> {
        let id = args.id;
        self.blocking(move |workspace| workspace.run_record(id.trim()))
            .await
    }

    #[tool(
        description = "Bake a pipeline into one standalone executable, with the engine \
                          inside, for this machine or another platform. A pipeline that uses a \
                          secret is refused: baking it would write the secret into the file."
    )]
    async fn build_executable(
        &self,
        Parameters(args): Parameters<Build>,
    ) -> Result<CallToolResult, ErrorData> {
        let bindings = args.with.bindings();
        let target = args.target;
        match (self.path(&args.path), self.path(&args.out)) {
            (Ok(path), Ok(out)) => {
                self.blocking(move |workspace| {
                    workspace.build(&path, target.as_deref(), &out, &bindings)
                })
                .await
            }
            (Err(message), _) | (_, Err(message)) => Ok(outcome(Err(message))),
        }
    }

    #[tool(
        description = "The workspace's contexts (named sets of variables, one active) and \
                          secrets, by name. Refer to a secret as ${SECRET:name}; its value is \
                          never shown."
    )]
    async fn list_connections(&self) -> Result<CallToolResult, ErrorData> {
        self.blocking(|workspace| workspace.connections()).await
    }
}

#[tool_handler(router = self.tool_router)]
impl<W: Workspace> ServerHandler for Server<W> {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("etl", env!("CARGO_PKG_VERSION")).with_title("etl pipelines"),
            )
            .with_instructions(INSTRUCTIONS)
    }
}

/// Serve `workspace` over stdin and stdout until the client goes away.
pub fn serve_stdio<W: Workspace>(workspace: W) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async move {
        let service = Server::new(workspace)
            .serve(rmcp::transport::stdio())
            .await
            .map_err(|error| error.to_string())?;
        service.waiting().await.map_err(|error| error.to_string())?;
        Ok(())
    })
}
