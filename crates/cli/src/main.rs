//! `etl` — compile and run pipelines from the command line.
//!
//! The CLI and the desktop app are meant to be interchangeable on the same
//! file, so everything here is a thin shell over the engine: read JSON,
//! compile, optionally run, print. No behaviour lives in this crate that the
//! GUI would then have to reimplement.

use clap::{Args, Parser, Subcommand};
use etl_console as console;
use etl_duckdb_engine::{
    compile_with, lineage, params, registry, remember, run, CompileOptions, Contexts, EngineError,
    ExecError, ParamWarning, Plan, Resolved, Resolver, RunOptions, RunReport, Warning,
};
use etl_metadata::Namespace;
use etl_metadata::PipelineDoc;
use etl_scheduler as sched;
use etl_secrets::SecretStore;
use etl_state as state;
#[cfg(test)]
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[cfg(test)]
mod tests;

/// Exit codes, fixed so scripts and CI can branch on them.
mod exit {
    /// Everything worked.
    pub const OK: u8 = 0;
    /// The file could not be read or parsed, or the arguments made no sense.
    pub const USAGE: u8 = 1;
    /// The pipeline is not valid.
    pub const INVALID: u8 = 2;
    /// The pipeline is valid but the run failed.
    pub const FAILED: u8 = 3;
}

#[derive(Parser)]
#[command(
    name = "etl",
    version,
    about = "Compile and run local-first ETL pipelines on DuckDB.",
    after_help = "Exit codes: 0 ok, 1 usage or I/O error, 2 invalid pipeline, 3 run failed."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// The settings every pipeline command shares: where the workspace is, which
/// context to read, and what the parameters are bound to.
#[derive(Args, Clone, Debug, Default)]
struct Settings {
    /// Bind a parameter: --param since=2026-01-01. Repeatable. Beats the
    /// context and the pipeline's own default.
    #[arg(long = "param", short = 'p', value_name = "NAME=VALUE", global = true)]
    params: Vec<String>,

    /// Use this context rather than the workspace's active one.
    #[arg(long, value_name = "NAME", global = true)]
    context: Option<String>,

    /// The workspace root: what ${workspace} expands to, and where the
    /// pipeline's relative paths resolve from. Defaults to the current
    /// directory. One flag for both, so the two can never disagree.
    #[arg(long, alias = "workdir", value_name = "DIR", global = true)]
    workspace: Option<PathBuf>,

    /// Read contexts from here instead of <workspace>/.etl/contexts.json.
    #[arg(long, value_name = "FILE", global = true)]
    contexts: Option<PathBuf>,

    /// Read schedules from here instead of <workspace>/.etl/schedules.json.
    #[arg(long, value_name = "FILE", global = true)]
    schedules: Option<PathBuf>,
}

impl Settings {
    /// Where this workspace's schedules are.
    fn schedules_path(&self) -> PathBuf {
        self.schedules
            .clone()
            .unwrap_or_else(|| sched::ScheduleFile::path_in(&self.workspace_root()))
    }

    /// These settings as one schedule's run would see them.
    ///
    /// The schedule's own bindings first, then whatever was given on the
    /// command line, so a `--param` passed to `schedule start` beats the file
    /// — the same precedence `run` already gives an explicit binding over a
    /// context. The context works the other way round only in that an
    /// explicit `--context` wins; otherwise the schedule's own is used.
    fn for_schedule(&self, schedule: &sched::Schedule) -> Settings {
        let mut settings = self.clone();

        let mut params: Vec<String> = schedule
            .params
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect();
        params.extend(self.params.iter().cloned());

        settings.params = params;
        settings.context = self.context.clone().or_else(|| schedule.context.clone());

        settings
    }

    fn workspace_root(&self) -> PathBuf {
        self.workspace
            .clone()
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    }

    /// Build the resolver: explicit bindings first, then the workspace's
    /// contexts, then whichever one is active.
    fn resolver(&self) -> Result<Resolver, u8> {
        self.try_resolver().map_err(|message| {
            eprintln!("error: {message}");
            exit::USAGE
        })
    }

    /// `resolver`, with the reason it failed returned rather than printed.
    fn try_resolver(&self) -> Result<Resolver, String> {
        let root = self.workspace_root();
        let mut resolver = Resolver::new(&root);

        for binding in &self.params {
            let Some((name, value)) = binding.split_once('=') else {
                return Err(format!("--param expects NAME=VALUE, but got '{binding}'"));
            };

            resolver = resolver.bind(name.trim(), value);
        }

        let path = self
            .contexts
            .clone()
            .unwrap_or_else(|| Contexts::path_in(&root));

        // Opened only when the workspace has a key. A pipeline with no
        // `${SECRET:...}` in it must not need one to run, and a run never mints
        // a key it was not given.
        if SecretStore::has_key(&root) {
            let store = SecretStore::open_existing(&root).map_err(|error| error.to_string())?;

            resolver = resolver.secrets(store);
        }

        let contexts = Contexts::load(&path).map_err(|error| error.to_string())?;

        contexts
            .apply(resolver, self.context.as_deref())
            .map_err(|error| error.to_string())
    }

    /// These settings with one request's bindings on top: its parameters beat
    /// the ones already here, and its context replaces the active one.
    fn with(&self, bindings: &etl_mcp::Bindings) -> Settings {
        let mut settings = self.clone();
        settings.params.extend(
            bindings
                .params
                .iter()
                .map(|(name, value)| format!("{name}={value}")),
        );
        if bindings.context.is_some() {
            settings.context = bindings.context.clone();
        }
        settings
    }
}

#[derive(Subcommand)]
enum SecretAction {
    /// Create this workspace's key, if it has none.
    Init,

    /// Store a secret. Reference it from a pipeline as ${SECRET:name}.
    Set {
        /// What to call it.
        name: String,

        /// The value. Prefer --stdin: an argument is visible in the process
        /// list and stays in your shell history.
        #[arg(value_name = "VALUE", required_unless_present = "stdin")]
        value: Option<String>,

        /// Read the value from standard input instead.
        #[arg(long)]
        stdin: bool,

        /// A note about what this is for. Not encrypted.
        #[arg(long, value_name = "TEXT")]
        description: Option<String>,
    },

    /// List the secrets this workspace holds. Names only, never values.
    List,

    /// Forget a secret.
    Remove {
        /// Which one.
        name: String,
    },
}

#[derive(Subcommand)]
enum Command {
    /// Compile and execute a pipeline.
    Run {
        /// The pipeline JSON file.
        pipeline: PathBuf,

        /// DuckDB binary to use. Defaults to ETL_DUCKDB_BIN, then the
        /// vendored copy in tools/duckdb, then PATH.
        #[arg(long, value_name = "PATH")]
        duckdb: Option<PathBuf>,

        /// Skip per-stage row counts. Faster, because a count forces its
        /// view to materialise, but the run cannot then say which stage
        /// failed.
        #[arg(long)]
        no_counts: bool,

        /// Print the generated SQL before running it.
        #[arg(long)]
        show_sql: bool,

        /// Emit the run record as JSON instead of a table. The same shape
        /// that is written to history, so what a script parses is what was
        /// kept.
        #[arg(long)]
        json: bool,

        #[command(flatten)]
        settings: Settings,
    },

    /// Serve a web console over this workspace.
    Serve {
        /// The port to listen on.
        #[arg(long, default_value_t = console::DEFAULT_PORT)]
        port: u16,

        /// The address to bind. Loopback by default: this console speaks no
        /// TLS, so anything else needs saying out loud.
        #[arg(long, value_name = "ADDRESS")]
        bind: Option<String>,

        /// DuckDB binary to use, as `run` takes it.
        #[arg(long, value_name = "PATH")]
        duckdb: Option<PathBuf>,

        /// Skip per-stage row counts on runs the console starts.
        #[arg(long)]
        no_counts: bool,

        #[command(flatten)]
        settings: Settings,
    },

    /// Run pipelines on a schedule.
    Schedule {
        #[command(subcommand)]
        action: ScheduleAction,

        #[command(flatten)]
        settings: Settings,
    },

    /// Check a pipeline without running it. Touches nothing.
    Validate {
        /// The pipeline JSON file.
        pipeline: PathBuf,

        #[command(flatten)]
        settings: Settings,
    },

    /// List the workspace's contexts.
    Contexts {
        #[command(flatten)]
        settings: Settings,
    },

    /// Manage the workspace's encrypted secrets.
    Secret {
        #[command(subcommand)]
        action: SecretAction,

        #[command(flatten)]
        settings: Settings,
    },

    /// List the available components, or emit the manifest the canvas and
    /// agents read.
    Components {
        /// Only this namespace: src, xf, snk, qa, ctl, or code.
        #[arg(long, value_name = "NS")]
        namespace: Option<String>,

        /// Emit the full JSON manifest, including every property schema.
        #[arg(long)]
        manifest: bool,
    },

    /// Look at what past runs did.
    Runs {
        #[command(subcommand)]
        action: RunsAction,

        #[command(flatten)]
        settings: Settings,
    },

    /// Print where this pipeline's data comes from and where it goes.
    Lineage {
        /// The pipeline JSON file.
        pipeline: PathBuf,

        /// Emit JSON rather than an outline.
        #[arg(long)]
        json: bool,

        #[command(flatten)]
        settings: Settings,
    },

    /// Inspect or reset what incremental sources remember.
    State {
        #[command(subcommand)]
        action: StateAction,

        #[command(flatten)]
        settings: Settings,
    },

    /// Print the execution plan and the SQL it will run.
    Plan {
        /// The pipeline JSON file.
        pipeline: PathBuf,

        /// Print the script as it would be sent to DuckDB, rather than
        /// stage by stage.
        #[arg(long)]
        script: bool,

        /// Leave the row-count probes out of the script.
        #[arg(long)]
        no_counts: bool,

        #[command(flatten)]
        settings: Settings,
    },

    /// Bake a pipeline into a standalone executable.
    ///
    /// The output is one file with the resolved pipeline inside it. Copy it to
    /// a machine with no Rust, no workspace and no contexts, and run it.
    Build {
        /// The pipeline JSON file.
        pipeline: PathBuf,

        /// Where to write the executable. Defaults to the pipeline's name in
        /// the current directory.
        #[arg(long, short = 'o', value_name = "FILE")]
        out: Option<PathBuf>,

        /// The runner to copy. Defaults to the `etl-runner` built beside this
        /// `etl`. Phase 9c is where this becomes a target-OS selector.
        #[arg(long, value_name = "FILE")]
        runner: Option<PathBuf>,

        /// Bake in a pipeline that resolves a secret, writing that secret's
        /// plaintext into the output file.
        #[arg(long)]
        allow_secrets: bool,

        /// Leave the engine out, producing a small artifact that needs a DuckDB
        /// wherever it runs. The default embeds one.
        #[arg(long)]
        no_embed: bool,

        /// Build for another operating system, named as DuckDB names its
        /// platforms: `linux_amd64`, `windows_amd64`, `osx_arm64`. Defaults to
        /// this machine. See `scripts/fetch-duckdb.ps1 -Platform` and
        /// `scripts/build-runner.ps1` for what a target needs vendored first.
        #[arg(long, value_name = "PLATFORM")]
        target: Option<String>,

        #[command(flatten)]
        settings: Settings,
    },

    /// Serve this workspace to an agent over MCP, on stdin and stdout.
    ///
    /// Started by the agent, not by hand: Claude Code runs it from a
    /// `.mcp.json` entry such as `{"command": "etl", "args": ["mcp",
    /// "--workspace", "."]}`. Nothing listens on a port. See docs/mcp.md.
    Mcp {
        /// DuckDB binary to use, as `run` takes it.
        #[arg(long, value_name = "PATH")]
        duckdb: Option<PathBuf>,

        /// Skip per-stage row counts on the runs it starts.
        #[arg(long)]
        no_counts: bool,

        #[command(flatten)]
        settings: Settings,
    },

    /// Ask a local model for a pipeline, in words.
    ///
    /// Runs llama.cpp's llama-server and a small coding model on this machine
    /// (fetch both with scripts/fetch-model.ps1); nothing leaves it. What the
    /// model writes is checked as `validate` checks a file before it is shown
    /// or written. A minute or two on a laptop's CPU.
    Assist {
        /// What the pipeline should do, e.g. "read the orders table from
        /// Postgres, dedupe on id, write Parquet".
        request: String,

        /// Write the pipeline here rather than to standard output.
        #[arg(long, short = 'o', value_name = "FILE")]
        out: Option<PathBuf>,

        /// Replace --out if it already exists.
        #[arg(long)]
        overwrite: bool,

        /// The GGUF model to run. Defaults to ETL_ASSIST_MODEL, then the
        /// vendored copy in tools/models.
        #[arg(long, value_name = "FILE")]
        model: Option<PathBuf>,

        /// llama.cpp's server. Defaults to ETL_LLAMA_SERVER, then the
        /// vendored copy in tools/llama.
        #[arg(long, value_name = "FILE")]
        llama_server: Option<PathBuf>,

        /// Sampling seed, for an answer that can be had again. Random unless
        /// given.
        #[arg(long)]
        seed: Option<u64>,

        #[command(flatten)]
        settings: Settings,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let code = match cli.command {
        Command::Run {
            pipeline,
            duckdb,
            no_counts,
            show_sql,
            json,
            settings,
        } => command_run(&pipeline, duckdb, !no_counts, show_sql, json, &settings),

        Command::Serve {
            port,
            bind,
            duckdb,
            no_counts,
            settings,
        } => command_serve(port, bind, duckdb, !no_counts, &settings),

        Command::Schedule { action, settings } => command_schedule(action, &settings),

        Command::Validate { pipeline, settings } => command_validate(&pipeline, &settings),

        Command::Contexts { settings } => command_contexts(&settings),

        Command::State { action, settings } => command_state(action, &settings),

        Command::Runs { action, settings } => command_runs(action, &settings),

        Command::Lineage {
            pipeline,
            json,
            settings,
        } => command_lineage(&pipeline, json, &settings),

        Command::Secret { action, settings } => command_secret(action, &settings),

        Command::Components {
            namespace,
            manifest,
        } => command_components(namespace.as_deref(), manifest),

        Command::Plan {
            pipeline,
            script,
            no_counts,
            settings,
        } => command_plan(&pipeline, script, !no_counts, &settings),

        Command::Build {
            pipeline,
            out,
            runner,
            allow_secrets,
            no_embed,
            target,
            settings,
        } => command_build(
            &pipeline,
            out,
            runner,
            allow_secrets,
            !no_embed,
            target.as_deref(),
            &settings,
        ),

        Command::Mcp {
            duckdb,
            no_counts,
            settings,
        } => command_mcp(duckdb, !no_counts, settings),

        Command::Assist {
            request,
            out,
            overwrite,
            model,
            llama_server,
            seed,
            settings,
        } => command_assist(
            &request,
            out.as_deref(),
            overwrite,
            model.as_deref(),
            llama_server.as_deref(),
            seed,
            &settings,
        ),
    };

    ExitCode::from(code)
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// One execution, from a file on disk to a record in history.
///
/// Shared by `etl run` and the scheduler, so there is exactly one code path
/// that knows how a run is compiled, executed and recorded. That is the same
/// reason Settled decision 5 put the runner on `etl` rather than in a second
/// binary: two paths that have to agree about the same file forever are two
/// paths that eventually do not.
struct Performed {
    /// What was appended to history. Always present — a run that failed is
    /// still a run that happened.
    record: state::RunRecord,

    /// Absent when DuckDB failed before producing a report at all.
    report: Option<RunReport>,

    state_key: String,
}

/// Compile and run one pipeline, recording whatever happened.
///
/// `Err` is a pipeline that never started: unreadable, invalid, or a DuckDB
/// that is not there. Those are not recorded, because they are a broken
/// installation or a broken file rather than a failed run — the distinction
/// 8b drew and this keeps.
fn perform(
    pipeline: &Path,
    duckdb: Option<PathBuf>,
    counts: bool,
    show_sql: bool,
    settings: &Settings,
    quiet: bool,
) -> Result<Performed, u8> {
    let Loaded {
        plan,
        resolved,
        state_key,
    } = load_and_compile(pipeline, settings)?;

    if !quiet {
        report_warnings(&plan);
    }

    let options = RunOptions {
        duckdb_bin: duckdb,
        // The same directory `${workspace}` expands to, so a pipeline's
        // relative paths and its interpolated ones cannot disagree.
        working_dir: Some(settings.workspace_root()),
        counts,
        // Discovered: the vendored tools/duckdb/extensions/, or
        // ETL_DUCKDB_EXTENSIONS. Not a flag until someone needs one.
        extension_dir: None,
        redact: resolved.secret_values(),
    };

    if show_sql {
        // Masked: this goes to a terminal, and a connection string with a
        // password in it must not.
        println!("{}", resolved.redact(&plan.script(counts)));
    }

    // Stamped before the run rather than after, so the record says when the
    // work started -- which is what you want when reading back a run that
    // took an hour.
    let started = state::now_utc();
    let id = state::runs::id_for_now(std::process::id() as u64);

    match run(&plan, &options) {
        Ok(report) => {
            let record = record_of(id, &state_key, pipeline, started, &report);
            remember(settings, &state_key, &record);

            Ok(Performed {
                record,
                report: Some(report),
                state_key,
            })
        }

        Err(error) => {
            // A pipeline that could not run at all is a thing that happened,
            // and is exactly what someone asks history about the next morning.
            // The one exception is a missing DuckDB, which is a broken
            // installation rather than a failed pipeline.
            if let ExecError::DuckdbNotFound { .. } = error {
                eprintln!("error: {error}");
                return Err(exit::USAGE);
            }

            let record = failed_record(id, &state_key, pipeline, started, error.to_string());
            remember(settings, &state_key, &record);

            Ok(Performed {
                record,
                report: None,
                state_key,
            })
        }
    }
}

fn command_run(
    pipeline: &Path,
    duckdb: Option<PathBuf>,
    counts: bool,
    show_sql: bool,
    json: bool,
    settings: &Settings,
) -> u8 {
    let performed = match perform(pipeline, duckdb, counts, show_sql, settings, false) {
        Ok(performed) => performed,
        Err(code) => return code,
    };

    // JSON is the whole of stdout when it is asked for, so a script can parse
    // it without stripping a table off the front.
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&performed.record).expect("a record serialises")
        );
    }

    let Some(report) = &performed.report else {
        if !json {
            for failure in &performed.record.failures {
                eprintln!("error: {failure}");
            }
        }

        return exit::FAILED;
    };

    if !json {
        print_report(report);
    }

    // A report can describe a failed run: `continue_on_failure` hands back
    // everything that happened rather than only the first error, and the exit
    // code is what says it still failed.
    if report.failed() {
        // Deliberately no state written. The run wrote some of its output and
        // failed; leaving the watermark behind that output is the recoverable
        // direction, because the next run redoes the window rather than
        // skipping it.
        if !json && !report.watermarks.is_empty() {
            println!("  · watermarks not advanced: the run failed");
        }

        return exit::FAILED;
    }

    match save_state(settings, &performed.state_key, report, json) {
        Ok(()) => exit::OK,
        Err(code) => code,
    }
}

/// The per-stage table `etl run` prints.
fn print_report(report: &RunReport) {
    // The formatting lives in the engine, beside the type it formats, so that
    // `etl run` and a standalone runner cannot drift apart on what a rejected
    // count or a missing timing means. See `etl_duckdb_engine::report`.
    for line in etl_duckdb_engine::report_lines(report) {
        println!("{line}");
    }

    println!();
    println!(
        "Ran {} stage(s) in {:.2}s",
        report.stages.len(),
        report.elapsed.as_secs_f64()
    );
}

fn command_lineage(pipeline: &Path, json: bool, settings: &Settings) -> u8 {
    let Loaded { plan, resolved, .. } = match load_and_compile(pipeline, settings) {
        Ok(loaded) => loaded,
        Err(code) => return code,
    };

    let found = lineage(&plan);

    if json {
        let text = serde_json::to_string_pretty(&found).expect("lineage serialises");
        // Redacted for the same reason the script is: a resolved path can hold
        // a secret, and lineage is the output most likely to be pasted into a
        // ticket.
        println!("{}", resolved.redact(&text));
        return exit::OK;
    }

    println!("{}", pipeline.display());

    println!(
        "
Reads:"
    );
    if found.inputs.is_empty() {
        println!("  (nothing outside the pipeline)");
    }
    for input in &found.inputs {
        println!(
            "  {}  ({})  via {}",
            resolved.redact(&input.name),
            input.component_id,
            input.node_id
        );
    }

    println!(
        "
Writes:"
    );
    if found.outputs.is_empty() {
        // Every non-sink stage is a lazy view, so this pipeline computes
        // nothing when it runs. Worth saying plainly.
        println!("  (nothing — this pipeline has no sink)");
    }
    for output in &found.outputs {
        println!(
            "  {}  ({})  via {}",
            resolved.redact(&output.name),
            output.component_id,
            output.node_id
        );
    }

    println!(
        "
Flow:"
    );
    for edge in &found.edges {
        // The dead-letter branch is called out, because reading it as the main
        // flow gets the meaning backwards: those are the rows that failed.
        let port = if edge.port == "main" {
            String::new()
        } else {
            format!("  [{}]", edge.port)
        };
        println!("  {} → {}{}", edge.from, edge.to, port);
    }

    for node in found
        .nodes
        .iter()
        .filter(|n| n.incremental_column.is_some())
    {
        println!(
            "
'{}' loads incrementally on '{}', so a run reads only what is new.",
            node.id,
            node.incremental_column.as_deref().unwrap_or_default()
        );
    }

    exit::OK
}

// ---------------------------------------------------------------------------
// The console
// ---------------------------------------------------------------------------

/// The workspace, as the console sees it.
///
/// The seam `etl-console` is built around: it knows HTTP, tokens and a page,
/// and nothing about the engine. Everything it needs to answer comes through
/// here, where the engine, the resolver and the secret store already are — the
/// same arrangement the scheduler has, for the same reason.
struct ConsoleWorkspace {
    settings: Settings,
    duckdb: Option<PathBuf>,
    counts: bool,

    /// Held for the length of a triggered run.
    ///
    /// Requests are served from several threads, so without this two people
    /// clicking Run at the same moment would have two runs of the same
    /// pipeline racing on its watermark. Sequential runs are the same posture
    /// 8c took, and for the same reason: it keeps the state store's
    /// single-writer assumption true rather than hoping about it.
    running: std::sync::Mutex<()>,
}

impl ConsoleWorkspace {
    /// Every pipeline file in the workspace, as `<workspace>/**/ *.json` that
    /// parses as a pipeline document.
    ///
    /// Scanned rather than configured: the workspace is a folder of JSON, and
    /// a console that needed a manifest listing its own pipelines would be a
    /// second place to keep them in step.
    fn documents(&self) -> Vec<(String, PathBuf, PipelineDoc)> {
        let root = self.settings.workspace_root();
        let mut found = Vec::new();

        collect_pipelines(&root, 0, &mut found);
        // By name, so the console's ordering does not depend on the order the
        // filesystem happened to hand back.
        found.sort_by(|left, right| left.0.cmp(&right.0));

        found
    }

    /// Resolve a name from a request to a pipeline on disk.
    ///
    /// The one place a name from the network becomes a path, and it does so by
    /// *lookup* rather than by joining: a name that is not in the workspace's
    /// own list finds nothing, so `../../etc/passwd` is a 404 rather than a
    /// file read.
    fn locate(&self, name: &str) -> Result<PathBuf, console::Failure> {
        self.documents()
            .into_iter()
            .find(|(known, _, _)| known == name)
            .map(|(_, path, _)| path)
            .ok_or_else(|| console::Failure::not_found(format!("no pipeline called '{name}'")))
    }
}

impl console::Workspace for ConsoleWorkspace {
    fn label(&self) -> String {
        let root = self.settings.workspace_root();

        root.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| root.display().to_string())
    }

    fn pipelines(&self) -> Result<Vec<console::PipelineSummary>, console::Failure> {
        let history = state::History::at(self.settings.workspace_root());
        let mut summaries = Vec::new();

        for (name, path, document) in self.documents() {
            // Compiled so the console can say which pipelines will not run,
            // which is the thing worth knowing before 3am. A failure here is a
            // property of that one pipeline, not of the request.
            let (stages, problem) = match load_and_compile_quietly(&path, &self.settings) {
                Ok(loaded) => (Some(loaded.plan.stages.len()), None),
                Err(message) => (None, Some(message)),
            };

            let key = state::key_for(document.name.as_deref(), &path);
            let recent = history.recent(&key, 1).unwrap_or_default();
            let last = recent.first();

            summaries.push(console::PipelineSummary {
                name,
                path: relative_to(&self.settings.workspace_root(), &path),
                stages,
                problem,
                last_outcome: last.map(|record| record.outcome.name().to_string()),
                last_run: last.map(|record| record.started.clone()),
            });
        }

        Ok(summaries)
    }

    fn lineage(&self, name: &str) -> Result<serde_json::Value, console::Failure> {
        let path = self.locate(name)?;

        let Loaded { plan, resolved, .. } =
            load_and_compile_quietly(&path, &self.settings).map_err(console::Failure::invalid)?;

        let found = lineage(&plan);

        let text = serde_json::to_string(&found)
            .map_err(|error| console::Failure::internal(error.to_string()))?;

        // Redacted for the same reason `etl lineage --json` is: a resolved
        // path can hold a secret, and this one crosses a socket.
        serde_json::from_str(&resolved.redact(&text))
            .map_err(|error| console::Failure::internal(error.to_string()))
    }

    fn runs(
        &self,
        pipeline: Option<&str>,
        limit: usize,
    ) -> Result<Vec<state::RunRecord>, console::Failure> {
        let history = state::History::at(self.settings.workspace_root());

        let keys = match pipeline {
            Some(name) => {
                let path = self.locate(name)?;
                let document = read_document(&path).map_err(console::Failure::invalid)?;

                vec![state::key_for(document.name.as_deref(), &path)]
            }
            None => history
                .keys()
                .map_err(|error| console::Failure::internal(error.to_string()))?,
        };

        let mut records = Vec::new();

        for key in keys {
            records.extend(history.recent(&key, limit).unwrap_or_default());
        }

        // Newest first across every pipeline. The timestamps are UTC and
        // fixed-width, so a string sort is a chronological one — which is why
        // `now_utc` writes them that way.
        records.sort_by(|left, right| right.started.cmp(&left.started));
        records.truncate(limit);

        Ok(records)
    }

    fn run(&self, id: &str) -> Result<state::RunRecord, console::Failure> {
        let history = state::History::at(self.settings.workspace_root());

        history
            .find(None, id)
            .map_err(|error| console::Failure::internal(error.to_string()))?
            .ok_or_else(|| console::Failure::not_found(format!("no run called '{id}'")))
    }

    fn schedules(&self) -> Result<Vec<console::ScheduleSummary>, console::Failure> {
        let file = sched::ScheduleFile::load(&self.settings.schedules_path())
            .map_err(|error| console::Failure::invalid(error.to_string()))?;

        let scheduler = build_scheduler(file.schedules.iter().cloned(), &self.settings);

        Ok(scheduler
            .entries()
            .iter()
            .map(|entry| console::ScheduleSummary {
                name: entry.schedule.name.clone(),
                pipeline: entry.schedule.pipeline.display().to_string(),
                trigger: entry.schedule.trigger.describe(),
                enabled: entry.schedule.enabled,
                next: entry.next.map(state::time::to_rfc3339),
                last_run: entry.last_run.map(state::time::to_rfc3339),
            })
            .collect())
    }

    fn start(&self, name: &str) -> Result<state::RunRecord, console::Failure> {
        let path = self.locate(name)?;

        // One at a time, whatever the threads are doing. A poisoned lock means
        // an earlier run panicked; the lock is still usable and refusing every
        // subsequent run over it would be worse than carrying on.
        let _one_at_a_time = self.running.lock().unwrap_or_else(|held| held.into_inner());

        let performed = perform(
            &path,
            self.duckdb.clone(),
            self.counts,
            false,
            &self.settings,
            true,
        )
        .map_err(|_| {
            console::Failure::invalid(format!("'{name}' could not be compiled or started"))
        })?;

        if let Some(report) = &performed.report {
            if !report.failed() {
                // The same rule every other path follows: state advances only
                // on a run that fully succeeded.
                let _ = save_state(&self.settings, &performed.state_key, report, true);
            }
        }

        // A failed run is a 200 carrying a record that says it failed, not an
        // HTTP error: the request worked, and the record is the answer. The
        // page colours it by outcome.
        Ok(performed.record)
    }
}

// ---------------------------------------------------------------------------
// Assist
// ---------------------------------------------------------------------------

/// `etl assist` — a local model writes the pipeline; the engine checks it.
fn command_assist(
    request: &str,
    out: Option<&Path>,
    overwrite: bool,
    model: Option<&Path>,
    llama_server: Option<&Path>,
    seed: Option<u64>,
    settings: &Settings,
) -> u8 {
    if let Some(out) = out {
        if out.exists() && !overwrite {
            eprintln!(
                "error: {} already exists; pass --overwrite to replace it",
                out.display()
            );
            return exit::USAGE;
        }
    }

    // From the workspace, then from beside this `etl`, so a checkout's
    // vendored tools/ is found wherever the workspace is.
    let beside = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf));
    let find = |locate: fn(Option<&Path>, &Path) -> Result<PathBuf, String>,
                explicit: Option<&Path>| {
        locate(explicit, &settings.workspace_root()).or_else(|error| match &beside {
            Some(directory) => locate(explicit, directory).map_err(|_| error),
            None => Err(error),
        })
    };
    let located = find(etl_assistant::locate_server, llama_server)
        .and_then(|server| Ok((server, find(etl_assistant::locate_model, model)?)));
    let (server, model) = match located {
        Ok(found) => found,
        Err(message) => {
            eprintln!("error: {message}");
            return exit::USAGE;
        }
    };

    let log = std::env::temp_dir().join(format!("etl-llama-server-{}.log", std::process::id()));
    eprintln!(
        "Starting llama-server with {}...",
        model.file_name().unwrap_or_default().to_string_lossy()
    );
    let running = match etl_assistant::Server::start(&server, &model, &log) {
        Ok(running) => running,
        Err(message) => {
            eprintln!("error: {message}");
            return exit::USAGE;
        }
    };

    let seed = seed.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or_default()
    });
    eprintln!("Writing the pipeline (a minute or two on a CPU)...");
    let specs: Vec<etl_metadata::ComponentSpec> = registry().specs().cloned().collect();
    let drafted = etl_assistant::draft(&running, request, &specs, seed);
    drop(running);
    let draft = match drafted {
        Ok(draft) => draft,
        Err(message) => {
            eprintln!("error: {message}");
            eprintln!("  (llama-server's log: {})", log.display());
            return exit::FAILED;
        }
    };
    let _ = std::fs::remove_file(&log);

    let text = serde_json::to_string_pretty(&draft.document).unwrap_or_default();
    let checked = match check_document(&text, settings) {
        Ok(checked) => checked,
        Err(message) => {
            eprintln!("error: {message}");
            return exit::USAGE;
        }
    };
    let listed = |key: &str| -> Vec<String> {
        checked[key]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };

    if checked["valid"] != true {
        eprintln!("The model's pipeline does not validate (seed {seed}):");
        for error in listed("errors") {
            eprintln!("  error: {error}");
        }
        eprintln!("It wrote:\n{text}");
        return exit::INVALID;
    }
    for warning in listed("warnings") {
        eprintln!("warning: {warning}");
    }

    match out {
        None => println!("{text}"),
        Some(out) => {
            if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
                if let Err(error) = std::fs::create_dir_all(parent) {
                    eprintln!("error: {}: {error}", parent.display());
                    return exit::USAGE;
                }
            }
            if let Err(error) = std::fs::write(out, format!("{text}\n")) {
                eprintln!("error: {}: {error}", out.display());
                return exit::USAGE;
            }
            eprintln!(
                "Wrote {} ({} stages, valid; seed {seed})",
                out.display(),
                checked["stages"]
            );
        }
    }
    exit::OK
}

// ---------------------------------------------------------------------------
// MCP
// ---------------------------------------------------------------------------

/// `etl mcp` — serve this workspace to an agent over stdin and stdout.
///
/// Stdout is the protocol's from here on, so this says what it is doing on
/// stderr and nowhere else.
fn command_mcp(duckdb: Option<PathBuf>, counts: bool, settings: Settings) -> u8 {
    let root = settings.workspace_root();
    eprintln!(
        "etl mcp: serving {} over stdin and stdout",
        std::path::absolute(&root).unwrap_or(root).display()
    );

    let workspace = McpWorkspace {
        console: ConsoleWorkspace {
            settings,
            duckdb,
            counts,
            running: std::sync::Mutex::new(()),
        },
    };

    match etl_mcp::serve_stdio(workspace) {
        Ok(()) => exit::OK,
        Err(error) => {
            eprintln!("error: {error}");
            exit::USAGE
        }
    }
}

/// The seam `etl-mcp` is built around, as `ConsoleWorkspace` is the console's:
/// the tools know MCP, this knows the engine. The console's listing, history
/// and one-run-at-a-time lock are reused rather than written twice.
struct McpWorkspace {
    console: ConsoleWorkspace,
}

impl McpWorkspace {
    fn settings(&self) -> &Settings {
        &self.console.settings
    }
}

fn mcp_json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, String> {
    serde_json::to_value(value).map_err(|error| error.to_string())
}

impl etl_mcp::Workspace for McpWorkspace {
    fn root(&self) -> PathBuf {
        let root = self.settings().workspace_root();
        std::path::absolute(&root).unwrap_or(root)
    }

    fn manifest(&self) -> serde_json::Value {
        registry().manifest()
    }

    fn schema(&self) -> serde_json::Value {
        etl_metadata::schema::pipeline_schema(registry().specs())
    }

    fn pipelines(&self) -> Result<serde_json::Value, String> {
        let summaries = console::Workspace::pipelines(&self.console).map_err(|f| f.message)?;
        mcp_json(&summaries)
    }

    fn validate(
        &self,
        document: &str,
        bindings: &etl_mcp::Bindings,
    ) -> Result<serde_json::Value, String> {
        check_document(document, &self.settings().with(bindings))
    }

    fn plan(
        &self,
        pipeline: &Path,
        bindings: &etl_mcp::Bindings,
    ) -> Result<serde_json::Value, String> {
        let settings = self.settings().with(bindings);
        let Loaded { plan, resolved, .. } = load_and_compile_quietly(pipeline, &settings)?;

        let stages: Vec<serde_json::Value> = plan
            .stages
            .iter()
            .map(|stage| {
                serde_json::json!({
                    "node": stage.node_id,
                    "label": stage.label,
                    "component": stage.component_id,
                    "kind": format!("{:?}", stage.kind),
                    "sql": resolved.redact(&stage.sql),
                })
            })
            .collect();
        Ok(serde_json::json!({
            "stages": stages,
            "warnings": plan.warnings.iter().map(warning_text).collect::<Vec<_>>(),
        }))
    }

    fn lineage(
        &self,
        pipeline: &Path,
        bindings: &etl_mcp::Bindings,
    ) -> Result<serde_json::Value, String> {
        let settings = self.settings().with(bindings);
        let Loaded { plan, resolved, .. } = load_and_compile_quietly(pipeline, &settings)?;

        // Redacted as `etl lineage --json` is: a resolved path can hold a secret.
        let text = serde_json::to_string(&lineage(&plan)).map_err(|e| e.to_string())?;
        serde_json::from_str(&resolved.redact(&text)).map_err(|error| error.to_string())
    }

    fn run(
        &self,
        pipeline: &Path,
        bindings: &etl_mcp::Bindings,
    ) -> Result<serde_json::Value, String> {
        let settings = self.settings().with(bindings);

        // Compiled quietly first, so a pipeline that will not start says why
        // in the result rather than on a stderr the agent never reads.
        load_and_compile_quietly(pipeline, &settings)?;

        // One at a time, as the console does, for the same watermark reason.
        let _one_at_a_time = self
            .console
            .running
            .lock()
            .unwrap_or_else(|held| held.into_inner());

        let performed = perform(
            pipeline,
            self.console.duckdb.clone(),
            self.console.counts,
            false,
            &settings,
            true,
        )
        .map_err(|_| {
            format!(
                "{} could not be started: no DuckDB was found (tools/duckdb, ETL_DUCKDB_BIN or \
                 PATH)",
                pipeline.display()
            )
        })?;

        if let Some(report) = &performed.report {
            if !report.failed() {
                // State advances only on a run that fully succeeded.
                save_state(&settings, &performed.state_key, report, true).map_err(|_| {
                    "the run succeeded but its watermarks were not saved".to_string()
                })?;
            }
        }

        let mut record = mcp_json(&performed.record)?;
        record["outcome"] = serde_json::json!(performed.record.outcome.name());
        Ok(record)
    }

    fn runs(&self, pipeline: Option<&str>, limit: usize) -> Result<serde_json::Value, String> {
        let records =
            console::Workspace::runs(&self.console, pipeline, limit).map_err(|f| f.message)?;
        let listed: Vec<serde_json::Value> = records
            .iter()
            .map(|record| {
                serde_json::json!({
                    "id": record.id,
                    "pipeline": record.pipeline,
                    "outcome": record.outcome.name(),
                    "started": record.started,
                    "elapsedMs": record.elapsed_ms,
                    "rowsWritten": record.rows_written(),
                })
            })
            .collect();
        Ok(serde_json::Value::Array(listed))
    }

    fn run_record(&self, id: &str) -> Result<serde_json::Value, String> {
        let record = console::Workspace::run(&self.console, id).map_err(|f| f.message)?;
        let mut value = mcp_json(&record)?;
        value["outcome"] = serde_json::json!(record.outcome.name());
        Ok(value)
    }

    fn build(
        &self,
        pipeline: &Path,
        target: Option<&str>,
        out: &Path,
        bindings: &etl_mcp::Bindings,
    ) -> Result<serde_json::Value, String> {
        let settings = self.settings().with(bindings);

        // An agent naming `dist/orders` should not need a second call to make
        // `dist`; `inside` has already kept it in the workspace.
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("{}: {error}", parent.display()))?;
        }

        // Never with the secret baked in: over MCP there is no one to read the
        // warning `etl build --allow-secrets` exists to make someone read.
        let built = build_artifact(
            pipeline,
            Some(out.to_path_buf()),
            None,
            false,
            true,
            target,
            &settings,
            true,
        )
        .map_err(|(_, message)| {
            // The CLI's refusal names a flag this tool does not have.
            message.replace(
                "Pass --allow-secrets if that is what you want.",
                "Build it with `etl build --allow-secrets` if that is what you want.",
            )
        })?;

        Ok(serde_json::json!({
            "built": built.destination.display().to_string(),
            "pipeline": built.state_key,
            "stages": built.stages,
            "bytes": built.size,
            "engine": built.engine,
            "extensions": built.extensions,
            "notes": built.notes,
        }))
    }

    fn connections(&self) -> Result<serde_json::Value, String> {
        let root = self.settings().workspace_root();
        let path = self
            .settings()
            .contexts
            .clone()
            .unwrap_or_else(|| Contexts::path_in(&root));
        let contexts = Contexts::load(&path).map_err(|error| error.to_string())?;

        // Variable names, not values: the names are what a document refers to.
        let listed: Vec<serde_json::Value> = contexts
            .contexts
            .iter()
            .map(|(name, context)| {
                serde_json::json!({
                    "name": name,
                    "active": contexts.active.as_deref() == Some(name.as_str()),
                    "description": context.description,
                    "variables": context.variables.keys().collect::<Vec<_>>(),
                })
            })
            .collect();

        // Names and descriptions, as `etl secret list` prints them. There is
        // deliberately no path from here to a value.
        let secrets: Vec<serde_json::Value> = if SecretStore::has_key(&root) {
            let store = SecretStore::open_existing(&root).map_err(|error| error.to_string())?;
            store
                .names()
                .into_iter()
                .map(|name| {
                    serde_json::json!({
                        "name": name,
                        "description": store.description(name),
                        "reference": format!("${{SECRET:{name}}}"),
                    })
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(serde_json::json!({ "contexts": listed, "secrets": secrets }))
    }
}

/// Check a document's text as `etl validate` checks a file: parsed, its
/// parameters resolved, compiled. `{"valid": false, "errors": [...]}` is a
/// document at fault; `Err` is a workspace whose contexts or secrets cannot be
/// read. Shared by MCP's `validate_pipeline` and `etl assist`.
fn check_document(document: &str, settings: &Settings) -> Result<serde_json::Value, String> {
    let invalid = |errors: Vec<String>, warnings: Vec<String>| serde_json::json!({ "valid": false, "errors": errors, "warnings": warnings });

    let document = match PipelineDoc::from_json(document) {
        Ok(document) => document,
        Err(error) => {
            return Ok(invalid(
                vec![format!("not a pipeline document: {error}")],
                Vec::new(),
            ))
        }
    };
    let resolver = settings.try_resolver()?;
    let resolved = match params::resolve(&document, &resolver) {
        Ok(resolved) => resolved,
        Err(error) => return Ok(invalid(vec![error.to_string()], Vec::new())),
    };

    let mut warnings: Vec<String> = resolved.warnings.iter().map(param_warning_text).collect();
    match compile_with(&resolved.document, &CompileOptions::default()) {
        Err(error) => Ok(invalid(vec![resolved.redact(&error.to_string())], warnings)),
        Ok(plan) => {
            warnings.extend(plan.warnings.iter().map(warning_text));
            Ok(serde_json::json!({
                "valid": true,
                "stages": plan.stages.len(),
                "sinks": plan.sinks().count(),
                "warnings": warnings
                    .iter()
                    .map(|warning| resolved.redact(warning))
                    .collect::<Vec<_>>(),
                // A secret's entry reads [REDACTED] here, never its value.
                "resolved": resolved.used,
            }))
        }
    }
}

/// Every pipeline document under a directory, at most a few levels down.
///
/// Bounded rather than unbounded: a workspace is a folder somebody keeps
/// pipelines in, and walking an arbitrarily deep tree on every page refresh is
/// how a console becomes the slowest thing on the machine. `.etl/`, `target/`
/// and dot-directories are skipped — none of them hold pipelines, and `.etl/`
/// holds run history that would parse as nothing and cost a read each time.
fn collect_pipelines(
    directory: &Path,
    depth: usize,
    into: &mut Vec<(String, PathBuf, PipelineDoc)>,
) {
    const MAX_DEPTH: usize = 4;

    if depth > MAX_DEPTH {
        return;
    }

    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();

        let name = entry.file_name();
        let name = name.to_string_lossy();

        if path.is_dir() {
            if name.starts_with('.') || name == "target" || name == "node_modules" {
                continue;
            }

            collect_pipelines(&path, depth + 1, into);
            continue;
        }

        if !name.ends_with(".json") {
            continue;
        }

        // A document rather than a file: `contexts.json` and `schedules.json`
        // are JSON and are not pipelines, and this is what tells them apart
        // without keeping a list of names to exclude.
        let Ok(document) = read_document(&path) else {
            continue;
        };

        if document.nodes.is_empty() {
            continue;
        }

        // The same key `state::key_for` uses, so a name shown here agrees with
        // where that pipeline's run records were written.
        let key = state::key_for(document.name.as_deref(), &path);

        into.push((key, path, document));
    }
}

fn read_document(path: &Path) -> Result<PipelineDoc, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;

    PipelineDoc::from_json(&text)
        .map_err(|error| format!("{} is not a valid pipeline: {error}", path.display()))
}

/// `load_and_compile`, with the message returned rather than printed.
///
/// The CLI's version writes to stderr, which is right for a command and wrong
/// for a request: a console serving twelve pipelines would print twelve errors
/// into the terminal on every page refresh.
fn load_and_compile_quietly(pipeline: &Path, settings: &Settings) -> Result<Loaded, String> {
    let document = read_document(pipeline)?;

    let resolver = settings.try_resolver()?;

    let resolved = params::resolve(&document, &resolver).map_err(|error| error.to_string())?;

    let state_key = state::key_for(resolved.document.name.as_deref(), pipeline);
    let store = state::Store::at(settings.workspace_root());

    let stored = store.load(&state_key).map_err(|error| error.to_string())?;

    // Quietly: a set-aside watermark is said by `etl run`, not on every page
    // the console serves.
    let options = remember::compile_options(&resolved.document, &stored).options;

    let plan = compile_with(&resolved.document, &options)
        .map_err(|error| resolved.redact(&error.to_string()))?;

    Ok(Loaded {
        plan,
        resolved,
        state_key,
    })
}

/// A path under the workspace, written relative to it for display.
fn relative_to(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
        .replace('\\', "/")
}

fn command_serve(
    port: u16,
    bind: Option<String>,
    duckdb: Option<PathBuf>,
    counts: bool,
    settings: &Settings,
) -> u8 {
    let host = bind.unwrap_or_else(|| "127.0.0.1".to_string());

    let address = match format!("{host}:{port}").parse::<std::net::SocketAddr>() {
        Ok(address) => address,
        Err(_) => {
            eprintln!("error: '{host}' is not an address to bind. Try 127.0.0.1, or 0.0.0.0.");
            return exit::USAGE;
        }
    };

    let options = console::ServeOptions {
        address,
        ..console::ServeOptions::default()
    };

    let tokens = console::Tokens::from_environment_or_mint();

    // Refused rather than warned about: one token for both roles silently
    // promotes every viewer to an operator, which is the mistake here that
    // looks like it is working.
    if tokens.roles_collide() {
        eprintln!(
            "error: {} and {} are set to the same value, which would make every viewer an \
             operator. Give them different tokens, or unset one and let it be minted.",
            console::auth::OPERATOR_ENV,
            console::auth::VIEWER_ENV
        );
        return exit::USAGE;
    }

    let serving = match console::serve(options.clone(), tokens) {
        Ok(serving) => serving,
        Err(error) => {
            eprintln!("error: {error}");
            return exit::USAGE;
        }
    };

    for warning in options.warnings() {
        eprintln!("warning: {warning}\n");
    }

    let workspace = ConsoleWorkspace {
        settings: settings.clone(),
        duckdb,
        counts,
        running: std::sync::Mutex::new(()),
    };

    println!(
        "Console for {} on http://{}",
        settings.workspace_root().display(),
        serving.address()
    );

    // A minted token exists nowhere else, so it is printed. One taken from the
    // environment is somebody's standing secret and is not — putting it in the
    // scrollback and the CI log of every run is how a stable token leaks.
    let tokens = serving.tokens();

    match tokens.operator_source() {
        console::Source::Minted => println!("\n  operator  {}", serving.link(tokens.operator())),
        console::Source::Environment => {
            println!("\n  operator  from {}", console::auth::OPERATOR_ENV)
        }
    }

    match tokens.viewer_source() {
        console::Source::Minted => println!("  viewer    {}", serving.link(tokens.viewer())),
        console::Source::Environment => println!("  viewer    from {}", console::auth::VIEWER_ENV),
    }

    println!(
        "\nAn operator can start runs; a viewer can only read. Minted tokens last as long as \
         this process.\nCtrl-C to stop."
    );

    serving.run(std::sync::Arc::new(workspace));

    exit::OK
}

// ---------------------------------------------------------------------------
// Schedules
// ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
enum ScheduleAction {
    /// Show the workspace's schedules and when each one next fires.
    List {
        /// Emit JSON rather than a table.
        #[arg(long)]
        json: bool,
    },

    /// Check the schedule file without running anything.
    ///
    /// Every pipeline it names is compiled too, because a schedule pointing at
    /// a pipeline that will not compile is the failure you want to find now
    /// rather than at 3am.
    Check,

    /// Run pipelines as they come due, until interrupted.
    Start {
        /// Look once at what is due, run it, and exit. What a cron entry or a
        /// CI job wants; also the way to try a schedule without staying up.
        #[arg(long)]
        once: bool,

        /// Take the workspace lock even if a lock file is already there.
        #[arg(long)]
        force: bool,

        /// DuckDB binary to use, as `run` takes it.
        #[arg(long, value_name = "PATH")]
        duckdb: Option<PathBuf>,

        /// Skip per-stage row counts, as `run` does.
        #[arg(long)]
        no_counts: bool,
    },
}

fn command_schedule(action: ScheduleAction, settings: &Settings) -> u8 {
    let path = settings.schedules_path();

    let file = match sched::ScheduleFile::load(&path) {
        Ok(file) => file,
        Err(error) => {
            eprintln!("error: {error}");
            return exit::INVALID;
        }
    };

    match action {
        ScheduleAction::List { json } => command_schedule_list(&file, json, settings),
        ScheduleAction::Check => command_schedule_check(&file, &path, settings),
        ScheduleAction::Start {
            once,
            force,
            duckdb,
            no_counts,
        } => command_schedule_start(&file, force, once, duckdb, !no_counts, settings),
    }
}

fn command_schedule_list(file: &sched::ScheduleFile, json: bool, settings: &Settings) -> u8 {
    if file.schedules.is_empty() && !json {
        println!(
            "No schedules. Write some to {}",
            settings.schedules_path().display()
        );
        return exit::OK;
    }

    // Built over every schedule rather than only the enabled ones, so a
    // disabled row can still say what it would do.
    let scheduler = build_scheduler(file.schedules.iter().cloned(), settings);

    if json {
        let rows: Vec<serde_json::Value> = scheduler
            .entries()
            .iter()
            .map(|entry| {
                serde_json::json!({
                    "name": entry.schedule.name,
                    "pipeline": entry.schedule.pipeline.display().to_string(),
                    "trigger": entry.schedule.trigger.describe(),
                    "enabled": entry.schedule.enabled,
                    "next": entry.next.map(state::time::to_rfc3339),
                    "lastRun": entry.last_run.map(state::time::to_rfc3339),
                })
            })
            .collect();

        println!(
            "{}",
            serde_json::to_string_pretty(&rows).expect("schedules serialise")
        );

        return exit::OK;
    }

    let width = scheduler
        .entries()
        .iter()
        .map(|entry| entry.schedule.name.chars().count())
        .max()
        .unwrap_or(0);

    let now = state::time::now_unix();

    for entry in scheduler.entries() {
        // A disabled schedule is still listed — one that vanished would be
        // harder to debug than one that says it is off — but it must not
        // advertise a next fire it is never going to reach.
        let next = if entry.schedule.enabled {
            entry.describe_next(now)
        } else {
            "off".to_string()
        };

        println!(
            "  {:width$}  {:<22}  {}",
            entry.schedule.name,
            entry.schedule.trigger.describe(),
            next
        );
    }

    let enabled = file.enabled().count();
    println!(
        "\n{} schedule(s), {enabled} enabled. Times are UTC.",
        file.schedules.len()
    );

    exit::OK
}

fn command_schedule_check(file: &sched::ScheduleFile, path: &Path, settings: &Settings) -> u8 {
    if file.schedules.is_empty() {
        println!("No schedules in {}", path.display());
        return exit::OK;
    }

    // The file itself is already checked by `load`; what is left is whether
    // each schedule points at a pipeline that exists and compiles.
    let mut broken = 0;

    let width = file
        .schedules
        .iter()
        .map(|schedule| schedule.name.chars().count())
        .max()
        .unwrap_or(0);

    for schedule in &file.schedules {
        let pipeline = schedule.pipeline_in(&settings.workspace_root());
        let settings = settings.for_schedule(schedule);

        // Compiling prints its own errors, so a broken one has already said
        // why by the time its row is written.
        let verdict = match load_and_compile(&pipeline, &settings) {
            Ok(_) => "ok    ",
            Err(_) => {
                broken += 1;
                "BROKEN"
            }
        };

        println!(
            "  {verdict}  {:width$}  {}",
            schedule.name,
            pipeline.display()
        );
    }

    if broken > 0 {
        println!(
            "\n{broken} of {} schedule(s) will not run.",
            file.schedules.len()
        );
        return exit::INVALID;
    }

    println!("\n{} schedule(s), all runnable.", file.schedules.len());

    exit::OK
}

fn command_schedule_start(
    file: &sched::ScheduleFile,
    force: bool,
    once: bool,
    duckdb: Option<PathBuf>,
    counts: bool,
    settings: &Settings,
) -> u8 {
    let workspace = settings.workspace_root();

    if file.enabled().count() == 0 {
        eprintln!(
            "error: no enabled schedules in {}",
            settings.schedules_path().display()
        );
        return exit::USAGE;
    }

    // Taken before anything runs, and held for as long as this process does.
    // One scheduler per workspace is what keeps the state store's
    // single-writer assumption true rather than merely hoped for.
    let lock = match sched::WorkspaceLock::acquire(&workspace, force) {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("error: {error}");
            return exit::USAGE;
        }
    };

    let mut scheduler = build_scheduler(file.enabled().cloned(), settings);

    println!(
        "Scheduling {} pipeline(s) from {}. Times are UTC.",
        scheduler.entries().len(),
        settings.schedules_path().display()
    );

    let now = state::time::now_unix();

    for entry in scheduler.entries() {
        // Downtime, said once at startup. It is a fact about the past, and
        // repeating it every tick would turn it into noise.
        let behind = match entry.behind {
            0 => String::new(),
            1 => "  (1 interval behind)".to_string(),
            count => format!("  ({count} intervals behind)"),
        };

        println!(
            "  {}  {}  next {}{behind}",
            entry.schedule.name,
            entry.schedule.trigger.describe(),
            entry.describe_next(now)
        );
    }

    if once {
        println!("\nOne pass, then exiting.");
    } else {
        println!("\nRunning. Ctrl-C to stop.");
    }

    let clock = sched::SystemClock;

    let summary = scheduler.run(
        &clock,
        &mut |schedule| run_scheduled(schedule, duckdb.clone(), counts, settings),
        if once { Some(1) } else { None },
        &mut || false,
    );

    let nothing_left = scheduler.wake_at().is_none();

    // Released explicitly rather than at the end of the scope, so what is
    // printed below comes from a process that no longer holds the workspace.
    lock.release();

    if summary.missed() > 0 {
        println!(
            "\n{} tick(s) were missed while an earlier run was still going.",
            summary.missed()
        );
    }

    if !once && nothing_left {
        // The loop ended on its own, which only happens when nothing can ever
        // be due again. Saying so beats exiting silently and looking crashed.
        println!("\nNothing left to wait for: no schedule can fire again.");
    }

    println!(
        "Ran {} pipeline(s), {} failed.",
        summary.ran(),
        summary.failed()
    );

    if summary.failed() > 0 {
        exit::FAILED
    } else {
        exit::OK
    }
}

/// Run one scheduled pipeline, printing a line for it.
///
/// Goes through `perform`, which is the same path `etl run` takes, so a
/// scheduled run is recorded in history and advances its watermarks exactly as
/// a hand-run one does.
fn run_scheduled(
    schedule: &sched::Schedule,
    duckdb: Option<PathBuf>,
    counts: bool,
    settings: &Settings,
) -> sched::Outcome {
    let settings = settings.for_schedule(schedule);
    let pipeline = schedule.pipeline_in(&settings.workspace_root());
    let at = state::now_utc();

    println!("\n[{at}] {} — {}", schedule.name, pipeline.display());

    let performed = match perform(&pipeline, duckdb, counts, false, &settings, true) {
        Ok(performed) => performed,
        Err(_) => {
            // `perform` has already said what was wrong. A schedule pointing
            // at a broken pipeline keeps its place in the rota rather than
            // bringing the whole scheduler down — the other schedules are not
            // at fault.
            println!("  broken: it did not start");
            return sched::Outcome::Broken;
        }
    };

    let Some(report) = &performed.report else {
        for failure in &performed.record.failures {
            println!("  failed: {failure}");
        }

        return sched::Outcome::Failed;
    };

    if report.failed() {
        for failure in &report.failures {
            println!(
                "  ! {} ({}): {}",
                failure.label, failure.node_id, failure.message
            );
        }

        println!(
            "  failed after {:.2}s ({} stage(s))",
            report.elapsed.as_secs_f64(),
            report.stages.len()
        );

        return sched::Outcome::Failed;
    }

    let rows = performed
        .record
        .rows_written()
        .map(|rows| format!("{rows} rows"))
        .unwrap_or_else(|| "no sink".to_string());

    println!(
        "  ok, {rows} in {:.2}s ({} stage(s))",
        report.elapsed.as_secs_f64(),
        report.stages.len()
    );

    // Quiet: the scheduler prints one line per run, and a watermark note per
    // node would bury it.
    if save_state(&settings, &performed.state_key, report, true).is_err() {
        // The run worked and its output is written; the state did not save.
        // `save_state` has already said so on stderr. Reported as a
        // failure because the next run will now redo this window.
        return sched::Outcome::Failed;
    }

    sched::Outcome::Succeeded
}

/// Build a scheduler, telling it when each pipeline last ran.
///
/// The last run comes from `.etl/runs/` — 8b's history — which is why an
/// interval survives a restart: an hourly pipeline that ran at 02:00 is due at
/// 03:00 whether or not the scheduler was up in between.
fn build_scheduler(
    schedules: impl IntoIterator<Item = sched::Schedule>,
    settings: &Settings,
) -> sched::Scheduler {
    let workspace = settings.workspace_root();
    let history = state::History::at(&workspace);

    sched::Scheduler::new(schedules, &workspace, state::time::now_unix(), |schedule| {
        let pipeline = schedule.pipeline_in(&workspace);

        // Keyed by the document's `name` when it has one, so this agrees with
        // where the run records were actually written. A pipeline that cannot
        // be read has no history, which is the same answer as never having
        // run — and `check` is where that gets reported.
        let text = std::fs::read_to_string(&pipeline).ok()?;
        let document = PipelineDoc::from_json(&text).ok()?;
        let key = state::key_for(document.name.as_deref(), &pipeline);

        let recent = history.recent(&key, 1).ok()?;

        state::time::from_rfc3339(&recent.first()?.started)
    })
}

fn command_validate(pipeline: &Path, settings: &Settings) -> u8 {
    let Loaded { plan, resolved, .. } = match load_and_compile(pipeline, settings) {
        Ok(loaded) => loaded,
        Err(code) => return code,
    };

    report_warnings(&plan);

    println!(
        "{} is valid: {} stage(s), {} sink(s)",
        pipeline.display(),
        plan.stages.len(),
        plan.sinks().count()
    );

    if !resolved.used.is_empty() {
        println!(
            "
Resolved:"
        );
        for (reference, value) in &resolved.used {
            println!("  ${{{reference}}} = {value}");
        }
    }

    exit::OK
}

/// Record where each incremental source got to: watermarks, and the
/// checkpoints of native sources that keep a position (Kafka's offsets).
///
/// Called only after a run that fully succeeded. It writes the whole state in
/// one atomic replace, so a crash here leaves the previous watermarks and
/// checkpoints intact rather than a mixture of old and new.
///
/// A failure to save is a failure of the command even though the data landed:
/// the run is not repeatable-from-here if nobody recorded where here is, and
/// the next run would silently reload from the old mark. Saying so loudly is
/// the only way that gets noticed.
fn save_state(settings: &Settings, key: &str, report: &RunReport, quiet: bool) -> Result<(), u8> {
    if report.watermarks.is_empty() && report.checkpoints.is_empty() {
        return Ok(());
    }

    let store = state::Store::at(settings.workspace_root());

    let mut stored = store.load(key).map_err(|error| {
        eprintln!("error: the run succeeded but its state could not be read: {error}");
        exit::FAILED
    })?;

    // The rule itself is the engine's, shared with `etl-runner`.
    let remembered = remember::remember(report, &mut stored);

    if !quiet {
        for (node, value) in &remembered.advanced {
            println!("  · {node} watermark now {value}");
        }
        // A source that loaded nothing keeps the mark it had; saying so is
        // worth a line, because "nothing happened" is the normal outcome of an
        // incremental pipeline and should not look like a broken one.
        for node in &remembered.nothing_new {
            println!("  · {node} had nothing new");
        }
        for node in &remembered.positions {
            println!("  · {node} position saved");
        }
    }

    if !remembered.changed() {
        return Ok(());
    }

    store.save(key, &stored).map_err(|error| {
        eprintln!("error: the run succeeded but its state could not be saved: {error}");
        eprintln!("       the next run will re-read from the previous watermark or position");
        exit::FAILED
    })
}

#[derive(Subcommand, Debug)]
enum StateAction {
    /// Show every watermark and saved position this workspace holds.
    List {
        /// Only this pipeline. Defaults to all of them.
        #[arg(long, value_name = "KEY")]
        key: Option<String>,
    },

    /// Forget a watermark or position, so the next run reads that source from the start.
    Forget {
        /// The pipeline's state key, as `state list` prints it.
        key: String,

        /// The node to forget. Leaving this out forgets the whole pipeline.
        #[arg(long, value_name = "NODE")]
        node: Option<String>,
    },
}

fn command_state(action: StateAction, settings: &Settings) -> u8 {
    let store = state::Store::at(settings.workspace_root());

    match action {
        StateAction::List { key } => {
            let keys = match key {
                Some(one) => vec![one],
                None => match store.keys() {
                    Ok(keys) => keys,
                    Err(error) => {
                        eprintln!("error: {error}");
                        return exit::USAGE;
                    }
                },
            };

            if keys.is_empty() {
                println!("No pipeline in this workspace has run incrementally yet.");
                return exit::OK;
            }

            for key in keys {
                let stored = match store.load(&key) {
                    Ok(stored) => stored,
                    Err(error) => {
                        eprintln!("error: {error}");
                        return exit::INVALID;
                    }
                };

                println!("{key}");
                if stored.is_empty() {
                    println!("  (nothing remembered)");
                }
                for (node, watermark) in &stored.watermarks {
                    println!(
                        "  {node}  {} = {}  (at {})",
                        watermark.column, watermark.value, watermark.at
                    );
                }
                // A checkpoint is the connector's own, so it is shown as it was
                // saved: compact JSON, which for Kafka reads as offsets.
                for (node, checkpoint) in &stored.checkpoints {
                    println!(
                        "  {node}  {} position {}  (at {})",
                        checkpoint.component, checkpoint.value, checkpoint.at
                    );
                }
            }

            exit::OK
        }

        StateAction::Forget { key, node } => {
            let mut stored = match store.load(&key) {
                Ok(stored) => stored,
                Err(error) => {
                    eprintln!("error: {error}");
                    return exit::INVALID;
                }
            };

            match node {
                Some(node) => {
                    if !stored.forget(&node) {
                        // Not an error: the state someone asked to clear is
                        // already clear, which is the outcome they wanted.
                        println!("'{node}' had no watermark or position in '{key}'.");
                        return exit::OK;
                    }
                    println!("Forgot '{node}' in '{key}'. Its next run reads from the start.");
                }
                None => {
                    stored.forget_all();
                    println!("Forgot every watermark and position in '{key}'.");
                }
            }

            match store.save(&key, &stored) {
                Ok(()) => exit::OK,
                Err(error) => {
                    eprintln!("error: {error}");
                    exit::FAILED
                }
            }
        }
    }
}

fn command_contexts(settings: &Settings) -> u8 {
    let root = settings.workspace_root();
    let path = settings
        .contexts
        .clone()
        .unwrap_or_else(|| Contexts::path_in(&root));

    let contexts = match Contexts::load(&path) {
        Ok(contexts) => contexts,
        Err(error) => {
            eprintln!("error: {error}");
            return exit::USAGE;
        }
    };

    if contexts.is_empty() {
        println!(
            "No contexts defined. Create {} to add some.",
            path.display()
        );
        return exit::OK;
    }

    let width = contexts
        .names()
        .iter()
        .map(|name| name.chars().count())
        .max()
        .unwrap_or(0);

    for (name, context) in &contexts.contexts {
        let active = if contexts.active.as_deref() == Some(name.as_str()) {
            "*"
        } else {
            " "
        };

        println!(
            "{active} {:width$}  {} variable(s){}",
            name,
            context.variables.len(),
            context
                .description
                .as_deref()
                .map(|text| format!("  {text}"))
                .unwrap_or_default()
        );
    }

    println!(
        "
{} context(s); * is active",
        contexts.contexts.len()
    );
    exit::OK
}

fn command_components(namespace: Option<&str>, as_manifest: bool) -> u8 {
    let registry = registry();

    if as_manifest {
        println!(
            "{}",
            serde_json::to_string_pretty(&registry.manifest()).expect("the manifest serialises")
        );
        return exit::OK;
    }

    if let Some(prefix) = namespace {
        if Namespace::from_prefix(prefix).is_none() {
            eprintln!(
                "error: '{prefix}' is not a namespace; use one of: {}",
                Namespace::all()
                    .iter()
                    .map(|n| n.prefix())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            return exit::USAGE;
        }
    }

    let selected: Vec<_> = registry
        .specs()
        .filter(|spec| match namespace {
            Some(prefix) => spec.namespace.prefix() == prefix,
            None => true,
        })
        .collect();

    let width = selected
        .iter()
        .map(|spec| spec.id.chars().count())
        .max()
        .unwrap_or(0);

    for spec in &selected {
        let required: Vec<&str> = spec
            .properties
            .iter()
            .filter(|property| property.required)
            .map(|property| property.name.as_str())
            .collect();

        let needs = if required.is_empty() {
            String::new()
        } else {
            format!("  (needs {})", required.join(", "))
        };

        println!("  {:width$}  {}{}", spec.id, spec.label, needs);
    }

    println!(
        "
{} component(s)",
        selected.len()
    );
    exit::OK
}

fn command_secret(action: SecretAction, settings: &Settings) -> u8 {
    let root = settings.workspace_root();

    match action {
        SecretAction::Init => {
            let existed = SecretStore::has_key(&root);

            match SecretStore::initialise(&root) {
                Ok(path) if existed => {
                    println!("This workspace already has a key at {}", path.display());
                    println!("Leaving it alone: a new key would not replace the old one, it");
                    println!("would make every existing secret permanently unreadable.");
                    exit::OK
                }
                Ok(path) => {
                    println!("Created {}", path.display());
                    println!();
                    println!("Back this file up somewhere safe and keep it out of version");
                    println!("control. Without it the secrets in this workspace cannot be");
                    println!("read, by you or by anyone else.");
                    exit::OK
                }
                Err(error) => {
                    eprintln!("error: {error}");
                    exit::USAGE
                }
            }
        }

        SecretAction::Set {
            name,
            value,
            stdin,
            description,
        } => {
            let value = if stdin {
                let mut text = String::new();
                if let Err(error) = std::io::Read::read_to_string(&mut std::io::stdin(), &mut text)
                {
                    eprintln!("error: could not read the value from stdin: {error}");
                    return exit::USAGE;
                }
                stdin_secret(&text)
            } else {
                value.unwrap_or_default()
            };

            let mut store = match SecretStore::open(&root) {
                Ok(store) => store,
                Err(error) => {
                    eprintln!("error: {error}");
                    return exit::USAGE;
                }
            };

            if let Err(error) = store.set(&name, &value, description.as_deref()) {
                eprintln!("error: {error}");
                return exit::USAGE;
            }

            if let Err(error) = store.save() {
                eprintln!("error: {error}");
                return exit::USAGE;
            }

            println!("Stored '{name}'. Reference it as ${{SECRET:{name}}}");
            exit::OK
        }

        SecretAction::List => {
            let store = match SecretStore::open_existing(&root) {
                Ok(store) => store,
                Err(error) => {
                    eprintln!("error: {error}");
                    return exit::USAGE;
                }
            };

            if store.is_empty() {
                println!("This workspace holds no secrets.");
                return exit::OK;
            }

            let width = store
                .names()
                .iter()
                .map(|name| name.chars().count())
                .max()
                .unwrap_or(0);

            // Names and descriptions only. There is deliberately no command
            // that prints a value: the one legitimate reader is a pipeline.
            for name in store.names() {
                println!(
                    "  {:width$}  {}",
                    name,
                    store.description(name).unwrap_or_default()
                );
            }

            println!("\n{} secret(s)", store.names().len());
            exit::OK
        }

        SecretAction::Remove { name } => {
            let mut store = match SecretStore::open_existing(&root) {
                Ok(store) => store,
                Err(error) => {
                    eprintln!("error: {error}");
                    return exit::USAGE;
                }
            };

            if !store.remove(&name) {
                eprintln!("error: there is no secret named '{name}'");
                return exit::USAGE;
            }

            if let Err(error) = store.save() {
                eprintln!("error: {error}");
                return exit::USAGE;
            }

            println!("Removed '{name}'");
            exit::OK
        }
    }
}

// ---------------------------------------------------------------------------
// Baking a pipeline into a binary
// ---------------------------------------------------------------------------

/// `etl build` — copy the runner and append this pipeline to the copy.
///
/// The document baked in is the **resolved** one, because the machine that runs
/// the artifact has none of what would resolve it: no contexts, no `.etl/`, no
/// secret key. That is also the reason for both refusals below — each is a case
/// where the resolved document would be quietly wrong on the far end rather
/// than obviously wrong here.
fn command_build(
    pipeline: &Path,
    out: Option<PathBuf>,
    runner: Option<PathBuf>,
    allow_secrets: bool,
    embed: bool,
    target: Option<&str>,
    settings: &Settings,
) -> u8 {
    let built = match build_artifact(
        pipeline,
        out,
        runner,
        allow_secrets,
        embed,
        target,
        settings,
        false,
    ) {
        Ok(built) => built,
        Err((code, _)) => return code,
    };

    println!("Built {}", built.destination.display());
    println!("  pipeline   {}", built.state_key);
    println!("  stages     {}", built.stages);
    println!("  size       {:.1} MB", built.size as f64 / 1_048_576.0);

    if built.carries_secrets {
        println!();
        println!("This file has a secret baked into it. Treat it as a credential.");
    }

    match &built.engine {
        None => {
            // Said plainly rather than left to be discovered.
            println!();
            println!("No engine is embedded, so this needs a DuckDB where it runs.");
        }
        Some(engine) => {
            println!("  engine     {engine}");
            if built.extensions.is_empty() {
                println!("  extensions none needed");
            } else {
                println!("  extensions {}", built.extensions.join(", "));
            }
        }
    }

    exit::OK
}

/// What `build_artifact` made.
struct Built {
    destination: PathBuf,
    state_key: String,
    stages: usize,
    size: u64,
    carries_secrets: bool,
    /// `DuckDB <version> (<platform>)` when one is embedded.
    engine: Option<String>,
    extensions: Vec<String>,
    /// What the artifact's owner has to know: where it keeps its state.
    notes: Vec<String>,
}

/// Bake a pipeline into an executable: `etl build`'s work, and MCP's.
///
/// Loud for `etl build`, which prints warnings, notes and refusals as it goes;
/// quiet for MCP, whose stdout is the protocol's. Either way an `Err` carries
/// the exit code and the whole message.
#[allow(clippy::too_many_arguments)]
fn build_artifact(
    pipeline: &Path,
    out: Option<PathBuf>,
    runner: Option<PathBuf>,
    allow_secrets: bool,
    embed: bool,
    target: Option<&str>,
    settings: &Settings,
    quiet: bool,
) -> Result<Built, (u8, String)> {
    let refuse = |code: u8, message: String| {
        if !quiet {
            eprintln!("error: {message}");
        }
        (code, message)
    };

    let target = match target {
        Some(named) => Target::named(named),
        None => Target::host(),
    };

    // Compiled, not merely parsed: shipping a file that turns out not to
    // compile is the one failure the far end is least equipped to diagnose.
    //
    // Built-ins are deferred rather than substituted. `${workspace}` means
    // "wherever this runs" and `${date}` means "the day it runs"; baking either
    // into an artifact meant to be copied elsewhere and run repeatedly freezes
    // this machine's directory layout and today's date into it. Everything
    // else -- parameters, contexts, secrets -- must be resolved here, because
    // the far side has nothing to resolve them with.
    let Loaded {
        plan,
        resolved,
        state_key,
    } = load_and_compile_deferring_built_ins(pipeline, settings, quiet)
        .map_err(|(code, message)| refuse(code, message))?;

    if !quiet {
        report_warnings(&plan);
    }

    let mut notes = Vec::new();

    // An incremental source needs somewhere to keep its high-water mark, and
    // the runner has no state store. Shipping one anyway would produce a
    // pipeline that re-reads everything on every run and says nothing about it,
    // which is worse than refusing. Lifting this means giving the runner state.
    let incremental = incremental_nodes(&resolved.document);

    // Until Phase 10e this was a refusal: an artifact had nowhere to keep a
    // watermark and would have re-read everything on every run. It now keeps
    // its state where it runs (Settled decision 36), which is worth saying at
    // build time, because that directory is what has to persist between runs.
    if !incremental.is_empty() {
        notes.push(format!(
            "{} loads incrementally. The artifact keeps its watermarks in .etl/state/ \
             under the directory it runs in (or --workspace); keep that directory between runs.",
            incremental.join(", ")
        ));
    }
    let streams = stream_nodes(&resolved.document);
    if !streams.is_empty() {
        notes.push(format!(
            "{} keeps a read position. The artifact saves it in .etl/state/ under the \
             directory it runs in (or --workspace); a run from a fresh directory starts over.",
            streams.join(", ")
        ));
    }
    if !quiet {
        for note in &notes {
            println!("note: {note}");
        }
    }

    if resolved.uses_secrets() && !allow_secrets {
        return Err(refuse(
            exit::USAGE,
            "this pipeline resolves a secret, and baking it would write that\n       \
             secret's plaintext into the output file. Anyone holding the file\n       \
             holds the credential.\n       \
             Pass --allow-secrets if that is what you want."
                .to_string(),
        ));
    }

    let runner = match runner {
        Some(explicit) if explicit.is_file() => explicit,
        Some(explicit) => {
            return Err(refuse(
                exit::USAGE,
                format!("no runner at {}", explicit.display()),
            ))
        }
        None => match locate_runner(&target, settings) {
            Some(found) => found,
            None if target.is_host => {
                return Err(refuse(
                    exit::USAGE,
                    "no `etl-runner` found beside this executable.\n       \
                     Build it with `cargo build -p etl-runner`, or name one\n       \
                     with --runner."
                        .to_string(),
                ))
            }
            None => {
                return Err(refuse(
                    exit::USAGE,
                    format!(
                        "no runner vendored for {}. Expected one at {}.\n       \
                         Build it with:\n         \
                         .\\scripts\\build-runner.ps1 -Platform {}",
                        target.platform,
                        target.runner_path(&toolchain_roots(settings)[0]).display(),
                        target.platform
                    ),
                ))
            }
        },
    };

    let destination = out.unwrap_or_else(|| {
        // A cross-built artifact gets the target in its name, because a
        // directory holding two files called `orders` that run on different
        // operating systems is a bad afternoon.
        if target.is_host {
            PathBuf::from(format!("{state_key}{}", target.exe_suffix()))
        } else {
            PathBuf::from(format!(
                "{state_key}-{}{}",
                target.platform,
                target.exe_suffix()
            ))
        }
    });

    let mut payload =
        etl_runner::Payload::new(&state_key, state::now_utc(), resolved.document.clone());
    payload.carries_secrets = resolved.uses_secrets();

    // The engine and whichever extensions this plan's components asked for --
    // not all nine, which would be 284 MB of which most is never loaded. The
    // registry already knows the answer; `Plan::extensions` is it.
    let blobs = if embed {
        gather_embedded(&plan, settings, &target, &mut payload)
            .map_err(|message| refuse(exit::USAGE, message))?
    } else {
        Vec::new()
    };

    payload
        .write_built(&runner, &destination, &blobs)
        .map_err(|error| refuse(exit::USAGE, error.to_string()))?;

    let size = std::fs::metadata(&destination)
        .map(|meta| meta.len())
        .unwrap_or(0);

    let engine = (!payload.files.is_empty()).then(|| {
        format!(
            "DuckDB {}{}",
            payload.duckdb_version,
            if payload.platform.is_empty() {
                String::new()
            } else {
                format!(" ({})", payload.platform)
            }
        )
    });
    let extensions = payload
        .files
        .iter()
        .filter(|file| file.role == etl_runner::Role::Extension)
        .filter(|file| file.name.ends_with(".duckdb_extension"))
        .map(|file| file.name.trim_end_matches(".duckdb_extension").to_string())
        .collect();

    Ok(Built {
        destination,
        state_key,
        stages: plan.stages.len(),
        size,
        carries_secrets: payload.carries_secrets,
        engine,
        extensions,
        notes,
    })
}

/// `load_and_compile`, with `${workspace}` and `${date}` left in the document.
///
/// Only `etl build` wants this. Compiling still happens here so that a document
/// that will not compile is caught before it is shipped — the built-ins are
/// text inside a path literal, so a plan compiles identically whether they are
/// substituted or not.
fn load_and_compile_deferring_built_ins(
    pipeline: &Path,
    settings: &Settings,
    quiet: bool,
) -> Result<Loaded, (u8, String)> {
    let text = std::fs::read_to_string(pipeline).map_err(|error| {
        (
            exit::USAGE,
            format!("cannot read {}: {error}", pipeline.display()),
        )
    })?;

    let document = PipelineDoc::from_json(&text).map_err(|error| {
        (
            exit::USAGE,
            format!("{} is not a valid pipeline: {error}", pipeline.display()),
        )
    })?;

    let resolver = settings
        .try_resolver()
        .map_err(|message| (exit::USAGE, message))?
        .defer_built_ins();

    let resolved = params::resolve(&document, &resolver)
        .map_err(|error| (exit::INVALID, error.to_string()))?;

    if !quiet {
        report_param_warnings(&resolved.warnings);
    }

    let state_key = state::key_for(resolved.document.name.as_deref(), pipeline);

    let plan = compile_with(&resolved.document, &CompileOptions::default())
        .map_err(|error: EngineError| (exit::INVALID, resolved.redact(&error.to_string())))?;

    Ok(Loaded {
        plan,
        resolved,
        state_key,
    })
}

/// Collect the engine and the extensions this plan needs into the blob region.
///
/// Reads the same vendored `tools/duckdb/` the executor runs against, through
/// the same two lookup functions, so an artifact can never be built against a
/// different engine from the one the pipeline was tested on.
fn gather_embedded(
    plan: &Plan,
    settings: &Settings,
    target: &Target,
    payload: &mut etl_runner::Payload,
) -> Result<Vec<u8>, String> {
    use etl_duckdb_engine::exec::{locate_duckdb, locate_extension_dir, PINNED_DUCKDB_VERSION};

    // Searched from several roots, in order, because `--workspace` says where
    // the pipeline's *data* is and the vendored engine lives near the checkout.
    // A pipeline that reads a folder somewhere else must still find the toolchain
    // it is being built against.
    let roots = toolchain_roots(settings);

    // The host's engine is found by the same lookup the executor uses, so an
    // artifact is built against the binary the pipeline was tested on. Another
    // platform's has to be vendored deliberately -- there is nothing on this
    // machine that could stand in for it.
    let engine = if target.is_host {
        roots
            .iter()
            .map(|root| RunOptions {
                working_dir: Some(root.clone()),
                ..Default::default()
            })
            .find_map(|options| locate_duckdb(&options).ok())
            .ok_or_else(|| {
                format!(
                    "no vendored DuckDB found. Looked under: {}. Run scripts/fetch-duckdb.ps1.",
                    roots
                        .iter()
                        .map(|root| root.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?
    } else {
        roots
            .iter()
            .map(|root| target.engine_path(root))
            .find(|candidate| candidate.is_file())
            .ok_or_else(|| {
                format!(
                    "no DuckDB vendored for {}. Run: .\\scripts\\fetch-duckdb.ps1 -Platform {}",
                    target.platform, target.platform
                )
            })?
    };

    let mut blobs = Vec::new();
    let mut files = Vec::new();

    let bytes = std::fs::read(&engine)
        .map_err(|error| format!("could not read {}: {error}", engine.display()))?;

    files.push(etl_runner::EmbeddedFile {
        // Named for the target rather than copied from the source file, so a
        // Linux artifact never carries something called `duckdb.exe`.
        name: format!("duckdb{}", target.exe_suffix()),
        role: etl_runner::Role::Engine,
        offset: 0,
        length: bytes.len() as u64,
        // The one file that has to be runnable on the far side. On Windows the
        // extension decides and this is a no-op; on Linux it is the difference
        // between a working artifact and a confusing one.
        executable: true,
        extra: Default::default(),
    });
    blobs.extend_from_slice(&bytes);

    payload.duckdb_version = PINNED_DUCKDB_VERSION.to_string();
    // Recorded whether or not anything needs it: it is what `--info` shows, and
    // it is how an artifact says which platform it was built for.
    payload.platform = target.platform.clone();

    let wanted = plan.extensions();

    if !wanted.is_empty() {
        // Searched across the same roots as the engine, for the same reason: the
        // workspace holds the pipeline's data, and the vendored extensions live
        // near the checkout.
        let directory = roots
            .iter()
            .find_map(|root| {
                locate_extension_dir(&RunOptions {
                    working_dir: Some(root.clone()),
                    ..Default::default()
                })
            })
            .ok_or_else(|| {
                format!(
                    "no vendored extension directory found under: {}. \
                     Run scripts/fetch-duckdb-extensions.ps1.",
                    roots
                        .iter()
                        .map(|root| root.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?;

        // Addressed by platform rather than by "the only one there", which is
        // what 9b left behind and what stopped working the moment a second
        // platform was vendored beside the first.
        let platform_dir = directory.join(PINNED_DUCKDB_VERSION).join(&target.platform);

        if !platform_dir.is_dir() {
            return Err(format!(
                "no {} extensions vendored at {}. Run: \
                 .\\scripts\\fetch-duckdb-extensions.ps1 -Platform {}",
                target.platform,
                platform_dir.display(),
                target.platform
            ));
        }

        for extension in wanted {
            // A component says `postgres`; the file it installed is called
            // `postgres_scanner.duckdb_extension`. The fetch script resolves the
            // same two spellings, and this has to agree with it.
            let found = [
                format!("{extension}.duckdb_extension"),
                format!("{extension}_scanner.duckdb_extension"),
            ]
            .into_iter()
            .map(|name| platform_dir.join(name))
            .find(|candidate| candidate.is_file())
            .ok_or_else(|| {
                let nodes: Vec<&str> = plan
                    .stages_needing(extension)
                    .map(|stage| stage.node_id.as_str())
                    .collect();

                format!(
                    "'{}' needs the {extension} extension, which is not in {}. Run scripts/fetch-duckdb-extensions.ps1.",
                    nodes.join(", "),
                    platform_dir.display()
                )
            })?;

            // The `.info` sidecar travels with it. DuckDB writes one beside
            // every installed extension and reads it back; shipping the
            // extension alone is the sort of thing that works until it does not.
            for path in [found.clone(), with_info_suffix(&found)] {
                if !path.is_file() {
                    continue;
                }

                let bytes = std::fs::read(&path)
                    .map_err(|error| format!("could not read {}: {error}", path.display()))?;

                files.push(etl_runner::EmbeddedFile {
                    name: path
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                    role: etl_runner::Role::Extension,
                    offset: blobs.len() as u64,
                    length: bytes.len() as u64,
                    executable: false,
                    extra: Default::default(),
                });
                blobs.extend_from_slice(&bytes);
            }
        }
    }

    payload.files = files;

    Ok(blobs)
}

/// Where to look for the vendored engine and extensions, most specific first.
///
/// The workspace, then wherever `etl` was run from, then the directory holding
/// `etl` itself — which is the one that still works when somebody runs a built
/// `etl` from an unrelated directory against a pipeline somewhere else again.
fn toolchain_roots(settings: &Settings) -> Vec<PathBuf> {
    let mut roots = vec![settings.workspace_root()];

    if let Ok(current) = std::env::current_dir() {
        if !roots.contains(&current) {
            roots.push(current);
        }
    }

    if let Some(beside) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf))
    {
        if !roots.contains(&beside) {
            roots.push(beside);
        }
    }

    roots
}

/// `x.duckdb_extension` to `x.duckdb_extension.info`.
fn with_info_suffix(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".info");

    path.with_file_name(name)
}

/// The nodes in a document that ask to load incrementally.
///
/// Its own function so the build note can be tested rather than only
/// demonstrated. Until Phase 10e this list was a refusal: an artifact had
/// nowhere to keep a watermark.
fn incremental_nodes(document: &PipelineDoc) -> Vec<&str> {
    document
        .nodes
        .iter()
        .filter(|node| node.data.incremental.is_some())
        .map(|node| node.id.as_str())
        .collect()
}

/// A secret as `secret set --stdin` received it, without what the piping
/// added: a trailing newline, and a leading UTF-8 byte-order mark, which
/// Windows PowerShell 5.1 puts in front of text piped to a program. Neither is
/// ever part of a password, and a BOM is invisible, so a password stored with
/// one fails at the server with nothing on screen to explain it. Found in 10f
/// signing in to Kafka.
fn stdin_secret(text: &str) -> String {
    text.trim_start_matches('\u{feff}')
        .trim_end_matches(['\n', '\r'])
        .to_string()
}

/// The nodes that read a stream, and so keep a position between runs.
fn stream_nodes(document: &PipelineDoc) -> Vec<&str> {
    document
        .nodes
        .iter()
        .filter(|node| {
            node.data
                .component_id
                .as_deref()
                .is_some_and(|id| id.starts_with("src.stream."))
        })
        .map(|node| node.id.as_str())
        .collect()
}

/// Find the `etl-runner` to copy for a target.
///
/// For this machine: beside the executable, where cargo just built it.
/// Embedding it into `etl` instead would mean `etl-cli`'s build script building
/// another binary in the target directory cargo is already building, which is a
/// recursion worth avoiding for a property nothing needs — what ships is the
/// built artifact, not `etl` itself.
///
/// For anywhere else: vendored under `tools/runners/<platform>/`, because there
/// is no way to produce a Linux binary on demand from a Windows machine with no
/// cross toolchain. `scripts/build-runner.ps1` is what puts one there.
fn locate_runner(target: &Target, settings: &Settings) -> Option<PathBuf> {
    if target.is_host {
        let here = std::env::current_exe().ok()?;
        let candidate = here
            .parent()?
            .join(format!("etl-runner{}", target.exe_suffix()));

        if candidate.is_file() {
            return Some(candidate);
        }
    }

    toolchain_roots(settings)
        .iter()
        .map(|root| target.runner_path(root))
        .find(|candidate| candidate.is_file())
}

/// Which operating system an artifact is being built for.
///
/// Named the way DuckDB names its platforms — `windows_amd64`, `linux_amd64`,
/// `osx_arm64` — rather than as a Rust target triple. The extension directory
/// layout is DuckDB's and already keys on these, so borrowing the vocabulary
/// means one name for one concept instead of a mapping table to keep in step.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    platform: String,
    is_host: bool,
}

impl Target {
    /// The machine this is running on.
    fn host() -> Self {
        Target {
            platform: host_platform(),
            is_host: true,
        }
    }

    fn named(platform: &str) -> Self {
        Target {
            is_host: platform == host_platform(),
            platform: platform.to_string(),
        }
    }

    /// What an executable is called on this target.
    fn exe_suffix(&self) -> &'static str {
        if self.platform.starts_with("windows") {
            ".exe"
        } else {
            ""
        }
    }

    /// Where a cross-target's runner is vendored.
    fn runner_path(&self, root: &Path) -> PathBuf {
        root.join("tools")
            .join("runners")
            .join(&self.platform)
            .join(format!("etl-runner{}", self.exe_suffix()))
    }

    /// Where a cross-target's DuckDB CLI is vendored.
    ///
    /// Under `targets/` rather than beside the host's copy, so that the
    /// executor's own lookup — which finds `tools/duckdb/duckdb.exe` by
    /// searching upward — cannot accidentally pick up a Linux binary and try to
    /// run it.
    fn engine_path(&self, root: &Path) -> PathBuf {
        root.join("tools")
            .join("duckdb")
            .join("targets")
            .join(&self.platform)
            .join(format!("duckdb{}", self.exe_suffix()))
    }
}

/// This machine, in DuckDB's platform vocabulary.
fn host_platform() -> String {
    let os = match std::env::consts::OS {
        "windows" => "windows",
        "macos" => "osx",
        other => other,
    };

    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    };

    format!("{os}_{arch}")
}

fn command_plan(pipeline: &Path, as_script: bool, counts: bool, settings: &Settings) -> u8 {
    let Loaded { plan, resolved, .. } = match load_and_compile(pipeline, settings) {
        Ok(loaded) => loaded,
        Err(code) => return code,
    };

    report_warnings(&plan);

    if as_script {
        print!("{}", resolved.redact(&plan.script(counts)));
        return exit::OK;
    }

    for (position, stage) in plan.stages.iter().enumerate() {
        println!(
            "{}. {} [{}] {:?}",
            position + 1,
            stage.label,
            stage.component_id,
            stage.kind
        );

        for line in resolved.redact(&stage.sql).lines() {
            println!("     {line}");
        }
        println!();
    }

    exit::OK
}

// ---------------------------------------------------------------------------
// Shared
// ---------------------------------------------------------------------------

/// Read, resolve, compile.
///
/// Resolution comes first and is a separate step: `${...}` is substituted into
/// a new document, and only then is anything compiled. That is what lets an
/// unresolved parameter be reported as an unresolved parameter rather than as
/// a file that does not exist.
/// A compiled pipeline and the resolution that produced it. The resolution is
/// kept because it knows which secret values must be masked out of anything
/// this process prints.
struct Loaded {
    plan: Plan,
    resolved: Resolved,
    /// Which state file this pipeline's watermarks live in, carried so the run
    /// writes back to the same one it read.
    state_key: String,
}

fn load_and_compile(pipeline: &Path, settings: &Settings) -> Result<Loaded, u8> {
    let text = std::fs::read_to_string(pipeline).map_err(|error| {
        eprintln!("error: cannot read {}: {error}", pipeline.display());
        exit::USAGE
    })?;

    let document = PipelineDoc::from_json(&text).map_err(|error| {
        eprintln!(
            "error: {} is not a valid pipeline: {error}",
            pipeline.display()
        );
        exit::USAGE
    })?;

    let resolver = settings.resolver()?;

    let resolved = params::resolve(&document, &resolver).map_err(|error| {
        eprintln!("error: {error}");
        exit::INVALID
    })?;

    report_param_warnings(&resolved.warnings);

    // What the last successful run reached. Read before compiling, because the
    // watermark is what decides the predicate each incremental source gets.
    let state_key = state::key_for(resolved.document.name.as_deref(), pipeline);
    let store = state::Store::at(settings.workspace_root());

    let stored = store.load(&state_key).map_err(|error| {
        eprintln!("error: {error}");
        exit::INVALID
    })?;

    let options = compile_options_for(&resolved.document, &stored);

    let plan = compile_with(&resolved.document, &options).map_err(|error: EngineError| {
        eprintln!("error: {error}");
        exit::INVALID
    })?;

    Ok(Loaded {
        plan,
        resolved,
        state_key,
    })
}

/// What the workspace remembers, as compile options, with any stored value
/// that no longer fits its node said out loud. The rules are the engine's
/// (`etl_duckdb_engine::remember`), shared with `etl-runner`, so an artifact
/// and `etl run` cannot disagree about what applies.
fn compile_options_for(document: &PipelineDoc, stored: &state::PipelineState) -> CompileOptions {
    let remembering = remember::compile_options(document, stored);
    for warning in &remembering.warnings {
        eprintln!("warning: {warning}");
    }
    remembering.options
}

/// The watermarks alone, for the tests that pin how they apply.
#[cfg(test)]
fn watermarks_for(
    document: &PipelineDoc,
    stored: &state::PipelineState,
) -> BTreeMap<String, String> {
    compile_options_for(document, stored).watermarks
}

fn report_param_warnings(warnings: &[ParamWarning]) {
    for warning in warnings {
        eprintln!("warning: {}", param_warning_text(warning));
    }
}

fn param_warning_text(warning: &ParamWarning) -> String {
    match warning {
        ParamWarning::Undeclared { name } => {
            format!("'{name}' was supplied but this pipeline declares no such parameter")
        }
        ParamWarning::ShadowsBuiltIn { name } => {
            format!("'{name}' hides the built-in of the same name")
        }
    }
}

fn report_warnings(plan: &Plan) {
    for warning in &plan.warnings {
        eprintln!("warning: {}", warning_text(warning));
    }
}

/// How a compiler warning reads, on stderr or in an MCP result.
fn warning_text(warning: &Warning) -> String {
    match warning {
        Warning::DisabledSkipped { id } => format!("'{id}' is switched off and was skipped"),
        Warning::DroppedDownstreamOfDisabled { id, disabled } => {
            format!("'{id}' was dropped because '{disabled}' is switched off")
        }
        Warning::Orphan { id } => format!("'{id}' is not wired to anything"),
        Warning::IncrementalIgnored { id, component_id } => format!(
            "'{id}' asks to load incrementally, but '{component_id}' is not a source, so \
                 the whole of it is read"
        ),
        Warning::UnknownProperty { id, property } => {
            format!("'{id}' sets '{property}', which its component does not define")
        }
        Warning::UnknownMaterialize { id, value } => format!(
            "'{id}' asks to materialize as '{value}', which this engine does not know; \
                 using auto"
        ),
        Warning::NoSink => {
            "this pipeline has no sink, so it will read data and write nothing".to_string()
        }
    }
}

// ---------------------------------------------------------------------------
// Run history
// ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
enum RunsAction {
    /// The most recent runs, newest first.
    List {
        /// Only this pipeline. Defaults to every pipeline with history.
        #[arg(long, value_name = "KEY")]
        pipeline: Option<String>,

        /// How many to show per pipeline.
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },

    /// Everything recorded about one run.
    Show {
        /// The run id, as `runs list` prints it.
        id: String,

        /// Emit the record as JSON, which is exactly what was stored.
        #[arg(long)]
        json: bool,
    },

    /// Drop all but the most recent runs of one pipeline.
    ///
    /// Nothing does this on your behalf: history is append-only and is only
    /// ever shortened deliberately.
    Prune {
        /// The pipeline's key.
        pipeline: String,

        /// How many of the most recent runs to keep.
        #[arg(long, default_value_t = 100)]
        keep: usize,
    },
}

fn command_runs(action: RunsAction, settings: &Settings) -> u8 {
    let history = state::History::at(settings.workspace_root());

    match action {
        RunsAction::List { pipeline, limit } => {
            let keys = match pipeline {
                Some(one) => vec![one],
                None => match history.keys() {
                    Ok(keys) => keys,
                    Err(error) => {
                        eprintln!("error: {error}");
                        return exit::USAGE;
                    }
                },
            };

            if keys.is_empty() {
                println!("No pipeline in this workspace has run yet.");
                return exit::OK;
            }

            for key in keys {
                let records = match history.recent(&key, limit) {
                    Ok(records) => records,
                    Err(error) => {
                        eprintln!("error: {error}");
                        return exit::INVALID;
                    }
                };

                println!("{key}");
                if records.is_empty() {
                    println!("  (no runs recorded)");
                }

                for record in records {
                    let rows = match record.rows_written() {
                        Some(rows) => format!("{rows} rows"),
                        None => "-".to_string(),
                    };

                    println!(
                        "  {}  {:9}  {:>12}  {:.2}s",
                        record.id,
                        record.outcome.name(),
                        rows,
                        record.elapsed_ms as f64 / 1000.0
                    );
                }
            }

            exit::OK
        }

        RunsAction::Show { id, json } => {
            let found = match history.find(None, &id) {
                Ok(found) => found,
                Err(error) => {
                    eprintln!("error: {error}");
                    return exit::INVALID;
                }
            };

            let Some(record) = found else {
                eprintln!("error: no run with id '{id}'");
                return exit::USAGE;
            };

            if json {
                // The stored record, not a rendering of it: the point of
                // `--json` is that what you parse is what was kept.
                println!(
                    "{}",
                    serde_json::to_string_pretty(&record).expect("a record serialises")
                );
                return exit::OK;
            }

            println!("{}  {}", record.id, record.pipeline);
            if let Some(path) = &record.path {
                println!("  document   {path}");
            }
            println!("  started    {}", record.started);
            println!("  elapsed    {:.2}s", record.elapsed_ms as f64 / 1000.0);
            println!("  outcome    {}", record.outcome.name());

            if !record.stages.is_empty() {
                println!();
                let width = record
                    .stages
                    .iter()
                    .map(|stage| stage.label.chars().count())
                    .max()
                    .unwrap_or(0);

                for stage in &record.stages {
                    let rows = match (&stage.skipped, stage.rows) {
                        (Some(reason), _) => reason.clone(),
                        (None, Some(rows)) => format!("{rows} rows"),
                        (None, None) => "-".to_string(),
                    };

                    println!(
                        "  {:width$}  {:>12}  {}",
                        stage.label, rows, stage.component_id
                    );
                }
            }

            for watermark in &record.watermarks {
                match &watermark.value {
                    Some(value) => println!("  · {} watermark {}", watermark.node_id, value),
                    None => println!("  · {} had nothing new", watermark.node_id),
                }
            }

            for note in &record.notes {
                println!("  · {note}");
            }

            for warning in &record.warnings {
                println!("  ⚠ {warning}");
            }

            for failure in &record.failures {
                println!("  ! {failure}");
            }

            exit::OK
        }

        RunsAction::Prune { pipeline, keep } => {
            match history.prune(&pipeline, keep) {
                Ok(0) => {
                    println!("'{pipeline}' has {keep} or fewer runs recorded; nothing dropped.");
                    exit::OK
                }
                Ok(dropped) => {
                    println!("Dropped {dropped} run(s) from '{pipeline}', keeping the most recent {keep}.");
                    exit::OK
                }
                Err(error) => {
                    eprintln!("error: {error}");
                    exit::FAILED
                }
            }
        }
    }
}

/// Turn a finished run into the record that is stored and printed.
///
/// One shape for both, deliberately: a CI job parsing stdout and a person
/// running `etl runs show` are then looking at the same record rather than two
/// renderings that drift apart.
fn record_of(
    id: String,
    key: &str,
    pipeline: &Path,
    started: String,
    report: &RunReport,
) -> state::RunRecord {
    state::RunRecord {
        format_version: state::runs::CURRENT_FORMAT_VERSION,
        id,
        pipeline: key.to_string(),
        path: Some(pipeline.display().to_string()),
        started,
        elapsed_ms: report.elapsed.as_millis(),
        outcome: if report.failed() {
            state::Outcome::Failed
        } else {
            state::Outcome::Succeeded
        },
        stages: report
            .stages
            .iter()
            .map(|stage| state::StageRecord {
                node_id: stage.node_id.clone(),
                label: stage.label.clone(),
                component_id: stage.component_id.clone(),
                rows: stage.rows,
                rejected: stage.rejected,
                skipped: stage.skipped.as_ref().map(|reason| reason.describe()),
                elapsed_ms: stage.elapsed.map(|elapsed| elapsed.as_millis()),
            })
            .collect(),
        notes: report.notes.clone(),
        warnings: report.warnings.clone(),
        failures: report
            .failures
            .iter()
            .map(|failure| {
                format!(
                    "{} ({}): {}",
                    failure.label, failure.node_id, failure.message
                )
            })
            .collect(),
        watermarks: report
            .watermarks
            .iter()
            .map(|watermark| state::WatermarkRecord {
                node_id: watermark.node_id.clone(),
                column: watermark.column.clone(),
                value: watermark.value.clone(),
            })
            .collect(),
        extra: Default::default(),
    }
}

/// A record for a run that did not get far enough to produce a report.
fn failed_record(
    id: String,
    key: &str,
    pipeline: &Path,
    started: String,
    message: String,
) -> state::RunRecord {
    state::RunRecord {
        format_version: state::runs::CURRENT_FORMAT_VERSION,
        id,
        pipeline: key.to_string(),
        path: Some(pipeline.display().to_string()),
        started,
        elapsed_ms: 0,
        outcome: state::Outcome::Failed,
        stages: Vec::new(),
        notes: Vec::new(),
        warnings: Vec::new(),
        failures: vec![message],
        watermarks: Vec::new(),
        extra: Default::default(),
    }
}

/// Append a run to history, complaining but not failing if it cannot.
///
/// A run that worked must not be reported as failed because its *diary* could
/// not be written. The exit code belongs to the pipeline, not to the
/// bookkeeping — which is the opposite call from watermark state, where the
/// bookkeeping decides what the next run reads.
fn remember(settings: &Settings, key: &str, record: &state::RunRecord) {
    let history = state::History::at(settings.workspace_root());

    if let Err(error) = history.append(key, record) {
        eprintln!("warning: the run happened but was not recorded: {error}");
    }
}
