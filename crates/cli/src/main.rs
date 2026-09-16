//! `etl` — compile and run pipelines from the command line.
//!
//! The CLI and the desktop app are meant to be interchangeable on the same
//! file, so everything here is a thin shell over the engine: read JSON,
//! compile, optionally run, print. No behaviour lives in this crate that the
//! GUI would then have to reimplement.

use clap::{Args, Parser, Subcommand};
use etl_duckdb_engine::{
    compile_with, lineage, params, registry, run, CompileOptions, Contexts, EngineError, ExecError,
    ParamWarning, Plan, Resolved, Resolver, RunOptions, RunReport, Warning,
};
use etl_metadata::Namespace;
use etl_metadata::PipelineDoc;
use etl_secrets::SecretStore;
use etl_state as state;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

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
}

impl Settings {
    fn workspace_root(&self) -> PathBuf {
        self.workspace
            .clone()
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    }

    /// Build the resolver: explicit bindings first, then the workspace's
    /// contexts, then whichever one is active.
    fn resolver(&self) -> Result<Resolver, u8> {
        let root = self.workspace_root();
        let mut resolver = Resolver::new(&root);

        for binding in &self.params {
            let Some((name, value)) = binding.split_once('=') else {
                eprintln!("error: --param expects NAME=VALUE, but got '{binding}'");
                return Err(exit::USAGE);
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
            let store = SecretStore::open_existing(&root).map_err(|error| {
                eprintln!("error: {error}");
                exit::USAGE
            })?;

            resolver = resolver.secrets(store);
        }

        let contexts = Contexts::load(&path).map_err(|error| {
            eprintln!("error: {error}");
            exit::USAGE
        })?;

        contexts
            .apply(resolver, self.context.as_deref())
            .map_err(|error| {
                eprintln!("error: {error}");
                exit::USAGE
            })
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
    };

    ExitCode::from(code)
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

fn command_run(
    pipeline: &Path,
    duckdb: Option<PathBuf>,
    counts: bool,
    show_sql: bool,
    json: bool,
    settings: &Settings,
) -> u8 {
    let Loaded {
        plan,
        resolved,
        state_key,
    } = match load_and_compile(pipeline, settings) {
        Ok(loaded) => loaded,
        Err(code) => return code,
    };

    report_warnings(&plan);

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

            // JSON is the whole of stdout when it is asked for, so a script can
            // parse it without stripping a table off the front.
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&record).expect("a record serialises")
                );

                return if report.failed() {
                    exit::FAILED
                } else {
                    match save_watermarks(settings, &state_key, &report, true) {
                        Ok(()) => exit::OK,
                        Err(code) => code,
                    }
                };
            }

            let width = report
                .stages
                .iter()
                .map(|s| s.label.chars().count())
                .max()
                .unwrap_or(0);

            for stage in &report.stages {
                let rows = match (&stage.skipped, stage.rows) {
                    // A stage that did not run says why, rather than showing a
                    // dash that reads the same as "no counts were collected".
                    (Some(reason), _) => reason.describe(),
                    (None, Some(rows)) => format!("{rows} rows"),
                    (None, None) => "-".to_string(),
                };

                // A quality node's rejected count is shown even when it is
                // zero. Zero rejects is the result someone ran the check to
                // see, and hiding it would make a passing check look like a
                // node that did nothing.
                let rejected = match stage.rejected {
                    Some(rejected) => format!("  {rejected} rejected"),
                    None => String::new(),
                };

                // Most stages have no timing and must not be padded into a
                // column of blanks; the ones that do have earned it. See
                // `StageOutcome::elapsed` for which those are and why.
                let took = match stage.elapsed {
                    Some(elapsed) => format!("  {:.0}ms", elapsed.as_secs_f64() * 1000.0),
                    None => String::new(),
                };

                println!(
                    "  {:width$}  {:>12}  {}{}{}",
                    stage.label, rows, stage.component_id, rejected, took
                );
            }

            for failure in &report.failures {
                println!(
                    "  ! {} ({}): {}",
                    failure.label, failure.node_id, failure.message
                );
            }

            for note in &report.notes {
                println!("  · {note}");
            }

            println!(
                "\nRan {} stage(s) in {:.2}s",
                report.stages.len(),
                report.elapsed.as_secs_f64()
            );
            // A report can describe a failed run: `continue_on_failure` hands
            // back everything that happened rather than only the first error,
            // and the exit code is what says it still failed.
            if report.failed() {
                // Deliberately no state written. The run wrote some of its
                // output and failed; leaving the watermark behind that output
                // is the recoverable direction, because the next run redoes
                // the window rather than skipping it.
                if !report.watermarks.is_empty() {
                    println!("  · watermarks not advanced: the run failed");
                }
                exit::FAILED
            } else {
                match save_watermarks(settings, &state_key, &report, false) {
                    Ok(()) => exit::OK,
                    Err(code) => code,
                }
            }
        }

        Err(error) => {
            // A pipeline that could not run at all is a thing that happened,
            // and is exactly what someone asks history about the next morning.
            // The one exception is a missing DuckDB, which is a broken
            // installation rather than a failed pipeline.
            if let ExecError::DuckdbNotFound { .. } = error {
                eprintln!("error: {error}");
                return exit::USAGE;
            }

            let record = failed_record(id, &state_key, pipeline, started, error.to_string());
            remember(settings, &state_key, &record);

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&record).expect("a record serialises")
                );
            } else {
                eprintln!("error: {error}");
            }

            exit::FAILED
        }
    }
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

/// Record where each incremental source got to.
///
/// Called only after a run that fully succeeded. It writes the whole state in
/// one atomic replace, so a crash here leaves the previous watermarks intact
/// rather than a mixture of old and new.
///
/// A failure to save is a failure of the command even though the data landed:
/// the run is not repeatable-from-here if nobody recorded where here is, and
/// the next run would silently reload from the old mark. Saying so loudly is
/// the only way that gets noticed.
fn save_watermarks(
    settings: &Settings,
    key: &str,
    report: &RunReport,
    quiet: bool,
) -> Result<(), u8> {
    let advancing: Vec<&etl_duckdb_engine::Watermark> = report
        .watermarks
        .iter()
        .filter(|watermark| watermark.value.is_some())
        .collect();

    if report.watermarks.is_empty() {
        return Ok(());
    }

    let store = state::Store::at(settings.workspace_root());

    let mut stored = store.load(key).map_err(|error| {
        eprintln!("error: the run succeeded but its state could not be read: {error}");
        exit::FAILED
    })?;

    for watermark in &advancing {
        let value = watermark.value.as_deref().unwrap_or_default();
        stored.advance(&watermark.node_id, &watermark.column, value);
        if !quiet {
            println!("  · {} watermark now {}", watermark.node_id, value);
        }
    }

    // A source that loaded nothing keeps the mark it had; saying so is worth a
    // line, because "nothing happened" is the normal outcome of an incremental
    // pipeline and should not look like a broken one.
    if !quiet {
        for watermark in report.watermarks.iter().filter(|w| w.value.is_none()) {
            println!("  · {} had nothing new", watermark.node_id);
        }
    }

    if advancing.is_empty() {
        return Ok(());
    }

    store.save(key, &stored).map_err(|error| {
        eprintln!("error: the run succeeded but its state could not be saved: {error}");
        eprintln!("       the next run will reload from the previous watermark");
        exit::FAILED
    })
}

#[derive(Subcommand, Debug)]
enum StateAction {
    /// Show every watermark this workspace holds.
    List {
        /// Only this pipeline. Defaults to all of them.
        #[arg(long, value_name = "KEY")]
        key: Option<String>,
    },

    /// Forget a watermark, so the next run reads that source from the start.
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
                if stored.watermarks.is_empty() {
                    println!("  (nothing remembered)");
                }
                for (node, watermark) in &stored.watermarks {
                    println!(
                        "  {node}  {} = {}  (at {})",
                        watermark.column, watermark.value, watermark.at
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
                        println!("'{node}' had no watermark in '{key}'.");
                        return exit::OK;
                    }
                    println!("Forgot '{node}' in '{key}'. Its next run reads from the start.");
                }
                None => {
                    stored.watermarks.clear();
                    println!("Forgot every watermark in '{key}'.");
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
                // A trailing newline is an artefact of how it was piped in, not
                // part of the password.
                text.trim_end_matches(['\n', '\r']).to_string()
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

    let options = CompileOptions {
        watermarks: watermarks_for(&resolved.document, &stored),
    };

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

/// The stored watermarks that still apply to this document.
///
/// A node whose watermark column has been changed since it was recorded starts
/// over rather than comparing the new column against the old column's value.
/// That reloads data, which is the safe direction: the alternative is a
/// predicate that silently means something nobody wrote.
fn watermarks_for(
    document: &PipelineDoc,
    stored: &state::PipelineState,
) -> BTreeMap<String, String> {
    document
        .nodes
        .iter()
        .filter_map(|node| {
            let declared = node.data.incremental.as_ref()?;
            let watermark = stored.watermark(&node.id)?;

            if !watermark.matches_column(&declared.column) {
                eprintln!(
                    "warning: '{}' now watches '{}' but its watermark was taken from '{}';                      reading from the start",
                    node.id, declared.column, watermark.column
                );
                return None;
            }

            Some((node.id.clone(), watermark.value.clone()))
        })
        .collect()
}

fn report_param_warnings(warnings: &[ParamWarning]) {
    for warning in warnings {
        let text = match warning {
            ParamWarning::Undeclared { name } => {
                format!("'{name}' was supplied but this pipeline declares no such parameter")
            }
            ParamWarning::ShadowsBuiltIn { name } => {
                format!("'{name}' hides the built-in of the same name")
            }
        };

        eprintln!("warning: {text}");
    }
}

fn report_warnings(plan: &Plan) {
    for warning in &plan.warnings {
        let text = match warning {
            Warning::DisabledSkipped { id } => format!("'{id}' is switched off and was skipped"),
            Warning::DroppedDownstreamOfDisabled { id, disabled } => {
                format!("'{id}' was dropped because '{disabled}' is switched off")
            }
            Warning::Orphan { id } => format!("'{id}' is not wired to anything"),
            Warning::IncrementalIgnored { id, component_id } => format!(
                "'{id}' asks to load incrementally, but '{component_id}' is not a source, so                  the whole of it is read"
            ),
            Warning::UnknownProperty { id, property } => {
                format!("'{id}' sets '{property}', which its component does not define")
            }
            Warning::UnknownMaterialize { id, value } => format!(
                "'{id}' asks to materialize as '{value}', which this engine does not know;                  using auto"
            ),
            Warning::NoSink => {
                "this pipeline has no sink, so it will read data and write nothing".to_string()
            }
        };

        eprintln!("warning: {text}");
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
