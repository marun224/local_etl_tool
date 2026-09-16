//! `etl` — compile and run pipelines from the command line.
//!
//! The CLI and the desktop app are meant to be interchangeable on the same
//! file, so everything here is a thin shell over the engine: read JSON,
//! compile, optionally run, print. No behaviour lives in this crate that the
//! GUI would then have to reimplement.

use clap::{Args, Parser, Subcommand};
use etl_duckdb_engine::{
    compile, params, registry, run, Contexts, EngineError, ExecError, ParamWarning, Plan, Resolved,
    Resolver, RunOptions, Warning,
};
use etl_metadata::Namespace;
use etl_metadata::PipelineDoc;
use etl_secrets::SecretStore;
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
            settings,
        } => command_run(&pipeline, duckdb, !no_counts, show_sql, &settings),

        Command::Validate { pipeline, settings } => command_validate(&pipeline, &settings),

        Command::Contexts { settings } => command_contexts(&settings),

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
    settings: &Settings,
) -> u8 {
    let Loaded { plan, resolved } = match load_and_compile(pipeline, settings) {
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

    match run(&plan, &options) {
        Ok(report) => {
            let width = report
                .stages
                .iter()
                .map(|s| s.label.chars().count())
                .max()
                .unwrap_or(0);

            for stage in &report.stages {
                let rows = match stage.rows {
                    Some(rows) => format!("{rows} rows"),
                    None => "-".to_string(),
                };

                // A quality node's rejected count is shown even when it is
                // zero. Zero rejects is the result someone ran the check to
                // see, and hiding it would make a passing check look like a
                // node that did nothing.
                let rejected = match stage.rejected {
                    Some(rejected) => format!("  {rejected} rejected"),
                    None => String::new(),
                };

                println!(
                    "  {:width$}  {:>12}  {}{}",
                    stage.label, rows, stage.component_id, rejected
                );
            }

            println!(
                "\nRan {} stage(s) in {:.2}s",
                report.stages.len(),
                report.elapsed.as_secs_f64()
            );
            exit::OK
        }

        Err(error) => {
            eprintln!("error: {error}");

            if let ExecError::DuckdbNotFound { .. } = error {
                return exit::USAGE;
            }
            exit::FAILED
        }
    }
}

fn command_validate(pipeline: &Path, settings: &Settings) -> u8 {
    let Loaded { plan, resolved } = match load_and_compile(pipeline, settings) {
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
    let Loaded { plan, resolved } = match load_and_compile(pipeline, settings) {
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

    let plan = compile(&resolved.document).map_err(|error: EngineError| {
        eprintln!("error: {error}");
        exit::INVALID
    })?;

    Ok(Loaded { plan, resolved })
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
