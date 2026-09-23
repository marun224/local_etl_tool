//! `etl-runner` — one pipeline, baked in, with no workspace around it.
//!
//! This is the artifact Phase 9 exists to produce. A plain `etl-runner` is an
//! ordinary binary with nothing baked in; `etl build` copies it and appends a
//! pipeline, and the copy is what gets shipped. See [`etl_runner`] for the
//! format and for why it is an append rather than a compile.
//!
//! # What it deliberately does not do
//!
//! No contexts, no `--param`, no secret store: the document baked in is the
//! **resolved** one, and the machine this runs on has nothing left to resolve
//! against. A flag that appeared to re-bind a parameter here would be a flag
//! that silently did nothing.
//!
//! No run history. **State, yes** (Settled decision 36, Phase 10e): watermarks
//! and native sources' positions are loaded from `.etl/state/` under the
//! directory it runs in (or `--workspace`) and saved there after a run that
//! fully succeeded, by the same engine functions `etl run` uses. The file is the
//! one `etl` writes, so `etl state list --workspace <dir>` reads it. Before
//! that, `etl build` refused an incremental pipeline, because an artifact would
//! have re-read everything on every run.

use clap::Parser;
use etl_duckdb_engine::{
    compile_with, params, remember, report_lines, run, ExecError, Resolver, RunOptions,
};
use etl_runner::{Blobs, Payload};
use std::path::PathBuf;
use std::process::ExitCode;

/// Exit codes, the same four `etl` uses, so a script can branch identically on
/// either.
mod exit {
    pub const OK: u8 = 0;
    pub const USAGE: u8 = 1;
    pub const INVALID: u8 = 2;
    pub const FAILED: u8 = 3;
}

#[derive(Parser)]
#[command(
    name = "etl-runner",
    version,
    about = "Run the pipeline baked into this executable.",
    after_help = "Exit codes: 0 ok, 1 usage or I/O error, 2 invalid pipeline, 3 run failed."
)]
struct Cli {
    /// Where the pipeline's relative paths resolve from. Defaults to the
    /// current directory, which is what makes the artifact copyable: the
    /// machine it was built on is not the machine it runs on.
    #[arg(long, value_name = "DIR")]
    workspace: Option<PathBuf>,

    /// Say what is baked in and exit without running it.
    #[arg(long)]
    info: bool,

    /// Print the generated SQL before running.
    #[arg(long)]
    sql: bool,

    /// Skip per-stage row counts.
    #[arg(long)]
    no_counts: bool,
}

fn main() -> ExitCode {
    ExitCode::from(dispatch(Cli::parse()))
}

fn dispatch(cli: Cli) -> u8 {
    let found = match Payload::read_from_self() {
        Ok(found) => found,
        Err(error) => {
            eprintln!("error: {error}");
            return exit::USAGE;
        }
    };

    let Some((payload, blobs)) = found else {
        // Not a failure of this binary so much as a statement about it: this is
        // the runner as cargo built it, before `etl build` has copied it.
        eprintln!("error: no pipeline is baked into this runner.");
        eprintln!("       `etl build <pipeline>` produces one that has.");
        return exit::USAGE;
    };

    if cli.info {
        return describe(&payload, &blobs);
    }

    execute(&cli, &payload, &blobs)
}

/// `--info`: what this artifact carries, without running it.
///
/// Worth having for a file that arrives on a server with no way to look inside
/// it: the name, when it was built, how many stages, and — the one that
/// matters — whether it carries a secret and is therefore a credential.
fn describe(payload: &Payload, blobs: &Blobs) -> u8 {
    println!("pipeline    {}", payload.name);
    println!("built       {}", payload.built_at);
    println!("format      {}", payload.format_version);
    println!("nodes       {}", payload.pipeline.nodes.len());

    if payload.files.is_empty() {
        println!("engine      not embedded — found where this runs");
    } else {
        println!(
            "engine      DuckDB {}{}, embedded",
            payload.duckdb_version,
            if payload.platform.is_empty() {
                String::new()
            } else {
                format!(" ({})", payload.platform)
            }
        );
        println!(
            "embedded    {} file(s), {:.1} MB",
            payload.files.len(),
            blobs.len() as f64 / 1_048_576.0
        );

        for file in &payload.files {
            println!("            {} ({} bytes)", file.name, file.length);
        }

        // Where it will unpack to, without unpacking: somebody debugging a
        // read-only or full temp directory wants this before the run, not in
        // the error afterwards.
        println!("unpacks to  {}", payload.cache_dir(blobs).display());
    }

    if payload.carries_secrets {
        println!();
        println!("This artifact has a secret baked into it. Treat the file as a credential.");
    }

    exit::OK
}

fn execute(cli: &Cli, payload: &Payload, blobs: &Blobs) -> u8 {
    // `${workspace}` and `${date}` were left in the document by `etl build`,
    // because they mean "where this runs" and "the day it runs". This is where
    // they are answered -- on the machine that is actually running it, on the
    // day it is actually running. Everything else was resolved at build time.
    let workspace = cli
        .workspace
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));

    let resolved = match params::resolve(&payload.pipeline, &Resolver::new(&workspace)) {
        Ok(resolved) => resolved,
        Err(error) => {
            eprintln!("error: {error}");
            return exit::INVALID;
        }
    };

    let document = &resolved.document;

    // Before anything else: an artifact that carries its own engine has to put
    // it on disk, because the executor shells out to a binary and DuckDB loads
    // extensions from a directory. Skipped entirely for an artifact built with
    // --no-embed, which finds DuckDB the way the CLI does.
    let extracted = match payload.extract(blobs) {
        Ok(extracted) => extracted,
        Err(error) => {
            eprintln!("error: {error}");
            return exit::USAGE;
        }
    };

    // What earlier runs of this artifact, in this directory, got to. The key is
    // the one `etl build` baked in, which is the key `etl` itself uses.
    let store = etl_state::Store::at(&workspace);
    let mut stored = match store.load(&payload.name) {
        Ok(stored) => stored,
        Err(error) => {
            // Not "start from nothing": a state file that will not read is how
            // a whole topic or table gets reloaded by accident.
            eprintln!("error: {error}");
            return exit::INVALID;
        }
    };
    let remembering = remember::compile_options(document, &stored);
    for warning in &remembering.warnings {
        eprintln!("warning: {warning}");
    }

    let plan = match compile_with(document, &remembering.options) {
        Ok(plan) => plan,
        Err(error) => {
            // A baked pipeline compiled once already, at build time, so this is
            // all but unreachable. It is still reported rather than unwrapped:
            // "all but" is not "never", and a panic here would say nothing.
            eprintln!("error: the baked pipeline did not compile: {error}");
            return exit::INVALID;
        }
    };

    let counts = !cli.no_counts;

    if cli.sql {
        println!("{}", plan.script(counts));
    }

    let options = RunOptions {
        duckdb_bin: extracted.duckdb_bin.clone(),
        working_dir: Some(workspace.clone()),
        counts,
        extension_dir: extracted.extension_dir.clone(),
        // Nothing to mask: a resolved secret is already in the document, and
        // 9a refuses to bake one unless it was asked to in as many words.
        redact: Vec::new(),
    };

    match run(&plan, &options) {
        Ok(report) => {
            // The same lines `etl run` prints, from the same function. A
            // standalone artifact that reported a run differently from the tool
            // that built it would be the first thing to mistrust at 3am.
            for line in report_lines(&report) {
                println!("{line}");
            }

            println!();
            println!(
                "Ran {} stage(s) in {:.2}s",
                report.stages.len(),
                report.elapsed.as_secs_f64()
            );

            if report.failed() {
                return exit::FAILED;
            }

            // Saved only now, after a run that fully succeeded: the same rule,
            // and the same function, as `etl run`.
            let remembered = remember::remember(&report, &mut stored);
            for (node, value) in &remembered.advanced {
                println!("  · {node} watermark now {value}");
            }
            for node in &remembered.positions {
                println!("  · {node} position saved");
            }
            if remembered.changed() {
                if let Err(error) = store.save(&payload.name, &stored) {
                    eprintln!("error: the run succeeded but its state could not be saved: {error}");
                    eprintln!(
                        "       the next run will re-read from the previous watermark or position"
                    );
                    return exit::FAILED;
                }
            }

            exit::OK
        }

        Err(error) => {
            eprintln!("error: {error}");

            match error {
                // A missing engine is a broken installation of this artifact,
                // not a pipeline that failed — and in 9b it stops being
                // possible at all, because the engine travels inside the file.
                ExecError::DuckdbNotFound { .. } => exit::USAGE,
                _ => exit::FAILED,
            }
        }
    }
}
