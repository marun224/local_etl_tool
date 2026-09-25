//! The plan's check, on this machine only (Settled decision 97): "read this
//! Postgres table, dedupe, write Parquet" gives a pipeline that passes
//! `validate` on the first try in at least 9 of 10 runs. Skips without the
//! model, which CI never has; `scripts/fetch-model.ps1` fetches it.
//!
//! One server for all ten, each run with its own seed, so the runs can differ
//! and the prompt is read once.

use etl_assistant::{draft, locate_model, locate_server, Server};
use etl_duckdb_engine::{compile_with, registry, resolve, CompileOptions, Resolver};
use etl_metadata::{ComponentSpec, PipelineDoc};
use std::path::{Path, PathBuf};

const REQUEST: &str = "read this Postgres table, dedupe, write Parquet";
const RUNS: u64 = 10;
const NEEDED: usize = 9;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// What `etl validate` does to a file: parse, resolve, compile.
fn validate(text: &str, root: &Path) -> Result<(), String> {
    let document = PipelineDoc::from_json(text).map_err(|error| error.to_string())?;
    let resolved = resolve(&document, &Resolver::new(root)).map_err(|error| error.to_string())?;
    compile_with(&resolved.document, &CompileOptions::default())
        .map(|_| ())
        .map_err(|error| error.to_string())
}

#[test]
fn the_verify_request_validates_on_the_first_try_nine_times_in_ten() {
    let root = repo_root();
    let (Ok(server), Ok(model)) = (locate_server(None, &root), locate_model(None, &root)) else {
        eprintln!("skipped: no llama-server or model in tools/ (scripts/fetch-model.ps1)");
        return;
    };

    let log = std::env::temp_dir().join(format!("etl-assistant-model-{}.log", std::process::id()));
    let running = Server::start(&server, &model, &log).expect("llama-server starts");
    let specs: Vec<ComponentSpec> = registry().specs().cloned().collect();

    let mut failures = Vec::new();
    for seed in 1..=RUNS {
        let outcome = draft(&running, REQUEST, &specs, seed).and_then(|drafted| {
            let text = serde_json::to_string_pretty(&drafted.document).unwrap();
            validate(&text, &root).map_err(|error| format!("{error}\n{text}"))
        });
        match outcome {
            Ok(()) => eprintln!("run {seed}: valid"),
            Err(error) => {
                eprintln!("run {seed}: {error}");
                failures.push(seed);
            }
        }
    }

    let passed = RUNS as usize - failures.len();
    assert!(
        passed >= NEEDED,
        "{passed} of {RUNS} validated; seeds that failed: {failures:?}"
    );
}
