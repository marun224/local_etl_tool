//! `etl assist` from the command line. The refusals need no model and run in
//! CI; the one real request needs the vendored model and skips without it.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/cli sits two levels under the root")
        .to_path_buf()
}

fn workspace(name: &str) -> PathBuf {
    let root = repo_root().join("target").join("test-out").join(name);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn etl(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_etl"))
        .args(args)
        .arg("--workspace")
        .arg(root)
        .env_remove("ETL_ASSIST_MODEL")
        .env_remove("ETL_LLAMA_SERVER")
        .output()
        .unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

#[test]
fn a_model_that_is_not_there_is_named_and_nothing_starts() {
    let root = workspace("assist_no_model");
    let missing = root.join("nowhere.gguf");

    let output = etl(
        &root,
        &[
            "assist",
            "csv to parquet",
            "--model",
            missing.to_str().unwrap(),
        ],
    );

    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    let said = stderr(&output);
    // Without a vendored llama-server (CI), that is what is missing instead.
    assert!(
        said.contains("nowhere.gguf") || said.contains("fetch-model.ps1"),
        "{said}"
    );
    assert!(!said.contains("Starting llama-server"), "{said}");
}

#[test]
fn an_existing_file_is_not_replaced_without_overwrite() {
    let root = workspace("assist_no_overwrite");
    let out = root.join("pipeline.json");
    std::fs::write(&out, "kept").unwrap();

    let output = etl(
        &root,
        &["assist", "csv to parquet", "--out", out.to_str().unwrap()],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("--overwrite"),
        "{}",
        stderr(&output)
    );
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "kept");
}

#[test]
fn a_request_becomes_a_pipeline_file_that_validates() {
    let root = repo_root();
    if !root.join("tools/models").is_dir() || !root.join("tools/llama").is_dir() {
        eprintln!("skipped: no model in tools/ (scripts/fetch-model.ps1)");
        return;
    }
    let workspace = workspace("assist_writes");
    let out = workspace.join("pipelines/orders.json");

    let output = etl(
        &workspace,
        &[
            "assist",
            "read the orders table from Postgres, dedupe on order_id, write Parquet",
            "--out",
            out.to_str().unwrap(),
            "--seed",
            "1",
        ],
    );
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert!(stderr(&output).contains("valid"), "{}", stderr(&output));

    let validated = etl(&workspace, &["validate", out.to_str().unwrap()]);
    assert_eq!(validated.status.code(), Some(0), "{}", stderr(&validated));
}
