//! `xf.ai.classify`, `xf.ai.extract` and `xf.ai.prompt` answered by the local
//! model (Phase 11d3), through the sample `tickets_triaged.json`. Runs on this
//! machine and skips without `llama-server` and the model in `tools/`, which
//! CI does not have. What is checked is what the components promise (a label
//! from the list, typed fields, an answer per row), not the model's judgement,
//! with one case too plain to get wrong. The endpoint path is tested against a
//! fixture in `crates/connectors/src/ask/tests.rs`.

use etl_duckdb_engine::{compile, run, Resolver, RunOptions};
use etl_metadata::PipelineDoc;
use serde_json::{json, Value as JsonValue};
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/<name>/ sits two levels under the root")
        .to_path_buf()
}

fn have_model() -> bool {
    let root = repo_root();
    etl_duckdb_engine::exec::locate_duckdb(&RunOptions {
        working_dir: Some(root.clone()),
        ..Default::default()
    })
    .is_ok()
        && root.join("tools/llama").is_dir()
        && root
            .join("tools/models/qwen2.5-coder-1.5b-instruct-q4_k_m.gguf")
            .is_file()
}

#[test]
fn the_sample_tickets_are_triaged_by_the_local_model() {
    if !have_model() {
        return eprintln!("skipped: no local model in tools/ (scripts/fetch-model.ps1)");
    }
    let root = repo_root();
    let text =
        std::fs::read_to_string(root.join("samples/pipelines/tickets_triaged.json")).unwrap();
    let out = root.join("target/test-out/ai_ask/triaged.jsonl");
    let _ = std::fs::remove_file(&out);
    let text = text.replace(
        "${workspace}/samples/out/tickets_triaged.jsonl",
        &out.to_string_lossy().replace('\\', "/"),
    );
    let document = PipelineDoc::from_json(&text).unwrap();
    let resolved = etl_duckdb_engine::resolve(&document, &Resolver::new(&root)).unwrap();
    let options = RunOptions {
        working_dir: Some(root.clone()),
        ..Default::default()
    };

    let report = run(&compile(&resolved.document).unwrap(), &options).expect("runs");

    let rows: Vec<Option<u64>> = report.stages.iter().map(|s| s.rows).collect();
    assert_eq!(rows, [Some(4); 5]);
    let written: Vec<JsonValue> = std::fs::read_to_string(&out)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(written.len(), 4);
    for row in &written {
        let label = row["label"].as_str().expect("every row labelled");
        assert!(["billing", "technical", "thanks"].contains(&label), "{row}");
        for flag in ["urgent", "email_given"] {
            assert!(
                row[flag].is_boolean() || row[flag].is_null(),
                "{flag}: {row}"
            );
        }
        assert!(
            row["order_number"].is_string() || row["order_number"].is_null(),
            "{row}"
        );
        let gist = row["gist"].as_str().expect("every row summarised");
        assert!(!gist.trim().is_empty() && gist.len() < 300, "{gist}");
        // The ticket's own columns are still there, untouched.
        assert!(
            row["body"].is_string() && row["opened"].is_string(),
            "{row}"
        );
    }
    let thanks = written
        .iter()
        .find(|row| row["ticket_id"] == json!(103))
        .unwrap();
    assert_eq!(thanks["label"], json!("thanks"), "{thanks}");
}
