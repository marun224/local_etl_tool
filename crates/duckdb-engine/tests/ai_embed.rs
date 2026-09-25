//! `xf.ai.embed` with the real model (Phase 11d2): runs on this machine, and
//! skips without `llama-server` and bge-small-en-v1.5 in `tools/`
//! (`scripts/fetch-model.ps1`), which CI does not have. The bridge itself is
//! tested without a model in `src/native/tests.rs`.

use etl_duckdb_engine::{compile, preview, run, ExecError, RunOptions};
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
    let duckdb = etl_duckdb_engine::exec::locate_duckdb(&RunOptions {
        working_dir: Some(root.clone()),
        ..Default::default()
    });
    duckdb.is_ok()
        && root.join("tools/llama").is_dir()
        && root
            .join("tools/models/bge-small-en-v1.5-q8_0.gguf")
            .is_file()
}

/// Texts in, through `xf.ai.embed`, and optionally out to a JSON Lines file.
fn pipeline(name: &str, texts: &[JsonValue], sink: bool) -> (PipelineDoc, PathBuf, RunOptions) {
    let out = repo_root().join("target").join("test-out").join(name);
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).unwrap();
    let lines: Vec<String> = texts
        .iter()
        .enumerate()
        .map(|(id, text)| json!({ "id": id, "body": text }).to_string())
        .collect();
    std::fs::write(out.join("in.jsonl"), lines.join("\n") + "\n").unwrap();

    let mut nodes = vec![
        json!({ "id": "texts", "position": {"x": 0, "y": 0}, "data": {
            "label": "Texts", "componentId": "src.file.jsonl",
            "properties": { "path": "in.jsonl" } } }),
        json!({ "id": "vectors", "position": {"x": 1, "y": 0}, "data": {
            "label": "Vectors", "componentId": "xf.ai.embed",
            "properties": { "column": "body" } } }),
    ];
    let mut edges = vec![json!({ "id": "e1", "source": "texts", "target": "vectors" })];
    if sink {
        nodes.push(json!({ "id": "out", "position": {"x": 2, "y": 0}, "data": {
            "label": "Out", "componentId": "snk.file.jsonl",
            "properties": { "path": "out.jsonl" } } }));
        edges.push(json!({ "id": "e2", "source": "vectors", "target": "out" }));
    }
    let document = PipelineDoc::from_json(
        &json!({ "formatVersion": 1, "nodes": nodes, "edges": edges }).to_string(),
    )
    .unwrap();
    // The workspace is the test's own directory, so `tools/` is found by
    // walking up from it, as it would be from a workspace under the repo.
    let options = RunOptions {
        working_dir: Some(out.clone()),
        ..Default::default()
    };
    (document, out, options)
}

fn vector(row: &JsonValue) -> Option<Vec<f64>> {
    row["embedding"]
        .as_array()
        .map(|values| values.iter().map(|v| v.as_f64().unwrap()).collect())
}

fn cosine(a: &[f64], b: &[f64]) -> f64 {
    let dot: f64 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm = |v: &[f64]| v.iter().map(|x| x * x).sum::<f64>().sqrt();
    dot / (norm(a) * norm(b))
}

#[test]
fn similar_texts_sit_closer_than_unrelated_ones_and_the_same_text_embeds_the_same() {
    if !have_model() {
        return eprintln!("skipped: no embedding model in tools/ (scripts/fetch-model.ps1)");
    }
    let (document, out, options) = pipeline(
        "ai_embed_similarity",
        &[
            json!("the cat sat on the mat"),
            json!("a kitten rested on the rug"),
            json!("quarterly revenue grew by ten percent"),
            json!("the cat sat on the mat"),
            json!(null),
        ],
        true,
    );

    let report = run(&compile(&document).unwrap(), &options).expect("runs");
    let rows: Vec<Option<u64>> = report.stages.iter().map(|s| s.rows).collect();
    assert_eq!(rows, [Some(5), Some(5), Some(5)]);
    assert!(
        report.notes.iter().any(
            |note| note.contains("4 text(s) embedded") && note.contains("1 row(s) had no text")
        ),
        "{:?}",
        report.notes
    );

    let written: Vec<JsonValue> = std::fs::read_to_string(out.join("out.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let by_id = |id: u64| {
        written
            .iter()
            .find(|row| row["id"] == json!(id))
            .and_then(vector)
    };
    let (cat, kitten, revenue, again) = (
        by_id(0).unwrap(),
        by_id(1).unwrap(),
        by_id(2).unwrap(),
        by_id(3).unwrap(),
    );

    assert_eq!(cat.len(), 384);
    assert!(
        cosine(&cat, &kitten) > cosine(&cat, &revenue) + 0.2,
        "cat~kitten {} against cat~revenue {}",
        cosine(&cat, &kitten),
        cosine(&cat, &revenue)
    );
    assert!(
        cosine(&cat, &again) > 0.9999,
        "the same text, the same vector"
    );
    assert_eq!(by_id(4), None, "no text, no vector");
}

#[test]
fn a_preview_runs_the_model_between_its_feed_and_its_view() {
    if !have_model() {
        return eprintln!("skipped: no embedding model in tools/ (scripts/fetch-model.ps1)");
    }
    let (document, _, options) = pipeline(
        "ai_embed_preview",
        &[json!("one"), json!("two"), json!("three")],
        false,
    );

    let shown = preview(&compile(&document).unwrap(), "vectors", 2, &options).expect("previews");

    assert_eq!(shown.rows.len(), 2);
    assert!(shown.truncated, "there is a third");
    assert_eq!(vector(&shown.rows[0]).map(|v| v.len()), Some(384));
    assert!(
        shown.columns.contains(&"embedding".to_string()),
        "{:?}",
        shown.columns
    );
}

#[test]
fn a_text_too_long_for_the_model_fails_the_stage_and_says_to_chunk_it() {
    if !have_model() {
        return eprintln!("skipped: no embedding model in tools/ (scripts/fetch-model.ps1)");
    }
    let (document, _, options) = pipeline("ai_embed_too_long", &[json!("word ".repeat(900))], true);

    let error = run(&compile(&document).unwrap(), &options).expect_err("too long");

    let ExecError::StageFailed {
        node_id, message, ..
    } = &error
    else {
        panic!("{error}");
    };
    assert_eq!(node_id, "vectors");
    assert!(message.contains("xf.ai.chunk"), "{message}");
}
