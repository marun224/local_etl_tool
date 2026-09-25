//! `xf.ai.chunk` and `xf.ai.redact` against a real DuckDB (Phase 11d1).
//!
//! The golden-SQL tests pin the statements; these check what they mean: where
//! chunks end and overlap, and what redaction finds and what it leaves alone.
//! Skipped without the vendored DuckDB, as the other end-to-end tests are.

use etl_duckdb_engine::{compile, run, RunOptions};
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

fn have_duckdb() -> bool {
    let options = RunOptions {
        working_dir: Some(repo_root()),
        ..Default::default()
    };
    etl_duckdb_engine::exec::locate_duckdb(&options).is_ok()
}

/// Run `rows` through one transform and read back what it wrote.
fn through(
    name: &str,
    component: &str,
    properties: JsonValue,
    rows: &[JsonValue],
) -> Vec<JsonValue> {
    let out = repo_root().join("target").join("test-out").join(name);
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).unwrap();

    let input = out.join("in.jsonl");
    let lines: Vec<String> = rows.iter().map(JsonValue::to_string).collect();
    std::fs::write(&input, lines.join("\n") + "\n").unwrap();
    let output = out.join("out.jsonl");
    let slashed = |path: &Path| path.to_string_lossy().replace('\\', "/");

    let document = json!({
        "formatVersion": 1,
        "nodes": [
            {"id": "rows", "position": {"x": 0, "y": 0}, "data": {
                "label": "Rows", "componentId": "src.file.jsonl",
                "properties": {"path": slashed(&input)}}},
            {"id": "step", "position": {"x": 1, "y": 0}, "data": {
                "label": "Step", "componentId": component, "properties": properties}},
            {"id": "sink", "position": {"x": 2, "y": 0}, "data": {
                "label": "Out", "componentId": "snk.file.jsonl",
                "properties": {"path": slashed(&output)}}}
        ],
        "edges": [
            {"id": "e1", "source": "rows", "target": "step"},
            {"id": "e2", "source": "step", "target": "sink"}
        ]
    });
    let document = PipelineDoc::from_json(&document.to_string()).expect("parses");
    let plan = compile(&document).expect("compiles");
    let options = RunOptions {
        working_dir: Some(repo_root()),
        ..Default::default()
    };
    run(&plan, &options).expect("runs");

    std::fs::read_to_string(&output)
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn text(row: &JsonValue, key: &str) -> String {
    row[key].as_str().unwrap_or_default().to_string()
}

// ---------------------------------------------------------------------------
// Chunk
// ---------------------------------------------------------------------------

const PROSE: &str = "The quick brown fox jumps over the lazy dog while the small cat \
                     watches from the warm windowsill and the old farmer counts his sheep";

#[test]
fn chunks_end_at_whitespace_overlap_by_whole_words_and_cover_the_text() {
    if !have_duckdb() {
        return eprintln!("skipping: no DuckDB binary");
    }
    let chunks = through(
        "ai_chunk_prose",
        "xf.ai.chunk",
        json!({"column": "body", "size": 30, "overlap": 10}),
        &[json!({"id": 1, "body": PROSE, "kept": "yes"})],
    );
    let texts: Vec<String> = chunks.iter().map(|row| text(row, "chunk")).collect();
    assert!(texts.len() > 3, "{texts:#?}");

    for (index, row) in chunks.iter().enumerate() {
        assert_eq!(row["chunk_index"], json!(index));
        // The other columns kept, the text column replaced.
        assert_eq!(row["id"], json!(1));
        assert_eq!(row["kept"], json!("yes"));
        assert!(row.get("body").is_none(), "{row}");
    }

    let words: Vec<&str> = PROSE.split_whitespace().collect();
    for chunk in &texts {
        assert!(chunk.chars().count() <= 30, "{chunk:?} is longer than size");
        // Whole words only: a chunk's words are a run of the text's words.
        let own: Vec<&str> = chunk.split_whitespace().collect();
        assert!(
            words
                .windows(own.len())
                .any(|window| window == own.as_slice()),
            "{chunk:?} cuts a word"
        );
    }
    for pair in texts.windows(2) {
        // Each chunk begins with a word the one before ended with.
        let first = pair[1].split_whitespace().next().unwrap();
        assert!(
            pair[0].split_whitespace().any(|word| word == first),
            "{:?} does not overlap {:?}",
            pair[1],
            pair[0]
        );
    }
    // Every word is in some chunk.
    let joined = texts.join(" ");
    for word in words {
        assert!(joined.split_whitespace().any(|w| w == word), "{word} lost");
    }
}

#[test]
fn no_text_gives_no_chunks_and_short_text_one() {
    if !have_duckdb() {
        return eprintln!("skipping: no DuckDB binary");
    }
    let chunks = through(
        "ai_chunk_edges",
        "xf.ai.chunk",
        json!({"column": "body"}),
        &[
            json!({"id": 1, "body": null}),
            json!({"id": 2, "body": ""}),
            json!({"id": 3, "body": "   "}),
            json!({"id": 4, "body": "short"}),
        ],
    );

    assert_eq!(chunks.len(), 1, "{chunks:#?}");
    assert_eq!(chunks[0]["id"], json!(4));
    assert_eq!(chunks[0]["chunk"], json!("short"));
    assert_eq!(chunks[0]["chunk_index"], json!(0));
}

#[test]
fn a_long_run_of_whitespace_makes_no_empty_chunk() {
    if !have_duckdb() {
        return eprintln!("skipping: no DuckDB binary");
    }
    let body = format!("first{}last", " ".repeat(40));
    let chunks = through(
        "ai_chunk_blank_run",
        "xf.ai.chunk",
        json!({"column": "body", "size": 10, "overlap": 2}),
        &[json!({"body": body})],
    );
    let texts: Vec<String> = chunks.iter().map(|row| text(row, "chunk")).collect();

    assert_eq!(texts, ["first", "last"], "{chunks:#?}");
    // Numbered without gaps where blank windows were dropped.
    assert_eq!(chunks[1]["chunk_index"], json!(1));
}

#[test]
fn a_word_longer_than_size_is_cut_and_still_moves_on() {
    if !have_duckdb() {
        return eprintln!("skipping: no DuckDB binary");
    }
    let alphabet = "abcdefghijklmnopqrstuvwxyz";
    let chunks = through(
        "ai_chunk_long_word",
        "xf.ai.chunk",
        json!({"column": "body", "size": 10, "overlap": 2, "output": "piece"}),
        &[json!({"body": alphabet})],
    );
    let texts: Vec<String> = chunks.iter().map(|row| text(row, "piece")).collect();

    assert_eq!(texts, ["abcdefghij", "ijklmnopqr", "qrstuvwxyz"]);
    assert_eq!(chunks[2]["piece_index"], json!(2));
}

#[test]
fn without_overlap_the_chunks_are_the_text() {
    if !have_duckdb() {
        return eprintln!("skipping: no DuckDB binary");
    }
    let chunks = through(
        "ai_chunk_no_overlap",
        "xf.ai.chunk",
        json!({"column": "body", "size": 25, "overlap": 0}),
        &[json!({"body": PROSE})],
    );
    let joined: Vec<String> = chunks.iter().map(|row| text(row, "chunk")).collect();

    assert_eq!(
        joined.join(" ").split_whitespace().collect::<Vec<_>>(),
        PROSE.split_whitespace().collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// Redact
// ---------------------------------------------------------------------------

#[test]
fn each_kind_is_redacted_and_near_misses_are_left_alone() {
    if !have_duckdb() {
        return eprintln!("skipping: no DuckDB binary");
    }
    let cases = [
        (
            "write to Jane.Doe+work@mail.example.co.uk today",
            "write to [EMAIL] today",
        ),
        (
            "call 555-123-4567 or (555) 123-4567",
            "call [PHONE] or [PHONE]",
        ),
        ("abroad +44 20 7946 0958", "abroad [PHONE]"),
        ("card 4111 1111 1111 1111 on file", "card [CARD] on file"),
        ("card 5500-0055-5555-5559", "card [CARD]"),
        ("ssn 123-45-6789", "ssn [SSN]"),
        (
            "from 192.168.10.200 and 2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            "from [IP] and [IP]",
        ),
        // Near misses: an order number, a card failing Luhn, a date, a count.
        ("order 1234567890123", "order 1234567890123"),
        ("card 4111 1111 1111 1112", "card 4111 1111 1111 1112"),
        ("on 2026-09-25 at 10:30", "on 2026-09-25 at 10:30"),
        ("10 000 rows", "10 000 rows"),
        ("10 000 000 rows", "10 000 000 rows"),
        ("population 12 345 678", "population 12 345 678"),
        ("version 999.1.2.3", "version 999.1.2.3"),
    ];
    let rows: Vec<JsonValue> = cases
        .iter()
        .enumerate()
        .map(|(id, (note, _))| json!({"id": id, "note": note, "untouched": "a@b.com"}))
        .chain(std::iter::once(
            json!({"id": 99, "note": null, "untouched": "x"}),
        ))
        .collect();

    let out = through(
        "ai_redact_kinds",
        "xf.ai.redact",
        json!({"columns": ["note"]}),
        &rows,
    );

    for (id, (_, expected)) in cases.iter().enumerate() {
        let row = out.iter().find(|row| row["id"] == json!(id)).unwrap();
        assert_eq!(text(row, "note"), *expected, "case {id}");
        assert_eq!(row["untouched"], json!("a@b.com"), "only the named columns");
    }
    let null = out.iter().find(|row| row["id"] == json!(99)).unwrap();
    assert_eq!(null["note"], JsonValue::Null);
}

#[test]
fn a_hash_is_the_same_for_the_same_value_however_it_is_written() {
    if !have_duckdb() {
        return eprintln!("skipping: no DuckDB binary");
    }
    let out = through(
        "ai_redact_hash",
        "xf.ai.redact",
        json!({"columns": ["a", "b"], "replacement": "hash"}),
        &[
            json!({"id": 1, "a": "4111 1111 1111 1111", "b": "Jane@Example.com"}),
            json!({"id": 2, "a": "4111111111111111", "b": "jane@example.com"}),
            json!({"id": 3, "a": "5500 0055 5555 5559", "b": "john@example.com"}),
        ],
    );
    let by_id =
        |id: u64, key: &str| text(out.iter().find(|row| row["id"] == json!(id)).unwrap(), key);

    let card = by_id(1, "a");
    assert!(
        card.starts_with("[CARD:") && card.len() == "[CARD:]".len() + 12,
        "{card}"
    );
    assert_eq!(card, by_id(2, "a"), "spaces do not change a card's hash");
    assert_ne!(card, by_id(3, "a"));
    assert_eq!(
        by_id(1, "b"),
        by_id(2, "b"),
        "case does not change an email's hash"
    );
    assert_ne!(by_id(1, "b"), by_id(3, "b"));
}

#[test]
fn only_the_kinds_asked_for_are_redacted() {
    if !have_duckdb() {
        return eprintln!("skipping: no DuckDB binary");
    }
    let out = through(
        "ai_redact_some",
        "xf.ai.redact",
        json!({"columns": ["note"], "kinds": ["email"]}),
        &[json!({"note": "a@b.com, 555-123-4567"})],
    );

    assert_eq!(text(&out[0], "note"), "[EMAIL], 555-123-4567");
}
