//! The staging format, without DuckDB. The bridge end to end, with DuckDB, is
//! in `tests/end_to_end.rs`.

use super::*;
use serde_json::json;

fn record(value: serde_json::Value) -> Record {
    value.as_object().expect("an object").clone()
}

#[test]
fn a_record_is_one_line_of_json() {
    let mut out = Vec::new();
    let mut writer = JsonlWriter { out: &mut out };

    writer.write(record(json!({"a": "1", "b": "x"}))).unwrap();
    writer.write(record(json!({"a": "2"}))).unwrap();

    assert_eq!(
        String::from_utf8(out).unwrap(),
        "{\"a\":\"1\",\"b\":\"x\"}\n{\"a\":\"2\"}\n"
    );
}

#[test]
fn what_was_written_reads_back_in_order_and_blank_lines_are_skipped() {
    let text = "{\"n\":1}\n\n{\"n\":2}\r\n";
    let mut reader = JsonlReader {
        lines: text.as_bytes(),
        line: 0,
    };

    assert_eq!(reader.read().unwrap().unwrap()["n"], 1);
    assert_eq!(
        reader.read().unwrap().unwrap()["n"],
        2,
        "a CRLF line end too"
    );
    assert!(reader.read().unwrap().is_none());
}

#[test]
fn a_line_that_is_not_an_object_says_which_line() {
    let mut reader = JsonlReader {
        lines: "{\"n\":1}\n[1,2]\n".as_bytes(),
        line: 0,
    };

    reader.read().unwrap();
    let error = reader.read().unwrap_err().to_string();

    assert!(error.contains("line 2"), "{error}");
}

#[test]
fn a_non_finite_double_is_named_as_the_likely_cause() {
    // What DuckDB actually writes for `SELECT 1/0` in FORMAT json.
    let mut reader = JsonlReader {
        lines: "{\"boom\":Infinity}\n".as_bytes(),
        line: 0,
    };

    let error = reader.read().unwrap_err().to_string();
    assert!(error.contains("non-finite double"), "{error}");
}

#[test]
fn staging_files_are_removed_when_the_guard_goes() {
    let directory = std::env::temp_dir().join(format!("etl-native-guard-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let file = directory.join("a.jsonl");
    std::fs::write(&file, "{}\n").unwrap();

    {
        let mut staging = Staging::default();
        staging.track(file.clone());
        // A second path that never got created must not stop the first going.
        staging.track(directory.join("never-written.jsonl"));
    }

    assert!(!file.exists());
    let _ = std::fs::remove_dir_all(&directory);
}
