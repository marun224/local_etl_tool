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

// ---------------------------------------------------------------------------
// Checkpoints: the position a source keeps between runs
// ---------------------------------------------------------------------------

use etl_metadata::{ComponentSpec, PipelineDoc};
use etl_plugin_sdk::{Source, Summary};
use std::sync::Mutex;

/// A source that exists only here: it counts on from where it last stopped.
/// Its checkpoint is a plain number, and it notes what it was handed.
struct Counter {
    seen: Mutex<Vec<Option<serde_json::Value>>>,
}

impl Source for Counter {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("src.file.xml", "Counter")
    }

    fn read(
        &self,
        _properties: &serde_json::Value,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        self.seen.lock().unwrap().push(context.checkpoint.clone());
        let from = context
            .checkpoint
            .as_ref()
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        for n in from + 1..=from + 3 {
            out.write(record(json!({ "n": n })))?;
        }
        Ok(Summary {
            checkpoint: Some(json!(from + 3)),
            ..Summary::new(3, format!("counted {} to {}", from + 1, from + 3))
        })
    }
}

/// A plan with one native source, compiled against `checkpoints`. The node
/// is an XML source so the registry knows it; the reading is `Counter`'s.
fn counted_plan(checkpoints: &[(&str, serde_json::Value)]) -> crate::Plan {
    let document = PipelineDoc::from_json(
        r#"{ "formatVersion": 1, "nodes": [
            { "id": "count", "position": {"x":0,"y":0},
              "data": { "label": "Count", "componentId": "src.file.xml",
                        "properties": { "path": "never-read.xml", "record": "r" } } }
          ], "edges": [] }"#,
    )
    .unwrap();
    let options = crate::CompileOptions {
        checkpoints: checkpoints
            .iter()
            .map(|(node, value)| (node.to_string(), value.clone()))
            .collect(),
        ..Default::default()
    };
    crate::plan::compile_with(&document, &options).expect("compiles")
}

fn stage_with(counter: &'static Counter, plan: &crate::Plan) -> Staged {
    let directory = std::env::temp_dir().join(format!("etl-native-count-{}", std::process::id()));
    let options = RunOptions {
        working_dir: Some(directory),
        ..Default::default()
    };
    let mut staging = Staging::default();
    stage_sources_using(&plan.stages, &options, &mut staging, |id| {
        (id == "src.file.xml").then_some(Connector::Source(counter))
    })
    .expect("stages")
}

#[test]
fn a_saved_position_reaches_the_connector_and_the_new_one_comes_back() {
    let counter: &'static Counter = Box::leak(Box::new(Counter {
        seen: Mutex::new(Vec::new()),
    }));

    // First run: nothing saved, so the connector is handed nothing.
    let first = stage_with(counter, &counted_plan(&[]));
    assert_eq!(
        first.checkpoints,
        [Checkpoint {
            node_id: "count".into(),
            component_id: "src.file.xml".into(),
            value: json!(3),
        }]
    );
    assert_eq!(first.notes, ["Count: counted 1 to 3"]);

    // Second run, compiled against what the first returned: it carries on.
    let second = stage_with(counter, &counted_plan(&[("count", json!(3))]));
    assert_eq!(second.checkpoints[0].value, json!(6));

    assert_eq!(*counter.seen.lock().unwrap(), [None, Some(json!(3))]);
}

#[test]
fn a_checkpoint_goes_only_to_the_node_it_was_saved_for() {
    let plan = counted_plan(&[("someone_else", json!(99))]);
    let step = plan.stages[0].native.as_ref().expect("native");
    assert_eq!(step.checkpoint, None);

    let plan = counted_plan(&[("count", json!(99))]);
    assert_eq!(
        plan.stages[0].native.as_ref().unwrap().checkpoint,
        Some(json!(99))
    );
}
