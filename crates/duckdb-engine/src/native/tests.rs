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

// ---------------------------------------------------------------------------
// Receipts: what a queue source holds until the run's outcome is known
// ---------------------------------------------------------------------------

/// How each receipt ended, in order, shared with the test that made them.
type Ledger = std::sync::Arc<Mutex<Vec<String>>>;

/// A receipt that writes down how it was settled. `fail` makes it refuse to
/// acknowledge, quoting a secret the engine must mask.
struct Noted {
    name: &'static str,
    ledger: Ledger,
    fail: bool,
    settled: bool,
}

impl Noted {
    fn boxed(name: &'static str, ledger: &Ledger, fail: bool) -> Box<dyn etl_plugin_sdk::Receipt> {
        Box::new(Noted {
            name,
            ledger: ledger.clone(),
            fail,
            settled: false,
        })
    }
}

impl etl_plugin_sdk::Receipt for Noted {
    fn acknowledge(mut self: Box<Self>) -> Result<String, ConnectorError> {
        self.settled = true;
        if self.fail {
            self.ledger
                .lock()
                .unwrap()
                .push(format!("{} refused", self.name));
            return Err(ConnectorError::Data("token hunter2 expired".into()));
        }
        self.ledger
            .lock()
            .unwrap()
            .push(format!("{} acknowledged", self.name));
        Ok("3 message(s) acknowledged".into())
    }

    fn release(mut self: Box<Self>) -> Result<String, ConnectorError> {
        self.settled = true;
        self.ledger
            .lock()
            .unwrap()
            .push(format!("{} released", self.name));
        Ok("3 message(s) released".into())
    }
}

impl Drop for Noted {
    fn drop(&mut self) {
        if !self.settled {
            self.ledger
                .lock()
                .unwrap()
                .push(format!("{} dropped unsettled", self.name));
        }
    }
}

fn masking(secret: &str) -> RunOptions {
    RunOptions {
        redact: vec![secret.to_string()],
        ..Default::default()
    }
}

#[test]
fn a_success_acknowledges_every_receipt_and_a_refusal_is_a_masked_warning() {
    let ledger = Ledger::default();
    let mut receipts = Receipts::default();
    let options = masking("hunter2");
    receipts.hold("Orders", Noted::boxed("orders", &ledger, false), &options);
    receipts.hold("Refunds", Noted::boxed("refunds", &ledger, true), &options);

    let settled = receipts.acknowledge();
    assert_eq!(settled.notes, ["Orders: 3 message(s) acknowledged"]);
    assert_eq!(settled.warnings.len(), 1);
    let warning = &settled.warnings[0];
    assert!(
        warning.starts_with(
            "Refunds: the messages read could not be acknowledged and will be delivered again"
        ),
        "{warning}"
    );
    assert!(!warning.contains("hunter2"), "masked: {warning}");
    assert_eq!(
        *ledger.lock().unwrap(),
        ["orders acknowledged", "refunds refused"]
    );
}

#[test]
fn a_failure_releases_every_receipt_and_says_so() {
    let ledger = Ledger::default();
    let mut receipts = Receipts::default();
    receipts.hold(
        "Orders",
        Noted::boxed("orders", &ledger, false),
        &RunOptions::default(),
    );
    receipts.hold(
        "Refunds",
        Noted::boxed("refunds", &ledger, false),
        &RunOptions::default(),
    );

    let settled = receipts.release();
    assert_eq!(
        settled.notes,
        [
            "Orders: 3 message(s) released",
            "Refunds: 3 message(s) released"
        ]
    );
    assert!(settled.warnings.is_empty());
    assert_eq!(
        *ledger.lock().unwrap(),
        ["orders released", "refunds released"]
    );
}

#[test]
fn receipts_dropped_unsettled_are_released_not_forgotten() {
    let ledger = Ledger::default();
    {
        let mut receipts = Receipts::default();
        receipts.hold(
            "Orders",
            Noted::boxed("orders", &ledger, false),
            &RunOptions::default(),
        );
        // An early return: nobody settles it.
    }
    assert_eq!(*ledger.lock().unwrap(), ["orders released"]);
}

/// A queue source that exists only here: three rows, held under a receipt.
struct Queue {
    ledger: Ledger,
}

impl Source for Queue {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("src.file.xml", "Queue")
    }

    fn read(
        &self,
        properties: &serde_json::Value,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let (summary, receipt) = self.read_held(properties, out, context)?;
        if let Some(receipt) = receipt {
            receipt.release()?;
        }
        Ok(summary)
    }

    fn read_held(
        &self,
        _properties: &serde_json::Value,
        out: &mut dyn RecordWriter,
        _context: &Context,
    ) -> Result<(Summary, Option<Box<dyn etl_plugin_sdk::Receipt>>), ConnectorError> {
        for n in 1..=3 {
            out.write(record(json!({ "n": n })))?;
        }
        Ok((
            Summary::new(3, "received 3"),
            Some(Noted::boxed("queue", &self.ledger, false)),
        ))
    }
}

/// Two native sources: `first` then `second`, in that order.
fn two_source_plan() -> crate::Plan {
    let document = PipelineDoc::from_json(
        r#"{ "formatVersion": 1, "nodes": [
            { "id": "first", "position": {"x":0,"y":0},
              "data": { "label": "First", "componentId": "src.file.xml",
                        "properties": { "path": "never-read.xml", "record": "r" } } },
            { "id": "second", "position": {"x":0,"y":0},
              "data": { "label": "Second", "componentId": "src.saas.rest",
                        "properties": { "url": "http://127.0.0.1:9/never" } } }
          ], "edges": [] }"#,
    )
    .unwrap();
    crate::plan::compile_with(&document, &crate::CompileOptions::default()).expect("compiles")
}

#[test]
fn staging_collects_what_a_source_holds() {
    let ledger = Ledger::default();
    let queue: &'static Queue = Box::leak(Box::new(Queue {
        ledger: ledger.clone(),
    }));
    let staged = stage_with_source(queue, &counted_plan(&[]));
    assert_eq!(format!("{:?}", staged.receipts), r#"["Count"]"#);
    assert_eq!(staged.notes, ["Count: received 3"]);

    let settled = staged.receipts.acknowledge();
    assert_eq!(settled.notes, ["Count: 3 message(s) acknowledged"]);
    assert_eq!(*ledger.lock().unwrap(), ["queue acknowledged"]);
}

#[test]
fn a_later_source_failing_releases_what_an_earlier_one_holds() {
    let ledger = Ledger::default();
    let queue: &'static Queue = Box::leak(Box::new(Queue {
        ledger: ledger.clone(),
    }));
    let directory = std::env::temp_dir().join(format!("etl-native-held-{}", std::process::id()));
    let options = RunOptions {
        working_dir: Some(directory),
        ..Default::default()
    };
    let mut staging = Staging::default();
    let error = stage_sources_using(&two_source_plan().stages, &options, &mut staging, |id| {
        // The first source holds; the second is not registered, so it fails.
        (id == "src.file.xml").then_some(Connector::Source(queue))
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("no connector is registered"),
        "{error}"
    );
    assert_eq!(*ledger.lock().unwrap(), ["queue released"]);
}

fn stage_with_source(source: &'static dyn Source, plan: &crate::Plan) -> Staged {
    let directory = std::env::temp_dir().join(format!("etl-native-queue-{}", std::process::id()));
    let options = RunOptions {
        working_dir: Some(directory),
        ..Default::default()
    };
    let mut staging = Staging::default();
    stage_sources_using(&plan.stages, &options, &mut staging, |id| {
        (id == "src.file.xml").then_some(Connector::Source(source))
    })
    .expect("stages")
}

// ---------------------------------------------------------------------------
// Transforms: Rust between two stages (Phase 11d2)
// ---------------------------------------------------------------------------

use etl_plugin_sdk::{Transform, ROW_KEY};

/// A transform that exists only here, standing in for `xf.ai.embed` so the
/// bridge runs without a model: each text's vector is its length, its word
/// count, and 1. A row with no text gets nothing, and an error is on request.
struct Lengths {
    fail: Option<&'static str>,
}

impl Transform for Lengths {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("xf.ai.embed", "Lengths")
    }

    fn reads(&self, _properties: &serde_json::Value) -> Vec<String> {
        vec!["body".into()]
    }

    fn adds(&self, _properties: &serde_json::Value) -> Vec<(String, String)> {
        vec![("embedding".into(), "FLOAT[3]".into())]
    }

    fn transform(
        &self,
        _properties: &serde_json::Value,
        input: &mut dyn RecordReader,
        out: &mut dyn RecordWriter,
        _context: &Context,
    ) -> Result<Summary, ConnectorError> {
        if let Some(message) = self.fail {
            return Err(ConnectorError::Data(message.to_string()));
        }
        let mut count = 0;
        while let Some(record) = input.read()? {
            let Some(text) = record.get("body").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let words = text.split_whitespace().count();
            out.write(self::record(json!({
                ROW_KEY: record[ROW_KEY],
                "embedding": [text.chars().count(), words, 1]
            })))?;
            count += 1;
        }
        Ok(Summary::new(count, format!("measured {count}")))
    }
}

static LENGTHS: Lengths = Lengths { fail: None };
static FAILING: Lengths = Lengths {
    fail: Some("the model refused token-123"),
};

/// A JSON Lines source feeding `xf.ai.embed`, in a directory of its own, and a
/// DuckDB session there. `None` without the vendored DuckDB.
fn transform_setup(
    name: &str,
    embed: serde_json::Value,
) -> Option<(crate::Plan, RunOptions, Session)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap()
        .to_path_buf();
    let binary = crate::exec::locate_duckdb(&RunOptions {
        working_dir: Some(root.clone()),
        ..Default::default()
    })
    .ok()?;
    let directory = root.join("target").join("test-out").join(name);
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join("rows.jsonl"),
        "{\"id\": 1, \"body\": \"a b\", \"day\": \"2026-01-02\", \"amount\": 1.5}\n\
         {\"id\": 2, \"body\": null, \"day\": \"2026-01-03\", \"amount\": 2.25}\n\
         {\"id\": 3, \"body\": \"hello world again\", \"day\": \"2026-01-04\", \"amount\": 3}\n",
    )
    .unwrap();

    let document = PipelineDoc::from_json(
        &json!({ "formatVersion": 1, "nodes": [
            { "id": "rows", "position": {"x": 0, "y": 0}, "data": {
                "label": "Rows", "componentId": "src.file.jsonl",
                "properties": { "path": "rows.jsonl" } } },
            { "id": "vectors", "position": {"x": 1, "y": 0}, "data": {
                "label": "Vectors", "componentId": "xf.ai.embed", "properties": embed } }
          ], "edges": [ { "id": "e1", "source": "rows", "target": "vectors" } ] })
        .to_string(),
    )
    .unwrap();
    let plan = crate::plan::compile(&document).expect("compiles");
    let options = RunOptions {
        working_dir: Some(directory.clone()),
        redact: vec!["token-123".to_string()],
        ..Default::default()
    };
    let session = Session::open(&binary, Some(&directory), None, &[]).expect("a session");
    Some((plan, options, session))
}

fn rows_of(session: &mut Session, sql: &str) -> Vec<serde_json::Value> {
    let answer = session.execute(sql).unwrap();
    assert!(!answer.has_message(), "{}", answer.stderr);
    match answer.values.into_iter().last() {
        Some(serde_json::Value::Array(rows)) => rows,
        other => panic!("no rows: {other:?}"),
    }
}

#[test]
fn a_transform_adds_its_columns_and_every_other_column_keeps_its_type() {
    let Some((plan, options, mut session)) = transform_setup(
        "native_transform",
        json!({ "column": "body", "dimensions": 3 }),
    ) else {
        return eprintln!("skipping: no DuckDB binary");
    };
    let mut staging = Staging::default();

    session.execute(&plan.stages[0].sql).unwrap();
    let vectors = &plan.stages[1];
    let note = run_transform_using(&mut session, vectors, &options, &mut staging, |id| {
        (id == "xf.ai.embed").then_some(Connector::Transform(&LENGTHS))
    })
    .expect("the session holds")
    .expect("the transform runs");
    assert_eq!(note, "Vectors: measured 2");
    assert!(!session.execute(&vectors.sql).unwrap().has_message());

    let rows = rows_of(&mut session, "SELECT id, body, embedding FROM vectors;");
    assert_eq!(
        rows,
        [
            json!({ "id": 1, "body": "a b", "embedding": [3.0, 2.0, 1.0] }),
            json!({ "id": 2, "body": null, "embedding": null }),
            json!({ "id": 3, "body": "hello world again", "embedding": [17.0, 3.0, 1.0] }),
        ],
        "in order, joined on the row, a row without text left null"
    );
    let types = rows_of(
        &mut session,
        "SELECT typeof(day) AS day, typeof(amount) AS amount, typeof(embedding) AS embedding \
         FROM vectors LIMIT 1;",
    );
    assert_eq!(
        types,
        [json!({ "day": "DATE", "amount": "DOUBLE", "embedding": "FLOAT[3]" })],
        "the untouched columns kept DuckDB's own types"
    );
}

#[test]
fn a_transform_that_fails_is_the_stage_failing_with_secrets_masked() {
    let Some((plan, options, mut session)) = transform_setup(
        "native_transform_fails",
        json!({ "column": "body", "dimensions": 3 }),
    ) else {
        return eprintln!("skipping: no DuckDB binary");
    };
    let mut staging = Staging::default();
    session.execute(&plan.stages[0].sql).unwrap();

    let message = run_transform_using(
        &mut session,
        &plan.stages[1],
        &options,
        &mut staging,
        |id| (id == "xf.ai.embed").then_some(Connector::Transform(&FAILING)),
    )
    .expect("the session holds")
    .expect_err("the stage fails");

    assert!(message.contains("the model refused"), "{message}");
    assert!(!message.contains("token-123"), "{message}");
}

#[test]
fn a_column_the_input_does_not_have_fails_the_feed_by_name() {
    let Some((plan, options, mut session)) = transform_setup(
        "native_transform_no_column",
        json!({ "column": "nowhere", "dimensions": 3 }),
    ) else {
        return eprintln!("skipping: no DuckDB binary");
    };
    let mut staging = Staging::default();
    session.execute(&plan.stages[0].sql).unwrap();

    // The real transform reads what its properties say: `nowhere`.
    let message = run_transform(&mut session, &plan.stages[1], &options, &mut staging)
        .expect("the session holds")
        .expect_err("the feed fails");

    assert!(message.contains("nowhere"), "{message}");
}
