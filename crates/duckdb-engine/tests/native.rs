//! End-to-end for components written in Rust: a connector, the staging file,
//! and a real DuckDB on the other side of it.
//!
//! Each test gets a workspace of its own under `target/test-out/`, because a
//! staging file is named after its node id and two tests sharing a workspace
//! would share staging files -- the same trade spills make, and the reason
//! the scheduler runs one pipeline at a time.
//!
//! Like `end_to_end.rs`, these skip rather than fail without the vendored
//! DuckDB binary.

use etl_duckdb_engine::{compile, preview, run, ExecError, RunOptions, NATIVE_DIR};
use etl_metadata::PipelineDoc;
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/<name>/ sits two levels under the root")
        .to_path_buf()
}

/// A fresh workspace for one test, and the binary to run it with.
fn workspace(name: &str) -> Option<(PathBuf, PathBuf)> {
    let directory = repo_root()
        .join("target")
        .join("test-out")
        .join("native")
        .join(name);
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();

    let options = options(&directory);
    let Ok(binary) = etl_duckdb_engine::exec::locate_duckdb(&options) else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return None;
    };

    Some((directory, binary))
}

fn options(workspace: &Path) -> RunOptions {
    RunOptions {
        working_dir: Some(workspace.to_path_buf()),
        ..Default::default()
    }
}

/// A path for a pipeline document: forward slashes, which DuckDB and the XML
/// connector both take on every platform.
fn slashed(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn sample_xml() -> String {
    slashed(&repo_root().join("samples/data/orders.xml"))
}

fn sample_csv() -> String {
    slashed(&repo_root().join("samples/data/orders.csv"))
}

fn document(json: &str) -> PipelineDoc {
    PipelineDoc::from_json(json).expect("test pipeline parses")
}

fn query(binary: &Path, workspace: &Path, sql: &str) -> String {
    let output = Command::new(binary)
        .arg("-json")
        .arg("-c")
        .arg(sql)
        .current_dir(workspace)
        .output()
        .expect("duckdb runs");

    assert!(
        output.status.success(),
        "verification query failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Whatever is left in the staging directory. Always empty after a run.
fn leftovers(workspace: &Path) -> Vec<String> {
    std::fs::read_dir(workspace.join(NATIVE_DIR))
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

const TYPED_COLUMNS: &str = r#"{
    "@id": "INTEGER", "customer_id": "VARCHAR", "order_ts": "TIMESTAMP",
    "amount": "DECIMAL(10,2)", "amount@currency": "VARCHAR", "status": "VARCHAR", "note": "VARCHAR"
}"#;

/// XML in, filtered to 2026, written to Parquet. `policy` goes on the filter,
/// so passing one moves the whole plan onto the session transport.
fn xml_to_parquet(policy: &str) -> PipelineDoc {
    document(&format!(
        r#"{{
          "formatVersion": 1,
          "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{
                "label": "Orders XML", "componentId": "src.file.xml",
                "properties": {{ "path": "{xml}", "record": "order", "columns": {TYPED_COLUMNS} }} }} }},
            {{ "id": "recent", "position": {{"x":0,"y":0}}, "data": {{
                "label": "2026 only", "componentId": "xf.filter",
                "properties": {{ "predicate": "order_ts >= '2026-01-01'" }}{policy} }} }},
            {{ "id": "write", "position": {{"x":0,"y":0}}, "data": {{
                "label": "Parquet", "componentId": "snk.file.parquet",
                "properties": {{ "path": "out/recent.parquet" }} }} }}
          ],
          "edges": [
            {{ "id": "e1", "source": "read", "target": "recent" }},
            {{ "id": "e2", "source": "recent", "target": "write" }}
          ]
        }}"#,
        xml = sample_xml()
    ))
}

// ---------------------------------------------------------------------------
// In: XML to DuckDB
// ---------------------------------------------------------------------------

#[test]
fn xml_is_read_typed_and_filtered_on_the_one_script_path() {
    let Some((workspace, binary)) = workspace("xml_in_one_script") else {
        return;
    };

    let plan = compile(&xml_to_parquet("")).expect("compiles");
    assert!(
        !plan.needs_session(),
        "a native source does not earn a session"
    );

    let report = run(&plan, &options(&workspace)).expect("runs");

    let rows: Vec<Option<u64>> = report.stages.iter().map(|s| s.rows).collect();
    assert_eq!(rows, [Some(12), Some(7), Some(7)]);
    let expected = format!("Orders XML: 12 record(s) read from {}", sample_xml());
    assert!(report.notes.contains(&expected), "{:?}", report.notes);

    // Typed on the way in, and the XML's own quirks carried through.
    let answer = query(
        &binary,
        &workspace,
        "SELECT sum(amount)::VARCHAR AS total, min(\"@id\") AS first_id, \
         count(note) AS notes, any_value(\"amount@currency\") AS currency, \
         typeof(any_value(order_ts)) AS ts_type FROM 'out/recent.parquet';",
    );
    assert_eq!(
        answer,
        r#"[{"total":"1483.02","first_id":1006,"notes":1,"currency":"EUR","ts_type":"TIMESTAMP"}]"#
    );

    assert!(
        leftovers(&workspace).is_empty(),
        "{:?}",
        leftovers(&workspace)
    );
}

#[test]
fn the_same_xml_pipeline_gives_the_same_answer_on_the_session_path() {
    let Some((workspace, binary)) = workspace("xml_in_session") else {
        return;
    };

    let plan = compile(&xml_to_parquet(r#", "policy": { "retryAttempts": 1 }"#)).expect("compiles");
    assert!(
        plan.needs_session(),
        "the policy should have earned a session"
    );

    let report = run(&plan, &options(&workspace)).expect("runs");
    let rows: Vec<Option<u64>> = report.stages.iter().map(|s| s.rows).collect();
    assert_eq!(rows, [Some(12), Some(7), Some(7)]);

    assert_eq!(
        query(
            &binary,
            &workspace,
            "SELECT sum(amount)::VARCHAR AS t FROM 'out/recent.parquet';"
        ),
        r#"[{"t":"1483.02"}]"#
    );
    assert!(leftovers(&workspace).is_empty());
}

#[test]
fn without_declared_columns_duckdb_infers_timestamps_and_leaves_the_rest_as_text() {
    let Some((workspace, binary)) = workspace("xml_untyped") else {
        return;
    };

    let pipeline = document(&format!(
        r#"{{ "formatVersion": 1,
          "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "XML",
               "componentId": "src.file.xml", "properties": {{ "path": "{xml}", "record": "order" }} }} }},
            {{ "id": "write", "position": {{"x":0,"y":0}}, "data": {{ "label": "Parquet",
               "componentId": "snk.file.parquet", "properties": {{ "path": "out/raw.parquet" }} }} }}
          ],
          "edges": [ {{ "id": "e1", "source": "read", "target": "write" }} ] }}"#,
        xml = sample_xml()
    ));

    run(&compile(&pipeline).unwrap(), &options(&workspace)).expect("runs");

    // This was expected to be all text until it ran: DuckDB's JSON reader
    // recognises an ISO timestamp inside a string, the way `src.file.json` and
    // `src.file.csv` already type their columns. The amounts, the ids and the
    // status stay as XML wrote them. Declaring `columns` is how to be certain.
    let answer = query(
        &binary,
        &workspace,
        "SELECT string_agg(column_name || ' ' || column_type, ', ') AS shape \
         FROM (DESCRIBE SELECT * FROM 'out/raw.parquet');",
    );
    // `note` appears on only three rows, and is still found.
    assert_eq!(
        answer,
        r#"[{"shape":"@id VARCHAR, customer_id VARCHAR, order_ts TIMESTAMP, amount@currency VARCHAR, amount VARCHAR, status VARCHAR, note VARCHAR"}]"#
    );
}

#[test]
fn previewing_an_xml_source_reads_it_and_leaves_nothing_behind() {
    let Some((workspace, _)) = workspace("xml_preview") else {
        return;
    };

    let plan = compile(&xml_to_parquet("")).expect("compiles");
    let rows = preview(&plan, "recent", 3, &options(&workspace)).expect("previews");

    assert_eq!(rows.rows.len(), 3);
    assert!(rows.truncated, "7 rows, 3 asked for");
    assert_eq!(rows.rows[0]["@id"], 1006);

    assert!(
        !workspace.join("out/recent.parquet").exists(),
        "a preview never writes a sink"
    );
    assert!(leftovers(&workspace).is_empty());
}

#[test]
fn a_connector_that_fails_stops_the_run_by_name_and_masks_secrets() {
    let Some((workspace, _)) = workspace("xml_missing") else {
        return;
    };

    // A path that holds a "secret", so the masking of connector errors is
    // exercised through the same list DuckDB's errors are masked with.
    let pipeline = document(
        r#"{ "formatVersion": 1,
          "nodes": [
            { "id": "read", "position": {"x":0,"y":0}, "data": { "label": "Feed",
              "componentId": "src.file.xml",
              "properties": { "path": "in/hunter2/feed.xml", "record": "item" } } },
            { "id": "write", "position": {"x":0,"y":0}, "data": { "label": "Out",
              "componentId": "snk.file.parquet", "properties": { "path": "out/x.parquet" } } }
          ],
          "edges": [ { "id": "e1", "source": "read", "target": "write" } ] }"#,
    );

    let options = RunOptions {
        redact: vec!["hunter2".to_string()],
        ..options(&workspace)
    };
    let error = run(&compile(&pipeline).unwrap(), &options).unwrap_err();

    match &error {
        ExecError::StageFailed {
            node_id,
            label,
            message,
        } => {
            assert_eq!(node_id, "read");
            assert_eq!(label, "Feed");
            assert!(message.contains("feed.xml"), "{message}");
            assert!(!message.contains("hunter2"), "masked: {message}");
        }
        other => panic!("expected the stage to fail, got {other}"),
    }

    assert!(
        !workspace.join("out/x.parquet").exists(),
        "DuckDB never ran"
    );
    assert!(leftovers(&workspace).is_empty());
}

// ---------------------------------------------------------------------------
// Out: DuckDB to XML
// ---------------------------------------------------------------------------

/// CSV in, XML out.
fn csv_to_xml(target: &str, mode: &str) -> PipelineDoc {
    document(&format!(
        r#"{{ "formatVersion": 1,
          "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "CSV",
               "componentId": "src.file.csv", "properties": {{ "path": "{csv}" }} }} }},
            {{ "id": "write", "position": {{"x":0,"y":0}}, "data": {{ "label": "XML out",
               "componentId": "snk.file.xml",
               "properties": {{ "path": "{target}", "root": "orders", "record": "order", "mode": "{mode}" }} }} }}
          ],
          "edges": [ {{ "id": "e1", "source": "read", "target": "write" }} ] }}"#,
        csv = sample_csv()
    ))
}

/// XML in, XML out, untouched.
fn xml_to_xml(source: &str, target: &str) -> PipelineDoc {
    document(&format!(
        r#"{{ "formatVersion": 1,
          "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "XML in",
               "componentId": "src.file.xml", "properties": {{ "path": "{source}", "record": "order" }} }} }},
            {{ "id": "write", "position": {{"x":0,"y":0}}, "data": {{ "label": "XML out",
               "componentId": "snk.file.xml",
               "properties": {{ "path": "{target}", "root": "orders", "record": "order" }} }} }}
          ],
          "edges": [ {{ "id": "e1", "source": "read", "target": "write" }} ] }}"#
    ))
}

#[test]
fn csv_to_xml_and_back_through_duckdb_is_byte_stable() {
    let Some((workspace, _)) = workspace("xml_round_trip") else {
        return;
    };

    let report = run(
        &compile(&csv_to_xml("out/a.xml", "overwrite")).unwrap(),
        &options(&workspace),
    )
    .expect("csv to xml runs");
    assert!(
        report
            .notes
            .contains(&"XML out: 12 record(s) written to out/a.xml".to_string()),
        "{:?}",
        report.notes
    );

    run(
        &compile(&xml_to_xml("out/a.xml", "out/b.xml")).unwrap(),
        &options(&workspace),
    )
    .expect("xml to xml runs");

    let first = std::fs::read(workspace.join("out/a.xml")).unwrap();
    let second = std::fs::read(workspace.join("out/b.xml")).unwrap();

    assert!(
        first == second,
        "XML read by DuckDB and written again changed:\n--- a\n{}\n--- b\n{}",
        String::from_utf8_lossy(&first),
        String::from_utf8_lossy(&second)
    );

    let text = String::from_utf8(first).unwrap();
    assert!(text.contains("<order_id>1001</order_id>"), "{text}");
    assert!(
        text.contains("<amount>120.5</amount>"),
        "a DOUBLE, as DuckDB wrote it: {text}"
    );
    assert!(leftovers(&workspace).is_empty());
}

#[test]
fn error_if_exists_refuses_an_xml_sink_before_anything_runs() {
    let Some((workspace, _)) = workspace("xml_exists") else {
        return;
    };
    std::fs::create_dir_all(workspace.join("out")).unwrap();
    std::fs::write(workspace.join("out/keep.xml"), "keep me").unwrap();

    let error = run(
        &compile(&csv_to_xml("out/keep.xml", "error_if_exists")).unwrap(),
        &options(&workspace),
    )
    .unwrap_err();

    assert!(matches!(error, ExecError::OutputExists { .. }), "{error}");
    assert_eq!(
        std::fs::read_to_string(workspace.join("out/keep.xml")).unwrap(),
        "keep me"
    );
}

#[test]
fn a_run_that_fails_delivers_nothing_even_where_its_copy_succeeded() {
    // Two branches from one source, on the session transport with
    // continueOnFailure. The XML branch's COPY succeeds; the other branch
    // fails. The run ends failed, so the XML is withheld: a pipeline must not
    // half-deliver.
    let Some((workspace, _)) = workspace("xml_withheld") else {
        return;
    };

    let pipeline = document(&format!(
        r#"{{ "formatVersion": 1,
          "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "CSV",
               "componentId": "src.file.csv", "properties": {{ "path": "{csv}" }} }} }},
            {{ "id": "write_xml", "position": {{"x":0,"y":0}}, "data": {{ "label": "XML out",
               "componentId": "snk.file.xml",
               "properties": {{ "path": "out/orders.xml" }},
               "policy": {{ "continueOnFailure": true }} }} }},
            {{ "id": "broken", "position": {{"x":0,"y":0}}, "data": {{ "label": "Broken",
               "componentId": "xf.filter", "properties": {{ "predicate": "no_such_column > 1" }},
               "policy": {{ "continueOnFailure": true }} }} }},
            {{ "id": "write_csv", "position": {{"x":0,"y":0}}, "data": {{ "label": "CSV out",
               "componentId": "snk.file.csv", "properties": {{ "path": "out/broken.csv" }},
               "policy": {{ "continueOnFailure": true }} }} }}
          ],
          "edges": [
            {{ "id": "e1", "source": "read", "target": "write_xml" }},
            {{ "id": "e2", "source": "read", "target": "broken" }},
            {{ "id": "e3", "source": "broken", "target": "write_csv" }}
          ] }}"#,
        csv = sample_csv()
    ));

    let report = run(&compile(&pipeline).unwrap(), &options(&workspace))
        .expect("continueOnFailure returns a report");

    assert!(report.failed(), "the broken branch failed");
    assert!(
        !workspace.join("out/orders.xml").exists(),
        "delivered despite a failed run"
    );
    assert!(
        report
            .notes
            .contains(&"XML out: nothing delivered, because the run failed".to_string()),
        "{:?}",
        report.notes
    );
    assert!(leftovers(&workspace).is_empty());
}

#[test]
fn an_xml_sink_behind_a_branch_not_taken_is_not_an_error() {
    let Some((workspace, _)) = workspace("xml_not_taken") else {
        return;
    };

    let pipeline = document(&format!(
        r#"{{ "formatVersion": 1,
          "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "CSV",
               "componentId": "src.file.csv", "properties": {{ "path": "{csv}" }} }} }},
            {{ "id": "huge", "position": {{"x":0,"y":0}}, "data": {{ "label": "Any huge order?",
               "componentId": "ctl.branch", "properties": {{ "predicate": "amount > 100000" }} }} }},
            {{ "id": "write", "position": {{"x":0,"y":0}}, "data": {{ "label": "XML out",
               "componentId": "snk.file.xml", "properties": {{ "path": "out/huge.xml" }} }} }}
          ],
          "edges": [
            {{ "id": "e1", "source": "read", "target": "huge" }},
            {{ "id": "e2", "source": "huge", "target": "write" }}
          ] }}"#,
        csv = sample_csv()
    ));

    let report = run(&compile(&pipeline).unwrap(), &options(&workspace)).expect("runs");

    assert!(!report.failed());
    assert!(!workspace.join("out/huge.xml").exists());
    assert!(leftovers(&workspace).is_empty());
}

// ---------------------------------------------------------------------------
// REST, through a whole pipeline
// ---------------------------------------------------------------------------

use std::sync::{Arc, Mutex};

/// One request the fixture API received: method, URL, Authorization, body.
type Seen = (String, String, Option<String>, String);

/// A local stand-in for an orders API: `GET /orders` in two cursor pages of
/// the twelve sample orders, and `POST /large-orders` accepting anything.
/// `answer` may override a response by returning `Some((status, body))`.
struct Api {
    base: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    server: Arc<tiny_http::Server>,
}

impl Drop for Api {
    fn drop(&mut self) {
        self.server.unblock();
    }
}

fn sample_orders() -> Vec<serde_json::Value> {
    std::fs::read_to_string(repo_root().join("samples/data/orders.csv"))
        .unwrap()
        .lines()
        .skip(1)
        .map(|line| {
            let f: Vec<&str> = line.split(',').collect();
            serde_json::json!({
                "order_id": f[0].parse::<i64>().unwrap(),
                "customer_id": f[1],
                "order_ts": f[2],
                "amount": f[3].parse::<f64>().unwrap(),
                "status": f[4],
            })
        })
        .collect()
}

fn orders_api<F>(answer: F) -> Api
where
    F: Fn(&Seen) -> Option<(u16, String)> + Send + 'static,
{
    let server = Arc::new(tiny_http::Server::http("127.0.0.1:0").unwrap());
    let port = server.server_addr().to_ip().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let orders = sample_orders();

    let (listening, log) = (Arc::clone(&server), Arc::clone(&seen));
    std::thread::spawn(move || {
        for mut request in listening.incoming_requests() {
            let mut body = String::new();
            let _ = request.as_reader().read_to_string(&mut body);
            let authorization = request
                .headers()
                .iter()
                .find(|h| h.field.equiv("Authorization"))
                .map(|h| h.value.to_string());
            let received: Seen = (
                request.method().to_string(),
                request.url().to_string(),
                authorization,
                body,
            );
            log.lock().unwrap().push(received.clone());

            let (status, text) = answer(&received).unwrap_or_else(|| match received.1.as_str() {
                "/orders" => (
                    200,
                    serde_json::json!({ "data": orders[..7], "next": "p2" }).to_string(),
                ),
                "/orders?cursor=p2" => (
                    200,
                    serde_json::json!({ "data": orders[7..], "next": null }).to_string(),
                ),
                "/large-orders" => (201, String::new()),
                other => (404, format!("no route {other}")),
            });

            let _ =
                request.respond(tiny_http::Response::from_string(text).with_status_code(status));
        }
    });

    Api {
        base: format!("http://127.0.0.1:{port}"),
        seen,
        server,
    }
}

const TOKEN: &str = "t0k-e2e-secret";

/// The committed `rest_orders` sample, resolved against `api` with the token
/// in an encrypted secret store in `workspace`.
fn rest_orders(api: &Api, workspace: &Path) -> (etl_duckdb_engine::Plan, RunOptions) {
    let mut store = etl_secrets::SecretStore::open(workspace).unwrap();
    store.set("api_token", TOKEN, None).unwrap();

    let text = std::fs::read_to_string(repo_root().join("samples/pipelines/rest_orders.json"))
        .expect("the sample is committed");
    let resolver = etl_duckdb_engine::Resolver::new(workspace)
        .bind("api_base", &api.base)
        .secrets(store);
    let resolved = etl_duckdb_engine::resolve(&document(&text), &resolver).expect("resolves");

    let options = RunOptions {
        redact: resolved.secret_values(),
        ..options(workspace)
    };
    (compile(&resolved.document).expect("compiles"), options)
}

#[test]
fn the_rest_sample_reads_two_pages_filters_and_posts_in_batches() {
    let Some((workspace, _)) = workspace("rest_sample") else {
        return;
    };
    let api = orders_api(|_| None);
    let (plan, options) = rest_orders(&api, &workspace);

    let report = run(&plan, &options).expect("runs");

    let rows: Vec<Option<u64>> = report.stages.iter().map(|s| s.rows).collect();
    assert_eq!(
        rows,
        [Some(12), Some(6), Some(6)],
        "six orders are over 100"
    );

    let seen = api.seen.lock().unwrap().clone();
    let urls: Vec<(&str, &str)> = seen.iter().map(|s| (s.0.as_str(), s.1.as_str())).collect();
    assert_eq!(
        urls,
        [
            ("GET", "/orders"),
            ("GET", "/orders?cursor=p2"),
            ("POST", "/large-orders"),
            ("POST", "/large-orders"),
            ("POST", "/large-orders"),
        ]
    );

    // The secret reached the API on every request...
    let bearer = format!("Bearer {TOKEN}");
    for request in &seen {
        assert_eq!(request.2.as_deref(), Some(bearer.as_str()));
    }

    // ...as typed rows, in batches of two, wrapped.
    let posted: Vec<serde_json::Value> = seen[2..]
        .iter()
        .flat_map(|s| {
            let body: serde_json::Value = serde_json::from_str(&s.3).unwrap();
            body["orders"].as_array().unwrap().clone()
        })
        .collect();
    assert_eq!(posted.len(), 6);
    assert_eq!(posted[0]["order_id"], 1001);
    assert!(posted
        .iter()
        .all(|row| row["amount"].as_f64().unwrap() > 100.0));

    // ...and is nowhere in what a person sees.
    let seen_by_people = format!("{:?} {}", report.notes, report.script);
    assert!(!seen_by_people.contains(TOKEN), "{seen_by_people}");

    let read = format!(
        "Orders API: 12 record(s) from 2 page(s) of {}/orders",
        api.base
    );
    assert!(report.notes.contains(&read), "{:?}", report.notes);
    let posted_note = format!(
        "Large orders API: 6 record(s) in 3 request(s) to {}/large-orders",
        api.base
    );
    assert!(report.notes.contains(&posted_note), "{:?}", report.notes);

    let endpoint = format!("{}/orders", api.base);
    assert_eq!(
        plan.stage("read_orders").unwrap().external.as_deref(),
        Some(endpoint.as_str())
    );
    assert!(leftovers(&workspace).is_empty());
}

#[test]
fn an_api_that_echoes_the_token_in_its_error_is_masked_in_ours() {
    let Some((workspace, _)) = workspace("rest_unauthorised") else {
        return;
    };
    // Some APIs say which credential they refused. That text goes into our
    // error, so it has to be masked the way DuckDB's errors are.
    let api = orders_api(|seen| {
        (seen.0 == "GET").then(|| (401, format!("{{\"error\":\"token {TOKEN} is revoked\"}}")))
    });
    let (plan, options) = rest_orders(&api, &workspace);

    let error = run(&plan, &options).unwrap_err();

    match &error {
        ExecError::StageFailed {
            node_id, message, ..
        } => {
            assert_eq!(node_id, "read_orders");
            assert!(message.contains("HTTP 401"), "{message}");
            assert!(message.contains("token ******** is revoked"), "{message}");
            assert!(!message.contains(TOKEN), "{message}");
        }
        other => panic!("expected the source to fail, got {other}"),
    }

    let seen = api.seen.lock().unwrap().clone();
    assert_eq!(
        seen.len(),
        1,
        "a 401 is not retried, and nothing was posted"
    );
    assert!(leftovers(&workspace).is_empty());
}

#[test]
fn a_rest_sink_that_fails_fails_the_run_after_duckdb_succeeded() {
    let Some((workspace, _)) = workspace("rest_sink_fails") else {
        return;
    };
    let api = orders_api(|seen| (seen.0 == "POST").then(|| (500, "down".to_string())));

    let pipeline = document(&format!(
        r#"{{ "formatVersion": 1,
          "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "CSV",
               "componentId": "src.file.csv", "properties": {{ "path": "{csv}" }} }} }},
            {{ "id": "post", "position": {{"x":0,"y":0}}, "data": {{ "label": "Post",
               "componentId": "snk.saas.rest",
               "properties": {{ "url": "{base}/large-orders", "retries": 0 }} }} }}
          ],
          "edges": [ {{ "id": "e1", "source": "read", "target": "post" }} ] }}"#,
        csv = sample_csv(),
        base = api.base
    ));

    let error = run(&compile(&pipeline).unwrap(), &options(&workspace)).unwrap_err();

    match &error {
        ExecError::StageFailed {
            node_id, message, ..
        } => {
            assert_eq!(node_id, "post");
            assert!(
                message
                    .starts_with("batch 1 failed after 0 batch(es) (0 record(s)) were delivered"),
                "{message}"
            );
            assert!(message.contains("HTTP 500"), "{message}");
        }
        other => panic!("expected the sink to fail, got {other}"),
    }
    assert!(leftovers(&workspace).is_empty());
}
