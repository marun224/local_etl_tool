//! End-to-end: compile a pipeline, run it against a real DuckDB, verify the
//! bytes that came out.
//!
//! These are the tests that would have caught every mistake the unit tests
//! cannot — that the generated SQL is not merely well-formed but correct, and
//! that the executor reads DuckDB's output the way DuckDB actually writes it.
//!
//! They need the DuckDB binary. It is vendored at `tools/duckdb/` by
//! `scripts/fetch-duckdb.ps1`; without it these skip rather than fail, so a
//! fresh checkout is not red for a reason that has nothing to do with the code.

use etl_duckdb_engine::{
    compile, compile_with, run, CompileOptions, Contexts, ExecError, Resolver, RunOptions,
    RunReport, SkipReason,
};
use etl_metadata::PipelineDoc;
use std::path::{Path, PathBuf, MAIN_SEPARATOR};
use std::process::Command;

/// The repository root, found from this crate's manifest.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/<name>/ sits two levels under the root")
        .to_path_buf()
}

fn duckdb_binary() -> Option<PathBuf> {
    let options = RunOptions {
        working_dir: Some(repo_root()),
        ..Default::default()
    };

    etl_duckdb_engine::exec::locate_duckdb(&options).ok()
}

/// Give each test its own output directory, so they cannot tread on each other.
fn output_dir(name: &str) -> PathBuf {
    let directory = repo_root().join("target").join("test-out").join(name);

    let _ = std::fs::remove_dir_all(&directory);
    directory
}

/// Query the result independently of the code under test.
fn query(binary: &Path, sql: &str) -> String {
    let output = Command::new(binary)
        .arg("-json")
        .arg("-c")
        .arg(sql)
        .current_dir(repo_root())
        .output()
        .expect("duckdb runs");

    assert!(
        output.status.success(),
        "verification query failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// The five-stage sample, rewritten to write into `out`.
fn orders_enriched(out: &Path) -> PipelineDoc {
    let text = std::fs::read_to_string(repo_root().join("samples/pipelines/orders_enriched.json"))
        .expect("sample pipeline is committed");

    let mut document = PipelineDoc::from_json(&text).expect("sample parses");

    for node in &mut document.nodes {
        if node.data.component_id.as_deref() == Some("snk.file.parquet") {
            let properties = node.data.properties.as_mut().expect("sink has properties");
            properties["path"] = serde_json::json!(out
                .join("orders_enriched.parquet")
                .to_string_lossy()
                .to_string());
        }
    }

    document
}

fn options() -> RunOptions {
    RunOptions {
        working_dir: Some(repo_root()),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// The happy path
// ---------------------------------------------------------------------------

#[test]
fn csv_filter_join_parquet_produces_the_expected_rows_and_bytes() {
    let Some(binary) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("happy_path");
    let plan = compile(&orders_enriched(&out)).expect("sample compiles");

    let report = run(&plan, &options()).expect("sample runs");

    // 12 orders in, 5 customers; the 2026 filter keeps 7; the inner join drops
    // order 1010 because customer C006 is not in the customer file.
    let rows: Vec<Option<u64>> = report.stages.iter().map(|s| s.rows).collect();
    assert_eq!(
        rows,
        [Some(12), Some(5), Some(7), Some(6), Some(6)],
        "stage row counts"
    );

    let parquet = out.join("orders_enriched.parquet");
    assert!(parquet.is_file(), "the sink wrote nothing");

    let path = parquet.to_string_lossy().replace('\\', "/");

    assert_eq!(
        query(&binary, &format!("SELECT count(*) AS n FROM '{path}';")),
        r#"[{"n":6}]"#
    );

    // A content checksum rather than a file hash: Parquet embeds metadata that
    // need not be byte-stable between writes, but the data must be.
    assert_eq!(
        query(
            &binary,
            &format!(
                "SELECT md5(string_agg(order_id || '|' || customer_id || '|' || amount || '|' \
                 || name, ',' ORDER BY order_id)) AS checksum FROM '{path}';"
            )
        ),
        r#"[{"checksum":"3a528b4e19a789ba324972a2c082bda8"}]"#,
        "the joined data changed"
    );
}

#[test]
fn the_join_merges_both_schemas() {
    if duckdb_binary().is_none() {
        return;
    }

    let out = output_dir("schema");
    let plan = compile(&orders_enriched(&out)).expect("compiles");
    run(&plan, &options()).expect("runs");

    let binary = duckdb_binary().unwrap();
    let path = out
        .join("orders_enriched.parquet")
        .to_string_lossy()
        .replace('\\', "/");

    let columns = query(
        &binary,
        &format!(
            "SELECT string_agg(column_name, ',') AS c FROM (DESCRIBE SELECT * FROM '{path}');"
        ),
    );

    // USING merges the key, so it appears once rather than twice.
    assert_eq!(
        columns,
        r#"[{"c":"order_id,customer_id,order_ts,amount,status,name,segment,country"}]"#
    );
}

#[test]
fn a_missing_output_directory_is_created() {
    if duckdb_binary().is_none() {
        return;
    }

    let out = output_dir("nested").join("deeply").join("nested");
    assert!(!out.exists());

    let plan = compile(&orders_enriched(&out)).expect("compiles");
    run(&plan, &options()).expect("runs");

    assert!(out.join("orders_enriched.parquet").is_file());
}

#[test]
fn a_run_without_counts_still_writes_the_output() {
    if duckdb_binary().is_none() {
        return;
    }

    let out = output_dir("no_counts");
    let plan = compile(&orders_enriched(&out)).expect("compiles");

    let report = run(
        &plan,
        &RunOptions {
            counts: false,
            ..options()
        },
    )
    .expect("runs");

    assert!(
        report.stages.iter().all(|s| s.rows.is_none()),
        "counts were switched off"
    );
    assert!(out.join("orders_enriched.parquet").is_file());
}

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

#[test]
fn a_failure_is_attributed_to_the_stage_that_caused_it() {
    if duckdb_binary().is_none() {
        return;
    }

    let out = output_dir("attribution");
    let mut document = orders_enriched(&out);

    // Break the filter — the third of five stages, so a naive implementation
    // that always blamed the first or last stage would be caught here.
    for node in &mut document.nodes {
        if node.id == "filter_recent" {
            node.data.properties.as_mut().unwrap()["predicate"] =
                serde_json::json!("no_such_column > 1");
        }
    }

    let plan = compile(&document).expect("still compiles: the SQL is only checked by DuckDB");
    let error = run(&plan, &options()).expect_err("the run must fail");

    match error {
        ExecError::StageFailed {
            node_id, message, ..
        } => {
            assert_eq!(node_id, "filter_recent");
            assert!(message.contains("no_such_column"), "{message}");
        }
        other => panic!("expected a stage failure, got {other:?}"),
    }

    assert!(
        !out.join("orders_enriched.parquet").exists(),
        "a failed run must not leave output behind"
    );
}

#[test]
fn error_if_exists_refuses_before_anything_runs() {
    if duckdb_binary().is_none() {
        return;
    }

    let out = output_dir("no_clobber");
    std::fs::create_dir_all(&out).unwrap();

    let target = out.join("orders_enriched.parquet");
    std::fs::write(&target, b"existing content").unwrap();

    let mut document = orders_enriched(&out);
    for node in &mut document.nodes {
        if node.data.component_id.as_deref() == Some("snk.file.parquet") {
            node.data.properties.as_mut().unwrap()["mode"] = serde_json::json!("error_if_exists");
        }
    }

    let plan = compile(&document).expect("compiles");
    let error = run(&plan, &options()).expect_err("must refuse");

    assert!(matches!(error, ExecError::OutputExists { .. }), "{error:?}");
    assert_eq!(
        std::fs::read(&target).unwrap(),
        b"existing content",
        "the existing file must be untouched"
    );
}

#[test]
fn overwrite_is_the_default() {
    if duckdb_binary().is_none() {
        return;
    }

    let out = output_dir("overwrite");
    std::fs::create_dir_all(&out).unwrap();

    let target = out.join("orders_enriched.parquet");
    std::fs::write(&target, b"stale").unwrap();

    let plan = compile(&orders_enriched(&out)).expect("compiles");
    run(&plan, &options()).expect("runs");

    assert_ne!(std::fs::read(&target).unwrap(), b"stale");
}

// ---------------------------------------------------------------------------
// The script itself
// ---------------------------------------------------------------------------

#[test]
fn the_report_carries_the_script_that_ran() {
    if duckdb_binary().is_none() {
        return;
    }

    let out = output_dir("script");
    let plan = compile(&orders_enriched(&out)).expect("compiles");
    let report = run(&plan, &options()).expect("runs");

    assert_eq!(report.script, plan.script(true));
    assert!(report.script.contains("CREATE OR REPLACE TEMP VIEW"));
    assert!(report.script.contains("COPY ("));
}

// ---------------------------------------------------------------------------
// Transforms, against real data
//
// One representative per family. The golden-SQL tests prove the statement is
// the one intended; these prove DuckDB agrees it is valid and that it means
// what we think — which is the part a string comparison cannot check.
// ---------------------------------------------------------------------------

/// Build a document from JSON, with `{out}` replaced by a forward-slashed
/// output path. Going through the JSON keeps these tests on the same path a
/// real pipeline file takes.
fn document_with_out(json: &str, out: &Path) -> PipelineDoc {
    let path = out.to_string_lossy().replace('\\', "/");
    PipelineDoc::from_json(&json.replace("{out}", &path)).expect("document parses")
}

#[test]
fn a_derive_aggregate_sort_chain_runs_and_agrees_with_a_direct_query() {
    let Some(binary) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("transform_chain");
    let document = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "orders", "position": {"x": 0, "y": 0}, "data": {
              "label": "Orders", "componentId": "src.file.csv",
              "properties": {"path": "samples/data/orders.csv"}}},
            {"id": "with_tax", "position": {"x": 1, "y": 0}, "data": {
              "label": "With tax", "componentId": "xf.derive",
              "properties": {"expressions": "amount * 1.2 AS gross"}}},
            {"id": "per_customer", "position": {"x": 2, "y": 0}, "data": {
              "label": "Per customer", "componentId": "xf.aggregate",
              "properties": {"group_by": ["customer_id"],
                             "aggregations": "sum(amount) AS total, count(*) AS orders"}}},
            {"id": "ordered", "position": {"x": 3, "y": 0}, "data": {
              "label": "Ordered", "componentId": "xf.sort",
              "properties": {"by": "customer_id"}}},
            {"id": "sink", "position": {"x": 4, "y": 0}, "data": {
              "label": "Totals", "componentId": "snk.file.csv",
              "properties": {"path": "{out}/totals.csv"}}}
          ],
          "edges": [
            {"id": "e1", "source": "orders", "target": "with_tax"},
            {"id": "e2", "source": "with_tax", "target": "per_customer"},
            {"id": "e3", "source": "per_customer", "target": "ordered"},
            {"id": "e4", "source": "ordered", "target": "sink"}
          ]
        }"#,
        &out,
    );

    let plan = compile(&document).expect("compiles");
    let report = run(&plan, &options()).expect("runs");

    // 12 orders, unchanged by the derive, grouped into 6 customers.
    let rows: Vec<Option<u64>> = report.stages.iter().map(|s| s.rows).collect();
    assert_eq!(rows, [Some(12), Some(12), Some(6), Some(6), Some(6)]);

    let written = out.join("totals.csv").to_string_lossy().replace('\\', "/");

    // Compare against the same question asked directly of the source file, so
    // the assertion is not a second copy of the pipeline's own arithmetic.
    assert_eq!(
        query(
            &binary,
            &format!(
                "SELECT count(*) AS mismatched FROM '{written}' w FULL OUTER JOIN (SELECT \
                 customer_id, sum(amount) AS total, count(*) AS orders FROM \
                 read_csv('samples/data/orders.csv', header=true) GROUP BY customer_id) d USING \
                 (customer_id) WHERE w.total IS DISTINCT FROM d.total OR w.orders IS DISTINCT \
                 FROM d.orders;"
            )
        ),
        r#"[{"mismatched":0}]"#,
        "the pipeline and a direct query disagree"
    );
}

#[test]
fn a_union_of_two_filters_runs() {
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("transform_union");
    let document = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "orders", "position": {"x": 0, "y": 0}, "data": {
              "label": "Orders", "componentId": "src.file.csv",
              "properties": {"path": "samples/data/orders.csv"}}},
            {"id": "shipped", "position": {"x": 1, "y": 0}, "data": {
              "label": "Shipped", "componentId": "xf.filter",
              "properties": {"predicate": "status = 'shipped'"}}},
            {"id": "pending", "position": {"x": 1, "y": 1}, "data": {
              "label": "Pending", "componentId": "xf.filter",
              "properties": {"predicate": "status = 'pending'"}}},
            {"id": "both", "position": {"x": 2, "y": 0}, "data": {
              "label": "Both", "componentId": "xf.union", "properties": {}}},
            {"id": "sink", "position": {"x": 3, "y": 0}, "data": {
              "label": "Open orders", "componentId": "snk.file.csv",
              "properties": {"path": "{out}/open.csv"}}}
          ],
          "edges": [
            {"id": "e1", "source": "orders", "target": "shipped"},
            {"id": "e2", "source": "orders", "target": "pending"},
            {"id": "e3", "source": "shipped", "target": "both", "targetHandle": "left"},
            {"id": "e4", "source": "pending", "target": "both", "targetHandle": "right"},
            {"id": "e5", "source": "both", "target": "sink"}
          ]
        }"#,
        &out,
    );

    let plan = compile(&document).expect("compiles");
    let report = run(&plan, &options()).expect("runs");

    let rows: Vec<Option<u64>> = report.stages.iter().map(|s| s.rows).collect();

    // 7 shipped + 3 pending, stacked.
    assert_eq!(
        rows,
        [Some(12), Some(7), Some(3), Some(10), Some(10)],
        "stage row counts"
    );
    assert!(out.join("open.csv").is_file());
}

#[test]
fn pivot_and_dedup_survive_being_wrapped_in_a_view() {
    // The reason this test exists: every stage in a plan is a view, and DuckDB
    // refuses to build a view around a PIVOT whose result columns it would have
    // to learn by reading the data. That is why `values` is a required
    // property, and this is the test that keeps it that way.
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("transform_pivot");
    let document = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "orders", "position": {"x": 0, "y": 0}, "data": {
              "label": "Orders", "componentId": "src.file.csv",
              "properties": {"path": "samples/data/orders.csv"}}},
            {"id": "newest", "position": {"x": 1, "y": 0}, "data": {
              "label": "Newest per customer", "componentId": "xf.dedup",
              "properties": {"keys": ["customer_id"], "order_by": "order_ts DESC"}}},
            {"id": "by_status", "position": {"x": 2, "y": 0}, "data": {
              "label": "By status", "componentId": "xf.pivot",
              "properties": {"on": ["status"],
                             "values": ["shipped", "pending", "returned", "cancelled"],
                             "using": "sum(amount)",
                             "group_by": ["customer_id"]}}},
            {"id": "sink", "position": {"x": 3, "y": 0}, "data": {
              "label": "Matrix", "componentId": "snk.file.parquet",
              "properties": {"path": "{out}/by_status.parquet"}}}
          ],
          "edges": [
            {"id": "e1", "source": "orders", "target": "newest"},
            {"id": "e2", "source": "newest", "target": "by_status"},
            {"id": "e3", "source": "by_status", "target": "sink"}
          ]
        }"#,
        &out,
    );

    let plan = compile(&document).expect("compiles");
    let report = run(&plan, &options()).expect("runs");

    // One row per customer after the dedup, and the pivot keeps that shape.
    let rows: Vec<Option<u64>> = report.stages.iter().map(|s| s.rows).collect();
    assert_eq!(rows, [Some(12), Some(6), Some(6), Some(6)]);
    assert!(out.join("by_status.parquet").is_file());
}

#[test]
fn csv_out_to_json_and_back_in_round_trips() {
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("json_round_trip");

    // Write the orders out as JSON, then read that file back and write it again
    // as Parquet. If the writer and the reader disagree about the shape of a
    // JSON file, the second stage is where it shows.
    let document = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "orders", "position": {"x": 0, "y": 0}, "data": {
              "label": "Orders", "componentId": "src.file.csv",
              "properties": {"path": "samples/data/orders.csv"}}},
            {"id": "as_json", "position": {"x": 1, "y": 0}, "data": {
              "label": "As JSON", "componentId": "snk.file.json",
              "properties": {"path": "{out}/orders.json"}}}
          ],
          "edges": [{"id": "e1", "source": "orders", "target": "as_json"}]
        }"#,
        &out,
    );

    let report = run(&compile(&document).expect("compiles"), &options()).expect("write runs");
    assert_eq!(report.total_rows_written(), Some(12));

    let back = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "json_in", "position": {"x": 0, "y": 0}, "data": {
              "label": "Orders JSON", "componentId": "src.file.json",
              "properties": {"path": "{out}/orders.json"}}},
            {"id": "sink", "position": {"x": 1, "y": 0}, "data": {
              "label": "Parquet", "componentId": "snk.file.parquet",
              "properties": {"path": "{out}/orders.parquet"}}}
          ],
          "edges": [{"id": "e1", "source": "json_in", "target": "sink"}]
        }"#,
        &out,
    );

    let report = run(&compile(&back).expect("compiles"), &options()).expect("read-back runs");

    assert_eq!(
        report.stages.iter().map(|s| s.rows).collect::<Vec<_>>(),
        [Some(12), Some(12)],
        "every row that was written came back"
    );
}

// ---------------------------------------------------------------------------
// Connectors that need an extension
//
// These are the tests the vendored tools/duckdb/extensions/ exists for. They
// exercise the whole prelude — SET extension_directory, LOAD, the probe — as
// well as the connector's own SQL. Excel and SQLite are the two families that
// need no running server, so they stand in for the rest.
// ---------------------------------------------------------------------------

#[test]
fn an_excel_round_trip_loads_the_extension_and_moves_the_rows() {
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("excel_round_trip");

    let write = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "orders", "position": {"x": 0, "y": 0}, "data": {
              "label": "Orders", "componentId": "src.file.csv",
              "properties": {"path": "samples/data/orders.csv"}}},
            {"id": "book", "position": {"x": 1, "y": 0}, "data": {
              "label": "Workbook", "componentId": "snk.file.excel",
              "properties": {"path": "{out}/orders.xlsx", "sheet": "Orders"}}}
          ],
          "edges": [{"id": "e1", "source": "orders", "target": "book"}]
        }"#,
        &out,
    );

    let plan = compile(&write).expect("compiles");
    assert_eq!(
        plan.extensions(),
        ["excel"],
        "the sink declares its extension"
    );

    let report = run(&plan, &options()).expect("excel write runs");
    assert_eq!(report.total_rows_written(), Some(12));
    assert!(
        report.script.contains("LOAD excel;"),
        "the prelude should load excel:\n{}",
        report.script
    );

    let read_back = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "book", "position": {"x": 0, "y": 0}, "data": {
              "label": "Workbook", "componentId": "src.file.excel",
              "properties": {"path": "{out}/orders.xlsx", "sheet": "Orders"}}},
            {"id": "sink", "position": {"x": 1, "y": 0}, "data": {
              "label": "Parquet", "componentId": "snk.file.parquet",
              "properties": {"path": "{out}/from_excel.parquet"}}}
          ],
          "edges": [{"id": "e1", "source": "book", "target": "sink"}]
        }"#,
        &out,
    );

    let report = run(&compile(&read_back).expect("compiles"), &options()).expect("excel read runs");

    assert_eq!(
        report.stages.iter().map(|s| s.rows).collect::<Vec<_>>(),
        [Some(12), Some(12)],
        "every row written to the workbook came back"
    );
}

#[test]
fn a_sqlite_round_trip_attaches_writes_and_reads_back() {
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("sqlite_round_trip");

    // A database sink has a connection string rather than a path, so nothing
    // creates the directory for it the way it would for a file sink.
    std::fs::create_dir_all(&out).expect("output directory");

    let write = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "orders", "position": {"x": 0, "y": 0}, "data": {
              "label": "Orders", "componentId": "src.file.csv",
              "properties": {"path": "samples/data/orders.csv"}}},
            {"id": "shipped", "position": {"x": 1, "y": 0}, "data": {
              "label": "Shipped", "componentId": "xf.filter",
              "properties": {"predicate": "status = 'shipped'"}}},
            {"id": "db", "position": {"x": 2, "y": 0}, "data": {
              "label": "SQLite", "componentId": "snk.db.sqlite",
              "properties": {"connection": "{out}/orders.db", "table": "shipped_orders"}}}
          ],
          "edges": [
            {"id": "e1", "source": "orders", "target": "shipped"},
            {"id": "e2", "source": "shipped", "target": "db"}
          ]
        }"#,
        &out,
    );

    let plan = compile(&write).expect("compiles");
    assert_eq!(plan.extensions(), ["sqlite"]);

    let report = run(&plan, &options()).expect("sqlite write runs");
    assert_eq!(
        report.stages.iter().map(|s| s.rows).collect::<Vec<_>>(),
        [Some(12), Some(7), Some(7)]
    );
    assert!(
        out.join("orders.db").is_file(),
        "no database file was created"
    );

    // Read it back through the source component and append into a *second*
    // table, so both write modes are exercised.
    //
    // Deliberately not the same table: every stage is a lazy view, so a
    // pipeline that appends to the table it reads from would re-read its own
    // writes when the count probe evaluated the view, and report 14.
    let read_back = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "db", "position": {"x": 0, "y": 0}, "data": {
              "label": "SQLite", "componentId": "src.db.sqlite",
              "properties": {"connection": "{out}/orders.db", "table": "shipped_orders"}}},
            {"id": "again", "position": {"x": 1, "y": 0}, "data": {
              "label": "Append", "componentId": "snk.db.sqlite",
              "properties": {"connection": "{out}/orders.db", "table": "archive",
                             "mode": "append"}}}
          ],
          "edges": [{"id": "e1", "source": "db", "target": "again"}]
        }"#,
        &out,
    );

    let report =
        run(&compile(&read_back).expect("compiles"), &options()).expect("sqlite read runs");

    assert_eq!(
        report.stages.iter().map(|s| s.rows).collect::<Vec<_>>(),
        [Some(7), Some(7)],
        "the seven rows written came back out"
    );

    assert!(
        out.join("orders.db").is_file(),
        "the database should still be there after the append"
    );
}

#[test]
fn a_missing_extension_is_reported_as_a_missing_extension() {
    // Point the executor at an empty extension directory. The prelude then
    // fails before any stage runs, and the error must say so rather than
    // blaming the first stage, which has done nothing wrong.
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("missing_extension");
    std::fs::create_dir_all(&out).expect("output directory");

    let document = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "orders", "position": {"x": 0, "y": 0}, "data": {
              "label": "Orders", "componentId": "src.file.csv",
              "properties": {"path": "samples/data/orders.csv"}}},
            {"id": "book", "position": {"x": 1, "y": 0}, "data": {
              "label": "Workbook", "componentId": "snk.file.excel",
              "properties": {"path": "{out}/orders.xlsx"}}}
          ],
          "edges": [{"id": "e1", "source": "orders", "target": "book"}]
        }"#,
        &out,
    );

    let empty = out.join("no-extensions-here");
    std::fs::create_dir_all(&empty).expect("empty extension directory");

    let error = run(
        &compile(&document).expect("compiles"),
        &RunOptions {
            extension_dir: Some(empty),
            ..options()
        },
    )
    .expect_err("an empty extension directory should fail the run");

    assert!(
        matches!(error, ExecError::ExtensionLoadFailed { .. }),
        "expected ExtensionLoadFailed, got: {error}"
    );
    assert!(
        error.to_string().contains("excel"),
        "the message should name the extension: {error}"
    );
}

// ---------------------------------------------------------------------------
// Parameters and contexts
//
// Phase 5's goal is that one unedited document runs in more than one place.
// These run the committed sample the way the CLI does — resolve, then compile,
// then execute — because resolution is a separate step and the seam between it
// and compilation is where a mistake would hide.
// ---------------------------------------------------------------------------

/// The committed sample that takes its output directory from a context.
fn by_context() -> PipelineDoc {
    let text =
        std::fs::read_to_string(repo_root().join("samples/pipelines/orders_by_context.json"))
            .expect("sample pipeline is committed");

    PipelineDoc::from_json(&text).expect("sample parses")
}

/// A context whose only variable is where to write.
fn writing_to(directory: &Path) -> etl_duckdb_engine::Context {
    etl_duckdb_engine::Context {
        description: None,
        variables: [(
            "out_dir".to_string(),
            directory.to_string_lossy().replace('\\', "/"),
        )]
        .into(),
        extra: Default::default(),
    }
}

#[test]
fn the_committed_contexts_sample_still_defines_the_two_it_documents() {
    // The sample file is what someone copies to make their own; if it stops
    // parsing or loses a context, the documentation around it is wrong.
    let contexts = Contexts::load(&repo_root().join("samples/contexts.json")).expect("loads");

    assert_eq!(contexts.names(), ["dev", "prod"]);
    assert_eq!(contexts.active.as_deref(), Some("dev"));
    assert!(contexts.contexts["dev"].variables.contains_key("out_dir"));
    assert!(contexts.contexts["prod"].variables.contains_key("out_dir"));
}

#[test]
fn the_same_document_runs_in_two_contexts_and_lands_in_two_places() {
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("two_contexts");
    let document = by_context();

    // Two contexts differing in one variable, pointing at this test's own
    // directories rather than the sample's, so nothing is shared.
    let contexts = Contexts {
        format_version: 1,
        active: Some("dev".to_string()),
        contexts: [
            ("dev".to_string(), writing_to(&out.join("dev"))),
            ("prod".to_string(), writing_to(&out.join("prod"))),
        ]
        .into(),
        extra: Default::default(),
    };

    let mut counts = Vec::new();

    for name in ["dev", "prod"] {
        let resolver = contexts
            .apply(Resolver::new(repo_root()), Some(name))
            .expect("context applies");

        let resolved = etl_duckdb_engine::resolve(&document, &resolver).expect("resolves");
        let plan = compile(&resolved.document).expect("compiles");
        let report = run(&plan, &options()).expect("runs");

        counts.push(report.stages.iter().map(|s| s.rows).collect::<Vec<_>>());
    }

    // Identical work, both times: that is what "the same pipeline" means.
    assert_eq!(counts[0], counts[1], "the two contexts did different work");
    assert_eq!(counts[0], [Some(12), Some(6), Some(6)]);

    // In two different places: that is what the context changed.
    assert!(out.join("dev").join("large_orders.parquet").is_file());
    assert!(out.join("prod").join("large_orders.parquet").is_file());
}

#[test]
fn the_workspace_built_in_makes_a_pipeline_portable() {
    // `${workspace}` is what lets the sample name its input without hard-coding
    // the checkout directory. Resolve it against the repo root and the source
    // path must come out absolute and real.
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("workspace_builtin");
    let resolver =
        Resolver::new(repo_root()).bind("out_dir", &out.to_string_lossy().replace('\\', "/"));

    let resolved = etl_duckdb_engine::resolve(&by_context(), &resolver).expect("resolves");
    let source = resolved.document.nodes[0].data.properties.as_ref().unwrap()["path"]
        .as_str()
        .unwrap()
        .to_string();

    assert!(
        !source.contains("${"),
        "nothing should be left to expand: {source}"
    );
    assert!(Path::new(&source).is_file(), "{source} should exist");

    let plan = compile(&resolved.document).expect("compiles");
    assert_eq!(
        run(&plan, &options()).expect("runs").total_rows_written(),
        Some(6)
    );
}

#[test]
fn a_parameter_supplied_on_the_command_line_changes_the_run() {
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("param_override");
    let document = by_context();

    let resolver = |floor: &str| {
        Resolver::new(repo_root())
            .bind("out_dir", &out.to_string_lossy().replace('\\', "/"))
            .bind("floor", floor)
    };

    // The declared default is 100, which keeps 6 of the 12 orders.
    let rows = |floor: &str| {
        let resolved = etl_duckdb_engine::resolve(&document, &resolver(floor)).expect("resolves");
        let plan = compile(&resolved.document).expect("compiles");

        run(&plan, &options()).expect("runs").total_rows_written()
    };

    assert_eq!(rows("0"), Some(12), "nothing is filtered out at zero");
    assert_eq!(rows("300"), Some(3));
}

#[test]
fn the_parameterised_sample_from_phase_two_finally_runs() {
    // `samples/pipelines/csv_to_parquet.json` has been unrunnable since Phase 2
    // because it uses ${workspace} and ${since}. This is the test that says it
    // is not any more.
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("csv_to_parquet_params");
    let text = std::fs::read_to_string(repo_root().join("samples/pipelines/csv_to_parquet.json"))
        .expect("sample is committed");

    let mut document = PipelineDoc::from_json(&text).expect("sample parses");

    // Redirect the sink into this test's directory; everything else, including
    // both ${...} references, is left exactly as committed.
    for node in &mut document.nodes {
        if node.data.component_id.as_deref() == Some("snk.file.parquet") {
            node.data.properties.as_mut().unwrap()["path"] = serde_json::json!(format!(
                "{}/orders.parquet",
                out.to_string_lossy().replace('\\', "/")
            ));
        }
    }

    let resolved =
        etl_duckdb_engine::resolve(&document, &Resolver::new(repo_root())).expect("resolves");
    let report = run(&compile(&resolved.document).expect("compiles"), &options()).expect("runs");

    // The declared default of 2026-01-01 keeps 7 of the 12 orders.
    assert_eq!(
        report.stages.iter().map(|s| s.rows).collect::<Vec<_>>(),
        [Some(12), Some(7), Some(7)]
    );
    assert_eq!(resolved.used["since"], "2026-01-01");
}

#[test]
fn an_unresolved_parameter_is_caught_before_anything_touches_the_disk() {
    // No skip guard: resolution never spawns DuckDB, which is the property
    // being relied on here.
    let error = etl_duckdb_engine::resolve(&by_context(), &Resolver::new(repo_root()))
        .expect_err("out_dir is required and nothing provides it");

    assert!(
        matches!(&error, etl_duckdb_engine::ParamError::MissingRequired { name } if name == "out_dir"),
        "{error:?}"
    );
}

// ---------------------------------------------------------------------------
// Materialisation, against real data
//
// The golden tests pin the statement shapes. These prove the three modes agree
// on the answer, which is the property that matters: materialising is a choice
// about how the work is done, never about what comes out.
// ---------------------------------------------------------------------------

/// The orders pipeline with its middle stage set to one materialisation mode.
fn filtered_with_mode(mode: &str, out: &Path) -> PipelineDoc {
    let json = r#"{
      "formatVersion": 1,
      "nodes": [
        {"id": "orders", "position": {"x": 0, "y": 0}, "data": {
          "label": "Orders", "componentId": "src.file.csv",
          "properties": {"path": "samples/data/orders.csv"}}},
        {"id": "shipped", "position": {"x": 1, "y": 0}, "data": {
          "label": "Shipped", "componentId": "xf.filter",
          "materialize": "MODE",
          "properties": {"predicate": "status = 'shipped'"}}},
        {"id": "sink", "position": {"x": 2, "y": 0}, "data": {
          "label": "Out", "componentId": "snk.file.parquet",
          "properties": {"path": "{out}/MODE.parquet"}}}
      ],
      "edges": [
        {"id": "e1", "source": "orders", "target": "shipped"},
        {"id": "e2", "source": "shipped", "target": "sink"}
      ]
    }"#
    .replace("MODE", mode);

    document_with_out(&json, out)
}

#[test]
fn every_materialisation_mode_gives_the_same_answer() {
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("materialize_modes");

    for mode in ["auto", "view", "memory", "disk"] {
        let plan = compile(&filtered_with_mode(mode, &out)).expect("compiles");
        let report = run(&plan, &options()).expect("runs");

        assert_eq!(
            report.stages.iter().map(|s| s.rows).collect::<Vec<_>>(),
            [Some(12), Some(7), Some(7)],
            "mode {mode} disagreed"
        );
        assert!(out.join(format!("{mode}.parquet")).is_file(), "mode {mode}");
    }
}

#[test]
fn a_disk_stage_writes_a_spill_reads_it_back_and_clears_it_up() {
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("materialize_spill");
    let plan = compile(&filtered_with_mode("disk", &out)).expect("compiles");

    assert_eq!(plan.spills(), [".etl/tmp/shipped.parquet"]);

    // The spill is relative to the working directory, which is the repo root.
    let spill = repo_root().join(".etl/tmp/shipped.parquet");
    let _ = std::fs::remove_file(&spill);

    let report = run(&plan, &options()).expect("runs");

    // The rows came back through the Parquet file, not out of a lazy view.
    assert_eq!(
        report.stages.iter().map(|s| s.rows).collect::<Vec<_>>(),
        [Some(12), Some(7), Some(7)]
    );
    assert!(
        report
            .script
            .contains("COPY (SELECT * FROM \"orders\" WHERE status = 'shipped') TO"),
        "the spill should be written by a COPY:\n{}",
        report.script
    );

    assert_eq!(report.spilled, 1, "one spill file should have been cleared");
    assert!(
        !spill.exists(),
        "the spill file should not be left behind: {}",
        spill.display()
    );
}

#[test]
fn a_view_stage_leaves_no_spill_behind_to_clear() {
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("materialize_no_spill");
    let plan = compile(&filtered_with_mode("view", &out)).expect("compiles");

    assert!(plan.spills().is_empty());
    assert_eq!(run(&plan, &options()).expect("runs").spilled, 0);
}

// ---------------------------------------------------------------------------
// Secrets, against real data
//
// The property being tested is the awkward one: the true value has to reach
// DuckDB, because DuckDB needs the real password, while never reaching
// anything a person reads. Proving both halves at once needs a real run.
// ---------------------------------------------------------------------------

#[test]
fn a_secret_reaches_duckdb_but_not_the_report() {
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("secret_redaction");
    let workspace = out.join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");

    let mut store = etl_secrets::SecretStore::open(&workspace).expect("opens");
    store
        .set("target_status", "shipped", None)
        .expect("encrypts");

    let document = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "orders", "position": {"x": 0, "y": 0}, "data": {
              "label": "Orders", "componentId": "src.file.csv",
              "properties": {"path": "samples/data/orders.csv"}}},
            {"id": "picked", "position": {"x": 1, "y": 0}, "data": {
              "label": "Picked", "componentId": "xf.filter",
              "properties": {"predicate": "status = '${SECRET:target_status}'"}}},
            {"id": "sink", "position": {"x": 2, "y": 0}, "data": {
              "label": "Out", "componentId": "snk.file.parquet",
              "properties": {"path": "{out}/picked.parquet"}}}
          ],
          "edges": [
            {"id": "e1", "source": "orders", "target": "picked"},
            {"id": "e2", "source": "picked", "target": "sink"}
          ]
        }"#,
        &out,
    );

    let resolver = Resolver::new(repo_root()).secrets(store);
    let resolved = etl_duckdb_engine::resolve(&document, &resolver).expect("resolves");
    let plan = compile(&resolved.document).expect("compiles");

    let report = run(
        &plan,
        &RunOptions {
            redact: resolved.secret_values(),
            ..options()
        },
    )
    .expect("runs");

    // The real value reached DuckDB: seven orders have status 'shipped', and
    // no other value would give that number.
    assert_eq!(
        report.stages.iter().map(|s| s.rows).collect::<Vec<_>>(),
        [Some(12), Some(7), Some(7)]
    );

    // And it is nowhere in what the report carries.
    assert!(
        !report.script.contains("shipped"),
        "the secret is in the reported script:\n{}",
        report.script
    );
    assert!(
        report.script.contains("status = '********'"),
        "the secret should be masked, not removed:\n{}",
        report.script
    );
}

#[test]
fn a_secret_is_masked_in_duckdbs_own_error_output() {
    // The real leak path: a failed ATTACH quotes the whole connection string
    // back, password and all, and that text goes straight into ExecError.
    let Some(_) = duckdb_binary() else {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    };

    let out = output_dir("secret_error_redaction");
    let workspace = out.join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");

    let mut store = etl_secrets::SecretStore::open(&workspace).expect("opens");
    store
        .set("pg_password", "correct-horse-battery", None)
        .expect("encrypts");

    let document = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "orders", "position": {"x": 0, "y": 0}, "data": {
              "label": "Orders", "componentId": "src.db.postgres",
              "properties": {
                "connection": "dbname=a host=127.0.0.1 port=1 password=${SECRET:pg_password}",
                "table": "orders"}}},
            {"id": "sink", "position": {"x": 1, "y": 0}, "data": {
              "label": "Out", "componentId": "snk.file.parquet",
              "properties": {"path": "{out}/never.parquet"}}}
          ],
          "edges": [{"id": "e1", "source": "orders", "target": "sink"}]
        }"#,
        &out,
    );

    let resolver = Resolver::new(repo_root()).secrets(store);
    let resolved = etl_duckdb_engine::resolve(&document, &resolver).expect("resolves");
    let plan = compile(&resolved.document).expect("compiles");

    let error = run(
        &plan,
        &RunOptions {
            redact: resolved.secret_values(),
            ..options()
        },
    )
    .expect_err("there is no server on port 1");

    let message = error.to_string();

    assert!(
        !message.contains("correct-horse-battery"),
        "the password is in the error:\n{message}"
    );
    assert!(
        message.contains("********"),
        "the error should carry the mask, proving it went through redaction:\n{message}"
    );
}

#[test]
fn a_pipeline_that_needs_a_secret_will_not_run_without_the_key() {
    // Fails closed, and says which secret it wanted rather than reporting an
    // unresolved parameter.
    let out = output_dir("secret_no_key");

    let document = document_with_out(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "orders", "position": {"x": 0, "y": 0}, "data": {
              "label": "Orders", "componentId": "src.file.csv",
              "properties": {"path": "${SECRET:somewhere}/orders.csv"}}}
          ],
          "edges": []
        }"#,
        &out,
    );

    let error = etl_duckdb_engine::resolve(&document, &Resolver::new(repo_root()))
        .expect_err("there is no store");

    assert!(
        matches!(&error, etl_duckdb_engine::ParamError::NoSecretStore { name, .. }
            if name == "somewhere"),
        "{error:?}"
    );
}

// ---------------------------------------------------------------------------
// Quality nodes, against real data (Phase 6a)
//
// The golden-SQL tests prove the two statements are the ones intended. These
// prove DuckDB agrees they are valid, that the numbers come back attributed to
// the right node and the right port, and — the property the whole design rests
// on — that the two sides of a split add up to what went in.
// ---------------------------------------------------------------------------

/// A source feeding one validator, with no sink.
///
/// Sink-less on purpose: the count probes still run, and forcing the two views
/// through them is exactly what this needs to measure. It also keeps the test
/// about the split rather than about writing files.
fn quality_only(component_id: &str, properties: &str) -> PipelineDoc {
    let json = format!(
        r#"{{
          "formatVersion": 1,
          "nodes": [
            {{"id": "orders", "type": "source", "position": {{"x": 0, "y": 0}},
             "data": {{"label": "Orders", "componentId": "src.file.csv",
                      "properties": {{"path": "samples/data/orders.csv"}}}}}},
            {{"id": "check", "type": "transform", "position": {{"x": 200, "y": 0}},
             "data": {{"label": "Check", "componentId": "{component_id}",
                      "properties": {properties}}}}}
          ],
          "edges": [
            {{"id": "e1", "source": "orders", "target": "check",
             "sourceHandle": "main", "targetHandle": "in"}}
          ]
        }}"#
    );

    PipelineDoc::from_json(&json).expect("document parses")
}

#[test]
fn every_validator_splits_its_input_exactly() {
    if duckdb_binary().is_none() {
        eprintln!("skipping: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return;
    }

    // orders.csv holds 12 rows. Whatever each check decides, the two sides must
    // account for all 12 — a row for which the predicate is unknown is rejected,
    // never dropped from both. That is what `coalesce(pred, false)` buys, and
    // this is the test that would fail without it.
    let cases = [
        ("qa.not_null", r#"{"columns": ["customer_id"]}"#, 12, 0),
        ("qa.unique", r#"{"columns": ["order_id"]}"#, 12, 0),
        ("qa.unique", r#"{"columns": ["customer_id"]}"#, 1, 11),
        ("qa.range", r#"{"column": "amount", "min": 100}"#, 6, 6),
        (
            "qa.accepted_values",
            r#"{"column": "status", "values": ["shipped", "pending"]}"#,
            10,
            2,
        ),
        (
            "qa.regex",
            r#"{"column": "customer_id", "pattern": "^C00[1-3]$"}"#,
            7,
            5,
        ),
        ("qa.expression", r#"{"predicate": "amount > 100"}"#, 6, 6),
    ];

    for (component_id, properties, expected_ok, expected_bad) in cases {
        let plan = compile(&quality_only(component_id, properties)).expect("compiles");
        let report = run(&plan, &options()).unwrap_or_else(|e| panic!("{component_id}: {e}"));

        let check = report
            .stages
            .iter()
            .find(|s| s.node_id == "check")
            .expect("the validator is in the report");

        assert_eq!(
            (check.rows, check.rejected),
            (Some(expected_ok), Some(expected_bad)),
            "{component_id} {properties}"
        );

        assert_eq!(
            check.rows_in(),
            Some(12),
            "{component_id}: the two sides must account for every input row"
        );
    }
}

#[test]
fn a_rejected_row_reaches_a_dead_letter_sink_in_the_same_pass() {
    let Some(binary) = duckdb_binary() else {
        return;
    };

    let out = output_dir("quality_reject");

    // Good rows and bad rows leave by different ports, into different files,
    // from one run over the input.
    let json = r#"{
      "formatVersion": 1,
      "nodes": [
        {"id": "orders", "type": "source", "position": {"x": 0, "y": 0},
         "data": {"label": "Orders", "componentId": "src.file.csv",
                  "properties": {"path": "samples/data/orders.csv"}}},
        {"id": "check", "type": "transform", "position": {"x": 200, "y": 0},
         "data": {"label": "Status is known", "componentId": "qa.accepted_values",
                  "properties": {"column": "status", "values": ["shipped", "pending"]}}},
        {"id": "good", "type": "sink", "position": {"x": 400, "y": 0},
         "data": {"label": "Good", "componentId": "snk.file.csv",
                  "properties": {"path": "{out}/good.csv"}}},
        {"id": "bad", "type": "sink", "position": {"x": 400, "y": 120},
         "data": {"label": "Rejected", "componentId": "snk.file.csv",
                  "properties": {"path": "{out}/bad.csv"}}}
      ],
      "edges": [
        {"id": "e1", "source": "orders", "target": "check",
         "sourceHandle": "main", "targetHandle": "in"},
        {"id": "e2", "source": "check", "target": "good",
         "sourceHandle": "main", "targetHandle": "in"},
        {"id": "e3", "source": "check", "target": "bad",
         "sourceHandle": "rejected", "targetHandle": "in"}
      ]
    }"#;

    let plan = compile(&document_with_out(json, &out)).expect("compiles");
    let report = run(&plan, &options()).expect("runs");

    let rows = |id: &str| {
        report
            .stages
            .iter()
            .find(|s| s.node_id == id)
            .and_then(|s| s.rows)
    };

    assert_eq!(rows("orders"), Some(12));
    // 7 shipped + 3 pending accepted; 1 returned + 1 cancelled rejected.
    assert_eq!(
        rows("good"),
        Some(10),
        "the good sink reads the accepted port"
    );
    assert_eq!(rows("bad"), Some(2), "the bad sink reads the rejected port");

    // The files themselves, read back independently of the code under test.
    let read = |name: &str| {
        let path = out.join(name).to_string_lossy().replace('\\', "/");
        query(
            &binary,
            &format!("SELECT count(*) AS n FROM read_csv('{path}');"),
        )
    };

    assert_eq!(read("good.csv"), r#"[{"n":10}]"#);
    assert_eq!(read("bad.csv"), r#"[{"n":2}]"#);

    // And they hold the rows they should, not merely the right number of them.
    let bad_path = out.join("bad.csv").to_string_lossy().replace('\\', "/");
    assert_eq!(
        query(
            &binary,
            &format!(
                "SELECT string_agg(DISTINCT status, ',' ORDER BY status) AS s \
                 FROM read_csv('{bad_path}');"
            )
        ),
        r#"[{"s":"cancelled,returned"}]"#,
        "only the statuses outside the accepted set were rejected"
    );
}

#[test]
fn a_referential_check_finds_the_orphan_against_a_second_input() {
    if duckdb_binary().is_none() {
        return;
    }

    // Order 1010 belongs to C006, which is not in customers.csv — the same
    // orphan the inner join in the happy-path test silently drops. A quality
    // node is how you find out it happened.
    let json = r#"{
      "formatVersion": 1,
      "nodes": [
        {"id": "orders", "type": "source", "position": {"x": 0, "y": 0},
         "data": {"label": "Orders", "componentId": "src.file.csv",
                  "properties": {"path": "samples/data/orders.csv"}}},
        {"id": "customers", "type": "source", "position": {"x": 0, "y": 120},
         "data": {"label": "Customers", "componentId": "src.file.csv",
                  "properties": {"path": "samples/data/customers.csv"}}},
        {"id": "check", "type": "transform", "position": {"x": 200, "y": 0},
         "data": {"label": "Customer exists", "componentId": "qa.referential",
                  "properties": {"column": "customer_id", "reference_column": "customer_id"}}}
      ],
      "edges": [
        {"id": "e1", "source": "orders", "target": "check",
         "sourceHandle": "main", "targetHandle": "left"},
        {"id": "e2", "source": "customers", "target": "check",
         "sourceHandle": "main", "targetHandle": "right"}
      ]
    }"#;

    let plan = compile(&PipelineDoc::from_json(json).expect("parses")).expect("compiles");
    let report = run(&plan, &options()).expect("runs");

    let check = report
        .stages
        .iter()
        .find(|s| s.node_id == "check")
        .expect("in the report");

    assert_eq!((check.rows, check.rejected), (Some(11), Some(1)));
    assert_eq!(check.rows_in(), Some(12));
}

#[test]
fn counts_stay_attributed_correctly_after_a_validator() {
    if duckdb_binary().is_none() {
        return;
    }

    // The regression this whole change risked: a validator emits two counts, so
    // anything reading stdout one-count-per-stage attributes every later number
    // to the wrong node. The filter below keeps a different number of rows than
    // either side of the check, so a shift of one would be visible.
    let json = r#"{
      "formatVersion": 1,
      "nodes": [
        {"id": "orders", "type": "source", "position": {"x": 0, "y": 0},
         "data": {"label": "Orders", "componentId": "src.file.csv",
                  "properties": {"path": "samples/data/orders.csv"}}},
        {"id": "check", "type": "transform", "position": {"x": 200, "y": 0},
         "data": {"label": "Check", "componentId": "qa.range",
                  "properties": {"column": "amount", "min": 100}}},
        {"id": "big", "type": "transform", "position": {"x": 400, "y": 0},
         "data": {"label": "Very large", "componentId": "xf.filter",
                  "properties": {"predicate": "amount > 400"}}}
      ],
      "edges": [
        {"id": "e1", "source": "orders", "target": "check",
         "sourceHandle": "main", "targetHandle": "in"},
        {"id": "e2", "source": "check", "target": "big",
         "sourceHandle": "main", "targetHandle": "in"}
      ]
    }"#;

    let plan = compile(&PipelineDoc::from_json(json).expect("parses")).expect("compiles");
    let report = run(&plan, &options()).expect("runs");

    let outcome: Vec<(&str, Option<u64>, Option<u64>)> = report
        .stages
        .iter()
        .map(|s| (s.node_id.as_str(), s.rows, s.rejected))
        .collect();

    // 12 in; 6 at or above 100 and 6 below; of those 6, two are over 400.
    assert_eq!(
        outcome,
        [
            ("orders", Some(12), None),
            ("check", Some(6), Some(6)),
            ("big", Some(2), None),
        ]
    );
}

// ---------------------------------------------------------------------------
// Control flow and per-stage policy (Phase 6b)
//
// These all take the session path rather than the one-script path, which is the
// thing worth checking as much as the features themselves: the two transports
// must agree about everything except how the statements get there.
// ---------------------------------------------------------------------------

/// `orders` feeding one node, with whatever properties and policy are given.
fn guarded(component_id: &str, properties: &str, policy: &str) -> PipelineDoc {
    let policy = if policy.is_empty() {
        String::new()
    } else {
        format!(r#", "policy": {policy}"#)
    };

    let json = format!(
        r#"{{
          "formatVersion": 1,
          "nodes": [
            {{"id": "orders", "type": "source", "position": {{"x": 0, "y": 0}},
             "data": {{"label": "Orders", "componentId": "src.file.csv",
                      "properties": {{"path": "samples/data/orders.csv"}}}}}},
            {{"id": "gate", "type": "transform", "position": {{"x": 200, "y": 0}},
             "data": {{"label": "Gate", "componentId": "{component_id}",
                      "properties": {properties}{policy}}}}},
            {{"id": "after", "type": "transform", "position": {{"x": 400, "y": 0}},
             "data": {{"label": "After", "componentId": "xf.filter",
                      "properties": {{"predicate": "amount > 100"}}}}}}
          ],
          "edges": [
            {{"id": "e1", "source": "orders", "target": "gate",
             "sourceHandle": "main", "targetHandle": "in"}},
            {{"id": "e2", "source": "gate", "target": "after",
             "sourceHandle": "main", "targetHandle": "in"}}
          ]
        }}"#
    );

    PipelineDoc::from_json(&json).expect("document parses")
}

fn stage<'a>(report: &'a RunReport, node_id: &str) -> &'a etl_duckdb_engine::StageOutcome {
    report
        .stages
        .iter()
        .find(|s| s.node_id == node_id)
        .unwrap_or_else(|| panic!("{node_id} is in the report"))
}

#[test]
fn a_plan_earns_a_session_rather_than_always_getting_one() {
    // The dual path, asserted rather than assumed. A plan with no control node
    // and no policy must keep the transport all 47 components were built
    // against; adding either is what switches it.
    let plain = compile(&guarded("xf.distinct", "{}", "")).expect("compiles");
    assert!(
        !plain.needs_session(),
        "an ordinary plan stays on one script"
    );

    let with_control = compile(&guarded("ctl.log", r#"{"message": "hello"}"#, "")).expect("ok");
    assert!(with_control.needs_session());
    assert_eq!(with_control.session_reasons(), ["gate"]);

    let with_policy = compile(&guarded(
        "xf.distinct",
        "{}",
        r#"{"continueOnFailure": true}"#,
    ))
    .expect("ok");
    assert!(with_policy.needs_session(), "a policy needs one too");

    // An all-default policy says nothing, so it must not tip the plan over.
    let empty_policy = compile(&guarded("xf.distinct", "{}", "{}")).expect("ok");
    assert!(!empty_policy.needs_session());
}

#[test]
fn a_control_node_passes_its_rows_through_unchanged() {
    if duckdb_binary().is_none() {
        return;
    }

    // A ctl.log in the middle of a chain must not break the chain. Before 6b
    // control nodes produced no relation at all, which would have made them
    // unusable exactly where anyone would put one.
    let plan = compile(&guarded("ctl.log", r#"{"message": "went past"}"#, "")).expect("compiles");
    let report = run(&plan, &options()).expect("runs");

    assert_eq!(stage(&report, "orders").rows, Some(12));
    assert_eq!(stage(&report, "gate").rows, Some(12), "unchanged");
    assert_eq!(stage(&report, "after").rows, Some(6), "and still flows on");

    assert!(
        report.notes.iter().any(|n| n.contains("went past")),
        "the message is reported: {:?}",
        report.notes
    );
}

#[test]
fn a_branch_that_is_not_taken_skips_what_follows() {
    if duckdb_binary().is_none() {
        return;
    }

    let plan = compile(&guarded(
        "ctl.branch",
        r#"{"predicate": "amount > 99999", "message": "nothing that large"}"#,
        "",
    ))
    .expect("compiles");

    let report = run(&plan, &options()).expect("a branch not taken is not a failure");

    assert_eq!(stage(&report, "gate").rows, Some(12));
    assert_eq!(
        stage(&report, "after").skipped,
        Some(SkipReason::NotTaken {
            node_id: "gate".to_string()
        })
    );
    assert!(
        !report.failed(),
        "the pipeline said this might not run, and it did not"
    );
}

#[test]
fn a_branch_that_is_taken_lets_what_follows_run() {
    if duckdb_binary().is_none() {
        return;
    }

    let plan = compile(&guarded(
        "ctl.branch",
        r#"{"predicate": "amount > 400"}"#,
        "",
    ))
    .expect("compiles");

    let report = run(&plan, &options()).expect("runs");

    assert_eq!(stage(&report, "after").rows, Some(6));
    assert_eq!(stage(&report, "after").skipped, None);
}

#[test]
fn a_row_count_assertion_fails_the_run_by_name() {
    if duckdb_binary().is_none() {
        return;
    }

    let plan = compile(&guarded(
        "qa.row_count",
        r#"{"min": 999, "message": "not enough orders"}"#,
        "",
    ))
    .expect("compiles");

    match run(&plan, &options()) {
        Err(ExecError::StageFailed {
            node_id, message, ..
        }) => {
            assert_eq!(node_id, "gate");
            assert!(message.contains("not enough orders"), "{message}");
        }
        other => panic!("expected the assertion to stop the run, got {other:?}"),
    }
}

#[test]
fn a_row_count_assertion_that_holds_lets_the_run_through() {
    if duckdb_binary().is_none() {
        return;
    }

    let plan = compile(&guarded("qa.row_count", r#"{"min": 1, "max": 100}"#, "")).expect("ok");
    let report = run(&plan, &options()).expect("runs");

    assert_eq!(stage(&report, "gate").rows, Some(12));
    assert_eq!(stage(&report, "after").rows, Some(6));
}

#[test]
fn a_missing_column_is_named_by_the_schema_assertion() {
    if duckdb_binary().is_none() {
        return;
    }

    let plan = compile(&guarded(
        "qa.schema_match",
        r#"{"columns": ["order_id", "nope"], "message": "the file changed shape"}"#,
        "",
    ))
    .expect("compiles");

    match run(&plan, &options()) {
        Err(ExecError::StageFailed { message, .. }) => {
            // Both halves: the node's own message says why the check is there,
            // DuckDB's says which column is missing.
            assert!(message.contains("the file changed shape"), "{message}");
            assert!(message.contains("nope"), "{message}");
        }
        other => panic!("expected a failure naming the column, got {other:?}"),
    }
}

#[test]
fn ctl_fail_stops_the_run_only_when_its_condition_holds() {
    if duckdb_binary().is_none() {
        return;
    }

    // No row is negative, so this one passes straight through.
    let quiet = compile(&guarded(
        "ctl.fail",
        r#"{"when": "amount < 0", "message": "negative amounts found"}"#,
        "",
    ))
    .expect("compiles");

    let report = run(&quiet, &options()).expect("nothing matched, so nothing failed");
    assert_eq!(stage(&report, "after").rows, Some(6));

    // Three orders are pending, so this one stops the run.
    let loud = compile(&guarded(
        "ctl.fail",
        r#"{"when": "status = 'pending'", "message": "pending orders found"}"#,
        "",
    ))
    .expect("compiles");

    match run(&loud, &options()) {
        Err(ExecError::StageFailed { message, .. }) => {
            assert!(message.contains("pending orders found"), "{message}");
            assert!(message.contains('3'), "it says how many matched: {message}");
        }
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[test]
fn a_wait_holds_for_as_long_as_it_says() {
    if duckdb_binary().is_none() {
        return;
    }

    let plan = compile(&guarded("ctl.wait", r#"{"ms": 300}"#, "")).expect("compiles");
    let report = run(&plan, &options()).expect("runs");

    assert!(
        report.elapsed >= std::time::Duration::from_millis(300),
        "the run took {:?}, which is less than it was told to wait",
        report.elapsed
    );
    assert_eq!(stage(&report, "after").rows, Some(6), "and rows still flow");
}

#[test]
fn continue_on_failure_runs_the_rest_and_still_fails() {
    let Some(_) = duckdb_binary() else {
        return;
    };

    // `broken` fails; `downstream` reads it and cannot run; `independent` has
    // nothing to do with either and must still run. The run ends failed.
    let json = r#"{
      "formatVersion": 1,
      "nodes": [
        {"id": "orders", "type": "source", "position": {"x": 0, "y": 0},
         "data": {"label": "Orders", "componentId": "src.file.csv",
                  "properties": {"path": "samples/data/orders.csv"}}},
        {"id": "broken", "type": "transform", "position": {"x": 200, "y": 0},
         "data": {"label": "Broken", "componentId": "xf.sql",
                  "properties": {"query": "SELECT * FROM does_not_exist"},
                  "policy": {"continueOnFailure": true}}},
        {"id": "downstream", "type": "transform", "position": {"x": 400, "y": 0},
         "data": {"label": "Downstream", "componentId": "xf.distinct", "properties": {}}},
        {"id": "independent", "type": "transform", "position": {"x": 200, "y": 150},
         "data": {"label": "Independent", "componentId": "xf.filter",
                  "properties": {"predicate": "amount > 100"}}}
      ],
      "edges": [
        {"id": "e1", "source": "orders", "target": "broken",
         "sourceHandle": "main", "targetHandle": "in"},
        {"id": "e2", "source": "broken", "target": "downstream",
         "sourceHandle": "main", "targetHandle": "in"},
        {"id": "e3", "source": "orders", "target": "independent",
         "sourceHandle": "main", "targetHandle": "in"}
      ]
    }"#;

    let plan = compile(&PipelineDoc::from_json(json).expect("parses")).expect("compiles");

    // A report, not an error: the whole point of carrying on is to be able to
    // see what happened, and an error would throw that away.
    let report = run(&plan, &options()).expect("the run reaches the end");

    assert!(report.failed(), "reaching the end is not succeeding");
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].node_id, "broken");

    // The real cause, not the count probe complaining that "broken" is missing.
    assert!(
        report.failures[0].message.contains("does_not_exist"),
        "the message names the actual cause: {}",
        report.failures[0].message
    );

    assert_eq!(stage(&report, "broken").skipped, Some(SkipReason::Failed));
    assert_eq!(
        stage(&report, "downstream").skipped,
        Some(SkipReason::UpstreamFailed {
            node_id: "broken".to_string()
        })
    );
    assert_eq!(
        stage(&report, "independent").rows,
        Some(6),
        "a branch with nothing to do with the failure still ran"
    );
}

#[test]
fn without_continue_on_failure_the_run_stops_at_the_first_failure() {
    if duckdb_binary().is_none() {
        return;
    }

    // The same pipeline, minus the policy — but with a retry policy so it still
    // takes the session path. Fail-fast must mean the same thing on both.
    let json = r#"{
      "formatVersion": 1,
      "nodes": [
        {"id": "orders", "type": "source", "position": {"x": 0, "y": 0},
         "data": {"label": "Orders", "componentId": "src.file.csv",
                  "properties": {"path": "samples/data/orders.csv"}}},
        {"id": "broken", "type": "transform", "position": {"x": 200, "y": 0},
         "data": {"label": "Broken", "componentId": "xf.sql",
                  "properties": {"query": "SELECT * FROM does_not_exist"},
                  "policy": {"retryAttempts": 1, "retryBackoffMs": 1}}}
      ],
      "edges": [
        {"id": "e1", "source": "orders", "target": "broken",
         "sourceHandle": "main", "targetHandle": "in"}
      ]
    }"#;

    let plan = compile(&PipelineDoc::from_json(json).expect("parses")).expect("compiles");

    match run(&plan, &options()) {
        Err(ExecError::StageFailed { node_id, .. }) => assert_eq!(node_id, "broken"),
        other => panic!("expected the run to stop, got {other:?}"),
    }
}

#[test]
fn a_retry_policy_does_not_disturb_a_stage_that_works() {
    if duckdb_binary().is_none() {
        return;
    }

    // Retries are for the stage that fails. One that succeeds must run once and
    // report the same numbers it would without a policy at all.
    let plan = compile(&guarded(
        "xf.distinct",
        "{}",
        r#"{"retryAttempts": 3, "retryBackoffMs": 5000}"#,
    ))
    .expect("compiles");

    assert!(plan.needs_session());

    let report = run(&plan, &options()).expect("runs");

    assert_eq!(stage(&report, "gate").rows, Some(12));
    assert!(
        report.elapsed < std::time::Duration::from_secs(5),
        "a working stage must not have waited on a backoff: {:?}",
        report.elapsed
    );
}

#[test]
fn both_transports_agree_on_the_same_pipeline() {
    if duckdb_binary().is_none() {
        return;
    }

    // The dual path's one real risk: two transports that quietly disagree. The
    // same work, once batched and once driven, must give the same numbers.
    let batched = compile(&guarded("xf.distinct", "{}", "")).expect("compiles");
    let driven = compile(&guarded(
        "xf.distinct",
        "{}",
        r#"{"continueOnFailure": true}"#,
    ))
    .expect("compiles");

    assert!(!batched.needs_session() && driven.needs_session());

    let one = run(&batched, &options()).expect("runs");
    let other = run(&driven, &options()).expect("runs");

    let rows = |report: &RunReport| -> Vec<(String, Option<u64>)> {
        report
            .stages
            .iter()
            .map(|s| (s.node_id.clone(), s.rows))
            .collect()
    };

    assert_eq!(
        rows(&one),
        rows(&other),
        "the transport must not change the answer"
    );
}

// ---------------------------------------------------------------------------
// Per-stage timings
//
// The rule under test is not "does the executor measure time" — it plainly
// does — but which measurements it is willing to publish. A `0 ms` beside the
// transform that cost the most is worse than a blank, so the executor keeps a
// duration only where the stage did its work at the moment it ran.
// ---------------------------------------------------------------------------

#[test]
fn a_waiting_stage_reports_the_time_it_actually_held() {
    if duckdb_binary().is_none() {
        return;
    }

    let document = guarded("ctl.wait", r#"{"ms": 250}"#, "");
    let plan = compile(&document).expect("compiles");
    assert!(plan.needs_session(), "a control node takes the driven path");

    let report = run(&plan, &options()).expect("runs");

    // The wait is the one stage whose duration is unambiguous: it is the whole
    // of what the stage does. Under rather than equal, because a sleep may
    // overshoot and a timer may round, but it can never come back early.
    let waited = stage(&report, "gate").elapsed.expect("a wait is timed");
    assert!(
        waited >= std::time::Duration::from_millis(250),
        "the wait reported {waited:?}, which is less than it was asked to hold"
    );

    // And the lazy stages either side of it say nothing rather than zero.
    assert_eq!(
        stage(&report, "orders").elapsed,
        None,
        "a lazy view has no honest duration to report"
    );
    assert_eq!(stage(&report, "after").elapsed, None);
}

#[test]
fn a_materialised_stage_earns_a_timing_and_a_lazy_one_does_not() {
    if duckdb_binary().is_none() {
        return;
    }

    // `memory` builds its table there and then, so the time it takes is the
    // time its work took. Paired with a policy so the plan takes the driven
    // path at all — timing is a property of the transport as much as the stage.
    let document = guarded("xf.distinct", "{}", r#"{"retryAttempts": 1}"#);
    let mut document = document;
    for node in &mut document.nodes {
        if node.id == "gate" {
            node.data.materialize = Some("memory".to_string());
        }
    }

    let plan = compile(&document).expect("compiles");
    let report = run(&plan, &options()).expect("runs");

    assert!(
        stage(&report, "gate").elapsed.is_some(),
        "a memory-materialised stage does its work when it runs, so it is timed"
    );
    assert_eq!(
        stage(&report, "after").elapsed,
        None,
        "the lazy filter after it is not"
    );
}

#[test]
fn the_one_script_path_publishes_no_per_stage_timings() {
    if duckdb_binary().is_none() {
        return;
    }

    // Nothing here earns a session, so the whole plan goes to DuckDB in one
    // invocation. There is no boundary between stages to measure, and the
    // report must not invent one.
    let plan = compile(&guarded("xf.distinct", "{}", "")).expect("compiles");
    assert!(!plan.needs_session());

    let report = run(&plan, &options()).expect("runs");

    assert!(
        report.stages.iter().all(|stage| stage.elapsed.is_none()),
        "one invocation cannot be attributed to individual stages"
    );
    assert!(
        report.elapsed > std::time::Duration::ZERO,
        "the run as a whole is still timed"
    );
}

// ---------------------------------------------------------------------------
// Incremental loading
//
// The phase's own acceptance line: a watermarked load run twice loads only new
// rows, and a failed run does not advance the watermark. Both are asserted
// against a real CSV that grows between runs, because the failure mode being
// guarded against — silently skipping rows — is invisible in a test that mocks
// the data away.
// ---------------------------------------------------------------------------

/// A CSV of orders, written fresh.
fn write_orders(path: &Path, rows: &[(&str, &str)]) {
    let mut text = String::from("order_id,order_ts\n");
    for (id, ts) in rows {
        text.push_str(&format!("{id},{ts}\n"));
    }

    std::fs::create_dir_all(path.parent().expect("has a parent")).expect("directory");
    std::fs::write(path, text).expect("writes the csv");
}

/// `source -> parquet sink`, with the source watching `order_ts`.
fn incremental_doc(csv: &Path, out: &Path) -> PipelineDoc {
    let json = format!(
        r#"{{
          "formatVersion": 1,
          "name": "incremental_test",
          "nodes": [
            {{"id": "orders", "type": "source", "position": {{"x": 0, "y": 0}},
             "data": {{"label": "Orders", "componentId": "src.file.csv",
                      "properties": {{"path": {path}, "header": true}},
                      "incremental": {{"column": "order_ts"}}}}}},
            {{"id": "out", "type": "sink", "position": {{"x": 200, "y": 0}},
             "data": {{"label": "Out", "componentId": "snk.file.parquet",
                      "properties": {{"path": {out}, "mode": "overwrite"}}}}}}
          ],
          "edges": [
            {{"id": "e1", "source": "orders", "target": "out",
             "sourceHandle": "main", "targetHandle": "in"}}
          ]
        }}"#,
        path = serde_json::to_string(&csv.to_string_lossy()).expect("json"),
        out = serde_json::to_string(&out.to_string_lossy()).expect("json"),
    );

    PipelineDoc::from_json(&json).expect("document parses")
}

/// A plan for `document`, compiled as though `orders` had reached `mark`.
fn compiled_at(document: &PipelineDoc, mark: &str) -> etl_duckdb_engine::Plan {
    let mut watermarks = std::collections::BTreeMap::new();
    watermarks.insert("orders".to_string(), mark.to_string());

    compile_with(document, &CompileOptions { watermarks }).expect("compiles with a watermark")
}

#[test]
fn a_watermarked_load_run_twice_reads_only_what_is_new() {
    if duckdb_binary().is_none() {
        return;
    }

    let directory = output_dir("incremental");
    let csv = directory.join("orders.csv");
    let out = directory.join("out.parquet");

    write_orders(
        &csv,
        &[
            ("1", "2026-01-01 00:00:00"),
            ("2", "2026-01-02 00:00:00"),
            ("3", "2026-01-03 00:00:00"),
        ],
    );

    let document = incremental_doc(&csv, &out);

    // First run: nothing is remembered, so everything is read. A first run
    // that quietly skipped history would be very hard to notice.
    let report = run(&compile(&document).expect("compiles"), &options()).expect("runs");

    assert_eq!(stage(&report, "orders").rows, Some(3));
    assert_eq!(report.watermarks.len(), 1, "one source, one mark");
    assert_eq!(report.watermarks[0].column, "order_ts");

    let mark = report.watermarks[0].value.clone().expect("a mark was read");
    assert_eq!(mark, "2026-01-03 00:00:00");

    // Second run over unchanged data: the mark is already at the top, so there
    // is nothing after it.
    let report = run(&compiled_at(&document, &mark), &options()).expect("runs");

    assert_eq!(
        stage(&report, "orders").rows,
        Some(0),
        "a second run over unchanged data must read nothing"
    );
    assert_eq!(
        report.watermarks[0].value, None,
        "nothing loaded means nothing to move the mark to"
    );

    // Now the source grows. Only the rows after the mark are read, and the row
    // *at* the mark is not re-read -- which is what `>` rather than `>=` buys,
    // and is the difference between a duplicate and a correct load.
    write_orders(
        &csv,
        &[
            ("1", "2026-01-01 00:00:00"),
            ("2", "2026-01-02 00:00:00"),
            ("3", "2026-01-03 00:00:00"),
            ("4", "2026-01-04 00:00:00"),
            ("5", "2026-01-05 00:00:00"),
        ],
    );

    let report = run(&compiled_at(&document, &mark), &options()).expect("runs");

    assert_eq!(
        stage(&report, "orders").rows,
        Some(2),
        "only the two rows after the watermark"
    );
    assert_eq!(
        report.watermarks[0].value.as_deref(),
        Some("2026-01-05 00:00:00"),
        "the new mark is the highest value loaded"
    );

    // And the parquet holds exactly those rows, checked independently of the
    // code under test.
    let binary = duckdb_binary().expect("checked above");
    let written = out.to_string_lossy().replace(MAIN_SEPARATOR, "/");
    let found = query(
        &binary,
        &format!("SELECT count(*) AS n FROM read_parquet('{written}')"),
    );

    assert!(found.contains("2"), "two rows were written: {found}");
}

#[test]
fn a_declared_start_bounds_the_very_first_run() {
    if duckdb_binary().is_none() {
        return;
    }

    let directory = output_dir("incremental-start");
    let csv = directory.join("orders.csv");
    let out = directory.join("out.parquet");

    write_orders(
        &csv,
        &[
            ("1", "2026-01-01 00:00:00"),
            ("2", "2026-02-01 00:00:00"),
            ("3", "2026-03-01 00:00:00"),
        ],
    );

    let mut document = incremental_doc(&csv, &out);
    for node in &mut document.nodes {
        if let Some(incremental) = node.data.incremental.as_mut() {
            incremental.start = Some("2026-01-15 00:00:00".to_string());
        }
    }

    let report = run(&compile(&document).expect("compiles"), &options()).expect("runs");

    assert_eq!(
        stage(&report, "orders").rows,
        Some(2),
        "the declared start bounds the first run, before anything is remembered"
    );
}

#[test]
fn a_failed_run_hands_back_no_state_to_save() {
    if duckdb_binary().is_none() {
        return;
    }

    let directory = output_dir("incremental-failure");
    let csv = directory.join("orders.csv");
    let out = directory.join("out.parquet");

    write_orders(&csv, &[("1", "2026-01-01 00:00:00")]);

    // A watermark column that is not in the data.
    let mut document = incremental_doc(&csv, &out);
    for node in &mut document.nodes {
        if let Some(incremental) = node.data.incremental.as_mut() {
            incremental.column = "no_such_column".to_string();
        }
    }

    let error = run(&compile(&document).expect("compiles"), &options())
        .expect_err("a watermark column that does not exist must fail the run");

    assert!(
        format!("{error}").contains("no_such_column"),
        "the error names the column: {error}"
    );

    // The probe is emitted with the source rather than at the end of the
    // script, so a misconfigured watermark is caught before a sink writes.
    assert!(!out.exists(), "nothing was written");

    // And this is the structural half of "state advances only on success": a
    // failed run returns an error, so there is no report to take watermarks
    // from. Nothing downstream has to remember not to save them.
}

#[test]
fn incremental_outside_a_source_is_dropped_with_a_warning() {
    // Only a source reads from outside the pipeline, so only a source can read
    // part of it. On a transform the predicate would compile and quietly filter
    // a second time, which is the kind of thing that looks like it works.
    let document = PipelineDoc::from_json(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "orders", "type": "source", "position": {"x": 0, "y": 0},
             "data": {"label": "Orders", "componentId": "src.file.csv",
                      "properties": {"path": "samples/data/orders.csv"}}},
            {"id": "big", "type": "transform", "position": {"x": 200, "y": 0},
             "data": {"label": "Big", "componentId": "xf.filter",
                      "properties": {"predicate": "amount > 100"},
                      "incremental": {"column": "order_ts"}}}
          ],
          "edges": [
            {"id": "e1", "source": "orders", "target": "big",
             "sourceHandle": "main", "targetHandle": "in"}
          ]
        }"#,
    )
    .expect("parses");

    let plan = compile(&document).expect("compiles anyway");

    assert!(
        !plan.is_incremental(),
        "the declaration is dropped rather than honoured somewhere it does not apply"
    );
    assert!(
        plan.warnings.iter().any(|warning| matches!(
            warning,
            etl_duckdb_engine::Warning::IncrementalIgnored { id, .. } if id == "big"
        )),
        "and the drop is said out loud: {:?}",
        plan.warnings
    );
}
