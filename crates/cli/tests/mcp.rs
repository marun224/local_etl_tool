//! `etl mcp` as an agent meets it: a subprocess on stdin and stdout, driven by
//! `rmcp`'s own client. The tools' logic is tested in `crates/mcp`; this is the
//! real workspace behind them, the engine, history and secrets included.

use rmcp::model::CallToolRequestParams;
use rmcp::service::RunningService;
use rmcp::transport::TokioChildProcess;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{json, Value as JsonValue};
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/cli sits two levels under the root")
        .to_path_buf()
}

/// A fresh workspace under `target/`, where the vendored DuckDB is found by
/// walking up, holding a copy of the sample orders.
fn workspace(name: &str) -> PathBuf {
    let root = repo_root().join("target").join("test-out").join(name);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("data")).unwrap();
    std::fs::copy(
        repo_root().join("samples/data/orders.csv"),
        root.join("data/orders.csv"),
    )
    .unwrap();
    root
}

fn has_duckdb() -> bool {
    repo_root().join("tools/duckdb").is_dir() || std::env::var_os("ETL_DUCKDB_BIN").is_some()
}

type Client = RunningService<RoleClient, ()>;

async fn connect(root: &Path) -> Client {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_etl"));
    command.arg("mcp").arg("--workspace").arg(root);
    ().serve(TokioChildProcess::new(command).unwrap())
        .await
        .unwrap()
}

/// Call a tool: whether it said it failed, and its text.
async fn call(client: &Client, tool: &str, arguments: JsonValue) -> (bool, String) {
    let result = client
        .call_tool(
            CallToolRequestParams::new(tool.to_string())
                .with_arguments(arguments.as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    let text = result.content[0].as_text().unwrap().text.clone();
    (result.is_error == Some(true), text)
}

fn parsed(text: &str) -> JsonValue {
    serde_json::from_str(text).unwrap_or_else(|_| panic!("not JSON: {text}"))
}

/// Read the orders, keep one row per customer, write Parquet.
fn orders_pipeline() -> JsonValue {
    json!({
        "formatVersion": 1,
        "name": "latest_per_customer",
        "nodes": [
            { "id": "read", "type": "source", "position": { "x": 0, "y": 0 },
              "data": { "label": "Orders", "componentId": "src.file.csv",
                        "properties": { "path": "data/orders.csv" } } },
            { "id": "dedup", "type": "transform", "position": { "x": 200, "y": 0 },
              "data": { "label": "One per customer", "componentId": "xf.dedup",
                        "properties": { "keys": ["customer_id"], "order_by": "order_ts DESC" } } },
            { "id": "write", "type": "sink", "position": { "x": 400, "y": 0 },
              "data": { "label": "Latest", "componentId": "snk.file.parquet",
                        "properties": { "path": "out/latest.parquet" } } }
        ],
        "edges": [
            { "id": "e1", "source": "read", "target": "dedup", "sourceHandle": "main", "targetHandle": "in" },
            { "id": "e2", "source": "dedup", "target": "write", "sourceHandle": "main", "targetHandle": "in" }
        ]
    })
}

#[tokio::test]
async fn an_agent_writes_checks_plans_runs_and_reads_a_pipeline() {
    if !has_duckdb() {
        eprintln!("skipped: no DuckDB vendored in tools/duckdb");
        return;
    }
    let root = workspace("mcp_round_trip");
    let client = connect(&root).await;

    let (failed, created) = call(
        &client,
        "create_pipeline",
        json!({ "path": "pipes/latest.json", "document": orders_pipeline() }),
    )
    .await;
    assert!(!failed, "{created}");
    assert_eq!(parsed(&created)["stages"], json!(3));
    assert!(root.join("pipes/latest.json").is_file());

    let (_, listed) = call(&client, "list_pipelines", json!({})).await;
    assert_eq!(parsed(&listed)[0]["name"], json!("latest_per_customer"));

    let (_, plan) = call(
        &client,
        "plan_pipeline",
        json!({ "path": "pipes/latest.json" }),
    )
    .await;
    assert_eq!(parsed(&plan)["stages"][1]["component"], json!("xf.dedup"));

    let (failed, run) = call(
        &client,
        "run_pipeline",
        json!({ "path": "pipes/latest.json" }),
    )
    .await;
    assert!(!failed, "{run}");
    let run = parsed(&run);
    assert_eq!(run["outcome"], json!("succeeded"));
    assert!(root.join("out/latest.parquet").is_file());

    let (_, runs) = call(
        &client,
        "list_runs",
        json!({ "pipeline": "latest_per_customer" }),
    )
    .await;
    assert_eq!(parsed(&runs)[0]["id"], run["id"]);
    let (_, log) = call(&client, "get_run_log", json!({ "id": run["id"] })).await;
    let stages = parsed(&log)["stages"].clone();
    assert_eq!(stages[0]["rows"], json!(12), "{stages}");

    let (_, lineage) = call(
        &client,
        "get_lineage",
        json!({ "path": "pipes/latest.json" }),
    )
    .await;
    assert!(lineage.contains("orders.csv"), "{lineage}");

    // Built into a folder that is not there yet. The runner is built beside
    // `etl` by `cargo test --workspace`; alone, `-p etl-cli` may not have it.
    let (failed, built) = call(
        &client,
        "build_executable",
        json!({ "path": "pipes/latest.json", "out": "dist/latest.exe" }),
    )
    .await;
    if failed {
        assert!(built.contains("no `etl-runner` found"), "{built}");
    } else {
        assert!(root.join("dist/latest.exe").is_file(), "{built}");
        assert_eq!(parsed(&built)["pipeline"], json!("latest_per_customer"));
    }
}

#[tokio::test]
async fn a_pipeline_that_will_not_compile_says_why_and_is_not_written() {
    let root = workspace("mcp_invalid");
    let client = connect(&root).await;
    let mut broken = orders_pipeline();
    broken["nodes"][1]["data"]["componentId"] = json!("xf.nothing_like_this");

    let (invalid, report) = call(
        &client,
        "create_pipeline",
        json!({ "path": "broken.json", "document": broken }),
    )
    .await;
    let (not_found, why) = call(&client, "run_pipeline", json!({ "path": "missing.json" })).await;

    assert!(invalid);
    let report = parsed(&report);
    assert_eq!(report["valid"], json!(false));
    assert!(
        report["errors"][0]
            .as_str()
            .unwrap()
            .contains("xf.nothing_like_this"),
        "{report}"
    );
    assert!(!root.join("broken.json").exists());
    assert!(not_found && why.contains("missing.json"), "{why}");
}

#[tokio::test]
async fn a_secrets_value_never_reaches_the_agent() {
    let root = workspace("mcp_secrets");
    let etl = env!("CARGO_BIN_EXE_etl");
    let ran = |args: &[&str]| {
        let status = Command::new(etl)
            .args(args)
            .arg("--workspace")
            .arg(&root)
            .output()
            .unwrap();
        assert!(status.status.success(), "{args:?}: {status:?}");
    };
    ran(&["secret", "init"]);
    ran(&[
        "secret",
        "set",
        "pg_password",
        "hunter2-not-for-agents",
        "--description",
        "reader",
    ]);

    let document = json!({
        "formatVersion": 1, "name": "from_postgres",
        "nodes": [
            { "id": "read", "type": "source", "position": { "x": 0, "y": 0 },
              "data": { "label": "Orders", "componentId": "src.db.postgres", "properties": {
                  "connection": "host=127.0.0.1 port=1 user=etl password=${SECRET:pg_password}",
                  "table": "orders" } } },
            { "id": "write", "type": "sink", "position": { "x": 200, "y": 0 },
              "data": { "label": "Copy", "componentId": "snk.file.parquet",
                        "properties": { "path": "out/orders.parquet" } } }
        ],
        "edges": [{ "id": "e1", "source": "read", "target": "write",
                    "sourceHandle": "main", "targetHandle": "in" }]
    });
    let client = connect(&root).await;

    let mut said = Vec::new();
    for (tool, arguments) in [
        ("list_connections", json!({})),
        (
            "create_pipeline",
            json!({ "path": "pg.json", "document": document }),
        ),
        ("validate_pipeline", json!({ "path": "pg.json" })),
        ("plan_pipeline", json!({ "path": "pg.json" })),
        ("get_lineage", json!({ "path": "pg.json" })),
        (
            "build_executable",
            json!({ "path": "pg.json", "out": "dist/pg" }),
        ),
    ] {
        let (failed, text) = call(&client, tool, arguments).await;
        said.push((tool, failed, text));
    }

    for (tool, _, text) in &said {
        assert!(
            !text.contains("hunter2-not-for-agents"),
            "{tool} leaked it: {text}"
        );
    }
    let connections = parsed(&said[0].2);
    assert_eq!(
        connections["secrets"],
        json!([{ "name": "pg_password", "description": "reader",
                 "reference": "${SECRET:pg_password}" }])
    );
    assert!(
        said[3].2.contains("password=********"),
        "the plan masks it: {}",
        said[3].2
    );
    let (_, refused, why) = &said[5];
    assert!(*refused, "a secret is not baked in over MCP");
    assert!(why.contains("etl build --allow-secrets"), "{why}");
    assert!(!root.join("dist").join("pg").exists());
}

#[tokio::test]
async fn the_schema_is_the_registrys_every_component_a_node() {
    let root = workspace("mcp_schema");
    let client = connect(&root).await;

    let (_, schema) = call(&client, "get_schema", json!({})).await;
    let (_, components) = call(&client, "list_components", json!({})).await;

    let schema = parsed(&schema);
    let components = parsed(&components)["components"]
        .as_array()
        .unwrap()
        .clone();
    let nodes = schema["properties"]["nodes"]["items"]["anyOf"]
        .as_array()
        .unwrap();
    assert_eq!(nodes.len(), components.len());
    assert_eq!(
        schema["$defs"]["node.src.db.sqlserver"]["properties"]["data"]["properties"]["componentId"],
        json!({ "const": "src.db.sqlserver" })
    );
}
