//! The tools, driven by `rmcp`'s own client over an in-memory pipe, against a
//! workspace that records what it was asked. `etl`'s real workspace is tested
//! through `etl mcp` itself, in `crates/cli/tests/mcp.rs`.

use super::*;
use rmcp::model::CallToolRequestParams;
use rmcp::service::RunningService;
use rmcp::RoleClient;
use std::sync::Mutex;

/// A workspace that answers from fixed data and writes down each call.
#[derive(Clone)]
struct Fake {
    root: PathBuf,
    calls: Arc<Mutex<Vec<String>>>,
}

impl Fake {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!("etl-mcp-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        Fake {
            root,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn said(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn relative(&self, path: &Path) -> String {
        path.strip_prefix(&self.root)
            .unwrap()
            .display()
            .to_string()
            .replace('\\', "/")
    }
}

impl Workspace for Fake {
    fn root(&self) -> PathBuf {
        self.root.clone()
    }

    fn manifest(&self) -> JsonValue {
        json!({ "formatVersion": 1, "components": [
            { "id": "src.file.csv", "label": "CSV file", "namespace": "source",
              "description": "Read a CSV file.",
              "properties": [{ "name": "path", "type": "path", "required": true },
                             { "name": "header", "type": "bool" }] },
            { "id": "xf.dedup", "label": "Deduplicate", "namespace": "transform",
              "properties": [] },
            { "id": "snk.file.parquet", "label": "Parquet file", "namespace": "sink",
              "properties": [{ "name": "path", "type": "path", "required": true }] }
        ]})
    }

    fn schema(&self) -> JsonValue {
        json!({ "title": "etl pipeline document" })
    }

    fn pipelines(&self) -> Result<JsonValue, String> {
        Ok(json!([{ "name": "orders", "path": "orders.json", "stages": 3 }]))
    }

    fn validate(&self, document: &str, bindings: &Bindings) -> Result<JsonValue, String> {
        self.said(format!("validate {bindings:?}"));
        let document: JsonValue = serde_json::from_str(document).map_err(|e| e.to_string())?;
        Ok(match document["nodes"].as_array() {
            Some(nodes) if !nodes.is_empty() => json!({ "valid": true, "stages": nodes.len() }),
            _ => json!({ "valid": false, "errors": ["a pipeline needs at least one node"] }),
        })
    }

    fn plan(&self, pipeline: &Path, _: &Bindings) -> Result<JsonValue, String> {
        self.said(format!("plan {}", self.relative(pipeline)));
        Ok(json!({ "stages": [] }))
    }

    fn lineage(&self, pipeline: &Path, _: &Bindings) -> Result<JsonValue, String> {
        self.said(format!("lineage {}", self.relative(pipeline)));
        Ok(json!({ "nodes": [] }))
    }

    fn run(&self, pipeline: &Path, bindings: &Bindings) -> Result<JsonValue, String> {
        let name = self.relative(pipeline);
        self.said(format!("run {name} {bindings:?}"));
        Ok(if name.contains("broken") {
            json!({ "id": "r2", "outcome": "failed", "failures": ["read (n1): no such file"] })
        } else {
            json!({ "id": "r1", "outcome": "succeeded" })
        })
    }

    fn runs(&self, pipeline: Option<&str>, limit: usize) -> Result<JsonValue, String> {
        self.said(format!("runs {pipeline:?} {limit}"));
        Ok(json!([]))
    }

    fn run_record(&self, id: &str) -> Result<JsonValue, String> {
        match id {
            "r1" => Ok(json!({ "id": "r1", "outcome": "succeeded", "stages": [] })),
            other => Err(format!("no run called '{other}'")),
        }
    }

    fn build(
        &self,
        pipeline: &Path,
        target: Option<&str>,
        out: &Path,
        _: &Bindings,
    ) -> Result<JsonValue, String> {
        self.said(format!(
            "build {} {target:?} {}",
            self.relative(pipeline),
            self.relative(out)
        ));
        Ok(json!({ "built": self.relative(out) }))
    }

    fn connections(&self) -> Result<JsonValue, String> {
        Ok(json!({ "contexts": [], "secrets": [{ "name": "pg_password" }] }))
    }
}

type Client = RunningService<RoleClient, ()>;

async fn connect(workspace: Fake) -> Client {
    let (client_side, server_side) = tokio::io::duplex(1 << 20);
    tokio::spawn(async move {
        let server = Server::new(workspace).serve(server_side).await.unwrap();
        let _ = server.waiting().await;
    });
    ().serve(client_side).await.unwrap()
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
    serde_json::from_str(text).unwrap()
}

#[tokio::test]
async fn every_tool_the_plan_lists_is_offered_with_a_description() {
    let client = connect(Fake::new("tools")).await;

    let tools = client.list_all_tools().await.unwrap();
    let mut names: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
    names.sort_unstable();

    assert_eq!(
        names,
        [
            "build_executable",
            "create_pipeline",
            "get_component",
            "get_lineage",
            "get_run_log",
            "get_schema",
            "list_components",
            "list_connections",
            "list_pipelines",
            "list_runs",
            "plan_pipeline",
            "run_pipeline",
            "validate_pipeline",
        ]
    );
    for tool in &tools {
        assert!(
            tool.description.as_ref().is_some_and(|d| d.len() > 20),
            "{}",
            tool.name
        );
    }
    let info = client.peer_info().unwrap();
    assert_eq!(info.server_info.as_ref().unwrap().name, "etl");
    assert!(info
        .instructions
        .as_deref()
        .is_some_and(|text| text.contains("${SECRET:name}")));
}

#[tokio::test]
async fn components_are_listed_by_namespace_and_found_by_id() {
    let client = connect(Fake::new("components")).await;

    let (_, all) = call(&client, "list_components", json!({})).await;
    let (_, sources) = call(&client, "list_components", json!({ "namespace": "src" })).await;
    let (_, one) = call(&client, "get_component", json!({ "id": "src.file.csv" })).await;
    let (missing, why) = call(&client, "get_component", json!({ "id": "src.nope" })).await;

    assert_eq!(parsed(&all)["components"].as_array().unwrap().len(), 3);
    assert_eq!(
        parsed(&sources)["components"],
        json!([{ "id": "src.file.csv", "label": "CSV file", "namespace": "source",
                 "description": "Read a CSV file.", "required": ["path"] }])
    );
    assert_eq!(parsed(&one)["properties"][1]["name"], json!("header"));
    assert!(
        missing && why.starts_with("no component 'src.nope'"),
        "{why}"
    );
}

#[tokio::test]
async fn a_valid_pipeline_is_written_and_an_invalid_one_is_not() {
    let fake = Fake::new("create");
    let client = connect(fake.clone()).await;
    let good = json!({ "formatVersion": 1, "nodes": [{ "id": "n1" }], "edges": [] });

    let (failed, text) = call(
        &client,
        "create_pipeline",
        json!({ "path": "pipes/orders.json", "document": good, "params": { "since": "2026-01-01" } }),
    )
    .await;
    let (refused, why) = call(
        &client,
        "create_pipeline",
        json!({ "path": "pipes/orders.json", "document": good }),
    )
    .await;
    let (invalid, report) = call(
        &client,
        "create_pipeline",
        json!({ "path": "bad.json", "document": { "nodes": [] } }),
    )
    .await;
    let (replaced, _) = call(
        &client,
        "create_pipeline",
        json!({ "path": "pipes/orders.json", "document": good, "overwrite": true }),
    )
    .await;

    assert!(!failed, "{text}");
    let written = fake.root.join("pipes").join("orders.json");
    assert_eq!(
        parsed(&std::fs::read_to_string(&written).unwrap()),
        good,
        "the document as given"
    );
    assert_eq!(
        parsed(&text)["written"],
        json!(written.display().to_string())
    );
    assert!(refused && why.contains("pass overwrite: true"), "{why}");
    assert!(invalid, "an invalid document is an error result");
    assert_eq!(parsed(&report)["valid"], json!(false));
    assert!(
        !fake.root.join("bad.json").exists(),
        "and it is not written"
    );
    assert!(!replaced);
    assert!(fake.calls()[0].contains(r#"params: {"since": "2026-01-01"}"#));
}

#[tokio::test]
async fn nothing_outside_the_workspace_is_read_written_run_or_built() {
    let fake = Fake::new("outside");
    let client = connect(fake.clone()).await;
    let document = json!({ "nodes": [{ "id": "n1" }] });
    let elsewhere = std::env::temp_dir().join("etl-mcp-elsewhere.json");
    let elsewhere = elsewhere.display().to_string();

    let tries = [
        (
            "create_pipeline",
            json!({ "path": "../escape.json", "document": document }),
        ),
        (
            "create_pipeline",
            json!({ "path": elsewhere, "document": document }),
        ),
        (
            "create_pipeline",
            json!({ "path": "pipe.txt", "document": document }),
        ),
        ("run_pipeline", json!({ "path": "../../orders.json" })),
        (
            "validate_pipeline",
            json!({ "path": "a/../../orders.json" }),
        ),
        ("plan_pipeline", json!({ "path": "" })),
        (
            "build_executable",
            json!({ "path": "orders.json", "out": "../orders.exe" }),
        ),
    ];
    for (tool, arguments) in tries {
        let (failed, why) = call(&client, tool, arguments.clone()).await;
        assert!(failed, "{tool} {arguments}: {why}");
    }

    assert!(fake.calls().is_empty(), "{:?}", fake.calls());
    assert!(!fake.root.parent().unwrap().join("escape.json").exists());
}

#[tokio::test]
async fn paths_inside_are_the_workspaces_own() {
    let fake = Fake::new("inside");
    let client = connect(fake.clone()).await;

    call(
        &client,
        "run_pipeline",
        json!({ "path": "pipes/./orders.json", "context": "prod" }),
    )
    .await;
    call(
        &client,
        "plan_pipeline",
        json!({ "path": "pipes/../orders.json" }),
    )
    .await;
    call(&client, "get_lineage", json!({ "path": "orders.json" })).await;
    call(
        &client,
        "build_executable",
        json!({ "path": "orders.json", "out": "dist/orders", "target": "linux_amd64" }),
    )
    .await;

    assert_eq!(
        fake.calls(),
        [
            r#"run pipes/orders.json Bindings { params: {}, context: Some("prod") }"#,
            "plan orders.json",
            "lineage orders.json",
            r#"build orders.json Some("linux_amd64") dist/orders"#,
        ]
    );
}

#[tokio::test]
async fn a_failed_run_and_a_missing_run_are_results_the_agent_can_read() {
    let fake = Fake::new("runs");
    let client = connect(fake.clone()).await;

    let (ok, record) = call(&client, "run_pipeline", json!({ "path": "orders.json" })).await;
    let (failed, why) = call(&client, "run_pipeline", json!({ "path": "broken.json" })).await;
    let (_, log) = call(&client, "get_run_log", json!({ "id": " r1 " })).await;
    let (missing, not_found) = call(&client, "get_run_log", json!({ "id": "r9" })).await;
    call(
        &client,
        "list_runs",
        json!({ "pipeline": "orders", "limit": 100000 }),
    )
    .await;

    assert!(!ok);
    assert_eq!(parsed(&record)["outcome"], json!("succeeded"));
    assert!(failed, "a failed run is marked as an error");
    assert_eq!(
        parsed(&why)["failures"][0],
        json!("read (n1): no such file")
    );
    assert_eq!(parsed(&log)["id"], json!("r1"));
    assert!(missing && not_found == "no run called 'r9'");
    assert_eq!(fake.calls().last().unwrap(), r#"runs Some("orders") 500"#);
}

#[tokio::test]
async fn validate_takes_a_document_or_a_file_but_not_both() {
    let fake = Fake::new("validate");
    std::fs::write(fake.root.join("orders.json"), r#"{"nodes":[{"id":"n1"}]}"#).unwrap();
    let client = connect(fake.clone()).await;

    let (_, from_file) = call(
        &client,
        "validate_pipeline",
        json!({ "path": "orders.json" }),
    )
    .await;
    let (_, from_document) = call(
        &client,
        "validate_pipeline",
        json!({ "document": { "nodes": [{ "id": "a" }, { "id": "b" }] } }),
    )
    .await;
    let (both, why) = call(
        &client,
        "validate_pipeline",
        json!({ "document": {}, "path": "orders.json" }),
    )
    .await;
    let (missing, _) = call(&client, "validate_pipeline", json!({ "path": "nope.json" })).await;

    assert_eq!(parsed(&from_file)["stages"], json!(1));
    assert_eq!(parsed(&from_document)["stages"], json!(2));
    assert!(both && why == "give either document or path");
    assert!(missing);
}

#[tokio::test]
async fn schema_pipelines_and_connections_pass_through() {
    let client = connect(Fake::new("through")).await;

    let (_, schema) = call(&client, "get_schema", json!({})).await;
    let (_, pipelines) = call(&client, "list_pipelines", json!({})).await;
    let (_, connections) = call(&client, "list_connections", json!({})).await;

    assert_eq!(parsed(&schema)["title"], json!("etl pipeline document"));
    assert_eq!(parsed(&pipelines)[0]["name"], json!("orders"));
    assert_eq!(
        parsed(&connections)["secrets"][0]["name"],
        json!("pg_password")
    );
}

#[test]
fn inside_follows_dots_and_refuses_what_climbs_out() {
    let root = std::env::temp_dir().join("etl-mcp-root");

    assert_eq!(
        inside(&root, "a/b.json").unwrap(),
        root.join("a").join("b.json")
    );
    assert_eq!(inside(&root, "a/../b.json").unwrap(), root.join("b.json"));
    assert_eq!(
        inside(&root, &root.join("c.json").display().to_string()).unwrap(),
        root.join("c.json")
    );
    assert!(inside(&root, "../b.json").is_err());
    assert!(inside(&root, "a/../../b.json").is_err());
    assert!(inside(&root, ".").is_err(), "the root itself is not a file");
    assert!(inside(&root, "  ").is_err());
    assert!(inside(
        &root,
        &std::env::temp_dir().join("x.json").display().to_string()
    )
    .is_err());
}
