//! Phase 4's connectors, run against the real thing (Phase 10c).
//!
//! Phase 4 tested these components by comparing the SQL they generate, and
//! that caught nothing about whether the SQL works against a real table or a
//! real server. This file is where it is checked:
//!
//! - **Delta and Iceberg** read tables written by each format's own reference
//!   library (`deltalake` 1.6 and `pyiceberg` 0.12), committed as fixtures under
//!   `tests/fixtures/lake/`. The Iceberg table was written elsewhere and moved
//!   here, which is what `allow_moved_paths` is for. How the fixtures were made
//!   is in `tests/fixtures/lake/README.md`.
//! - **Postgres, MySQL and S3** run against servers named by environment
//!   variables, and skip without them: a container locally, a service in CI.
//!
//! Every test skips without the vendored DuckDB binary or the extension it
//! needs, the same bargain `end_to_end.rs` makes.

use etl_duckdb_engine::{compile, run, EngineError, RunOptions};
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

fn slashed(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn lake() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lake")
}

/// A fresh workspace for one test, and the binary; `None` to skip when the
/// binary or any of `extensions` is not vendored here.
fn workspace(name: &str, extensions: &[&str]) -> Option<(PathBuf, PathBuf)> {
    let directory = repo_root().join("target/test-out/verified").join(name);
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();

    let options = options(&directory);
    let Ok(binary) = etl_duckdb_engine::exec::locate_duckdb(&options) else {
        eprintln!("skipping {name}: no DuckDB binary; run scripts/fetch-duckdb.ps1");
        return None;
    };

    let vendored = etl_duckdb_engine::exec::locate_extension_dir(&options);
    for extension in extensions {
        let present = vendored
            .as_ref()
            .is_some_and(|root| has_extension(root, extension));
        if !present {
            eprintln!(
                "skipping {name}: the '{extension}' extension is not vendored; run \
                 scripts/fetch-duckdb-extensions.ps1"
            );
            return None;
        }
    }

    Some((directory, binary))
}

/// Whether `<root>/<version>/<platform>/<name>.duckdb_extension` exists for
/// any version and platform.
fn has_extension(root: &Path, name: &str) -> bool {
    let file = format!("{name}.duckdb_extension");
    let Ok(versions) = std::fs::read_dir(root) else {
        return false;
    };
    versions.filter_map(Result::ok).any(|version| {
        std::fs::read_dir(version.path())
            .map(|platforms| {
                platforms
                    .filter_map(Result::ok)
                    .any(|platform| platform.path().join(&file).is_file())
            })
            .unwrap_or(false)
    })
}

fn options(workspace: &Path) -> RunOptions {
    RunOptions {
        working_dir: Some(workspace.to_path_buf()),
        ..Default::default()
    }
}

fn document(json: &str) -> PipelineDoc {
    PipelineDoc::from_json(json).expect("test pipeline parses")
}

/// One source node feeding a Parquet file, so the result can be queried
/// independently of the code under test.
fn to_parquet(component_id: &str, properties: &str) -> PipelineDoc {
    to_parquet_at(component_id, properties, "out/rows.parquet")
}

/// The same, into a file of the caller's choosing. A test that reads back more
/// than once uses a new file each time: overwriting the one just queried can
/// fail on Windows with "Could not move file: Access is denied" while something
/// -- a virus scanner, a handle not yet released -- still holds the old one.
fn to_parquet_at(component_id: &str, properties: &str, out: &str) -> PipelineDoc {
    document(&format!(
        r#"{{ "formatVersion": 1,
          "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "Read",
               "componentId": "{component_id}", "properties": {properties} }} }},
            {{ "id": "write", "position": {{"x":0,"y":0}}, "data": {{ "label": "Write",
               "componentId": "snk.file.parquet", "properties": {{ "path": "{out}" }} }} }}
          ],
          "edges": [ {{ "id": "e1", "source": "read", "target": "write" }} ] }}"#
    ))
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

/// The twelve sample orders, summarised the same way whatever they came from.
const SUMMARY: &str = "SELECT count(*) AS n, sum(amount)::VARCHAR AS total, min(order_id) AS lo, \
    max(order_id) AS hi, typeof(any_value(order_ts)) AS ts, typeof(any_value(amount)) AS amt \
    FROM 'out/rows.parquet';";

const TWELVE_ORDERS: &str =
    r#"[{"n":12,"total":"2264.46","lo":1001,"hi":1012,"ts":"TIMESTAMP","amt":"DECIMAL(10,2)"}]"#;

/// The file name of an Iceberg metadata version, without `.metadata.json`.
/// Looked up rather than written down: pyiceberg names them with a UUID.
fn iceberg_version(prefix: &str) -> String {
    let directory = lake().join("orders_iceberg/metadata");
    std::fs::read_dir(&directory)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .find(|name| name.starts_with(prefix) && name.ends_with(".metadata.json"))
        .unwrap_or_else(|| panic!("no {prefix}*.metadata.json in {}", directory.display()))
        .trim_end_matches(".metadata.json")
        .to_string()
}

// ---------------------------------------------------------------------------
// Delta
// ---------------------------------------------------------------------------

#[test]
fn a_delta_table_written_by_deltalake_reads_back_whole_and_typed() {
    let Some((workspace, binary)) = workspace("delta", &["delta", "httpfs"]) else {
        return;
    };

    let pipeline = to_parquet(
        "src.lake.delta",
        &format!(
            r#"{{ "path": "{}" }}"#,
            slashed(&lake().join("orders_delta"))
        ),
    );
    let report = run(&compile(&pipeline).unwrap(), &options(&workspace)).expect("runs");

    // Two commits, seven rows then five: both are read.
    assert_eq!(report.stages[0].rows, Some(12));
    assert_eq!(query(&binary, &workspace, SUMMARY), TWELVE_ORDERS);
}

// ---------------------------------------------------------------------------
// Iceberg
// ---------------------------------------------------------------------------

#[test]
fn a_moved_iceberg_table_reads_from_its_root_at_the_latest_version() {
    let Some((workspace, binary)) = workspace("iceberg", &["iceberg", "httpfs"]) else {
        return;
    };

    let pipeline = to_parquet(
        "src.lake.iceberg",
        &format!(
            r#"{{ "path": "{}", "allow_moved_paths": true, "version": "{}" }}"#,
            slashed(&lake().join("orders_iceberg")),
            iceberg_version("00002-")
        ),
    );
    let report = run(&compile(&pipeline).unwrap(), &options(&workspace)).expect("runs");

    assert_eq!(report.stages[0].rows, Some(12));
    assert_eq!(query(&binary, &workspace, SUMMARY), TWELVE_ORDERS);
}

#[test]
fn an_earlier_iceberg_version_is_the_table_as_it_was_then() {
    let Some((workspace, _)) = workspace("iceberg_v1", &["iceberg", "httpfs"]) else {
        return;
    };

    let pipeline = to_parquet(
        "src.lake.iceberg",
        &format!(
            r#"{{ "path": "{}", "allow_moved_paths": true, "version": "{}" }}"#,
            slashed(&lake().join("orders_iceberg")),
            iceberg_version("00001-")
        ),
    );
    let report = run(&compile(&pipeline).unwrap(), &options(&workspace)).expect("runs");

    assert_eq!(
        report.stages[0].rows,
        Some(7),
        "the first commit held seven"
    );
}

#[test]
fn a_moved_iceberg_table_named_by_its_metadata_file_is_refused_before_it_runs() {
    // What Phase 4's help text told people to do, and what failed against the
    // real table with a message about a file nobody named.
    let metadata = lake()
        .join("orders_iceberg/metadata")
        .join(format!("{}.metadata.json", iceberg_version("00002-")));
    let pipeline = to_parquet(
        "src.lake.iceberg",
        &format!(
            r#"{{ "path": "{}", "allow_moved_paths": true }}"#,
            slashed(&metadata)
        ),
    );

    match compile(&pipeline) {
        Err(EngineError::InvalidProperty { property, .. }) => assert_eq!(property, "path"),
        other => panic!("expected the path to be refused, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Servers: Postgres, MySQL, S3 (MinIO)
// ---------------------------------------------------------------------------
//
// Each reads its server from an environment variable and skips without it.
// `scripts/test-services.ps1` starts all three in containers and says what to
// set; CI's Ubuntu gate runs it. Windows CI cannot run Linux containers, so
// these skip there, which is the same split the desktop crate makes.

fn server(variable: &str) -> Option<String> {
    match std::env::var(variable) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => {
            eprintln!("skipping: {variable} is not set; see scripts/test-services.ps1");
            None
        }
    }
}

fn sample_csv() -> String {
    slashed(&repo_root().join("samples/data/orders.csv"))
}

/// A table name no other test, and no earlier run, is using.
fn table(name: &str) -> String {
    format!("orders_{name}_{}", std::process::id())
}

/// CSV in, into a database table, with the given sink properties.
fn csv_into(component_id: &str, properties: &str) -> PipelineDoc {
    document(&format!(
        r#"{{ "formatVersion": 1,
          "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "CSV",
               "componentId": "src.file.csv", "properties": {{ "path": "{csv}" }} }} }},
            {{ "id": "write", "position": {{"x":0,"y":0}}, "data": {{ "label": "Write",
               "componentId": "{component_id}", "properties": {properties} }} }}
          ],
          "edges": [ {{ "id": "e1", "source": "read", "target": "write" }} ] }}"#,
        csv = sample_csv()
    ))
}

/// A database round trip: write twice (overwrite, then append) and read back
/// after each, through the components under test and nothing else.
fn round_trip(kind: &str, extension: &str, connection: &str) {
    let Some((workspace, binary)) = workspace(&format!("{kind}_round_trip"), &[extension]) else {
        return;
    };
    let name = table(kind);
    let connection = connection.replace('\\', "\\\\").replace('"', "\\\"");
    let options = options(&workspace);

    let write = |mode: &str| {
        let pipeline = csv_into(
            &format!("snk.db.{kind}"),
            &format!(r#"{{ "connection": "{connection}", "table": "{name}", "mode": "{mode}" }}"#),
        );
        run(&compile(&pipeline).unwrap(), &options).unwrap_or_else(|e| panic!("{mode}: {e}"));
    };
    let read_back = |out: &str| {
        let pipeline = to_parquet_at(
            &format!("src.db.{kind}"),
            &format!(r#"{{ "connection": "{connection}", "table": "{name}" }}"#),
            out,
        );
        run(&compile(&pipeline).unwrap(), &options).expect("reads back");
        query(
            &binary,
            &workspace,
            &format!("SELECT count(*) AS n, round(sum(amount), 2)::VARCHAR AS total FROM '{out}';"),
        )
    };

    // Overwrite onto a table that does not exist yet, then again onto one that
    // does: both have to leave exactly the twelve rows.
    write("overwrite");
    write("overwrite");
    assert_eq!(
        read_back("out/after_overwrite.parquet"),
        r#"[{"n":12,"total":"2264.46"}]"#,
        "after overwrite"
    );

    // Append adds the twelve again. The append that creates its table on first
    // use is Phase 4's bug fix, which only an execution test could have caught.
    write("append");
    assert_eq!(
        read_back("out/after_append.parquet"),
        r#"[{"n":24,"total":"4528.92"}]"#,
        "after append"
    );

    let first_append = table(&format!("{kind}_fresh_append"));
    let pipeline = csv_into(
        &format!("snk.db.{kind}"),
        &format!(
            r#"{{ "connection": "{connection}", "table": "{first_append}", "mode": "append" }}"#
        ),
    );
    run(&compile(&pipeline).unwrap(), &options).expect("an append onto a missing table creates it");
}

#[test]
fn postgres_is_written_and_read_back_through_attach() {
    if let Some(connection) = server("ETL_TEST_POSTGRES") {
        round_trip("postgres", "postgres_scanner", &connection);
    }
}

#[test]
fn mysql_is_written_and_read_back_through_attach() {
    if let Some(connection) = server("ETL_TEST_MYSQL") {
        round_trip("mysql", "mysql_scanner", &connection);
    }
}

#[test]
fn a_wrong_database_password_fails_and_is_masked() {
    let Some(connection) = server("ETL_TEST_POSTGRES") else {
        return;
    };
    let Some((workspace, _)) = workspace("postgres_bad_password", &["postgres_scanner"]) else {
        return;
    };

    let wrong = connection.replace("password=etl", "password=wrong-hunter2");
    assert_ne!(
        wrong, connection,
        "the connection string should hold password=etl"
    );

    let pipeline = to_parquet(
        "src.db.postgres",
        &format!(r#"{{ "connection": "{wrong}", "table": "anything" }}"#),
    );
    let options = RunOptions {
        redact: vec!["wrong-hunter2".to_string()],
        ..options(&workspace)
    };

    let error = run(&compile(&pipeline).unwrap(), &options)
        .unwrap_err()
        .to_string();
    assert!(!error.contains("wrong-hunter2"), "{error}");
}

/// S3 through MinIO: write Parquet and CSV, read both back.
#[test]
fn s3_is_written_and_read_back_through_an_s3_compatible_endpoint() {
    let Some(endpoint) = server("ETL_TEST_S3") else {
        return;
    };
    let Some((workspace, binary)) = workspace("s3_round_trip", &["httpfs"]) else {
        return;
    };
    let options = options(&workspace);
    let prefix = format!("s3://etl-test/run-{}", std::process::id());
    let access = format!(
        r#""key_id": "etl-test", "secret": "etl-test-secret", "region": "us-east-1",
           "endpoint": "{endpoint}", "url_style": "path""#
    );

    for (format, extra) in [("parquet", ""), ("csv", r#", "header": true"#)] {
        let path = format!("{prefix}/orders.{format}");

        let write = csv_into(
            "snk.cloud.s3",
            &format!(r#"{{ "path": "{path}", "format": "{format}"{extra}, {access} }}"#),
        );
        run(&compile(&write).unwrap(), &options)
            .unwrap_or_else(|e| panic!("writing {format} to S3: {e}"));

        let out = format!("out/from_{format}.parquet");
        let read = to_parquet_at(
            "src.cloud.s3",
            &format!(r#"{{ "path": "{path}", "format": "{format}"{extra}, {access} }}"#),
            &out,
        );
        run(&compile(&read).unwrap(), &options)
            .unwrap_or_else(|e| panic!("reading {format} from S3: {e}"));

        assert_eq!(
            query(
                &binary,
                &workspace,
                &format!(
                    "SELECT count(*) AS n, round(sum(amount), 2)::VARCHAR AS total FROM '{out}';"
                )
            ),
            r#"[{"n":12,"total":"2264.46"}]"#,
            "{format} through S3"
        );
    }

    // The Phase 4 bug this found: the sink treated `s3://...` as a local
    // directory to create. On Windows that failed outright; on Linux it
    // quietly made a folder called `s3:` and the round trip still passed, so
    // this is the assertion that catches it there.
    assert!(
        !workspace.join("s3:").exists(),
        "a local 's3:' folder was created for an S3 path"
    );
}

#[test]
fn s3_with_the_wrong_secret_is_refused_and_the_secret_is_masked() {
    let Some(endpoint) = server("ETL_TEST_S3") else {
        return;
    };
    let Some((workspace, _)) = workspace("s3_bad_secret", &["httpfs"]) else {
        return;
    };

    let pipeline = to_parquet(
        "src.cloud.s3",
        &format!(
            r#"{{ "path": "s3://etl-test/nothing.parquet", "key_id": "etl-test",
                 "secret": "not-the-secret-42", "region": "us-east-1",
                 "endpoint": "{endpoint}", "url_style": "path" }}"#
        ),
    );
    let options = RunOptions {
        redact: vec!["not-the-secret-42".to_string()],
        ..options(&workspace)
    };

    let error = run(&compile(&pipeline).unwrap(), &options)
        .unwrap_err()
        .to_string();
    assert!(!error.contains("not-the-secret-42"), "{error}");
}

// ---------------------------------------------------------------------------
// Kafka, as bounded micro-batches (Phase 10e)
// ---------------------------------------------------------------------------
//
// The connector's own tests prove the reading. These prove the loop around it
// that the engine owns: a run hands back where it got to, the next compile is
// given that position, and a failed run hands back nothing to save.

use etl_duckdb_engine::{compile_with, preview, resolve, CompileOptions, Resolver, RunReport};
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use std::collections::BTreeMap;

fn tokio() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// A topic of its own for one test, with `orders` produced into it as JSON
/// round-robin across three partitions.
fn kafka_topic(broker: &str, test: &str, orders: &[serde_json::Value]) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let topic = format!("etl-verified-{test}-{nanos}");
    tokio().block_on(async {
        let client = ClientBuilder::new(vec![broker.to_string()])
            .build()
            .await
            .expect("the test broker answers");
        client
            .controller_client()
            .unwrap()
            .create_topic(topic.as_str(), 3, 1, 10_000)
            .await
            .expect("creates the topic");
        // Where the sample sends its large orders back to (Phase 10f).
        client
            .controller_client()
            .unwrap()
            .create_topic(format!("{topic}-large"), 3, 1, 10_000)
            .await
            .expect("creates the large-orders topic");
        // Creation is asynchronous: wait until both are listed, or the
        // connectors, which list topics first, would call them missing.
        let large = format!("{topic}-large");
        for _ in 0..100 {
            let listed = client.list_topics().await.expect("lists topics");
            if [&topic, &large]
                .iter()
                .all(|name| listed.iter().any(|t| &t.name == *name))
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!("topics {topic} and {large} were created but never listed");
    });
    produce(broker, &topic, orders, 0);
    topic
}

/// How many records a topic holds: the sum of its partitions' high watermarks.
fn topic_size(broker: &str, topic: &str) -> i64 {
    tokio().block_on(async {
        let client = ClientBuilder::new(vec![broker.to_string()])
            .build()
            .await
            .unwrap();
        let mut total = 0;
        for partition in 0..3 {
            total += client
                .partition_client(topic, partition, UnknownTopicHandling::Retry)
                .await
                .unwrap()
                .get_offset(rskafka::client::partition::OffsetAt::Latest)
                .await
                .unwrap();
        }
        total
    })
}

/// Produce `orders` as JSON values; the n-th goes to partition (n + skip) % 3.
fn produce(broker: &str, topic: &str, orders: &[serde_json::Value], skip: usize) {
    tokio().block_on(async {
        let client = ClientBuilder::new(vec![broker.to_string()])
            .build()
            .await
            .unwrap();
        for (n, order) in orders.iter().enumerate() {
            let partition = ((n + skip) % 3) as i32;
            client
                .partition_client(topic, partition, UnknownTopicHandling::Retry)
                .await
                .unwrap()
                .produce(
                    vec![rskafka::record::Record {
                        key: Some(order["order_id"].to_string().into_bytes()),
                        value: Some(order.to_string().into_bytes()),
                        headers: BTreeMap::new(),
                        timestamp: {
                            use rskafka::chrono::TimeZone;
                            rskafka::chrono::Utc
                                .timestamp_millis_opt(1_790_157_907_000)
                                .unwrap()
                        },
                    }],
                    Compression::NoCompression,
                )
                .await
                .unwrap();
        }
    });
}

/// The twelve sample orders as JSON, the shape an orders service would emit.
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

/// The committed `kafka_orders` sample against `topic`, compiled against the
/// positions in `checkpoints`. `policy` goes on the filter, to take the plan
/// onto the session path.
fn kafka_orders(
    workspace: &Path,
    broker: &str,
    topic: &str,
    checkpoints: &BTreeMap<String, serde_json::Value>,
    policy: Option<serde_json::Value>,
) -> etl_duckdb_engine::Plan {
    let text = std::fs::read_to_string(repo_root().join("samples/pipelines/kafka_orders.json"))
        .expect("the sample is committed");
    let mut json: serde_json::Value = serde_json::from_str(&text).unwrap();
    if let Some(policy) = policy {
        json["nodes"][1]["data"]["policy"] = policy;
    }
    let resolver = Resolver::new(workspace)
        .bind("kafka_brokers", broker)
        .bind("topic", topic)
        .bind("large_topic", &format!("{topic}-large"));
    let resolved = resolve(&document(&json.to_string()), &resolver).expect("resolves");
    compile_with(
        &resolved.document,
        &CompileOptions {
            checkpoints: checkpoints.clone(),
            ..CompileOptions::default()
        },
    )
    .expect("compiles")
}

/// Where each native source got to, keyed the way `CompileOptions` takes it:
/// what `etl run` saves and hands to the next compile.
fn positions(report: &RunReport) -> BTreeMap<String, serde_json::Value> {
    report
        .checkpoints
        .iter()
        .map(|checkpoint| (checkpoint.node_id.clone(), checkpoint.value.clone()))
        .collect()
}

fn rows(report: &RunReport) -> Vec<Option<u64>> {
    report.stages.iter().map(|stage| stage.rows).collect()
}

/// The sample, three runs: everything, nothing new, then only what arrived.
fn the_sample_carries_on(name: &str, policy: Option<serde_json::Value>) {
    let Some(broker) = server("ETL_TEST_KAFKA") else {
        return;
    };
    let Some((workspace, binary)) = workspace(name, &[]) else {
        return;
    };
    let orders = sample_orders();
    let topic = kafka_topic(&broker, name, &orders[..10]);

    let first = run(
        &kafka_orders(
            &workspace,
            &broker,
            &topic,
            &BTreeMap::new(),
            policy.clone(),
        ),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&first), [Some(10), Some(5), Some(5), Some(5)]);
    assert_eq!(
        topic_size(&broker, &format!("{topic}-large")),
        5,
        "the large orders went back to Kafka too"
    );
    let saved = positions(&first);
    assert_eq!(
        saved["read_orders"]["offsets"]
            .as_object()
            .unwrap()
            .values()
            .map(|v| v.as_i64().unwrap())
            .sum::<i64>(),
        10,
        "{saved:?}"
    );
    assert!(
        first
            .notes
            .iter()
            .any(|n| n.contains("10 record(s) from 3 partition(s)")),
        "{:?}",
        first.notes
    );
    assert_eq!(
        query(
            &binary,
            &workspace,
            "SELECT count(*) AS n, min(_offset) >= 0 AS offsets, typeof(any_value(_timestamp)) AS t \
             FROM 'samples/out/kafka_large_orders.parquet';"
        ),
        r#"[{"n":5,"offsets":true,"t":"TIMESTAMP"}]"#
    );

    // Nothing new: the saved position is the end.
    let second = run(
        &kafka_orders(&workspace, &broker, &topic, &saved, policy.clone()),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&second), [Some(0), Some(0), Some(0), Some(0)]);
    assert_eq!(
        topic_size(&broker, &format!("{topic}-large")),
        5,
        "nothing sent twice"
    );

    // Two more orders arrive: exactly those are read.
    produce(&broker, &topic, &orders[10..], 10);
    let third = run(
        &kafka_orders(&workspace, &broker, &topic, &positions(&second), policy),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&third)[0], Some(2));
}

#[test]
fn the_kafka_sample_carries_on_between_runs_on_the_one_script_path() {
    the_sample_carries_on("kafka_script", None);
}

#[test]
fn the_kafka_sample_carries_on_between_runs_on_the_session_path() {
    the_sample_carries_on(
        "kafka_session",
        Some(serde_json::json!({ "retryAttempts": 1 })),
    );
}

#[test]
fn a_failed_kafka_run_hands_back_no_position_to_save() {
    let Some(broker) = server("ETL_TEST_KAFKA") else {
        return;
    };
    let Some((workspace, _)) = workspace("kafka_failed", &[]) else {
        return;
    };
    let topic = kafka_topic(&broker, "failed", &sample_orders()[..6]);

    let broken = |policy: &str| {
        document(&format!(
            r#"{{ "formatVersion": 1, "nodes": [
                {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "Kafka",
                   "componentId": "src.stream.kafka",
                   "properties": {{ "brokers": "{broker}", "topic": "{topic}" }} }} }},
                {{ "id": "broken", "position": {{"x":0,"y":0}}, "data": {{ "label": "Broken",
                   "componentId": "xf.filter", "properties": {{ "predicate": "no_such_column > 1" }}
                   {policy} }} }},
                {{ "id": "out", "position": {{"x":0,"y":0}}, "data": {{ "label": "Out",
                   "componentId": "snk.file.parquet", "properties": {{ "path": "out.parquet" }} }} }}
              ],
              "edges": [ {{ "id": "e1", "source": "read", "target": "broken" }},
                         {{ "id": "e2", "source": "broken", "target": "out" }} ] }}"#
        ))
    };

    // One script: the failure is an error, and there is no report to save.
    let plan = compile_with(&broken(""), &CompileOptions::default()).unwrap();
    assert!(run(&plan, &options(&workspace)).is_err());

    // Carrying on past the failure: the report survives, its position does not.
    let plan = compile_with(
        &broken(r#", "policy": { "continueOnFailure": true }"#),
        &CompileOptions::default(),
    )
    .unwrap();
    let report = run(&plan, &options(&workspace)).expect("a report, not an error");
    assert!(report.failed());
    assert!(report.checkpoints.is_empty(), "{:?}", report.checkpoints);
    assert_eq!(report.stages[0].rows, Some(6), "the source did read them");
}

#[test]
fn previewing_a_kafka_source_reads_from_the_saved_position() {
    let Some(broker) = server("ETL_TEST_KAFKA") else {
        return;
    };
    let Some((workspace, _)) = workspace("kafka_preview", &[]) else {
        return;
    };
    let topic = kafka_topic(&broker, "preview", &sample_orders()[..9]);

    let fresh = kafka_orders(&workspace, &broker, &topic, &BTreeMap::new(), None);
    let all = preview(&fresh, "read_orders", 50, &options(&workspace)).expect("previews");
    assert_eq!(all.rows.len(), 9);

    let report = run(&fresh, &options(&workspace)).expect("runs");
    let caught_up = kafka_orders(&workspace, &broker, &topic, &positions(&report), None);
    let nothing = preview(&caught_up, "read_orders", 50, &options(&workspace)).expect("previews");
    assert!(
        nothing.rows.is_empty(),
        "a preview reads what the next run would"
    );
}

// ---------------------------------------------------------------------------
// NATS JetStream, as bounded micro-batches (Phase 10g)
// ---------------------------------------------------------------------------

/// Two streams of their own for one test: `<name>` capturing `<name>.>`, with
/// `orders` published into it, and `<name>_LARGE` capturing
/// `<name>.large`-bound publishes under their own subject space.
fn nats_streams(url: &str, test: &str, orders: &[serde_json::Value]) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let name = format!("ETL_VERIFIED_{}_{nanos}", test.to_uppercase());
    tokio().block_on(async {
        let client = async_nats::connect(url)
            .await
            .expect("the test server answers");
        let jetstream = async_nats::jetstream::new(client);
        for (stream, subject) in [
            (name.clone(), format!("{name}.orders.>")),
            (format!("{name}_LARGE"), format!("{name}.large")),
        ] {
            jetstream
                .create_stream(async_nats::jetstream::stream::Config {
                    name: stream,
                    subjects: vec![subject],
                    ..Default::default()
                })
                .await
                .expect("creates the stream");
        }
    });
    nats_publish(url, &name, orders);
    name
}

fn nats_publish(url: &str, stream: &str, orders: &[serde_json::Value]) {
    tokio().block_on(async {
        let client = async_nats::connect(url).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);
        for order in orders {
            jetstream
                .publish(format!("{stream}.orders.eu"), order.to_string().into())
                .await
                .unwrap()
                .await
                .unwrap();
        }
    });
}

/// How many messages a stream holds.
fn nats_size(url: &str, stream: &str) -> u64 {
    tokio().block_on(async {
        let client = async_nats::connect(url).await.unwrap();
        let mut stream = async_nats::jetstream::new(client)
            .get_stream(stream)
            .await
            .unwrap();
        stream.info().await.unwrap().state.messages
    })
}

fn nats_orders(
    workspace: &Path,
    url: &str,
    stream: &str,
    checkpoints: &BTreeMap<String, serde_json::Value>,
    policy: Option<serde_json::Value>,
) -> etl_duckdb_engine::Plan {
    let text = std::fs::read_to_string(repo_root().join("samples/pipelines/nats_orders.json"))
        .expect("the sample is committed");
    let mut json: serde_json::Value = serde_json::from_str(&text).unwrap();
    if let Some(policy) = policy {
        json["nodes"][1]["data"]["policy"] = policy;
    }
    let resolver = Resolver::new(workspace)
        .bind("nats_url", url)
        .bind("stream", stream)
        .bind("large_subject", &format!("{stream}.large"));
    let resolved = resolve(&document(&json.to_string()), &resolver).expect("resolves");
    compile_with(
        &resolved.document,
        &CompileOptions {
            checkpoints: checkpoints.clone(),
            ..CompileOptions::default()
        },
    )
    .expect("compiles")
}

fn the_nats_sample_carries_on(name: &str, policy: Option<serde_json::Value>) {
    let Some(url) = server("ETL_TEST_NATS") else {
        return;
    };
    let Some((workspace, binary)) = workspace(name, &[]) else {
        return;
    };
    let orders = sample_orders();
    let stream = nats_streams(&url, name, &orders[..10]);

    let first = run(
        &nats_orders(&workspace, &url, &stream, &BTreeMap::new(), policy.clone()),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&first), [Some(10), Some(5), Some(5), Some(5)]);
    assert_eq!(nats_size(&url, &format!("{stream}_LARGE")), 5);
    assert_eq!(
        query(
            &binary,
            &workspace,
            "SELECT count(*) AS n, min(_sequence) AS s, typeof(any_value(_timestamp)) AS t \
             FROM 'samples/out/nats_large_orders.parquet';"
        ),
        r#"[{"n":5,"s":1,"t":"TIMESTAMP"}]"#
    );

    // Nothing new.
    let second = run(
        &nats_orders(
            &workspace,
            &url,
            &stream,
            &positions(&first),
            policy.clone(),
        ),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&second), [Some(0), Some(0), Some(0), Some(0)]);

    // Two arrive, and a first run's saved position is lost, so the next run
    // reads everything again: the large orders are published again, and the
    // message IDs keep the second copies out.
    nats_publish(&url, &stream, &orders[10..]);
    let again = run(
        &nats_orders(&workspace, &url, &stream, &BTreeMap::new(), policy),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&again)[0], Some(12));
    let large_in_all_twelve = orders
        .iter()
        .filter(|order| order["amount"].as_f64().unwrap() > 100.0)
        .count() as u64;
    assert_eq!(
        nats_size(&url, &format!("{stream}_LARGE")),
        large_in_all_twelve,
        "re-published large orders were dropped as duplicates"
    );
}

#[test]
fn the_nats_sample_carries_on_between_runs_on_the_one_script_path() {
    the_nats_sample_carries_on("nats_script", None);
}

#[test]
fn the_nats_sample_carries_on_between_runs_on_the_session_path() {
    the_nats_sample_carries_on(
        "nats_session",
        Some(serde_json::json!({ "retryAttempts": 1 })),
    );
}

#[test]
fn previewing_a_nats_source_reads_and_publishes_nothing() {
    let Some(url) = server("ETL_TEST_NATS") else {
        return;
    };
    let Some((workspace, _)) = workspace("nats_preview", &[]) else {
        return;
    };
    let stream = nats_streams(&url, "preview", &sample_orders()[..9]);

    let plan = nats_orders(&workspace, &url, &stream, &BTreeMap::new(), None);
    let rows = preview(&plan, "read_orders", 50, &options(&workspace)).expect("previews");
    assert_eq!(rows.rows.len(), 9);
    assert_eq!(
        nats_size(&url, &format!("{stream}_LARGE")),
        0,
        "a preview publishes nothing"
    );
}
