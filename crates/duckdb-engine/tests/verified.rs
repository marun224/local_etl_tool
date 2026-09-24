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
// Servers: Postgres, MySQL, S3 (SeaweedFS; MinIO before 10u)
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
fn round_trip(label: &str, kind: &str, extension: &str, connection: &str) {
    let Some((workspace, binary)) = workspace(&format!("{label}_round_trip"), &[extension]) else {
        return;
    };
    let name = table(label);
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

    let first_append = table(&format!("{label}_fresh_append"));
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
        round_trip("postgres", "postgres", "postgres_scanner", &connection);
    }
}

#[test]
fn mysql_is_written_and_read_back_through_attach() {
    if let Some(connection) = server("ETL_TEST_MYSQL") {
        round_trip("mysql", "mysql", "mysql_scanner", &connection);
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

/// S3 through an S3-compatible server: write Parquet and CSV, read both back.
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

// ---------------------------------------------------------------------------
// Amazon Kinesis, as bounded micro-batches (Phase 10h), against kinesis-mock
// ---------------------------------------------------------------------------

/// One Kinesis call to the test server, which accepts any signature: this is
/// set-up for the test, and the connector's own signing is proved elsewhere.
fn kinesis_call(endpoint: &str, target: &str, body: serde_json::Value) -> serde_json::Value {
    let mut response = ureq::post(format!("{endpoint}/"))
        .header("Content-Type", "application/x-amz-json-1.1")
        .header("X-Amz-Target", format!("Kinesis_20131202.{target}"))
        .header("X-Amz-Date", "20260101T000000Z")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/kinesis/aws4_request, \
             SignedHeaders=host, Signature=0",
        )
        .send(body.to_string().as_bytes())
        .unwrap_or_else(|error| panic!("{target}: {error}"));
    let text = response.body_mut().read_to_string().unwrap();
    if text.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&text).unwrap()
    }
}

/// Every record a stream holds, from each shard's start.
fn kinesis_records(endpoint: &str, stream: &str) -> usize {
    let shards = kinesis_call(
        endpoint,
        "ListShards",
        serde_json::json!({ "StreamName": stream }),
    );
    let mut count = 0;
    for shard in shards["Shards"].as_array().unwrap() {
        let iterator = kinesis_call(
            endpoint,
            "GetShardIterator",
            serde_json::json!({
                "StreamName": stream, "ShardId": shard["ShardId"],
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        );
        let records = kinesis_call(
            endpoint,
            "GetRecords",
            serde_json::json!({ "ShardIterator": iterator["ShardIterator"], "Limit": 10000 }),
        );
        count += records["Records"].as_array().unwrap().len();
    }
    count
}

/// A two-shard stream for one test, holding `orders`, deleted when dropped.
struct KinesisStream {
    endpoint: String,
    name: String,
}

impl Drop for KinesisStream {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(|| {
            kinesis_call(
                &self.endpoint,
                "DeleteStream",
                serde_json::json!({ "StreamName": self.name, "EnforceConsumerDeletion": true }),
            )
        });
    }
}

fn kinesis_stream(endpoint: &str, test: &str, orders: &[serde_json::Value]) -> KinesisStream {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let name = format!("etl-verified-{test}-{nanos}");
    kinesis_call(
        endpoint,
        "CreateStream",
        serde_json::json!({ "StreamName": name, "ShardCount": 2 }),
    );
    for _ in 0..100 {
        let summary = kinesis_call(
            endpoint,
            "DescribeStreamSummary",
            serde_json::json!({ "StreamName": name }),
        );
        if summary["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE" {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    kinesis_put(endpoint, &name, orders);
    KinesisStream {
        endpoint: endpoint.to_string(),
        name,
    }
}

fn kinesis_put(endpoint: &str, stream: &str, orders: &[serde_json::Value]) {
    for order in orders {
        let data = base64_standard(order.to_string().as_bytes());
        kinesis_call(
            endpoint,
            "PutRecord",
            serde_json::json!({
                "StreamName": stream, "Data": data,
                "PartitionKey": order["customer_id"].as_str().unwrap_or("none"),
            }),
        );
    }
}

/// Standard base64, for the test's own records.
fn base64_standard(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for (i, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> shift) as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn kinesis_orders(
    workspace: &Path,
    endpoint: &str,
    stream: &str,
    checkpoints: &BTreeMap<String, serde_json::Value>,
    policy: Option<serde_json::Value>,
) -> etl_duckdb_engine::Plan {
    let text = std::fs::read_to_string(repo_root().join("samples/pipelines/kinesis_orders.json"))
        .expect("the sample is committed");
    let mut json: serde_json::Value = serde_json::from_str(&text).unwrap();
    if let Some(policy) = policy {
        json["nodes"][1]["data"]["policy"] = policy;
    }
    let resolver = Resolver::new(workspace)
        .bind("kinesis_endpoint", endpoint)
        .bind("stream", stream)
        .bind("large_stream", &format!("{stream}-large"));
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

fn the_kinesis_sample_carries_on(name: &str, policy: Option<serde_json::Value>) {
    let Some(endpoint) = server("ETL_TEST_KINESIS") else {
        return;
    };
    let Some((workspace, binary)) = workspace(name, &[]) else {
        return;
    };
    let orders = sample_orders();
    let stream = kinesis_stream(&endpoint, name, &orders[..10]);
    // Where the sample puts the large orders: the name kinesis_orders binds.
    let large = KinesisStream {
        endpoint: endpoint.clone(),
        name: format!("{}-large", stream.name),
    };
    kinesis_call(
        &endpoint,
        "CreateStream",
        serde_json::json!({ "StreamName": large.name, "ShardCount": 2 }),
    );
    for _ in 0..100 {
        let summary = kinesis_call(
            &endpoint,
            "DescribeStreamSummary",
            serde_json::json!({ "StreamName": large.name }),
        );
        if summary["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE" {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let first = run(
        &kinesis_orders(
            &workspace,
            &endpoint,
            &stream.name,
            &BTreeMap::new(),
            policy.clone(),
        ),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&first), [Some(10), Some(5), Some(5), Some(5)]);
    assert_eq!(kinesis_records(&endpoint, &large.name), 5);
    assert_eq!(
        query(
            &binary,
            &workspace,
            "SELECT count(*) AS n, count(DISTINCT _shard) <= 2 AS shards, \
             typeof(any_value(_timestamp)) AS t FROM 'samples/out/kinesis_large_orders.parquet';"
        ),
        r#"[{"n":5,"shards":true,"t":"TIMESTAMP"}]"#
    );

    let second = run(
        &kinesis_orders(
            &workspace,
            &endpoint,
            &stream.name,
            &positions(&first),
            policy.clone(),
        ),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&second), [Some(0), Some(0), Some(0), Some(0)]);
    assert_eq!(kinesis_records(&endpoint, &large.name), 5, "nothing new");

    kinesis_put(&endpoint, &stream.name, &orders[10..]);
    let third = run(
        &kinesis_orders(
            &workspace,
            &endpoint,
            &stream.name,
            &positions(&second),
            policy,
        ),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&third)[0], Some(2));
    let put = rows(&third)[3].unwrap() as usize;
    assert_eq!(kinesis_records(&endpoint, &large.name), 5 + put);
}

#[test]
fn the_kinesis_sample_carries_on_between_runs_on_the_one_script_path() {
    the_kinesis_sample_carries_on("kinesis_script", None);
}

#[test]
fn the_kinesis_sample_carries_on_between_runs_on_the_session_path() {
    the_kinesis_sample_carries_on(
        "kinesis_session",
        Some(serde_json::json!({ "retryAttempts": 1 })),
    );
}

#[test]
fn previewing_a_kinesis_source_reads_it() {
    let Some(endpoint) = server("ETL_TEST_KINESIS") else {
        return;
    };
    let Some((workspace, _)) = workspace("kinesis_preview", &[]) else {
        return;
    };
    let stream = kinesis_stream(&endpoint, "preview", &sample_orders()[..9]);
    let plan = kinesis_orders(&workspace, &endpoint, &stream.name, &BTreeMap::new(), None);
    let rows = preview(&plan, "read_orders", 50, &options(&workspace)).expect("previews");
    assert_eq!(rows.rows.len(), 9);
}

// ---------------------------------------------------------------------------
// Amazon SQS, held until the run's outcome is known (Phase 10j), against ElasticMQ
// ---------------------------------------------------------------------------

/// One SQS call to the test server, which accepts any signature.
fn sqs_call(endpoint: &str, target: &str, body: serde_json::Value) -> serde_json::Value {
    let mut response = ureq::post(format!("{endpoint}/"))
        .header("Content-Type", "application/x-amz-json-1.0")
        .header("X-Amz-Target", format!("AmazonSQS.{target}"))
        .header("X-Amz-Date", "20260101T000000Z")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/sqs/aws4_request, \
             SignedHeaders=host, Signature=0",
        )
        .send(body.to_string().as_bytes())
        .unwrap_or_else(|error| panic!("{target}: {error}"));
    let text = response.body_mut().read_to_string().unwrap();
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

/// A queue for one test, deleted when dropped.
struct SqsQueue {
    endpoint: String,
    name: String,
    url: String,
}

impl Drop for SqsQueue {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(|| {
            sqs_call(
                &self.endpoint,
                "DeleteQueue",
                serde_json::json!({ "QueueUrl": self.url }),
            )
        });
    }
}

fn sqs_queue(endpoint: &str, name: &str) -> SqsQueue {
    let answer = sqs_call(
        endpoint,
        "CreateQueue",
        serde_json::json!({ "QueueName": name }),
    );
    SqsQueue {
        endpoint: endpoint.to_string(),
        name: name.to_string(),
        url: answer["QueueUrl"].as_str().unwrap().to_string(),
    }
}

/// An orders queue holding `orders`, and an empty one for the large orders,
/// named as `sqs_orders` binds them.
fn sqs_queues(endpoint: &str, test: &str, orders: &[serde_json::Value]) -> (SqsQueue, SqsQueue) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let name = format!("etl-verified-{test}-{nanos}");
    let queue = sqs_queue(endpoint, &name);
    let large = sqs_queue(endpoint, &format!("{name}-large"));
    for chunk in orders.chunks(10) {
        let entries: Vec<serde_json::Value> = chunk
            .iter()
            .enumerate()
            .map(|(i, order)| {
                serde_json::json!({ "Id": i.to_string(), "MessageBody": order.to_string() })
            })
            .collect();
        sqs_call(
            endpoint,
            "SendMessageBatch",
            serde_json::json!({ "QueueUrl": queue.url, "Entries": entries }),
        );
    }
    (queue, large)
}

/// (visible, hidden) messages in a queue: waiting, and received but not yet
/// deleted or given back.
fn sqs_counts(queue: &SqsQueue) -> (u64, u64) {
    let answer = sqs_call(
        &queue.endpoint,
        "GetQueueAttributes",
        serde_json::json!({ "QueueUrl": queue.url, "AttributeNames": ["All"] }),
    );
    let count = |key: &str| {
        answer["Attributes"][key]
            .as_str()
            .and_then(|n| n.parse().ok())
            .unwrap_or(0)
    };
    (
        count("ApproximateNumberOfMessages"),
        count("ApproximateNumberOfMessagesNotVisible"),
    )
}

fn sqs_orders(
    workspace: &Path,
    endpoint: &str,
    queue: &SqsQueue,
    policy: Option<serde_json::Value>,
) -> etl_duckdb_engine::Plan {
    let text = std::fs::read_to_string(repo_root().join("samples/pipelines/sqs_orders.json"))
        .expect("the sample is committed");
    let mut json: serde_json::Value = serde_json::from_str(&text).unwrap();
    if let Some(policy) = policy {
        json["nodes"][1]["data"]["policy"] = policy;
    }
    let resolver = Resolver::new(workspace)
        .bind("sqs_endpoint", endpoint)
        .bind("queue", &queue.name)
        .bind("large_queue", &format!("{}-large", queue.name));
    let resolved = resolve(&document(&json.to_string()), &resolver).expect("resolves");
    compile_with(&resolved.document, &CompileOptions::default()).expect("compiles")
}

fn the_sqs_sample_takes_what_it_read(name: &str, policy: Option<serde_json::Value>) {
    let Some(endpoint) = server("ETL_TEST_SQS") else {
        return;
    };
    let Some((workspace, binary)) = workspace(name, &[]) else {
        return;
    };
    let (queue, large) = sqs_queues(&endpoint, name, &sample_orders());

    let first = run(
        &sqs_orders(&workspace, &endpoint, &queue, policy.clone()),
        &options(&workspace),
    )
    .expect("runs");
    let put = rows(&first)[3].expect("the large orders were sent");
    assert_eq!(rows(&first)[0], Some(12));
    assert_eq!(rows(&first)[1], Some(put));
    assert!(first.warnings.is_empty(), "{:?}", first.warnings);
    let acknowledged = format!(
        "Orders queue: 12 message(s) deleted from queue '{}'",
        queue.name
    );
    assert!(
        first.notes.iter().any(|note| note == &acknowledged),
        "{:?}",
        first.notes
    );
    assert_eq!(sqs_counts(&queue), (0, 0), "acknowledged: gone");
    assert_eq!(sqs_counts(&large), (put, 0));
    assert_eq!(
        query(
            &binary,
            &workspace,
            "SELECT count(*) AS n, min(_receive_count) AS r \
             FROM 'samples/out/sqs_large_orders.parquet';"
        ),
        format!(r#"[{{"n":{put},"r":1}}]"#)
    );

    let second = run(
        &sqs_orders(&workspace, &endpoint, &queue, policy),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&second), [Some(0), Some(0), Some(0), Some(0)]);
}

#[test]
fn the_sqs_sample_takes_what_it_read_on_the_one_script_path() {
    the_sqs_sample_takes_what_it_read("sqs_script", None);
}

#[test]
fn the_sqs_sample_takes_what_it_read_on_the_session_path() {
    the_sqs_sample_takes_what_it_read(
        "sqs_session",
        Some(serde_json::json!({ "retryAttempts": 1 })),
    );
}

/// A pipeline reading `queue` into a filter that fails at run time.
fn sqs_broken(endpoint: &str, queue: &str, policy: &str) -> PipelineDoc {
    document(&format!(
        r#"{{ "formatVersion": 1, "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "Queue",
               "componentId": "src.queue.sqs",
               "properties": {{ "queue": "{queue}", "endpoint": "{endpoint}", "region": "us-east-1",
                                "access_key_id": "test", "secret_access_key": "test" }} }} }},
            {{ "id": "broken", "position": {{"x":0,"y":0}}, "data": {{ "label": "Broken",
               "componentId": "xf.filter", "properties": {{ "predicate": "no_such_column > 1" }}
               {policy} }} }},
            {{ "id": "out", "position": {{"x":0,"y":0}}, "data": {{ "label": "Out",
               "componentId": "snk.file.parquet", "properties": {{ "path": "out.parquet" }} }} }}
          ],
          "edges": [ {{ "id": "e1", "source": "read", "target": "broken" }},
                     {{ "id": "e2", "source": "broken", "target": "out" }} ] }}"#
    ))
}

#[test]
fn a_failed_sqs_run_gives_every_message_back() {
    let Some(endpoint) = server("ETL_TEST_SQS") else {
        return;
    };
    let Some((workspace, _)) = workspace("sqs_failed", &[]) else {
        return;
    };
    let (queue, _large) = sqs_queues(&endpoint, "failed", &sample_orders()[..6]);

    // One script: the failure is an error, and the messages are back at once,
    // not after their visibility timeout.
    let plan = compile_with(
        &sqs_broken(&endpoint, &queue.name, ""),
        &CompileOptions::default(),
    )
    .unwrap();
    assert!(run(&plan, &options(&workspace)).is_err());
    assert_eq!(sqs_counts(&queue), (6, 0), "released");

    // Carrying on past the failure: still a failed run, still released, and
    // the report says so.
    let plan = compile_with(
        &sqs_broken(
            &endpoint,
            &queue.name,
            r#", "policy": { "continueOnFailure": true }"#,
        ),
        &CompileOptions::default(),
    )
    .unwrap();
    let report = run(&plan, &options(&workspace)).expect("a report, not an error");
    assert!(report.failed());
    assert_eq!(report.stages[0].rows, Some(6), "the source did read them");
    let released = format!(
        "Queue: 6 message(s) released back to queue '{}'",
        queue.name
    );
    assert!(
        report.notes.iter().any(|note| note == &released),
        "{:?}",
        report.notes
    );
    assert_eq!(sqs_counts(&queue), (6, 0), "released again");
}

#[test]
fn previewing_an_sqs_source_gives_everything_back() {
    let Some(endpoint) = server("ETL_TEST_SQS") else {
        return;
    };
    let Some((workspace, _)) = workspace("sqs_preview", &[]) else {
        return;
    };
    let (queue, _large) = sqs_queues(&endpoint, "preview", &sample_orders()[..9]);
    let plan = sqs_orders(&workspace, &endpoint, &queue, None);
    let shown = preview(&plan, "read_orders", 50, &options(&workspace)).expect("previews");
    assert_eq!(shown.rows.len(), 9);
    assert_eq!(
        sqs_counts(&queue),
        (9, 0),
        "a preview is a look, not a take"
    );
}

// ---------------------------------------------------------------------------
// Google Pub/Sub, held until the run's outcome is known (Phase 10k), against its emulator
// ---------------------------------------------------------------------------

/// The project the emulator tests work in; the emulator takes any.
const PUBSUB_PROJECT: &str = "etl-test";

/// One call to the emulator, which checks no sign-in: `PUT` to create,
/// `POST` for a verb, `DELETE` to remove.
fn pubsub_call(
    endpoint: &str,
    method: &str,
    path: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let url = format!("{endpoint}/v1/projects/{PUBSUB_PROJECT}/{path}");
    let sent = match method {
        "PUT" => ureq::put(&url)
            .header("Content-Type", "application/json")
            .send(body.to_string().as_bytes()),
        "POST" => ureq::post(&url)
            .header("Content-Type", "application/json")
            .send(body.to_string().as_bytes()),
        _ => ureq::delete(&url).call(),
    };
    let mut response = sent.unwrap_or_else(|error| panic!("{method} {path}: {error}"));
    let text = response.body_mut().read_to_string().unwrap();
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

/// A topic and its subscription, `<name>-sub`, deleted when dropped.
struct PubsubTopic {
    endpoint: String,
    name: String,
}

impl PubsubTopic {
    fn new(endpoint: &str, name: &str) -> Self {
        pubsub_call(
            endpoint,
            "PUT",
            &format!("topics/{name}"),
            serde_json::json!({}),
        );
        pubsub_call(
            endpoint,
            "PUT",
            &format!("subscriptions/{name}-sub"),
            serde_json::json!({
                "topic": format!("projects/{PUBSUB_PROJECT}/topics/{name}"),
                "ackDeadlineSeconds": 10,
            }),
        );
        PubsubTopic {
            endpoint: endpoint.to_string(),
            name: name.to_string(),
        }
    }

    /// How many messages the subscription would hand out now. Each is pulled,
    /// counted and given straight back.
    fn waiting(&self) -> u64 {
        let subscription = format!("subscriptions/{}-sub", self.name);
        let mut ids: Vec<serde_json::Value> = Vec::new();
        loop {
            let answer = pubsub_call(
                &self.endpoint,
                "POST",
                &format!("{subscription}:pull"),
                serde_json::json!({ "maxMessages": 1000, "returnImmediately": true }),
            );
            let got: Vec<serde_json::Value> = answer["receivedMessages"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .iter()
                .map(|message| message["ackId"].clone())
                .collect();
            if got.is_empty() {
                break;
            }
            ids.extend(got);
        }
        if !ids.is_empty() {
            pubsub_call(
                &self.endpoint,
                "POST",
                &format!("{subscription}:modifyAckDeadline"),
                serde_json::json!({ "ackIds": ids, "ackDeadlineSeconds": 0 }),
            );
        }
        ids.len() as u64
    }
}

impl Drop for PubsubTopic {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(|| {
            let subscription = format!("subscriptions/{}-sub", self.name);
            let topic = format!("topics/{}", self.name);
            pubsub_call(
                &self.endpoint,
                "DELETE",
                &subscription,
                serde_json::Value::Null,
            );
            pubsub_call(&self.endpoint, "DELETE", &topic, serde_json::Value::Null);
        });
    }
}

/// Standard base64, to publish with.
fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for (index, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            if index <= chunk.len() {
                out.push(ALPHABET[(n >> shift) as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// An orders topic holding `orders`, and an empty one for the large orders,
/// named as `pubsub_orders` binds them.
fn pubsub_topics(
    endpoint: &str,
    test: &str,
    orders: &[serde_json::Value],
) -> (PubsubTopic, PubsubTopic) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let name = format!("etl-verified-{test}-{nanos}");
    let topic = PubsubTopic::new(endpoint, &name);
    let large = PubsubTopic::new(endpoint, &format!("{name}-large"));
    let messages: Vec<serde_json::Value> = orders
        .iter()
        .map(|order| serde_json::json!({ "data": base64_encode(order.to_string().as_bytes()) }))
        .collect();
    pubsub_call(
        endpoint,
        "POST",
        &format!("topics/{name}:publish"),
        serde_json::json!({ "messages": messages }),
    );
    (topic, large)
}

fn pubsub_orders(
    workspace: &Path,
    endpoint: &str,
    topic: &PubsubTopic,
    policy: Option<serde_json::Value>,
) -> etl_duckdb_engine::Plan {
    let text = std::fs::read_to_string(repo_root().join("samples/pipelines/pubsub_orders.json"))
        .expect("the sample is committed");
    let mut json: serde_json::Value = serde_json::from_str(&text).unwrap();
    if let Some(policy) = policy {
        json["nodes"][1]["data"]["policy"] = policy;
    }
    let resolver = Resolver::new(workspace)
        .bind("pubsub_endpoint", endpoint)
        .bind("subscription", &format!("{}-sub", topic.name))
        .bind("large_topic", &format!("{}-large", topic.name));
    let resolved = resolve(&document(&json.to_string()), &resolver).expect("resolves");
    compile_with(&resolved.document, &CompileOptions::default()).expect("compiles")
}

fn the_pubsub_sample_takes_what_it_read(name: &str, policy: Option<serde_json::Value>) {
    let Some(endpoint) = server("ETL_TEST_PUBSUB") else {
        return;
    };
    let Some((workspace, binary)) = workspace(name, &[]) else {
        return;
    };
    let (topic, large) = pubsub_topics(&endpoint, name, &sample_orders());

    let first = run(
        &pubsub_orders(&workspace, &endpoint, &topic, policy.clone()),
        &options(&workspace),
    )
    .expect("runs");
    let put = rows(&first)[3].expect("the large orders were published");
    assert_eq!(rows(&first)[0], Some(12));
    assert_eq!(rows(&first)[1], Some(put));
    assert!(first.warnings.is_empty(), "{:?}", first.warnings);
    let acknowledged = format!(
        "Orders subscription: 12 message(s) acknowledged on subscription '{}-sub'",
        topic.name
    );
    assert!(
        first.notes.iter().any(|note| note == &acknowledged),
        "{:?}",
        first.notes
    );
    assert_eq!(topic.waiting(), 0, "acknowledged: gone");
    assert_eq!(large.waiting(), put);
    assert_eq!(
        query(
            &binary,
            &workspace,
            "SELECT count(*) AS n, count(DISTINCT _message_id) AS m, \
                    min(_publish_time) IS NOT NULL AS t \
             FROM 'samples/out/pubsub_large_orders.parquet';"
        ),
        format!(r#"[{{"n":{put},"m":{put},"t":true}}]"#)
    );

    let second = run(
        &pubsub_orders(&workspace, &endpoint, &topic, policy),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&second), [Some(0), Some(0), Some(0), Some(0)]);
}

#[test]
fn the_pubsub_sample_takes_what_it_read_on_the_one_script_path() {
    the_pubsub_sample_takes_what_it_read("pubsub_script", None);
}

#[test]
fn the_pubsub_sample_takes_what_it_read_on_the_session_path() {
    the_pubsub_sample_takes_what_it_read(
        "pubsub_session",
        Some(serde_json::json!({ "retryAttempts": 1 })),
    );
}

/// A pipeline reading `subscription` into a filter that fails at run time.
fn pubsub_broken(endpoint: &str, subscription: &str, policy: &str) -> PipelineDoc {
    document(&format!(
        r#"{{ "formatVersion": 1, "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "Subscription",
               "componentId": "src.queue.pubsub",
               "properties": {{ "subscription": "{subscription}", "project": "{PUBSUB_PROJECT}",
                                "endpoint": "{endpoint}" }} }} }},
            {{ "id": "broken", "position": {{"x":0,"y":0}}, "data": {{ "label": "Broken",
               "componentId": "xf.filter", "properties": {{ "predicate": "no_such_column > 1" }}
               {policy} }} }},
            {{ "id": "out", "position": {{"x":0,"y":0}}, "data": {{ "label": "Out",
               "componentId": "snk.file.parquet", "properties": {{ "path": "out.parquet" }} }} }}
          ],
          "edges": [ {{ "id": "e1", "source": "read", "target": "broken" }},
                     {{ "id": "e2", "source": "broken", "target": "out" }} ] }}"#
    ))
}

#[test]
fn a_failed_pubsub_run_gives_every_message_back() {
    let Some(endpoint) = server("ETL_TEST_PUBSUB") else {
        return;
    };
    let Some((workspace, _)) = workspace("pubsub_failed", &[]) else {
        return;
    };
    let (topic, _large) = pubsub_topics(&endpoint, "failed", &sample_orders()[..6]);
    let subscription = format!("{}-sub", topic.name);

    // One script: the failure is an error, and the messages are back at once,
    // not after their ack deadline.
    let plan = compile_with(
        &pubsub_broken(&endpoint, &subscription, ""),
        &CompileOptions::default(),
    )
    .unwrap();
    assert!(run(&plan, &options(&workspace)).is_err());
    assert_eq!(topic.waiting(), 6, "released");

    // Carrying on past the failure: still a failed run, still released, and
    // the report says so.
    let plan = compile_with(
        &pubsub_broken(
            &endpoint,
            &subscription,
            r#", "policy": { "continueOnFailure": true }"#,
        ),
        &CompileOptions::default(),
    )
    .unwrap();
    let report = run(&plan, &options(&workspace)).expect("a report, not an error");
    assert!(report.failed());
    assert_eq!(report.stages[0].rows, Some(6), "the source did read them");
    let released =
        format!("Subscription: 6 message(s) released back to subscription '{subscription}'");
    assert!(
        report.notes.iter().any(|note| note == &released),
        "{:?}",
        report.notes
    );
    assert_eq!(topic.waiting(), 6, "released again");
}

#[test]
fn previewing_a_pubsub_source_gives_everything_back() {
    let Some(endpoint) = server("ETL_TEST_PUBSUB") else {
        return;
    };
    let Some((workspace, _)) = workspace("pubsub_preview", &[]) else {
        return;
    };
    let (topic, _large) = pubsub_topics(&endpoint, "preview", &sample_orders()[..9]);
    let plan = pubsub_orders(&workspace, &endpoint, &topic, None);
    let shown = preview(&plan, "read_orders", 50, &options(&workspace)).expect("previews");
    assert_eq!(shown.rows.len(), 9);
    assert_eq!(topic.waiting(), 9, "a preview is a look, not a take");
}

// ---------------------------------------------------------------------------
// RabbitMQ, held until the run's outcome is known (Phase 10l)
// ---------------------------------------------------------------------------

/// One call to the test broker's management API, as its test user.
fn rabbitmq_http(
    http: &str,
    method: &str,
    path: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let url = format!("{http}/api/{path}");
    let auth = "Basic ZXRsOmV0bC1zZWNyZXQ="; // etl:etl-secret
    let sent = match method {
        "PUT" => ureq::put(&url)
            .header("Authorization", auth)
            .header("Content-Type", "application/json")
            .send(body.to_string().as_bytes()),
        "POST" => ureq::post(&url)
            .header("Authorization", auth)
            .header("Content-Type", "application/json")
            .send(body.to_string().as_bytes()),
        _ => ureq::delete(&url).header("Authorization", auth).call(),
    };
    let mut response = sent.unwrap_or_else(|error| panic!("{method} {path}: {error}"));
    let text = response.body_mut().read_to_string().unwrap();
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

/// A durable queue in the default vhost, deleted when dropped.
struct RabbitQueue {
    http: String,
    name: String,
}

impl RabbitQueue {
    fn new(http: &str, name: &str) -> Self {
        rabbitmq_http(
            http,
            "PUT",
            &format!("queues/%2F/{name}"),
            serde_json::json!({ "durable": true }),
        );
        RabbitQueue {
            http: http.to_string(),
            name: name.to_string(),
        }
    }

    /// How many messages are waiting: each is taken, counted and put straight
    /// back. Those a run is holding are not waiting.
    fn waiting(&self) -> u64 {
        let got = rabbitmq_http(
            &self.http,
            "POST",
            &format!("queues/%2F/{}/get", self.name),
            serde_json::json!({ "count": 1000, "ackmode": "reject_requeue_true", "encoding": "auto" }),
        );
        got.as_array().map_or(0, |messages| messages.len() as u64)
    }
}

impl Drop for RabbitQueue {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(|| {
            rabbitmq_http(
                &self.http,
                "DELETE",
                &format!("queues/%2F/{}", self.name),
                serde_json::Value::Null,
            )
        });
    }
}

/// The broker's AMQP URL and its management API, or `None` to skip.
fn rabbitmq() -> Option<(String, String)> {
    Some((
        server("ETL_TEST_RABBITMQ")?,
        server("ETL_TEST_RABBITMQ_HTTP")?,
    ))
}

/// An orders queue holding `orders`, and an empty one for the large orders,
/// named as `rabbitmq_orders` binds them.
fn rabbitmq_queues(
    http: &str,
    test: &str,
    orders: &[serde_json::Value],
) -> (RabbitQueue, RabbitQueue) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let name = format!("etl-verified-{test}-{nanos}");
    let queue = RabbitQueue::new(http, &name);
    let large = RabbitQueue::new(http, &format!("{name}-large"));
    for order in orders {
        let routed = rabbitmq_http(
            http,
            "POST",
            "exchanges/%2F/amq.default/publish",
            serde_json::json!({
                "properties": {}, "routing_key": name,
                "payload": order.to_string(), "payload_encoding": "string",
            }),
        );
        assert_eq!(routed["routed"], true, "{routed}");
    }
    (queue, large)
}

fn rabbitmq_orders(
    workspace: &Path,
    url: &str,
    queue: &RabbitQueue,
    policy: Option<serde_json::Value>,
) -> etl_duckdb_engine::Plan {
    let text = std::fs::read_to_string(repo_root().join("samples/pipelines/rabbitmq_orders.json"))
        .expect("the sample is committed");
    let mut json: serde_json::Value = serde_json::from_str(&text).unwrap();
    if let Some(policy) = policy {
        json["nodes"][1]["data"]["policy"] = policy;
    }
    let resolver = Resolver::new(workspace)
        .bind("rabbitmq_url", url)
        .bind("queue", &queue.name)
        .bind("large_queue", &format!("{}-large", queue.name));
    let resolved = resolve(&document(&json.to_string()), &resolver).expect("resolves");
    compile_with(&resolved.document, &CompileOptions::default()).expect("compiles")
}

fn the_rabbitmq_sample_takes_what_it_read(name: &str, policy: Option<serde_json::Value>) {
    let Some((url, http)) = rabbitmq() else {
        return;
    };
    let Some((workspace, binary)) = workspace(name, &[]) else {
        return;
    };
    let (queue, large) = rabbitmq_queues(&http, name, &sample_orders());

    let first = run(
        &rabbitmq_orders(&workspace, &url, &queue, policy.clone()),
        &options(&workspace),
    )
    .expect("runs");
    let put = rows(&first)[3].expect("the large orders were published");
    assert_eq!(rows(&first)[0], Some(12));
    assert_eq!(rows(&first)[1], Some(put));
    assert!(first.warnings.is_empty(), "{:?}", first.warnings);
    let acknowledged = format!(
        "Orders queue: 12 message(s) acknowledged on queue '{}'",
        queue.name
    );
    assert!(
        first.notes.iter().any(|note| note == &acknowledged),
        "{:?}",
        first.notes
    );
    assert_eq!(queue.waiting(), 0, "acknowledged: gone");
    assert_eq!(large.waiting(), put);
    assert_eq!(
        query(
            &binary,
            &workspace,
            "SELECT count(*) AS n, bool_or(_redelivered) AS r, min(_routing_key) AS k \
             FROM 'samples/out/rabbitmq_large_orders.parquet';"
        ),
        format!(r#"[{{"n":{put},"r":false,"k":"{}"}}]"#, queue.name)
    );

    let second = run(
        &rabbitmq_orders(&workspace, &url, &queue, policy),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&second), [Some(0), Some(0), Some(0), Some(0)]);
}

#[test]
fn the_rabbitmq_sample_takes_what_it_read_on_the_one_script_path() {
    the_rabbitmq_sample_takes_what_it_read("rabbitmq_script", None);
}

#[test]
fn the_rabbitmq_sample_takes_what_it_read_on_the_session_path() {
    the_rabbitmq_sample_takes_what_it_read(
        "rabbitmq_session",
        Some(serde_json::json!({ "retryAttempts": 1 })),
    );
}

/// A pipeline reading `queue` into a filter that fails at run time.
fn rabbitmq_broken(url: &str, queue: &str, policy: &str) -> PipelineDoc {
    document(&format!(
        r#"{{ "formatVersion": 1, "nodes": [
            {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "Queue",
               "componentId": "src.queue.rabbitmq",
               "properties": {{ "queue": "{queue}", "url": "{url}" }} }} }},
            {{ "id": "broken", "position": {{"x":0,"y":0}}, "data": {{ "label": "Broken",
               "componentId": "xf.filter", "properties": {{ "predicate": "no_such_column > 1" }}
               {policy} }} }},
            {{ "id": "out", "position": {{"x":0,"y":0}}, "data": {{ "label": "Out",
               "componentId": "snk.file.parquet", "properties": {{ "path": "out.parquet" }} }} }}
          ],
          "edges": [ {{ "id": "e1", "source": "read", "target": "broken" }},
                     {{ "id": "e2", "source": "broken", "target": "out" }} ] }}"#
    ))
}

#[test]
fn a_failed_rabbitmq_run_gives_every_message_back() {
    let Some((url, http)) = rabbitmq() else {
        return;
    };
    let Some((workspace, _)) = workspace("rabbitmq_failed", &[]) else {
        return;
    };
    let (queue, _large) = rabbitmq_queues(&http, "failed", &sample_orders()[..6]);

    // One script: the failure is an error, and the messages are back at once.
    let plan = compile_with(
        &rabbitmq_broken(&url, &queue.name, ""),
        &CompileOptions::default(),
    )
    .unwrap();
    assert!(run(&plan, &options(&workspace)).is_err());
    assert_eq!(queue.waiting(), 6, "released");

    // Carrying on past the failure: still a failed run, still released, and
    // the report says so.
    let plan = compile_with(
        &rabbitmq_broken(
            &url,
            &queue.name,
            r#", "policy": { "continueOnFailure": true }"#,
        ),
        &CompileOptions::default(),
    )
    .unwrap();
    let report = run(&plan, &options(&workspace)).expect("a report, not an error");
    assert!(report.failed());
    assert_eq!(report.stages[0].rows, Some(6), "the source did read them");
    let released = format!(
        "Queue: 6 message(s) released back to queue '{}'",
        queue.name
    );
    assert!(
        report.notes.iter().any(|note| note == &released),
        "{:?}",
        report.notes
    );
    assert_eq!(queue.waiting(), 6, "released again");
}

#[test]
fn previewing_a_rabbitmq_source_gives_everything_back() {
    let Some((url, http)) = rabbitmq() else {
        return;
    };
    let Some((workspace, _)) = workspace("rabbitmq_preview", &[]) else {
        return;
    };
    let (queue, _large) = rabbitmq_queues(&http, "preview", &sample_orders()[..9]);
    let plan = rabbitmq_orders(&workspace, &url, &queue, None);
    let shown = preview(&plan, "read_orders", 50, &options(&workspace)).expect("previews");
    assert_eq!(shown.rows.len(), 9);
    assert_eq!(queue.waiting(), 9, "a preview is a look, not a take");
}

// ---------------------------------------------------------------------------
// MongoDB, incremental by a checkpoint (Phase 10m)
// ---------------------------------------------------------------------------

/// A database for one test, dropped when dropped.
struct MongoDatabase {
    uri: String,
    name: String,
}

impl MongoDatabase {
    fn new(uri: &str, test: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        MongoDatabase {
            uri: uri.to_string(),
            name: format!("etl_verified_{test}_{nanos}"),
        }
    }

    fn collection(&self, name: &str) -> mongodb::sync::Collection<mongodb::bson::Document> {
        mongodb::sync::Client::with_uri_str(&self.uri)
            .unwrap()
            .database(&self.name)
            .collection(name)
    }

    /// Orders as MongoDB would hold them: a real date, a decimal amount.
    fn insert_orders(&self, orders: &[serde_json::Value]) {
        use mongodb::bson::{doc, DateTime, Decimal128};
        let documents: Vec<_> = orders
            .iter()
            .map(|order| {
                let at = order["order_ts"].as_str().unwrap().replace(' ', "T") + "Z";
                doc! {
                    "order_id": order["order_id"].as_i64().unwrap(),
                    "customer_id": order["customer_id"].as_str().unwrap(),
                    "order_ts": DateTime::parse_rfc3339_str(&at).unwrap(),
                    "amount": format!("{:.2}", order["amount"].as_f64().unwrap())
                        .parse::<Decimal128>()
                        .unwrap(),
                    "status": order["status"].as_str().unwrap(),
                }
            })
            .collect();
        self.collection("orders")
            .insert_many(documents)
            .run()
            .unwrap();
    }

    fn count(&self, collection: &str) -> u64 {
        self.collection(collection)
            .count_documents(mongodb::bson::doc! {})
            .run()
            .unwrap()
    }
}

impl Drop for MongoDatabase {
    fn drop(&mut self) {
        if let Ok(client) = mongodb::sync::Client::with_uri_str(&self.uri) {
            let _ = client.database(&self.name).drop().run();
        }
    }
}

fn mongodb_orders(
    workspace: &Path,
    uri: &str,
    database: &MongoDatabase,
    checkpoints: &BTreeMap<String, serde_json::Value>,
    policy: Option<serde_json::Value>,
) -> etl_duckdb_engine::Plan {
    let text = std::fs::read_to_string(repo_root().join("samples/pipelines/mongodb_orders.json"))
        .expect("the sample is committed");
    let mut json: serde_json::Value = serde_json::from_str(&text).unwrap();
    if let Some(policy) = policy {
        json["nodes"][1]["data"]["policy"] = policy;
    }
    let resolver = Resolver::new(workspace)
        .bind("mongodb_uri", uri)
        .bind("database", &database.name);
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

/// The sample, three runs: everything, nothing new, then only what arrived.
fn the_mongodb_sample_carries_on(name: &str, policy: Option<serde_json::Value>) {
    let Some(uri) = server("ETL_TEST_MONGODB") else {
        return;
    };
    let Some((workspace, binary)) = workspace(name, &[]) else {
        return;
    };
    let orders = sample_orders();
    let database = MongoDatabase::new(&uri, name);
    database.insert_orders(&orders[..10]);

    let first = run(
        &mongodb_orders(
            &workspace,
            &uri,
            &database,
            &BTreeMap::new(),
            policy.clone(),
        ),
        &options(&workspace),
    )
    .expect("runs");
    let put = rows(&first)[3].expect("the large orders were written");
    assert_eq!(rows(&first)[0], Some(10));
    assert_eq!(database.count("large_orders"), put);
    let saved = positions(&first);
    let highest = orders[..10]
        .iter()
        .map(|order| order["order_id"].as_i64().unwrap())
        .max()
        .unwrap();
    assert_eq!(
        saved["read_orders"]["value"],
        serde_json::json!({ "$numberLong": highest.to_string() }),
        "{saved:?}"
    );
    assert_eq!(
        query(
            &binary,
            &workspace,
            "SELECT count(*) AS n, typeof(any_value(order_ts)) AS t, \
                    typeof(any_value(amount)) AS a, min(length(_id)) AS id \
             FROM 'samples/out/mongodb_large_orders.parquet';"
        ),
        format!(r#"[{{"n":{put},"t":"TIMESTAMP","a":"DECIMAL(10,2)","id":24}}]"#)
    );

    // Nothing new: the saved position is the end, and nothing is written twice.
    let second = run(
        &mongodb_orders(&workspace, &uri, &database, &saved, policy.clone()),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&second), [Some(0), Some(0), Some(0), Some(0)]);

    // Two more orders arrive: exactly those are read.
    database.insert_orders(&orders[10..]);
    let third = run(
        &mongodb_orders(&workspace, &uri, &database, &saved, policy),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&third)[0], Some(2));
}

#[test]
fn the_mongodb_sample_carries_on_between_runs_on_the_one_script_path() {
    the_mongodb_sample_carries_on("mongodb_script", None);
}

#[test]
fn the_mongodb_sample_carries_on_between_runs_on_the_session_path() {
    the_mongodb_sample_carries_on(
        "mongodb_session",
        Some(serde_json::json!({ "retryAttempts": 1 })),
    );
}

#[test]
fn a_failed_mongodb_run_saves_no_position() {
    let Some(uri) = server("ETL_TEST_MONGODB") else {
        return;
    };
    let Some((workspace, _)) = workspace("mongodb_failed", &[]) else {
        return;
    };
    let database = MongoDatabase::new(&uri, "failed");
    database.insert_orders(&sample_orders()[..6]);

    let mut json: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(repo_root().join("samples/pipelines/mongodb_orders.json"))
            .unwrap(),
    )
    .unwrap();
    json["nodes"][1]["data"]["properties"]["predicate"] = serde_json::json!("no_such_column > 1");
    json["nodes"][1]["data"]["policy"] = serde_json::json!({ "continueOnFailure": true });
    let resolver = Resolver::new(&workspace)
        .bind("mongodb_uri", &uri)
        .bind("database", &database.name);
    let resolved = resolve(&document(&json.to_string()), &resolver).expect("resolves");
    let plan = compile_with(&resolved.document, &CompileOptions::default()).unwrap();

    let report = run(&plan, &options(&workspace)).expect("a report, not an error");
    assert!(report.failed());
    assert_eq!(report.stages[0].rows, Some(6), "the source did read them");
    assert!(
        report.checkpoints.is_empty(),
        "a failed run moves no position: {:?}",
        report.checkpoints
    );
    assert_eq!(database.count("large_orders"), 0, "and delivers nothing");
}

#[test]
fn previewing_a_mongodb_source_reads_without_remembering() {
    let Some(uri) = server("ETL_TEST_MONGODB") else {
        return;
    };
    let Some((workspace, _)) = workspace("mongodb_preview", &[]) else {
        return;
    };
    let database = MongoDatabase::new(&uri, "preview");
    database.insert_orders(&sample_orders()[..9]);
    let plan = mongodb_orders(&workspace, &uri, &database, &BTreeMap::new(), None);
    let shown = preview(&plan, "read_orders", 50, &options(&workspace)).expect("previews");
    assert_eq!(shown.rows.len(), 9);
    assert_eq!(
        database.count("large_orders"),
        0,
        "a preview writes nothing"
    );
}

// ---------------------------------------------------------------------------
// BigQuery, incremental by a checkpoint, against the emulator (Phase 10o)
// ---------------------------------------------------------------------------

/// The project the emulator was started with.
const BIGQUERY_PROJECT: &str = "etl-test";

/// A dataset with empty `orders` and `large_orders` tables, deleted when dropped.
struct BigqueryDataset {
    endpoint: String,
    name: String,
}

impl BigqueryDataset {
    fn new(endpoint: &str, test: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let name = format!("etl_verified_{test}_{nanos}");
        let base = format!("{endpoint}/bigquery/v2/projects/{BIGQUERY_PROJECT}");
        let post = |url: String, body: serde_json::Value| {
            ureq::post(&url)
                .header("Content-Type", "application/json")
                .send(body.to_string().as_bytes())
                .unwrap_or_else(|error| panic!("{url}: {error}"));
        };
        post(
            format!("{base}/datasets"),
            serde_json::json!({ "datasetReference": { "projectId": BIGQUERY_PROJECT, "datasetId": name } }),
        );
        for table in ["orders", "large_orders"] {
            post(
                format!("{base}/datasets/{name}/tables"),
                serde_json::json!({
                    "tableReference": { "projectId": BIGQUERY_PROJECT, "datasetId": name, "tableId": table },
                    "schema": { "fields": [
                        { "name": "order_id", "type": "INT64" },
                        { "name": "customer_id", "type": "STRING" },
                        { "name": "order_ts", "type": "TIMESTAMP" },
                        { "name": "amount", "type": "NUMERIC" },
                        { "name": "status", "type": "STRING" },
                    ]},
                }),
            );
        }
        BigqueryDataset {
            endpoint: endpoint.to_string(),
            name,
        }
    }

    fn properties(&self, table: &str) -> serde_json::Value {
        serde_json::json!({
            "project": BIGQUERY_PROJECT, "dataset": self.name, "table": table,
            "endpoint": self.endpoint,
        })
    }

    /// Orders into `orders`, through the connector's own load job.
    fn insert_orders(&self, orders: &[serde_json::Value]) {
        use etl_plugin_sdk::Sink;
        let rows: Vec<etl_plugin_sdk::Record> = orders
            .iter()
            .map(|order| order.as_object().unwrap().clone())
            .collect();
        etl_connectors::bigquery::BigquerySink
            .write(
                &self.properties("orders"),
                &mut etl_plugin_sdk::Records(rows.into_iter()),
                &etl_plugin_sdk::Context::default(),
            )
            .expect("loads");
    }

    fn count(&self, table: &str) -> u64 {
        let mut response = ureq::post(&format!(
            "{}/bigquery/v2/projects/{BIGQUERY_PROJECT}/queries",
            self.endpoint
        ))
        .header("Content-Type", "application/json")
        .send(
            serde_json::json!({
                "query": format!("SELECT count(*) FROM `{BIGQUERY_PROJECT}.{}.{table}`", self.name),
                "useLegacySql": false,
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap();
        let answer: serde_json::Value =
            serde_json::from_str(&response.body_mut().read_to_string().unwrap()).unwrap();
        answer["rows"][0]["f"][0]["v"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap()
    }
}

impl Drop for BigqueryDataset {
    fn drop(&mut self) {
        let _ = ureq::delete(&format!(
            "{}/bigquery/v2/projects/{BIGQUERY_PROJECT}/datasets/{}?deleteContents=true",
            self.endpoint, self.name
        ))
        .call();
    }
}

fn bigquery_orders(
    workspace: &Path,
    endpoint: &str,
    dataset: &BigqueryDataset,
    checkpoints: &BTreeMap<String, serde_json::Value>,
    policy: Option<serde_json::Value>,
) -> etl_duckdb_engine::Plan {
    let text = std::fs::read_to_string(repo_root().join("samples/pipelines/bigquery_orders.json"))
        .expect("the sample is committed");
    let mut json: serde_json::Value = serde_json::from_str(&text).unwrap();
    if let Some(policy) = policy {
        json["nodes"][1]["data"]["policy"] = policy;
    }
    let resolver = Resolver::new(workspace)
        .bind("bigquery_endpoint", endpoint)
        .bind("dataset", &dataset.name);
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

/// The sample, three runs: everything, nothing new, then only what arrived.
fn the_bigquery_sample_carries_on(name: &str, policy: Option<serde_json::Value>) {
    let Some(endpoint) = server("ETL_TEST_BIGQUERY") else {
        return;
    };
    let Some((workspace, binary)) = workspace(name, &[]) else {
        return;
    };
    let orders = sample_orders();
    let dataset = BigqueryDataset::new(&endpoint, name);
    dataset.insert_orders(&orders[..10]);

    let first = run(
        &bigquery_orders(
            &workspace,
            &endpoint,
            &dataset,
            &BTreeMap::new(),
            policy.clone(),
        ),
        &options(&workspace),
    )
    .expect("runs");
    let put = rows(&first)[3].expect("the large orders were loaded");
    assert_eq!(rows(&first)[0], Some(10));
    assert_eq!(dataset.count("large_orders"), put);
    let saved = positions(&first);
    let highest = orders[..10]
        .iter()
        .map(|order| order["order_id"].as_i64().unwrap())
        .max()
        .unwrap();
    assert_eq!(saved["read_orders"]["type"], "INTEGER", "{saved:?}");
    assert_eq!(
        saved["read_orders"]["value"],
        highest.to_string(),
        "{saved:?}"
    );
    assert_eq!(
        query(
            &binary,
            &workspace,
            "SELECT count(*) AS n, typeof(any_value(order_ts)) AS t, typeof(any_value(amount)) AS a \
             FROM 'samples/out/bigquery_large_orders.parquet';"
        ),
        format!(r#"[{{"n":{put},"t":"TIMESTAMP","a":"DECIMAL(10,2)"}}]"#)
    );

    // Nothing new: nothing read, nothing loaded twice.
    let second = run(
        &bigquery_orders(&workspace, &endpoint, &dataset, &saved, policy.clone()),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&second), [Some(0), Some(0), Some(0), Some(0)]);
    assert_eq!(dataset.count("large_orders"), put);

    // Two more orders arrive: exactly those are read.
    dataset.insert_orders(&orders[10..]);
    let third = run(
        &bigquery_orders(&workspace, &endpoint, &dataset, &saved, policy),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&third)[0], Some(2));
}

#[test]
fn the_bigquery_sample_carries_on_between_runs_on_the_one_script_path() {
    the_bigquery_sample_carries_on("bigquery_script", None);
}

#[test]
fn the_bigquery_sample_carries_on_between_runs_on_the_session_path() {
    the_bigquery_sample_carries_on(
        "bigquery_session",
        Some(serde_json::json!({ "retryAttempts": 1 })),
    );
}

#[test]
fn a_failed_bigquery_run_saves_no_position_and_loads_nothing() {
    let Some(endpoint) = server("ETL_TEST_BIGQUERY") else {
        return;
    };
    let Some((workspace, _)) = workspace("bigquery_failed", &[]) else {
        return;
    };
    let dataset = BigqueryDataset::new(&endpoint, "failed");
    dataset.insert_orders(&sample_orders()[..6]);

    let mut json: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(repo_root().join("samples/pipelines/bigquery_orders.json"))
            .unwrap(),
    )
    .unwrap();
    json["nodes"][1]["data"]["properties"]["predicate"] = serde_json::json!("no_such_column > 1");
    json["nodes"][1]["data"]["policy"] = serde_json::json!({ "continueOnFailure": true });
    let resolver = Resolver::new(&workspace)
        .bind("bigquery_endpoint", &endpoint)
        .bind("dataset", &dataset.name);
    let resolved = resolve(&document(&json.to_string()), &resolver).expect("resolves");
    let plan = compile_with(&resolved.document, &CompileOptions::default()).unwrap();

    let report = run(&plan, &options(&workspace)).expect("a report, not an error");
    assert!(report.failed());
    assert_eq!(report.stages[0].rows, Some(6), "the source did read them");
    assert!(report.checkpoints.is_empty(), "{:?}", report.checkpoints);
    assert_eq!(dataset.count("large_orders"), 0);
}

#[test]
fn previewing_a_bigquery_source_reads_without_remembering() {
    let Some(endpoint) = server("ETL_TEST_BIGQUERY") else {
        return;
    };
    let Some((workspace, _)) = workspace("bigquery_preview", &[]) else {
        return;
    };
    let dataset = BigqueryDataset::new(&endpoint, "preview");
    dataset.insert_orders(&sample_orders()[..9]);
    let plan = bigquery_orders(&workspace, &endpoint, &dataset, &BTreeMap::new(), None);
    let shown = preview(&plan, "read_orders", 50, &options(&workspace)).expect("previews");
    assert_eq!(shown.rows.len(), 9);
    assert_eq!(dataset.count("large_orders"), 0, "a preview loads nothing");
}

// ---------------------------------------------------------------------------
// MariaDB, through the MySQL components (Phase 10q)
// ---------------------------------------------------------------------------

#[test]
fn mariadb_is_written_and_read_back_through_the_mysql_components() {
    if let Some(connection) = server("ETL_TEST_MARIADB") {
        round_trip("mariadb", "mysql", "mysql_scanner", &connection);
    }
}

/// SQL straight to a MySQL-protocol server through DuckDB's mysql extension,
/// for setting up what the components under test then read.
fn on_server(binary: &Path, workspace: &Path, connection: &str, statements: &[&str]) {
    let extensions = etl_duckdb_engine::exec::locate_extension_dir(&options(workspace))
        .expect("the vendored extensions");
    let mut sql = format!(
        "SET extension_directory = '{}'; SET autoinstall_known_extensions = false; \
         LOAD mysql_scanner; ATTACH '{connection}' AS server (TYPE mysql);",
        slashed(&extensions)
    );
    for statement in statements {
        sql.push_str(&format!(
            " CALL mysql_execute('server', '{}');",
            statement.replace('\'', "''")
        ));
    }
    query(binary, workspace, &sql);
}

#[test]
fn mariadbs_own_types_are_read_as_their_values() {
    let Some(connection) = server("ETL_TEST_MARIADB") else {
        return;
    };
    let Some((workspace, binary)) = workspace("mariadb_types", &["mysql_scanner"]) else {
        return;
    };
    let name = table("mariadb_types");
    on_server(
        &binary,
        &workspace,
        &connection,
        &[
            &format!("DROP TABLE IF EXISTS {name}"),
            &format!(
                "CREATE TABLE {name} (id INT PRIMARY KEY, u UUID, ip INET6, doc JSON, \
                 amount DECIMAL(10,2), stamp DATETIME(6), ok BOOLEAN, kind ENUM('a','b'), \
                 flag BIT(1), yr YEAR)"
            ),
            &format!(
                "INSERT INTO {name} VALUES (1, 'f47ac10b-58cc-4372-a567-0e02b2c3d479', '::1', \
                 '{{\"a\": 1}}', 12.30, '2026-09-24 10:00:00.123456', TRUE, 'b', b'1', 2026)"
            ),
        ],
    );

    let connection_json = connection.replace('\\', "\\\\").replace('"', "\\\"");
    let pipeline = to_parquet_at(
        "src.db.mysql",
        &format!(r#"{{ "connection": "{connection_json}", "table": "{name}" }}"#),
        "out/types.parquet",
    );
    run(&compile(&pipeline).unwrap(), &options(&workspace)).expect("reads");
    assert_eq!(
        query(
            &binary,
            &workspace,
            "SELECT u, ip, doc, amount::VARCHAR AS amount, stamp::VARCHAR AS stamp, ok, kind, flag, yr, \
                    typeof(amount) AS amount_type, typeof(stamp) AS stamp_type \
             FROM 'out/types.parquet';"
        ),
        r#"[{"u":"f47ac10b-58cc-4372-a567-0e02b2c3d479","ip":"::1","doc":"{\"a\": 1}","amount":"12.30","stamp":"2026-09-24 10:00:00.123456","ok":true,"kind":"b","flag":true,"yr":2026,"amount_type":"DECIMAL(10,2)","stamp_type":"TIMESTAMP"}]"#
    );
}

/// Found in 10q, on MySQL 8.4 as on MariaDB 11.8: DuckDB's mysql extension
/// creates a TIMESTAMP column as `DATETIME`, whole seconds, so a table the
/// sink created dropped every fraction. Fixed in 10q (open question 16): a
/// table the sink creates, by `overwrite` or by a first `append`, keeps the
/// microseconds; a table its owner made keeps its own column types, even
/// `DATETIME` without a fraction.
fn sub_second_timestamps_survive_a_table_the_sink_creates(variable: &str, label: &str) {
    let Some(connection) = server(variable) else {
        return;
    };
    let Some((workspace, binary)) = workspace(&format!("{label}_fractions"), &["mysql_scanner"])
    else {
        return;
    };
    std::fs::write(
        workspace.join("stamps.csv"),
        "id,stamp\n1,2026-09-24 10:00:00.123456\n",
    )
    .unwrap();
    let created = table(&format!("{label}_created"));
    let appended = table(&format!("{label}_appended"));
    let prepared = table(&format!("{label}_prepared"));
    let whole = table(&format!("{label}_whole"));
    on_server(
        &binary,
        &workspace,
        &connection,
        &[
            &format!("DROP TABLE IF EXISTS {created}"),
            &format!("DROP TABLE IF EXISTS {appended}"),
            &format!("DROP TABLE IF EXISTS {prepared}"),
            &format!("DROP TABLE IF EXISTS {whole}"),
            &format!("CREATE TABLE {prepared} (id BIGINT, stamp DATETIME(6))"),
            &format!("CREATE TABLE {whole} (id BIGINT, stamp DATETIME)"),
        ],
    );

    let connection_json = connection.replace('\\', "\\\\").replace('"', "\\\"");
    let options = options(&workspace);
    let write = |name: &str, mode: &str| {
        let pipeline = document(&format!(
            r#"{{ "formatVersion": 1,
              "nodes": [
                {{ "id": "read", "position": {{"x":0,"y":0}}, "data": {{ "label": "CSV",
                   "componentId": "src.file.csv", "properties": {{ "path": "stamps.csv" }} }} }},
                {{ "id": "write", "position": {{"x":0,"y":0}}, "data": {{ "label": "Write",
                   "componentId": "snk.db.mysql",
                   "properties": {{ "connection": "{connection_json}", "table": "{name}", "mode": "{mode}" }} }} }}
              ],
              "edges": [ {{ "id": "e1", "source": "read", "target": "write" }} ] }}"#
        ));
        run(&compile(&pipeline).unwrap(), &options).expect("writes");
    };
    let read = |name: &str| {
        let out = format!("out/{name}.parquet");
        let pipeline = to_parquet_at(
            "src.db.mysql",
            &format!(r#"{{ "connection": "{connection_json}", "table": "{name}" }}"#),
            &out,
        );
        run(&compile(&pipeline).unwrap(), &options).expect("reads");
        query(
            &binary,
            &workspace,
            &format!("SELECT stamp::VARCHAR AS stamp FROM '{out}';"),
        )
    };

    let fraction = r#"[{"stamp":"2026-09-24 10:00:00.123456"}]"#;
    write(&created, "overwrite");
    assert_eq!(read(&created), fraction, "{label}: created by overwrite");
    write(&created, "overwrite");
    assert_eq!(read(&created), fraction, "{label}: replaced by overwrite");
    write(&appended, "append");
    write(&appended, "append");
    read(&appended);
    assert_eq!(
        query(
            &binary,
            &workspace,
            &format!(
                "SELECT count(*) AS n, count(DISTINCT stamp)::INTEGER AS d,                  min(stamp)::VARCHAR AS stamp FROM 'out/{appended}.parquet';"
            ),
        ),
        r#"[{"n":2,"d":1,"stamp":"2026-09-24 10:00:00.123456"}]"#,
        "{label}: created by the first append, kept by the second"
    );
    write(&whole, "append");
    assert_eq!(
        read(&whole),
        r#"[{"stamp":"2026-09-24 10:00:00"}]"#,
        "{label}: the owner's DATETIME is left as the owner made it"
    );
    write(&prepared, "append");
    assert_eq!(
        read(&prepared),
        r#"[{"stamp":"2026-09-24 10:00:00.123456"}]"#,
        "{label}: made with DATETIME(6)"
    );
}

#[test]
fn sub_second_timestamps_survive_a_table_the_sink_creates_on_mysql() {
    sub_second_timestamps_survive_a_table_the_sink_creates("ETL_TEST_MYSQL", "mysql");
}

#[test]
fn sub_second_timestamps_survive_a_table_the_sink_creates_on_mariadb() {
    sub_second_timestamps_survive_a_table_the_sink_creates("ETL_TEST_MARIADB", "mariadb");
}

#[test]
fn a_wrong_mariadb_password_fails_and_is_masked() {
    let Some(connection) = server("ETL_TEST_MARIADB") else {
        return;
    };
    let Some((workspace, _)) = workspace("mariadb_bad_password", &["mysql_scanner"]) else {
        return;
    };
    let wrong = connection.replace("passwd=etl", "passwd=wrong-hunter2");
    assert_ne!(
        wrong, connection,
        "the connection string should hold passwd=etl"
    );
    let pipeline = to_parquet(
        "src.db.mysql",
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
    assert!(error.contains("Access denied"), "{error}");
}

// ---------------------------------------------------------------------------
// ClickHouse, incremental by a checkpoint (Phase 10r)
// ---------------------------------------------------------------------------

/// One statement to the test server's HTTP interface, as its test user.
fn clickhouse_sql(url: &str, sql: &str) -> String {
    let mut response = ureq::post(&format!("{url}/"))
        .header("Authorization", "Basic ZXRsOmV0bC1zZWNyZXQ=") // etl:etl-secret
        .send(sql.as_bytes())
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    response
        .body_mut()
        .read_to_string()
        .unwrap()
        .trim()
        .to_string()
}

/// A database with empty `orders` and `large_orders` tables, dropped when dropped.
struct ClickhouseDatabase {
    url: String,
    name: String,
}

impl ClickhouseDatabase {
    fn new(url: &str, test: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let name = format!("etl_verified_{test}_{nanos}");
        clickhouse_sql(url, &format!("CREATE DATABASE {name}"));
        for table in ["orders", "large_orders"] {
            clickhouse_sql(
                url,
                &format!(
                    "CREATE TABLE {name}.{table} (order_id Int64, customer_id String, \
                     order_ts DateTime64(6, 'UTC'), amount Decimal(10, 2), status String) \
                     ENGINE = MergeTree ORDER BY order_id"
                ),
            );
        }
        ClickhouseDatabase {
            url: url.to_string(),
            name,
        }
    }

    fn insert_orders(&self, orders: &[serde_json::Value]) {
        use etl_plugin_sdk::Sink;
        let rows: Vec<etl_plugin_sdk::Record> = orders
            .iter()
            .map(|order| order.as_object().unwrap().clone())
            .collect();
        etl_connectors::clickhouse::ClickhouseSink
            .write(
                &serde_json::json!({
                    "url": self.url, "username": "etl", "password": "etl-secret",
                    "database": self.name, "table": "orders",
                }),
                &mut etl_plugin_sdk::Records(rows.into_iter()),
                &etl_plugin_sdk::Context::default(),
            )
            .expect("inserts");
    }

    fn count(&self, table: &str) -> u64 {
        clickhouse_sql(
            &self.url,
            &format!("SELECT count() FROM {}.{table}", self.name),
        )
        .parse()
        .unwrap()
    }
}

impl Drop for ClickhouseDatabase {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(|| {
            clickhouse_sql(&self.url, &format!("DROP DATABASE IF EXISTS {}", self.name))
        });
    }
}

fn clickhouse_orders(
    workspace: &Path,
    url: &str,
    database: &ClickhouseDatabase,
    checkpoints: &BTreeMap<String, serde_json::Value>,
    policy: Option<serde_json::Value>,
) -> etl_duckdb_engine::Plan {
    let text =
        std::fs::read_to_string(repo_root().join("samples/pipelines/clickhouse_orders.json"))
            .expect("the sample is committed");
    let mut json: serde_json::Value = serde_json::from_str(&text).unwrap();
    if let Some(policy) = policy {
        json["nodes"][1]["data"]["policy"] = policy;
    }
    let resolver = Resolver::new(workspace)
        .bind("clickhouse_url", url)
        .bind("database", &database.name);
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

/// The sample, three runs: everything, nothing new, then only what arrived.
fn the_clickhouse_sample_carries_on(name: &str, policy: Option<serde_json::Value>) {
    let Some(url) = server("ETL_TEST_CLICKHOUSE") else {
        return;
    };
    let Some((workspace, binary)) = workspace(name, &[]) else {
        return;
    };
    let orders = sample_orders();
    let database = ClickhouseDatabase::new(&url, name);
    database.insert_orders(&orders[..10]);

    let first = run(
        &clickhouse_orders(
            &workspace,
            &url,
            &database,
            &BTreeMap::new(),
            policy.clone(),
        ),
        &options(&workspace),
    )
    .expect("runs");
    let put = rows(&first)[3].expect("the large orders were inserted");
    assert_eq!(rows(&first)[0], Some(10));
    assert_eq!(database.count("large_orders"), put);
    let saved = positions(&first);
    let highest = orders[..10]
        .iter()
        .map(|order| order["order_id"].as_i64().unwrap())
        .max()
        .unwrap();
    assert_eq!(saved["read_orders"]["type"], "Int64", "{saved:?}");
    assert_eq!(
        saved["read_orders"]["value"],
        highest.to_string(),
        "{saved:?}"
    );
    assert_eq!(
        query(
            &binary,
            &workspace,
            "SELECT count(*) AS n, typeof(any_value(order_ts)) AS t, typeof(any_value(amount)) AS a \
             FROM 'samples/out/clickhouse_large_orders.parquet';"
        ),
        format!(r#"[{{"n":{put},"t":"TIMESTAMP","a":"DECIMAL(10,2)"}}]"#)
    );

    let second = run(
        &clickhouse_orders(&workspace, &url, &database, &saved, policy.clone()),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&second), [Some(0), Some(0), Some(0), Some(0)]);
    assert_eq!(
        database.count("large_orders"),
        put,
        "nothing inserted twice"
    );

    database.insert_orders(&orders[10..]);
    let third = run(
        &clickhouse_orders(&workspace, &url, &database, &saved, policy),
        &options(&workspace),
    )
    .expect("runs");
    assert_eq!(rows(&third)[0], Some(2));
}

#[test]
fn the_clickhouse_sample_carries_on_between_runs_on_the_one_script_path() {
    the_clickhouse_sample_carries_on("clickhouse_script", None);
}

#[test]
fn the_clickhouse_sample_carries_on_between_runs_on_the_session_path() {
    the_clickhouse_sample_carries_on(
        "clickhouse_session",
        Some(serde_json::json!({ "retryAttempts": 1 })),
    );
}

#[test]
fn a_failed_clickhouse_run_saves_no_position_and_inserts_nothing() {
    let Some(url) = server("ETL_TEST_CLICKHOUSE") else {
        return;
    };
    let Some((workspace, _)) = workspace("clickhouse_failed", &[]) else {
        return;
    };
    let database = ClickhouseDatabase::new(&url, "failed");
    database.insert_orders(&sample_orders()[..6]);

    let mut json: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(repo_root().join("samples/pipelines/clickhouse_orders.json"))
            .unwrap(),
    )
    .unwrap();
    json["nodes"][1]["data"]["properties"]["predicate"] = serde_json::json!("no_such_column > 1");
    json["nodes"][1]["data"]["policy"] = serde_json::json!({ "continueOnFailure": true });
    let resolver = Resolver::new(&workspace)
        .bind("clickhouse_url", &url)
        .bind("database", &database.name);
    let resolved = resolve(&document(&json.to_string()), &resolver).expect("resolves");
    let plan = compile_with(&resolved.document, &CompileOptions::default()).unwrap();

    let report = run(&plan, &options(&workspace)).expect("a report, not an error");
    assert!(report.failed());
    assert_eq!(report.stages[0].rows, Some(6), "the source did read them");
    assert!(report.checkpoints.is_empty(), "{:?}", report.checkpoints);
    assert_eq!(database.count("large_orders"), 0);
}

#[test]
fn previewing_a_clickhouse_source_reads_without_remembering() {
    let Some(url) = server("ETL_TEST_CLICKHOUSE") else {
        return;
    };
    let Some((workspace, _)) = workspace("clickhouse_preview", &[]) else {
        return;
    };
    let database = ClickhouseDatabase::new(&url, "preview");
    database.insert_orders(&sample_orders()[..9]);
    let plan = clickhouse_orders(&workspace, &url, &database, &BTreeMap::new(), None);
    let shown = preview(&plan, "read_orders", 50, &options(&workspace)).expect("previews");
    assert_eq!(shown.rows.len(), 9);
    assert_eq!(
        database.count("large_orders"),
        0,
        "a preview inserts nothing"
    );
}
