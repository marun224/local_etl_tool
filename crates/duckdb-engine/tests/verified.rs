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
