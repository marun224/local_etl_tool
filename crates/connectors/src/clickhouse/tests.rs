//! What decides whether a row is read once, again, or never, and whether a
//! failure is seen: the SQL an incremental run sends and its parameter, wide
//! integers kept exact, an error arriving after a `200`, inserts in batches.
//! Settings and values without a server; the rest against ClickHouse when
//! `ETL_TEST_CLICKHOUSE` names it (`scripts/test-services.ps1` starts it).

use super::*;
use crate::fixture;
use etl_plugin_sdk::Record;

// ---------------------------------------------------------------------------
// Without a server
// ---------------------------------------------------------------------------

fn base(extra: JsonValue) -> JsonValue {
    let mut all = json!({ "url": "http://clickhouse.local:8123", "table": "orders" });
    for (key, value) in extra.as_object().unwrap() {
        all[key] = value.clone();
    }
    all
}

#[test]
fn a_setting_that_cannot_work_is_refused_by_property() {
    let source = |extra: JsonValue| {
        ClickhouseSource
            .check(&base(extra))
            .map_err(|e| e.to_string())
    };
    let sink = |extra: JsonValue| {
        ClickhouseSink
            .check(&base(extra))
            .map_err(|e| e.to_string())
    };
    let refused = |result: Result<(), String>, property: &str| {
        let error = result.unwrap_err();
        assert!(
            error.starts_with(&format!("property '{property}'")),
            "{property}: {error}"
        );
    };

    refused(source(json!({ "url": null })), "url");
    refused(source(json!({ "url": "clickhouse.local:8123" })), "url");
    refused(source(json!({ "url": "http://h:8123/?user=x" })), "url");
    refused(source(json!({ "query": "SELECT 1" })), "query");
    refused(source(json!({ "table": null })), "table");
    refused(source(json!({ "table": "a.b.c" })), "table");
    refused(
        source(json!({ "table": null, "query": "SELECT 1 FORMAT CSV" })),
        "query",
    );
    refused(source(json!({ "start": "1" })), "start");
    source(json!({ "table": "sales.orders", "incremental_column": "id" })).expect("fine");

    refused(sink(json!({ "query": "SELECT 1" })), "query");
    refused(sink(json!({ "mode": "merge" })), "mode");
    sink(json!({ "mode": "truncate" })).expect("fine");
}

#[test]
fn wide_integers_stay_exact_and_wrappers_come_off() {
    assert_eq!(bare_type("Nullable(LowCardinality(String))"), "String");
    assert_eq!(bare_type("DateTime64(6, 'UTC')"), "DateTime64(6, 'UTC')");
    assert_eq!(value("Int64", json!("-42")), json!(-42));
    assert_eq!(
        value("UInt64", json!("18446744073709551615")),
        json!(18_446_744_073_709_551_615u64)
    );
    assert_eq!(
        value("Int128", json!("170141183460469231731687303715884105727")),
        json!("170141183460469231731687303715884105727"),
        "too wide for a number: its exact text"
    );
    assert_eq!(value("Nullable(Int64)", JsonValue::Null), JsonValue::Null);
    assert_eq!(value("Decimal(38, 10)", json!("1.5")), json!("1.5"));
    assert_eq!(value("Array(Int32)", json!([1, 2])), json!([1, 2]));
}

#[test]
fn names_are_backticked_and_an_error_row_is_recognised() {
    assert_eq!(table_name("sales.orders").unwrap(), "`sales`.`orders`");
    assert_eq!(identifier("we`ird"), "`we\\`ird`");
    assert!(exception_row(&[json!("Code: 395. DB::Exception: boom")]).is_some());
    assert!(exception_row(&[json!("Code: 1")]).is_none());
    assert!(exception_row(&[json!(1), json!(2)]).is_none());
    assert_eq!(
        clickhouse_error("Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE) (version 25.8.33.6 (official build))"),
        "Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE)"
    );
}

fn settings(extra: JsonValue) -> SourceSettings {
    SourceSettings::from(&base(extra)).unwrap()
}

#[test]
fn an_incremental_run_passes_its_value_as_a_parameter() {
    assert_eq!(
        statement(&settings(json!({ "max_records": 5 })), None).0,
        "SELECT * FROM `orders` LIMIT 5 FORMAT JSONCompactEachRowWithNamesAndTypes"
    );
    let incremental = settings(json!({ "incremental_column": "at" }));
    let saved = json!({
        "read": "table `orders`", "column": "at", "type": "DateTime64(6, 'UTC')",
        "value": "2026-09-24 10:00:00.123456') OR 1=1 --",
    });
    let (sql, parameters, note) = statement(&incremental, Some(&saved));
    assert_eq!(
        sql,
        "SELECT * FROM (SELECT * FROM `orders`) WHERE `at` > {etl_after:DateTime64(6, 'UTC')} \
         ORDER BY `at` FORMAT JSONCompactEachRowWithNamesAndTypes"
    );
    assert!(!sql.contains("OR 1=1"));
    assert_eq!(
        parameters,
        [(
            "param_etl_after".to_string(),
            "2026-09-24 10:00:00.123456') OR 1=1 --".to_string()
        )]
    );
    assert!(note.is_none());

    let elsewhere =
        json!({ "read": "table `other`", "column": "at", "type": "UInt64", "value": "1" });
    let started = settings(json!({ "incremental_column": "at", "start": "100" }));
    let (sql, parameters, note) = statement(&started, Some(&elsewhere));
    assert_eq!(
        sql,
        "SELECT * FROM (SELECT * FROM `orders`) WHERE `at` > (100) ORDER BY `at` FORMAT \
         JSONCompactEachRowWithNamesAndTypes"
    );
    assert!(parameters.is_empty());
    assert_eq!(
        note.unwrap(),
        "; the saved position was for table `other` by 'at', so this read started over"
    );
}

// ---------------------------------------------------------------------------
// Against ClickHouse
// ---------------------------------------------------------------------------

fn server_url() -> Option<String> {
    match std::env::var("ETL_TEST_CLICKHOUSE") {
        Ok(url) if !url.trim().is_empty() => Some(url.trim().to_string()),
        _ => {
            eprintln!("skipping: ETL_TEST_CLICKHOUSE is not set; see scripts/test-services.ps1");
            None
        }
    }
}

fn properties(url: &str, extra: JsonValue) -> JsonValue {
    let mut all =
        json!({ "url": url, "username": "etl", "password": "etl-secret", "timeout_ms": 10_000 });
    for (key, value) in extra.as_object().unwrap() {
        all[key] = value.clone();
    }
    all
}

/// Run SQL for set-up, through the sink's own statement path.
fn admin(url: &str, sql: &str) {
    let server = Server::from(&properties(url, json!({}))).unwrap();
    let mut client = Client::new(Settings::signed(
        format!("{url}/"),
        Method::Post,
        Duration::from_secs(10),
        0,
    ));
    execute(&mut client, &server, sql, b"", None).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// A table for one test, dropped when the test ends.
struct TestTable {
    url: String,
    name: String,
}

impl TestTable {
    fn new(url: &str, test: &str, columns: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let name = format!("etl_{test}_{}_{nanos}", std::process::id());
        admin(
            url,
            &format!("CREATE TABLE {name} ({columns}) ENGINE = MergeTree ORDER BY tuple()"),
        );
        TestTable {
            url: url.to_string(),
            name,
        }
    }

    fn put(&self, rows: Vec<JsonValue>) -> Summary {
        ClickhouseSink
            .write(
                &properties(&self.url, json!({ "table": self.name })),
                &mut fixture::records(rows),
                &Context::default(),
            )
            .expect("inserts")
    }

    fn take(
        &self,
        extra: JsonValue,
        saved: Option<&JsonValue>,
    ) -> Result<(Vec<Record>, Summary), ConnectorError> {
        let mut all = properties(&self.url, json!({ "table": self.name }));
        for (key, value) in extra.as_object().unwrap() {
            all[key] = value.clone();
        }
        let server = Server::from(&all).unwrap();
        let settings = SourceSettings::from(&all).unwrap();
        let mut rows: Vec<Record> = Vec::new();
        read(&server, &settings, &mut rows, saved).map(|summary| (rows, summary))
    }
}

impl Drop for TestTable {
    fn drop(&mut self) {
        let server = Server::from(&properties(&self.url, json!({}))).unwrap();
        let mut client = Client::new(Settings::signed(
            format!("{}/", self.url),
            Method::Post,
            Duration::from_secs(10),
            0,
        ));
        let _ = execute(
            &mut client,
            &server,
            &format!("DROP TABLE IF EXISTS {}", self.name),
            b"",
            None,
        );
    }
}

fn orders(range: std::ops::RangeInclusive<u64>) -> Vec<JsonValue> {
    range
        .map(|id| {
            json!({
                "id": id, "amount": format!("{id}.25"),
                "at": format!("2026-09-24 10:00:{:02}.123456", id % 60),
            })
        })
        .collect()
}

const ORDERS: &str = "id UInt64, amount Decimal(18, 2), at DateTime64(6, 'UTC')";

fn ids(rows: &[Record]) -> Vec<u64> {
    rows.iter().map(|r| r["id"].as_u64().unwrap()).collect()
}

#[test]
fn every_type_is_read_exactly() {
    let Some(url) = server_url() else { return };
    let table = TestTable::new(
        &url,
        "types",
        "id UInt64, big Int128, d Decimal(38, 10), f Float64, at DateTime64(6, 'UTC'), \
         day Date, s LowCardinality(String), n Nullable(String), a Array(Int32), \
         m Map(String, UInt8), u UUID, ok Bool, e Enum8('x' = 1, 'y' = 2)",
    );
    table.put(vec![json!({
        "id": 1, "big": "170141183460469231731687303715884105727",
        "d": "12345678901234567890.123456789", "f": 0.25, "at": "2026-09-24 10:00:00.123456",
        "day": "2026-09-24", "s": "a", "n": null, "a": [1, 2], "m": { "k": 1 },
        "u": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "ok": true, "e": "y",
    })]);
    let (rows, summary) = table.take(json!({}), None).unwrap();
    assert_eq!(
        JsonValue::Object(rows[0].clone()),
        json!({
            "id": 1, "big": "170141183460469231731687303715884105727",
            "d": "12345678901234567890.123456789", "f": 0.25,
            "at": "2026-09-24 10:00:00.123456", "day": "2026-09-24", "s": "a", "n": null,
            "a": [1, 2], "m": { "k": 1 }, "u": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "ok": true, "e": "y",
        })
    );
    assert!(!summary.detail.contains("etl-secret"), "{}", summary.detail);
}

#[test]
fn incremental_runs_read_only_what_is_new_by_time_and_by_number() {
    let Some(url) = server_url() else { return };
    let table = TestTable::new(&url, "incremental", ORDERS);
    table.put(orders(1..=5));

    for column in ["at", "id"] {
        let incremental = json!({ "incremental_column": column });
        let (first, summary) = table.take(incremental.clone(), None).unwrap();
        assert_eq!(ids(&first), [1, 2, 3, 4, 5], "{column}");
        let saved = summary.checkpoint.expect("a position");

        let (second, summary) = table.take(incremental.clone(), Some(&saved)).unwrap();
        assert!(second.is_empty(), "{column}: nothing new");
        assert!(summary.checkpoint.is_none());

        table.put(orders(6..=7));
        let (third, _) = table.take(incremental.clone(), Some(&saved)).unwrap();
        assert_eq!(ids(&third), [6, 7], "{column}: only what arrived");

        let (capped, summary) = table
            .take(
                json!({ "incremental_column": column, "max_records": 2 }),
                None,
            )
            .unwrap();
        assert_eq!(ids(&capped), [1, 2]);
        assert!(
            summary.detail.contains("stopped at max_records"),
            "{}",
            summary.detail
        );
        admin(&url, &format!("TRUNCATE TABLE {}", table.name));
        table.put(orders(1..=5));
    }

    // A first run from start, capped: the lowest after start, in order.
    let (from_start, _) = table
        .take(
            json!({ "incremental_column": "id", "start": "2", "max_records": 2 }),
            None,
        )
        .unwrap();
    assert_eq!(ids(&from_start), [3, 4]);
}

#[test]
fn an_error_after_the_first_rows_fails_the_read() {
    let Some(url) = server_url() else { return };
    let mut all = properties(
        &url,
        json!({
            "query": "SELECT number, throwIf(number = 50000, 'boom') AS x FROM numbers(100000) \
                      SETTINGS max_block_size = 1000",
        }),
    );
    all.as_object_mut().unwrap().remove("table");
    let mut rows: Vec<Record> = Vec::new();
    let error = ClickhouseSource
        .read(&all, &mut rows, &Context::default())
        .unwrap_err()
        .to_string();
    assert!(!rows.is_empty(), "rows came before the error");
    assert!(error.contains("DB::Exception: boom"), "{error}");
    assert!(
        error.contains("FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"),
        "{error}"
    );
}

#[test]
fn what_cannot_be_reached_is_named_without_the_password() {
    let Some(url) = server_url() else { return };
    let read_with = |properties: JsonValue| {
        ClickhouseSource
            .read(&properties, &mut Vec::new(), &Context::default())
            .unwrap_err()
            .to_string()
    };
    let missing = read_with(properties(&url, json!({ "table": "etl_no_such_table" })));
    assert!(missing.contains("HTTP 404"), "{missing}");
    assert!(missing.contains("UNKNOWN_TABLE"), "{missing}");

    let refused = read_with(properties(
        &url,
        json!({ "table": "t", "password": "not-the-password" }),
    ));
    assert!(refused.contains("HTTP 403"), "{refused}");
    assert!(refused.contains("AUTHENTICATION_FAILED"), "{refused}");
    assert!(!refused.contains("not-the-password"), "{refused}");
}

#[test]
fn rows_are_inserted_in_batches_and_a_refusal_says_what_landed() {
    let Some(url) = server_url() else { return };
    let table = TestTable::new(&url, "insert", ORDERS);
    let server = Server::from(&properties(&url, json!({}))).unwrap();
    let settings = SinkSettings::from(&properties(&url, json!({ "table": table.name }))).unwrap();
    let summary = insert(
        &server,
        &settings,
        &mut fixture::records(orders(1..=25)),
        10,
        INSERT_BYTES,
    )
    .unwrap();
    assert_eq!(summary.records, 25);
    assert!(
        summary.detail.ends_with("in 3 INSERT(s)"),
        "{}",
        summary.detail
    );
    let (rows, _) = table.take(json!({}), None).unwrap();
    assert_eq!(rows.len(), 25);

    let truncate = SinkSettings::from(&properties(
        &url,
        json!({ "table": table.name, "mode": "truncate" }),
    ))
    .unwrap();
    insert(
        &server,
        &truncate,
        &mut fixture::records(orders(1..=3)),
        10,
        INSERT_BYTES,
    )
    .unwrap();
    assert_eq!(
        table.take(json!({}), None).unwrap().0.len(),
        3,
        "truncated first"
    );

    let mut bad = orders(1..=15);
    bad[12]["id"] = json!("not a number");
    let error = insert(
        &server,
        &settings,
        &mut fixture::records(bad),
        10,
        INSERT_BYTES,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("DB::Exception"), "{error}");
    assert!(
        error.contains("10 row(s) had been inserted into"),
        "{error}"
    );
}
