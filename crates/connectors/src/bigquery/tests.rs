//! What decides whether a row is read once, again, or never, and whether it
//! lands: the SQL an incremental run sends, the typed parameter it carries,
//! timestamps read to the microsecond, pages followed, jobs polled, load jobs
//! split and checked. First without a server and against the local fixture;
//! then against the BigQuery emulator when `ETL_TEST_BIGQUERY` names it
//! (`scripts/test-services.ps1` starts it). Without it those skip.

use super::*;
use crate::fixture::{self, Fixture, Seen};
use crate::gcp::tests::{service_account, tokens_from, Scratch};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Without a server
// ---------------------------------------------------------------------------

fn base(extra: JsonValue) -> JsonValue {
    let mut all = json!({ "project": "p", "dataset": "d", "table": "t" });
    for (key, value) in extra.as_object().unwrap() {
        all[key] = value.clone();
    }
    all
}

#[test]
fn a_setting_that_cannot_work_is_refused_by_property() {
    let source = |extra: JsonValue| {
        BigquerySource
            .check(&base(extra))
            .map_err(|e| e.to_string())
    };
    let sink = |extra: JsonValue| BigquerySink.check(&base(extra)).map_err(|e| e.to_string());
    let refused = |result: Result<(), String>, property: &str| {
        let error = result.unwrap_err();
        assert!(
            error.starts_with(&format!("property '{property}'")),
            "{property}: {error}"
        );
    };

    refused(source(json!({ "project": null })), "project");
    refused(source(json!({ "table": null })), "table");
    refused(source(json!({ "dataset": null })), "dataset");
    refused(source(json!({ "query": "SELECT 1" })), "query");
    refused(source(json!({ "dataset": null, "table": null })), "table");
    refused(source(json!({ "start": "1" })), "start");
    refused(
        source(json!({ "incremental_column": "`id`" })),
        "incremental_column",
    );
    refused(source(json!({ "table": "a.b" })), "table");
    refused(source(json!({ "endpoint": "localhost:9050" })), "endpoint");
    source(json!({ "dataset": "other.d" })).expect("another project's dataset");
    source(json!({ "dataset": null, "table": null, "query": "SELECT 1" })).expect("a query");

    refused(sink(json!({ "table": null })), "table");
    refused(sink(json!({ "query": "SELECT 1" })), "query");
    refused(sink(json!({ "mode": "merge" })), "mode");
    sink(json!({ "mode": "truncate" })).expect("fine");
}

#[test]
fn a_timestamp_is_read_to_the_microsecond_in_every_form_bigquery_writes() {
    let expected = 1_790_244_000_123_456;
    assert_eq!(
        timestamp_micros("1790244000123456"),
        Some(expected),
        "as asked for"
    );
    assert_eq!(
        timestamp_micros("1790244000.123456"),
        Some(expected),
        "the emulator's"
    );
    assert_eq!(
        timestamp_micros("1.790244000123456E9"),
        Some(expected),
        "scientific"
    );
    assert_eq!(
        timestamp_micros("1.79024400012345E9"),
        Some(1_790_244_000_123_450)
    );
    assert_eq!(timestamp_micros("1.7902440E9"), Some(1_790_244_000_000_000));
    assert_eq!(timestamp_micros("-1.5"), Some(-1_500_000));
    assert_eq!(timestamp_micros("soon"), None);
    assert_eq!(micros_text(expected), "2026-09-24 10:00:00.123456");
    assert_eq!(micros_text(0), "1970-01-01 00:00:00.000000");
}

#[test]
fn a_row_is_typed_by_the_results_schema() {
    let schema = json!({ "fields": [
        { "name": "id", "type": "INTEGER" },
        { "name": "price", "type": "NUMERIC" },
        { "name": "at", "type": "TIMESTAMP" },
        { "name": "local", "type": "DATETIME" },
        { "name": "ok", "type": "BOOLEAN" },
        { "name": "ratio", "type": "FLOAT" },
        { "name": "tags", "type": "STRING", "mode": "REPEATED" },
        { "name": "customer", "type": "RECORD", "fields": [
            { "name": "id", "type": "STRING" }, { "name": "since", "type": "DATE" } ] },
        { "name": "gone", "type": "STRING" },
    ]});
    let cells = json!({ "f": [
        { "v": "7" }, { "v": "12345678901234567890.123456789" }, { "v": "1790244000123456" },
        { "v": "2026-09-24T10:00:00" }, { "v": "true" }, { "v": "0.25" },
        { "v": [{ "v": "a" }, { "v": "b" }] },
        { "v": { "f": [{ "v": "C1" }, { "v": "2026-01-01" }] } },
        { "v": null },
    ]});
    assert_eq!(
        JsonValue::Object(row(&Field::list(&schema), &cells).unwrap()),
        json!({
            "id": 7, "price": "12345678901234567890.123456789",
            "at": "2026-09-24 10:00:00.123456", "local": "2026-09-24 10:00:00",
            "ok": true, "ratio": 0.25, "tags": ["a", "b"],
            "customer": { "id": "C1", "since": "2026-01-01" }, "gone": null,
        })
    );
    let error = row(
        &Field::list(&json!({ "fields": [{ "name": "id", "type": "INT64" }] })),
        &json!({ "f": [{ "v": "seven" }] }),
    )
    .unwrap_err()
    .to_string();
    assert_eq!(error, "BigQuery gave 'seven' for INT64 column 'id'");
}

fn settings(extra: JsonValue) -> SourceSettings {
    SourceSettings::from(&base(extra)).unwrap()
}

#[test]
fn an_incremental_run_compares_with_a_typed_parameter_never_pasted_in() {
    let plain = settings(json!({}));
    assert_eq!(statement(&plain, None).0, "SELECT * FROM `p.d.t`");

    let incremental = settings(json!({ "incremental_column": "loaded_at" }));
    let (sql, parameters, note) = statement(&incremental, None);
    assert_eq!(
        sql,
        "SELECT * FROM (SELECT * FROM `p.d.t`) WHERE `loaded_at` IS NOT NULL ORDER BY `loaded_at`"
    );
    assert!(parameters.is_empty() && note.is_none());

    let started = settings(json!({
        "incremental_column": "loaded_at", "start": "TIMESTAMP '2026-01-01 00:00:00'",
    }));
    assert_eq!(
        statement(&started, None).0,
        "SELECT * FROM (SELECT * FROM `p.d.t`) WHERE `loaded_at` > (TIMESTAMP '2026-01-01 \
         00:00:00') ORDER BY `loaded_at`"
    );

    let saved = json!({
        "read": "table p.d.t", "column": "loaded_at", "type": "TIMESTAMP",
        "value": "2026-09-24 10:00:00.123456 UTC'; DROP TABLE x; --",
    });
    let (sql, parameters, _) = statement(&started, Some(&saved));
    assert_eq!(
        sql,
        "SELECT * FROM (SELECT * FROM `p.d.t`) WHERE `loaded_at` > @etl_after ORDER BY `loaded_at`"
    );
    assert!(!sql.contains("DROP"), "the value is a parameter, not SQL");
    assert_eq!(
        parameters,
        [json!({
            "name": "etl_after", "parameterType": { "type": "TIMESTAMP" },
            "parameterValue": { "value": "2026-09-24 10:00:00.123456 UTC'; DROP TABLE x; --" },
        })]
    );

    // A position saved for another read is set aside, and the run says so.
    let elsewhere =
        json!({ "read": "table p.d.other", "column": "loaded_at", "type": "INT64", "value": "9" });
    let (sql, parameters, note) = statement(&started, Some(&elsewhere));
    assert!(sql.contains("> (TIMESTAMP '2026-01-01 00:00:00')"), "{sql}");
    assert!(parameters.is_empty());
    assert_eq!(
        note.unwrap(),
        "; the saved position was for table p.d.other by 'loaded_at', so this read started over"
    );
}

#[test]
fn googles_error_body_comes_down_to_its_message() {
    assert_eq!(
        google_error(
            r#"HTTP 404 from http://x: {"error":{"code":404,"message":"Not found: Table p:d.t","status":"NOT_FOUND"}}"#
        ),
        "HTTP 404 from http://x: Not found: Table p:d.t"
    );
    assert_eq!(google_error("could not reach x"), "could not reach x");
}

// ----- against the fixture -----

fn fixture_api(fixture: &Fixture) -> Api {
    Api::connect(
        &base(json!({ "endpoint": fixture.url(""), "retries": 0 })),
        &Sources {
            var: &|_| None,
            home: None,
        },
    )
    .unwrap()
}

fn page(rows: std::ops::Range<i64>, token: Option<&str>) -> JsonValue {
    let mut answer = json!({
        "jobComplete": true,
        "jobReference": { "jobId": "job-1", "location": "EU" },
        "schema": { "fields": [{ "name": "id", "type": "INTEGER" }] },
        "rows": rows.map(|n| json!({ "f": [{ "v": n.to_string() }] })).collect::<Vec<_>>(),
        "totalBytesProcessed": "1024",
    });
    if let Some(token) = token {
        answer["pageToken"] = json!(token);
    }
    answer
}

/// A query that is still running when first asked, then three pages.
fn slow_query() -> Fixture {
    fixture::serve(|index, _| {
        fixture::ok(match index {
            0 => {
                json!({ "jobComplete": false, "jobReference": { "jobId": "job-1", "location": "EU" } })
            }
            1 => page(0..2, Some("p2")),
            2 => page(2..4, Some("p3")),
            _ => page(4..5, None),
        })
    })
}

fn ids(rows: &[Record]) -> Vec<i64> {
    rows.iter().map(|r| r["id"].as_i64().unwrap()).collect()
}

#[test]
fn a_running_job_is_polled_and_every_page_followed() {
    let fixture = slow_query();
    let mut rows: Vec<Record> = Vec::new();
    let summary = read(
        &mut fixture_api(&fixture),
        &settings(json!({})),
        &mut rows,
        None,
    )
    .unwrap();
    assert_eq!(ids(&rows), [0, 1, 2, 3, 4]);
    assert_eq!(
        summary.detail,
        "5 row(s) from table p.d.t by job job-1 (no sign-in (an emulator over plain http)), 1024 \
         bytes processed"
    );

    let seen = fixture.seen();
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].path(), "/bigquery/v2/projects/p/queries");
    let request: JsonValue = serde_json::from_str(&seen[0].body).unwrap();
    assert_eq!(request["useLegacySql"], false);
    assert_eq!(request["formatOptions"]["useInt64Timestamp"], true);
    assert_eq!(seen[1].method, "GET");
    assert_eq!(seen[1].path(), "/bigquery/v2/projects/p/queries/job-1");
    assert_eq!(
        seen[1].query("location").as_deref(),
        Some("EU"),
        "the job's own location"
    );
    assert_eq!(seen[2].query("pageToken").as_deref(), Some("p2"));
    assert_eq!(seen[3].query("pageToken").as_deref(), Some("p3"));
    assert_eq!(seen.len(), 4);
}

#[test]
fn max_records_stops_mid_page_and_asks_for_no_more() {
    let fixture = slow_query();
    let mut rows: Vec<Record> = Vec::new();
    let summary = read(
        &mut fixture_api(&fixture),
        &settings(json!({ "max_records": 3 })),
        &mut rows,
        None,
    )
    .unwrap();
    assert_eq!(ids(&rows), [0, 1, 2]);
    assert!(summary
        .detail
        .ends_with("stopped at max_records, with more for the next run"));
    assert_eq!(fixture.seen().len(), 3, "the last page never asked for");
}

#[test]
fn an_incremental_read_hands_back_its_highest_value_as_a_parameter() {
    let fixture = fixture::serve(|_, _| {
        fixture::ok(json!({
            "jobComplete": true, "jobReference": { "jobId": "j" },
            "schema": { "fields": [{ "name": "at", "type": "TIMESTAMP" }] },
            "rows": [{ "f": [{ "v": "1790244000000000" }] }, { "f": [{ "v": "1790244000123456" }] }],
        }))
    });
    let settings = settings(json!({ "incremental_column": "at" }));
    let summary = read(&mut fixture_api(&fixture), &settings, &mut Vec::new(), None).unwrap();
    assert_eq!(
        summary.checkpoint.unwrap(),
        json!({
            "read": "table p.d.t", "column": "at", "type": "TIMESTAMP",
            "value": "2026-09-24 10:00:00.123456 UTC",
        })
    );

    let wrong = settings_for_column("missing");
    let error = read(&mut fixture_api(&fixture), &wrong, &mut Vec::new(), None)
        .unwrap_err()
        .to_string();
    assert_eq!(
        error,
        "property 'incremental_column': 'missing' is not a column of what is read"
    );
}

fn settings_for_column(column: &str) -> SourceSettings {
    settings(json!({ "incremental_column": column }))
}

#[test]
fn a_failed_job_and_a_refused_query_are_named() {
    let fixture = fixture::serve(|_, _| {
        fixture::ok(json!({
            "jobComplete": true, "jobReference": { "jobId": "j" },
            "errors": [{ "message": "Resources exceeded during query execution" }],
        }))
    });
    let error = read(
        &mut fixture_api(&fixture),
        &settings(json!({})),
        &mut Vec::new(),
        None,
    )
    .unwrap_err()
    .to_string();
    assert_eq!(
        error,
        "BigQuery job j: Resources exceeded during query execution"
    );

    let fixture = fixture::serve(|_, _| {
        fixture::status(
            400,
            r#"{"error":{"code":400,"message":"Unrecognized name: nope at [1:8]"}}"#,
        )
    });
    let error = read(
        &mut fixture_api(&fixture),
        &settings(json!({})),
        &mut Vec::new(),
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.starts_with("table p.d.t (p): BigQuery query: HTTP 400"),
        "{error}"
    );
    assert!(
        error.ends_with("Unrecognized name: nope at [1:8]"),
        "{error}"
    );
}

#[test]
fn every_call_carries_one_cached_token() {
    let fixture = fixture::serve(|index, request| {
        if request.path() == "/token" {
            fixture::ok(json!({ "access_token": format!("token-{index}"), "expires_in": 3600 }))
        } else {
            fixture::ok(page(0..1, None))
        }
    });
    let scratch = Scratch::new("bigquery-token");
    let file = scratch.write("key.json", &service_account(&fixture.url("/token")));
    let mut api = fixture_api(&fixture);
    api.tokens = tokens_from(&file);
    for _ in 0..2 {
        read(&mut api, &settings(json!({})), &mut Vec::new(), None).unwrap();
    }
    let seen = fixture.seen();
    assert_eq!(seen.iter().filter(|r| r.path() == "/token").count(), 1);
    for request in seen.iter().filter(|r| r.path() != "/token") {
        assert_eq!(request.header("Authorization"), Some("Bearer token-0"));
    }
}

// ----- the sink, against the fixture -----

/// A BigQuery that takes load jobs: each upload is answered as running, and
/// the job is done on the next poll, or fails if `fail` names it.
/// Each load job's write disposition and the ids of the rows it carried.
type Loads = Arc<Mutex<Vec<(String, String)>>>;

fn loader(fail: Option<usize>) -> (Fixture, Loads) {
    let loads = Arc::new(Mutex::new(Vec::new()));
    let seen_loads = loads.clone();
    let fixture = fixture::serve(move |_, request: &Seen| {
        if request.path().starts_with("/upload/") {
            let mut loads = seen_loads.lock().unwrap();
            let disposition = request
                .body
                .split("\"writeDisposition\":\"")
                .nth(1)
                .and_then(|rest| rest.split('"').next())
                .unwrap_or("?")
                .to_string();
            let rows = request
                .body
                .lines()
                .filter(|line| line.starts_with("{\"id\""))
                .collect::<Vec<_>>()
                .join(",");
            loads.push((disposition, rows));
            let id = format!("load-{}", loads.len());
            return fixture::ok(
                json!({ "jobReference": { "jobId": id }, "status": { "state": "RUNNING" } }),
            );
        }
        let id = request.path().rsplit('/').next().unwrap().to_string();
        let number: usize = id.trim_start_matches("load-").parse().unwrap();
        if Some(number) == fail {
            return fixture::ok(json!({ "status": { "state": "DONE",
                "errorResult": { "message": "Error while reading data" },
                "errors": [{ "message": "JSON parsing error in row starting at position 0: No such field: extra." }] } }));
        }
        fixture::ok(json!({ "status": { "state": "DONE" } }))
    });
    (fixture, loads)
}

fn sink_settings(extra: JsonValue) -> SinkSettings {
    SinkSettings::from(&base(extra)).unwrap()
}

fn orders(count: i64) -> Vec<JsonValue> {
    (1..=count)
        .map(|id| json!({ "id": id, "note": "x".repeat(20) }))
        .collect()
}

#[test]
fn rows_go_in_load_jobs_the_first_replacing_when_truncating() {
    let (fixture, loads) = loader(None);
    let summary = load(
        &mut fixture_api(&fixture),
        &sink_settings(json!({ "mode": "truncate" })),
        &mut fixture::records(orders(5)),
        100,
    )
    .unwrap();
    let loads = loads.lock().unwrap().clone();
    assert_eq!(
        loads.len(),
        3,
        "about two rows to a 100-byte job: {loads:?}"
    );
    assert_eq!(loads[0].0, "WRITE_TRUNCATE");
    assert!(loads[1..]
        .iter()
        .all(|(disposition, _)| disposition == "WRITE_APPEND"));
    assert_eq!(summary.records, 5);
    assert!(
        summary
            .detail
            .starts_with("5 row(s) replaced the rows of p.d.t by 3 load job(s)"),
        "{}",
        summary.detail
    );
    let upload = &fixture.seen()[0];
    assert_eq!(upload.path(), "/upload/bigquery/v2/projects/p/jobs");
    assert_eq!(upload.query("uploadType").as_deref(), Some("multipart"));
    assert!(upload
        .header("Content-Type")
        .unwrap()
        .starts_with("multipart/related; boundary="));
    assert!(upload
        .body
        .contains("\"createDisposition\":\"CREATE_NEVER\""));
}

#[test]
fn truncating_with_no_rows_still_empties_the_table() {
    let (fixture, loads) = loader(None);
    load(
        &mut fixture_api(&fixture),
        &sink_settings(json!({ "mode": "truncate" })),
        &mut fixture::records(Vec::new()),
        100,
    )
    .unwrap();
    assert_eq!(
        loads.lock().unwrap().clone(),
        [("WRITE_TRUNCATE".to_string(), String::new())]
    );
}

#[test]
fn a_failed_load_job_says_what_loaded_before_it() {
    let (fixture, _) = loader(Some(2));
    let error = load(
        &mut fixture_api(&fixture),
        &sink_settings(json!({})),
        &mut fixture::records(orders(5)),
        100,
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.starts_with(
            "BigQuery load job load-2 failed: Error while reading data (JSON parsing error"
        ),
        "{error}"
    );
    assert!(
        error.ends_with("2 row(s) had been loaded into p.d.t by 1 job(s) before this, and stay"),
        "{error}"
    );
}

// ---------------------------------------------------------------------------
// Against the emulator
// ---------------------------------------------------------------------------

/// The project the emulator was started with.
const PROJECT: &str = "etl-test";

fn server() -> Option<String> {
    match std::env::var("ETL_TEST_BIGQUERY") {
        Ok(url) if !url.trim().is_empty() => Some(url.trim().trim_end_matches('/').to_string()),
        _ => {
            eprintln!("skipping: ETL_TEST_BIGQUERY is not set; see scripts/test-services.ps1");
            None
        }
    }
}

/// A dataset with an orders table for one test, deleted when the test ends.
struct TestDataset {
    endpoint: String,
    name: String,
}

impl TestDataset {
    fn new(endpoint: &str, test: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let name = format!("etl_{test}_{}_{nanos}", std::process::id());
        let base = format!("{endpoint}/bigquery/v2/projects/{PROJECT}");
        let post = |url: String, body: JsonValue| {
            ureq::post(&url)
                .header("Content-Type", "application/json")
                .send(body.to_string())
        };
        post(
            format!("{base}/datasets"),
            json!({ "datasetReference": { "projectId": PROJECT, "datasetId": name } }),
        )
        .expect("the dataset");
        post(
            format!("{base}/datasets/{name}/tables"),
            json!({
                "tableReference": { "projectId": PROJECT, "datasetId": name, "tableId": "orders" },
                "schema": { "fields": [
                    { "name": "id", "type": "INT64" },
                    { "name": "amount", "type": "NUMERIC" },
                    { "name": "loaded_at", "type": "TIMESTAMP" },
                    { "name": "customer", "type": "RECORD", "fields": [{ "name": "id", "type": "STRING" }] },
                ]},
            }),
        )
        .expect("the table");
        TestDataset {
            endpoint: endpoint.to_string(),
            name,
        }
    }

    fn properties(&self, extra: JsonValue) -> JsonValue {
        let mut all = json!({
            "project": PROJECT, "dataset": self.name, "table": "orders", "endpoint": self.endpoint,
        });
        for (key, value) in extra.as_object().unwrap() {
            all[key] = value.clone();
        }
        all
    }

    /// Rows `range`, each loaded a minute apart, through the sink.
    fn put(&self, range: std::ops::RangeInclusive<i64>, mode: &str) {
        let rows = range
            .map(|id| {
                json!({
                    "id": id, "amount": format!("{id}.25"),
                    "loaded_at": format!("2026-09-24 10:{:02}:00.123456", id % 60),
                    "customer": { "id": format!("C{}", id % 3) },
                })
            })
            .collect();
        BigquerySink
            .write(
                &self.properties(json!({ "mode": mode })),
                &mut fixture::records(rows),
                &Context::default(),
            )
            .expect("loads");
    }
}

impl Drop for TestDataset {
    fn drop(&mut self) {
        let _ = ureq::delete(&format!(
            "{}/bigquery/v2/projects/{PROJECT}/datasets/{}?deleteContents=true",
            self.endpoint, self.name
        ))
        .call();
    }
}

fn take(
    dataset: &TestDataset,
    extra: JsonValue,
    saved: Option<&JsonValue>,
) -> (Vec<Record>, Summary) {
    let properties = dataset.properties(extra);
    let settings = SourceSettings::from(&properties).unwrap();
    let mut api = Api::connect(&properties, &Sources::process()).unwrap();
    let mut rows: Vec<Record> = Vec::new();
    let summary = read(&mut api, &settings, &mut rows, saved).expect("reads");
    (rows, summary)
}

fn sorted_ids(rows: &[Record]) -> Vec<i64> {
    let mut ids = ids(rows);
    ids.sort_unstable();
    ids
}

#[test]
fn a_table_loaded_and_read_back_keeps_its_types() {
    let Some(endpoint) = server() else { return };
    let dataset = TestDataset::new(&endpoint, "types");
    dataset.put(1..=3, "append");

    let (rows, summary) = take(&dataset, json!({}), None);
    assert_eq!(sorted_ids(&rows), [1, 2, 3]);
    let first = rows.iter().find(|row| row["id"] == 1).unwrap();
    assert_eq!(
        JsonValue::Object(first.clone()),
        json!({
            "id": 1, "amount": "1.25", "loaded_at": "2026-09-24 10:01:00.123456",
            "customer": { "id": "C1" },
        })
    );
    assert!(summary.detail.starts_with(&format!(
        "3 row(s) from table {PROJECT}.{}.orders",
        dataset.name
    )));

    // Truncating replaces; a query reads what it selects.
    dataset.put(7..=8, "truncate");
    let (rows, _) = take(
        &dataset,
        json!({
            "dataset": null, "table": null,
            "query": format!("SELECT id FROM `{PROJECT}.{}.orders` WHERE id > 7", dataset.name),
        }),
        None,
    );
    assert_eq!(ids(&rows), [8]);
}

#[test]
fn incremental_runs_read_only_what_is_new_by_int_and_by_timestamp() {
    let Some(endpoint) = server() else { return };
    let dataset = TestDataset::new(&endpoint, "incremental");
    dataset.put(1..=5, "append");

    for (column, start) in [
        ("id", "2"),
        ("loaded_at", "TIMESTAMP '2026-09-24 10:02:00.123456'"),
    ] {
        let incremental = json!({ "incremental_column": column, "start": start });
        let (first, summary) = take(&dataset, incremental.clone(), None);
        assert_eq!(ids(&first), [3, 4, 5], "{column}: after start, in order");
        let saved = summary.checkpoint.expect("a position");

        let (second, summary) = take(&dataset, incremental.clone(), Some(&saved));
        assert!(second.is_empty(), "{column}: nothing new");
        assert!(summary.checkpoint.is_none());

        dataset.put(10..=11, "append");
        let (third, summary) = take(&dataset, incremental, Some(&saved));
        assert_eq!(ids(&third), [10, 11], "{column}: only what arrived");
        assert!(summary.checkpoint.is_some());
        // Back to five rows for the next column.
        dataset.put(1..=5, "truncate");
    }
}

#[test]
fn a_table_that_does_not_exist_is_named() {
    let Some(endpoint) = server() else { return };
    let dataset = TestDataset::new(&endpoint, "missing");
    let properties = dataset.properties(json!({ "table": "nope" }));
    let error = BigquerySource
        .read(&properties, &mut Vec::new(), &Context::default())
        .unwrap_err()
        .to_string();
    assert!(error.contains("nope"), "{error}");
    assert!(
        error.contains("not found") || error.contains("Not found"),
        "{error}"
    );

    let error = BigquerySink
        .write(
            &properties,
            &mut fixture::records(orders(1)),
            &Context::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("nope"), "{error}");
}
