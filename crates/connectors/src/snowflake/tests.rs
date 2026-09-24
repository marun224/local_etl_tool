//! Snowflake has no emulator and no account is used (decision 78), so this is
//! the whole proof: the key's fingerprint against `openssl`'s, the JWT's
//! claims and signature, values typed as Snowflake's documentation writes
//! them, bind variables instead of pasted values, and the SQL API's flow of
//! statements, polls and partitions against the local fixture.

use super::*;
use crate::fixture::{self, Fixture, Seen};
use crate::gcp::tests::rfc_key_pem;
use ring::signature::{UnparsedPublicKey, RSA_PKCS1_2048_8192_SHA256};
use std::sync::{Arc, Mutex};

/// `openssl rsa -in <RFC 7515's key> -pubout -outform DER | openssl dgst
/// -sha256 -binary | openssl enc -base64`, run once in 10p's session: the
/// command Snowflake's documentation gives for the fingerprint it stores.
const RFC_KEY_FINGERPRINT: &str = "SHA256:b9E8JDWjYefFiM0X9V9a098Bd6ZsFyemogCEX016uIw=";

fn base(extra: JsonValue) -> JsonValue {
    let mut all = json!({
        "account": "myorg-acct", "user": "etl_user", "private_key": rfc_key_pem(),
        "table": "orders", "warehouse": "LOAD_WH", "role": "LOADER",
    });
    for (key, value) in extra.as_object().unwrap() {
        all[key] = value.clone();
    }
    all
}

// ---------------------------------------------------------------------------
// Signing in
// ---------------------------------------------------------------------------

fn from_base64url(text: &str) -> Vec<u8> {
    let mut standard = text.replace('-', "+").replace('_', "/");
    while !standard.len().is_multiple_of(4) {
        standard.push('=');
    }
    crate::http::base64_decode(&standard).unwrap()
}

#[test]
fn the_key_is_named_by_the_fingerprint_openssl_gives() {
    let key = rsa_key(&rfc_key_pem()).unwrap();
    assert_eq!(fingerprint(&key), RFC_KEY_FINGERPRINT);
    assert_eq!(jwt_account("xy12345.eu-west-1"), "XY12345");
    assert_eq!(jwt_account("myorg-acct"), "MYORG-ACCT");
}

#[test]
fn the_jwt_names_account_user_and_key_and_is_signed_by_it() {
    let login = Login::from(&base(json!({})), &Context::default()).unwrap();
    let before = etl_state::time::now_unix();
    let token = login.token().unwrap();
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3);
    let header: JsonValue = serde_json::from_slice(&from_base64url(parts[0])).unwrap();
    assert_eq!(header, json!({ "alg": "RS256", "typ": "JWT" }));
    let claims: JsonValue = serde_json::from_slice(&from_base64url(parts[1])).unwrap();
    assert_eq!(
        claims["iss"],
        format!("MYORG-ACCT.ETL_USER.{RFC_KEY_FINGERPRINT}")
    );
    assert_eq!(claims["sub"], "MYORG-ACCT.ETL_USER");
    let issued = claims["iat"].as_i64().unwrap();
    assert!((before..=before + 5).contains(&issued));
    assert!(
        claims["exp"].as_i64().unwrap() - issued <= 3600,
        "Snowflake's hour"
    );

    let key = rsa_key(&rfc_key_pem()).unwrap();
    UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, key.public().as_ref())
        .verify(
            format!("{}.{}", parts[0], parts[1]).as_bytes(),
            &from_base64url(parts[2]),
        )
        .expect("signed by the user's key");
}

#[test]
fn a_setting_that_cannot_work_is_refused_by_property() {
    let source = |extra: JsonValue| {
        SnowflakeSource
            .check(&base(extra))
            .map_err(|e| e.to_string())
    };
    let sink = |extra: JsonValue| SnowflakeSink.check(&base(extra)).map_err(|e| e.to_string());
    let refused = |result: Result<(), String>, property: &str| {
        let error = result.unwrap_err();
        assert!(
            error.starts_with(&format!("property '{property}'")),
            "{property}: {error}"
        );
    };

    refused(source(json!({ "account": null })), "account");
    refused(source(json!({ "user": "" })), "user");
    refused(source(json!({ "private_key": null })), "private_key_file");
    refused(
        source(json!({ "private_key_file": "key.p8" })),
        "private_key",
    );
    refused(source(json!({ "query": "SELECT 1" })), "query");
    refused(source(json!({ "table": null })), "table");
    refused(source(json!({ "table": "a.b.c.d" })), "table");
    refused(source(json!({ "table": "a..b" })), "table");
    refused(source(json!({ "start": "1" })), "start");
    refused(source(json!({ "endpoint": "snowflake.local" })), "endpoint");
    source(json!({ "table": "db.sales.orders", "incremental_column": "id" })).expect("fine");

    refused(sink(json!({ "query": "SELECT 1" })), "query");
    refused(sink(json!({ "mode": "merge" })), "mode");
    sink(json!({ "mode": "truncate" })).expect("fine");

    let encrypted = Api::connect(
        &base(json!({ "private_key": "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAA==\n-----END ENCRYPTED PRIVATE KEY-----" })),
        &Context::default(),
    )
    .err()
    .unwrap()
    .to_string();
    assert!(encrypted.contains("holds an encrypted key"), "{encrypted}");
}

// ---------------------------------------------------------------------------
// Values and statements
// ---------------------------------------------------------------------------

#[test]
fn identifiers_are_bare_when_snowflake_reads_them_bare() {
    assert_eq!(identifier("order_id"), "order_id");
    assert_eq!(identifier("Order Id"), "\"Order Id\"");
    assert_eq!(identifier("a\"b"), "\"a\"\"b\"");
    assert_eq!(identifier("1st"), "\"1st\"");
    assert_eq!(table_name("db.sales.orders").unwrap(), "db.sales.orders");
    assert_eq!(
        table_name("db.sales.Big Orders").unwrap(),
        "db.sales.\"Big Orders\""
    );
}

#[test]
fn a_row_is_typed_by_the_results_row_type() {
    let meta = json!({ "rowType": [
        { "name": "ID", "type": "fixed", "scale": 0 },
        { "name": "AMOUNT", "type": "fixed", "scale": 2 },
        { "name": "HUGE", "type": "fixed", "scale": 0 },
        { "name": "RATIO", "type": "real" },
        { "name": "OK", "type": "boolean" },
        { "name": "DAY", "type": "date" },
        { "name": "AT_TIME", "type": "time" },
        { "name": "LOADED", "type": "timestamp_ntz" },
        { "name": "SEEN", "type": "timestamp_tz" },
        { "name": "DOC", "type": "variant" },
        { "name": "NAME", "type": "text" },
        { "name": "GONE", "type": "text" },
    ]});
    let cells = json!([
        "7",
        "12.30",
        "123456789012345678901234567890",
        "0.25",
        "true",
        "20720",
        "36000.123456789",
        "1790244000.123456789",
        "1790244000.000000000 1500",
        "{\"a\": [1, 2]}",
        "Ann",
        null
    ]);
    assert_eq!(
        JsonValue::Object(row(&Column::list(&meta), &cells).unwrap()),
        json!({
            "ID": 7, "AMOUNT": "12.30", "HUGE": "123456789012345678901234567890",
            "RATIO": 0.25, "OK": true, "DAY": "2026-09-24", "AT_TIME": "10:00:00.123456",
            "LOADED": "2026-09-24 10:00:00.123456", "SEEN": "2026-09-24 10:00:00.000000",
            "DOC": { "a": [1, 2] }, "NAME": "Ann", "GONE": null,
        })
    );
}

fn settings(extra: JsonValue) -> SourceSettings {
    SourceSettings::from(&base(extra)).unwrap()
}

#[test]
fn an_incremental_run_binds_its_value_never_pasting_it() {
    let plain = settings(json!({}));
    assert_eq!(statement(&plain, None).0, "SELECT * FROM orders");

    let started =
        settings(json!({ "incremental_column": "loaded", "start": "'2026-01-01'::TIMESTAMP_NTZ" }));
    assert_eq!(
        statement(&started, None).0,
        "SELECT * FROM (SELECT * FROM orders) WHERE loaded > ('2026-01-01'::TIMESTAMP_NTZ) ORDER BY loaded"
    );
    let saved = json!({
        "read": "table orders", "column": "loaded", "type": "TIMESTAMP_NTZ",
        "value": "2026-09-24 10:00:00.123456'); DROP TABLE x; --",
    });
    let (sql, bindings, note) = statement(&started, Some(&saved));
    assert_eq!(
        sql,
        "SELECT * FROM (SELECT * FROM orders) WHERE loaded > CAST(? AS TIMESTAMP_NTZ) ORDER BY loaded"
    );
    assert!(!sql.contains("DROP"));
    assert_eq!(
        bindings.unwrap(),
        json!({ "1": { "type": "TEXT", "value": "2026-09-24 10:00:00.123456'); DROP TABLE x; --" } })
    );
    assert!(note.is_none());

    let elsewhere =
        json!({ "read": "table other", "column": "loaded", "type": "DATE", "value": "x" });
    let (sql, bindings, note) = statement(&started, Some(&elsewhere));
    assert!(sql.contains("> ('2026-01-01'::TIMESTAMP_NTZ)"));
    assert!(bindings.is_none());
    assert_eq!(
        note.unwrap(),
        "; the saved position was for table other by 'loaded', so this read started over"
    );
}

// ---------------------------------------------------------------------------
// The SQL API, against the fixture
// ---------------------------------------------------------------------------

fn fixture_api(fixture: &Fixture) -> Api {
    Api::connect(
        &base(json!({ "endpoint": fixture.url(""), "retries": 0 })),
        &Context::default(),
    )
    .unwrap()
}

fn ids(rows: &[Record]) -> Vec<i64> {
    rows.iter().map(|r| r["ID"].as_i64().unwrap()).collect()
}

fn meta(partitions: usize) -> JsonValue {
    json!({
        "numRows": 5, "format": "jsonv2",
        "partitionInfo": (0..partitions).map(|_| json!({ "rowCount": 2 })).collect::<Vec<_>>(),
        "rowType": [
            { "name": "ID", "type": "fixed", "scale": 0 },
            { "name": "LOADED", "type": "timestamp_ntz", "scale": 9 },
        ],
    })
}

fn data(ids: std::ops::Range<i64>) -> Vec<JsonValue> {
    ids.map(|id| json!([id.to_string(), format!("{}.000000000", 1_790_244_000 + id)]))
        .collect()
}

/// A statement that runs a moment, then has three partitions.
fn slow_statement() -> Fixture {
    fixture::serve(|index, request| {
        if request.method == "POST" && index == 0 {
            return Answer202::running();
        }
        match request.query("partition").as_deref() {
            None => fixture::ok(json!({
                "code": "090001", "statementHandle": "h1", "resultSetMetaData": meta(3),
                "data": data(0..2),
            })),
            Some("1") => fixture::ok(json!({ "data": data(2..4) })),
            _ => fixture::ok(json!({ "data": data(4..5) })),
        }
    })
}

struct Answer202;

impl Answer202 {
    fn running() -> fixture::Answer {
        fixture::Answer {
            status: 202,
            body: json!({
                "code": "333334", "message": "Asynchronous execution in progress.",
                "statementHandle": "h1", "statementStatusUrl": "/api/v2/statements/h1",
            })
            .to_string(),
            headers: Vec::new(),
        }
    }
}

#[test]
fn a_running_statement_is_polled_and_every_partition_read() {
    let fixture = slow_statement();
    let mut rows: Vec<Record> = Vec::new();
    let summary = read(
        &mut fixture_api(&fixture),
        &settings(json!({})),
        &mut rows,
        None,
    )
    .unwrap();
    assert_eq!(ids(&rows), [0, 1, 2, 3, 4]);
    assert_eq!(rows[0]["LOADED"], "2026-09-24 10:00:00.000000");
    assert_eq!(
        summary.detail,
        "5 row(s) from table orders at account myorg-acct as ETL_USER by statement h1"
    );

    let seen = fixture.seen();
    let submit = &seen[0];
    assert_eq!(submit.method, "POST");
    assert_eq!(submit.path(), "/api/v2/statements");
    assert!(
        submit.query("requestId").is_some(),
        "a retried submit is the same statement"
    );
    assert_eq!(
        submit.header("X-Snowflake-Authorization-Token-Type"),
        Some("KEYPAIR_JWT")
    );
    assert!(submit
        .header("Authorization")
        .unwrap()
        .starts_with("Bearer ey"));
    let body: JsonValue = serde_json::from_str(&submit.body).unwrap();
    assert_eq!(body["statement"], "SELECT * FROM orders");
    assert_eq!(body["warehouse"], "LOAD_WH");
    assert_eq!(body["role"], "LOADER");
    assert_eq!(body["parameters"]["TIMEZONE"], "UTC");

    assert_eq!(seen[1].method, "GET");
    assert_eq!(seen[1].path(), "/api/v2/statements/h1");
    assert_eq!(seen[2].query("partition").as_deref(), Some("1"));
    assert_eq!(seen[3].query("partition").as_deref(), Some("2"));
    assert_eq!(seen.len(), 4);
}

#[test]
fn max_records_stops_without_the_later_partitions() {
    let fixture = slow_statement();
    let mut rows: Vec<Record> = Vec::new();
    let summary = read(
        &mut fixture_api(&fixture),
        &settings(json!({ "max_records": 2 })),
        &mut rows,
        None,
    )
    .unwrap();
    assert_eq!(ids(&rows), [0, 1]);
    assert!(summary
        .detail
        .ends_with("stopped at max_records, with more for the next run"));
    assert_eq!(fixture.seen().len(), 2, "no partition fetched");
}

#[test]
fn an_incremental_read_saves_its_highest_value_and_binds_it_next_time() {
    let fixture = fixture::serve(|_, _| {
        fixture::ok(
            json!({ "statementHandle": "h", "resultSetMetaData": meta(1), "data": data(0..3) }),
        )
    });
    let incremental = settings(json!({ "incremental_column": "loaded" }));
    let summary = read(
        &mut fixture_api(&fixture),
        &incremental,
        &mut Vec::new(),
        None,
    )
    .unwrap();
    let saved = summary.checkpoint.expect("a position");
    assert_eq!(
        saved,
        json!({
            "read": "table orders", "column": "loaded", "type": "TIMESTAMP_NTZ",
            "value": "2026-09-24 10:00:02.000000",
        })
    );

    read(
        &mut fixture_api(&fixture),
        &incremental,
        &mut Vec::new(),
        Some(&saved),
    )
    .unwrap();
    let second: JsonValue = serde_json::from_str(&fixture.seen()[1].body).unwrap();
    assert_eq!(
        second["statement"],
        "SELECT * FROM (SELECT * FROM orders) WHERE loaded > CAST(? AS TIMESTAMP_NTZ) ORDER BY loaded"
    );
    assert_eq!(
        second["bindings"]["1"]["value"],
        "2026-09-24 10:00:02.000000"
    );

    let wrong = settings(json!({ "incremental_column": "nope" }));
    let error = read(&mut fixture_api(&fixture), &wrong, &mut Vec::new(), None)
        .unwrap_err()
        .to_string();
    assert_eq!(
        error,
        "property 'incremental_column': 'nope' is not a column of what is read"
    );
}

#[test]
fn snowflakes_refusal_is_named_with_its_code() {
    let fixture = fixture::serve(|_, _| {
        fixture::status(
            422,
            r#"{"code":"002003","message":"SQL compilation error:\nObject 'ORDERS' does not exist or not authorized.","sqlState":"02000","statementHandle":"h"}"#,
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
        error.starts_with(
            "table orders at account myorg-acct as ETL_USER: Snowflake statement: HTTP 422"
        ),
        "{error}"
    );
    assert!(
        error.ends_with("Object 'ORDERS' does not exist or not authorized. (code 002003)"),
        "{error}"
    );
}

// ----- the sink -----

/// A Snowflake taking INSERTs, answering each with its row count, or refusing
/// the `fail`th statement.
fn warehouse(fail: Option<usize>) -> (Fixture, Arc<Mutex<Vec<JsonValue>>>) {
    let statements = Arc::new(Mutex::new(Vec::new()));
    let seen = statements.clone();
    let fixture = fixture::serve(move |index, request: &Seen| {
        let body: JsonValue = serde_json::from_str(&request.body).unwrap();
        seen.lock().unwrap().push(body.clone());
        if Some(index) == fail {
            return fixture::status(
                422,
                r#"{"code":"100038","message":"Numeric value 'x' is not recognized"}"#,
            );
        }
        let rows = body["bindings"]["1"]["value"]
            .as_array()
            .map_or(0, Vec::len);
        fixture::ok(json!({
            "statementHandle": format!("h{index}"),
            "resultSetMetaData": { "rowType": [{ "name": "number of rows inserted", "type": "fixed", "scale": 0 }] },
            "data": [[rows.to_string()]],
        }))
    });
    (fixture, statements)
}

fn orders(count: i64) -> Vec<JsonValue> {
    (1..=count)
        .map(|id| json!({ "order_id": id, "amount": 12.5, "note": null, "meta": { "a": 1 } }))
        .collect()
}

fn sink_settings(extra: JsonValue) -> SinkSettings {
    SinkSettings::from(&base(extra)).unwrap()
}

#[test]
fn rows_are_inserted_a_thousand_to_a_statement_bound_as_arrays() {
    let (fixture, statements) = warehouse(None);
    let summary = insert(
        &mut fixture_api(&fixture),
        &sink_settings(json!({ "mode": "truncate" })),
        &mut fixture::records(orders(2500)),
    )
    .unwrap();
    let statements = statements.lock().unwrap().clone();
    assert_eq!(statements[0]["statement"], "TRUNCATE TABLE orders");
    let inserts = &statements[1..];
    assert_eq!(inserts.len(), 3);
    assert_eq!(
        inserts[0]["statement"],
        "INSERT INTO orders (order_id, amount, note, meta) VALUES (?, ?, ?, ?)"
    );
    let first = &inserts[0]["bindings"];
    assert_eq!(first["1"]["type"], "TEXT");
    assert_eq!(first["1"]["value"].as_array().unwrap().len(), 1000);
    assert_eq!(first["1"]["value"][0], "1");
    assert_eq!(first["2"]["value"][0], "12.5");
    assert_eq!(first["3"]["value"][0], JsonValue::Null);
    assert_eq!(first["4"]["value"][0], "{\"a\":1}");
    assert_eq!(
        inserts[2]["bindings"]["1"]["value"]
            .as_array()
            .unwrap()
            .len(),
        500
    );
    assert_eq!(summary.records, 2500);
    assert_eq!(
        summary.detail,
        "2500 row(s) replaced the rows of orders at account myorg-acct as ETL_USER in 3 INSERT(s)"
    );
}

#[test]
fn a_refused_insert_says_what_landed_before_it() {
    let (fixture, _) = warehouse(Some(1));
    let error = insert(
        &mut fixture_api(&fixture),
        &sink_settings(json!({})),
        &mut fixture::records(orders(1500)),
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("Numeric value 'x' is not recognized (code 100038)"),
        "{error}"
    );
    assert!(
        error.ends_with("1000 row(s) had been inserted into orders before this, and stay"),
        "{error}"
    );

    let (fixture, _) = warehouse(None);
    let mut rows = orders(2);
    rows[1]["surprise"] = json!(1);
    let error = insert(
        &mut fixture_api(&fixture),
        &sink_settings(json!({})),
        &mut fixture::records(rows),
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.starts_with("row 2 has a column 'surprise' the first row did not"),
        "{error}"
    );
}
