//! What decides whether a document is read once, again, or never: the filter
//! the run adds, the checkpoint it hands back, a changed collection starting
//! over; and how values cross, both ways. Settings and values without a
//! server; the rest against MongoDB when `ETL_TEST_MONGODB` names it
//! (`scripts/test-services.ps1` starts it). Without it those skip.

use super::*;
use crate::fixture;
use ::mongodb::bson::oid::ObjectId;
use ::mongodb::bson::{Binary, DateTime, Decimal128};

// ---------------------------------------------------------------------------
// Without a server
// ---------------------------------------------------------------------------

fn base(extra: JsonValue) -> JsonValue {
    let mut all = json!({ "uri": "mongodb://h", "database": "d", "collection": "c" });
    for (key, value) in extra.as_object().unwrap() {
        all[key] = value.clone();
    }
    all
}

#[test]
fn a_setting_that_cannot_work_is_refused_by_property() {
    let source = |extra: JsonValue| MongoSource.check(&base(extra)).map_err(|e| e.to_string());
    let sink = |extra: JsonValue| MongoSink.check(&base(extra)).map_err(|e| e.to_string());
    let refused = |result: Result<(), String>, property: &str| {
        let error = result.unwrap_err();
        assert!(
            error.starts_with(&format!("property '{property}'")),
            "{property}: {error}"
        );
    };

    refused(source(json!({ "uri": "http://h" })), "uri");
    refused(source(json!({ "database": null })), "database");
    refused(source(json!({ "collection": "" })), "collection");
    refused(source(json!({ "filter": "{not json" })), "filter");
    refused(source(json!({ "filter": "[1, 2]" })), "filter");
    refused(source(json!({ "start": 5 })), "start");
    refused(
        source(json!({ "incremental_field": "at", "filter": { "at": 1 } })),
        "filter",
    );
    source(json!({
        "filter": r#"{"status": "paid", "at": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}}"#,
        "projection": { "status": 1 }, "sort": { "at": -1 },
    }))
    .expect("fine");
    source(json!({ "incremental_field": "_id", "start": { "$oid": "6ab4cdc2005a24e5bf518260" } }))
        .expect("fine");

    refused(sink(json!({ "mode": "upsert" })), "key_fields");
    refused(sink(json!({ "key_fields": ["id"] })), "key_fields");
    refused(sink(json!({ "mode": "replace" })), "mode");
    sink(json!({ "mode": "upsert", "key_fields": ["order_id"] })).expect("fine");
}

#[test]
fn values_that_are_not_json_are_made_plain() {
    let id = ObjectId::parse_str("6ab4cdc2005a24e5bf518260").unwrap();
    let document = doc! {
        "_id": id,
        "at": DateTime::from_millis(1_790_157_907_089),
        "amount": "123.45".parse::<Decimal128>().unwrap(),
        "big": 9_007_199_254_740_993i64,
        "customer": { "id": id, "since": DateTime::from_millis(0) },
        "tags": ["a", 1, Bson::Null],
        "blob": Binary { subtype: ::mongodb::bson::spec::BinarySubtype::Generic, bytes: vec![0, 1] },
    };
    assert_eq!(
        JsonValue::Object(row(&document)),
        json!({
            "_id": "6ab4cdc2005a24e5bf518260",
            "at": "2026-09-23 10:05:07.089",
            "amount": "123.45",
            "big": 9_007_199_254_740_993i64,
            "customer": { "id": "6ab4cdc2005a24e5bf518260", "since": "1970-01-01 00:00:00.000" },
            "tags": ["a", 1, null],
            "blob": "AAE=",
        })
    );
}

#[test]
fn a_row_becomes_a_document_by_extended_json() {
    let written = document(
        1,
        json!({
            "order_id": 7, "at": { "$date": "2026-09-23T10:05:07.089Z" },
            "owner": { "$oid": "6ab4cdc2005a24e5bf518260" }, "note": "plain",
        })
        .as_object()
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        written
            .get_i64("order_id")
            .ok()
            .or(written.get_i32("order_id").ok().map(i64::from)),
        Some(7)
    );
    assert!(matches!(written.get("at"), Some(Bson::DateTime(_))));
    assert!(matches!(written.get("owner"), Some(Bson::ObjectId(_))));
    assert_eq!(written.get_str("note").unwrap(), "plain");
}

// ---------------------------------------------------------------------------
// Against MongoDB
// ---------------------------------------------------------------------------

fn server() -> Option<String> {
    match std::env::var("ETL_TEST_MONGODB") {
        Ok(uri) if !uri.trim().is_empty() => Some(uri.trim().to_string()),
        _ => {
            eprintln!("skipping: ETL_TEST_MONGODB is not set; see scripts/test-services.ps1");
            None
        }
    }
}

/// A collection for one test, dropped when the test ends, pass or fail.
struct TestCollection {
    uri: String,
    name: String,
}

impl TestCollection {
    fn new(uri: &str, test: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        TestCollection {
            uri: uri.to_string(),
            name: format!("{test}_{}_{nanos}", std::process::id()),
        }
    }

    fn handle(&self) -> Collection<Document> {
        Client::with_uri_str(&self.uri)
            .unwrap()
            .database("etl_test")
            .collection(&self.name)
    }

    fn insert(&self, documents: Vec<Document>) {
        self.handle().insert_many(documents).run().unwrap();
    }

    fn count(&self) -> u64 {
        self.handle().count_documents(doc! {}).run().unwrap()
    }

    fn properties(&self, extra: JsonValue) -> JsonValue {
        let mut all = json!({
            "uri": self.uri, "database": "etl_test", "collection": self.name,
            "timeout_ms": 5_000,
        });
        for (key, value) in extra.as_object().unwrap() {
            all[key] = value.clone();
        }
        all
    }
}

impl Drop for TestCollection {
    fn drop(&mut self) {
        let _ = self.handle().drop().run();
    }
}

/// Orders 1..=count, one a minute from a fixed time.
fn orders(range: std::ops::RangeInclusive<i64>) -> Vec<Document> {
    range
        .map(|n| {
            doc! {
                "order_id": n,
                "customer": { "id": format!("C{}", n % 3), "vip": n % 2 == 0 },
                "amount": format!("{n}.50").parse::<Decimal128>().unwrap(),
                "at": DateTime::from_millis(1_790_000_000_000 + n * 60_000),
                "status": if n % 2 == 0 { "paid" } else { "open" },
            }
        })
        .collect()
}

fn take(
    collection: &TestCollection,
    extra: JsonValue,
    saved: Option<&JsonValue>,
) -> (Vec<Record>, Summary) {
    let settings =
        SourceSettings::from(&collection.properties(extra), &Context::default()).unwrap();
    let mut rows: Vec<Record> = Vec::new();
    let summary = read(&settings, &mut rows, saved).expect("reads");
    (rows, summary)
}

fn ids(rows: &[Record]) -> Vec<i64> {
    rows.iter()
        .map(|r| r["order_id"].as_i64().unwrap())
        .collect()
}

#[test]
fn a_filter_projection_and_sort_read_typed_rows() {
    let Some(uri) = server() else { return };
    let collection = TestCollection::new(&uri, "read");
    collection.insert(orders(1..=6));

    let (rows, summary) = take(
        &collection,
        json!({
            "filter": r#"{"status": "paid"}"#,
            "projection": { "_id": 0, "order_id": 1, "amount": 1, "at": 1, "customer": 1 },
            "sort": { "order_id": -1 },
        }),
        None,
    );
    assert_eq!(ids(&rows), [6, 4, 2]);
    assert_eq!(
        JsonValue::Object(rows[0].clone()),
        json!({
            "order_id": 6, "amount": "6.50", "at": "2026-09-21 14:19:20.000",
            "customer": { "id": "C0", "vip": true },
        })
    );
    assert_eq!(
        summary.checkpoint, None,
        "not incremental: nothing to remember"
    );
    assert!(
        summary.detail.starts_with(&format!(
            "3 document(s) from collection '{}' at 127.0.0.1:57017, database 'etl_test'",
            collection.name
        )),
        "{}",
        summary.detail
    );
    assert!(!summary.detail.contains("etl-secret"));
}

#[test]
fn an_incremental_read_takes_only_what_is_new_and_says_where_it_got_to() {
    let Some(uri) = server() else { return };
    let collection = TestCollection::new(&uri, "incremental");
    collection.insert(orders(1..=5));
    let incremental = json!({ "incremental_field": "at" });

    let (first, summary) = take(&collection, incremental.clone(), None);
    assert_eq!(ids(&first), [1, 2, 3, 4, 5], "in the field's order");
    let saved = summary.checkpoint.expect("a position");
    assert_eq!(saved["field"], "at");
    assert_eq!(
        saved["value"],
        json!({ "$date": { "$numberLong": "1790000300000" } }),
        "kept as a date"
    );

    let (second, summary) = take(&collection, incremental.clone(), Some(&saved));
    assert!(second.is_empty());
    assert_eq!(summary.checkpoint, None, "nothing read: the position stays");
    assert!(
        summary.detail.contains("nothing new after at"),
        "{}",
        summary.detail
    );

    collection.insert(orders(6..=9));
    let (third, summary) = take(
        &collection,
        json!({ "incremental_field": "at", "max_records": 2 }),
        Some(&saved),
    );
    assert_eq!(ids(&third), [6, 7], "the oldest new ones first");
    assert!(
        summary.detail.contains("with more for the next run"),
        "{}",
        summary.detail
    );
    let (rest, _) = take(&collection, incremental, summary.checkpoint.as_ref());
    assert_eq!(ids(&rest), [8, 9]);
}

#[test]
fn start_and_a_changed_node_decide_where_a_read_begins() {
    let Some(uri) = server() else { return };
    let collection = TestCollection::new(&uri, "start");
    collection.insert(orders(1..=4));

    let (rows, _) = take(
        &collection,
        json!({ "incremental_field": "order_id", "start": 2 }),
        None,
    );
    assert_eq!(ids(&rows), [3, 4]);

    // A position saved for another collection is set aside, and said so.
    let elsewhere = json!({ "collection": "etl_test.other", "field": "order_id", "value": 99 });
    let (rows, summary) = take(
        &collection,
        json!({ "incremental_field": "order_id" }),
        Some(&elsewhere),
    );
    assert_eq!(ids(&rows), [1, 2, 3, 4]);
    assert!(
        summary.detail.ends_with(
            "the saved position was for etl_test.other by 'order_id', so this read started over"
        ),
        "{}",
        summary.detail
    );
}

#[test]
fn what_cannot_be_reached_is_named_without_the_password() {
    let Some(uri) = server() else { return };
    let read_with = |properties: JsonValue| {
        MongoSource
            .read(&properties, &mut Vec::new(), &Context::default())
            .unwrap_err()
            .to_string()
    };
    let collection = TestCollection::new(&uri, "missing");

    let missing = read_with(collection.properties(json!({})));
    assert!(
        missing.ends_with(&format!("there is no collection '{}'", collection.name)),
        "{missing}"
    );

    let refused = read_with(collection.properties(json!({ "password": "not-the-password" })));
    assert!(refused.contains("uthentication failed"), "{refused}");
    assert!(!refused.contains("not-the-password") && !refused.contains("etl-secret"));

    let nobody = read_with(json!({
        "uri": "mongodb://127.0.0.1:1", "database": "d", "collection": "c", "timeout_ms": 500,
    }));
    assert!(nobody.contains("no server answered in time"), "{nobody}");
}

#[test]
fn tls_trusts_the_ca_it_is_given_and_nothing_else() {
    let (Ok(uri), Ok(ca)) = (
        std::env::var("ETL_TEST_MONGODB_TLS"),
        std::env::var("ETL_TEST_KAFKA_CA"),
    ) else {
        eprintln!("skipping: ETL_TEST_MONGODB_TLS is not set; see scripts/test-services.ps1");
        return;
    };
    let plain = server().expect("the same server, plain");
    let collection = TestCollection::new(&plain, "tls");
    collection.insert(orders(1..=3));
    let over_tls = |extra: JsonValue| {
        let mut properties = collection.properties(extra);
        properties["uri"] = json!(uri);
        properties
    };

    let mut rows: Vec<Record> = Vec::new();
    MongoSource
        .read(
            &over_tls(json!({ "ca_cert": ca })),
            &mut rows,
            &Context::default(),
        )
        .expect("reads over TLS");
    assert_eq!(rows.len(), 3);

    let error = MongoSource
        .read(&over_tls(json!({})), &mut Vec::new(), &Context::default())
        .unwrap_err()
        .to_string();
    assert!(error.contains("UnknownIssuer"), "{error}");
}

// ----- the sink -----

fn rows_of(documents: Vec<Document>) -> Vec<JsonValue> {
    documents
        .iter()
        .map(|document| JsonValue::Object(row(document)))
        .collect()
}

#[test]
fn insert_adds_every_row_and_a_refused_one_is_named() {
    let Some(uri) = server() else { return };
    let collection = TestCollection::new(&uri, "insert");
    let summary = MongoSink
        .write(
            &collection.properties(json!({})),
            &mut fixture::records(rows_of(orders(1..=2500))),
            &Context::default(),
        )
        .unwrap();
    assert_eq!(summary.records, 2500);
    assert!(
        summary.detail.ends_with("in 3 call(s)"),
        "{}",
        summary.detail
    );
    assert_eq!(collection.count(), 2500);

    // The same _id twice, in the middle: the rest of the batch still lands,
    // the row after it included.
    let twice = vec![
        json!({ "_id": "a", "n": 1 }),
        json!({ "_id": "a", "n": 2 }),
        json!({ "_id": "b", "n": 3 }),
    ];
    let other = TestCollection::new(&uri, "refused");
    let error = MongoSink
        .write(
            &other.properties(json!({})),
            &mut fixture::records(twice),
            &Context::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with("row 2 was refused (0 more in its batch): E11000"),
        "{error}"
    );
    assert!(error.contains("2 document(s) had been written"), "{error}");
    assert_eq!(other.count(), 2);
}

#[test]
fn upsert_replaces_on_its_keys_so_a_rerun_adds_nothing() {
    let Some(uri) = server() else { return };
    let collection = TestCollection::new(&uri, "upsert");
    let properties = collection.properties(json!({ "mode": "upsert", "key_fields": ["order_id"] }));
    let write = |rows: Vec<JsonValue>| {
        MongoSink
            .write(
                &properties,
                &mut fixture::records(rows),
                &Context::default(),
            )
            .unwrap()
    };

    let first = write(rows_of(orders(1..=5)));
    assert!(
        first
            .detail
            .starts_with("0 document(s) replaced and 5 added"),
        "{}",
        first.detail
    );
    let mut changed = rows_of(orders(1..=5));
    changed[0]["status"] = json!("refunded");
    let second = write(changed);
    assert!(
        second
            .detail
            .starts_with("5 document(s) replaced and 0 added"),
        "{}",
        second.detail
    );
    assert_eq!(collection.count(), 5, "a re-run adds nothing");
    let first_order = collection
        .handle()
        .find_one(doc! { "order_id": 1 })
        .run()
        .unwrap()
        .unwrap();
    assert_eq!(first_order.get_str("status").unwrap(), "refunded");

    let error = MongoSink
        .write(
            &properties,
            &mut fixture::records(vec![json!({ "status": "x" })]),
            &Context::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with("row 1 has no value for key field 'order_id'"),
        "{error}"
    );
}
