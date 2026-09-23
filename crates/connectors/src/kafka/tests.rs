//! The rules that decide whether a record is read once, twice or never, first
//! without a broker, then against a real one when `ETL_TEST_KAFKA` names it
//! (`scripts/test-services.ps1` starts it). Without it the broker tests skip,
//! and say so.

use super::*;
use rskafka::client::controller::ControllerClient;
use rskafka::client::partition::Compression;
use rskafka::client::Client;
use rskafka::record::Record as KafkaRecord;
use serde_json::json;

fn settings(properties: JsonValue) -> Result<Settings, ConnectorError> {
    Settings::from(&properties)
}

fn bounds(list: &[(i32, i64, i64)]) -> Vec<Bounds> {
    list.iter()
        .map(|&(partition, earliest, latest)| Bounds {
            partition,
            earliest,
            latest,
        })
        .collect()
}

fn position(topic: &str, next: &[(i32, i64)]) -> Position {
    Position {
        topic: topic.to_string(),
        next: next.iter().copied().collect(),
    }
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

#[test]
fn brokers_are_host_port_pairs_and_spaces_are_forgiven() {
    let parsed = settings(json!({ "brokers": " a:9092, b:9093 ,", "topic": "t" })).unwrap();
    assert_eq!(parsed.brokers, ["a:9092", "b:9093"]);
    assert_eq!(parsed.start, Start::Earliest);
    assert_eq!(parsed.format, Format::Json);
    assert_eq!(parsed.max_records, 100_000);

    for bad in ["localhost", "a:nine", ":", ":9092", ""] {
        let error = settings(json!({ "brokers": bad, "topic": "t" }))
            .unwrap_err()
            .to_string();
        assert!(error.starts_with("property 'brokers'"), "{bad}: {error}");
    }
}

#[test]
fn a_configuration_that_cannot_work_is_refused_by_property() {
    let refused = |properties: JsonValue| settings(properties).unwrap_err().to_string();
    let base = || json!({ "brokers": "a:9092", "topic": "t" });

    let mut no_topic = base();
    no_topic["topic"] = json!("  ");
    assert!(refused(no_topic).starts_with("property 'topic'"));

    let mut start = base();
    start["start"] = json!("middle");
    assert!(refused(start).starts_with("property 'start'"));

    let mut format = base();
    format["value_format"] = json!("avro");
    assert!(refused(format).starts_with("property 'value_format'"));

    let mut security = base();
    security["security"] = json!("sasl_ssl");
    assert!(refused(security).contains("plaintext only"));

    let mut cap = base();
    cap["max_records"] = json!(0);
    assert!(refused(cap).starts_with("property 'max_records'"));
}

// ---------------------------------------------------------------------------
// The position a run hands to the next
// ---------------------------------------------------------------------------

#[test]
fn a_position_round_trips_through_its_json() {
    let written = position("orders", &[(0, 12), (2, 7)]);
    let value = written.to_json();

    assert_eq!(
        value,
        json!({ "topic": "orders", "offsets": { "0": 12, "2": 7 } })
    );
    assert_eq!(Position::from_json(&value).unwrap(), written);
}

#[test]
fn a_position_this_connector_did_not_write_is_refused_with_the_way_out() {
    for bad in [
        json!(5),
        json!({ "topic": "t" }),
        json!({ "topic": "t", "offsets": { "zero": 1 } }),
        json!({ "topic": "t", "offsets": { "0": -1 } }),
    ] {
        let error = Position::from_json(&bad).unwrap_err().to_string();
        assert!(error.contains("etl state forget"), "{bad}: {error}");
    }
}

// ---------------------------------------------------------------------------
// Where each partition starts
// ---------------------------------------------------------------------------

#[test]
fn a_first_run_starts_where_start_says() {
    let partitions = bounds(&[(0, 3, 10), (1, 0, 4)]);

    let (spans, notes) = plan_spans("t", &partitions, None, Start::Earliest).unwrap();
    assert_eq!(
        spans,
        [
            Span {
                partition: 0,
                from: 3,
                to: 10
            },
            Span {
                partition: 1,
                from: 0,
                to: 4
            },
        ]
    );
    assert!(notes.is_empty());

    let (spans, _) = plan_spans("t", &partitions, None, Start::Latest).unwrap();
    assert!(spans.iter().all(|span| span.from == span.to), "{spans:?}");
}

#[test]
fn a_saved_position_is_carried_on_from_exactly() {
    let (spans, notes) = plan_spans(
        "t",
        &bounds(&[(0, 0, 10), (1, 0, 4)]),
        Some(&position("t", &[(0, 6), (1, 4)])),
        Start::Earliest,
    )
    .unwrap();

    assert_eq!(
        spans[0],
        Span {
            partition: 0,
            from: 6,
            to: 10
        }
    );
    assert_eq!(
        spans[1],
        Span {
            partition: 1,
            from: 4,
            to: 4
        },
        "nothing new"
    );
    assert!(notes.is_empty());
}

#[test]
fn records_deleted_before_they_were_read_are_an_error_that_counts_them() {
    let error = plan_spans(
        "t",
        &bounds(&[(0, 0, 10), (1, 25, 30)]),
        Some(&position("t", &[(0, 10), (1, 20)])),
        Start::Earliest,
    )
    .unwrap_err()
    .to_string();

    assert!(
        error.contains("partition 1: offsets 20 to 24 were deleted"),
        "{error}"
    );
    assert!(error.contains("5 record(s) lost"), "{error}");
    assert!(error.contains("etl state forget"), "{error}");
}

#[test]
fn a_saved_position_past_the_end_says_the_topic_was_probably_remade() {
    let error = plan_spans(
        "t",
        &bounds(&[(0, 0, 3)]),
        Some(&position("t", &[(0, 500)])),
        Start::Earliest,
    )
    .unwrap_err()
    .to_string();

    assert!(
        error.contains("past the end of the partition (3)"),
        "{error}"
    );
}

#[test]
fn a_position_for_another_topic_is_set_aside_and_said_so() {
    let (spans, notes) = plan_spans(
        "new",
        &bounds(&[(0, 2, 9)]),
        Some(&position("old", &[(0, 7)])),
        Start::Earliest,
    )
    .unwrap();

    assert_eq!(spans[0].from, 2, "from start, not from the old topic's 7");
    assert!(notes[0].contains("was for topic 'old'"), "{notes:?}");
}

#[test]
fn a_partition_added_since_the_last_run_starts_from_start_with_a_note() {
    let (spans, notes) = plan_spans(
        "t",
        &bounds(&[(0, 0, 9), (1, 0, 5)]),
        Some(&position("t", &[(0, 9)])),
        Start::Earliest,
    )
    .unwrap();

    assert_eq!(
        spans[1],
        Span {
            partition: 1,
            from: 0,
            to: 5
        }
    );
    assert!(notes[0].contains("partition 1 is new"), "{notes:?}");
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

fn row_of(value: Option<&[u8]>, format: Format) -> Result<Record, ConnectorError> {
    row(
        "orders",
        2,
        41,
        "2026-09-23 10:00:00.000",
        Some(b"k1"),
        value,
        format,
    )
}

#[test]
fn a_json_value_becomes_columns_beside_the_underscore_ones() {
    let row = row_of(Some(br#"{"id": 7, "status": "paid"}"#), Format::Json).unwrap();

    assert_eq!(row["id"], 7);
    assert_eq!(row["status"], "paid");
    assert_eq!(row["_topic"], "orders");
    assert_eq!(row["_partition"], 2);
    assert_eq!(row["_offset"], 41);
    assert_eq!(row["_timestamp"], "2026-09-23 10:00:00.000");
    assert_eq!(row["_key"], "k1");
}

#[test]
fn a_tombstone_is_a_row_of_only_the_underscore_columns() {
    let row = row_of(None, Format::Json).unwrap();
    let mut names: Vec<&str> = row.keys().map(String::as_str).collect();
    names.sort_unstable();
    let mut expected = METADATA_COLUMNS.to_vec();
    expected.sort_unstable();
    assert_eq!(names, expected);
}

#[test]
fn a_value_json_cannot_take_is_refused_naming_where_it_is() {
    let not_json = row_of(Some(b"hello"), Format::Json)
        .unwrap_err()
        .to_string();
    assert!(
        not_json.contains("partition 2 offset 41: the value is not JSON"),
        "{not_json}"
    );

    let bom = row_of(Some(b"\xEF\xBB\xBF{\"id\": 1}"), Format::Json)
        .unwrap_err()
        .to_string();
    assert!(bom.contains("byte-order mark"), "{bom}");

    let array = row_of(Some(b"[1,2]"), Format::Json)
        .unwrap_err()
        .to_string();
    assert!(array.contains("is an array, not a JSON object"), "{array}");

    let clash = row_of(Some(br#"{"_offset": 1}"#), Format::Json)
        .unwrap_err()
        .to_string();
    assert!(clash.contains("a field '_offset'"), "{clash}");
}

#[test]
fn text_and_bytes_give_one_value_column() {
    let text = row_of(Some("naïve".as_bytes()), Format::Text).unwrap();
    assert_eq!(text["value"], "naïve");

    let bytes = row_of(Some(&[0xff, 0x00, 0x10]), Format::Bytes).unwrap();
    assert_eq!(bytes["value"], "/wAQ");

    let not_text = row_of(Some(&[0xff]), Format::Text).unwrap_err().to_string();
    assert!(
        not_text.contains("not UTF-8 text; use value_format bytes"),
        "{not_text}"
    );

    let tombstone = row_of(None, Format::Text).unwrap();
    assert!(tombstone["value"].is_null());
}

#[test]
fn a_timestamp_is_utc_to_the_millisecond() {
    assert_eq!(timestamp_text(0), "1970-01-01 00:00:00.000");
    // 2026-09-23 10:05:07.089 UTC.
    assert_eq!(timestamp_text(1_790_157_907_089), "2026-09-23 10:05:07.089");
    // Before 1970 still counts forwards within the day.
    assert_eq!(timestamp_text(-1), "1969-12-31 23:59:59.999");
}

#[test]
fn a_key_that_is_not_text_is_base64_and_a_missing_one_is_null() {
    let binary = row(
        "t",
        0,
        0,
        "x",
        Some(&[0xff, 0xfe]),
        Some(b"{}"),
        Format::Json,
    )
    .unwrap();
    assert_eq!(binary["_key"], "//4=");

    let none = row("t", 0, 0, "x", None, Some(b"{}"), Format::Json).unwrap();
    assert!(none["_key"].is_null());
}

#[test]
fn a_broker_that_is_not_there_fails_within_the_timeout_rather_than_hanging() {
    // rskafka's own retries have no deadline; this is the test that ours do.
    let started = std::time::Instant::now();
    let mut out: Vec<Record> = Vec::new();

    let error = KafkaSource
        .read(
            &json!({ "brokers": "127.0.0.1:1", "topic": "t", "timeout_ms": 1500 }),
            &mut out,
            &Context::default(),
        )
        .unwrap_err()
        .to_string();

    assert!(error.starts_with("connecting to 127.0.0.1:1"), "{error}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "gave up in {:?}",
        started.elapsed()
    );
}

// ---------------------------------------------------------------------------
// Against a real broker
// ---------------------------------------------------------------------------

/// The broker, or `None` to skip.
fn broker() -> Option<String> {
    match std::env::var("ETL_TEST_KAFKA") {
        Ok(address) if !address.trim().is_empty() => Some(address),
        _ => {
            eprintln!("skipping: ETL_TEST_KAFKA is not set; see scripts/test-services.ps1");
            None
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

async fn client(broker: &str) -> Client {
    ClientBuilder::new(vec![broker.to_string()])
        .backoff_config(backoff(Duration::from_secs(30)))
        .build()
        .await
        .expect("the test broker answers")
}

/// A new topic for one test, so tests never see each other's records.
fn topic_for(test: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("etl-{test}-{}-{nanos}", std::process::id())
}

async fn create(controller: &ControllerClient, topic: &str, partitions: i32) {
    controller
        .create_topic(topic, partitions, 1, 10_000)
        .await
        .expect("creates the topic");
}

/// Produce `ids` as JSON values, round-robin across `partitions`.
async fn produce(client: &Client, topic: &str, partitions: i32, ids: std::ops::Range<u64>) {
    for id in ids {
        let partition = (id % partitions as u64) as i32;
        let partition_client = client
            .partition_client(topic, partition, UnknownTopicHandling::Retry)
            .await
            .unwrap();
        partition_client
            .produce(
                vec![KafkaRecord {
                    key: Some(format!("k{id}").into_bytes()),
                    value: Some(json!({ "id": id }).to_string().into_bytes()),
                    headers: BTreeMap::new(),
                    timestamp: now(),
                }],
                Compression::NoCompression,
            )
            .await
            .unwrap();
    }
}

fn now() -> rskafka::chrono::DateTime<rskafka::chrono::Utc> {
    use rskafka::chrono::TimeZone;
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    rskafka::chrono::Utc.timestamp_millis_opt(millis).unwrap()
}

/// One run: read with `saved`, return the ids read and the new checkpoint.
fn run_once(properties: &JsonValue, saved: Option<JsonValue>) -> (Vec<u64>, JsonValue, Summary) {
    let mut out: Vec<Record> = Vec::new();
    let context = Context {
        checkpoint: saved,
        ..Context::default()
    };
    let summary = KafkaSource
        .read(properties, &mut out, &context)
        .expect("reads");
    let mut ids: Vec<u64> = out.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    ids.sort_unstable();
    let checkpoint = summary
        .checkpoint
        .clone()
        .expect("a source always returns one");
    (ids, checkpoint, summary)
}

#[test]
fn batches_carry_on_exactly_where_the_last_one_stopped() {
    let Some(broker) = broker() else { return };
    let topic = topic_for("batches");
    runtime().block_on(async {
        let client = client(&broker).await;
        create(&client.controller_client().unwrap(), &topic, 3).await;
        produce(&client, &topic, 3, 0..25).await;
    });

    let properties = json!({ "brokers": broker, "topic": topic, "max_records": 10 });

    let mut seen = Vec::new();
    let mut saved = None;
    let mut sizes = Vec::new();
    for _ in 0..4 {
        let (ids, checkpoint, summary) = run_once(&properties, saved.take());
        sizes.push(ids.len());
        seen.extend(ids);
        saved = Some(checkpoint);
        if sizes.len() == 1 {
            assert!(
                summary.detail.contains("more for the next run"),
                "{}",
                summary.detail
            );
        }
    }

    assert_eq!(sizes, [10, 10, 5, 0]);
    seen.sort_unstable();
    assert_eq!(
        seen,
        (0..25).collect::<Vec<u64>>(),
        "every record once, none twice"
    );
}

#[test]
fn a_run_that_is_not_saved_is_read_again() {
    // The engine saves a checkpoint only after a run that succeeded. From the
    // connector's side that means: handed the same position, it reads the same
    // records.
    let Some(broker) = broker() else { return };
    let topic = topic_for("reread");
    runtime().block_on(async {
        let client = client(&broker).await;
        create(&client.controller_client().unwrap(), &topic, 2).await;
        produce(&client, &topic, 2, 0..6).await;
    });
    let properties = json!({ "brokers": broker, "topic": topic, "max_records": 4 });

    let (first, _, _) = run_once(&properties, None);
    let (again, _, _) = run_once(&properties, None);
    assert_eq!(first, again);
    assert_eq!(first.len(), 4);
}

#[test]
fn latest_reads_nothing_first_and_then_only_what_arrived() {
    let Some(broker) = broker() else { return };
    let topic = topic_for("latest");
    runtime().block_on(async {
        let client = client(&broker).await;
        create(&client.controller_client().unwrap(), &topic, 2).await;
        produce(&client, &topic, 2, 0..5).await;
    });
    let properties = json!({ "brokers": broker, "topic": topic, "start": "latest" });

    let (first, saved, _) = run_once(&properties, None);
    assert!(first.is_empty());

    runtime().block_on(async {
        let client = client(&broker).await;
        produce(&client, &topic, 2, 5..8).await;
    });
    let (second, _, _) = run_once(&properties, Some(saved));
    assert_eq!(second, [5, 6, 7]);
}

#[test]
fn deleted_records_make_the_next_read_fail_with_how_many() {
    let Some(broker) = broker() else { return };
    let topic = topic_for("gap");
    runtime().block_on(async {
        let client = client(&broker).await;
        create(&client.controller_client().unwrap(), &topic, 1).await;
        produce(&client, &topic, 1, 0..6).await;
    });
    let properties = json!({ "brokers": broker, "topic": topic, "max_records": 2 });

    let (_, saved, _) = run_once(&properties, None);
    runtime().block_on(async {
        // What retention does, done by hand: everything below offset 5 goes.
        let client = client(&broker).await;
        let partition = client
            .partition_client(topic.as_str(), 0, UnknownTopicHandling::Retry)
            .await
            .unwrap();
        partition.delete_records(5, 10_000).await.unwrap();
    });

    let mut out: Vec<Record> = Vec::new();
    let error = KafkaSource
        .read(
            &properties,
            &mut out,
            &Context {
                checkpoint: Some(saved),
                ..Context::default()
            },
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("offsets 2 to 4 were deleted"), "{error}");
    assert!(error.contains("3 record(s) lost"), "{error}");
    assert!(out.is_empty(), "nothing is read past a gap");
}

#[test]
fn a_topic_that_does_not_exist_is_named() {
    let Some(broker) = broker() else { return };
    let mut out: Vec<Record> = Vec::new();
    let error = KafkaSource
        .read(
            &json!({ "brokers": broker, "topic": "etl-no-such-topic" }),
            &mut out,
            &Context::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("there is no topic 'etl-no-such-topic'"),
        "{error}"
    );
}
