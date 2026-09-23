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
    assert_eq!(parsed.connection.brokers, ["a:9092", "b:9093"]);
    assert_eq!(parsed.connection.security, Security::Plaintext);
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
    security["security"] = json!("kerberos");
    assert!(refused(security).starts_with("property 'security'"));

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

/// Create a topic and wait until the broker lists it. Creation is
/// asynchronous in Kafka: for a moment after `create_topic` returns, metadata
/// does not show the topic yet, and a connector that lists topics first (as
/// both of ours do) would call it missing. Seen under parallel tests in 10f.
async fn create(client: &Client, topic: &str, partitions: i32) {
    let controller: ControllerClient = client.controller_client().unwrap();
    controller
        .create_topic(topic, partitions, 1, 10_000)
        .await
        .expect("creates the topic");
    for _ in 0..100 {
        let listed = client.list_topics().await.expect("lists topics");
        if listed
            .iter()
            .any(|t| t.name == topic && t.partitions.len() == partitions as usize)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("topic {topic} was created but never listed");
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
        create(&client, &topic, 3).await;
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
        create(&client, &topic, 2).await;
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
        create(&client, &topic, 2).await;
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
        create(&client, &topic, 1).await;
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

// ---------------------------------------------------------------------------
// 10f: keys and partitions, without a broker
// ---------------------------------------------------------------------------

#[test]
fn murmur2_matches_the_values_kafkas_own_tests_pin() {
    // From Apache Kafka's UtilsTest.testMurmur2: the Java implementation's
    // results, so a key hashes here exactly as it does in a Java producer.
    let cases: [(&[u8], i32); 6] = [
        (b"21", -973_932_308),
        (b"foobar", -790_332_482),
        (b"a-little-bit-long-string", -985_981_536),
        (b"a-little-bit-longer-string", -1_486_304_829),
        (
            b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
            -58_897_971,
        ),
        (b"abc", 479_470_107),
    ];
    for (key, expected) in cases {
        assert_eq!(murmur2(key), expected, "{}", String::from_utf8_lossy(key));
    }
}

#[test]
fn a_key_always_lands_on_the_same_partition_and_a_negative_hash_is_made_positive() {
    let partitions: Vec<i32> = (0..6).collect();
    // "21" hashes negative; masking, not abs(), is what Java does.
    let expected = (-973_932_308_i32 & 0x7fff_ffff) % 6;
    assert_eq!(partition_for(b"21", &partitions), expected);
    assert_eq!(
        partition_for(b"21", &partitions),
        partition_for(b"21", &partitions)
    );
}

#[test]
fn a_key_is_its_text_its_written_number_or_its_json_and_null_is_none() {
    assert_eq!(key_bytes(&json!("C001")), Some(b"C001".to_vec()));
    assert_eq!(key_bytes(&json!(1001)), Some(b"1001".to_vec()));
    assert_eq!(key_bytes(&json!(true)), Some(b"true".to_vec()));
    assert_eq!(key_bytes(&json!({ "a": 1 })), Some(br#"{"a":1}"#.to_vec()));
    assert_eq!(key_bytes(&JsonValue::Null), None);
}

#[test]
fn rows_are_split_by_key_and_keyless_rows_share_the_batch_partition() {
    let partitions = [0, 1, 2];
    let rows: Vec<Record> = [json!({ "id": 1, "k": "a" }), json!({ "id": 2, "k": null })]
        .into_iter()
        .map(|v| v.as_object().unwrap().clone())
        .collect();

    let assigned = assign(rows, Some("k"), &partitions, 2).unwrap();

    let keyed = partition_for(b"a", &partitions);
    assert_eq!(assigned[&keyed][0].key.as_deref(), Some(&b"a"[..]));
    let keyless = assigned[&2]
        .iter()
        .find(|r| r.key.is_none())
        .expect("row 2");
    let value: JsonValue = serde_json::from_slice(keyless.value.as_ref().unwrap()).unwrap();
    assert_eq!(
        value,
        json!({ "id": 2, "k": null }),
        "the whole row, key column included"
    );

    let missing = assign(
        vec![json!({ "id": 1 }).as_object().unwrap().clone()],
        Some("customer"),
        &partitions,
        0,
    )
    .unwrap_err()
    .to_string();
    assert!(missing.contains("'customer' is not a column"), "{missing}");
}

// ---------------------------------------------------------------------------
// 10f: security settings, without a broker
// ---------------------------------------------------------------------------

#[test]
fn a_security_setting_that_cannot_work_is_refused_by_property() {
    let refused = |properties: JsonValue| settings(properties).unwrap_err().to_string();
    let with = |extra: JsonValue| {
        let mut properties = json!({ "brokers": "a:9092", "topic": "t" });
        for (key, value) in extra.as_object().unwrap() {
            properties[key] = value.clone();
        }
        properties
    };

    let no_user = refused(with(json!({ "security": "sasl_ssl", "password": "p" })));
    assert!(no_user.starts_with("property 'username'"), "{no_user}");

    let no_password = refused(with(
        json!({ "security": "sasl_plaintext", "username": "u" }),
    ));
    assert!(
        no_password.starts_with("property 'password'"),
        "{no_password}"
    );

    let ignored = refused(with(json!({ "security": "ssl", "username": "u" })));
    assert!(ignored.contains("signs in with nothing"), "{ignored}");

    let ca = refused(with(json!({ "ca_cert": "ca.pem" })));
    assert!(ca.starts_with("property 'ca_cert'"), "{ca}");

    let mechanism = refused(with(json!({
        "security": "sasl_ssl", "sasl_mechanism": "gssapi", "username": "u", "password": "p"
    })));
    assert!(
        mechanism.starts_with("property 'sasl_mechanism'"),
        "{mechanism}"
    );

    let fine = settings(with(json!({
        "security": "sasl_ssl", "sasl_mechanism": "scram-sha-512",
        "username": "u", "password": "p", "ca_cert": "ca.pem"
    })))
    .unwrap();
    assert_eq!(
        fine.connection.sasl,
        Some((Mechanism::ScramSha512, "u".to_string(), "p".to_string()))
    );
}

#[test]
fn a_connection_is_described_without_its_password() {
    let connection = Connection::from(&json!({
        "brokers": "a:9092", "security": "sasl_ssl", "sasl_mechanism": "scram-sha-256",
        "username": "etl", "password": "hunter2"
    }))
    .unwrap();

    let described = connection.describe();
    assert_eq!(described, "a:9092 (sasl_ssl, SCRAM-SHA-256 as 'etl')");
    assert!(!described.contains("hunter2"));
}

#[test]
fn a_ca_cert_that_is_missing_or_holds_no_certificate_is_named() {
    let context = Context::default();
    let missing = Connection::from(&json!({
        "brokers": "a:9092", "security": "ssl", "ca_cert": "no/such/ca.pem"
    }))
    .unwrap()
    .tls(&context)
    .unwrap_err()
    .to_string();
    assert!(missing.starts_with("property 'ca_cert'"), "{missing}");

    let empty = std::env::temp_dir().join(format!("etl-empty-ca-{}.pem", std::process::id()));
    std::fs::write(&empty, "not a certificate\n").unwrap();
    let nothing = Connection::from(&json!({
        "brokers": "a:9092", "security": "ssl", "ca_cert": empty.to_string_lossy()
    }))
    .unwrap()
    .tls(&context)
    .unwrap_err()
    .to_string();
    assert!(nothing.contains("holds no PEM certificate"), "{nothing}");
}

#[test]
fn a_sink_setting_that_cannot_work_is_refused_by_property() {
    let refused = |extra: JsonValue| {
        let mut properties = json!({ "brokers": "a:9092", "topic": "t" });
        for (key, value) in extra.as_object().unwrap() {
            properties[key] = value.clone();
        }
        KafkaSink.check(&properties).unwrap_err().to_string()
    };

    assert!(refused(json!({ "compression": "brotli" })).starts_with("property 'compression'"));
    assert!(refused(json!({ "batch_size": 0 })).starts_with("property 'batch_size'"));
    assert!(refused(json!({ "topic": "" })).starts_with("property 'topic'"));
    KafkaSink
        .check(&json!({ "brokers": "a:9092", "topic": "t", "compression": "zstd" }))
        .expect("fine");
}

// ---------------------------------------------------------------------------
// 10f: against a real broker
// ---------------------------------------------------------------------------

fn write_rows(properties: &JsonValue, rows: Vec<JsonValue>) -> Result<Summary, ConnectorError> {
    let mut reader = crate::fixture::records(rows);
    KafkaSink.write(properties, &mut reader, &Context::default())
}

/// Everything a topic holds, read from the start.
fn read_all(properties: &JsonValue) -> Vec<Record> {
    let mut out: Vec<Record> = Vec::new();
    KafkaSource
        .read(properties, &mut out, &Context::default())
        .expect("reads");
    out
}

fn new_topic(broker: &str, test: &str, partitions: i32) -> String {
    let topic = topic_for(test);
    runtime().block_on(async {
        let client = client(broker).await;
        create(&client, &topic, partitions).await;
    });
    topic
}

#[test]
fn rows_written_are_read_back_keyed_to_the_partitions_java_would_pick() {
    let Some(broker) = broker() else { return };
    let topic = new_topic(&broker, "sink", 6);

    let rows: Vec<JsonValue> = (1..=20)
        .map(|id| json!({ "id": id, "customer": format!("C{}", id % 7) }))
        .collect();
    let summary = write_rows(
        &json!({ "brokers": broker, "topic": topic, "key_column": "customer", "batch_size": 8 }),
        rows,
    )
    .expect("writes");
    assert_eq!(summary.records, 20);
    assert!(
        summary.detail.contains("in 3 batch(es)"),
        "{}",
        summary.detail
    );

    let read = read_all(&json!({ "brokers": broker, "topic": topic }));
    assert_eq!(read.len(), 20);
    let partitions: Vec<i32> = (0..6).collect();
    for row in &read {
        let key = row["_key"].as_str().unwrap();
        assert_eq!(row["customer"], key, "the key is the column's value");
        assert_eq!(
            row["_partition"].as_i64().unwrap() as i32,
            partition_for(key.as_bytes(), &partitions),
            "{key}"
        );
    }
}

#[test]
fn keyless_rows_spread_across_partitions_by_batch() {
    let Some(broker) = broker() else { return };
    let topic = new_topic(&broker, "keyless", 3);

    write_rows(
        &json!({ "brokers": broker, "topic": topic, "batch_size": 2 }),
        (0..6).map(|id| json!({ "id": id })).collect(),
    )
    .expect("writes");

    let read = read_all(&json!({ "brokers": broker, "topic": topic }));
    let mut per_partition = [0; 3];
    for row in &read {
        assert!(row["_key"].is_null());
        per_partition[row["_partition"].as_i64().unwrap() as usize] += 1;
    }
    assert_eq!(per_partition, [2, 2, 2]);
}

#[test]
fn every_compression_codec_round_trips() {
    let Some(broker) = broker() else { return };
    for codec in ["gzip", "snappy", "lz4", "zstd"] {
        let topic = new_topic(&broker, codec, 1);
        write_rows(
            &json!({ "brokers": broker, "topic": topic, "compression": codec }),
            (0..3)
                .map(|id| json!({ "id": id, "codec": codec }))
                .collect(),
        )
        .unwrap_or_else(|error| panic!("{codec}: {error}"));

        let read = read_all(&json!({ "brokers": broker, "topic": topic }));
        assert_eq!(read.len(), 3, "{codec}");
        assert!(read.iter().all(|row| row["codec"] == codec), "{codec}");
    }
}

#[test]
fn a_batch_the_broker_refuses_says_how_much_was_already_delivered() {
    let Some(broker) = broker() else { return };
    let topic = new_topic(&broker, "partial", 1);

    // The fifth row is larger than a broker takes by default (1 MB), so the
    // third batch of two is refused.
    let mut rows: Vec<JsonValue> = (1..=4).map(|id| json!({ "id": id })).collect();
    rows.push(json!({ "id": 5, "blob": "x".repeat(2 * 1024 * 1024) }));

    let error = write_rows(
        &json!({ "brokers": broker, "topic": topic, "batch_size": 2, "timeout_ms": 10000 }),
        rows,
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.starts_with("batch 3 failed after 2 batch(es) (4 record(s)) were delivered"),
        "{error}"
    );
    assert_eq!(
        read_all(&json!({ "brokers": broker, "topic": topic })).len(),
        4
    );
}

/// A secured listener, or `None` to skip.
fn secured(variable: &str) -> Option<(String, String, String)> {
    let listener = std::env::var(variable)
        .ok()
        .filter(|v| !v.trim().is_empty());
    let (Some(listener), Some(plain)) = (listener, broker()) else {
        eprintln!("skipping: {variable} is not set; see scripts/test-services.ps1");
        return None;
    };
    let ca = std::env::var("ETL_TEST_KAFKA_CA").unwrap_or_default();
    Some((listener, plain, ca))
}

/// Write two rows and read them back through one secured listener.
fn round_trip_through(listener: &str, plain: &str, test: &str, security: JsonValue) {
    let topic = new_topic(plain, test, 1);
    let mut properties = json!({ "brokers": listener, "topic": topic, "timeout_ms": 15000 });
    for (key, value) in security.as_object().unwrap() {
        properties[key] = value.clone();
    }

    write_rows(&properties, vec![json!({ "id": 1 }), json!({ "id": 2 })])
        .unwrap_or_else(|error| panic!("{test} write: {error}"));
    let mut out: Vec<Record> = Vec::new();
    KafkaSource
        .read(&properties, &mut out, &Context::default())
        .unwrap_or_else(|error| panic!("{test} read: {error}"));
    assert_eq!(out.len(), 2, "{test}");
}

#[test]
fn sasl_plain_and_both_scram_mechanisms_sign_in() {
    let Some((listener, plain, _)) = secured("ETL_TEST_KAFKA_SASL") else {
        return;
    };
    for mechanism in ["plain", "scram-sha-256", "scram-sha-512"] {
        round_trip_through(
            &listener,
            &plain,
            &format!("sasl-{mechanism}"),
            json!({ "security": "sasl_plaintext", "sasl_mechanism": mechanism,
                    "username": "etl", "password": "etl-secret" }),
        );
    }
}

#[test]
fn tls_with_the_clusters_ca_connects_and_without_it_is_refused() {
    let Some((listener, plain, ca)) = secured("ETL_TEST_KAFKA_TLS") else {
        return;
    };
    round_trip_through(
        &listener,
        &plain,
        "tls",
        json!({ "security": "ssl", "ca_cert": ca }),
    );

    // The public roots do not include a CA made a moment ago, so trusting only
    // them must fail: TLS that trusted anything would pass the line above too.
    let mut out: Vec<Record> = Vec::new();
    let error = KafkaSource
        .read(
            &json!({ "brokers": listener, "topic": "anything", "security": "ssl",
                     "timeout_ms": 3000 }),
            &mut out,
            &Context::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with(&format!("connecting to {listener} (ssl)")),
        "{error}"
    );
    assert!(
        error.contains("UnknownIssuer"),
        "says why, not only that: {error}"
    );
}

#[test]
fn sasl_over_tls_signs_in() {
    let Some((listener, plain, ca)) = secured("ETL_TEST_KAFKA_SASL_TLS") else {
        return;
    };
    round_trip_through(
        &listener,
        &plain,
        "sasl-tls",
        json!({ "security": "sasl_ssl", "sasl_mechanism": "scram-sha-512",
                "username": "etl", "password": "etl-secret", "ca_cert": ca }),
    );
}

#[test]
fn a_wrong_password_fails_naming_the_mechanism_and_not_the_password() {
    let Some((listener, _, _)) = secured("ETL_TEST_KAFKA_SASL") else {
        return;
    };
    let mut out: Vec<Record> = Vec::new();
    let error = KafkaSource
        .read(
            &json!({ "brokers": listener, "topic": "anything", "security": "sasl_plaintext",
                     "sasl_mechanism": "scram-sha-256", "username": "etl",
                     "password": "not-the-password", "timeout_ms": 5000 }),
            &mut out,
            &Context::default(),
        )
        .unwrap_err()
        .to_string();

    assert!(error.contains("SCRAM-SHA-256 as 'etl'"), "{error}");
    // The reason, not a timeout: rskafka retries a failed sign-in until any
    // timeout wins, and an error that only said "no answer" hid this once.
    assert!(error.contains("SaslAuthenticationFailed"), "{error}");
    assert!(!error.contains("not-the-password"), "{error}");
}
