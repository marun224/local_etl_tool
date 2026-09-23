//! The rules that decide whether a message is read once, twice or never, first
//! without a server, then against real NATS servers when `ETL_TEST_NATS` and
//! its siblings name them (`scripts/test-services.ps1` starts them). Without
//! them the server tests skip, and say so.

use super::*;
use async_nats::jetstream::stream::Config as StreamConfig;
use serde_json::json;

fn connection(properties: JsonValue) -> Result<Connection, ConnectorError> {
    Connection::from(&properties)
}

fn refused(properties: JsonValue) -> String {
    connection(properties).unwrap_err().to_string()
}

// ---------------------------------------------------------------------------
// Settings, without a server
// ---------------------------------------------------------------------------

#[test]
fn urls_and_sign_in_are_read_and_a_tls_url_turns_tls_on() {
    let plain = connection(json!({ "url": " nats://a:4222, nats://b:4222 " })).unwrap();
    assert_eq!(plain.urls, ["nats://a:4222", "nats://b:4222"]);
    assert_eq!(plain.auth, Auth::None);
    assert!(!plain.tls);

    let tls = connection(json!({ "url": "tls://a:4222" })).unwrap();
    assert!(tls.tls);

    let users = connection(json!({
        "url": "nats://a:4222", "auth": "user_password", "username": "u", "password": "p"
    }))
    .unwrap();
    assert_eq!(users.auth, Auth::UserPassword("u".into(), "p".into()));
}

#[test]
fn a_sign_in_that_cannot_work_is_refused_by_property() {
    let base = |extra: JsonValue| {
        let mut properties = json!({ "url": "nats://a:4222" });
        for (key, value) in extra.as_object().unwrap() {
            properties[key] = value.clone();
        }
        properties
    };

    assert!(refused(json!({ "url": "" })).starts_with("property 'url'"));
    assert!(refused(json!({ "url": "http://a:4222" })).contains("use nats:// or tls://"));
    assert!(
        refused(base(json!({ "auth": "user_password", "username": "u" })))
            .starts_with("property 'password'")
    );
    assert!(refused(base(json!({ "auth": "token" }))).starts_with("property 'token'"));
    assert!(refused(base(json!({ "auth": "creds" }))).starts_with("property 'creds_file'"));
    assert!(refused(base(json!({ "auth": "kerberos" }))).starts_with("property 'auth'"));

    let ignored = refused(base(json!({ "token": "t" })));
    assert!(
        ignored.contains("is 'none', which does not use token"),
        "{ignored}"
    );
    let wrong = refused(base(
        json!({ "auth": "token", "token": "t", "password": "p" }),
    ));
    assert!(wrong.contains("does not use password"), "{wrong}");

    let ca = refused(base(json!({ "ca_cert": "ca.pem" })));
    assert!(ca.starts_with("property 'ca_cert'"), "{ca}");
}

#[test]
fn a_connection_is_described_without_its_secrets() {
    let users = connection(json!({
        "url": "nats://a:4222", "auth": "user_password", "username": "etl",
        "password": "hunter2", "tls": true
    }))
    .unwrap()
    .describe();
    assert_eq!(users, "nats://a:4222 (tls, user_password as 'etl')");

    let token = connection(json!({ "url": "nats://a:4222", "auth": "token", "token": "s3cret" }))
        .unwrap()
        .describe();
    assert_eq!(token, "nats://a:4222 (plaintext, token)");
    assert!(!token.contains("s3cret"));
}

#[test]
fn a_sink_subject_must_be_one_exact_subject() {
    let refused = |subject: &str| {
        NatsSink
            .check(&json!({ "url": "nats://a:4222", "subject": subject }))
            .unwrap_err()
            .to_string()
    };
    assert!(refused("orders.>").contains("wildcard"));
    assert!(refused("orders.*.eu").contains("wildcard"));
    assert!(refused("").starts_with("property 'subject'"));
    NatsSink
        .check(&json!({ "url": "nats://a:4222", "subject": "orders.large" }))
        .expect("fine");
}

// ---------------------------------------------------------------------------
// Where a run starts, without a server
// ---------------------------------------------------------------------------

fn position(stream: &str, filter: &str, next: u64) -> Position {
    Position {
        stream: stream.to_string(),
        filter: filter.to_string(),
        next,
    }
}

#[test]
fn a_position_round_trips_and_one_this_connector_did_not_write_is_refused() {
    let written = position("ORDERS", "orders.eu.>", 42);
    assert_eq!(Position::from_json(&written.to_json()).unwrap(), written);

    for bad in [
        json!(1),
        json!({ "stream": "S", "next": 1 }),
        json!({ "stream": "S", "filter": "", "next": 0 }),
    ] {
        let error = Position::from_json(&bad).unwrap_err().to_string();
        assert!(error.contains("etl state forget"), "{bad}: {error}");
    }
}

#[test]
fn a_first_run_starts_where_start_says_and_an_empty_stream_is_fine() {
    let (from, notes) = plan_start("S", "", 5, 20, None, Start::Earliest).unwrap();
    assert_eq!((from, notes.len()), (5, 0));

    let (from, _) = plan_start("S", "", 5, 20, None, Start::Latest).unwrap();
    assert_eq!(from, 21);

    // A new, empty stream reports first 0 and last 0.
    let (from, _) = plan_start("S", "", 0, 0, None, Start::Earliest).unwrap();
    assert_eq!(from, 1);
}

#[test]
fn a_saved_position_is_carried_on_from_and_one_at_the_end_reads_nothing() {
    let saved = position("S", "", 11);
    assert_eq!(
        plan_start("S", "", 1, 20, Some(&saved), Start::Earliest)
            .unwrap()
            .0,
        11
    );
    let caught_up = position("S", "", 21);
    assert_eq!(
        plan_start("S", "", 1, 20, Some(&caught_up), Start::Earliest)
            .unwrap()
            .0,
        21
    );
}

#[test]
fn messages_discarded_before_they_were_read_are_an_error_that_counts_them() {
    let error = plan_start(
        "S",
        "",
        30,
        40,
        Some(&position("S", "", 25)),
        Start::Earliest,
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("sequences 25 to 29 were discarded"),
        "{error}"
    );
    assert!(error.contains("5 message(s) lost"), "{error}");
    assert!(error.contains("etl state forget"), "{error}");
}

#[test]
fn a_saved_position_past_the_end_says_the_stream_was_probably_remade() {
    let error = plan_start(
        "S",
        "",
        1,
        3,
        Some(&position("S", "", 500)),
        Start::Earliest,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("past the end of the stream (3)"), "{error}");
}

#[test]
fn a_position_for_another_stream_or_filter_is_set_aside_and_said_so() {
    let (from, notes) = plan_start(
        "S",
        "b.>",
        1,
        9,
        Some(&position("S", "a.>", 7)),
        Start::Earliest,
    )
    .unwrap();
    assert_eq!(from, 1);
    assert!(notes[0].contains("filtered to 'a.>'"), "{notes:?}");

    let (from, notes) = plan_start(
        "NEW",
        "",
        3,
        9,
        Some(&position("OLD", "", 7)),
        Start::Latest,
    )
    .unwrap();
    assert_eq!(from, 10);
    assert!(notes[0].contains("was for stream 'OLD'"), "{notes:?}");
}

// ---------------------------------------------------------------------------
// Rows, without a server
// ---------------------------------------------------------------------------

#[test]
fn a_message_becomes_its_fields_beside_the_underscore_columns() {
    let mut headers = HeaderMap::new();
    headers.insert("trace", "abc");
    headers.append("tag", "x");
    headers.append("tag", "y");

    let row = row(
        "ORDERS",
        "orders.eu",
        7,
        "2026-09-23 10:00:00.000",
        Some(&headers),
        br#"{"id": 1}"#,
        Format::Json,
    )
    .unwrap();

    assert_eq!(row["id"], 1);
    assert_eq!(row["_stream"], "ORDERS");
    assert_eq!(row["_subject"], "orders.eu");
    assert_eq!(row["_sequence"], 7);
    assert_eq!(
        row["_headers"],
        json!({ "tag": ["x", "y"], "trace": "abc" })
    );
}

#[test]
fn an_empty_payload_and_no_headers_are_empty_not_errors() {
    let row = row("S", "s", 1, "t", None, b"", Format::Json).unwrap();
    assert!(row["_headers"].is_null());
    assert!(!row.contains_key("value"));

    let text = super::row("S", "s", 1, "t", None, b"", Format::Text).unwrap();
    assert_eq!(text["value"], "");
}

#[test]
fn a_value_json_cannot_take_is_refused_naming_its_sequence() {
    let error = row("S", "s", 9, "t", None, b"nope", Format::Json)
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with("sequence 9: the value is not JSON"),
        "{error}"
    );

    let clash = row("S", "s", 9, "t", None, br#"{"_subject": 1}"#, Format::Json)
        .unwrap_err()
        .to_string();
    assert!(clash.contains("a field '_subject'"), "{clash}");
}

#[test]
fn a_message_id_is_its_columns_text_and_a_missing_column_is_named() {
    let row = json!({ "id": 7, "code": "A1", "none": null })
        .as_object()
        .unwrap()
        .clone();
    assert_eq!(msg_id(&row, "id").unwrap(), Some("7".to_string()));
    assert_eq!(msg_id(&row, "code").unwrap(), Some("A1".to_string()));
    assert_eq!(msg_id(&row, "none").unwrap(), None);
    assert!(msg_id(&row, "order")
        .unwrap_err()
        .to_string()
        .contains("'order' is not a column"));
}

#[test]
fn a_server_that_is_not_there_fails_within_the_timeout() {
    let started = std::time::Instant::now();
    let mut out: Vec<Record> = Vec::new();
    let error = NatsSource
        .read(
            &json!({ "url": "nats://127.0.0.1:1", "stream": "S", "timeout_ms": 1500 }),
            &mut out,
            &Context::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with("connecting to nats://127.0.0.1:1"),
        "{error}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
}

// ---------------------------------------------------------------------------
// Against real NATS servers
// ---------------------------------------------------------------------------

fn server(variable: &str) -> Option<String> {
    match std::env::var(variable) {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!("skipping: {variable} is not set; see scripts/test-services.ps1");
            None
        }
    }
}

fn block_on<F: Future>(work: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(work)
}

fn unique(test: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("ETL_{}_{}_{nanos}", test.to_uppercase(), std::process::id())
}

/// A stream of its own for one test, capturing `<name>.>`, on the open
/// server, with `config` changing whatever the test needs.
fn new_stream(url: &str, test: &str, config: impl FnOnce(&mut StreamConfig)) -> String {
    let name = unique(test);
    let mut stream_config = StreamConfig {
        name: name.clone(),
        subjects: vec![format!("{name}.>")],
        ..Default::default()
    };
    config(&mut stream_config);
    block_on(async {
        let client = async_nats::connect(url)
            .await
            .expect("the test server answers");
        async_nats::jetstream::new(client)
            .create_stream(stream_config)
            .await
            .expect("creates the stream");
    });
    name
}

/// Publish `ids` as JSON to `<stream>.<part>`, `part` taking turns between
/// `a` and `b`, and wait for each acknowledgement.
fn publish(url: &str, stream: &str, ids: std::ops::Range<u64>) {
    block_on(async {
        let client = async_nats::connect(url).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);
        for id in ids {
            let part = if id % 2 == 0 { "a" } else { "b" };
            jetstream
                .publish(
                    format!("{stream}.{part}"),
                    json!({ "id": id }).to_string().into(),
                )
                .await
                .unwrap()
                .await
                .unwrap();
        }
    });
}

fn run_once(properties: &JsonValue, saved: Option<JsonValue>) -> (Vec<u64>, JsonValue, Summary) {
    let mut out: Vec<Record> = Vec::new();
    let context = Context {
        checkpoint: saved,
        ..Context::default()
    };
    let summary = NatsSource
        .read(properties, &mut out, &context)
        .expect("reads");
    let ids: Vec<u64> = out.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    let checkpoint = summary
        .checkpoint
        .clone()
        .expect("a source always returns one");
    (ids, checkpoint, summary)
}

#[test]
fn batches_carry_on_exactly_where_the_last_one_stopped() {
    let Some(url) = server("ETL_TEST_NATS") else {
        return;
    };
    let stream = new_stream(&url, "batches", |_| {});
    publish(&url, &stream, 0..25);
    let properties = json!({ "url": url, "stream": stream, "max_records": 10 });

    let mut seen = Vec::new();
    let mut sizes = Vec::new();
    let mut saved = None;
    for _ in 0..4 {
        let (ids, checkpoint, _) = run_once(&properties, saved.take());
        sizes.push(ids.len());
        seen.extend(ids);
        saved = Some(checkpoint);
    }
    assert_eq!(sizes, [10, 10, 5, 0]);
    assert_eq!(seen, (0..25).collect::<Vec<u64>>(), "in order, each once");
}

#[test]
fn a_run_that_is_not_saved_is_read_again_and_latest_reads_only_what_arrives() {
    let Some(url) = server("ETL_TEST_NATS") else {
        return;
    };
    let stream = new_stream(&url, "again", |_| {});
    publish(&url, &stream, 0..4);

    let properties = json!({ "url": url, "stream": stream });
    let (first, _, _) = run_once(&properties, None);
    let (again, _, _) = run_once(&properties, None);
    assert_eq!(first, again);

    let latest = json!({ "url": url, "stream": stream, "start": "latest" });
    let (nothing, saved, _) = run_once(&latest, None);
    assert!(nothing.is_empty());
    publish(&url, &stream, 4..6);
    let (arrived, _, _) = run_once(&latest, Some(saved));
    assert_eq!(arrived, [4, 5]);
}

#[test]
fn a_filter_reads_only_its_subjects_and_moves_past_the_rest() {
    let Some(url) = server("ETL_TEST_NATS") else {
        return;
    };
    let stream = new_stream(&url, "filter", |_| {});
    publish(&url, &stream, 0..9); // a: 0,2,4,6,8  b: 1,3,5,7

    let properties =
        json!({ "url": url, "stream": stream, "filter_subject": format!("{stream}.a") });
    let (ids, saved, summary) = run_once(&properties, None);
    assert_eq!(ids, [0, 2, 4, 6, 8]);
    assert!(summary.detail.contains("matching"), "{}", summary.detail);
    assert_eq!(
        saved["next"], 10,
        "past the end of the stream, not just past the last match"
    );

    publish(&url, &stream, 9..11); // b: 9, a: 10
    let (ids, _, _) = run_once(&properties, Some(saved));
    assert_eq!(ids, [10]);
}

#[test]
fn messages_the_streams_limits_discarded_unread_fail_the_next_read() {
    let Some(url) = server("ETL_TEST_NATS") else {
        return;
    };
    let stream = new_stream(&url, "limits", |config| config.max_messages = 3);
    publish(&url, &stream, 0..2);
    let properties = json!({ "url": url, "stream": stream });
    let (_, saved, _) = run_once(&properties, None); // next = 3

    publish(&url, &stream, 2..8); // sequences 3..=8; the stream keeps 6..=8
    let mut out: Vec<Record> = Vec::new();
    let error = NatsSource
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
    assert!(error.contains("sequences 3 to 5 were discarded"), "{error}");
    assert!(out.is_empty());
}

#[test]
fn messages_deleted_from_the_middle_are_simply_not_there() {
    let Some(url) = server("ETL_TEST_NATS") else {
        return;
    };
    let stream = new_stream(&url, "interior", |_| {});
    publish(&url, &stream, 0..5); // sequences 1..=5
    block_on(async {
        let client = async_nats::connect(url.as_str()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);
        jetstream
            .get_stream(&stream)
            .await
            .unwrap()
            .delete_message(3)
            .await
            .unwrap();
    });

    let (ids, saved, _) = run_once(&json!({ "url": url, "stream": stream }), None);
    assert_eq!(ids, [0, 1, 3, 4]);
    assert_eq!(saved["next"], 6);
}

/// What a stream holds, read back through the source.
fn read_back(url: &str, stream: &str) -> Vec<Record> {
    let mut out: Vec<Record> = Vec::new();
    NatsSource
        .read(
            &json!({ "url": url, "stream": stream }),
            &mut out,
            &Context::default(),
        )
        .expect("reads back");
    out
}

fn write_rows(properties: &JsonValue, rows: Vec<JsonValue>) -> Result<Summary, ConnectorError> {
    let mut reader = crate::fixture::records(rows);
    NatsSink.write(properties, &mut reader, &Context::default())
}

#[test]
fn rows_published_are_read_back_and_a_message_id_stops_a_second_copy() {
    let Some(url) = server("ETL_TEST_NATS") else {
        return;
    };
    let stream = new_stream(&url, "sink", |_| {});
    let properties = json!({
        "url": url, "subject": format!("{stream}.out"), "batch_size": 4, "msg_id_column": "id"
    });
    let rows: Vec<JsonValue> = (1..=10).map(|id| json!({ "id": id, "v": "x" })).collect();

    let first = write_rows(&properties, rows.clone()).expect("publishes");
    assert_eq!(first.records, 10);
    assert!(first.detail.contains("in 3 batch(es)"), "{}", first.detail);

    // The same rows again, as a re-run would send them: JetStream drops them.
    let again = write_rows(&properties, rows).expect("publishes");
    assert!(
        again.detail.contains("dropped 10 as duplicates"),
        "{}",
        again.detail
    );

    let read = read_back(&url, &stream);
    assert_eq!(read.len(), 10, "each row once");
    assert_eq!(read[0]["v"], "x");
    assert_eq!(read[0]["_subject"], format!("{stream}.out"));
}

#[test]
fn publishing_where_no_stream_listens_fails_saying_what_was_delivered() {
    let Some(url) = server("ETL_TEST_NATS") else {
        return;
    };
    let error = write_rows(
        &json!({ "url": url, "subject": "etl.nobody.listens", "timeout_ms": 3000 }),
        vec![json!({ "id": 1 })],
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.starts_with("batch 1 failed after 0 batch(es) (0 message(s)) were delivered"),
        "{error}"
    );
}

/// A stream on a signing-in server, one message in it, read back through
/// `properties`: the whole round trip a user's pipeline would make.
fn signs_in(variable: &str, sign_in: JsonValue) {
    let Some(url) = server(variable) else { return };
    let mut properties = json!({ "url": url, "timeout_ms": 10000 });
    for (key, value) in sign_in.as_object().unwrap() {
        properties[key] = value.clone();
    }

    let stream = unique(variable);
    block_on(async {
        let client = settings_connect(&properties).await;
        async_nats::jetstream::new(client)
            .create_stream(StreamConfig {
                name: stream.clone(),
                subjects: vec![format!("{stream}.>")],
                ..Default::default()
            })
            .await
            .expect("creates the stream");
    });

    let mut sink = properties.clone();
    sink["subject"] = json!(format!("{stream}.in"));
    write_rows(&sink, vec![json!({ "id": 1 })]).unwrap_or_else(|e| panic!("{variable}: {e}"));

    let mut source = properties;
    source["stream"] = json!(stream);
    let mut out: Vec<Record> = Vec::new();
    NatsSource
        .read(&source, &mut out, &Context::default())
        .unwrap_or_else(|e| panic!("{variable}: {e}"));
    assert_eq!(out.len(), 1, "{variable}");
}

/// A client signed in the way `properties` says, through the connector's own
/// code, for setting up the test.
async fn settings_connect(properties: &JsonValue) -> async_nats::Client {
    Connection::from(properties)
        .unwrap()
        .connect(&Context::default())
        .await
        .expect("signs in")
}

#[test]
fn a_user_and_password_signs_in() {
    signs_in(
        "ETL_TEST_NATS_USERS",
        json!({ "auth": "user_password", "username": "etl", "password": "etl-secret" }),
    );
}

#[test]
fn a_token_signs_in() {
    signs_in(
        "ETL_TEST_NATS_TOKEN",
        json!({ "auth": "token", "token": "etl-token" }),
    );
}

#[test]
fn a_creds_file_signs_in() {
    let Ok(file) = std::env::var("ETL_TEST_NATS_CREDS_FILE") else {
        eprintln!("skipping: ETL_TEST_NATS_CREDS_FILE is not set");
        return;
    };
    signs_in(
        "ETL_TEST_NATS_CREDS",
        json!({ "auth": "creds", "creds_file": file }),
    );
}

#[test]
fn tls_with_the_servers_ca_signs_in_and_without_it_is_refused() {
    let Ok(ca) = std::env::var("ETL_TEST_KAFKA_CA") else {
        eprintln!("skipping: ETL_TEST_KAFKA_CA is not set");
        return;
    };
    signs_in("ETL_TEST_NATS_TLS", json!({ "tls": true, "ca_cert": ca }));

    let Some(url) = server("ETL_TEST_NATS_TLS") else {
        return;
    };
    let mut out: Vec<Record> = Vec::new();
    let error = NatsSource
        .read(
            &json!({ "url": url, "stream": "S", "tls": true, "timeout_ms": 3000 }),
            &mut out,
            &Context::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with(&format!("connecting to {url} (tls)")),
        "{error}"
    );
    assert!(
        error.to_lowercase().contains("certificate"),
        "says why, not only that: {error}"
    );
}

#[test]
fn a_wrong_password_fails_naming_the_method_and_not_the_password() {
    let Some(url) = server("ETL_TEST_NATS_USERS") else {
        return;
    };
    let mut out: Vec<Record> = Vec::new();
    let error = NatsSource
        .read(
            &json!({ "url": url, "stream": "S", "auth": "user_password", "username": "etl",
                     "password": "not-the-password", "timeout_ms": 3000 }),
            &mut out,
            &Context::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("user_password as 'etl'"), "{error}");
    assert!(error.to_lowercase().contains("authorization"), "{error}");
    assert!(!error.contains("not-the-password"), "{error}");
}
