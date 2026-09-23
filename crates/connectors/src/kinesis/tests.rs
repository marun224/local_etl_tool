//! The rules that decide whether a record is read once, twice or never, first
//! without a server, then against `kinesis-mock` when `ETL_TEST_KINESIS` names
//! it (`scripts/test-services.ps1` starts it). Without it the server tests
//! skip, and say so.

use super::*;
use crate::http::base64;

// ---------------------------------------------------------------------------
// Without a server
// ---------------------------------------------------------------------------

#[test]
fn the_host_is_what_the_client_sends_default_ports_left_out() {
    assert_eq!(
        host_of("https://kinesis.eu-west-1.amazonaws.com").as_deref(),
        Some("kinesis.eu-west-1.amazonaws.com")
    );
    assert_eq!(host_of("https://h:443").as_deref(), Some("h"));
    assert_eq!(
        host_of("http://127.0.0.1:54568").as_deref(),
        Some("127.0.0.1:54568")
    );
    assert_eq!(host_of("ftp://h"), None);
    assert_eq!(host_of("https://"), None);
}

#[test]
fn a_position_round_trips_and_one_this_connector_did_not_write_is_refused() {
    let written = Position {
        stream: "orders".into(),
        shards: [
            ("shardId-0".to_string(), ShardPosition::After("4960".into())),
            ("shardId-1".to_string(), ShardPosition::Done),
            (
                "shardId-2".to_string(),
                ShardPosition::Since(1_790_157_907_000),
            ),
        ]
        .into_iter()
        .collect(),
    };
    assert_eq!(Position::from_json(&written.to_json()).unwrap(), written);

    for bad in [
        json!({ "stream": "s" }),
        json!({ "stream": "s", "shards": { "a": { "after": "12x" } } }),
        json!({ "stream": "s", "shards": { "a": {} } }),
    ] {
        let error = Position::from_json(&bad).unwrap_err().to_string();
        assert!(error.contains("etl state forget"), "{bad}: {error}");
    }
}

fn shard(id: &str, parents: &[&str]) -> Shard {
    Shard {
        id: id.to_string(),
        parents: parents.iter().map(|p| p.to_string()).collect(),
    }
}

#[test]
fn a_child_waits_for_its_parents_and_an_aged_out_parent_counts_as_finished() {
    // 0 split into 1 and 2; 1 and 2 merged into 3.
    let shards = [
        shard("0", &[]),
        shard("1", &["0"]),
        shard("2", &["0"]),
        shard("3", &["1", "2"]),
    ];
    let mut positions = BTreeMap::new();
    assert_eq!(readable(&shards, &positions), ["0"]);

    positions.insert("0".to_string(), ShardPosition::Done);
    assert_eq!(readable(&shards, &positions), ["1", "2"]);

    positions.insert("1".to_string(), ShardPosition::Done);
    assert_eq!(
        readable(&shards, &positions),
        ["2"],
        "3 waits for both parents"
    );

    positions.insert("2".to_string(), ShardPosition::Done);
    assert_eq!(readable(&shards, &positions), ["3"]);

    // Parent 0 has aged out of the stream and is no longer listed.
    let later = [shard("1", &["0"]), shard("2", &["0"])];
    assert_eq!(readable(&later, &BTreeMap::new()), ["1", "2"]);
}

#[test]
fn a_record_becomes_its_fields_beside_the_underscore_columns() {
    let record = json!({
        "SequenceNumber": "49590338271490256608559692538361571095921575989136588898",
        "Data": base64(r#"{"id": 7}"#),
        "PartitionKey": "C001",
        "ApproximateArrivalTimestamp": 1_790_157_907.089,
    });
    let row = row("orders", "shardId-000000000000", &record, Format::Json).unwrap();

    assert_eq!(row["id"], 7);
    assert_eq!(row["_shard"], "shardId-000000000000");
    assert_eq!(
        row["_sequence"], "49590338271490256608559692538361571095921575989136588898",
        "text: it does not fit in 64 bits"
    );
    assert_eq!(row["_timestamp"], "2026-09-23 10:05:07.089");
    assert_eq!(row["_partition_key"], "C001");

    let bad = json!({ "SequenceNumber": "1", "Data": "not base64!" });
    let error = super::row("s", "shard", &bad, Format::Json)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("sequence 1: the record's data is not base64"),
        "{error}"
    );
}

#[test]
fn base64_decodes_what_the_encoder_writes() {
    for text in [
        "",
        "f",
        "fo",
        "foo",
        "foob",
        "fooba",
        "foobar",
        r#"{"id": 1}"#,
    ] {
        assert_eq!(
            base64_decode(&base64(text)).unwrap(),
            text.as_bytes(),
            "{text}"
        );
    }
    assert!(base64_decode("abc").is_none(), "not a multiple of four");
    assert!(base64_decode("ab=c").is_none(), "padding in the middle");
}

#[test]
fn a_setting_that_cannot_work_is_refused_by_property() {
    let refused = |properties: JsonValue| KinesisSource.check(&properties).unwrap_err().to_string();
    assert!(refused(json!({})).starts_with("property 'stream'"));
    assert!(refused(json!({ "stream": "s", "on_expired": "ignore" }))
        .starts_with("property 'on_expired'"));
    assert!(refused(json!({ "stream": "s", "access_key_id": "K" }))
        .starts_with("property 'access_key_id'"));
    assert!(
        refused(json!({ "stream": "s", "endpoint": "kinesis.local" }))
            .starts_with("property 'endpoint'")
    );
    KinesisSource
        .check(&json!({ "stream": "s", "endpoint": "http://127.0.0.1:4568" }))
        .expect("fine");
}

/// An `Api` aimed at a local fixture, with temporary credentials.
fn fixture_api(fixture: &crate::fixture::Fixture) -> Api {
    let endpoint = fixture.url("");
    Api::connect(
        &json!({
            "endpoint": endpoint, "region": "eu-west-1", "retries": 3,
            "access_key_id": "AKIDTEST", "secret_access_key": "test-secret",
            "session_token": "test-token",
        }),
        &Sources::process(),
    )
    .unwrap()
}

#[test]
fn every_attempt_is_signed_afresh_over_exactly_what_arrives() {
    // Neither kinesis-mock nor LocalStack checks signatures, so this does:
    // each attempt's signature is recomputed from the host, headers and body
    // the server actually received, and must match.
    let fixture = crate::fixture::serve(|index, _| match index {
        0 => crate::fixture::status(
            400,
            r#"{"__type":"ProvisionedThroughputExceededException","message":"slow down"}"#,
        ),
        _ => crate::fixture::ok(json!({ "Shards": [] })),
    });
    let mut api = fixture_api(&fixture);
    api.call("ListShards", &json!({ "StreamName": "s" }))
        .expect("retried to success");

    let seen = fixture.seen();
    assert_eq!(
        seen.len(),
        2,
        "the throttled attempt, then the one that passed"
    );
    let credentials = Credentials {
        access_key_id: "AKIDTEST".into(),
        secret_access_key: "test-secret".into(),
        session_token: Some("test-token".into()),
        source: "a test".into(),
    };
    for request in &seen {
        let header = |name: &str| request.header(name).unwrap_or_else(|| panic!("{name}"));
        assert_eq!(header("X-Amz-Target"), "Kinesis_20131202.ListShards");
        assert_eq!(header("X-Amz-Security-Token"), "test-token");
        let unsigned = [
            ("Host".to_string(), header("Host").to_string()),
            (
                "Content-Type".to_string(),
                header("Content-Type").to_string(),
            ),
            (
                "X-Amz-Target".to_string(),
                header("X-Amz-Target").to_string(),
            ),
        ];
        let expected = aws::sign(
            &aws::Unsigned {
                method: &request.method,
                target: &request.url,
                headers: &unsigned,
                body: request.body.as_bytes(),
            },
            &aws::Signer {
                credentials: &credentials,
                region: "eu-west-1",
                service: "kinesis",
                amz_date: header("X-Amz-Date"),
                normalize: true,
                sign_body: false,
                omit_session_token: false,
            },
        );
        let authorization = &expected.headers.last().unwrap().1;
        assert_eq!(header("Authorization"), authorization);
    }
}

#[test]
fn a_throttled_400_is_retried_and_any_other_400_is_not() {
    let attempts = |body: &'static str| {
        let fixture = crate::fixture::serve(move |_, _| crate::fixture::status(400, body));
        let error = fixture_api(&fixture)
            .call("GetRecords", &json!({}))
            .unwrap_err()
            .to_string();
        (fixture.seen().len(), error)
    };

    for throttled in [
        r#"{"__type":"ProvisionedThroughputExceededException","message":"Rate exceeded for shard"}"#,
        r#"{"__type":"ThrottlingException","message":"Rate exceeded"}"#,
        r#"{"__type":"LimitExceededException","message":"Rate exceeded for stream s"}"#,
    ] {
        let (seen, error) = attempts(throttled);
        assert_eq!(seen, 4, "the first attempt and 3 retries: {throttled}");
        assert!(error.starts_with("Kinesis GetRecords"), "{error}");
    }

    for final_answer in [
        r#"{"__type":"InvalidArgumentException","message":"Bad iterator"}"#,
        // The same exception for an account's shard limit: waiting will not help.
        r#"{"__type":"LimitExceededException","message":"This request would exceed the shard limit for the account"}"#,
        r#"{"__type":"ResourceNotFoundException","message":"Stream s not found"}"#,
    ] {
        let (seen, error) = attempts(final_answer);
        assert_eq!(seen, 1, "not retried: {final_answer}");
        assert!(error.contains("Exception"), "Kinesis's own words: {error}");
    }
}

// ---------------------------------------------------------------------------
// Against kinesis-mock
// ---------------------------------------------------------------------------

fn server() -> Option<String> {
    match std::env::var("ETL_TEST_KINESIS") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!("skipping: ETL_TEST_KINESIS is not set; see scripts/test-services.ps1");
            None
        }
    }
}

/// Properties that reach the test server with made-up credentials, which it
/// accepts: it does not check signatures.
fn properties(endpoint: &str, stream: &str) -> JsonValue {
    json!({
        "stream": stream, "endpoint": endpoint, "region": "us-east-1",
        "access_key_id": "AKIDTEST", "secret_access_key": "test-secret",
    })
}

fn api(endpoint: &str) -> Api {
    Api::connect(&properties(endpoint, "unused"), &Sources::process()).unwrap()
}

fn unique(test: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("etl-{test}-{}-{nanos}", std::process::id())
}

/// Wait until the stream is ACTIVE again after a create, split or merge.
fn settle(api: &mut Api, stream: &str) {
    for _ in 0..100 {
        let summary = api
            .call("DescribeStreamSummary", &json!({ "StreamName": stream }))
            .unwrap();
        if summary["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE" {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("stream {stream} never became ACTIVE");
}

/// A stream for one test, deleted when the test ends, pass or fail: the test
/// server keeps AWS's account-wide shard limit, and runs add up.
struct TestStream {
    endpoint: String,
    name: String,
}

impl std::ops::Deref for TestStream {
    type Target = String;
    fn deref(&self) -> &String {
        &self.name
    }
}

impl Drop for TestStream {
    fn drop(&mut self) {
        let _ = api(&self.endpoint).call(
            "DeleteStream",
            &json!({ "StreamName": self.name, "EnforceConsumerDeletion": true }),
        );
    }
}

fn create(endpoint: &str, test: &str, shards: u32) -> TestStream {
    let stream = unique(test);
    let mut api = api(endpoint);
    api.call(
        "CreateStream",
        &json!({ "StreamName": stream, "ShardCount": shards }),
    )
    .unwrap();
    settle(&mut api, &stream);
    TestStream {
        endpoint: endpoint.to_string(),
        name: stream,
    }
}

/// Put `ids` as JSON, each under partition key `key(id)`.
fn put(endpoint: &str, stream: &str, ids: std::ops::Range<u64>, key: impl Fn(u64) -> String) {
    let mut api = api(endpoint);
    for id in ids {
        api.call(
            "PutRecord",
            &json!({
                "StreamName": stream,
                "Data": base64(&json!({ "id": id }).to_string()),
                "PartitionKey": key(id),
            }),
        )
        .unwrap();
    }
}

fn run_once(properties: &JsonValue, saved: Option<JsonValue>) -> (Vec<u64>, JsonValue, Summary) {
    let mut out: Vec<Record> = Vec::new();
    let context = Context {
        checkpoint: saved,
        ..Context::default()
    };
    let summary = KinesisSource
        .read(properties, &mut out, &context)
        .expect("reads");
    let ids = out.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    (
        ids,
        summary.checkpoint.clone().expect("a checkpoint"),
        summary,
    )
}

#[test]
fn batches_carry_on_exactly_where_the_last_one_stopped() {
    let Some(endpoint) = server() else { return };
    let stream = create(&endpoint, "batches", 2);
    put(&endpoint, &stream, 0..25, |id| format!("k{id}"));

    let mut properties = properties(&endpoint, &stream);
    properties["max_records"] = json!(10);

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
    seen.sort_unstable();
    assert_eq!(seen, (0..25).collect::<Vec<u64>>(), "every record once");
}

#[test]
fn a_run_that_is_not_saved_is_read_again_and_latest_reads_only_what_arrives() {
    let Some(endpoint) = server() else { return };
    let stream = create(&endpoint, "latest", 1);
    put(&endpoint, &stream, 0..3, |_| "k".into());

    let properties_ = properties(&endpoint, &stream);
    let (first, _, _) = run_once(&properties_, None);
    let (again, _, _) = run_once(&properties_, None);
    assert_eq!(first, again);

    let mut latest = properties_.clone();
    latest["start"] = json!("latest");
    let (nothing, saved, _) = run_once(&latest, None);
    assert!(nothing.is_empty());
    assert!(
        saved["shards"]["shardId-000000000000"]["since"].is_i64(),
        "{saved}"
    );

    std::thread::sleep(Duration::from_millis(1100));
    put(&endpoint, &stream, 3..5, |_| "k".into());
    let (arrived, _, _) = run_once(&latest, Some(saved));
    assert_eq!(arrived, [3, 4]);
}

/// The hash-key midpoint of a shard, for splitting it.
fn midpoint(api: &mut Api, stream: &str, shard: &str) -> String {
    let answer = api
        .call("ListShards", &json!({ "StreamName": stream }))
        .unwrap();
    let range = answer["Shards"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["ShardId"] == shard)
        .unwrap()["HashKeyRange"]
        .clone();
    let start: u128 = range["StartingHashKey"].as_str().unwrap().parse().unwrap();
    let end: u128 = range["EndingHashKey"].as_str().unwrap().parse().unwrap();
    (start + (end - start) / 2).to_string()
}

#[test]
fn a_split_reads_the_parent_to_its_end_before_its_children_keeping_a_keys_order() {
    let Some(endpoint) = server() else { return };
    let stream = create(&endpoint, "split", 1);
    put(&endpoint, &stream, 0..4, |_| "one-key".into());

    let mut api = api(&endpoint);
    let parent = "shardId-000000000000";
    let middle = midpoint(&mut api, &stream, parent);
    api.call(
        "SplitShard",
        &json!({ "StreamName": stream.as_str(), "ShardToSplit": parent, "NewStartingHashKey": middle }),
    )
    .unwrap();
    settle(&mut api, &stream);
    put(&endpoint, &stream, 4..8, |_| "one-key".into());

    let (ids, saved, _) = run_once(&properties(&endpoint, &stream), None);
    assert_eq!(
        ids,
        (0..8).collect::<Vec<u64>>(),
        "the key's order survives the split"
    );
    assert_eq!(saved["shards"][parent]["done"], true, "{saved}");

    // Nothing is read twice on the next run.
    let (again, _, _) = run_once(&properties(&endpoint, &stream), Some(saved));
    assert!(again.is_empty(), "{again:?}");
}

#[test]
fn a_merge_reads_both_parents_before_the_child() {
    let Some(endpoint) = server() else { return };
    let stream = create(&endpoint, "merge", 2);
    put(&endpoint, &stream, 0..6, |id| format!("key-{id}"));

    let mut api = api(&endpoint);
    api.call(
        "MergeShards",
        &json!({
            "StreamName": stream.as_str(),
            "ShardToMerge": "shardId-000000000000",
            "AdjacentShardToMerge": "shardId-000000000001",
        }),
    )
    .unwrap();
    settle(&mut api, &stream);
    put(&endpoint, &stream, 6..9, |id| format!("key-{id}"));

    let (ids, saved, _) = run_once(&properties(&endpoint, &stream), None);
    let (before, after) = ids.split_at(6);
    let mut before = before.to_vec();
    before.sort_unstable();
    assert_eq!(
        before,
        (0..6).collect::<Vec<u64>>(),
        "the parents first: {ids:?}"
    );
    assert_eq!(after, [6, 7, 8], "then the child");
    assert_eq!(saved["shards"]["shardId-000000000000"]["done"], true);
    assert_eq!(saved["shards"]["shardId-000000000001"]["done"], true);
}

#[test]
fn a_last_record_no_longer_held_fails_by_default_and_continues_when_asked() {
    let Some(endpoint) = server() else { return };
    let stream = create(&endpoint, "expired", 1);
    put(&endpoint, &stream, 0..3, |_| "k".into());

    // What an expired position looks like: a sequence the shard no longer has.
    let gone = json!({
        "stream": stream.as_str(),
        "shards": { "shardId-000000000000": { "after": "1" } }
    });
    let properties_ = properties(&endpoint, &stream);
    let mut out: Vec<Record> = Vec::new();
    let error = KinesisSource
        .read(
            &properties_,
            &mut out,
            &Context {
                checkpoint: Some(gone.clone()),
                ..Context::default()
            },
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("may have expired"), "{error}");
    assert!(error.contains("on_expired"), "{error}");
    assert!(out.is_empty());

    let mut carry_on = properties_;
    carry_on["on_expired"] = json!("continue");
    let (ids, _, summary) = run_once(&carry_on, Some(gone));
    assert_eq!(ids, [0, 1, 2], "from the oldest record held");
    assert!(
        summary.detail.contains("some records may have been lost"),
        "{}",
        summary.detail
    );
}

#[test]
fn a_stream_that_does_not_exist_is_named() {
    let Some(endpoint) = server() else { return };
    let mut out: Vec<Record> = Vec::new();
    let error = KinesisSource
        .read(
            &properties(&endpoint, "etl-no-such-stream"),
            &mut out,
            &Context::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("ResourceNotFoundException"), "{error}");
    assert!(error.contains("etl-no-such-stream"), "{error}");
}

/// Put `ids` pinned to one end of the hash-key range: `low` lands on the first
/// shard of a two-shard stream, and the other end on the second.
fn put_pinned(endpoint: &str, stream: &str, ids: std::ops::Range<u64>, low: bool) {
    let mut api = api(endpoint);
    let hash = if low {
        "0".to_string()
    } else {
        u128::MAX.to_string()
    };
    for id in ids {
        api.call(
            "PutRecord",
            &json!({
                "StreamName": stream,
                "Data": base64(&json!({ "id": id }).to_string()),
                "PartitionKey": "pinned",
                "ExplicitHashKey": hash,
            }),
        )
        .unwrap();
    }
}

#[test]
fn a_shard_max_records_never_reached_is_read_from_its_start_next_time() {
    // The first draft saved "from now on" for a shard this run read nothing
    // from, which skipped everything already in it. The first shard here fills
    // max_records before the second has its turn.
    let Some(endpoint) = server() else { return };
    let stream = create(&endpoint, "unreached", 2);
    put_pinned(&endpoint, &stream, 0..12, true);
    put_pinned(&endpoint, &stream, 100..103, false);

    let mut properties = properties(&endpoint, &stream);
    properties["max_records"] = json!(10);
    let (first, saved, _) = run_once(&properties, None);
    assert_eq!(first, (0..10).collect::<Vec<u64>>());
    assert_eq!(
        saved["shards"]["shardId-000000000001"]["start"], true,
        "{saved}"
    );

    properties["max_records"] = json!(100);
    let (second, _, _) = run_once(&properties, Some(saved));
    let mut second = second;
    second.sort_unstable();
    assert_eq!(second, [10, 11, 100, 101, 102], "nothing skipped");
}
