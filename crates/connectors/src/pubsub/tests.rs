//! What decides whether a message is read once, again, or lost: the receipt
//! settling on every path, each pull's messages extended to our deadline, the
//! lease keeper, refusals. First against the local fixture, which answers as
//! told; then against Google's Pub/Sub emulator when `ETL_TEST_PUBSUB` names it
//! (`scripts/test-services.ps1` starts it). Without it those skip.

use super::*;
use crate::fixture::{self, Fixture, Seen};
use crate::gcp::tests::{service_account, tokens_from, Scratch};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Without a server
// ---------------------------------------------------------------------------

#[test]
fn a_setting_that_cannot_work_is_refused_by_property() {
    let source = |properties: JsonValue| PubsubSource.check(&properties).map_err(|e| e.to_string());
    let sink = |properties: JsonValue| PubsubSink.check(&properties).map_err(|e| e.to_string());
    let refused = |result: Result<(), String>, property: &str| {
        let error = result.unwrap_err();
        assert!(
            error.starts_with(&format!("property '{property}'")),
            "{property}: {error}"
        );
    };

    refused(source(json!({ "project": "p" })), "subscription");
    refused(source(json!({ "subscription": "orders" })), "project");
    refused(
        source(json!({ "subscription": "projects/p/topics/orders" })),
        "subscription",
    );
    refused(
        source(json!({ "subscription": "a/b", "project": "p" })),
        "subscription",
    );
    refused(
        source(json!({ "subscription": "projects/p/subscriptions/s", "project": "q" })),
        "project",
    );
    refused(
        source(json!({ "subscription": "s", "project": "p", "ack_deadline_seconds": 601 })),
        "ack_deadline_seconds",
    );
    refused(
        source(json!({ "subscription": "s", "project": "p", "value_format": "xml" })),
        "value_format",
    );
    refused(
        source(json!({ "subscription": "s", "project": "p", "endpoint": "localhost:8085" })),
        "endpoint",
    );
    source(json!({ "subscription": "projects/p/subscriptions/s" })).expect("fine");
    source(json!({ "subscription": "s", "project": "p", "endpoint": "http://127.0.0.1:1" }))
        .expect("fine");

    refused(sink(json!({ "project": "p" })), "topic");
    refused(sink(json!({ "topic": "t" })), "project");
    sink(json!({ "topic": "projects/p/topics/t", "ordering_key_column": "k" })).expect("fine");
}

#[test]
fn a_message_becomes_its_fields_beside_the_underscore_columns() {
    let received = json!({
        "ackId": "a-1",
        "deliveryAttempt": 2,
        "message": {
            "data": base64_bytes(br#"{"id": 7, "customer": "C1"}"#),
            "messageId": "m-1",
            "publishTime": "2026-09-24T10:05:07.089123456Z",
            "orderingKey": "C1",
            "attributes": { "source": "web" },
        },
    });
    let row = row("orders-sub", &received, Format::Json).unwrap();
    assert_eq!(row["id"], 7);
    assert_eq!(row["_subscription"], "orders-sub");
    assert_eq!(row["_message_id"], "m-1");
    assert_eq!(row["_publish_time"], "2026-09-24 10:05:07.089123");
    assert_eq!(row["_ordering_key"], "C1");
    assert_eq!(row["_attributes"], json!({ "source": "web" }));
    assert_eq!(row["_delivery_attempt"], 2);

    // No key, no attributes, no dead-letter policy: nulls and an empty object.
    let plain = json!({ "ackId": "a", "message": {
        "data": base64_bytes(b"plain"), "messageId": "m", "orderingKey": "",
        "publishTime": "2026-09-24T10:05:07Z",
    }});
    let text = super::row("s", &plain, Format::Text).unwrap();
    assert_eq!(text["value"], "plain");
    assert_eq!(text["_publish_time"], "2026-09-24 10:05:07");
    assert_eq!(text["_ordering_key"], JsonValue::Null);
    assert_eq!(text["_delivery_attempt"], JsonValue::Null);
    assert_eq!(text["_attributes"], json!({}));

    // A message of attributes alone has no data: a row of the underscore
    // columns, as a Kafka tombstone is.
    let empty =
        json!({ "ackId": "a", "message": { "messageId": "m", "attributes": { "k": "v" } } });
    let row = super::row("s", &empty, Format::Json).unwrap();
    assert_eq!(row["_attributes"], json!({ "k": "v" }));
    assert!(!row.contains_key("value"));
}

#[test]
fn acknowledgement_ids_go_a_thousand_to_a_call_and_under_400_kb() {
    let ids: Vec<String> = (0..2500).map(|n| format!("id-{n}")).collect();
    let sizes: Vec<usize> = id_chunks(&ids).iter().map(|chunk| chunk.len()).collect();
    assert_eq!(sizes, [1000, 1000, 500]);

    let long: Vec<String> = (0..10)
        .map(|n| format!("{n}{}", "x".repeat(99_999)))
        .collect();
    let sizes: Vec<usize> = id_chunks(&long).iter().map(|chunk| chunk.len()).collect();
    assert_eq!(sizes, [3, 3, 3, 1]);
    assert!(id_chunks(&[]).is_empty());
}

// ----- the source, against the fixture -----

/// The call a request made, from its path: `pull`, `acknowledge`, ...
fn verb(request: &Seen) -> String {
    request
        .path()
        .rsplit(':')
        .next()
        .unwrap_or_default()
        .to_string()
}

fn body(request: &Seen) -> JsonValue {
    serde_json::from_str(&request.body).unwrap_or(JsonValue::Null)
}

fn fixture_api(fixture: &Fixture) -> Api {
    Api::connect(
        &json!({ "endpoint": fixture.url(""), "retries": 1 }),
        &Sources {
            var: &|_| None,
            home: None,
        },
    )
    .unwrap()
}

/// A subscription of `count` messages that each pull drains, at most
/// `per_pull` at a time. `refuse` names a call to answer with a 400.
fn subscription_of(count: usize, per_pull: usize, refuse: Option<&'static str>) -> Fixture {
    let taken = Arc::new(Mutex::new(0usize));
    fixture::serve(move |_, request| {
        let verb = verb(request);
        if Some(verb.as_str()) == refuse {
            return fixture::status(400, r#"{"error":{"code":400,"message":"no"}}"#);
        }
        match verb.as_str() {
            "pull" => {
                let wanted = body(request)["maxMessages"].as_u64().unwrap() as usize;
                let mut taken = taken.lock().unwrap();
                let end = count.min(*taken + wanted.min(per_pull));
                let messages: Vec<JsonValue> = (*taken..end)
                    .map(|n| {
                        json!({ "ackId": format!("a{n}"), "message": {
                            "data": base64_bytes(format!(r#"{{"n": {n}}}"#).as_bytes()),
                            "messageId": format!("m{n}"),
                            "publishTime": "2026-09-24T10:00:00Z",
                        }})
                    })
                    .collect();
                *taken = end;
                fixture::ok(json!({ "receivedMessages": messages }))
            }
            "acknowledge" | "modifyAckDeadline" => fixture::ok(json!({})),
            other => fixture::status(404, &format!(r#"{{"error":"{other} not expected"}}"#)),
        }
    })
}

fn settings(properties: JsonValue) -> SourceSettings {
    let mut all = json!({ "subscription": "projects/p/subscriptions/orders" });
    for (key, value) in properties.as_object().unwrap() {
        all[key] = value.clone();
    }
    SourceSettings::from(&all).unwrap()
}

/// Each `verb` call's IDs, and the deadline it set if any, in order.
fn calls(fixture: &Fixture, wanted: &str) -> Vec<(Vec<String>, Option<u64>)> {
    fixture
        .seen()
        .iter()
        .filter(|request| verb(request) == wanted)
        .map(|request| {
            let body = body(request);
            let ids = body["ackIds"]
                .as_array()
                .unwrap()
                .iter()
                .map(|id| id.as_str().unwrap().to_string())
                .collect();
            (ids, body["ackDeadlineSeconds"].as_u64())
        })
        .collect()
}

fn ids(range: std::ops::Range<usize>) -> Vec<String> {
    range.map(|n| format!("a{n}")).collect()
}

#[test]
fn each_pull_is_extended_to_our_deadline_and_acknowledged_at_the_end() {
    let fixture = subscription_of(23, 10, None);
    let mut rows: Vec<Record> = Vec::new();
    let (summary, receipt) =
        receive(fixture_api(&fixture), &settings(json!({})), &mut rows).unwrap();
    assert_eq!(rows.len(), 23);
    assert_eq!(
        summary.detail,
        "23 message(s) from subscription 'orders' (no sign-in (an emulator over plain http)), \
         held until the run ends; the subscription answered empty"
    );
    let paths: Vec<String> = fixture
        .seen()
        .iter()
        .map(|r| r.path().to_string())
        .collect();
    assert_eq!(paths[0], "/v1/projects/p/subscriptions/orders:pull");

    // Every pull's messages extended at once, from the subscription's
    // deadline to ours.
    assert_eq!(
        calls(&fixture, "modifyAckDeadline"),
        [
            (ids(0..10), Some(60)),
            (ids(10..20), Some(60)),
            (ids(20..23), Some(60))
        ]
    );

    let line = Box::new(receipt).acknowledge().unwrap();
    assert_eq!(line, "23 message(s) acknowledged on subscription 'orders'");
    assert_eq!(calls(&fixture, "acknowledge"), [(ids(0..23), None)]);
}

#[test]
fn releasing_or_dropping_hands_every_message_out_again_at_once() {
    let fixture = subscription_of(4, 10, None);
    let (_, receipt) =
        receive(fixture_api(&fixture), &settings(json!({})), &mut Vec::new()).unwrap();
    assert_eq!(
        Box::new(receipt).release().unwrap(),
        "4 message(s) released back to subscription 'orders'"
    );
    let released = calls(&fixture, "modifyAckDeadline");
    assert_eq!(released.last().unwrap(), &(ids(0..4), Some(0)));

    let fixture = subscription_of(4, 10, None);
    let (_, receipt) =
        receive(fixture_api(&fixture), &settings(json!({})), &mut Vec::new()).unwrap();
    drop(receipt);
    let released = calls(&fixture, "modifyAckDeadline");
    assert_eq!(
        released.last().unwrap(),
        &(ids(0..4), Some(0)),
        "dropped: released"
    );
    assert!(calls(&fixture, "acknowledge").is_empty());
}

#[test]
fn max_records_stops_pulling_and_says_there_is_more() {
    let fixture = subscription_of(40, 10, None);
    let mut rows: Vec<Record> = Vec::new();
    let (summary, receipt) = receive(
        fixture_api(&fixture),
        &settings(json!({ "max_records": 15 })),
        &mut rows,
    )
    .unwrap();
    assert_eq!(rows.len(), 15);
    assert!(
        summary.detail.contains("stopped at max_records (15)"),
        "{}",
        summary.detail
    );
    let asked: Vec<u64> = fixture
        .seen()
        .iter()
        .filter(|r| verb(r) == "pull")
        .map(|r| body(r)["maxMessages"].as_u64().unwrap())
        .collect();
    assert_eq!(asked, [15, 5], "never more than max_records");
    Box::new(receipt).acknowledge().unwrap();
}

#[test]
fn a_hold_is_extended_while_the_run_goes_on() {
    let fixture = subscription_of(3, 10, None);
    let (_, receipt) = receive(
        fixture_api(&fixture),
        &settings(json!({ "ack_deadline_seconds": 1 })),
        &mut Vec::new(),
    )
    .unwrap();
    // Held for three half-periods: the extension at the pull, then at least
    // two by the keeper, each for the whole deadline again, and none after
    // the receipt is settled.
    std::thread::sleep(Duration::from_millis(1_300));
    Box::new(receipt).acknowledge().unwrap();
    let extended = calls(&fixture, "modifyAckDeadline");
    assert!(extended.len() >= 3, "{extended:?}");
    assert!(
        extended
            .iter()
            .all(|(held, deadline)| *held == ids(0..3) && *deadline == Some(1)),
        "{extended:?}"
    );

    let seen = fixture.seen();
    let last_extension = seen
        .iter()
        .rposition(|r| verb(r) == "modifyAckDeadline")
        .unwrap();
    let acknowledged = seen.iter().position(|r| verb(r) == "acknowledge").unwrap();
    assert!(
        last_extension < acknowledged,
        "the keeper stopped before the acknowledgement"
    );
}

#[test]
fn a_failed_acknowledgement_says_how_far_it_got() {
    let fixture = subscription_of(3, 10, Some("acknowledge"));
    let (_, receipt) =
        receive(fixture_api(&fixture), &settings(json!({})), &mut Vec::new()).unwrap();
    let error = Box::new(receipt).acknowledge().unwrap_err().to_string();
    assert!(
        error.starts_with(
            "0 of 3 message(s) were acknowledged on subscription 'orders' before: Pub/Sub \
             acknowledge: HTTP 400"
        ),
        "{error}"
    );
}

#[test]
fn a_row_that_will_not_decode_gives_everything_back() {
    let fixture = fixture::serve(|_, request| match verb(request).as_str() {
        "pull" => fixture::ok(json!({ "receivedMessages": [
            { "ackId": "a0", "message": { "messageId": "a", "data": base64_bytes(br#"{"n": 1}"#) } },
            { "ackId": "a1", "message": { "messageId": "b", "data": base64_bytes(b"not json") } },
        ]})),
        _ => fixture::ok(json!({})),
    });
    let error = receive(fixture_api(&fixture), &settings(json!({})), &mut Vec::new())
        .err()
        .expect("fails")
        .to_string();
    assert!(error.contains("message b"), "{error}");
    assert_eq!(
        calls(&fixture, "modifyAckDeadline").last().unwrap(),
        &(ids(0..2), Some(0))
    );
}

#[test]
fn a_missing_subscription_is_named() {
    let fixture = fixture::serve(|_, _| {
        fixture::status(
            404,
            r#"{"error":{"code":404,"message":"Subscription does not exist","status":"NOT_FOUND"}}"#,
        )
    });
    let error = receive(fixture_api(&fixture), &settings(json!({})), &mut Vec::new())
        .err()
        .expect("fails")
        .to_string();
    assert!(
        error.starts_with("subscription 'projects/p/subscriptions/orders': Pub/Sub pull: HTTP 404"),
        "{error}"
    );
}

// ----- signing in, and where requests go -----

#[test]
fn every_call_carries_one_cached_token() {
    let fixture = fixture::serve(|index, request| {
        if request.path() == "/token" {
            fixture::ok(json!({ "access_token": format!("token-{index}"), "expires_in": 3600 }))
        } else {
            fixture::ok(json!({ "messageIds": ["1"] }))
        }
    });
    let scratch = Scratch::new("pubsub-token");
    let file = scratch.write("key.json", &service_account(&fixture.url("/token")));
    // As Api::connect builds it for an https endpoint, but pointed at the
    // fixture, which speaks only http.
    let mut api = Api {
        client: Client::new(Settings::signed_post(
            fixture.url(""),
            Duration::from_secs(5),
            0,
        )),
        endpoint: fixture.url(""),
        tokens: tokens_from(&file),
        timeout: Duration::from_secs(5),
        retries: 0,
    };
    for _ in 0..2 {
        api.call("projects/p/topics/t", "publish", &json!({ "messages": [] }))
            .unwrap();
    }
    api.duplicate()
        .call("projects/p/topics/t", "publish", &json!({ "messages": [] }))
        .unwrap();

    let seen = fixture.seen();
    let paths: Vec<&str> = seen.iter().map(Seen::path).collect();
    assert_eq!(
        paths,
        [
            "/token",
            "/v1/projects/p/topics/t:publish",
            "/v1/projects/p/topics/t:publish",
            "/v1/projects/p/topics/t:publish"
        ]
    );
    for request in &seen[1..] {
        assert_eq!(request.header("Authorization"), Some("Bearer token-0"));
    }
    assert_eq!(
        api.signed_in_as(),
        format!(
            "service account etl@etl-test.iam.gserviceaccount.com, from credentials_file {}",
            file.display()
        )
    );
}

#[test]
fn plain_http_is_an_emulator_and_signs_nothing() {
    let fixture = fixture::serve(|_, _| fixture::ok(json!({ "messageIds": [] })));
    let mut api = fixture_api(&fixture);
    api.call("projects/p/topics/t", "publish", &json!({ "messages": [] }))
        .unwrap();
    assert_eq!(fixture.seen()[0].header("Authorization"), None);

    let emulator =
        |name: &str| (name == "PUBSUB_EMULATOR_HOST").then(|| "127.0.0.1:8085".to_string());
    let sources = Sources {
        var: &emulator,
        home: None,
    };
    assert_eq!(
        endpoint(&json!({}), &sources).unwrap(),
        "http://127.0.0.1:8085"
    );
    assert_eq!(
        endpoint(
            &json!({ "endpoint": "https://europe-west1-pubsub.googleapis.com/" }),
            &sources
        )
        .unwrap(),
        "https://europe-west1-pubsub.googleapis.com",
        "the node's endpoint wins"
    );
    let nothing = Sources {
        var: &|_| None,
        home: None,
    };
    assert_eq!(endpoint(&json!({}), &nothing).unwrap(), DEFAULT_ENDPOINT);

    // Google itself needs credentials, and says where it looked.
    let error = Api::connect(&json!({}), &nothing)
        .err()
        .expect("no credentials")
        .to_string();
    assert!(
        error.starts_with("property 'credentials_file': no Google credentials found"),
        "{error}"
    );
}

// ----- the sink, against the fixture -----

fn sink_settings(properties: JsonValue) -> SinkSettings {
    let mut all = json!({ "topic": "projects/p/topics/orders" });
    for (key, value) in properties.as_object().unwrap() {
        all[key] = value.clone();
    }
    SinkSettings::from(&all).unwrap()
}

/// A topic that takes every publish, or answers the `fail`th with a 400.
fn topic(fail: Option<usize>) -> Fixture {
    fixture::serve(move |index, request| {
        if Some(index) == fail {
            return fixture::status(400, r#"{"error":{"code":400,"message":"too big"}}"#);
        }
        let count = body(request)["messages"].as_array().unwrap().len();
        let ids: Vec<String> = (0..count).map(|n| n.to_string()).collect();
        fixture::ok(json!({ "messageIds": ids }))
    })
}

fn publish_to(
    fixture: &Fixture,
    settings: &SinkSettings,
    rows: Vec<JsonValue>,
) -> Result<Summary, ConnectorError> {
    publish(
        &mut fixture_api(fixture),
        settings,
        &mut fixture::records(rows),
    )
}

fn orders(count: u64) -> Vec<JsonValue> {
    (1..=count)
        .map(|id| json!({ "id": id, "customer": format!("C{}", id % 3) }))
        .collect()
}

/// The messages each publish carried, decoded.
fn published(fixture: &Fixture) -> Vec<Vec<JsonValue>> {
    fixture
        .seen()
        .iter()
        .map(|request| {
            body(request)["messages"]
                .as_array()
                .unwrap()
                .iter()
                .map(|message| {
                    let data = base64_decode(message["data"].as_str().unwrap()).unwrap();
                    let mut decoded: JsonValue = serde_json::from_slice(&data).unwrap();
                    for key in ["orderingKey", "attributes"] {
                        if let Some(value) = message.get(key) {
                            decoded[format!("<{key}>")] = value.clone();
                        }
                    }
                    decoded
                })
                .collect()
        })
        .collect()
}

#[test]
fn messages_go_a_thousand_to_a_call_and_under_10_mb() {
    let fixture = topic(None);
    let summary = publish_to(&fixture, &sink_settings(json!({})), orders(2300)).unwrap();
    let sizes: Vec<usize> = published(&fixture).iter().map(Vec::len).collect();
    assert_eq!(sizes, [1000, 1000, 300]);
    assert_eq!(
        fixture.seen()[0].path(),
        "/v1/projects/p/topics/orders:publish"
    );
    assert!(
        summary
            .detail
            .starts_with("2300 message(s) in 3 call(s) to topic 'orders'"),
        "{}",
        summary.detail
    );
    assert_eq!(published(&fixture)[2][299]["id"], 2300, "in order");

    // Three rows of 3 MB, 4 MB each once base64: two fit under 10 MB.
    let fixture = topic(None);
    let rows = (1..=3)
        .map(|id| json!({ "id": id, "blob": "x".repeat(3_000_000) }))
        .collect();
    publish_to(&fixture, &sink_settings(json!({})), rows).unwrap();
    let sizes: Vec<usize> = published(&fixture).iter().map(Vec::len).collect();
    assert_eq!(sizes, [2, 1]);

    let big = vec![json!({ "id": 1, "blob": "x".repeat(8_000_000) })];
    let error = publish_to(&topic(None), &sink_settings(json!({})), big)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("row 1 is 8000"), "{error}");
}

#[test]
fn a_message_carries_its_ordering_key_and_attributes() {
    let fixture = topic(None);
    let settings = sink_settings(json!({
        "ordering_key_column": "customer", "attributes_column": "meta",
    }));
    let rows = vec![
        json!({ "id": 1, "customer": "C1", "meta": { "source": "web", "tries": 2, "gone": null } }),
        json!({ "id": 2, "customer": null, "meta": null }),
    ];
    publish_to(&fixture, &settings, rows).unwrap();
    let messages = &published(&fixture)[0];
    assert_eq!(messages[0]["<orderingKey>"], "C1");
    assert_eq!(
        messages[0]["<attributes>"],
        json!({ "source": "web", "tries": "2" }),
        "values as text, nulls left out"
    );
    assert_eq!(messages[1].get("<orderingKey>"), None, "null: no key");
    assert_eq!(messages[1].get("<attributes>"), None);

    let error = publish_to(
        &topic(None),
        &settings,
        vec![json!({ "id": 1, "customer": "C1", "meta": "web" })],
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.starts_with("row 1: 'meta' is a string, and attributes_column needs an object"),
        "{error}"
    );

    let error = publish_to(
        &topic(None),
        &sink_settings(json!({ "ordering_key_column": "region" })),
        orders(1),
    )
    .unwrap_err()
    .to_string();
    assert_eq!(
        error,
        "property 'ordering_key_column': 'region' is not a column of the rows"
    );
}

#[test]
fn a_refused_publish_fails_saying_what_landed() {
    let fixture = topic(Some(1));
    let error = publish_to(&fixture, &sink_settings(json!({})), orders(1500))
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with("topic 'orders': Pub/Sub publish: HTTP 400"),
        "{error}"
    );
    assert!(
        error.ends_with(
            "1000 message(s) had been published to topic 'orders' before this, and stay there"
        ),
        "{error}"
    );
    assert_eq!(fixture.seen().len(), 2, "a 400 is not sent again");
}

// ---------------------------------------------------------------------------
// Against the emulator
// ---------------------------------------------------------------------------

/// The project every emulator test works in; the emulator takes any.
const PROJECT: &str = "etl-test";

fn server() -> Option<String> {
    match std::env::var("ETL_TEST_PUBSUB") {
        Ok(url) if !url.trim().is_empty() => Some(url.trim().trim_end_matches('/').to_string()),
        _ => {
            eprintln!("skipping: ETL_TEST_PUBSUB is not set; see scripts/test-services.ps1");
            None
        }
    }
}

/// A topic and a subscription to it for one test, deleted when the test
/// ends, pass or fail.
struct TestTopic {
    endpoint: String,
    topic: String,
    subscription: String,
}

impl TestTopic {
    fn new(endpoint: &str, test: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let name = format!("etl-{test}-{}-{nanos}", std::process::id());
        let made = TestTopic {
            endpoint: endpoint.to_string(),
            topic: name.clone(),
            subscription: format!("{name}-sub"),
        };
        let base = format!("{endpoint}/v1/projects/{PROJECT}");
        ureq::put(&format!("{base}/topics/{}", made.topic))
            .header("Content-Type", "application/json")
            .send("{}")
            .expect("the topic");
        ureq::put(&format!("{base}/subscriptions/{}", made.subscription))
            .header("Content-Type", "application/json")
            .send(
                json!({
                    "topic": format!("projects/{PROJECT}/topics/{}", made.topic),
                    "ackDeadlineSeconds": 10,
                    "enableMessageOrdering": true,
                })
                .to_string(),
            )
            .expect("the subscription");
        made
    }
}

impl Drop for TestTopic {
    fn drop(&mut self) {
        let base = format!("{}/v1/projects/{PROJECT}", self.endpoint);
        let _ = ureq::delete(&format!("{base}/subscriptions/{}", self.subscription)).call();
        let _ = ureq::delete(&format!("{base}/topics/{}", self.topic)).call();
    }
}

fn properties(endpoint: &str, extra: JsonValue) -> JsonValue {
    let mut all = json!({ "project": PROJECT, "endpoint": endpoint });
    for (key, value) in extra.as_object().unwrap() {
        all[key] = value.clone();
    }
    all
}

/// Publish `rows` through the sink, as a pipeline would.
fn put(endpoint: &str, test: &TestTopic, rows: Vec<JsonValue>, extra: JsonValue) {
    let mut sink = properties(endpoint, extra);
    sink["topic"] = json!(test.topic);
    PubsubSink
        .write(&sink, &mut fixture::records(rows), &Context::default())
        .expect("publishes");
}

fn take(
    endpoint: &str,
    test: &TestTopic,
    extra: JsonValue,
) -> (Vec<Record>, Summary, Box<dyn Receipt>) {
    let mut source = properties(endpoint, extra);
    source["subscription"] = json!(test.subscription);
    let mut rows: Vec<Record> = Vec::new();
    let (summary, receipt) = PubsubSource
        .read_held(&source, &mut rows, &Context::default())
        .expect("pulls");
    (rows, summary, receipt.expect("a receipt"))
}

fn sorted_ids(rows: &[Record]) -> Vec<u64> {
    let mut ids: Vec<u64> = rows.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    ids.sort_unstable();
    ids
}

#[test]
fn an_acknowledged_run_empties_the_subscription() {
    let Some(endpoint) = server() else { return };
    let test = TestTopic::new(&endpoint, "ack");
    put(&endpoint, &test, orders(25), json!({}));

    let (rows, summary, receipt) = take(&endpoint, &test, json!({}));
    assert_eq!(sorted_ids(&rows), (1..=25).collect::<Vec<_>>());
    assert!(
        summary.detail.ends_with("the subscription answered empty"),
        "{}",
        summary.detail
    );
    assert_eq!(
        receipt.acknowledge().unwrap(),
        format!(
            "25 message(s) acknowledged on subscription '{}'",
            test.subscription
        )
    );

    let (again, _, receipt) = take(&endpoint, &test, json!({}));
    assert!(again.is_empty(), "nothing left: {again:?}");
    receipt.acknowledge().unwrap();
}

#[test]
fn a_released_or_dropped_run_is_received_again() {
    let Some(endpoint) = server() else { return };
    let test = TestTopic::new(&endpoint, "release");
    put(&endpoint, &test, orders(12), json!({}));

    let (first, _, receipt) = take(&endpoint, &test, json!({}));
    assert_eq!(first.len(), 12);
    receipt.release().unwrap();

    let (second, _, receipt) = take(&endpoint, &test, json!({}));
    assert_eq!(
        sorted_ids(&second),
        (1..=12).collect::<Vec<_>>(),
        "all back, at once"
    );
    let message_ids = |rows: &[Record]| {
        let mut ids: Vec<String> = rows
            .iter()
            .map(|row| row["_message_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids
    };
    assert_eq!(
        message_ids(&second),
        message_ids(&first),
        "the same messages"
    );
    drop(receipt);

    let (third, _, receipt) = take(&endpoint, &test, json!({}));
    assert_eq!(third.len(), 12, "dropping released them too");
    receipt.acknowledge().unwrap();
}

#[test]
fn max_records_leaves_the_rest_for_the_next_run() {
    let Some(endpoint) = server() else { return };
    let test = TestTopic::new(&endpoint, "cap");
    put(&endpoint, &test, orders(25), json!({}));

    let (first, summary, receipt) = take(&endpoint, &test, json!({ "max_records": 10 }));
    assert_eq!(first.len(), 10);
    assert!(
        summary.detail.contains("stopped at max_records (10)"),
        "{}",
        summary.detail
    );
    receipt.acknowledge().unwrap();

    let (rest, _, receipt) = take(&endpoint, &test, json!({}));
    let mut all = sorted_ids(&first);
    all.extend(sorted_ids(&rest));
    all.sort_unstable();
    assert_eq!(all, (1..=25).collect::<Vec<_>>(), "each once, none lost");
    receipt.acknowledge().unwrap();
}

#[test]
fn a_held_message_stays_held_past_its_ack_deadline() {
    let Some(endpoint) = server() else { return };
    let test = TestTopic::new(&endpoint, "lease");
    put(&endpoint, &test, orders(3), json!({}));

    let (held, _, receipt) = take(&endpoint, &test, json!({ "ack_deadline_seconds": 2 }));
    assert_eq!(held.len(), 3);
    // Three times the deadline: without the lease keeper, another pull would
    // be handed all three by now.
    std::thread::sleep(Duration::from_secs(6));
    let (meanwhile, _, other) = take(&endpoint, &test, json!({}));
    assert!(meanwhile.is_empty(), "still held: {meanwhile:?}");
    other.acknowledge().unwrap();

    receipt.release().unwrap();
    let (back, _, receipt) = take(&endpoint, &test, json!({}));
    assert_eq!(back.len(), 3);
    receipt.acknowledge().unwrap();
}

#[test]
fn ordering_keys_and_attributes_make_the_round_trip() {
    let Some(endpoint) = server() else { return };
    let test = TestTopic::new(&endpoint, "order");
    let rows: Vec<JsonValue> = orders(9)
        .into_iter()
        .map(|mut row| {
            row["meta"] = json!({ "id": row["id"].to_string() });
            row
        })
        .collect();
    put(
        &endpoint,
        &test,
        rows,
        json!({ "ordering_key_column": "customer", "attributes_column": "meta" }),
    );

    let (rows, _, receipt) = take(&endpoint, &test, json!({}));
    assert_eq!(rows.len(), 9);
    let mut by_key: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for row in &rows {
        assert_eq!(row["_ordering_key"], row["customer"]);
        assert_eq!(row["_attributes"], json!({ "id": row["id"].to_string() }));
        assert!(row["_publish_time"].as_str().unwrap().starts_with("20"));
        by_key
            .entry(row["_ordering_key"].as_str().unwrap().to_string())
            .or_default()
            .push(row["id"].as_u64().unwrap());
    }
    assert_eq!(by_key.len(), 3);
    for (key, ids) in &by_key {
        assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "{key}: {ids:?}"
        );
    }
    receipt.acknowledge().unwrap();
}

#[test]
fn a_subscription_that_does_not_exist_is_named() {
    let Some(endpoint) = server() else { return };
    let error = PubsubSource
        .read_held(
            &properties(
                &endpoint,
                json!({ "subscription": "etl-no-such-subscription" }),
            ),
            &mut Vec::new(),
            &Context::default(),
        )
        .err()
        .expect("fails")
        .to_string();
    assert!(
        error.starts_with(&format!(
            "subscription 'projects/{PROJECT}/subscriptions/etl-no-such-subscription': Pub/Sub \
             pull: HTTP 404"
        )),
        "{error}"
    );
}
