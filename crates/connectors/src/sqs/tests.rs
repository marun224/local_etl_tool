//! What decides whether a message is read once, again, or lost: the receipt
//! settling on every path, the lease keeper, refusals. First against the local
//! fixture, which answers as told; then against ElasticMQ when `ETL_TEST_SQS`
//! names it (`scripts/test-services.ps1` starts it). Without it those skip.

use super::*;
use crate::fixture::{self, Fixture, Seen};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Without a server
// ---------------------------------------------------------------------------

#[test]
fn a_setting_that_cannot_work_is_refused_by_property() {
    let source = |properties: JsonValue| SqsSource.check(&properties).map_err(|e| e.to_string());
    let sink = |properties: JsonValue| SqsSink.check(&properties).map_err(|e| e.to_string());
    let refused = |result: Result<(), String>, property: &str| {
        let error = result.unwrap_err();
        assert!(
            error.starts_with(&format!("property '{property}'")),
            "{property}: {error}"
        );
    };

    refused(source(json!({})), "queue_url");
    refused(
        source(json!({ "queue_url": "https://q/1/a", "queue": "a" })),
        "queue_url",
    );
    refused(source(json!({ "queue_url": "sqs.local/1/a" })), "queue_url");
    refused(
        source(json!({ "queue_url": "https://q/1/a", "queue_owner": "1" })),
        "queue_owner",
    );
    refused(
        source(json!({ "queue": "a", "visibility_seconds": 43_201 })),
        "visibility_seconds",
    );
    refused(
        source(json!({ "queue": "a", "value_format": "xml" })),
        "value_format",
    );
    refused(
        source(json!({ "queue": "a", "access_key_id": "K" })),
        "access_key_id",
    );
    source(json!({ "queue": "a", "queue_owner": "123456789012" })).expect("fine");

    refused(
        sink(json!({ "queue": "a.fifo" })),
        "message_group_id_column",
    );
    refused(
        sink(json!({ "queue": "a", "deduplication_id_column": "id" })),
        "deduplication_id_column",
    );
    refused(
        sink(json!({ "queue": "a", "delay_seconds": 901 })),
        "delay_seconds",
    );
    refused(
        sink(json!({ "queue": "a.fifo", "message_group_id_column": "g", "delay_seconds": 5 })),
        "delay_seconds",
    );
    sink(json!({ "queue": "a.fifo", "message_group_id_column": "g" })).expect("fine");
}

#[test]
fn a_message_becomes_its_fields_beside_the_underscore_columns() {
    let message = json!({
        "MessageId": "m-1",
        "Body": r#"{"id": 7, "customer": "C1"}"#,
        "Attributes": {
            "SentTimestamp": "1790157907089",
            "ApproximateReceiveCount": "2",
            "MessageGroupId": "C1",
        },
        "MessageAttributes": {
            "source": { "DataType": "String", "StringValue": "web" },
            "blob": { "DataType": "Binary", "BinaryValue": "AAE=" },
        },
    });
    let row = row("orders.fifo", &message, Format::Json).unwrap();
    assert_eq!(row["id"], 7);
    assert_eq!(row["_queue"], "orders.fifo");
    assert_eq!(row["_message_id"], "m-1");
    assert_eq!(row["_sent_timestamp"], "2026-09-23 10:05:07.089");
    assert_eq!(row["_receive_count"], 2);
    assert_eq!(row["_group_id"], "C1");
    assert_eq!(
        row["_attributes"],
        json!({ "source": "web", "blob": "AAE=" })
    );

    let text = super::row(
        "q",
        &json!({ "MessageId": "m", "Body": "plain" }),
        Format::Text,
    )
    .unwrap();
    assert_eq!(text["value"], "plain");
    assert_eq!(
        text["_group_id"],
        JsonValue::Null,
        "a standard queue has no group"
    );
    assert_eq!(text["_attributes"], json!({}));
}

/// The operation a request named, from its `X-Amz-Target`.
fn operation(request: &Seen) -> String {
    request
        .header("X-Amz-Target")
        .unwrap_or_default()
        .trim_start_matches("AmazonSQS.")
        .to_string()
}

fn body(request: &Seen) -> JsonValue {
    serde_json::from_str(&request.body).unwrap()
}

fn fixture_api(fixture: &Fixture) -> JsonApi {
    JsonApi::connect(
        &json!({
            "endpoint": fixture.url(""), "region": "eu-west-1", "retries": 1,
            "access_key_id": "AKIDTEST", "secret_access_key": "test-secret",
        }),
        &Sources::process(),
        &SQS,
    )
    .unwrap()
}

/// A queue of `count` messages that each receive drains, ten at a time;
/// batch calls succeed unless `refuse` names a handle to refuse.
fn queue_of(count: usize, refuse: Option<&'static str>) -> Fixture {
    let taken = Arc::new(Mutex::new(0usize));
    fixture::serve(move |_, request| match operation(request).as_str() {
        "ReceiveMessage" => {
            let wanted = body(request)["MaxNumberOfMessages"].as_u64().unwrap() as usize;
            let mut taken = taken.lock().unwrap();
            let messages: Vec<JsonValue> = (*taken..count.min(*taken + wanted))
                .map(|n| {
                    json!({
                        "MessageId": format!("m{n}"), "ReceiptHandle": format!("h{n}"),
                        "Body": format!(r#"{{"n": {n}}}"#),
                        "Attributes": { "ApproximateReceiveCount": "1" },
                    })
                })
                .collect();
            *taken += messages.len();
            fixture::ok(json!({ "Messages": messages }))
        }
        "DeleteMessageBatch" | "ChangeMessageVisibilityBatch" => {
            let entries = body(request)["Entries"].as_array().unwrap().clone();
            let (failed, successful): (Vec<_>, Vec<_>) = entries
                .iter()
                .partition(|entry| Some(entry["ReceiptHandle"].as_str().unwrap()) == refuse);
            fixture::ok(json!({
                "Successful": successful.iter().map(|e| json!({ "Id": e["Id"] })).collect::<Vec<_>>(),
                "Failed": failed.iter().map(|e| json!({
                    "Id": e["Id"], "SenderFault": true,
                    "Code": "ReceiptHandleIsInvalid", "Message": "gone",
                })).collect::<Vec<_>>(),
            }))
        }
        other => fixture::status(400, &format!(r#"{{"__type":"{other} not expected"}}"#)),
    })
}

fn settings(properties: JsonValue) -> SourceSettings {
    let mut all = json!({ "queue_url": "http://127.0.0.1/000000000000/orders" });
    for (key, value) in properties.as_object().unwrap() {
        all[key] = value.clone();
    }
    SourceSettings::from(&all).unwrap()
}

/// The handles each batch call carried, and their visibility if any.
fn settled(fixture: &Fixture, operation_name: &str) -> Vec<(String, Option<u64>)> {
    fixture
        .seen()
        .iter()
        .filter(|request| operation(request) == operation_name)
        .flat_map(|request| body(request)["Entries"].as_array().unwrap().clone())
        .map(|entry| {
            (
                entry["ReceiptHandle"].as_str().unwrap().to_string(),
                entry["VisibilityTimeout"].as_u64(),
            )
        })
        .collect()
}

fn handles(range: std::ops::Range<usize>) -> Vec<(String, Option<u64>)> {
    range.map(|n| (format!("h{n}"), None)).collect()
}

#[test]
fn acknowledging_deletes_every_message_received_in_tens() {
    let fixture = queue_of(23, None);
    let mut rows: Vec<Record> = Vec::new();
    let (summary, receipt) =
        receive(fixture_api(&fixture), &settings(json!({})), &mut rows).unwrap();
    assert_eq!(rows.len(), 23);
    assert!(
        summary.detail.ends_with("; the queue answered empty"),
        "{}",
        summary.detail
    );

    let line = Box::new(receipt).acknowledge().unwrap();
    assert_eq!(line, "23 message(s) deleted from queue 'orders'");
    assert_eq!(settled(&fixture, "DeleteMessageBatch"), handles(0..23));
    let calls = fixture
        .seen()
        .iter()
        .filter(|r| operation(r) == "DeleteMessageBatch")
        .count();
    assert_eq!(calls, 3, "ten to a call");
}

#[test]
fn releasing_or_dropping_shows_every_message_again_at_once() {
    let fixture = queue_of(4, None);
    let (_, receipt) =
        receive(fixture_api(&fixture), &settings(json!({})), &mut Vec::new()).unwrap();
    assert_eq!(
        Box::new(receipt).release().unwrap(),
        "4 message(s) released back to queue 'orders'"
    );
    let zero: Vec<_> = (0..4).map(|n| (format!("h{n}"), Some(0))).collect();
    assert_eq!(settled(&fixture, "ChangeMessageVisibilityBatch"), zero);

    let fixture = queue_of(4, None);
    let (_, receipt) =
        receive(fixture_api(&fixture), &settings(json!({})), &mut Vec::new()).unwrap();
    drop(receipt);
    assert_eq!(
        settled(&fixture, "ChangeMessageVisibilityBatch"),
        zero,
        "dropped: released"
    );
    assert!(settled(&fixture, "DeleteMessageBatch").is_empty());
}

#[test]
fn max_records_stops_receiving_and_says_there_is_more() {
    let fixture = queue_of(40, None);
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
        .filter(|r| operation(r) == "ReceiveMessage")
        .map(|r| body(r)["MaxNumberOfMessages"].as_u64().unwrap())
        .collect();
    assert_eq!(asked, [10, 5], "never more than max_records");
    Box::new(receipt).acknowledge().unwrap();
}

#[test]
fn a_hold_is_extended_while_the_run_goes_on() {
    let fixture = queue_of(3, None);
    let (_, receipt) = receive(
        fixture_api(&fixture),
        &settings(json!({ "visibility_seconds": 1 })),
        &mut Vec::new(),
    )
    .unwrap();
    // Held for three half-periods: at least two extensions, each for the
    // whole visibility again, and none after the receipt is settled.
    std::thread::sleep(Duration::from_millis(1_300));
    Box::new(receipt).acknowledge().unwrap();
    let extended: Vec<(String, Option<u64>)> = settled(&fixture, "ChangeMessageVisibilityBatch");
    assert!(extended.len() >= 6, "{extended:?}");
    assert!(
        extended
            .iter()
            .all(|(_, visibility)| *visibility == Some(1)),
        "{extended:?}"
    );

    let last_extension = fixture
        .seen()
        .iter()
        .rposition(|r| operation(r) == "ChangeMessageVisibilityBatch")
        .unwrap();
    let delete = fixture
        .seen()
        .iter()
        .position(|r| operation(r) == "DeleteMessageBatch")
        .unwrap();
    assert!(
        last_extension < delete,
        "the keeper stopped before the delete"
    );
}

#[test]
fn a_message_that_cannot_be_deleted_is_counted_and_named() {
    let fixture = queue_of(12, Some("h11"));
    let (_, receipt) =
        receive(fixture_api(&fixture), &settings(json!({})), &mut Vec::new()).unwrap();
    let error = Box::new(receipt).acknowledge().unwrap_err().to_string();
    assert_eq!(
        error,
        "11 message(s) deleted from queue 'orders', and 1 could not be; the first said \
         ReceiptHandleIsInvalid: gone"
    );
}

#[test]
fn a_row_that_will_not_decode_gives_everything_back() {
    let fixture = fixture::serve(|_, request| match operation(request).as_str() {
        "ReceiveMessage" => fixture::ok(json!({ "Messages": [
            { "MessageId": "a", "ReceiptHandle": "h0", "Body": r#"{"n": 1}"# },
            { "MessageId": "b", "ReceiptHandle": "h1", "Body": "not json" },
        ]})),
        _ => fixture::ok(json!({ "Successful": [], "Failed": [] })),
    });
    let error = receive(fixture_api(&fixture), &settings(json!({})), &mut Vec::new())
        .err()
        .expect("fails")
        .to_string();
    assert!(error.contains("message b"), "{error}");
    assert_eq!(
        settled(&fixture, "ChangeMessageVisibilityBatch"),
        [("h0".to_string(), Some(0)), ("h1".to_string(), Some(0))]
    );
}

// ----- the sink, against the fixture -----

fn sink_settings(properties: JsonValue) -> SinkSettings {
    let mut all = json!({ "queue_url": "http://127.0.0.1/000000000000/orders" });
    for (key, value) in properties.as_object().unwrap() {
        all[key] = value.clone();
    }
    SinkSettings::from(&all).unwrap()
}

/// Answers `SendMessageBatch`, refusing the entries `refuse` picks by row and
/// attempt, with `sender_fault`.
fn inbox(refuse: impl Fn(usize, u64) -> Option<bool> + Send + 'static) -> Fixture {
    fixture::serve(move |index, request| {
        let entries = body(request)["Entries"].as_array().unwrap().clone();
        let (mut successful, mut failed) = (Vec::new(), Vec::new());
        for entry in entries {
            let row: u64 = entry["Id"].as_str().unwrap().parse().unwrap();
            match refuse(index, row) {
                Some(sender_fault) => failed.push(json!({
                    "Id": entry["Id"], "SenderFault": sender_fault,
                    "Code": if sender_fault { "InvalidParameterValue" } else { "InternalError" },
                    "Message": "no",
                })),
                None => successful.push(json!({ "Id": entry["Id"], "MessageId": "x" })),
            }
        }
        fixture::ok(json!({ "Successful": successful, "Failed": failed }))
    })
}

fn send_to(
    fixture: &Fixture,
    settings: &SinkSettings,
    rows: Vec<JsonValue>,
) -> Result<Summary, ConnectorError> {
    send_messages(
        &mut fixture_api(fixture),
        settings,
        &mut fixture::records(rows),
        Duration::from_millis(1),
    )
}

fn orders(count: u64) -> Vec<JsonValue> {
    (1..=count)
        .map(|id| json!({ "id": id, "customer": format!("C{}", id % 3) }))
        .collect()
}

fn sent_rows(fixture: &Fixture) -> Vec<Vec<u64>> {
    fixture
        .seen()
        .iter()
        .map(|request| {
            body(request)["Entries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| {
                    let message: JsonValue =
                        serde_json::from_str(entry["MessageBody"].as_str().unwrap()).unwrap();
                    message["id"].as_u64().unwrap()
                })
                .collect()
        })
        .collect()
}

#[test]
fn messages_go_ten_to_a_call_and_under_1_mib() {
    let fixture = inbox(|_, _| None);
    let summary = send_to(&fixture, &sink_settings(json!({})), orders(23)).unwrap();
    assert_eq!(
        sent_rows(&fixture),
        [
            (1..=10).collect::<Vec<_>>(),
            (11..=20).collect(),
            (21..=23).collect()
        ]
    );
    assert!(
        summary
            .detail
            .starts_with("23 message(s) in 3 call(s) to queue 'orders'"),
        "{}",
        summary.detail
    );

    // Three of 400 KB: two fit under 1 MiB, the third waits.
    let fixture = inbox(|_, _| None);
    let rows = (1..=3)
        .map(|id| json!({ "id": id, "blob": "x".repeat(400_000) }))
        .collect();
    send_to(&fixture, &sink_settings(json!({})), rows).unwrap();
    assert_eq!(sent_rows(&fixture), [vec![1, 2], vec![3]]);

    let big = vec![json!({ "id": 1, "blob": "x".repeat(1024 * 1024) })];
    let error = send_to(&inbox(|_, _| None), &sink_settings(json!({})), big)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("row 1 is 104"), "{error}");
}

#[test]
fn messages_refused_on_sqss_side_alone_are_sent_again() {
    // The first call refuses rows 2 and 4 through no fault of ours.
    let fixture = inbox(|attempt, row| (attempt == 0 && (row == 2 || row == 4)).then_some(false));
    let summary = send_to(&fixture, &sink_settings(json!({})), orders(5)).unwrap();
    assert_eq!(sent_rows(&fixture), [vec![1, 2, 3, 4, 5], vec![2, 4]]);
    assert_eq!(summary.records, 5);
    assert!(
        summary
            .detail
            .ends_with("; 2 sent again after SQS refused them on its own side"),
        "{}",
        summary.detail
    );
}

#[test]
fn a_refusal_that_is_ours_to_fix_fails_at_once_saying_what_landed() {
    let fixture = inbox(|_, row| (row == 3).then_some(true));
    let error = send_to(&fixture, &sink_settings(json!({})), orders(5))
        .unwrap_err()
        .to_string();
    assert_eq!(fixture.seen().len(), 1, "not sent again");
    assert!(
        error.starts_with("SQS refused row 3: InvalidParameterValue: no"),
        "{error}"
    );
    assert!(
        error.contains("4 message(s) had been sent to queue 'orders'"),
        "{error}"
    );

    let fixture = inbox(|_, row| (row == 1).then_some(false));
    let error = send_to(&fixture, &sink_settings(json!({ "retries": 2 })), orders(2))
        .unwrap_err()
        .to_string();
    assert_eq!(fixture.seen().len(), 3, "the call and 2 resends");
    assert!(
        error.starts_with("SQS still refused 1 message(s), the first row 1, after 2 resend(s)"),
        "{error}"
    );
}

#[test]
fn a_fifo_message_carries_its_group_and_deduplication_id() {
    let fixture = inbox(|_, _| None);
    let settings = SinkSettings::from(&json!({
        "queue_url": "http://127.0.0.1/000000000000/orders.fifo",
        "message_group_id_column": "customer", "deduplication_id_column": "id",
    }))
    .unwrap();
    send_to(&fixture, &settings, orders(2)).unwrap();
    let entries = body(&fixture.seen()[0])["Entries"].clone();
    assert_eq!(entries[0]["MessageGroupId"], "C1");
    assert_eq!(entries[0]["MessageDeduplicationId"], "1");
    assert_eq!(entries[1]["MessageGroupId"], "C2");

    let mut rows = orders(2);
    rows[1]["customer"] = JsonValue::Null;
    let error = send_to(&inbox(|_, _| None), &settings, rows)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("row 2: 'customer' is null"), "{error}");
}

// ---------------------------------------------------------------------------
// Against ElasticMQ
// ---------------------------------------------------------------------------

fn server() -> Option<String> {
    match std::env::var("ETL_TEST_SQS") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!("skipping: ETL_TEST_SQS is not set; see scripts/test-services.ps1");
            None
        }
    }
}

fn properties(endpoint: &str, queue: &str) -> JsonValue {
    json!({
        "queue": queue, "endpoint": endpoint, "region": "us-east-1",
        "access_key_id": "AKIDTEST", "secret_access_key": "test-secret",
    })
}

fn api(endpoint: &str) -> JsonApi {
    JsonApi::connect(&properties(endpoint, "unused"), &Sources::process(), &SQS).unwrap()
}

/// A queue for one test, deleted when the test ends, pass or fail.
struct TestQueue {
    endpoint: String,
    name: String,
    url: String,
}

impl Drop for TestQueue {
    fn drop(&mut self) {
        let _ = api(&self.endpoint).call("DeleteQueue", &json!({ "QueueUrl": self.url }));
    }
}

fn create(endpoint: &str, test: &str, fifo: bool) -> TestQueue {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let mut name = format!("etl-{test}-{}-{nanos}", std::process::id());
    let mut request = json!({ "QueueName": name });
    if fifo {
        name.push_str(".fifo");
        request = json!({ "QueueName": name, "Attributes": {
            "FifoQueue": "true", "ContentBasedDeduplication": "true" } });
    }
    let answer = api(endpoint).call("CreateQueue", &request).unwrap();
    TestQueue {
        endpoint: endpoint.to_string(),
        name,
        url: answer["QueueUrl"].as_str().unwrap().to_string(),
    }
}

/// Send `rows` through the sink, as a pipeline would.
fn put(endpoint: &str, queue: &TestQueue, rows: Vec<JsonValue>, extra: JsonValue) {
    let mut sink = properties(endpoint, &queue.name);
    for (key, value) in extra.as_object().unwrap() {
        sink[key] = value.clone();
    }
    SqsSink
        .write(&sink, &mut fixture::records(rows), &Context::default())
        .expect("sends");
}

fn take(
    endpoint: &str,
    queue: &TestQueue,
    extra: JsonValue,
) -> (Vec<Record>, Summary, Box<dyn Receipt>) {
    let mut source = properties(endpoint, &queue.name);
    for (key, value) in extra.as_object().unwrap() {
        source[key] = value.clone();
    }
    let mut rows: Vec<Record> = Vec::new();
    let (summary, receipt) = SqsSource
        .read_held(&source, &mut rows, &Context::default())
        .expect("receives");
    (rows, summary, receipt.expect("a receipt"))
}

fn ids(rows: &[Record]) -> Vec<u64> {
    let mut ids: Vec<u64> = rows.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    ids.sort_unstable();
    ids
}

#[test]
fn an_acknowledged_run_empties_the_queue() {
    let Some(endpoint) = server() else { return };
    let queue = create(&endpoint, "ack", false);
    put(&endpoint, &queue, orders(25), json!({}));

    let (rows, summary, receipt) = take(&endpoint, &queue, json!({}));
    assert_eq!(ids(&rows), (1..=25).collect::<Vec<_>>());
    assert!(
        summary.detail.ends_with("the queue answered empty"),
        "{}",
        summary.detail
    );
    assert_eq!(
        receipt.acknowledge().unwrap(),
        format!("25 message(s) deleted from queue '{}'", queue.name)
    );

    let (again, _, receipt) = take(&endpoint, &queue, json!({}));
    assert!(again.is_empty(), "nothing left");
    receipt.acknowledge().unwrap();
}

#[test]
fn a_released_or_dropped_run_is_received_again_counted_twice() {
    let Some(endpoint) = server() else { return };
    let queue = create(&endpoint, "release", false);
    put(&endpoint, &queue, orders(12), json!({}));

    let (first, _, receipt) = take(&endpoint, &queue, json!({}));
    assert_eq!(first.len(), 12);
    receipt.release().unwrap();

    let (second, _, receipt) = take(&endpoint, &queue, json!({}));
    assert_eq!(
        ids(&second),
        (1..=12).collect::<Vec<_>>(),
        "all back, at once"
    );
    assert!(
        second.iter().all(|row| row["_receive_count"] == 2),
        "{second:?}"
    );
    drop(receipt);

    let (third, _, receipt) = take(&endpoint, &queue, json!({}));
    assert_eq!(third.len(), 12, "dropping released them too");
    receipt.acknowledge().unwrap();
}

#[test]
fn max_records_leaves_the_rest_for_the_next_run() {
    let Some(endpoint) = server() else { return };
    let queue = create(&endpoint, "cap", false);
    put(&endpoint, &queue, orders(25), json!({}));

    let (first, summary, receipt) = take(&endpoint, &queue, json!({ "max_records": 10 }));
    assert_eq!(first.len(), 10);
    assert!(
        summary.detail.contains("stopped at max_records (10)"),
        "{}",
        summary.detail
    );
    receipt.acknowledge().unwrap();

    let (rest, _, receipt) = take(&endpoint, &queue, json!({}));
    let mut all = ids(&first);
    all.extend(ids(&rest));
    all.sort_unstable();
    assert_eq!(all, (1..=25).collect::<Vec<_>>(), "each once, none lost");
    receipt.acknowledge().unwrap();
}

#[test]
fn a_held_message_stays_hidden_past_its_visibility_timeout() {
    let Some(endpoint) = server() else { return };
    let queue = create(&endpoint, "lease", false);
    put(&endpoint, &queue, orders(3), json!({}));

    let (held, _, receipt) = take(&endpoint, &queue, json!({ "visibility_seconds": 1 }));
    assert_eq!(held.len(), 3);
    // Three times the visibility timeout: without the lease keeper, another
    // consumer would be handed all three by now.
    std::thread::sleep(Duration::from_secs(3));
    let (meanwhile, _, other) = take(&endpoint, &queue, json!({}));
    assert!(meanwhile.is_empty(), "still held: {meanwhile:?}");
    other.acknowledge().unwrap();

    receipt.release().unwrap();
    let (back, _, receipt) = take(&endpoint, &queue, json!({}));
    assert_eq!(back.len(), 3);
    receipt.acknowledge().unwrap();
}

#[test]
fn a_fifo_queue_keeps_each_groups_order() {
    let Some(endpoint) = server() else { return };
    let queue = create(&endpoint, "fifo", true);
    put(
        &endpoint,
        &queue,
        orders(9),
        json!({ "message_group_id_column": "customer" }),
    );

    let (rows, _, receipt) = take(&endpoint, &queue, json!({}));
    let mut by_group: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for row in &rows {
        assert_eq!(row["_group_id"], row["customer"]);
        by_group
            .entry(row["_group_id"].as_str().unwrap().to_string())
            .or_default()
            .push(row["id"].as_u64().unwrap());
    }
    assert!(!by_group.is_empty());
    for (group, ids) in &by_group {
        assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "{group}: {ids:?}"
        );
    }
    receipt.acknowledge().unwrap();
}

#[test]
fn a_queue_that_does_not_exist_is_named() {
    let Some(endpoint) = server() else { return };
    let error = SqsSource
        .read_held(
            &properties(&endpoint, "etl-no-such-queue"),
            &mut Vec::new(),
            &Context::default(),
        )
        .err()
        .expect("fails")
        .to_string();
    assert!(
        error.starts_with("queue 'etl-no-such-queue': SQS GetQueueUrl"),
        "{error}"
    );
    assert!(error.contains("QueueDoesNotExist"), "{error}");
}
