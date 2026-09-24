//! What decides whether a message is read once, again, or lost: the receipt
//! settling on every path, a connection lost while holding, refusals, and
//! publisher confirms. Settings first, without a server; then against RabbitMQ
//! when `ETL_TEST_RABBITMQ` names it (`scripts/test-services.ps1` starts it).
//! Without it those skip.

use super::*;
use crate::fixture;
use lapin::options::{
    ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions, QueueDeclareOptions,
    QueueDeleteOptions,
};
use lapin::types::{FieldArray, LongString, ShortString};
use lapin::ExchangeKind;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Without a server
// ---------------------------------------------------------------------------

#[test]
fn a_setting_that_cannot_work_is_refused_by_property() {
    let source =
        |properties: JsonValue| RabbitmqSource.check(&properties).map_err(|e| e.to_string());
    let sink = |properties: JsonValue| RabbitmqSink.check(&properties).map_err(|e| e.to_string());
    let refused = |result: Result<(), String>, property: &str| {
        let error = result.unwrap_err();
        assert!(
            error.starts_with(&format!("property '{property}'")),
            "{property}: {error}"
        );
    };

    refused(source(json!({ "url": "amqp://h" })), "queue");
    refused(source(json!({ "queue": "q" })), "url");
    refused(source(json!({ "queue": "q", "url": "http://h" })), "url");
    refused(
        source(json!({ "queue": "q", "url": "amqp://h:notaport" })),
        "url",
    );
    refused(
        source(json!({ "queue": "q", "url": "amqp://h", "ca_cert": "ca.pem" })),
        "ca_cert",
    );
    refused(
        source(json!({ "queue": "q", "url": "amqp://h", "value_format": "xml" })),
        "value_format",
    );
    source(json!({ "queue": "q", "url": "amqps://h", "ca_cert": "ca.pem" })).expect("fine");

    refused(sink(json!({ "url": "amqp://h" })), "routing_key");
    refused(
        sink(json!({ "url": "amqp://h", "routing_key": "a", "routing_key_column": "b" })),
        "routing_key",
    );
    sink(json!({ "url": "amqp://h", "routing_key": "orders" })).expect("the default exchange");
    sink(json!({ "url": "amqp://h", "exchange": "fanout" })).expect("no key needed");
}

#[test]
fn the_properties_fill_in_the_url_and_the_password_is_never_shown() {
    let server = Server::from(&json!({
        "url": "amqp://someone:in-the-url@broker.local/sales",
        "username": "etl", "password": "s3cret", "vhost": "orders",
    }))
    .unwrap();
    assert_eq!(server.uri.authority.userinfo.username, "etl");
    assert_eq!(server.uri.authority.userinfo.password, "s3cret");
    assert_eq!(server.place(), "broker.local:5672, vhost 'orders'");
    let shown = format!("{server:?}");
    assert!(
        !shown.contains("s3cret") && !shown.contains("in-the-url"),
        "{shown}"
    );

    let plain = Server::from(&json!({ "url": "amqps://broker.local" })).unwrap();
    assert_eq!(plain.place(), "broker.local:5671, vhost '/'");

    let error = Server::from(&json!({ "url": "amqp://etl:pass@word@h:x" }))
        .unwrap_err()
        .to_string();
    assert!(!error.contains("pass@word"), "{error}");
}

#[test]
fn headers_become_json() {
    let mut nested = BTreeMap::new();
    nested.insert(ShortString::from("depth"), AMQPValue::LongInt(2));
    let mut table = BTreeMap::new();
    table.insert(
        ShortString::from("x-delivery-count"),
        AMQPValue::LongLongInt(3),
    );
    table.insert(
        ShortString::from("source"),
        AMQPValue::LongString(LongString::from("web")),
    );
    table.insert(ShortString::from("flag"), AMQPValue::Boolean(true));
    table.insert(
        ShortString::from("tags"),
        AMQPValue::FieldArray(FieldArray::from(vec![
            AMQPValue::ShortString("a".into()),
            AMQPValue::Void,
        ])),
    );
    table.insert(
        ShortString::from("inner"),
        AMQPValue::FieldTable(FieldTable::from(nested)),
    );
    assert_eq!(
        table_json(&FieldTable::from(table)),
        json!({
            "x-delivery-count": 3, "source": "web", "flag": true,
            "tags": ["a", null], "inner": { "depth": 2 },
        })
    );
}

// ---------------------------------------------------------------------------
// Against RabbitMQ
// ---------------------------------------------------------------------------

fn server() -> Option<String> {
    match std::env::var("ETL_TEST_RABBITMQ") {
        Ok(url) if !url.trim().is_empty() => Some(url.trim().to_string()),
        _ => {
            eprintln!("skipping: ETL_TEST_RABBITMQ is not set; see scripts/test-services.ps1");
            None
        }
    }
}

fn with(url: &str, extra: JsonValue) -> JsonValue {
    let mut all = json!({ "url": url, "timeout_ms": 10_000 });
    for (key, value) in extra.as_object().unwrap() {
        all[key] = value.clone();
    }
    all
}

fn link(url: &str) -> Link {
    Link::open(
        &Server::from(&with(url, json!({}))).unwrap(),
        &Context::default(),
    )
    .unwrap()
}

fn unique(test: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("etl-{test}-{}-{nanos}", std::process::id())
}

/// A queue for one test, deleted when the test ends, pass or fail.
struct TestQueue {
    url: String,
    name: String,
}

impl TestQueue {
    fn new(url: &str, test: &str, quorum: bool) -> Self {
        let name = unique(test);
        let mut arguments = BTreeMap::new();
        if quorum {
            arguments.insert(
                ShortString::from("x-queue-type"),
                AMQPValue::LongString(LongString::from("quorum")),
            );
        }
        let link = link(url);
        link.run(
            "queue.declare",
            link.channel.queue_declare(
                name.as_str().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::from(arguments),
            ),
        )
        .unwrap();
        link.close();
        TestQueue {
            url: url.to_string(),
            name,
        }
    }

    /// Messages waiting, as a passive declare counts them: not those held.
    fn waiting(&self) -> u32 {
        let link = link(&self.url);
        let queue = link
            .run(
                "queue.declare",
                link.channel.queue_declare(
                    self.name.as_str().into(),
                    QueueDeclareOptions {
                        passive: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                ),
            )
            .unwrap();
        link.close();
        queue.message_count()
    }
}

impl Drop for TestQueue {
    fn drop(&mut self) {
        if let Ok(link) = Link::open(
            &Server::from(&with(&self.url, json!({}))).unwrap(),
            &Context::default(),
        ) {
            let _ = link.run(
                "queue.delete",
                link.channel
                    .queue_delete(self.name.as_str().into(), QueueDeleteOptions::default()),
            );
            link.close();
        }
    }
}

fn orders(count: u64) -> Vec<JsonValue> {
    (1..=count)
        .map(|id| json!({ "id": id, "customer": format!("C{}", id % 3) }))
        .collect()
}

/// Publish `rows` through the sink to the default exchange, into `queue`.
fn put(url: &str, queue: &TestQueue, rows: Vec<JsonValue>) -> Summary {
    RabbitmqSink
        .write(
            &with(url, json!({ "routing_key": queue.name })),
            &mut fixture::records(rows),
            &Context::default(),
        )
        .expect("publishes")
}

fn take(
    url: &str,
    queue: &TestQueue,
    extra: JsonValue,
) -> (Vec<Record>, Summary, Box<dyn Receipt>) {
    let mut properties = with(url, extra);
    properties["queue"] = json!(queue.name);
    let mut rows: Vec<Record> = Vec::new();
    let (summary, receipt) = RabbitmqSource
        .read_held(&properties, &mut rows, &Context::default())
        .expect("receives");
    (rows, summary, receipt.expect("a receipt"))
}

fn ids(rows: &[Record]) -> Vec<u64> {
    rows.iter().map(|r| r["id"].as_u64().unwrap()).collect()
}

#[test]
fn an_acknowledged_run_empties_the_queue() {
    let Some(url) = server() else { return };
    let queue = TestQueue::new(&url, "ack", false);
    let sent = put(&url, &queue, orders(25));
    assert!(
        sent.detail.starts_with(&format!(
            "25 message(s) to the default exchange, routing key '{}'",
            queue.name
        )),
        "{}",
        sent.detail
    );

    let (rows, summary, receipt) = take(&url, &queue, json!({}));
    assert_eq!(ids(&rows), (1..=25).collect::<Vec<_>>(), "in order");
    assert!(
        summary.detail.ends_with("the queue answered empty"),
        "{}",
        summary.detail
    );
    assert!(!summary.detail.contains("etl-secret"), "{}", summary.detail);
    let first = &rows[0];
    assert_eq!(first["_queue"], json!(queue.name));
    assert_eq!(first["_exchange"], JsonValue::Null, "the default exchange");
    assert_eq!(first["_routing_key"], json!(queue.name));
    assert_eq!(first["_redelivered"], false);
    assert_eq!(queue.waiting(), 0, "held: not waiting");
    assert_eq!(
        receipt.acknowledge().unwrap(),
        format!("25 message(s) acknowledged on queue '{}'", queue.name)
    );
    assert_eq!(queue.waiting(), 0, "acknowledged: gone");
}

#[test]
fn a_released_or_dropped_run_is_received_again_marked_redelivered() {
    let Some(url) = server() else { return };
    let queue = TestQueue::new(&url, "release", false);
    put(&url, &queue, orders(12));

    let (first, _, receipt) = take(&url, &queue, json!({}));
    assert_eq!(first.len(), 12);
    assert_eq!(
        receipt.release().unwrap(),
        format!("12 message(s) released back to queue '{}'", queue.name)
    );
    assert_eq!(queue.waiting(), 12, "back at once");

    let (second, _, receipt) = take(&url, &queue, json!({}));
    assert_eq!(ids(&second), (1..=12).collect::<Vec<_>>());
    assert!(second.iter().all(|row| row["_redelivered"] == true));
    drop(receipt);
    assert_eq!(queue.waiting(), 12, "dropping released them too");

    let (_, _, receipt) = take(&url, &queue, json!({}));
    receipt.acknowledge().unwrap();
    assert_eq!(queue.waiting(), 0);
}

#[test]
fn a_connection_lost_while_holding_gives_everything_back() {
    let Some(url) = server() else { return };
    let queue = TestQueue::new(&url, "lost", false);
    put(&url, &queue, orders(6));

    let mut properties = with(&url, json!({ "queue": queue.name }));
    properties["timeout_ms"] = json!(2_000);
    let settings = SourceSettings::from(&properties).unwrap();
    let (_, mut receipt) = receive(&settings, &mut Vec::new(), &Context::default()).unwrap();
    // The broker's side goes away mid-run: the connection closes under the
    // receipt, as a restart or a consumer_timeout would close it.
    let held = receipt.link.as_ref().unwrap();
    held.run(
        "connection.close",
        held.connection.close(320, "gone".into()),
    )
    .unwrap();
    assert_eq!(queue.waiting(), 6, "the broker requeued them");

    let error = receipt.settle(true).unwrap_err().to_string();
    assert!(
        error.starts_with(&format!(
            "6 message(s) from queue '{}' could not be acknowledged, so the broker will deliver \
             them again",
            queue.name
        )),
        "{error}"
    );
    let (again, _, receipt) = take(&url, &queue, json!({}));
    assert_eq!(again.len(), 6);
    assert!(again.iter().all(|row| row["_redelivered"] == true));
    receipt.acknowledge().unwrap();
}

#[test]
fn max_records_leaves_the_rest_for_the_next_run() {
    let Some(url) = server() else { return };
    let queue = TestQueue::new(&url, "cap", false);
    put(&url, &queue, orders(25));

    let (first, summary, receipt) = take(&url, &queue, json!({ "max_records": 10 }));
    assert_eq!(ids(&first), (1..=10).collect::<Vec<_>>());
    assert!(
        summary.detail.contains("stopped at max_records (10)"),
        "{}",
        summary.detail
    );
    receipt.acknowledge().unwrap();
    assert_eq!(queue.waiting(), 15);

    let (rest, _, receipt) = take(&url, &queue, json!({}));
    assert_eq!(
        ids(&rest),
        (11..=25).collect::<Vec<_>>(),
        "each once, none lost"
    );
    receipt.acknowledge().unwrap();
}

#[test]
fn a_quorum_queue_counts_its_redeliveries() {
    let Some(url) = server() else { return };
    let queue = TestQueue::new(&url, "quorum", true);
    put(&url, &queue, orders(3));

    let (first, _, receipt) = take(&url, &queue, json!({}));
    assert_eq!(first.len(), 3);
    receipt.release().unwrap();
    let (second, _, receipt) = take(&url, &queue, json!({}));
    assert_eq!(ids(&second), [1, 2, 3]);
    for row in &second {
        assert_eq!(row["_redelivered"], true);
        // RabbitMQ 4 counts every hand-out after the first here; its
        // x-delivery-count now counts only deliveries that failed.
        assert_eq!(row["_headers"]["x-acquired-count"], 1, "{row:?}");
    }
    receipt.acknowledge().unwrap();
    assert_eq!(queue.waiting(), 0);
}

#[test]
fn what_cannot_be_reached_is_named() {
    let Some(url) = server() else { return };
    let read = |properties: JsonValue| {
        RabbitmqSource
            .read_held(&properties, &mut Vec::new(), &Context::default())
            .err()
            .expect("fails")
            .to_string()
    };

    let missing = read(with(&url, json!({ "queue": "etl-no-such-queue" })));
    assert!(
        missing.starts_with("queue 'etl-no-such-queue': RabbitMQ basic.get:"),
        "{missing}"
    );
    assert!(missing.contains("NOT_FOUND"), "{missing}");

    let refused = read(with(
        &url,
        json!({ "queue": "q", "password": "not-the-password" }),
    ));
    assert!(refused.contains("ACCESS_REFUSED"), "{refused}");
    assert!(!refused.contains("not-the-password"), "{refused}");

    let started = Instant::now();
    let vhost = read(with(
        &url,
        json!({ "queue": "q", "vhost": "etl-no-such-vhost", "timeout_ms": 1_500 }),
    ));
    assert!(
        vhost.ends_with(
            "vhost 'etl-no-such-vhost' gave no answer within 1500 ms while connecting. It gives \
             none at all when the vhost does not exist, so check the vhost first"
        ),
        "{vhost}"
    );
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn tls_trusts_the_ca_it_is_given_and_nothing_else() {
    let (Ok(url), Ok(ca)) = (
        std::env::var("ETL_TEST_RABBITMQ_TLS"),
        std::env::var("ETL_TEST_KAFKA_CA"),
    ) else {
        eprintln!("skipping: ETL_TEST_RABBITMQ_TLS is not set; see scripts/test-services.ps1");
        return;
    };
    let plain = server().expect("the plain listener is up beside the TLS one");
    let queue = TestQueue::new(&plain, "tls", false);
    let tls = |extra: JsonValue| {
        let mut properties = with(&url, extra);
        properties["queue"] = json!(queue.name);
        properties
    };

    RabbitmqSink
        .write(
            &tls(json!({ "ca_cert": ca, "routing_key": queue.name })),
            &mut fixture::records(orders(4)),
            &Context::default(),
        )
        .expect("publishes over TLS");
    let mut rows: Vec<Record> = Vec::new();
    let (_, receipt) = RabbitmqSource
        .read_held(
            &tls(json!({ "ca_cert": ca })),
            &mut rows,
            &Context::default(),
        )
        .expect("reads over TLS");
    assert_eq!(ids(&rows), [1, 2, 3, 4]);
    receipt.unwrap().acknowledge().unwrap();

    let error = RabbitmqSource
        .read_held(&tls(json!({})), &mut Vec::new(), &Context::default())
        .err()
        .expect("the public roots do not know the test CA")
        .to_string();
    assert!(error.contains("UnknownIssuer"), "{error}");
}

// ----- the sink -----

/// A topic exchange with a queue per customer, all deleted afterwards.
struct TestExchange {
    url: String,
    name: String,
    queues: Vec<TestQueue>,
}

impl TestExchange {
    fn new(url: &str, test: &str, customers: &[&str]) -> Self {
        let name = unique(test);
        let link = link(url);
        link.run(
            "exchange.declare",
            link.channel.exchange_declare(
                name.as_str().into(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            ),
        )
        .unwrap();
        let mut queues = Vec::new();
        for customer in customers {
            let queue = TestQueue::new(url, &format!("{test}-{customer}"), false);
            link.run(
                "queue.bind",
                link.channel.queue_bind(
                    queue.name.as_str().into(),
                    name.as_str().into(),
                    format!("orders.{customer}").as_str().into(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                ),
            )
            .unwrap();
            queues.push(queue);
        }
        link.close();
        TestExchange {
            url: url.to_string(),
            name,
            queues,
        }
    }
}

impl Drop for TestExchange {
    fn drop(&mut self) {
        let link = link(&self.url);
        let _ = link.run(
            "exchange.delete",
            link.channel
                .exchange_delete(self.name.as_str().into(), ExchangeDeleteOptions::default()),
        );
        link.close();
    }
}

fn routed(orders: Vec<JsonValue>) -> Vec<JsonValue> {
    orders
        .into_iter()
        .map(|mut row| {
            row["key"] = json!(format!("orders.{}", row["customer"].as_str().unwrap()));
            row
        })
        .collect()
}

#[test]
fn rows_are_routed_by_their_own_key_and_kept() {
    let Some(url) = server() else { return };
    let exchange = TestExchange::new(&url, "route", &["C0", "C1", "C2"]);
    let summary = RabbitmqSink
        .write(
            &with(
                &url,
                json!({ "exchange": exchange.name, "routing_key_column": "key" }),
            ),
            &mut fixture::records(routed(orders(9))),
            &Context::default(),
        )
        .unwrap();
    assert!(
        summary.detail.starts_with(&format!(
            "9 message(s) to exchange '{}', routing keys from 'key'",
            exchange.name
        )),
        "{}",
        summary.detail
    );
    assert!(
        summary.detail.ends_with("confirmed in 1 batch(es)"),
        "{}",
        summary.detail
    );

    for (customer, queue) in ["C0", "C1", "C2"].iter().zip(&exchange.queues) {
        let (rows, _, receipt) = take(&url, queue, json!({}));
        assert_eq!(rows.len(), 3, "{customer}");
        assert!(rows.iter().all(|row| row["customer"] == *customer));
        assert!(rows
            .iter()
            .all(|row| row["_exchange"] == json!(exchange.name)));
        receipt.acknowledge().unwrap();
    }
}

#[test]
fn a_row_no_queue_receives_fails_saying_what_landed() {
    let Some(url) = server() else { return };
    let exchange = TestExchange::new(&url, "unrouted", &["C1", "C2"]);
    // Order 3 is customer C0, which no queue is bound for.
    let error = RabbitmqSink
        .write(
            &with(
                &url,
                json!({ "exchange": exchange.name, "routing_key_column": "key" }),
            ),
            &mut fixture::records(routed(orders(4))),
            &Context::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with(
            "row 3 reached no queue: RabbitMQ returned it (312 NO_ROUTE) for routing key \
             'orders.C0'"
        ),
        "{error}"
    );
    assert!(error.contains("2 message(s) to exchange"), "{error}");

    let missing = RabbitmqSink
        .write(
            &with(
                &url,
                json!({ "exchange": "etl-no-such-exchange", "routing_key": "k" }),
            ),
            &mut fixture::records(orders(1)),
            &Context::default(),
        )
        .unwrap_err()
        .to_string();
    assert!(missing.contains("NOT_FOUND"), "{missing}");
    assert!(missing.contains("etl-no-such-exchange"), "{missing}");
}
