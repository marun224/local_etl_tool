//! Every behaviour against a local fixture server: no network, no container.

use super::*;
use crate::fixture::*;
use serde_json::json;

/// Read everything the source yields.
fn read(properties: JsonValue) -> Result<(Vec<Record>, Summary), ConnectorError> {
    let mut out: Vec<Record> = Vec::new();
    let summary = RestSource.read(&properties, &mut out, &Context::default())?;
    Ok((out, summary))
}

fn rows(n: std::ops::Range<u64>) -> JsonValue {
    JsonValue::Array(n.map(|id| json!({ "id": id })).collect())
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

#[test]
fn a_bare_array_is_read_as_rows() {
    let api = serve(|_, _| ok(json!([{ "id": 1, "name": "a" }, { "id": 2, "name": "b" }])));

    let (rows, summary) = read(json!({ "url": api.url("/items") })).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["name"], "b", "types as the API sent them");
    assert_eq!(summary.records, 2);
    assert!(summary.detail.starts_with("2 record(s) from 1 page(s) of "));

    let seen = api.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].header("accept"), Some("application/json"));
    assert!(seen[0].header("user-agent").unwrap().starts_with("etl/"));
}

#[test]
fn records_points_into_the_response() {
    let api = serve(|_, _| ok(json!({ "meta": { "total": 1 }, "data": [{ "id": 7 }] })));

    let (rows, _) = read(json!({ "url": api.url("/x"), "records": "/data" })).unwrap();
    assert_eq!(rows, vec![json!({ "id": 7 }).as_object().unwrap().clone()]);
}

#[test]
fn page_pagination_counts_up_until_an_empty_page() {
    let api = serve(|index, _| match index {
        0 => ok(rows(0..2)),
        1 => ok(rows(2..4)),
        _ => ok(json!([])),
    });

    let (rows, summary) = read(json!({
        "url": api.url("/items"), "pagination": "page"
    }))
    .unwrap();

    assert_eq!(rows.len(), 4);
    assert_eq!(
        summary.detail.split(" of ").next(),
        Some("4 record(s) from 3 page(s)")
    );

    let pages: Vec<String> = api
        .seen()
        .iter()
        .map(|s| s.query("page").unwrap())
        .collect();
    assert_eq!(pages, ["1", "2", "3"]);
}

#[test]
fn a_page_size_sends_the_size_and_stops_on_a_short_page_without_asking_again() {
    let api = serve(|index, _| match index {
        0 => ok(rows(0..3)),
        1 => ok(rows(3..5)),
        _ => panic!("a short page was the last one; there should be no third request"),
    });

    let (rows, _) = read(json!({
        "url": api.url("/items"), "pagination": "page",
        "page_start": 0, "page_size": 3, "size_param": "per_page"
    }))
    .unwrap();

    assert_eq!(rows.len(), 5);
    let seen = api.seen();
    assert_eq!(
        seen[0].query("page").as_deref(),
        Some("0"),
        "page_start honoured"
    );
    assert_eq!(seen[1].query("page").as_deref(), Some("1"));
    assert_eq!(seen[0].query("per_page").as_deref(), Some("3"));
}

#[test]
fn offset_pagination_moves_by_what_arrived() {
    let api = serve(|index, _| match index {
        0 => ok(rows(0..2)),
        1 => ok(rows(2..4)),
        _ => ok(rows(4..5)),
    });

    let (rows, _) = read(json!({
        "url": api.url("/items"), "pagination": "offset", "page_size": 2
    }))
    .unwrap();

    assert_eq!(rows.len(), 5);
    let offsets: Vec<String> = api
        .seen()
        .iter()
        .map(|s| s.query("offset").unwrap())
        .collect();
    assert_eq!(offsets, ["0", "2", "4"]);
    assert_eq!(
        api.seen()[0].query("limit").as_deref(),
        Some("2"),
        "limit is the default size_param"
    );
}

#[test]
fn cursor_pagination_follows_the_cursor_until_there_is_none() {
    let api = serve(|index, _| match index {
        0 => ok(json!({ "data": rows(0..2), "next": "c1" })),
        1 => ok(json!({ "data": rows(2..3), "next": "c2" })),
        _ => ok(json!({ "data": rows(3..4), "next": null })),
    });

    let (rows, _) = read(json!({
        "url": api.url("/items"), "records": "/data",
        "pagination": "cursor", "cursor_path": "/next", "cursor_param": "after"
    }))
    .unwrap();

    assert_eq!(rows.len(), 4);
    let cursors: Vec<Option<String>> = api.seen().iter().map(|s| s.query("after")).collect();
    assert_eq!(cursors, [None, Some("c1".into()), Some("c2".into())]);
}

#[test]
fn a_cursor_that_repeats_is_refused_rather_than_followed_forever() {
    let api = serve(|_, _| ok(json!({ "data": rows(0..1), "next": "same" })));

    let error = read(json!({
        "url": api.url("/items"), "records": "/data",
        "pagination": "cursor", "cursor_path": "/next"
    }))
    .unwrap_err()
    .to_string();

    assert!(error.contains("'same' twice"), "{error}");
    assert_eq!(api.seen().len(), 2);
}

#[test]
fn link_pagination_follows_rel_next_absolute_or_by_path() {
    let api = serve(|index, seen| match index {
        0 => ok(rows(0..1)).with("Link", r#"</items?page=2>; rel="next""#),
        1 => {
            // An absolute URL, and a second rel in the same header.
            let host = seen.header("host").unwrap().to_string();
            ok(rows(1..2)).with(
                "Link",
                &format!(
                    r#"<http://{host}/items?page=3>; rel="next", </items?page=9>; rel="last""#
                ),
            )
        }
        _ => ok(rows(2..3)).with("Link", r#"</items?page=1>; rel="first""#),
    });

    let (rows, _) = read(json!({ "url": api.url("/items"), "pagination": "link" })).unwrap();

    assert_eq!(rows.len(), 3);
    let urls: Vec<String> = api.seen().iter().map(|s| s.url.clone()).collect();
    assert_eq!(urls, ["/items", "/items?page=2", "/items?page=3"]);
}

#[test]
fn reaching_max_pages_is_an_error_not_a_quiet_stop() {
    let api = serve(|_, _| ok(rows(0..5)));

    let error = read(json!({
        "url": api.url("/items"), "pagination": "page", "max_pages": 3
    }))
    .unwrap_err()
    .to_string();

    assert!(error.contains("reached max_pages (3)"), "{error}");
    assert_eq!(api.seen().len(), 3, "exactly the cap, then stop");
}

#[test]
fn query_and_header_maps_are_sent_on_every_page() {
    let api = serve(|index, _| {
        if index == 0 {
            ok(rows(0..1))
        } else {
            ok(json!([]))
        }
    });

    read(json!({
        "url": api.url("/items"), "pagination": "page",
        "query": { "status": "open" }, "headers": { "X-Tenant": "acme" }
    }))
    .unwrap();

    for seen in api.seen() {
        assert_eq!(seen.query("status").as_deref(), Some("open"));
        assert_eq!(seen.header("x-tenant"), Some("acme"));
    }
}

#[test]
fn a_search_can_be_a_post_with_a_body() {
    let api = serve(|_, _| ok(json!([])));

    read(json!({
        "url": api.url("/search"), "method": "POST", "body": "{\"q\":\"x\"}"
    }))
    .unwrap();

    let seen = &api.seen()[0];
    assert_eq!(seen.method, "POST");
    assert_eq!(seen.body, r#"{"q":"x"}"#);
    assert_eq!(seen.header("content-type"), Some("application/json"));
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

#[test]
fn each_kind_of_auth_sends_what_it_should() {
    let api = serve(|_, _| ok(json!([])));

    read(json!({ "url": api.url("/a"), "auth": "bearer", "token": "t0k" })).unwrap();
    read(json!({ "url": api.url("/b"), "auth": "basic", "username": "user", "password": "pass" }))
        .unwrap();
    read(json!({ "url": api.url("/c"), "auth": "header", "token": "k3y", "auth_header": "X-Key" }))
        .unwrap();
    read(json!({ "url": api.url("/d"), "auth": "header", "token": "k3y" })).unwrap();
    read(json!({ "url": api.url("/e") })).unwrap();

    let seen = api.seen();
    assert_eq!(seen[0].header("authorization"), Some("Bearer t0k"));
    assert_eq!(seen[1].header("authorization"), Some("Basic dXNlcjpwYXNz"));
    assert_eq!(seen[2].header("x-key"), Some("k3y"));
    assert_eq!(
        seen[3].header("x-api-key"),
        Some("k3y"),
        "the default header name"
    );
    assert_eq!(seen[4].header("authorization"), None);
}

// ---------------------------------------------------------------------------
// Failure, and what is retried
// ---------------------------------------------------------------------------

#[test]
fn a_429_is_retried_after_the_wait_the_server_asked_for() {
    let api = serve(|index, _| match index {
        0 => status(429, "slow down").with("Retry-After", "0"),
        _ => ok(rows(0..2)),
    });

    let (rows, _) = read(json!({ "url": api.url("/x"), "retry_backoff_ms": 1 })).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(api.seen().len(), 2);
}

#[test]
fn a_5xx_is_retried_with_backoff() {
    let api = serve(|index, _| match index {
        0 => status(500, "oops"),
        1 => status(503, "busy"),
        _ => ok(rows(0..1)),
    });

    let started = Instant::now();
    let (rows, _) = read(json!({ "url": api.url("/x"), "retry_backoff_ms": 20 })).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(api.seen().len(), 3);
    assert!(
        started.elapsed() >= Duration::from_millis(60),
        "20 ms then 40 ms: the wait doubles, took {:?}",
        started.elapsed()
    );
}

#[test]
fn a_4xx_other_than_429_is_not_retried_and_says_why() {
    let api = serve(|_, _| status(401, r#"{"error":"bad token"}"#));

    let error = read(json!({ "url": api.url("/x"), "retries": 5, "retry_backoff_ms": 1 }))
        .unwrap_err()
        .to_string();

    assert!(error.contains("HTTP 401"), "{error}");
    assert!(
        error.contains("bad token"),
        "the body is the explanation: {error}"
    );
    assert_eq!(api.seen().len(), 1, "a 401 retried is still a 401");
}

#[test]
fn retries_run_out_and_say_how_many_attempts_there_were() {
    let api = serve(|_, _| status(503, "down"));

    let error = read(json!({ "url": api.url("/x"), "retries": 2, "retry_backoff_ms": 1 }))
        .unwrap_err()
        .to_string();

    assert!(error.contains("HTTP 503"), "{error}");
    assert!(error.contains("after 3 attempt(s)"), "{error}");
    assert_eq!(api.seen().len(), 3);
}

#[test]
fn a_retry_after_too_long_to_honour_is_an_error_not_a_hang() {
    let api = serve(|_, _| status(429, "later").with("Retry-After", "86400"));

    let error = read(json!({ "url": api.url("/x") }))
        .unwrap_err()
        .to_string();

    assert!(error.contains("86400s"), "{error}");
    assert_eq!(api.seen().len(), 1);
}

#[test]
fn a_server_that_is_not_there_is_retried_then_reported() {
    // A port that was free a moment ago, and is closed now.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();

    let error = read(json!({
        "url": format!("http://127.0.0.1:{port}/x"), "retries": 1, "retry_backoff_ms": 1
    }))
    .unwrap_err()
    .to_string();

    assert!(error.contains("could not reach"), "{error}");
    assert!(error.contains("after 2 attempt(s)"), "{error}");
}

#[test]
fn min_interval_spaces_requests_out() {
    let api = serve(|index, _| {
        if index < 2 {
            ok(rows(0..1))
        } else {
            ok(json!([]))
        }
    });

    let started = Instant::now();
    read(json!({ "url": api.url("/x"), "pagination": "page", "min_interval_ms": 40 })).unwrap();

    assert_eq!(api.seen().len(), 3);
    assert!(
        started.elapsed() >= Duration::from_millis(80),
        "three requests, two gaps of 40 ms, took {:?}",
        started.elapsed()
    );
}

#[test]
fn a_response_that_is_not_what_was_promised_is_named() {
    let api = serve(|index, _| match index {
        0 => ok(json!({ "data": { "not": "a list" } })),
        1 => ok(json!([1, 2])),
        _ => status(200, "<html>maintenance</html>"),
    });

    let not_array = read(json!({ "url": api.url("/a"), "records": "/data" }))
        .unwrap_err()
        .to_string();
    assert!(
        not_array.contains("points at an object, not an array"),
        "{not_array}"
    );

    let not_objects = read(json!({ "url": api.url("/b") }))
        .unwrap_err()
        .to_string();
    assert!(
        not_objects.contains("row 1 on page 1 is a number"),
        "{not_objects}"
    );

    let not_json = read(json!({ "url": api.url("/c") }))
        .unwrap_err()
        .to_string();
    assert!(not_json.contains("page 1 is not JSON"), "{not_json}");
    assert!(
        not_json.contains("maintenance"),
        "and shows what came back: {not_json}"
    );
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

fn write(properties: JsonValue, rows: Vec<JsonValue>) -> Result<Summary, ConnectorError> {
    RestSink.write(&properties, &mut records(rows), &Context::default())
}

#[test]
fn rows_are_sent_as_arrays_of_batch_size() {
    let api = serve(|_, _| status(201, ""));

    let summary = write(
        json!({ "url": api.url("/in"), "batch_size": 2 }),
        (0..5).map(|id| json!({ "id": id })).collect(),
    )
    .unwrap();

    assert_eq!(summary.records, 5);
    assert!(summary
        .detail
        .starts_with("5 record(s) in 3 request(s) to "));

    let bodies: Vec<JsonValue> = api
        .seen()
        .iter()
        .map(|s| serde_json::from_str(&s.body).unwrap())
        .collect();
    assert_eq!(
        bodies,
        [
            json!([{ "id": 0 }, { "id": 1 }]),
            json!([{ "id": 2 }, { "id": 3 }]),
            json!([{ "id": 4 }]),
        ],
        "the last batch of one is still an array: the shape never depends on the count"
    );

    let first = &api.seen()[0];
    assert_eq!(first.method, "POST");
    assert_eq!(first.header("content-type"), Some("application/json"));
}

#[test]
fn a_batch_size_of_one_sends_bare_objects_and_wrap_nests_them() {
    let api = serve(|_, _| status(200, ""));

    write(
        json!({ "url": api.url("/one"), "batch_size": 1, "method": "PUT" }),
        vec![json!({ "a": 1 }), json!({ "a": 2 })],
    )
    .unwrap();
    write(
        json!({ "url": api.url("/wrapped"), "batch_size": 10, "wrap": "records" }),
        vec![json!({ "a": 1 })],
    )
    .unwrap();

    let seen = api.seen();
    assert_eq!(seen[0].method, "PUT");
    assert_eq!(seen[0].body, r#"{"a":1}"#);
    assert_eq!(seen[1].body, r#"{"a":2}"#);
    assert_eq!(seen[2].path(), "/wrapped");
    assert_eq!(seen[2].body, r#"{"records":[{"a":1}]}"#);
}

#[test]
fn a_batch_that_fails_says_how_much_was_already_delivered() {
    let api = serve(|index, _| {
        if index == 2 {
            status(500, "boom")
        } else {
            status(200, "")
        }
    });

    let error = write(
        json!({ "url": api.url("/in"), "batch_size": 2, "retries": 0 }),
        (0..7).map(|id| json!({ "id": id })).collect(),
    )
    .unwrap_err()
    .to_string();

    assert!(
        error.starts_with("batch 3 failed after 2 batch(es) (4 record(s)) were delivered"),
        "{error}"
    );
    assert_eq!(api.seen().len(), 3, "nothing after the failed batch");
}

#[test]
fn no_rows_sends_nothing() {
    let api = serve(|_, _| status(200, ""));

    let summary = write(json!({ "url": api.url("/in") }), vec![]).unwrap();

    assert_eq!(summary.records, 0);
    assert!(
        summary.detail.starts_with("0 records; nothing sent"),
        "{}",
        summary.detail
    );
    assert!(api.seen().is_empty());
}

// ---------------------------------------------------------------------------
// What check() refuses before anything runs
// ---------------------------------------------------------------------------

#[test]
fn a_configuration_that_cannot_work_is_refused_by_property() {
    let refused = |source: bool, properties: JsonValue| -> String {
        let outcome = if source {
            RestSource.check(&properties)
        } else {
            RestSink.check(&properties)
        };
        outcome.expect_err("should be refused").to_string()
    };

    assert!(refused(true, json!({ "url": "ftp://x" })).starts_with("property 'url'"));
    assert!(
        refused(true, json!({ "url": "https://x", "pagination": "cursor" }))
            .starts_with("property 'cursor_path'")
    );
    assert!(
        refused(true, json!({ "url": "https://x", "auth": "bearer" }))
            .starts_with("property 'token': is required for bearer auth")
    );
    assert!(
        refused(true, json!({ "url": "https://x", "auth": "basic" }))
            .starts_with("property 'username'")
    );
    assert!(
        refused(true, json!({ "url": "https://x", "body": "{not json" }))
            .starts_with("property 'body'")
    );
    assert!(
        refused(false, json!({ "url": "https://x", "batch_size": 0 }))
            .starts_with("property 'batch_size': must be at least 1")
    );
    // A GET has no body, so a sink sending one would deliver nothing and
    // report success. The first draft did exactly that.
    assert!(
        refused(false, json!({ "url": "https://x", "method": "GET" }))
            .starts_with("property 'method': GET cannot send rows")
    );

    assert!(RestSource
        .check(&json!({ "url": "https://x", "pagination": "cursor", "cursor_path": "/n" }))
        .is_ok());
}

// ---------------------------------------------------------------------------
// The small pieces
// ---------------------------------------------------------------------------

#[test]
fn base64_matches_rfc_4648() {
    for (input, expected) in [
        ("", ""),
        ("f", "Zg=="),
        ("fo", "Zm8="),
        ("foo", "Zm9v"),
        ("foob", "Zm9vYg=="),
        ("fooba", "Zm9vYmE="),
        ("foobar", "Zm9vYmFy"),
    ] {
        assert_eq!(base64(input), expected, "{input}");
    }
}

#[test]
fn a_link_header_is_read_for_rel_next_only() {
    assert_eq!(
        next_link(r#"<https://a/x?p=2>; rel="next", <https://a/x?p=9>; rel="last""#).as_deref(),
        Some("https://a/x?p=2")
    );
    assert_eq!(
        next_link(r#"<https://a/x?p=9>; rel="last", <https://a/x?p=3>; rel=next"#).as_deref(),
        Some("https://a/x?p=3"),
        "unquoted, and not first"
    );
    assert_eq!(
        next_link(r#"<https://a/x?p=4>; rel="prev next""#).as_deref(),
        Some("https://a/x?p=4"),
        "one of several rels"
    );
    assert_eq!(next_link(r#"<https://a/x?p=1>; rel="first""#), None);
    assert_eq!(next_link(""), None);
}

#[test]
fn a_link_target_is_resolved_against_where_it_came_from() {
    assert_eq!(
        resolve_link("https://api.x.com/v1/items?page=1", "/v1/items?page=2"),
        "https://api.x.com/v1/items?page=2"
    );
    assert_eq!(
        resolve_link("https://api.x.com/v1/items", "https://other.com/p"),
        "https://other.com/p"
    );
}
