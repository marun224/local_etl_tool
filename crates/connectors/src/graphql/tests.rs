//! Every behaviour against a local fixture server: no network, no container.

use super::*;
use crate::fixture::*;
use serde_json::json;
use std::time::{Duration, Instant};

/// Read everything the source yields.
fn read(properties: JsonValue) -> Result<(Vec<Record>, Summary), ConnectorError> {
    let mut out: Vec<Record> = Vec::new();
    let summary = GraphqlSource.read(&properties, &mut out, &Context::default())?;
    Ok((out, summary))
}

fn write(properties: JsonValue, rows: Vec<JsonValue>) -> Result<Summary, ConnectorError> {
    GraphqlSink.write(&properties, &mut records(rows), &Context::default())
}

/// What a request carried: its parsed `{query, variables}`.
fn sent(seen: &Seen) -> JsonValue {
    serde_json::from_str(&seen.body).expect("the request body is JSON")
}

fn nodes(ids: std::ops::Range<u64>) -> JsonValue {
    JsonValue::Array(ids.map(|id| json!({ "id": id })).collect())
}

/// A relay page of `ids`, with `next` as the cursor if there is one.
fn relay_page(ids: std::ops::Range<u64>, next: Option<&str>) -> JsonValue {
    json!({ "data": { "orders": {
        "nodes": nodes(ids),
        "pageInfo": { "hasNextPage": next.is_some(), "endCursor": next },
    }}})
}

const RELAY_QUERY: &str = "query ($after: String) {\n  orders(first: 2, after: $after) {\n    \
                           nodes { id }\n    pageInfo { hasNextPage endCursor }\n  }\n}";

fn relay(api: &Fixture) -> JsonValue {
    json!({
        "url": api.url("/graphql"),
        "query": RELAY_QUERY,
        "records": "/data/orders/nodes",
        "pagination": "relay",
    })
}

/// Fast retries, so the tests that wait do not wait long.
fn quick(mut properties: JsonValue) -> JsonValue {
    properties["retry_backoff_ms"] = json!(10);
    properties
}

fn ids(rows: &[Record]) -> Vec<u64> {
    rows.iter().map(|row| row["id"].as_u64().unwrap()).collect()
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

#[test]
fn one_page_is_read_from_where_records_points() {
    let api =
        serve(|_, _| ok(json!({ "data": { "countries": [{ "code": "NZ" }, { "code": "IS" }] } })));

    let (rows, summary) = read(json!({
        "url": api.url("/graphql"),
        "query": "{ countries { code } }",
        "records": "/data/countries",
    }))
    .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["code"], "NZ");
    assert_eq!(summary.records, 2);
    assert!(summary.detail.contains("1 page(s)"), "{}", summary.detail);

    let seen = api.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].header("content-type"), Some("application/json"));
    assert_eq!(sent(&seen[0])["query"], "{ countries { code } }");
    assert_eq!(sent(&seen[0])["variables"], json!({}));
}

#[test]
fn variables_and_auth_are_sent() {
    let api = serve(|_, _| ok(json!({ "data": { "orders": [] } })));

    read(json!({
        "url": api.url("/graphql"),
        "query": "query ($status: String) { orders(status: $status) { id } }",
        "variables": "{\"status\": \"shipped\", \"limit\": 5}",
        "records": "/data/orders",
        "auth": "bearer",
        "token": "s3cret",
        "headers": { "X-Shop": "north" },
    }))
    .unwrap();

    let seen = &api.seen()[0];
    assert_eq!(
        sent(seen)["variables"],
        json!({ "status": "shipped", "limit": 5 })
    );
    assert_eq!(seen.header("authorization"), Some("Bearer s3cret"));
    assert_eq!(seen.header("x-shop"), Some("north"));
}

#[test]
fn relay_follows_end_cursor_until_has_next_page_is_false() {
    let api = serve(|index, _| {
        ok(match index {
            0 => relay_page(1..3, Some("c2")),
            1 => relay_page(3..5, Some("c4")),
            _ => relay_page(5..6, None),
        })
    });

    let (rows, summary) = read(relay(&api)).unwrap();

    assert_eq!(ids(&rows), [1, 2, 3, 4, 5]);
    assert!(summary.detail.contains("3 page(s)"), "{}", summary.detail);

    let after: Vec<JsonValue> = api
        .seen()
        .iter()
        .map(|seen| sent(seen)["variables"]["after"].clone())
        .collect();
    assert_eq!(after, [JsonValue::Null, json!("c2"), json!("c4")]);
}

#[test]
fn page_info_defaults_beside_the_records_and_can_be_pointed_elsewhere() {
    assert_eq!(beside("/data/orders/nodes"), "/data/orders/pageInfo");
    assert_eq!(beside("/data/orders/edges"), "/data/orders/pageInfo");

    // An API that keeps it somewhere else.
    let api = serve(|index, _| {
        ok(json!({ "data": {
            "orders": { "nodes": nodes(index as u64..index as u64 + 1) },
            "meta": { "paging": { "hasNextPage": index == 0, "endCursor": "k1" } },
        }}))
    });
    let mut properties = relay(&api);
    properties["page_info"] = json!("/data/meta/paging");

    let (rows, _) = read(properties).unwrap();
    assert_eq!(ids(&rows), [0, 1]);
}

#[test]
fn relay_without_page_info_in_the_response_says_what_to_ask_for() {
    let api = serve(|_, _| ok(json!({ "data": { "orders": { "nodes": nodes(1..3) } } })));

    let error = read(relay(&api)).unwrap_err().to_string();
    assert!(
        error.contains("no pageInfo at '/data/orders/pageInfo'"),
        "{error}"
    );
    assert!(error.contains("hasNextPage endCursor"), "{error}");
}

#[test]
fn a_cursor_that_repeats_is_refused_rather_than_followed_forever() {
    let api = serve(|_, _| ok(relay_page(1..3, Some("same"))));

    let error = read(relay(&api)).unwrap_err().to_string();
    assert!(error.contains("'same' twice"), "{error}");
    assert_eq!(api.seen().len(), 2);
}

#[test]
fn offset_counts_up_until_a_short_page() {
    let api = serve(|index, _| {
        ok(match index {
            0 => json!({ "data": { "orders": nodes(0..2) } }),
            1 => json!({ "data": { "orders": nodes(2..4) } }),
            _ => json!({ "data": { "orders": nodes(4..5) } }),
        })
    });

    let (rows, _) = read(json!({
        "url": api.url("/graphql"),
        "query": "query ($offset: Int, $limit: Int) { orders(offset: $offset, limit: $limit) { id } }",
        "records": "/data/orders",
        "pagination": "offset",
        "page_size": 2,
    }))
    .unwrap();

    assert_eq!(ids(&rows), [0, 1, 2, 3, 4]);
    let paging: Vec<(JsonValue, JsonValue)> = api
        .seen()
        .iter()
        .map(|seen| {
            let variables = &sent(seen)["variables"];
            (variables["offset"].clone(), variables["limit"].clone())
        })
        .collect();
    assert_eq!(
        paging,
        [
            (json!(0), json!(2)),
            (json!(2), json!(2)),
            (json!(4), json!(2))
        ]
    );
}

#[test]
fn reaching_max_pages_is_an_error_not_a_quiet_stop() {
    let api = serve(|index, _| ok(relay_page(0..1, Some(&format!("c{index}")))));
    let mut properties = relay(&api);
    properties["max_pages"] = json!(3);

    let error = read(properties).unwrap_err().to_string();
    // The whole sentence, which is shared with REST through `http.rs`: a
    // mangled line continuation once put ten spaces into the middle of it.
    assert!(
        error.contains(
            "reached max_pages (3) with more still to read; raise max_pages if the API really \
             has that many pages, or check the pagination settings"
        ),
        "{error}"
    );
    assert_eq!(api.seen().len(), 3);
}

#[test]
fn errors_fail_the_read_quoting_message_code_and_path() {
    let api = serve(|_, _| {
        ok(json!({
            "errors": [
                { "message": "Field 'totl' doesn't exist", "extensions": { "code": "undefinedField" } },
                { "message": "Not allowed", "path": ["orders", 0, "customer"] },
            ]
        }))
    });

    let error = read(quick(relay(&api))).unwrap_err().to_string();
    assert!(
        error.contains("page 1: the API answered with 2 error(s)"),
        "{error}"
    );
    assert!(
        error.contains("\"Field 'totl' doesn't exist\" [undefinedField]"),
        "{error}"
    );
    assert!(
        error.contains("\"Not allowed\" at orders.0.customer"),
        "{error}"
    );
    assert_eq!(api.seen().len(), 1, "a real error is not retried");
}

#[test]
fn partial_data_with_errors_still_fails() {
    // The hole in the data is the point: loading the rest as if whole is
    // exactly the partial load this connector refuses.
    let api = serve(|_, _| {
        ok(json!({
            "data": { "orders": { "nodes": nodes(1..3), "pageInfo": { "hasNextPage": false } } },
            "errors": [{ "message": "customer is private", "path": ["orders", "nodes", 1] }],
        }))
    });

    let error = read(relay(&api)).unwrap_err().to_string();
    assert!(error.contains("customer is private"), "{error}");
}

#[test]
fn many_errors_are_counted_after_the_first_few() {
    let errors: Vec<JsonValue> = (1..=5)
        .map(|n| json!({ "message": format!("problem {n}") }))
        .collect();
    let api = serve(move |_, _| ok(json!({ "errors": errors })));

    let error = read(relay(&api)).unwrap_err().to_string();
    assert!(error.contains("problem 3"), "{error}");
    assert!(!error.contains("problem 4"), "{error}");
    assert!(error.contains("and 2 more"), "{error}");
}

#[test]
fn a_response_with_neither_data_nor_errors_fails() {
    let api = serve(|_, _| ok(json!({ "data": null })));

    let error = read(relay(&api)).unwrap_err().to_string();
    assert!(
        error.contains("page 1: the response has neither data nor errors"),
        "{error}"
    );
}

#[test]
fn a_response_that_is_not_json_is_named() {
    let api = serve(|_, _| Answer {
        status: 200,
        body: "<html>maintenance</html>".into(),
        headers: Vec::new(),
    });

    let error = read(relay(&api)).unwrap_err().to_string();
    assert!(
        error.contains("page 1: the response is not JSON"),
        "{error}"
    );
    assert!(error.contains("maintenance"), "{error}");
}

#[test]
fn throttled_by_extensions_code_is_retried_then_read() {
    let api = serve(|index, _| {
        ok(match index {
            0 => {
                json!({ "errors": [{ "message": "Throttled", "extensions": { "code": "THROTTLED" } }] })
            }
            _ => relay_page(1..3, None),
        })
    });

    let (rows, _) = read(quick(relay(&api))).unwrap();
    assert_eq!(ids(&rows), [1, 2]);
    assert_eq!(api.seen().len(), 2);
}

#[test]
fn rate_limited_by_type_is_retried_and_retry_after_is_honoured() {
    let api = serve(|index, _| match index {
        0 => ok(
            json!({ "errors": [{ "type": "RATE_LIMITED", "message": "API rate limit exceeded" }] }),
        )
        .with("Retry-After", "1"),
        _ => ok(relay_page(1..2, None)),
    });

    let started = Instant::now();
    let (rows, _) = read(quick(relay(&api))).unwrap();

    assert_eq!(rows.len(), 1);
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "waited as long as the server asked, not the 10 ms backoff"
    );
}

#[test]
fn throttling_that_never_stops_runs_out_of_retries() {
    let api = serve(|_, _| {
        ok(json!({ "errors": [{ "message": "Throttled", "extensions": { "code": "THROTTLED" } }] }))
    });
    let mut properties = quick(relay(&api));
    properties["retries"] = json!(2);

    let error = read(properties).unwrap_err().to_string();
    assert!(
        error.contains("page 1: the API is throttling (THROTTLED)"),
        "{error}"
    );
    assert!(error.contains("after 3 attempt(s)"), "{error}");
    assert_eq!(api.seen().len(), 3);
}

#[test]
fn throttling_mixed_with_a_real_error_is_not_retried() {
    let api = serve(|_, _| {
        ok(json!({ "errors": [
            { "message": "Throttled", "extensions": { "code": "THROTTLED" } },
            { "message": "Field 'x' doesn't exist" },
        ]}))
    });

    let error = read(quick(relay(&api))).unwrap_err().to_string();
    assert!(error.contains("2 error(s)"), "{error}");
    assert_eq!(api.seen().len(), 1);
}

#[test]
fn retry_codes_can_be_changed_or_emptied() {
    let throttled = |_: usize, _: &Seen| {
        ok(json!({ "errors": [{ "message": "slow", "extensions": { "code": "SLOW_DOWN" } }] }))
    };

    // Not a default code, so a plain failure...
    let api = serve(throttled);
    read(quick(relay(&api))).unwrap_err();
    assert_eq!(api.seen().len(), 1);

    // ...until named.
    let api = serve(throttled);
    let mut properties = quick(relay(&api));
    properties["retry_codes"] = json!(["SLOW_DOWN"]);
    properties["retries"] = json!(1);
    read(properties).unwrap_err();
    assert_eq!(api.seen().len(), 2);
}

#[test]
fn a_429_is_still_retried_by_the_shared_layer() {
    let api = serve(|index, _| match index {
        0 => status(429, "slow down").with("Retry-After", "0"),
        _ => ok(relay_page(1..2, None)),
    });

    let (rows, _) = read(quick(relay(&api))).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(api.seen().len(), 2);
}

#[test]
fn a_401_is_not_retried() {
    let api = serve(|_, _| status(401, "bad credentials"));

    let error = read(quick(relay(&api))).unwrap_err().to_string();
    assert!(error.contains("HTTP 401"), "{error}");
    assert_eq!(api.seen().len(), 1);
}

#[test]
fn a_row_that_is_not_an_object_is_named() {
    let api = serve(|_, _| ok(json!({ "data": { "codes": ["NZ", "IS"] } })));

    let error = read(json!({
        "url": api.url("/graphql"),
        "query": "{ codes }",
        "records": "/data/codes",
    }))
    .unwrap_err()
    .to_string();
    assert!(error.contains("row 1 on page 1 is a string"), "{error}");
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

const MUTATION: &str = "mutation ($rows: [OrderInput!]!) { addOrders(input: $rows) { count } }";

fn accepted() -> Answer {
    ok(json!({ "data": { "addOrders": { "count": 1 } } }))
}

fn sink(api: &Fixture) -> JsonValue {
    json!({ "url": api.url("/graphql"), "mutation": MUTATION })
}

#[test]
fn rows_go_in_batches_as_a_list_with_the_other_variables() {
    let api = serve(|_, _| accepted());
    let mut properties = sink(&api);
    properties["batch_size"] = json!(2);
    properties["variables"] = json!("{\"shop\": \"north\"}");

    let summary = write(properties, (1..=5).map(|id| json!({ "id": id })).collect()).unwrap();

    assert_eq!(summary.records, 5);
    assert!(
        summary.detail.contains("in 3 request(s)"),
        "{}",
        summary.detail
    );

    let seen = api.seen();
    assert_eq!(seen.len(), 3);
    let first = sent(&seen[0]);
    assert_eq!(first["query"], MUTATION);
    assert_eq!(first["variables"]["shop"], "north");
    assert_eq!(
        first["variables"]["rows"],
        json!([{ "id": 1 }, { "id": 2 }])
    );

    // The last batch holds one row, and is still a list.
    assert_eq!(sent(&seen[2])["variables"]["rows"], json!([{ "id": 5 }]));
}

#[test]
fn a_batch_of_one_is_still_a_list_under_the_named_variable() {
    let api = serve(|_, _| accepted());
    let properties = json!({
        "url": api.url("/graphql"),
        "mutation": "mutation ($input: [OrderInput!]!) { addOrders(input: $input) { count } }",
        "rows_variable": "input",
        "batch_size": 1,
    });

    write(properties, vec![json!({ "id": 7 })]).unwrap();

    assert_eq!(
        sent(&api.seen()[0])["variables"]["input"],
        json!([{ "id": 7 }])
    );
}

#[test]
fn a_batch_that_errors_says_how_much_was_already_delivered() {
    let api = serve(|index, _| match index {
        2 => ok(json!({ "errors": [{ "message": "duplicate order", "path": ["addOrders"] }] })),
        _ => accepted(),
    });
    let mut properties = sink(&api);
    properties["batch_size"] = json!(2);

    let error = write(properties, (1..=6).map(|id| json!({ "id": id })).collect())
        .unwrap_err()
        .to_string();

    assert!(
        error.contains("batch 3 failed after 2 batch(es) (4 record(s)) were delivered"),
        "{error}"
    );
    assert!(
        error.contains("\"duplicate order\" at addOrders"),
        "{error}"
    );
    assert_eq!(api.seen().len(), 3, "nothing is sent after the failure");
}

#[test]
fn no_rows_sends_nothing() {
    let api = serve(|_, _| accepted());

    let summary = write(sink(&api), Vec::new()).unwrap();

    assert_eq!(summary.records, 0);
    assert!(
        summary.detail.contains("nothing sent"),
        "{}",
        summary.detail
    );
    assert!(api.seen().is_empty());
}

// ---------------------------------------------------------------------------
// Checking, before anything runs
// ---------------------------------------------------------------------------

fn refused_source(properties: JsonValue) -> String {
    GraphqlSource
        .check(&properties)
        .expect_err("should be refused")
        .to_string()
}

fn refused_sink(properties: JsonValue) -> String {
    GraphqlSink
        .check(&properties)
        .expect_err("should be refused")
        .to_string()
}

#[test]
fn a_source_configuration_that_cannot_work_is_refused_by_property() {
    let base = json!({
        "url": "https://api.example.com/graphql",
        "query": RELAY_QUERY,
        "records": "/data/orders/nodes",
        "pagination": "relay",
    });
    GraphqlSource.check(&base).expect("the base case is fine");

    let with = |key: &str, value: JsonValue| {
        let mut properties = base.clone();
        properties[key] = value;
        properties
    };

    let blank = refused_source(with("query", json!("   ")));
    assert!(blank.starts_with("property 'query'"), "{blank}");

    let undeclared = refused_source(with("query", json!("{ orders { nodes { id } } }")));
    assert!(
        undeclared.contains("relay pagination sends $after"),
        "{undeclared}"
    );

    // `$afterward` is not `$after`.
    let lookalike = refused_source(with(
        "query",
        json!("query ($afterward: String) { orders(after: $afterward) { id } }"),
    ));
    assert!(lookalike.contains("$after"), "{lookalike}");

    let offset = refused_source(json!({
        "url": "https://api.example.com/graphql",
        "query": "query ($offset: Int) { orders(offset: $offset) { id } }",
        "records": "/data/orders",
        "pagination": "offset",
    }));
    assert!(
        offset.contains("offset pagination sends $limit"),
        "{offset}"
    );

    let not_object = refused_source(with("variables", json!("[1, 2]")));
    assert!(
        not_object.contains("property 'variables': is an array"),
        "{not_object}"
    );

    let not_json = refused_source(with("variables", json!("{status: shipped}")));
    assert!(
        not_json.contains("property 'variables': is not JSON"),
        "{not_json}"
    );

    let clash = refused_source(with("variables", json!("{\"after\": \"x\"}")));
    assert!(clash.contains("sets 'after'"), "{clash}");

    let pointer = refused_source(with("records", json!("data.orders")));
    assert!(pointer.starts_with("property 'records'"), "{pointer}");

    let no_records = refused_source(json!({
        "url": "https://api.example.com/graphql",
        "query": "{ orders { id } }",
    }));
    assert!(no_records.starts_with("property 'records'"), "{no_records}");

    let bad_name = refused_source(with("cursor_variable", json!("1st")));
    assert!(
        bad_name.starts_with("property 'cursor_variable'"),
        "{bad_name}"
    );

    let url = refused_source(with("url", json!("ftp://example.com")));
    assert!(url.starts_with("property 'url'"), "{url}");

    let codes = refused_source(with("retry_codes", json!([1])));
    assert!(codes.starts_with("property 'retry_codes'"), "{codes}");
}

#[test]
fn a_sink_configuration_that_cannot_work_is_refused_by_property() {
    let base = json!({ "url": "https://api.example.com/graphql", "mutation": MUTATION });
    GraphqlSink.check(&base).expect("the base case is fine");

    let with = |key: &str, value: JsonValue| {
        let mut properties = base.clone();
        properties[key] = value;
        properties
    };

    let undeclared = refused_sink(with("rows_variable", json!("input")));
    assert!(undeclared.contains("sent as $input"), "{undeclared}");

    let clash = refused_sink(with("variables", json!("{\"rows\": []}")));
    assert!(clash.contains("sets 'rows'"), "{clash}");

    let batch = refused_sink(with("batch_size", json!(0)));
    assert!(batch.starts_with("property 'batch_size'"), "{batch}");

    let missing = refused_sink(json!({ "url": "https://api.example.com/graphql" }));
    assert!(missing.starts_with("property 'mutation'"), "{missing}");
}

#[test]
fn a_dollar_prefix_on_a_variable_name_is_forgiven() {
    let api = serve(|_, _| accepted());
    let mut properties = sink(&api);
    properties["rows_variable"] = json!("$rows");

    write(properties, vec![json!({ "id": 1 })]).unwrap();
    assert!(sent(&api.seen()[0])["variables"]["rows"].is_array());
}

#[test]
fn neither_spec_offers_a_method() {
    // GraphQL is always a POST; a method property would be a knob that
    // could only break it.
    assert!(GraphqlSource.spec().property("method").is_none());
    assert!(GraphqlSink.spec().property("method").is_none());
    assert_eq!(
        GraphqlSource
            .spec()
            .property("query")
            .unwrap()
            .property_type,
        etl_metadata::PropertyType::Code
    );
}
