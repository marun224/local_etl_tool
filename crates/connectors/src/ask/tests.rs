//! The three against the local fixture as an OpenAI-compatible endpoint: no
//! model, no network.

use super::*;
use crate::fixture::{ok, serve, status};

/// An answer as `/chat/completions` gives one.
fn said(content: &str) -> crate::fixture::Answer {
    ok(json!({ "choices": [ { "message": { "role": "assistant", "content": content } } ] }))
}

fn rows(bodies: &[JsonValue]) -> Vec<Record> {
    bodies
        .iter()
        .enumerate()
        .map(|(index, body)| {
            json!({ ROW_KEY: index + 1, "body": body })
                .as_object()
                .unwrap()
                .clone()
        })
        .collect()
}

fn run(
    transform: &dyn Transform,
    properties: JsonValue,
    input: Vec<Record>,
) -> Result<Vec<Record>, ConnectorError> {
    let mut out: Vec<Record> = Vec::new();
    transform.transform(
        &properties,
        &mut etl_plugin_sdk::Records(input.into_iter()),
        &mut out,
        &Context::default(),
    )?;
    Ok(out)
}

fn sent(seen: &crate::fixture::Seen) -> JsonValue {
    serde_json::from_str(&seen.body).unwrap()
}

// ---------------------------------------------------------------------------
// The prompt template
// ---------------------------------------------------------------------------

#[test]
fn a_template_is_text_and_columns_with_braces_escaped() {
    assert_eq!(
        template("Summarise {body} for {{team}} {who }").unwrap(),
        [
            Piece::Text("Summarise ".into()),
            Piece::Column("body".into()),
            Piece::Text(" for {team} ".into()),
            Piece::Column("who".into()),
        ]
    );
    for broken in ["open {body", "stray } here", "empty {}"] {
        assert!(template(broken).is_err(), "{broken}");
    }
}

// ---------------------------------------------------------------------------
// xf.ai.prompt
// ---------------------------------------------------------------------------

#[test]
fn a_prompt_is_made_from_each_row_and_its_answer_kept_by_row() {
    let endpoint = serve(|_, _| said("  One sentence.  "));
    let properties = json!({
        "prompt": "Summarise: {body}",
        "system": "Be brief.",
        "base_url": endpoint.url("/v1"),
        "model": "gpt-test",
        "api_key": "sk-secret",
    });

    let out = run(
        &PromptTransform,
        properties,
        rows(&[json!("first"), json!(null)]),
    )
    .unwrap();

    assert_eq!(
        out,
        [
            json!({ ROW_KEY: 1, "answer": "One sentence." })
                .as_object()
                .unwrap()
                .clone(),
            json!({ ROW_KEY: 2, "answer": "One sentence." })
                .as_object()
                .unwrap()
                .clone(),
        ]
    );
    let seen = endpoint.seen();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].path(), "/v1/chat/completions");
    assert_eq!(seen[0].header("Authorization"), Some("Bearer sk-secret"));
    let bodies: Vec<JsonValue> = seen.iter().map(sent).collect();
    for body in &bodies {
        assert_eq!(body["model"], "gpt-test");
        assert_eq!(body["temperature"], 0);
        assert_eq!(
            body["messages"][0],
            json!({ "role": "system", "content": "Be brief." })
        );
        assert!(body.get("response_format").is_none());
    }
    let prompts: Vec<&str> = bodies
        .iter()
        .map(|body| body["messages"][1]["content"].as_str().unwrap())
        .collect();
    assert!(prompts.contains(&"Summarise: first"), "{prompts:?}");
    assert!(
        prompts.contains(&"Summarise: "),
        "a null is empty: {prompts:?}"
    );
}

#[test]
fn answers_come_back_in_row_order_however_many_are_asked_at_once() {
    // The endpoint echoes each prompt, so an answer shows which row it was.
    let endpoint = serve(|_, seen| {
        let body: JsonValue = serde_json::from_str(&seen.body).unwrap();
        said(body["messages"][0]["content"].as_str().unwrap())
    });
    let bodies: Vec<JsonValue> = (1..=12).map(|n| json!(format!("row {n}"))).collect();
    let properties = json!({
        "prompt": "{body}", "base_url": endpoint.url("/v1"), "model": "m", "concurrency": 4
    });

    let out = run(&PromptTransform, properties, rows(&bodies)).unwrap();

    for (index, record) in out.iter().enumerate() {
        assert_eq!(record[ROW_KEY], json!(index + 1));
        assert_eq!(record["answer"], json!(format!("row {}", index + 1)));
    }
    assert_eq!(endpoint.seen().len(), 12);
}

#[test]
fn more_rows_than_max_rows_are_refused_before_any_call() {
    let endpoint = serve(|_, _| said("never"));
    let properties = json!({
        "prompt": "{body}", "base_url": endpoint.url("/v1"), "model": "m", "max_rows": 2
    });

    let error = run(
        &PromptTransform,
        properties,
        rows(&[json!("a"), json!("b"), json!("c")]),
    )
    .unwrap_err()
    .to_string();

    assert!(
        error.contains("max_rows") && error.contains("nothing was sent"),
        "{error}"
    );
    assert!(endpoint.seen().is_empty());
}

#[test]
fn a_call_that_keeps_failing_fails_the_stage_naming_the_row() {
    let endpoint = serve(|_, _| status(500, "overloaded"));
    let properties = json!({
        "prompt": "{body}", "base_url": endpoint.url("/v1"), "model": "m",
        "concurrency": 1, "retries": 1
    });

    let error = run(
        &PromptTransform,
        properties,
        rows(&[json!("a"), json!("b")]),
    )
    .unwrap_err()
    .to_string();

    assert!(error.starts_with("row 1: "), "{error}");
    assert!(error.contains("500"), "{error}");
    // One call and one retry, then the stage stops: row 2 is never sent.
    assert_eq!(endpoint.seen().len(), 2);
}

#[test]
fn a_429_is_retried_and_then_answered() {
    let endpoint = serve(|index, _| {
        if index == 0 {
            status(429, "slow down").with("Retry-After", "0")
        } else {
            said("done")
        }
    });
    let properties = json!({ "prompt": "{body}", "base_url": endpoint.url("/v1"), "model": "m" });

    let out = run(&PromptTransform, properties, rows(&[json!("a")])).unwrap();

    assert_eq!(out[0]["answer"], json!("done"));
    assert_eq!(endpoint.seen().len(), 2);
}

#[test]
fn a_401_is_not_retried() {
    let endpoint = serve(|_, _| status(401, "bad key"));
    let properties = json!({ "prompt": "{body}", "base_url": endpoint.url("/v1"), "model": "m" });

    let error = run(&PromptTransform, properties, rows(&[json!("a")]))
        .unwrap_err()
        .to_string();

    assert!(error.contains("401"), "{error}");
    assert_eq!(endpoint.seen().len(), 1);
}

// ---------------------------------------------------------------------------
// xf.ai.classify
// ---------------------------------------------------------------------------

fn classify(endpoint: &crate::fixture::Fixture, extra: JsonValue) -> JsonValue {
    let mut properties = json!({
        "column": "body",
        "labels": ["refund", "bug", "other"],
        "base_url": endpoint.url("/v1"),
        "model": "m",
    });
    for (key, value) in extra.as_object().unwrap() {
        properties[key] = value.clone();
    }
    properties
}

#[test]
fn a_label_is_one_of_the_list_and_the_schema_says_so() {
    let endpoint = serve(|_, _| said(r#"{"label": "refund"}"#));

    let out = run(
        &ClassifyTransform,
        classify(&endpoint, json!({})),
        rows(&[json!("money back please")]),
    )
    .unwrap();

    assert_eq!(out[0]["label"], json!("refund"));
    let body = sent(&endpoint.seen()[0]);
    assert_eq!(
        body["response_format"]["json_schema"]["schema"]["properties"]["label"],
        json!({ "enum": ["refund", "bug", "other"] })
    );
    assert_eq!(body["messages"][1]["content"], "money back please");
}

#[test]
fn a_bare_answer_is_matched_and_one_off_the_list_is_refused() {
    let endpoint = serve(|index, _| said(if index == 0 { " Bug." } else { "banana" }));
    let properties = classify(&endpoint, json!({ "json_schema": false, "concurrency": 1 }));

    let first = run(
        &ClassifyTransform,
        properties.clone(),
        rows(&[json!("it crashed")]),
    )
    .unwrap();
    assert_eq!(
        first[0]["label"],
        json!("bug"),
        "matched without case or punctuation"
    );
    assert!(sent(&endpoint.seen()[0]).get("response_format").is_none());

    let error = run(&ClassifyTransform, properties, rows(&[json!("?")]))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("\"banana\", which is not one of refund, bug, other"),
        "{error}"
    );
}

// ---------------------------------------------------------------------------
// xf.ai.extract
// ---------------------------------------------------------------------------

fn extract(endpoint: &crate::fixture::Fixture) -> JsonValue {
    json!({
        "column": "body",
        "fields": { "name": "text", "count": "integer", "amount": "number",
                    "paid": "boolean", "due": "date" },
        "base_url": endpoint.url("/v1"),
        "model": "m",
    })
}

#[test]
fn fields_come_back_typed_and_null_where_they_are_not_that_type() {
    let endpoint = serve(|_, _| {
        said(
            "```json\n{\"name\": \"Acme\", \"count\": \"3\", \"amount\": \"12.50\", \
             \"paid\": \"yes\", \"due\": \"next week\"}\n```",
        )
    });

    let out = run(
        &ExtractTransform,
        extract(&endpoint),
        rows(&[json!("invoice text")]),
    )
    .unwrap();

    assert_eq!(
        out[0],
        json!({ ROW_KEY: 1, "name": "Acme", "count": 3, "amount": 12.5, "paid": true, "due": null })
            .as_object()
            .unwrap()
            .clone()
    );
    let schema = &sent(&endpoint.seen()[0])["response_format"]["json_schema"]["schema"];
    assert_eq!(
        schema["properties"]["count"],
        json!({ "type": ["integer", "null"] })
    );
    assert_eq!(
        schema["required"],
        json!(["name", "count", "amount", "paid", "due"])
    );
}

#[test]
fn an_answer_that_is_not_an_object_fails_the_row() {
    let endpoint = serve(|_, _| said("I could not find any of those."));

    let error = run(&ExtractTransform, extract(&endpoint), rows(&[json!("x")]))
        .unwrap_err()
        .to_string();

    assert!(
        error.contains("row 1: the model's answer is not a JSON object"),
        "{error}"
    );
}

#[test]
fn extract_adds_one_typed_column_per_field() {
    let properties = json!({ "column": "body", "fields": { "due": "date", "total": "number" } });
    assert_eq!(
        ExtractTransform.adds(&properties),
        [
            ("due".to_string(), "DATE".to_string()),
            ("total".to_string(), "DOUBLE".to_string()),
        ]
    );
}

// ---------------------------------------------------------------------------
// Configuration, and what a built executable can carry
// ---------------------------------------------------------------------------

#[test]
fn a_configuration_that_cannot_work_is_refused_before_anything_runs() {
    let refused = |transform: &dyn Transform, properties: JsonValue| {
        transform.check(&properties).unwrap_err().to_string()
    };

    assert!(refused(
        &PromptTransform,
        json!({ "prompt": "{body}", "base_url": "https://api.x/v1" })
    )
    .contains("model"));
    assert!(refused(
        &PromptTransform,
        json!({ "prompt": "{body}", "base_url": "ftp://x" })
    )
    .contains("http"));
    assert!(refused(&PromptTransform, json!({ "prompt": "open {body" })).contains("prompt"));
    assert!(refused(
        &PromptTransform,
        json!({ "prompt": "{body}", "output": ROW_KEY })
    )
    .contains("engine's"));
    assert!(refused(
        &ClassifyTransform,
        json!({ "column": "b", "labels": ["only"] })
    )
    .contains("at least two"));
    assert!(refused(
        &ExtractTransform,
        json!({ "column": "b", "fields": { "x": "money" } })
    )
    .contains("not one of text, integer"));
    assert!(refused(&ExtractTransform, json!({ "column": "b", "fields": {} })).contains("fields"));
}

#[test]
fn it_reads_only_the_columns_it_needs() {
    assert_eq!(
        PromptTransform.reads(&json!({ "prompt": "{a} and {b} and {a}" })),
        ["a", "b"]
    );
    assert_eq!(
        ClassifyTransform.reads(&json!({ "column": "text" })),
        ["text"]
    );
}

#[test]
fn only_an_endpoint_travels_in_a_built_executable() {
    let remote = json!({ "prompt": "{b}", "base_url": "https://api.x/v1", "model": "m" });
    let local = json!({ "prompt": "{b}" });

    for transform in [
        &PromptTransform as &dyn Transform,
        &ClassifyTransform,
        &ExtractTransform,
    ] {
        assert!(transform.portable(&remote));
        assert!(
            !transform.portable(&local),
            "the local model stays on this machine"
        );
    }
}

#[test]
fn the_report_names_the_endpoints_host_and_never_its_credentials() {
    assert_eq!(
        host_of("https://user:pw@api.example.com:8443/v1"),
        "api.example.com:8443"
    );
    assert_eq!(host_of("http://127.0.0.1:9000/v1"), "127.0.0.1:9000");
}
