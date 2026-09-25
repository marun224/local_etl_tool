//! The grammar's input, checked without a model (Settled decision 97): the
//! schema generated from the real registry accepts every sample pipeline and
//! refuses broken ones, and the prompt builder's choices over the real
//! manifest are the ones the model needs.

use etl_assistant::{pick, prompt};
use etl_duckdb_engine::registry;
use etl_metadata::schema::pipeline_schema;
use etl_metadata::ComponentSpec;
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;

fn specs() -> Vec<ComponentSpec> {
    registry().specs().cloned().collect()
}

fn validator(schema: &JsonValue) -> jsonschema::Validator {
    jsonschema::draft202012::new(schema).expect("the schema compiles")
}

fn samples() -> Vec<(String, JsonValue)> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../samples/pipelines");
    let mut found: Vec<(String, JsonValue)> = std::fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .map(|path| {
            let text = std::fs::read_to_string(&path).unwrap();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            (name, serde_json::from_str(&text).unwrap())
        })
        .collect();
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found
}

/// Every reason a document is refused, for an assertion's message.
fn errors(validator: &jsonschema::Validator, document: &JsonValue) -> Vec<String> {
    validator
        .iter_errors(document)
        .map(|error| format!("{} at {}", error, error.instance_path()))
        .collect()
}

#[test]
fn every_sample_pipeline_is_a_document_the_schema_describes() {
    let validator = validator(&pipeline_schema(&specs()));
    let samples = samples();
    assert!(samples.len() >= 20, "found {} samples", samples.len());

    for (name, document) in samples {
        let refused = errors(&validator, &document);
        assert!(refused.is_empty(), "{name}: {refused:#?}");
    }
}

#[test]
fn the_prompts_example_is_a_document_the_schema_describes() {
    let validator = validator(&pipeline_schema(&specs()));
    let example: JsonValue = serde_json::from_str(prompt::EXAMPLE).unwrap();
    assert!(
        validator.is_valid(&example),
        "{:#?}",
        errors(&validator, &example)
    );
}

/// The CSV-to-Parquet sample with one thing broken.
fn broken(change: impl FnOnce(&mut JsonValue)) -> JsonValue {
    let (_, mut document) = samples()
        .into_iter()
        .find(|(name, _)| name == "csv_to_parquet.json")
        .unwrap();
    change(&mut document);
    document
}

#[test]
fn broken_documents_are_refused() {
    let validator = validator(&pipeline_schema(&specs()));

    let cases: Vec<(&str, JsonValue)> = vec![
        (
            "an unknown property",
            broken(|d| d["nodes"][0]["data"]["properties"]["colour"] = json!("red")),
        ),
        (
            "a value of the wrong type",
            broken(|d| d["nodes"][0]["data"]["properties"]["header"] = json!("yes")),
        ),
        (
            "a value outside an enum",
            broken(|d| d["nodes"][2]["data"]["properties"]["compression"] = json!("rar")),
        ),
        (
            "a required property missing",
            broken(|d| {
                d["nodes"][0]["data"]["properties"]
                    .as_object_mut()
                    .unwrap()
                    .remove("path");
            }),
        ),
        (
            "an unknown component",
            broken(|d| d["nodes"][1]["data"]["componentId"] = json!("xf.teleport")),
        ),
        (
            "a source drawn as a sink",
            broken(|d| d["nodes"][0]["type"] = json!("sink")),
        ),
        (
            "an unknown key on a node",
            broken(|d| d["nodes"][0]["data"]["colour"] = json!("red")),
        ),
        (
            "an unknown top-level key",
            broken(|d| d["owner"] = json!("me")),
        ),
        (
            "another format version",
            broken(|d| d["formatVersion"] = json!(2)),
        ),
        ("no nodes", broken(|d| d["nodes"] = json!([]))),
        (
            "an edge without a target",
            broken(|d| {
                d["edges"][0].as_object_mut().unwrap().remove("target");
            }),
        ),
        (
            "a node without a position",
            broken(|d| {
                d["nodes"][1].as_object_mut().unwrap().remove("position");
            }),
        ),
    ];

    for (what, document) in cases {
        assert!(!validator.is_valid(&document), "{what} was accepted");
    }
}

#[test]
fn a_reference_may_stand_for_a_typed_value() {
    let validator = validator(&pipeline_schema(&specs()));

    let referenced =
        broken(|d| d["nodes"][0]["data"]["properties"]["header"] = json!("${has_header}"));
    assert!(
        validator.is_valid(&referenced),
        "{:#?}",
        errors(&validator, &referenced)
    );

    let not_a_reference =
        broken(|d| d["nodes"][0]["data"]["properties"]["header"] = json!("$has_header"));
    assert!(!validator.is_valid(&not_a_reference));
}

#[test]
fn the_verify_request_is_offered_what_it_needs_and_little_else() {
    let specs = specs();
    let picked = pick::pick("read this Postgres table, dedupe, write Parquet", &specs);
    let ids: Vec<&str> = picked.iter().map(|spec| spec.id.as_str()).collect();

    for wanted in ["src.db.postgres", "xf.dedup", "snk.file.parquet"] {
        assert!(ids.contains(&wanted), "{wanted} not in {ids:?}");
    }
    assert!(ids.len() <= pick::MAX_PICKED, "{ids:?}");
    assert!(ids
        .iter()
        .all(|id| !id.starts_with("ctl.") && !id.starts_with("code.")));
}

#[test]
fn a_comparison_is_offered_the_filter() {
    let specs = specs();
    let picked = pick::pick(
        "read orders.csv, keep orders over 100, sort by amount, write JSON",
        &specs,
    );
    let ids: Vec<&str> = picked.iter().map(|spec| spec.id.as_str()).collect();

    for wanted in ["src.file.csv", "xf.filter", "xf.sort", "snk.file.json"] {
        assert!(ids.contains(&wanted), "{wanted} not in {ids:?}");
    }
}

#[test]
fn the_grammar_allows_the_offered_components_and_no_others() {
    let specs = specs();
    let picked = pick::pick("read this Postgres table, dedupe, write Parquet", &specs);
    let validator = validator(&prompt::schema(&picked));

    let wanted = json!({
        "formatVersion": 1,
        "nodes": [
            { "id": "pg", "type": "source", "position": { "x": 0, "y": 0 },
              "data": { "label": "Orders", "componentId": "src.db.postgres",
                        "properties": { "connection": "host=localhost dbname=app", "table": "orders" } } },
            { "id": "dedup", "type": "transform", "position": { "x": 280, "y": 0 },
              "data": { "label": "One per id", "componentId": "xf.dedup",
                        "properties": { "keys": ["id"] } } },
            { "id": "out", "type": "sink", "position": { "x": 560, "y": 0 },
              "data": { "label": "Parquet", "componentId": "snk.file.parquet",
                        "properties": { "path": "out/orders.parquet" } } }
        ],
        "edges": [
            { "id": "e1", "source": "pg", "target": "dedup" },
            { "id": "e2", "source": "dedup", "target": "out" }
        ]
    });
    assert!(
        validator.is_valid(&wanted),
        "{:#?}",
        errors(&validator, &wanted)
    );

    let mut elsewhere = wanted.clone();
    elsewhere["nodes"][0]["data"] = json!({
        "label": "Orders", "componentId": "src.db.mysql",
        "properties": { "connection": "host=localhost", "table": "orders" }
    });
    assert!(
        !validator.is_valid(&elsewhere),
        "a component not offered was allowed"
    );
}

#[test]
fn every_components_prompt_entry_names_its_required_properties() {
    // The model is told what it must fill in; a required property left out of
    // the description is one it will not know to write.
    let specs = specs();
    let all: Vec<&ComponentSpec> = specs.iter().collect();
    let text = prompt::system(&all);
    for spec in &specs {
        for property in spec
            .properties
            .iter()
            .filter(|p| p.required && p.default.is_none())
        {
            let marker = format!("  - {} (", property.name);
            let entry = text
                .split(&format!("\n{} (", spec.id))
                .nth(1)
                .unwrap_or_else(|| panic!("{} is not described", spec.id));
            let line = entry
                .split("\n\n")
                .next()
                .unwrap()
                .lines()
                .find(|line| line.starts_with(&marker))
                .unwrap_or_else(|| panic!("{}.{} is not described", spec.id, property.name));
            assert!(
                line.contains(", required)"),
                "{}.{} is not described as required: {line}",
                spec.id,
                property.name
            );
        }
    }
}
