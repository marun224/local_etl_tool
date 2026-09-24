use super::*;
use etl_metadata::{Namespace, PropertyType};
use serde_json::json;

fn spec(component_id: &str) -> &'static ComponentSpec {
    &registry().get(component_id).expect("registered").spec
}

// ---------------------------------------------------------------------------
// The registry as a whole
// ---------------------------------------------------------------------------

#[test]
fn the_registry_holds_exactly_these_components() {
    // An inventory, deliberately. Adding a component should be a decision that
    // shows up in a diff, and a component vanishing through a bad merge should
    // turn something red. Update this list when you add one — it is the single
    // line beyond the spec, the builder, and the builder's own test.
    let ids: Vec<&str> = registry().specs().map(|s| s.id.as_str()).collect();

    assert_eq!(
        ids,
        [
            "ctl.branch",
            "ctl.fail",
            "ctl.log",
            "ctl.sequence",
            "ctl.wait",
            "qa.accepted_values",
            "qa.expression",
            "qa.not_null",
            "qa.range",
            "qa.referential",
            "qa.regex",
            "qa.row_count",
            "qa.schema_match",
            "qa.unique",
            "snk.cloud.s3",
            "snk.db.mongodb",
            "snk.db.mysql",
            "snk.db.postgres",
            "snk.db.sqlite",
            "snk.file.csv",
            "snk.file.excel",
            "snk.file.json",
            "snk.file.jsonl",
            "snk.file.parquet",
            "snk.file.xml",
            "snk.queue.pubsub",
            "snk.queue.rabbitmq",
            "snk.queue.sqs",
            "snk.saas.graphql",
            "snk.saas.rest",
            "snk.stream.kafka",
            "snk.stream.kinesis",
            "snk.stream.nats",
            "snk.warehouse.bigquery",
            "snk.warehouse.snowflake",
            "src.cloud.http",
            "src.cloud.s3",
            "src.db.mongodb",
            "src.db.mysql",
            "src.db.postgres",
            "src.db.sqlite",
            "src.file.csv",
            "src.file.excel",
            "src.file.json",
            "src.file.jsonl",
            "src.file.parquet",
            "src.file.xml",
            "src.lake.delta",
            "src.lake.iceberg",
            "src.queue.pubsub",
            "src.queue.rabbitmq",
            "src.queue.sqs",
            "src.saas.graphql",
            "src.saas.rest",
            "src.stream.kafka",
            "src.stream.kinesis",
            "src.stream.nats",
            "src.warehouse.bigquery",
            "src.warehouse.snowflake",
            "xf.aggregate",
            "xf.cast",
            "xf.dedup",
            "xf.derive",
            "xf.distinct",
            "xf.except",
            "xf.filter",
            "xf.intersect",
            "xf.join",
            "xf.limit",
            "xf.pivot",
            "xf.rename",
            "xf.sample",
            "xf.select",
            "xf.sort",
            "xf.sql",
            "xf.union",
            "xf.unpivot",
            "xf.window",
        ],
        "sorted by id, so the listing is stable"
    );
}

#[test]
fn every_component_has_a_label_and_a_description() {
    for spec in registry().specs() {
        assert!(!spec.label.is_empty(), "{} has no label", spec.id);
        assert!(
            spec.description.is_some(),
            "{} has no description; the canvas shows it on hover",
            spec.id
        );
        assert!(spec.icon.is_some(), "{} has no icon", spec.id);
    }
}

#[test]
fn every_component_id_matches_its_namespace() {
    for spec in registry().specs() {
        let prefix = spec.id.split('.').next().unwrap();

        assert_eq!(
            Namespace::from_prefix(prefix),
            Some(spec.namespace),
            "{} claims the wrong namespace",
            spec.id
        );
    }
}

#[test]
fn every_property_is_fully_described() {
    // The canvas generates the property panel from these, so a property with
    // no label or no help renders as a mystery box.
    for spec in registry().specs() {
        for property in &spec.properties {
            assert!(
                !property.label.is_empty(),
                "{}.{} has no label",
                spec.id,
                property.name
            );

            if property.property_type == PropertyType::Enum {
                assert!(
                    !property.options.is_empty(),
                    "{}.{} is an enum with no options",
                    spec.id,
                    property.name
                );
            }
        }
    }
}

#[test]
fn a_default_is_always_valid_for_its_own_property() {
    for spec in registry().specs() {
        for property in &spec.properties {
            let Some(default) = &property.default else {
                continue;
            };

            assert!(
                property.property_type.accepts(default),
                "{}.{} has a default of the wrong type",
                spec.id,
                property.name
            );

            if let Some(text) = default.as_str() {
                if !property.options.is_empty() {
                    assert!(
                        property.options.iter().any(|o| o == text),
                        "{}.{} defaults to '{}', which is not one of its options",
                        spec.id,
                        property.name,
                        text
                    );
                }
            }
        }
    }
}

#[test]
fn a_required_property_never_carries_a_default() {
    // A required property with a default is never actually required, so one of
    // the two is a mistake.
    for spec in registry().specs() {
        for property in &spec.properties {
            assert!(
                !(property.required && property.default.is_some()),
                "{}.{} is required but also has a default",
                spec.id,
                property.name
            );
        }
    }
}

#[test]
fn sources_have_no_inputs_and_sinks_have_no_outputs() {
    for spec in registry().specs() {
        match spec.namespace {
            Namespace::Source => assert!(spec.inputs.is_empty(), "{}", spec.id),
            Namespace::Sink => assert!(spec.outputs.is_empty(), "{}", spec.id),
            _ => assert!(!spec.outputs.is_empty(), "{}", spec.id),
        }
    }
}

#[test]
fn port_names_are_unique_within_a_component() {
    for spec in registry().specs() {
        let mut names: Vec<&str> = spec.inputs.iter().map(|p| p.name.as_str()).collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();

        assert_eq!(count, names.len(), "{} has duplicate input ports", spec.id);
    }
}

// ---------------------------------------------------------------------------
// The manifest
// ---------------------------------------------------------------------------

#[test]
fn the_manifest_round_trips_through_json() {
    let manifest = registry().manifest();
    let text = serde_json::to_string(&manifest).expect("serialises");
    let parsed: JsonValue = serde_json::from_str(&text).expect("parses");

    assert_eq!(parsed, manifest);

    let components = parsed["components"].as_array().expect("an array");
    assert_eq!(components.len(), registry().len());
    assert_eq!(parsed["formatVersion"], 1);
}

#[test]
fn the_manifest_carries_the_property_schema_the_canvas_needs() {
    let manifest = registry().manifest();
    let components = manifest["components"].as_array().unwrap();

    let csv = components
        .iter()
        .find(|c| c["id"] == "src.file.csv")
        .expect("src.file.csv is in the manifest");

    let path = csv["properties"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "path")
        .expect("path is described");

    assert_eq!(path["type"], "path");
    assert_eq!(path["required"], true);
    assert_eq!(path["label"], "Path");
    assert!(path["help"].is_string());
}

// ---------------------------------------------------------------------------
// Property resolution
// ---------------------------------------------------------------------------

fn resolve(component_id: &str, properties: JsonValue) -> Result<JsonValue, EngineError> {
    resolve_properties("n", spec(component_id), &properties, &mut Vec::new())
}

#[test]
fn defaults_are_filled_in_from_the_spec() {
    let resolved = resolve("src.file.csv", json!({ "path": "in.csv" })).unwrap();

    assert_eq!(resolved["header"], true, "the spec default applies");
    assert_eq!(resolved["path"], "in.csv");
    assert!(
        resolved.get("delimiter").is_none(),
        "a property with no default and not required stays absent"
    );
}

#[test]
fn a_supplied_value_beats_the_default() {
    let resolved = resolve("src.file.csv", json!({ "path": "in.csv", "header": false })).unwrap();

    assert_eq!(resolved["header"], false);
}

#[test]
fn an_explicit_null_falls_back_to_the_default() {
    let resolved = resolve("src.file.csv", json!({ "path": "in.csv", "header": null })).unwrap();

    assert_eq!(resolved["header"], true);
}

#[test]
fn every_required_property_is_checked_by_name() {
    // This is the gap Duckle leaves open: it only catches the properties a
    // builder happens to read. Driving the check from the spec means a
    // component that gains a required property gains the check with it.
    let error = resolve("xf.filter", json!({})).unwrap_err();
    assert!(matches!(error, EngineError::MissingProperty { .. }));

    let error = resolve("snk.file.parquet", json!({})).unwrap_err();
    assert_eq!(
        error.to_string(),
        "node 'n' (snk.file.parquet) needs the 'path' property"
    );

    let error = resolve("xf.select", json!({})).unwrap_err();
    assert!(error.to_string().contains("'columns'"), "{error}");
}

#[test]
fn a_rule_spanning_two_properties_stays_in_the_builder() {
    // `xf.join` needs keys *or* condition, which is not something a
    // per-property schema can express: neither is required on its own. The
    // spec covers types, options, and defaults; relationships between
    // properties remain the builder's job, and this test exists so that
    // division stays deliberate rather than forgotten.
    assert!(
        spec("xf.join").properties.iter().all(|p| !p.required),
        "no single join property is required on its own"
    );

    assert!(
        resolve("xf.join", json!({})).is_ok(),
        "resolution passes; the builder is what rejects it"
    );
}

#[test]
fn a_wrong_type_is_reported_against_the_property() {
    let error = resolve("src.file.csv", json!({ "path": "in.csv", "header": "yes" })).unwrap_err();

    assert_eq!(
        error,
        EngineError::InvalidProperty {
            id: "n".to_string(),
            property: "header".to_string(),
            reason: "must be true or false".to_string(),
        }
    );
}

#[test]
fn an_enum_rejects_a_value_outside_its_options_and_lists_them() {
    let error = resolve(
        "snk.file.parquet",
        json!({ "path": "o.parquet", "compression": "rar" }),
    )
    .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("must be one of"), "{message}");
    assert!(message.contains("zstd"), "{message}");
}

#[test]
fn a_required_text_property_may_not_be_blank() {
    let error = resolve("src.file.csv", json!({ "path": "  " })).unwrap_err();

    assert!(error.to_string().contains("must not be empty"), "{error}");
}

#[test]
fn a_string_list_must_hold_only_strings() {
    let error = resolve("xf.select", json!({ "columns": ["ok", 7] })).unwrap_err();

    assert!(
        error.to_string().contains("must be a list of names"),
        "{error}"
    );
}

#[test]
fn properties_that_are_not_an_object_are_rejected() {
    let error = resolve("src.file.csv", json!("in.csv")).unwrap_err();

    assert!(error.to_string().contains("must be an object"), "{error}");
}

#[test]
fn a_node_with_no_properties_still_gets_its_defaults() {
    let resolved =
        resolve_properties("n", spec("xf.join"), &JsonValue::Null, &mut Vec::new()).unwrap();

    assert_eq!(resolved["type"], "inner");
}

// ---------------------------------------------------------------------------
// Unknown properties
// ---------------------------------------------------------------------------

#[test]
fn an_unknown_property_is_reported_but_does_not_stop_the_run() {
    let mut unknown = Vec::new();
    let resolved = resolve_properties(
        "n",
        spec("src.file.csv"),
        &json!({ "path": "in.csv", "headr": true }),
        &mut unknown,
    )
    .expect("an unknown property is not fatal");

    assert_eq!(
        unknown,
        [UnknownProperty {
            node_id: "n".to_string(),
            property: "headr".to_string(),
        }]
    );

    assert_eq!(
        resolved["headr"], true,
        "it is carried through rather than dropped, so a newer version can read it"
    );
}

// ---------------------------------------------------------------------------
// Input arity
// ---------------------------------------------------------------------------

#[test]
fn input_arity_comes_from_the_declared_ports() {
    assert!(check_input_count("n", spec("src.file.csv"), 0).is_ok());
    assert!(check_input_count("n", spec("xf.filter"), 1).is_ok());
    assert!(check_input_count("n", spec("xf.join"), 2).is_ok());

    let error = check_input_count("n", spec("xf.join"), 1).unwrap_err();
    assert_eq!(
        error,
        EngineError::WrongInputCount {
            id: "n".to_string(),
            component_id: "xf.join".to_string(),
            expected: 2,
            actual: 1,
        }
    );
}

#[test]
fn the_join_declares_its_handles_so_the_canvas_can_label_them() {
    let join = spec("xf.join");

    let names: Vec<&str> = join.inputs.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, ["left", "right"]);
    assert!(join.inputs.iter().all(|p| p.help.is_some()));
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

#[test]
fn an_unregistered_component_is_named_in_the_error() {
    let error = lookup("n", "xf.nope").unwrap_err();

    assert_eq!(
        error,
        EngineError::UnsupportedComponent {
            id: "n".to_string(),
            component_id: "xf.nope".to_string(),
        }
    );
}
