//! A JSON Schema for pipeline documents, generated from the component specs.
//!
//! One schema, three readers (Settled decision 96): MCP's `get_schema` hands it
//! to an agent writing a pipeline; Phase 11b hands it to `llama-server`, which
//! turns it into a grammar so a small local model can only emit documents of
//! this shape; and tests check documents against it.
//!
//! It is stricter than the reader. [`crate::PipelineDoc`] keeps fields it does
//! not know and the engine only warns about a property its component does not
//! define; the schema allows neither, because it describes what should be
//! *written*, and a model that may add any key will. Each node is tied to one
//! component: its `componentId` is a constant, its `type` the canvas kind that
//! component's namespace draws as, and its `properties` exactly that
//! component's, typed.
//!
//! A value that could come from a parameter may be a `${name}` reference
//! instead, whatever its type, because a document is checked before its
//! parameters are resolved.

use crate::{ComponentSpec, Namespace, PropertySpec, PropertyType};
use serde_json::{json, Map, Value as JsonValue};

/// What a `${...}` reference looks like: a parameter, a context variable, a
/// secret or a built-in.
pub const REFERENCE_PATTERN: &str = r"^\$\{[^}]+\}$";

/// The canvas's kind for a node of this namespace, as the frontend's
/// `flowTypeFor` draws it.
pub fn flow_type(namespace: Namespace) -> &'static str {
    match namespace {
        Namespace::Source => "source",
        Namespace::Sink => "sink",
        _ => "transform",
    }
}

/// The schema for a pipeline document built from these components.
pub fn pipeline_schema<'a>(specs: impl IntoIterator<Item = &'a ComponentSpec>) -> JsonValue {
    let mut defs = Map::new();
    let mut nodes = Vec::new();
    for spec in specs {
        let key = format!("node.{}", spec.id);
        nodes.push(json!({ "$ref": format!("#/$defs/{key}") }));
        defs.insert(key, node_schema(spec));
    }
    defs.insert("position".into(), position_schema());
    defs.insert("edge".into(), edge_schema());
    defs.insert("policy".into(), policy_schema());
    defs.insert("incremental".into(), incremental_schema());
    defs.insert("parameter".into(), parameter_schema());

    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "etl pipeline document",
        "description": "A pipeline: nodes, each running one component, wired by edges from an output \
                        handle to an input handle. formatVersion is 1.",
        "type": "object",
        "required": ["formatVersion", "nodes", "edges"],
        "properties": {
            "formatVersion": { "const": 1 },
            "name": { "type": "string", "minLength": 1 },
            "resourcePool": { "type": "string" },
            "nodes": { "type": "array", "minItems": 1, "items": { "anyOf": nodes } },
            "edges": { "type": "array", "items": { "$ref": "#/$defs/edge" } },
            "parameters": {
                "type": "object",
                "additionalProperties": { "$ref": "#/$defs/parameter" }
            }
        },
        "additionalProperties": false,
        "$defs": defs
    })
}

/// One component's node: its kind, its id, and its properties.
fn node_schema(spec: &ComponentSpec) -> JsonValue {
    let mut properties = Map::new();
    let mut required = Vec::new();
    for property in &spec.properties {
        properties.insert(property.name.clone(), property_schema(property));
        if property.required && property.default.is_none() {
            required.push(JsonValue::String(property.name.clone()));
        }
    }

    let mut data_required = vec![json!("label"), json!("componentId")];
    if !required.is_empty() {
        data_required.push(json!("properties"));
    }
    let mut description = spec.label.clone();
    if let Some(text) = &spec.description {
        description.push_str(": ");
        description.push_str(text);
    }

    json!({
        "description": description,
        "type": "object",
        "required": ["id", "type", "position", "data"],
        "properties": {
            "id": { "type": "string", "minLength": 1 },
            "type": { "const": flow_type(spec.namespace) },
            "position": { "$ref": "#/$defs/position" },
            "data": {
                "type": "object",
                "required": data_required,
                "properties": {
                    "label": { "type": "string" },
                    "subtitle": { "type": "string" },
                    "componentId": { "const": spec.id },
                    "properties": {
                        "type": "object",
                        "required": required,
                        "properties": properties,
                        "additionalProperties": false
                    },
                    "alias": { "type": "string" },
                    "disabled": { "type": "boolean" },
                    "materialize": { "enum": ["auto", "view", "memory", "disk"] },
                    "policy": { "$ref": "#/$defs/policy" },
                    "incremental": { "$ref": "#/$defs/incremental" }
                },
                "additionalProperties": false
            }
        },
        "additionalProperties": false
    })
}

/// One property's value, or a reference standing in for it.
fn property_schema(property: &PropertySpec) -> JsonValue {
    let reference = json!({ "type": "string", "pattern": REFERENCE_PATTERN });
    let value = match property.property_type {
        PropertyType::Text | PropertyType::Path | PropertyType::Sql | PropertyType::Code => {
            // Any text, a reference included.
            json!({ "type": "string" })
        }
        PropertyType::Enum => {
            json!({ "anyOf": [{ "enum": property.options }, reference] })
        }
        PropertyType::Bool => json!({ "anyOf": [{ "type": "boolean" }, reference] }),
        PropertyType::Integer => json!({ "anyOf": [{ "type": "integer" }, reference] }),
        PropertyType::Number => json!({ "anyOf": [{ "type": "number" }, reference] }),
        PropertyType::StringList => json!({
            "anyOf": [{ "type": "array", "items": { "type": "string" } }, reference]
        }),
        PropertyType::Map => json!({
            "type": "object",
            "additionalProperties": { "type": "string" }
        }),
    };
    let mut value = match value {
        JsonValue::Object(map) => map,
        _ => unreachable!("every arm is an object"),
    };
    if let Some(help) = &property.help {
        value.insert("description".into(), json!(help));
    }
    if let Some(default) = &property.default {
        value.insert("default".into(), default.clone());
    }
    JsonValue::Object(value)
}

fn position_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["x", "y"],
        "properties": { "x": { "type": "number" }, "y": { "type": "number" } },
        "additionalProperties": false
    })
}

fn edge_schema() -> JsonValue {
    json!({
        "description": "From one node's output handle (sourceHandle, usually \"main\") to another's \
                        input handle (targetHandle, usually \"in\").",
        "type": "object",
        "required": ["id", "source", "target"],
        "properties": {
            "id": { "type": "string", "minLength": 1 },
            "source": { "type": "string", "minLength": 1 },
            "target": { "type": "string", "minLength": 1 },
            "sourceHandle": { "type": "string" },
            "targetHandle": { "type": "string" }
        },
        "additionalProperties": false
    })
}

fn policy_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "retryAttempts": { "type": "integer", "minimum": 0 },
            "retryBackoffMs": { "type": "integer", "minimum": 0 },
            "continueOnFailure": { "type": "boolean" },
            "memoryLimitMb": { "type": "integer", "minimum": 1 }
        },
        "additionalProperties": false
    })
}

fn incremental_schema() -> JsonValue {
    json!({
        "description": "Load only rows whose column is above the last successful run's highest.",
        "type": "object",
        "required": ["column"],
        "properties": { "column": { "type": "string" }, "start": { "type": "string" } },
        "additionalProperties": false
    })
}

fn parameter_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "type": { "enum": ["string", "number", "integer", "boolean"] },
            "required": { "type": "boolean" },
            "default": {},
            "description": { "type": "string" }
        },
        "additionalProperties": false
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs() -> Vec<ComponentSpec> {
        vec![
            ComponentSpec::new("src.file.csv", "CSV file").properties(vec![
                PropertySpec::path("path").required(),
                PropertySpec::boolean("header").default(json!(true)),
                PropertySpec::integer("skip"),
                PropertySpec::enumerated("mode", &["append", "overwrite"])
                    .required()
                    .default(json!("append")),
            ]),
            ComponentSpec::new("xf.dedup", "Deduplicate")
                .properties(vec![PropertySpec::string_list("columns")]),
            ComponentSpec::new("snk.file.parquet", "Parquet file")
                .description("Write Parquet.")
                .properties(vec![PropertySpec::map("rename").help("Old to new names.")]),
        ]
    }

    #[test]
    fn each_component_is_a_node_tied_to_its_id_and_kind() {
        let schema = pipeline_schema(&specs());
        let defs = &schema["$defs"];

        let csv = &defs["node.src.file.csv"];
        assert_eq!(csv["properties"]["type"], json!({ "const": "source" }));
        assert_eq!(
            csv["properties"]["data"]["properties"]["componentId"],
            json!({ "const": "src.file.csv" })
        );
        assert_eq!(
            defs["node.xf.dedup"]["properties"]["type"],
            json!({ "const": "transform" })
        );
        assert_eq!(
            defs["node.snk.file.parquet"]["properties"]["type"],
            json!({ "const": "sink" })
        );
        assert_eq!(
            defs["node.snk.file.parquet"]["description"],
            json!("Parquet file: Write Parquet.")
        );
        assert_eq!(
            schema["properties"]["nodes"]["items"]["anyOf"],
            json!([
                { "$ref": "#/$defs/node.src.file.csv" },
                { "$ref": "#/$defs/node.xf.dedup" },
                { "$ref": "#/$defs/node.snk.file.parquet" }
            ])
        );
    }

    #[test]
    fn required_means_required_and_without_a_default() {
        let schema = pipeline_schema(&specs());
        let data = &schema["$defs"]["node.src.file.csv"]["properties"]["data"];

        // `mode` is required but has a default, so leaving it out is fine.
        assert_eq!(
            data["properties"]["properties"]["required"],
            json!(["path"])
        );
        assert_eq!(
            data["required"],
            json!(["label", "componentId", "properties"])
        );

        let dedup = &schema["$defs"]["node.xf.dedup"]["properties"]["data"];
        assert_eq!(dedup["required"], json!(["label", "componentId"]));
        assert_eq!(
            dedup["properties"]["properties"]["additionalProperties"],
            json!(false)
        );
    }

    #[test]
    fn values_are_typed_and_may_be_references() {
        let schema = pipeline_schema(&specs());
        let csv = &schema["$defs"]["node.src.file.csv"]["properties"]["data"]["properties"]
            ["properties"]["properties"];
        let reference = json!({ "type": "string", "pattern": REFERENCE_PATTERN });

        assert_eq!(csv["path"], json!({ "type": "string" }));
        assert_eq!(
            csv["header"],
            json!({ "anyOf": [{ "type": "boolean" }, reference], "default": true })
        );
        assert_eq!(
            csv["skip"],
            json!({ "anyOf": [{ "type": "integer" }, reference] })
        );
        assert_eq!(
            csv["mode"]["anyOf"][0],
            json!({ "enum": ["append", "overwrite"] })
        );

        let parquet = &schema["$defs"]["node.snk.file.parquet"]["properties"]["data"]["properties"]
            ["properties"]["properties"];
        assert_eq!(
            parquet["rename"],
            json!({
                "type": "object",
                "additionalProperties": { "type": "string" },
                "description": "Old to new names."
            })
        );
    }

    #[test]
    fn the_reference_pattern_matches_what_resolution_substitutes() {
        // Kept as a literal so the schema's text is reviewable; this pins it.
        assert_eq!(REFERENCE_PATTERN, "^\\$\\{[^}]+\\}$");
    }
}
