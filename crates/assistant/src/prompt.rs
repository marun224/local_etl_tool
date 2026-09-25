//! What the model is told, and the request that carries the grammar.

use etl_metadata::schema::{flow_type, pipeline_schema};
use etl_metadata::{ComponentSpec, PropertySpec, PropertyType};
use serde_json::{json, Value as JsonValue};

/// Enough for a pipeline of a dozen nodes; a model that runs past it is
/// looping inside a string, and the answer would not have parsed anyway.
pub const MAX_TOKENS: u32 = 3072;

/// Low, so the likeliest document wins, but not zero, so a second attempt can
/// differ from the first.
pub const TEMPERATURE: f64 = 0.2;

/// The grammar: the manifest's schema over the offered components only, so
/// the model cannot reach for one the prompt did not describe.
pub fn schema(picked: &[&ComponentSpec]) -> JsonValue {
    pipeline_schema(picked.iter().copied())
}

pub fn system(picked: &[&ComponentSpec]) -> String {
    let mut text = String::from(
        "You write pipeline documents for a local ETL tool, as JSON. A pipeline is nodes wired by \
         edges: rows flow from a source node, through transform nodes, into a sink node.\n\
         \n\
         Rules:\n\
         - Each node runs one component: its id in data.componentId, its settings in \
         data.properties, a short human label in data.label.\n\
         - Node ids are short snake_case names. Each edge goes from one node (source) to the next \
         (target), with sourceHandle \"main\" and targetHandle \"in\".\n\
         - Lay the nodes out left to right: position x 0, 280, 560 and so on, y 0.\n\
         - Use the request's own names and values. Where it leaves one out, write a short \
         plausible value (a connection string such as host=localhost dbname=app user=etl, a \
         table named for the data, an output path under out/) rather than leaving it empty.\n\
         - Leave out optional properties the request does not call for.\n\
         - Never write ${...} references.\n\
         \n\
         Components you may use:\n",
    );
    for spec in picked {
        describe(&mut text, spec);
    }
    text.push_str("\nAn example, for \"read orders.csv, keep orders over 100, write JSON\":\n");
    text.push_str(EXAMPLE);
    text
}

pub fn user(request: &str) -> String {
    format!("Write the pipeline for this request: {request}")
}

/// The body of a chat completion to `llama-server`, which turns the schema
/// into a GBNF grammar (Settled decision 96): every token it samples keeps the
/// output a document of that shape.
pub fn request_body(request: &str, picked: &[&ComponentSpec], seed: u64) -> JsonValue {
    json!({
        "messages": [
            { "role": "system", "content": system(picked) },
            { "role": "user", "content": user(request) }
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": { "name": "pipeline", "strict": true, "schema": schema(picked) }
        },
        "temperature": TEMPERATURE,
        "seed": seed,
        "max_tokens": MAX_TOKENS,
        "stream": false
    })
}

fn describe(text: &mut String, spec: &ComponentSpec) {
    text.push_str(&format!(
        "\n{} ({}): {}",
        spec.id,
        flow_type(spec.namespace),
        spec.label
    ));
    if let Some(description) = &spec.description {
        text.push_str(&format!(". {description}"));
    }
    text.push('\n');
    for property in &spec.properties {
        text.push_str(&format!("  - {}", property_line(property)));
        text.push('\n');
    }
}

fn property_line(property: &PropertySpec) -> String {
    let mut kind = match property.property_type {
        PropertyType::Text | PropertyType::Path => "text".to_string(),
        PropertyType::Sql => "SQL".to_string(),
        PropertyType::Code => "code".to_string(),
        PropertyType::Bool => "true or false".to_string(),
        PropertyType::Integer => "whole number".to_string(),
        PropertyType::Number => "number".to_string(),
        PropertyType::StringList => "list of column names".to_string(),
        PropertyType::Map => "object of name to text".to_string(),
        PropertyType::Enum => format!("one of {}", property.options.join(", ")),
    };
    if property.required && property.default.is_none() {
        kind.push_str(", required");
    }
    let mut line = format!("{} ({kind})", property.name);
    if let Some(help) = &property.help {
        line.push_str(": ");
        line.push_str(help);
    }
    line
}

/// In the order the grammar makes the model write keys: required ones as the
/// schema lists them, then optional ones.
pub const EXAMPLE: &str = r#"{"formatVersion": 1, "nodes": [{"id": "read_orders", "type": "source", "position": {"x": 0, "y": 0}, "data": {"label": "Orders CSV", "componentId": "src.file.csv", "properties": {"path": "data/orders.csv"}}}, {"id": "big_orders", "type": "transform", "position": {"x": 280, "y": 0}, "data": {"label": "Orders over 100", "componentId": "xf.filter", "properties": {"predicate": "amount > 100"}}}, {"id": "write_json", "type": "sink", "position": {"x": 560, "y": 0}, "data": {"label": "Big orders JSON", "componentId": "snk.file.json", "properties": {"path": "out/big_orders.json"}}}], "edges": [{"id": "e1", "source": "read_orders", "target": "big_orders", "sourceHandle": "main", "targetHandle": "in"}, {"id": "e2", "source": "big_orders", "target": "write_json", "sourceHandle": "main", "targetHandle": "in"}], "name": "big_orders"}
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use etl_metadata::PropertySpec;

    fn postgres() -> ComponentSpec {
        ComponentSpec::new("src.db.postgres", "PostgreSQL table")
            .description("Read a table from a PostgreSQL database.")
            .properties(vec![
                PropertySpec::text("connection")
                    .required()
                    .help("Connection string."),
                PropertySpec::text("table").required(),
                PropertySpec::enumerated("mode", &["append", "overwrite"])
                    .required()
                    .default(json!("append")),
            ])
    }

    fn dedup() -> ComponentSpec {
        ComponentSpec::new("xf.dedup", "Deduplicate")
            .properties(vec![PropertySpec::string_list("keys").required()])
    }

    #[test]
    fn each_offered_component_is_described_with_its_properties() {
        let (postgres, dedup) = (postgres(), dedup());
        let text = system(&[&postgres, &dedup]);

        assert!(text.contains(
            "\nsrc.db.postgres (source): PostgreSQL table. Read a table from a PostgreSQL database.\n"
        ));
        assert!(text.contains("  - connection (text, required): Connection string.\n"));
        assert!(text.contains("  - table (text, required)\n"));
        // Required but defaulted: the model may leave it out.
        assert!(text.contains("  - mode (one of append, overwrite)\n"));
        assert!(text.contains("\nxf.dedup (transform): Deduplicate\n"));
        assert!(text.contains("  - keys (list of column names, required)\n"));
    }

    #[test]
    fn the_example_is_a_document_the_schema_describes() {
        let example: JsonValue = serde_json::from_str(EXAMPLE).unwrap();
        assert_eq!(example["formatVersion"], 1);
        let keys: Vec<&String> = example.as_object().unwrap().keys().collect();
        assert_eq!(keys, ["formatVersion", "nodes", "edges", "name"]);
        let data: Vec<&String> = example["nodes"][0]["data"]
            .as_object()
            .unwrap()
            .keys()
            .collect();
        assert_eq!(data, ["label", "componentId", "properties"]);
    }

    #[test]
    fn the_request_carries_the_schema_of_the_offered_components_only() {
        let (postgres, dedup) = (postgres(), dedup());
        let body = request_body("dedupe postgres", &[&postgres, &dedup], 7);

        let schema = &body["response_format"]["json_schema"]["schema"];
        assert_eq!(
            schema["properties"]["nodes"]["items"]["anyOf"],
            json!([
                { "$ref": "#/$defs/node.src.db.postgres" },
                { "$ref": "#/$defs/node.xf.dedup" }
            ])
        );
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(body["seed"], 7);
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(
            body["messages"][1]["content"],
            "Write the pipeline for this request: dedupe postgres"
        );
    }
}
