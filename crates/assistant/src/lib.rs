//! A local model writes pipeline documents (Phase 11b).
//!
//! llama.cpp's `llama-server` runs a small coding model on this machine; the
//! request carries the manifest's JSON Schema, which the server turns into a
//! grammar, so the model can only emit a document of that shape (Settled
//! decisions 95 and 96). Whether the document is *valid* (its edges joined, its
//! values acceptable) is the engine's to say: this crate knows the model and
//! not the engine, and the CLI checks what comes back before anyone sees it.

pub mod pick;
pub mod prompt;
pub mod server;

pub use server::{
    locate_model, locate_server, Server, DEFAULT_MODEL, MODEL_ENV, SERVER_ENV, STOPPED,
};

use etl_metadata::ComponentSpec;
use serde_json::Value as JsonValue;

/// What the model wrote for one request.
pub struct Draft {
    pub document: JsonValue,
    /// The components it was offered.
    pub offered: Vec<String>,
}

/// Ask the model for a pipeline doing `request`, built from `specs`.
pub fn draft(
    server: &Server,
    request: &str,
    specs: &[ComponentSpec],
    seed: u64,
) -> Result<Draft, String> {
    let picked = pick::pick(request, specs);
    let body = prompt::request_body(request, &picked, seed);
    let text = server.complete(&body)?;

    // The grammar makes this JSON; a parse failure means the server ignored
    // the schema, which is worth saying plainly.
    let mut document = serde_json::from_str(&text).map_err(|error| {
        format!(
            "the model's answer is not JSON ({error}); is llama-server too old for json_schema?"
        )
    })?;
    drop_blank_options(&mut document, specs);

    Ok(Draft {
        document,
        offered: picked.iter().map(|spec| spec.id.clone()).collect(),
    })
}

/// An optional property written as empty text is dropped. The model means
/// "none", but the engine reads `""` as a value: an empty schema name would
/// qualify the table as `"db".""."orders"`.
fn drop_blank_options(document: &mut JsonValue, specs: &[ComponentSpec]) {
    let Some(nodes) = document["nodes"].as_array_mut() else {
        return;
    };
    for node in nodes {
        let Some(spec) = node["data"]["componentId"]
            .as_str()
            .and_then(|id| specs.iter().find(|spec| spec.id == id))
        else {
            continue;
        };
        let Some(properties) = node["data"]["properties"].as_object_mut() else {
            continue;
        };
        properties.retain(|name, value| {
            let blank = value.as_str().is_some_and(|text| text.trim().is_empty());
            let needed = spec.properties.iter().any(|property| {
                &property.name == name && property.required && property.default.is_none()
            });
            !blank || needed
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl_metadata::PropertySpec;
    use serde_json::json;

    #[test]
    fn a_blank_optional_value_is_dropped_and_a_required_one_kept() {
        let specs = vec![
            ComponentSpec::new("src.db.postgres", "PostgreSQL table").properties(vec![
                PropertySpec::text("connection").required(),
                PropertySpec::text("table").required(),
                PropertySpec::text("schema"),
            ]),
        ];
        let mut document = json!({ "nodes": [{ "data": {
            "componentId": "src.db.postgres",
            "properties": { "connection": "", "table": "orders", "schema": " " }
        } }] });

        drop_blank_options(&mut document, &specs);

        // A blank required value stays, for the engine to refuse by name.
        assert_eq!(
            document["nodes"][0]["data"]["properties"],
            json!({ "connection": "", "table": "orders" })
        );
    }
}
