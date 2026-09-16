//! The pipeline document model.
//!
//! This crate owns the JSON contract that every other part of the product
//! speaks: the canvas writes it, the CLI reads it, the engine compiles it.
//! Keeping it in one dependency-light crate is what lets the GUI and the
//! headless runner stay interchangeable on the same file.
//!
//! Two properties matter more than convenience here:
//!
//! * **Round-trip fidelity.** A document written by a newer version must
//!   survive being loaded and re-saved by an older one. Every struct carries
//!   an `extra` catch-all so unrecognised keys are preserved rather than
//!   silently dropped — a saved pipeline is a user's file, not our scratch
//!   space.
//! * **A declared format version.** [`PipelineDoc::format_version`] is carried
//!   on the struct rather than checked only by the file reader, because a
//!   document reaches the engine from several directions and a version checked
//!   at most of them reads as covered while leaving a hole.

pub mod component;

pub use component::{
    ComponentSpec, ControlKind, Namespace, PortSpec, PropertySpec, PropertyType, MAIN_PORT,
    REJECTED_PORT,
};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

/// Unrecognised JSON keys, captured so they survive a load/save cycle.
pub type Extra = BTreeMap<String, JsonValue>;

/// A node's column list. Populated by schema inspection, absent until then.
pub type Schema = Vec<Column>;

/// The format version this crate writes.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Cell types
// ---------------------------------------------------------------------------

/// The primitive cell types the product understands.
///
/// Serialised in snake_case so the token on the wire is the same string the
/// frontend's `DataType` union uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    Date,
    Timestamp,
    Time,
    Decimal,
    Json,
    Binary,
    /// DuckDB's native `GEOMETRY`, available once the spatial extension loads.
    Geometry,
}

impl DataType {
    /// The wire token, also used in user-facing type pickers.
    pub fn name(self) -> &'static str {
        match self {
            DataType::String => "string",
            DataType::Int32 => "int32",
            DataType::Int64 => "int64",
            DataType::Float32 => "float32",
            DataType::Float64 => "float64",
            DataType::Bool => "bool",
            DataType::Date => "date",
            DataType::Timestamp => "timestamp",
            DataType::Time => "time",
            DataType::Decimal => "decimal",
            DataType::Json => "json",
            DataType::Binary => "binary",
            DataType::Geometry => "geometry",
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// One column of a node's output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
    #[serde(default = "default_true")]
    pub nullable: bool,
    #[serde(
        default,
        rename = "primaryKey",
        skip_serializing_if = "Option::is_none"
    )]
    pub primary_key: Option<bool>,
    /// Per-column parse format for date and timestamp columns, so several date
    /// columns can each parse differently in one read. `None` leaves parsing to
    /// the source's own detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// What the column holds — `pii` and `secret` are the values the engine
    /// acts on when masking; anything else is documentation for the reader.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<String>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: Extra,
}

fn default_true() -> bool {
    true
}

fn is_zero(v: &u32) -> bool {
    *v == 0
}

// ---------------------------------------------------------------------------
// Graph
// ---------------------------------------------------------------------------

/// Canvas coordinates. Carried through the engine untouched so a round-trip
/// through the CLI does not rearrange someone's layout.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

/// One node on the canvas.
///
/// Note the two different notions of "type": [`PipelineNode::flow_type`] is the
/// canvas's coarse rendering kind (`source` / `transform` / `sink`), while the
/// component that actually decides behaviour is [`NodeData::component_id`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineNode {
    pub id: String,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub flow_type: Option<String>,
    pub position: Position,
    pub data: NodeData,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: Extra,
}

/// Everything about a node that is not its place on the canvas.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeData {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subtitle: Option<String>,
    /// The namespaced component this node runs, e.g. `src.file.csv`.
    #[serde(
        default,
        rename = "componentId",
        skip_serializing_if = "Option::is_none"
    )]
    pub component_id: Option<String>,
    /// The node's configuration, shaped by the component's property schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
    #[serde(
        default,
        rename = "sampleRows",
        skip_serializing_if = "Option::is_none"
    )]
    pub sample_rows: Option<Vec<JsonValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled: Option<bool>,
    /// How this node's output is realised: `auto`, `view`, `memory` or `disk`.
    ///
    /// Carried as text rather than an enum so a document written by a newer
    /// version keeps whatever it says. The engine parses it and warns about
    /// anything it does not recognise, which is safe to do because the choice
    /// changes only how the work is done, never what the answer is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialize: Option<String>,
    /// A friendly SQL relation name for this node's output, so raw SQL nodes
    /// downstream can say `FROM orders` instead of `FROM n1`. Edge wiring still
    /// keys off the node id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    /// How this node behaves when it fails, and what it is allowed to consume.
    ///
    /// Beside `materialize` rather than inside `properties` for the same
    /// reason: this is how the node is *run*, not what the component does, and
    /// every component takes the same four settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<NodePolicy>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: Extra,
}

/// How a node behaves when it fails, and what it may consume.
///
/// A node carrying any of these is asking to be run on its own rather than as
/// part of one batched script — retrying a stage means addressing that stage —
/// so a plan holding one of these runs through a session. See
/// `docs/DECISION_execution_model.md`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodePolicy {
    /// How many times to run this stage again if it fails. Zero, the default,
    /// means run it once.
    #[serde(
        default,
        rename = "retryAttempts",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_attempts: Option<u32>,
    /// How long to wait before the first retry, doubling each time after.
    #[serde(
        default,
        rename = "retryBackoffMs",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_backoff_ms: Option<u64>,
    /// Let the rest of the run continue when this stage fails.
    ///
    /// The run still ends failed — this changes how much of it happens, not
    /// whether it counts as a success. A stage that depends on a failed one is
    /// skipped rather than run against a relation that was never created.
    #[serde(
        default,
        rename = "continueOnFailure",
        skip_serializing_if = "Option::is_none"
    )]
    pub continue_on_failure: Option<bool>,
    /// A memory ceiling for this stage, applied to the session around it.
    #[serde(
        default,
        rename = "memoryLimitMb",
        skip_serializing_if = "Option::is_none"
    )]
    pub memory_limit_mb: Option<u64>,
}

impl NodePolicy {
    /// Whether this says anything at all. An all-default policy is the same as
    /// none, and must not be what tips a plan onto the session path.
    pub fn is_default(&self) -> bool {
        self == &NodePolicy::default()
    }
}

impl NodeData {
    /// Whether the node is switched off on the canvas. Absent means enabled.
    pub fn is_disabled(&self) -> bool {
        self.disabled.unwrap_or(false)
    }

    /// The node's own property object, or JSON `null` when it has none, so
    /// callers can index into it without unwrapping first.
    pub fn properties_or_null(&self) -> &JsonValue {
        const NULL: &JsonValue = &JsonValue::Null;
        self.properties.as_ref().unwrap_or(NULL)
    }
}

/// A wire between two nodes.
///
/// `source_handle` and `target_handle` name the ports. A quality node's reject
/// rows leave through a second source handle, which is why the handle is part
/// of the edge rather than implied by the node pair.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineEdge {
    pub id: String,
    pub source: String,
    pub target: String,
    #[serde(
        default,
        rename = "sourceHandle",
        skip_serializing_if = "Option::is_none"
    )]
    pub source_handle: Option<String>,
    #[serde(
        default,
        rename = "targetHandle",
        skip_serializing_if = "Option::is_none"
    )]
    pub target_handle: Option<String>,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub edge_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<EdgeData>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeData {
    #[serde(
        default,
        rename = "connectionType",
        skip_serializing_if = "Option::is_none"
    )]
    pub connection_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Predicate guarding this branch, for control-flow edges.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: Extra,
}

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// One declared parameter in a pipeline's parameter contract.
///
/// Declaring parameters is optional. When a pipeline declares none, any
/// unresolved `${name}` is prompted for as a string; when it does declare them,
/// they are validated once before compilation so every entry point — canvas,
/// CLI, scheduler, agent — gets the same answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParameterSpec {
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub param_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: Extra,
}

// ---------------------------------------------------------------------------
// Document
// ---------------------------------------------------------------------------

/// A whole pipeline: the file on disk and the payload the engine compiles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineDoc {
    /// Which format this document is in. `0` — the default — means a document
    /// written before the marker existed, and is readable as-is.
    #[serde(default, rename = "formatVersion", skip_serializing_if = "is_zero")]
    pub format_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub nodes: Vec<PipelineNode>,
    #[serde(default)]
    pub edges: Vec<PipelineEdge>,
    /// Which admission pool this pipeline queues in. Empty is the default pool.
    ///
    /// Typed here rather than dug out of raw JSON by each caller, so an agent or
    /// an API call cannot bypass the limits a scheduled run observes.
    #[serde(
        default,
        rename = "resourcePool",
        skip_serializing_if = "String::is_empty"
    )]
    pub resource_pool: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, ParameterSpec>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: Extra,
}

impl PipelineDoc {
    /// Parse a document from JSON text.
    pub fn from_json(text: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(text)
    }

    /// Render the document back to pretty-printed JSON.
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Look up a node by id.
    pub fn node(&self, id: &str) -> Option<&PipelineNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// The nodes that are actually going to run.
    pub fn enabled_nodes(&self) -> impl Iterator<Item = &PipelineNode> {
        self.nodes.iter().filter(|n| !n.data.is_disabled())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../samples/pipelines/csv_to_parquet.json");

    fn value(text: &str) -> JsonValue {
        serde_json::from_str(text).expect("sample is valid JSON")
    }

    #[test]
    fn sample_pipeline_parses() {
        let doc = PipelineDoc::from_json(SAMPLE).expect("sample parses");

        assert_eq!(doc.format_version, CURRENT_FORMAT_VERSION);
        assert_eq!(doc.nodes.len(), 3);
        assert_eq!(doc.edges.len(), 2);
        assert_eq!(
            doc.node("filter_recent")
                .unwrap()
                .data
                .component_id
                .as_deref(),
            Some("xf.filter")
        );
    }

    #[test]
    fn round_trip_preserves_the_document() {
        let doc = PipelineDoc::from_json(SAMPLE).unwrap();
        let rendered = doc.to_json_pretty().unwrap();

        assert_eq!(
            value(&rendered),
            value(SAMPLE),
            "re-serialising the sample changed it"
        );
    }

    #[test]
    fn round_trip_is_stable_on_a_second_pass() {
        let once = PipelineDoc::from_json(SAMPLE)
            .unwrap()
            .to_json_pretty()
            .unwrap();
        let twice = PipelineDoc::from_json(&once)
            .unwrap()
            .to_json_pretty()
            .unwrap();

        assert_eq!(once, twice);
    }

    #[test]
    fn unknown_keys_survive_a_round_trip() {
        // A document written by a future version: keys we do not model, at every
        // level. Dropping any of them would corrupt a user's file on save.
        let future = r#"{
          "formatVersion": 2,
          "unknownTopLevel": {"a": 1},
          "nodes": [{
            "id": "n1",
            "type": "source",
            "position": {"x": 0.0, "y": 0.0},
            "unknownNodeKey": true,
            "data": {
              "label": "Orders",
              "componentId": "src.file.csv",
              "properties": {"path": "orders.csv"},
              "unknownDataKey": ["keep", "me"]
            }
          }],
          "edges": []
        }"#;

        let doc = PipelineDoc::from_json(future).unwrap();
        let out = value(&doc.to_json_pretty().unwrap());

        assert_eq!(out["unknownTopLevel"]["a"], 1);
        assert_eq!(out["nodes"][0]["unknownNodeKey"], true);
        assert_eq!(out["nodes"][0]["data"]["unknownDataKey"][0], "keep");
        assert_eq!(doc.format_version, 2, "a newer format version is preserved");
    }

    #[test]
    fn absent_format_version_defaults_to_zero_and_is_not_written_back() {
        let legacy = r#"{"nodes": [], "edges": []}"#;
        let doc = PipelineDoc::from_json(legacy).unwrap();

        assert_eq!(doc.format_version, 0);
        assert!(
            !doc.to_json_pretty().unwrap().contains("formatVersion"),
            "a document with no version marker should not gain one on save"
        );
    }

    #[test]
    fn columns_default_to_nullable() {
        let col: Column = serde_json::from_str(r#"{"name": "id", "type": "int64"}"#).unwrap();

        assert!(col.nullable);
        assert_eq!(col.data_type, DataType::Int64);
        assert_eq!(col.data_type.name(), "int64");
    }

    #[test]
    fn disabled_nodes_are_excluded_from_the_run() {
        let doc = PipelineDoc::from_json(SAMPLE).unwrap();
        assert_eq!(doc.enabled_nodes().count(), 3);

        let with_disabled = SAMPLE.replace(
            r#""label": "Recent orders""#,
            r#""label": "Recent orders", "disabled": true"#,
        );
        let doc = PipelineDoc::from_json(&with_disabled).unwrap();
        assert_eq!(doc.enabled_nodes().count(), 2);
    }

    #[test]
    fn every_data_type_round_trips_through_its_wire_token() {
        let all = [
            DataType::String,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Bool,
            DataType::Date,
            DataType::Timestamp,
            DataType::Time,
            DataType::Decimal,
            DataType::Json,
            DataType::Binary,
            DataType::Geometry,
        ];

        for dt in all {
            let json = serde_json::to_string(&dt).unwrap();
            assert_eq!(json, format!("\"{}\"", dt.name()));
            assert_eq!(serde_json::from_str::<DataType>(&json).unwrap(), dt);
        }
    }
}
