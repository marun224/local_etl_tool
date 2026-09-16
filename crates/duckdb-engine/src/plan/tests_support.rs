//! Graph builders shared by the plan and builder test modules.

use crate::plan::{compile, Plan};
use crate::EngineError;
use etl_metadata::{NodeData, PipelineDoc, PipelineEdge, PipelineNode, Position};
use serde_json::Value as JsonValue;

pub(crate) fn node(id: &str, component_id: &str, properties: JsonValue) -> PipelineNode {
    PipelineNode {
        id: id.to_string(),
        flow_type: None,
        position: Position { x: 0.0, y: 0.0 },
        data: NodeData {
            label: id.to_string(),
            subtitle: None,
            component_id: Some(component_id.to_string()),
            properties: Some(properties),
            schema: None,
            sample_rows: None,
            disabled: None,
            materialize: None,
            alias: None,
            extra: Default::default(),
        },
        extra: Default::default(),
    }
}

pub(crate) fn edge(id: &str, source: &str, target: &str, handle: Option<&str>) -> PipelineEdge {
    PipelineEdge {
        id: id.to_string(),
        source: source.to_string(),
        target: target.to_string(),
        source_handle: Some("main".to_string()),
        target_handle: handle.map(str::to_string),
        edge_type: None,
        data: None,
        extra: Default::default(),
    }
}

/// An edge that leaves a named output port, for wiring a quality node's two
/// sides. [`edge`] always leaves `main`, which is every other component.
pub(crate) fn edge_from(
    id: &str,
    source: &str,
    source_handle: &str,
    target: &str,
    target_handle: Option<&str>,
) -> PipelineEdge {
    PipelineEdge {
        source_handle: Some(source_handle.to_string()),
        ..edge(id, source, target, target_handle)
    }
}

pub(crate) fn document(nodes: Vec<PipelineNode>, edges: Vec<PipelineEdge>) -> PipelineDoc {
    PipelineDoc {
        format_version: 1,
        name: None,
        nodes,
        edges,
        resource_pool: String::new(),
        parameters: Default::default(),
        extra: Default::default(),
    }
}

/// The generated SQL for one node.
pub(crate) fn sql_of<'a>(plan: &'a Plan, node_id: &str) -> &'a str {
    &plan.stage(node_id).expect("stage exists").sql
}

// ---------------------------------------------------------------------------
// One node
// ---------------------------------------------------------------------------

pub(crate) fn compile_one_result(
    component_id: &str,
    properties: JsonValue,
    alias: Option<&str>,
) -> Result<Plan, EngineError> {
    let mut only = node("n", component_id, properties);
    only.data.alias = alias.map(str::to_string);

    compile(&document(vec![only], vec![]))
}

pub(crate) fn compile_one(component_id: &str, properties: JsonValue) -> Plan {
    compile_one_result(component_id, properties, None).expect("compiles")
}

pub(crate) fn compile_one_aliased(component_id: &str, properties: JsonValue, alias: &str) -> Plan {
    compile_one_result(component_id, properties, Some(alias)).expect("compiles")
}

// ---------------------------------------------------------------------------
// Two nodes, wired a → b
// ---------------------------------------------------------------------------

pub(crate) fn compile_two_result(
    first: (&str, JsonValue),
    second: (&str, JsonValue),
) -> Result<Plan, EngineError> {
    compile(&document(
        vec![node("a", first.0, first.1), node("b", second.0, second.1)],
        vec![edge("e0", "a", "b", Some("in"))],
    ))
}

pub(crate) fn compile_two(first: (&str, JsonValue), second: (&str, JsonValue)) -> Plan {
    compile_two_result(first, second).expect("compiles")
}

/// Two nodes wired a → b, with `b` asking for a materialisation mode.
pub(crate) fn compile_materialized(mode: Option<&str>, alias: Option<&str>) -> Plan {
    let mut second = node("b", "xf.distinct", serde_json::json!({}));
    second.data.materialize = mode.map(str::to_string);
    second.data.alias = alias.map(str::to_string);

    compile(&document(
        vec![
            node("a", "src.file.csv", serde_json::json!({ "path": "in.csv" })),
            second,
        ],
        vec![edge("e0", "a", "b", Some("in"))],
    ))
    .expect("compiles")
}

// ---------------------------------------------------------------------------
// Two sources joined
// ---------------------------------------------------------------------------

/// Build `left`/`right` sources feeding a join node `j`.
///
/// `handles` optionally names the target handle each side arrives at, so a
/// test can prove that handles override edge order.
fn join_document(properties: JsonValue, handles: Option<(&str, &str)>) -> PipelineDoc {
    let (left_handle, right_handle) = match handles {
        Some((left, right)) => (Some(left), Some(right)),
        None => (Some("left"), Some("right")),
    };

    document(
        vec![
            node(
                "left",
                "src.file.csv",
                serde_json::json!({ "path": "l.csv" }),
            ),
            node(
                "right",
                "src.file.csv",
                serde_json::json!({ "path": "r.csv" }),
            ),
            node("j", "xf.join", properties),
        ],
        vec![
            edge("e0", "left", "j", left_handle),
            edge("e1", "right", "j", right_handle),
        ],
    )
}

/// Two sources feeding one two-input transform `j`, for the set operations.
pub(crate) fn compile_two_sided(component_id: &str, properties: JsonValue) -> String {
    let plan = compile(&document(
        vec![
            node(
                "left",
                "src.file.csv",
                serde_json::json!({ "path": "l.csv" }),
            ),
            node(
                "right",
                "src.file.csv",
                serde_json::json!({ "path": "r.csv" }),
            ),
            node("j", component_id, properties),
        ],
        vec![
            edge("e0", "left", "j", Some("left")),
            edge("e1", "right", "j", Some("right")),
        ],
    ))
    .expect("compiles");

    sql_of(&plan, "j").to_string()
}

pub(crate) fn compile_join(properties: JsonValue, handles: Option<(&str, &str)>) -> String {
    let plan = compile(&join_document(properties, handles)).expect("compiles");

    sql_of(&plan, "j").to_string()
}

pub(crate) fn join_err(properties: JsonValue, handles: Option<(&str, &str)>) -> EngineError {
    compile(&join_document(properties, handles)).expect_err("expected this to fail")
}
