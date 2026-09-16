//! Where the data came from and where it went.
//!
//! Derived from a compiled [`Plan`] and nothing else: no run is needed, no
//! files are touched, and the answer for a given document is always the same.
//! That is what makes lineage something you can put in review alongside the
//! diff, rather than something you only learn after a pipeline has written
//! somewhere you did not expect.
//!
//! **Node-level, not column-level, and that distinction is not a detail.** This
//! says "`clean_orders` reads `read_orders`", never "`clean_orders.total` is
//! derived from `read_orders.amount`". Column lineage needs the schema of every
//! relation, which this engine does not collect — DuckDB knows it, we do not
//! ask, and inferring it by parsing the generated SQL would be a guess that
//! looks authoritative. The shape here leaves room for it: [`Node::columns`] is
//! absent rather than empty, so a later phase that does collect schemas can
//! fill it in without changing what the field means.

use crate::plan::{Plan, StageKind};
use etl_metadata::MAIN_PORT;
use serde::Serialize;

/// The graph a plan describes, ready to serialise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Lineage {
    /// The schema of this document, so a reader can tell what it is looking at.
    pub format_version: u32,
    /// Every stage, in execution order.
    pub nodes: Vec<Node>,
    /// Every connection between stages, in the order the plan lists them.
    pub edges: Vec<Edge>,
    /// What the pipeline reads that it did not produce.
    pub inputs: Vec<External>,
    /// What the pipeline writes.
    pub outputs: Vec<External>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    pub id: String,
    pub label: String,
    pub component_id: String,
    /// `source`, `transform`, `sink`, `quality` or `control`.
    pub kind: String,
    /// What it reads from or writes to outside the pipeline, when it does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external: Option<String>,
    /// The watermark column, for an incremental source. Its presence is the
    /// answer to "is this a full load or a partial one", which is the first
    /// thing anyone reading lineage after a surprise wants to know.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incremental_column: Option<String>,
    /// Column-level lineage, when it is ever collected. Always absent today —
    /// see the module comment. Absent rather than `[]`, because an empty list
    /// would claim the question was asked and the answer was "none".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Edge {
    pub from: String,
    pub to: String,
    /// Which output of `from` this edge carries.
    ///
    /// `rejected` is the one worth reading closely: it is the dead-letter
    /// branch of a quality node, so an edge marked this way carries the rows
    /// that *failed* a check. A lineage diagram that drew it the same as the
    /// main flow would be actively misleading.
    pub port: String,
}

/// Something outside the pipeline that it reads or writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct External {
    /// The node that touches it.
    pub node_id: String,
    pub component_id: String,
    /// The path, URI or table.
    pub name: String,
}

/// Derive the lineage of a compiled plan.
pub fn lineage(plan: &Plan) -> Lineage {
    let nodes = plan
        .stages
        .iter()
        .map(|stage| Node {
            id: stage.node_id.clone(),
            label: stage.label.clone(),
            component_id: stage.component_id.clone(),
            kind: kind_name(stage.kind).to_string(),
            external: stage.external.clone(),
            incremental_column: stage.incremental.as_ref().map(|state| state.column.clone()),
            columns: None,
        })
        .collect();

    let edges = plan
        .stages
        .iter()
        .flat_map(|stage| {
            stage.inputs.iter().map(move |input| Edge {
                from: input.node_id.clone(),
                to: stage.node_id.clone(),
                // An unset handle is the default output, which is `main`.
                // Spelling it out here means a consumer never has to know
                // that, and never mistakes a missing port for a special one.
                port: input
                    .source_handle
                    .clone()
                    .unwrap_or_else(|| MAIN_PORT.to_string()),
            })
        })
        .collect();

    // Only a stage that reaches outside the pipeline is an input or an output,
    // and which of the two it is depends on the direction it reaches — which
    // is exactly what its kind says.
    let external = |wanted: StageKind| -> Vec<External> {
        plan.stages
            .iter()
            .filter(|stage| stage.kind == wanted)
            .filter_map(|stage| {
                stage.external.as_ref().map(|name| External {
                    node_id: stage.node_id.clone(),
                    component_id: stage.component_id.clone(),
                    name: name.clone(),
                })
            })
            .collect()
    };

    Lineage {
        format_version: plan.format_version,
        nodes,
        edges,
        inputs: external(StageKind::Source),
        outputs: external(StageKind::Sink),
    }
}

fn kind_name(kind: StageKind) -> &'static str {
    match kind {
        StageKind::Source => "source",
        StageKind::Transform => "transform",
        StageKind::Sink => "sink",
        StageKind::Quality => "quality",
        StageKind::Control => "control",
    }
}

#[cfg(test)]
mod tests;
