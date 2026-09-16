//! Pipeline → DuckDB SQL compiler.
//!
//! The engine takes a [`PipelineDoc`](etl_metadata::PipelineDoc) and produces a
//! [`Plan`]: an ordered list of [`Stage`]s, each carrying the SQL that realises
//! one node. Execution is a separate step — compiling is pure, does not touch
//! the filesystem, and never spawns DuckDB, which is what makes `etl validate`
//! cheap and safe to run against an untrusted document.
//!
//! A document reaches [`compile`] with its `${...}` references already
//! substituted: [`params::resolve`] is a document-to-document step that runs
//! first, so compilation never has to know where a value came from.

pub mod context;
pub mod exec;
pub mod params;
pub mod plan;
pub mod sql;

use thiserror::Error;

pub use context::{Context, ContextError, Contexts};
pub use exec::{run, ExecError, RunOptions, RunReport, StageOutcome};
pub use params::{resolve, ParamError, ParamWarning, Resolved, Resolver};
pub use plan::specs::{registry, Component, Registry};
pub use plan::{
    compile, reject_relation, CountProbe, Input, Plan, Stage, StageKind, Warning, REJECT_SUFFIX,
};
pub use sql::{quote_identifier, quote_literal, quote_path};

/// Everything that can go wrong turning a document into a plan.
///
/// Each variant names the specific node or edge at fault. A validation error
/// that says only "invalid pipeline" makes the user hunt through a canvas for
/// it, so every message here carries the id it is talking about.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EngineError {
    #[error("pipeline has no nodes")]
    EmptyPipeline,

    #[error("two nodes share the id '{id}'; node ids must be unique")]
    DuplicateNodeId { id: String },

    #[error("edge '{edge_id}' points at '{node_id}', which is not a node in this pipeline")]
    UnknownEdgeEndpoint { edge_id: String, node_id: String },

    #[error("edge '{edge_id}' connects '{node_id}' to itself")]
    SelfEdge { edge_id: String, node_id: String },

    #[error("node '{id}' has no component; every node must name a componentId")]
    MissingComponentId { id: String },

    #[error(
        "node '{id}' has component '{component_id}', whose namespace '{namespace}' is not one of \
         src, xf, snk, qa, ctl, code"
    )]
    UnknownNamespace {
        id: String,
        component_id: String,
        namespace: String,
    },

    #[error("these nodes form a cycle: {}", .nodes.join(" → "))]
    Cycle { nodes: Vec<String> },

    #[error("node '{id}' ({component_id}) needs the '{property}' property")]
    MissingProperty {
        id: String,
        component_id: String,
        property: String,
    },

    #[error("node '{id}': property '{property}' {reason}")]
    InvalidProperty {
        id: String,
        property: String,
        reason: String,
    },

    #[error("node '{id}' uses component '{component_id}', which is not implemented yet")]
    UnsupportedComponent { id: String, component_id: String },

    #[error("node '{id}' ({component_id}) takes {expected} input(s) but {actual} are wired to it")]
    WrongInputCount {
        id: String,
        component_id: String,
        expected: usize,
        actual: usize,
    },

    #[error(
        "an edge leaves node '{id}' by a port named '{port}', which '{component_id}' does not \
         have. Its outputs are: {}",
        .known.join(", ")
    )]
    UnknownPort {
        id: String,
        component_id: String,
        port: String,
        known: Vec<String>,
    },

    #[error(
        "node '{id}' ends in '{suffix}', which is reserved: it is the name given to the rows a \
         quality node rejects, so a node using it would collide with one. Rename the node."
    )]
    ReservedNodeId { id: String, suffix: String },
}

impl EngineError {
    /// The node this error is about, when it is about one. Lets the canvas put
    /// the error on the right box instead of in a toast.
    pub fn node_id(&self) -> Option<&str> {
        match self {
            EngineError::DuplicateNodeId { id }
            | EngineError::MissingComponentId { id }
            | EngineError::UnknownNamespace { id, .. }
            | EngineError::MissingProperty { id, .. }
            | EngineError::InvalidProperty { id, .. }
            | EngineError::UnsupportedComponent { id, .. }
            | EngineError::WrongInputCount { id, .. }
            | EngineError::UnknownPort { id, .. }
            | EngineError::ReservedNodeId { id, .. } => Some(id),
            EngineError::UnknownEdgeEndpoint { node_id, .. }
            | EngineError::SelfEdge { node_id, .. } => Some(node_id),
            EngineError::Cycle { nodes } => nodes.first().map(String::as_str),
            EngineError::EmptyPipeline => None,
        }
    }
}
