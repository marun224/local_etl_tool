//! Document → ordered plan.
//!
//! [`compile`] validates the graph and topologically sorts it into the order
//! the executor will run. Two properties are deliberate:
//!
//! * **Deterministic output.** Ties in the topological sort break by document
//!   order, so the same document always compiles to the same plan. A plan that
//!   reorders between runs makes the generated SQL unreviewable and turns
//!   golden-file tests into coin flips.
//! * **Validation is complete before ordering.** Every node is checked even if
//!   it is later dropped as disabled, so `validate` reports the problem in a
//!   switched-off node rather than going quiet until someone switches it on.

pub(crate) mod builders;
pub mod specs;

use crate::EngineError;
use etl_metadata::{PipelineDoc, PipelineNode};
use serde_json::Value as JsonValue;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};

/// What a stage does, taken from its component's namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StageKind {
    /// `src.*` — reads data in.
    Source,
    /// `xf.*` and `code.*` — reshapes data.
    Transform,
    /// `snk.*` — writes data out. Only sinks pull; everything upstream is lazy.
    Sink,
    /// `qa.*` — validates rows, with a second output for the ones that fail.
    Quality,
    /// `ctl.*` — control flow and side effects rather than a relation.
    Control,
}

impl StageKind {
    fn from_component_id(node_id: &str, component_id: &str) -> Result<Self, EngineError> {
        let namespace = component_id.split('.').next().unwrap_or("");

        match namespace {
            "src" => Ok(StageKind::Source),
            "xf" | "code" => Ok(StageKind::Transform),
            "snk" => Ok(StageKind::Sink),
            "qa" => Ok(StageKind::Quality),
            "ctl" => Ok(StageKind::Control),
            other => Err(EngineError::UnknownNamespace {
                id: node_id.to_string(),
                component_id: component_id.to_string(),
                namespace: other.to_string(),
            }),
        }
    }

    /// Whether this stage produces a relation downstream nodes can select from.
    pub fn produces_relation(self) -> bool {
        !matches!(self, StageKind::Sink | StageKind::Control)
    }
}

/// How a stage's output is realised.
///
/// The choice never changes the answer, only the work done to get it — which
/// is why an unrecognised value is a warning and a fall back to [`Auto`], not
/// a refusal to run.
///
/// [`Auto`]: Materialize::Auto
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Materialize {
    /// Let the engine decide. Today that means a view; a real heuristic needs
    /// statistics the engine does not collect yet.
    #[default]
    Auto,
    /// A lazy view. Nothing is computed until a sink pulls, and a filter above
    /// a source pushes down into the source's own scan.
    View,
    /// A temp table. The work happens once, at this point in the plan, and is
    /// held in memory — worth it for a relation several stages read.
    Memory,
    /// Spilled to a temporary Parquet file, then read back. For a relation too
    /// large to sit in memory but too expensive to recompute.
    Disk,
}

impl Materialize {
    /// Parse the token from a document. `None` for anything unrecognised, which
    /// the caller turns into a warning.
    pub fn parse(token: &str) -> Option<Self> {
        match token.trim() {
            "auto" => Some(Materialize::Auto),
            "view" => Some(Materialize::View),
            "memory" => Some(Materialize::Memory),
            "disk" => Some(Materialize::Disk),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Materialize::Auto => "auto",
            Materialize::View => "view",
            Materialize::Memory => "memory",
            Materialize::Disk => "disk",
        }
    }

    /// Whether this mode computes its relation once rather than on each read.
    pub fn is_materialised(self) -> bool {
        matches!(self, Materialize::Memory | Materialize::Disk)
    }
}

/// Where a disk-materialised stage spills to, relative to the working
/// directory.
///
/// Relative and derived from the node id so that [`compile`] stays pure: the
/// path in the generated SQL is the path that will be written, with nothing
/// substituted in later. The cost is that two concurrent runs of the same
/// pipeline in the same directory would share spill files — fine for a CLI,
/// and something Phase 8's scheduler will have to give a run id.
pub const SPILL_DIR: &str = ".etl/tmp";

/// One upstream connection into a stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Input {
    /// The upstream node id, which is also the name of its relation.
    pub node_id: String,
    /// Which output port of the upstream node this came from. A quality node's
    /// rejected rows leave through a different handle than its accepted ones,
    /// so this is what tells the two apart.
    pub source_handle: Option<String>,
    /// Which input port of this stage it arrives at.
    pub target_handle: Option<String>,
}

/// One node, lowered and placed in execution order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stage {
    pub node_id: String,
    pub component_id: String,
    pub label: String,
    pub kind: StageKind,
    /// The complete statement(s) that realise this stage — a `CREATE OR
    /// REPLACE TEMP VIEW` for anything producing a relation, a `COPY ... TO`
    /// for a sink. Held whole rather than as a fragment so the plan view can
    /// show exactly what will run.
    pub sql: String,
    /// The count query that follows this stage, so the run can report rows.
    /// `None` for stages that neither produce a relation nor read one.
    pub count_sql: Option<String>,
    /// Upstream connections, in document order.
    pub inputs: Vec<Input>,
    /// The single upstream relation this stage reads from, when there is
    /// exactly one. The executor uses it to attribute a row count. Always the
    /// upstream **node id**, never its alias, because the node id is the
    /// relation the engine actually creates.
    pub from: Option<String>,
    /// A friendlier relation name for this stage's output, so raw SQL nodes
    /// downstream can say `FROM orders` instead of `FROM read_orders`.
    pub alias: Option<String>,
    /// For a sink: where it writes. The executor needs this before running, to
    /// create the destination directory and to honour `sink_mode`.
    pub sink_path: Option<String>,
    /// For a sink: `overwrite` (the default) or `error_if_exists`.
    pub sink_mode: Option<String>,
    /// How this stage's relation is realised.
    pub materialize: Materialize,
    /// Where a `disk` stage spills to, so the executor can clear it up.
    pub spill_path: Option<String>,
    /// DuckDB extensions this stage's component needs, straight from its spec.
    pub requires_extensions: Vec<String>,
}

impl Stage {
    /// The name of the relation this stage creates. Edge wiring and generated
    /// SQL both key off the node id; the alias is an additional view.
    pub fn relation_name(&self) -> &str {
        &self.node_id
    }
}

/// Something worth telling the user that does not stop the pipeline running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Warning {
    /// A node is switched off on the canvas.
    DisabledSkipped { id: String },
    /// A node was dropped because something it depends on is switched off.
    /// Running it would reference a relation that no longer exists.
    DroppedDownstreamOfDisabled { id: String, disabled: String },
    /// A node is wired to nothing at either end, so it cannot contribute.
    Orphan { id: String },
    /// A node sets a property its component does not define. Usually a typo;
    /// also what a document from a newer version looks like, which is why it
    /// does not stop the run.
    UnknownProperty { id: String, property: String },
    /// A node asks for a materialisation mode this engine does not know. It
    /// falls back to `auto`, which is safe: the mode changes how the work is
    /// done, never what the answer is.
    UnknownMaterialize { id: String, value: String },
    /// Nothing in the plan writes anything. Every non-sink stage is a lazy
    /// view, so a pipeline with no sink compiles fine and then does nothing.
    NoSink,
}

/// Emitted after the extension prelude when counts are on, so that a failed
/// prelude is distinguishable from a failed first stage.
///
/// `LOAD` returns no rows and therefore prints no JSON, so without this probe
/// both failures look identical to the executor: nothing arrived either way.
/// With it, "nothing arrived" means the prelude and nothing else.
pub const PRELUDE_PROBE: &str = "SELECT 0 AS n;";

/// An ordered, validated pipeline ready for execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub format_version: u32,
    /// Stages in execution order.
    pub stages: Vec<Stage>,
    pub warnings: Vec<Warning>,
}

impl Plan {
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    pub fn stage(&self, node_id: &str) -> Option<&Stage> {
        self.stages.iter().find(|s| s.node_id == node_id)
    }

    /// Where this plan spills to, in plan order. Empty unless some node asked
    /// for `disk`.
    pub fn spills(&self) -> Vec<&str> {
        self.stages
            .iter()
            .filter_map(|stage| stage.spill_path.as_deref())
            .collect()
    }

    /// The stages that actually write something.
    pub fn sinks(&self) -> impl Iterator<Item = &Stage> {
        self.stages.iter().filter(|s| s.kind == StageKind::Sink)
    }

    /// Every DuckDB extension this plan needs, sorted and deduplicated.
    ///
    /// Derived from the stages rather than stored beside them, so the list can
    /// never disagree with the components actually in the plan. The canvas
    /// reads it to warn that a pipeline needs `postgres` *before* someone
    /// starts a run, and Phase 9 reads it to know which extension files to
    /// vendor.
    pub fn extensions(&self) -> Vec<&str> {
        let mut found: Vec<&str> = self
            .stages
            .iter()
            .flat_map(|stage| stage.requires_extensions.iter().map(String::as_str))
            .collect();

        found.sort_unstable();
        found.dedup();
        found
    }

    /// The stages that need an extension the plan loads, for a message that can
    /// name the node rather than only the extension.
    pub fn stages_needing<'a>(&'a self, extension: &'a str) -> impl Iterator<Item = &'a Stage> {
        self.stages
            .iter()
            .filter(move |s| s.requires_extensions.iter().any(|e| e == extension))
    }

    /// Whether [`Plan::script`] will emit [`PRELUDE_PROBE`]. The executor needs
    /// this to know whether the first row count belongs to the prelude.
    pub fn has_prelude_probe(&self, counts: bool) -> bool {
        counts && !self.extensions().is_empty()
    }

    /// Node ids in execution order — the useful form for tests and for the
    /// canvas's "step 3 of 7" display.
    pub fn order(&self) -> Vec<&str> {
        self.stages.iter().map(|s| s.node_id.as_str()).collect()
    }

    /// The whole plan as one SQL script.
    ///
    /// It has to be one script: temp views live in a session, and every `-c`
    /// invocation is a fresh process, so running stages one at a time would
    /// throw away every view between them.
    ///
    /// With `counts` on, each stage is followed by a `SELECT count(*)`. That
    /// buys per-node row counts and — because DuckDB emits one JSON array per
    /// statement that returns rows — tells the executor exactly which stage
    /// failed. It costs something real: a count forces its view to
    /// materialise, so the lazy chain is evaluated for the probe as well as
    /// for the sink. Turn it off for the fastest possible run.
    pub fn script(&self, counts: bool) -> String {
        let mut script = String::new();
        let extensions = self.extensions();

        // LOAD, never INSTALL: a run must not depend on reaching the internet,
        // and Phase 9 ships the extension files alongside the binary.
        if !extensions.is_empty() {
            script.push_str("-- extensions\n");
            for extension in &extensions {
                script.push_str(&format!("LOAD {extension};\n"));
            }
            if counts {
                script.push_str(PRELUDE_PROBE);
                script.push('\n');
            }
            script.push('\n');
        }

        for stage in &self.stages {
            script.push_str(&format!("-- {} ({})\n", stage.node_id, stage.component_id));
            script.push_str(&stage.sql);
            script.push('\n');

            if counts {
                if let Some(count_sql) = &stage.count_sql {
                    script.push_str(count_sql);
                    script.push('\n');
                }
            }

            script.push('\n');
        }

        script
    }

    /// Stages that will emit a row count when the script runs with counts on,
    /// in the order their counts appear on stdout.
    pub fn counted_stages(&self) -> impl Iterator<Item = &Stage> {
        self.stages.iter().filter(|s| s.count_sql.is_some())
    }
}

/// Validate a document and order it for execution.
pub fn compile(doc: &PipelineDoc) -> Result<Plan, EngineError> {
    if doc.nodes.is_empty() {
        return Err(EngineError::EmptyPipeline);
    }

    let index = build_index(&doc.nodes)?;
    let kinds = classify(&doc.nodes)?;
    let edges = resolve_edges(doc, &index)?;

    let mut warnings = Vec::new();
    let dropped = drop_disabled(doc, &edges, &mut warnings);

    let order = topological_order(doc, &edges, &dropped)?;

    let mut unknown_properties = Vec::new();
    let mut unknown_materialize = Vec::new();
    let stages = build_stages(
        doc,
        &kinds,
        &edges,
        &order,
        &mut unknown_properties,
        &mut unknown_materialize,
    )?;

    warnings.extend(
        unknown_properties
            .into_iter()
            .map(|unknown| Warning::UnknownProperty {
                id: unknown.node_id,
                property: unknown.property,
            }),
    );
    warnings.extend(unknown_materialize);

    collect_graph_warnings(doc, &edges, &dropped, &stages, &mut warnings);

    Ok(Plan {
        format_version: doc.format_version,
        stages,
        warnings,
    })
}

/// An edge with both endpoints already resolved to node positions.
struct ResolvedEdge {
    source: usize,
    target: usize,
    source_handle: Option<String>,
    target_handle: Option<String>,
}

fn build_index(nodes: &[PipelineNode]) -> Result<HashMap<&str, usize>, EngineError> {
    let mut index = HashMap::with_capacity(nodes.len());

    for (position, node) in nodes.iter().enumerate() {
        if index.insert(node.id.as_str(), position).is_some() {
            return Err(EngineError::DuplicateNodeId {
                id: node.id.clone(),
            });
        }
    }

    Ok(index)
}

/// Classify every node, including disabled ones, so validation is complete.
fn classify(nodes: &[PipelineNode]) -> Result<Vec<StageKind>, EngineError> {
    nodes
        .iter()
        .map(|node| {
            let component_id = node.data.component_id.as_deref().ok_or_else(|| {
                EngineError::MissingComponentId {
                    id: node.id.clone(),
                }
            })?;

            StageKind::from_component_id(&node.id, component_id)
        })
        .collect()
}

fn resolve_edges(
    doc: &PipelineDoc,
    index: &HashMap<&str, usize>,
) -> Result<Vec<ResolvedEdge>, EngineError> {
    let mut resolved = Vec::with_capacity(doc.edges.len());

    for edge in &doc.edges {
        let endpoint = |node_id: &String| {
            index
                .get(node_id.as_str())
                .copied()
                .ok_or_else(|| EngineError::UnknownEdgeEndpoint {
                    edge_id: edge.id.clone(),
                    node_id: node_id.clone(),
                })
        };

        let source = endpoint(&edge.source)?;
        let target = endpoint(&edge.target)?;

        if source == target {
            return Err(EngineError::SelfEdge {
                edge_id: edge.id.clone(),
                node_id: edge.source.clone(),
            });
        }

        resolved.push(ResolvedEdge {
            source,
            target,
            source_handle: edge.source_handle.clone(),
            target_handle: edge.target_handle.clone(),
        });
    }

    Ok(resolved)
}

/// Remove switched-off nodes and everything that depends on them.
///
/// The cascade matters: a transform whose only input is disabled would compile
/// to SQL selecting from a relation that was never created. Dropping it with a
/// warning is honest; leaving it in fails at runtime with a DuckDB error about
/// a missing table, which tells the user nothing about the switch they flipped.
fn drop_disabled(
    doc: &PipelineDoc,
    edges: &[ResolvedEdge],
    warnings: &mut Vec<Warning>,
) -> Vec<bool> {
    let mut dropped = vec![false; doc.nodes.len()];
    let mut queue: VecDeque<usize> = VecDeque::new();

    for (position, node) in doc.nodes.iter().enumerate() {
        if node.data.is_disabled() {
            dropped[position] = true;
            queue.push_back(position);
            warnings.push(Warning::DisabledSkipped {
                id: node.id.clone(),
            });
        }
    }

    while let Some(current) = queue.pop_front() {
        for edge in edges.iter().filter(|e| e.source == current) {
            if !dropped[edge.target] {
                dropped[edge.target] = true;
                queue.push_back(edge.target);
                warnings.push(Warning::DroppedDownstreamOfDisabled {
                    id: doc.nodes[edge.target].id.clone(),
                    disabled: doc.nodes[current].id.clone(),
                });
            }
        }
    }

    dropped
}

/// Kahn's algorithm, with ties broken by document order so the plan is stable.
fn topological_order(
    doc: &PipelineDoc,
    edges: &[ResolvedEdge],
    dropped: &[bool],
) -> Result<Vec<usize>, EngineError> {
    let kept = |position: usize| !dropped[position];
    let live: Vec<&ResolvedEdge> = edges
        .iter()
        .filter(|e| kept(e.source) && kept(e.target))
        .collect();

    let mut in_degree = vec![0usize; doc.nodes.len()];
    for edge in &live {
        in_degree[edge.target] += 1;
    }

    // Reverse turns the max-heap into a min-heap, so the lowest document
    // position is always taken first.
    let mut ready: BinaryHeap<Reverse<usize>> = (0..doc.nodes.len())
        .filter(|&position| kept(position) && in_degree[position] == 0)
        .map(Reverse)
        .collect();

    let expected = (0..doc.nodes.len()).filter(|&p| kept(p)).count();
    let mut order = Vec::with_capacity(expected);

    while let Some(Reverse(current)) = ready.pop() {
        order.push(current);

        for edge in live.iter().filter(|e| e.source == current) {
            in_degree[edge.target] -= 1;
            if in_degree[edge.target] == 0 {
                ready.push(Reverse(edge.target));
            }
        }
    }

    if order.len() != expected {
        // Whatever still has an in-degree is either in a cycle or downstream of
        // one. Report in document order so the message is stable.
        let nodes = (0..doc.nodes.len())
            .filter(|&p| kept(p) && !order.contains(&p))
            .map(|p| doc.nodes[p].id.clone())
            .collect();

        return Err(EngineError::Cycle { nodes });
    }

    Ok(order)
}

fn build_stages(
    doc: &PipelineDoc,
    kinds: &[StageKind],
    edges: &[ResolvedEdge],
    order: &[usize],
    unknown_properties: &mut Vec<specs::UnknownProperty>,
    unknown_materialize: &mut Vec<Warning>,
) -> Result<Vec<Stage>, EngineError> {
    order
        .iter()
        .map(|&position| {
            let node = &doc.nodes[position];
            let kind = kinds[position];
            let component_id = node.data.component_id.clone().unwrap_or_default();

            let inputs: Vec<Input> = edges
                .iter()
                .filter(|e| e.target == position)
                .map(|e| Input {
                    node_id: doc.nodes[e.source].id.clone(),
                    source_handle: e.source_handle.clone(),
                    target_handle: e.target_handle.clone(),
                })
                .collect();

            let from = match inputs.as_slice() {
                [only] => Some(only.node_id.clone()),
                _ => None,
            };

            // Everything the component needs is derived from its spec: which
            // properties are required, what they default to, and how many
            // inputs it takes. The builder only turns valid input into SQL.
            let component = specs::lookup(&node.id, &component_id)?;
            specs::check_input_count(&node.id, &component.spec, inputs.len())?;

            let properties = specs::resolve_properties(
                &node.id,
                &component.spec,
                node.data.properties_or_null(),
                unknown_properties,
            )?;

            // An unrecognised mode falls back to `auto` with a warning: the
            // choice affects only how the work is done, never the answer, so
            // refusing to run over it would be the wrong trade.
            let materialize = match node.data.materialize.as_deref() {
                None => Materialize::Auto,
                Some(token) => Materialize::parse(token).unwrap_or_else(|| {
                    unknown_materialize.push(Warning::UnknownMaterialize {
                        id: node.id.clone(),
                        value: token.to_string(),
                    });
                    Materialize::Auto
                }),
            };

            // Only a stage that produces a relation can be materialised; a sink
            // writes its output and a control node has none.
            let materialize = if kind.produces_relation() {
                materialize
            } else {
                Materialize::Auto
            };

            let spill_path = (materialize == Materialize::Disk)
                .then(|| format!("{SPILL_DIR}/{}.parquet", node.id));

            let sql = (component.build)(&builders::Lowering {
                node_id: &node.id,
                component_id: &component_id,
                properties: &properties,
                inputs: &inputs,
                alias: node.data.alias.as_deref(),
                materialize,
                spill_path: spill_path.as_deref(),
            })?;

            let count_sql = builders::count_probe(&node.id, kind, from.as_deref());

            let (sink_path, sink_mode) = if kind == StageKind::Sink {
                let read = |key: &str| {
                    properties
                        .get(key)
                        .and_then(JsonValue::as_str)
                        .map(str::to_string)
                };
                (read("path"), read("mode"))
            } else {
                (None, None)
            };

            Ok(Stage {
                node_id: node.id.clone(),
                component_id,
                label: node.data.label.clone(),
                kind,
                sql,
                count_sql,
                inputs,
                from,
                alias: node.data.alias.clone(),
                sink_path,
                sink_mode,
                requires_extensions: component.spec.requires_extensions.clone(),
                materialize,
                spill_path,
            })
        })
        .collect()
}

fn collect_graph_warnings(
    doc: &PipelineDoc,
    edges: &[ResolvedEdge],
    dropped: &[bool],
    stages: &[Stage],
    warnings: &mut Vec<Warning>,
) {
    // A lone node in a one-node pipeline is the pipeline, not an orphan.
    if doc.nodes.len() > 1 {
        for (position, node) in doc.nodes.iter().enumerate() {
            let connected = edges
                .iter()
                .any(|e| e.source == position || e.target == position);

            if !dropped[position] && !connected {
                warnings.push(Warning::Orphan {
                    id: node.id.clone(),
                });
            }
        }
    }

    if !stages.is_empty() && !stages.iter().any(|s| s.kind == StageKind::Sink) {
        warnings.push(Warning::NoSink);
    }
}

#[cfg(test)]
mod builder_tests;
#[cfg(test)]
mod tests;
// Shared with the params and context test modules, not only this one.
#[cfg(test)]
pub(crate) mod tests_support;
