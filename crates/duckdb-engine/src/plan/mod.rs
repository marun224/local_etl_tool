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
use etl_metadata::{ControlKind, NodePolicy, PipelineDoc, PipelineNode, REJECTED_PORT};
use serde_json::Value as JsonValue;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};

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
    ///
    /// Control nodes do: they pass their input along unchanged. A `ctl.log` or
    /// `ctl.wait` that broke the chain it sits in would be unusable in the
    /// middle of a pipeline, which is the only place anyone puts one. What
    /// makes them control nodes is the effect they have on the way past, not an
    /// absence of rows.
    pub fn produces_relation(self) -> bool {
        self != StageKind::Sink
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

impl Input {
    /// The relation this input reads.
    ///
    /// Usually the upstream node id, because a stage names its relation after
    /// itself. A quality node is the exception: it creates two relations, and
    /// the handle the edge left by is what says which one this is. An absent
    /// handle means `main`, which is what a canvas emits for a component that
    /// has only one output.
    pub fn relation(&self) -> String {
        match self.source_handle.as_deref() {
            Some(REJECTED_PORT) => reject_relation(&self.node_id),
            _ => self.node_id.clone(),
        }
    }

    /// Whether this input comes from a dead-letter port.
    pub fn is_rejected(&self) -> bool {
        self.source_handle.as_deref() == Some(REJECTED_PORT)
    }
}

/// The suffix that names a quality node's dead-letter relation.
///
/// A quality node `check_email` creates `check_email` for the rows that passed
/// and `check_email__rejected` for the rows that did not. The suffix is
/// reserved: [`compile`] refuses a document where a node id collides with one,
/// because the collision would otherwise be a silent overwrite of one node's
/// output by another's.
pub const REJECT_SUFFIX: &str = "__rejected";

/// The relation holding the rows a quality node rejected.
pub fn reject_relation(node_id: &str) -> String {
    format!("{node_id}{REJECT_SUFFIX}")
}

/// How a stage behaves when it fails, with the document's defaults filled in.
///
/// The resolved form of [`NodePolicy`]: the document carries options, a stage
/// carries answers, so nothing downstream has to remember what an absent value
/// meant.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StagePolicy {
    /// Extra attempts after the first. Zero means run it once.
    pub retry_attempts: u32,
    /// The wait before the first retry, doubling each attempt after it.
    pub retry_backoff_ms: u64,
    /// Let the run carry on past this stage's failure. The run still ends
    /// failed; this decides how much of it happens first.
    pub continue_on_failure: bool,
    /// A memory ceiling applied around this stage.
    pub memory_limit_mb: Option<u64>,
}

/// The backoff used when a stage asks for retries without naming one.
pub const DEFAULT_RETRY_BACKOFF_MS: u64 = 250;

impl StagePolicy {
    fn from_node(policy: Option<&NodePolicy>) -> Self {
        let Some(policy) = policy else {
            return Self::default();
        };

        let retry_attempts = policy.retry_attempts.unwrap_or(0);

        Self {
            retry_attempts,
            retry_backoff_ms: policy.retry_backoff_ms.unwrap_or(if retry_attempts > 0 {
                DEFAULT_RETRY_BACKOFF_MS
            } else {
                0
            }),
            continue_on_failure: policy.continue_on_failure.unwrap_or(false),
            memory_limit_mb: policy.memory_limit_mb,
        }
    }

    /// Whether this stage has to be addressable on its own, which is what makes
    /// a plan need a session rather than one batched script.
    ///
    /// Retrying a stage means re-running that stage; carrying on past a failure
    /// means the batch must not abort. A memory ceiling is set and cleared
    /// around the statement. None of the three is expressible in one script.
    pub fn needs_session(&self) -> bool {
        self.retry_attempts > 0 || self.continue_on_failure || self.memory_limit_mb.is_some()
    }

    /// How long to wait before attempt `attempt`, counting the first retry as 1.
    pub fn backoff_for(&self, attempt: u32) -> std::time::Duration {
        let doubled = self
            .retry_backoff_ms
            .saturating_mul(1u64 << attempt.saturating_sub(1).min(16));

        std::time::Duration::from_millis(doubled)
    }
}

/// What a control stage does on the way past, beyond passing rows along.
///
/// Built here rather than in a builder because a builder returns SQL, and most
/// of this is not SQL: a duration to sleep for, a message to print, a decision
/// to take. The SQL that *is* here is a probe the driver evaluates before
/// deciding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Control {
    pub kind: ControlKind,
    /// A query the driver runs before passing rows through. Its meaning depends
    /// on the kind: a match count for [`ControlKind::Fail`] and
    /// [`ControlKind::Branch`], a single `ok` boolean for
    /// [`ControlKind::Assert`], and for [`ControlKind::Sequence`] a read of the
    /// other input, whose only purpose is to have happened.
    pub probe: Option<String>,
    /// What to say — printed by a log, and used as the failure message by a
    /// fail or a failed assertion.
    pub message: Option<String>,
    /// How long [`ControlKind::Wait`] holds for.
    pub wait_ms: Option<u64>,
}

/// One `SELECT count(*)` a stage emits, and which of its outputs it counts.
///
/// A list rather than a single statement because a quality node reports two
/// numbers. That matters more than it looks: the executor attributes counts to
/// stages **positionally**, by how many JSON arrays DuckDB has printed, so a
/// stage that emits two counts while the executor expects one shifts every
/// later stage's number by one — silently, and against the right node names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CountProbe {
    /// The statement itself.
    pub sql: String,
    /// The output port being counted, or `None` for a stage's only count.
    pub port: Option<String>,
}

impl CountProbe {
    /// Whether this probe counts a dead-letter output.
    pub fn is_rejected(&self) -> bool {
        self.port.as_deref() == Some(REJECTED_PORT)
    }
}

/// One node, lowered and placed in execution order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stage {
    pub node_id: String,
    pub component_id: String,
    pub label: String,
    pub kind: StageKind,
    /// Whether this stage partitions its input across two outputs instead of
    /// producing one relation. True for a quality node, and the reason
    /// [`Stage::counts`] can hold two entries.
    pub splits: bool,
    /// The complete statement(s) that realise this stage — a `CREATE OR
    /// REPLACE TEMP VIEW` for anything producing a relation, a `COPY ... TO`
    /// for a sink. Held whole rather than as a fragment so the plan view can
    /// show exactly what will run.
    pub sql: String,
    /// The count queries that follow this stage, so the run can report rows.
    /// Empty for a stage that neither produces a relation nor reads one; two
    /// entries for a quality node, which counts both of its outputs.
    pub counts: Vec<CountProbe>,
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
    /// How this stage behaves when it fails.
    pub policy: StagePolicy,
    /// What this stage does besides producing rows, for a control node.
    pub control: Option<Control>,
    /// How this stage loads only what is new, for an incremental source.
    pub incremental: Option<StageIncremental>,
    /// What this stage reads from or writes to *outside* the pipeline: a file
    /// path, an S3 URI, a database table. `None` for a transform, whose inputs
    /// are all other stages.
    ///
    /// Deliberately never the connection string. A table name is what lineage
    /// is asking about, and a connection string is the one property most
    /// likely to hold a password.
    pub external: Option<String>,
}

/// An incremental source, resolved against what the workspace remembers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageIncremental {
    /// The column being watched.
    pub column: String,
    /// The watermark this run read past, or `None` when it loaded everything.
    /// Kept so the run report can say which it was — "loaded 0 rows" means
    /// something very different on a first run than on a tenth.
    pub since: Option<String>,
    /// The query that reads this run's new high-water mark.
    ///
    /// Emitted directly after the stage rather than at the end of the script,
    /// which costs nothing — the relation is scanned either way — and buys the
    /// thing that matters: a watermark column that does not exist fails here,
    /// before any sink has written, instead of after.
    pub probe: String,
}

impl Stage {
    /// Whether this stage does its work at the moment it runs.
    ///
    /// True for a sink, whose `COPY ... TO` is the thing that pulls the whole
    /// pipeline; for a materialised stage, whose table or spill file is built
    /// there and then; and for a control node, whose waiting or branching is
    /// all it does. False for a lazy view, which is registered in microseconds
    /// and computed later by whatever reads it.
    ///
    /// The executor asks this before attaching a duration to a stage: a
    /// timing on a lazy view would read as "this step was free" when the step
    /// was merely deferred. See [`StageOutcome::elapsed`].
    ///
    /// [`StageOutcome::elapsed`]: crate::exec::StageOutcome::elapsed
    pub fn work_happens_here(&self) -> bool {
        self.kind == StageKind::Sink
            || self.kind == StageKind::Control
            || self.materialize.is_materialised()
    }

    /// The name of the relation this stage creates. Edge wiring and generated
    /// SQL both key off the node id; the alias is an additional view.
    pub fn relation_name(&self) -> &str {
        &self.node_id
    }

    /// Whether this stage has to be addressable on its own rather than batched
    /// into one script with the rest.
    pub fn needs_session(&self) -> bool {
        self.control.is_some() || self.policy.needs_session()
    }

    /// The dead-letter relation this stage creates, if it is one that splits.
    pub fn reject_relation_name(&self) -> Option<String> {
        self.splits.then(|| reject_relation(&self.node_id))
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
    /// A node declares `incremental` somewhere it cannot apply. Only a source
    /// loads from outside the pipeline, so only a source can load part of it.
    IncrementalIgnored { id: String, component_id: String },
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

    /// Whether this plan has to run through a persistent session rather than
    /// as one batched script.
    ///
    /// True when something in it needs a stage to be addressable on its own: a
    /// control node, or a stage carrying a retry or failure policy. Everything
    /// else keeps the one-script path it was written against — see
    /// `docs/DECISION_execution_model.md` for why a plan earns the session
    /// instead of every plan getting one.
    pub fn needs_session(&self) -> bool {
        self.stages.iter().any(Stage::needs_session)
    }

    /// The stages this plan would run one at a time, for a caller that wants to
    /// explain why a session was used.
    pub fn session_reasons(&self) -> Vec<&str> {
        self.stages
            .iter()
            .filter(|s| s.needs_session())
            .map(|s| s.node_id.as_str())
            .collect()
    }

    /// The stages needed to produce one node's relation, in execution order.
    ///
    /// Sinks are left out even when they are ancestors, because a preview must
    /// not write anything. Looking at what a node holds is a read, and a read
    /// that overwrites someone's output file would be a trap — the canvas calls
    /// this while a person clicks around a half-built pipeline.
    ///
    /// `None` when the node is not in the plan.
    pub fn upto(&self, node_id: &str) -> Option<Vec<&Stage>> {
        self.stage(node_id)?;

        // Walk back over inputs to find everything the node depends on, then
        // keep plan order — which is already topological, so the result is
        // runnable as it stands.
        let mut needed: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut frontier = vec![node_id];

        while let Some(current) = frontier.pop() {
            if !needed.insert(current) {
                continue;
            }

            if let Some(stage) = self.stage(current) {
                for input in &stage.inputs {
                    frontier.push(input.node_id.as_str());
                }
            }
        }

        Some(
            self.stages
                .iter()
                .filter(|stage| needed.contains(stage.node_id.as_str()))
                .filter(|stage| stage.kind != StageKind::Sink)
                .collect(),
        )
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
                for probe in &stage.counts {
                    script.push_str(&probe.sql);
                    script.push('\n');
                }
            }

            // Not gated on `counts`: a row count is a convenience, whereas
            // a watermark that failed to be read is a pipeline that reloads
            // the world next time. Its output carries a different key, so it
            // passes through the count parser without disturbing it.
            if let Some(incremental) = &stage.incremental {
                script.push_str(&incremental.probe);
                script.push('\n');
            }

            script.push('\n');
        }

        script
    }

    /// Every count the script will emit with counts on, paired with the stage
    /// that emits it, **in the order they appear on stdout**.
    ///
    /// That order is the contract: the executor has no other way to tell whose
    /// number it is holding. A stage contributes as many entries here as it
    /// emits statements — one for most, two for a quality node.
    pub fn count_probes(&self) -> impl Iterator<Item = (&Stage, &CountProbe)> {
        self.stages
            .iter()
            .flat_map(|stage| stage.counts.iter().map(move |probe| (stage, probe)))
    }

    /// The incremental sources, in the order their probes are emitted.
    ///
    /// The executor pairs this with the values it read back, so the order
    /// here and the order in [`Plan::script`] have to stay the same one.
    pub fn watermark_probes(&self) -> impl Iterator<Item = (&Stage, &StageIncremental)> {
        self.stages
            .iter()
            .filter_map(|stage| stage.incremental.as_ref().map(|state| (stage, state)))
    }

    /// Whether anything in this plan remembers where it got to.
    pub fn is_incremental(&self) -> bool {
        self.stages.iter().any(|stage| stage.incremental.is_some())
    }
}

/// What the compiler needs to know beyond the document itself.
///
/// Only one thing so far: what the workspace remembers about this pipeline. It
/// is passed in rather than read from disk here because [`compile`] is pure —
/// `validate` runs it against untrusted documents and must not touch the
/// filesystem — and because the GUI compiles a document that has no file yet.
#[derive(Debug, Clone, Default)]
pub struct CompileOptions {
    /// Node id → the high-water mark already loaded from it.
    pub watermarks: BTreeMap<String, String>,
}

/// Validate a document and order it for execution.
///
/// Compiles as though nothing has ever run: every incremental source loads
/// from its declared `start`, or from the beginning. [`compile_with`] is the
/// one that reads a watermark.
pub fn compile(doc: &PipelineDoc) -> Result<Plan, EngineError> {
    compile_with(doc, &CompileOptions::default())
}

/// Validate and order a document against what the workspace remembers.
pub fn compile_with(doc: &PipelineDoc, options: &CompileOptions) -> Result<Plan, EngineError> {
    if doc.nodes.is_empty() {
        return Err(EngineError::EmptyPipeline);
    }

    reserve_suffix(&doc.nodes)?;

    let index = build_index(&doc.nodes)?;
    let kinds = classify(&doc.nodes)?;
    let edges = resolve_edges(doc, &index)?;

    let mut warnings = Vec::new();
    let dropped = drop_disabled(doc, &edges, &mut warnings);

    let order = topological_order(doc, &edges, &dropped)?;

    let mut noticed = Noticed::default();
    let stages = build_stages(doc, &kinds, &edges, &order, options, &mut noticed)?;

    warnings.extend(noticed.unknown_properties.into_iter().map(|unknown| {
        Warning::UnknownProperty {
            id: unknown.node_id,
            property: unknown.property,
        }
    }));
    warnings.extend(noticed.unknown_materialize);
    warnings.extend(noticed.ignored_incremental);

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

/// What lowering noticed but did not refuse over.
///
/// Gathered into one place rather than passed as three out-parameters: they
/// are all the same kind of thing — something worth saying that is not worth
/// stopping for — and a fourth would otherwise mean a fourth argument.
#[derive(Debug, Default)]
struct Noticed {
    unknown_properties: Vec<specs::UnknownProperty>,
    unknown_materialize: Vec<Warning>,
    ignored_incremental: Vec<Warning>,
}

fn build_stages(
    doc: &PipelineDoc,
    kinds: &[StageKind],
    edges: &[ResolvedEdge],
    order: &[usize],
    options: &CompileOptions,
    noticed: &mut Noticed,
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

            // The relation, not the node id: an input taken from a quality
            // node's reject port reads a different relation than its main one,
            // and this is what the executor attributes a row count to.
            let from = match inputs.as_slice() {
                [only] => Some(only.relation()),
                _ => None,
            };

            check_ports(doc, &inputs)?;

            // Everything the component needs is derived from its spec: which
            // properties are required, what they default to, and how many
            // inputs it takes. The builder only turns valid input into SQL.
            let component = specs::lookup(&node.id, &component_id)?;
            specs::check_input_count(&node.id, &component.spec, inputs.len())?;

            let splits = component.spec.has_reject_port();

            let properties = specs::resolve_properties(
                &node.id,
                &component.spec,
                node.data.properties_or_null(),
                &mut noticed.unknown_properties,
            )?;

            // An unrecognised mode falls back to `auto` with a warning: the
            // choice affects only how the work is done, never the answer, so
            // refusing to run over it would be the wrong trade.
            let materialize = match node.data.materialize.as_deref() {
                None => Materialize::Auto,
                Some(token) => Materialize::parse(token).unwrap_or_else(|| {
                    noticed
                        .unknown_materialize
                        .push(Warning::UnknownMaterialize {
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

            // Incremental loading is a source's feature. On a transform the
            // predicate would compile and quietly filter a second time, which
            // is the kind of thing that looks like it works, so it is dropped
            // with a warning rather than honoured somewhere it does not mean
            // what it says.
            let declared = node.data.incremental.as_ref().filter(|_| {
                let is_source = kind == StageKind::Source;
                if !is_source {
                    noticed
                        .ignored_incremental
                        .push(Warning::IncrementalIgnored {
                            id: node.id.clone(),
                            component_id: component_id.clone(),
                        });
                }
                is_source
            });

            let incremental = declared.map(|declared| {
                // What the last successful run reached, falling back to the
                // document's own starting point, falling back to everything.
                let since = options
                    .watermarks
                    .get(&node.id)
                    .cloned()
                    .or_else(|| declared.start.clone());

                StageIncremental {
                    column: declared.column.clone(),
                    since,
                    probe: builders::watermark_probe(&node.id, &declared.column),
                }
            });

            let sql = (component.build)(&builders::Lowering {
                node_id: &node.id,
                component_id: &component_id,
                properties: &properties,
                inputs: &inputs,
                alias: node.data.alias.as_deref(),
                materialize,
                spill_path: spill_path.as_deref(),
                incremental: incremental
                    .as_ref()
                    .map(|state| builders::IncrementalFilter {
                        column: &state.column,
                        since: state.since.as_deref(),
                    }),
            })?;

            let counts = builders::count_probes(&node.id, kind, splits, from.as_deref());
            let policy = StagePolicy::from_node(node.data.policy.as_ref());

            let control = match component.spec.control {
                None => None,
                Some(kind) => Some(builders::control_for(
                    kind,
                    &node.id,
                    &component_id,
                    &properties,
                    &inputs,
                )?),
            };

            // Where this stage touches the world. Files name a path, databases
            // name a table; a transform names nothing, because everything it
            // reads is another stage.
            let external = match kind {
                StageKind::Source | StageKind::Sink => {
                    let read = |key: &str| properties.get(key).and_then(JsonValue::as_str);

                    read("path").map(str::to_string).or_else(|| {
                        read("table").map(|table| match read("schema") {
                            Some(schema) => format!("{schema}.{table}"),
                            None => table.to_string(),
                        })
                    })
                }
                _ => None,
            };

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

            // LOAD-only, at the one place every stage's SQL passes through, so
            // that `validate`, `plan`, `run`, `build`, the console and the
            // scheduler all refuse the same document. Generated SQL never has an
            // INSTALL in it; this is for the components that let somebody write
            // their own.
            if crate::sql::contains_install(&sql) {
                return Err(EngineError::RawInstall {
                    id: node.id.clone(),
                });
            }

            Ok(Stage {
                node_id: node.id.clone(),
                component_id,
                label: node.data.label.clone(),
                kind,
                splits,
                sql,
                counts,
                inputs,
                from,
                alias: node.data.alias.clone(),
                sink_path,
                sink_mode,
                requires_extensions: component.spec.requires_extensions.clone(),
                policy,
                control,
                materialize,
                spill_path,
                incremental,
                external,
            })
        })
        .collect()
}

/// Refuse a node id that would collide with a generated reject relation.
///
/// The suffix is reserved outright rather than only where a collision actually
/// exists today. The precise check would pass now and start failing later, when
/// someone adds a quality node elsewhere in the document — a validation error
/// on a node nobody touched. Reserving the suffix costs a name nobody wants and
/// fails at the moment the name is chosen.
fn reserve_suffix(nodes: &[PipelineNode]) -> Result<(), EngineError> {
    match nodes.iter().find(|node| node.id.ends_with(REJECT_SUFFIX)) {
        None => Ok(()),
        Some(node) => Err(EngineError::ReservedNodeId {
            id: node.id.clone(),
            suffix: REJECT_SUFFIX.to_string(),
        }),
    }
}

/// Check every input's handle against the outputs its upstream component
/// actually declares.
///
/// Until quality nodes existed, a handle was decorative: every component had
/// one output, so nothing read it and a typo was harmless. Now it selects a
/// relation, and a typo would silently read the wrong branch — the accepted
/// rows where the document asked for the rejected ones. Hence an error.
///
/// An upstream whose component is not registered is skipped rather than
/// reported: it fails on its own account, and because stages are built in
/// topological order that failure is reported first anyway.
fn check_ports(doc: &PipelineDoc, inputs: &[Input]) -> Result<(), EngineError> {
    for input in inputs {
        let Some(upstream) = doc.nodes.iter().find(|node| node.id == input.node_id) else {
            continue;
        };

        let Some(component_id) = upstream.data.component_id.as_deref() else {
            continue;
        };

        let Some(component) = specs::registry().get(component_id) else {
            continue;
        };

        if !component.spec.has_output(input.source_handle.as_deref()) {
            return Err(EngineError::UnknownPort {
                id: upstream.id.clone(),
                component_id: component_id.to_string(),
                port: input.source_handle.clone().unwrap_or_default(),
                known: component.spec.output_names(),
            });
        }
    }

    Ok(())
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
