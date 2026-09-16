use super::*;
use etl_metadata::{NodeData, PipelineEdge, Position};
use serde_json::json;

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

/// Minimal valid properties for each component, so graph-shape tests do not
/// have to restate them. Lowering validates properties, so a node built
/// without these would fail for a reason the test is not about.
fn default_properties(component_id: &str) -> JsonValue {
    match component_id {
        "src.file.csv" => json!({ "path": "in.csv" }),
        "src.file.parquet" => json!({ "path": "in.parquet" }),
        "xf.sql" => json!({ "query": "SELECT 1 AS a" }),
        "xf.filter" => json!({ "predicate": "1 = 1" }),
        "xf.select" => json!({ "columns": ["a"] }),
        "xf.join" => json!({ "keys": ["id"] }),
        "snk.file.parquet" => json!({ "path": "out.parquet" }),
        "snk.file.csv" => json!({ "path": "out.csv" }),
        _ => json!({}),
    }
}

fn node(id: &str, component_id: &str) -> PipelineNode {
    PipelineNode {
        id: id.to_string(),
        flow_type: None,
        position: Position { x: 0.0, y: 0.0 },
        data: NodeData {
            label: id.to_string(),
            subtitle: None,
            component_id: Some(component_id.to_string()),
            properties: Some(default_properties(component_id)),
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

fn edge(id: &str, source: &str, target: &str) -> PipelineEdge {
    PipelineEdge {
        id: id.to_string(),
        source: source.to_string(),
        target: target.to_string(),
        source_handle: Some("main".to_string()),
        target_handle: Some("in".to_string()),
        edge_type: None,
        data: None,
        extra: Default::default(),
    }
}

/// Build a document from `(id, component_id)` pairs and `(source, target)`
/// edges. Edge ids are generated as `e0`, `e1`, …
fn doc(nodes: &[(&str, &str)], wires: &[(&str, &str)]) -> PipelineDoc {
    PipelineDoc {
        format_version: 1,
        name: None,
        nodes: nodes.iter().map(|(id, c)| node(id, c)).collect(),
        edges: wires
            .iter()
            .enumerate()
            .map(|(i, (s, t))| edge(&format!("e{i}"), s, t))
            .collect(),
        resource_pool: String::new(),
        parameters: Default::default(),
        extra: Default::default(),
    }
}

fn disable(doc: &mut PipelineDoc, id: &str) {
    doc.nodes
        .iter_mut()
        .find(|n| n.id == id)
        .expect("node exists")
        .data
        .disabled = Some(true);
}

/// A straight source → transform → sink line.
fn linear() -> PipelineDoc {
    doc(
        &[
            ("read", "src.file.csv"),
            ("filter", "xf.filter"),
            ("write", "snk.file.parquet"),
        ],
        &[("read", "filter"), ("filter", "write")],
    )
}

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

#[test]
fn linear_graph_runs_in_order() {
    let plan = compile(&linear()).unwrap();

    assert_eq!(plan.order(), ["read", "filter", "write"]);
    assert_eq!(plan.format_version, 1);
}

#[test]
fn order_follows_dependencies_not_document_order() {
    // Declared sink-first, so a compiler that just echoed the document would
    // get this wrong.
    let pipeline = doc(
        &[
            ("write", "snk.file.parquet"),
            ("filter", "xf.filter"),
            ("read", "src.file.csv"),
        ],
        &[("read", "filter"), ("filter", "write")],
    );

    assert_eq!(
        compile(&pipeline).unwrap().order(),
        ["read", "filter", "write"]
    );
}

#[test]
fn diamond_graph_puts_each_node_after_its_dependencies() {
    //        left
    //       /     \
    //   read       join → write
    //       \     /
    //        right
    let pipeline = doc(
        &[
            ("read", "src.file.csv"),
            ("left", "xf.filter"),
            ("right", "xf.filter"),
            ("join", "xf.join"),
            ("write", "snk.file.parquet"),
        ],
        &[
            ("read", "left"),
            ("read", "right"),
            ("left", "join"),
            ("right", "join"),
            ("join", "write"),
        ],
    );

    let plan = compile(&pipeline).unwrap();
    let order = plan.order();
    let at = |id: &str| order.iter().position(|&n| n == id).unwrap();

    assert!(at("read") < at("left"));
    assert!(at("read") < at("right"));
    assert!(at("left") < at("join"));
    assert!(at("right") < at("join"));
    assert!(at("join") < at("write"));
}

#[test]
fn ties_break_by_document_order_so_plans_are_reproducible() {
    // Two independent branches: nothing orders `a` against `b`, so the tie
    // must break the same way every time or golden SQL tests become flaky.
    let pipeline = doc(
        &[
            ("a", "src.file.csv"),
            ("b", "src.file.csv"),
            ("write_a", "snk.file.parquet"),
            ("write_b", "snk.file.parquet"),
        ],
        &[("a", "write_a"), ("b", "write_b")],
    );

    let first = compile(&pipeline).unwrap().order().join(",");

    for _ in 0..16 {
        assert_eq!(compile(&pipeline).unwrap().order().join(","), first);
    }
    assert_eq!(first, "a,b,write_a,write_b");
}

// ---------------------------------------------------------------------------
// Stage contents
// ---------------------------------------------------------------------------

#[test]
fn stages_carry_kind_label_and_inputs() {
    let plan = compile(&linear()).unwrap();
    let filter = plan.stage("filter").unwrap();

    assert_eq!(filter.kind, StageKind::Transform);
    assert_eq!(filter.component_id, "xf.filter");
    assert_eq!(filter.label, "filter");
    assert_eq!(filter.inputs.len(), 1);
    assert_eq!(filter.inputs[0].node_id, "read");
    assert_eq!(filter.inputs[0].source_handle.as_deref(), Some("main"));
    assert_eq!(filter.inputs[0].target_handle.as_deref(), Some("in"));
    assert_eq!(
        filter.sql,
        r#"CREATE OR REPLACE TEMP VIEW "filter" AS (SELECT * FROM "read" WHERE 1 = 1);"#
    );
}

#[test]
fn sources_have_no_inputs_and_sinks_name_the_relation_they_read() {
    let plan = compile(&linear()).unwrap();

    assert!(plan.stage("read").unwrap().inputs.is_empty());
    assert_eq!(plan.stage("read").unwrap().from, None);
    assert_eq!(plan.stage("write").unwrap().from.as_deref(), Some("filter"));
}

#[test]
fn from_is_none_when_a_stage_has_several_inputs() {
    let pipeline = doc(
        &[
            ("a", "src.file.csv"),
            ("b", "src.file.csv"),
            ("join", "xf.join"),
        ],
        &[("a", "join"), ("b", "join")],
    );

    let plan = compile(&pipeline).unwrap();
    let join = plan.stage("join").unwrap();

    assert_eq!(join.inputs.len(), 2);
    assert_eq!(
        join.from, None,
        "a row count cannot be attributed to one input"
    );
}

#[test]
fn every_namespace_maps_to_a_kind() {
    let kind = |component: &str| StageKind::from_component_id("n", component).unwrap();

    assert_eq!(kind("src.file.csv"), StageKind::Source);
    assert_eq!(kind("xf.filter"), StageKind::Transform);
    assert_eq!(kind("code.sql"), StageKind::Transform);
    assert_eq!(kind("qa.not_null"), StageKind::Quality);
    assert_eq!(kind("ctl.wait"), StageKind::Control);
    assert_eq!(kind("snk.file.parquet"), StageKind::Sink);
}

#[test]
fn a_known_namespace_with_an_unimplemented_component_says_so() {
    // `qa.*` is a real namespace whose components arrive in Phase 6. Until
    // then the error should name the component, not complain about the
    // namespace.
    let pipeline = doc(&[("check", "qa.not_null")], &[]);

    assert_eq!(
        compile(&pipeline),
        Err(EngineError::UnsupportedComponent {
            id: "check".to_string(),
            component_id: "qa.not_null".to_string(),
        })
    );
}

#[test]
fn only_sources_transforms_and_quality_checks_produce_relations() {
    assert!(StageKind::Source.produces_relation());
    assert!(StageKind::Transform.produces_relation());
    assert!(StageKind::Quality.produces_relation());
    assert!(!StageKind::Sink.produces_relation());
    assert!(!StageKind::Control.produces_relation());
}

#[test]
fn an_alias_is_carried_onto_the_stage() {
    let mut pipeline = linear();
    pipeline.nodes[0].data.alias = Some("orders".to_string());

    let plan = compile(&pipeline).unwrap();
    let read = plan.stage("read").unwrap();

    assert_eq!(read.alias.as_deref(), Some("orders"));
    assert_eq!(
        read.relation_name(),
        "read",
        "the engine creates the relation under the node id; the alias is extra"
    );
}

// ---------------------------------------------------------------------------
// Disabled nodes
// ---------------------------------------------------------------------------

#[test]
fn a_disabled_node_drops_itself_and_everything_downstream() {
    let mut pipeline = linear();
    disable(&mut pipeline, "filter");

    let plan = compile(&pipeline).unwrap();

    assert_eq!(plan.order(), ["read"]);
    assert!(plan.warnings.contains(&Warning::DisabledSkipped {
        id: "filter".to_string()
    }));
    assert!(
        plan.warnings
            .contains(&Warning::DroppedDownstreamOfDisabled {
                id: "write".to_string(),
                disabled: "filter".to_string(),
            }),
        "the sink cannot run without its input and must say so"
    );
}

#[test]
fn disabling_a_leaf_leaves_the_rest_intact() {
    let mut pipeline = linear();
    disable(&mut pipeline, "write");

    let plan = compile(&pipeline).unwrap();

    assert_eq!(plan.order(), ["read", "filter"]);
}

#[test]
fn the_drop_cascades_through_a_whole_chain() {
    let pipeline_ids = &[
        ("a", "src.file.csv"),
        ("b", "xf.filter"),
        ("c", "xf.filter"),
        ("d", "snk.file.parquet"),
    ];
    let mut pipeline = doc(pipeline_ids, &[("a", "b"), ("b", "c"), ("c", "d")]);
    disable(&mut pipeline, "b");

    let plan = compile(&pipeline).unwrap();

    assert_eq!(plan.order(), ["a"]);
}

#[test]
fn an_unrelated_branch_survives_a_disabled_node() {
    let mut pipeline = doc(
        &[
            ("a", "src.file.csv"),
            ("write_a", "snk.file.parquet"),
            ("b", "src.file.csv"),
            ("write_b", "snk.file.parquet"),
        ],
        &[("a", "write_a"), ("b", "write_b")],
    );
    disable(&mut pipeline, "a");

    let plan = compile(&pipeline).unwrap();

    assert_eq!(plan.order(), ["b", "write_b"]);
}

#[test]
fn a_disabled_node_is_still_validated() {
    // Switching a broken node off must not make the error disappear, or the
    // problem resurfaces later for whoever switches it back on.
    let mut pipeline = linear();
    disable(&mut pipeline, "filter");
    pipeline.nodes[1].data.component_id = Some("nope.filter".to_string());

    assert!(matches!(
        compile(&pipeline),
        Err(EngineError::UnknownNamespace { .. })
    ));
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[test]
fn an_empty_pipeline_is_an_error() {
    let pipeline = doc(&[], &[]);

    assert_eq!(compile(&pipeline), Err(EngineError::EmptyPipeline));
}

#[test]
fn a_cycle_is_reported_with_the_nodes_in_it() {
    let pipeline = doc(
        &[("a", "xf.filter"), ("b", "xf.filter"), ("c", "xf.filter")],
        &[("a", "b"), ("b", "c"), ("c", "a")],
    );

    let Err(EngineError::Cycle { nodes }) = compile(&pipeline) else {
        panic!("a cycle must not compile");
    };

    assert_eq!(nodes, ["a", "b", "c"]);
}

#[test]
fn a_cycle_downstream_of_a_valid_source_is_still_caught() {
    let pipeline = doc(
        &[
            ("read", "src.file.csv"),
            ("a", "xf.filter"),
            ("b", "xf.filter"),
        ],
        &[("read", "a"), ("a", "b"), ("b", "a")],
    );

    let Err(EngineError::Cycle { nodes }) = compile(&pipeline) else {
        panic!("a cycle must not compile");
    };

    assert_eq!(nodes, ["a", "b"]);
}

#[test]
fn an_edge_to_a_missing_node_names_the_edge_and_the_node() {
    let mut pipeline = linear();
    pipeline.edges[1].target = "ghost".to_string();

    assert_eq!(
        compile(&pipeline),
        Err(EngineError::UnknownEdgeEndpoint {
            edge_id: "e1".to_string(),
            node_id: "ghost".to_string(),
        })
    );
}

#[test]
fn an_edge_from_a_missing_node_is_caught_too() {
    let mut pipeline = linear();
    pipeline.edges[0].source = "ghost".to_string();

    assert!(matches!(
        compile(&pipeline),
        Err(EngineError::UnknownEdgeEndpoint { .. })
    ));
}

#[test]
fn a_self_edge_is_reported_as_itself_not_as_a_cycle() {
    let mut pipeline = linear();
    pipeline.edges[0].target = "read".to_string();

    assert_eq!(
        compile(&pipeline),
        Err(EngineError::SelfEdge {
            edge_id: "e0".to_string(),
            node_id: "read".to_string(),
        })
    );
}

#[test]
fn duplicate_node_ids_are_rejected() {
    let pipeline = doc(&[("read", "src.file.csv"), ("read", "xf.filter")], &[]);

    assert_eq!(
        compile(&pipeline),
        Err(EngineError::DuplicateNodeId {
            id: "read".to_string()
        })
    );
}

#[test]
fn a_node_without_a_component_is_rejected() {
    let mut pipeline = linear();
    pipeline.nodes[1].data.component_id = None;

    assert_eq!(
        compile(&pipeline),
        Err(EngineError::MissingComponentId {
            id: "filter".to_string()
        })
    );
}

#[test]
fn an_unknown_namespace_lists_the_ones_that_exist() {
    let mut pipeline = linear();
    pipeline.nodes[1].data.component_id = Some("weird.filter".to_string());

    let message = compile(&pipeline).unwrap_err().to_string();

    assert!(message.contains("'weird'"), "{message}");
    assert!(message.contains("src, xf, snk, qa, ctl, code"), "{message}");
}

#[test]
fn errors_point_at_the_node_so_the_canvas_can_highlight_it() {
    let mut pipeline = linear();
    pipeline.nodes[1].data.component_id = None;

    assert_eq!(compile(&pipeline).unwrap_err().node_id(), Some("filter"));
    assert_eq!(EngineError::EmptyPipeline.node_id(), None);
}

// ---------------------------------------------------------------------------
// Warnings
// ---------------------------------------------------------------------------

#[test]
fn a_clean_pipeline_warns_about_nothing() {
    assert!(compile(&linear()).unwrap().warnings.is_empty());
}

#[test]
fn a_node_wired_to_nothing_is_flagged_as_an_orphan() {
    let mut pipeline = linear();
    pipeline.nodes.push(node("stray", "src.file.csv"));

    let plan = compile(&pipeline).unwrap();

    assert!(plan.warnings.contains(&Warning::Orphan {
        id: "stray".to_string()
    }));
    assert!(
        plan.order().contains(&"stray"),
        "an orphan is a warning, not a removal"
    );
}

#[test]
fn a_single_node_pipeline_is_not_an_orphan() {
    // A lone node is the whole pipeline, so "wired to nothing" is not a
    // complaint worth making. It still earns a NoSink warning.
    let pipeline = doc(&[("only", "src.file.csv")], &[]);

    let warnings = compile(&pipeline).unwrap().warnings;

    assert!(!warnings.iter().any(|w| matches!(w, Warning::Orphan { .. })));
    assert_eq!(warnings, [Warning::NoSink]);
}

#[test]
fn a_pipeline_with_no_sink_is_flagged_because_it_would_do_nothing() {
    let pipeline = doc(
        &[("read", "src.file.csv"), ("filter", "xf.filter")],
        &[("read", "filter")],
    );

    let plan = compile(&pipeline).unwrap();

    assert!(plan.warnings.contains(&Warning::NoSink));
    assert!(plan.sinks().next().is_none());
}

// ---------------------------------------------------------------------------
// The real sample
// ---------------------------------------------------------------------------

#[test]
fn the_sample_pipeline_compiles() {
    const SAMPLE: &str = include_str!("../../../../samples/pipelines/csv_to_parquet.json");
    let pipeline = PipelineDoc::from_json(SAMPLE).expect("sample parses");

    let plan = compile(&pipeline).unwrap();

    assert_eq!(
        plan.order(),
        ["read_orders", "filter_recent", "write_parquet"]
    );
    assert!(plan.warnings.is_empty());
    assert_eq!(plan.sinks().count(), 1);
    assert_eq!(
        plan.stage("read_orders").unwrap().alias.as_deref(),
        Some("orders")
    );
}
