//! Lineage says what the plan says, and does not overstate it.

use super::*;
use crate::compile;
use etl_metadata::PipelineDoc;

/// A source, a quality node with both outputs wired, and two sinks.
fn document() -> PipelineDoc {
    PipelineDoc::from_json(
        r#"{
          "formatVersion": 1,
          "name": "checked",
          "nodes": [
            {"id": "orders", "type": "source", "position": {"x": 0, "y": 0},
             "data": {"label": "Orders", "componentId": "src.file.csv",
                      "properties": {"path": "data/orders.csv"},
                      "incremental": {"column": "order_ts"}}},
            {"id": "checked", "type": "quality", "position": {"x": 200, "y": 0},
             "data": {"label": "Amount is sane", "componentId": "qa.range",
                      "properties": {"column": "amount", "min": 0}}},
            {"id": "good", "type": "sink", "position": {"x": 400, "y": 0},
             "data": {"label": "Clean", "componentId": "snk.file.parquet",
                      "properties": {"path": "out/clean.parquet"}}},
            {"id": "bad", "type": "sink", "position": {"x": 400, "y": 200},
             "data": {"label": "Rejects", "componentId": "snk.file.csv",
                      "properties": {"path": "out/rejects.csv"}}}
          ],
          "edges": [
            {"id": "e1", "source": "orders", "target": "checked",
             "sourceHandle": "main", "targetHandle": "in"},
            {"id": "e2", "source": "checked", "target": "good",
             "sourceHandle": "main", "targetHandle": "in"},
            {"id": "e3", "source": "checked", "target": "bad",
             "sourceHandle": "rejected", "targetHandle": "in"}
          ]
        }"#,
    )
    .expect("parses")
}

fn of(document: &PipelineDoc) -> Lineage {
    lineage(&compile(document).expect("compiles"))
}

#[test]
fn inputs_and_outputs_are_what_the_pipeline_touches_outside_itself() {
    let found = of(&document());

    assert_eq!(found.inputs.len(), 1);
    assert_eq!(found.inputs[0].name, "data/orders.csv");
    assert_eq!(found.inputs[0].node_id, "orders");

    let mut written: Vec<&str> = found.outputs.iter().map(|out| out.name.as_str()).collect();
    written.sort();
    assert_eq!(written, ["out/clean.parquet", "out/rejects.csv"]);
}

#[test]
fn a_transform_touches_nothing_outside_the_pipeline() {
    let found = of(&document());
    let quality = found
        .nodes
        .iter()
        .find(|node| node.id == "checked")
        .expect("there");

    // Everything it reads is another stage, so there is nothing external to
    // name. `None`, not an empty string.
    assert_eq!(quality.external, None);
    assert_eq!(quality.kind, "quality");
}

#[test]
fn a_dead_letter_edge_is_distinguishable_from_the_main_flow() {
    let found = of(&document());

    let rejected: Vec<&Edge> = found
        .edges
        .iter()
        .filter(|edge| edge.port == "rejected")
        .collect();

    assert_eq!(rejected.len(), 1, "exactly one dead-letter branch");
    assert_eq!(rejected[0].from, "checked");
    assert_eq!(rejected[0].to, "bad");

    // Drawing this the same as the main flow would say the rejects are the
    // result, which is the opposite of what happened to them.
    let main: Vec<&Edge> = found
        .edges
        .iter()
        .filter(|edge| edge.from == "checked" && edge.port == "main")
        .collect();
    assert_eq!(main.len(), 1);
    assert_eq!(main[0].to, "good");
}

#[test]
fn an_incremental_source_says_so() {
    let found = of(&document());
    let source = found
        .nodes
        .iter()
        .find(|n| n.id == "orders")
        .expect("there");

    // "Is this a full load or a partial one" is the first question anyone
    // reading lineage after a surprise asks.
    assert_eq!(source.incremental_column.as_deref(), Some("order_ts"));
}

#[test]
fn column_lineage_is_absent_rather_than_empty() {
    let found = of(&document());

    for node in &found.nodes {
        assert_eq!(
            node.columns, None,
            "an empty list would claim the question was asked and answered"
        );
    }

    // And it serialises as absent, so a consumer cannot read `[]` as "this
    // node derives from no columns".
    let json = serde_json::to_string(&found).expect("serialises");
    assert!(!json.contains("columns"), "{json}");
}

#[test]
fn nodes_are_in_execution_order() {
    let found = of(&document());
    let order: Vec<&str> = found.nodes.iter().map(|node| node.id.as_str()).collect();

    // The source before the check before either sink: the same order the plan
    // runs in, so lineage and the run report can be read side by side.
    assert_eq!(order[0], "orders");
    assert_eq!(order[1], "checked");
    assert!(order[2..].contains(&"good") && order[2..].contains(&"bad"));
}

#[test]
fn a_database_source_names_its_table_and_never_its_connection() {
    let document = PipelineDoc::from_json(
        r#"{
          "formatVersion": 1,
          "nodes": [
            {"id": "pg", "type": "source", "position": {"x": 0, "y": 0},
             "data": {"label": "PG", "componentId": "src.db.postgres",
                      "properties": {"connection": "postgres://user:hunter2@host/db",
                                     "table": "orders", "schema": "public"}}},
            {"id": "out", "type": "sink", "position": {"x": 200, "y": 0},
             "data": {"label": "Out", "componentId": "snk.file.parquet",
                      "properties": {"path": "out/o.parquet"}}}
          ],
          "edges": [
            {"id": "e1", "source": "pg", "target": "out",
             "sourceHandle": "main", "targetHandle": "in"}
          ]
        }"#,
    )
    .expect("parses");

    let found = of(&document);
    let json = serde_json::to_string(&found).expect("serialises");

    assert_eq!(found.inputs[0].name, "public.orders");
    // Lineage gets read, logged and pasted into tickets. A connection string
    // is the one property most likely to hold a password.
    assert!(!json.contains("hunter2"), "{json}");
    assert!(!json.contains("postgres://"), "{json}");
}
