//! Golden SQL for every component in the registry.
//!
//! These assert the exact generated text. That is deliberately brittle: the
//! SQL is shown to users on the plan view and is the thing that runs, so a
//! change to it should be a decision someone makes, not a diff nobody notices.

use super::tests_support::{
    compile_materialized, compile_one, compile_two, compile_two_sided, sql_of,
};
use crate::EngineError;
use serde_json::json;

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

#[test]
fn csv_source_reads_with_a_header_by_default() {
    let plan = compile_one("src.file.csv", json!({ "path": "samples/data/orders.csv" }));

    assert_eq!(
        sql_of(&plan, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM read_csv('samples/data/orders.csv', header=true));"#
    );
}

#[test]
fn csv_source_pins_the_delimiter_only_when_asked() {
    let plan = compile_one(
        "src.file.csv",
        json!({ "path": "in.csv", "header": false, "delimiter": ";" }),
    );

    assert_eq!(
        sql_of(&plan, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM read_csv('in.csv', header=false, delim=';'));"#
    );
}

#[test]
fn a_windows_path_is_normalised_but_not_escaped() {
    let plan = compile_one("src.file.csv", json!({ "path": r"D:\data\orders.csv" }));

    assert!(
        sql_of(&plan, "n").contains("read_csv('D:/data/orders.csv'"),
        "{}",
        sql_of(&plan, "n")
    );
}

#[test]
fn parquet_source_reads_the_path() {
    let plan = compile_one("src.file.parquet", json!({ "path": "in.parquet" }));

    assert_eq!(
        sql_of(&plan, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM read_parquet('in.parquet'));"#
    );
}

#[test]
fn a_source_without_a_path_names_the_missing_property() {
    let error = compile_one_err("src.file.csv", json!({}));

    assert_eq!(
        error,
        EngineError::MissingProperty {
            id: "n".to_string(),
            component_id: "src.file.csv".to_string(),
            property: "path".to_string(),
        }
    );
    assert!(error.to_string().contains("needs the 'path' property"));
}

#[test]
fn an_empty_path_is_rejected_rather_than_producing_empty_sql() {
    let error = compile_one_err("src.file.csv", json!({ "path": "   " }));

    assert!(matches!(error, EngineError::InvalidProperty { .. }));
}

#[test]
fn a_path_of_the_wrong_type_is_rejected() {
    let error = compile_one_err("src.file.csv", json!({ "path": 42 }));

    assert!(error.to_string().contains("must be text"), "{error}");
}

#[test]
fn jsonl_source_reads_newline_delimited_json() {
    let plan = compile_one("src.file.jsonl", json!({ "path": "in.jsonl" }));

    assert_eq!(
        sql_of(&plan, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM read_json('in.jsonl', format='newline_delimited', ignore_errors=false));"#
    );
}

// ---------------------------------------------------------------------------
// Transforms
// ---------------------------------------------------------------------------

#[test]
fn filter_selects_from_its_upstream() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("xf.filter", json!({ "predicate": "amount > 100" })),
    );

    assert_eq!(
        sql_of(&plan, "b"),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT * FROM "a" WHERE amount > 100);"#
    );
}

#[test]
fn select_quotes_column_names_so_reserved_words_work() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("xf.select", json!({ "columns": ["order", "amount"] })),
    );

    assert_eq!(
        sql_of(&plan, "b"),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT "order", "amount" FROM "a");"#
    );
}

#[test]
fn select_rejects_an_empty_column_list() {
    let error = compile_two_err(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("xf.select", json!({ "columns": [] })),
    );

    assert!(error.to_string().contains("at least one column"), "{error}");
}

#[test]
fn select_rejects_a_non_string_column() {
    let error = compile_two_err(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("xf.select", json!({ "columns": ["ok", 7] })),
    );

    assert!(matches!(error, EngineError::InvalidProperty { .. }));
}

#[test]
fn raw_sql_is_passed_through_untouched() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        (
            "xf.sql",
            json!({ "query": "SELECT customer_id, sum(amount) AS total FROM \"a\" GROUP BY 1" }),
        ),
    );

    assert_eq!(
        sql_of(&plan, "b"),
        "CREATE OR REPLACE TEMP VIEW \"b\" AS (SELECT customer_id, sum(amount) AS total FROM \"a\" GROUP BY 1);"
    );
}

#[test]
fn raw_sql_loses_a_trailing_semicolon_so_the_wrapper_stays_valid() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("xf.sql", json!({ "query": "  SELECT 1 AS x;  " })),
    );

    assert_eq!(
        sql_of(&plan, "b"),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT 1 AS x);"#
    );
}

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

/// A join needs two upstreams, which the one- and two-node helpers cannot
/// build, so these construct the graph directly.
mod joins {
    use super::super::tests_support::{compile_join, join_err};
    use crate::EngineError;
    use serde_json::json;

    #[test]
    fn keys_become_a_using_clause() {
        let sql = compile_join(json!({ "keys": ["customer_id"] }), None);

        assert_eq!(
            sql,
            r#"CREATE OR REPLACE TEMP VIEW "j" AS (SELECT * FROM "left" INNER JOIN "right" USING ("customer_id"));"#
        );
    }

    #[test]
    fn several_keys_are_all_quoted() {
        let sql = compile_join(json!({ "keys": ["a", "order"] }), None);

        assert!(sql.contains(r#"USING ("a", "order")"#), "{sql}");
    }

    #[test]
    fn a_condition_becomes_an_on_clause() {
        let sql = compile_join(
            json!({ "type": "left", "condition": "l.id = r.id AND r.active" }),
            None,
        );

        assert!(
            sql.contains(r#"LEFT JOIN "right" ON l.id = r.id AND r.active"#),
            "{sql}"
        );
    }

    #[test]
    fn every_join_type_lowers() {
        for (name, expected) in [
            ("inner", "INNER JOIN"),
            ("left", "LEFT JOIN"),
            ("right", "RIGHT JOIN"),
            ("full", "FULL OUTER JOIN"),
            ("outer", "FULL OUTER JOIN"),
        ] {
            let sql = compile_join(json!({ "type": name, "keys": ["id"] }), None);
            assert!(sql.contains(expected), "{name}: {sql}");
        }
    }

    #[test]
    fn a_cross_join_takes_no_condition() {
        let sql = compile_join(json!({ "type": "cross" }), None);

        assert_eq!(
            sql,
            r#"CREATE OR REPLACE TEMP VIEW "j" AS (SELECT * FROM "left" CROSS JOIN "right");"#
        );
    }

    #[test]
    fn a_cross_join_with_keys_is_a_mistake_worth_reporting() {
        let error = join_err(json!({ "type": "cross", "keys": ["id"] }), None);

        assert!(error.to_string().contains("neither keys nor a condition"));
    }

    #[test]
    fn an_unknown_join_type_lists_the_valid_ones() {
        // Caught by the spec's enum before the builder ever sees it, which is
        // why the message lists the options rather than describing them.
        let error = join_err(json!({ "type": "sideways", "keys": ["id"] }), None);
        let message = error.to_string();

        assert!(message.contains("must be one of"), "{message}");
        assert!(message.contains("inner"), "{message}");
        assert!(message.contains("cross"), "{message}");
    }

    #[test]
    fn a_join_with_neither_keys_nor_condition_is_rejected() {
        let error = join_err(json!({}), None);

        assert!(matches!(error, EngineError::MissingProperty { .. }));
    }

    #[test]
    fn setting_both_keys_and_condition_is_rejected_rather_than_silently_picking_one() {
        let error = join_err(json!({ "keys": ["id"], "condition": "a.x = b.x" }), None);

        assert!(error.to_string().contains("not both"));
    }

    #[test]
    fn handles_decide_the_sides_not_edge_order() {
        // Wire the second input to the `left` handle: the join must respect
        // that, or re-wiring the canvas would silently flip an outer join.
        let sql = compile_join(
            json!({ "type": "left", "keys": ["id"] }),
            Some(("right", "left")),
        );

        assert!(
            sql.contains(r#"FROM "right" LEFT JOIN "left""#),
            "handles ignored: {sql}"
        );
    }
}

// ---------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------

#[test]
fn parquet_sink_copies_with_zstd_by_default() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("snk.file.parquet", json!({ "path": "out/x.parquet" })),
    );

    assert_eq!(
        sql_of(&plan, "b"),
        r#"COPY (SELECT * FROM "a") TO 'out/x.parquet' (FORMAT parquet, COMPRESSION 'zstd');"#
    );
}

#[test]
fn parquet_sink_honours_an_explicit_compression() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        (
            "snk.file.parquet",
            json!({ "path": "out.parquet", "compression": "snappy" }),
        ),
    );

    assert!(sql_of(&plan, "b").contains("COMPRESSION 'snappy'"));
}

#[test]
fn csv_sink_writes_a_header_by_default() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("snk.file.csv", json!({ "path": "out.csv" })),
    );

    // The delimiter is written explicitly because the sink's spec defaults it
    // to a comma. A source can sniff its delimiter; a sink has to choose one,
    // so stating it makes the generated SQL self-describing.
    assert_eq!(
        sql_of(&plan, "b"),
        r#"COPY (SELECT * FROM "a") TO 'out.csv' (FORMAT csv, HEADER true, DELIMITER ',');"#
    );
}

#[test]
fn csv_sink_takes_a_delimiter() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        (
            "snk.file.csv",
            json!({ "path": "out.tsv", "header": false, "delimiter": "\t" }),
        ),
    );

    assert!(
        sql_of(&plan, "b").contains("FORMAT csv, HEADER false, DELIMITER '\t'"),
        "{}",
        sql_of(&plan, "b")
    );
}

#[test]
fn a_sink_records_where_it_writes_so_the_executor_can_prepare_it() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        (
            "snk.file.parquet",
            json!({ "path": "out/x.parquet", "mode": "error_if_exists" }),
        ),
    );

    let sink = plan.stage("b").unwrap();

    assert_eq!(sink.sink_path.as_deref(), Some("out/x.parquet"));
    assert_eq!(sink.sink_mode.as_deref(), Some("error_if_exists"));
}

#[test]
fn a_sink_with_no_input_is_rejected() {
    let error = compile_one_err("snk.file.parquet", json!({ "path": "out.parquet" }));

    assert_eq!(
        error,
        EngineError::WrongInputCount {
            id: "n".to_string(),
            component_id: "snk.file.parquet".to_string(),
            expected: 1,
            actual: 0,
        }
    );
}

// ---------------------------------------------------------------------------
// Aliases and count probes
// ---------------------------------------------------------------------------

#[test]
fn an_alias_adds_a_second_view_without_renaming_the_first() {
    use super::tests_support::compile_one_aliased;

    let plan = compile_one_aliased("src.file.csv", json!({ "path": "in.csv" }), "orders");

    assert_eq!(
        sql_of(&plan, "n"),
        "CREATE OR REPLACE TEMP VIEW \"n\" AS (SELECT * FROM read_csv('in.csv', header=true));\n\
         CREATE OR REPLACE TEMP VIEW \"orders\" AS SELECT * FROM \"n\";"
    );
}

#[test]
fn a_relation_producing_stage_counts_itself() {
    let plan = compile_one("src.file.csv", json!({ "path": "in.csv" }));

    assert_eq!(
        plan.stage("n").unwrap().count_sql.as_deref(),
        Some(r#"SELECT count(*) AS n FROM "n";"#)
    );
}

#[test]
fn a_sink_counts_its_upstream_because_copy_reports_nothing() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("snk.file.parquet", json!({ "path": "out.parquet" })),
    );

    assert_eq!(
        plan.stage("b").unwrap().count_sql.as_deref(),
        Some(r#"SELECT count(*) AS n FROM "a";"#)
    );
}

#[test]
fn the_script_interleaves_stages_with_their_counts() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("snk.file.parquet", json!({ "path": "out.parquet" })),
    );

    let script = plan.script(true);
    let statements: Vec<&str> = script
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with("--"))
        .collect();

    assert_eq!(statements.len(), 4, "stage, count, stage, count: {script}");
    assert!(statements[1].starts_with("SELECT count(*)"));
    assert!(statements[3].starts_with("SELECT count(*)"));
}

#[test]
fn counts_can_be_left_out_entirely() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("snk.file.parquet", json!({ "path": "out.parquet" })),
    );

    assert!(!plan.script(false).contains("count(*)"));
}

// ---------------------------------------------------------------------------
// Transforms: reshaping columns
// ---------------------------------------------------------------------------

/// Every transform test reads from this, so the upstream relation is always
/// the view named `a`.
fn from_source(component_id: &str, properties: serde_json::Value) -> String {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        (component_id, properties),
    );

    sql_of(&plan, "b").to_string()
}

#[test]
fn derive_appends_expressions_to_the_existing_columns() {
    assert_eq!(
        from_source(
            "xf.derive",
            json!({ "expressions": "amount * 1.2 AS gross" })
        ),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT *, amount * 1.2 AS gross FROM "a");"#
    );
}

#[test]
fn rename_uses_a_star_rename_so_other_columns_survive() {
    assert_eq!(
        from_source("xf.rename", json!({ "columns": { "id": "order_id" } })),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT * RENAME ("id" AS "order_id") FROM "a");"#
    );
}

#[test]
fn rename_keeps_the_order_the_pairs_were_written_in() {
    // Not alphabetical: the generated SQL follows the document, so the plan is
    // reviewable and does not reorder between runs.
    let sql = from_source(
        "xf.rename",
        json!({ "columns": { "z": "last", "a": "first" } }),
    );

    assert!(
        sql.contains(r#"RENAME ("z" AS "last", "a" AS "first")"#),
        "{sql}"
    );
}

#[test]
fn cast_replaces_only_the_named_columns() {
    assert_eq!(
        from_source(
            "xf.cast",
            json!({ "columns": { "amount": "DECIMAL(10,2)" } })
        ),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT * REPLACE (CAST("amount" AS DECIMAL(10,2)) AS "amount") FROM "a");"#
    );
}

#[test]
fn a_type_name_cannot_smuggle_in_a_second_statement() {
    // A type is the one document string that reaches the statement unquoted,
    // because `DECIMAL(10,2)` stops being a type if it is quoted.
    let error = compile_two_err(
        ("src.file.csv", json!({ "path": "in.csv" })),
        (
            "xf.cast",
            json!({ "columns": { "a": "INT); DROP TABLE orders; --" } }),
        ),
    );

    assert!(
        error.to_string().contains("is not a SQL type name"),
        "{error}"
    );
}

#[test]
fn an_empty_rename_map_is_rejected() {
    let error = compile_two_err(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("xf.rename", json!({ "columns": {} })),
    );

    assert!(
        matches!(error, EngineError::InvalidProperty { .. }),
        "{error}"
    );
}

// ---------------------------------------------------------------------------
// Transforms: choosing rows
// ---------------------------------------------------------------------------

#[test]
fn distinct_takes_no_configuration() {
    assert_eq!(
        from_source("xf.distinct", json!({})),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT DISTINCT * FROM "a");"#
    );
}

#[test]
fn dedup_keeps_the_first_row_per_key() {
    assert_eq!(
        from_source(
            "xf.dedup",
            json!({ "keys": ["customer_id"], "order_by": "updated_at DESC" })
        ),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT * FROM "a" QUALIFY row_number() OVER (PARTITION BY "customer_id" ORDER BY updated_at DESC) = 1);"#
    );
}

#[test]
fn dedup_without_an_order_leaves_the_window_unordered() {
    assert_eq!(
        from_source("xf.dedup", json!({ "keys": ["id"] })),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT * FROM "a" QUALIFY row_number() OVER (PARTITION BY "id") = 1);"#
    );
}

#[test]
fn sort_passes_its_ordering_through_as_written() {
    assert_eq!(
        from_source("xf.sort", json!({ "by": "amount DESC, order_id" })),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT * FROM "a" ORDER BY amount DESC, order_id);"#
    );
}

#[test]
fn limit_omits_a_zero_offset() {
    assert_eq!(
        from_source("xf.limit", json!({ "count": 10 })),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT * FROM "a" LIMIT 10);"#
    );
}

#[test]
fn limit_writes_an_offset_when_there_is_one() {
    assert_eq!(
        from_source("xf.limit", json!({ "count": 10, "offset": 20 })),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT * FROM "a" LIMIT 10 OFFSET 20);"#
    );
}

#[test]
fn a_negative_limit_is_caught_before_duckdb_sees_it() {
    let error = compile_two_err(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("xf.limit", json!({ "count": -1 })),
    );

    assert!(
        error.to_string().contains("must not be negative"),
        "{error}"
    );
}

#[test]
fn sample_counts_rows_by_default() {
    assert_eq!(
        from_source("xf.sample", json!({ "size": 500 })),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT * FROM "a" USING SAMPLE 500 ROWS);"#
    );
}

#[test]
fn a_percentage_sample_uses_reservoir_sampling() {
    // The default system sampler works a row group at a time and returns
    // nothing at all from a small input.
    assert_eq!(
        from_source("xf.sample", json!({ "size": 10, "unit": "percent" })),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT * FROM "a" USING SAMPLE reservoir(10 PERCENT));"#
    );
}

// ---------------------------------------------------------------------------
// Transforms: summarising
// ---------------------------------------------------------------------------

#[test]
fn aggregate_repeats_the_grouping_columns_in_the_projection() {
    assert_eq!(
        from_source(
            "xf.aggregate",
            json!({ "aggregations": "sum(amount) AS total", "group_by": ["region"] })
        ),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT "region", sum(amount) AS total FROM "a" GROUP BY "region");"#
    );
}

#[test]
fn aggregate_without_grouping_columns_summarises_everything() {
    assert_eq!(
        from_source("xf.aggregate", json!({ "aggregations": "count(*) AS n" })),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT count(*) AS n FROM "a");"#
    );
}

#[test]
fn a_window_names_its_output_column() {
    assert_eq!(
        from_source(
            "xf.window",
            json!({
                "expression": "sum(amount)",
                "output_column": "running_total",
                "partition_by": ["region"],
                "order_by": "order_ts"
            })
        ),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT *, sum(amount) OVER (PARTITION BY "region" ORDER BY order_ts) AS "running_total" FROM "a");"#
    );
}

#[test]
fn a_window_over_everything_has_an_empty_over_clause() {
    assert_eq!(
        from_source(
            "xf.window",
            json!({ "expression": "count(*)", "output_column": "total" })
        ),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT *, count(*) OVER () AS "total" FROM "a");"#
    );
}

#[test]
fn pivot_lists_its_values_because_a_view_cannot_discover_them() {
    assert_eq!(
        from_source(
            "xf.pivot",
            json!({
                "on": ["year"],
                "values": ["2025", "2026"],
                "using": "sum(amount)",
                "group_by": ["region"]
            })
        ),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (PIVOT "a" ON "year" IN ('2025', '2026') USING sum(amount) GROUP BY "region");"#
    );
}

#[test]
fn a_pivot_without_values_says_so() {
    let error = compile_two_err(
        ("src.file.csv", json!({ "path": "in.csv" })),
        (
            "xf.pivot",
            json!({ "on": ["year"], "using": "sum(amount)" }),
        ),
    );

    assert_eq!(
        error,
        EngineError::MissingProperty {
            id: "b".to_string(),
            component_id: "xf.pivot".to_string(),
            property: "values".to_string(),
        }
    );
}

#[test]
fn unpivot_defaults_its_output_column_names() {
    assert_eq!(
        from_source("xf.unpivot", json!({ "columns": ["q1", "q2"] })),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (UNPIVOT "a" ON "q1", "q2" INTO NAME "name" VALUE "value");"#
    );
}

#[test]
fn unpivot_takes_its_output_column_names_when_given_them() {
    assert_eq!(
        from_source(
            "xf.unpivot",
            json!({
                "columns": ["q1"],
                "name_column": "quarter",
                "value_column": "amount"
            })
        ),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (UNPIVOT "a" ON "q1" INTO NAME "quarter" VALUE "amount");"#
    );
}

// ---------------------------------------------------------------------------
// Transforms: combining two inputs
// ---------------------------------------------------------------------------

#[test]
fn union_keeps_duplicates_by_default() {
    assert_eq!(
        compile_two_sided("xf.union", json!({})),
        r#"CREATE OR REPLACE TEMP VIEW "j" AS (SELECT * FROM "left" UNION ALL SELECT * FROM "right");"#
    );
}

#[test]
fn union_can_drop_duplicates_and_match_on_names() {
    assert_eq!(
        compile_two_sided("xf.union", json!({ "all": false, "by_name": true })),
        r#"CREATE OR REPLACE TEMP VIEW "j" AS (SELECT * FROM "left" UNION BY NAME SELECT * FROM "right");"#
    );
}

#[test]
fn intersect_keeps_the_rows_in_both_inputs() {
    assert_eq!(
        compile_two_sided("xf.intersect", json!({})),
        r#"CREATE OR REPLACE TEMP VIEW "j" AS (SELECT * FROM "left" INTERSECT SELECT * FROM "right");"#
    );
}

#[test]
fn except_subtracts_the_right_input_from_the_left() {
    assert_eq!(
        compile_two_sided("xf.except", json!({})),
        r#"CREATE OR REPLACE TEMP VIEW "j" AS (SELECT * FROM "left" EXCEPT SELECT * FROM "right");"#
    );
}

#[test]
fn a_set_operation_can_keep_duplicates() {
    assert_eq!(
        compile_two_sided("xf.except", json!({ "all": true })),
        r#"CREATE OR REPLACE TEMP VIEW "j" AS (SELECT * FROM "left" EXCEPT ALL SELECT * FROM "right");"#
    );
}

#[test]
fn a_two_input_transform_wired_to_one_input_says_so() {
    let error = compile_two_err(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("xf.union", json!({})),
    );

    assert!(
        matches!(
            error,
            EngineError::WrongInputCount {
                expected: 2,
                actual: 1,
                ..
            }
        ),
        "{error}"
    );
}

// ---------------------------------------------------------------------------
// The extension prelude
// ---------------------------------------------------------------------------

#[test]
fn a_plan_of_transforms_needs_no_extensions() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("xf.distinct", json!({})),
    );

    assert!(plan.extensions().is_empty());
    assert!(!plan.script(true).contains("LOAD"));
    assert!(!plan.has_prelude_probe(true));
}

#[test]
fn json_source_reads_an_array_of_records() {
    let plan = compile_one("src.file.json", json!({ "path": "in.json" }));

    assert_eq!(
        sql_of(&plan, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM read_json('in.json', format='array', ignore_errors=false));"#
    );
}

#[test]
fn the_json_sink_writes_an_array_and_the_jsonl_sink_does_not() {
    // The only difference between the two is ARRAY, so they are pinned together
    // — a change to one that does not change the other is almost always a bug.
    let array = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("snk.file.json", json!({ "path": "out.json" })),
    );

    assert_eq!(
        sql_of(&array, "b"),
        r#"COPY (SELECT * FROM "a") TO 'out.json' (FORMAT json, ARRAY true);"#
    );

    let lines = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("snk.file.jsonl", json!({ "path": "out.jsonl" })),
    );

    assert_eq!(
        sql_of(&lines, "b"),
        r#"COPY (SELECT * FROM "a") TO 'out.jsonl' (FORMAT json);"#
    );
}

// ---------------------------------------------------------------------------
// Connectors: spreadsheets, object storage, lakehouse
// ---------------------------------------------------------------------------

#[test]
fn the_excel_source_reads_the_first_sheet_unless_told_otherwise() {
    let plan = compile_one("src.file.excel", json!({ "path": "book.xlsx" }));

    assert_eq!(
        sql_of(&plan, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM read_xlsx('book.xlsx', header=true));"#
    );

    let named = compile_one(
        "src.file.excel",
        json!({ "path": "book.xlsx", "sheet": "Orders", "header": false }),
    );

    assert_eq!(
        sql_of(&named, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM read_xlsx('book.xlsx', header=false, sheet='Orders'));"#
    );
}

#[test]
fn the_excel_sink_names_its_sheet_only_when_asked() {
    let plain = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("snk.file.excel", json!({ "path": "out.xlsx" })),
    );

    assert_eq!(
        sql_of(&plain, "b"),
        r#"COPY (SELECT * FROM "a") TO 'out.xlsx' (FORMAT xlsx, HEADER true);"#
    );

    let named = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        (
            "snk.file.excel",
            json!({ "path": "out.xlsx", "sheet": "Totals" }),
        ),
    );

    assert_eq!(
        sql_of(&named, "b"),
        r#"COPY (SELECT * FROM "a") TO 'out.xlsx' (FORMAT xlsx, HEADER true, SHEET 'Totals');"#
    );
}

#[test]
fn an_s3_source_defaults_to_parquet_and_switches_reader_by_format() {
    let parquet = compile_one(
        "src.cloud.s3",
        json!({ "path": "s3://bucket/orders.parquet" }),
    );

    assert_eq!(
        sql_of(&parquet, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM read_parquet('s3://bucket/orders.parquet'));"#
    );

    let csv = compile_one(
        "src.cloud.s3",
        json!({ "path": "s3://bucket/orders.csv", "format": "csv" }),
    );

    assert_eq!(
        sql_of(&csv, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM read_csv('s3://bucket/orders.csv', header=true));"#
    );
}

#[test]
fn an_http_source_reads_the_url_it_is_given() {
    let plan = compile_one(
        "src.cloud.http",
        json!({ "path": "https://example.com/orders.parquet" }),
    );

    assert_eq!(
        sql_of(&plan, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM read_parquet('https://example.com/orders.parquet'));"#
    );
}

#[test]
fn an_s3_sink_writes_the_format_it_was_given() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("snk.cloud.s3", json!({ "path": "s3://bucket/out.parquet" })),
    );

    assert_eq!(
        sql_of(&plan, "b"),
        r#"COPY (SELECT * FROM "a") TO 's3://bucket/out.parquet' (FORMAT parquet, COMPRESSION 'zstd');"#
    );
}

#[test]
fn the_lakehouse_sources_call_their_own_scan_functions() {
    let iceberg = compile_one("src.lake.iceberg", json!({ "path": "warehouse/orders" }));

    assert_eq!(
        sql_of(&iceberg, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM iceberg_scan('warehouse/orders', allow_moved_paths=false));"#
    );

    let delta = compile_one("src.lake.delta", json!({ "path": "warehouse/orders" }));

    assert_eq!(
        sql_of(&delta, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM delta_scan('warehouse/orders'));"#
    );
}

// ---------------------------------------------------------------------------
// Connectors: databases
// ---------------------------------------------------------------------------

#[test]
fn a_database_source_attaches_under_an_alias_of_its_own() {
    // The alias is derived from the node id, so two nodes reading different
    // tables out of the same database do not collide on it.
    let plan = compile_one(
        "src.db.postgres",
        json!({ "connection": "dbname=analytics", "table": "orders" }),
    );

    assert_eq!(
        sql_of(&plan, "n"),
        "ATTACH 'dbname=analytics' AS \"n_db\" (TYPE postgres, READ_ONLY);\n\
         CREATE OR REPLACE TEMP VIEW \"n\" AS (SELECT * FROM \"n_db\".\"orders\");"
    );
}

#[test]
fn a_named_schema_is_qualified_and_an_absent_one_is_left_out() {
    let plan = compile_one(
        "src.db.postgres",
        json!({ "connection": "dbname=a", "table": "orders", "schema": "sales" }),
    );

    assert!(
        sql_of(&plan, "n").contains(r#"FROM "n_db"."sales"."orders""#),
        "{}",
        sql_of(&plan, "n")
    );
}

#[test]
fn each_database_source_attaches_with_its_own_type() {
    for (component, database_type) in [
        ("src.db.postgres", "postgres"),
        ("src.db.mysql", "mysql"),
        ("src.db.sqlite", "sqlite"),
    ] {
        let plan = compile_one(
            component,
            json!({ "connection": "somewhere", "table": "orders" }),
        );

        assert!(
            sql_of(&plan, "n").contains(&format!("(TYPE {database_type}, READ_ONLY)")),
            "{component}: {}",
            sql_of(&plan, "n")
        );
    }
}

#[test]
fn a_database_sink_replaces_the_table_by_default_and_can_append() {
    let replace = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        (
            "snk.db.sqlite",
            json!({ "connection": "out.db", "table": "orders" }),
        ),
    );

    assert_eq!(
        sql_of(&replace, "b"),
        "ATTACH 'out.db' AS \"b_db\" (TYPE sqlite);\n\
         CREATE OR REPLACE TABLE \"b_db\".\"orders\" AS SELECT * FROM \"a\";"
    );

    let append = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        (
            "snk.db.sqlite",
            json!({ "connection": "out.db", "table": "orders", "mode": "append" }),
        ),
    );

    assert_eq!(
        sql_of(&append, "b"),
        "ATTACH 'out.db' AS \"b_db\" (TYPE sqlite);\n\
         CREATE TABLE IF NOT EXISTS \"b_db\".\"orders\" AS SELECT * FROM \"a\" WHERE false;\n\
         INSERT INTO \"b_db\".\"orders\" SELECT * FROM \"a\";"
    );
}

#[test]
fn a_database_sink_is_not_write_protected() {
    // READ_ONLY on a sink would fail at run time, well after the point where it
    // could be explained.
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        (
            "snk.db.postgres",
            json!({ "connection": "dbname=a", "table": "t" }),
        ),
    );

    assert!(
        !sql_of(&plan, "b").contains("READ_ONLY"),
        "{}",
        sql_of(&plan, "b")
    );
}

#[test]
fn a_connection_string_with_a_quote_stays_inside_its_literal() {
    let plan = compile_one(
        "src.db.sqlite",
        json!({ "connection": "Bob's.db", "table": "orders" }),
    );

    assert!(
        sql_of(&plan, "n").contains("ATTACH 'Bob''s.db'"),
        "{}",
        sql_of(&plan, "n")
    );
}

// ---------------------------------------------------------------------------
// The extension prelude, now that components declare extensions
// ---------------------------------------------------------------------------

#[test]
fn a_plan_loads_what_its_components_declare() {
    let plan = compile_two(
        (
            "src.db.postgres",
            json!({ "connection": "dbname=a", "table": "orders" }),
        ),
        ("snk.file.parquet", json!({ "path": "out.parquet" })),
    );

    assert_eq!(plan.extensions(), ["postgres"]);
    assert!(plan.has_prelude_probe(true));

    assert!(
        plan.script(true)
            .starts_with("-- extensions\nLOAD postgres;\nSELECT 0 AS n;\n"),
        "{}",
        plan.script(true)
    );
}

#[test]
fn extensions_are_sorted_and_deduplicated_across_the_plan() {
    // Both stages need httpfs; iceberg needs it as well as its own.
    let plan = compile_two(
        ("src.lake.iceberg", json!({ "path": "warehouse/orders" })),
        ("snk.cloud.s3", json!({ "path": "s3://bucket/out.parquet" })),
    );

    assert_eq!(plan.extensions(), ["httpfs", "iceberg"]);
}

#[test]
fn a_plan_that_needs_nothing_emits_no_prelude_and_no_probe() {
    let plan = compile_two(
        ("src.file.csv", json!({ "path": "in.csv" })),
        ("snk.file.parquet", json!({ "path": "out.parquet" })),
    );

    assert!(plan.extensions().is_empty());
    assert!(!plan.has_prelude_probe(true));
    assert!(!plan.script(true).contains("LOAD"));
}

#[test]
fn the_stages_needing_an_extension_can_be_named() {
    // So a warning can say which node is the reason, not only which extension.
    let plan = compile_two(
        ("src.lake.delta", json!({ "path": "warehouse/orders" })),
        ("snk.file.parquet", json!({ "path": "out.parquet" })),
    );

    let culprits: Vec<&str> = plan
        .stages_needing("delta")
        .map(|stage| stage.node_id.as_str())
        .collect();

    assert_eq!(culprits, ["a"]);
}

// ---------------------------------------------------------------------------
// Materialisation
//
// The mode changes the statement shape and nothing else — same relation name,
// same downstream SQL, same answer. These pin that.
// ---------------------------------------------------------------------------

#[test]
fn a_node_with_no_mode_is_a_view_exactly_as_before() {
    assert_eq!(
        sql_of(&compile_materialized(None, None), "b"),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT DISTINCT * FROM "a");"#
    );
}

#[test]
fn auto_and_view_produce_the_same_statement_for_now() {
    // `auto` will one day choose; until there are statistics to choose on, it
    // is a view, and saying so out loud keeps the eventual change visible.
    assert_eq!(
        sql_of(&compile_materialized(Some("auto"), None), "b"),
        sql_of(&compile_materialized(Some("view"), None), "b")
    );
}

#[test]
fn memory_makes_a_temp_table() {
    assert_eq!(
        sql_of(&compile_materialized(Some("memory"), None), "b"),
        r#"CREATE OR REPLACE TEMP TABLE "b" AS (SELECT DISTINCT * FROM "a");"#
    );
}

#[test]
fn disk_spills_to_parquet_and_reads_it_back() {
    assert_eq!(
        sql_of(&compile_materialized(Some("disk"), None), "b"),
        "COPY (SELECT DISTINCT * FROM \"a\") TO '.etl/tmp/b.parquet' (FORMAT parquet);\n\
         CREATE OR REPLACE TEMP VIEW \"b\" AS (SELECT * FROM read_parquet('.etl/tmp/b.parquet'));"
    );
}

#[test]
fn only_a_disk_node_has_a_spill_path() {
    for (mode, expected) in [
        (None, None),
        (Some("auto"), None),
        (Some("view"), None),
        (Some("memory"), None),
        (Some("disk"), Some(".etl/tmp/b.parquet")),
    ] {
        let plan = compile_materialized(mode, None);

        assert_eq!(
            plan.stage("b").unwrap().spill_path.as_deref(),
            expected,
            "mode {mode:?}"
        );
        assert_eq!(plan.spills(), expected.into_iter().collect::<Vec<_>>());
    }
}

#[test]
fn the_alias_view_is_added_whichever_mode_is_chosen() {
    // The alias is a view on top of the relation, so it must not depend on how
    // the relation underneath it was realised.
    for mode in [None, Some("view"), Some("memory"), Some("disk")] {
        let plan = compile_materialized(mode, Some("clean"));

        assert!(
            sql_of(&plan, "b")
                .contains(r#"CREATE OR REPLACE TEMP VIEW "clean" AS SELECT * FROM "b";"#),
            "mode {mode:?}: {}",
            sql_of(&plan, "b")
        );
    }
}

#[test]
fn a_mode_this_engine_does_not_know_warns_and_falls_back() {
    // Safe to carry on: the mode changes how the work is done, never the
    // answer, so refusing to run over it would be the wrong trade.
    let plan = compile_materialized(Some("quantum"), None);

    // Alongside the NoSink warning this two-node document earns anyway.
    assert!(
        plan.warnings
            .contains(&crate::plan::Warning::UnknownMaterialize {
                id: "b".to_string(),
                value: "quantum".to_string(),
            }),
        "{:?}",
        plan.warnings
    );
    assert_eq!(
        sql_of(&plan, "b"),
        r#"CREATE OR REPLACE TEMP VIEW "b" AS (SELECT DISTINCT * FROM "a");"#
    );
}

#[test]
fn a_sink_cannot_be_materialised() {
    // A sink writes its output; there is no relation to hold, and a COPY
    // wrapped in a temp table would be nonsense.
    let mut sink =
        super::tests_support::node("b", "snk.file.parquet", json!({ "path": "out.parquet" }));
    sink.data.materialize = Some("memory".to_string());

    let plan = crate::plan::compile(&super::tests_support::document(
        vec![
            super::tests_support::node("a", "src.file.csv", json!({ "path": "in.csv" })),
            sink,
        ],
        vec![super::tests_support::edge("e0", "a", "b", Some("in"))],
    ))
    .expect("compiles");

    assert_eq!(
        plan.stage("b").unwrap().materialize,
        crate::plan::Materialize::Auto
    );
    assert_eq!(
        sql_of(&plan, "b"),
        r#"COPY (SELECT * FROM "a") TO 'out.parquet' (FORMAT parquet, COMPRESSION 'zstd');"#
    );
}

#[test]
fn materialising_does_not_change_what_downstream_nodes_select_from() {
    // The relation is named after the node whichever way it is realised, so a
    // downstream stage's SQL is identical.
    let mut middle = super::tests_support::node("b", "xf.distinct", json!({}));
    middle.data.materialize = Some("memory".to_string());

    let plan = crate::plan::compile(&super::tests_support::document(
        vec![
            super::tests_support::node("a", "src.file.csv", json!({ "path": "in.csv" })),
            middle,
            super::tests_support::node("c", "xf.sort", json!({ "by": "id" })),
        ],
        vec![
            super::tests_support::edge("e0", "a", "b", Some("in")),
            super::tests_support::edge("e1", "b", "c", Some("in")),
        ],
    ))
    .expect("compiles");

    assert_eq!(
        sql_of(&plan, "c"),
        r#"CREATE OR REPLACE TEMP VIEW "c" AS (SELECT * FROM "b" ORDER BY id);"#
    );
}

// ---------------------------------------------------------------------------
// Helpers that need to return errors
// ---------------------------------------------------------------------------

fn compile_one_err(component_id: &str, properties: serde_json::Value) -> EngineError {
    super::tests_support::compile_one_result(component_id, properties, None)
        .expect_err("expected this to fail")
}

fn compile_two_err(
    first: (&str, serde_json::Value),
    second: (&str, serde_json::Value),
) -> EngineError {
    super::tests_support::compile_two_result(first, second).expect_err("expected this to fail")
}
