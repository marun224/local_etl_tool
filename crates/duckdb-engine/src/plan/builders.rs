//! Component → SQL.
//!
//! Each builder turns one node into the complete statement (or statements)
//! that realise it. Two shapes, following DuckDB's strengths:
//!
//! * Anything that produces a relation becomes
//!   `CREATE OR REPLACE TEMP VIEW "<node_id>" AS (...)`. Views are lazy, so
//!   nothing computes until a sink pulls, and a filter written above a source
//!   pushes down into the source's own scan.
//! * Sinks become `COPY (...) TO '...'`, which is the only thing in a plan
//!   that does real work.
//!
//! The eight components here are hand-written. Phase 3 replaces this module's
//! dispatch with a spec registry so that adding the ninth is data rather than
//! another match arm.

use super::{reject_relation, Control, CountProbe, Input, Materialize, StageKind};
use crate::sql::{quote_identifier, quote_literal, quote_path};
use crate::EngineError;
use etl_metadata::{ControlKind, MAIN_PORT, REJECTED_PORT};
use etl_plugin_sdk::ROW_KEY;
use serde_json::{Map as JsonMap, Value as JsonValue};

/// What a builder needs to know about the node it is lowering.
pub(crate) struct Lowering<'a> {
    pub node_id: &'a str,
    pub component_id: &'a str,
    pub properties: &'a JsonValue,
    pub inputs: &'a [Input],
    pub alias: Option<&'a str>,
    /// How this node's relation is realised. Builders do not read it: they hand
    /// a query body to `create_view`, which decides the statement shape.
    pub materialize: Materialize,
    /// Where a `disk` node spills to. Set by the planner, not by the builder.
    pub spill_path: Option<&'a str>,
    /// Where a native component's records are staged. Set by the planner for
    /// a component written in Rust, and `None` for everything else.
    pub staging: Option<&'a str>,
    /// The watermark filter for an incremental source, when there is one to
    /// apply. Builders do not read it either: like `materialize`, it is applied
    /// by `create_view`, so it lands on all twelve sources rather than on
    /// whichever ones remembered to ask.
    pub incremental: Option<IncrementalFilter<'a>>,
}

/// "Only the rows after this", resolved.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IncrementalFilter<'a> {
    pub column: &'a str,
    /// The high-water mark to read past. `None` on the first run of a source
    /// that declared no `start`, which means load everything.
    pub since: Option<&'a str>,
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

pub(crate) fn source_csv(node: &Lowering<'_>) -> Result<String, EngineError> {
    let path = required_str(node, "path")?;

    let mut arguments = vec![quote_path(path)];
    arguments.push(format!("header={}", resolved_bool(node, "header")?));

    // Only pin the delimiter when the user asked for one; left out, DuckDB
    // sniffs it, which is right more often than a guess of ours would be.
    if let Some(delimiter) = optional_str(node, "delimiter")? {
        arguments.push(format!("delim={}", quote_literal(delimiter)));
    }

    let body = format!("SELECT * FROM read_csv({})", arguments.join(", "));
    Ok(create_view(node, &body))
}

pub(crate) fn source_parquet(node: &Lowering<'_>) -> Result<String, EngineError> {
    let path = required_str(node, "path")?;
    let body = format!("SELECT * FROM read_parquet({})", quote_path(path));

    Ok(create_view(node, &body))
}

pub(crate) fn source_jsonl(node: &Lowering<'_>) -> Result<String, EngineError> {
    let path = required_str(node, "path")?;

    let body = format!(
        "SELECT * FROM read_json({}, format='newline_delimited', ignore_errors={})",
        quote_path(path),
        resolved_bool(node, "ignore_errors")?
    );

    Ok(create_view(node, &body))
}

// ---------------------------------------------------------------------------
// Transforms
// ---------------------------------------------------------------------------

pub(crate) fn transform_sql(node: &Lowering<'_>) -> Result<String, EngineError> {
    // Deliberately unescaped: the whole point of this component is that the
    // user writes SQL. Upstream nodes are in scope under their node ids.
    let query = required_str(node, "query")?;
    let body = query.trim().trim_end_matches(';').to_string();

    Ok(create_view(node, &body))
}

pub(crate) fn transform_filter(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let predicate = required_str(node, "predicate")?;

    let body = format!(
        "SELECT * FROM {} WHERE {}",
        quote_identifier(&upstream),
        predicate.trim()
    );

    Ok(create_view(node, &body))
}

pub(crate) fn transform_select(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;

    // Column names, not expressions — they are quoted as identifiers so a
    // column called `order` works. Expressions belong to `xf.derive`.
    let projected = column_list(node, "columns", required_array(node, "columns")?)?;

    let body = format!(
        "SELECT {} FROM {}",
        projected.join(", "),
        quote_identifier(&upstream)
    );

    Ok(create_view(node, &body))
}

pub(crate) fn transform_join(node: &Lowering<'_>) -> Result<String, EngineError> {
    let (left, right) = exactly_two_inputs(node)?;

    let join_type = match resolved_str(node, "type")? {
        "inner" => "INNER JOIN",
        "left" => "LEFT JOIN",
        "right" => "RIGHT JOIN",
        "full" | "outer" => "FULL OUTER JOIN",
        "cross" => "CROSS JOIN",
        other => {
            return Err(EngineError::InvalidProperty {
                id: node.node_id.to_string(),
                property: "type".to_string(),
                reason: format!(
                    "'{other}' is not a join type; use inner, left, right, full, or cross"
                ),
            })
        }
    };

    let condition = join_condition(node, join_type)?;

    let body = format!(
        "SELECT * FROM {} {} {}{}",
        quote_identifier(&left),
        join_type,
        quote_identifier(&right),
        condition
    );

    Ok(create_view(node, &body))
}

/// A join is keyed either on shared column names (`keys`) or on a raw
/// predicate (`condition`). A cross join takes neither.
fn join_condition(node: &Lowering<'_>, join_type: &str) -> Result<String, EngineError> {
    let keys = optional_array(node, "keys")?;
    let condition = optional_str(node, "condition")?;

    if join_type == "CROSS JOIN" {
        return if keys.is_some() || condition.is_some() {
            Err(EngineError::InvalidProperty {
                id: node.node_id.to_string(),
                property: "type".to_string(),
                reason: "a cross join takes neither keys nor a condition".to_string(),
            })
        } else {
            Ok(String::new())
        };
    }

    match (keys, condition) {
        (Some(_), Some(_)) => Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: "keys".to_string(),
            reason: "set either keys or condition, not both".to_string(),
        }),

        (Some(keys), None) if !keys.is_empty() => {
            let names = keys
                .iter()
                .map(|key| {
                    key.as_str()
                        .map(quote_identifier)
                        .ok_or_else(|| EngineError::InvalidProperty {
                            id: node.node_id.to_string(),
                            property: "keys".to_string(),
                            reason: "every key must be a column name".to_string(),
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(format!(" USING ({})", names.join(", ")))
        }

        (None, Some(condition)) => Ok(format!(" ON {}", condition.trim())),

        _ => Err(EngineError::MissingProperty {
            id: node.node_id.to_string(),
            component_id: node.component_id.to_string(),
            property: "keys".to_string(),
        }),
    }
}

pub(crate) fn source_json(node: &Lowering<'_>) -> Result<String, EngineError> {
    let path = required_str(node, "path")?;

    // `format='array'` rather than DuckDB's auto-detection: this component and
    // `src.file.jsonl` differ in exactly that setting, and a source that
    // silently accepted either shape would make the pair pointless.
    let body = format!(
        "SELECT * FROM read_json({}, format='array', ignore_errors={})",
        quote_path(path),
        resolved_bool(node, "ignore_errors")?
    );

    Ok(create_view(node, &body))
}

// ---------------------------------------------------------------------------
// Sources: spreadsheets
// ---------------------------------------------------------------------------

pub(crate) fn source_excel(node: &Lowering<'_>) -> Result<String, EngineError> {
    let path = required_str(node, "path")?;

    let mut arguments = vec![quote_path(path)];
    arguments.push(format!("header={}", resolved_bool(node, "header")?));

    // Unset, DuckDB reads the first sheet, which is right more often than any
    // name we could guess.
    if let Some(sheet) = optional_str(node, "sheet")? {
        arguments.push(format!("sheet={}", quote_literal(sheet)));
    }

    let body = format!("SELECT * FROM read_xlsx({})", arguments.join(", "));

    Ok(create_view(node, &body))
}

// ---------------------------------------------------------------------------
// Sources: object storage
// ---------------------------------------------------------------------------

/// `src.cloud.s3` and `src.cloud.http` differ only in the URI scheme their
/// `path` carries, so they share a body. Both go through `httpfs`.
fn cloud_reader(node: &Lowering<'_>) -> Result<String, EngineError> {
    let path = required_str(node, "path")?;

    let body = match resolved_str(node, "format")? {
        "parquet" => format!("SELECT * FROM read_parquet({})", quote_path(path)),
        "csv" => format!(
            "SELECT * FROM read_csv({}, header={})",
            quote_path(path),
            resolved_bool(node, "header")?
        ),
        "json" => format!("SELECT * FROM read_json({})", quote_path(path)),
        other => {
            return Err(EngineError::InvalidProperty {
                id: node.node_id.to_string(),
                property: "format".to_string(),
                reason: format!("'{other}' is not a readable format; use parquet, csv, or json"),
            })
        }
    };

    Ok(create_view(node, &body))
}

pub(crate) fn source_s3(node: &Lowering<'_>) -> Result<String, EngineError> {
    with_s3_secret(node, cloud_reader(node)?)
}

/// Put this node's S3 credentials ahead of its statement, if it has any.
///
/// A DuckDB secret, `TEMPORARY` by default so nothing is written to the
/// user's DuckDB home, named after the node and scoped to its bucket. The scope
/// is what lets two nodes reach two buckets as two different accounts: DuckDB
/// picks the secret whose scope matches the path. Two nodes giving different
/// credentials for the *same* bucket is the one arrangement this cannot serve.
///
/// Nothing set means no secret at all, which leaves DuckDB's own defaults -- the
/// environment's AWS variables, or anonymous access -- exactly as before.
fn with_s3_secret(node: &Lowering<'_>, statement: String) -> Result<String, EngineError> {
    let key_id = optional_str(node, "key_id")?;
    let secret = optional_str(node, "secret")?;
    let session_token = optional_str(node, "session_token")?;
    let region = optional_str(node, "region")?;
    let endpoint = optional_str(node, "endpoint")?;
    let url_style = optional_str(node, "url_style")?;

    if [key_id, secret, session_token, region, endpoint, url_style]
        .iter()
        .all(Option::is_none)
    {
        return Ok(statement);
    }

    if key_id.is_some() != secret.is_some() {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: if key_id.is_some() { "secret" } else { "key_id" }.to_string(),
            reason: "is needed too: key_id and secret only work as a pair".to_string(),
        });
    }

    let mut options = vec!["TYPE s3".to_string()];
    let mut option = |name: &str, value: Option<&str>| {
        if let Some(value) = value {
            options.push(format!("{name} {}", quote_literal(value)));
        }
    };
    option("KEY_ID", key_id);
    option("SECRET", secret);
    option("SESSION_TOKEN", session_token);
    option("REGION", region);
    option("URL_STYLE", url_style);

    if let Some(endpoint) = endpoint {
        // DuckDB wants host:port. A scheme, which is how people usually write
        // an endpoint, is read as the answer to use_ssl rather than refused.
        let (host, ssl) = match endpoint.split_once("://") {
            Some(("http", host)) => (host, false),
            Some(("https", host)) => (host, true),
            _ => (endpoint, resolved_bool(node, "use_ssl")?),
        };
        options.push(format!(
            "ENDPOINT {}",
            quote_literal(host.trim_end_matches('/'))
        ));
        options.push(format!("USE_SSL {ssl}"));
    }

    let path = required_str(node, "path")?;
    options.push(format!("SCOPE {}", quote_literal(&bucket_of(path))));

    Ok(format!(
        "CREATE OR REPLACE SECRET {} ({});\n{statement}",
        quote_identifier(&format!("{}_s3", node.node_id)),
        options.join(", ")
    ))
}

/// `s3://bucket/some/key*.csv` → `s3://bucket`.
fn bucket_of(path: &str) -> String {
    match path.split_once("://") {
        Some((scheme, rest)) => {
            let bucket = rest.split('/').next().unwrap_or(rest);
            format!("{scheme}://{bucket}")
        }
        None => path.to_string(),
    }
}

pub(crate) fn source_http(node: &Lowering<'_>) -> Result<String, EngineError> {
    cloud_reader(node)
}

// ---------------------------------------------------------------------------
// Sources: lakehouse table formats
// ---------------------------------------------------------------------------

pub(crate) fn source_iceberg(node: &Lowering<'_>) -> Result<String, EngineError> {
    let path = required_str(node, "path")?;
    let moved = resolved_bool(node, "allow_moved_paths")?;
    let version = optional_str(node, "version")?;

    // Found in Phase 10c against a table written by pyiceberg and then moved:
    // with allow_moved_paths, DuckDB treats `path` as the table's root and
    // joins `metadata/...` onto it, so a metadata-file path becomes
    // `….metadata.json/metadata/snap-….avro` and fails with a message about a
    // file nobody named. Said plainly here instead.
    if moved
        && path
            .trim_end_matches(['/', '\\'])
            .ends_with(".metadata.json")
    {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: "path".to_string(),
            reason: "must be the table's root directory when allow_moved_paths is set, not a \
                     .metadata.json file; name the metadata file in version instead"
                .to_string(),
        });
    }

    let mut arguments = vec![quote_path(path), format!("allow_moved_paths={moved}")];
    if let Some(version) = version {
        arguments.push(format!("version={}", quote_literal(version)));
    }

    let body = format!("SELECT * FROM iceberg_scan({})", arguments.join(", "));

    Ok(create_view(node, &body))
}

pub(crate) fn source_delta(node: &Lowering<'_>) -> Result<String, EngineError> {
    let path = required_str(node, "path")?;
    let body = format!("SELECT * FROM delta_scan({})", quote_path(path));

    Ok(create_view(node, &body))
}

// ---------------------------------------------------------------------------
// Databases, read and written through ATTACH
// ---------------------------------------------------------------------------

/// Attach the database this node talks to, under an alias of its own.
///
/// The alias is derived from the node id rather than fixed, because two nodes
/// reading different tables out of the same database both attach it, and a
/// shared alias would make the second one fail. Attaching the same database
/// twice under two aliases is fine.
fn attach_database(
    node: &Lowering<'_>,
    database_type: &str,
    read_only: bool,
) -> Result<(String, String), EngineError> {
    let connection = required_str(node, "connection")?;
    let alias = format!("{}_db", node.node_id);

    let mut options = format!("TYPE {database_type}");
    if read_only {
        options.push_str(", READ_ONLY");
    }

    let statement = format!(
        "ATTACH {} AS {} ({});",
        quote_literal(connection),
        quote_identifier(&alias),
        options
    );

    Ok((statement, alias))
}

/// The table's name inside the attached database. The schema segment is left
/// out when the node does not name one, so the database's own default applies.
fn qualified_table(node: &Lowering<'_>, alias: &str) -> Result<String, EngineError> {
    let table = required_str(node, "table")?;
    let mut name = quote_identifier(alias);

    if let Some(schema) = optional_str(node, "schema")? {
        name.push('.');
        name.push_str(&quote_identifier(schema));
    }

    name.push('.');
    name.push_str(&quote_identifier(table));

    Ok(name)
}

fn source_database(node: &Lowering<'_>, database_type: &str) -> Result<String, EngineError> {
    let (attach, alias) = attach_database(node, database_type, true)?;
    let body = format!("SELECT * FROM {}", qualified_table(node, &alias)?);

    Ok(format!("{attach}\n{}", create_view(node, &body)))
}

pub(crate) fn source_postgres(node: &Lowering<'_>) -> Result<String, EngineError> {
    source_database(node, "postgres")
}

pub(crate) fn source_mysql(node: &Lowering<'_>) -> Result<String, EngineError> {
    // Found in Phase 10c against a real MySQL 8.4: with DuckDB 1.5.5's mysql
    // extension, any aggregate over a *view* of a MySQL table -- a count, a
    // sum, a GROUP BY -- fails with "INTERNAL Error: Failed to bind column
    // reference". The same aggregate straight on the table works. Every source
    // here is a view, so the row count alone tripped it. Turning the
    // extension's aggregate pushdown off fixes every case tried; the aggregate
    // then runs in DuckDB, which is slower for a huge table and correct for
    // all of them. Revisit when the pinned DuckDB moves.
    Ok(format!(
        "SET mysql_aggregate_pushdown_enabled=false;\n{}",
        source_database(node, "mysql")?
    ))
}

pub(crate) fn source_sqlite(node: &Lowering<'_>) -> Result<String, EngineError> {
    source_database(node, "sqlite")
}

fn sink_database(node: &Lowering<'_>, database_type: &str) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let (attach, alias) = attach_database(node, database_type, false)?;

    let table = qualified_table(node, &alias)?;
    let source = quote_identifier(&upstream);

    // A database sink's modes are not a file sink's: there is no
    // `error_if_exists` here, because the useful third option against a table
    // is appending to it.
    let write = match resolved_str(node, "mode")? {
        "overwrite" => format!("CREATE OR REPLACE TABLE {table} AS SELECT * FROM {source};"),
        // `IF NOT EXISTS ... WHERE false` creates the table with the right
        // columns and no rows when it is not there yet, so the first run of an
        // appending pipeline works instead of failing on a missing table. It is
        // idempotent, so every run after that is just the insert.
        "append" => format!(
            "CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM {source} WHERE false;\n\
             INSERT INTO {table} SELECT * FROM {source};"
        ),
        other => {
            return Err(EngineError::InvalidProperty {
                id: node.node_id.to_string(),
                property: "mode".to_string(),
                reason: format!("'{other}' is not a write mode; use overwrite or append"),
            })
        }
    };

    Ok(format!("{attach}\n{write}"))
}

pub(crate) fn sink_postgres(node: &Lowering<'_>) -> Result<String, EngineError> {
    sink_database(node, "postgres")
}

/// Found in Phase 10q, on MySQL 8.4 and MariaDB 11.8 alike: DuckDB's mysql
/// extension creates a TIMESTAMP column as `DATETIME`, whole seconds, so a
/// table the sink created dropped every timestamp's fraction without a word.
/// So when the sink creates the table, it creates it empty, has MySQL widen
/// each timestamp column to `DATETIME(6)`, and then inserts. The `ALTER` is
/// written by DuckDB at run time, from the upstream's own columns, and sent
/// with `mysql_execute`; a table that was already there is left exactly as its
/// owner made it (Open question 16, answered: fix it).
pub(crate) fn sink_mysql(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let (attach, alias) = attach_database(node, "mysql", false)?;
    let table = qualified_table(node, &alias)?;
    let source = quote_identifier(&upstream);

    let backticked = |name: &str| format!("`{}`", name.replace('`', "``"));
    let name = required_str(node, "table")?;
    let schema = optional_str(node, "schema")?;
    let mysql_table = match schema {
        Some(schema) => format!("{}.{}", backticked(schema), backticked(name)),
        None => backticked(name),
    };
    let widen = |already_there: &str| {
        format!(
            "SET VARIABLE etl_fractions = (SELECT CASE WHEN {already_there} THEN 'DO 0' ELSE \
             coalesce({} || string_agg('MODIFY `' || replace(column_name, '`', '``') || '` \
             DATETIME(6)', ', '), 'DO 0') END FROM (DESCRIBE {source}) WHERE column_type LIKE \
             'TIMESTAMP%');\n\
             CALL mysql_execute({}, getvariable('etl_fractions'));\n\
             CALL mysql_clear_cache();",
            quote_literal(&format!("ALTER TABLE {mysql_table} ")),
            quote_literal(&alias),
        )
    };

    let write = match resolved_str(node, "mode")? {
        "overwrite" => format!(
            "CREATE OR REPLACE TABLE {table} AS SELECT * FROM {source} WHERE false;\n{}\n\
             INSERT INTO {table} SELECT * FROM {source};",
            widen("false")
        ),
        "append" => {
            let mut there = format!(
                "database_name = {} AND table_name = {}",
                quote_literal(&alias),
                quote_literal(name)
            );
            if let Some(schema) = schema {
                there.push_str(&format!(" AND schema_name = {}", quote_literal(schema)));
            }
            format!(
                "SET VARIABLE etl_existed = (SELECT count(*) > 0 FROM duckdb_tables() WHERE {there});\n\
                 CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM {source} WHERE false;\n{}\n\
                 INSERT INTO {table} SELECT * FROM {source};",
                widen("getvariable('etl_existed')")
            )
        }
        other => {
            return Err(EngineError::InvalidProperty {
                id: node.node_id.to_string(),
                property: "mode".to_string(),
                reason: format!("'{other}' is not a write mode; use overwrite or append"),
            })
        }
    };

    Ok(format!("{attach}\n{write}"))
}

pub(crate) fn sink_sqlite(node: &Lowering<'_>) -> Result<String, EngineError> {
    sink_database(node, "sqlite")
}

// ---------------------------------------------------------------------------
// Transforms: reshaping columns
// ---------------------------------------------------------------------------

pub(crate) fn transform_derive(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let expressions = required_str(node, "expressions")?;

    let body = format!(
        "SELECT *, {} FROM {}",
        expressions.trim().trim_end_matches(','),
        quote_identifier(&upstream)
    );

    Ok(create_view(node, &body))
}

pub(crate) fn transform_rename(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let pairs = non_empty_map(node, "columns")?;

    let renames = map_entries(node, "columns", pairs, |from, to| {
        Ok(format!(
            "{} AS {}",
            quote_identifier(from),
            quote_identifier(to)
        ))
    })?;

    let body = format!(
        "SELECT * RENAME ({}) FROM {}",
        renames.join(", "),
        quote_identifier(&upstream)
    );

    Ok(create_view(node, &body))
}

pub(crate) fn transform_cast(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let pairs = non_empty_map(node, "columns")?;

    // `REPLACE` rather than a full projection, so casting one column does not
    // mean naming every other column that should come through untouched.
    let casts = map_entries(node, "columns", pairs, |column, declared| {
        Ok(format!(
            "CAST({} AS {}) AS {}",
            quote_identifier(column),
            type_name(node, declared)?,
            quote_identifier(column)
        ))
    })?;

    let body = format!(
        "SELECT * REPLACE ({}) FROM {}",
        casts.join(", "),
        quote_identifier(&upstream)
    );

    Ok(create_view(node, &body))
}

// ---------------------------------------------------------------------------
// Transforms: choosing rows
// ---------------------------------------------------------------------------

pub(crate) fn transform_distinct(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let body = format!("SELECT DISTINCT * FROM {}", quote_identifier(&upstream));

    Ok(create_view(node, &body))
}

pub(crate) fn transform_dedup(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let keys = column_list(node, "keys", required_array(node, "keys")?)?;

    // `QUALIFY` rather than `DISTINCT ON`: it keeps the whole row, and it lets
    // the caller say which of a set of duplicates wins.
    let mut window = format!("PARTITION BY {}", keys.join(", "));
    if let Some(order_by) = optional_str(node, "order_by")? {
        window.push_str(&format!(" ORDER BY {}", order_by.trim()));
    }

    let body = format!(
        "SELECT * FROM {} QUALIFY row_number() OVER ({}) = 1",
        quote_identifier(&upstream),
        window
    );

    Ok(create_view(node, &body))
}

pub(crate) fn transform_sort(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let by = required_str(node, "by")?;

    let body = format!(
        "SELECT * FROM {} ORDER BY {}",
        quote_identifier(&upstream),
        by.trim()
    );

    Ok(create_view(node, &body))
}

pub(crate) fn transform_limit(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let count = non_negative(node, "count")?;
    let offset = non_negative(node, "offset")?;

    let mut body = format!(
        "SELECT * FROM {} LIMIT {}",
        quote_identifier(&upstream),
        count
    );
    if offset > 0 {
        body.push_str(&format!(" OFFSET {offset}"));
    }

    Ok(create_view(node, &body))
}

pub(crate) fn transform_sample(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let size = non_negative(node, "size")?;

    // `reservoir` for percentages: the default system sampler works a row group
    // at a time and returns nothing at all from a small input, which reads as a
    // broken pipeline rather than as a choice of sampling method.
    let clause = match resolved_str(node, "unit")? {
        "rows" => format!("USING SAMPLE {size} ROWS"),
        "percent" => format!("USING SAMPLE reservoir({size} PERCENT)"),
        other => {
            return Err(EngineError::InvalidProperty {
                id: node.node_id.to_string(),
                property: "unit".to_string(),
                reason: format!("'{other}' is not a sample unit; use rows or percent"),
            })
        }
    };

    let body = format!("SELECT * FROM {} {}", quote_identifier(&upstream), clause);

    Ok(create_view(node, &body))
}

// ---------------------------------------------------------------------------
// Transforms: summarising
// ---------------------------------------------------------------------------

pub(crate) fn transform_aggregate(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let aggregations = required_str(node, "aggregations")?
        .trim()
        .trim_end_matches(',');
    let groups = optional_column_list(node, "group_by")?;

    // No grouping columns is a whole-table aggregate, which is a legitimate
    // thing to ask for rather than a property someone forgot.
    let body = if groups.is_empty() {
        format!(
            "SELECT {} FROM {}",
            aggregations,
            quote_identifier(&upstream)
        )
    } else {
        let grouped = groups.join(", ");
        format!(
            "SELECT {}, {} FROM {} GROUP BY {}",
            grouped,
            aggregations,
            quote_identifier(&upstream),
            grouped
        )
    };

    Ok(create_view(node, &body))
}

pub(crate) fn transform_window(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let expression = required_str(node, "expression")?;
    let output = required_str(node, "output_column")?;

    let mut over = String::new();
    let partition = optional_column_list(node, "partition_by")?;
    if !partition.is_empty() {
        over.push_str(&format!("PARTITION BY {}", partition.join(", ")));
    }
    if let Some(order_by) = optional_str(node, "order_by")? {
        if !over.is_empty() {
            over.push(' ');
        }
        over.push_str(&format!("ORDER BY {}", order_by.trim()));
    }

    let body = format!(
        "SELECT *, {} OVER ({}) AS {} FROM {}",
        expression.trim(),
        over,
        quote_identifier(output),
        quote_identifier(&upstream)
    );

    Ok(create_view(node, &body))
}

pub(crate) fn transform_pivot(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let on = column_list(node, "on", required_array(node, "on")?)?;
    let using = required_str(node, "using")?;

    // The value list is required rather than optional. Every stage in a plan is
    // a view, and DuckDB refuses to build a view around a PIVOT whose result
    // columns it would have to discover by reading the data first.
    let values = required_array(node, "values")?;
    if values.is_empty() {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: "values".to_string(),
            reason: "must list at least one value".to_string(),
        });
    }

    let values = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(quote_literal)
                .ok_or_else(|| EngineError::InvalidProperty {
                    id: node.node_id.to_string(),
                    property: "values".to_string(),
                    reason: "every value must be text".to_string(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut body = format!(
        "PIVOT {} ON {} IN ({}) USING {}",
        quote_identifier(&upstream),
        on.join(", "),
        values.join(", "),
        using.trim()
    );

    let groups = optional_column_list(node, "group_by")?;
    if !groups.is_empty() {
        body.push_str(&format!(" GROUP BY {}", groups.join(", ")));
    }

    Ok(create_view(node, &body))
}

pub(crate) fn transform_unpivot(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let columns = column_list(node, "columns", required_array(node, "columns")?)?;

    let body = format!(
        "UNPIVOT {} ON {} INTO NAME {} VALUE {}",
        quote_identifier(&upstream),
        columns.join(", "),
        quote_identifier(resolved_str(node, "name_column")?),
        quote_identifier(resolved_str(node, "value_column")?)
    );

    Ok(create_view(node, &body))
}

// ---------------------------------------------------------------------------
// Transforms: text for AI
// ---------------------------------------------------------------------------

/// Split a text column into chunks that end at whitespace, overlapping by
/// whole words where the overlap holds a word boundary.
///
/// A recursive query, one step per chunk: where the next chunk starts, then
/// how long it is. Every step moves forward, because a chunk is always longer
/// than the overlap: a whitespace cut that would leave it shorter is not taken,
/// and a hard cut is `size`, which the builder has checked is larger.
pub(crate) fn transform_ai_chunk(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let column = quote_identifier(resolved_str(node, "column")?);
    let size = non_negative(node, "size")?;
    let overlap = non_negative(node, "overlap")?;
    if size == 0 {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: "size".to_string(),
            reason: "must be at least 1".to_string(),
        });
    }
    if overlap >= size {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: "overlap".to_string(),
            reason: format!("must be smaller than size ({size})"),
        });
    }
    let output = resolved_str(node, "output")?;
    let index = quote_identifier(&format!("{output}_index"));
    let output = quote_identifier(output);

    // Where the last whitespace in the `size` characters from `start` is,
    // counted back from the end of that window: 0 when there is none.
    let from_end = |text: &str, start: &str| {
        format!("strpos(reverse(regexp_replace(substring({text}, {start}, {size}), '\\s', ' ', 'g')), ' ')")
    };
    // How long the chunk from `start` is: the rest of the text if it fits,
    // else up to the last whitespace if that leaves more than the overlap,
    // else `size`.
    let cut = |text: &str, start: &str| {
        let back = from_end(text, start);
        format!(
            "CASE WHEN {start} + {size} - 1 >= length({text}) THEN length({text}) - {start} + 1 \
             WHEN {back} BETWEEN 1 AND {limit} THEN {size} - {back} \
             ELSE {size} END",
            limit = size - overlap - 1
        )
    };
    // Where the chunk after one ends starts: `overlap` characters back, moved
    // on to the next word's start within them when there is one.
    let raw = format!("p.__etl_start + p.__etl_cut - {overlap}");
    let next = format!(
        "CASE WHEN regexp_matches(substring(p.__etl_text, {raw} - 1, 1), '\\s') THEN {raw} \
         WHEN strpos(regexp_replace(substring(p.__etl_text, {raw}, {overlap}), '\\s', ' ', 'g'), ' ') > 0 \
         THEN {raw} + strpos(regexp_replace(substring(p.__etl_text, {raw}, {overlap}), '\\s', ' ', 'g'), ' ') \
         ELSE {raw} END"
    );

    let body = format!(
        "WITH RECURSIVE \
         __etl_rows AS (SELECT *, row_number() OVER () AS __etl_row, CAST({column} AS VARCHAR) AS __etl_text FROM {upstream}), \
         __etl_chunks AS (\
         SELECT r.__etl_row, r.__etl_text, CAST(1 AS BIGINT) AS __etl_start, CAST(0 AS BIGINT) AS __etl_step, c.__etl_cut \
         FROM __etl_rows r, LATERAL (SELECT {first_cut} AS __etl_cut) c \
         WHERE length(trim(r.__etl_text)) > 0 \
         UNION ALL \
         SELECT p.__etl_row, p.__etl_text, n.__etl_next, p.__etl_step + 1, c.__etl_cut \
         FROM __etl_chunks p, LATERAL (SELECT {next} AS __etl_next) n, LATERAL (SELECT {next_cut} AS __etl_cut) c \
         WHERE p.__etl_start + {size} - 1 < length(p.__etl_text)) \
         SELECT r.* EXCLUDE (__etl_row, __etl_text, {column}), \
         row_number() OVER (PARTITION BY c.__etl_row ORDER BY c.__etl_step) - 1 AS {index}, c.__etl_chunk AS {output} \
         FROM (SELECT __etl_row, __etl_step, trim(substring(__etl_text, __etl_start, __etl_cut)) AS __etl_chunk FROM __etl_chunks) c \
         JOIN __etl_rows r USING (__etl_row) \
         WHERE c.__etl_chunk <> '' \
         ORDER BY c.__etl_row, c.__etl_step",
        upstream = quote_identifier(&upstream),
        first_cut = cut("r.__etl_text", "1"),
        next_cut = cut("p.__etl_text", "n.__etl_next"),
    );

    Ok(create_view(node, &body))
}

/// What `xf.ai.redact` can find, in the order it looks: an IP address before a
/// phone number, whose pattern would otherwise take three of its groups.
///
/// Each is (name, pattern, token). RE2 syntax, as DuckDB's regular expressions
/// are; `\b` is an ASCII word boundary.
const REDACT_PATTERNS: [(&str, &str, &str); 5] = [
    (
        "email",
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}",
        "EMAIL",
    ),
    (
        "ip",
        r"\b(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\b|\b(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}\b",
        "IP",
    ),
    ("credit_card", r"\b(?:[0-9][ -]?){12,18}[0-9]\b", "CARD"),
    ("ssn", r"\b[0-9]{3}-[0-9]{2}-[0-9]{4}\b", "SSN"),
    // A whole run of digit groups, judged afterwards (`accepts`): a pattern
    // alone would take the first three groups of a longer number.
    (
        "phone",
        r"(?:\+[0-9]{1,3}[ .-]?)?(?:\([0-9]{1,4}\)[ .-]?)?\b[0-9]+(?:[ .-][0-9]+)*\b",
        "PHONE",
    ),
];

/// Every kind `xf.ai.redact` knows, which is also its default.
pub(crate) const REDACT_KINDS: [&str; 5] = ["email", "phone", "credit_card", "ssn", "ip"];

/// Replace personal data found by its shape, one kind at a time.
///
/// Each kind is one layer of `SELECT * REPLACE`, so a column is read by name at
/// every layer rather than the expression for the layer below being repeated
/// inside the one above. A card number must also pass the Luhn check, and a
/// hash is over what identifies the value (digits for a number, lower case for
/// an email), so `4111 1111 1111 1111` and `4111111111111111` hash alike.
pub(crate) fn transform_ai_redact(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let columns = checked_columns(node, "columns")?;
    let hashed = resolved_str(node, "replacement")? == "hash";

    let mut wanted = Vec::new();
    for kind in required_array(node, "kinds")? {
        let Some(kind) = kind.as_str().filter(|kind| REDACT_KINDS.contains(kind)) else {
            return Err(EngineError::InvalidProperty {
                id: node.node_id.to_string(),
                property: "kinds".to_string(),
                reason: format!("{kind} is not one of {}", REDACT_KINDS.join(", ")),
            });
        };
        wanted.push(kind);
    }
    if wanted.is_empty() {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: "kinds".to_string(),
            reason: "must list at least one kind".to_string(),
        });
    }

    let mut relation = quote_identifier(&upstream);
    for (kind, pattern, token) in REDACT_PATTERNS
        .iter()
        .filter(|(kind, _, _)| wanted.contains(kind))
    {
        let replaced: Vec<String> = columns
            .iter()
            .map(|column| {
                format!(
                    "{} AS {column}",
                    redact_expression(
                        &format!("CAST({column} AS VARCHAR)"),
                        kind,
                        pattern,
                        token,
                        hashed
                    )
                )
            })
            .collect();
        relation = format!(
            "(SELECT * REPLACE ({}) FROM {relation})",
            replaced.join(", ")
        );
    }

    Ok(create_view(node, &format!("SELECT * FROM {relation}")))
}

/// What a match of this kind must also be, over `__etl_m`, when its pattern
/// alone would take too much: a card number passes the Luhn check; a phone
/// number has 10 to 15 digits and a separator, and is not a date.
fn accepts(kind: &str) -> Option<String> {
    let digits = "regexp_replace(__etl_m, '[^0-9]', '', 'g')";
    match kind {
        "credit_card" => {
            let digit = format!("CAST(substring(reverse({digits}), __etl_i, 1) AS INTEGER)");
            Some(format!(
                "list_sum(list_transform(range(1, length({digits}) + 1), lambda __etl_i: \
                 CASE WHEN __etl_i % 2 = 0 THEN {digit} * 2 - CASE WHEN {digit} * 2 > 9 THEN 9 ELSE 0 END \
                 ELSE {digit} END)) % 10 = 0"
            ))
        }
        "phone" => Some(format!(
            "length({digits}) BETWEEN 10 AND 15 AND regexp_matches(__etl_m, '[ .()+-]') \
             AND NOT regexp_matches(__etl_m, '^(?:19|20)[0-9]{{2}}-[0-9]{{2}}-[0-9]{{2}}')"
        )),
        _ => None,
    }
}

/// One kind's replacement over one value.
fn redact_expression(value: &str, kind: &str, pattern: &str, token: &str, hashed: bool) -> String {
    let pattern = quote_literal(pattern);
    let judged = accepts(kind);
    if judged.is_none() && !hashed {
        return format!("regexp_replace({value}, {pattern}, '[{token}]', 'g')");
    }

    // Each match found, kept if it really is one, then replaced one at a
    // time, starting from the value itself.
    let digits = "regexp_replace(__etl_m, '[^0-9]', '', 'g')";
    let mut matches = format!("regexp_extract_all({value}, {pattern})");
    if let Some(test) = judged {
        matches = format!("list_filter({matches}, lambda __etl_m: {test})");
    }
    let replacement = if hashed {
        let identity = match kind {
            "email" | "ip" => "lower(__etl_m)".to_string(),
            _ => digits.to_string(),
        };
        format!("'[{token}:' || left(sha256({identity}), 12) || ']'")
    } else {
        format!("'[{token}]'")
    };
    format!(
        "list_reduce(list_prepend({value}, {matches}), lambda __etl_acc, __etl_m: replace(__etl_acc, __etl_m, {replacement}))"
    )
}

// ---------------------------------------------------------------------------
// Transforms: combining two inputs
// ---------------------------------------------------------------------------

pub(crate) fn transform_union(node: &Lowering<'_>) -> Result<String, EngineError> {
    let (left, right) = exactly_two_inputs(node)?;

    let mut operator = String::from("UNION");
    if resolved_bool(node, "all")? {
        operator.push_str(" ALL");
    }
    // BY NAME matches columns by name rather than by position, which is what is
    // wanted when two sources agree on names but not on column order.
    if resolved_bool(node, "by_name")? {
        operator.push_str(" BY NAME");
    }

    Ok(create_view(node, &two_sided(&left, &operator, &right)))
}

pub(crate) fn transform_intersect(node: &Lowering<'_>) -> Result<String, EngineError> {
    set_operation(node, "INTERSECT")
}

pub(crate) fn transform_except(node: &Lowering<'_>) -> Result<String, EngineError> {
    set_operation(node, "EXCEPT")
}

fn set_operation(node: &Lowering<'_>, operator: &str) -> Result<String, EngineError> {
    let (left, right) = exactly_two_inputs(node)?;

    let mut operator = operator.to_string();
    if resolved_bool(node, "all")? {
        operator.push_str(" ALL");
    }

    Ok(create_view(node, &two_sided(&left, &operator, &right)))
}

fn two_sided(left: &str, operator: &str, right: &str) -> String {
    format!(
        "SELECT * FROM {} {} SELECT * FROM {}",
        quote_identifier(left),
        operator,
        quote_identifier(right)
    )
}

// ---------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------

pub(crate) fn sink_parquet(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let path = required_str(node, "path")?;
    let options = format!(
        "FORMAT parquet, COMPRESSION {}",
        quote_literal(resolved_str(node, "compression")?)
    );

    Ok(copy_to(&upstream, path, &options))
}

pub(crate) fn sink_csv(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let path = required_str(node, "path")?;

    let mut options = vec!["FORMAT csv".to_string()];
    options.push(format!("HEADER {}", resolved_bool(node, "header")?));

    if let Some(delimiter) = optional_str(node, "delimiter")? {
        options.push(format!("DELIMITER {}", quote_literal(delimiter)));
    }

    Ok(copy_to(&upstream, path, &options.join(", ")))
}

pub(crate) fn sink_json(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let path = required_str(node, "path")?;

    Ok(copy_to(&upstream, path, "FORMAT json, ARRAY true"))
}

pub(crate) fn sink_jsonl(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let path = required_str(node, "path")?;

    // The same writer as `snk.file.json` without ARRAY: one JSON value per
    // line, which is what streams and log pipelines expect.
    Ok(copy_to(&upstream, path, "FORMAT json"))
}

pub(crate) fn sink_excel(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let path = required_str(node, "path")?;

    // A header by default, like the CSV sink. Without it the column names are
    // simply gone, and `src.file.excel` reading with header=true then eats the
    // first row of real data.
    let mut options = format!("FORMAT xlsx, HEADER {}", resolved_bool(node, "header")?);
    if let Some(sheet) = optional_str(node, "sheet")? {
        options.push_str(&format!(", SHEET {}", quote_literal(sheet)));
    }

    Ok(copy_to(&upstream, path, &options))
}

pub(crate) fn sink_s3(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let path = required_str(node, "path")?;

    let options = match resolved_str(node, "format")? {
        "parquet" => format!(
            "FORMAT parquet, COMPRESSION {}",
            quote_literal(resolved_str(node, "compression")?)
        ),
        "csv" => format!("FORMAT csv, HEADER {}", resolved_bool(node, "header")?),
        "json" => "FORMAT json".to_string(),
        other => {
            return Err(EngineError::InvalidProperty {
                id: node.node_id.to_string(),
                property: "format".to_string(),
                reason: format!("'{other}' is not a writable format; use parquet, csv, or json"),
            })
        }
    };

    with_s3_secret(node, copy_to(&upstream, path, &options))
}

// ---------------------------------------------------------------------------
// Native components: the SQL half
// ---------------------------------------------------------------------------

/// A source written in Rust: a view over the file its connector stages.
///
/// The connector runs before DuckDB and writes JSON Lines to `staging`; this is
/// the view that reads them. Everything a view gets -- the incremental filter,
/// materialisation, an alias, a count probe -- it gets through `create_view`
/// like any other source.
///
/// Without `columns`, DuckDB infers the types the way it does for
/// `src.file.json` and `src.file.csv`: an ISO date or timestamp becomes a
/// `DATE` or `TIMESTAMP`, and everything else a connector wrote as a string
/// stays text. The whole file is sampled, so a field that first appears on row
/// 50,000 is not dropped. With `columns`, DuckDB reads exactly those, cast --
/// which is the way to be certain, and the way to keep a timestamp's text
/// exactly as written (inference turns `T` into a space).
pub(crate) fn native_source(node: &Lowering<'_>) -> Result<String, EngineError> {
    let staging = staging_path(node)?;

    let shape = match node.properties.get("columns") {
        Some(JsonValue::Object(pairs)) if !pairs.is_empty() => {
            let columns = map_entries(node, "columns", pairs, |column, declared| {
                Ok(format!(
                    "{}: {}",
                    quote_literal(column),
                    quote_literal(&type_name(node, declared)?)
                ))
            })?;
            format!("columns={{{}}}", columns.join(", "))
        }
        _ => "sample_size=-1".to_string(),
    };

    let body = format!(
        "SELECT * FROM read_json({}, format='newline_delimited', {shape})",
        quote_path(staging)
    );

    Ok(create_view(node, &body))
}

/// A sink written in Rust: DuckDB writes the rows to the staging file as JSON
/// Lines, and the connector delivers them after the run succeeds.
pub(crate) fn native_sink(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let staging = staging_path(node)?;

    Ok(copy_to(&upstream, staging, "FORMAT json"))
}

/// A transform written in Rust: its view joins what the connector wrote back
/// onto the numbered input rows, so every column it did not touch keeps its
/// DuckDB type, and a row it wrote nothing for has nulls in its columns.
pub(crate) fn native_transform(node: &Lowering<'_>) -> Result<String, EngineError> {
    let staging = staging_path(node)?;
    let transform = registered_transform(node)?;
    let added = transform.adds(node.properties);

    let mut columns = vec![format!("{}: 'BIGINT'", quote_literal(ROW_KEY))];
    columns.extend(
        added.iter().map(|(name, sql_type)| {
            format!("{}: {}", quote_literal(name), quote_literal(sql_type))
        }),
    );
    let selected: Vec<String> = added
        .iter()
        .map(|(name, _)| format!("o.{}", quote_identifier(name)))
        .collect();

    let body = format!(
        "SELECT i.* EXCLUDE ({ROW_KEY}), {} FROM {} i LEFT JOIN read_json({}, format='newline_delimited', columns={{{}}}) o USING ({ROW_KEY}) ORDER BY i.{ROW_KEY}",
        selected.join(", "),
        quote_identifier(&native_input_table(node.node_id)),
        quote_path(staging),
        columns.join(", ")
    );

    Ok(create_view(node, &body))
}

/// What runs before a transform: the input rows numbered into a table, and
/// the columns the transform reads written to `file` for it.
pub(crate) fn native_feed(node: &Lowering<'_>, file: &str) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let transform = registered_transform(node)?;
    let table = quote_identifier(&native_input_table(node.node_id));

    let mut read = vec![ROW_KEY.to_string()];
    read.extend(
        transform
            .reads(node.properties)
            .iter()
            .map(|column| quote_identifier(column)),
    );

    Ok(format!(
        "CREATE OR REPLACE TEMP TABLE {table} AS SELECT row_number() OVER () AS {ROW_KEY}, * FROM {};\n\
         COPY (SELECT {} FROM {table}) TO {} (FORMAT json);",
        quote_identifier(&upstream),
        read.join(", "),
        quote_path(file)
    ))
}

/// The numbered input a transform's view reads from.
fn native_input_table(node_id: &str) -> String {
    format!("{node_id}__native_in")
}

fn registered_transform(
    node: &Lowering<'_>,
) -> Result<&'static dyn etl_plugin_sdk::Transform, EngineError> {
    match etl_connectors::find(node.component_id) {
        Some(etl_plugin_sdk::Connector::Transform(transform)) => Ok(transform),
        _ => Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: "componentId".to_string(),
            reason: format!(
                "'{}' is registered as a native transform but has no connector",
                node.component_id
            ),
        }),
    }
}

fn staging_path<'a>(node: &Lowering<'a>) -> Result<&'a str, EngineError> {
    // The planner sets this for every component the connector registry knows,
    // and the registry is where these builders are paired with them -- so this
    // is a registry that disagrees with itself, not a user mistake.
    node.staging.ok_or_else(|| EngineError::InvalidProperty {
        id: node.node_id.to_string(),
        property: "componentId".to_string(),
        reason: format!(
            "'{}' is registered as native but has no connector",
            node.component_id
        ),
    })
}

// ---------------------------------------------------------------------------
// Statement shapes
// ---------------------------------------------------------------------------

/// Realise a query body as this node's relation, plus the alias view when the
/// node declares one.
///
/// Every builder that produces a relation ends here, which is what keeps the
/// materialisation modes in one place instead of in twenty-five builders.
fn create_view(node: &Lowering<'_>, body: &str) -> String {
    let name = quote_identifier(node.node_id);
    let body = &apply_watermark(node, body);

    let mut statement = match (node.materialize, node.spill_path) {
        // A temp table: the work happens once, here, rather than on each read.
        (Materialize::Memory, _) => {
            format!("CREATE OR REPLACE TEMP TABLE {name} AS ({body});")
        }

        // Spill to Parquet and read it back. Two statements, because there is
        // no single one that both writes the file and defines the relation.
        (Materialize::Disk, Some(spill)) => format!(
            "COPY ({body}) TO {} (FORMAT parquet);\n\
             CREATE OR REPLACE TEMP VIEW {name} AS (SELECT * FROM read_parquet({}));",
            quote_path(spill),
            quote_path(spill)
        ),

        // `auto` is a view until there is something to base a better choice on.
        _ => format!("CREATE OR REPLACE TEMP VIEW {name} AS ({body});"),
    };

    // The alias is an additional view, not a rename: edge wiring and every
    // generated reference still use the node id.
    if let Some(alias) = node.alias {
        statement.push_str(&format!(
            "\nCREATE OR REPLACE TEMP VIEW {} AS SELECT * FROM {};",
            quote_identifier(alias),
            name
        ));
    }

    statement
}

/// The query that reads this run's new high-water mark.
///
/// `max` over the node's own relation, which is already narrowed to this run's
/// rows — so the answer is the highest value *loaded*, not the highest in the
/// source. Those are the same number when the run read everything after the
/// mark, and the former is the correct one to remember when it did not.
///
/// A relation that loaded nothing gives `null`, which the executor reads as
/// "leave the watermark where it was" rather than as a value to store.
pub(crate) fn watermark_probe(node_id: &str, column: &str) -> String {
    format!(
        "SELECT max({}) AS w FROM {};",
        quote_identifier(column),
        quote_identifier(node_id)
    )
}

/// Narrow a source's body to the rows after its watermark.
///
/// Wrapped around the body rather than pushed into each reader, because the
/// twelve sources build their bodies twelve different ways and only some of
/// them have somewhere to put a predicate. DuckDB pushes the filter back down
/// into the scan for the formats that support it, so wrapping costs nothing
/// where it matters and works everywhere.
///
/// Strictly greater than, never `>=`: the watermark is a value already loaded,
/// and re-reading it would duplicate every row sharing that timestamp. The cost
/// is the mirror-image risk — a row written *later* with a timestamp at or
/// below the mark is never seen — which is inherent to watermarking and is why
/// [`Incremental::column`] has to be a column that only goes up.
///
/// [`Incremental::column`]: etl_metadata::Incremental::column
fn apply_watermark(node: &Lowering<'_>, body: &str) -> String {
    let Some(filter) = node.incremental else {
        return body.to_string();
    };

    let Some(since) = filter.since else {
        // Declared incremental, but nothing remembered and no start given: the
        // first run loads everything, which is what makes the second one
        // meaningful.
        return body.to_string();
    };

    format!(
        "SELECT * FROM ({body}) WHERE {} > {}",
        quote_identifier(filter.column),
        watermark_literal(since)
    )
}

/// A watermark as a SQL literal.
///
/// Stored state is always text, because the store has no business deciding
/// whether a column is a timestamp or an id. Something that reads as a number
/// goes back unquoted so a numeric column compares against a number; everything
/// else is quoted, and DuckDB casts the string to the column's type — which is
/// the right authority for the job.
fn watermark_literal(value: &str) -> String {
    let numeric = !value.is_empty()
        && value.parse::<f64>().is_ok()
        // `parse::<f64>` accepts "inf" and "NaN", which are not what anyone's
        // watermark column holds and must not go through unquoted.
        && value
            .chars()
            .all(|c| c.is_ascii_digit() || c == '.' || c == '-' || c == '+');

    if numeric {
        value.to_string()
    } else {
        quote_literal(value)
    }
}

fn copy_to(upstream: &str, path: &str, options: &str) -> String {
    format!(
        "COPY (SELECT * FROM {}) TO {} ({});",
        quote_identifier(upstream),
        quote_path(path),
        options
    )
}

// ---------------------------------------------------------------------------
// Property access
// ---------------------------------------------------------------------------

fn required_str<'a>(node: &Lowering<'a>, key: &str) -> Result<&'a str, EngineError> {
    match node.properties.get(key) {
        Some(JsonValue::String(value)) if !value.trim().is_empty() => Ok(value),

        Some(JsonValue::String(_)) => Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: key.to_string(),
            reason: "must not be empty".to_string(),
        }),

        Some(_) => Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: key.to_string(),
            reason: "must be text".to_string(),
        }),

        None => Err(EngineError::MissingProperty {
            id: node.node_id.to_string(),
            component_id: node.component_id.to_string(),
            property: key.to_string(),
        }),
    }
}

/// Read a property the spec guarantees is present, because it is required or
/// carries a default. A missing value here means the spec and the builder
/// disagree, which is a bug rather than bad input.
fn resolved_str<'a>(node: &Lowering<'a>, key: &str) -> Result<&'a str, EngineError> {
    required_str(node, key)
}

fn resolved_bool(node: &Lowering<'_>, key: &str) -> Result<bool, EngineError> {
    optional_bool(node, key)?.ok_or_else(|| EngineError::MissingProperty {
        id: node.node_id.to_string(),
        component_id: node.component_id.to_string(),
        property: key.to_string(),
    })
}

fn optional_str<'a>(node: &Lowering<'a>, key: &str) -> Result<Option<&'a str>, EngineError> {
    match node.properties.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) => Ok(Some(value.as_str())),
        Some(_) => Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: key.to_string(),
            reason: "must be text".to_string(),
        }),
    }
}

fn optional_bool(node: &Lowering<'_>, key: &str) -> Result<Option<bool>, EngineError> {
    match node.properties.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: key.to_string(),
            reason: "must be true or false".to_string(),
        }),
    }
}

fn required_array<'a>(node: &Lowering<'a>, key: &str) -> Result<&'a Vec<JsonValue>, EngineError> {
    optional_array(node, key)?.ok_or_else(|| EngineError::MissingProperty {
        id: node.node_id.to_string(),
        component_id: node.component_id.to_string(),
        property: key.to_string(),
    })
}

fn optional_array<'a>(
    node: &Lowering<'a>,
    key: &str,
) -> Result<Option<&'a Vec<JsonValue>>, EngineError> {
    match node.properties.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Array(values)) => Ok(Some(values)),
        Some(_) => Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: key.to_string(),
            reason: "must be a list".to_string(),
        }),
    }
}

/// Read an integer the spec guarantees is present, and refuse a negative one.
///
/// `LIMIT -1` is not a smaller limit, it is a syntax error at run time; caught
/// here it names the property instead.
fn non_negative(node: &Lowering<'_>, key: &str) -> Result<i64, EngineError> {
    let value = match node.properties.get(key) {
        Some(JsonValue::Number(number)) => number.as_i64(),

        Some(JsonValue::Null) | None => {
            return Err(EngineError::MissingProperty {
                id: node.node_id.to_string(),
                component_id: node.component_id.to_string(),
                property: key.to_string(),
            })
        }

        Some(_) => None,
    };

    match value {
        Some(value) if value >= 0 => Ok(value),

        Some(_) => Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: key.to_string(),
            reason: "must not be negative".to_string(),
        }),

        None => Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: key.to_string(),
            reason: "must be a whole number".to_string(),
        }),
    }
}

fn non_empty_map<'a>(
    node: &Lowering<'a>,
    key: &str,
) -> Result<&'a JsonMap<String, JsonValue>, EngineError> {
    let pairs = match node.properties.get(key) {
        Some(JsonValue::Object(pairs)) => pairs,

        Some(JsonValue::Null) | None => {
            return Err(EngineError::MissingProperty {
                id: node.node_id.to_string(),
                component_id: node.component_id.to_string(),
                property: key.to_string(),
            })
        }

        Some(_) => {
            return Err(EngineError::InvalidProperty {
                id: node.node_id.to_string(),
                property: key.to_string(),
                reason: "must be a set of name/value pairs".to_string(),
            })
        }
    };

    if pairs.is_empty() {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: key.to_string(),
            reason: "must name at least one column".to_string(),
        });
    }

    Ok(pairs)
}

/// Render each name/value pair in the order it was entered.
///
/// The order matters: it decides the order of the generated SQL, and a plan
/// that reorders between runs is not reviewable. `serde_json`'s `preserve_order`
/// feature is what makes the entry order survive parsing.
fn map_entries<F>(
    node: &Lowering<'_>,
    key: &str,
    pairs: &JsonMap<String, JsonValue>,
    mut render: F,
) -> Result<Vec<String>, EngineError>
where
    F: FnMut(&str, &str) -> Result<String, EngineError>,
{
    pairs
        .iter()
        .map(|(name, value)| {
            let value = value.as_str().ok_or_else(|| EngineError::InvalidProperty {
                id: node.node_id.to_string(),
                property: key.to_string(),
                reason: format!("the value for '{name}' must be text"),
            })?;

            render(name, value)
        })
        .collect()
}

/// Quote a list of column names, rejecting an empty list or a non-name entry.
fn column_list(
    node: &Lowering<'_>,
    key: &str,
    values: &[JsonValue],
) -> Result<Vec<String>, EngineError> {
    if values.is_empty() {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: key.to_string(),
            reason: "must name at least one column".to_string(),
        });
    }

    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(quote_identifier)
                .ok_or_else(|| EngineError::InvalidProperty {
                    id: node.node_id.to_string(),
                    property: key.to_string(),
                    reason: "every entry must be a column name".to_string(),
                })
        })
        .collect()
}

/// The same, for a property that may be absent. An absent list and an empty one
/// mean the same thing to every caller here, so both give an empty vector.
fn optional_column_list(node: &Lowering<'_>, key: &str) -> Result<Vec<String>, EngineError> {
    match optional_array(node, key)? {
        None => Ok(Vec::new()),
        Some(values) if values.is_empty() => Ok(Vec::new()),
        Some(values) => column_list(node, key, values),
    }
}

/// A SQL type name, checked rather than quoted.
///
/// A type cannot be quoted as an identifier — `DECIMAL(10,2)` and `VARCHAR[]`
/// would both stop being types — so this is the one place a string from the
/// document reaches a statement unquoted. It is restricted to the characters a
/// type name can be spelled with, which leaves no way to close the expression
/// and start something else.
fn type_name(node: &Lowering<'_>, declared: &str) -> Result<String, EngineError> {
    let trimmed = declared.trim();
    let permitted =
        |character: char| character.is_ascii_alphanumeric() || " _,()[]".contains(character);

    if trimmed.is_empty() || !trimmed.chars().all(permitted) {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: "columns".to_string(),
            reason: format!("'{declared}' is not a SQL type name"),
        });
    }

    Ok(trimmed.to_string())
}

// ---------------------------------------------------------------------------
// Input arity
// ---------------------------------------------------------------------------

fn exactly_one_input(node: &Lowering<'_>) -> Result<String, EngineError> {
    match node.inputs {
        [only] => Ok(only.relation()),
        inputs => Err(EngineError::WrongInputCount {
            id: node.node_id.to_string(),
            component_id: node.component_id.to_string(),
            expected: 1,
            actual: inputs.len(),
        }),
    }
}

/// Two inputs, ordered.
///
/// A handle named `left` or `right` wins over edge order, so re-wiring the
/// canvas cannot silently swap the sides of an outer join. Without handles,
/// document order decides.
fn exactly_two_inputs(node: &Lowering<'_>) -> Result<(String, String), EngineError> {
    let [first, second] = node.inputs else {
        return Err(EngineError::WrongInputCount {
            id: node.node_id.to_string(),
            component_id: node.component_id.to_string(),
            expected: 2,
            actual: node.inputs.len(),
        });
    };

    let side = |input: &Input| input.target_handle.clone().unwrap_or_default();

    if side(second) == "left" || side(first) == "right" {
        Ok((second.relation(), first.relation()))
    } else {
        Ok((first.relation(), second.relation()))
    }
}

/// The count probes that follow a stage, so the run can report how many rows
/// it produced.
///
/// A sink writes exactly the rows its input yields, so counting the upstream
/// relation and counting the file would give the same answer — and `COPY`
/// reports nothing we could read instead.
pub(crate) fn count_probes(
    node_id: &str,
    kind: StageKind,
    splits: bool,
    from: Option<&str>,
) -> Vec<CountProbe> {
    let relation = if kind.produces_relation() {
        node_id.to_string()
    } else {
        match from {
            Some(upstream) => upstream.to_string(),
            None => return Vec::new(),
        }
    };

    let probe = |relation: &str, port: Option<&str>| CountProbe {
        sql: format!("SELECT count(*) AS n FROM {};", quote_identifier(relation)),
        port: port.map(str::to_string),
    };

    let mut probes = vec![probe(&relation, splits.then_some(MAIN_PORT))];

    // A quality node reports both sides. The order is the contract with the
    // executor, which reads counts off stdout positionally: accepted first,
    // because that is the order the two views are created in.
    if splits {
        probes.push(probe(&reject_relation(node_id), Some(REJECTED_PORT)));
    }

    probes
}

// ---------------------------------------------------------------------------
// Quality
// ---------------------------------------------------------------------------

/// Lower a validator into the two relations it produces.
///
/// Every `qa.*` component reduces to the same three things: a `base` relation
/// to test, a boolean `predicate` over it, and any helper columns the base
/// added that must not reach the output.
///
/// **The split is exact, and that is the point.** Accepted is
/// `coalesce(<pred>, false)` and rejected is `NOT coalesce(<pred>, false)`,
/// both reading the same expression — so every input row lands on exactly one
/// side and none is lost. The `coalesce` is load-bearing rather than
/// defensive: SQL predicates are three-valued, and a NULL is an *unknown*, not
/// a pass. Without it, `WHERE pred` and `WHERE NOT pred` would both drop the
/// unknowns, and the two outputs would quietly fail to add up to the input.
fn quality_split(
    node: &Lowering<'_>,
    base: &str,
    predicate: &str,
    helper_columns: &[&str],
) -> String {
    let projection = if helper_columns.is_empty() {
        "*".to_string()
    } else {
        let excluded: Vec<String> = helper_columns.iter().map(|c| quote_identifier(c)).collect();
        format!("* EXCLUDE ({})", excluded.join(", "))
    };

    let accepted = format!("SELECT {projection} FROM ({base}) WHERE coalesce({predicate}, false)");
    let rejected =
        format!("SELECT {projection} FROM ({base}) WHERE NOT coalesce({predicate}, false)");

    // The accepted side goes through `create_view`, so a validator honours
    // `materialize` exactly as any other stage does. The rejected side is
    // always a plain view: it is normally small and normally terminal, so
    // spilling it would buy nothing and would need a second spill path.
    let mut statement = create_view(node, &accepted);

    statement.push_str(&format!(
        "\nCREATE OR REPLACE TEMP VIEW {} AS ({});",
        quote_identifier(&reject_relation(node.node_id)),
        rejected
    ));

    statement
}

/// The columns a validator checks: required, quoted, and at least one.
fn checked_columns(node: &Lowering<'_>, key: &str) -> Result<Vec<String>, EngineError> {
    let values = required_array(node, key)?;
    column_list(node, key, values)
}

/// A validator's list of literal values: required, quoted, and at least one.
///
/// The sibling of [`column_list`], and separate from it because the difference
/// matters: these become string literals, not identifiers. Quoting a value as
/// an identifier would turn `IN ('paid')` into `IN ("paid")` — a column
/// reference, which is a different query that usually still runs.
fn checked_values(node: &Lowering<'_>, key: &str) -> Result<Vec<String>, EngineError> {
    let values = required_array(node, key)?;

    if values.is_empty() {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: key.to_string(),
            reason: "must list at least one value".to_string(),
        });
    }

    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(quote_literal)
                .ok_or_else(|| EngineError::InvalidProperty {
                    id: node.node_id.to_string(),
                    property: key.to_string(),
                    reason: "every entry must be text".to_string(),
                })
        })
        .collect()
}

/// One end of a numeric range, if the node set it.
fn optional_number(node: &Lowering<'_>, key: &str) -> Result<Option<String>, EngineError> {
    match node.properties.get(key) {
        None | Some(JsonValue::Null) => Ok(None),

        // The number's own text rather than a parsed float: a bound written as
        // 9007199254740993 has to reach SQL as that, not as the nearest double.
        Some(JsonValue::Number(number)) => Ok(Some(number.to_string())),

        Some(_) => Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: key.to_string(),
            reason: "must be a number".to_string(),
        }),
    }
}

/// `SELECT * FROM <relation>` — the base every validator but `qa.unique` tests.
fn plain_base(upstream: &str) -> String {
    format!("SELECT * FROM {}", quote_identifier(upstream))
}

pub(crate) fn quality_not_null(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let columns = checked_columns(node, "columns")?;

    let predicate = columns
        .iter()
        .map(|column| format!("{column} IS NOT NULL"))
        .collect::<Vec<_>>()
        .join(" AND ");

    Ok(quality_split(node, &plain_base(&upstream), &predicate, &[]))
}

pub(crate) fn quality_unique(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let columns = checked_columns(node, "columns")?;
    let key = columns.join(", ");

    // A duplicate is a property of a row's neighbours rather than of the row,
    // so this is the one validator whose predicate needs a helper column.
    //
    // Every copy of a duplicated key is rejected, deliberately. Keeping one is
    // deduplication, which is what `xf.dedup` is for; a validator that quietly
    // kept a survivor would be doing something its name does not say.
    let helper = "__etl_occurrences";

    let base = format!(
        "SELECT *, count(*) OVER (PARTITION BY {key}) AS {} FROM {}",
        quote_identifier(helper),
        quote_identifier(&upstream)
    );

    let predicate = format!("{} = 1", quote_identifier(helper));
    Ok(quality_split(node, &base, &predicate, &[helper]))
}

pub(crate) fn quality_range(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let column = quote_identifier(required_str(node, "column")?);

    let mut bounds = Vec::new();
    if let Some(low) = optional_number(node, "min")? {
        bounds.push(format!("{column} >= {low}"));
    }
    if let Some(high) = optional_number(node, "max")? {
        bounds.push(format!("{column} <= {high}"));
    }

    if bounds.is_empty() {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: "min".to_string(),
            reason: "or max must be set; a range with neither bound checks nothing".to_string(),
        });
    }

    let predicate = bounds.join(" AND ");
    Ok(quality_split(node, &plain_base(&upstream), &predicate, &[]))
}

pub(crate) fn quality_regex(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let column = quote_identifier(required_str(node, "column")?);
    let pattern = quote_literal(required_str(node, "pattern")?);

    let predicate = format!("regexp_matches({column}, {pattern})");
    Ok(quality_split(node, &plain_base(&upstream), &predicate, &[]))
}

pub(crate) fn quality_accepted_values(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;
    let column = quote_identifier(required_str(node, "column")?);
    let listed = checked_values(node, "values")?.join(", ");
    let predicate = format!("{column} IN ({listed})");
    Ok(quality_split(node, &plain_base(&upstream), &predicate, &[]))
}

pub(crate) fn quality_expression(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = exactly_one_input(node)?;

    // Deliberately unescaped, for the same reason `xf.sql` is: the point of
    // this component is that the user writes SQL. Anyone who can edit the
    // document can already run arbitrary SQL through `xf.sql`, so quoting here
    // would buy nothing and break every legitimate use.
    let predicate = required_str(node, "predicate")?;
    Ok(quality_split(node, &plain_base(&upstream), predicate, &[]))
}

pub(crate) fn quality_referential(node: &Lowering<'_>) -> Result<String, EngineError> {
    let (left, right) = exactly_two_inputs(node)?;

    let column = quote_identifier(required_str(node, "column")?);
    let reference = quote_identifier(required_str(node, "reference_column")?);

    // `IN` rather than a join, so a row is tested without being duplicated by a
    // reference side holding the key more than once. Its three-valued result is
    // wanted here: a NULL key, or a reference set that contains NULLs and no
    // match, both yield NULL — an unconfirmed reference — which
    // `quality_split`'s coalesce sends to the reject side.
    let predicate = format!(
        "{column} IN (SELECT {reference} FROM {})",
        quote_identifier(&right)
    );

    Ok(quality_split(node, &plain_base(&left), &predicate, &[]))
}

// ---------------------------------------------------------------------------
// Control
//
// Every control component lowers to the same SQL: a view equal to its input.
// The thing that makes it a control node is the [`Control`] built alongside it,
// which the driven executor acts on before letting the rows past. Keeping the
// SQL trivial is deliberate — a control node must not change what the data is,
// only what happens around it.
// ---------------------------------------------------------------------------

/// The pass-through every control component shares.
pub(crate) fn control_passthrough(node: &Lowering<'_>) -> Result<String, EngineError> {
    let upstream = match node.inputs {
        [] => {
            return Err(EngineError::WrongInputCount {
                id: node.node_id.to_string(),
                component_id: node.component_id.to_string(),
                expected: 1,
                actual: 0,
            })
        }
        // A sequence node takes two and passes the first along; every other
        // control node takes one. Taking the first input in both cases is what
        // makes one builder enough.
        inputs => inputs[0].relation(),
    };

    Ok(create_view(node, &plain_base(&upstream)))
}

/// Work out what a control stage does, from its kind and its properties.
///
/// Called by the planner rather than by a builder, because most of what a
/// control node does is not SQL and a builder returns SQL. The parts that *are*
/// SQL are probes: queries the driver runs to decide something.
pub(crate) fn control_for(
    kind: ControlKind,
    node_id: &str,
    component_id: &str,
    properties: &JsonValue,
    inputs: &[Input],
) -> Result<Control, EngineError> {
    let node = Lowering {
        node_id,
        component_id,
        properties,
        inputs,
        alias: None,
        materialize: Materialize::Auto,
        spill_path: None,
        staging: None,
        // A control node reads properties through this, and builds no
        // relation of its own; there is nothing for a watermark to narrow.
        incremental: None,
    };

    let upstream = inputs
        .first()
        .map(Input::relation)
        .unwrap_or_else(|| node_id.to_string());

    let control = match kind {
        ControlKind::Wait => Control {
            kind,
            probe: None,
            message: optional_str(&node, "message")?.map(str::to_string),
            wait_ms: Some(non_negative(&node, "ms")? as u64),
        },

        ControlKind::Log => Control {
            kind,
            probe: None,
            message: optional_str(&node, "message")?.map(str::to_string),
            wait_ms: None,
        },

        // No `when` means fail whenever the run reaches this node. With one, it
        // fails only if some row matches — a dead-letter check that stops the
        // run rather than writing a file.
        ControlKind::Fail => Control {
            kind,
            probe: optional_str(&node, "when")?.map(|predicate| {
                format!(
                    "SELECT count(*) AS n FROM {} WHERE coalesce({predicate}, false);",
                    quote_identifier(&upstream)
                )
            }),
            message: Some(required_str(&node, "message")?.to_string()),
            wait_ms: None,
        },

        // Taken when at least one row matches. Expressed as a count rather than
        // a scalar so the predicate is written about a row, which is the same
        // thing every other component's predicate is written about.
        ControlKind::Branch => Control {
            kind,
            probe: Some(format!(
                "SELECT count(*) AS n FROM {} WHERE coalesce({}, false);",
                quote_identifier(&upstream),
                required_str(&node, "predicate")?
            )),
            message: optional_str(&node, "message")?.map(str::to_string),
            wait_ms: None,
        },

        // The probe reads the *other* input. Nothing is done with the answer —
        // the point is that reading it forced that branch to run first.
        ControlKind::Sequence => {
            let (_, after) = exactly_two_inputs(&node)?;

            Control {
                kind,
                probe: Some(format!(
                    "SELECT count(*) AS n FROM {};",
                    quote_identifier(&after)
                )),
                message: None,
                wait_ms: None,
            }
        }

        ControlKind::Assert => Control {
            kind,
            probe: Some(assertion_probe(&node, &upstream)?),
            message: Some(assertion_message(&node)?),
            wait_ms: None,
        },
    };

    Ok(control)
}

/// The `ok` query behind `qa.row_count` and `qa.schema_match`.
///
/// Both answer one boolean about a whole relation rather than about a row,
/// which is why neither has a reject port: there is nothing to reject, only a
/// run to stop.
fn assertion_probe(node: &Lowering<'_>, upstream: &str) -> Result<String, EngineError> {
    let relation = quote_identifier(upstream);

    // `qa.schema_match` names columns; `qa.row_count` sets bounds. The property
    // present is what decides, so neither needs to know the other exists.
    if let Some(values) = optional_array(node, "columns")? {
        let columns = column_list(node, "columns", values)?;

        // Selecting the columns is the check: DuckDB raises if one is missing,
        // and a raised probe is already a failed stage. The wrapper exists only
        // so a passing check still returns a row to read.
        return Ok(format!(
            "SELECT count(*) >= 0 AS ok FROM (SELECT {} FROM {relation} LIMIT 0);",
            columns.join(", ")
        ));
    }

    let minimum = optional_number(node, "min")?;
    let maximum = optional_number(node, "max")?;

    let mut bounds = Vec::new();
    if let Some(low) = &minimum {
        bounds.push(format!("count(*) >= {low}"));
    }
    if let Some(high) = &maximum {
        bounds.push(format!("count(*) <= {high}"));
    }

    if bounds.is_empty() {
        return Err(EngineError::InvalidProperty {
            id: node.node_id.to_string(),
            property: "min".to_string(),
            reason: "or max must be set; a row count with neither bound checks nothing".to_string(),
        });
    }

    Ok(format!(
        "SELECT ({}) AS ok FROM {relation};",
        bounds.join(" AND ")
    ))
}

/// What an assertion says when it does not hold.
fn assertion_message(node: &Lowering<'_>) -> Result<String, EngineError> {
    if let Some(message) = optional_str(node, "message")? {
        return Ok(message.to_string());
    }

    if let Some(values) = optional_array(node, "columns")? {
        let columns = column_list(node, "columns", values)?;
        return Ok(format!("expected the columns {}", columns.join(", ")));
    }

    let minimum = optional_number(node, "min")?;
    let maximum = optional_number(node, "max")?;

    Ok(match (minimum, maximum) {
        (Some(low), Some(high)) => format!("expected between {low} and {high} rows"),
        (Some(low), None) => format!("expected at least {low} rows"),
        (None, Some(high)) => format!("expected at most {high} rows"),
        (None, None) => "the row count is outside its bounds".to_string(),
    })
}
