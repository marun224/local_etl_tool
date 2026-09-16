//! The component registry.
//!
//! One table maps a component id to its [`ComponentSpec`] and the function that
//! lowers it. Everything else — property validation, default values, input
//! arity, the JSON manifest the canvas builds its palette and property panels
//! from — is derived from that table, so a component is described in exactly
//! one place.
//!
//! **Adding a component** is therefore three things and no more: a spec, a
//! builder function, and a test. There is deliberately no `match` on component
//! id anywhere in the engine — the builder travels with the spec as a function
//! pointer, so a new component cannot be half-registered.
//!
//! See `docs/adding_a_component.md`.

use super::builders::{self, Lowering};
use crate::EngineError;
use etl_metadata::{ComponentSpec, PortSpec, PropertySpec};
use serde_json::{Map, Value as JsonValue};
use std::collections::BTreeMap;
use std::sync::OnceLock;

/// The signature every builder has.
pub(crate) type BuildFn = fn(&Lowering<'_>) -> Result<String, EngineError>;

/// A component: what it is, and how to lower it.
#[derive(Debug)]
pub struct Component {
    pub spec: ComponentSpec,
    pub(crate) build: BuildFn,
}

/// Every component this engine knows, keyed by id.
pub struct Registry {
    components: BTreeMap<String, Component>,
}

impl Registry {
    pub fn get(&self, component_id: &str) -> Option<&Component> {
        self.components.get(component_id)
    }

    pub fn specs(&self) -> impl Iterator<Item = &ComponentSpec> {
        self.components.values().map(|component| &component.spec)
    }

    pub fn len(&self) -> usize {
        self.components.len()
    }

    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }

    /// The registry as JSON — what the canvas loads to build its palette and
    /// generate property panels, and what an agent reads over MCP.
    pub fn manifest(&self) -> JsonValue {
        serde_json::json!({
            "formatVersion": 1,
            "components": self.specs().collect::<Vec<_>>(),
        })
    }
}

/// The registry. Built once, on first use.
pub fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();

    REGISTRY.get_or_init(|| {
        let mut components = BTreeMap::new();

        for (spec, build) in all_components() {
            let previous = components.insert(
                spec.id.clone(),
                Component {
                    spec: spec.clone(),
                    build,
                },
            );

            debug_assert!(
                previous.is_none(),
                "component '{}' is registered twice",
                spec.id
            );
        }

        Registry { components }
    })
}

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

/// Every component, paired with the function that lowers it.
fn all_components() -> Vec<(ComponentSpec, BuildFn)> {
    vec![
        // -- Sources ------------------------------------------------------
        (
            ComponentSpec::new("src.file.csv", "CSV file")
                .description("Read a delimited text file.")
                .icon("file-text")
                .properties(vec![
                    PropertySpec::path("path")
                        .required()
                        .help("File to read. Globs such as data/*.csv are allowed."),
                    PropertySpec::boolean("header")
                        .default(JsonValue::Bool(true))
                        .help("Treat the first row as column names."),
                    PropertySpec::text("delimiter")
                        .help("Left unset, the delimiter is detected from the file."),
                ]),
            builders::source_csv,
        ),
        (
            ComponentSpec::new("src.file.parquet", "Parquet file")
                .description("Read a Parquet file.")
                .icon("file-box")
                .properties(vec![PropertySpec::path("path")
                    .required()
                    .help("File to read. Globs are allowed.")]),
            builders::source_parquet,
        ),
        (
            ComponentSpec::new("src.file.jsonl", "JSON Lines file")
                .description("Read a newline-delimited JSON file.")
                .icon("file-json")
                .properties(vec![
                    PropertySpec::path("path")
                        .required()
                        .help("File to read. Globs are allowed."),
                    PropertySpec::boolean("ignore_errors")
                        .default(JsonValue::Bool(false))
                        .help("Skip lines that do not parse."),
                ]),
            builders::source_jsonl,
        ),
        // -- Transforms ---------------------------------------------------
        (
            ComponentSpec::new("xf.filter", "Filter")
                .description("Keep only the rows matching a condition.")
                .icon("filter")
                .properties(vec![PropertySpec::sql("predicate")
                    .required()
                    .help("A SQL condition, e.g. amount > 100.")]),
            builders::transform_filter,
        ),
        (
            ComponentSpec::new("xf.select", "Select columns")
                .description("Keep a subset of columns, in the order given.")
                .icon("columns-3")
                .properties(vec![PropertySpec::string_list("columns")
                    .required()
                    .help("Column names to keep.")]),
            builders::transform_select,
        ),
        (
            ComponentSpec::new("xf.join", "Join")
                .description("Combine two inputs on shared keys or a condition.")
                .icon("git-merge")
                .inputs(vec![
                    PortSpec::new("left").help("The left side of the join."),
                    PortSpec::new("right").help("The right side of the join."),
                ])
                .properties(vec![
                    PropertySpec::enumerated(
                        "type",
                        &["inner", "left", "right", "full", "outer", "cross"],
                    )
                    .default(JsonValue::String("inner".into()))
                    .label("Join type"),
                    PropertySpec::string_list("keys")
                        .help("Columns present in both inputs. Use this or a condition."),
                    PropertySpec::sql("condition")
                        .help("A join condition, when the key names differ."),
                ]),
            builders::transform_join,
        ),
        (
            ComponentSpec::new("xf.sql", "SQL")
                .description("Write the query yourself. Upstream nodes are in scope by id.")
                .icon("code")
                .properties(vec![PropertySpec::sql("query")
                    .required()
                    .help("A SELECT statement.")]),
            builders::transform_sql,
        ),
        (
            ComponentSpec::new("src.file.json", "JSON file")
                .description("Read a JSON file holding an array of records.")
                .icon("file-json")
                .properties(vec![
                    PropertySpec::path("path")
                        .required()
                        .help("File to read. Globs are allowed."),
                    PropertySpec::boolean("ignore_errors")
                        .default(JsonValue::Bool(false))
                        .help("Skip records that do not parse."),
                ]),
            builders::source_json,
        ),
        (
            ComponentSpec::new("src.file.excel", "Excel file")
                .description("Read a sheet from an .xlsx workbook.")
                .icon("sheet")
                .requires_extension("excel")
                .properties(vec![
                    PropertySpec::path("path")
                        .required()
                        .help("Workbook to read."),
                    PropertySpec::text("sheet").help("Which sheet. Unset, the first one is read."),
                    PropertySpec::boolean("header")
                        .default(JsonValue::Bool(true))
                        .help("Treat the first row as column names."),
                ]),
            builders::source_excel,
        ),
        // -- Sources: object storage ---------------------------------------
        (
            ComponentSpec::new("src.cloud.s3", "S3 object")
                .description("Read a file from S3 or another S3-compatible store.")
                .icon("cloud-download")
                .requires_extension("httpfs")
                .properties(vec![
                    PropertySpec::path("path")
                        .required()
                        .help("An s3:// URI. Globs are allowed."),
                    cloud_format(),
                    PropertySpec::boolean("header")
                        .default(JsonValue::Bool(true))
                        .help("For CSV: treat the first row as column names."),
                ]),
            builders::source_s3,
        ),
        (
            ComponentSpec::new("src.cloud.http", "HTTP file")
                .description("Read a file over HTTP or HTTPS.")
                .icon("globe")
                .requires_extension("httpfs")
                .properties(vec![
                    PropertySpec::path("path")
                        .required()
                        .help("An http:// or https:// URL."),
                    cloud_format(),
                    PropertySpec::boolean("header")
                        .default(JsonValue::Bool(true))
                        .help("For CSV: treat the first row as column names."),
                ]),
            builders::source_http,
        ),
        // -- Sources: lakehouse table formats ------------------------------
        (
            ComponentSpec::new("src.lake.iceberg", "Iceberg table")
                .description("Read an Apache Iceberg table.")
                .icon("mountain-snow")
                .requires_extension("iceberg")
                // Iceberg tables usually live on object storage, and a run that
                // failed at the scan with "httpfs not loaded" would be exactly
                // the late failure this mechanism exists to prevent.
                .requires_extension("httpfs")
                .properties(vec![
                    PropertySpec::path("path")
                        .required()
                        .help("The table's metadata location."),
                    PropertySpec::boolean("allow_moved_paths")
                        .default(JsonValue::Bool(false))
                        .help(
                            "Resolve data files relative to the table, for a table that \
                               has been copied elsewhere.",
                        ),
                ]),
            builders::source_iceberg,
        ),
        (
            ComponentSpec::new("src.lake.delta", "Delta Lake table")
                .description("Read a Delta Lake table.")
                .icon("triangle")
                .requires_extension("delta")
                .requires_extension("httpfs")
                .properties(vec![PropertySpec::path("path")
                    .required()
                    .help("The table's root directory.")]),
            builders::source_delta,
        ),
        // -- Sources: databases --------------------------------------------
        (
            ComponentSpec::new("src.db.postgres", "PostgreSQL table")
                .description("Read a table from a PostgreSQL database.")
                .icon("database")
                .requires_extension("postgres")
                .properties(database_read(
                    "dbname=analytics host=localhost user=postgres",
                )),
            builders::source_postgres,
        ),
        (
            ComponentSpec::new("src.db.mysql", "MySQL table")
                .description("Read a table from a MySQL or MariaDB database.")
                .icon("database")
                .requires_extension("mysql")
                .properties(database_read("host=localhost user=root database=analytics")),
            builders::source_mysql,
        ),
        (
            ComponentSpec::new("src.db.sqlite", "SQLite table")
                .description("Read a table from a SQLite database file.")
                .icon("database")
                .requires_extension("sqlite")
                .properties(database_read("data/analytics.db")),
            builders::source_sqlite,
        ),
        // -- Transforms: reshaping columns ---------------------------------
        (
            ComponentSpec::new("xf.derive", "Derive columns")
                .description("Add computed columns, keeping every column already there.")
                .icon("square-plus")
                .properties(vec![PropertySpec::sql("expressions").required().help(
                    "A select list, e.g. amount * 1.2 AS gross, upper(name) AS name_upper.",
                )]),
            builders::transform_derive,
        ),
        (
            ComponentSpec::new("xf.rename", "Rename columns")
                .description("Rename columns, leaving the rest untouched.")
                .icon("pencil")
                .properties(vec![PropertySpec::map("columns")
                    .required()
                    .help("Each column's current name, mapped to its new one.")]),
            builders::transform_rename,
        ),
        (
            ComponentSpec::new("xf.cast", "Cast types")
                .description("Change the type of one or more columns.")
                .icon("type")
                .properties(vec![PropertySpec::map("columns").required().help(
                    "Each column name, mapped to a SQL type such as DECIMAL(10,2).",
                )]),
            builders::transform_cast,
        ),
        // -- Transforms: choosing rows -------------------------------------
        (
            ComponentSpec::new("xf.distinct", "Distinct rows")
                .description("Remove rows that are identical across every column.")
                .icon("copy-minus"),
            builders::transform_distinct,
        ),
        (
            ComponentSpec::new("xf.dedup", "Deduplicate")
                .description("Keep one row per key.")
                .icon("copy-check")
                .properties(vec![
                    PropertySpec::string_list("keys")
                        .required()
                        .help("The columns that identify a duplicate."),
                    PropertySpec::sql("order_by").help(
                        "Which duplicate wins, e.g. updated_at DESC. Unset, any one of them does.",
                    ),
                ]),
            builders::transform_dedup,
        ),
        (
            ComponentSpec::new("xf.sort", "Sort")
                .description("Put the rows in order.")
                .icon("arrow-down-up")
                .properties(vec![PropertySpec::sql("by")
                    .required()
                    .help("e.g. amount DESC, order_id.")]),
            builders::transform_sort,
        ),
        (
            ComponentSpec::new("xf.limit", "Limit")
                .description("Keep at most a fixed number of rows.")
                .icon("list-end")
                .properties(vec![
                    PropertySpec::integer("count")
                        .required()
                        .help("How many rows to keep."),
                    PropertySpec::integer("offset")
                        .default(JsonValue::from(0))
                        .help("How many rows to skip first."),
                ]),
            builders::transform_limit,
        ),
        (
            ComponentSpec::new("xf.sample", "Sample")
                .description("Take a random subset, for a quick look at a large input.")
                .icon("dices")
                .properties(vec![
                    PropertySpec::integer("size")
                        .required()
                        .help("How many rows, or what percentage."),
                    PropertySpec::enumerated("unit", &["rows", "percent"])
                        .default(JsonValue::String("rows".into())),
                ]),
            builders::transform_sample,
        ),
        // -- Transforms: summarising ---------------------------------------
        (
            ComponentSpec::new("xf.aggregate", "Aggregate")
                .description("Group rows and summarise each group.")
                .icon("sigma")
                .properties(vec![
                    PropertySpec::sql("aggregations")
                        .required()
                        .help("e.g. sum(amount) AS total, count(*) AS orders."),
                    PropertySpec::string_list("group_by").help(
                        "Columns to group by. Left empty, the whole input is a single group.",
                    ),
                ]),
            builders::transform_aggregate,
        ),
        (
            ComponentSpec::new("xf.window", "Window function")
                .description("Add a column computed across a window of related rows.")
                .icon("panel-top")
                .properties(vec![
                    PropertySpec::sql("expression")
                        .required()
                        .help("e.g. sum(amount), or row_number()."),
                    PropertySpec::text("output_column")
                        .required()
                        .help("What to call the new column."),
                    PropertySpec::string_list("partition_by")
                        .help("Restart the window for each distinct value of these columns."),
                    PropertySpec::sql("order_by").help("Row order within the window."),
                ]),
            builders::transform_window,
        ),
        (
            ComponentSpec::new("xf.pivot", "Pivot")
                .description("Turn row values into columns.")
                .icon("table-2")
                .properties(vec![
                    PropertySpec::string_list("on")
                        .required()
                        .help("The columns whose values become new columns."),
                    PropertySpec::string_list("values").required().help(
                        "The values to make columns for. Required rather than detected: a \
                         pivot inside a view cannot discover them from the data.",
                    ),
                    PropertySpec::sql("using")
                        .required()
                        .help("How to combine each cell, e.g. sum(amount)."),
                    PropertySpec::string_list("group_by").help("Columns to keep as rows."),
                ]),
            builders::transform_pivot,
        ),
        (
            ComponentSpec::new("xf.unpivot", "Unpivot")
                .description("Turn columns into rows.")
                .icon("table-columns-split")
                .properties(vec![
                    PropertySpec::string_list("columns")
                        .required()
                        .help("The columns to fold into rows."),
                    PropertySpec::text("name_column")
                        .default(JsonValue::String("name".into()))
                        .help("The column that will hold the old column name."),
                    PropertySpec::text("value_column")
                        .default(JsonValue::String("value".into()))
                        .help("The column that will hold the value."),
                ]),
            builders::transform_unpivot,
        ),
        // -- Transforms: combining two inputs ------------------------------
        (
            ComponentSpec::new("xf.union", "Union")
                .description("Stack two inputs on top of one another.")
                .icon("rows-3")
                .inputs(vec![
                    PortSpec::new("left").help("The rows that come first."),
                    PortSpec::new("right").help("The rows appended to them."),
                ])
                .properties(vec![
                    PropertySpec::boolean("all")
                        .default(JsonValue::Bool(true))
                        .help("Keep duplicate rows. Switched off, identical rows collapse."),
                    PropertySpec::boolean("by_name")
                        .default(JsonValue::Bool(false))
                        .help("Match columns by name rather than by position."),
                ]),
            builders::transform_union,
        ),
        (
            ComponentSpec::new("xf.intersect", "Intersect")
                .description("Keep only the rows present in both inputs.")
                .icon("circle-dot")
                .inputs(vec![PortSpec::new("left"), PortSpec::new("right")])
                .properties(vec![PropertySpec::boolean("all")
                    .default(JsonValue::Bool(false))
                    .help("Keep duplicate matches rather than one row each.")]),
            builders::transform_intersect,
        ),
        (
            ComponentSpec::new("xf.except", "Except")
                .description("Keep the rows from the left input that are not in the right.")
                .icon("circle-minus")
                .inputs(vec![PortSpec::new("left"), PortSpec::new("right")])
                .properties(vec![PropertySpec::boolean("all")
                    .default(JsonValue::Bool(false))
                    .help("Keep duplicate rows rather than one of each.")]),
            builders::transform_except,
        ),
        // -- Sinks --------------------------------------------------------
        (
            ComponentSpec::new("snk.file.parquet", "Parquet file")
                .description("Write a Parquet file.")
                .icon("file-box")
                .properties(vec![
                    PropertySpec::path("path").required().help("File to write."),
                    write_mode(),
                    PropertySpec::enumerated(
                        "compression",
                        &["zstd", "snappy", "gzip", "brotli", "lz4", "uncompressed"],
                    )
                    .default(JsonValue::String("zstd".into())),
                ]),
            builders::sink_parquet,
        ),
        (
            ComponentSpec::new("snk.file.csv", "CSV file")
                .description("Write a delimited text file.")
                .icon("file-text")
                .properties(vec![
                    PropertySpec::path("path").required().help("File to write."),
                    write_mode(),
                    PropertySpec::boolean("header")
                        .default(JsonValue::Bool(true))
                        .help("Write column names as the first row."),
                    PropertySpec::text("delimiter").default(JsonValue::String(",".into())),
                ]),
            builders::sink_csv,
        ),
        (
            ComponentSpec::new("snk.file.json", "JSON file")
                .description("Write a JSON file holding an array of records.")
                .icon("file-json")
                .properties(vec![
                    PropertySpec::path("path").required().help("File to write."),
                    write_mode(),
                ]),
            builders::sink_json,
        ),
        (
            ComponentSpec::new("snk.file.jsonl", "JSON Lines file")
                .description("Write one JSON record per line.")
                .icon("file-json")
                .properties(vec![
                    PropertySpec::path("path").required().help("File to write."),
                    write_mode(),
                ]),
            builders::sink_jsonl,
        ),
        (
            ComponentSpec::new("snk.file.excel", "Excel file")
                .description("Write an .xlsx workbook.")
                .icon("sheet")
                .requires_extension("excel")
                .properties(vec![
                    PropertySpec::path("path")
                        .required()
                        .help("Workbook to write."),
                    write_mode(),
                    PropertySpec::boolean("header")
                        .default(JsonValue::Bool(true))
                        .help("Write column names as the first row."),
                    PropertySpec::text("sheet").help("Name for the sheet."),
                ]),
            builders::sink_excel,
        ),
        (
            ComponentSpec::new("snk.cloud.s3", "S3 object")
                .description("Write a file to S3 or another S3-compatible store.")
                .icon("cloud-upload")
                .requires_extension("httpfs")
                .properties(vec![
                    PropertySpec::path("path").required().help("An s3:// URI."),
                    cloud_format(),
                    PropertySpec::boolean("header")
                        .default(JsonValue::Bool(true))
                        .help("For CSV: write column names as the first row."),
                    PropertySpec::enumerated(
                        "compression",
                        &["zstd", "snappy", "gzip", "brotli", "lz4", "uncompressed"],
                    )
                    .default(JsonValue::String("zstd".into()))
                    .help("For Parquet."),
                ]),
            builders::sink_s3,
        ),
        // -- Sinks: databases ----------------------------------------------
        (
            ComponentSpec::new("snk.db.postgres", "PostgreSQL table")
                .description("Write a table into a PostgreSQL database.")
                .icon("database")
                .requires_extension("postgres")
                .properties(database_write(
                    "dbname=analytics host=localhost user=postgres",
                )),
            builders::sink_postgres,
        ),
        (
            ComponentSpec::new("snk.db.mysql", "MySQL table")
                .description("Write a table into a MySQL or MariaDB database.")
                .icon("database")
                .requires_extension("mysql")
                .properties(database_write(
                    "host=localhost user=root database=analytics",
                )),
            builders::sink_mysql,
        ),
        (
            ComponentSpec::new("snk.db.sqlite", "SQLite table")
                .description("Write a table into a SQLite database file.")
                .icon("database")
                .requires_extension("sqlite")
                .properties(database_write("data/analytics.db")),
            builders::sink_sqlite,
        ),
        // -- Quality ------------------------------------------------------
        //
        // Every one of these splits its input: the rows that passed leave by
        // `main`, the ones that did not by `rejected`. Leaving the reject port
        // unwired drops those rows, which is the ordinary case — wiring it to a
        // sink is what turns a check into a dead-letter report.
        (
            ComponentSpec::new("qa.not_null", "Not null")
                .description("Reject rows where any of the named columns is null.")
                .icon("shield-alert")
                .properties(vec![PropertySpec::string_list("columns")
                    .required()
                    .help("Every one of these must have a value.")]),
            builders::quality_not_null,
        ),
        (
            ComponentSpec::new("qa.unique", "Unique")
                .description("Reject rows whose key appears more than once.")
                .icon("fingerprint")
                .properties(vec![PropertySpec::string_list("columns").required().help(
                    "The key. Named together they form one compound key, not one check \
                         each. Every copy of a repeated key is rejected — to keep one instead, \
                         use xf.dedup.",
                )]),
            builders::quality_unique,
        ),
        (
            ComponentSpec::new("qa.range", "Range")
                .description("Reject rows whose value falls outside a numeric range.")
                .icon("ruler")
                .properties(vec![
                    PropertySpec::text("column")
                        .required()
                        .help("The column to bound."),
                    PropertySpec::number("min").help("Lowest accepted value, inclusive."),
                    PropertySpec::number("max").help("Highest accepted value, inclusive."),
                ]),
            builders::quality_range,
        ),
        (
            ComponentSpec::new("qa.regex", "Pattern")
                .description("Reject rows whose text does not match a pattern.")
                .icon("regex")
                .properties(vec![
                    PropertySpec::text("column")
                        .required()
                        .help("The column to test."),
                    PropertySpec::text("pattern")
                        .required()
                        .help("A regular expression, e.g. ^[^@]+@[^@]+$ for an email address."),
                ]),
            builders::quality_regex,
        ),
        (
            ComponentSpec::new("qa.accepted_values", "Accepted values")
                .description("Reject rows whose value is not one of a listed set.")
                .icon("list-checks")
                .properties(vec![
                    PropertySpec::text("column")
                        .required()
                        .help("The column to test."),
                    PropertySpec::string_list("values")
                        .required()
                        .help("The values this column is allowed to hold."),
                ]),
            builders::quality_accepted_values,
        ),
        (
            ComponentSpec::new("qa.expression", "Expression")
                .description("Reject rows for which a SQL expression is not true.")
                .icon("code")
                .properties(vec![PropertySpec::sql("predicate").required().help(
                    "A boolean SQL expression over the input's columns, e.g. total >= 0 AND \
                     status <> 'void'. A row for which it is unknown is rejected.",
                )]),
            builders::quality_expression,
        ),
        (
            ComponentSpec::new("qa.referential", "Referential")
                .description("Reject rows whose key is not present in a second input.")
                .icon("link")
                .inputs(vec![PortSpec::new("left"), PortSpec::new("right")])
                .properties(vec![
                    PropertySpec::text("column")
                        .required()
                        .help("The key on the left input."),
                    PropertySpec::text("reference_column")
                        .required()
                        .help("The column on the right input it must be found in."),
                ]),
            builders::quality_referential,
        ),
    ]
}

/// The file formats the object-storage components can read and write. Narrower
/// than the file components' own list because these go over the network, where
/// a columnar format is almost always the right answer.
fn cloud_format() -> PropertySpec {
    PropertySpec::enumerated("format", &["parquet", "csv", "json"])
        .default(JsonValue::String("parquet".into()))
}

/// What every database source is configured with. The three of them differ only
/// in their connection string, so only the example changes.
fn database_read(example: &str) -> Vec<PropertySpec> {
    vec![
        PropertySpec::text("connection")
            .required()
            .help(&format!("Connection string, e.g. {example}")),
        PropertySpec::text("table")
            .required()
            .help("Table to read."),
        PropertySpec::text("schema").help("Unset, the database's own default schema applies."),
    ]
}

/// The same for a database sink, plus how to write.
fn database_write(example: &str) -> Vec<PropertySpec> {
    vec![
        PropertySpec::text("connection")
            .required()
            .help(&format!("Connection string, e.g. {example}")),
        PropertySpec::text("table")
            .required()
            .help("Table to write."),
        PropertySpec::text("schema").help("Unset, the database's own default schema applies."),
        PropertySpec::enumerated("mode", &["overwrite", "append"])
            .default(JsonValue::String("overwrite".into()))
            .label("If the table exists")
            .help("overwrite replaces the table; append adds rows to it."),
    ]
}

/// Every file sink shares this property, so it is written once.
fn write_mode() -> PropertySpec {
    PropertySpec::enumerated("mode", &["overwrite", "error_if_exists"])
        .default(JsonValue::String("overwrite".into()))
        .label("If the file exists")
        .help("error_if_exists refuses before anything is written.")
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// A property set in a node that its component does not define.
///
/// A warning rather than an error: it is usually a typo, but it is also what a
/// document from a newer version looks like, and refusing to run someone's
/// pipeline over an extra key would be the wrong trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownProperty {
    pub node_id: String,
    pub property: String,
}

/// Check a node's properties against its spec and fill in defaults.
///
/// This is where Duckle's known gap is closed: **every** required property is
/// checked here, by name, rather than only the ones a builder happens to read.
/// A component that gains a required property gains the check for free.
pub(crate) fn resolve_properties(
    node_id: &str,
    spec: &ComponentSpec,
    properties: &JsonValue,
    unknown: &mut Vec<UnknownProperty>,
) -> Result<JsonValue, EngineError> {
    let supplied = match properties {
        JsonValue::Object(map) => map.clone(),
        JsonValue::Null => Map::new(),
        _ => {
            return Err(EngineError::InvalidProperty {
                id: node_id.to_string(),
                property: "properties".to_string(),
                reason: "must be an object".to_string(),
            })
        }
    };

    for name in supplied.keys() {
        if spec.property(name).is_none() {
            unknown.push(UnknownProperty {
                node_id: node_id.to_string(),
                property: name.clone(),
            });
        }
    }

    let mut resolved = Map::new();

    for property in &spec.properties {
        match supplied.get(&property.name) {
            Some(JsonValue::Null) | None => {
                if let Some(default) = &property.default {
                    resolved.insert(property.name.clone(), default.clone());
                } else if property.required {
                    return Err(EngineError::MissingProperty {
                        id: node_id.to_string(),
                        component_id: spec.id.clone(),
                        property: property.name.clone(),
                    });
                }
            }

            Some(value) => {
                check_value(node_id, property, value)?;
                resolved.insert(property.name.clone(), value.clone());
            }
        }
    }

    // Unknown keys are carried through rather than dropped, so a builder in a
    // newer version can still see them.
    for (name, value) in supplied {
        resolved.entry(name).or_insert(value);
    }

    Ok(JsonValue::Object(resolved))
}

fn check_value(
    node_id: &str,
    property: &PropertySpec,
    value: &JsonValue,
) -> Result<(), EngineError> {
    if !property.property_type.accepts(value) {
        return Err(EngineError::InvalidProperty {
            id: node_id.to_string(),
            property: property.name.clone(),
            reason: property.property_type.expectation().to_string(),
        });
    }

    if let Some(text) = value.as_str() {
        if property.required && text.trim().is_empty() {
            return Err(EngineError::InvalidProperty {
                id: node_id.to_string(),
                property: property.name.clone(),
                reason: "must not be empty".to_string(),
            });
        }

        if !property.options.is_empty() && !property.options.iter().any(|o| o == text) {
            return Err(EngineError::InvalidProperty {
                id: node_id.to_string(),
                property: property.name.clone(),
                reason: format!("must be one of: {}", property.options.join(", ")),
            });
        }
    }

    Ok(())
}

/// Check that the wiring matches the component's declared ports.
pub(crate) fn check_input_count(
    node_id: &str,
    spec: &ComponentSpec,
    actual: usize,
) -> Result<(), EngineError> {
    if spec.input_count() == actual {
        return Ok(());
    }

    Err(EngineError::WrongInputCount {
        id: node_id.to_string(),
        component_id: spec.id.clone(),
        expected: spec.input_count(),
        actual,
    })
}

/// Look up a component, or say clearly that it does not exist.
pub(crate) fn lookup(node_id: &str, component_id: &str) -> Result<&'static Component, EngineError> {
    registry()
        .get(component_id)
        .ok_or_else(|| EngineError::UnsupportedComponent {
            id: node_id.to_string(),
            component_id: component_id.to_string(),
        })
}

#[cfg(test)]
mod tests;
