# Adding a component

Three things: a **spec**, a **builder**, a **test** — plus one line in the
registry inventory test. There is no dispatch `match` to extend, no separate
frontend list, no manifest to regenerate. If you find yourself editing anything
else, something has drifted and is worth fixing before adding the component.

With ~400 components to reach, that property is the whole point of the registry.
It was verified rather than assumed: `src.file.jsonl` — the example used
throughout this document — was added exactly this way, and the only test that
broke was the inventory.

## 1. The spec

Add an entry to `all_components()` in
[`crates/duckdb-engine/src/plan/specs.rs`](../crates/duckdb-engine/src/plan/specs.rs).
The spec is pure data: it says what the component is called, what it is wired to,
and what can be configured.

```rust
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
```

The namespace comes from the id, so `src.file.jsonl` is a source and gets no
input port and one `main` output automatically. A `qa.*` id gets one input and
**two** outputs, `main` and `rejected`. Override with `.inputs(...)` /
`.outputs(...)` only when the component is unusual within its namespace — a join
takes two named inputs, and so does `qa.referential`.

**Property types** are `text`, `path`, `sql`, `boolean`, `integer`, `number`,
`string_list`, `map`, and `enumerated(&[...])`. The type decides both the
validation and the control the canvas renders, which is why a path is not just
text. A `map` is ordered name/value pairs — `xf.rename` maps old name to new,
`xf.cast` maps column to type — and the entry order is preserved all the way
into the generated SQL.

Two rules the tests enforce:

- A property is either `.required()` **or** has a `.default(...)` — never both.
  A required property with a default is never actually required.
- An enum's default must be one of its own options.

### Components that need a DuckDB extension

Declare it on the spec rather than leaving it implied by the generated SQL:

```rust
ComponentSpec::new("src.db.postgres", "PostgreSQL table")
    .requires_extension("postgres")
```

[`Plan::extensions`](../crates/duckdb-engine/src/plan/mod.rs) then collects the
union across the plan's stages and `Plan::script` emits a `LOAD` prelude before
the first stage. Declaring it buys two things the SQL alone cannot: the canvas
can warn that a pipeline needs `postgres` *before* someone starts a run, and
Phase 9 knows exactly which extension files to vendor for an air-gapped build.

`LOAD`, never `INSTALL` — a run must not depend on reaching the internet. The extensions are
vendored into `tools/duckdb/extensions/` by `scripts/fetch-duckdb-extensions.ps1`; add the name
to that script's list when a new component needs one that is not there yet.

### Components that take a credential

There is no `Secret` property type, and deliberately so. What lives in the document is the
*reference* — a `connection` property holding `password=${SECRET:pg}` — which is ordinary text
and safe to commit. Only the resolved value is sensitive, and that never reaches the document.

So a component that needs a credential needs nothing special: a `text` property, and the
documentation to say a `${SECRET:...}` reference belongs in it. Resolution, masking in the plan
view, and masking in DuckDB's error output are all handled before the builder is reached.

## 2. The builder

Add the function to
[`builders.rs`](../crates/duckdb-engine/src/plan/builders.rs). By the time it is
called, the engine has already checked the component exists, the input count
matches the declared ports, every required property is present, every supplied
property has the right type, and defaults have been filled in.

So the builder only turns valid input into SQL:

```rust
pub(crate) fn source_jsonl(node: &Lowering<'_>) -> Result<String, EngineError> {
    let path = required_str(node, "path")?;

    let body = format!(
        "SELECT * FROM read_json({}, format='newline_delimited', ignore_errors={})",
        quote_path(path),
        resolved_bool(node, "ignore_errors")?
    );

    Ok(create_view(node, &body))
}
```

**Never restate a default in a builder.** `resolved_bool` and `resolved_str` read
values the spec guarantees are present. A `.unwrap_or(true)` here is a second
copy of the default that will eventually disagree with the spec.

**Always go through `quote_identifier`, `quote_literal`, or `quote_path`** for
anything that came from the document. The exceptions are properties typed `sql`,
which are user-written SQL by definition — that is what the type means.

Use `create_view(node, &body)` for anything producing a relation and
`copy_to(upstream, path, &options)` for a sink; they handle the statement
wrapper and the alias view.

Note that `exactly_one_input` and `exactly_two_inputs` hand back the **relation**
to read, not the upstream node id. When the edge came from a quality node's
`rejected` port those differ, and the relation is the one that exists. Nothing in
a builder should reach for `input.node_id` directly.

### Quality components

A `qa.*` component splits rather than filters, so it does not call `create_view`
itself. It works out a base relation and a boolean predicate and hands both to
`quality_split`, which writes the accepted and rejected views together:

```rust
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
```

Write the predicate for the rows that **pass**; `quality_split` derives the
rejected side by negating it. Do not write the two separately — the split is
exact precisely because both sides read one expression, and a hand-written
negation is where the rows would start going missing. If the predicate needs a
helper column, as `qa.unique` does for its window function, name it in the
fourth argument and it is projected away.

The row counts follow automatically: a component whose spec has a `rejected`
port gets two count probes, and the executor reports both.

### Control components

A `ctl.*` component, and the two `qa.*` assertions, do something a single
batched script cannot express. They declare it on the spec with
`.control(ControlKind::…)`, and that one call is what makes a plan containing
them run through a persistent session instead:

```rust
ComponentSpec::new("ctl.wait", "Wait")
    .control(ControlKind::Wait)
    .properties(vec![PropertySpec::integer("ms").required()]),
builders::control_passthrough,
```

All of them share one builder. `control_passthrough` emits a view equal to the
input and nothing else — a control node must not change what the data *is*, only
what happens around it, and a node that broke the chain it sits in would be
unusable where anyone would put one.

What the node actually *does* is built by `control_for` in `builders.rs`, from
the kind and the properties. Most of it is not SQL — a duration, a message, a
decision — which is why it is not a builder's job. The part that is SQL is a
**probe**: a query the executor runs to decide something, whose meaning depends
on the kind (a match count for `Fail` and `Branch`, a single `ok` boolean for
`Assert`).

Adding a control component therefore touches one more place than an ordinary
one: a `ControlKind` arm in `control_for`, and, if the kind is new, an arm in
`exec::act` that says what to do with the answer. Both are `match`es on an enum,
not on a component id — the registry's no-dispatch rule still holds.

See `docs/DECISION_execution_model.md` for why a session exists at all.

### What the spec cannot express

Per-property rules only. A constraint spanning two properties — `xf.join` needing
`keys` *or* `condition`, never both — stays in the builder. That division is
deliberate and there is a test pinning it
(`a_rule_spanning_two_properties_stays_in_the_builder`), so it does not quietly
become "some validation is here, some is there, who knows which".

## 3. The test

Add a golden-SQL test to
[`builder_tests.rs`](../crates/duckdb-engine/src/plan/builder_tests.rs)
asserting the **exact** generated statement:

```rust
#[test]
fn jsonl_source_reads_newline_delimited_json() {
    let plan = compile_one("src.file.jsonl", json!({ "path": "in.jsonl" }));

    assert_eq!(
        sql_of(&plan, "n"),
        r#"CREATE OR REPLACE TEMP VIEW "n" AS (SELECT * FROM read_json('in.jsonl', format='newline_delimited', ignore_errors=false));"#
    );
}
```

Exact-match assertions are deliberately brittle. The SQL is shown to users on the
plan view and is the thing that actually runs, so a change to it should be a
decision someone makes rather than a diff nobody notices.

### The inventory

`the_registry_holds_exactly_these_components` in `specs/tests.rs` lists every
component id. Add yours, in sorted order. This is deliberate: adding a component
should show up in a diff, and one disappearing through a bad merge should turn
something red.

The rest of the registry-wide tests in
[`specs/tests.rs`](../crates/duckdb-engine/src/plan/specs/tests.rs) then cover
the new component automatically: label and description present, defaults valid
for their own type, enum defaults among their options, ports unique, manifest
round-trips. You do not add anything for those.

## Check it

```powershell
cargo test --workspace
.\target\debug\etl.exe components --namespace src
.\target\debug\etl.exe components --manifest
```

The component should appear in the listing and in the manifest, with its
property schema, without any further work — that is the registry doing its job.

## Native components (written in Rust)

For data DuckDB cannot reach: a format with no reader, an API with no extension. Everything
above still applies to the *spec*. What differs is that there is no builder to write and no
line in `specs.rs` at all. Added in Phase 10a; `src.file.xml` and `snk.file.xml` are the worked
example, in [`crates/connectors/src/xml.rs`](../crates/connectors/src/xml.rs).

1. **Implement the trait** from `etl-plugin-sdk` in a new module of `crates/connectors/`:
   `Source` (read records, write them to a `RecordWriter`) or `Sink` (read records from a
   `RecordReader`, deliver them). A record is a JSON object. `spec()` returns the
   `ComponentSpec` exactly as above, and a source's spec should include
   `etl_plugin_sdk::columns_property()`.
2. **List it** in `etl_connectors::all()`. That is the whole registration: the engine adds
   every connector listed there to its registry, paired with the one builder for its
   direction, so the canvas, `etl components`, validation, lineage, the scheduler, the
   console and a built artifact all have it.
3. **Test the connector** in its own crate, against strings and temporary files, with no
   DuckDB. The engine's side of the bridge is already tested once for everybody, in
   `crates/duckdb-engine/tests/native.rs`. A web connector tests against the local HTTP
   server in `crates/connectors/src/fixture.rs`, which REST and GraphQL share.

**A source that reads only what is new** (a stream, a change feed) keeps a position rather
than re-reading everything. Read `context.checkpoint` for where the last successful run
stopped (`None` on a first run), and return the new position as `Summary::checkpoint`. The
value is yours, as JSON: nobody else reads it. Put enough of the configuration in it to
recognise it as stale (Kafka's names its topic), and refuse, with the way out (`etl state
forget`), a position you did not write. The engine saves it only after a run that fully
succeeded, in the same file and write as watermarks, for `etl run`, the scheduler, the
console and a built artifact alike (`etl_duckdb_engine::remember`). `src.stream.kafka` in
[`crates/connectors/src/kafka.rs`](../crates/connectors/src/kafka.rs) is the worked example.
A connector that needs an async client keeps its runtime inside `read`, as that one does.

**A web connector** (anything over HTTP) builds on
[`crates/connectors/src/http.rs`](../crates/connectors/src/http.rs) rather than on `ureq`
directly: `connection_properties` for the spec, `Settings` and `Client` for the requests. That
gives it auth, retries on 429 and 5xx with `Retry-After`, pacing, timeouts and the shared
page-cap error for free. If the protocol can report failure or throttling inside a 2xx, as
GraphQL does, pass a judgement to `Client::send_judged` instead of inventing a second retry
loop. `rest.rs` and `graphql.rs` are the worked examples.
4. **Add it to the inventory** in `specs/tests.rs`, as for any component.
5. **Write its delivery semantics** in [connectors.md](connectors.md): what it promises, what
   it does not, and what a failure partway leaves behind. That is half of "done" for a
   connector.

What the engine does with it, so you do not have to:

- A source runs **before** DuckDB, writing its records to `.etl/tmp/native/<node_id>.jsonl`.
  The node's SQL is a view over that file (see `builders::native_source`), so counts,
  `incremental`, `materialize` and aliases work unchanged.
- A sink's SQL is a `COPY` into its staging file (`builders::native_sink`). The connector is
  called **after** DuckDB, and only when the whole run succeeded.
- A `path` property on a sink is where `mode` is checked and the parent directory is made,
  the same as for a file sink DuckDB writes itself.
- Errors from the connector become a stage failure naming the node, with secrets masked.
- Staging files are deleted on every path out of a run.

Two rules that are easy to break:

- **Connectors never depend on the engine.** They know about records and their own
  properties, and nothing about SQL or plans.
- **Pure Rust, blocking where possible** (Settled decision 10). No system C libraries, since
  the Linux runner is built in a bare bookworm image and ships with no dependencies. An async
  runtime only if a family genuinely cannot avoid one, and then it is a decision recorded in
  the tracker, not a line in a `Cargo.toml`.
