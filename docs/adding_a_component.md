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
input port and one `main` output automatically. Override with `.inputs(...)` /
`.outputs(...)` only when the component is unusual — a join takes two named
inputs, a quality check has a second `reject` output.

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
