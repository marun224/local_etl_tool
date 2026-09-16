# PLAN — Duckle parity build

Goal: a local-first visual ETL/ELT studio matching Duckle feature-for-feature — Rust Cargo
workspace, Tauri 2 + React 19 canvas, pipeline DAG compiled to DuckDB SQL and executed by
shelling out to the DuckDB CLI.

Reference checkout: `D:\workspace\duckle-main`. Clean-room: take architecture and behaviour,
never source, manifests, or docs. See *Licensing* in [ET_Local_Tool.md](ET_Local_Tool.md).

---

## Source-verified corrections to ET_Local_Tool.md

Read the checkout before trusting the report on these. All four were confirmed at file level
on 2026-09-15.

**1. Most crates are empty stubs.** The 16-crate workspace looks like rich layering; it is not.
Measured Rust LOC:

| Crate | LOC | State |
|---|---:|---|
| `duckdb-engine` | 133,151 | The entire product |
| `duckle-runner` | 27,685 | Headless CLI + serve |
| `duckle-mcp` | 2,834 | Real |
| `scheduler` | 2,207 | Real |
| `duckle-secrets` | 1,379 | Real |
| `duckle-gpu` | 1,016 | Real |
| `connectors` | 642 | **CSV only** |
| `duckle-lance` | 438 | Real |
| `metadata` | 253 | Shared types |
| `plugin-sdk` | 61 | Schema-inspection trait only |
| `execution-core`, `runtime`, `workflow-engine`, `transform-engine`, `stream-engine`, `slothdb-engine` | 6–8 each | **Doc comment only — zero implementation** |

Those six carry an architectural intent ("engine-agnostic logical plan", "Arrow-native
vectorized operators", "backpressure-aware streaming") that exists purely as a `//!` block
pointing at an `ARCHITECTURE.md`. There is no engine abstraction, no Arrow transform engine,
no streaming engine, and no SlothDB adapter. **Everything real lives in one monolithic
`duckdb-engine` crate.** Parity with shipped Duckle is therefore far smaller than the crate
list implies — but so is Duckle's architecture, and we should not copy the stub layering as
if it were load-bearing.

**2. The node parameter key is `data.properties`, not `config`.** The report's draft schema was
wrong. The real shape (from `crates/metadata/src/lib.rs`):

```jsonc
{
  "formatVersion": 1,
  "resourcePool": "",
  "parameters": { },              // typed parameter contract
  "nodes": [{
    "id": "n1",
    "type": "source",             // ReactFlow node type: source | transform | sink
    "position": { "x": 0, "y": 0 },
    "data": {
      "label": "Orders",
      "componentId": "src.file.parquet",   // the namespaced component
      "properties": { },                   // <-- node config lives here
      "schema": null,
      "sampleRows": null,
      "disabled": false,
      "alias": null                        // friendly SQL relation name
    }
  }],
  "edges": [ ]
}
```

**3. Component count is ~417 unique ids**, not 385: `xf` 163, `src` 123, `snk` 75, `qa` 29,
`ctl` 21, `code` 6. (Literal-scan of the engine crate; includes some aliases, so treat as an
upper bound.)

**4. No Zustand.** The report inferred it; `frontend/package.json` has no state library.
Confirmed stack: React 19.2, `@xyflow/react` 12.11, Vite 8, TypeScript 7, Tauri 2 plugins,
i18next (i18n), Vega/Vega-Lite (charts), Prism + react-simple-code-editor (SQL editing),
lucide-react + simple-icons (icons).

Confirmed as reported: DuckDB is invoked as an external CLI (`std::process::Command`, located
via `DUCKLE_DUCKDB_BIN`), and non-sink stages lower to
`CREATE OR REPLACE TEMP VIEW "<node_id>" AS (...)` with sinks as `COPY (...) TO '...'`.

---

## Naming

Crates mirror Duckle's names with an `etl-` prefix (`etl-duckdb-engine`, `etl-runner`,
`etl-mcp`, `etl-metadata`, …); product binary `etl`. This is a find-and-replace away from
anything else — decide before Phase 1 ends, it gets expensive after.

**Deliberate divergence from Duckle's layout:** we do *not* create the six stub crates. They
are added when something real goes in them (earliest: Phase 10). A stub crate that claims an
architecture it does not have is worse than no crate.

---

## Phases

Each phase is one sitting. Assume total context loss between phases: the phase text must be
enough to work from alone. Status lives in [task_tracker.md](task_tracker.md), never here.

### Phase 0 — Workspace skeleton + pipeline document model

**Goal.** `cargo build` and `cargo test` pass on an empty workspace that can deserialize a
pipeline JSON document round-trip.

**Files.** `Cargo.toml` (workspace), `rust-toolchain.toml`, `.gitignore`,
`crates/metadata/{Cargo.toml,src/lib.rs}`, `samples/pipelines/csv_to_parquet.json`.

**Do.** Workspace with resolver 2, shared `[workspace.package]`, edition 2021. `etl-metadata`
defines `PipelineDoc`, `PipelineNode`, `NodeData`, `PipelineEdge`, `Position`, `Schema`,
`Field` with the exact serde renames in *Correction 2* above.

**Verify.** `cargo test -p etl-metadata` — round-trip test: sample JSON → structs → JSON is
byte-identical for the fields we model; unknown fields survive.

**Done.** Sample pipeline deserializes; `cargo build` clean; no warnings.

### Phase 1 — DAG validation + topological sort + plan skeleton

**Goal.** A pipeline compiles to an ordered list of `Stage`s with SQL still empty.

**Files.** `crates/duckdb-engine/{Cargo.toml,src/lib.rs,src/plan/mod.rs}`.

**Do.** `Stage { node_id, component_id, label, sql, kind, from, .. }` and `StageKind`
(source/transform/sink). Kahn topological sort over edges; detect cycles, orphan nodes,
unknown edge endpoints, disabled-node skipping. Return typed `EngineError`.

**Verify.** Unit tests: linear DAG orders 1..N; diamond DAG orders correctly; cycle errors;
edge to missing node errors; disabled node drops it and its dependents.

**Done.** `compile()` returns ordered stages for the sample; every error case has a test.

### Phase 2 — SQL lowering for the first 8 components + CLI executor

**Goal.** `etl run samples/pipelines/csv_to_parquet.json` actually moves data. **This is the
vertical slice — everything after is breadth.**

**Files.** `crates/duckdb-engine/src/plan/builders.rs`, `src/exec.rs`, `crates/cli/`.

**Do.** Lower `src.file.csv`, `src.file.parquet`, `xf.sql`, `xf.filter`, `xf.select`,
`xf.join`, `snk.file.parquet`, `snk.file.csv`. Non-sinks become
`CREATE OR REPLACE TEMP VIEW "<node_id>" AS (...)`; sinks become
`COPY (...) TO '...' (FORMAT ...)`. Executor locates the DuckDB binary via `ETL_DUCKDB_BIN`
then PATH, runs `duckdb -json -c`, parses JSON output, reports per-stage row counts and
timings. **SQL identifier and literal escaping is written here and tested here** — every later
component depends on it.

**Verify.** End-to-end: CSV → filter → join → Parquet produces a file with the expected row
count and checksum. `etl validate` compiles without executing. Escaping tests cover quotes,
backslashes, and unicode in identifiers and paths.

**Done.** The sample pipeline runs green from a cold shell on Windows.


#### Verified DuckDB CLI behaviour (v1.5.5, Windows, 2026-09-15)

Established by smoke test before implementation. Do not re-derive these.

1. **`-json` emits one JSON array per result-producing statement, concatenated** — not one array
   for the batch. `SELECT 1 AS a; SELECT 2 AS b;` prints `[{"a":1}]` then `[{"b":2}]`. The
   executor must read stdout as a *stream* of JSON values
   (`serde_json::Deserializer::from_str(..).into_iter::<Value>()`), never `from_str` over the
   whole buffer.
2. **`COPY` prints nothing at all** — no row count. Sink row counts must come from an explicit
   `SELECT count(*)` against the sink's `from` relation; there is nothing to harvest from the
   write itself.
3. **A failed statement aborts the rest of the batch.** Exit code 1, message on stderr, and
   stdout holds the results of the statements that already succeeded. Batching the whole plan
   into one `-c` therefore gives fail-fast for free, and the count of JSON values on stdout
   identifies which stage failed.
4. **Escaping is doubling, and it is load-bearing.** `"` doubles inside quoted identifiers, `'`
   doubles inside literals. An unescaped quote is a parse error rather than silent corruption,
   which is the good failure mode — but it is still a failure, so `sql_escape` is not optional.
5. **Windows paths need no special handling.** Single-quoted literals do no escape processing,
   so `D:\path\file.csv`, `D:\path\file.csv`, and `D:/path/file.csv` all resolve identically.
   Generate forward slashes for readability; do **not** double backslashes.

**Acceptance fixture, already in place.** `samples/data/orders.csv` holds 12 rows; the sample
pipeline's filter `order_ts >= '2026-01-01'` keeps 7. Verified end to end by hand:
temp views → `COPY ... TO ... (FORMAT parquet, COMPRESSION zstd)` → read back 7 rows, earliest
`2026-01-04 10:05:00`. Phase 2's acceptance test asserts exactly that.

**The binary.** `tools/duckdb/duckdb.exe` v1.5.5, vendored by `scripts/fetch-duckdb.ps1` and
git-ignored. The executor looks at `ETL_DUCKDB_BIN` first, then `tools/duckdb/`, then PATH.

### Phase 3 — Component spec registry (the thing that makes 417 tractable)

**Goal.** Adding a component is data plus one builder function, not a new match arm in five
files.

**Files.** `crates/duckdb-engine/src/plan/specs.rs`, `crates/metadata/src/component.rs`,
`docs/adding_a_component.md`.

**Do.** A `ComponentSpec` describing id, namespace, label, icon, input/output ports, and a
typed property schema (name, type, required, default, help) that the UI renders generically in
Phase 7. Registry keyed by component id. Builders look up spec, validate properties, emit SQL.
Port the Phase 2 eight into the registry.

**Verify.** Registry round-trips to a JSON manifest; every registered component has a property
schema; `validate` rejects a missing required property *by name* (Duckle's known gap — we fix
it here rather than inherit it).

**Done.** Adding a ninth component touches exactly: one spec, one builder, one test.

### Phase 4 — Connector breadth wave 1: files + DuckDB-reachable databases

**Goal.** ~40 components covering what DuckDB can reach natively.

**Files.** `specs.rs`, `builders.rs`, `src/catalog.rs`, tests.

**Do.** Sources: CSV/Parquet/JSON/JSONL/Excel/XML, Postgres/MySQL/SQLite via `ATTACH`, S3 and
HTTP via `httpfs`, Iceberg/Delta/DuckLake. Sinks: the same file formats plus database writes.
Transforms: map, filter, select, rename, cast, dedup, sort, limit, distinct, union, aggregate,
window, pivot/unpivot, join variants. Extension prelude management (`LOAD` list per pipeline).

**Verify.** Golden-file test per component: fixture in → SQL out matches a committed snapshot,
plus an execution test for one representative of each family.

**Done.** ~40 components registered, each with a golden test.

**Amended 2026-09-15, on completion (40 components).** Two items in "Do" above were not built,
and are deferred rather than quietly dropped:

- **XML.** DuckDB has no core XML reader. The only route is a community extension, which
  contradicts Phase 9's vendored, air-gapped extension set. Revisit in Phase 10 (Rust-native
  connectors), where an XML reader is a better fit than a DuckDB extension anyway.
- **DuckLake.** A catalog format rather than a file format: it needs its own design pass on how
  catalog and data paths are configured, not a thirteenth copy of the `ATTACH` shape. The
  extension is already vendored, so this is design work and not fetching.

Two constraints found while building it, which the plan text did not anticipate:

- **A PIVOT cannot be wrapped in a view unless its values are listed explicitly**, and every
  stage in a plan is a view. `xf.pivot` therefore has a required `values` property.
- **`LOAD` of an uninstalled extension fails even when autoinstall is on**, because autoload
  triggers on use of a function rather than on an explicit LOAD. The prelude is therefore
  strictly more brittle than relying on autoload, which is the intended trade: it fails early
  and by name instead of downloading an extension mid-run.

### Phase 5 — Parameters, contexts, secrets, materialization

**Goal.** Pipelines are portable across environments without editing them.

**Files.** `src/params.rs`, `src/context.rs`, `crates/secrets/`, `src/materialize.rs`.

**Do.** `${VAR}`, `${ContextName.VAR}`, `${ENV:KEY}`, `${workspace}`, `${date}` interpolation
with a typed parameter contract validated once before compilation. Connections AES-256-GCM
encrypted at rest under `.etl/keys/` with a per-workspace key. Per-node materialize
`auto | view | memory | disk` (disk = temp Parquet spill).

**Verify.** An unresolved parameter fails validation naming the parameter; an encrypted
connection round-trips; a `disk`-materialized node produces a temp Parquet and cleans up; a
wrong key fails closed.

**Done.** The sample runs identically in two contexts with different paths.

### Phase 6 — Quality nodes, reject ports, control flow

**Split into 6a and 6b, 2026-09-16, before starting.** As written this phase held two pieces of
work with different risk. `qa.*` extends the existing model: a stage gains a second output and a
second row count, and nothing about how a plan *runs* changes. `ctl.*` does not extend it — it
breaks it. `foreach`, `if/branch`, `wait`, `run-pipeline` and per-stage `retry_attempts` cannot
exist inside one SQL script, and one SQL script handed to DuckDB in one process is the execution
model that Phase 2 chose deliberately and all 40 components sit on. Discovering that mid-sitting
means rewriting `exec.rs` with a phase half-built. They are therefore separate phases, and 6b
starts with a written decision rather than with code.

---

#### Phase 6a — Quality nodes and reject ports

**Goal.** The `qa.*` namespace: a validator splits its input into rows that passed and rows that
did not, in one pass, with both counted.

**Files.** `crates/metadata/src/component.rs`, `src/plan/mod.rs`, `src/plan/builders.rs`,
`src/plan/specs.rs`, `src/exec.rs`, `crates/cli/src/main.rs`.

**Three invariants this breaks, which is the whole of the work.** Each is currently relied on in
code and documented there:

1. **One relation per stage.** `create_view` names the relation after the node id. A quality node
   needs two, so it needs a name for the second and downstream wiring has to read
   `Input::source_handle` to choose between them. The field is already carried end to end and has
   never been read by anything — 6a is what it was plumbed for.
2. **One row count per stage.** `exec::outcomes` zips `counted_stages()` against DuckDB's stdout
   positionally, and `attribute_failure` states the invariant outright. A stage emitting two
   counts shifts every later stage's count by one — silently, with no error, reporting wrong
   numbers against the right node names. `Stage.count_sql` therefore becomes a list.
3. **A handle is decorative.** Nothing validates `source_handle` against the upstream component's
   declared output ports today, because nothing reads it. Once it selects a relation, a typo in a
   handle has to be an error naming the port, not a silent read of the wrong branch.

**Do.**

- `PortSpec::rejected()`, and `qa.*` defaulting to outputs `[main, rejected]`.
- `Input::relation()` — the relation an input reads, which is the upstream node id for `main` and
  a suffixed name for `rejected`. `exactly_one_input` and `exactly_two_inputs` are the only two
  places that resolve an upstream relation, so routing every existing component through it is a
  two-line change.
- `EngineError::UnknownPort`, naming the port and listing the ones that exist.
- The reject relation name is reserved: a node id that collides with one is an error, not a
  silent overwrite.
- Seven validators, each lowering to the same shape — a base relation, a predicate, and an
  optional list of helper columns to project away:

  | Component | Predicate |
  |---|---|
  | `qa.not_null` | the named columns are all non-null |
  | `qa.unique` | the row's key occurs exactly once |
  | `qa.range` | a numeric column sits within `min`/`max` |
  | `qa.regex` | a text column matches a pattern |
  | `qa.accepted_values` | a column's value is in a listed set |
  | `qa.expression` | an arbitrary boolean SQL predicate — the escape hatch |
  | `qa.referential` | the key exists in a second input |

- **The split is exact, and that is the property to test.** Accepted is
  `WHERE coalesce(<pred>, false)` and rejected is `WHERE NOT coalesce(<pred>, false)`. Both sides
  read the same expression, so a row where the predicate is NULL — an unknown, not a pass — is
  rejected rather than lost. Every input row lands on exactly one side; a test asserts
  accepted + rejected = input for every validator.

**Verify.** A pipeline with a failing check routes bad rows to a reject sink and good rows onward
in one pass, and reports both counts against the right nodes. Accepted + rejected = input for all
seven. A handle naming a port that does not exist fails validation by name. The gate stays green.

**Done.** Seven `qa.*` components registered and tested; row counts still attributed correctly
across a plan that mixes quality nodes with ordinary ones.

**Deliberately not in 6a.** `qa.row_count` and `qa.schema_match` are assertions about a whole
relation, not partitions of it: they have no reject rows and their only outcome is to fail the
run. That is `ctl.fail`'s shape, and it needs 6b's execution-model decision. Recorded here rather
than dropped, as Phase 4 did with XML and DuckLake.

---

#### Phase 6b — Control flow and per-stage policy

**Starts with a decision, not with code.** `ctl.*` and per-stage retry both require a stage to be
runnable on its own, which the one-script model forbids: temp views live in a session, and every
`-c` is a fresh process. Write the options up before building — a persistent connection
(stdin-driven CLI, or linking `duckdb-rs` and dropping the subprocess), re-running scripts per
iteration, or splitting a plan into script segments at control boundaries — and record which one
was chosen and why. The risks section already flags keeping the invocation behind one function so
an embedded engine is an option rather than a rewrite; this is the phase that cashes that in.

**Then.** Control flow: foreach, if/branch, wait, throttle, sequence, run-pipeline, fail, log.
Per-stage `retry_attempts`, `retry_backoff_ms`, `continue_on_failure`, `memory_limit_mb` in
`src/policy.rs`. Plus `qa.row_count` and `qa.schema_match`, deferred from 6a.

**Verify.** `continue_on_failure` lets downstream stages run while the run still ends failed;
retry backs off and does not retry on cancellation; a foreach over three values runs its body
three times.

**Done.** Both namespaces registered and tested.

**Amended 2026-09-16, on completion.** The decision was made and recorded in
[DECISION_execution_model.md](DECISION_execution_model.md): **option A, a persistent session,
with the dual path** — a plan takes the session only when it holds a control node or a stage
policy, and every other plan keeps the one-script transport it was built against. The measured
case for it is in that file.

Built: `crates/duckdb-engine/src/session.rs`, the driven path in `exec.rs`, `StagePolicy` on
every stage, five control components (`ctl.wait`, `ctl.log`, `ctl.fail`, `ctl.branch`,
`ctl.sequence`) and the two assertions deferred out of 6a (`qa.row_count`, `qa.schema_match`).
Per-stage `retry_attempts`, `retry_backoff_ms`, `continue_on_failure` and `memory_limit_mb` all
work. Policy lives on `NodeData` beside `materialize` rather than in a `src/policy.rs`: it is
four fields resolved once, and a module for it would have been a file holding a struct.

**Three of the listed components were not built, and are deferred rather than dropped:**

- **`ctl.foreach`.** Everything else here is one stage deciding something about itself. A foreach
  is a stage deciding about *other* stages: it needs its body identified as a subgraph, that
  subgraph re-executed per binding, and the relations inside it named per iteration so the runs
  do not overwrite one another. That is a planner change, not an executor one, and it wants its
  own phase. The session it needs now exists, which was the hard part.
- **`ctl.run_pipeline`.** A nested document, loaded and compiled at run time, with its own
  parameters and its own session. Raises questions this phase did not settle: recursion depth,
  whether the child shares the parent's session, and how a child's failure reads in the parent's
  report. Design work, not typing.
- **`ctl.throttle`.** A rate limiter needs a rate to limit, and every stage here is one statement
  rather than a stream of rows. It would be `ctl.wait` with extra steps until there is a row
  cursor to throttle, which arrives with Phase 10's connectors.

**One thing found while building, worth not rediscovering:** the stderr grace period must never be
waited on the way through. A `CREATE VIEW` returns no rows whether it worked or not, so pausing
on "no rows arrived" put 250 ms on *every* stage — a seven-stage sample took 2.0 s instead of
0.18 s. The verdict comes from the count probes; stderr is asked for the message only once
something is already known to have failed.

### Phase 7 — Desktop app: Tauri 2 + React 19 + xyflow canvas

**Goal.** Build, run, and inspect a pipeline entirely in the GUI. **Multi-sitting — split at
the sub-bullets.**

**Files.** `apps/desktop/`, `frontend/`.

**Do.**
- 7a. Tauri 2 shell + Vite 8 + React 19 + TypeScript 7; IPC commands `compile`, `run`,
  `validate`, `list_components`, `preview`.
- 7b. `@xyflow/react` canvas: component palette from the Phase 3 manifest, drag-drop, edge
  wiring with port validation, save/load to the same JSON.
- 7c. Manifest-driven property panel — forms generated from the property schema, with no
  per-component React.
- 7d. Run view: per-node row counts, timings, live data preview, and a **Plan tab showing the
  generated SQL** (Prism-highlighted).

**Stack note.** No state library — React state and context, as Duckle does. Add i18next, Vega
for preview charts, lucide-react for icons.

**Verify.** Build the Phase 2 sample from scratch in the GUI, run it, see row counts and SQL;
the saved JSON round-trips through the CLI unchanged.

**Done.** GUI and CLI are interchangeable on the same file.

### Phase 8 — Headless runner: serve, scheduler, RBAC, incremental

**Goal.** Production execution without the desktop app.

**Files.** `crates/runner/`, `crates/scheduler/`.

**Do.** `etl-runner run|validate|serve`. Schedules (cron with timezone, interval, file-watch),
watermark incremental loading with state that advances only on full success, a web console with
shared-token auth and roles, run history and audit trail, structured run logs, lineage JSON.

**Verify.** A watermarked load run twice loads only new rows; a failed run does not advance the
watermark; the console lists runs and enforces roles.

**Done.** Runner executes the sample on a schedule with history.

### Phase 9 — Standalone binary export + air-gapped packaging

**Goal.** "Build Pipeline" produces one self-contained executable, cross-OS.

**Files.** `crates/runner/src/build.rs`, `packaging/`, CI matrix.

**Do.** Embed resolved pipeline JSON via a build script; bundle runner + DuckDB CLI + required
extensions; Target OS selector; `cargo-zigbuild`/`cross`; **a LOAD-only prelude that rejects
raw `INSTALL` at every entry point**; pin the DuckDB version against extension-ABI drift.

**Verify.** A Linux binary cross-built on Windows runs the sample with no network and no runtime
dependencies.

**Done.** Cross-build matrix green in CI.

### Phase 10 — Rust-native connectors (beyond DuckDB's reach)

**Goal.** The `connectors` crate earns its name. **Ongoing — one family per sitting.**

**Files.** `crates/connectors/`, `crates/plugin-sdk/`.

**Do.** `Connector` and `Transform` traits in the plugin SDK first, then families: streaming
(Kafka, NATS, Pub/Sub, RabbitMQ, Kinesis) as **bounded micro-batch reads with a watermark —
documented as such, never implied to be continuous streaming**; SaaS REST/GraphQL; NoSQL
(Mongo, Cassandra, Elastic, DynamoDB); warehouses over their own protocols (Databricks,
Snowflake); vector DBs.

**Verify.** Per family: an integration test against a container or a recorded fixture, plus a
documented statement of delivery semantics.

**Done (per family).** Registered, tested, semantics documented.

### Phase 11 — AI assistant + MCP server

**Goal.** A local model writes valid pipeline JSON; external agents drive the studio.

**Files.** `crates/mcp/`, `crates/assistant/`, frontend chat panel.

**Do.** A llama.cpp `llama-server` subprocess on a localhost OpenAI-compatible API with a small
local coding model. **GBNF grammar-constrained decoding against the Phase 3 manifest** — this is
what makes a 1.5B model reliably emit valid pipeline JSON. Six `xf.ai.*` transforms (three fully
local: embeddings, chunk, PII redact; three bring-your-own-endpoint via `baseUrl`). An MCP
server exposing list-components, get-schema, create/validate/run pipeline, read logs, build
executable, manage connections.

**Verify.** "read this Postgres table, dedupe, write Parquet" produces JSON that passes
`validate` on the first try in 9 of 10 runs; Claude Code drives a run over MCP.

**Done.** Both surfaces work against the same manifest.

### Phase 12 — Benchmarks + parity audit

**Goal.** Prove it, and know exactly where we stand against Duckle.

**Files.** `benchmarks/`, `docs/parity.md`.

**Do.** A harness that **verifies row counts and checksums before reporting a time** (Duckle's
discipline — copy it). Headline benchmark: TPC-H lineitem Postgres → Parquet. A parity audit
enumerating Duckle's ~417 components against ours.

**Done.** Benchmarks in CI; parity table published.

---

## Standing gates

Run before calling any phase done:

```powershell
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Phase 7 onward add:

```powershell
npm --prefix frontend run build
npm --prefix frontend run typecheck
```

## Risks

- **DuckDB CLI dependency.** Correctness and performance ride on an external binary, with
  process-spawn overhead per call. Pin the version, document the floor, and keep the invocation
  behind one function so an embedded `duckdb-rs` engine is a later option rather than a rewrite.
- **Extension ABI drift.** Vendor exact `.duckdb_extension` files per platform; never `INSTALL`
  at runtime.
- **Connector breadth is the schedule.** Phases 0–3 are roughly six weeks of real work; Phases 4
  and 10 are the long tail. Phase 3's registry decides whether that tail is tractable.
- **Windows-first.** Path escaping, `\\?\` long paths, and CRLF will bite in Phase 2 escaping and
  Phase 9 cross-builds. Test on Windows from the start.
