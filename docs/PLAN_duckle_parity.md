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

**Goal.** The `qa.*` (29) and `ctl.*` (21) namespaces.

**Files.** `specs.rs`, `builders.rs`, `src/plan/mod.rs` (multi-output edges), `src/policy.rs`.

**Do.** Validators (not_null, unique, range, regex, referential, row_count, schema_match, …)
each with a second **reject** output port for dead-letter rows. Control flow: foreach,
if/branch, wait, throttle, sequence, run-pipeline, fail, log. Per-stage `retry_attempts`,
`retry_backoff_ms`, `continue_on_failure`, `memory_limit_mb`.

**Verify.** A pipeline with a failing quality check routes bad rows to the reject sink and good
rows onward in one pass; `continue_on_failure` lets downstream stages run while the run still
ends failed; retry backs off and does not retry on cancellation.

**Done.** Both namespaces registered and tested.

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
