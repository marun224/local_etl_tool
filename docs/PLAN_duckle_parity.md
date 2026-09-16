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

**7a done 2026-09-16.** Versions were checked rather than assumed, and every one the plan named
is available stable: Tauri 2.11.5, React 19.3, Vite 8.3, TypeScript 7.0.2, `@xyflow/react`
12.11.6. (crates.io offers `tauri` 3.0.0-alpha.1 as latest — **not** that.)

Built: `apps/desktop/` holding the five commands and nothing else, `frontend/` with a typed
`src/ipc.ts` mirroring the wire structs, and `preview` added to the engine. `apps/desktop` is a
workspace member, and the Rust gate does **not** depend on the frontend being built — checked by
deleting `frontend/dist` and rebuilding, because a `cargo test` that needs `npm install` first
would be a bad trade for a crate nobody's test touches.

Three decisions worth keeping:

- **The desktop crate holds no logic.** No SQL, no DuckDB, no component list — just the five
  commands over the same engine the CLI calls. The GUI and CLI can only stay interchangeable if
  they are two thin callers rather than two implementations that agree today.
- **`validate` resolves rather than rejects for an invalid document.** A pipeline under
  construction is invalid most of the time and the canvas re-validates on every edit; throwing
  would make every keystroke between two valid states an exception to catch.
- **`preview` drops sinks from the stages it runs.** Looking at what a node holds is a read, and
  a read that overwrote someone's output file while they clicked around would be a trap.

The IPC surface is tested without a window: `#[tauri::command]` leaves each function callable as
an ordinary one, so all five are exercised against the real engine in `cargo test`. Nine tests.
A GUI-only check would mean the contract 7b is written against is only verified by clicking.

The app was launched and confirmed running (window process alive, WebView2 children spawned, no
runtime errors) — not only compiled.

**Not yet done in 7a:** i18next, Vega and lucide-react are listed in the stack note and are not
installed. Each belongs with the thing that needs it — icons with the palette in 7b, charts with
the preview in 7d — and installing them now would be three unused dependencies.

**7b done 2026-09-16.** `@xyflow/react` 12.11.6 and lucide-react 1.46.0 added; vitest 5 added
with it, because the wiring rules are logic and deserved tests rather than clicking.

`frontend/src/document.ts` owns the document and the wiring rules; `PipelineCanvas` draws it and
turns xyflow's events back into edits, holding no pipeline state of its own. One copy of the
document, in `App`, is what makes the round-trip promise structural rather than remembered.

**The round-trip promise needed correcting, and the correction is the finding.** "The saved JSON
round-trips through the CLI unchanged" cannot mean byte-identical: `JSON.stringify` always
expands arrays onto their own lines, so a hand-written `"values": ["a", "b"]` comes back
reformatted no matter how carefully the data is preserved. Byte-identity would mean writing a
format-preserving JSON editor, which is not worth it. What holds, and is tested against all five
committed samples, is that **nothing is lost or altered** — every key, every value, every
ordering, including fields this version does not understand — and that formatting normalises
once on first save and never moves again. So opening and saving a canvas-written file produces
no diff.

Two further things the tests pin, both against the real samples rather than fixtures:

- **The canvas and the engine agree about what is valid.** Every edge in every committed sample
  is one the canvas would have allowed. A canvas that refuses something the engine accepts is as
  wrong as the reverse, just less obviously — so the check is that the two agree, not merely that
  each is self-consistent.
- **Node ids are readable and never reserved.** An id becomes the relation name in the generated
  SQL, which a person reads on the Plan tab, so `src.file.csv` becomes `csv` and then `csv_2`.
  The `__rejected` suffix can never be produced, because the engine refuses a document using it.

**Bundle:** lucide's barrel import pulled in all ~1500 of its icons and cost 600 KB. The 42 the
registry actually uses are now imported by name in `src/icons.ts`, with a generic box for
anything new, so a component with an unknown icon degrades rather than breaks. 1031 KB → 430 KB.
That file carries the command to regenerate its list.

**Not in 7b:** the property panel is 7c's — the inspector shows a node's values read-only until
then — and the run view is 7d's. i18next and Vega remain uninstalled.

**7c done 2026-09-16.** `Inspector.tsx` holds nine controls, one per `PropertyType`, and no
knowledge of any component: a spec it has never seen gets a working form. The node's own fields —
name, label, alias, materialize, disabled — sit above the properties, because they are how a node
is *run* rather than what its component does and every component has them.

Two rules run through it, both now tested:

- **Absent and empty are different.** A property the document does not hold takes the spec's
  default; one set to `""` is an empty string the engine complains about by name. So clearing a
  field *removes the key* rather than writing a blank, which is what makes "leave it unset and
  the default applies" work as written. The panel also marks which values are only there because
  the spec says so — a defaulted `true` reads identically to a chosen one, and the difference
  matters when working out what a run did.
- **A typo is not a value.** `fromText` returns `undefined` rather than `NaN` for a number that
  will not parse, because `NaN` serialises to `null` and would turn a slip into something the
  engine has to interpret. An integer field refuses `1.5` instead of rounding it: silently
  changing a number someone typed is worse than ignoring it.

Renaming a node moves its edges with it, since the id is the relation name in the generated SQL
and what every edge refers to. It refuses a duplicate, a blank, and the reserved `__rejected`
suffix, and says which — committed on blur rather than per keystroke, so a half-typed name is not
checked against a collision.

**Clicking a palette entry adds the node too.** Dragging is the obvious gesture but must not be
the only one: it is unreachable from a keyboard, and a webview will not always start an HTML5
drag — which is exactly what happened when this was driven with synthetic mouse input, and is a
fair proxy for the people who will hit it for other reasons.

**Verified by rendering, not by screenshot.** 25 tests hand the panel a made-up component using
all nine property types and check what comes out: the control chosen for each type, what editing
writes, what is marked required, what is marked default. A screenshot would show one component's
form on one day; these keep showing that the mapping holds. jsdom and Testing Library arrive with
them.

**Not in 7c:** per-stage policy (`retryAttempts` and the rest) has no panel yet — it is 6b's
feature and belongs with the run view in 7d, where a retry is something you watch happen.

**7d done 2026-09-16. Phase 7 is complete.** Per-node row counts, timings, the data preview, the
Plan tab and the policy panel. `RunView.tsx` holds the three tab bodies; `App.tsx` keeps only the
state they read.

Four decisions, the first of which is the phase's real content:

- **A timing is published only where it means what it looks like.** The obvious reading of "per-node
  timings" is a duration on every node, and it would be wrong: in a plan of lazy views every
  transform takes microseconds to declare and the sink takes the whole pipeline's work. `0 ms`
  beside the transform that cost the most is worse than a blank, so `StageOutcome::elapsed` is
  `Option<Duration>` and is `Some` only when **both** hold — the stage was sent on its own (the
  driven path; the one-script path has no boundary to measure) and the stage did its work when it
  ran (`Stage::work_happens_here`: a sink, a control node, or a `memory`/`disk` materialisation).
  Most stages on most runs therefore report nothing, and the Status tab explains why once rather
  than per row. This was raised as an open question before starting and settled this way rather
  than by routing every run through the session, which would reopen
  [DECISION_execution_model.md](DECISION_execution_model.md) for a cosmetic gain.

  A consequence worth knowing: the clock starts *before* a control node acts, because a `ctl.wait`
  does all its work in `act()`. Timed from below that, a 250 ms wait reported 26 ms — caught by the
  test that asserts a wait cannot come back early.

- **The SQL highlighter is hand-written, not Prism.** This deviates from the plan text above, on
  purpose. Prism returns a string of HTML, which React renders through
  `dangerouslySetInnerHTML` — and the strings involved are not ours alone: a file path, a raw
  `xf.sql` body and a node id all reach the panel inside the generated SQL, so a pipeline document
  would be choosing what HTML this window renders, in a webview holding IPC commands that read and
  write files. `sql-highlight.tsx` tokenises to React elements instead, which makes the hole
  structurally impossible because React escapes text children. ~150 lines, one dialect (ours), and
  a test that every character survives tokenising — including a path with a `<img onerror=...>` in
  it. One fewer dependency, and Vega and i18next stay uninstalled.

- **The Plan tab shows SQL per stage, not as one script.** The question it answers is "what is this
  node doing"; a single blob makes the reader find the node themselves. The whole script is one
  toggle away. The tab also names the **transport** and what asked for it, which is invisible in
  the SQL and is the reason a retry or a branch is possible at all.

- **A policy that is empty is removed.** `setPolicyField` deletes a cleared knob rather than writing
  a zero, and drops `policy` entirely once nothing is left in it — the same rule 7c set for
  properties, for the same reason. The panel carries a `session` badge and a sentence saying that
  any of these four moves the whole pipeline onto the session transport, because finding that out
  from the Plan tab afterwards is finding out too late.

**Verified.** 339 Rust tests (247 engine, 47 e2e, 20 secrets, 15 metadata, 10 desktop — up from
335 because of three timing tests and one IPC mapping test) and 111 frontend tests (up from 80).
`cargo fmt --check` and `cargo clippy --workspace --all-targets` are clean. The app was launched
and the window confirmed responding, not merely compiled.

### Phase 8 — Headless runner: serve, scheduler, RBAC, incremental

**Goal.** Production execution without the desktop app.

**Files.** `crates/runner/`, `crates/scheduler/`.

**Do.** `etl-runner run|validate|serve`. Schedules (cron with timezone, interval, file-watch),
watermark incremental loading with state that advances only on full success, a web console with
shared-token auth and roles, run history and audit trail, structured run logs, lineage JSON.

**Verify.** A watermarked load run twice loads only new rows; a failed run does not advance the
watermark; the console lists runs and enforces roles.

**Done.** Runner executes the sample on a schedule with history.

**Split into 8a–8d on 2026-09-16, before starting.** The phase as written is four features that
happen to share a crate, and three of them need a supply-chain decision this project has always
taken deliberately. Ordered so that each one has something real underneath it:

- **8a — Watermark incremental loading.** The correctness core, and the one slice that needs no
  new dependency: a state store, an `incremental` block on a source node, and the rule that
  state advances only on a run that fully succeeded. Everything else in the phase is plumbing
  around this.
- **8b — The runner: `run`/`validate`, run history, structured logs, lineage JSON.** Needs 8a's
  state store to have a place to write run records. One decision: a separate `etl-runner`
  binary as the plan names, or subcommands on the existing `etl`.
- **8c — Scheduler: interval, cron with timezone, file-watch.** Needs 8b's run records to have a
  history to append to. Two dependencies: cron parsing with a timezone, and filesystem watching.
- **8d — Web console: `serve`, shared-token auth, roles.** Last, because it is a view over what
  8b and 8c produce. One dependency: an HTTP server.

**Settled 2026-09-16 — the dependency posture and the runner's shape.** 8b is subcommands on
`etl` rather than a second binary; schedules are UTC and interval only, with a `tz` field
refused rather than approximated; file-watch polls `mtime`; the 8d console uses `tiny_http`.
Reasons in the tracker's Settled decisions 5–8. Net new dependencies for the whole phase: one.

**Originally open, kept for the reasoning — the dependency posture.** This workspace has four external crates
(`serde`, `serde_json`, `thiserror`, `clap`) plus RustCrypto, and has hand-rolled a topological
sort and a civil-date conversion rather than take `petgraph` or a date crate. Phase 8 asks for
three things where that stance has a real cost: **cron with timezone** (a timezone database is
not something to hand-roll — it changes several times a year and being wrong is silent),
**file-watching** (platform-specific; `notify` is the only sane answer), and an **HTTP server**
for the console. These are recorded as an open decision in the tracker rather than picked in a
diff. 8a and 8b need none of them and go first regardless.

**Note.** `PipelineDoc::resource_pool` already exists and is read by nothing — its doc comment
says it is for "the limits a scheduled run observes". Admission pools are not in this phase's
**Do** list, so 8c will schedule without them and the field stays unused until a phase claims
it. Worth knowing before someone assumes it works.

**8a done 2026-09-16.** Watermark incremental loading, end to end: an `incremental` block on a
source node, a predicate the compiler adds from it, a probe that reads the new mark, and state
that advances only on a run that fully succeeded.

Where each piece went, and why there:

- **`incremental` sits on the node beside `materialize` and `policy`**, not in a component's
  property schema. It is how a node is *run* rather than what its component does, and it reads
  the same on all twelve sources; in the schema it would be twelve copies of one idea, and a
  thirteenth source would silently not have it.
- **The predicate is applied in `create_view`**, the one place every relation-producing builder
  already ends. Twelve sources build their bodies twelve ways and only some have somewhere to
  put a `WHERE`, so the filter wraps the body instead: `SELECT * FROM (<body>) WHERE col > mark`.
  DuckDB pushes it back down into the scan for the formats that support it.
- **Strictly greater than, never `>=`.** The watermark is a value already loaded; re-reading it
  would duplicate every row sharing that timestamp. The mirror-image risk — a row written later
  with a timestamp at or below the mark is never seen — is inherent to watermarking, and is why
  the column has to be one that only goes up. Said in the doc comment rather than pretended away.
- **The probe rides the existing count stream.** A count probe emits `n`; a watermark probe emits
  `w`; each parser ignores what it does not recognise. That is why this needed no change to count
  attribution, which is delicate enough that threading a second value through it would have been
  the risky way to do it.
- **The probe is emitted with its source, not at the end of the script.** It costs the same — the
  relation is scanned either way — and buys the thing that matters: a watermark column that does
  not exist fails before any sink has written, rather than after.
- **`compile_with` rather than a changed `compile`.** `compile` still compiles as though nothing
  has ever run, which is what `validate` and the canvas want; only a real run reads a watermark.
- **A changed column starts over.** If the stored mark came from a different column than the
  document now names, it is discarded with a warning rather than compared across columns. That
  reloads data, which is the safe direction.
- **`incremental` outside a source is dropped with a warning.** On a transform the predicate
  would compile and quietly filter a second time — the kind of thing that looks like it works.

**Verify.** Automated as the phase's own acceptance line:
`a_watermarked_load_run_twice_reads_only_what_is_new` grows a real CSV between runs and checks
3 → 0 → 2 rows and the marks either side; `a_failed_run_hands_back_no_state_to_save` checks that
a bad watermark column fails before the sink writes. 361 Rust tests, 114 frontend, fmt and clippy
clean. `samples/pipelines/orders_incremental.json` is the worked example, and `etl state
list|forget` is the surface for looking at and resetting what is remembered.

**Not in 8a:** the canvas cannot yet *edit* an `incremental` block — it survives a GUI round trip
untouched, but there is no panel for it. It belongs with a Phase 8 that has a runner to schedule,
and is noted in the tracker rather than left to be discovered.

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
