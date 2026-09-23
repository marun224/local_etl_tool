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

**Superseded 2026-09-23:** the grace period is gone. CI's first Linux run showed that "whatever
stderr holds by now" is a race even with it, since a late message was attributed to the next
statement. stderr is now framed per statement with an `error()` marker, the way stdout is
framed, so the message arrives with the answer and nothing waits. See `session.rs`'s module
docs, and the tracker's *From Phase 9d, once CI actually ran*.

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

**8b done 2026-09-16.** The runner, as Settled decision 5 shaped it: subcommands on `etl`, not
a second binary. `etl run --json`, run history under `.etl/runs/`, `etl runs list|show|prune`,
and `etl lineage`.

- **The structured log and the history are one record.** What `--json` prints is byte-for-byte
  what gets appended to history, so a CI job parsing stdout and a person running `etl runs show`
  are reading the same thing rather than two renderings that drift. `--json` is the *whole* of
  stdout when asked for, so nothing has to be stripped off the front.
- **A run is recorded whether it succeeded or not**, including one that failed before producing
  a report at all. History that only remembers successes cannot answer the question anybody
  actually has. The one exception is a missing DuckDB binary, which is a broken installation
  rather than a failed pipeline.
- **History is append-only and never pruned behind your back.** One JSON object per line, opened
  for append, so a crash can cost the record being written and nothing before it. `etl runs
  prune` exists and only a person runs it. A record is a few hundred bytes; silently discarding
  the history of a pipeline that turns out to have been wrong for a month is a worse failure
  than a large file.
- **A corrupt history line is skipped; a corrupt watermark is fatal.** Opposite calls, on
  purpose. A bad watermark silently changes what the next run *loads*, so it has to stop
  everything. A bad history line costs one record of hindsight, and refusing to show the other
  nine hundred over it would be the wrong trade.
- **Failing to record does not fail the run.** The exit code belongs to the pipeline, not to the
  bookkeeping — again the opposite of watermark state, where failing to save *is* a failure
  because the next run would silently reload from the old mark.
- **Lineage is derived from the plan, so it needs no run.** It can go in review beside the diff
  rather than being learned after a pipeline wrote somewhere unexpected. It is **node-level, not
  column-level**, and says so: column lineage needs schemas this engine does not collect, and
  `columns` is absent rather than `[]` so a consumer cannot read "not collected" as "none".
- **Lineage never carries a connection string.** A database source contributes `schema.table`;
  the connection string is the one property most likely to hold a password, and lineage is the
  output most likely to be pasted into a ticket. Output is redacted the same way the script is.
  Two tests assert a password cannot appear.

**Verify.** 378 Rust tests (254 engine lib incl. 7 lineage, 51 e2e, 28 state incl. 10 history,
20 secrets, 15 metadata, 10 desktop), 114 frontend, fmt and clippy clean. Exercised by hand: a
failing pipeline lands in history as `failed`; `etl run --json` parses as a single JSON document;
`etl runs list|show` read it back.

**Not in 8b:** no `serve`, no scheduling — those are 8c and 8d. The desktop app does not read
history yet; it has no panel for it and none is planned before the phase is done.

**8c done 2026-09-16.** The scheduler: interval, UTC cron and file-watch, in a new
`crates/scheduler/`, driven by `etl schedule list|check|start`. Settled decisions 6 and 7 held
up — no timezone database and no `notify` — so the phase added **no external dependency at all**.

Three questions were settled before any code was written, because each one forks the design:

- **A schedule lives in its own file**, `.etl/schedules.json`, with `--schedules` to point
  somewhere else and `samples/schedules.json` as the committed example. Not in the pipeline
  document: a schedule is a property of *this workspace*, and the same pipeline is a
  five-minute job on a laptop and a nightly one in production. Baking one cadence into the
  document would force those to be two documents. It is the arrangement contexts already use,
  for the same reason and with the same `.etl/`-is-git-ignored consequence.
- **A run that overruns its tick skips to now and counts what it missed**, rather than working
  through a backlog. For a watermarked pipeline each run already reads everything new since the
  last mark, so five catch-up runs do exactly what one does — and a backlog that grows without
  bound is a worse failure than a gap somebody can see.
- **A workspace lock, and runs one at a time.** `crates/state` says out loud that it has no
  locking and that single-writer is the assumption "until a scheduler exists to break it". 8c
  is that scheduler, and it does not break it: sequential execution means its own runs cannot
  overlap, and the lock means there is only ever one scheduler. The assumption is kept *true*
  rather than made safe to violate. A hand-run `etl run` alongside a scheduler is still
  unguarded, deliberately — a lock a person running one command has to wait on would make the
  common case worse to protect against an uncommon one.

Where each piece went, and what it turned out to cost:

- **The scheduler crate does not depend on the engine.** It answers "what is due, and when
  should I wake", and is handed a closure that runs one pipeline. That is what lets a fake
  clock drive a day of scheduling in microseconds with no DuckDB anywhere near it — 113 tests,
  none of which sleep. Execution stays in the CLI, which already knew how to do it.
- **`command_run` was split into `perform` plus its printer.** The scheduler goes through the
  same `perform`, so a scheduled run is compiled, recorded in history and advances its
  watermarks by exactly the code a hand-run one uses. This is Settled decision 5's reasoning
  applied one level down: two paths that must agree about the same file forever eventually do
  not.
- **An interval is counted from the last recorded run**, read out of 8b's `.etl/runs/`. That is
  why 8c depends on 8b rather than merely following it: restarting the scheduler must not
  restart the clock, and an hourly pipeline that ran at 02:00 is due at 03:00 whether or not
  anything was up in between.
- **A pipeline that is overdue runs at once, on the next tick's grid afterwards.** Down for
  three hours on an hourly schedule means run now, not wait fifty minutes for the next whole
  hour. The next tick is then anchored on the *due* time rather than the finish, so ticks do
  not drift later by however long each run takes — pinned by a test that runs ten hours of
  schedule and checks every fire is still on the hour.
- **The civil-date conversion moved to `etl_state::time`.** It had been written twice already
  (the engine's `${date}` and the state crate's timestamps); the scheduler needed a third, plus
  the inverse and a weekday. The state crate's copy was promoted and the scheduler reads it.
  The engine's copy is still its own — it does not depend on `etl-state`, and making it do so
  is a change to the engine rather than to the scheduler, so it is left as a known duplicate
  rather than smuggled into this phase.

**Three things were found by running it rather than by reasoning about it:**

- **Directory mtimes are not a reliable signal for nested content.** The first watch tests
  asserted that a file created one level down fires and an edit does not; both failed, in
  opposite directions. NTFS defers directory timestamp updates, so neither direction is
  guaranteed. The top level is solid for a reason that has nothing to do with the directory's
  clock — a new file is an entry with its own fresh mtime, and a removed one changes the summed
  length — so the contract is now "immediate entries, and watch the directory whose files
  matter", with the measurement written into the module docs.
- **"Missed ticks" conflated two different things.** The first real run printed *34 tick(s)
  were missed while an earlier run was still going* when nothing had been running — it was
  counting the eight hours since the last run. Being **behind** because the scheduler was down
  and **missing** a tick because a run overran are different problems that send you looking in
  different places, so they are now counted separately: `Tick::missed` is measured from when
  the run started, and `Entry::behind` is said once in the startup banner.
- **The lock had to be a held handle, not a file that exists.** Ctrl-C is how a foreground
  scheduler is stopped, so an existence-check lock would be left behind on almost every stop
  and every restart would need `--force` — a guard people learn to bypass by reflex. On Windows
  the file is held with `share_mode(0)`, which the OS releases however the process dies;
  verified by killing a scheduler mid-sleep and starting another. Elsewhere std offers no
  equivalent without a dependency, so it falls back to an exclusive create and `--force`, and
  the error message differs per platform because the situations genuinely do.

**Not in 8c, and why:** admission pools (`PipelineDoc::resource_pool` is still read by nothing,
as the note below has said since the phase was split); catching up on missed ticks; a canvas
panel for schedules; and any notion of local time. Ctrl-C ends the process without unwinding,
so a run in progress is killed — which is safe by construction, because a watermark advances
only on a run that fully succeeded, and the next run redoes the window.

**Verify.** `etl schedule list|check` against `samples/schedules.json`; `etl schedule start
--once` ran the overdue incremental pipeline and re-anchored it; a watch on a directory with a
one-second poll fired once when a file landed and settled; a second `schedule start` was
refused by name and pid while the first held the workspace, and succeeded once it had been
killed. 508 Rust tests (254 engine, 113 scheduler, 51 e2e, 45 state, 20 secrets, 15 metadata,
10 desktop) and 114 frontend, fmt and clippy clean.

**8d done 2026-09-16. Phase 8 is complete.** The web console: `etl serve`, two roles, and a
page. Settled decision 8 held — `tiny_http`, and five crates arrive with it (`tiny_http`,
`ascii`, `chunked_transfer`, `httpdate`, `log`). That is the whole of Phase 8's dependency
budget, as the split predicted when it said "net new dependencies for the whole phase: one".

**Four layers, each testable without the one below it.** `auth` (roles and tokens), `routes`
(every route as a pure function from a method, a path, a query and a token to a status and a
body), `ui` (the page, as one string), `server` (the only part that knows what a socket is).
Sixty-five tests, none of which bind a port: a route is a function, so being refused is as
testable as being served.

`Workspace` is the seam, the third time this shape has been used — the console does not depend
on the engine and cannot compile SQL, exactly as the scheduler does not. The CLI implements it,
where the engine and the resolver already live.

**Security, because this is the first code in the project that parses untrusted input off a
socket:**

- **Loopback by default**, and binding anything else prints a warning that says what the actual
  exposure is: no TLS, so the tokens cross the network in clear.
- **Tokens are minted per process and printed once**, the way a local notebook server does.
  Nothing is stored, so there is no token file to leak and a stopped console cannot be reached
  with yesterday's link. A stable token comes from the **environment**, never a flag — an
  argument is visible in the process list, which is the call this project already made for `etl
  secret set`. A token that came from the environment is deliberately **not** printed: doing so
  would put somebody's standing secret in the scrollback and the CI log of every run.
- **Constant-time comparison, and both tokens are always compared.** Returning as soon as one
  matched would make the operator check measurably faster than the viewer one.
- **Both roles set to the same token is refused at startup**, because it silently promotes every
  viewer to an operator — the one mistake in that file that looks like it is working.
- **A `?token=` works on the page and nowhere else.** That is what stops a console link pasted
  into a chat from being a usable API credential, and stops another site's form from posting one
  for you. The page moves the token out of the address bar on load and sends a header from then
  on.
- **A pipeline name from the network is resolved by lookup, never joined onto a path.** A name
  that is not in the workspace's own list finds nothing, so `../../etc/passwd` is a 404 rather
  than a file read — verified against the running console, not only in a test.
- **Everything the page renders goes through `textContent`.** A pipeline named `<img onerror=…>`
  is a string, not markup, and a test asserts the page contains no `innerHTML`, `outerHTML`,
  `insertAdjacentHTML` or `document.write` so it stays that way.
- **Every response is hardened, including the refusals**: a content security policy of
  `default-src 'none'` with `frame-ancestors` and `form-action` also `'none'`, `nosniff`,
  `no-referrer`, and `no-store` because the page URL carries a token.
- **`limit` is capped** at a thousand, so `?limit=100000000` cannot serialise a year of history
  into memory to answer one request. Request bodies are bounded off the socket before routing.

**The role is stated in a header** — `X-Etl-Role`, on every authenticated response including the
refusals — so the page knows whether to draw a Run button. The first version had the page probe
with a request it expected to fail and read the role out of the error text, which works until
somebody rewords the error.

**Two roles, not three.** Viewer reads; operator reads and starts runs. A third role able to do
exactly what the second can is decoration rather than access control, and there is nothing else
in the console to gate: secrets are not exposed over HTTP at all, and editing a pipeline is the
canvas's job.

**Runs are serialised by the workspace, not by the transport.** Requests are served from four
threads so a twenty-second run does not stop everybody reading, and `Workspace::start` holds a
mutex for the length of a run so two people clicking Run cannot race on a watermark. Running
beside a *scheduler* is the same unguarded case a hand-run `etl run` is, and is documented in
the same place.

**Not in 8d, and why:** no TLS (a reverse proxy's job, and bundling one would mean certificates,
renewal and a config file); no accounts or per-user tokens (two shared tokens is the right
weight for a console somebody runs next to their work); no editing — the console reads and
starts runs, and changing a pipeline is the canvas; no live log streaming, because a run is
synchronous and the record it returns is the answer.

**Verify.** Against the running console rather than only in tests: health answers without a
token and says nothing else; every API route refuses without one; a token in the URL is refused
on the API for both a GET and a POST; a viewer starting a run gets 403 and the run does not
happen; an operator gets 200 and a record; a traversal attempt is a 404; the hardening headers
and `X-Etl-Role` are present on the wire. 576 Rust tests (65 console, 3 new in secrets) and 114
frontend, fmt and clippy clean.

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

#### Phase 10: split and design (questions answered 2026-09-23)

Decisions, recorded in the tracker as Settled decisions 9–15:

| # | Question | Answer |
|---|---|---|
| 9 | How rows from Rust reach DuckDB | **A staging file in JSON Lines**, read with `read_json`. No new dependency, and the same shape as a `disk` spill |
| 10 | Client dependencies | **Pure-Rust, blocking where possible.** No system C libraries. `tokio` only if a family cannot do without it, decided family by family |
| 11 | How the phase splits | **10a** the SDK and the staging bridge, proven by XML; **10b** SaaS REST; later families one sub-phase each |
| 12 | Which network family comes first | **SaaS REST** |
| 13 | Direction | **Sources and sinks both, from the start** |
| 14 | Do built artifacts run native connectors | **Yes.** `etl-runner` gets them through the engine, one code path |
| 15 | Verifying Phase 4's database and lake connectors | **Its own small phase after 10b**, called 10c |

**Transforms are not part of this.** The plan text above names a `Transform` trait. It is
deferred, because every transform so far is SQL that DuckDB runs better than Rust would, and
a trait with no implementation is the stub-crate mistake this plan warns against. It arrives
with the first transform DuckDB cannot express.

##### The bridge: how a native connector joins a plan

A native component is registered like any other, as a spec and a builder in `specs.rs`, so
the canvas palette, `etl components`, validation and lineage all get it for free. What
differs is that its builder also fills in `Stage::native`:

```rust
pub struct NativeStep {
    pub component_id: String,
    pub properties: JsonValue,  // resolved: parameters, contexts and secrets already applied
    pub staging: String,        // .etl/tmp/native/<node_id>.jsonl, relative like a spill path
    pub direction: Direction,   // Ingest (a source) or Egress (a sink)
}
```

- **A native source** runs *before* DuckDB starts. It writes its records to the staging file
  as JSON Lines, and its SQL is an ordinary view over that file:
  `CREATE OR REPLACE TEMP VIEW "<id>" AS (SELECT * FROM read_json('<staging>',
  format='newline_delimited', columns={...}))`. A source has no upstream, so it can always run
  first. That is what keeps **both transports and `preview` unchanged**: all three call one
  `stage_native_sources` before DuckDB, the way they already call `prepare_spills`.
- **A native sink** is a `COPY (SELECT * FROM "<from>") TO '<staging>' (FORMAT json)`, and the
  connector delivers the staging file *after* DuckDB finishes. It runs only if the run
  succeeded, so a failed pipeline never half-delivers. `preview` drops sinks as it already
  does.
- **Types.** JSON loses them, so a source declares its output columns, either fixed by the
  connector or from a `columns` map property (name to DuckDB type, the `xf.cast` shape), and
  the builder passes them as `read_json`'s `columns`. Unset means all `VARCHAR`, which is
  honest: XML and most REST payloads are text until somebody says otherwise.
- **Counts, incremental, materialisation and lineage need no change.** A native source *is* a
  view once staged, so the count probe, the incremental `WHERE col > mark` wrapper and
  `materialize` all apply as they do to `src.file.json`. `Stage::external` is the file path
  or URL, never a token.
- **Secrets.** Properties are resolved before compile like everyone else's. A connector's
  error text goes through the same `redact` as DuckDB's stderr, because an HTTP client quotes
  URLs and headers back just as `ATTACH` quotes connection strings.
- **Staging is scratch.** It is cleared after the run, best-effort and counted, as spills are.
  Its path is derived from the node id so `compile` stays pure, which carries the spill trade:
  two concurrent runs of one pipeline in one directory would share staging files. The
  scheduler already serialises runs, and a hand-run beside it is the known unguarded case.
- **The report** gains a line per native stage: records staged or delivered, and for a sink,
  how far it got if it failed partway.

##### Crates

- **`crates/plugin-sdk`** (`etl-plugin-sdk`): the traits, and nothing that does I/O.
  `Source::read(&self, props, &mut dyn RecordWriter, &Context) -> Result<Summary, ConnectorError>`
  and `Sink::write(&self, props, &mut dyn RecordReader, &Context) -> Result<Summary, ConnectorError>`.
  A record is a `serde_json::Map`, which is what JSON Lines holds. `Context` carries the working
  directory, the redaction list and a cancellation flag for later.
- **`crates/connectors`** (`etl-connectors`): the implementations, and `specs()` listing each
  one's `ComponentSpec`. It depends on `plugin-sdk` and `metadata`, **never on the engine**. The
  engine depends on it, so the CLI, the runner, the desktop app and the console all have it
  through the engine without a line of their own. That is decision 14.

##### Phase 10a — the SDK, the bridge, and XML

**Goal.** A Rust-native connector runs end to end in both directions, on both transports, in
`preview`, and inside a built artifact, proven by the XML reader Phase 4 deferred here.

**Files.** `crates/plugin-sdk/`, `crates/connectors/{lib.rs, xml.rs}`,
`crates/duckdb-engine/src/{plan/mod.rs, plan/specs.rs, plan/builders.rs, exec.rs, native.rs}`,
`samples/data/orders.xml`, `samples/pipelines/orders_xml.json`, `docs/connectors.md` (new:
delivery semantics per family), `docs/adding_a_component.md` (a native section).

**Do.**
- `src.file.xml`: `path`, `record` (the element that is one row, e.g. `order`), optional
  `columns`. A record's child elements and attributes become fields, attributes prefixed `@`.
  Nested elements beyond one level are refused by name rather than flattened by guesswork.
- `snk.file.xml`: `path`, `root` and `record` element names, `mode` (`overwrite` or
  `error_if_exists`, the same two sinks already take). Written to a temporary file and renamed
  into place, so a failed write never leaves half a document.
- Parser and writer: **`quick-xml`**, which is pure Rust with no C. The dependency that decision
  10 allows here.
- Wire the bridge into `run_one_script`, `run_driven` and `preview`, then delivery and cleanup.

**Verify.**
- Unit: XML parsing (attributes, empty elements, entities, CDATA, a deeper nesting refused) and
  writing (escaping round-trips).
- Golden SQL for both builders.
- End to end: `orders.xml` to Parquet on the one-script path; CSV to XML and back, byte-stable;
  the same pipeline with a `policy` so it takes the session path; `preview` of an XML source
  node writes nothing; a failing upstream means the XML sink never writes.
- A built artifact of the XML pipeline runs from outside the repo (CI's artifact job gains it).
- Mutation check on the "deliver only after success" rule.
- The gate: fmt, clippy, all tests, samples; 56 components.

**Done.** Both XML components registered and tested, the bridge on all three entry points,
and XML's delivery semantics written in `docs/connectors.md`.

**Amended 2026-09-23, on completion.** One thing in the design above was wrong, and running it
is what showed it: *"Unset means all `VARCHAR`"* is not what DuckDB does. Its JSON reader
recognises an ISO date or timestamp inside a string and types it, the way `src.file.csv` and
`src.file.json` already type their columns. The first probe missed this because its two
sample dates had different shapes, so inference gave up and left them as text. There is no
`all_varchar` for `read_json`, and an impossible `dateformat` is refused. So forcing text would
mean a hack, and the behaviour is consistent with every other source. Kept as DuckDB does it,
and documented: unset means DuckDB infers, and `columns` is how to be certain.

##### Phase 10b — SaaS REST

**Goal.** Read from and write to an HTTP JSON API with authentication, pagination, rate limits
and retries, with none of it running at 3 am untested.

**Files.** `crates/connectors/src/rest.rs` plus tests with a fixture server, `docs/connectors.md`,
a sample against a local fixture.

**Do.**
- `src.saas.rest`: `url`, `method`, `headers` (map), `query` (map), `auth` (`none`, `bearer`,
  `basic`, `header`) whose token takes `${SECRET:...}`, `records` (a JSON pointer to the array,
  e.g. `/data`), `pagination` (`none`, `page`, `offset`, `cursor`, `link`) with its own settings,
  `max_pages` (a cap, default 1000, hitting it is an error rather than a silent stop),
  `min_interval_ms`, `timeout_ms`, `columns`.
- `snk.saas.rest`: `url`, `method` (`POST`/`PUT`/`PATCH`), `headers`, `auth`, `batch_size`
  (1 sends one object per request, more sends an array), `wrap` (an optional key to nest the
  batch under).
- Retries on 429 and 5xx, with doubling backoff that honours `Retry-After`, and never on other 4xx.
- HTTP client: **`ureq`** (blocking, no async runtime) with `rustls` and bundled `webpki-roots`
  certificates, so an artifact carries its trust store the way it carries its engine. **One
  thing to know:** rustls needs a cryptography provider, and the default, `ring`, contains C and
  assembly. It is compiled from source by `cargo` with no *system* library, so the bookworm build
  and the Windows build are unaffected. That meets decision 10's reason, but not its letter.
  The alternative, a pure-Rust provider, is not yet production-grade. **The phase starts by
  confirming this with the user.**

**Verify.** Every behaviour against a local `tiny_http` fixture server, which is already in the
lockfile, so there is no network and no Docker: each pagination mode, the `max_pages` cap, 429
with `Retry-After`, a 500 then success, a 401 not retried, auth headers present, a secret masked
in an error, sink batching, and a sink failing on batch 3 reporting 2 delivered. Both OSes in CI.

**Done.** Both REST components registered and tested, and delivery semantics written: the source
is a snapshot per run, not transactional, since pages can shift while being read; the sink is
at-least-once per batch, and a partial failure says how many batches landed.

**Amended 2026-09-23, on completion.** Built as designed, with these additions and deviations:

- **The SDK grew a `check` hook** (default: accept) on `Source` and `Sink`, which the engine
  calls while compiling. Rules spanning properties, such as "cursor pagination needs
  `cursor_path`", are now refused by `etl validate` and the canvas, not by the first page of a
  run. XML moved its element-name checks there too.
- **`ureq` is pinned `~3.2.1`**, not the newest 3.4, because 3.4 needs Rust 1.85 and the
  connectors crate declares 1.80. The workspace uses resolver 2, which does not consider Rust
  versions when locking, so a caret requirement would have let the lock drift past the
  declared minimum. That check also showed **the workspace-wide 1.80 has not been true for
  some time** (clap, indexmap, zeroize and the Tauri stack need up to 1.88). That is an open
  decision in the tracker, not something 10b changed.
- **Lineage names an API by its endpoint only.** `Stage::external` for a component with a
  `url` is the URL without `user:pass@`, query string or fragment. `src.cloud.http` gains a
  lineage entry the same way.
- **The source can POST a search** (`method` = `POST`, `body`), because enough search APIs
  work that way to make it a first-page problem rather than a later one.
- **No watermark push-down.** The source reads every page on every run, like every native
  source; the incremental filter applies afterwards. Binding `${since}` into `query` does it
  by hand. A first-class version is a later addition, not a 10b one.
- **Real HTTPS was checked once, by hand:** GitHub's public releases API over TLS, with
  `Link` pagination, typed columns, and `max_pages` refusing to stop quietly. The suite itself
  never leaves 127.0.0.1.

##### Phase 10c — verifying Phase 4's database and lake connectors

**Goal.** Postgres, MySQL, Delta, Iceberg and S3 have been run against real systems, not only
had their SQL compared. The website's site-to-product sync is waiting on this.

**Do.** Postgres and MySQL through Docker (the daemon has to be running); local Delta and
Iceberg test tables; S3 against MinIO, which needs credential, region and endpoint properties
that `src.cloud.s3` does not have yet. Written up per connector, working or not.

**Done.** Each of the five is either verified with a test, or has a written reason why not.

**Amended 2026-09-23, on completion.** Done in one sitting. **Three of the five were broken**
in ways only running them could show: the S3 sink had never worked on Windows (it tried to
make a local directory of `s3://...`), MySQL sources failed on any aggregate over their view
(a DuckDB 1.5.5 extension bug, worked around), and Iceberg could not read a moved table by
its metadata file. All three are fixed, with the S3 access properties the plan anticipated and
an Iceberg `version`. Delta and Postgres worked as written. The tracker's *From Phase 10c* has
the detail. The servers run in Docker from `scripts/test-services.ps1`, locally and in CI's
Ubuntu gate; the lake tables are committed fixtures.

##### Phase 10d — SaaS GraphQL

**Questions answered 2026-09-23, all as recommended** (Settled decisions 18–24 in the tracker).

**Goal.** Read from and write to a GraphQL API, with the same patience and the same refusal to
load partially that 10b gave REST, plus the one thing GraphQL adds: **an HTTP 200 can be a
failure.**

**Files.** `crates/connectors/src/{http.rs (new), rest.rs, graphql.rs (new), graphql/tests.rs
(new), lib.rs}`, `crates/metadata/src/component.rs` (a `code` property kind),
`frontend/src/{Inspector.tsx, properties.ts}` and their tests, `crates/duckdb-engine/tests/native.rs`,
`samples/pipelines/graphql_orders.json`, `docs/connectors.md`, `docs/adding_a_component.md`
(only if the move to `http.rs` changes what it says).

**Do.**

1. **Move the HTTP layer out of `rest.rs` into `http.rs`** (decision 18): `Client`, `Settings`,
   `Auth`, `Reply`, retries, pacing, `connection_properties` and the small property helpers.
   REST keeps pagination, batching and its own specs. **This step changes no behaviour**, and
   its proof is that REST's 28 fixture tests (`rest/tests.rs`) pass unedited before any GraphQL code exists.
   Two additions the move makes room for:
   - `Settings` takes the allowed methods from its caller, so GraphQL can pin POST without a
     `method` property.
   - `Client::send` takes a **verdict** on a 2xx reply: accept, retry with a reason, or fail.
     REST passes "accept". This is how a throttling error inside a 200 gets the same backoff
     as a 429, through the one retry loop rather than a second one.
2. **`src.saas.graphql`.** Always POSTs `{"query", "variables"}` as JSON.
   - Properties: the connection set without `method`; `query` (required, `code`); `variables`
     (`code`, a JSON object, `${...}` resolved like any property); `records` (required, a JSON
     pointer such as `/data/orders/nodes`); `pagination` (`none`, `relay`, `offset`, decision
     19); `max_pages` (default 1000, an error when reached, as in REST); `retry_codes`;
     `columns`.
   - **relay:** sends `cursor_variable` (default `after`) from `pageInfo.endCursor` and stops
     when `hasNextPage` is false. `page_info` is a pointer that **defaults to the parent of
     `records` plus `/pageInfo`**, which is right for both `.../nodes` and `.../edges`, and can
     be set when an API puts it elsewhere. The same cursor twice in a row is an error, as in
     REST. The first page sends the variable as null, which is what Relay servers expect.
   - **offset:** sends `offset_variable` (default `offset`) and `limit_variable` (default
     `limit`) with `page_size` (default 100), and stops at a short page.
   - **Records must be objects.** Pointing at `edges` gives rows with a `node` column (a
     struct in DuckDB); the help says to prefer `nodes` where the API has it.
3. **Errors (decisions 20 and 21).** After a 2xx:
   - An `errors` array that is not empty **fails the read**, even when `data` came back. The
     message quotes up to three errors, each with its `path`, masked like every connector
     error.
   - **Unless every error is throttling:** an error's `extensions.code` or `type` (Shopify uses
     the first, GitHub the second) is in `retry_codes` (default `THROTTLED`, `RATE_LIMITED`).
     Then it is retried with the ordinary backoff, `Retry-After` honoured if the header is
     sent, and it counts against `retries`.
   - `data` null or missing with no `errors` fails too, naming the page.
4. **`check` (decision 22)**, no parser dependency: `query` is not blank; `variables` is a JSON
   object; `records` starts with `/`; relay's query declares `$<cursor_variable>`; offset's
   declares `$<offset_variable>` and `$<limit_variable>`. "Declares" means the text holds `$`
   and the name followed by something that is not a name character, which is enough to catch
   the mistake and cannot reject a valid query.
5. **`snk.saas.graphql` (decision 23).** Properties: the connection set without `method`;
   `mutation` (required, `code`); `rows_variable` (default `rows`); `variables` (extra,
   merged in, a JSON object); `batch_size` (default 100); `retry_codes`. Each batch is sent as
   **a list**, always, even a last batch of one, because a GraphQL input type is typed as a
   list and the shape must not depend on the row count. `check` requires the mutation to
   declare `$<rows_variable>` and forbids `variables` from also setting it. A batch whose
   reply has `errors` fails the write, saying how many batches landed before it:
   **at-least-once per batch**, as REST. Mutations that report failure inside `data` (such as
   Shopify's `userErrors`) are not inspected, and `docs/connectors.md` says so.
6. **A `code` property kind.** A multi-line monospace text box that is not SQL. `query`,
   `mutation` and `variables` use it. The canvas renders it like `sql` does today. REST's
   `body` moves to it too, since it is the same kind of value. Validation treats it as text.
7. **A sample**, `samples/pipelines/graphql_orders.json`, relay-paged, typed with `columns`, run
   against a fixture server by an engine test, as `rest_orders.json` is.

**Verify.**
- REST's tests pass unchanged after step 1, before anything else is written.
- Fixture tests (`tiny_http`, 127.0.0.1 only) for the source: one page; variables and auth
  sent; relay over three pages with the cursor sent and `hasNextPage` obeyed; `page_info`
  default and override; the repeated-cursor loop refused; offset stops at a short page;
  `max_pages` errors; `errors` fails with `path` in the message; partial `data` with `errors`
  fails; `data` null fails; `THROTTLED` by `extensions.code` retried then success;
  `RATE_LIMITED` by `type` retried; throttling past `retries` fails; a non-throttling error is
  not retried; 429 with `Retry-After` still honoured; a secret masked in an error.
- For the sink: batches of N sent as a list under `rows_variable` with the extra variables
  merged; a last batch of one is still a list; `errors` on batch 3 reports 2 delivered.
- `check` refusals, each by name: blank query, relay without `$after`, offset without
  `$offset`/`$limit`, `variables` not an object, a mutation without `$rows`, `variables`
  setting `rows`.
- End to end, in `tests/native.rs`: the sample through both transports and `preview`; CSV
  to the sink against the fixture; a failing upstream means the sink sends nothing.
- `code`: a metadata test that it accepts text, and a frontend test that it renders a textarea.
- **By hand, once (decision 24):** a public, token-free endpoint (`countries.trevorblades.com`)
  over real TLS. Written up in the tracker, not in the suite.
- The gate: fmt, clippy, all tests, frontend tests, typecheck and build, samples; **60
  components**.

**Done.** Both GraphQL components registered and tested, their delivery semantics in
`docs/connectors.md`, REST unchanged, and the tracker, `learnings.md` and `assignments.md`
updated.

**Amended 2026-09-23, on completion.** Built as designed, with these small additions:

- **REST's test server moved to a shared `crates/connectors/src/fixture.rs`**, after step 1's
  proof had run against the untouched `rest/tests.rs`. `rows_at` moved to `http.rs` too, since
  both connectors use it.
- **`variables` may not set a variable the pagination sends** (`after`, `offset`, `limit`),
  the source-side twin of the sink's rule about `rows`.
- **A `$` typed in front of a variable-name property is forgiven** (`rows_variable: "$rows"`),
  and a name that is not a GraphQL name is refused by `check`.
- **`page_info` is checked as a pointer**, and a relay response missing `pageInfo`,
  `hasNextPage` or (when there is a next page) `endCursor` fails naming what to ask for.
- **No golden-SQL tests were added.** Native builders are shared by every connector and
  already covered by 10a's; the end-to-end tests exercise the GraphQL stages through them.
- 29 connector tests and 5 end-to-end. Beyond the *Verify* list: the checks above, and the
  shared `max_pages` message pinned whole.

##### Phase 10e and 10f — Kafka, as bounded micro-batches

**Questions answered 2026-09-23, all as recommended** (Settled decisions 25–35 in the tracker).
One more question arose while planning; it is at the end of this section, and the plan
assumes its recommended answer.

**Why two sub-phases.** The answers fit together but are more than a day's work: the first
native source that *remembers where it stopped*, which touches the SDK, the engine, the state
store, the CLI and the runner, and then a Kafka connector in both directions with four ways to
authenticate. **10e** builds the checkpoint machinery and the Kafka *source* on plaintext;
**10f** adds the *sink*, TLS and SASL. Each ends green on its own.

**The shape of a streaming read.** Kafka is not polled forever. Each run is a **bounded
micro-batch**: when it starts, it records each partition's latest offset (the high
watermark), reads from where the last *successful* run stopped up to those offsets or
`max_records`, and stops. The offsets it reached are saved **only if the whole run succeeds**,
exactly as watermarks are. `docs/connectors.md` says this in its first line: this is not
continuous streaming, and nothing is implied to be.

###### The checkpoint: how a source remembers

Today a native source reads everything on every run. A **checkpoint** is a connector's own
record of where it got to, as a JSON value the engine stores and hands back without reading.

- **SDK.** `Context` gains `checkpoint: Option<JsonValue>`, the position the last successful
  run saved for this node. `Summary` gains `checkpoint: Option<JsonValue>`, the position this
  read reached; `None` means "leave it where it was". Existing connectors return `None`.
- **Engine.** `CompileOptions` gains `checkpoints` (node id to JSON), which the builder puts on
  the stage's `NativeStep`, so `compile` stays pure and the plan carries it the way it carries
  a watermark literal. `stage_sources` passes it in and collects what comes back.
  `RunReport` gains `checkpoints`, **dropped when the run fails**, including under
  `continueOnFailure`, the same line watermarks follow.
- **State store.** The state file gains a `checkpoints` map beside `watermarks`: node id to
  `{component, value, at}`. `formatVersion` stays 1 and an old file reads as having none, so
  nothing existing needs migrating. `etl state list` shows them; `etl state forget --node`
  forgets either kind.
- **CLI.** `save_watermarks` becomes `save_state` and saves both, in the one atomic write it
  already does. The scheduler and the console already go through it.
- **Preview** reads from the stored position and saves nothing, as it saves no watermark.
- **Question 12 (below):** the runner inside a built artifact gets the same load and save.

A checkpoint is only valid for the configuration that made it. Kafka's records its topic; a
node whose `topic` changed starts from `start` and the report says so, the way a changed
watermark column starts over today.

###### Crates and `tokio`

`rskafka` 0.6 (Settled decision 26), with its compression features (decision 31). It needs
`tokio`, the first use of Settled decision 10's clause for a family that cannot avoid it.
**The runtime stays inside the connector:** each `read` or `write` builds a single-threaded
runtime, `block_on`s the work, and drops it. The SDK, the engine and every other crate stay
blocking and never see it. TLS is `rustls` with `ring` and default features off, which is what
`rskafka` asks for and what `ureq` already uses, so there is still one TLS stack and one
cryptography provider (Settled decision 16). **Checked at the start of 10e with `cargo tree`:**
no `aws-lc-rs`, no `openssl`, no system library. If that is wrong, the phase stops and asks.

###### Phase 10e — checkpoints, and the Kafka source

**Goal.** A pipeline reads a Kafka topic in bounded batches, picking up each run exactly where
the last successful run stopped, from `etl run`, the scheduler and a built artifact.

**Files.** `crates/plugin-sdk/src/lib.rs`; `crates/duckdb-engine/src/{native.rs, exec.rs,
plan/mod.rs, plan/builders.rs}`; `crates/state/src/lib.rs`; `crates/cli/src/main.rs`;
`crates/runner/src/{main.rs, lib.rs}`; `crates/connectors/src/{kafka.rs, kafka/tests.rs,
lib.rs}`, `Cargo.toml`s; `scripts/test-services.ps1`; `.github/workflows/gate.yml`;
`crates/duckdb-engine/tests/verified.rs`; `samples/pipelines/kafka_orders.json`;
`docs/connectors.md`, `docs/adding_a_component.md`.

**Do.**
1. **The checkpoint machinery**, as above, with no Kafka in it yet: SDK, engine, report, state
   store, CLI, runner. Proved by a test-only connector in the engine's tests that counts up
   from its checkpoint: run twice and it continues; fail the run and it does not; forget it
   and it restarts.
2. **Dependencies:** `rskafka`, `tokio` (`rt`, `net`, `time`), and the `cargo tree` check.
3. **`src.stream.kafka`.** Properties:
   - `brokers` (required, `host:port`, comma-separated) and `topic` (required).
   - `start`: `earliest` (default) or `latest`, for a partition with no checkpoint.
   - `max_records` (default 100,000): reaching it **stops normally** and checkpoints there. The
     report says how many were left behind, per the recorded high watermarks.
   - `value_format`: `json` (default) makes each value's object fields into columns; `text`
     gives a `value` column; `bytes` gives `value` as base64. Every row also has `_topic`,
     `_partition`, `_offset`, `_timestamp` (UTC) and `_key` (text, or null).
   - `security` (`plaintext` only in 10e; 10f adds the rest), `timeout_ms`, `columns`.
   - **A value that is not a JSON object under `json` fails the read**, naming the partition
     and offset, rather than being skipped. A tombstone (a null value) becomes a row with only
     the underscore columns.
4. **Gaps are errors.** If a checkpointed offset is older than the partition's oldest
   surviving offset, retention deleted messages this pipeline never read. The read **fails**,
   naming the partition and how many offsets were lost. The fix is deliberate: `etl state
   forget` restarts the node from `start`. A new partition (the topic grew) starts from `start`
   with a note in the report.
5. **Test services.** An `apache/kafka` container (KRaft, one node) in
   `scripts/test-services.ps1`, with `ETL_TEST_KAFKA` set, and in CI's Ubuntu gate. **Needs
   Docker running on this machine**, which the user starts.
6. **The sample**, `samples/pipelines/kafka_orders.json`: topic to filter to Parquet, run by a
   verification test against the container.

**Verify.**
- Checkpoint machinery, with the test connector and no Kafka: continues; a failed run saves
  nothing; `continueOnFailure` saves nothing; `forget` restarts; an old state file with no
  `checkpoints` still loads; preview saves nothing; the runner saves and reloads beside
  itself.
- Unit, no broker: value formats, the underscore columns, a tombstone, a non-object refused,
  the checkpoint's shape, a changed topic ignoring the checkpoint, the gap arithmetic.
- Against the container (skipped without `ETL_TEST_KAFKA`): produce 25 records to 3 partitions
  with a test helper; `max_records` 10 reads 10, then 10, then 5, then 0, each continuing
  exactly; a failed downstream stage means the next run re-reads the same records; `latest`
  on a first run reads nothing and checkpoints the ends; deleting records makes the next read
  fail with the gap message; both transports; a built artifact reads, stops, and continues
  on its second run.
- A mutation check on "saved only after success".
- The gate: fmt, clippy, all tests (locally with the container up, and without it, when those
  tests skip), frontend, samples; **61 components**.

**Done.** The Kafka source registered and verified against a real broker; checkpoints on every
path that runs a pipeline; its delivery semantics in `docs/connectors.md`.

**Amended 2026-09-23, on completion.** Built as designed, with these changes:

- **The state rules moved into the engine**, as `etl_duckdb_engine::remember`
  (`compile_options` and `remember`), because `etl run`, the scheduler, the console and the
  runner all need them and copying them four times is how they would drift. The engine now
  depends on `etl-state`.
- **`etl build` notes instead of refusing.** Question 12's premise was wrong: the build already
  refused incremental pipelines, so option (a) replaced a refusal rather than fixing a silent
  re-read. The note names the nodes and says the directory the artifact runs in holds its
  state.
- **The "test-only connector" is injected, not registered.** `native::stage_sources_using`
  takes the registry lookup as an argument, so a counting source in the engine's unit tests
  proves the checkpoint's path without adding a component anyone could see.
- **The runner check was done by hand, not in CI:** a built artifact ran 15, 0, then 1 against
  the broker, and `etl state list` read its state. CI's artifact jobs have no broker.
- **`rskafka`'s backoff has no deadline by default**, so the connector sets one and times out
  every call. **Its `chrono` cannot format**, so timestamps use `etl_state::time`.
- **Found by running the sample:** a micro-batch into an `overwrite` file sink keeps only the
  latest batch. Documented, not changed.
- **The frontend gained an icon** (`radio`); the canvas otherwise needed nothing.

###### Phase 10f — the Kafka sink, TLS and SASL

**Goal.** Write to Kafka, and reach a real hosted cluster, which always means SASL over TLS.

**Do.**
1. **`snk.stream.kafka`.** `brokers`, `topic`, the security set, `key_column` (optional),
   `batch_size` (default 500), `compression` (`none`, `gzip`, `snappy`, `lz4`, `zstd`;
   default `none`). Each row goes as one JSON object, the key column included. **Partition by
   key the way Java clients do** (murmur2 of the key bytes, positive, modulo the partition
   count), so a key lands where other producers put it; rows with no key go round-robin by
   batch. Every produce waits for all in-sync replicas. At-least-once per batch, with the
   delivered count in the error on a partial failure, as REST and GraphQL.
2. **Security for both directions:** `security` = `plaintext`, `ssl`, `sasl_plaintext`,
   `sasl_ssl`; `sasl_mechanism` = `plain`, `scram-sha-256`, `scram-sha-512`; `username`,
   `password` (`${SECRET:...}`, masked); `ca_cert` (a PEM file, for a private CA; unset uses the
   bundled roots). `check` refuses a combination that cannot work, such as SASL without a
   username.
3. **The container gains listeners** for SASL (PLAIN and SCRAM users made at start) and TLS
   (a certificate made by the script, in a throwaway container, so the machine needs no
   `openssl`).

**Verify.** Murmur2 against Java's published values; key placement across partitions;
round-robin without a key; compression each way (produce with each codec, read back through
the source); a batch failing partway reports what landed; each security mode connects, and a
wrong password fails naming the mechanism, not the password; the sample extended to write
back to a second topic. **62 components.**

**Done.** Both Kafka components, every security mode, verified against a real broker.

###### Question 12 (new, raised while planning)

**Should a built artifact remember state?** The runner calls `compile` with no state, so an
artifact already re-reads every incremental source from its `start` on every run. Nothing
documents that, and nothing has tripped on it, because no one has scheduled an incremental
artifact yet. For Kafka it would mean re-reading the whole topic every run.
- (a) **The runner loads and saves `.etl/state/` in its working directory** (or
  `--workspace`), in the same format, so `etl state list` can read it. Watermarks and
  checkpoints both. **Recommended**, and assumed above: it fixes the watermark gap too, and an
  artifact on a server run by cron is exactly how a micro-batch reader gets deployed.
- (b) Artifacts stay stateless, and `etl build` refuses a pipeline with an incremental or
  checkpointing source.
- (c) Artifacts stay stateless, documented.

**Later families**, planned one at a time when reached: the other streaming brokers (NATS
JetStream and Kinesis fit the checkpoint model; Pub/Sub and RabbitMQ acknowledge instead and
need their own design), then NoSQL, warehouses over their own protocols, vector DBs.

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
