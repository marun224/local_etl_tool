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

**Amended 2026-09-23, on completion.** Built as designed, with these additions:

- **A failed sign-in is diagnosed, not timed out.** `rskafka` retries it until any timeout
  wins, so a connect that times out makes one attempt with retries off and reports its reason.
  Every broker call also gets 5 s of slack past `timeout_ms`, for the same reason.
- **Murmur2 was also checked live** against Kafka's Java console producer (30 keys, identical
  partitions), beyond the published values.
- **The listeners are named `TLS`, `SASL` and `SASLTLS`**, not `SSL`/`SASL_*`, to step around the
  image's own start-up rules; certificates come from `scripts/kafka-test-secrets.sh` in a
  throwaway container, into a Docker volume.
- **`etl secret set --stdin` drops a leading BOM**, found signing in with a password piped from
  PowerShell.
- **Test helpers wait for a new topic to be listed**, because Kafka creates topics
  asynchronously.
- **A refused batch may have partly landed** (it goes to each partition in turn); the error and
  `connectors.md` say so.
- **Not supported, and documented as such:** OAUTHBEARER, Kerberos, mutual TLS, idempotent or
  transactional producing, headers.

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

##### Phase 10g — NATS JetStream, both ways

**Questions answered 2026-09-23, all as recommended** (Settled decisions 37–46 in the
tracker). One sub-phase: the checkpoint machinery exists since 10e, and the connection and
security patterns since 10f.

**Goal.** Read a JetStream stream in bounded micro-batches and publish to one, with every
way hosted NATS signs in, verified against a real server.

**Why JetStream only.** Core NATS keeps nothing: a message goes to whoever is listening at
that moment. There is nothing to read back in a batch, so only JetStream, NATS's persistence
layer, is a source. The sink publishes to a subject a stream captures, and waits for
JetStream's acknowledgement.

**Crates.** `async-nats` 0.50 with `default-features = false` and `jetstream`, `ring` and
`nkeys`. Checked in a throwaway project before planning: `ring` and one `rustls` 0.23 (the
ones already in use), `nkeys` on pure-Rust `ed25519-dalek`, no `aws-lc-rs`, OpenSSL or CMake.
It always brings `rustls-native-certs`, which uses `schannel` on Windows: bindings to an
operating-system API, not a C library we compile or ship. Its trust store is not used,
because the connector passes its own `rustls` config (decision 42). The runtime is Kafka's
pattern: single-threaded `tokio`, built and dropped inside each read or write (decision 43).

**Files.** `crates/connectors/src/{nats.rs, nats/tests.rs, lib.rs}`; the shared TLS set-up
moves out of `kafka.rs` into a small `tls.rs` both use; `crates/connectors/Cargo.toml`;
`scripts/test-services.ps1` and a NATS config and credentials script; `gate.yml`;
`crates/duckdb-engine/tests/verified.rs`; `samples/pipelines/nats_orders.json`;
`docs/connectors.md`; `frontend/src/icons.ts` (nothing, if `radio` serves).

**Do.**

1. **Shared TLS.** `Connection::tls` from 10f becomes `tls::client_config(ca_cert, context)`,
   used by Kafka and NATS alike, with Kafka's tests unchanged to prove the move.
2. **`src.stream.nats`** (decisions 37–39). Properties: `url` (e.g. `nats://host:4222`,
   comma-separated for a cluster), `stream` (required), `filter_subject` (optional, e.g.
   `orders.eu.>`), `start` (`earliest`/`latest`), `max_records` (100,000), `value_format`
   (`json`/`text`/`bytes`), the sign-in set, `timeout_ms`, `columns`.
   - **A batch** is from the saved next sequence up to the stream's last sequence recorded at
     the start, or `max_records`, read through an ordered, ephemeral consumer
     (`DeliverPolicy::ByStartSequence`). **Nothing is left on the server**: no durable consumer
     (decision 38).
   - **The checkpoint** is `{stream, filter_subject, next}`. A changed stream or filter starts
     from `start`, with a note, as Kafka's changed topic does.
   - **Gaps:** a saved position older than the stream's first sequence means the stream's
     limits (age, count, size) discarded messages this pipeline never read. The read fails and
     counts them, as for Kafka. **Messages deleted from the middle** of a stream (by
     `max_msgs_per_subject`, or by hand) are normal in JetStream and are simply not there;
     they are not a gap error, and `connectors.md` says so.
   - **Rows:** the value as Kafka's (`json`/`text`/`bytes`, the same refusals and BOM hint),
     plus `_stream`, `_subject`, `_sequence`, `_timestamp` and `_headers` (a JSON object of
     name to value, repeated names as a list; null when there are none).
3. **`snk.stream.nats`** (decision 40). Properties: `url`, the sign-in set, `subject`
   (required; the stream that captures it must exist), `batch_size` (500), `msg_id_column`
   (optional). Each row is published as JSON and each batch's acknowledgements are awaited
   before the next batch. **`msg_id_column`** sets `Nats-Msg-Id`, so JetStream drops a
   re-sent message within the stream's duplicate window: the one place in this project
   where a re-run can be free of duplicates, and `connectors.md` says exactly how far that
   goes. At-least-once per batch otherwise, with the delivered count in the error.
4. **Sign-in, both directions** (decision 41): `auth` = `none`, `user_password` (`username`,
   `password`), `token` (`token`), `creds` (`creds_file`, a `.creds` file of JWT and NKey seed,
   relative to the workspace); `tls` = `true`/`false` (default `false`), with `ca_cert`. The
   secret-bearing values take `${SECRET:...}` and are masked. `check` refuses a setting that
   would be ignored, as Kafka's does.
5. **Test services** (decision 45): NATS 2.x containers with JetStream: one open, one with
   users, one with a token, one with TLS (the certificate from 10f's script, in the same
   volume), and one in operator mode for `.creds`, its operator, account and user made by
   `nsc` in a throwaway `nats-box` container. Variables `ETL_TEST_NATS`,
   `ETL_TEST_NATS_USERS`, `ETL_TEST_NATS_TOKEN`, `ETL_TEST_NATS_TLS`, `ETL_TEST_NATS_CREDS`
   (the server) and `ETL_TEST_NATS_CREDS_FILE`. **Operator mode is the riskiest part**; if it
   will not come up in a day, `.creds` is verified by hand against it and the automated test
   follows, recorded as such.
6. **The sample**, `samples/pipelines/nats_orders.json`: a stream of orders to a filter to
   Parquet, and the large orders published back to another subject, in `verified.rs`.

**Verify.**
- Kafka's tests pass unchanged after the TLS move (step 1).
- Unit, no server: settings and every refusal; the checkpoint's shape, a changed stream or
  filter, the gap arithmetic; rows in each format, headers, a missing value.
- Against the server: batches of 10/10/5/0 continuing exactly; a run not saved reads again;
  `latest`; a filter reading only its subjects; a stream whose limit discarded unread messages
  fails with the count; a stream with interior deletes reads what remains without error; the
  sink's round trip; `msg_id_column` publishing twice lands once; a refused publish reports
  what was delivered; each sign-in method; a wrong password names the method and not the
  password; TLS without the right CA is refused with the reason.
- The sample in `verified.rs`, both transports and preview; a built artifact continuing
  between runs, by hand.
- The gate: fmt, clippy, all tests with every server up and without them, frontend, samples;
  **64 components**.

**Done.** Both NATS components, every sign-in method, verified against real servers, and
their semantics in `docs/connectors.md`.

**Amended 2026-09-24, on completion.** Built as designed, in one sitting, with these notes:

- **`.creds` got its automated test after all.** Operator mode came up at the first attempt,
  tried in throwaway containers before it went into the project.
- **Value decoding became shared** (`kafka::value_columns`, `key_text`) alongside the TLS
  move, both proved by Kafka's unchanged tests.
- **A read ends by count, not by waiting**: the consumer's pending count at creation, and each
  message's own pending count, say when the batch is done.
- **A filtered read's position moves past the end of the stream**, so unmatched messages are
  not revisited.
- **The frontend's sample test needs `etl` rebuilt first**, since it reads the built manifest;
  noted in the tracker.
- **Components: 64.** The icon is `radio`, shared with Kafka; the canvas needed nothing new.

**After NATS** (decision 46): decided then, among Kinesis, a design for the
acknowledgement-based brokers (Pub/Sub, RabbitMQ), and the plan's next family, NoSQL.
**Chosen 2026-09-24: Kinesis.**

##### Phase 10h and 10i — Amazon Kinesis Data Streams

**Questions answered 2026-09-24, all as recommended** (Settled decisions 47–56). Question 9
(a hand check against real AWS) was answered (b): no AWS account is used, so signing is
proven by AWS's published test suite and Kinesis is recorded as **not yet checked against
real AWS** until someone does.

**Why two sub-phases.** Kinesis needs three things no connector has had: AWS request signing,
AWS's credential sources, and shards that split and merge. **10h** builds those and the
source; **10i** the sink. Each ends green on its own.

**No new runtime, no new cryptography.** Kinesis is a JSON-over-HTTPS API, so it goes through
the `ureq` layer REST and GraphQL use (`http.rs`): blocking, no `tokio`. Signing is
HMAC-SHA256 from `ring`, already in the tree. The AWS crates were rejected because they
declare Rust 1.94.1 against our 1.88 (Settled decision 17), and the SDK brings `tokio` and a
large tree (decision 47).

###### Phase 10h — signing, credentials, and the Kinesis source

**Files.** `crates/connectors/src/{aws.rs, aws/tests.rs, kinesis.rs, kinesis/tests.rs, http.rs,
lib.rs}`; `crates/connectors/tests/fixtures/sigv4/` (AWS's test vectors, with their licence
and where they came from); `scripts/test-services.ps1`; `gate.yml`;
`crates/duckdb-engine/tests/verified.rs`; `samples/pipelines/kinesis_orders.json`;
`docs/connectors.md`.

**Do.**

1. **SigV4** (decision 47) in `aws.rs`: canonical request, string to sign, derived signing key,
   `Authorization` header, `x-amz-date`, and `x-amz-security-token` for temporary
   credentials. Proved against **AWS's own SigV4 test suite** (the `aws-sig-v4-test-suite`
   vectors, Apache-2.0, copied under `tests/fixtures/sigv4/` with attribution): each case's
   canonical request, string to sign and signature must match byte for byte. Only the cases a
   JSON `POST` exercises matter for Kinesis, but every applicable one is run.
2. **Credentials and region** (decision 48), in this order, the first that answers winning:
   properties (`access_key_id`, `secret_access_key`, `session_token`, as secrets); then
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`; then a named profile
   (`profile` property, else `AWS_PROFILE`, else `default`) from `~/.aws/credentials` and
   `~/.aws/config` (or `AWS_SHARED_CREDENTIALS_FILE`, `AWS_CONFIG_FILE`). Region: `region`,
   else `AWS_REGION`/`AWS_DEFAULT_REGION`, else the profile's. **Instance roles (EC2, EKS
   IRSA, ECS) are not in 10h**; a missing credential says so and names the sources it tried.
   Credentials are read when the connector runs, never baked into an artifact unless a
   property holds them.
3. **The HTTP layer learns two things**: a per-request header set (the signature changes with
   every request), and a judgement on an **error** response as well as a success, so a `400`
   whose body says `ProvisionedThroughputExceededException` or `LimitExceededException` is
   retried with the ordinary backoff, as 10d did for GraphQL's throttling in a `200`. REST's
   and GraphQL's tests must pass unchanged.
4. **`src.stream.kinesis`.** Properties: `stream` (required), the AWS set (`region`,
   `profile`, the keys, `endpoint` for a VPC endpoint or a test server), `start`
   (`earliest`/`latest`), `max_records` (100,000), `value_format`, `timeout_ms`,
   `on_expired` (`fail`/`continue`), `columns`.
   - **Shards** from `ListShards`, following `NextToken`. **Lineage** (decision 51): a shard is
     read only after its parents (`ParentShardId`, `AdjacentParentShardId`) are finished, so a
     partition key's records stay in order across a split or a merge. A parent that has aged
     out of the stream's retention counts as finished. A closed shard read to its end is
     recorded as done, and its children become readable in the same run.
   - **A batch** (decision 50): each readable shard is read from its saved position
     (`AFTER_SEQUENCE_NUMBER`) or from `start`, shards taking turns, until Kinesis reports it
     caught up (`MillisBehindLatest` 0), the shard ends, or `max_records` is reached. So it is
     "up to now", not a snapshot taken at the start; `connectors.md` says how that differs
     from Kafka. `GetRecords` is paced to Kinesis's five calls a second per shard.
   - **The checkpoint** is `{stream, shards: {id: {"after": seq} | {"done": true}}}`, sequence
     numbers kept as text: they are 128-bit integers.
   - **Expiry** (decision 52): the saved record is looked up with `AT_SEQUENCE_NUMBER`. If it
     is no longer held, records after it **may** have expired unread; Kinesis's sequence
     numbers leave gaps, so the count cannot be known. With `on_expired: fail` (the default)
     the read fails, saying exactly that, and naming the fix (`etl state forget`, or
     `on_expired: continue`) and the false-alarm case: a stream quiet for longer than its
     retention loses nothing but still trips this.
   - **Rows** (decision 54): the value as Kafka's (`json`/`text`/`bytes`), plus `_stream`,
     `_shard`, `_sequence` (text), `_timestamp` (arrival, UTC, milliseconds) and
     `_partition_key`. `Data` arrives base64-encoded; a decoder sits beside the encoder in
     `http.rs`.
5. **Test services** (decision 55): `kinesis-mock` 0.4.13 in `scripts/test-services.ps1`, its
   plain-HTTP port as `ETL_TEST_KINESIS`; the tests create their streams and reshard them
   with `SplitShard`/`MergeShards` against it. It does not check signatures, so every
   request's signature is also checked in a unit test against a recomputation, and the SigV4
   vectors carry the real proof.
6. **The sample**, `samples/pipelines/kinesis_orders.json`, orders to a filter to Parquet, in
   `verified.rs` on both transports.

**Verify.** SigV4 vectors, byte for byte; each credential source and its order, with
temporary files for the profile cases; a missing credential naming what was tried; REST's and
GraphQL's tests unchanged after the HTTP change; a throttled `400` retried and a real `400`
not. Against the container: batches of 10/10/5/0 continuing exactly across two shards;
`latest`; a split mid-way, the parent read to its end before its children, and a key's
records in order across it; a merge likewise; expiry, which cannot be waited for (Kinesis keeps
records at least 24 hours), so exercised with a checkpoint naming a sequence number the shard
does not hold, the same thing `AT_SEQUENCE_NUMBER` sees once a record has expired: failing
with `fail`, continuing from the oldest record with `continue`; a missing stream named. The sample through both transports and preview. The gate;
**65 components**.

**Done.** The source against a Kinesis-compatible server, signing proven by AWS's vectors,
semantics in `connectors.md`, and "not yet checked against real AWS" recorded in the tracker.

**As built (2026-09-24).** Done as planned, with these differences:

- **The checkpoint has two more shard states** than planned: `{"since": ms}` (a `latest`
  first run, read from that moment next time) and `{"start": true}` (a shard `max_records`
  never reached, read from `start` next time). Without the second, a shard left unread got
  "from now" and its records were skipped: a data-loss bug a rerun found.
- **`LimitExceededException` is retried only when its message says "rate exceeded"**; the
  same exception for an account's shard limit fails at once.
- **"Not held"** is AWS's `InvalidArgumentException` or `kinesis-mock`'s
  `ResourceNotFoundException` naming the sequence number; both are accepted.
- **The signature check against a recomputation** runs against the local fixture server,
  not the container: each attempt's `Authorization` is recomputed from the host, headers and
  body the server received. A mutation (signing a different `Content-Type`) fails it.
- `retries` (default 5) is a property, as the HTTP connectors have it.

###### Phase 10i — the Kinesis sink

**Do.** `snk.stream.kinesis` (decision 53): the AWS set, `stream`, `partition_key_column`
(unset: keys spread evenly by row number), `batch_size` (default and maximum 500, Kinesis's
limit per `PutRecords`; also kept under its 5 MB per call). Each row as one JSON record.
**Partial failures**: `PutRecords` can refuse some entries of a call (usually throttling);
those entries alone are retried with backoff, up to `retries`, and if some still fail the
error says how many records were delivered. At-least-once per batch, as the others.

**Verify.** Round trip through the source; keys landing on the shard their hash says; a
partial failure (from `kinesis-mock`'s limits, or a deliberately oversized record) reported
with what landed; the sample extended to write back to a second stream. **66 components.**

**Done.** Both Kinesis components, verified against the container, semantics documented.

**As built (2026-09-24).** Done as planned, with these differences:

- **Partial failures are tested against the local fixture server**, which refuses exactly
  the records a test names, rather than provoked from `kinesis-mock`'s limits. Only
  `ProvisionedThroughputExceededException`, `InternalFailure` and `KMSThrottlingException`
  are sent again; any other refusal fails at once.
- **Size limits are checked before sending** (1 MiB per record with its key, keys of 1 to
  256 characters), so an oversized row fails naming itself instead of failing its call.
- **"Keys landing on the shard their hash says"** is checked as each key landing on one
  shard, with the key exactly the column's value. The hash is Kinesis's work, not the sink's.
- A resent record lands after later records of its call, so per-key order holds only when
  nothing is resent. `connectors.md` says so.

**Chosen 2026-09-24, after Kinesis: the acknowledgement-based brokers** (RabbitMQ, SQS,
Pub/Sub).

##### Phase 10j, 10k and 10l — the acknowledgement-based brokers: SQS, Pub/Sub, RabbitMQ

**Questions answered 2026-09-24, all as recommended** (Settled decisions 57–70). No AWS
account or Google Cloud project is used (decision 70): SQS and Pub/Sub will be recorded as
**not yet checked against the real services**, as Kinesis is. RabbitMQ is tested against
RabbitMQ itself.

**Why these need their own design.** Kafka, NATS and Kinesis keep messages and let a
consumer read from a position, so a run saves where it got to and nothing on the broker
changes. A queue works the other way round: a consumer *receives* messages, *holds* them,
and then **acknowledges** them (they are gone) or **releases** them (they come back). Until
now a source's `read` returns before DuckDB starts, so a source has had no way to hold
anything until the run's outcome is known. The engine already has that moment, after the
sinks delivered (`exec.rs`, both transports), and `preview` never reaches it.

**Order** (decision 57): **10j** builds the shared design and SQS, **10k** Pub/Sub, **10l**
RabbitMQ, each with its source and sink and each green on its own. SQS goes first because it
reuses 10h's signing and credentials; RabbitMQ, the strictest case for the design (its
acknowledgements belong to an open connection), was checked against the design before 10j
starts building it.

###### The shared design: a receipt, settled once

1. **The plugin SDK gains `Receipt`** (decision 58):

   ```rust
   pub trait Receipt: Send {
       /// The run fully succeeded: the messages are done with.
       fn acknowledge(self: Box<Self>) -> Result<String, ConnectorError>;
       /// Anything else: give the messages back for another run.
       fn release(self: Box<Self>) -> Result<String, ConnectorError>;
   }
   ```

   and `Source` gains `read_held(...) -> Result<(Summary, Option<Box<dyn Receipt>>), _>`,
   whose default calls `read` and holds nothing, so every existing source is unchanged. A
   queue source implements `read_held`; its `read` receives and releases at once. The
   receipt comes back **beside** the `Summary`, not inside it, so `Summary` keeps `Clone`
   and `Eq`. Each method returns a line for the report.
2. **The engine settles every receipt exactly once.** Staging collects them in a `Receipts`
   guard, like `Staging` for files:
   - **acknowledge** once the run fully succeeded **and** the native sinks delivered: the
     same point checkpoints become saveable;
   - **release** on every other path: a failed stage, a `continue_on_failure` run with
     failures, `preview`, an error anywhere, an interrupted run. The guard's `Drop` releases
     whatever is still held, so an early return cannot forget one.

   The script path, the session path, `preview`, the runner in a built artifact, the
   scheduler and the console all go through these two functions, so all get it.
3. **When acknowledging fails after the sinks delivered** (decision 59): the run still counts
   as successful (other sources' positions are saved, watermarks advance), and the report
   gets a new `warnings` list: *"N message(s) from <node> could not be acknowledged and will
   be delivered again: <why>"*. `etl run` prints warnings after the stages, `--json`
   carries them, and run history keeps them. This is duplication, not loss, and marking the
   run failed would cause more of it.
4. **Holding long enough** (decision 60): a hold ends on its own at SQS's visibility timeout
   (up to 12 hours), Pub/Sub's ack deadline (up to 10 minutes) or RabbitMQ's
   `consumer_timeout` (30 minutes by default). For SQS and Pub/Sub the receipt starts a
   **lease keeper**, a thread that extends the hold every half-period until the receipt is
   settled; it stops when the receipt is settled or dropped. RabbitMQ holds messages as long
   as the channel is open, so the receipt keeps it open; its timeout is documented.
5. **A batch** (decision 61) ends at `max_records` (default **10,000**: everything read is
   held until the run ends), when the queue answers empty (SQS: a receive with a 1-second
   wait returns nothing; Pub/Sub: a pull returns nothing; RabbitMQ: `basic.get` says empty),
   or at `max_wait_ms` (default 30,000) of receiving.
6. **Rows** (decision 62): `value_format` as the other brokers (`json`/`text`/`bytes`), plus
   each broker's underscore columns (below). Receive and redelivery counts let a pipeline
   spot repeats.
7. **No checkpoint.** These sources keep no position: `etl state` lists nothing for them and
   `etl state forget` has nothing to forget. The broker holds the state.
8. **Delivery, stated in `connectors.md`:** at-least-once. A message comes again after a
   failed run, a failed acknowledgement, or a hold that ran out; it is never acknowledged
   before the run succeeded. Order is the broker's: none for SQS standard queues and
   Pub/Sub without ordering keys, per group or key otherwise.

**The engine's tests** use a connector that exists only in the test (as `native/tests.rs`
already does) with a receipt that records how it was settled: acknowledged after success on
both transports; released after a failed stage, with `continue_on_failure`, in `preview`,
and when a sink fails; released by `Drop` on an early error; an acknowledgement error ending
up in `warnings` with the run successful and its checkpoints saved.

###### Phase 10j — the shared design, and SQS

**Files.** `crates/plugin-sdk/src/lib.rs` (`Receipt`, `read_held`);
`crates/duckdb-engine/src/{native.rs, exec.rs}` (`Receipts`, settling, `warnings`),
`native/tests.rs`; `crates/cli` (print warnings); `crates/connectors/src/{sqs.rs,
sqs/tests.rs, lib.rs}`, reusing `aws.rs` and `http.rs`; `scripts/test-services.ps1`;
`gate.yml`; `verified.rs`; `samples/pipelines/sqs_orders.json`; `docs/connectors.md`.

**Do.**

1. The shared design above.
2. **`src.queue.sqs`** (decisions 63, 69): `queue_url`, or `queue` resolved with
   `GetQueueUrl` (and `queue_owner` for another account's queue); the AWS set exactly as for
   Kinesis (region, profile, keys, session token, `endpoint`, `timeout_ms`, `retries`);
   `max_records`, `max_wait_ms`, `visibility_seconds` (default 300, the lease keeper's
   period), `value_format`, `columns`. Standard and FIFO queues. `ReceiveMessage` in 10s;
   `DeleteMessageBatch` to acknowledge, `ChangeMessageVisibilityBatch` to 0 to release, and
   to extend. The JSON protocol (`AmazonSQS.*`, `application/x-amz-json-1.0`), signed as
   Kinesis is. Rows: `_queue`, `_message_id`, `_sent_timestamp`, `_receive_count`,
   `_group_id` (FIFO), `_attributes` (message attributes as JSON).
3. **`snk.queue.sqs`** (decision 67): each row one JSON message, `SendMessageBatch` in 10s
   and under the batch payload limit; `delay_seconds`; for FIFO, `message_group_id_column`
   (required there) and `deduplication_id_column` (else content-based deduplication must be
   on). Entries refused within a batch: throttling-like ones sent again with backoff, others
   fail at once saying what was sent, as the Kinesis sink does.
4. **Test services** (decision 68): ElasticMQ 1.7.1 (`softwaremill/elasticmq-native`, 32 MB)
   as `ETL_TEST_SQS`, locally and in CI's Ubuntu gate.
5. **The sample**, `sqs_orders.json`: orders from a queue, filtered, to Parquet and to a
   second queue.

**Verify.** Without a server: settings refused by property; rows; batching of sends. Against
ElasticMQ: a run acknowledges (the queue is empty after it); a failed run releases (the next
run reads the same messages, `_receive_count` 2); `preview` releases; `max_records` leaves
the rest; a hold shorter than the run is kept alive by the lease keeper (a `visibility_seconds`
of 2 and a receipt held 6 seconds, nothing redelivered meanwhile); FIFO order within a
group; a missing queue named; the sink round trip, FIFO groups and deduplication IDs. The
engine tests above. The sample on both transports and `preview`. The gate: **68 components**.

**Done.** Receipts settled correctly on every path, SQS both ways against ElasticMQ,
semantics documented, "not yet checked against real AWS" recorded.

**As built (2026-09-24).** Done as planned, with these differences:

- **The engine's tests are split.** The run functions look connectors up in the real
  registry, so the test connector covers the guard (collecting, acknowledging, releasing,
  `Drop`, warnings and masking, a later source's failure releasing an earlier one's hold),
  and the transports, `preview` and `continueOnFailure` are covered with SQS against
  ElasticMQ in `verified.rs`. An acknowledgement that fails mid-run cannot be provoked from
  ElasticMQ; it is covered by the guard's tests.
- **Kinesis's signed client became `aws::JsonApi`**, shared by both services, with a
  `Protocol` naming each one's target prefix, content type and throttling.
- **Release on a failed row decode**: the SQS receipt exists before the first message
  arrives, so a message that will not decode gives back everything received.
- `Receipt` settles by value (`self: Box<Self>`); each method returns a report line.

###### Phase 10k — Pub/Sub

**Files.** `crates/connectors/src/{gcp.rs, gcp/tests.rs, pubsub.rs, pubsub/tests.rs}`,
`tests/fixtures/` for the RFC vector; the services script, `gate.yml`, `verified.rs`, a
sample, `connectors.md`.

**Do.**

1. **Google sign-in, our own** (decision 64), in `gcp.rs`: a service-account key file (from
   `credentials_file` or `GOOGLE_APPLICATION_CREDENTIALS`): a JWT signed RS256 with `ring`'s
   `RsaKeyPair`, exchanged at the key's `token_uri` for an access token, cached until
   shortly before it expires; gcloud's user login file (`authorized_user`: a refresh token
   exchanged for an access token); none when `endpoint` names an emulator (or
   `PUBSUB_EMULATOR_HOST` is set). The metadata server (GCE, GKE) later, like AWS roles.
   RS256 proved by RFC 7515's appendix A.2 example, byte for byte.
2. **`src.queue.pubsub`**: `project`, `subscription`, the sign-in set, `endpoint`;
   `max_records`, `max_wait_ms`, `ack_deadline_seconds` (default 60, the lease keeper's
   period, at most 600), `value_format`, `columns`. `:pull`, `:acknowledge`,
   `:modifyAckDeadline` (0 to release, and to extend). Rows: `_subscription`,
   `_message_id`, `_publish_time`, `_ordering_key`, `_attributes`, `_delivery_attempt`.
3. **`snk.queue.pubsub`**: `project`, `topic`; each row one JSON message; `:publish` in
   batches of up to 1,000 messages and 10 MB; `ordering_key_column`; `attributes_column`
   (an object column becoming string attributes).
4. **Test services**: the Pub/Sub emulator (`google-cloud-cli:586.0.0-emulators`, 445 MB) as
   `ETL_TEST_PUBSUB`, locally and in CI. It does not check sign-in, so the token exchange is
   tested against the local fixture server.

**Verify.** The RFC 7515 vector; the token request's form and its caching, against the
fixture; each credential source and its order. Against the emulator, the same receipt
behaviours as SQS (acknowledge, release, preview, lease keeping with a short deadline),
ordering keys, a missing subscription named; the sink round trip. The sample on both
transports. **70 components.**

**Done.** Pub/Sub both ways against the emulator, sign-in proven by the RFC vector and the
fixture, "not yet checked against real Google Cloud" recorded.

**As built (2026-09-24).** Done as planned, with these differences:

- **Each pull is extended at once to `ack_deadline_seconds`.** A pull holds messages for the
  *subscription's* deadline (10 seconds by default), not ours, so the keeper's first
  extension at half of ours would come too late. One `:modifyAckDeadline` per pull closes it.
- **A pull asks for an immediate answer** (`returnImmediately`). Without it Pub/Sub waits "a
  bounded amount of time" it does not state, which could outlast `timeout_ms` on an empty
  subscription. Google discourages the flag: a pull can come back empty while messages
  wait, which ends a batch early. Nothing is lost, and it is written in `connectors.md`.
- **`_delivery_attempt` is null unless the subscription has a dead-letter policy**; only
  then does Pub/Sub count deliveries. A repeat is spotted by `_message_id`.
- **A plain `http://` endpoint signs nothing**, whether it came from `endpoint` or
  `PUBSUB_EMULATOR_HOST`: that is how "an emulator" is recognised, and a token is never sent
  over plain HTTP.
- **`project` may be left out** when the subscription or topic is a full `projects/...` path.
- **The lease keeper moved to `lease.rs`**, shared with SQS, whose tests pass unchanged on it.
- `credentials_file` is a path, not the key's JSON; a key as a `${SECRET:...}` value is not
  offered yet.

###### Phase 10l — RabbitMQ

**Files.** `crates/connectors/src/{rabbitmq.rs, rabbitmq/tests.rs}`; `lapin` added with
`rustls--ring` and `tokio`, default features off; the services script (a RabbitMQ container
with a plain and a TLS listener, certificates made as Kafka's are), `gate.yml`,
`verified.rs`, a sample, `connectors.md`.

**Do.**

1. **`src.queue.rabbitmq`** (decisions 65, 66): `url` (`amqp://` or `amqps://`, user and
   password in it or as `username`/`password`), `vhost`, `queue`, `ca_cert`; `max_records`,
   `max_wait_ms`, `value_format`, `columns`. Classic and quorum queues. `basic.get` until
   empty or `max_records`, **without** acknowledging; the receipt owns the runtime, the
   connection and the channel, and settles with one `basic.ack` (multiple) or `basic.nack`
   (requeue). Rows: `_queue`, `_exchange`, `_routing_key`, `_redelivered`, `_message_id`,
   `_timestamp`, `_headers`.
2. **`snk.queue.rabbitmq`**: `exchange` (empty: the default exchange), `routing_key` or
   `routing_key_column`, `persistent` (default on); publisher confirms awaited per batch.
3. **TLS**: bundled public roots plus `ca_cert`, as Kafka and NATS. Whether `lapin` takes our
   shared `tls.rs` configuration directly is checked first in a scratchpad probe; if not,
   its own `rustls` configuration gets the same roots.
4. **Test services**: `rabbitmq:4.3-alpine` (84 MB), plain and TLS listeners, as
   `ETL_TEST_RABBITMQ` and `ETL_TEST_RABBITMQ_TLS`.

**Verify.** The receipt behaviours (acknowledge, release with `_redelivered` true next time,
preview, a connection closed while holding: the broker requeues, the next run reads them);
`max_records`; a missing queue named; bad credentials named; TLS with `ca_cert`, and
refused without it; the sink round trip through an exchange with routing keys, and publisher
confirms. The sample on both transports. **72 components.**

**Done.** RabbitMQ both ways, plain and TLS, semantics documented.

**As built (2026-09-24).** Done as planned, with these differences:

- **TLS is `tls.rs`'s, as hoped.** `lapin` takes no `rustls` configuration directly, but its
  `Connection::connector` takes a connect function, and `RustlsConnector` is built from a
  `rustls::ClientConfig`; that needs `amq-protocol-tcp`'s `rustls-common` feature, named
  directly. No `aws-lc`, OpenSSL or platform verifier comes in. Found in a scratchpad probe.
- **A multi-threaded `tokio` runtime with one worker**, owned by the receipt, rather than
  NATS's current-thread one: `lapin`'s heartbeat is a task on it, and has to run while the
  engine is busy elsewhere. `lapin`'s I/O loop is a thread of its own.
- **Every call has a deadline, `timeout_ms`.** The probe found a connect to a missing vhost
  never answered by `lapin`, though the broker refused it in 30 ms.
- **Release falls back to closing.** If the `nack` cannot be sent, closing the connection
  gives the messages back all the same, so a release never fails.
- **Sends are `mandatory`**, so an unroutable message fails the run (a returned message
  arrives with its confirm) instead of being dropped by the exchange.
- **Quorum queues count redeliveries in `x-acquired-count`** (RabbitMQ 4), not
  `x-delivery-count`.
- **The engine's tests reach the broker through its management API** (`ETL_TEST_RABBITMQ_HTTP`,
  the plugin enabled in the container), since `verified.rs` cannot use the connector crate's
  AMQP client. Ports 5767x: 55621-56220 were reserved by Windows on this machine.

**Later families**, planned one at a time when reached: NoSQL, warehouses over their own
protocols, vector DBs.

##### Phases 10m–10u — databases and warehouses, one connector at a time

**Questions answered 2026-09-24** (Settled decisions 71–81): all as recommended, except that
**Elasticsearch is not built** (decision 76): its test server needs more memory than the
project will give a test container. **One connector per sub-phase**, each committed and
pushed when green, the website updated after each, and each started only when the user says
so. Oracle is deferred (decision 80). **Cassandra (10s) and Neo4j (10t) were removed from the
plan** (decision 86, the user, 2026-09-24, after 10r); their letters are not reused.

| Phase | Connector | Components | Test server | Checked against |
|---|---|---|---|---|
| 10m | MongoDB | `src.db.mongodb`, `snk.db.mongodb` | `mongo:8.0` (315 MB) | MongoDB itself |
| 10n | Redis | `src.db.redis`, `snk.db.redis`, `src.stream.redis`, `snk.stream.redis` | `redis:8.2-alpine` (28 MB) | Redis itself |
| 10o | BigQuery | `src.warehouse.bigquery`, `snk.warehouse.bigquery` | `goccy/bigquery-emulator` | the emulator; not real Google Cloud |
| 10p | Snowflake | `src.warehouse.snowflake`, `snk.warehouse.snowflake` | none exists: the local fixture | the fixture; not real Snowflake |
| 10q | MariaDB | none new: `src.db.mysql` and `snk.db.mysql` | `mariadb:11.8` (104 MB) | MariaDB itself |
| 10r | ClickHouse | `src.db.clickhouse`, `snk.db.clickhouse` | `clickhouse:25.8` (231 MB) | ClickHouse itself |
| 10u | SQL Server | `src.db.sqlserver`, `snk.db.sqlserver` | none: its image needs 2 GB; the local fixture (decision 87) | the fixture; not real SQL Server |

Components: 72 before 10m; 74 after 10m, 76 after 10o, 78 after 10p and 10q, 80 after 10r,
and 82 after 10u (10n, 10s and 10t are not built).

###### What every one of these shares

1. **Native connectors** in `crates/connectors`, one module and one `tests.rs` each, like
   10a–10l. Blocking where the client allows it; otherwise the NATS and RabbitMQ pattern (a
   small `tokio` runtime of the connector's own, and **a deadline on every call**,
   `timeout_ms`, because 10l found a client that never answers).
2. **A probe first**, in the scratchpad, against the real test server, before any project
   code: the client's behaviour on a wrong password, a missing database or table, TLS, and
   anything the phase's design leans on. What it finds goes in the as-built notes.
3. **Reads are bounded and typed.** Every source has `columns` (the fixed schema, as the
   other native sources) and `max_records`; a document or row that does not fit is an error
   naming it, never a silently dropped field.
4. **Incremental reads use checkpoints** (10e's design), not the DuckDB sources'
   `incremental` block, which applies to DuckDB-lowered sources only: a property
   `incremental_field` (or `incremental_column`) with `start`, the highest value read saved
   only after a fully successful run, and the checkpoint recording the field so a changed
   field starts over. `etl state list` and `forget` see them as they see Kafka's. Documented,
   as for watermarks: a field that can go *down* skips rows.
5. **Sinks write in batches, at-least-once**, and a failure says how many rows landed before
   it. Where the target has keys, an **upsert** mode makes a re-run idempotent.
6. **Secrets and TLS as before.** Passwords and keys as `${SECRET:...}`; TLS through the
   shared `tls.rs` where the client lets us hand it a `rustls` configuration, and the phase's
   probe says so where it does not. Reports and errors name host, database and user, never a
   password.
7. **Test services**: each server is added to `test-services.ps1` in its own phase, with a
   variable `ETL_TEST_<NAME>`, **capped at 1 GB of memory** (decision 79), and in CI's Ubuntu
   gate. Tests skip without it. The engine's tests use each server's own admin surface to
   set up (as 10l used RabbitMQ's management API).
8. **Per phase:** a sample under `samples/pipelines/`, both transports and `preview` in
   `verified.rs`, mutations on the settle and write paths, `connectors.md`, the plan's
   as-built notes, `learnings.md`, `assignments.md`, the tracker, and the website's
   `CLAIMS.md` naming the test that makes it `working` (only for phases checked against the
   real software, decision 78).

###### Phase 10m — MongoDB

**Files.** `crates/connectors/src/{mongodb.rs, mongodb/tests.rs}`; `mongodb` 3.9 with `sync`
and `rustls-tls` (`ring`), default features off where they pull more; the services script
(`mongo:8.0`, a user and a TLS listener with Kafka's CA), `gate.yml`, `verified.rs`, a
sample, `connectors.md`.

**Do.**

1. **`src.db.mongodb`** (decision 73): `uri` (`mongodb://` or `mongodb+srv://`), `username`,
   `password`, `database`, `collection`; `filter` (a JSON query document, Extended JSON),
   `projection`, `sort`; `batch_size`; `max_records` (default unbounded, as the DB sources);
   `incremental_field` with `start` (a checkpoint of the highest value, as point 4); `columns`.
   A document becomes a row: top-level fields to columns, nested documents and arrays as
   JSON, `ObjectId` as its hex text, dates as UTC timestamps, `Decimal128` as text. `_id` is
   always a column.
2. **`snk.db.mongodb`**: `mode` `insert` (`insertMany`, unordered, in batches of 1,000) or
   `upsert` on `key_fields` (`bulkWrite` of `replaceOne` with `upsert`), so a re-run
   replaces rather than duplicates. Duplicate-key errors in `insert` name the row.
3. **TLS**: the driver's own `rustls` configuration takes a CA file; `ca_cert` passes it.

**Verify.** Against MongoDB: a filter and projection; nested fields as JSON; types
(`ObjectId`, dates, decimals); incremental runs reading only newer documents, and a failed
run saving no position; `max_records`; a wrong password and a missing collection named; TLS
with `ca_cert` and refused without; insert and upsert round trips, a re-run of upsert adding
nothing. The sample on both transports and `preview`. **74 components.**

**Done.** MongoDB both ways, incremental, plain and TLS, semantics documented.

###### Phase 10n — Redis

**Files.** `crates/connectors/src/{redis.rs, redis/tests.rs}`; `redis` 1.7 (blocking API,
`streams`, `tls-rustls`); the services script (`redis:8.2-alpine`, a password, a TLS
listener), `gate.yml`, `verified.rs`, a sample, `connectors.md`, the frontend icon if a new
group needs one.

**Do.** (decision 74)

1. **`src.stream.redis`**: a Redis **Stream** read through a **consumer group**,
   `XREADGROUP` in bounded batches, **held until the run's outcome** with 10j's receipts:
   `XACK` on acknowledge; on release the entries stay pending and are claimed again by the
   next run (`XAUTOCLAIM` of this consumer's pending entries first, then new ones). Creates
   the group if asked (`create_group`, from `$` or `0`). Rows: the entry's fields as columns
   (or `value_format` over one field), plus `_stream`, `_id`, `_delivery_count`.
2. **`src.db.redis`**: a **snapshot of keys** matching `pattern`, by `SCAN` (never `KEYS`):
   hashes become rows of their fields, strings a `value` column (JSON if `value_format`
   says so), plus `_key` and `_type`; other types refused by name. `max_records`.
3. **`snk.stream.redis`**: each row one entry, `XADD` in pipelined batches, `maxlen`
   optional (approximate trimming).
4. **`snk.db.redis`**: each row a **hash** at `key_template` (e.g. `order:{order_id}`), or a
   string of the row's JSON; `ttl_seconds` optional; pipelined. Idempotent by key.
5. `url` (`redis://` or `rediss://`), `username`, `password`, `database`, `ca_cert`,
   `timeout_ms`.

**Verify.** Against Redis: a stream acknowledged, released (the next run gets the same
entries, `_delivery_count` 2), `preview` releasing, `max_records` leaving the rest; a
consumer group created from `0`; a key snapshot of hashes and strings, a wrong type named;
the sinks round trip, a re-run of the hash sink adding no keys; a wrong password named; TLS.
The samples on both transports. **78 components.**

**Done.** Redis streams (held) and keys, both ways, semantics documented.

###### Phase 10o — BigQuery

**Files.** `crates/connectors/src/{bigquery.rs, bigquery/tests.rs}`, reusing `gcp.rs` and
`http.rs`; the services script (`ghcr.io/goccy/bigquery-emulator`, pinned), `gate.yml`,
`verified.rs`, a sample, `connectors.md`, a `warehouse` icon.

**Do.** (decision 75)

1. **`src.warehouse.bigquery`**: `project`, `dataset` and `table`, or `query` (GoogleSQL);
   `location`; the Google sign-in set from 10k (`credentials_file`, the variable, gcloud's
   login; none for a plain-`http://` emulator endpoint); `max_records`;
   `incremental_column` with `start`, as a **query parameter**, never pasted into the SQL.
   `jobs.query`, then `getQueryResults` page by page; BigQuery's typed JSON rows converted
   by the result schema (INT64 as integers, NUMERIC as text, TIMESTAMP as UTC timestamps,
   RECORD and REPEATED as JSON). The report names the job and the bytes it processed.
2. **`snk.warehouse.bigquery`**: `mode` `append` or `truncate`; rows as NDJSON in a **load
   job** (free, unlike streaming inserts), one job per run up to a size limit, then more;
   the job polled to completion, its errors named with the row where BigQuery says.
3. The sign-in scope `bigquery`. Everything else as 10k's `Api`.

**Verify.** Against the emulator: a table read and a query read, types, pages, an
incremental second run reading only new rows, a missing table named; the load job
round trip, `truncate` replacing. Against the fixture: sign-in carried, a job still running
polled, a failed job's errors reported. The sample on both transports. **76 components.**

**Done.** BigQuery both ways against the emulator; "not yet checked against real Google
Cloud" recorded.

###### Phase 10p — Snowflake

**Files.** `crates/connectors/src/{snowflake.rs, snowflake/tests.rs}`, reusing `gcp.rs`'s
RS256 (moved to a shared `jwt.rs` if both need it) and `http.rs`; `connectors.md`, a sample
(parameters only, as no server runs it), `verify.rs` not touched.

**Do.** (decision 77)

1. **Sign-in by key pair**: `account`, `user`, `private_key_file` (PKCS#8 PEM), the JWT
   Snowflake's SQL API asks for (`iss` = `ACCOUNT.USER.SHA256:<public key fingerprint>`,
   `sub` = `ACCOUNT.USER`, an hour), sent with `X-Snowflake-Authorization-Token-Type:
   KEYPAIR_JWT`. `role`, `warehouse`, `database`, `schema`. Programmatic access tokens later.
2. **`src.warehouse.snowflake`**: `table` or `query`; `POST /api/v2/statements`, polled while
   it runs, then every result **partition** fetched; rows converted by the result metadata
   (NUMBER with scale as text, TIMESTAMP_* as UTC, VARIANT/OBJECT/ARRAY as JSON);
   `incremental_column` with `start` as a **bind variable**; `max_records`.
3. **`snk.warehouse.snowflake`**: batched `INSERT` with bind variables (arrays of values),
   `mode` `append` or `truncate`; a failed batch says how many rows were inserted before it.
   (`PUT` and `COPY` are not available through the SQL API.)

**Verify.** Against the fixture only: the JWT's claims and fingerprint, checked against a
public-key fingerprint computed independently in the test; a statement polled (`202`, then
`200`); partitions fetched; types; bind variables sent, never interpolated; errors with
Snowflake's `code` and `message` named; the sink's batches. **78 components.**

**Done.** Snowflake both ways against the fixture; "not yet checked against real Snowflake"
recorded, and not marked working on the website.

**As built (2026-09-24).** Done as planned, with these differences:

- **RS256 stays in `gcp.rs`** and Snowflake calls it (`jwt`, `rsa_key`); moving it to a
  `jwt.rs` would have touched 10k's tests for no gain.
- **The fingerprint is proved against `openssl`**: RFC 7515's key through Snowflake's
  documented command (`openssl rsa -pubout -outform DER | openssl dgst -sha256 -binary |
  openssl enc -base64`) gives `b9E8JDWj...uIw=`, and the connector's SubjectPublicKeyInfo,
  built around `ring`'s PKCS#1 public key, hashes to the same.
- **Incremental values are bound as TEXT and cast** (`CAST(? AS <type>)`), the type from the
  result's `rowType`: one path for every type, with no per-type encoding of bound values to
  get wrong unseen, and the saved value readable. The session time zone is UTC so both directions agree.
- **`private_key` (PEM text) beside `private_key_file`**, so a key can be a secret; an
  encrypted key is refused with the command that decrypts it.
- **Submissions carry a `requestId`**, so a retried POST is the same statement.

###### Phase 10q — MariaDB

**Files.** The services script (`mariadb:11.8`), `verified.rs`, `connectors.md` or the DB
section of the docs, the website.

**Do.** Run the existing `src.db.mysql` and `snk.db.mysql` against MariaDB: reads, both write
modes, types (MariaDB's `UUID`, `INET6`, `JSON` as `LONGTEXT`), a wrong password. Fix what
fails. No new component unless the probe shows MariaDB needs one (then `src.db.mariadb`).

**Verify.** The same tests as MySQL's in `verified.rs`, against MariaDB. **78 components.**

**Done.** MariaDB proven through the MySQL components, or given its own if it must be.

**As built (2026-09-24).** No new component: `src.db.mysql` and `snk.db.mysql` read and write
MariaDB 11.8 unchanged, its own types included (`UUID`, `INET6`, `JSON`, `ENUM`, `BIT`,
`YEAR`, `DATETIME(6)`). The round trip, a types test, a masked wrong password and a
timestamp test run against it in `verified.rs`, set up through the extension's
`mysql_execute`. **Found, on MySQL 8.4 as on MariaDB: a table `snk.db.mysql` creates holds
timestamps as `DATETIME`, whole seconds**, because the extension creates the column so; a
table made with `DATETIME(6)` keeps microseconds through `append`. **Fixed the same day**
(open question 16, answered "fix it"): a table the sink creates is created empty, widened to
`DATETIME(6)` by an `ALTER` DuckDB writes at run time from the upstream's `DESCRIBE` (held in
a `SET VARIABLE`, sent with `mysql_execute`), then filled; a table that was already there is
left alone. Proved on both servers, with two mutations each caught. `at` is a reserved word in DuckDB's
SQL too; the tests say `stamp`.

###### Phase 10r — ClickHouse

**Files.** `crates/connectors/src/{clickhouse.rs, clickhouse/tests.rs}` over `http.rs` (the
`clickhouse` crate needs Rust 1.89, above the project's 1.88); the services script
(`clickhouse:25.8`, a user), `gate.yml`, `verified.rs`, a sample, `connectors.md`.

**Do.**

1. **`src.db.clickhouse`**: `url` (the HTTP interface, `http://` or `https://`), `username`,
   `password`, `database`; `table` or `query`; rows streamed as `JSONEachRow`, read line by
   line so memory stays flat; `incremental_column` as a **query parameter**
   (`{name:Type}`); `max_records`.
2. **`snk.db.clickhouse`**: `INSERT ... FORMAT JSONEachRow` in batches of 100,000 rows or
   16 MB; `insert_deduplication_token` per batch, so a retried batch is not inserted twice.
3. Errors: ClickHouse's `Code: N. DB::Exception` text, trimmed and named.

**Verify.** Against ClickHouse: a table and a query read, types (`Decimal`, `DateTime64`,
`Array`, `Nullable`, `LowCardinality`), incremental runs, a missing table and a wrong
password named; the sink round trip and a retried batch deduplicated. **80 components.**

**Done.** ClickHouse both ways, semantics documented.

**As built (2026-09-24).** Done as planned, with these differences:

- **Reads use `JSONCompactEachRowWithNamesAndTypes`**, not `JSONEachRow`: the types line is
  what makes wide integers exact and gives an incremental parameter its type.
- **64-bit and wider integers are asked for quoted** (`output_format_json_quote_64bit_integers`)
  and made numbers by type where they fit: unquoted, a 128-bit value arrived as a number no
  `f64` can hold.
- **An error after the first rows arrives with status 200** as a last row holding the
  exception (the probe's `throwIf` at row 50,000); the reader recognises it and fails.
- **The deduplication token is sent but is not a promise**: a plain MergeTree keeps a
  retried batch twice (seen in the probe); `connectors.md` says which tables deduplicate.
- **`max_records` is a `LIMIT`**, and `start` a SQL literal as for the warehouses.
- **Pushed without CI** (`[skip ci]`), at the user's request.

###### Phase 10u — SQL Server (against a fixture only: decision 87)

**Files.** `crates/connectors/src/{sqlserver.rs, sqlserver/tests.rs}`; `tiberius` with
`rustls`; a local fixture that speaks enough TDS (SQL Server's binary protocol) for the tests,
in `crates/connectors/src/sqlserver/`; a sample against the fixture, `connectors.md`. No test
container and no `gate.yml` service: SQL Server's image needs 2 GB, over decision 79's cap.

**Do.** `src.db.sqlserver` (`table` or `query`, `incremental_column` as a parameter) and
`snk.db.sqlserver` (batched inserts, `mode` `append`, `truncate` or `merge` on `key_columns`
through a staging table); SQL and Windows-free authentication (SQL logins), TLS with
`ca_cert` or `trust_server_certificate` for a test server.

**Verify.** Against the fixture only: sign-in, reads, types (`decimal`, `datetime2`,
`datetimeoffset`, `uniqueidentifier`, `nvarchar(max)`), incremental runs, the three write
modes, a server error named with its number. **82 components.**

**Done.** SQL Server both ways against the fixture; "not yet checked against real SQL Server"
recorded, and it stays off the website's `working` list until it is, as BigQuery and
Snowflake do.

**Question 15 answered** (the user, 2026-09-24): (c), a fixture only (decision 87). Deferring
it, or a separate CI job on a larger runner, were the alternatives. The fixture is larger
than Snowflake's: TDS is a binary protocol, not HTTP.

**As built (2026-09-24).** Done as planned, with these differences:

- **`tiberius` 0.12 with `tds73` and `rustls` only**: not `native-tls` (OpenSSL on Linux) and
  not `winauth`. It brings rustls 0.21 beside the project's 0.23 (the same `ring`), because it
  builds its own TLS configuration and cannot take one from `tls.rs`.
- **TLS trust stays within decision 42**: the client offers a CA file, trust-all, or the
  machine's store; the connector requires `ca_cert` or `trust_server_certificate` to encrypt,
  and with `encryption: none` points the client at a CA file that cannot exist, so a server
  insisting on TLS is refused, not trusted through the store. `encryption` is `required`
  (the client's `Required`, since its `On` panics against a server that refuses),
  `login_only` or `none`.
- **The fixture** (`sqlserver/fixture.rs`) speaks PRELOGIN, TLS inside PRELOGIN packets
  (rustls 0.23 on the server side, with `tests/fixtures/sqlserver/`'s CA and certificate),
  LOGIN7 with its refusals (18456, 4060), SQL batches, `sp_executesql` calls, COLMETADATA and
  ROW for twenty types, errors, DONE tokens, and Azure SQL's routing ENVCHANGE. `tiberius`
  decodes all of it, so a wrong encoding fails a test.
- **Incremental positions keep the column's full precision**: the saved text is the value as
  SQL Server casts it back (`datetime2(7)` to seven digits, `datetime` as `.003`), bound as a
  parameter and `CAST` to the type the value came with.
- **Every value is bound as text**, as Snowflake's are; `merge` stages into `#etl_stage`
  (made by a plain batch, since one made in `sp_executesql` dies with the call), then one
  `MERGE ... WITH (HOLDLOCK)` with `ROW_NUMBER()` so the later of two rows with one key wins.
- **Beyond the plan**: Azure SQL's redirect followed once; a `tiberius` panic on a column it
  cannot read (`sql_variant`, `geography`) caught and made an error naming a `CAST`; a second
  result set, a duplicate or unnamed column refused; SQL Server's error 1033 explained when
  `incremental_column` wraps a query with its own `ORDER BY`.
- **Not in `verified.rs`**, as no server runs; `samples/pipelines/sqlserver_orders.json` is
  for a real server, and `etl validate` passes it.

**Deferred.** **Elasticsearch** (decision 76: memory), **Oracle** (decision 80: Oracle's
native client library would break the single binary), OpenSearch with Elasticsearch.
**Later families**, planned when reached: the site's remaining warehouses (Redshift,
Databricks, DuckDB), file formats (TSV, Arrow, Avro), object storage (GCS, Azure Blob, real
S3), and named SaaS connectors.

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

#### Phases 11a–11d — planned 2026-09-24

**Questions answered 2026-09-24** (Settled decisions 91–98), all as recommended. **Four
sub-phases**, each committed and pushed when green and started only when the user says so;
MCP first, because it needs no model and is useful at once with Claude Code, and it gives the
assistant a tested surface to stand on.

| Phase | What | Needs a model |
|---|---|---|
| 11a | The MCP server: `etl mcp` over stdio | no |
| 11b | The local model and grammar-constrained pipeline JSON | yes, on this machine only |
| 11c | The chat panel on the canvas | yes |
| 11d | The six `xf.ai.*` transforms: scope decided when it starts (decision 98) | some |

**This machine** (2026-09-24): 16 GB of memory, an i7-8665U (4 cores), no usable GPU. A
local model runs on the CPU, estimated at 10–20 tokens a second, so a generated pipeline takes
a minute or two. The manifest (`etl components --manifest`) is 156 KB: too much to put in a
small model's prompt, which is why the output is constrained by a grammar rather than
described.

##### Phase 11a — the MCP server

**Files.** `crates/mcp/` (a library: the tools, over the engine the CLI already uses), `etl
mcp` in `crates/cli`; `rmcp` 3.4 (the official Rust SDK, Rust 1.88) with `server`,
`transport-io` and `macros`; `docs/mcp.md`; a sample `.mcp.json` for Claude Code.

**Do.**

1. **`etl mcp`**: an MCP server on **stdin/stdout only** (decision 92): Claude Code or any
   agent starts it as a subprocess; nothing listens on a port. `--workspace` as the other
   commands take it. Logs go to stderr, never stdout, which is the protocol's.
2. **Tools** (decision 93: everything the plan lists, gated by the agent's own permission
   prompts): `list_components` (ids, labels, one line each) and `get_component` (one
   component's full manifest entry); `get_schema` (the pipeline document's JSON Schema, the
   one 11b constrains the model with); `validate_pipeline` (a document or a file, with the
   CLI's messages); `create_pipeline` (validate, then write under the workspace, never
   outside it, never over a file without `overwrite: true`); `run_pipeline` (with
   parameters and a context, returning the run's summary and id); `list_runs` and
   `get_run_log`; `plan_pipeline` (the SQL); `lineage`; `build_executable` (a target from
   the CLI's list); `list_connections` (contexts, and secrets **by name only**).
3. **Secrets never leave**: no tool returns a secret's value; results pass through the same
   masking the CLI's messages do.
4. **Errors are results**: a pipeline that fails validation or a run that fails is a tool
   result the agent can read and act on, not a protocol error.

**Verify.** Each tool through `rmcp`'s client against `etl mcp` as a subprocess, in the
test suite (CI too: no model); a secret set in the workspace never appearing in any result;
a `create_pipeline` path outside the workspace refused; **Claude Code drives a run over MCP**
(by hand, recorded in the as-built notes).

**Done.** An agent can find components, write, validate, run and build a pipeline, and read
what happened, through `etl mcp`.

**As built (2026-09-24).** Done as planned, with these differences:

- **`crates/mcp` knows MCP and not the engine**: a `Workspace` trait, as `etl-console` has,
  implemented in `etl`'s `main.rs` (`McpWorkspace`), which reuses the console's listing,
  history and one-run-at-a-time lock. Blocking work runs on `spawn_blocking`.
- **Thirteen tools**: the plan's list, with `list_pipelines` added and "read logs" as
  `list_runs` and `get_run_log`. `list_connections` lists contexts (variable names only) and
  secrets (names and descriptions); there is no tool that sets a secret, since the agent
  would have had to see its value.
- **`build_executable` never bakes a secret in** (`etl build --allow-secrets` is the way, on
  purpose), and makes the output's folder. `etl build`'s work moved into `build_artifact`,
  loud for the CLI and quiet for MCP, the CLI's output unchanged.
- **The schema lives in `etl-metadata`** (`schema.rs`): each node tied to one component, its
  type the canvas kind, its properties typed and closed, `${...}` allowed for any value.
- **Messages that were lost are kept**: `load_and_compile_quietly` now returns the
  resolver's own reason, and a pipeline file that cannot be read is named, for the console
  too.
- **No `.mcp.json` in the repo**: Claude Code would pick it up in every session here;
  `docs/mcp.md` has the snippet.
- **Verified** by `rmcp`'s client against a fake workspace (9 tests) and against `etl mcp`
  as a subprocess (4 tests: write, check, plan, run, read, lineage and build of a real
  pipeline whose executable then runs; a secret's value absent from every result; an
  invalid document not written; the schema matching the registry).

##### Phase 11b — the local model and grammar-constrained output

**Files.** `crates/assistant/`; `scripts/fetch-model.ps1` (decision 95: `llama-server` from
llama.cpp's releases and **Qwen2.5-Coder-1.5B-Instruct Q4_K_M**, about 1 GB, into the
git-ignored `tools/`, as `fetch-duckdb.ps1` does); `etl assist "<request>"` in the CLI.

**Do.** Start `llama-server` as a subprocess on a localhost port of its own, and stop it
after. **The JSON Schema from the manifest** (11a's `get_schema`) is passed as the request's
`json_schema`; `llama-server` turns it into a GBNF grammar, so every token keeps the output
a valid document (decision 96). A short prompt with the components likeliest to matter,
picked by the request's words from the manifest. The result goes through `validate` before
anyone sees it. The model is a setting, so another GGUF file can be pointed at.

**Verify.** In CI, without a model: the schema generated from the manifest accepts every
sample in `samples/pipelines/` and refuses broken ones; the prompt builder's choices. **On
this machine only** (decision 97), a test that skips without the model: "read this Postgres
table, dedupe, write Parquet" passes `validate` on the first try in **9 of 10** runs.

**Done.** `etl assist` writes pipelines that validate, locally, with no network.

##### Phase 11c — the chat panel

**Files.** `frontend/` (a panel beside the canvas), the console's API (`etl serve`) for the
desktop app and the browser alike.

**Do.** Ask in words; the assistant's pipeline appears on the canvas as a draft to accept or
discard; validation messages shown as the canvas shows them. Planned in detail when 11b is
done.

##### Phase 11d — the `xf.ai.*` transforms

Scope decided when it starts (decision 98). The plan's six: three fully local (embeddings,
chunk, PII redact) and three with the user's own OpenAI-compatible endpoint (`baseUrl`, the
key a secret).

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
