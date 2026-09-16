# Building Your Own "Duckle": A Verified Teardown of the Project and a Phased Build Plan for Arun

## TL;DR
- **Duckle is real, substantial, and mostly matches the creator's claims**: a Rust + React + Tauri 2 desktop ETL/ELT studio that compiles a visual DAG of ~385 components to SQL and runs it by shelling out to the DuckDB CLI. It is dual-licensed MIT OR Apache-2.0 (not MIT-only), sits at ~1.3k GitHub stars with 1,507 commits, and the visible version is v0.7.0 (the "0.5.x" in the brief is stale). One important correction: DuckDB is invoked as an external CLI binary, not linked via `duckdb-rs`.
- **The smallest viable clone is a CLI runner**: a Rust crate that parses a `{formatVersion, nodes, edges}` JSON pipeline, topologically sorts it, lowers each node to DuckDB SQL (views for most stages, `ATTACH` for databases, `COPY ... TO` for sinks), and shells out to `duckdb`. Everything else — visual canvas, 385 connectors, AI assistant, standalone-binary export, distributed execution — is an increment on top of that core.
- **Arun's existing work is a genuine differentiator, not overlap**: his DataFusion/Arrow-Flight lakehouse engine, PyIceberg readers, and Argo Workflows cost-observability project let him beat Duckle on the two things it is weakest at — a real native (non-CLI) execution path and true distributed/scheduled execution — rather than re-cloning a 385-connector desktop app he can't sustain solo.


## Local reference checkout

The Duckle source is checked out locally at **`D:\workspace\duckle-main`** — a Cargo workspace
(`version = "0.0.1"`), dual-licensed MIT OR Apache-2.0 (`LICENSE-MIT` and `LICENSE-APACHE` both
present). Use it as the primary source for anything this report marks as inferred rather than
file-verified.

Top level: `apps/ benchmarks/ branding/ crates/ docs/ examples/ frontend/ marketing/ packaging/
samples/ scripts/ website/`, plus `CONTRIBUTING.md`, `Cargo.toml`, `rust-toolchain.toml`,
`build.cmd`, `dev.cmd`, `dev.ps1`, `Dockerfile.web`, `docker-compose.web.yml`.

**Crates actually present (16)** — more than the Architecture section below lists:

```
connectors        duckdb-engine     duckle-gpu        duckle-lance
duckle-mcp        duckle-runner     duckle-secrets    execution-core
metadata          plugin-sdk        runtime           scheduler
slothdb-engine    stream-engine     transform-engine  workflow-engine
```

Seven were not described in this report and need reading before planning depends on them:
`duckle-secrets`, `execution-core`, `runtime`, `scheduler`, `slothdb-engine` (SlothDB vendored
in-tree as an alternate engine), `stream-engine`, `workflow-engine`. Their existence suggests the
engine/scheduling layering is richer than the "topo sort in `duckdb-engine`" picture below.

> **Reference only.** Reading the checkout does not change the clean-room stance in *Caveats →
> Licensing*: take architecture and behaviour, not source, manifests, or docs.

## Key Findings

**What Duckle actually is.** Duckle (org `slothflowlabs`, author Sourav Roy / `SouravRoy-ETL`, site duckle.org) is an open-source, local-first ETL/ELT studio. It is a Cargo workspace paired with a Tauri 2 + React 19 desktop app. You build a pipeline by dragging nodes (sources, transforms, quality checks, control-flow, sinks) onto a ReactFlow canvas; Duckle compiles that graph into an ordered set of DuckDB SQL statements and executes them, showing per-node row counts, timings, live previews, and the generated SQL on a "Plan" tab. It ships as a single ~65 MB desktop binary; the DuckDB engine and extensions are downloaded on first launch. It is genuinely popular for a young project (~1.3k stars, 99 forks, 1,507 commits, 56 open issues, an active PR stream from outside contributors like LouisDeconinck, and a test suite of ~2,158 passing tests).

**Verification of the creator's specific claims:**
- *"Rust and React, DuckDB vectorized execution"* — **True.** Rust workspace + React 19 frontend + Tauri 2 shell; DuckDB is the default engine.
- *"350+ / 385 components"* — **True and then some.** The component reference lists 380+ across six namespaces: `src.*` (sources), `xf.*` (transforms), `snk.*` (sinks), `qa.*` (data-quality validators), `ctl.*` (control-flow), `code.*` (code runners). Counts drift between docs pages (this is the source of the "329 / 360 / 380 / 385" inconsistency), so treat any single number as approximate.
- *"Run anywhere, no install/cloud"* — **True** for the desktop app and headless runner; note the Linux desktop binary needs WebKitGTK/GTK, so servers use the headless `duckle-runner`, not the GUI binary.
- *"Portable binary: design on one OS, compile to a standalone executable for another"* — **True.** The "Build Pipeline" dialog has a Target OS selector and can cross-build a Linux server file from any host; the exported artifact bundles engine + DuckDB CLI + needed extensions + the resolved pipeline into one self-contained file.
- *"Benchmarks: 1M parquet read+transform ≈2s, write ≈7s"* — **Not directly verifiable from a primary doc.** The public, reproducible benchmark the creator published is a different one: 96 million rows of TPC-H lineitem (~14 GB) out of Postgres to Parquet in 39.9s on a laptop, with a harness in `benchmarks/` that verifies row counts and checksums before granting a time. The specific 1M-record figures appear only in the creator's promotional post, not in the repo's benchmark harness, so treat them as vendor-reported.
- *"Embedded AI assistant Ducky + MCP"* — **True**, spelled **Duckie**. Runs Qwen2.5-Coder-1.5B locally via llama.cpp (a `llama-server` subprocess on an OpenAI-compatible localhost API, ~1.1 GB download). Qwen2.5-Coder-1.5B was released October 2024 by Alibaba Cloud's Qwen team under Apache 2.0, scores 43.3% on HumanEval, and runs in roughly 1.2 GB at Q4 quantization — consistent with the download size. It streams valid pipeline JSON that you insert on the canvas. A separate bundled MCP server exposes ~8 tools (list components, get schema, create/validate/run pipeline, read logs, build executable, manage connections) to Claude/Cursor.
- *"Connectors implemented directly in Rust beyond DuckDB's native capabilities"* — **True.** Many sources are pure DuckDB SQL (Parquet/CSV/JSON, Postgres/MySQL via extensions, Iceberg/Delta/DuckLake, httpfs/S3), but streaming brokers (Kafka, NATS, Pub/Sub, RabbitMQ, Kinesis), SaaS REST/GraphQL APIs, NoSQL (Mongo, Cassandra, Elastic, DynamoDB), warehouses reached over their own protocols (Databricks SQL Statement Execution, Snowflake SQL API), and vector DBs are implemented in Rust in a `connectors` crate.
- *"Pipelines as JSON, git-native, no proprietary formats"* — **True.** A workspace is a folder of plain JSON under `pipelines/`, `connections/` (encrypted), `contexts/`, `routines/`. Pipeline files are `{ "formatVersion": N, "nodes": [...], "edges": [...] }`.
- *"MIT licensed, v0.5.x"* — **Partially outdated.** It is dual-licensed **MIT OR Apache-2.0**, and the current release line is **v0.7.0**.
- *"Distributed execution via Quack"* — **Aspirational on both sides.** Quack is DuckDB Labs' real new client-server protocol, announced May 12, 2026 as a beta and requiring DuckDB v1.5.2+ (v1.5.2 itself shipped April 13, 2026). Crucially, Quack is a **multi-writer client-server protocol over HTTP, not distributed query processing** — DuckDB has stated it plans a production-ready Quack with DuckDB 2.0 later in 2026. Duckle can *read* a Quack source/sink, but Duckle's own "Native" distributed engine is listed as *planned*, not shipped.

**The creator's other repositories** (username `SouravRoy-ETL`; note the distinct `sourav234698`/`Sourav692`/`souravas` accounts are different people):
- **`slothdb`** — the most important sibling project. A from-scratch **C++20** embedded columnar SQL engine (573 commits, ~409 stars, MIT), explicitly a "DuckDB alternative": queries Parquet/CSV/JSON/Arrow/Avro/SQLite/Excel in-process, vectorized (2,048 values/batch), morsel-driven parallelism, compiles to a ~1–4 MB native binary or a ~1.3 MB WASM bundle, has Python/Node/C bindings, a `.ask` natural-language-to-SQL REPL (rules parser + local Qwen tiers), and a ClickBench harness. **Duckle can use SlothDB as an alternate, per-pipeline execution engine.** Lesson for Arun: this is where the creator learned vectorized execution, Parquet decoding, WASM builds, and honest benchmarking — directly relevant to Arun's own DataFusion engine. Note SlothDB is candidly self-labelled "experimental / a learning exercise," with no distributed execution, no MVCC/multi-writer, and partial window-function coverage.
- **`duckle`** (personal mirror/fork of the org repo) — same codebase; its `CONTRIBUTING.md` is the clearest guide to the crate layout and "how to add a component."
- The creator's public writing (Medium, the `souravetl.netlify.app` portfolio) frames him as a Qlik/Talend solution architect who built SlothDB "as a love letter to DuckDB's design," which explains Duckle's Talend-migration importer and enterprise-ETL framing.

**Architecture, verified at the code level (via DeepWiki's code index and the repo's own docs/CONTRIBUTING):**
- **Repo layout:** `apps/desktop` (Tauri shell), `crates/` (Rust workspace), `frontend/` (React/ReactFlow), plus `benchmarks/`, `packaging/`, `docs/`, `website/`, `samples/`, `examples/starter-workspace`.
- **Crates:** `duckdb-engine` (topological sort + SQL lowering + execution; `crates/duckdb-engine/src/lib.rs` and `plan.rs`), `duckle-runner` (headless CLI + `serve` web console + RBAC + scheduler), `duckle-mcp` (MCP server), `duckle-lance` (LanceDB/Lance + Arrow sidecar), `duckle-gpu` (GPU crate, excluded from default test runs), `plugin-sdk` (the `Transform` trait for custom transforms), `transform-engine` (in-process Rust transform ops under `src/ops/`), `connectors` (registered in `crates/connectors/src/lib.rs`), and `metadata`.
- **DuckDB integration:** Duckle **shells out to the official DuckDB CLI** (e.g. `duckdb -json -c "<sql>"`), located via `DUCKLE_DUCKDB_BIN` or PATH — it does **not** statically link `duckdb-rs`. This keeps the app binary small and the engine swappable. The Python package depends on the `duckdb-cli` wheel published by the DuckDB Foundation so it works offline. It tracks DuckDB v1.5.x (docs reference v1.5.3; test PRs pin v1.5.4). For context, the current DuckDB stable at the report date is v1.5.5 (July 21, 2026), alongside the LTS line v1.4.5 "Andium" (June 17, 2026).
- **Extension bundling:** on first run it pre-fetches `httpfs, azure, sqlite, postgres, mysql, excel, iceberg, delta, ducklake, vss, fts`; `spatial` (~50 MB GDAL) is lazy-loaded on first geospatial use. In restricted/air-gapped mode the engine emits **LOAD-only preludes** and rejects raw `INSTALL` SQL (a security PR guards every CLI entry point). Exported build artifacts embed the extensions.
- **SQL compilation / execution:** the graph is topologically sorted into a numbered 1..N plan; **most non-sink stages compile to `CREATE VIEW`** (lazy — nothing computes until a sink pulls), some materialize as `CREATE OR REPLACE TABLE`; database sources use `ATTACH '...' AS duckle_src` and query in place, then detach; sinks become `COPY ... TO`. Concurrency auto-detects from CPU cores; quality nodes have a second "reject" output for dead-letter rows.
- **Materialization:** a per-node **Materialize** setting with `auto` / `view` (lazy) / `memory` (read once into a RAM table) / `disk` (stream through a temp Parquet file for huge intermediates). Default is one fast batched pass; flip a node to on-disk for a safe checkpoint.
- **Frontend:** ReactFlow (`@xyflow/react`) canvas; node config forms are generated from a component manifest (`frontend/src/workflow-ui/fields/manifest-synth.ts`); node modules under `frontend/src/canvas/nodes/`. Frontend↔backend is Tauri IPC commands. (State management is very likely Zustand — xyflow's built-in store — but I could not confirm this from `package.json`.)
- **Standalone binary export:** the headless runner is embedded into the app at build time; "Build Pipeline" produces one self-contained executable containing the runner, the DuckDB CLI, needed extensions, and the resolved pipeline JSON. Cross-OS builds are supported via a Target OS selector (Linux server file cross-built from any host).
- **Secrets/config:** connections are AES-256-GCM encrypted at rest with a per-workspace key under `.duckle/keys/`; fields are parameterized with `${VAR}`, `${ContextName.VAR}`, `${ENV:KEY}`, plus built-ins like `${workspace}`, `${date}`.
- **AI:** Duckie = Qwen2.5-Coder-1.5B via llama.cpp; six AI transforms (`xf.ai.*`) — three fully local (embeddings, chunk, PII redact), three call any OpenAI-compatible endpoint (bring-your-own-model via `baseUrl`).
- **Maturity/gaps:** strong test count (~2,158 passing), active external contributors, dual license, honest self-benchmarking. Weaknesses/risks visible in issues: no versioned/migratable workspace format yet (issue #299), `validate` doesn't catch every missing required property, the "Native" streaming engine and distributed execution are unshipped, and the CLI-shell-out model means correctness/perf depend on an external DuckDB binary and per-call process overhead.

## Details

### How Duckle works, end to end
1. A **workspace** is a git-friendly folder of plain JSON. A pipeline is `{ formatVersion, nodes[], edges[] }`. Each node has an `id`, a namespaced `type` (`src.file.csv`, `xf.map`, `snk.parquet`, `qa.not_null`, `ctl.foreach`, `code.sql`), a `position`, and a parameter object; edges wire an output port (e.g. `source.main`) to a downstream input, with a `reject` port on quality nodes. (The exact JSON key for parameters — `params` vs `config` vs `data` — I could not confirm from source; the ReactFlow convention is `data`.)
2. On **Run**, `duckdb-engine` topologically sorts the DAG into ordered SQL stages, lowers each node to SQL (mostly `CREATE VIEW`; `ATTACH` for DB sources; `COPY ... TO` for sinks), and executes by shelling out to the DuckDB CLI in JSON mode. Intermediate results can be materialized to memory or a temp Parquet file per the node's Materialize setting.
3. **Production** uses `duckle-runner` headlessly (cron/interval/file-watch schedules, watermark incremental loads, a web console with a shared-token/RBAC model, audit trail). "Build Pipeline" exports a self-contained cross-OS binary for CI/air-gapped deployment.
4. **AI**: Duckie generates pipeline JSON locally; the MCP server lets external agents drive the whole studio.

### Draft pipeline JSON schema (for your clone)
```json
{
  "formatVersion": 1,
  "name": "orders_to_iceberg",
  "params": { "since": "2026-01-01" },
  "nodes": [
    { "id": "n1", "type": "src.file.parquet", "position": {"x":0,"y":0},
      "config": { "path": "${workspace}/data/orders/*.parquet" } },
    { "id": "n2", "type": "xf.filter", "position": {"x":250,"y":0},
      "config": { "predicate": "order_ts >= '${params.since}'" } },
    { "id": "n3", "type": "xf.dedup", "position": {"x":500,"y":0},
      "config": { "keys": ["order_id"], "keep": "latest", "order_by": "order_ts" } },
    { "id": "n4", "type": "snk.iceberg", "position": {"x":750,"y":0},
      "config": { "table": "warehouse.orders", "mode": "upsert",
                  "connection": "${ENV:ICEBERG_CATALOG}", "materialize": "auto" } }
  ],
  "edges": [
    { "from": "n1.main", "to": "n2.in" },
    { "from": "n2.main", "to": "n3.in" },
    { "from": "n3.main", "to": "n4.in" }
  ]
}
```

### Proposed repo/crate layout (Cargo workspace)
```
apps/desktop/           # Tauri 2 shell (later phase)
frontend/               # React 19 + @xyflow/react + Zustand + Vite
crates/
  spec/                 # serde structs + JSON Schema for pipeline files
  planner/              # DAG validation, topo sort (petgraph), lineage
  engine/               # Engine trait (compile -> plan; execute)
  engine-duckdb/        # duckdb-rs (bundled) implementation
  engine-datafusion/    # DataFusion + Arrow (your differentiator)
  connectors/           # source/sink impls + registry
  transform-engine/     # in-process Rust transforms
  plugin-sdk/           # public traits for third-party components
  runner/               # headless CLI + scheduler + serve
  mcp/                  # MCP server
  distributed/          # Argo Workflow compiler / Arrow Flight fan-out
```

### The comparable-tools landscape (for design lessons only)
- **dlt** — Python-first EL with schema inference/normalization; the gold standard for *connector ergonomics* and incremental loading. Lesson: adopt its "declarative source + state/watermark" model.
- **dbt-duckdb / SQLMesh** — SQL transformation, lineage, tests, virtual data environments. SQLMesh's column-level lineage and blue/green virtual environments are worth stealing. Duckle overlaps here with its `xf.dbt` node.
- **Dagster** — asset-based orchestration and lineage; the right mental model if Arun leans on Argo for scheduling.
- **Apache Hop / NiFi / KNIME / Talend** — the visual-ETL heritage Duckle is explicitly chasing (it even ships a Talend importer). Lesson: visual breadth is a maintenance sink; 385 connectors is why Duckle needs outside contributors.
- **DuckLake / Quack / Arrow Flight** — the distributed-DuckDB frontier. Notably, DuckDB Labs deliberately chose *not* to use Arrow Flight SQL for Quack, preferring to build straight on HTTP and serialize DuckDB's native vectors — but Arrow Flight remains the pragmatic choice for a *DataFusion*-based engine like Arun's.

### Where Arun's existing work plugs in
- **DataFusion + Arrow Flight lakehouse engine** → this is Arun's answer to Duckle's biggest weakness. Instead of shelling out to a DuckDB CLI, he can offer a **real embedded/native engine** (DataFusion) with an **Arrow Flight** data path, giving zero-copy streaming and a genuine server mode that Duckle's CLI model can't match. He can still keep DuckDB as an alternate engine for SQL-compatibility breadth.
- **PyIceberg + DuckDB + Polars distributed reader** → a ready-made Iceberg source/sink connector with snapshot/time-travel reads, which Duckle only does through DuckDB's `iceberg` extension.
- **PySpark + DuckDB enrichment on large Iceberg tables** → real-world pipeline patterns (upsert/merge, SCD, watermarking) to validate the transform library against.
- **Argo Workflows cost-observability on EKS** → Arun's distributed-execution and scheduling story. Rather than inventing a "Native distributed engine" (Duckle's unshipped promise), he can compile a pipeline DAG into an **Argo Workflow** and get distributed, observable, cost-attributed execution on Kubernetes for free.

## Recommendations

**Overall stance:** Do **not** clone Duckle feature-for-feature — its 385 connectors and desktop polish are a multi-person, multi-year effort you cannot match solo. Instead, build a **lean, engine-pluggable, lakehouse-native pipeline runner** where your DataFusion/Arrow-Flight/Iceberg/Argo assets are the core differentiators, and adopt Duckle's genuinely good ideas (JSON DAG, compile-to-SQL, git-native workspace, compile-to-binary, MCP). Reuse *ideas and architecture*, not code, to stay clean-room (see Licensing).

**Recommended stack:**
- **Core language:** Rust (Cargo workspace), matching Duckle and letting you reuse the Rust Iceberg/DataFusion ecosystem.
- **Execution engine:** default to an **embedded DuckDB via `duckdb-rs` (bundled feature)** for MVP speed and SQL breadth — but architect an `Engine` trait so DataFusion (native, Arrow-Flight-capable) and DuckDB are interchangeable. This is a deliberate improvement over Duckle's CLI-shell-out (no per-call process spawn, in-process Arrow).
- **Frontend (later phases):** React 19 + `@xyflow/react` + Zustand + Vite, wrapped in **Tauri 2** for the desktop shell. This mirrors Duckle because the stack is genuinely the right one.
- **AI:** llama.cpp + a small local coding model with **GBNF grammar-constrained JSON output** (Duckle's Qwen approach; the grammar constraint is what makes local models reliably emit valid pipeline JSON) plus an MCP server.
- **Distributed:** compile to **Argo Workflows** (your existing expertise) and/or Arrow Flight worker fan-out — your distributed story, not Quack.

**Phased roadmap** (effort estimates assume one experienced engineer, part-time):

**Phase 0 — Pipeline spec + CLI runner (MVP). ~3–4 weeks.**
- *Goal:* `mytool run pipeline.json` reads a JSON DAG, topo-sorts it, compiles to DuckDB SQL, executes, prints per-node row counts.
- *Modules/crates:* `spec` (serde structs + JSON Schema for `{formatVersion, nodes, edges}`), `planner` (DAG validation, topological sort via `petgraph`), `engine-duckdb` (SQL lowering + execution via `duckdb-rs` bundled), `cli` (using `clap`).
- *Connectors (handful):* `src.file.csv`, `src.file.parquet`, `src.db.postgres` (ATTACH), `xf.sql` (raw SQL), `xf.filter`, `xf.join`, `snk.file.parquet`, `snk.db.postgres`.
- *Suggested crates:* `serde`/`serde_json`, `schemars` (JSON Schema), `petgraph`, `duckdb` (rs), `clap`, `anyhow`/`thiserror`, `tracing` (logging/lineage).
- *Acceptance:* a CSV→filter→join→Parquet pipeline runs end-to-end; `validate` compiles without executing; exit codes stable; a golden-file test per connector.

**Phase 1 — Engine abstraction + Iceberg/DataFusion. ~4–6 weeks.**
- *Goal:* pluggable `Engine` trait; add a DataFusion engine and an Iceberg source/sink using your PyIceberg/Rust-Iceberg knowledge.
- *Tasks:* define `Engine::compile(plan) -> ExecutablePlan` and `Engine::execute`; implement `src.iceberg` (snapshot/time-travel), `snk.iceberg` (append/upsert); wire Arrow as the in-process interchange.
- *Acceptance:* the same pipeline JSON runs on either engine; an Iceberg time-travel read + merge-upsert passes a row-count/checksum test (borrow Duckle's "verify the output, not just the timing" benchmark discipline).

**Phase 2 — Materialization, incremental loads, logging/lineage. ~3–4 weeks.**
- *Goal:* per-node materialization (`auto/view/memory/disk`), watermark incremental loading, structured run logs + column/table lineage.
- *Acceptance:* a watermarked incremental load advances state only on full success; lineage graph emitted as JSON; memory-limit setting respected.

**Phase 3 — Visual editor (Tauri + ReactFlow). ~6–8 weeks.**
- *Goal:* drag-and-drop canvas that reads/writes the same JSON, live preview, generated-SQL "Plan" tab, node property forms generated from a component manifest.
- *Tasks:* Tauri 2 shell, `@xyflow/react` canvas, Zustand store, manifest-driven forms, Tauri IPC to the Rust core.
- *Acceptance:* build/run/preview a pipeline entirely in the GUI; round-trips to JSON with no proprietary fields.

**Phase 4 — Standalone binary export + air-gapped packaging. ~3–4 weeks.**
- *Goal:* "build pipeline" → one self-contained executable embedding runner + engine + resolved pipeline; offline extension bundling.
- *Tasks:* embed pipeline JSON via `include_bytes!`/build script; prebuilt per-target runner templates; cross-compile with `cargo-zigbuild`/`cross`; bundle DuckDB extensions for LOAD-only offline use.
- *Acceptance:* a Linux server binary cross-built from macOS runs with no network and no runtime deps.

**Phase 5 — Component library expansion + plugin SDK. Ongoing.**
- *Goal:* a `Transform`/`Connector` trait + registry so connectors are declarative specs + code; community-addable.
- *Acceptance:* adding a connector is "implement trait + register + add manifest + integration test," documented like Duckle's CONTRIBUTING.

**Phase 6 — AI assistant + MCP. ~4–6 weeks.**
- *Goal:* local model generates pipeline JSON with grammar-constrained decoding; MCP server exposes list/create/validate/run/build tools.
- *Acceptance:* "read this Postgres table, dedupe, write Iceberg" produces a valid pipeline that passes `validate`; Claude/Cursor can drive it over MCP.

**Phase 7 — Distributed execution via Argo / Arrow Flight. ~6–10 weeks.**
- *Goal:* compile a pipeline DAG into an Argo Workflow (one node/stage = one step, with cost labels from your EKS observability work) and/or fan out partitions to Arrow Flight workers.
- *Acceptance:* a partitioned Iceberg read runs as a parallel Argo Workflow on EKS with per-step cost attribution.

**Benchmarks/thresholds that change the plan:** if single-node DuckDB handles your target datasets (tens of GB) within latency budget, **defer Phases 1 and 7** — the native engine and distributed execution only pay off when you're either memory-bound on one box or need multi-writer/parallel scale. If you find yourself adding more than ~20 connectors by hand, invest early in Phase 5's plugin SDK.

### Key technical risks and mitigations
- **DuckDB extension bundling offline** — pre-fetch and vendor the exact extension `.duckdb_extension` files per platform; ship a LOAD-only prelude and forbid `INSTALL` at runtime (exactly Duckle's air-gapped model). Pin the DuckDB version to avoid extension-ABI drift.
- **Cross-compilation** — use `cargo-zigbuild`/`cross` and per-target prebuilt runner templates; the hard part is bundling the right native DuckDB/extension binaries per target, so build a matrix in CI early.
- **Connector breadth** — don't hand-write 385; ship ~15 great ones and a plugin SDK. Lean on DuckDB extensions (postgres/mysql/iceberg/delta/httpfs) for the SQL-reachable sources so you write Rust only for streaming/SaaS/NoSQL.
- **Streaming semantics in a batch engine** — Duckle "supports" Kafka/RabbitMQ by micro-batching into a bounded read, not true streaming. Be explicit: model streaming sources as windowed/bounded batch reads with a watermark, and document the semantics rather than implying continuous streaming.
- **Memory limits** — expose DuckDB's `memory_limit` and `temp_directory` per run, default to spill-to-disk, and use the `disk` materialization mode for large intermediates. DataFusion's streaming operators help here versus DuckDB's more memory-hungry defaults.

### Differentiation ideas (where you can beat Duckle)
1. **True native engine + server mode** via DataFusion + Arrow Flight — no external CLI process, zero-copy Arrow, real concurrency. This is Duckle's structural weakness.
2. **First-class lakehouse** — Iceberg/Delta as native, catalog-aware sources with snapshot isolation, time-travel diffs, and maintenance (compaction/expire) as `ctl.*` nodes, using your Rust-Iceberg work rather than a generic extension.
3. **Distributed + cost-observable execution** on Kubernetes via Argo, with per-stage cost attribution from your EKS observability project — a genuine "distributed Duckle" that actually ships.
4. **Versioned, migratable workspace format from day one** (Duckle's own open issue #299) — publish JSON Schemas and deterministic migrations so pipelines survive upgrades.
5. **Deterministic, checksum-verified benchmarks** baked into CI (copy SlothDB/Duckle's honesty discipline) as a trust signal.

## Caveats
- **Documentation numbers are inconsistent.** Component counts vary by page (329/360/380/385) and the star count and version drift over time; I've flagged the ranges rather than pick one. The "1M records in 2s/7s" figures are vendor-reported and not in the public benchmark harness (which instead documents 96M rows Postgres→Parquet in 39.9s).
- **Some internals are inferred, not file-verified.** DeepWiki's code index and the repo's CONTRIBUTING/docs are strong primary sources, but I could not open every source file directly; specifically the exact JSON key for node parameters, and the frontend's use of Zustand/Vite, are reasoned inferences flagged as such. **These are now resolvable directly** against the local checkout at `D:\workspace\duckle-main` (see *Local reference checkout* above) — treat the inferred items there as open verification tasks, not settled facts.
- **Duckle is early software.** It self-describes as beta; it lacks a versioned/migratable workspace format, `validate` is incomplete, and the marquee "distributed via Quack / Native engine" features are unshipped. Don't assume its roadmap claims are working code.
- **Quack ≠ distributed execution.** Quack is a client-server protocol (multi-writer, remote access) announced May 12, 2026 (beta), requiring DuckDB v1.5.2+; DuckDB has said a production-ready version targets DuckDB 2.0 later in 2026. It explicitly does not do distributed query processing yet. Any "distributed Duckle" story is aspirational on both sides.
- **Licensing.** Duckle is dual-licensed **MIT OR Apache-2.0**. *Reusing or forking its code* (verbatim or derivative) requires preserving the copyright and license notices (MIT) and, under Apache-2.0, retaining `NOTICE` attributions and noting changes; Apache-2.0 also grants an explicit patent license. *A clean-room reimplementation* — building from the behaviour/architecture described here without copying source — carries **no attribution obligation**, though you cannot copy non-trivial code, manifests, or docs. Given how much you'll diverge (DataFusion/Iceberg/Argo core), clean-room is both cleaner legally and a better fit; borrow ideas freely, copy files not at all.
