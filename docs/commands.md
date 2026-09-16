# Command Log

Every executed command, appended in order. Commands are written for PowerShell on Windows;
where a POSIX shell was used instead, it is marked.

## 2026-09-15 — Session 1 (pre-plan: recording the Duckle reference checkout)

```powershell
# Inspect the Duckle source checkout the user provided
Get-ChildItem D:\workspace\duckle-main
Get-ChildItem D:\workspace\duckle-main\crates
Select-String -Path D:\workspace\duckle-main\Cargo.toml -Pattern '^version|^name' | Select-Object -First 5
```

```text
# Edits to ET_Local_Tool.md (POSIX shell: awk/perl/sed/python)
#  - inserted the "Local reference checkout" section before "## Key Findings"
#  - normalised the blank line before that heading
#  - extended the "Some internals are inferred" caveat to point at the checkout
```

No files were written outside D:\workspace\ETL_Local_Tool. No git commands run.

## 2026-09-15 — Session 2 (source verification + planning)

```powershell
# Project state
Get-ChildItem D:\workspace\ETL_Local_Tool
Get-ChildItem D:\workspace\ETL_Local_Tool\docs
git status --short
git branch --show-current
git log --oneline -5

# Duckle source verification (POSIX shell used for the scans below)
Get-Content D:\workspace\duckle-main\Cargo.toml -TotalCount 40
# per-crate Rust LOC     : find crates/*/ -name '*.rs' | xargs wc -l
# stub check             : cat crates/<c>/src/lib.rs for the six suspect crates
# duckdb-engine tree     : Get-ChildItem crates\duckdb-engine\src
# pipeline doc model     : plan/mod.rs PipelineDoc, metadata/src/lib.rs PipelineNode/NodeData
# component id scan      : grep -rhoE '"(src|xf|snk|qa|ctl|code)\.[a-z0-9_.]+"' crates/duckdb-engine/src
# frontend stack         : frontend/package.json
# duckdb invocation      : grep -n 'DUCKLE_DUCKDB_BIN|Command::new' crates/duckdb-engine/src/lib.rs
```

```text
# Files written (Write tool)
#   docs/PLAN_duckle_parity.md   - phased parity plan + source-verified corrections
#   docs/task_tracker.md         - state, phase table, open decisions
```

No commands run outside D:\workspace\ETL_Local_Tool except read-only scans of the reference
checkout. No git write commands. Nothing committed.

## 2026-09-15 — Session 3 (Phase 0: workspace skeleton + document model)

```powershell
# Toolchain verification (Phase 0 gate: stop if cargo is missing)
cargo --version        # 1.96.0
rustc --version        # 1.96.0
node --version         # v24.18.0
npm --version          # 11.16.0
duckdb --version       # NOT FOUND - needed from Phase 2, install v1.5.5

# Duckle edge/schema shapes (read-only reference scan)
Select-String -Path D:\workspace\duckle-main\crates\metadata\src\lib.rs -Pattern 'PipelineEdge|Position|Column|DataType|Schema'

# Scaffold
New-Item -ItemType Directory -Force crates\metadata\src, samples\pipelines, samples\data

# Gate
cargo build --workspace
cargo test --workspace
cargo fmt --check
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
```

```text
# Files written (Write/Edit tools)
#   Cargo.toml, rust-toolchain.toml, .gitignore
#   crates/metadata/Cargo.toml, crates/metadata/src/lib.rs
#   samples/pipelines/csv_to_parquet.json
#   docs/task_tracker.md (Phase 0 marked done)
```

Gate green: fmt clean, clippy clean with -D warnings, 8 tests passing. Nothing committed.

## 2026-09-15 — Session 3 (Phase 1: DAG validation + topological sort)

```powershell
cargo test --workspace
cargo fmt
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
git ls-files --others --exclude-standard
```

```text
# Files written (Write/Edit tools)
#   crates/duckdb-engine/Cargo.toml
#   crates/duckdb-engine/src/lib.rs          - EngineError
#   crates/duckdb-engine/src/plan/mod.rs     - compile(), Plan, Stage, StageKind, Input, Warning
#   crates/duckdb-engine/src/plan/tests.rs   - 30 tests
#   Cargo.toml                               - added crates/duckdb-engine to members + deps
#   docs/task_tracker.md                     - Phase 1 marked done
```

Gate green: fmt clean, clippy clean with -D warnings, 38 tests passing. Nothing committed.

## 2026-09-15 — Session 4 (Phase 2 prerequisites)

```powershell
# Environment probe
winget --version                      # v1.29.290 (available, NOT used - no global install)
where.exe duckdb                      # not found
Get-ChildItem $HOME\.duckdb\extensions   # v1.5.4, v1.5.5 present from some earlier tool

# Which DuckDB release to take
curl https://api.github.com/repos/duckdb/duckdb/releases/latest        # v1.5.5, 2026-07-22
curl https://api.github.com/repos/duckdb/duckdb/releases/tags/v1.5.5   # asset list

# Vendor the pinned CLI into the project (no system change)
Invoke-WebRequest https://github.com/duckdb/duckdb/releases/download/v1.5.5/duckdb_cli-windows-amd64.zip
#   SHA256 e1428b7114a841626b5054723731cbf45c6df91b42ae1a6c355f88fad1f6dc4c
Expand-Archive -DestinationPath D:\workspace\ETL_Local_Tool\tools\duckdb -Force
.\tools\duckdb\duckdb.exe --version   # v1.5.5 (Variegata) d8cdaa33fd
.\scripts\fetch-duckdb.ps1            # idempotent re-run: detects existing install

# Smoke tests establishing Phase 2 behaviour (see the plan's Phase 2 addendum)
.\tools\duckdb\duckdb.exe -json -c "SELECT 1 AS a, 'x' AS b, NULL AS c;"
.\tools\duckdb\duckdb.exe -json -c "<temp views + COPY TO parquet>"   # 7 of 12 rows
.\tools\duckdb\duckdb.exe -json -c "SELECT 1 AS first; SELECT 2 AS second;"
.\tools\duckdb\duckdb.exe -json -c "COPY (SELECT 1 AS x) TO '...' (FORMAT parquet);"
.\tools\duckdb\duckdb.exe -json -c "SELECT count(*) FROM 'samples/out/orders.parquet';"
.\tools\duckdb\duckdb.exe -json -c "<windows path variants: / and \ and \>"
.\tools\duckdb\duckdb.exe -json -c "<quote escaping: identifiers, literals, unescaped>"
.\tools\duckdb\duckdb.exe -json -c "SELECT * FROM does_not_exist;"     # exit 1, stderr
```

```text
# Files written
#   tools/duckdb/duckdb.exe          - vendored, git-ignored
#   scripts/fetch-duckdb.ps1         - reproducible pinned fetch
#   samples/data/orders.csv          - 12-row acceptance fixture
#   .gitignore                       - /tools/
#   docs/PLAN_duckle_parity.md       - Phase 2 addendum: verified DuckDB CLI behaviour
#   docs/task_tracker.md             - prerequisites recorded
```

No global install. Nothing committed.

## 2026-09-15 — Session 5 (Phase 2: SQL lowering + CLI executor)

```powershell
cargo build --workspace
cargo test -p etl-duckdb-engine
cargo test --workspace
cargo fmt
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings

# Acceptance, from the repo root
.\target\debug\etl.exe run samples\pipelines\orders_enriched.json      # 12/5/7/6/6, exit 0
.\target\debug\etl.exe validate samples\pipelines\orders_enriched.json # exit 0
.\target\debug\etl.exe plan samples\pipelines\orders_enriched.json

# Independent verification of the output
.\tools\duckdb\duckdb.exe -json -c "SELECT count(*), count(DISTINCT customer_id), sum(amount) FROM 'samples/out/orders_enriched.parquet';"
.\tools\duckdb\duckdb.exe -c "DESCRIBE SELECT * FROM 'samples/out/orders_enriched.parquet';"
.\tools\duckdb\duckdb.exe -json -c "SELECT md5(string_agg(...)) FROM 'samples/out/orders_enriched.parquet';"

# Error paths
.\target\debug\etl.exe run samples\pipelines\csv_to_parquet.json   # exit 3, blames 'Orders CSV'
.\target\debug\etl.exe run <broken filter>                          # exit 3, blames 'Orders from 2026'
.\target\debug\etl.exe run nope.json                                # exit 1
.\target\debug\etl.exe validate <bad componentId>                   # exit 2
```

```text
# Files written
#   crates/duckdb-engine/src/sql.rs, src/exec.rs
#   crates/duckdb-engine/src/plan/{builders.rs,builder_tests.rs,tests_support.rs}
#   crates/duckdb-engine/tests/end_to_end.rs
#   crates/cli/{Cargo.toml,src/main.rs}
#   samples/data/customers.csv, samples/pipelines/orders_enriched.json
#   Cargo.toml (clap, crates/cli), docs/task_tracker.md
```

Gate green: fmt clean, clippy clean with -D warnings, 95 tests passing. Nothing committed.

## 2026-09-15 — Session 5 (tracker refresh)

```text
# docs/task_tracker.md edits only (no code):
#   + "What works today" quick-start block for cold resumption
#   ~ decision 2 marked applied (exec::PINNED_DUCKDB_VERSION, fetch-duckdb.ps1)
#   ~ merged the "four errors" / "fifth error" entries into one accurate list of five
```

## 2026-09-15 — Session 6 (Phase 3: component spec registry)

```powershell
cargo build --workspace
cargo test --workspace
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings

# New command
.\target\debug\etl.exe components
.\target\debug\etl.exe components --namespace snk
.\target\debug\etl.exe components --manifest

# Verifying the phase's "done" criterion by actually adding a ninth component
.\tools\duckdb\duckdb.exe -json -c "SELECT * FROM read_json('samples/data/probe.jsonl', format='newline_delimited', ignore_errors=false);"
#   -> src.file.jsonl added via spec + builder + test; only the inventory test broke

# Unknown property is a warning, not a failure
.\target\debug\etl.exe validate <pipeline with a typo'd property>   # warns, exit 0
```

```text
# Files written
#   crates/metadata/src/component.rs         - ComponentSpec, PropertySpec, PortSpec, PropertyType
#   crates/metadata/src/lib.rs               - re-exports
#   crates/duckdb-engine/src/plan/specs.rs   - registry, resolution, manifest
#   crates/duckdb-engine/src/plan/specs/tests.rs
#   crates/duckdb-engine/src/plan/builders.rs    - dispatch removed, defaults removed
#   crates/duckdb-engine/src/plan/mod.rs         - registry-driven compile, UnknownProperty warning
#   crates/cli/src/main.rs                       - components command
#   docs/adding_a_component.md
#   docs/task_tracker.md
```

Gate green: fmt clean, clippy clean with -D warnings, 128 tests passing. Nothing committed.

## 2026-09-15 — Session 6 (pause)

```powershell
git status --short          # 32 entries staged/modified, 4 untracked, no commits
cargo test --workspace      # 128 passing
```

```text
# docs/task_tracker.md only:
#   + pause banner at the top: resume commands, tree state, backup warning
#   + "Picking up Phase 4" orientation notes
#   ~ "What works today" refreshed for Phase 3 (128 tests, 9 components, components command)
```

Paused after Phase 3 at a clean boundary. Nothing committed.

## 2026-09-15 — Session 7 (Phase 4, part 1)

Probing DuckDB before writing any builder, so the generated SQL is verified behaviour rather
than remembered syntax. (POSIX shell; `$D` is `.\tools\duckdb\duckdb.exe`.)

```powershell
.\tools\duckdb\duckdb.exe --version
# QUALIFY, UNION ALL BY NAME, PIVOT, UNPIVOT, RENAME, REPLACE, USING SAMPLE, EXCEPT/INTERSECT
.\tools\duckdb\duckdb.exe -c "SELECT * FROM t QUALIFY row_number() OVER (PARTITION BY id ORDER BY x) = 1;"
.\tools\duckdb\duckdb.exe -c "SELECT 1 a, 2 b UNION ALL BY NAME SELECT 2 b, 1 a;"
.\tools\duckdb\duckdb.exe -c "PIVOT s ON yr USING sum(amt) GROUP BY name;"
.\tools\duckdb\duckdb.exe -c "UNPIVOT w ON q1, q2 INTO NAME quarter VALUE amount;"
.\tools\duckdb\duckdb.exe -c "SELECT * RENAME (a AS id) FROM t;"
.\tools\duckdb\duckdb.exe -c "SELECT * REPLACE (CAST(a AS VARCHAR) AS a) FROM t;"
.\tools\duckdb\duckdb.exe -c "SELECT * FROM range(100) USING SAMPLE 5 ROWS;"

# Which extensions exist, and whether they are installed
.\tools\duckdb\duckdb.exe -c "SELECT extension_name, installed, loaded FROM duckdb_extensions() WHERE extension_name IN ('excel','httpfs','postgres_scanner','mysql_scanner','sqlite_scanner','iceberg','delta','ducklake','json','parquet','spatial');"
#   -> only json and parquet are loaded; httpfs installed but not loaded; the rest absent

# How LOAD behaves, and whether it prints anything
.\tools\duckdb\duckdb.exe -c "LOAD json; SELECT 'ok' AS r;"                 # idempotent
.\tools\duckdb\duckdb.exe -c "LOAD excel; SELECT 'ok' AS r;"                # exit 1, IO Error
.\tools\duckdb\duckdb.exe -c "SELECT current_setting('autoinstall_known_extensions'), current_setting('autoload_known_extensions');"
#   -> both true, yet LOAD of an uninstalled extension still fails: autoload triggers on use
.\tools\duckdb\duckdb.exe -json -c "LOAD json; SELECT 1 AS n; LOAD parquet; SELECT 2 AS n;"
#   -> LOAD prints no JSON array, so a failed prelude needs its own probe

# PIVOT/UNPIVOT/QUALIFY inside a view — the shape every stage actually takes
.\tools\duckdb\duckdb.exe -json -c "CREATE OR REPLACE TEMP VIEW p AS (PIVOT s ON yr USING sum(amt) GROUP BY name);"
#   -> Parser Error: PIVOT with values extracted from the data cannot be used in views
.\tools\duckdb\duckdb.exe -json -c "CREATE OR REPLACE TEMP VIEW p AS (PIVOT s ON yr IN ('2025','2026') USING sum(amt) GROUP BY name);"
#   -> works; hence xf.pivot's required `values` property
.\tools\duckdb\duckdb.exe -json -c "SELECT count(*) AS n FROM (SELECT * FROM range(1000) USING SAMPLE 10 PERCENT);"
#   -> 0 rows: the system sampler needs reservoir() to be exact on small inputs

# JSON read/write shapes
.\tools\duckdb\duckdb.exe -c "COPY (SELECT 1 a, 'x' b) TO 'target/probe/arr.json' (FORMAT json, ARRAY true);"
.\tools\duckdb\duckdb.exe -c "COPY (SELECT 1 a UNION ALL SELECT 2 a) TO 'target/probe/nd.json' (FORMAT json);"
.\tools\duckdb\duckdb.exe -json -c "SELECT * FROM read_json('target/probe/arr.json', format='array', ignore_errors=false);"
```

```powershell
# The gate, run after each family
cargo build --workspace
cargo test --workspace          # 128 -> 135 -> 137 engine unit; 164 total at the end
cargo fmt --all
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
.\target\debug\etl.exe components --namespace xf                   # 19 transforms
.\target\debug\etl.exe components                                  # 27 total
.\target\debug\etl.exe run samples\pipelines\orders_enriched.json   # still 12/5/7/6/6
```

```text
# Files written (POSIX shell: python for precise in-place splices)
#   crates/metadata/src/component.rs             - requires_extension, PropertyType::Map
#   crates/duckdb-engine/src/plan/mod.rs         - Stage::requires_extensions, Plan::extensions,
#                                                  stages_needing, has_prelude_probe,
#                                                  PRELUDE_PROBE, the LOAD prelude
#   crates/duckdb-engine/src/exec.rs             - prelude-aware attribution,
#                                                  ExecError::ExtensionLoadFailed
#   crates/duckdb-engine/src/plan/builders.rs    - 18 builders + shared helpers
#   crates/duckdb-engine/src/plan/specs.rs       - 18 specs
#   crates/duckdb-engine/src/plan/specs/tests.rs - inventory 9 -> 27
#   crates/duckdb-engine/src/plan/tests_support.rs - compile_two_sided
#   crates/duckdb-engine/src/plan/builder_tests.rs - golden SQL for all 18
#   crates/duckdb-engine/tests/end_to_end.rs     - 4 execution tests
#   docs/adding_a_component.md, docs/task_tracker.md
#
# Cleaned up: a stray 'a' file in the repo root, an empty DuckDB database created by a
# mis-quoted probe command, plus target/probe/.
```

Gate green: fmt clean, clippy clean with -D warnings, 164 tests passing. Nothing committed.
Phase 4 is part done (27 of ~40); the remaining families are blocked on the extension-install
decision recorded in the tracker.

## 2026-09-15 — Session 8 (Phase 4, part 2)

Extensions vendored into the project rather than installed system-wide. `INSTALL` with
`extension_directory` set is what keeps it inside the repo — without the SET, DuckDB writes to
`~/.duckdb/extensions`.

```powershell
# The layout DuckDB expects, discovered rather than assumed
.\tools\duckdb\duckdb.exe -c "SET extension_directory='tools/duckdb/extensions'; INSTALL excel;"
#   -> tools/duckdb/extensions/v1.5.5/windows_amd64/excel.duckdb_extension

# The rest, then the script that reproduces all of it
.\scripts\fetch-duckdb-extensions.ps1
#   -> 9 extension file(s), 246.7 MB  (iceberg/ducklake pull in avro)

.\tools\duckdb\duckdb.exe -noheader -list -c "PRAGMA platform;"   # windows_amd64
```

Confirmed the pre-existing `~/.duckdb/extensions/httpfs` dates from 2026-08-30 and was not
written by this session; everything installed here went to the project directory.

```powershell
# Connector SQL, verified before the builders were written
.\tools\duckdb\duckdb.exe -c "LOAD excel; COPY (SELECT 1 AS a) TO 'x.xlsx' (FORMAT xlsx, HEADER true);"
.\tools\duckdb\duckdb.exe -c "LOAD excel; SELECT count(*) FROM read_xlsx('x.xlsx', sheet='Orders', header=true);"
.\tools\duckdb\duckdb.exe -c "LOAD sqlite; ATTACH 'demo.db' AS \"n1_db\" (TYPE sqlite, READ_ONLY);"
.\tools\duckdb\duckdb.exe -c "LOAD postgres; ATTACH 'dbname=nope host=127.0.0.1 port=1' AS pg (TYPE postgres);"
#   -> connection refused, not a parse error: the statement shape is right
.\tools\duckdb\duckdb.exe -c "LOAD iceberg; LOAD delta; SELECT function_name FROM duckdb_functions() WHERE function_name IN ('iceberg_scan','delta_scan');"
.\tools\duckdb\duckdb.exe -c "CREATE TABLE IF NOT EXISTS \"w_db\".\"archive\" AS SELECT * FROM \"a\" WHERE false; INSERT INTO \"w_db\".\"archive\" SELECT * FROM \"a\";"
#   -> 2 rows then 4: creates on the first run, appends on the second
```

```powershell
# The gate
cargo build --workspace
cargo test --workspace          # 183 passing
cargo fmt --all; cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
.\target\debug\etl.exe components                                  # 40
.\target\debug\etl.exe components --namespace src                  # 12
.\target\debug\etl.exe run samples\pipelines\orders_enriched.json   # still 12/5/7/6/6
git check-ignore -v tools/duckdb/extensions/...                     # covered by /tools/
```

```text
# Files written (POSIX shell: python for precise in-place splices)
#   scripts/fetch-duckdb-extensions.ps1          - new
#   crates/duckdb-engine/src/exec.rs             - RunOptions::extension_dir,
#                                                  locate_extension_dir, SET extension_directory
#   crates/duckdb-engine/src/plan/builders.rs    - 13 connector builders, attach_database,
#                                                  qualified_table
#   crates/duckdb-engine/src/plan/specs.rs       - 13 specs, cloud_format, database_read/write
#   crates/duckdb-engine/src/plan/specs/tests.rs - inventory 27 -> 40
#   crates/duckdb-engine/src/plan/builder_tests.rs - golden SQL + prelude tests
#   crates/duckdb-engine/tests/end_to_end.rs     - excel, sqlite, missing-extension tests
#   crates/cli/src/main.rs                       - extension_dir field
#   docs/task_tracker.md
#
# Two bugs the execution tests caught, which golden tests could not have:
#   - snk.file.excel wrote no header, so a 12-row round trip came back as 11
#   - append could not create a missing table, so first runs always failed
```

Gate green: fmt clean, clippy clean with -D warnings, 183 tests passing. **Phase 4 done, 40
components.** Nothing committed.

## 2026-09-15 — Session 9 (Phase 5, part 1)

Probing the materialisation statement shapes before writing them, as for every phase.

```powershell
# memory and disk, against the real thing
.\tools\duckdb\duckdb.exe -c "CREATE OR REPLACE TEMP TABLE m AS (SELECT * FROM src);"
.\tools\duckdb\duckdb.exe -c "COPY (SELECT * FROM src) TO 'spill.parquet' (FORMAT parquet);"
.\tools\duckdb\duckdb.exe -c "CREATE OR REPLACE TEMP VIEW d AS (SELECT * FROM read_parquet('spill.parquet'));"
#   -> both round-trip 12 rows
```

The acceptance criterion for the phase, run by hand before it was written as a test:

```powershell
.\target\debug\etl.exe contexts --contexts samples\contexts.json
.\target\debug\etl.exe run samples\pipelines\orders_by_context.json --contexts samples\contexts.json
.\target\debug\etl.exe run samples\pipelines\orders_by_context.json --contexts samples\contexts.json --context prod
#   -> both print 12 / 6 / 6, writing to samples/out/dev and samples/out/prod

# The Phase 2 sample that has never run before now does
.\target\debug\etl.exe validate samples\pipelines\csv_to_parquet.json
.\target\debug\etl.exe run samples\pipelines\csv_to_parquet.json                        # 12/7/7
.\target\debug\etl.exe run samples\pipelines\csv_to_parquet.json --param since=2026-03-01  # 12/3/3
```

Failure modes, checked by hand including their exit codes:

```powershell
.\target\debug\etl.exe run samples\pipelines\orders_by_context.json --contexts samples\contexts.json --context prd
#   -> error: there is no context named 'prd'. Defined: dev, prod            (exit 1)
.\target\debug\etl.exe validate samples\pipelines\orders_by_context.json
#   -> error: parameter 'out_dir' is required and has no value and no default (exit 2)
.\target\debug\etl.exe validate samples\pipelines\csv_to_parquet.json --param oops
#   -> error: --param expects NAME=VALUE, but got 'oops'                     (exit 1)
.\target\debug\etl.exe validate samples\pipelines\csv_to_parquet.json --param since=
#   -> error: parameter 'since' is required, but the value given for it is empty
.\target\debug\etl.exe validate samples\pipelines\csv_to_parquet.json --param snce=2026-01-01
#   -> warning: 'snce' was supplied but this pipeline declares no such parameter (exit 0)
```

```powershell
# The gate
cargo build --workspace
cargo test --workspace          # 240 -> 252 passing
cargo fmt --all; cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
.\target\debug\etl.exe run samples\pipelines\orders_enriched.json   # still 12/5/7/6/6
```

```text
# Files written (POSIX shell: python for precise in-place splices)
#   crates/duckdb-engine/src/params.rs + params/tests.rs      - new
#   crates/duckdb-engine/src/context.rs + context/tests.rs    - new
#   crates/duckdb-engine/src/lib.rs                           - both modules, re-exports
#   crates/metadata/src/lib.rs                                - NodeData::materialize
#   crates/duckdb-engine/src/plan/mod.rs                      - Materialize, spill_path,
#                                                               Plan::spills, UnknownMaterialize
#   crates/duckdb-engine/src/plan/builders.rs                 - create_view emits view /
#                                                               temp table / Parquet spill
#   crates/duckdb-engine/src/exec.rs                          - spill prepare + cleanup,
#                                                               RunReport::spilled
#   crates/duckdb-engine/src/plan/tests_support.rs            - crate-visible; compile_materialized
#   crates/cli/src/main.rs                                    - Settings group, etl contexts
#   samples/contexts.json, samples/pipelines/orders_by_context.json
#   docs/task_tracker.md
#
# One repeated snag worth noting for whoever automates this next: writing Rust string
# escapes through a shell heredoc kept collapsing `\n` and line continuations. Writing the
# snippet to a file first and splicing it in with python avoids it entirely.
```

Gate green: fmt clean, clippy clean with -D warnings, 252 tests passing. Parameters, contexts
and materialisation done; secrets not started. Nothing committed.

## 2026-09-15 — Session 10 (Phase 5, part 2: secrets)

The dependency decision first, taken deliberately rather than in a diff.

```powershell
cargo search aes-gcm --limit 3          # crates.io reachable; 0.11.1 current
cargo info aes-gcm@0.11.1               # rust-version: 1.85
cargo add aes-gcm --package etl-secrets # resolved to 0.10.3, not 0.11.1
```

Cargo's MSRV-aware resolution picked 0.10.3 because 0.11 wants Rust 1.85 and this workspace
declares 1.80. That is the right outcome and was left alone: 0.10.3 is the mature line, and
raising the MSRV would be a decision in its own right. 21 transitive crates.

The secret flow, driven by hand before it was written as tests:

```powershell
$w = "target\secret-demo"
.\target\debug\etl.exe secret init --workspace $w
.\target\debug\etl.exe secret set db_password "hunter2-very-secret" --description "Analytics DB" --workspace $w
.\target\debug\etl.exe secret list --workspace $w
Get-Content $w\.etl\secrets.json          # nonce + ciphertext only
Select-String -Path $w\* -Pattern hunter2 -Recurse   # no plaintext anywhere
```

The three places a secret could escape, each checked:

```powershell
.\target\debug\etl.exe plan $w\pipeline.json --script --workspace $w
#   -> ATTACH 'dbname=analytics ... password=********' AS "orders_db" (TYPE postgres, READ_ONLY);
.\target\debug\etl.exe validate $w\pipeline.json --workspace $w
#   -> Resolved:  ${SECRET:db_password} = ********
.\target\debug\etl.exe run $w\pipeline.json --workspace $w
#   -> error: ... Unable to connect to Postgres at "dbname=... password=********": ...
#      DuckDB quotes the whole connection string back; masking only the script would have
#      looked complete and leaked anyway.
```

Argument order, which needed fixing:

```powershell
.\target\debug\etl.exe secret init --workspace $w   # failed at first: unexpected argument
.\target\debug\etl.exe secret --workspace $w init   # worked
#   -> made the shared Settings args `global`, so both orders now work
```

```powershell
# The gate
cargo build --workspace
cargo test --workspace          # 272 -> 284 passing
cargo fmt --all; cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
#   -> caught usize::is_multiple_of (stable 1.87) against the declared 1.80 MSRV;
#      rewritten as % 2 != 0 rather than raising the floor, as in Phase 3
.\target\debug\etl.exe run samples\pipelines\orders_enriched.json   # still 12/5/7/6/6
```

```text
# Files written (POSIX shell: python for precise in-place splices)
#   crates/secrets/{Cargo.toml,src/lib.rs,src/tests.rs}  - new crate
#   Cargo.toml                                           - workspace member + dependency
#   crates/duckdb-engine/Cargo.toml, crates/cli/Cargo.toml
#   crates/duckdb-engine/src/params.rs                   - ${SECRET:...}, Resolved::redact,
#                                                          Trace, masked `used`
#   crates/duckdb-engine/src/exec.rs                     - RunOptions::redact, stderr masking
#   crates/cli/src/main.rs                               - etl secret *, global settings args
#   docs/task_tracker.md
#
# Cleaned up: target/secret-demo (the by-hand workspace).
```

Gate green: fmt clean, clippy clean with -D warnings, 284 tests passing. **Phase 5 done.**
Nothing committed — asked, and the answer was "not yet".

## 2026-09-16 — Pause

Every command in the tracker's resume block, run verbatim to confirm it prints what the tracker
claims. Run in PowerShell, which is what the tracker documents.

```powershell
cd D:\workspace\ETL_Local_Tool
cargo test --workspace                                            # 284 passing
.\target\debug\etl.exe components                                 # 40
.\target\debug\etl.exe run samples\pipelines\orders_enriched.json # 12/5/7/6/6

$c = "--contexts", "samples\contexts.json"
.\target\debug\etl.exe run samples\pipelines\orders_by_context.json @c              # 12/6/6
.\target\debug\etl.exe run samples\pipelines\orders_by_context.json @c --context prod # 12/6/6

cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
git ls-files --others --exclude-standard    # 10 untracked, all new work, no strays
```

All five resume commands printed exactly what the tracker says they would, including the
PowerShell splat form, which had been written into the tracker without being run until now.

Paused after Phase 5. Nothing committed.


## 2026-09-16 — Project analysis, then first commit and push

Analysis pass: the gate re-run from scratch to confirm the tracker's claims independently.

```powershell
cargo test --workspace                                            # 284 passing
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
.\target\debug\etl.exe components                                 # 40
.\target\debug\etl.exe run samples\pipelines\orders_enriched.json # 12/5/7/6/6

$c = "--contexts", "samples\contexts.json"
.\target\debug\etl.exe run samples\pipelines\orders_by_context.json @c               # 12/6/6
.\target\debug\etl.exe run samples\pipelines\orders_by_context.json @c --context prod # 12/6/6
```

Everything the tracker claimed was true, command for command.

Then the thing the tracker had been flagging since Phase 0: **the work got committed and
pushed.** Target `github.com/marun224/local_etl_tool` — private, and empty until now.

```powershell
git add -A
git status --short                          # 42 files, 614K, no tools/ .etl/ or samples/out/
git diff --cached | Select-String -Pattern "password|api_key|PRIVATE KEY"  # only hunter2, a test placeholder
git branch -M main
git commit -m "..."                         # one commit; there is no incremental history to preserve
git remote add origin https://github.com/marun224/local_etl_tool.git
git push -u origin main
```

One commit rather than a reconstructed per-phase history: the phases were real, but the commits
never existed, and inventing them after the fact would put dates on work that git never saw.

**The tree is no longer the only copy.** `tools/` stays out — 284 MB of vendored DuckDB binary
and extensions, both reproducible from `scripts/`.

## 2026-09-16 — Phase 6a: quality nodes and reject ports

Phase 6 was split before any code was written. `qa.*` extends the existing model; `ctl.*`
breaks it, because control flow and per-stage retry cannot live inside one SQL script and one
script per run is what `exec.rs` is built on. Splitting kept a known rewrite out of a phase that
did not need one. The reasoning is in the plan under Phase 6.

Built: seven validators, a second output port, two row counts per quality stage, and two new
validation errors. The gate, then the acceptance run:

```powershell
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                          # 300 passing, up from 284
.\target\debug\etl.exe components               # 47, up from 40

.\target\debug\etl.exe validate samples\pipelines\orders_checked.json
.\target\debug\etl.exe run samples\pipelines\orders_checked.json
Get-Content samples\out\rejected_status.csv     # 1003 returned, 1005 cancelled
Get-Content samples\out\rejected_amount.csv     # 1012, 610.00
```

The run prints `12 / 10 rows + 2 rejected / 9 rows + 1 rejected / 9 / 2 / 1`: twelve orders in,
two with a status outside the accepted set, one over the amount ceiling, nine clean. Both
rejects are written to their own files in the same pass as the good rows.

Two things worth remembering, because both cost time:

- **`cargo clippy --fix` after a blanket `&` rewrite.** Changing `exactly_one_input` to return
  `String` meant every call site needed `&`, and a regex over-applied it to the helpers that
  already took `&str`. Clippy named all four; `--fix` took them.
- **The expected row counts in the first e2e test were wrong, not the code.** `shipped` is 7
  rows in orders.csv, not 6 — counted by hand and miscounted. `awk -F, 'NR>1{c[$5]++} END{...}'`
  settled it in one line. Count the fixture, do not remember it.

## 2026-09-16 — Phase 6b: the execution-model decision, then control flow

The phase opened with the decision the plan said it had to. Three options were written up in
[DECISION_execution_model.md](DECISION_execution_model.md) and measured against the vendored
DuckDB rather than reasoned about, using throwaway probe scripts:

```powershell
# Can the CLI be driven as a read-eval loop, or does it buffer until EOF?
# Send a statement, read the result, branch on it, send another.
python probe_interactive.py      # verdict: read-eval loop works

# What does a round trip actually cost, measured with a sentinel not a timeout?
python probe_latency.py          # 0.54 ms warm session vs 41.45 ms per spawn - 77x

# Does a failed statement kill a driven session? (continue_on_failure depends on it)
python probe_errors.py           # no: temp views survive, new state can still be made

# .bail on|off
printf ".bail on`nSELECT * FROM nope;`nSELECT 'reached';`n" | .\tools\duckdb\duckdb.exe -json
```

`.bail on` turned out to terminate the **session**, not just the batch, so fail-fast has to be
the driver's decision. That is where per-stage `continueOnFailure` needs it anyway.

Chosen: **option A, persistent session, dual path** — a plan earns a session by holding a control
node or a stage policy; everything else keeps the one-script transport.

Then the build, and the gate:

```powershell
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                          # 326 passing, up from 300
.\target\debug\etl.exe components               # 54, up from 47

.\target\debug\etl.exe run samples\pipelines\orders_guarded.json
```

Negative paths checked by hand before they were written as tests, each against a sed-edited copy
of the sample:

```powershell
# branch not taken -> downstream skipped, exit 0 (not a failure)
# row_count min 999 -> exit 3, names the node and the message
# a column renamed to "nope" -> exit 3, node message AND DuckDB's candidate list
# continueOnFailure -> broken fails, its dependent is skipped, the independent branch still runs
```

Three things cost real time and are worth not rediscovering:

- **The stderr grace period was on the happy path.** A `CREATE VIEW` returns no rows whether it
  worked or not, so "no rows means look for an error message" put 250 ms on *every* stage: the
  seven-stage sample took 2.00 s instead of 0.18 s. The verdict belongs to the count probes;
  stderr is only asked for the message once something is already known to have failed.
- **The first failure message named the symptom.** When a stage's `CREATE VIEW` fails, the count
  probe after it fails too — complaining the view does not exist. Reporting the probe's message
  hid the actual cause. Keep the statement's own stderr and prefer it.
- **A bounded-window assertion caught a bad scripted edit.** A find/replace meant to delete one
  enum variant deleted 400 lines, because the variant was last in the enum and the closing
  pattern matched far below. `exec.rs` was restored from HEAD and the patches re-applied. Any
  scripted deletion that searches for its own end needs a sanity check on how much it is about
  to remove.

## 2026-09-16 — Phase 7a: the desktop shell and its five commands

Versions were checked before anything was written, because the plan names four of them and a
stale plan is how you end up on an alpha:

```powershell
node --version; npm --version            # v24.18.0, 11.16.0
cargo info tauri                         # 2.11.5  (latest is 3.0.0-alpha.1 - NOT that)
npm view react version                   # 19.3.0
npm view vite version                    # 8.3.0
npm view typescript version              # 7.0.2
npm view @xyflow/react version           # 12.11.6
npm ping                                 # both registries reachable
```

WebView2 is already present on this machine, so nothing had to be installed for the window.

Built, then the gate — which now has four parts rather than three:

```powershell
npm --prefix frontend install
npm --prefix frontend run typecheck
npm --prefix frontend run build
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                   # 335 passing, up from 326
```

Launched for real rather than only compiled:

```powershell
npm --prefix frontend run dev            # vite on :5173
cargo run -p etl-desktop                 # the window
tasklist | Select-String "etl-desktop"   # alive, with WebView2 children
```

Four things worth remembering:

- **The Rust gate must not need npm.** Adding a Tauri crate to the workspace risks making
  `cargo test --workspace` depend on `frontend/dist`. Checked by deleting `dist`, touching
  `build.rs` to force the build script to re-run, and rebuilding: it passes. Only a real bundle
  needs the frontend built.
- **Tauri needs `icons/icon.ico` on Windows or `build.rs` fails**, with a message that says so
  plainly. The icon was generated with `struct` and `zlib` from the standard library rather than
  adding Pillow to draw a 32px square.
- **The `?` in `tsconfig` strictness costs one file.** `"types": []` means a side-effect CSS
  import does not typecheck until `src/env.d.ts` declares it.
- **`cargo fmt` reflows `pub use` lists**, so a scripted patch that matches one by its exact text
  will silently miss after a format run. Two edits here failed that way; match on a shorter
  anchor or re-read after formatting.

## 2026-09-16 — Phase 7b: the canvas

```powershell
npm view @xyflow/react version           # 12.11.6
npm view lucide-react version            # 1.46.0
cargo add tauri-plugin-dialog@2 -p etl-desktop --dry-run   # 3.0.0-alpha is latest; pin to 2
npm --prefix frontend install @xyflow/react lucide-react @tauri-apps/plugin-dialog
npm --prefix frontend install -D vitest @types/node
```

The gate has five parts now:

```powershell
npm --prefix frontend run typecheck
npm --prefix frontend run test           # 36 passing
npm --prefix frontend run build
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                   # 335 passing
```

Run and looked at, not only built:

```powershell
npm --prefix frontend run dev
cargo run -p etl-desktop
# 54 components in the palette, grouped by namespace, "ext" badges on the ones
# needing a DuckDB extension; canvas, inspector, and the Status/SQL/Data tabs.
```

Four things worth remembering:

- **The round-trip promise had to be corrected, and the correction is the useful part.** Byte
  identity is not achievable: `JSON.stringify` always expands arrays, so a hand-written
  `"values": ["a", "b"]` reformats however carefully the data is kept. The promise that holds —
  and that is tested against all five committed samples — is that nothing is lost or altered,
  and that formatting settles after one save. Five failing tests said so before any of this was
  written down, which is the argument for testing against the real files rather than a fixture.
- **lucide's barrel import costs 600 KB.** `import { icons } from "lucide-react"` pulls in all
  ~1500. Importing the 42 the registry uses by name took the bundle from 1031 KB to 430 KB.
  `src/icons.ts` carries the command that regenerates its own list.
- **`exactOptionalPropertyTypes` refuses `className: undefined`.** Absent and
  present-but-undefined are different things to it; use a conditional spread.
- **`it.each(files)` does not typecheck under this tsconfig.** A plain `for` loop around `it()`
  does, and reads no worse.

## 2026-09-16 — Phase 7c: the generated property panel

```powershell
npm --prefix frontend install -D jsdom @testing-library/react @testing-library/dom
npm --prefix frontend run typecheck
npm --prefix frontend run test            # 80 passing, up from 36
npm --prefix frontend run build
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                    # 335 passing, unchanged
```

Four things worth remembering:

- **A webview will not always start an HTML5 drag from synthetic mouse input.** Driving the
  palette with real `mouse_event` calls moved the cursor and never produced a node, which is a
  fair proxy for the people who will hit the same wall for other reasons. Clicking a palette
  entry now adds the node too — and that is the better feature anyway, because dragging is
  unreachable from a keyboard.
- **Verify a generated form by rendering it, not by photographing it.** A screenshot shows one
  component on one day. 25 tests hand the panel a made-up component using all nine property
  types and check the control chosen for each, what editing writes, and what gets marked. That
  is what actually pins "no per-component React".
- **`environmentMatchGlobs` is gone in vitest 5.** A `@vitest-environment jsdom` pragma at the
  top of the file that needs a DOM does the same job and says so where you can see it.
- **Testing Library only unmounts automatically when vitest globals are on.** Without an explicit
  `cleanup()` every render piles into the same document, and the second test onward finds two of
  everything — which reads as a component bug for a good few minutes.

## 2026-09-16 — Phase 8c: the scheduler

```powershell
# Gate before starting, to confirm the tracker's numbers
cargo test --workspace                       # 378 passing, as the tracker said

# Read what 8c had to build on
#   docs/PLAN_duckle_parity.md  - Phase 8, and the 8a/8b notes
#   crates/state/src/lib.rs     - the single-writer assumption, stated out loud
#   crates/cli/src/main.rs      - command_run, load_and_compile, save_watermarks
```

```text
# Files written
#   crates/scheduler/            - new crate: lib, cron, every, watch, lock, run (+ tests)
#   crates/state/src/time.rs     - civil dates promoted out of lib.rs, plus from_rfc3339
#   crates/cli/src/main.rs       - perform/print_report split; etl schedule list|check|start
#   samples/schedules.json       - the committed example
#   Cargo.toml                   - crates/scheduler added to the workspace
```

```powershell
# Per-crate, while building
cargo test -p etl-state
cargo test -p etl-scheduler
cargo build -p etl-cli

# The phase's own acceptance, from a cold shell at the repo root
$s = "--schedules", "samples\schedules.json", "--contexts", "samples\contexts.json"
.\target\debug\etl.exe schedule list @s
.\target\debug\etl.exe schedule check @s
.\target\debug\etl.exe schedule start --once @s
```

```bash
# The lock, verified by killing a scheduler rather than by reasoning about it (POSIX shell)
./target/debug/etl.exe schedule start --schedules samples/schedules.json ... &
BGPID=$!; sleep 3
./target/debug/etl.exe schedule start --once ...   # refused: names pid and host
kill $BGPID; sleep 1
./target/debug/etl.exe schedule start --once ...   # takes it: a leftover file is not a held lock

# The watch, with a one-second poll and a file dropped into an inbox mid-run
mkdir -p samples/inbox
./target/debug/etl.exe schedule start --schedules <scratch>/watch.json ... &
cp samples/data/orders.csv samples/inbox/landed.csv    # fired once, 7 rows, then settled
rm -rf samples/inbox
```

```powershell
# Full gate
cargo fmt --all
cargo fmt --all --check                      # clean
cargo clippy --workspace --all-targets -- -D warnings   # clean
cargo test --workspace                       # 508 passing
npm --prefix frontend run test               # 114 passing
npm --prefix frontend run typecheck          # clean

# Regression check: every acceptance run from the tracker still prints what it printed
.\target\debug\etl.exe components                                 # 54
.\target\debug\etl.exe run samples\pipelines\orders_enriched.json # 12/5/7/6/6
.\target\debug\etl.exe run samples\pipelines\orders_checked.json  # 12/10+2/9+1/9/2/1
.\target\debug\etl.exe run samples\pipelines\orders_guarded.json  # 12 through, branch taken
```

Nothing was written outside D:\workspace\ETL_Local_Tool except scratch files under the session
temp directory. `.etl/scheduler.lock` and `.etl/scheduler.status` were removed after the lock
test; `samples/inbox/` was removed after the watch test.

## 2026-09-16 — Phase 8d: the web console

```powershell
# Gate before starting
cargo test --workspace                       # 508 passing, as the tracker said

# What the one dependency actually costs, before taking it
cargo add tiny_http --dry-run -p etl-cli
cargo tree -p etl-console                    # 5 new: tiny_http, ascii, chunked_transfer, httpdate, log
```

```text
# Files written
#   crates/console/             - new crate: auth, routes, ui, server, workspace (+ tests)
#   crates/secrets/src/lib.rs   - random_token, reusing the OsRng that AES already brought
#   crates/cli/src/main.rs      - etl serve; ConsoleWorkspace implementing console::Workspace
#   Cargo.toml                  - crates/console added to the workspace
```

```powershell
# Per-crate, while building
cargo test -p etl-secrets
cargo test -p etl-console
cargo build -p etl-cli

# The console itself
.\target\debug\etl.exe serve --port 8099 --contexts samples\contexts.json --schedules samples\schedules.json

# Refusals, checked at startup rather than at runtime
$env:ETL_CONSOLE_OPERATOR_TOKEN="same"; $env:ETL_CONSOLE_VIEWER_TOKEN="same"
.\target\debug\etl.exe serve --port 8098    # exit 1, names both variables
.\target\debug\etl.exe serve --bind 0.0.0.0 # warns: no TLS, tokens cross the network in clear
```

```bash
# The security properties, against the running console rather than only in tests (POSIX shell)
curl -o /dev/null -w "%{http_code}" http://127.0.0.1:8099/api/health           # 200, no token
curl -o /dev/null -w "%{http_code}" http://127.0.0.1:8099/api/pipelines        # 401
curl -X POST "http://127.0.0.1:8099/api/runs?pipeline=orders_enriched&token=$OP"   # 401: URL token
curl -X POST -H "Authorization: Bearer $VIEWER" ".../api/runs?pipeline=orders_enriched"  # 403
curl -X POST -H "Authorization: Bearer $OP"     ".../api/runs?pipeline=orders_enriched"  # 200
curl -H "Authorization: Bearer $VIEWER" ".../api/pipelines/..%2F..%2Fetc%2Fpasswd/lineage"  # 404
curl -D - -o /dev/null -H "Authorization: Bearer $VIEWER" .../api/pipelines
#   X-Etl-Role, Content-Security-Policy, X-Content-Type-Options, Cache-Control all present

# Stopping it: pkill does not exist in Git Bash here
taskkill //F //IM etl.exe
```

```powershell
# Full gate
cargo fmt --all
cargo fmt --all --check                      # clean
cargo clippy --workspace --all-targets -- -D warnings   # clean
cargo test --workspace                       # 576 passing
npm --prefix frontend run test               # 114 passing
npm --prefix frontend run typecheck          # clean

# Regression check: the acceptance runs still print what they printed
.\target\debug\etl.exe components                                 # 54
.\target\debug\etl.exe run samples\pipelines\orders_enriched.json # 12/5/7/6/6
.\target\debug\etl.exe schedule list @s                           # 4 schedules, 3 enabled
```

Nothing was written outside D:\workspace\ETL_Local_Tool except scratch files under the session
temp directory.
