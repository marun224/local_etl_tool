# Task Tracker

**State only.** Design lives in [PLAN_duckle_parity.md](PLAN_duckle_parity.md). Read this file
first when picking the project back up.

> ## ⏸ Paused 2026-09-16, after Phase 8d — **Phase 8 is complete**
>
> Stopped at a clean boundary — gate green, nothing mid-edit. **Phases 0–8 are all done.**
> There is a CLI, a desktop canvas, watermark incremental loading, run history and lineage, a
> scheduler, and a web console. What is left is packaging (9), breadth (10), AI (11) and
> benchmarks (12) — none of which the working product needs in order to work.
>
> Phases 0–5 are dated 2026-09-15 because that is when the work was done; the clock rolled
> past midnight while pausing, which is the only reason those lines read a day later.
>
> **To resume:** read this file, then Phase 9 in the plan. Phase 9 is the standalone binary
> export — one self-contained executable with a pipeline baked into it, cross-built. Two things
> from earlier phases are waiting for it and are the reason it should be next: Settled decision
> 3 made the vendored, air-gapped extension path *the only path* from Phase 4 onward, so Phase 9
> is exercising something that has been in use for days rather than discovering it at the end;
> and Settled decision 5 reserved the name `etl-runner` for exactly this artifact. The known
> risk is the cross-build matrix, not the embedding — bundling the right native DuckDB and
> extension binaries per target is the part that will take the time.
>
> ```powershell
> cd D:\workspace\ETL_Local_Tool
> cargo test --workspace                                            # expect 576 passing
> npm --prefix frontend run test                                    # expect 114 passing
> npm --prefix frontend run typecheck                               # expect clean
> npm --prefix frontend run build                                   # expect clean
> .\target\debug\etl.exe components                                 # expect 54
> .\target\debug\etl.exe run samples\pipelines\orders_enriched.json # expect 12/5/7/6/6
> .\target\debug\etl.exe run samples\pipelines\orders_checked.json  # expect 12/10+2/9+1/9/2/1
> .\target\debug\etl.exe run samples\pipelines\orders_guarded.json  # expect 12 through, branch taken
> ```
>
> Phase 7's acceptance criterion, which should print the same 12/6/6 twice into two different
> directories:
>
> ```powershell
> $c = "--contexts", "samples\contexts.json"
> .\target\debug\etl.exe run samples\pipelines\orders_by_context.json @c
> .\target\debug\etl.exe run samples\pipelines\orders_by_context.json @c --context prod
> ```
>
> Phase 8a's, which is the whole point of a watermark — **run it twice**:
>
> ```powershell
> .\target\debug\etl.exe run samples\pipelines\orders_incremental.json  # 12 rows, mark recorded
> .\target\debug\etl.exe run samples\pipelines\orders_incremental.json  # 0 rows, nothing new
> .\target\debug\etl.exe state list
> ```
>
> 8b's:
>
> ```powershell
> .\target\debug\etl.exe runs list --limit 5
> .\target\debug\etl.exe lineage samples\pipelines\orders_checked.json
> ```
>
> 8c's:
>
> ```powershell
> $s = "--schedules", "samples\schedules.json", "--contexts", "samples\contexts.json"
> .\target\debug\etl.exe schedule list @s      # 4 schedules, 3 enabled, times in UTC
> .\target\debug\etl.exe schedule check @s     # all runnable
> .\target\debug\etl.exe schedule start --once @s
> ```
>
> And 8d's — open the operator link it prints:
>
> ```powershell
> .\target\debug\etl.exe serve @s              # 6 pipelines, 4 schedules, run history
> ```
>
> All of these were run verbatim at the moment of pausing and printed exactly what is written
> above. If any of them disagrees with this file later, trust the commands and fix the file.
>
> `schedule start --once` prints nothing on a second run within fifteen minutes, which is
> correct rather than broken: the interval is anchored on the run it just did.
>
> **`etl serve` holds the terminal** and is stopped with Ctrl-C. On Windows a killed `etl.exe`
> keeps its port until the process really goes; `taskkill /F /IM etl.exe` is the blunt way, and
> `pkill` does not exist in Git Bash here. A port already in use is reported clearly and exits 1.
>
> **`.etl/` was reset before those runs**, so the incremental sample really did start from
> nothing. It is git-ignored local state; a fresh clone starts empty anyway, and
> `etl state forget` and `etl runs prune` are how you get back here deliberately.
>
> **State of the tree:** clean and committed — 15 commits on `main`, the last being Phase 8d.
> Everything is pushed; `origin/main` at `github.com/marun224/local_etl_tool` (private) is at
> the same commit as `HEAD`.
>
> **Phases 0–5 are one commit, not six.** The phases happened on the dates recorded below;
> the commits did not exist, and dating them after the fact would have git assert a history
> it never saw. Every phase from 6a on has its own commit, which is the arrangement to keep.
>
> **`tools/` is still not backed up, deliberately.** It is git-ignored and holds the DuckDB
> CLI (37 MB) plus 9 extension files (247 MB) — 284 MB that does not belong in a repo and is
> reproducible with `.\scripts\fetch-duckdb.ps1` and `.\scripts\fetch-duckdb-extensions.ps1`.
> A fresh clone needs both scripts run before the tests will pass.

## Where things stand

- **Next phase:** Phase 9 — standalone binary export and air-gapped packaging.
- **In progress:** nothing. **Phase 8 is complete** (8a–8d, all 2026-09-16).
- **Blocked on:** nothing.

Phase 6 was split into 6a and 6b on 2026-09-16 before starting; **both are complete**
(2026-09-16). The execution-model decision 6b turned on is recorded in
[DECISION_execution_model.md](DECISION_execution_model.md) — read that before changing how
anything runs.

**Deferred out of 6b, with reasons, in the plan:** `ctl.foreach` (needs the planner to treat a
body as a re-runnable subgraph), `ctl.run_pipeline` (nested documents need their own design
pass), `ctl.throttle` (nothing to throttle until Phase 10 has a row cursor).

## What works today

**Phases 0–8 are complete.** There is a working CLI, **a canvas you can build a pipeline on**,
**a scheduler that runs them**, and **a console to watch it from**. From the repo root:

```powershell
cargo test --workspace        # 576 tests: 254 engine, 113 scheduler, 65 console, 51 e2e, 45 state, 23 secrets, 15 metadata, 10 desktop
.\target\debug\etl.exe run samples\pipelines\orders_enriched.json
.\target\debug\etl.exe validate samples\pipelines\orders_enriched.json
.\target\debug\etl.exe plan samples\pipelines\orders_enriched.json --script
.\target\debug\etl.exe components              # the registry, listed
.\target\debug\etl.exe components --manifest   # what the canvas will consume
```

The run prints `12 / 5 / 7 / 6 / 6` rows and writes `samples/out/orders_enriched.parquet`.

**Quality nodes split rather than filter.** A `qa.*` node sends the rows that passed out of
`main` and the ones that did not out of `rejected`; wiring the second to a sink is what turns a
check into a dead-letter report, and leaving it unwired drops those rows.

```powershell
.\target\debug\etl.exe run samples\pipelines\orders_checked.json   # 12 / 10+2 / 9+1 / 9 / 2 / 1
```

The split is exact by construction: accepted is `coalesce(<predicate>, false)` and rejected is
its exact negation, so a row whose predicate is *unknown* is rejected rather than lost by both
sides. A test asserts accepted + rejected = input for every validator, against real data.

**There are two execution transports, and a plan earns the second one.** Most plans go to DuckDB
as a single script, which is what every component was built against. A plan holding a control
node or a stage policy runs instead through a **persistent session** — one DuckDB process with
its stdin held open, statements sent and answered one at a time. That is what makes a stage
retryable, a failure survivable, and a branch possible. A round trip costs 0.54 ms against
41.5 ms to spawn a process, which is why per-stage execution is affordable at all.

```powershell
.\target\debug\etl.exe run samples\pipelines\orders_guarded.json
```

That sample asserts its schema and row count, logs, and branches on whether any large order
exists — writing the report only if one does. `plan.needs_session()` decides the transport;
`plan.session_reasons()` names the stages that asked for it.

**Per-stage policy** sits on a node beside `materialize`:

```jsonc
"policy": {
  "retryAttempts": 2,        // extra attempts after the first
  "retryBackoffMs": 100,     // doubling each attempt
  "continueOnFailure": true, // the run goes on; it still ends failed
  "memoryLimitMb": 512       // set and reset around this stage
}
```

`continueOnFailure` returns a **report**, not an error: the stages that ran, the ones skipped
because they read something that never got created, and the failures. The exit code is 3 either
way. Getting the report back is the entire point of asking a run to continue.

**Fifty-four components exist.** Sources: `src.cloud.http`, `src.cloud.s3`, `src.db.mysql`,
`src.db.postgres`, `src.db.sqlite`, `src.file.csv`, `src.file.excel`, `src.file.json`,
`src.file.jsonl`, `src.file.parquet`, `src.lake.delta`, `src.lake.iceberg`. Transforms:
`xf.aggregate`, `xf.cast`, `xf.dedup`, `xf.derive`, `xf.distinct`, `xf.except`,
`xf.filter`, `xf.intersect`, `xf.join`, `xf.limit`,
`xf.pivot`, `xf.rename`, `xf.sample`, `xf.select`, `xf.sort`, `xf.sql`, `xf.union`,
`xf.unpivot`, `xf.window`. Sinks: `snk.cloud.s3`, `snk.db.mysql`, `snk.db.postgres`,
`snk.db.sqlite`, `snk.file.csv`, `snk.file.excel`, `snk.file.json`, `snk.file.jsonl`,
`snk.file.parquet`. Quality: `qa.accepted_values`, `qa.expression`, `qa.not_null`, `qa.range`,
`qa.referential`, `qa.regex`, `qa.unique`. Quality assertions, which fail the run rather than
partitioning rows and so have no reject port: `qa.row_count`, `qa.schema_match`. Control:
`ctl.branch`, `ctl.fail`, `ctl.log`, `ctl.sequence`, `ctl.wait`. Everything else in the six
namespaces compiles to `UnsupportedComponent`, by design.

**There is a desktop shell, and it is a thin caller.** `apps/desktop/` is a Tauri 2 window over
five IPC commands — `list_components`, `compile_pipeline`, `validate_pipeline`, `run_pipeline`,
`preview_node` — and holds no SQL, no DuckDB, and no component list of its own. The frontend
holds none either: the palette and property panels are generated from the manifest the engine
serves. That is what keeps the GUI and the CLI interchangeable on the same file rather than
merely similar.

```powershell
npm --prefix frontend install       # once
npm --prefix frontend run dev       # vite on :5173, in one terminal
cargo run -p etl-desktop            # the window, in another
```

`npm --prefix frontend run tauri dev` does both once you want one command.

**The Rust gate does not need the frontend.** `cargo test --workspace` builds the desktop crate
without `frontend/dist` existing — verified by deleting it and rebuilding. Only an actual bundle
needs the frontend built first.

**Previewing is a read.** `preview_node` runs only the stages a node depends on and **drops the
sinks**, so looking at what a node holds can never overwrite an output file. It returns at most
500 rows and says whether there are more.

**There is a canvas.** Drag a component out of the palette, wire it up, open and save the same
JSON the CLI reads. The palette is generated from the manifest and holds no component list of
its own, so a component added to the registry appears in it with no frontend change at all —
which is the property the whole registry design exists to buy.

Wiring is refused *while the mouse is down*, with the reason, for everything the engine would
have rejected later: a node feeding itself, a port a component does not have, a second edge into
an occupied input, anything out of a sink or into a source, and any connection that would close
a loop. A quality node's `rejected` output is drawn in amber and animated, because mistaking a
dead-letter branch for the main flow is the sort of misreading that costs an afternoon.

**Saving preserves content, not bytes.** `JSON.stringify` always expands arrays, so a
hand-written `"values": ["a", "b"]` comes back on several lines. Nothing is lost or altered —
every key, every value, every ordering, including fields this version does not understand — and
formatting normalises once on first save and never moves again, so a canvas-written file
re-saves with no diff. Tested against all five committed samples.

**Properties are edited in a generated form.** Nine controls, one per property type, and no
React written for any component — a spec the panel has never seen gets a working form. Required
properties that are unanswered are marked; a value that is only there because the spec declares
a default is labelled as such, because it otherwise reads identically to one somebody chose.

Clearing a field **removes** the property rather than writing `""`, which is what makes "leave it
unset and the default applies" true. A number that will not parse is not written at all, so a
typo never becomes a value.

Renaming a node moves its edges with it — the id is the relation name in the SQL — and refuses a
duplicate, a blank, or the reserved `__rejected` suffix.

Palette entries can be **clicked** as well as dragged. Dragging is unreachable from a keyboard,
and a webview will not always start an HTML5 drag.

**The run view says what happened, and no more than that.** Each node carries its row count,
its rejected count, and — sometimes — a timing. Sometimes is the whole design: a duration is
shown only where it means what it looks like, which is a sink, a control node, or a
`memory`/`disk` materialisation on the session transport. Everything else shows **nothing**,
because a lazy view is declared in microseconds and computed later by the sink that reads it,
and `0 ms` beside the transform that cost the most is worse than a blank. The Status tab says
so once, under the table, rather than on every row.

The three commands above that print timings are `orders_guarded.json` (a control node and a
sink), and any pipeline with a per-stage policy. `orders_enriched.json` prints none at all,
which is correct: it is one script, and one invocation cannot be attributed to its stages.

**The Plan tab shows each stage's SQL in run order**, syntax-highlighted, with a toggle for the
whole script. It also names the transport — one script or session, and which stages asked for a
session — because that is invisible in the SQL and is why a retry or a branch is possible.
Highlighting is hand-written rather than Prism; the reason is in the plan, and it is that Prism
needs `dangerouslySetInnerHTML` over strings that contain user-controlled file paths.

**Per-stage policy finally has a panel**, at the bottom of the inspector under "When it fails".
Clearing a knob removes it rather than writing a zero, and emptying the last one removes
`policy` altogether. A `session` badge and one sentence say that setting any of them moves the
whole pipeline onto the session transport.

**Extensions are vendored**, not installed system-wide: `.\scripts\fetch-duckdb-extensions.ps1`
puts them under `tools/duckdb/extensions/` and the executor points DuckDB at that directory. A
component declares what it needs with `.requires_extension(...)`, and a plan emits a `LOAD`
prelude for the union.

**Adding a component** is a spec, a builder, a test, and one line in the registry inventory —
see [adding_a_component.md](adding_a_component.md).

**Pipelines are portable.** `${name}`, `${Context.name}`, `${ENV:KEY}`, `${workspace}` and
`${date}` are substituted into node properties before compilation. Values come from `--param`,
then the active context, then the parameter's declared default, then a built-in. Contexts live
in `.etl/contexts.json`; `samples/contexts.json` is a committed example, because `.etl/` is
git-ignored and so cannot hold one anybody else can read.

```powershell
.\target\debug\etl.exe contexts --contexts samples\contexts.json
.\target\debug\etl.exe run samples\pipelines\orders_by_context.json --contexts samples\contexts.json --context prod
.\target\debug\etl.exe run samples\pipelines\csv_to_parquet.json --param since=2026-03-01
```

**Secrets are encrypted at rest.** `etl secret init` makes a per-workspace AES-256-GCM key
under `.etl/keys/`; `etl secret set NAME VALUE` stores a value in `.etl/secrets.json`; a pipeline
reads it as `${SECRET:NAME}`. Values are masked everywhere a person could see them — the plan
view, the run report, and DuckDB's own error output, which quotes a failed connection string
back in full.

```powershell
.\target\debug\etl.exe secret init
.\target\debug\etl.exe secret set pg_password --stdin --description "Analytics DB"
.\target\debug\etl.exe secret list          # names and descriptions, never values
```

**Every run is recorded, and `--json` prints exactly what was recorded.** History lives in
`.etl/runs/<pipeline>.jsonl`, one JSON object per line, appended.

```powershell
.\target\debug\etl.exe run samples\pipelines\orders_enriched.json --json
.\target\debug\etl.exe runs list --limit 5
.\target\debug\etl.exe runs show <id>
.\target\debug\etl.exe runs prune orders_enriched --keep 100
```

A **failed** run is recorded too — history that only remembers successes cannot answer the
question anybody has. Nothing prunes behind your back; `runs prune` is something a person runs.
A corrupt history line is skipped rather than fatal, which is the **opposite** of the call made
for watermark state: a bad watermark silently changes what the next run loads, a bad history
line costs one record of hindsight. Failing to record does not fail the run, for the same
reason in reverse — the exit code belongs to the pipeline, not the bookkeeping.

**Lineage needs no run.** It is derived from the compiled plan, so it can go in review beside
the diff.

```powershell
.\target\debug\etl.exe lineage samples\pipelines\orders_checked.json
.\target\debug\etl.exe lineage samples\pipelines\orders_checked.json --json
```

It is **node-level, not column-level**, and the shape says so: `columns` is absent rather than
`[]`, so nobody can read "not collected" as "none". A dead-letter edge is marked `[rejected]`,
because reading it as the main flow gets the meaning backwards. A database source contributes
`schema.table` and **never** its connection string.

**A source can load only what is new.** An `incremental` block on a source node names a column
to watch; the workspace remembers the highest value loaded, and the next run reads past it.

```jsonc
"incremental": {
  "column": "order_ts",   // must be a column that only ever goes up
  "start": "2026-01-01"   // optional: where to begin before anything is remembered
}
```

```powershell
.\target\debug\etl.exe run samples\pipelines\orders_incremental.json   # 12 rows, mark recorded
.\target\debug\etl.exe run samples\pipelines\orders_incremental.json   # 0 rows, nothing new
.\target\debug\etl.exe state list
.\target\debug\etl.exe state forget orders_incremental --node read_orders
```

**The watermark advances only on a run that fully succeeded.** A run that wrote some of its
output and then failed leaves the mark behind its output, which is the recoverable direction:
the next run redoes the window rather than skipping it. A failed run says so
(`watermarks not advanced: the run failed`) rather than doing it quietly.

The comparison is strictly `>`, never `>=` — re-reading the mark would duplicate every row
sharing that timestamp. The cost is the mirror image: a row written *later* with a timestamp at
or below the mark is never seen. That is inherent to watermarking, and is why the column has to
be one that only goes up. State lives in `.etl/state/<pipeline>.json`, keyed by the document's
`name` when it has one so a renamed *file* keeps its history.

**The workspace has a scheduler.** Schedules live in their own file — `.etl/schedules.json`,
or wherever `--schedules` points — because a schedule is a property of *this workspace* rather
than of the pipeline: the same document is a five-minute job on a laptop and a nightly one in
production. `samples/schedules.json` is the committed example, since `.etl/` is git-ignored.

```powershell
$s = "--schedules", "samples\schedules.json", "--contexts", "samples\contexts.json"
.\target\debug\etl.exe schedule list @s          # what exists, and when each next fires
.\target\debug\etl.exe schedule check @s         # every pipeline it names, compiled
.\target\debug\etl.exe schedule start --once @s  # one pass over what is due
.\target\debug\etl.exe schedule start @s         # stay up. Ctrl-C to stop
```

```jsonc
{
  "name": "orders_hourly",
  "pipeline": "samples/pipelines/orders_incremental.json",
  "trigger": { "every": "1h" },   // or {"cron": "0 3 * * *"}, or {"watch": "data/inbox"}
  "enabled": true,                // default; a disabled one is still listed, saying "off"
  "context": "prod",              // as --context would
  "params": { "since": "2026-01-01" }
}
```

**A scheduled run is the same run.** It goes through the same code `etl run` does, so it is
recorded in `.etl/runs/`, it advances watermarks on the same rule, and `etl runs show` cannot
tell the difference. That is Settled decision 5's reasoning one level down: two paths that must
agree about the same file forever eventually do not.

**An interval is counted from the last recorded run**, not from when the scheduler started — so
restarting does not restart the clock, and an hourly pipeline that ran at 02:00 is due at 03:00
whether or not anything was up in between. This is the reason 8c depends on 8b rather than
merely following it. A pipeline that is **overdue** runs at once rather than waiting for the
next whole hour, and `schedule list` says `due now` rather than printing a timestamp in the past.

**It does not catch up, and it does not run two things at once.** A run that overruns its next
tick means that tick is missed, counted and reported — never queued, because for a watermarked
pipeline each run already reads everything new since the last mark. Runs are sequential, and a
workspace lock keeps a second scheduler out; between them, `crates/state`'s single-writer
assumption stays *true* rather than becoming something to hope about. A hand-run `etl run`
alongside a scheduler is still unguarded, deliberately.

**Being behind and missing a tick are counted separately.** Behind is downtime — how many
intervals passed between the last run and startup — and is said once in the banner. Missed is
overrun, measured from when a run started, and is said at the end. They send you looking in
different places, so conflating them would be worse than saying neither.

**Cron is five fields and always UTC.** `*`, `n`, `a-b`, `*/step`, `a-b/step`, lists, `jan`–`dec`
and `sun`–`sat` names, and the `@hourly`/`@daily`/`@weekly`/`@monthly`/`@yearly` shorthands. Both
day fields restricted means **either** matches, which is what every Unix cron does — `0 0 13 * fri`
is the 13th *and* every Friday, not Friday the 13th. A `tz` field is **refused with the reason**
rather than approximated (Settled decision 6), and an expression that cannot fire within five
years reports `never` rather than parsing happily and doing nothing forever.

**A file-watch polls, and fires only once a change has settled.** Stable across two polls, so a
2 GB CSV still being copied into the inbox is never read half-written; with the default
ten-second poll that costs ten to twenty seconds of latency. The first poll takes a baseline
and never fires, so a restart does not reprocess an inbox that has been sitting there since
yesterday. **Immediate entries only** — what happens below a subdirectory rides on that
directory's own mtime, which NTFS defers, so watch the directory whose files actually matter.

**The lock is a held handle, not a file that exists.** Ctrl-C is how you stop a foreground
scheduler, so an existence check would be left behind on almost every stop and every restart
would need `--force` — a guard people learn to bypass by reflex. On Windows the file is held
with no sharing and the OS releases it however the process dies; elsewhere it falls back to an
exclusive create and `--force`, and the error message differs per platform because the
situations genuinely do. Who holds it is written to a readable `.etl/scheduler.status` beside
it, since the lock itself cannot be opened while held.

**There is a web console.** `etl serve` binds loopback, mints a pair of tokens, and prints a
link for each. It lists the workspace's pipelines with whether each one compiles and how its
last run went, shows what is scheduled and when it next fires, shows recent run history, and
lets an **operator** start a run.

```powershell
$s = "--schedules", "samples\schedules.json", "--contexts", "samples\contexts.json"
.\target\debug\etl.exe serve @s                      # http://127.0.0.1:8087
.\target\debug\etl.exe serve --port 9000 @s
.\target\debug\etl.exe serve --bind 0.0.0.0 @s       # warns, loudly, and says why
```

**Two roles, and both have powers the other does not.** A viewer reads; an operator reads and
starts runs. A third role that could do exactly what the second can would be decoration rather
than access control, and there is nothing else to gate — secrets are not exposed over HTTP at
all, and editing a pipeline is the canvas's job.

**Tokens are minted per process and printed once**, the way a local notebook server does.
Nothing is stored, so there is no token file to leak and a console that has been stopped cannot
be reached with yesterday's link. A stable token — for CI, or a console that restarts — comes
from `ETL_CONSOLE_OPERATOR_TOKEN` and `ETL_CONSOLE_VIEWER_TOKEN`, **never a flag**, because an
argument is visible in the process list; that is the call already made for `etl secret set`. A
token taken from the environment is not printed, since that would put a standing secret in the
scrollback and the CI log of every run. Setting both variables to the same value is **refused at
startup**: it silently promotes every viewer to an operator.

**A `?token=` works on the page and nowhere else.** The printed link has to carry one to open
anything; the page moves it out of the address bar on load, keeps it in session storage, and
sends a header from then on. The API takes only the header — which is what stops a console link
pasted into a chat from being a usable credential, and stops another site's form from posting
one for you.

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8087/api/pipelines
curl -H "Authorization: Bearer $TOKEN" -X POST 'http://127.0.0.1:8087/api/runs?pipeline=orders_enriched'
curl http://127.0.0.1:8087/api/health   # the one route with no token, and it says only "ok"
```

**Eight routes.** The page, health, pipelines, one pipeline's lineage, runs, one run, schedules,
and a POST to start a run. Every authenticated response carries `X-Etl-Role`, which is how the
page knows whether to draw a Run button rather than guessing it from an error message.

**It is not a public service, and it says so.** Loopback unless told otherwise, no TLS, no
accounts. Binding elsewhere prints a warning naming the actual exposure — the tokens cross that
network in clear — and points at a reverse proxy or an SSH tunnel.

**A pipeline name from the network is resolved by lookup, never joined onto a path.** A name
that is not in the workspace's own list finds nothing, so `../../etc/passwd` is a 404 rather
than a file read. Everything the page renders goes in through `textContent`, so a pipeline named
`<img onerror=…>` is a string rather than script running with an operator's token; a test
asserts the page contains no `innerHTML` and never gains one.

**Runs the console starts are the same runs.** They go through the same `perform` that `etl run`
and the scheduler use, so they are recorded in history and advance watermarks identically, and
`etl runs show` cannot tell where one came from. They are serialised by a mutex, so two people
clicking Run cannot race; running beside a *scheduler* is the same unguarded case a hand-run
`etl run` is.

**Nodes can be materialised.** `"materialize": "auto" | "view" | "memory" | "disk"` on a node.
`view` is the lazy default, `memory` a temp table, `disk` a Parquet spill under `.etl/tmp/` that
the executor clears up afterwards. Every mode gives the same answer; there is a test that says
so.

**Deliberately not built in Phase 4:** XML (DuckDB has no core reader — only a community
extension, which sits badly with Phase 9's vendored set) and DuckLake (a catalog format that
needs its own design pass rather than a thirteenth copy of the ATTACH shape). Both are listed
under Phase 4 in the plan, so both need a decision recorded there rather than quietly dropping.

**Not built yet:** Phases 9–12, and the three control components deferred out of 6b. **The canvas cannot edit an `incremental` block or a schedule** — both survive a GUI
round trip untouched, but there is no panel for either; a schedule is not in the pipeline
document at all, so its panel is a workspace-level screen rather than an inspector tab, and
that is 8d's shape rather than 7's. i18next and Vega are in the plan's stack note and stay
uninstalled until the thing that needs them exists; lucide-react arrived with the palette in 7b,
and 7d added nothing, having written its own SQL highlighter rather than taking Prism.

**Deferred out of 6a, on purpose:** `qa.row_count` and `qa.schema_match`. Both assert something
about a whole relation rather than partitioning it — no reject rows, and the only outcome is to
fail the run. That is `ctl.fail`'s shape and it needs 6b's execution-model decision first.

## Phase status

| # | Phase | Status | Date |
|---|---|---|---|
| — | Research: Duckle teardown ([ET_Local_Tool.md](ET_Local_Tool.md)) | done | 2026-09-15 |
| — | Source verification against `D:\workspace\duckle-main` | done | 2026-09-15 |
| — | Plan written and signed off | done | 2026-09-15 |
| 0 | Workspace skeleton + pipeline document model | **done** | 2026-09-15 |
| 1 | DAG validation + topological sort + plan skeleton | **done** | 2026-09-15 |
| 2 | SQL lowering (8 components) + CLI executor | **done** | 2026-09-15 |
| 3 | Component spec registry | **done** | 2026-09-15 |
| 4 | Connector breadth wave 1 (~40 components) | **done** (40) | 2026-09-15 |
| 5 | Parameters, contexts, secrets, materialization | **done** | 2026-09-15 |
| 6 | Quality nodes, reject ports, control flow | **done** | 2026-09-16 |
| 6a | — quality nodes and reject ports | **done** | 2026-09-16 |
| 6b | — control flow, per-stage policy, persistent session | **done** | 2026-09-16 |
| 7 | Desktop app (Tauri 2 + React 19 + xyflow) | **done** | 2026-09-16 |
| 7a | — Tauri shell and its five IPC commands | **done** | 2026-09-16 |
| 7b | — the canvas | **done** | 2026-09-16 |
| 7c | — the generated property panel | **done** | 2026-09-16 |
| 7d | — the run view, Plan tab, and the policy panel | **done** | 2026-09-16 |
| 8 | Headless runner: serve, scheduler, RBAC, incremental | **done** | 2026-09-16 |
| 8a | — watermark incremental loading | **done** | 2026-09-16 |
| 8b | — the runner: history, `--json`, lineage | **done** | 2026-09-16 |
| 8c | — scheduler: interval, cron, file-watch | **done** | 2026-09-16 |
| 8d | — web console: serve, token auth, roles | **done** | 2026-09-16 |
| 9 | Standalone binary export + air-gapped packaging | not started | |
| 10 | Rust-native connectors | not started | |
| 11 | AI assistant + MCP server | not started | |
| 12 | Benchmarks + parity audit | not started | |

## Settled decisions

1. **Crate naming** — `etl-` prefix mirroring Duckle's crate names, product binary `etl`.
   Agreed 2026-09-15. Closed; reopening it after Phase 1 is expensive.
2. **DuckDB version floor** — **stable v1.5.5**, not the LTS line. Agreed 2026-09-15; applied in
   Phase 2 (`exec::PINNED_DUCKDB_VERSION`, and the default in `scripts/fetch-duckdb.ps1`).
3. **Extensions are vendored into the project, not installed system-wide.** Agreed 2026-09-15,
   the same call already made for the CLI binary. `scripts/fetch-duckdb-extensions.ps1` runs
   `SET extension_directory=<project>; INSTALL <name>` so the files land under
   `tools/duckdb/extensions/` and nothing outside the project changes. The executor finds that
   directory the same way it finds the binary (explicit path, then `ETL_DUCKDB_EXTENSIONS`, then
   a search upward) and emits `SET extension_directory=...` ahead of the `LOAD` prelude. This
   makes Phase 9's air-gapped path the *only* path, exercised from now on rather than discovered
   at the end.
4. **Cryptography is RustCrypto's `aes-gcm`.** Agreed 2026-09-15. Pure Rust, so the Phase 9
   cross-builds need no C toolchain. **Cargo resolved it to 0.10.3, not the current 0.11.1,**
   because 0.11 declares `rust-version = 1.85` and this workspace declares 1.80 — MSRV-aware
   resolution doing exactly its job, and 0.10.3 is the mature, widely-deployed line. Raising the
   MSRV to get 0.11 would be a decision in its own right and has not been taken. 21 transitive
   crates, all RustCrypto core plus `getrandom`/`libc`; the first dependencies this project has
   taken beyond serde, clap and thiserror.

5. **The runner is subcommands on `etl`, not a separate `etl-runner` binary.** Agreed
   2026-09-16, diverging from the plan's wording for Phase 8. `etl` already has `run` and
   `validate`; a second binary with its own copies is two code paths that have to agree about
   the same file forever, which is the thing Phase 7 spent its effort *avoiding* between the
   GUI and the CLI. 8b adds `--json` output and run history to what is there. The `etl-runner`
   name is reserved for Phase 9's standalone export, which is a genuinely different artifact:
   one self-contained file with a pipeline baked into it.

6. **Schedules are UTC and interval only; no timezone database.** Agreed 2026-09-16. The cron
   *expression* is parsed by hand — it is a short, well-understood grammar, the same call made
   for the topological sort and civil dates. The **timezone database** is not, and must not be:
   it changes several times a year, and a stale copy is wrong *silently*, at 2am, twice a year.
   A `tz` field is **refused with an error** rather than accepted and approximated, because a
   schedule that quietly runs an hour off is worse than one that will not start. Revisit only
   when someone actually needs local-time scheduling, and take `jiff` if so.

7. **File-watch schedules poll `mtime`; no `notify`.** Agreed 2026-09-16. The only thing native
   events buy is latency, and for "a file landed, run the pipeline" a ten-second poll is
   indistinguishable from instant. Native events are also genuinely unreliable on network and
   virtual filesystems, which is where a watched inbox most often lives — so the dependency
   would buy speed in the easy case and nothing in the hard one.

8. **The 8d console uses `tiny_http`.** Agreed 2026-09-16. Small and blocking, with no async
   runtime; a console serving a handful of local requests needs nothing more, and routing for
   ~8 endpoints is less code than wiring a framework. `axum` was the alternative and brings
   tokio, tower and hyper into a workspace that has four external crates. **Hand-rolling HTTP
   was considered and rejected** — unlike the topological sort and the date conversion, this one
   parses untrusted input off a socket and checks auth tokens, which is a different risk class,
   and "write your own HTTP server" is the wrong instinct there.

## Open decisions

None. (Resolved 2026-09-16: the Phase 8 dependency posture and the runner's shape — see
Settled decisions 5–8.)

<details>
<summary>Resolved: which cryptography dependency?</summary>

1. **The secrets crate needs a cryptography dependency.** The plan specifies AES-256-GCM, and
   hand-rolling that is not an option — the rest of this project has been kept dependency-light
   deliberately (no `petgraph`, no date crate), but cryptography is the one place where writing
   it yourself is the wrong call every time.

   The obvious choice is RustCrypto's `aes-gcm` plus a key-derivation crate. That is a real
   supply-chain decision rather than a technical one, so it is worth taking deliberately rather
   than discovering in a diff. Alternatives: `ring` (fewer crates, C and assembly), or the OS
   keychain (DPAPI on Windows), which moves the problem outside the project and breaks the
   air-gapped, copy-the-folder story the rest of Phase 9 depends on.

   **Chosen: RustCrypto `aes-gcm`.** See Settled decisions 4.

</details>

<details>
<summary>Resolved: may DuckDB extensions be installed on this machine?</summary>

1. **May DuckDB extensions be installed on this machine?** Every remaining Phase 4 family needs
   one, and none are installed: `excel`, `postgres_scanner`, `mysql_scanner`, `sqlite_scanner`,
   `iceberg`, `delta` and `ducklake` all report `installed = false`; `httpfs` is installed but
   not loaded. `INSTALL` writes to `C:\Users\mr\.duckdb\extensions\`, which is outside the
   project, so the workflow requires approval first.

   Without it those components can still be **written** — the golden-SQL tests are string
   comparisons and need no extension — but their SQL cannot be **verified against a real
   DuckDB**, and that check has caught every real mistake so far. The PIVOT-in-a-view constraint
   recorded below was found exactly that way, after the component had already been written and
   its golden test was passing. Writing thirteen unverified connectors is the thing most likely
   to produce work that looks finished and is not.

   Options: install locally; vendor the extension files under `tools/duckdb/` so Phase 9's
   air-gapped path is exercised from the start; or write them with golden tests only and mark
   the execution tests ignored-by-default.

   **Chosen: vendor into the project.** See Settled decisions 3.

</details>

## Environment

- Repo: `d:\workspace\ETL_Local_Tool`, branch `main`, 7 commits, pushed to
  `github.com/marun224/local_etl_tool` (private). Renamed from `master` on 2026-09-16 while
  the remote was still empty. Phases 0–5 are one commit; 6a onward commit per phase.
- Duckle reference checkout: `D:\workspace\duckle-main` (read-only reference; clean-room rules
  apply — architecture and behaviour, never source).
- Toolchain verified 2026-09-15: **cargo/rustc 1.96.0**, **node v24.18.0**, **npm 11.16.0**.
  `rust-toolchain.toml` pins 1.96.0 with rustfmt and clippy.
- **DuckDB v1.5.5 (Variegata) vendored** at `tools/duckdb/duckdb.exe`, fetched by
  `scripts/fetch-duckdb.ps1` (idempotent, pinned, re-runnable). Git-ignored — it is a 37 MB
  build input, not source. **Not installed system-wide**, deliberately: the workflow requires
  approval for global installs, and pinning locally means the version the project runs against
  is the version it was tested against. `winget` is available if a global install is ever
  wanted. Binary lookup order for the executor: `ETL_DUCKDB_BIN`, then `tools/duckdb/`,
  then PATH.
- **DuckDB extensions vendored** at `tools/duckdb/extensions/` (9 files, 247 MB), fetched by
  `scripts/fetch-duckdb-extensions.ps1` — idempotent, re-runnable, and it verifies that each one
  actually loads rather than only that it downloaded. Git-ignored, like the CLI, and installed
  with `SET extension_directory` so nothing is written outside the project. Lookup order for the
  executor: `RunOptions::extension_dir`, then `ETL_DUCKDB_EXTENSIONS`, then a search upward for
  `tools/duckdb/extensions/`. Present: `avro`, `delta`, `ducklake`, `excel`, `httpfs`,
  `iceberg`, `mysql_scanner`, `postgres_scanner`, `sqlite_scanner`.

## Known gaps and discoveries

- Six of Duckle's crates (`execution-core`, `runtime`, `workflow-engine`, `transform-engine`,
  `stream-engine`, `slothdb-engine`) are **doc-comment stubs with zero implementation**. All
  real code is in `duckdb-engine` (133k LOC) and `duckle-runner` (28k LOC). The plan
  deliberately does not reproduce the stub crates.
- **`ET_Local_Tool.md` has five source-verified errors.** Four are corrected at the top of the
  plan: the node parameter key is `data.properties` (not `config`), component count is ~417 (not
  385), there is no Zustand, and the crate layering is mostly aspirational. The fifth was found
  during Phase 0: edges are plain ReactFlow
  (`id`/`source`/`target`/`sourceHandle`/`targetHandle`), *not* the `"from": "n1.main"` form the
  report shows. Treat the report as background, the plan as current.
- Duckle's own open gaps we should not inherit: no versioned/migratable workspace format
  (their issue #299 — we carry `formatVersion` from Phase 0), and `validate` not catching every
  missing required property (fixed in our Phase 3).
- **Divergence from Duckle, deliberate:** every struct carries a `#[serde(flatten)] extra` map,
  so a document written by a newer version survives a load/save by an older one. Duckle drops
  unrecognised keys. This costs nothing now and is very hard to retrofit later.

### From Phase 1

- **`StageKind` has five variants, not the three the plan text named.** `Source`, `Transform`,
  `Sink`, `Quality`, `Control` — the namespace already tells us which, so classifying all six
  namespaces now avoids a breaking change when `qa.*` and `ctl.*` arrive in Phase 6.
- **Disabled nodes cascade.** Dropping a node drops everything downstream of it, because a
  transform whose input was switched off would otherwise compile to SQL selecting a relation
  that was never created — failing at runtime with a DuckDB "table not found" that says nothing
  about the switch someone flipped. Each drop emits a warning naming the disabled node.
- **Validation runs before the drop**, so a broken node that is switched off still reports its
  error rather than going quiet until someone switches it back on.
- **Topological ties break by document order.** Deliberate: Phase 4's golden-file SQL tests are
  worthless if plan order can vary between runs. Covered by a test that compiles the same
  document 16 times.
- **`compile()` returns warnings as well as errors** — orphan nodes, disabled drops, and a
  pipeline with no sink (which compiles fine and then does nothing, since every non-sink stage
  is a lazy view). Warnings do not stop a run.
- **`Stage::from` is the upstream node id, never the alias**, because the node id is the relation
  the engine actually creates; the alias is an extra view on top.
- No `petgraph` dependency — Kahn's algorithm is ~30 lines and hand-rolling it gave control over
  deterministic tie-breaking and error messages that name the specific nodes in a cycle.

### From Phase 8c

- **Directory mtimes are not a reliable signal for nested content.** Two watch tests asserted
  that a file created one level down fires and an edit does not; both failed, in *opposite*
  directions. NTFS defers directory timestamp updates, so neither is guaranteed. The top level
  is solid for a reason unrelated to the directory's clock — a new file is an entry with its
  own fresh mtime, a removed one changes the summed length — so the contract is "immediate
  entries", with the measurement written into the module docs rather than a behaviour asserted
  that the filesystem does not promise.
- **The civil-date conversion now has two copies, not three.** `etl_state::time` is the shared
  one, promoted out of the state crate so the scheduler could use it and gain the inverse plus
  a weekday. **The engine's `${date}` still has its own** — `etl-duckdb-engine` does not depend
  on `etl-state`, and making it do so is a change to the engine rather than to the scheduler.
  Worth folding in whenever something else touches `params.rs`.
- **`PipelineDoc::resource_pool` is still read by nothing.** 8c schedules without admission
  pools, as the plan's note said it would. The field stays unused until a phase claims it.
- **Ctrl-C kills a run in flight.** There is no signal handling — that needs a dependency this
  workspace has not taken — so stopping a scheduler mid-run terminates DuckDB with it. Safe by
  construction rather than by care: a watermark advances only on a run that fully succeeded, so
  the next run redoes the window. A half-written output file is possible, and `mode:
  "overwrite"` is what makes that recoverable.
- **`schedule start` returns exit 3 if any run failed**, matching `run`'s codes. A scheduler
  staying up therefore only reports at the end, which is fine for `--once` and means nothing
  for a long-lived one.

### From Phase 8d

- **The console is the first code here that takes untrusted input off a socket**, and the
  dependency posture reflects it: `tiny_http` rather than a hand-rolled server (Settled decision
  8), and the five crates it brings are the whole of Phase 8's dependency budget.
- **A `?token=` is accepted on the page and refused on the API.** Worth knowing before somebody
  "fixes" it: it is not an oversight. It is what stops a console link in a chat log from being a
  working API credential and stops another origin's form from posting one.
- **The page has no build step and must not gain one.** It is a string in `ui.rs` so the
  headless runner can serve its own console; Phase 9's standalone binary inherits that for free.
  A test asserts the page loads nothing external, so it cannot drift into working only where
  there is a network.
- **`innerHTML` is forbidden in `ui.rs` and there is a test that says so.** Everything the page
  renders is workspace data somebody else wrote.
- **Two roles is the whole model, and a third would need a reason.** `Role::allows` is an
  ordering rather than a permission set, which is right while one role is strictly the other
  plus one power and wrong the moment it is not.
- **`etl serve` holds the terminal and has no shutdown path.** Ctrl-C is how it stops; a
  shutdown route nothing calls is a route nothing tests. On Windows a leftover `etl.exe` keeps
  its port — `taskkill /F /IM etl.exe`, since `pkill` does not exist in Git Bash here. A port
  already in use is reported clearly and exits 1.
- **Pipelines are found by scanning, not by a manifest.** Any `.json` under the workspace that
  parses as a document with nodes, at most four levels deep, skipping `.etl/`, `target/`,
  `node_modules/` and dot-directories. A manifest would be a second place to keep in step with
  the folder, and the folder is the source of truth everywhere else in this product.
- **The console compiles every pipeline on every listing.** That is what lets it say which ones
  will not run, which is the thing worth knowing before 3am — but it is real work per page
  refresh, and a workspace with many pipelines is where that would first be felt.

## Session log

### 2026-09-15 — Session 1
Read the Duckle teardown report. Recorded the reference checkout path in the report. Verified
the checkout at file level: workspace manifest, crate LOC, pipeline document model, component-id
count, frontend dependencies, DuckDB CLI invocation. Wrote the plan and this tracker. No code
written; nothing committed.

### 2026-09-15 — Session 2 (Phase 0)
Naming and DuckDB-version decisions settled; plan signed off. Verified the toolchain. Built the
workspace skeleton and `etl-metadata`:

- `Cargo.toml` (workspace, resolver 2), `rust-toolchain.toml` (1.96.0), `.gitignore`
- `crates/metadata/src/lib.rs` — `PipelineDoc`, `PipelineNode`, `NodeData`, `PipelineEdge`,
  `EdgeData`, `Position`, `Column`, `DataType`, `ParameterSpec`, `Schema`, `Extra`
- `samples/pipelines/csv_to_parquet.json` — the 3-node sample Phase 2 will execute

Gate green: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace` (8 passed). Nothing committed.

### 2026-09-15 — Session 3 (Phase 1)
Built `etl-duckdb-engine`: graph validation, deterministic topological sort, and the stage
skeleton.

- `crates/duckdb-engine/src/lib.rs` — `EngineError` (7 variants, each naming the node or edge
  at fault) plus `node_id()` so the canvas can highlight the right box
- `crates/duckdb-engine/src/plan/mod.rs` — `compile()`, `Plan`, `Stage`, `StageKind`, `Input`,
  `Warning`
- `crates/duckdb-engine/src/plan/tests.rs` — 30 tests

Gate green: fmt clean, clippy clean with `-D warnings`, **38 tests passing** (30 engine,
8 metadata). Nothing committed.

`Stage.sql` is deliberately still empty — Phase 2 fills it.

### 2026-09-15 — Session 4 (Phase 2 prerequisites)
Cleared everything Phase 2 needs before it can start.

- **DuckDB v1.5.5 vendored** to `tools/duckdb/`, with `scripts/fetch-duckdb.ps1` to reproduce it.
- **`samples/data/orders.csv`** — the 12-row acceptance fixture the sample pipeline reads.
- **Smoke-tested the CLI's real behaviour** and wrote it into the plan as a Phase 2 addendum.
  Four findings change the executor's design, listed below.

No code written. Nothing committed.

#### Phase 2 design constraints, established by smoke test

- **`-json` returns concatenated JSON arrays, one per statement** — not one array per batch. The
  executor must stream-parse stdout; `serde_json::from_str` over the whole buffer will fail.
- **`COPY` emits no row count.** Sink counts need an explicit `SELECT count(*)` against the
  sink's `from` relation.
- **A failed statement aborts the remainder of the batch** (exit 1, stderr). Batching the plan
  into one `-c` gives fail-fast for free, and the number of JSON values on stdout pinpoints the
  failing stage.
- **Windows backslashes are a non-issue** — single-quoted literals do no escape processing, so
  all three path spellings work. The real escaping risk is quotes: doubling `"` in identifiers
  and `'` in literals, verified to fail loudly when omitted.

### 2026-09-15 — Session 5 (Phase 2)
The vertical slice: `etl run` now moves real data.

- `crates/duckdb-engine/src/sql.rs` — `quote_identifier`, `quote_literal`, `quote_path`
- `crates/duckdb-engine/src/plan/builders.rs` — the eight components, lowered
- `crates/duckdb-engine/src/exec.rs` — binary discovery, one-script execution, stream parsing,
  row counts, stage attribution, sink preparation
- `crates/cli/` — the `etl` binary: `run`, `validate`, `plan`
- `samples/data/customers.csv`, `samples/pipelines/orders_enriched.json` — acceptance fixture
- `crates/duckdb-engine/tests/end_to_end.rs` — 8 tests against real DuckDB

Gate green: fmt clean, clippy clean with `-D warnings`, **95 tests passing** (79 engine unit,
8 end-to-end, 8 metadata). Nothing committed.

**Acceptance, verified from a cold shell at the repo root:** 12 orders + 5 customers → filter
keeps 7 → inner join keeps 6 (order 1010 drops, customer C006 is absent) → 6 rows written to
Parquet with the 8-column merged schema. Content checksum `3a528b4e…` pinned in the test.

#### From Phase 2

- **Per-stage *timings* are not reported, only per-stage row counts.** In a plan of lazy views
  every transform would report ~0 and the sink would report the whole pipeline's work, so a
  per-stage number would be actively misleading. `RunReport::elapsed` is one honest wall-clock
  figure. Real per-stage timing needs materialisation (Phase 5) or DuckDB profiling.
- **Row counts are not free.** A `SELECT count(*)` probe forces its view to materialise, so
  counts-on evaluates the lazy chain for the probe as well as for the sink. `--no-counts` exists
  for the fastest path; counts are on by default because per-node counts are core to the canvas.
- **Stage attribution falls out of the count probes.** Each stage emits exactly one JSON array,
  so the number that arrived before a failure identifies the stage that failed. With
  `--no-counts` there is nothing to count and errors stay unattributed — a real trade-off, and
  the reason counts default on.
- **Relative paths resolve from the current directory**, not from the pipeline file. `--workdir`
  overrides. Phase 5's `${workspace}` is the real answer to portability; two competing rules in
  the meantime would be worse than one plain one.
- **`samples/pipelines/csv_to_parquet.json` does not run yet** — it uses `${workspace}` and
  `${since}`, which land in Phase 5. It still *validates*, because validation is a compile check,
  not a filesystem check. `orders_enriched.json` is the runnable Phase 2 sample.
- **Exit codes are fixed:** 0 ok, 1 usage/IO, 2 invalid pipeline, 3 run failed.
- **Sinks are prepared before DuckDB starts** — output directories are created, and
  `error_if_exists` refuses without writing anything.

### 2026-09-15 — Session 6 (Phase 3)
The component registry: one table, and everything else derived from it.

- `crates/metadata/src/component.rs` — `ComponentSpec`, `PropertySpec`, `PortSpec`,
  `PropertyType`, `Namespace`. In the metadata crate so the desktop app can read specs without
  depending on SQL generation.
- `crates/duckdb-engine/src/plan/specs.rs` — the registry, property resolution, the manifest
- `crates/duckdb-engine/src/plan/specs/tests.rs` — registry-wide invariants
- `crates/cli` — `etl components [--namespace NS] [--manifest]`
- `docs/adding_a_component.md`

Gate green: fmt clean, clippy clean with `-D warnings`, **128 tests passing** (105 engine unit,
8 end-to-end, 15 metadata). Nothing committed.

#### From Phase 3

- **There is no dispatch `match` anywhere in the engine.** The registry stores the builder as a
  function pointer beside its spec, so a component cannot be half-registered — specced but not
  buildable, or buildable but invisible to the canvas.
- **The "done" criterion was verified, not assumed.** `src.file.jsonl` was added by following
  `docs/adding_a_component.md`: three files touched (spec, builder, test), and the only test that
  broke was the registry inventory, which is *designed* to break so that adding a component shows
  up in a diff. The doc says three-plus-inventory rather than the three I first claimed.
- **Builders no longer restate defaults.** Defaults live in the spec and are applied before
  lowering. The old `.unwrap_or(true)` / `.unwrap_or("zstd")` were second copies waiting to
  drift.
- **Every required property is now checked, by name.** Previously only the ones a builder
  happened to read were — which is exactly Duckle's open gap. A component that gains a required
  property gains the check for free.
- **The spec expresses per-property rules only.** `xf.join` needing `keys` *or* `condition`
  spans two properties, so it stays in the builder. A test
  (`a_rule_spanning_two_properties_stays_in_the_builder`) pins that division so it stays
  deliberate.
- **An unknown property is a warning, not an error**, and is carried through rather than
  dropped. It is usually a typo, but it is also what a document from a newer version looks like,
  and refusing to run someone's pipeline over an extra key would be the wrong trade.
- **Two golden-SQL changes, both from spec defaults:** the CSV sink now always writes
  `DELIMITER ','` (a sink must choose a delimiter; a source can sniff one), and an invalid join
  type is now caught by the spec's enum with a message listing the valid options, rather than by
  the builder.
- **Clippy caught an MSRV violation** — `is_none_or` is stable in 1.82 but the workspace declares
  1.80. Rewritten rather than bumping the declared MSRV.

### From Phase 4 (part done)

- **A PIVOT cannot live in a view unless its values are listed.** DuckDB refuses outright:
  "PIVOT statements with pivot elements extracted from the data cannot be used in views." Every
  stage in a plan *is* a view, so `xf.pivot` has a **required** `values` property, which no
  other pivot UI asks for. Found by execution test, after the golden test was already green —
  the clearest argument yet for running the SQL rather than only comparing it.
- **`LOAD` on an uninstalled extension fails hard, even with autoinstall on.** DuckDB's
  `autoinstall_known_extensions` and `autoload_known_extensions` are both `true` by default,
  but they trigger on *use* of a function, never on an explicit `LOAD`. So the prelude is
  strictly more brittle than relying on autoload — and that is the point: a run must fail
  loudly and early rather than quietly downloading an extension mid-pipeline, which is exactly
  what Phase 9 forbids.
- **The prelude needs a probe.** `LOAD` returns no rows, so it prints no JSON, so a failed
  prelude and a failed first stage looked identical to the executor: nothing arrived either way.
  `PRELUDE_PROBE` (`SELECT 0 AS n;`) is emitted after the LOADs when counts are on, so "nothing
  arrived" now means the prelude and only the prelude. `ExecError::ExtensionLoadFailed` names
  the extensions rather than blaming an innocent stage.
- **`Plan::extensions()` is derived, not stored.** It is the sorted union over the stages'
  specs, computed on demand, so it cannot disagree with the components actually in the plan.
- **`PropertyType` grew a `Map`,** as Phase 3 predicted it would. `xf.rename` (old name to new)
  and `xf.cast` (column to type) both need ordered string pairs. Order is preserved because
  `serde_json` is built with `preserve_order`, and it has to be: the pairs become SQL in the
  order they were entered.
- **A SQL type name is the one document string that reaches a statement unquoted.** Quoting it
  would break `DECIMAL(10,2)` and `VARCHAR[]`. `type_name()` restricts it to the characters a
  type can be spelled with instead, and a test fires `INT); DROP TABLE orders; --` at it.
- **A percentage sample must say `reservoir`.** DuckDB's default system sampler works a row
  group at a time and returns *nothing at all* from a small input, which reads as a broken
  pipeline rather than as a choice of sampling method.
- **The Excel sink writes no header unless told to**, and `read_xlsx(header=true)` then eats the
  first row of real data — a 12-row write came back as 11. Caught by the round-trip execution
  test; a golden test comparing strings could not have seen it. `snk.file.excel` now defaults
  `header` to true, matching `snk.file.csv`.
- **`INSERT INTO` fails when the table is not there**, so an appending pipeline failed on its
  first run and worked ever after — the worst shape a bug can take. `append` now emits
  `CREATE TABLE IF NOT EXISTS <t> AS SELECT ... WHERE false;` ahead of the insert; it is
  idempotent and costs nothing on later runs. Also caught by an execution test.
- **A pipeline that reads and writes the same database table sees its own writes.** Every stage
  is a lazy view, so the count probe re-evaluates the source view *after* the insert and reports
  the post-write total. Not a bug to fix — it falls out of laziness, and Phase 5's
  materialisation is what would change it — but surprising enough to be worth knowing.
- **Nothing creates the output directory for a database sink.** `prepare_sinks` works from the
  `path` property and a database sink carries a `connection` string instead. Harmless against a
  server, but `snk.db.sqlite` pointed at a file in a directory that does not exist fails at
  ATTACH. Worth fixing when Phase 5 gives connections a real shape.
- **No XML component, deliberately.** DuckDB has no core XML reader; the only route is a
  community extension, which sits badly with Phase 9's vendored, air-gapped extension set. The
  plan text lists XML under Phase 4 — that line needs revisiting rather than quietly satisfying.

### From Phase 5

- **Substitution is single-pass, deliberately.** A value that is substituted in is never itself
  scanned for `${...}`. That rules out runaway expansion, and it rules out the uglier case: a
  `--param` value containing `${ENV:AWS_SECRET_ACCESS_KEY}` that would otherwise read the
  environment of the process that ran it. A test pins it.
- **Only node properties are interpolated** — not labels, ids or positions. A parameter belongs
  in a node's configuration, not in its name on the canvas.
- **`--workdir` became `--workspace`, and now means both things.** It is what `${workspace}`
  expands to *and* where relative paths resolve from. Two flags for those would eventually
  disagree, and a pipeline whose interpolated paths and relative paths point at different roots
  is very hard to debug. `--workdir` is kept as an alias.
- **A required parameter with an empty value is rejected**, because `--param since=` is a slip
  rather than a deliberate empty string, and a required *property* is already refused the same
  way. An optional parameter may still be empty.
- **A required parameter *may* have a default, unlike a required property.** The property rule
  (required or default, never both) is enforced by a test; for parameters the two mean different
  things, because `required` also tells the canvas to prompt, which is worth saying even when
  there is something to prompt with.
- **No date crate.** `${date}` needs today's date, which is Howard Hinnant's `civil_from_days` in
  about fifteen lines — the same call as hand-rolling Kahn's algorithm in Phase 1. Tested
  against the cases that actually catch mistakes: 2000 has a 29th of February, 1900 does not.
- **A misspelled `--context` is an error, never a fallback.** Running against dev because "prod"
  was typed "prd" is the worst outcome available. An `active` naming a context that is not
  defined is likewise caught when the file loads, rather than silently meaning "no context".
- **`.etl/` is git-ignored, so it cannot hold a shareable example.** `samples/contexts.json` is
  committed as the thing to copy; the real file lives at `.etl/contexts.json`.
- **An unknown materialisation mode is a warning, not an error.** The mode changes how the work
  is done and never what the answer is, so falling back to `auto` is safe — which is not true of
  an unknown component, and the difference is why the two are handled differently.
- **A spill path is relative and derived from the node id**, so [`compile`] stays pure: the path
  in the generated SQL is the path that will be written, with nothing substituted in later. The
  cost: two concurrent runs of the same pipeline in the same directory would share spill files.
  Fine for a CLI; **Phase 8's scheduler will have to give each run an id.**
- **Spill cleanup is best-effort and reported as a count.** A scratch file that will not delete
  should not fail a run that otherwise succeeded, so `RunReport::spilled` says how many were
  actually cleared rather than promising that none remain.
- **Materialisation does not change what downstream nodes select from.** The relation is named
  after the node whichever way it is realised, so switching a node to `memory` changes one
  statement and nothing else. There is a test for that, because it is the property that makes
  the mode safe to change.

#### From the secrets crate

- **`PropertyType` did not need a `Secret` variant after all.** The Phase 4 note predicted one.
  It is unnecessary because the *reference* is what lives in the document — a `connection`
  property holds `password=${SECRET:pg}`, which is ordinary text and safe to commit. Only the
  resolved value is sensitive, and that never reaches the document at all. A `Secret` property
  type would have been a second mechanism doing the first one's job.
- **DuckDB quotes the whole connection string back in its errors.** A failed `ATTACH` reports
  `Unable to connect to Postgres at "dbname=... password=hunter2"`, and that text goes straight
  into `ExecError` and out to a terminal. Redacting the generated script alone would have looked
  complete and leaked anyway; the executor masks stderr too, and there is an end-to-end test
  that fails a real connection on purpose to prove it.
- **The mask has to survive being displayed, not just be removed.** `redact` replaces the value
  with `********` rather than deleting it, so `password=********` still reads as a password that
  was set. The tests assert the mask is present, not merely that the plaintext is absent.
- **An empty secret would mask everything.** `"".replace()` inserts the replacement between
  every character, so an empty value is skipped explicitly. Found by writing the test, not by
  the failure.
- **The secret's name is the AEAD associated data.** Renaming an entry inside `secrets.json`, or
  swapping the dev and prod passwords by editing the file, breaks decryption. Without that the
  swap would succeed silently and every check would still pass.
- **`open` and `open_existing` are deliberately different.** A *run* uses `open_existing` and
  refuses when there is no key; only `etl secret init`/`set` will create one. A run that quietly
  minted a key would then fail to decrypt everything, and "wrong key" is a far clearer thing to
  be told than "your secret is not there".
- **`SecretStore`'s `Debug` is hand-written.** Everything else in this workspace derives it; here
  a derive would print the key, so it prints `<redacted>` and there is a test that greps its own
  `Debug` output for the key.
- **Clippy caught a second MSRV violation** — `usize::is_multiple_of` is stable in 1.87, the
  workspace declares 1.80. Rewritten as `% 2 != 0` rather than bumping the floor, the same call
  as Phase 3's `is_none_or`.
- **The threat model is written into the crate's own docs**, because it is easy to overstate:
  the key sits beside the secrets, so anyone who can read `.etl/` can read them. What it buys is
  that `secrets.json` alone is useless — the file that gets pasted into an issue, committed by
  accident, or synced to a backup leaks nothing. `.etl/keys/` wants treating like an SSH key.

**Known gap:** the key and decrypted values are not zeroized in memory. `aes-gcm` has a
`zeroize` feature that is not enabled. Worth doing, but it is a smaller hole than the process
holding plaintext in a `String` anyway, and it needs care rather than a feature flag.

## Picking up Phase 6

Phase 6 is the `qa.*` and `ctl.*` namespaces — quality nodes with reject ports, and control
flow. Read it in the plan. What Phases 1–5 already put in place for it:

- **`StageKind::Quality` and `StageKind::Control` have existed since Phase 1**, and every
  namespace already classifies. That was done deliberately then so this phase would not be a
  breaking change.
- **Ports are already per-node and named.** `PortSpec` and the edge's `source_handle` are what a
  reject port needs; `xf.join`'s two named inputs are the working example to copy.
- **`adding_a_component.md` is still the whole procedure**, and a `qa.*` component is a spec, a
  builder and a test like any other — the new part is the second output, not the registration.
- **Watch the count probes.** `count_probe` assumes one relation per stage; a quality node with
  an accepted and a rejected output has two, so `Stage::count_sql` will need to grow before the
  row counts mean anything for `qa.*`.

### 2026-09-15 — Session 7 (Phase 4, part 1: extensions, transforms, JSON)

Built the extension mechanism first, because it is the thing the whole rest of Phase 4 hangs
off, then the two families that need no extension.

- `crates/metadata/src/component.rs` — `ComponentSpec::requires_extension`, `PropertyType::Map`
- `crates/duckdb-engine/src/plan/mod.rs` — `Stage::requires_extensions`, `Plan::extensions()`,
  `Plan::stages_needing()`, `Plan::has_prelude_probe()`, `PRELUDE_PROBE`, the `LOAD` prelude
- `crates/duckdb-engine/src/exec.rs` — prelude-aware count attribution,
  `ExecError::ExtensionLoadFailed`
- `crates/duckdb-engine/src/plan/builders.rs` — 18 new builders, plus shared `column_list`,
  `non_empty_map`, `map_entries`, `non_negative` and `type_name` helpers; `xf.select` moved onto
  the shared column helper rather than keeping its own copy
- `crates/duckdb-engine/src/plan/specs.rs` — 18 new specs
- `crates/duckdb-engine/src/plan/builder_tests.rs` — golden SQL for all 18
- `crates/duckdb-engine/tests/end_to_end.rs` — 4 execution tests: a derive/aggregate/sort chain
  checked against a direct query, a union of two filters, pivot-and-dedup inside views, and a
  CSV to JSON and back round trip
- `docs/adding_a_component.md` — the `map` type and the extension section

**Components: 9 to 27.** Transforms `xf.aggregate`, `xf.cast`, `xf.dedup`, `xf.derive`,
`xf.distinct`, `xf.except`, `xf.intersect`, `xf.limit`, `xf.pivot`, `xf.rename`, `xf.sample`,
`xf.sort`, `xf.union`, `xf.unpivot`, `xf.window`; files `src.file.json`, `snk.file.json`,
`snk.file.jsonl`.

Gate green: fmt clean, clippy clean with `-D warnings`, **164 tests passing** (137 engine unit,
12 end-to-end, 15 metadata). The acceptance run still prints 12/5/7/6/6. Nothing committed.

Stopped here rather than starting the database family, because verifying it needs an extension
install outside the project — see **Open decisions**.

### 2026-09-15 — Session 8 (Phase 4, part 2: extensions vendored, connectors)

Settled the open decision — extensions are vendored into the project rather than installed
system-wide — and then finished the phase.

- `scripts/fetch-duckdb-extensions.ps1` — `SET extension_directory` + `INSTALL`, so the files
  land in `tools/duckdb/extensions/` and nothing outside the project changes. Verifies each
  extension loads, not merely that it downloaded. 9 files, 247 MB.
- `crates/duckdb-engine/src/exec.rs` — `RunOptions::extension_dir`, `locate_extension_dir`,
  and `SET extension_directory=...` prepended ahead of the LOAD prelude
- `crates/duckdb-engine/src/plan/builders.rs` — 13 connector builders, plus `attach_database`
  and `qualified_table` shared by the six database components
- `crates/duckdb-engine/src/plan/specs.rs` — 13 specs, plus `cloud_format`, `database_read`
  and `database_write` written once rather than six times
- `crates/duckdb-engine/src/plan/builder_tests.rs` — golden SQL for all 13, and the prelude
  tests that now have real extensions to assert about
- `crates/duckdb-engine/tests/end_to_end.rs` — an Excel round trip, a SQLite round trip
  covering both write modes, and a run against an empty extension directory that must report
  `ExtensionLoadFailed` rather than blaming the first stage

**Components: 27 to 40. Phase 4 is done.**

Two real bugs found by the execution tests, both of which would have shipped under golden tests
alone: the Excel sink wrote no header (losing a row on read-back), and `append` could not create
a table that did not exist yet (so an appending pipeline failed on its first run and worked
thereafter). Both recorded above.

Gate green: fmt clean, clippy clean with `-D warnings`, **183 tests passing** (153 engine unit,
15 end-to-end, 15 metadata). The acceptance run still prints 12/5/7/6/6. Nothing committed.

### 2026-09-15 — Session 9 (Phase 5, part 1: parameters, contexts, materialisation)

Three of Phase 5's four parts. The acceptance criterion is met: the same document, unedited,
runs in two contexts and lands in two places.

- `crates/duckdb-engine/src/params.rs` — `${...}` interpolation, the `Resolver` and its
  precedence chain, the parameter contract, and `civil_from_days` for `${date}`
- `crates/duckdb-engine/src/context.rs` — `.etl/contexts.json`, loading and applying
- `crates/metadata/src/lib.rs` — `NodeData::materialize`
- `crates/duckdb-engine/src/plan/mod.rs` — `Materialize`, `Stage::spill_path`, `Plan::spills`,
  the `UnknownMaterialize` warning
- `crates/duckdb-engine/src/plan/builders.rs` — `create_view` now emits a view, a temp table or
  a Parquet spill, so the modes live in one place rather than in twenty-five builders
- `crates/duckdb-engine/src/exec.rs` — spill preparation and best-effort cleanup,
  `RunReport::spilled`
- `crates/cli/src/main.rs` — the shared `Settings` group: `--param`, `--context`, `--workspace`,
  `--contexts`; plus an `etl contexts` command
- `samples/contexts.json`, `samples/pipelines/orders_by_context.json` — the acceptance fixture
- 48 new unit tests and 7 new end-to-end tests

**`samples/pipelines/csv_to_parquet.json` runs for the first time.** It has been committed and
unrunnable since Phase 2 because it uses `${workspace}` and `${since}`; there is now a test
whose name says exactly that.

Gate green: fmt clean, clippy clean with `-D warnings`, **252 tests passing** (213 engine unit,
24 end-to-end, 15 metadata). The Phase 4 acceptance run still prints 12/5/7/6/6. Nothing
committed.

Stopped before the secrets crate: it needs a cryptography dependency, which is worth deciding
deliberately rather than slipping into a diff.

### 2026-09-15 — Session 10 (Phase 5, part 2: secrets)

The last quarter of Phase 5, after the dependency question was settled in favour of RustCrypto.

- `crates/secrets/` — `SecretStore`, AES-256-GCM, a per-workspace key under `.etl/keys/`,
  values in `.etl/secrets.json`, hand-rolled hex so no encoding dependency is needed
- `crates/duckdb-engine/src/params.rs` — the `${SECRET:name}` reference form, `Resolved::redact`,
  and `used` recording the mask rather than the value
- `crates/duckdb-engine/src/exec.rs` — `RunOptions::redact`, applied to the reported script and
  to DuckDB's stderr
- `crates/cli/src/main.rs` — `etl secret init | set | list | remove`; the shared settings are
  now `global` so `--workspace` works before or after a subcommand
- 20 secrets tests, 10 parameter tests, 3 end-to-end tests

**Phase 5 is done.** Gate green: fmt clean, clippy clean with `-D warnings`, **284 tests
passing** (222 engine, 27 end-to-end, 20 secrets, 15 metadata). Both acceptance runs still
print what they should. Nothing committed — asked, and the answer was "not yet".

### 2026-09-16 — Phase 8c: the scheduler

Three design questions were settled first, because each forks the work: a schedule lives in its
own file (not the pipeline document), an overrun tick is skipped and counted (not queued), and
concurrency is handled by a workspace lock plus sequential runs (not by adding locking to
`crates/state`).

- `crates/scheduler/` — new crate, no engine dependency and **no new external dependency**:
  - `lib.rs` — `ScheduleFile`, `Schedule`, `Trigger`, and a `RawTrigger` wire form that exists
    so a `tz` field can be *seen* in order to be refused by name
  - `cron.rs` — five fields, UTC, bitmask sets, `next_after` that skips whole months and days
  - `every.rs` — `30s`/`5m`/`1h`/`2h30m`, and what an interval is counted from
  - `watch.rs` — mtime polling, settle-across-two-polls, baseline on first poll
  - `lock.rs` — a held handle on Windows, an exclusive create elsewhere
  - `run.rs` — the loop, behind a `Clock` trait so the tests never sleep
- `crates/state/src/time.rs` — the civil-date conversion promoted out of `lib.rs`, plus
  `days_from_civil`, `weekday` and `from_rfc3339`
- `crates/cli/src/main.rs` — `command_run` split into `perform` and `print_report`, so the
  scheduler runs pipelines through the same path; `etl schedule list|check|start`;
  `--schedules` on the shared settings
- `samples/schedules.json` — the committed example, since `.etl/` is git-ignored

**113 scheduler tests, 17 new state tests.** Gate green: fmt clean, clippy clean with
`-D warnings`, **508 Rust tests** and 114 frontend, typecheck clean.

#### What running it changed

- **The first end-to-end run printed a wrong message about its own behaviour** — *34 tick(s)
  were missed while an earlier run was still going*, when nothing had been running. It was
  counting the hours since the last run. Downtime and overrun are now counted separately, as
  `Entry::behind` and `Tick::missed`, because they send you looking in different places.
- **Two watch tests failed in opposite directions**, which is what established that directory
  mtimes say nothing dependable about nested content. The tests now pin the contract that
  actually holds rather than a behaviour the filesystem does not promise.
- **A schedule that was overdue waited instead of running.** `next_after` skips to the next
  grid slot, which is right *after* a run and wrong *at startup*: down for three hours on an
  hourly schedule should mean run now, not wait fifty minutes. Startup now uses the anchor plus
  one interval, which leaves the tick in the past so it fires at once.
- **The lock survived being killed, on purpose.** Started a scheduler, confirmed a second was
  refused by pid and host, killed the first without unwinding, and confirmed the leftover file
  did not wedge the workspace. That is the Ctrl-C case, which is how a foreground scheduler is
  stopped every single time.

### 2026-09-16 — Phase 8d: the web console

The last slice of Phase 8, and a view over what 8b and 8c produce rather than anything new
underneath. Settled decision 8 held: `tiny_http`, and five crates arrive with it.

- `crates/console/` — new crate, no engine dependency:
  - `auth.rs` — two roles, constant-time comparison, tokens minted per process or taken from
    the environment
  - `routes.rs` — eight routes as pure functions; hardening headers on every response
  - `ui.rs` — the page, one string, no build step, no external loads
  - `server.rs` — `tiny_http` and a four-thread pool; the only part that knows about sockets
  - `workspace.rs` — the `Workspace` trait the CLI implements
- `crates/secrets/src/lib.rs` — `random_token`, because this is where the project keeps its
  cryptography and `OsRng` was already a dependency of the AES decision 4 chose
- `crates/cli/src/main.rs` — `etl serve`, and `ConsoleWorkspace` implementing the trait

**65 console tests, 3 new in secrets.** Gate green: fmt clean, clippy clean with `-D warnings`,
**576 Rust tests** and 114 frontend, typecheck clean.

#### What running it changed

- **The page originally worked out its own role by probing.** It sent a `POST` it expected to
  fail and read the role out of the error message, which works exactly until somebody rewords
  the error. The server now states it in `X-Etl-Role` on every authenticated response,
  including the refusals — which is where knowing your own role is most useful.
- **`with_role` returned before attaching that header on a refusal.** Caught by a test that
  asserted the property rather than the happy path.
- **Clippy found a parameter threaded through a recursion and never used** — `collect_pipelines`
  carried a `root` it did not need.
- **A leftover `etl.exe` from an earlier test held the port**, so a later console failed to bind
  and every request went to the *old* server with the *new* tokens, returning 401. The 401s were
  correct on both sides; `pkill` simply does not exist in Git Bash on Windows. Worth knowing
  before debugging an auth problem that is not one.
- **The security properties were checked against the running console**, not only in tests: a
  token in the URL refused on the API for both a GET and a POST, a viewer's `POST` refused with
  the run not happening, a traversal attempt answered 404, and the hardening headers read off
  the wire.
