# Learnings

What each phase taught: the concepts it used, the decisions it made and why, and the mistakes
worth not repeating. One section per phase, dated.

Phases 0–9 were back-filled on 2026-09-23 from [task_tracker.md](task_tracker.md), which holds
the fuller record. From Phase 10 on, a section is added at the end of each phase.

---

## Phase 0 — Workspace skeleton and document model (2026-09-15)

**Concepts**
- A Cargo **workspace** with one crate per concern, and `rust-toolchain.toml` pinning the
  compiler so every machine builds with the same one.
- A **pipeline document** is ReactFlow's shape (`nodes`, `edges`, `data.properties`) so the
  canvas and the engine read the same JSON without translating it.
- **Forward compatibility with `#[serde(flatten)] extra`**: every struct keeps keys it does not
  recognise, so a newer file survives a load and save by an older version.

**Decisions and why**
- `formatVersion` from day one. Duckle has no versioned format and it is one of their open
  issues; adding a version later means every old file is ambiguous.
- Keep unknown keys rather than drop them. It costs nothing now and is very hard to add later.

**Mistakes worth not repeating**
- Research notes were treated as fact until they were checked against the source. Five claims
  in `ET_Local_Tool.md` turned out wrong (node key, component count, edge shape, and others).
  *Verify a report against the code before designing on it.*

## Phase 1 — DAG validation and topological sort (2026-09-15)

**Concepts**
- **Kahn's algorithm** for topological order, and cycle detection that names the nodes in
  the cycle.
- **Deterministic ordering**: ties break by document order, so the same file always compiles
  to the same plan.
- Errors carry a `node_id()` so a GUI can highlight the box at fault.

**Decisions and why**
- No `petgraph`: about 30 lines of Kahn's gave control over tie-breaking and error messages.
- Five stage kinds (source, transform, sink, quality, control) from the start, even though
  only three were needed. Adding the other two later would have been a breaking change.
- Disabled nodes cascade: everything downstream is dropped with a warning, rather than
  failing later with "table not found".
- Validate *before* dropping disabled nodes, so a broken switched-off node still reports.

**Mistakes worth not repeating**
- None recorded. The lesson was preventive: golden-file tests are worthless if output order
  can vary, so there is a test that compiles the same document 16 times.

## Phase 2 — SQL lowering and the CLI executor (2026-09-15)

**Concepts**
- **Lowering**: each node becomes a `CREATE VIEW`, so a pipeline is a chain of lazy views
  that the sink finally evaluates.
- **Driving an external process**: DuckDB's CLI, fed a script, returning stream-parsed JSON.
- **SQL quoting**: doubling `"` in identifiers and `'` in literals is the whole escaping story.

**Decisions and why**
- Smoke-test the real tool before designing the executor. Four facts came out of it: JSON is
  one array *per statement*, `COPY` returns no count, a failure aborts the rest, and
  backslashes in paths are harmless.
- Row counts on by default, `--no-counts` to skip. Counts cost a re-evaluation, but they are
  also how a failure is attributed to a stage.
- No per-stage timings: in a chain of lazy views they would all read ~0 and mislead.
- Fixed exit codes: 0 ok, 1 usage/IO, 2 invalid pipeline, 3 run failed.

**Mistakes worth not repeating**
- `serde_json::from_str` over the whole stdout would have failed on concatenated arrays.
  *Find out what the tool actually prints before parsing it.*

## Phase 3 — The component registry (2026-09-15)

**Concepts**
- **One table as the source of truth.** Each component's spec and builder are registered
  together as a function pointer. There is no dispatch `match`.
- A **manifest** derived from the registry that the frontend consumes, so the palette has no
  component list of its own.

**Decisions and why**
- Defaults live in the spec, not in the builder, so there is only one copy of each default.
- An unknown property is a warning, not an error: it is usually a typo, but it is also what a
  newer document looks like.
- Rules spanning two properties (join `keys` *or* `condition`) stay in the builder, and a test
  pins that split.

**Mistakes worth not repeating**
- "Adding a component touches three files" was claimed before being tried. Following the doc
  showed it was three files plus the inventory test. *Verify a "done" criterion by doing it.*
- Clippy caught `is_none_or`, which is newer than the declared minimum Rust version (MSRV).
  Keep the declared MSRV honest.

## Phase 4 — Connector breadth (2026-09-15)

**Concepts**
- **DuckDB extensions**, and a `LOAD` prelude derived from what the plan's components need.
- **Golden-SQL tests versus execution tests**: one compares strings, the other runs them.
- The `ATTACH` pattern for database connectors, written once and shared by six components.

**Decisions and why**
- Vendor extensions into `tools/` rather than install them system-wide (Settled decision 3).
  That makes the air-gapped path the only path from then on.
- A `LOAD` failure is loud and early. Autoinstall downloading mid-run is what Phase 9 forbids.
- No XML and no DuckLake: each needs its own decision rather than a quiet copy of the pattern.

**Mistakes worth not repeating**
- Three bugs passed their golden tests and were caught only by running the SQL:
  - PIVOT cannot live in a view unless its values are listed.
  - The Excel sink wrote no header, so reading it back lost a row.
  - `append` failed on the first run because the table did not exist yet.

  *Run the SQL; do not only compare it.*
- A failed prelude and a failed first stage looked identical until a probe was added.

## Phase 5 — Parameters, contexts, secrets, materialisation (2026-09-15)

**Concepts**
- **`${...}` interpolation** with a precedence chain: `--param`, then the context, then the
  default, then a built-in.
- **AES-256-GCM** with the secret's *name* as associated data, so values cannot be swapped
  between entries unnoticed.
- **Materialisation modes** (`view`/`memory`/`disk`) that change the work but not the answer.

**Decisions and why**
- Substitution is single-pass, so a `--param` value cannot smuggle in
  `${ENV:AWS_SECRET_ACCESS_KEY}`.
- A misspelled `--context` is an error, never a fallback to dev.
- Use RustCrypto `aes-gcm` (Settled decision 4). Never hand-roll cryptography.
- Mask secrets in DuckDB's *stderr* too: it quotes the connection string back in full.

**Mistakes worth not repeating**
- An empty secret would have masked every character (`"".replace` matches everywhere). Found
  by writing the test.
- `usize::is_multiple_of` broke the MSRV again. Clippy catches it; keep running clippy.

## Phase 6 — Quality nodes and control flow (2026-09-16)

**Concepts**
- **Reject ports**: a quality node splits rows into `main` and `rejected` rather than
  filtering, and the split is exact by construction (`coalesce(pred, false)` and its negation).
- **Two transports**: one DuckDB script for most plans, and a **persistent session** (stdin held
  open) when a plan needs retries, branches or `continueOnFailure`.

**Decisions and why**
- The execution-model decision is recorded in
  [DECISION_execution_model.md](DECISION_execution_model.md).
- A session round trip costs 0.54 ms against 41.5 ms to spawn a process. That measurement is
  what made per-stage execution affordable.
- `continueOnFailure` returns a report, not an error, but the exit code is still 3.
- `ctl.foreach`, `ctl.run_pipeline` and `ctl.throttle` are deferred, each with its reason in
  the plan.

**Mistakes worth not repeating**
- A predicate that evaluates to NULL would have been lost by both sides of a naive split.
  *Test that accepted + rejected = input for every validator, on real data.*

## Phase 7 — The desktop app (2026-09-16)

**Concepts**
- **Tauri 2** as a thin shell: five IPC commands, and no SQL or component list in the GUI.
- A **generated property panel**: nine controls, one per property type, and no React written
  per component.
- **Connection validation while dragging**: refusing a bad edge while the mouse is down, with
  the reason.

**Decisions and why**
- Preview drops sinks, so looking at a node can never overwrite a file.
- Clearing a field *removes* the property, so "unset means the default applies" stays true.
- Timings are shown only where they mean what they look like. A blank beats a misleading `0 ms`.
- A hand-written SQL highlighter instead of Prism, which needs `dangerouslySetInnerHTML` over
  strings containing user file paths.

**Mistakes worth not repeating**
- `JSON.stringify` reformats arrays. The requirement is "preserve content, not bytes", and a
  second save must produce no diff. *Define what round-trip means before testing it.*

## Phase 8 — Headless runner, scheduler, console (2026-09-16)

**Concepts**
- **Watermarks** for incremental loads: compare strictly `>`, and advance only on full success.
- **Cron parsing** by hand: five fields, UTC, and "either day field matches" semantics.
- **A lock that is a held handle**, not a file that merely exists, so Ctrl-C does not wedge it.
- **Token auth with two roles**, constant-time comparison, and tokens minted per process.

**Decisions and why**
- The runner is subcommands on `etl`, not a second binary (Settled decision 5): two code paths
  that must agree forever eventually will not.
- Timezones are refused rather than approximated (Settled decision 6). A schedule that is an
  hour off twice a year is worse than one that will not start.
- File-watch polls `mtime` (Settled decision 7). Native events are unreliable exactly where
  inboxes live.
- `tiny_http`, not a hand-rolled server (Settled decision 8): untrusted input off a socket is a
  different risk class.
- `?token=` works on the page and never on the API, so a pasted link is not a credential.

**Mistakes worth not repeating**
- The scheduler reported "34 ticks missed" when nothing had run: downtime and overrun had been
  conflated.
- An overdue schedule waited for the next slot instead of running at once.
- A leftover `etl.exe` held the port, and the new tokens got 401 from the old server.
  *Check what is actually listening before debugging auth.*
- Directory mtimes on NTFS say nothing reliable about nested files. Pin the contract the
  filesystem actually keeps.

## Between phases — The CLI gets tests (2026-09-17)

**Concepts**
- **Mutation testing by hand**: break the code on purpose and check that the right tests fail.

**Mistakes worth not repeating**
- A suite passing on its first run is not evidence. Three mutations caused exactly four
  failures, which is.
- `git checkout` to undo a scratch mutation also reverted uncommitted work in the same file.
  *Never use checkout as an undo in a dirty file.*

## Phase 9 — Standalone export, cross-building, CI (2026-09-17, pushed 2026-09-23)

**Concepts**
- **A payload appended to an executable**, found by a trailer at the end of the file.
- **Content-addressed extraction**: unpack once to a keyed directory, and publish with one
  atomic `rename` so no lock is needed.
- **Cross-building by building natively in a container**, not cross-compiling.
- **glibc floors**: the build image decides the oldest Linux an artifact runs on.

**Decisions and why**
- Baking is copy-and-append, not compile, so exporting needs no toolchain on the exporting
  machine.
- `etl build` refuses incremental pipelines and, without `--allow-secrets`, secrets. The second
  was checked by grepping a built file for the password.
- `${workspace}` and `${date}` are resolved by the runner at run time, not at build time.
- Linux CI excludes the Tauri crate. Only the `excel` extension is fetched in CI, since only one
  test needs any.

**Mistakes worth not repeating**
- **Every artifact baked the build machine's paths** (`D:/workspace/...`), and nobody noticed
  until one ran somewhere else. *Test portability by running somewhere else.* The empty
  `D<U+F03A>` directory removed from the repo root on 2026-09-23 was that bug's footprint.
- A cache key built from lengths collided on same-size engines. A key must cover *content*.
- A process-global environment variable in tests made five of them clobber each other. Pass
  the location in as an argument instead.
- The Bash heredoc ate a level of backslashes (`\t` in `targets` became a TAB). Write anything
  containing backslashes with the Write tool.
- A replacement in a patch script silently matched nothing. *Assert every anchored replace.*
- Checking an exit code through a pipe measured `tail`, not the binary.

### Phase 9d, once CI ran (2026-09-23)

**Concepts**
- **Framing both streams.** A child's stdout and stderr are two pipes with no ordering between
  them. A marker on each, read up to its own marker, gives exact attribution; a sleep only
  makes the race less likely.
- **Where a binary is built decides where it runs.** The glibc floor comes from the build
  image, so CI has to build the way the project ships, not the way that is most convenient.

**Decisions and why**
- Fix the race in the protocol, not in the test. The test was right; so was the engine code it
  resembled, until a busier machine lost the race.
- Re-run the failed job *before* the fix, knowing it might pass by luck. It did, and that luck
  bought the artifact jobs their first run, which found the glibc problem a full cycle sooner.

**Mistakes worth not repeating**
- A local rehearsal inherits whatever the machine already has, like an old `etl.exe` in
  `target/`. CI starts clean.
- Fixing one script and not its sibling, called one line later in the same step.
- Trusting `| Out-String` to capture `Write-Host`. It goes to stream 6.
- Passing a race test 200 times on one machine is not evidence about another. Force the
  timing instead.
- A check that cannot fail: the session prelude's "did `SELECT 1` return rows" was always true.
  Writing the test for the new behaviour is what exposed it.

## Phase 10a — The plugin SDK, the staging bridge, and XML (2026-09-23)

**Concepts**
- **A bridge through a file.** Rust code and a separate DuckDB process exchange rows as
  JSON Lines: one JSON object per line, which DuckDB's `read_json` reads and `COPY … (FORMAT
  json)` writes. A source runs *before* DuckDB and its node is a view over the file; a sink is
  a `COPY` into the file, delivered *after* DuckDB.
- **Trait objects in a static registry.** `Connector::Source(&'static dyn Source)` lets the
  engine hold every connector in one list, and the component registry pairs each with the one
  builder for its direction, so adding a connector needs no line in the engine.
- **A streaming XML parser.** `quick-xml` hands out events (start, text, entity, end) rather
  than a tree. The reader is a small state machine over them: outside a record, inside one,
  inside a column.
- **RAII cleanup.** `Staging` deletes its files in `Drop`, so every early return cleans up
  without remembering to.
- **Write-then-rename.** Writing to `<file>.partial` and renaming makes a file replacement
  all-or-nothing.

**Decisions and why**
- JSON Lines, not Parquet, for the bridge (Settled decision 9): no new dependency, and the
  same shape as a `disk` spill.
- Refuse nested XML, repeated elements and mixed content **by name and byte position** rather
  than flatten them by a rule somebody would have to guess.
- The writer is the reader in reverse (`@x` becomes an attribute, `child@x` an attribute of the
  child, a null is left out), so a flat document round-trips byte for byte.
- A failed run delivers nothing, including under `continueOnFailure`, which is the rule
  watermarks already follow.
- Accept DuckDB's type inference for undeclared columns instead of fighting it, because it
  matches every other source and the alternative was a hack.

**Mistakes worth not repeating**
- **A probe that agreed with the plan was taken as proof.** Its two sample dates had different
  shapes, so DuckDB gave up inferring and the "all text" claim looked true. An end-to-end test
  on realistic data showed otherwise. *Probe with the data the feature will actually see.*
- Counting tests from a baseline without checking the baseline: a run that already included
  five new tests made seven more look like five had gone missing. *Before chasing a missing
  number, recount what the old number held.*
- A `vec![...]` of function items stopped coercing to function pointers as soon as it was
  chained with an iterator, so the type had to be written down. Easy to lose ten minutes to.
- A deprecated method (`unescape_value`) passed the test build and failed clippy's
  `-D warnings`. Run clippy, not just the tests, before calling a crate done.

## Phase 10b — SaaS REST (2026-09-23)

**Concepts**
- **Pagination is a state machine** that decides two things after each page: what to ask for
  next, and whether there is a next at all. Page, offset, cursor and `Link` headers differ only
  in those two answers.
- **Which failures to retry.** 429 and 5xx are the server saying "not now"; 400, 401, 403 and
  404 are "not like this", and sending the same request again cannot change them.
  `Retry-After` is the server's own answer to "how long", and wins over any backoff.
- **At-least-once delivery.** A batch that the API processed but did not acknowledge will be
  sent again. The safe targets are APIs that deduplicate or upsert.
- **A recording test fixture.** A local server that records every request, and not only
  answers it, lets a test assert on what was *sent*: method, headers, query, body.

**Decisions and why**
- `max_pages` errors rather than stops, so a capped load can never pass for a complete one.
- A cursor that repeats is refused at once, not 1,000 requests later.
- A sink batch of one is a bare object; any larger size is always an array, even for a last
  batch of one, so the request's shape never depends on the row count.
- The SDK gained `check`, so rules spanning properties fail at `etl validate` and in the
  canvas, not on page one of a run.
- Pinned `ureq` to the 3.2 series rather than let the lock drift past the declared Rust
  version, and raised the wider problem instead of quietly living with it.

**Mistakes worth not repeating**
- **A default that is right in one direction and silently wrong in the other.** The sink
  inherited "GET unless told otherwise" from the source: no body, no error, no data. Only the
  recording fixture showed it. *When code is shared by two directions, check each default
  against each direction.*
- **A test that depends on a build artifact went stale.** The frontend asked an `etl.exe`
  built before REST existed, and failed for a reason that had nothing to do with the code.
  *Rebuild what a test shells out to before believing its failure.*
- **`Set-Content -Encoding utf8` on PowerShell 5.1 writes a BOM.** It happened to a
  `Cargo.toml`. Write files with the editor tools, or with `UTF8Encoding($false)`.
- **Quoting heredocs for a shell inside another tool** broke twice. Long text with quotes goes
  through a file.

## Phase 10c — Phase 4's connectors against real systems (2026-09-23)

**Concepts**
- **Verification is a different thing from testing.** Phase 4's golden tests proved the SQL was
  the SQL we meant. Running it against a real server proved whether that SQL does anything
  useful. Three of five did not.
- **Reference fixtures.** A table written by the format's *own* library (`deltalake`,
  `pyiceberg`) tests the reader against something it did not make itself, which is the only
  test of a reader worth having.
- **Throwaway servers.** Containers started by a script, on high fixed ports, with a readiness
  probe that runs a real query rather than a ping.
- **Tests that skip honestly.** A test needing a server skips without one, and says so.
  `cargo test` still counts it as passed, which has to be remembered when reading a total.

**Decisions and why**
- Declared Rust 1.88, the truth the lockfile already required (Settled decision 17).
- Worked around DuckDB's MySQL bug with a documented setting rather than routing through
  `mysql_query`, which would have sent raw SQL past our own quoting.
- Refused the failing Iceberg combination at compile time with the fix in the message, rather
  than leaving users DuckDB's message about a file nobody named.
- S3 credentials become a *temporary*, bucket-scoped DuckDB secret: nothing on disk, and two
  buckets can use two accounts.

**Mistakes worth not repeating**
- **Help text that nobody had followed.** "The table's metadata location" was the one input
  that failed for a moved table. *Test the instructions, not only the code.*
- **A path check that assumed every path is local.** `prepare_sinks` made a directory of
  `s3://...`. It failed loudly on Windows and silently on Linux, where it made a folder called
  `s3:`. *When a bug can pass silently on one platform, assert its absence.*
- **`cat > file` with nothing piped in waits forever.** It hung a command for ten minutes.
- **Anchors that assume line wrapping.** A splice script's assert caught it before any harm.
