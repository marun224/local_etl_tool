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

## Phase 10d — SaaS GraphQL (2026-09-23)

**Concepts**
- **A status code is the transport's opinion, not the application's.** GraphQL puts its
  failures in the body of a 200. A client that trusts the status loads the hole in the data as
  if it were whole. Knowing where a protocol reports failure is the first question for any
  connector.
- **Relay connections.** `first`/`after` in, `nodes` and `pageInfo { hasNextPage endCursor }`
  out. The server holds the position, so a cursor shifts less than an offset when the data
  moves under the read.
- **Refactor, prove, then build.** Moving REST's HTTP layer was proved behaviour-neutral by
  REST's own tests, unedited, before a line of GraphQL existed. A refactor proved by tests
  written after it proves much less.
- **Mutation checks.** Breaking a rule on purpose and watching the tests fail is how you know
  the tests are about the rule.

**Decisions and why**
- **One retry loop, with a judgement hook** (`Client::send_judged`), rather than GraphQL
  wrapping REST's client in a second loop. Two loops would multiply their `retries` budgets
  and could disagree about `Retry-After`.
- **Any error fails**, even with partial data (Settled decision 20); **only all-throttling is
  retried** (21), so a real error is never hidden behind a retry.
- **A light textual check, not a parser** (22). It catches the query that never mentions
  `$after` and cannot reject a valid query; the server stays the authority.
- **The sink always sends a list**, even for one row: the variable is typed as a list, and a
  request's shape should not depend on how many rows happened to be left.
- **A `code` property kind** rather than labelling a GraphQL query as `sql`. Kinds are how the
  canvas and the manifest describe a value; a wrong label misleads both.

**Mistakes worth not repeating**
- **Escapes through two interpreters.** `\n` and a trailing `\` inside a Python heredoc run by
  the Bash tool arrived as real newlines: three broken string literals, which the compiler
  caught, and ten spaces in the middle of an error message, which nothing caught until it was
  read by eye. *Edits containing backslashes go through the editor tool, and a message worth
  keeping is worth one test that pins it whole.*
- **A splice by line numbers was off by one**, leaving a closing brace in the wrong file. The
  compiler caught it, but *print the boundaries before cutting.*
- **`Set-Content -Encoding utf8` wrote a BOM again**, this time into a scratch pipeline, and
  the pipeline parser refuses a BOM. Recorded in the tracker as a real, small gap.

## Phase 10e — checkpoints, and the Kafka source (2026-09-23)

**Concepts**
- **A bounded micro-batch.** Record the end first (each partition's high watermark), read up
  to it, save where you stopped. Freshness is then a scheduling question, and a run is
  repeatable: the same saved position reads the same records.
- **Offsets as state, saved after success.** The same rule as watermarks, for the same
  reason: state that lags output is recoverable (read again), state that runs ahead of output
  loses rows silently.
- **An opaque checkpoint.** The engine stores and returns a connector's position without
  reading it. Only the connector knows what "Kafka partition 1, offset 12" means; the engine
  only has to keep it safe and hand it back.
- **An async client inside a blocking program.** A single-threaded `tokio` runtime built in
  `read` and dropped at its end keeps async out of every other crate.
- **One code path for a rule four callers share**, in the engine rather than in any of the
  callers.

**Decisions and why**
- **Gaps fail the run.** Records deleted before they were read are named and counted, and the
  fix (`etl state forget`) is deliberate, because deciding what to lose is a person's call.
- **The cap is not an error here**, unlike REST's `max_pages`: the saved position is exactly
  where reading stopped, so nothing is lost by stopping.
- **Partitions take turns**, so a backlog in one cannot starve the others under a cap.
- **A test connector is injected, not registered**, so the registry stays what users see.

**Mistakes worth not repeating**
- **Framing a question on an unchecked premise.** Question 12 said artifacts "silently
  re-read"; the build refused them, and the runner's doc comment said so. *Read the code the
  question is about before writing the question.* Caught while building, and told to the user.
- **Library defaults can hang.** `rskafka` retries for ever unless told otherwise. *Read a
  client's retry and timeout defaults before trusting it on a network.*
- **The shell is part of the data.** PowerShell 5.1 added a BOM when piping to `docker exec`,
  and read `a,b` as an array in a command line. *Produce test data from a file, and quote
  anything with a comma.*
- **Mangled line continuations had happened before.** Four messages from earlier phases had
  lost their `\`. *When a mistake is found, search for its siblings.*

## Phase 10f — the Kafka sink, TLS and SASL (2026-09-23)

**Concepts**
- **Partitioning is a contract with other producers.** Consumers rely on "one key, one
  partition, in order". A sink that hashed keys its own way would split a key's history across
  partitions. Matching Java's murmur2 bit for bit is what keeps the contract.
- **SASL and TLS are separate choices**: TLS is who the broker is and privacy on the wire;
  SASL is who we are. `sasl_ssl` is both, and it is what hosted Kafka uses.
- **SCRAM** keeps the password off the wire and off the server (it stores a salted hash);
  PLAIN sends it, so PLAIN without TLS is only for tests.
- **At-least-once, precisely.** A batch spans partitions, so "the batch failed" can mean "some
  of it landed". Saying which is part of the semantics.

**Decisions and why**
- **Diagnose instead of wait.** When connecting times out, one attempt with retries off finds
  the real reason. Changing the library's retry policy was not an option; asking once more
  was.
- **A private CA is a file, public roots are the default**: right for hosted Kafka, and an
  explicit `UnknownIssuer` for a private cluster that forgot `ca_cert`.
- **Refuse settings that would be ignored** (a password without SASL, a CA without TLS): a
  silent no-op is how "it connected, so it must be secure" happens.

**Mistakes worth not repeating**
- **A test that checks a prefix passes a useless message.** The wrong-password test passed
  while the error said only "no answer". *Assert the reason, not just that it failed.*
- **Library deadlines may not mean wall time.** `rskafka`'s counts only the waits between
  attempts. *Measure what a timeout covers before relying on it.*
- **BOMs, again, three times this session.** *Anything read from a pipe or a Windows-written
  file should be checked for one where it matters: passwords, JSON, pipeline files.*
- **New resources are eventually consistent.** A topic just created is not yet listed.
  *Wait for what you made to be visible before using it, in tests at least.*
- **Backslashes through heredocs, once more** (`\015` vanished from a tracker line). The rule
  from 10d holds: the editor tool for anything with a backslash.

## Phase 10g — NATS JetStream, both ways (2026-09-24)

**Concepts**
- **Persistence decides what can be a batch source.** Core NATS forgets a message once it is
  delivered; JetStream keeps it in a stream with sequence numbers. Only something that keeps
  messages can be read "since last time".
- **One sequence number is a simpler position than Kafka's offset per partition**, but a
  subject filter means the next message to read is not always the next sequence: the
  position moves past the stream's end, not the filter's last match.
- **Idempotent publishing.** A message ID plus a server-side duplicate window turns
  at-least-once into "no copies within the window", which is the difference between a safe
  re-run and a messy one.
- **Operator mode** (operator, account, user as signed JWTs; an NKey seed to prove who you
  are) is how multi-tenant NATS is run, and why a `.creds` file is the usual credential.

**Decisions and why**
- **Share before adding.** TLS set-up and value decoding moved out of Kafka before NATS used
  them, each proved by Kafka's untouched tests, so the second broker did not copy the first.
- **Interior deletes are not gaps.** A stream losing its head to limits is data this pipeline
  never saw; a message removed from the middle is ordinary JetStream behaviour.
- **End a read by what the server says is pending, not by a timeout.** Waiting for silence
  would make every run as slow as the timeout.

**Mistakes worth not repeating**
- **A command in a `while read` loop that reads standard input** eats the rest of the loop's
  input. *Give it `< /dev/null`.* It turned a 12-message check into a 1-message one.
- **Stale build artifacts again**: the frontend test read an `etl.exe` built before the new
  components. *Build `etl` before the frontend tests, every time.*
- **Probing in the wrong place**: a throwaway project went into `%TEMP%` instead of the
  scratchpad. Harmless, deleted, recorded. *The scratchpad is for exactly this.*

## Phase 10h — Kinesis: signing, credentials, the source (2026-09-24)

**Concepts**
- **SigV4 is a hash of a canonical request, not of the bytes sent.** Method, normalised
  path, sorted query, lower-cased and trimmed headers, and the body's hash become one text;
  a key derived from the secret, date, region and service signs it. Anything the server sees
  differently from what was signed (a `Host` with `:443`, another `Content-Type`) fails.
- **Every attempt is signed afresh**, because the date is part of the signature and a retry
  may cross a second boundary.
- **Shards split and merge.** A shard's records are ordered; a stream's are not. Reading a
  child only after its parents is what keeps one partition key's records in order.
- **Kinesis keeps records for a retention period**, and its sequence numbers leave gaps, so
  "records were lost" can be suspected but never counted.
- **"Nothing read" has three meanings**: caught up, not started, not reached. Only the first
  may move a position forward.

**Decisions and why**
- **Our own signing rather than the AWS SDK**: the SDK needs a newer Rust than we declare
  and brings its own async stack; about 300 lines plus AWS's own 38 test cases prove
  ours.
- **A published test suite over a hand-written one.** The vendored cases found nothing, which
  is the point: a mutation showed they would have.
- **Fail on possible expiry by default.** A false alarm on a quiet stream costs a command;
  silent loss costs the data.

**Mistakes worth not repeating**
- **A test that passes once is not a passing test.** The data-loss bug hid behind which shard
  went first. *Run new stateful tests several times, and in parallel, before believing them.*
- **A mock keeps some of the real service's limits.** Tests that never cleaned up used up its
  50 shards. *Delete what a test creates, in a guard that runs on failure too.*
- **Seconds are too coarse for "from now".** *Keep time to the unit the server keeps it in.*
- **One exception name, two meanings.** *Read the message before retrying.*

## Phase 10i — the Kinesis sink (2026-09-24)

**Concepts**
- **A batch call can half-succeed.** `PutRecords` answers 200 with some records refused;
  success is per record, and the answer's order matches the request's.
- **Resending changes order.** Sending again only the refused records is cheaper and avoids
  duplicates of the ones that landed, at the price of those records landing later.
- **A partition key is hashed by the service.** The producer chooses the key, not the shard.

**Decisions and why**
- **Refuse before sending what the service would refuse**: one row's size should not fail
  499 others' call, and the message can name the row.
- **A fixture for failure modes, the container for the happy path.** A mock's limits are
  neither the real service's nor deterministic; a fixture's answers are both known.

**Mistakes worth not repeating**
- **Git's line-ending warnings are worth reading.** `LF will be replaced by CRLF` on a
  byte-exact fixture meant the Windows gate would fail. *Mark such fixtures `-text`.*
- **A text anchor for an edit script must be unique in the file**, and three samples share
  one test shape. *Anchor on the function that follows, or check the count first.*

## Phase 10j — receipts, and SQS (2026-09-24)

**Concepts**
- **Two models of "what has been read".** A log (Kafka, Kinesis) lets the reader remember a
  position; a queue remembers for the reader, and must be told when a message is done. The
  first saves state after success; the second has to *act* after success.
- **Visibility timeout.** A received SQS message is hidden, not taken. Delete it, or it comes
  back; change its visibility to 0 to give it back now.
- **A lease.** A hold that ends on its own is renewed while the work goes on, so a slow run
  and a crashed one are told apart by whether renewals stop.
- **RAII for outcomes.** A guard whose `Drop` releases means every early return, panic path
  and preview gives messages back without anyone remembering to.

**Decisions and why**
- **Acknowledge after the sinks delivered, not before**: a failure in delivery then gives the
  messages back. The price is that a failed acknowledgement after delivery duplicates;
  duplication is recoverable, loss is not.
- **A new method with a default over a new field**: every existing source compiles and
  behaves as before.
- **Long polling to decide "empty"**: an instant receive can miss messages that are there.

**Mistakes worth not repeating**
- **A shell heredoc is the wrong way to write Rust with `r#"..."#` and quotes** into a file
  twice in one session. *Write the text to a file first, then append it.*
- **When a whole-workspace build says a crate cannot be found but the crate builds alone,
  suspect the build directory, not the code.**

## Phase 10k — Pub/Sub, and signing in to Google (2026-09-24)

**Concepts**
- **An OAuth 2.0 JWT bearer grant.** A service account proves who it is by signing a short
  JWT (issuer, scope, audience, an hour's validity) with its private key; the token endpoint
  checks it with the public key and hands back an access token. The key never leaves the
  machine.
- **RS256 is deterministic.** RSASSA-PKCS1-v1_5 gives the same signature for the same key and
  input, which is why a published example can check a signer byte for byte. (PSS and ECDSA
  are randomised; they are checked by verifying instead.)
- **DER and PEM.** PEM is base64 of DER between `BEGIN`/`END` lines; PKCS#8 wraps a PKCS#1
  RSA key with an algorithm identifier. Twenty lines of DER writing turned the RFC's JWK into
  both forms for the tests.
- **Two deadlines.** A subscription has its own ack deadline, and a source asks for another;
  a message is held for whichever was set last.

**Decisions and why**
- **Sign-in of our own over Google's crates**: their Rust version and async runtime cost more
  than the ~300 lines of RS256, key files and a token cache, and the RFC proves the part that
  is easy to get wrong.
- **Plain http means an emulator**: the one rule both decides when to sign and guarantees a
  token is never sent in clear.
- **An immediate pull over a waiting one**: a run that ends early is recovered by the next
  one; a request that outlasts its timeout on an empty subscription fails the run.
- **One lease keeper for both queues**: the thread, the stop and the "first trouble" were the
  same; only what "extend" means differs, so that became a closure.

**Mistakes worth not repeating**
- **Expected paths in tests are built with `Path::join`**, never with `/`, on a project that
  runs on Windows.
- **Documentation with Windows paths is written with the editor, not through a
  string-escaping script**: `\a` is a bell in Python.

## Phase 10l — RabbitMQ (2026-09-24)

**Concepts**
- **An AMQP hold is a channel.** Unacknowledged deliveries belong to the channel that
  received them; close it, or lose the connection, and the broker puts them back. So the
  receipt owns a live connection instead of a list of handles.
- **Delivery tags and `multiple`.** Tags count up per channel, and one `ack` or `nack` with
  `multiple` settles everything up to a tag: a whole batch in one frame.
- **Publisher confirms and `mandatory`.** A confirm says the broker took a message, not that
  any queue did; `mandatory` makes the broker hand back a message no queue matched, so
  "published into nothing" becomes an error.
- **An async client in blocking code** needs its runtime alive between calls when the
  protocol has background work (heartbeats): a worker thread, not a current-thread runtime.

**Decisions and why**
- **Probe before planning the code**: the plan named the TLS question as open; ten minutes
  in a scratch project answered it and found the vhost hang, which would otherwise have
  surfaced as a hung test.
- **A deadline on every call** over trusting the client: a hang is the worst way a run can
  fail, since nothing reports it.
- **The management API for the engine's tests** over another AMQP client in the engine crate:
  `ureq` was already there.

**Mistakes worth not repeating**
- **Passing a string with nested double quotes to a native program from PowerShell 5.1.**
  Use single quotes inside, or build the argument some other way.
- **Assuming a header a broker documents is stable across major versions** (`x-delivery-count`).

## Phase 10m — MongoDB (2026-09-24)

**Concepts**
- **Extended JSON** is how JSON carries BSON's extra types (`$date`, `$oid`,
  `$numberDecimal`, `$numberLong`): the same text goes into a filter, a `start` and a
  checkpoint and comes back as the right type.
- **Incremental reads on a document store** are a filter the run adds (`field > saved`) and a
  sort, so the last document read is the highest; saving it only after success is 10e's
  checkpoint, unchanged.
- **Unordered bulk writes** keep going past a refused document, so "how many landed" is the
  batch size minus the refusals, not zero.

**Decisions and why**
- **The server command over the driver's newest API** for upserts: `update` with many
  statements works on every server version; `bulkWrite` only on MongoDB 8.
- **Check that a collection exists**: a silent empty read of a misspelt collection is the
  worst kind of success.
- **Make a test prove the connector, not the clocks**: a wait longer than any skew, and the
  dependency written down for users.

**Mistakes worth not repeating**
- **A mutation test that cannot tell two behaviours apart** proves nothing: put the case
  where they differ (a refusal mid-batch) in the test.
- **Assuming a test that passed once is deterministic**: environmental drift (a VM clock)
  can turn it red an hour later.

## Phase 10o — BigQuery (2026-09-24)

**Concepts**
- **A warehouse read is a job.** The SQL runs on BigQuery's side and is billed by bytes
  scanned; the client asks, polls until done, then pages through results. `max_records` saves
  transfer, not scanning.
- **Query parameters** carry values into SQL with a type and without text substitution:
  the only safe way to put a saved value back into a query.
- **Load jobs versus streaming inserts**: loads are free and batch, streams are billed and
  immediate. For an ETL run the batch is the unit anyway.
- **Exact decimal parsing** of `1.790244000123456E9`: shift the decimal point in the digits,
  do not multiply a double.

**Decisions and why**
- **`start` as a literal the author writes**: the column's type is only known once a job has
  run, and a literal carries its own type.
- **Prove behaviour where the emulator cannot**: paging and polling against the fixture,
  types and loads against the emulator.

**Mistakes worth not repeating**
- **Blaming the code for a dead test server.** Check the container's state (`Exited (137)`)
  before reading test failures that all start at once.
- **Working out expected timestamps by hand** when the server under test had already shown
  the right answer in the probe.

## Phase 10p — Snowflake (2026-09-24)

**Concepts**
- **Key-pair authentication by fingerprint**: the server stores the public key and knows it
  by the SHA-256 of its SubjectPublicKeyInfo; the JWT names that fingerprint in `iss`, so the
  server knows which key to verify with before it verifies.
- **SubjectPublicKeyInfo versus RSAPublicKey**: the same key in two DER wrappings; hash the
  wrong one and every sign-in fails with an unhelpful "JWT token is invalid".
- **Asynchronous statements**: submit, get a handle and a `202`, poll, then read results in
  partitions, which may arrive gzip-compressed.

**Decisions and why**
- **An independent oracle for the one thing that must be exact**: `openssl` produced the
  fingerprint, so the test does not check the code against itself.
- **Bind as text and cast in SQL** where typed bindings have awkward encodings: one path for
  every type, and the value stays readable in the saved position.

**Mistakes worth not repeating**
- None new; the lesson of 10o (the server under test, or an independent tool, is the source
  of expected values) held.

## Phase 10q — MariaDB (2026-09-24)

**Concepts**
- **Wire compatibility is not type compatibility**: MariaDB speaks MySQL's protocol, and its
  own types (`UUID`, `INET6`) still have to be read to be trusted.
- **`CREATE TABLE ... AS SELECT` lets the driver choose the column types**, and a driver's
  default can be narrower than the data (`DATETIME` versus `DATETIME(6)`).

**Decisions and why**
- **Pin a known flaw with a test before deciding to fix it**: the documentation then cannot
  drift from the behaviour, and a fix will announce itself by changing the test.

**Mistakes worth not repeating**
- **Round-trip tests on coarse data hide precision loss**: include a value at the finest
  precision the type allows.

## Question 16 and Phase 10r — ClickHouse (2026-09-24)

**Concepts**
- **Run-time SQL from a compiled plan**: DuckDB's `SET VARIABLE x = (query)` and
  `getvariable('x')` let a statement fixed at compile time carry text computed at run time,
  here an `ALTER` built from the upstream's columns.
- **Streaming results over HTTP** mean the status is sent before the result is complete; an
  error discovered later can only arrive in the body. A reader must look for it.
- **Deduplication is a table property** in ClickHouse, not a request's: a token asks, the
  engine decides.

**Decisions and why**
- **Fix the MySQL sink by widening only tables it creates**: never alter a table someone else
  made.
- **HTTP over a driver** for ClickHouse: fewer dependencies, one Rust version, and streaming
  under our own control.

**Mistakes worth not repeating**
- **`contains` in a test of generated SQL** lets a wrong clause through; compare the whole
  statement.

## Phase 10u — SQL Server (2026-09-24)

**Concepts**
- **TDS carries its own TLS**: the handshake travels inside PRELOGIN packets, and
  `Encrypt=false` still encrypts the sign-in, then drops TLS. A test server has to do the same
  to be believed.
- **A temporary table made inside `sp_executesql` dies when that call ends.** One that must
  outlive a statement is made by a plain batch; the calls after it can still see it.
- **A fixture decoded by a real client checks itself**: every answer the fixture encodes
  wrongly, `tiberius` refuses. It cannot say what SQL Server would do with the SQL, which stays
  unproved until a real server runs it.
- **Saving a position at the precision the display loses**: a `datetime2(7)` shown to the
  microsecond would skip rows that differ in the seventh digit, so the position keeps all
  seven.

**Decisions and why**
- **Refuse rather than trust the machine's store** when the client can only choose between a
  CA file, trusting everything, and the store (decision 42): one of the first two must be
  named, and `encryption: none` refuses a server that insists.
- **Turn a client's panic into an error** (`catch_unwind`) where the client has `todo!()`s on
  real input: a `sql_variant` column should not end the process.
- **Ask for `Required`, not `On`**: `tiberius` panics when it asked for `On` and the server
  cannot, and returns an error for `Required`.

**Mistakes worth not repeating**
- **Shell heredocs through the Bash tool can swallow a script whose Rust holds `\` line
  continuations**: write edits with backslashes to a file and run that.
