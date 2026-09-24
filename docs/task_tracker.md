# Task Tracker

**State only.** Design lives in [PLAN_duckle_parity.md](PLAN_duckle_parity.md). Read this file
first when picking the project back up.

> ## ✅ Phase 10u (SQL Server) — built 2026-09-24, green locally, against a fixture only
>
> **What 10u built:** `src.db.sqlserver` and `snk.db.sqlserver` over TDS through `tiberius`
> (rustls, no OpenSSL, SQL logins): reads streamed and typed, incremental by a **parameter**
> with the position saved at the column's full precision; batched `INSERT`s of bound text,
> `append`, `truncate`, or `merge` on key columns through a staging table and one `MERGE`.
> TLS required, login-only or none, trusting `ca_cert` or `trust_server_certificate`, never
> this machine's store (decision 42). Azure SQL's redirect followed once. **No SQL Server runs
> here** (decision 87): everything is proved against a local TDS fixture that `tiberius`
> decodes, and "not yet checked against real SQL Server" is recorded (assignment A67).
> **82 components, 1117 Rust tests** (1107 on Linux) with every server up, twice, none
> skipped; **158 frontend** (three for the new sample); nine mutations each caught. **Pushed with CI** (decision 89),
> which covers 10p–10r for the first time.
>
> **Phases 10m–10u are finished** (MongoDB, BigQuery, Snowflake, MariaDB, ClickHouse, SQL
> Server; Redis, Cassandra and Neo4j not built). The next family is not planned yet: the
> plan's *Later families* lists what is left.

> ## ▶ Resumed 2026-09-24 — checks green; 10s and 10t removed from the plan
>
> Every check re-run against the 13 test servers: **1086** Rust tests, none failed or
> ignored; `etl components` **80**; frontend **155**. MinIO moved from port 59000 to **57900**
> (`test-services.ps1`): Windows now reserves 58921–59020 as well. **Cassandra (10s) and
> Neo4j (10t) are removed from the plan** (the user, decision 86). **Next: 10u, SQL Server**,
> against a fixture only (question 15 answered: decision 87), when the user says "start 10u".
> The website drops Cassandra, Neo4j, Redis and Elasticsearch (decision 88; `475ef5d`, pushed:
> 20 of 46 working). Engine docs and the services script pushed with `[skip ci]` (`6663395`).
> **CI runs for 10u** (decision 89), covering 10p–10r for the first time.

> ## ⏸ PAUSED — 2026-09-24, at the user's request, after Phase 10r
>
> **Nothing is half-done.** Both repositories are committed, pushed and clean:
>
> | Repository | Last commit | State |
> |---|---|---|
> | Engine (`E:\workspace_09212026\ETL_Local_Tool`) | `ad7b5ee` Phase 10r, pushed with `[skip ci]` | clean |
> | Website (`E:\workspace_09212026\ETL_Local_WebApp`) | `39ec206` MariaDB and ClickHouse working, 20 of 50 | clean; the live site still needs a redeploy |
>
> No test containers are running (`./scripts/test-services.ps1 -Stop` was run) and no
> background CI watch is left. Docker Desktop was running; start it again before resuming.
>
> **Where Phases 10m–10u stand** (one connector each, decision 71):
>
> | Phase | Connector | State |
> |---|---|---|
> | 10m | MongoDB | done, green in CI (run 35972853098) |
> | 10n | Redis | **not built** (the user, decision 82) |
> | 10o | BigQuery | done, green in CI (run 35976507434); emulator only |
> | 10p | Snowflake | done; CI passed both gates, its artifact jobs were cancelled by the next push; fixture only |
> | 10q | MariaDB | done; its CI run was cancelled at the user's request |
> | 10r | ClickHouse, and question 16's MySQL fix | done; **no CI** at the user's request |
> | 10s | Cassandra | **removed from the plan** (the user, decision 86, after resuming) |
> | 10t | Neo4j | **removed from the plan** (the user, decision 86, after resuming) |
> | 10u | SQL Server | **done** after resuming, against the fixture only (decision 87) |
>
> **CI:** the last fully green run is 10o's (35976507434). 10p, 10q and 10r have passed
> everything locally (1086 Rust tests with every server up, twice) but not in CI. **Ask the
> user before the next push whether CI should run**: they stopped it ("Stop CI runs") and
> asked for none for 10r. A normal push would run CI over 10p–10r together.
>
> **To resume:**
>
> 1. Start Docker Desktop, then `./scripts/test-services.ps1` (13 servers, all 1 GB-capped
>    where the image allows; the BigQuery emulator leaks memory, so `docker restart
>    etl-test-bigquery` between repeated full runs).
> 2. `cargo test --workspace` with the variables it prints: expect **1086** passing, none
>    skipped; `etl components`: **80**; `npm --prefix frontend run test`: **155**.
> 3. Wait for the user's "start 10u" (SQL Server, fixture only). CI runs when it is pushed
>    (decision 89). (Was "start 10s"; 10s and 10t were removed after resuming, decision 86.)
>
> **Open for the user:** nothing (question 15: decision 87; CI for 10u: decision 89);
> the website's redeploy. BigQuery and Snowflake stay off the website until checked against
> the real services (assignment A62 is the Snowflake check).
>
> **Working notes for whoever resumes** (learned this session): Windows reserves ports
> 55621–56220 here, and 58921–59020 since the machine restarted (so MinIO moved to 57900), so
> new services use 57xxx or 58xxx below 58921; `at` is a reserved word in DuckDB's and
> the BigQuery emulator's SQL; PowerShell 5.1 mangles nested double quotes passed to native
> programs; shell heredocs and Python strings through the Bash tool can corrupt backslashes,
> so edits with backslashes go through the editor or a script file.

> ## ✅ Phase 10r (ClickHouse) — built 2026-09-24, green locally; **pushed without CI** at the user's request
>
> **What 10r built:** `src.db.clickhouse` and `snk.db.clickhouse` over the HTTP interface:
> reads streamed a line at a time with names and types, wide integers exact, an error after
> a `200` recognised; incremental by a **query parameter**; inserts of 100,000 rows or 16 MB.
> ClickHouse 25.8 in the test services, 1 GB. **And question 16 is fixed**: a table
> `snk.db.mysql` creates keeps sub-second timestamps (`DATETIME(6)`), on MySQL and MariaDB.
> **80 components, 1086 Rust tests** (1076 on Linux) with every server up, twice, none
> skipped; **155 frontend**; six ClickHouse and two MySQL mutations each caught.
>
> ## ✅ Phase 10q (MariaDB) — built 2026-09-24, green locally
>
> **What 10q did:** proved MariaDB 11.8 through the existing MySQL components: no new
> component. Round trip, MariaDB's own types, a masked wrong password. **Found a data-loss bug
> on MySQL and MariaDB alike**: a table `snk.db.mysql` creates keeps timestamps to the whole
> second (the DuckDB extension creates `DATETIME`); a `DATETIME(6)` table made beforehand keeps
> microseconds. Pinned by tests, documented, **open question 16** asks whether to fix.
> **78 components, 1072 Rust tests** (1062 on Linux) with every server up, twice, none
> skipped; 152 frontend.
>
> ## ✅ Phase 10p (Snowflake) — built 2026-09-24, green locally
>
> **What 10p built:** `src.warehouse.snowflake` and `snk.warehouse.snowflake` over the SQL
> API: key-pair sign-in (a JWT naming the key's fingerprint, **proved equal to `openssl`'s**),
> statements polled and read by partition, rows typed by `rowType`, incremental by a
> **bind variable**, and batched bound `INSERT`s. **78 components, 1067 Rust tests** (1057 on
> Linux) with every server up, twice, none skipped; **152 frontend**; seven mutations each
> caught. **Not checked against real Snowflake**: no emulator exists and no account is used.
>
> ## ✅ Phase 10o (BigQuery) — `8bab325`, green in CI (run 35976507434)
>
> **What 10o built:** `src.warehouse.bigquery` (a table or a query as one query job, polled
> and paged; rows typed by the result's schema, timestamps exact to the microsecond;
> incremental by a column through a checkpoint, the saved value a **typed query parameter**)
> and `snk.warehouse.bigquery` (load jobs of newline-delimited JSON, 4 MB each, `append` or
> `truncate`). Signed in as Pub/Sub is. goccy's emulator 0.8.1 in the test services, capped
> at 1 GB. **76 components, 1055 Rust tests** (1045 on Linux) with every server up, twice
> (the emulator restarted between, as it leaks), none skipped; **149 frontend**; seven
> mutations each caught. Not checked against real Google Cloud.
>
> ## ✅ Phase 10m (MongoDB) — `d1fff00`, green in CI (run 35972853098, with `1dfd2d0`)
>
> **What 10m built:** `src.db.mongodb` (filter, projection and sort in Extended JSON;
> incremental by a field through a checkpoint, kept as its BSON type; a missing collection
> named) and `snk.db.mongodb` (insert, unordered; upsert on `key_fields` through the `update`
> command). MongoDB 8.0 in the test services, plain and TLS on one port, capped at 1 GB.
> **74 components, 1035 Rust tests** (1025 on Linux) with every server up, twice, none
> skipped; **146 frontend**; six mutations each caught. Found on the way: Docker Desktop's
> clock drifting ahead broke a Kinesis `latest` test, now clock-proof and documented.
>
> ## ✅ Phase 10l (RabbitMQ) — `44d1aaa`, green in CI (run 35964870851)
>
> **What 10l built:** `src.queue.rabbitmq` and `snk.queue.rabbitmq` through `lapin`: the
> receipt owns the connection and channel and settles with one `basic.ack`/`basic.nack`
> (`multiple`); a lost connection gives everything back; publisher confirms with
> `mandatory`, so an unroutable row fails naming itself. TLS through the shared `tls.rs`
> (a probe found the way in); every call bounded by `timeout_ms`, since `lapin` never answers
> a connect to a missing vhost. RabbitMQ 4.3 (plain, TLS, management API) in the test
> services. **72 components, 1021 Rust tests** (1011 on Linux) with every server up, twice,
> none skipped; **143 frontend**; fmt and clippy clean; four mutations each caught. **All
> three acknowledgement-based brokers are done.** What it found is under *From Phase 10l*.
>
> **CI for 10h–10k is green.** [Run 35959734855](https://github.com/marun224/local_etl_tool/actions/runs/35959734855)
> (`4b44de9`, 10j and 10k in one commit) passed all six jobs, the Windows gate included, so
> 10i's `.gitattributes` fix holds.
>
> ## ✅ Phase 10k (Pub/Sub) — built 2026-09-24, in `4b44de9`, green in CI
>
> **What 10k built:** `src.queue.pubsub` and `snk.queue.pubsub` on 10j's receipts, and
> Google sign-in of our own (`gcp.rs`): service-account keys (an RS256 JWT through `ring`),
> gcloud's user login, a shared token cache, and no sign-in for a plain-`http://` emulator.
> **RS256 signs RFC 7515's A.2 example byte for byte**; the token exchange, its cache and the
> credential order are tested against the local fixture. Each pull is extended at once from
> the subscription's deadline to `ack_deadline_seconds`, and the lease keeper (now shared,
> `lease.rs`, SQS moved onto it) extends it from there. The Pub/Sub emulator is in the test
> services. **70 components, 1005 Rust tests** (995 on Linux) with every server up, twice,
> none skipped; **140 frontend**; fmt and clippy clean; five mutations each caught. Not
> checked against real Google Cloud. What it found is under *From Phase 10k*.
>
> ## ✅ Phase 10j (receipts, and SQS) — built 2026-09-24, in `4b44de9`, green in CI
>
> **What 10j built:** the design for queues (Settled decisions 57–62), and SQS both ways.
> A source may now hand the engine a **receipt** (`Source::read_held`); the engine
> **acknowledges** it after the run fully succeeded and its sinks delivered, and **releases**
> it on every other path, `preview` and `Drop` included. A failed acknowledgement is a new
> report **warning** (⚠, kept in run history), not a failure. `src.queue.sqs` and
> `snk.queue.sqs`: standard and FIFO, a **lease keeper** extending the hold while the run
> goes on, ElasticMQ in the test services. Kinesis's signed client moved into
> `aws::JsonApi` for both. **68 components, 971 Rust tests** (961 on Linux) with every
> server up, twice, **137 frontend** (three more for the new sample). Not checked against real AWS.
>
> ## ✅ Phase 10i (the Kinesis sink) — `e4fb0a4`, pushed with `[skip ci]`
>
> **What 10i built:** `snk.stream.kinesis`: each row one JSON record, up to 500 to a
> `PutRecords` call and under 5 MiB; `partition_key_column` (unset, the row number spreads
> rows across shards); records Kinesis refuses for throughput sent again on their own, with
> backoff, and any other refusal failing at once, saying how many records were put before
> it. The sample now also puts its large orders into a second stream. 66 components, 942
> Rust tests. Not checked against real AWS, like the source.
>
> ## ✅ Phase 10h (Kinesis: signing, credentials, the source) — `ce2db9c`, pushed with `[skip ci]`
>
> **What 10h built:** `src.stream.kinesis`, through the `ureq` layer (no `tokio`): SigV4
> signing of our own (`aws.rs`), **proved by all 38 cases of AWS's published SigV4 suite**,
> vendored with its licence; credentials from properties, `AWS_*` variables or named
> profiles; shard lineage followed across splits and merges; expiry refused by default with
> `on_expired: continue`; a `kinesis-mock` container in the test services. 65 components,
> 931 Rust tests with every server up. **Not checked
> against real AWS** (Settled decision 56). What it found, including a data-loss bug caught by
> a rerun, is under *From Phase 10h*.
>
> **CI for 10e, 10f and 10g is green.** [Run 35904782091](https://github.com/marun224/local_etl_tool/actions/runs/35904782091)
> (`a659edc`) passed all six jobs at its first attempt: Ubuntu 896 tests with Postgres,
> MySQL, MinIO, Kafka and the five NATS servers up and nothing skipped, Windows 906, 64
> components on both, both artifact jobs, `build-runner.ps1` and frontend. The Windows gate
> took 13 minutes, a cold cache.
>
> **10g, for the record:** NATS JetStream both ways (`src.stream.nats`, `snk.stream.nats`,
> `msg_id_column` for duplicate-free re-runs), every sign-in method, the shared `tls.rs`.
>
> **10f, for the record:** `snk.stream.kafka` (Java-identical partitioning, four codecs,
> `acks=all`), and TLS and SASL for both Kafka directions.
>
> **10e, for the record:** checkpoints (a native source's saved position, saved only after
> a fully successful run, through the shared `etl_duckdb_engine::remember`), artifacts that
> keep state (Settled decision 36; open question 12's premise was wrong, since `etl build`
> already *refused* incremental pipelines), and `src.stream.kafka`.
>
> **State of the tree:** everything through `a659edc` (10f and 10g, one commit, since the
> user had staged both together; 10e is `fc143d4`) is committed, pushed and green in CI.
> **10h is `ce2db9c`**, committed and pushed with `[skip ci]` at the user's request, so CI
> has not run on it. **On its own it would fail the Windows gate**: Windows runners check out
> with `autocrlf`, which turns the SigV4 fixtures' LF into CRLF and all 38 cases differ
> (reproduced locally). The fix, a `.gitattributes` marking them `-text`, is in the 10i
> commit, also pushed with `[skip ci]` at the user's request. **CI has run on neither 10h
> nor 10i**; the first run on or after the 10i commit covers both, and pulls the 1.6 GB
> `kinesis-mock` image in the Ubuntu gate.
>
> **Since then:** 10j and 10k are `4b44de9` (one commit: 10k moved 10j's lease keeper),
> pushed and green in CI on run 35959734855, which also covers 10h and 10i. **10l is
> `44d1aaa`, green in CI on run 35964870851** (all six jobs; the Windows gate took 21
> minutes). Everything through 10l is committed and pushed; the plan for 10m–10u is not yet
> committed.
>
> **Next:** **10u, SQL Server**, against a fixture only (decision 87), when the user says so: one
> connector each (MongoDB, BigQuery, Snowflake, MariaDB and ClickHouse done; SQL Server left),
> planned 2026-09-24. **Redis (10n), Cassandra (10s) and Neo4j (10t) are not built** (the
> user, 2026-09-24; decisions 82 and 86); Elasticsearch is
> not built (memory); Oracle is deferred.
> Question 15 is answered: SQL Server against a fixture only. **The website** (`b035bd9`,
> pushed) shows 18 of 50 connectors working, MongoDB the latest (both ways); its roadmap
> names BigQuery and Snowflake as next. BigQuery and Snowflake stay off the working list
> until read against the real services (decision 78). Kinesis, SQS and Pub/Sub stay off it until
> read against the real services. The live site still needs a redeploy.
>
> **10d, for the record:** `src.saas.graphql` and `snk.saas.graphql`, the shared `http.rs`,
> the `code` property kind. CI warnings seen on its run, neither failing anything: the `@v4`
> actions target Node.js 20 (moving to current majors is due), and `ubuntu-latest` moves to
> Ubuntu 26 from 2026-10-19 (the shipped Linux runner is built in a bookworm container, so its
> glibc floor does not move).
>
> **The website** (`E:\workspace_09212026\ETL_Local_WebApp`) finished its site-to-product
> sync on 2026-09-23 (`ff46c49`, pushed); its own `docs/RESUME_HERE.md` says what is next
> there (a redeploy).
>
> **Commits.** The user commits. Claude commits or pushes only when explicitly asked in the
> moment. This machine has **no global git identity**; when asked, Claude uses
> `-c user.name="Arun M" -c user.email=marun.mahadevu@gmail.com` per command, the identity on
> every commit here, and writes no git config. (User instructions, 2026-09-23.)
>
> **This machine** (the project moved here from `D:\workspace\ETL_Local_Tool`, user `mr`, to
> `E:\workspace_09212026\ETL_Local_Tool`, user `admin`; checked 2026-09-23):
>
> | Thing | State here |
> |---|---|
> | `tools/` (DuckDB 1.5.5, extensions, Linux engine and runner) | present — copied with the folder |
> | Rust toolchain | `rustup` 1.29.1 and the pinned **1.96.0** with rustfmt and clippy, installed 2026-09-23 via winget (approved). `~/.cargo/bin` is not on PATH in terminals opened before the install |
> | MSVC Build Tools (the linker Rust needs on Windows) | **VS Build Tools 2022 17.14**, C++ workload, Windows SDK 10.0.26100, installed 2026-09-23 via winget (approved) |
> | node 24, npm, git, Docker (daemon not running), Python 3.12 (no PyYAML) | present |
> | `gh` | 2.101.0, installed 2026-09-23, logged in as `marun224`. Not on PATH in terminals opened before the install: use `"C:\Program Files\GitHub CLI\gh.exe"` or restart VS Code |
>
> **What CI's `gate.yml` does:**
>
> | Job | Checked locally by |
> |---|---|
> | `gate (windows)` — fmt, clippy, 1117 tests (servers skip), samples | running it, repeatedly |
> | `gate (ubuntu)` — fmt, clippy, **1107 tests** with Postgres, MySQL, MariaDB, ClickHouse, MinIO, Kafka (four listeners), NATS (five servers), kinesis-mock, ElasticMQ, the Pub/Sub emulator, RabbitMQ, MongoDB and the BigQuery emulator, samples | `cargo test` in `rust:1.96-slim-bookworm` |
> | `artifact (both)` — bake, run from elsewhere, run in a bare container | Phase 9b and 9c |
> | `cross-build-script` — `build-runner.ps1`, ELF check, unbaked contract, no-op rerun | each assertion run by hand |
> | `frontend` — 158 tests, typecheck, build | running it |
>
> **The Linux job excludes `apps/desktop`** (Tauri needs WebKitGTK and GTK to compile), so
> **1107 + 10 desktop = 1117** is the arithmetic to check if either number moves. **CI fetches
> only the extensions the tests load** (`DUCKDB_TEST_EXTENSIONS`, hyphen-separated because
> `actions/cache` refuses a comma in a key). **CI cannot do the Windows-to-Linux cross-build**
> (GitHub's Windows runners run no Linux containers), so it proves the output instead: a Linux
> artifact built on Linux and run in a bare container.
>
> **To check everything, from the repo root:**
>
> ```powershell
> ./scripts/test-services.ps1                                       # Postgres, MySQL, MinIO, Kafka, NATS, Kinesis, SQS, Pub/Sub, RabbitMQ, MongoDB, BigQuery in Docker
> cargo test --workspace                                            # expect 1117 passing
> ./scripts/test-services.ps1 -Stop                                 # tidy up afterwards
> npm --prefix frontend run test                                    # expect 158 passing
> npm --prefix frontend run typecheck                               # expect clean
> npm --prefix frontend run build                                   # expect clean
> .\target\debug\etl.exe components                                 # expect 82
> .\target\debug\etl.exe run samples\pipelines\orders_enriched.json # expect 12/5/7/6/6
> .\target\debug\etl.exe run samples\pipelines\orders_checked.json  # expect 12/10+2/9+1/9/2/1
> .\target\debug\etl.exe run samples\pipelines\orders_guarded.json  # expect 12 through, branch taken
> .\target\debug\etl.exe run samples\pipelines\orders_xml.json      # expect 12/7/7, XML written
> gh run list --limit 3                                             # the last run should be green
> ```
>
> Phase 9's own acceptance, still reproducible:
>
> ```powershell
> .\scripts\build-runner.ps1 -Platform linux_amd64
> .\target\debug\etl.exe build samples\pipelines\orders_checked.json --target linux_amd64
> docker run --rm --network none -v "${PWD}:/w" -w /w debian:12-slim ./orders_checked-linux_amd64
> ```
>
> **`tools/` is git-ignored and reproducible**: host DuckDB and 9 extensions, a Linux DuckDB,
> one Linux extension, and a Linux `etl-runner`. `fetch-duckdb.ps1`,
> `fetch-duckdb-extensions.ps1` and `build-runner.ps1` each take a `-Platform`.


## Where things stand

- **Next phase:** none planned. **Phases 10m–10u are finished**; the next family (the plan's
  *Later families*: Redshift, Databricks, DuckDB, file formats, object storage, SaaS) is
  planned when the user chooses it. **10n (Redis), 10s (Cassandra) and 10t (Neo4j) are not
  built** (decisions 82 and 86).
- **In progress:** nothing. **Phases 0–9, 10a–10m, 10o–10r and 10u are done** (10a–10f on
  2026-09-23, 10g–10q on 2026-09-24; CI green through 10o on run 35976507434). **10p
  (`2a474a1`) and 10q (`9073352`) are pushed, but their CI never finished**: 10p's run
  (35983333700) passed both gates before the 10q push cancelled its artifact jobs, and 10q's
  (35984538505) was cancelled at the user's request ("Stop CI runs", 2026-09-24). **10r is
  pushed with `[skip ci]`** (the user: "do not run CI for 10r").
- **Blocked on:** nothing.

Phase 9 was split into 9a–9d on 2026-09-17 before starting, the same way 6 and 8 were:
**9a** the artifact and the payload format, **9b** the engine and extensions inside it,
**9c** cross-building, **9d** the CI matrix that makes Phase 9's "done" true. **All four
are complete**; 9d on 2026-09-23, when CI went green on its third run.

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
cargo test --workspace        # 1117 tests: 324 engine, 321 connectors, 113 scheduler, 65 console, 51 e2e, 51 cli, 48 state, 48 verified, 26 runner, 23 secrets, 17 native e2e, 15 metadata, 10 desktop, 5 plugin-sdk
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

**Eighty components exist.** Sources: `src.cloud.http`, `src.cloud.s3`, `src.db.clickhouse`, `src.db.mongodb`, `src.db.mysql`,
`src.db.postgres`, `src.db.sqlite`, `src.file.csv`, `src.file.excel`, `src.file.json`,
`src.file.jsonl`, `src.file.parquet`, `src.file.xml`, `src.lake.delta`, `src.lake.iceberg`,
`src.queue.pubsub`, `src.queue.rabbitmq`, `src.queue.sqs`, `src.saas.graphql`, `src.saas.rest`, `src.stream.kafka`, `src.stream.kinesis`, `src.stream.nats`, `src.warehouse.bigquery`, `src.warehouse.snowflake`. Transforms:
`xf.aggregate`, `xf.cast`, `xf.dedup`, `xf.derive`, `xf.distinct`, `xf.except`,
`xf.filter`, `xf.intersect`, `xf.join`, `xf.limit`,
`xf.pivot`, `xf.rename`, `xf.sample`, `xf.select`, `xf.sort`, `xf.sql`, `xf.union`,
`xf.unpivot`, `xf.window`. Sinks: `snk.cloud.s3`, `snk.db.clickhouse`, `snk.db.mongodb`, `snk.db.mysql`, `snk.db.postgres`,
`snk.db.sqlite`, `snk.file.csv`, `snk.file.excel`, `snk.file.json`, `snk.file.jsonl`,
`snk.file.parquet`, `snk.file.xml`, `snk.queue.pubsub`, `snk.queue.rabbitmq`, `snk.queue.sqs`, `snk.saas.graphql`, `snk.saas.rest`, `snk.stream.kafka`, `snk.stream.kinesis`, `snk.stream.nats`, `snk.warehouse.bigquery`, `snk.warehouse.snowflake`. Quality: `qa.accepted_values`, `qa.expression`, `qa.not_null`, `qa.range`,
`qa.referential`, `qa.regex`, `qa.unique`. Quality assertions, which fail the run rather than
partitioning rows and so have no reject port: `qa.row_count`, `qa.schema_match`. Control:
`ctl.branch`, `ctl.fail`, `ctl.log`, `ctl.sequence`, `ctl.wait`. Everything else in the six
namespaces compiles to `UnsupportedComponent`, by design.

**Twenty-six of them are written in Rust, not lowered to DuckDB alone** (the list below,
`src.stream.kinesis` and `snk.stream.kinesis` from 10h and 10i, `src.queue.sqs` and
`snk.queue.sqs` from 10j, the first that hold messages until the run's outcome,
`src.queue.pubsub` and `snk.queue.pubsub` from 10k, and `src.queue.rabbitmq` and
`snk.queue.rabbitmq` from 10l, and `src.db.mongodb` and `snk.db.mongodb` from 10m, and `src.warehouse.bigquery` and
`snk.warehouse.bigquery` from 10o, and `src.warehouse.snowflake` and
`snk.warehouse.snowflake` from 10p, and `src.db.clickhouse` and `snk.db.clickhouse` from
10r). `src.file.xml` and
`snk.file.xml` (Phase 10a), `src.saas.rest` and `snk.saas.rest` (Phase 10b),
`src.saas.graphql` and `snk.saas.graphql` (Phase 10d), `src.stream.kafka` (10e),
`snk.stream.kafka` (10f), and `src.stream.nats` and `snk.stream.nats` (10g) are the *native*
components, for data DuckDB cannot reach. Kafka is the first that keeps a
position between runs (a checkpoint) rather than reading everything.
They are registered like any other, so the canvas, validation, lineage, the scheduler, the
console and a built artifact all have them, but their rows cross to and from DuckDB through a
JSON Lines staging file under `.etl/tmp/native/`. A native source reads **before** DuckDB
starts and its node is a view over that file. A native sink is a `COPY` into it, delivered
**after** DuckDB, and only when the whole run succeeded.

```powershell
.\target\debug\etl.exe run samples\pipelines\orders_xml.json   # 12 / 7 / 7, samples/out/orders_2026.xml
# rest_orders.json reads ${api_base}/orders and posts to ${api_base}/large-orders, with the
# token from ${SECRET:api_token}. It needs an API: tests/native.rs runs it against a fixture.
```

What each connector promises is in [connectors.md](connectors.md); adding one is the last
section of [adding_a_component.md](adding_a_component.md). The SDK is `crates/plugin-sdk`,
the connectors are `crates/connectors`, and the engine's half is `crates/duckdb-engine/src/native.rs`.

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
**XML arrived in Phase 10a** as a native component, which is where the plan's amendment sent
it. DuckLake is still undecided.

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
| 9 | Standalone binary export + air-gapped packaging | **done** | 2026-09-23 |
| 9a | — the artifact, and the payload format | **done** | 2026-09-17 |
| 9b | — the engine and its extensions inside the file | **done** | 2026-09-17 |
| 9c | — cross-building (Linux from Windows) | **done** | 2026-09-17 |
| 9d | — the CI matrix | **done** (green on the third run) | 2026-09-23 |
| 10 | Rust-native connectors | **in progress** (10a–10m, 10o–10r and 10u done; the next family not yet chosen) | |
| 10a | — plugin SDK, staging bridge, XML | **done** | 2026-09-23 |
| 10b | — SaaS REST, source and sink | **done** | 2026-09-23 |
| 10c | — verify Phase 4's database and lake connectors | **done** | 2026-09-23 |
| 10d | — SaaS GraphQL, source and sink | **done** (green in CI, run 35888523818) | 2026-09-23 |
| 10e | — checkpoints, and the Kafka source | **done** (pushed with `[skip ci]`; CI not run) | 2026-09-23 |
| 10f | — the Kafka sink, TLS and SASL | **done** (green in CI, run 35904782091) | 2026-09-23 |
| 10g | — NATS JetStream, source and sink | **done** (green in CI, run 35904782091) | 2026-09-24 |
| 10h | — Kinesis: SigV4, AWS credentials, the source | **done** (pushed with `[skip ci]`; not checked against real AWS) | 2026-09-24 |
| 10i | — the Kinesis sink | **done** (pushed with `[skip ci]`; not checked against real AWS) | 2026-09-24 |
| 10j | — receipts (acknowledge after success), and SQS | **done** (green in CI, run 35959734855; not checked against real AWS) | 2026-09-24 |
| 10k | — Pub/Sub | **done** (green in CI, run 35959734855; not checked against real Google Cloud) | 2026-09-24 |
| 10l | — RabbitMQ | **done** (`44d1aaa`; against RabbitMQ 4.3 itself) | 2026-09-24 |
| 10m | — MongoDB | **done** (green in CI, run 35972853098; against MongoDB 8.0 itself) | 2026-09-24 |
| 10n | — Redis | **not built** (the user's choice, decision 82) | 2026-09-24 |
| 10o | — BigQuery | **done** (green in CI, run 35976507434; against the emulator; not checked against real Google Cloud) | 2026-09-24 |
| 10p | — Snowflake | **done** (against the fixture only; not checked against real Snowflake) | 2026-09-24 |
| 10q | — MariaDB (through the MySQL components) | **done** (against MariaDB 11.8 itself; question 16 fixed with 10r) | 2026-09-24 |
| 10r | — ClickHouse | **done** (against ClickHouse 25.8 itself; pushed without CI) | 2026-09-24 |
| 10s | — Cassandra | **removed from the plan** (the user's choice, decision 86) | 2026-09-24 |
| 10t | — Neo4j | **removed from the plan** (the user's choice, decision 86) | 2026-09-24 |
| 10u | — SQL Server | **done** (against the local TDS fixture only; not checked against real SQL Server) | 2026-09-24 |
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

9. **Rust-native connectors reach DuckDB through a JSON Lines staging file.** Agreed
   2026-09-23. A native source writes records before DuckDB starts and compiles to a view over
   `read_json`; a native sink is a `COPY ... (FORMAT json)` that the connector delivers after
   a successful run. No new dependency, the same shape as a `disk` spill, and both transports
   and `preview` stay as they are. Parquet (the `arrow` crates) and linking DuckDB in-process
   were the alternatives. The second would reverse the execution-model decision.
10. **Connector dependencies are pure Rust, blocking where possible.** Agreed 2026-09-23. No
    system C libraries, so the bookworm build of the Linux runner and the air-gapped story are
    unaffected. `tokio` only when a family cannot avoid it, decided per family.
11. **Phase 10 is split 10a / 10b / 10c.** Agreed 2026-09-23. 10a is the SDK and the bridge,
    proven by XML (deferred from Phase 4); 10b is SaaS REST; later families one sub-phase each.
12. **SaaS REST is the first network family.** Agreed 2026-09-23. It is testable against a local
    `tiny_http` fixture with no Docker, and it makes true the website's "REST / GraphQL" entry
    that the 2026-09-23 audit found unbuilt.
13. **Sources and sinks both, from the start.** Agreed 2026-09-23. This was the one answer that
    differed from the recommendation (sources first), so 10a builds an XML writer and 10b a REST
    sink.
14. **Built artifacts run native connectors.** Agreed 2026-09-23. `etl-runner` gets them through
    the engine, so `etl run` and an artifact share one code path; Settled decision 5's reasoning.
15. **Phase 4's database and lake connectors get verified in their own phase, 10c, after 10b.**
    Agreed 2026-09-23. It needs Docker running and test tables. The website's site-to-product
    sync waits on it.
16. **`ring` is rustls's cryptography provider.** Agreed 2026-09-23, at the start of 10b. `ring`
    contains C and assembly that `cargo` compiles itself, with no *system* library, so the
    bookworm build of the Linux runner and the Windows build are unaffected. It meets Settled
    decision 10's reason but not its letter, which is why it was asked rather than assumed.
    The pure-Rust alternative (`rustls-rustcrypto`) is not yet production-grade. Needs a C
    compiler at build time: MSVC on Windows (installed 2026-09-23), gcc in the bookworm image.
17. **The declared Rust version is 1.88.** Agreed 2026-09-23, option (a) of the question raised
    in 10b. The lockfile already needed it: `clap` 4.6, `indexmap` and `zeroize` 1.85, the Tauri
    stack 1.88. The one lint it changed was `usize::is_multiple_of` in `etl-secrets`, which the
    old 1.80 had forced into `% 2 != 0` back in Phase 5, and is now restored. `quick-xml` 0.41 and
    `ureq` `~3.2.1` stay pinned where 10a and 10b put them, because they are tested there. Moving
    them up is a separate, deliberate step, as is `aes-gcm` 0.11.

Decisions 18–24 are Phase 10d's (SaaS GraphQL), all agreed 2026-09-23 as recommended:

18. **The HTTP layer moves to `crates/connectors/src/http.rs`**, shared by REST and GraphQL,
    rather than GraphQL importing it from `rest.rs`. REST's behaviour and tests do not change.
19. **GraphQL pagination is `none`, `relay` and `offset`.** Relay sends `$after` from
    `pageInfo.endCursor` until `hasNextPage` is false; offset sends `$offset`/`$limit` as
    variables. Offset is there for APIs such as Hasura.
20. **Any GraphQL `errors` fails the read**, even with partial `data`. A partial page that looks
    like a complete load is what `max_pages` exists to prevent. No `allow_partial` switch.
21. **Throttling reported inside a 200 is retried**: when an error's `extensions.code` or
    `type` is in `retry_codes` (default `THROTTLED`, `RATE_LIMITED`), with 10b's backoff.
22. **`check` validates the query lightly, with no parser dependency**: not blank, `variables` a
    JSON object, and the pagination variables declared. The server stays the authority on the
    rest. `graphql-parser` was the alternative.
23. **Sources and sinks both** (Settled decision 13 applied): `snk.saas.graphql` sends each
    batch as a list in a `rows_variable`, checks `errors` per batch, and is at-least-once per
    batch like REST.
24. **One hand check against a real, token-free public endpoint**
    (`countries.trevorblades.com`), as 10b did with GitHub. The suite stays on 127.0.0.1.

The tracker's stale pause notes were brought up to date in the same edit (question 8).

Decisions 25–35 are Phase 10e/10f's (Kafka), all agreed 2026-09-23 as recommended:

25. **Kafka is the first streaming broker**, one broker per sub-phase after it. NATS JetStream
    and Kinesis fit the same checkpoint model; Pub/Sub and RabbitMQ acknowledge instead of
    seeking, and need their own design.
26. **The client is `rskafka`**, on a single-threaded `tokio` runtime built and dropped inside
    the connector: the first use of Settled decision 10's "tokio if a family cannot avoid it".
    A hand-written client on `kafka-protocol` and `rdkafka` (C++) were the alternatives.
27. **Read positions live in our state file**, one offset per partition, saved only after a
    fully successful run, like watermarks. Not in Kafka consumer groups, so Kafka's own tools
    will not show the pipeline as a consumer. `etl state forget` replays.
28. **A batch ends at each partition's high watermark recorded at the start, or at
    `max_records`** (default 100,000). Reaching the cap is a normal stop, checkpointed there,
    not an error: nothing is lost, because the checkpoint is exactly where reading stopped.
29. **A first run starts at `earliest` (default) or `latest`.** A timestamp start can come
    later.
30. **`value_format` is `json` (default), `text` or `bytes`**, and every row carries `_topic`,
    `_partition`, `_offset`, `_timestamp` and `_key`. Avro and Protobuf through a schema
    registry are a phase of their own.
31. **All four compression codecs.** gzip and snappy are pure Rust; lz4 and zstd contain C that
    `cargo` compiles, the same terms as `ring` (Settled decision 16).
32. **Authentication: plaintext, TLS, and SASL PLAIN and SCRAM-SHA-256/512**, each tested
    against a container. Every hosted Kafka needs SASL over TLS.
33. **A sink too** (Settled decision 13 again): `snk.stream.kafka`, JSON values, key from
    `key_column`, partitioned the way Java clients partition, at-least-once per batch.
34. **Tested against a real broker**: an `apache/kafka` container in
    `scripts/test-services.ps1` and CI's Ubuntu gate, with the tests skipping without it.
    Recorded protocol fixtures were the alternative.
35. **Named `src.stream.kafka` and `snk.stream.kafka`**, starting a `stream` group.

36. **A built artifact remembers state** (option (a) of open question 12, raised while
    planning 10e). Agreed 2026-09-23. The runner loads and saves `.etl/state/` in its working
    directory (or `--workspace`), in the same format as `etl`, watermarks and checkpoints
    both. Until now an artifact re-read every incremental source from its `start` on every
    run; for Kafka that would have been the whole topic every time. Refusing such pipelines
    in `etl build`, or documenting the gap, were the alternatives. **Corrected while
    building:** the question's premise was wrong. `etl build` already *refused* incremental
    pipelines (the alternative offered as (b)), so the gap was guarded, not silent. Option (a)
    stands, and replaced that refusal with a note.

Decisions 37–46 are Phase 10g's (NATS JetStream), all agreed 2026-09-23 as recommended:

37. **NATS JetStream is the next streaming broker**, before Kinesis: a position that is one
    sequence number, a maintained client within the rules, and a tiny test server. Only
    JetStream, since core NATS keeps nothing to read.
38. **Read from our saved sequence, up to the stream's last sequence at the start, capped by
    `max_records`, through an ephemeral ordered consumer.** Nothing on the server; the same
    model as Kafka (27). A durable consumer acknowledging after success was the alternative.
39. **Rows as Kafka's** (`json`/`text`/`bytes`) plus `_stream`, `_subject`, `_sequence`,
    `_timestamp` and `_headers`.
40. **A sink too**: `snk.stream.nats`, JSON to a subject, waiting for JetStream's
    acknowledgements, at-least-once per batch.
41. **Sign-in: none, user and password, token, `.creds` (JWT and NKey), and TLS with
    `ca_cert`.** `.creds` is how hosted NATS works.
42. **Bundled public roots plus `ca_cert`**, as for Kafka and HTTPS, not the operating system's
    store that `async-nats` defaults to, so an artifact trusts the same everywhere.
43. **`tokio` inside the connector again**, Kafka's pattern (26): `async-nats` has no blocking
    API, and the blocking `nats` crate is deprecated.
44. **Named `src.stream.nats` and `snk.stream.nats`.**
45. **Tested against real NATS servers in containers**, one per sign-in method, locally and in
    CI's Ubuntu gate; the tests skip without them.
46. **What follows NATS is decided when NATS is done**: Kinesis, a design for the
    acknowledgement-based brokers, or NoSQL.

Decisions 47–56 are Phases 10h and 10i's (Amazon Kinesis), all agreed 2026-09-24 as
recommended:

47. **Our own SigV4 signing over `ureq`**, with `ring`'s HMAC-SHA256, proved by AWS's
    published SigV4 test suite. No `tokio`, no new dependency, and the declared Rust stays
    1.88. `aws-sigv4` and `aws-sdk-kinesis` declare Rust 1.94.1, and the SDK brings `tokio`.
48. **Credentials from properties, then `AWS_*` variables, then named profiles**
    (`~/.aws/credentials`, `~/.aws/config`). Instance roles (EC2, EKS IRSA, ECS) are a
    follow-up, not 10h.
49. **Kinesis is the third streaming broker**, after Kafka and NATS, and the last that fits
    the saved-position model (decision 46's choice).
50. **A batch reads each shard until it is caught up or `max_records`**, shards taking turns:
    "up to now", not a snapshot at the start, and documented as such.
51. **Shard lineage is followed**: a child is read only after its parents are finished, so a
    partition key keeps its order across a split or a merge.
52. **Possible expiry fails by default, with `on_expired: continue` as the deliberate way
    out.** The count cannot be known; the message says so and names the false-alarm case.
53. **A sink too**: `snk.stream.kinesis`, `PutRecords` in batches of up to 500, partial
    failures retried entry by entry, at-least-once per batch.
54. **Rows as the other brokers'** plus `_stream`, `_shard`, `_sequence` (text: 128-bit),
    `_timestamp` and `_partition_key`.
55. **Tested against `kinesis-mock`** in a container, locally and in CI's Ubuntu gate, with
    AWS's SigV4 vectors as unit tests.
56. **No check against real AWS** (question 9, answer (b)): no account is used, so Kinesis
    stays "not yet checked against real AWS" until someone runs one.

Decisions 57–70 are Phases 10j–10l's (SQS, Pub/Sub, RabbitMQ), all agreed 2026-09-24 as
recommended:

57. **SQS first, then Pub/Sub, then RabbitMQ**, one sub-phase each (10j, 10k, 10l) with
    source and sink; the shared design is built with SQS (the SDK and engine change first,
    proved with a test connector) and planned against all three.
58. **A receipt, settled once.** A source may return a `Receipt` beside its summary
    (`Source::read_held`); the engine acknowledges it after the run fully succeeded and the
    sinks delivered, and releases it on every other path, `Drop` included. Saving tokens in
    the state file, or acknowledging at read time, were the alternatives.
59. **A failed acknowledgement after the sinks delivered is a warning**, not a failure: the
    run succeeds, positions are saved, and a new `warnings` list says how many messages will
    come again.
60. **A lease keeper** extends SQS's visibility and Pub/Sub's ack deadline every half-period
    until the receipt is settled; RabbitMQ's hold is the open channel.
61. **A batch ends at `max_records` (default 10,000), when the queue answers empty, or at
    `max_wait_ms`.**
62. **Rows as the other brokers' plus each broker's underscore columns**, receive and
    redelivery counts included.
63. **SQS: standard and FIFO; `queue_url` or `queue`; AWS credentials and signing as
    Kinesis's; tested against ElasticMQ.**
64. **Pub/Sub sign-in is ours**: service-account keys (RS256 JWT with `ring`), gcloud's user
    login, none for the emulator; proved by RFC 7515's RS256 example. The metadata server
    later. `gcp_auth` and `google-cloud-pubsub` (Rust 1.90) were the alternatives.
65. **RabbitMQ: AMQP 0-9-1 classic and quorum queues through `lapin`**, the channel held
    open until settled. Streams later.
66. **RabbitMQ sign-in: user and password, TLS with bundled roots plus `ca_cert`.** Client
    certificates later.
67. **Sinks for all three**, at-least-once per batch: SQS batches of 10 with FIFO group and
    deduplication columns; Pub/Sub batches with ordering keys and attributes; RabbitMQ
    publish with confirms.
68. **Test services in CI too**: ElasticMQ, the Pub/Sub emulator and RabbitMQ, each added in
    its own sub-phase; tests skip without them.
69. **A new `queue` group**: `src.queue.sqs`, `src.queue.pubsub`, `src.queue.rabbitmq` and
    their sinks. 66 components become 72.
70. **No real AWS or Google Cloud** (question 14): SQS and Pub/Sub are recorded as not yet
    checked against the real services, as Kinesis is.

Decisions 71–81 are Phases 10m–10u's (databases and warehouses), agreed 2026-09-24: all as
recommended, except Elasticsearch, which is not built.

71. **One connector per sub-phase** (10m MongoDB, 10n Redis (dropped: decision 82), 10o BigQuery, 10p Snowflake, 10q
    MariaDB, 10r ClickHouse, 10s Cassandra and 10t Neo4j (both dropped: decision 86), 10u SQL Server), each committed and
    pushed when green, the website updated after each, each started only when the user says so.
72. **"Others on the site's list"** are the rest of its Databases group: MariaDB, ClickHouse,
    Cassandra, SQL Server and Neo4j. Redshift, Databricks and DuckDB wait for a later family.
73. **MongoDB**: a collection with filter, projection and sort, batched; incremental by a
    field through a checkpoint; insert, or upsert on key fields. Change streams later.
74. **Redis** (superseded by decision 82: not built): a Stream through a consumer group, held with 10j's receipts (`XACK` after
    success), and a key snapshot by pattern (`SCAN`); sinks to a stream (`XADD`) and to hashes
    by a key template.
75. **BigQuery**: our own REST client with 10k's Google sign-in; query jobs paged for reads,
    load jobs for writes; tested against `goccy/bigquery-emulator`. DuckDB's community
    extension was the alternative.
76. **Elasticsearch is not built** (the user, answering question 5): its test server needs
    more memory than a test container is given. OpenSearch goes with it.
77. **Snowflake**: the SQL API with key-pair sign-in (RS256, 10k's), results by partition,
    batched inserts with bind variables; tested against the local fixture only.
78. **No real cloud accounts** (question 8): BigQuery and Snowflake are recorded as not yet
    checked against the real services and stay off the website's `working` list until they
    are, as Kinesis, SQS and Pub/Sub do.
79. **Test containers are capped at 1 GB of memory** (question 10), in CI's Ubuntu gate like
    the others.
80. **Oracle is deferred**: its client needs Oracle's native library on every machine, which
    breaks the single binary.
81. **Incremental native reads use 10e's checkpoints** (`incremental_field` or
    `incremental_column` with `start`), not the DuckDB sources' `incremental` block, which
    does not reach native sources.
82. **Redis (10n) is not built** (the user, 2026-09-24, after 10m). The phase letters stay as
    they were, so 10o BigQuery follows 10m; every later phase has four components fewer than
    planned (86 after 10u). Its design is kept in the plan.
83. **Sub-second timestamps in tables `snk.db.mysql` creates are kept** (question 16,
    answered as recommended with 10r): created empty, widened to `DATETIME(6)` by an `ALTER`
    written at run time, then filled; a table already there is left alone.
84. **MariaDB is marked working on the website** (question 17, as recommended), with
    ClickHouse, both tested against the real servers; CI ran for neither (the user stopped
    it, then asked for none for 10r), which `CLAIMS.md` says.
85. **No CI for 10r** (the user): pushed with `[skip ci]`.
86. **Cassandra (10s) and Neo4j (10t) are not built** (the user, 2026-09-24, after 10r:
    "remove from the plan itself"). Unlike Redis, their designs are deleted from the plan, not
    kept. The letters are not reused, so 10u SQL Server follows 10r; 82 components after 10u.
87. **SQL Server is tested against a local fixture only** (question 15, the user: (c)): its
    image needs 2 GB, over decision 79's cap, so no test container and no CI service. The
    fixture speaks enough TDS for the tests; "not yet checked against real SQL Server" is
    recorded and it stays off the website's `working` list, as BigQuery and Snowflake do.
88. **The website drops Cassandra, Neo4j, Redis and Elasticsearch** (the user, as
    recommended): out of its connector list (20 of 46 working, 26 planned), and its roadmap
    names only SQL Server as next. Website `475ef5d`, pushed; the live site still needs a
    redeploy.
89. **CI runs for 10u** (the user, as recommended): a normal push, which also covers 10p–10r,
    unchecked in CI since 10o.

## Open decisions

None open. (15, SQL Server's test server, was answered 2026-09-24: a fixture only, decision 87.)

Resolved 2026-09-23, all as recommended: (1) the session's stderr race is fixed by
framing stderr with an `error()` marker, not by softening the test; (2) the failed Ubuntu job
was re-run before the fix, which got the `artifact` jobs their first run; (3) DuckDB's
`Infinity` in `-json` output is a known gap, recorded under *From Phase 9d, once CI actually
ran*. Earlier ones were resolved 2026-09-16 (Settled decisions 5–8).

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

- Repo: `E:\workspace_09212026\ETL_Local_Tool`, branch `main`, pushed to
  `github.com/marun224/local_etl_tool` (private). **Moved here 2026-09-23** from
  `d:\workspace\ETL_Local_Tool` on another machine; paths in older entries below say `D:` and
  are left as they were. Renamed from `master` on 2026-09-16 while the remote was still empty.
- Duckle reference checkout: `D:\workspace\duckle-main` (read-only reference; clean-room rules
  apply — architecture and behaviour, never source).
- Toolchain verified 2026-09-15: **cargo/rustc 1.96.0**, **node v24.18.0**, **npm 11.16.0**.
  `rust-toolchain.toml` pins 1.96.0 with rustfmt and clippy.
  **On this machine (2026-09-23):** rustup 1.29.1 with the pinned 1.96.0, VS Build Tools 2022
  (17.14, C++ workload), node 24.19, npm 11.17, git, Docker (daemon off), Python 3.12 (no
  PyYAML), and `gh` 2.101 logged in as `marun224`. All but node, git, Docker and Python were
  installed that day.
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

### From Phase 9a

- **A built artifact is not self-contained yet, and says so on every build.** It carries the
  pipeline and nothing else, so it needs a DuckDB where it runs. 9b is what changes that. The
  worse half is that the engine's not-found error tells you to run `scripts/fetch-duckdb.ps1`
  — sensible in a checkout, nonsense on the server the artifact was shipped to. Whichever
  phase makes the engine travel inside the file should fix that message at the same time.
- **Baking is a copy and an append, not a compile.** The alternative — generate Rust, compile
  it — would make exporting need a toolchain on the machine doing the exporting, which is fine
  on this laptop and wrong for a Build Pipeline button in a shipped app. It also makes 9c
  *choosing a different runner to copy* rather than cross-compiling on demand.
- **Appending invalidates a code signature**, on both Windows and Linux. Nothing here is signed
  yet, so nothing is broken yet; it is a 9d packaging constraint and is written down now
  because it is the kind of thing found late and expensively.
- **`etl build` refuses an incremental pipeline.** The runner has no state store, so a baked
  incremental pipeline would re-read everything on every run and say nothing about it. Refusing
  is not the permanent answer — giving the runner `etl-state` is — but a silent wrong answer is
  the one outcome worth ruling out first.
- **`etl build` refuses to bake a secret without `--allow-secrets`.** The baked document is the
  *resolved* one, so a `${SECRET:...}` becomes plaintext inside the file and anyone holding the
  file holds the credential. Confirmed by grepping a built artifact for the password, rather
  than assumed: the warning is true, not decorative. `--info` on such an artifact says so, and
  `Payload::carries_secrets` is recorded at build time so it can be said on a machine that has
  no way to check.
- **The run report's formatting moved into the engine** (`etl_duckdb_engine::report`), because
  the runner needed the same lines. It returns lines rather than printing them, so the rules —
  which timings are honest, why a zero rejected count is shown and a `None` is not — are
  testable without a terminal. `etl run` and a built artifact now print byte-identical output
  for the same pipeline, which was checked rather than assumed.
- **The payload format has room for 9b already.** `Payload::files` and the blob region are
  written, read and tested; 9a simply leaves them empty. That was deliberate — a second format
  later is a migration, and an empty list now is free.

### From Phase 9b

- **An artifact is now self-contained, and about 38 MB.** That is the DuckDB CLI, and it is the
  floor: a pipeline needing `excel` is about 60 MB, one needing `delta` would be about 94 MB.
  `--no-embed` produces the small 9a-shaped artifact for anyone who would rather install DuckDB
  on the target. Only the extensions a plan actually asked for are embedded, which is what
  `Plan::extensions` has been for since Phase 4.
- **Unpacking is cached, and the cache key needs the blob digest to be correct.** The key is
  FNV-1a over the *header*, which is a few hundred bytes, because it is computed on every run.
  The header alone distinguishes embedded files by **length**, not content — so two builds
  whose engines happened to be the same size would have shared a directory, and the second
  artifact would have silently run the first one's engine. `Payload::blob_digest` is computed
  once at build time to close that, and `write_built` sets it rather than the caller, because a
  digest that can be forgotten will be. A test pins the collision case.
- **`rename` will not land on a non-empty directory**, on Windows or POSIX. An extraction
  interrupted between creating the directory and writing its marker therefore wedged every
  later run until somebody cleared the temp directory by hand. An incomplete directory — one
  with no `.complete` marker — is now removed before the rename. Nothing reads a directory
  without a marker, which is what makes that safe.
- **Extraction publishes with a single `rename`, and needs no lock.** Work happens in a
  `.partial-<pid>` directory and is renamed into place at the end. Two processes racing produce
  one winner; the loser sees the destination is complete, discards its own copy and uses the
  winner's. Compare the scheduler's lock file, which needs one because it is coordinating
  *runs* rather than bytes.
- **An artifact's header is data somebody else wrote.** An embedded file's name is refused
  unless it is a plain filename — no separators, no `..`, no drive letters — and every name is
  checked before any byte is written, so a bad one leaves nothing on disk. Refused rather than
  sanitised.
- **`INSTALL` is refused in `compile`**, so `validate`, `plan`, `run`, `build`, the console and
  the scheduler all reject the same document. The scanner skips string literals, quoted
  identifiers, dollar-quoted blocks and both comment forms, because a check that fires on
  `SELECT 'preinstall'` is one people learn to route around. It is not a SQL parser and does
  not need to be: the failure it can still have is refusing SQL that was harmless, never
  passing SQL that was not.
- **Building searches several roots for the vendored toolchain.** `--workspace` says where the
  pipeline's *data* is, and the engine lives near the checkout; a pipeline reading a folder
  somewhere else could not be built at all until the lookup tried the workspace, then the
  current directory, then the directory holding `etl`.
- **The platform is recorded even when no extension needs it.** It costs nothing and it is the
  field 9c will use to say which of two platforms an artifact was built for.
- **Still true from 9a:** appending invalidates a code signature, and the engine's
  "run scripts/fetch-duckdb.ps1" message is still wrong advice on a server — though an
  embedded artifact no longer reaches it.

### From Phase 9c

- **Cross-building is done by building natively inside a Linux container**, not by
  cross-compiling on Windows. Inside the container the target *is* the host, so it is an
  ordinary `cargo build` — no cross toolchain, no linker configuration, nothing installed
  globally. `cross` and `cargo-zigbuild` were both considered and declined for the option that
  adds nothing to the machine, which is the same call Settled decision 3 made for DuckDB.
- **Only `linux_amd64` can be built this way, and the script says so.** macOS needs Apple's SDK,
  which is not redistributable and has no licensed image; a second Windows platform would need
  a Windows container. Both need a different approach, and `build-runner.ps1` refuses rather
  than failing obscurely.
- **A cross-target's extensions cannot be verified here, and that is a real gap.** The host's
  are installed *through* DuckDB and then load-tested, which has caught every genuine mistake so
  far. A Linux extension has to be downloaded from `extensions.duckdb.org` directly, because a
  Windows DuckDB will only install Windows binaries — so it is trusted on the strength of its
  URL. The artifact running on Linux is the only thing that can confirm it, which is exactly why
  9c's acceptance is *running* a cross-built artifact rather than building one.
- **A downloaded extension has no `.info` sidecar.** `INSTALL` writes one; a direct download
  does not, and the build simply embeds fewer files. Whether DuckDB needs it to `LOAD` from an
  extension directory is **not known** and is the most likely thing to go wrong when the Linux
  artifact is first run. If it does, the fix is to synthesise the sidecar at download time.
- **A cross-target's engine is vendored under `tools/duckdb/targets/<platform>/`**, not beside
  the host's. The executor finds its engine by searching upward for `tools/duckdb/`, and a Linux
  binary sitting where it looks would be found and then fail to run. Verified by fetching the
  Linux engine and re-running the whole suite.
- **Having two platforms vendored at once is what 9b could not do.** Its extension lookup took
  "the only platform directory there" and errored on a second — a stub left deliberately, with a
  comment saying 9c was where choosing belonged. It now addresses by target.
- **The extension lookup was pinned to the wrong root for one commit.** It searched only the
  workspace, not the roots the engine lookup walks, so building a pipeline whose data lived
  outside the checkout failed with "no vendored extension directory". Caught by the same scratch
  workspace that caught the engine version of this in 9b, which is an argument for keeping that
  check in the loop rather than only building samples in place.

### From Phase 9c, once it actually ran

- **`${workspace}` and `${date}` are no longer resolved at build time**, and this was a real bug
  rather than a cross-build inconvenience. `etl build` bakes a *resolved* document, and the
  sample pipelines write `${workspace}/samples/data/orders.csv` — so every artifact built before
  this carried `D:/workspace/ETL_Local_Tool/...` inside it. It went unnoticed through 9a and 9b
  because every artifact happened to run on the machine that built it; a Linux container has no
  `D:` drive and said so immediately. **A host artifact copied to another machine would have
  failed the same way.**
  The fix is `Resolver::defer_built_ins`: parameters, contexts and secrets are resolved at build
  time, because the far side has nothing to resolve them with, and the two built-ins are left in
  the document for the runner to answer against its own workspace and its own clock. `${date}`
  is the worse of the two to have got wrong — a scheduled artifact writing to `out/2026-09-17/`
  forever, because that is the day somebody built it.
- **The base image sets the oldest Linux an artifact can run on.** `rust:1.96-slim` is trixie
  (glibc 2.39) and produced a runner that would not start on Debian 12. Pinned to
  `rust:1.96-slim-bookworm` (glibc 2.36), which is where DuckDB's own published Linux CLI runs —
  so the artifact's floor is DuckDB's floor rather than one we added on top. Moving that image
  forward silently raises the floor for everybody.
- **A container build tree is only reusable by the image that made it.** Cargo caches compiled
  *build scripts* and runs them next time, so switching images left a bookworm build dying with
  "GLIBC_2.39 not found" while compiling `proc-macro2` — a confusing way to say "wrong
  leftovers". The target directory is now keyed by image, and each keeps its cache.
- **Windows cannot set the Unix executable bit**, so a cross-built artifact is written `rw-r--r--`.
  Docker Desktop's bind mount presents it as executable, which is why the acceptance run works;
  copying one to a Linux box over `scp` would need a `chmod +x` first. `make_executable` is
  `#[cfg(unix)]` and a no-op on the building machine. Worth solving before anyone ships one.
- **The `.info` sidecar question is still open.** `orders_checked` needs no extension, so the
  acceptance run never loaded one. A cross-target's extensions are downloaded without the
  `.info` file `INSTALL` would have written, and whether DuckDB needs it is still unverified —
  it now just needs an extension-using pipeline cross-built and run, which is no longer blocked
  on anything.

### From Phase 9d

- **`cargo test --workspace` does not build on Linux**, and this was news. `apps/desktop` is
  Tauri and needs WebKitGTK, GTK, glib and `pkg-config` as *system* packages; the build dies in
  `glib-sys` looking for `pkg-config`. Linux CI therefore runs
  `--workspace --exclude etl-desktop`. Anyone running the suite on Linux by hand needs the same
  flag or the same apt install.
- **668 of the 678 tests pass on Linux**, verified in `rust:1.96-slim-bookworm` before writing a
  workflow that claims it. The missing 10 are the desktop crate's. Nothing else needed changing
  — no path-separator or CRLF failures, which the plan's "Windows-first" risk had warned about.
- **Only one test needs an extension.** Parking `tools/duckdb/extensions` and re-running leaves
  exactly one failure, `an_excel_round_trip_loads_the_extension_and_moves_the_rows`, naming
  `excel`. CI fetches that one and skips ~250 MB per run. If a second test ever needs another,
  the failure will say which.
- **`fetch-duckdb.ps1` detected the host with `$env:PROCESSOR_ARCHITECTURE`**, which is
  Windows-only — on a Linux runner it silently concluded `windows_amd64` and would have
  downloaded the wrong engine. Now uses PowerShell Core's `$IsWindows`/`$IsLinux`/`$IsMacOS`,
  with the 5.1 case (where those are absent) meaning Windows.
- **GitHub's Windows runners cannot run Linux containers**, so 9c's Windows-to-Linux Docker path
  has no runner. CI proves the output instead: a Linux artifact built on Linux and run in a bare
  container. The container hop stays hand-verified, and the workflow says so at the top rather
  than leaving somebody to wonder why it is missing.
- **No toolchain action and no `rust-cache` action.** `rust-toolchain.toml` already pins 1.96.0
  with rustfmt and clippy and rustup honours it, so a toolchain action would be a second place
  to keep in step. Caching uses first-party `actions/cache`, which is the same dependency
  posture the rest of the project takes.
- **Windows will not execute a file with no extension**, so the baked artifact is named
  `checked.exe` there and `checked` on Linux. Obvious in hindsight, and exactly the kind of
  thing that fails on the first CI run rather than locally.

### From Phase 9d, once CI actually ran (2026-09-23)

- **Every failure was in the workflow or a script. None was in the Rust.** On Windows, fmt,
  clippy and all the tests passed on a cold runner with an empty cache, on the first try.
- **A local rehearsal inherits whatever the machine already has.** `target/debug/etl.exe` existed
  on every machine the workflow was rehearsed on, so no rehearsal could notice that nothing in
  the job builds it. `cargo test` writes the CLI only as `deps/etl-<hash>.exe`. The artifact job
  was right to build explicitly; the gate job simply never needed to until it ran from a clean
  checkout.
- **A fix to one script is a question about its siblings.** 9d fixed host detection in
  `fetch-duckdb.ps1` and left `fetch-duckdb-extensions.ps1` hard-coding `duckdb.exe`. The
  two are called together, one line apart, in the same step.
- **`Write-Host` is not output.** In PowerShell 5 and later it writes to the information stream
  (6), so `$x = script | Out-String` is empty while the console shows the text. That makes
  the failure look impossible in the log: the expected line is printed right above the throw
  that says it was not seen. `6>&1` is the fix, and it was reproduced on Windows before it was
  written.
- **The first CI run had cold caches**, so it also measured the worst case: 8m48s for the
  Windows gate. The cache keys will be warm next time.
- **GitHub is retiring Node 20 for actions**, and `actions/checkout@v4`, `actions/cache@v4` and
  `actions/setup-node@v4` are being forced onto Node 24. Annotations only for now; worth
  moving to the next majors when they exist, rather than on the day they stop working.
  `ubuntu-latest` becomes Ubuntu 26 from 2026-10-19; the Linux artifact's floor is set by the
  bookworm build image, not by the runner, so that should not move it.
- **The second run found a race that three machines had passed.** stdout and stderr are
  separate pipes, and "whatever stderr holds by now" attributed a late message to the next
  statement. Fixed by framing stderr with its own marker, raised with `error()` (see
  `session.rs`'s module docs). A timing assumption that holds on a quiet machine needs a test
  that forces the bad timing, which is what the channel-based test does.
- **A test for the fix found an older bug next to it.** The session prelude checked only that
  `SELECT 1` returned rows, and a failed `LOAD` does not stop it. So a session-transport
  pipeline with a missing extension failed later, at the first stage that used it, instead of
  at the prelude. The one-script path never had this: its prelude probe is separate.
- **CI built the Linux runner on the host and so tested an artifact nobody ships.** The
  project's Linux runner comes from `build-runner.ps1` in the bookworm image (glibc 2.36). The
  host-built one needed 2.39 and would not start on Debian 12. The artifact job now bakes with
  the shipping runner. Measured from the binaries: the bookworm runner needs glibc ≤ 2.34, and
  DuckDB's Linux CLI ≤ 2.25.
- **Known gap, deliberately deferred (2026-09-23):** DuckDB prints non-finite doubles as bare
  `Infinity`/`NaN` in `-json` output (`SELECT 1/0` gives `{"boom":Infinity}`), and that is not
  JSON. `parse_values` would report `BadOutput` for any row containing one, on the session path
  and in previews. No sample produces one. The fix is to rewrite those tokens outside string
  literals before parsing, or to cast doubles in probes.

### From Phase 10a

- **"Unset `columns` means all text" was wrong, and a test said so.** DuckDB's JSON reader types
  an ISO date or timestamp found inside a string, as `src.file.csv` and `src.file.json` do. The
  first scratch probe missed it because its two dates had different shapes. `read_json` has no
  `all_varchar`, and an impossible `dateformat` is refused, so this is now the documented
  behaviour rather than something to fight. The plan's 10a section carries a dated amendment.
- **quick-xml 0.41, not 0.42.** 0.42 needs Rust 1.86 and the workspace declares 1.80; Cargo's
  MSRV-aware resolver picked 0.41, which is Settled decision 4's situation again. One
  dependency came with it (`memchr`). 0.41 reports each entity as its own `GeneralRef` event,
  splitting the text around it, so text is gathered across events. Its `trim_text` option would
  have eaten the spaces in `fish &amp; chips`, and is not used.
- **A connector's failure is a `StageFailed`, not a new error type.** It names the node, and
  secrets are masked by the same `redact` DuckDB's errors go through. A new
  `ExecError` variant would have been one more thing for the CLI, the console and the scheduler
  to match on, for no difference anyone sees.
- **Staging cleanup is a guard, not a call.** `Staging` deletes its files on drop, so the session
  path's early returns, which already skip spill cleanup, cannot leak staging files. The test
  suite checks the staging directory is empty after every native run, failures included.
- **The run's clock now starts before native staging**, on both transports. `RunReport::elapsed`
  is documented as the whole run, and reading an XML file is part of it.
- **Deliver-only-after-success was mutation-checked.** Making the session path deliver
  regardless fails exactly one test, `a_run_that_fails_delivers_nothing_even_where_its_copy_succeeded`,
  with "delivered despite a failed run". Reverted by edit.
- **DuckDB writes `DECIMAL` as a JSON number**, so `72.40` reaches an XML sink as `72.4`.
  Documented in connectors.md; cast to `VARCHAR` before the sink to keep a fixed form.
- **The frontend went from 114 tests to 117 with no frontend change.** It runs three tests per
  committed sample, one of which rebuilds the sample's wiring against the engine's real
  manifest. So the canvas has already been shown to accept the XML sample.
- **A built artifact grew 0.4 MB** (39.1 to 39.5 MB) for both connectors. CI's `artifact` job
  now bakes the XML pipeline too, and runs it from elsewhere and in the bare container.

### From Phase 10b

- **The first draft of the sink sent GET requests**, with no body, and would have reported
  success. `Settings::from` fell back to GET when no method was given. In a pipeline the spec's
  default (POST) is always filled in first, so only a direct call could hit it. The fixture
  showed it straight away, because it records every request rather than only answering them.
  The default method now comes from the direction, and a sink refuses GET outright.
- **A connector's own rules now run at compile time.** The SDK's new `check` hook lets
  `etl validate` and the canvas refuse "cursor pagination without `cursor_path`" instead of
  the first page of a run. The engine maps a `ConnectorError::Property` to the ordinary
  `EngineError::InvalidProperty`, so nothing downstream learned a new error.
- **The declared Rust version is fiction.** Adding `ureq` showed that `cargo add` picks a
  version for the declared 1.80 but resolver 2 locks the newest, so `^3.2.1` locked 3.4.2 (needs
  1.85). Looking further: the lock has needed up to 1.88 for a while. Pinned `~3.2.1` to stop
  10b adding to it; the rest is an open decision.
- **An API may quote the credential back in its error**, and that text goes into ours. The
  existing `redact` masks it, and a test sends a 401 whose body contains the token to prove it.
- **Lineage strips credentials from URLs**: `user:pass@`, the query string and the fragment. A
  query string is where an API key most often travels.
- **The frontend's wiring test asks the built `etl.exe` for the manifest.** A stale binary
  (built before REST) made it report "That component is not in the registry" for the new
  sample. Rebuilding fixed it. CI's frontend job has no binary, so there that check is skipped;
  it only really runs where someone has built `etl` first.
- **`ring` compiled from source with no trouble** on MSVC here; CI's bookworm image has gcc.
  The connectors crate's tree is ureq, rustls, ring, webpki-roots, flate2 and small helpers,
  with no system library.
- **Real HTTPS, once, by hand**: GitHub's public releases API over TLS, following real
  `Link: rel="next"` headers until `max_pages` stopped it with an error, then one page into a
  CSV with `published_at` typed as `TIMESTAMP`. The suite stays on 127.0.0.1.
- **PowerShell 5.1's `Set-Content -Encoding utf8` wrote a BOM into `Cargo.toml`** while pinning
  ureq. Caught by checking the bytes, and stripped. Cargo tolerated it; git diffs would not
  have.

### From Phase 10c

**The five connectors Phase 4 had only compared as SQL, run against real systems:**

| Connector | Against | Result |
|---|---|---|
| `src.lake.delta` | a table written by `deltalake` 1.6.5, two commits | **works**: 12 rows, typed, both commits |
| `src.lake.iceberg` | a table written by `pyiceberg` 0.12 and then moved | **was broken for moved tables**; fixed |
| `src.db.postgres`, `snk.db.postgres` | PostgreSQL 16 in Docker | **works**: overwrite, append, append onto a missing table |
| `src.db.mysql`, `snk.db.mysql` | MySQL 8.4 in Docker | **reading was broken**; fixed. Writing worked |
| `src.cloud.s3`, `snk.cloud.s3` | MinIO in Docker | **writing never worked on Windows**, and could not reach MinIO at all; fixed |

- **`snk.cloud.s3` treated `s3://bucket/key` as a local directory to create** before the run.
  On Windows that is an invalid path, so the S3 sink had **never once worked there**. On Linux
  it quietly created a local folder called `s3:` and carried on. `prepare_sinks` now skips any
  path with `://`, and the round-trip test asserts no `s3:` folder appears, which is what
  catches it on Linux, where the run itself would pass.
- **S3 could not be pointed at anything but AWS's defaults.** No key, no region, no endpoint,
  so public buckets only. Added `key_id`/`secret`, `session_token`, `region`, `endpoint`,
  `url_style` and `use_ssl`. They become a DuckDB `CREATE OR REPLACE SECRET`, temporary (nothing
  written to `~/.duckdb`), named for the node and **scoped to its bucket**, so two nodes can
  reach two buckets as two accounts. An `http://` or `https://` endpoint sets `use_ssl`. Nothing
  set means no secret, exactly as before.
- **DuckDB 1.5.5's MySQL extension cannot aggregate over a view.** `count(*)`, `sum`, and GROUP
  BY over a view of a MySQL table fail with `INTERNAL Error: Failed to bind column reference`;
  the same straight on the table works. Every source here is a view, so even the row count
  tripped it. `src.db.mysql` now sets `mysql_aggregate_pushdown_enabled=false` first, which
  fixes every case tried (count, sum, GROUP BY, filter then count, ORDER BY with LIMIT). The
  aggregate runs in DuckDB instead: slower on a huge table, correct on all of them. Revisit
  when the pinned DuckDB moves.
- **`src.lake.iceberg` told people to point at the metadata file**, and with
  `allow_moved_paths` that fails: DuckDB takes the path as the table's root and appends
  `metadata/...`, giving `….metadata.json/metadata/snap-….avro`. The working form is the
  table's root plus `version` (the metadata file's name without `.metadata.json`). Added
  `version`, which also reads an **earlier snapshot**: version 00001 of the fixture returns the
  first commit's seven rows. The failing combination is refused at compile time with the fix
  in the message. Checked with the original table hidden, so the moved copy really is what
  is read.
- **MinIO no longer publishes to Docker Hub.** `minio/minio` answers "repository does not
  exist". `quay.io/minio/minio` and `quay.io/minio/mc` still serve. Any S3-compatible server
  would do for these tests.
- **Windows PowerShell 5.1 made the service script die on docker's own stderr** (pull
  progress, "not found") under `$ErrorActionPreference = 'Stop'`, even with `2>$null`. The
  script now decides by exit code, which is what should decide it anyway.
- **Overwriting a Parquet file on Windows can fail with "Could not move file: Access is
  denied"**, intermittently, while something still holds the old file: a virus scanner, or the
  query that read it a moment before. Seen once in a test, never reproduced in three further
  runs. The tests now write each read-back to its own file. A user overwriting outputs on
  Windows could meet the same thing; worth watching, not yet worth code.
- **`cargo test` reports a skipped test as `ok`.** Without the servers, `tests/verified.rs` says
  "9 passed" when four ran. Counting what passed means knowing which tests had their servers.
- **CI's engine cache key now names the extension set.** The fetch step is skipped on a cache
  hit, so a key naming only the DuckDB version would restore last week's excel-only engine,
  and the new tests would skip rather than fail.
- **How the lake fixtures were made** is in `crates/duckdb-engine/tests/fixtures/lake/README.md`,
  with the script. pyiceberg on Windows needs a plain warehouse path, not a `file://` URI, which
  it turns into `/C:/...`.

### From Phase 10d

- **Moving the HTTP layer changed no behaviour, and that was proved before GraphQL existed.**
  REST's 28 fixture tests passed against the moved code with `rest/tests.rs` untouched. Only
  after that was the test server moved into the shared `crates/connectors/src/fixture.rs`,
  which took 140 lines out of REST's tests and nothing else.
- **A 200 that is really a retry needed one change to the client, not a second loop.**
  `Client::send_judged` lets a connector judge a 2xx as accept, retry or fail, and a retry goes
  through the existing backoff, `Retry-After` and `retries` budget. REST passes "accept".
- **Both rules that matter were mutation-checked.** Accepting partial `data` beside `errors`
  fails one test; turning off throttling retries fails four.
- **The real server behaves as designed:** `countries.trevorblades.com` answers an unknown
  field with HTTP 200 and `errors` (`GRAPHQL_VALIDATION_FAILED`), which the run reports as a
  stage failure, exit 3. Some servers (Apollo Server 4) send validation errors as HTTP 400
  instead; that goes through the shared layer as a non-retried 4xx, quoting the body, so the
  message is still there, just less tidy. Not worth code until someone meets it.
- **A pipeline file with a UTF-8 BOM is refused** ("expected value at line 1 column 1"). Windows
  PowerShell 5.1's `Set-Content -Encoding utf8` writes one, which is how it was found while
  editing a scratch pipeline for the hand check. Existing behaviour, not 10d's; a one-line fix
  in `PipelineDoc::from_json` if it bites someone. Recorded rather than fixed, because it is
  outside 10d's scope.
- **The `code` property kind renders like `sql`** (a monospace textarea) and validates as
  text. REST's `body` moved to it too. The frontend switch passes `spec.type` to `fromText`
  rather than a literal, so the next multi-line kind needs one `case`, not two edits.
- **A process lesson:** text containing `\n` or a trailing `\` must not go through a Python
  heredoc in the Bash tool here. The escapes arrived as real newlines, which broke three string
  literals and silently put ten spaces into the shared `max_pages` message. No test pinned
  that message exactly, so it would have shipped; GraphQL's `max_pages` test now pins the
  whole sentence. Edits with backslashes go through the editor tool.

### From Phase 10e

- **Kafka 4.x works with `rskafka` 0.6.** Kafka 4.0 removed old protocol versions, and a
  client that only spoke them would have failed against any current cluster. Tested against
  Apache Kafka 4.1.0 (KRaft, one node) before anything was built on it.
- **`rskafka`'s retries never give up by default** (`deadline: None`, backing off to 500 s),
  so a mistyped broker address would hang a run for ever. The connector sets a deadline of
  `timeout_ms` and wraps every call in a timeout too; a test proves a closed port fails in
  about 1.5 s.
- **Its `chrono` has no clock and no formatting** (default features off), so the record
  timestamp is formatted with the scheduler's own civil-date arithmetic, `etl_state::time`.
- **The checkpoint had to be shared, not copied.** `etl run`, the scheduler, the console and
  the runner each load and save state, so the rules moved into one engine module,
  `remember`, which all of them call. The engine now depends on `etl-state`, a small crate
  with nothing bundled.
- **Two guards, each mutation-checked on its own:** the engine empties a failed report's
  checkpoints, and `remember` refuses a failed report anyway. Turning off either fails a
  test.
- **The artifact premise was wrong** (see Settled decision 36): `etl build` already refused
  incremental pipelines. Recorded rather than quietly rewritten.
- **A micro-batch into an `overwrite` file sink keeps only the latest batch**, and an empty
  batch empties the file. Seen running the sample by hand. Documented in `connectors.md`; a
  per-run file-name built-in would be the fix, and it is not in scope.
- **Four more mangled messages, from before this phase.** Four warnings in
  `crates/cli/src/main.rs` had lost a `\` line continuation, leaving a run of spaces
  mid-sentence (one came in with Phase 8a, `8430a16`). Found by searching for the pattern
  10d's own mistake left behind, and fixed. There may be more in other file types; only Rust
  sources were searched.
- **PowerShell 5.1 added a UTF-8 BOM** to text piped into `docker exec`, so the first test
  record was not JSON. The connector refused it correctly; its message now names the BOM.
- **PowerShell also turned `-e KEY=a,b` into an array** in the services script and passed
  `System.Object[]` to Docker. Comma-bearing values are quoted.
- **`cargo check --workspace --all-targets` failed with "can't find crate"** for two crates,
  while `cargo test` and `cargo build` of the same code were fine. Zero-length `.rmeta` files
  from interrupted builds sit in `target/debug/deps`. Not chased: the gate uses `cargo test`,
  and a clean `target` would settle it.
- **Kafka's console producer puts keyless records all on one partition** (the sticky
  partitioner), which is why the hand-run sample shows offsets on partition 1 only.

### From Phase 10f

- **Murmur2 matches Java, checked two ways.** The six values Kafka's own `UtilsTest` pins, and
  a live comparison: 30 keys written through `etl` and through Kafka's Java console producer
  into two six-partition topics landed on identical partitions.
- **A failed sign-in looked like a timeout.** `rskafka` retries a failed SASL exchange like a
  network blip, and its deadline counts only the waits *between* attempts. In a debug build a
  SCRAM attempt (TLS plus 4,096 rounds of hashing) is slow, so retries overran any timeout and
  the reason was lost: "no answer within 30000 ms". Now a connect that times out makes one
  more attempt with retries off, and reports its reason (`SaslAuthenticationFailed`,
  `UnknownIssuer`). The tests that "passed" before checked only the message's prefix; they now
  require the reason.
- **Every broker call gets 5 s of slack past `timeout_ms`** for the same reason: so a
  library's own failure arrives before our timeout does.
- **`etl secret set --stdin` stored a BOM.** Windows PowerShell 5.1 prepends one when piping to
  a program, so a correct password piped in failed to sign in. `--stdin` now drops a leading
  BOM as well as the trailing newline. The third BOM this session (10e's test records, the
  scratch pipeline); worth a look at every place text enters from a pipe or a file.
- **A new topic is not listed at once.** Kafka creates topics asynchronously, so a connector
  that lists topics first (both of ours do) can call a just-made topic missing. Seen only under
  parallel tests; the test helpers now wait until a topic is listed. A user creating a topic
  and running a pipeline in the same second would meet the same message; not worth code.
- **The test broker's startup script has its own rules** for listeners literally named `SSL`
  or `SASL_...` (keystore file names, `KAFKA_OPTS` checks). The listeners are named `TLS`,
  `SASL` and `SASLTLS` to step around them and set Kafka's own properties instead.
- **Kafka refuses to change one user's SCRAM credentials twice in one request**, so the
  script adds SHA-256 and SHA-512 in two calls.
- **PowerShell 5.1 mangles double quotes in arguments to native programs**, so the shell
  command for the certificate script avoids them (`tr -d '\015'`).
- **A partial batch can land partly.** A batch goes to each partition in turn, so when one
  partition refuses, earlier partitions of the same batch may have been written. The error
  says so, and `connectors.md` does too.

### From Phase 10g

- **Operator mode came up first time**, the step the plan named as riskiest: `nsc` in a
  throwaway `nats-box` container makes an operator, a system account, an account with
  JetStream (`nsc edit account --js-enable 1`) and a user, and the server preloads them from a
  memory resolver. Tried in throwaway containers before it went into the project.
- **Refactors proved by the untouched suite.** TLS moved into `tls.rs` and value decoding into
  `kafka::value_columns`/`key_text`; Kafka's 38 tests passed with their file unchanged.
- **`PublishAckFuture` is `IntoFuture`, not `Future`**, so waiting on it with a timeout needs
  `.into_future()`. **A NATS read ends by count, not by time**: the consumer reports how many
  messages are pending when it is made, and each message says how many are left, so a batch
  never waits for messages that are not coming.
- **A filtered read moves past the end of the stream**, not only past its last match, so
  messages that did not match are not looked at again next run.
- **The frontend's sample test reads the manifest from the built `etl.exe`**, so a new
  component's sample fails it until `etl` is rebuilt: the 10b lesson, met again. The gate
  order that avoids it is `cargo build -p etl-cli` before the frontend tests.
- **A hand check published 1 of 12 messages**: `nats pub` inside a `while read` loop swallowed
  the rest of the file from standard input. The scratch script, not the product; fixed with
  `< /dev/null` and the check redone (12, 0, 1).
- **The harness's safety check refused a command** whose shell snippet had `'\015'` inside a
  PowerShell line that also used `Remove-Item`; it read the snippet as a path. Shell snippets
  now go in files.
- **The probe for `async-nats` went into `%TEMP%`** rather than the session scratchpad, while
  planning; deleted, and recorded here and in the command log.

### From Phase 10h

- **A data-loss bug, caught only by running a test twice.** A draft saved "read from now on"
  for every shard a run read nothing from, meant for a `latest` first run. But a shard that
  `max_records` stopped the run before reaching also read nothing, got "now", and **its
  existing records were skipped** the next run. The batches test passed once and failed on
  the rerun, depending on which shard went first. Fixed by recording how each shard was
  opened, and pinned by a deterministic test (`ExplicitHashKey` puts records on known
  shards) that fails when the old behaviour is put back.
- **All 38 SigV4 cases passed at the first attempt**, so the suite was mutation-checked:
  dropping the space-collapsing breaks `get-header-value-trim`, the one case that uses it.
- **`kinesis-mock` and AWS differ on an unknown sequence number**: AWS answers
  `InvalidArgumentException`, `kinesis-mock` `ResourceNotFoundException` naming the sequence.
  Both mean "not held", and both are read that way.
- **`LimitExceededException` means two things**: a call rate ("Rate exceeded"), which passes,
  and an account's shard limit, which does not. Only the first is retried now; a draft
  retried both six times.
- **The test server keeps AWS's 50-shard account limit**, so tests that never deleted their
  streams used it up after a few runs. Streams are now deleted when each test ends, and the
  test server's limit is raised.
- **A `latest` start saved to the second read records from earlier in that second.** It is
  kept to the millisecond now.
- **Byte-for-byte fixtures need `-text` in `.gitattributes`.** Found after the push: a
  Windows checkout with `autocrlf` adds `\r` to every line, and the SigV4 suite then fails all
  38 cases. Reproduced by converting the files to CRLF; fixed with `.gitattributes`, and a
  scratch checkout with `autocrlf=true` shows them left as LF.
- **Kinesis's LocalStack is `kinesis-mock` inside**, which is why the lighter of the two was
  used. Neither checks SigV4 signatures, so the vendored AWS suite is the signing proof, and
  real AWS remains unchecked (Settled decision 56).

### From Phase 10i

- **Kinesis's `PutRecords` succeeds while refusing records.** HTTP 200, `FailedRecordCount`
  above 0, and an `ErrorCode` beside each refused record. A sink that checked only the
  status would lose them silently. Only those records are sent again, which means **a
  resent record lands after later ones of its call**, even with the same key: documented,
  not hidden.
- **Partial failures were tested against the local fixture, not `kinesis-mock`.** The plan
  suggested provoking them from the container's limits; the fixture refuses exactly the
  records a test names, every time. Three mutations (no 5 MiB split, every code resendable,
  resending the whole call) each break the test meant to catch them.
- **"Keys land on the shard their hash says" became "each key on one shard".** Which shard
  an MD5 falls in is Kinesis's work, not ours, and the tree has no MD5; what the sink controls
  is that the key sent is the column's value, which the round trip checks, and that
  unkeyed rows spread over both shards.
- **Size limits are checked before sending**: a record over 1 MiB, or a key outside 1 to 256
  characters, fails naming the row, rather than failing a whole call of 500.

### From Phase 10j

- **The receipt comes back beside the summary, not inside it.** `Summary` derives `Clone`
  and `Eq`, which a boxed receipt cannot; a new trait method with a default kept every
  existing source unchanged.
- **The engine's whole-run paths look connectors up in the real registry**, so a test-only
  connector cannot reach them. The guard (collect, acknowledge, release, `Drop`, warnings,
  masking) is tested with a test connector; the transports, `preview` and
  `continueOnFailure` with SQS against ElasticMQ. Five mutations (each settle point, the
  guard's `Drop`, the lease keeper, the SQS receipt's `Drop`) each break a test.
- **Preview releases by dropping.** The staged rows are already in the file the preview
  reads, so the messages go back before DuckDB even starts.
- **SQS's "The specified queue does not exist." names no queue**; the connector now does.
- **A corrupted build artifact** made a workspace-wide `cargo check` say it could not find
  `etl_metadata` while the crate built alone; `cargo clean -p` for two crates fixed it.
- **Kinesis's signed client became `aws::JsonApi`** for SQS to share, proved by Kinesis's
  unchanged tests.

### From Phase 10r

- **ClickHouse sends an error after the first rows with status 200**, as a last row holding
  the exception: found in the probe before any code, and now a test.
- **Wide integers had to be asked for quoted**: unquoted, `Int128` arrived as a bare number
  that no `f64` holds; quoted, they are made numbers by type where they fit.
- **The deduplication token does nothing on a plain MergeTree** (the probe inserted a batch
  twice under one token); documented rather than promised.
- **A mutation (an incremental first run from `start` sorted the wrong way) was not caught
  at first**: the unit test used `contains`, and no server test used `start`. Both fixed.
- **The MySQL fix (question 16) works through `SET VARIABLE` and `getvariable()`**: DuckDB
  folds the variable at bind time, so `CALL mysql_execute(...)` receives SQL computed from
  the upstream's `DESCRIBE`.

### From Phase 10q

- **A table `snk.db.mysql` creates drops sub-second timestamps**, on MySQL 8.4 as on MariaDB
  11.8: the extension creates `DATETIME`. Phase 10c's round trip checked counts and totals of
  minute-precision orders, so it could not see it. Pinned by a test on both servers;
  open question 16.
- **MariaDB's own types read cleanly** through the MySQL extension, and nothing else in the
  MySQL path needed a change.
- **`at` is reserved in DuckDB's SQL** as in the BigQuery emulator's; twice in one day. The
  tests say `stamp`.

### From Phase 10p

- **Every Snowflake test passed at its first run**, and the fingerprint matched `openssl`'s
  at the first attempt: the SPKI around `ring`'s public key was written from the DER rules,
  and `openssl` was the independent check.
- **No server, so the proof is narrower**: the fixture checks what is sent and how answers
  are read, not that Snowflake accepts it. The first real run is owed before the website can
  list Snowflake.

### From Phase 10o

- **The emulator was killed for memory** (exit 137) during the mutation checks: it keeps
  what dropped datasets used, about 450 MB per full round of its tests, and a dozen rounds
  passed the 1 GB cap. The failures that followed were the emulator's, not the connector's;
  every mutation was also caught by a test that needs no server. One round fits, which is
  what CI runs; locally the container is restarted between rounds.
- **The emulator returns every row on the first page** and does not run inserted query jobs,
  so paging and polling are proved against the fixture, and reads use `jobs.query`.
- **`at` is a reserved word** in the emulator's SQL (ZetaSQL): a probe query with a column of
  that name failed to parse. The tests say `loaded_at`.
- **Three test expectations were wrong, not the code**: 1790244000 seconds is 10:00:00 UTC,
  not 10:40:00, as the emulator itself had shown in the probe.
- **The CI run for 10m was green** (35972853098) once the Ubuntu runner's disk was freed.

### From Phase 10m

- **CI's Ubuntu runner ran out of disk** on the first push of 10m (`d1fff00`, run
  35971619028): the Rust build and every test server's image no longer fit, and `rustc`
  failed with "No space left on device" compiling `mongodb`. Not a test failure. `gate.yml`
  now frees the runner's unused toolchains (.NET, Android, GHC, CodeQL, about 20 GB) before
  anything else on Ubuntu. Every phase that adds a server adds an image, so this is the
  budget to watch.
- **Every MongoDB test passed at its first run**, after a probe of the driver answered the
  design's questions first (TLS, upserts, errors, a missing collection).
- **The mutation that inserts stop at the first refusal was not caught at first**: the test
  put its duplicate last, where ordered and unordered agree. The duplicate is now in the
  middle.
- **Docker Desktop's clock ran 150 ms ahead of Windows'** by the second full run, and a
  Kinesis `latest` test failed every time from then on: records put a moment before the run
  carried arrival times after its start. Not the connector; the test now waits longer than
  any skew, and `connectors.md` says `latest` depends on the machine's clock.
- **`mongod`'s first start creates the user and restarts**: readiness waits for the image's
  "init process complete" before a signed-in ping.

### From Phase 10l

- **`lapin` never answers a connect to a vhost that does not exist.** The broker refused it
  in 30 ms (its log says so); the client waited forever. Found in the probe before any code
  was written; every call now has `timeout_ms`, and the message names the vhost.
- **Its TLS could be ours after all**, through `Connection::connector` and
  `RustlsConnector::from(ClientConfig)`, once `amq-protocol-tcp`'s `rustls-common` feature
  was named directly: `lapin`'s own features do not reach it.
- **A dropped connection is itself a release.** The probe showed the broker requeue at once,
  so a `nack` that cannot be sent is not a failure to release.
- **RabbitMQ 4 moved the redelivery count** to `x-acquired-count`; the quorum test caught it.
- **Windows reserves port ranges near 55672** on this machine (55621-56220), so a container
  could not publish there; 5767x instead.
- **PowerShell 5.1 mangles nested double quotes** in an argument to a native program: the
  container got a broken `printf` and exited. Single quotes pass through.
- **An `ack` has no reply in AMQP**; closing the channel waits for the broker's close-ok,
  which comes after it has processed the `ack`, so the receipt closes before it reports.

### From Phase 10k

- **RS256 matched RFC 7515's example at the first attempt**, and every emulator test passed at
  its first run: the fixture tests had already pinned down what the emulator then did.
- **A pull holds for the subscription's deadline, not the one the source was given.** The
  plan had the keeper alone keeping the hold; its first extension, at half of 60 seconds,
  would come after a default 10-second deadline ran out. Each pull is now extended at once.
- **`returnImmediately` is deprecated and still the safer choice here**: without it an empty
  subscription waits server-side for an unstated time. Recorded in `connectors.md` as a
  trade-off, unseen against real Pub/Sub.
- **Pub/Sub counts deliveries only with a dead-letter policy**, so "a receive count" (decision
  62) is null for most subscriptions.
- **Two test assertions built Windows paths with `/`**; comparing to `Path::join` fixed them.
- **A Python string wrote `\a` in a Windows path as a bell character** into `connectors.md`.
  Found by reading the file back; fixed with an edit, not another script.
- **The heredoc mistake from 10j came back** (a long block of Rust with `r#"`); written to a
  file and appended instead, as 10j's learning says.

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

### 2026-09-17 — Between phases: the CLI gets tests

Not a phase. `cargo test -p etl-cli` reported **0 passing** — the one crate the 576 did not
reach, and the crate Phase 9's build logic is about to land in.

It is not only argument parsing. `ConsoleWorkspace` is the seam `etl-console` is built around,
and every route in that crate is tested against a *fake* workspace; the real one, which turns a
name off a socket into a path on disk, had nothing on it. `collect_pipelines`, `watermarks_for`,
`Settings::for_schedule` and `record_of` were in the same position.

- `crates/cli/src/tests.rs` — **39 tests**, the `src/tests.rs` shape every other crate uses.
  Scanning (what counts as a pipeline, what is skipped, the depth bound, name order), `locate`
  including the traversal cases, the console's listings and run history, the two watermark
  precedence rules, schedule inheritance, and the records a run leaves behind.
- `crates/cli/src/main.rs` — one line: `#[cfg(test)] mod tests;`.

Nothing under test changed. **615 Rust tests**, 114 frontend, clippy clean, fmt clean.

#### What running it changed

- **The suite passed on the first run, which is not evidence.** Three mutations were made to the
  code under test — `MAX_DEPTH` 4 → 5, `record_of`'s outcome inverted, and
  `watermarks_for`'s column guard short-circuited — and exactly four tests failed, no more and
  no fewer. Reverted after. A test that cannot fail is worse than no test, because it is
  counted.
- **`git checkout` to undo those mutations also removed the `mod tests;` line**, since it was an
  uncommitted change to the same file. Obvious afterwards; worth the line here.
- **The traversal test asserts *not found* rather than found-and-refused.** `locate` resolves a
  name by looking it up in the workspace's own list, so there is no join for `../../etc/passwd`
  to escape through. Pinning "404" rather than "403" is what stops somebody later replacing the
  lookup with a join plus a check, which is the shape that has holes in it.

### 2026-09-17 — Phase 9a: the artifact, and the payload format

Phase 9 split into 9a–9d before starting. 9a is the part that makes the phase real: one file
you can copy somewhere else and run.

- `crates/runner/` — new crate, and the first with both a lib and a bin:
  - `lib.rs` — the payload format. A trailer at the very end of the file (header length, blob
    length, magic), found by reading backwards, because the end is the only anchor that does
    not depend on knowing how long the executable is. 15 tests.
  - `main.rs` — `etl-runner`: reads its own tail, compiles, runs, prints. `--info`,
    `--workspace`, `--sql`, `--no-counts`, and the same four exit codes `etl` uses.
- `crates/duckdb-engine/src/report.rs` — the run report's formatting, moved out of the CLI so
  the runner could share it rather than grow a second copy. 8 tests.
- `crates/cli/src/main.rs` — `etl build`, `incremental_nodes`, and `print_report` reduced to a
  caller of the above.

**640 Rust tests** (15 runner, 8 report, 2 CLI new). Gate green: fmt, clippy with `-D warnings`,
frontend typecheck and build.

#### What running it changed

- **The runner printed less than `etl run` did, and it took building a real artifact to see
  it.** The first version formatted stages itself and quietly dropped rejected counts, skipped
  reasons and control-flow notes — so `orders_checked` reported `10 rows` where `etl run` says
  `10 rows  2 rejected`. Two printers for one report is the drift Settled decision 5 exists to
  prevent, so the formatting moved into the engine and both now call it. They print identical
  lines for the same pipeline, timing aside.
- **`--allow-secrets` was checked by grepping the built file for the password.** It is there,
  in plaintext, exactly as the warning says. Worth doing once: a security warning nobody has
  verified is a security warning that might be wrong in the reassuring direction.
- **`orders_guarded` exercises the session transport, and was built and run on purpose.**
  Control flow and per-stage policy take a different execution path, and "it worked on the
  simple sample" would not have said anything about it.
- **An exit code read through a pipe is `tail`'s, not the binary's.** Two of the first
  measurements were meaningless because of it. Re-checked without the pipe: 0 for a good run,
  1 for a missing engine, 1 for an unbaked runner, 2 for the incremental refusal.
- **`git checkout` is not a safe undo for a scratch mutation** when the file also holds
  uncommitted work — the same lesson as the previous session, arrived at from the other side.

### 2026-09-17 — Phase 9b: the engine and its extensions inside the file

The half of Phase 9 that makes the artifact worth shipping: it now carries DuckDB and whichever
extensions its pipeline asked for, and runs where there is neither.

- `crates/runner/src/lib.rs` — the payload format gains `Role`, `duckdbVersion`, `platform` and
  `blobDigest`; `write_built` takes `&mut self` so it can record the digest itself.
- `crates/runner/src/extract.rs` — new. Unpack to a keyed directory, once, published by a single
  rename. 12 tests.
- `crates/duckdb-engine/src/sql.rs` — `contains_install`, a scanner that skips literals,
  identifiers, dollar-quoted blocks and comments. 14 tests.
- `crates/duckdb-engine/src/plan/mod.rs` — `EngineError::RawInstall`, raised in `compile`.
- `crates/cli/src/main.rs` — `gather_embedded`, `toolchain_roots`, `platform_directory`, and
  `--no-embed`.

**665 Rust tests** (26 runner, 276 engine). Gate green: fmt, clippy with `-D warnings`, frontend
typecheck and build.

#### What running it changed

- **Two tests failed the first time, and both were real.** The cache key could not tell two
  builds apart whose engines were the same length — the header records lengths, not content —
  so the second artifact would have run the first one's engine. And `rename` refused to land on
  the directory left by an interrupted extraction, wedging every later run. Both are written up
  under *From Phase 9b*. The tests were written to catch exactly these and did.
- **The first version of those tests was wrong in a way worth remembering.** They isolated
  themselves by setting an environment variable for the cache location, which is
  process-global: five tests failed, clobbering each other's value, and one read another's
  directory. The fix was to remove the global rather than serialise the tests — `extract_into`
  takes the base directory as an argument, and `extract` is the thin wrapper that reads the
  environment. Where to unpack is an input; pretending otherwise was the bug.
- **Building a pipeline whose data lived elsewhere could not find the engine at all.** The
  lookup searched from `--workspace`, and the vendored toolchain is near the checkout. Found by
  building a scratch pipeline in a temp directory, which is the first thing a real user would
  do.
- **`cargo fmt` collapsed three `\`-continued error messages onto one line and left the
  indentation inside the string**, so the messages read "...the extensions          vendored
  with it...". Caught by reading the output of the refusal rather than trusting the source.
- **The extension path was written one directory too high** in the first draft —
  `<root>/extensions` rather than `<root>/extensions/<version>/<platform>` — which DuckDB would
  simply not have found. Caught while re-reading before building, not by a test.

### 2026-09-17 — Phase 9c, part done: cross-building

Chose Docker directly over `cross` and `cargo-zigbuild`, on the grounds that it installs nothing
on the machine. Then found the Docker daemon is not running, so the phase stops one step short.

- `crates/cli/src/main.rs` — `Target`, `host_platform`, `--target`, per-target runner and engine
  lookup; the 9b "single platform directory" helper deleted.
- `crates/cli/src/tests.rs` — 7 tests for the target model.
- `scripts/fetch-duckdb.ps1` — `-Platform`; cross-targets to `tools/duckdb/targets/<platform>/`.
- `scripts/fetch-duckdb-extensions.ps1` — `-Platform`; cross-targets downloaded and gunzipped
  from `extensions.duckdb.org`, with an explicit note that they are not verified.
- `scripts/build-runner.ps1` — new. Builds the runner inside `rust:1.96-slim`, caching the cargo
  registry in a named volume. **Written and parsed, never executed.**

**672 Rust tests.** Gate green: fmt, clippy with `-D warnings`.

#### What running it changed

- **The Bash heredoc had been silently eating one level of backslashes all session.** It turned
  `tools\duckdb\targets` into `tools\duckdb<TAB>argets` in a generated PowerShell path, and it
  explains the mangled paths in two earlier sessions as well. Anything containing a backslash
  now goes through the Write tool instead. Worth knowing before debugging a path that looks fine
  in the source.
- **`$input` is an automatic variable in PowerShell.** Used it for a gzip stream, renamed it
  before running rather than after.
- **The Linux extension is a different size from the Windows one**, which is what made the
  selection verifiable without ever running it: 11.9 MB against 22.7 MB for `excel`, and 61.9 MB
  against 37 MB for the engine. Reading those back out of a built artifact is a real check, not
  a proxy for one.
- **Docker is installed and its daemon is not running.** Found by asking it to run `alpine`
  before building anything around it, which is the right order.

### 2026-09-17 — Phase 9c finished: a Linux artifact that runs

Docker was started, and the three remaining commands took four attempts rather than one. Every
failure was real, and the last one was a bug that had been in the project since 9a.

- `scripts/build-runner.ps1` — pinned to `rust:1.96-slim-bookworm`; target directory keyed by
  image.
- `crates/duckdb-engine/src/params.rs` — `Resolver::defer_built_ins`.
- `crates/duckdb-engine/src/params/tests.rs` — 6 tests for it.
- `crates/cli/src/main.rs` — `etl build` resolves with built-ins deferred.
- `crates/runner/src/main.rs` — the runner resolves them at startup, against its own workspace.

**678 Rust tests.** Gate green: fmt, clippy with `-D warnings`, frontend typecheck and build.

#### What running it changed

Nothing in this section was predicted; all four came out of running the thing.

1. **The runner would not start on Debian 12** — built against glibc 2.39 by a trixie image.
2. **Switching to bookworm failed on stale build scripts** left by the trixie build.
3. **The artifact looked for `D:/workspace/...`** — the `${workspace}` bug above, the one that
   mattered.
4. **A stale runner made the fix look like it had not worked.** `tools/runners/linux_amd64/` still
   held the previous binary, so the first rebuild-and-test after the fix failed identically.
   Worth remembering: there are now *two* artifacts to rebuild after a runner change, and the
   embedded one does not rebuild itself.

Also: three `str.replace` calls in a patch script had no assertion on them, and one of them
silently did nothing — which is what made (4) hard to read. Every anchored replacement in a
patch script gets an assert.

### 2026-09-17 — Phase 9d: CI, written and unrun

The project's first `.github/`. Four jobs, and the work was less in the YAML than in finding out
what would actually be true on a Linux runner before claiming it.

- `.github/workflows/gate.yml` — `gate` (windows + ubuntu), `artifact` (both),
  `cross-build-script` (ubuntu), `frontend`.
- `scripts/fetch-duckdb.ps1` — host detection that works off Windows.

**678 tests on Windows, 668 on Linux.** Local gate green: fmt, clippy with `-D warnings`,
frontend typecheck.

#### What running it changed

Everything here came from running things rather than from writing YAML.

- **The Linux build failed on Tauri's system dependencies**, which is why the Linux job excludes
  the desktop crate. Found by running `cargo test --workspace` in a container, which is the only
  reason the workflow does not claim something false.
- **Parking the extensions directory** showed exactly one test depends on one extension, which
  turned a 250 MB per-run download into a 23 MB one.
- **The host-detection bug in `fetch-duckdb.ps1`** would have had Linux CI download a Windows
  binary and then fail confusingly. Found by reading the script while thinking about where it
  would run, not by running it.
- **The two `build-runner.ps1` assertions were checked by hand first** — the unbaked runner's
  exit 1 and message, and the second-run no-op — because a CI assertion nobody has seen pass is
  a guess.

#### What is not done

**CI has never run.** Phase 9's "done" is a green matrix, and this has only been reasoned about
and locally rehearsed. The first real run should be expected to find something; that is what
first CI runs do.

### 2026-09-23 — Phase 9 closed: three CI runs, and a new machine

The project moved to a new machine. It was set up, the first CI run was read, and
Phase 9 was closed on the third run. Committed and pushed by the user as `91a5f24`,
`14be255`, `64878e6` and `e7f629b`.

#### What each CI run found

**Phase 9 is committed and pushed** as `ad7fc51` (2026-09-23 07:42 +0530), and `gate.yml`
ran on it: [run 35809441173](https://github.com/marun224/local_etl_tool/actions/runs/35809441173),
**failure**. Read on 2026-09-23. **The Rust itself passed**: on Windows, fmt, clippy and all the
tests were green. What failed was the workflow and one script, in three places:

| Job | Failed at | Cause | Fix (uncommitted) |
|---|---|---|---|
| `gate (windows)` | *The registry is all there* | `target/debug/etl.exe` not found. `cargo test` builds the CLI only as a test harness under `deps/`; every rehearsal machine had an old `cargo build` lying around | `gate.yml`: new step `cargo build -p etl-cli` after the tests |
| `gate (ubuntu)` | *Fetch DuckDB…* | `fetch-duckdb-extensions.ps1` hard-coded `duckdb.exe`. 9d fixed host detection in `fetch-duckdb.ps1` and missed this script | the script picks `.exe` only on Windows |
| `build-runner.ps1` | *Running it again is a no-op* | the second run **did** skip; the script says so with `Write-Host` (stream 6) and `\| Out-String` captured nothing | `gate.yml`: `6>&1` |
| `artifact` (both) | never ran | `needs: gate` | — |
| `frontend` | **passed** | | |

**How each fix was checked locally, before any push:**
- The no-op bug was **reproduced on this machine**. Captured length 0 without `6>&1`, and 151
  with it and a match. The Linux runner left in `tools/` from 9c makes the script take the
  same "already present" branch without Docker.
- The extensions script still works on Windows: `excel already present`, `excel loads`, exit 0.
  The Linux branch is checked only by reading it, because Docker's daemon is not running here.
- The registry and samples steps' exact bash lines pass against the prebuilt `etl.exe`.
- The Windows `artifact` job, which has never run in CI, was rehearsed: bake `orders_checked`,
  copy it out of the repo, run it with `--workspace` pointing back, and `Ran 6 stage(s)`.
- The YAML parses: 4 jobs, and the new step sits between *Tests* and *The registry…*.

**The second run** ([35831651720](https://github.com/marun224/local_etl_tool/actions/runs/35831651720),
after `91a5f24` + `14be255` were pushed on 2026-09-23 at the user's request) confirmed all three
fixes: **`gate (windows)`, `build-runner.ps1` and `frontend` are green.** `gate (ubuntu)` passed fmt
and clippy, then failed **one test of 668**:
`session::tests::an_error_message_does_not_leak_into_the_next_statement`. The `artifact` jobs
still have not run, because they need `gate`.

**That failure is a real race, not a Linux quirk.** stdout and stderr are separate pipes. A
failed statement's marker can come back on stdout before its message arrives on stderr, and
the next `execute` then picks the message up as its own. It passed in the 9d container and
200 times in a row on Windows; a busier runner lost the race. **The engine has the same
exposure, not just the test:** `exec.rs:1270` (session transport with `--no-counts`) can
report a failed stage as succeeded and blame the next one, and `exec.rs:1322` can turn "no new
rows" into an error. The counts-on path is covered only by `STDERR_GRACE`'s 250 ms, which is
timing rather than a guarantee.

**Fixed in the working tree, not pushed:** stderr is now framed the way stdout is. Every
statement is followed by `SELECT error('__etl_errmark_N__')` as well as the stdout marker,
and `read_answer` reads each stream up to its own marker. `STDERR_GRACE` and
`Session::message()` are gone. Tried in the scratchpad first against the real `duckdb.exe`
(0 misattributed messages in 500 alternating statements, under 1 ms per statement), then built.
Seven new tests, **685 in all**. `a_message_that_arrives_after_the_rows_still_belongs_to_its_statement`
forces the race with channels and a 150 ms delay, and it **fails** when the old
"whatever stderr holds now" read is put back (checked by mutation, reverted by edit). The
end-to-end suite went from 9.7 s to 2.8 s, since nothing waits out a grace period any more.

**The new prelude test found a second, older bug.** A session's prelude checked only that
`SELECT 1` returned rows, and a failed `LOAD` does not stop it. **A session never detected a
missing extension.** It now also refuses when the prelude said anything. A successful `LOAD`
writes 0 bytes to stderr, which was measured before relying on it.

**The re-run of the failed job** (same run, attempt 2, at the user's request) passed the Ubuntu
gate, by luck as expected, and so ran the `artifact` jobs for the first time:
**`artifact (windows)` green**. `artifact (ubuntu)` baked and ran from elsewhere, then failed in
the bare `debian:12-slim` container: `GLIBC_2.39 not found`. That job built `etl-runner` on the
ubuntu-24.04 host, not in the bookworm image the project ships from. It is the 9c glibc lesson,
found by a different road. **Fixed in the working tree, not pushed:** on Linux the job now runs
`build-runner.ps1` and bakes with `--runner tools/runners/linux_amd64/etl-runner`. Checked here
without Docker: the bookworm runner needs glibc ≤ 2.34 and the Linux DuckDB ≤ 2.25, both under
Debian 12's 2.36; the YAML parses.

**After those two land, every job has a fix for what it last found.** Nothing is known to be
failing. Phase 9 is done when the next run is green.

**The third run**, [35833839177](https://github.com/marun224/local_etl_tool/actions/runs/35833839177),
on `e7f629b`: **green on all six jobs.** Windows gate 685 tests, Ubuntu gate 675, both artifact
jobs including the bare, offline `debian:12-slim` container. Phase 9's "done" met.

#### The local gate on the new machine

**Checked here on 2026-09-23 without Rust:** frontend 114 tests passing, typecheck clean,
build clean; the prebuilt `etl.exe` lists 54 components and the three samples print
12/5/7/6/6, 12/10+2/9+1/9/2/1, and 12 through with the branch taken.
**The full local gate is green on this machine** (2026-09-23, after the toolchain install):
`cargo fmt --all --check` clean, `cargo clippy --workspace --all-targets -- -D warnings`
clean, **`cargo test --workspace` 678 passing** (48 cli, 65 console, 10 desktop, 282 engine,
51 e2e, 15 metadata, 26 runner, 113 scheduler, 23 secrets, 45 state). A cold build took
4m26s for clippy and 8m39s to the end of the tests. After the tests, `target\debug\etl.exe`
*still* carried its 2026-09-17 timestamp — the CI bug, seen directly — and the new
`cargo build -p etl-cli` step replaced it (12:51), with the samples unchanged.

#### Housekeeping

**Two stray directories were removed from the repo root** on 2026-09-23, both empty and
untracked: `${workspace}\samples\out` (from a pre-Phase-5 run, before `${workspace}` was
substituted) and `D\workspace\ETL_Local_Tool\samples\out`, whose name was `D` + U+F03A — the
character Windows stores for a `:` written from a Linux container. That one is the
Phase 9c `${workspace}` bug's footprint: a container run wrote to a literal `D:/workspace/...`.

### 2026-09-23 — Phase 10a: the plugin SDK, the staging bridge, and XML

Signed off and built the same day. Probed DuckDB's `read_json` and `COPY … (FORMAT json)` in the
scratchpad first, on the five behaviours the bridge rests on. One of those probes was wrong in a
way only the end-to-end test found (see *From Phase 10a*).

- `crates/plugin-sdk/` — new. `Source`, `Sink`, `RecordWriter`, `RecordReader`, `Context`,
  `Summary`, `ConnectorError`, `columns_property`. No I/O. 5 tests.
- `crates/connectors/` — new. `all()` and `find()`; `xml.rs` with `XmlSource` and `XmlSink`.
  28 tests: entities, CDATA, attributes on records and children, namespaces, the four
  refusals, a byte-stable round trip, the atomic write leaving the old file untouched.
- `crates/duckdb-engine/src/plan/` — `NativeStep`, `Direction`, `NATIVE_DIR`, `Stage::native`;
  `native_source` and `native_sink` builders; `native_components()` appended to the registry.
- `crates/duckdb-engine/src/native.rs` — new. Staging sources, preparing and delivering sinks,
  the `Staging` guard, and the JSON Lines reader and writer. 5 tests.
- `crates/duckdb-engine/src/exec.rs` — the bridge on `run_one_script`, `run_driven` and
  `preview`; delivery only when the run has no failures; the clock moved before staging.
- `crates/duckdb-engine/tests/native.rs` — new. 9 end-to-end tests against real DuckDB.
- `samples/data/orders.xml`, `samples/pipelines/orders_xml.json` — the acceptance fixture.
- `.github/workflows/gate.yml` — 56 components, `orders_xml` in the samples, and baked, run
  from elsewhere and run in the bare container by the `artifact` job.
- `docs/connectors.md` — new: delivery semantics. `docs/adding_a_component.md` — the native
  section.

**739 Rust tests, 117 frontend.** Fmt, clippy with `-D warnings`, typecheck and build clean.
`etl run samples/pipelines/orders_xml.json` prints 12 / 7 / 7, and the same pipeline baked with
`etl build` runs from a directory outside the repo. Not committed.

### 2026-09-23 — Phase 10b: SaaS REST

`ring` confirmed as rustls's provider at the start (Settled decision 16).

- `crates/plugin-sdk/src/lib.rs` — `Source::check` and `Sink::check`, default accept;
  `Connector::check`.
- `crates/connectors/src/rest.rs` — new. `RestSource`, `RestSink`, and the shared `Client`:
  auth, retries with doubling backoff and `Retry-After`, `min_interval_ms`, timeouts; five
  pagination styles; `max_pages`; base64 for basic auth. 25 tests against a `tiny_http`
  fixture that records every request.
- `crates/connectors/src/xml.rs` — element-name checks moved into `check`.
- `crates/duckdb-engine/src/plan/mod.rs` — connectors' `check` called while compiling;
  `url_for_lineage`; `Stage::external` from `url`.
- `crates/duckdb-engine/tests/native.rs` — 3 REST pipeline tests: the committed sample end to
  end with an encrypted token, a 401 echoing the token (masked), a sink failing after DuckDB.
- `samples/pipelines/rest_orders.json` — cursor pagination, bearer secret, batches of 2.
- `docs/connectors.md` — the REST section. `gate.yml` — 58 components.

**773 Rust tests, 120 frontend.** Fmt, clippy with `-D warnings`, typecheck clean. Not
committed.

### 2026-09-23 — Phase 10c: Phase 4's connectors against real systems

The declared Rust version was raised to 1.88 first (Settled decision 17). Delta and Iceberg
were verified before Docker was running; the servers once the user started it.

- `crates/duckdb-engine/tests/verified.rs` — new. 4 lake tests against committed fixtures,
  5 server tests that skip without `ETL_TEST_POSTGRES`, `ETL_TEST_MYSQL` or `ETL_TEST_S3`.
- `crates/duckdb-engine/tests/fixtures/lake/` — Delta and Iceberg tables from `deltalake` and
  `pyiceberg`, 30 KB, with the README and script that made them.
- `scripts/test-services.ps1` — new. Postgres 16, MySQL 8.4 and MinIO in Docker, readiness
  waits, the bucket via `mc`; prints the variables, or writes them to `$GITHUB_ENV` in CI.
- `crates/duckdb-engine/src/exec.rs` — `prepare_sinks` skips remote paths.
- `crates/duckdb-engine/src/plan/{specs.rs, builders.rs}` — S3 access properties and the
  scoped secret; Iceberg `version` and the moved-path refusal; the MySQL pushdown setting.
- `crates/secrets/src/lib.rs` — `is_multiple_of` back, now that 1.88 is declared.
- `.github/workflows/gate.yml` — the services step on Ubuntu; `DUCKDB_TEST_EXTENSIONS` in the
  fetch step and the cache key.

**788 Rust tests with the servers up**, three runs in a row for the verification suite. Fmt,
clippy clean. Not committed.

### 2026-09-23 — Paused by the user

After 10c the user chose the website's site-to-product sync. In the WebApp repo the site was
re-audited against this engine (58 components, 10c's verification): 12 of its 46 listed
connectors are built and verified, Amazon S3 works but was checked only against MinIO, 33 are
not built, and XML is built but not listed. Eight questions were written there, and the user
paused before answering them. This repo was left with the gate green at the last change, the
Docker test services stopped and removed, and all of Phase 10 uncommitted.


### 2026-09-23 — Phase 10d: SaaS GraphQL

Started by the user ("start phase 10"); eight questions answered all as recommended (Settled
decisions 18–24), the plan approved, then built in the same sitting.

- `crates/connectors/src/http.rs` — new: the HTTP layer moved out of `rest.rs`, plus
  `Settings::posting`, `Client::send_judged`/`Judged`, `Reply::retry_after`,
  `page_cap_reached` and `rows_at`. Proved behaviour-neutral by REST's 28 tests, unedited.
- `crates/connectors/src/fixture.rs` — new: REST's test server, now shared.
- `crates/connectors/src/graphql.rs` and `graphql/tests.rs` — new: both components, 29 tests.
- `crates/connectors/src/{lib.rs, rest.rs, rest/tests.rs}` — registration; REST uses the
  shared layer; `body` is a `code` property.
- `crates/metadata/src/component.rs`, `frontend/src/{ipc.ts, Inspector.tsx, properties.ts}`
  and their tests — the `code` property kind.
- `crates/duckdb-engine/tests/native.rs` — 5 end-to-end tests: the sample on both
  transports, preview, `errors` in a 200 masked by stage, and a failed upstream sending
  nothing. `plan/specs/tests.rs` — the inventory.
- `samples/pipelines/graphql_orders.json` — new. `.github/workflows/gate.yml` — 60 components.
- Docs: `connectors.md` (GraphQL's semantics), `adding_a_component.md` (web connectors),
  `learnings.md`, `assignments.md` (A32–A35).

**822 Rust tests** (servers down, so 5 of them skip), **125 frontend**. Fmt, clippy with `-D
warnings`, typecheck clean. Two mutation checks caught. Hand-checked once against
`countries.trevorblades.com` through `etl run`. Committed at the user's request; not pushed.

### 2026-09-23 — Phase 10e: checkpoints, and the Kafka source

Started by the user ("let us start phase 10's next family"); eleven questions answered all as
recommended (Settled decisions 25–35), a twelfth raised in planning and answered (a), which
became Settled decision 36. Docker started by the user.

- `crates/plugin-sdk` — `Context::checkpoint`, `Summary::checkpoint`, `Summary::new`.
- `crates/state` — `Checkpoint`, a `checkpoints` map, `record_checkpoint`, `forget` of both.
- `crates/duckdb-engine` — `NativeStep::checkpoint`, `CompileOptions::checkpoints`,
  `RunReport::checkpoints` (empty for a failed run), `native::stage_sources_using` for a
  test-only connector, and the new `remember` module.
- `crates/cli` — `save_state` and compile options through `remember`; `etl state` lists and
  forgets positions; `etl build` notes instead of refusing; four mangled messages fixed.
- `crates/runner` — loads and saves `.etl/state/` where it runs.
- `crates/connectors/src/kafka.rs` and `kafka/tests.rs` — the source, 22 tests (5 need a
  broker). `rskafka` 0.6 and `tokio` (`rt`, `net`, `time`) added; `cargo tree` clean.
- `crates/duckdb-engine/tests/verified.rs` — 4 Kafka pipeline tests. `samples/pipelines/
  kafka_orders.json`. `scripts/test-services.ps1` — a Kafka 4.1 container. `gate.yml` — 61
  components. `frontend/src/icons.ts` — `radio`.
- Docs: `connectors.md` (Kafka's semantics, and what checkpoints mean for every native
  source), `adding_a_component.md`, `learnings.md`, `assignments.md` (A36–A39).

**859 Rust tests with every server up, none skipped; 128 frontend.** Fmt, clippy, typecheck,
build clean. By hand through `etl`: 12, then 0, then 3 new, then `forget` and 15; and a built
artifact run three times from its own directory, 15, 0, 1, with `etl state list` reading its
state. Test services stopped and removed. Committed and pushed at the user's request, with
`[skip ci]` because the user asked for no CI run.

### 2026-09-23 — Phase 10f: the Kafka sink, TLS and SASL

Started by the user ("pls start 10f"); the plan and decisions were already in place from 10e's
questions (Settled decisions 31–33).

- `crates/connectors/src/kafka.rs` — the shared `Connection` (security, SASL, `ca_cert`, the
  retry-off diagnosis), `snk.stream.kafka`, `murmur2`/`partition_for`/`assign`. `rskafka`'s
  `transport-tls`, `rustls` (ring) and `webpki-roots` added; `cargo tree` still one TLS stack.
- `crates/connectors/src/kafka/tests.rs` — 16 more: murmur2, placement, settings, and against
  the broker: sink round trip, keyless spread, all codecs, a refused batch, each security mode,
  a wrong password, an untrusted CA.
- `crates/cli` — `--stdin` drops a leading BOM (`stdin_secret`, one test).
- `scripts/test-services.ps1` and `scripts/kafka-test-secrets.sh` — SASL, TLS and SASL_SSL
  listeners, SCRAM users, the CA under `target/test-services/`.
- `samples/pipelines/kafka_orders.json` sends large orders back to a second topic;
  `verified.rs` creates it, waits for topics to be listed, and checks what arrived.
- `gate.yml` — 62 components. Docs: `connectors.md`, `learnings.md`, `assignments.md`
  (A40–A42).

**876 Rust tests with every server and listener up, twice in a row; 128 frontend.** By hand:
the Java placement comparison, and SASL_SSL with SCRAM-SHA-512 through `etl run` with the
password in the secret store, then a wrong password and a missing CA, each failing with its
reason. Not committed.

### 2026-09-24 — Phase 10g: NATS JetStream, both ways

Started by the user ("pls start 10g"), after ten questions answered all as recommended
(Settled decisions 37–46). Docker running.

- `crates/connectors/src/nats.rs` and `nats/tests.rs` — both components, 27 tests (15 need a
  server). `async-nats` 0.50 (`jetstream`, `ring`, `nkeys`) and `futures-util` added; the tree
  still has one `rustls` and one `ring`.
- `crates/connectors/src/tls.rs` — the TLS set-up both brokers use. `kafka.rs` gains
  `value_columns` and `key_text`, shared with NATS.
- `scripts/test-services.ps1` and `scripts/nats-test-creds.sh` — five NATS servers: open, user
  and password, token, TLS (Kafka's certificate), operator mode for `.creds`.
- `samples/pipelines/nats_orders.json`; `verified.rs` — 3 tests: the sample on both transports
  (including message IDs keeping re-published orders out after a lost position) and preview.
- `gate.yml` — 64 components. Docs: `connectors.md`, `learnings.md`, `assignments.md`
  (A43–A45).

**906 Rust tests with every server up, twice in a row; 131 frontend.** By hand: the built
artifact of the NATS sample read 12, then 0, then the 1 new message, and published 7 large
orders to the second subject; `etl state list` read its position. Committed and pushed with
10f at the user's request, as one commit because the user had staged both together.

### 2026-09-24 — Phase 10h: Kinesis signing, credentials and source

Started by the user ("start 10h") after ten questions answered all as recommended (Settled
decisions 47–56), CI for 10e–10g running meanwhile and passing.

- `crates/connectors/src/aws.rs` and `aws/tests.rs` — SigV4 and the credential and region
  sources; 7 tests, one of which runs AWS's 38 SigV4 cases (`tests/fixtures/sigv4/`, from
  `awslabs/aws-c-auth` at `c4bc791`, Apache-2.0, with its README and licence).
- `crates/connectors/src/kinesis.rs` and `kinesis/tests.rs` — the source; 15 tests (7 need
  `kinesis-mock`), including a real split and a real merge. Every attempt's signature is
  recomputed from what a local server received, and throttled `400`s are told from final ones.
- `crates/connectors/src/http.rs` — `Extra` (per-attempt headers, a content type, throttled
  errors), `Settings::signed_post`, `base64_decode`; REST's and GraphQL's tests unchanged.
  `ring` made a direct dependency (it already was one underneath).
- `scripts/test-services.ps1` — `kinesis-mock` 0.4.13. `samples/pipelines/kinesis_orders.json`;
  `verified.rs` — 3 tests; the engine takes `ureq` as a dev-dependency to set streams up.
- `gate.yml` — 65 components. Docs: `connectors.md`, `learnings.md`, `assignments.md`
  (A46–A48).

**931 Rust tests with every server up, twice, none skipped; 134 frontend.** Committed and
pushed at the user's request with `[skip ci]`: CI has not run on 10h.

### 2026-09-24 — Phase 10h committed; Phase 10i: the Kinesis sink

At the user's request ("commit and push, dont run git CI. after that pls start 10i"), 10h was
committed as `ce2db9c` with `[skip ci]` and pushed. Git's LF-to-CRLF warnings then led to the
finding that a Windows checkout would break the SigV4 fixtures; `.gitattributes` fixes it,
uncommitted.

- `crates/connectors/src/kinesis.rs` — `snk.stream.kinesis`; `check_connection` shared by
  both components. `kinesis/tests.rs` — 11 tests, 8 against the local fixture, 3 against
  `kinesis-mock` (keys, spreading, a missing stream).
- `samples/pipelines/kinesis_orders.json` — a `put_large` node and a `large_stream`
  parameter; `verified.rs` checks the second stream holds 5, then 5, then 5 more than the new
  large orders.
- Inventory and `gate.yml`: 66 components. Docs: `connectors.md` (the sink), the plan's
  as-built notes, `learnings.md`, `assignments.md` (A49–A50).

**942 Rust tests with every server up, twice, none skipped; 134 frontend.** Committed and
pushed with `.gitattributes` at the user's request, with `[skip ci]`: CI has not run on 10h
or 10i.

### 2026-09-24 — Phase 10j: receipts, and SQS

Started by the user ("pls start 10j") after questions 57–70 were answered all as recommended
and the plan written.

- `crates/plugin-sdk` — `Receipt`, `Source::read_held` (default: hold nothing).
- `crates/duckdb-engine` — `native::Receipts` (settled once; `Drop` releases), acknowledged
  after delivery on both transports, released otherwise; `RunReport::warnings`, printed with
  ⚠. 5 tests in `native/tests.rs`, 1 in `report/tests.rs`.
- `crates/state` and `crates/cli` — `RunRecord::warnings`, kept in history and shown by
  `etl runs show`; 1 test.
- `crates/connectors` — `aws::JsonApi` (from Kinesis's client), `sqs.rs`: `src.queue.sqs`,
  `snk.queue.sqs`, the lease keeper; 18 tests (6 against ElasticMQ).
- `samples/pipelines/sqs_orders.json`; `verified.rs` — 4 tests (both transports, a failed run
  with and without `continueOnFailure`, preview).
- `scripts/test-services.ps1` — ElasticMQ 1.7.1; `gate.yml` — 68 components;
  `frontend/src/icons.ts` — `inbox`.
- Docs: `connectors.md` (queues in general, SQS), the plan's as-built notes, `learnings.md`,
  `assignments.md` (A51–A52).

**971 Rust tests with every server up, twice, none skipped; 137 frontend** (each sample
pipeline is three frontend tests). Not committed.

### 2026-09-24 — Phase 10k: Pub/Sub (built; emulator checks pending)

Started by the user ("2 --> b": start 10k now, beside the uncommitted 10j), in the same
session that marked GraphQL, Kafka and NATS JetStream working on the website.

- `crates/connectors` — `gcp.rs` (RS256, JWTs, key files, gcloud's login, the token cache)
  with 9 tests; `pubsub.rs` (`src.queue.pubsub`, `snk.queue.pubsub`) with 21 tests (15 against
  the fixture, 6 against the emulator); `lease.rs`, the lease keeper SQS and Pub/Sub share;
  `aws::Sources::get` made crate-visible for the Google lookups.
- `tests/fixtures/rfc7515/` — RFC 7515's A.2 example and its source.
- `samples/pipelines/pubsub_orders.json`; `verified.rs` — 4 tests (both transports, a failed
  run with and without `continueOnFailure`, preview).
- `scripts/test-services.ps1` — the Pub/Sub emulator as `ETL_TEST_PUBSUB` (port 58085);
  `gate.yml` — 70 components; the registry test.
- Docs: `connectors.md` (Pub/Sub).

**1005 Rust tests with no servers, 140 frontend, fmt and clippy clean.** Docker's daemon was
not running at first. Once the user started it: the emulator in the test services, the 21
Pub/Sub tests and the 4 in `verified.rs` passed at their first run; five mutations (the
keeper, `Drop`, acknowledging, the per-pull extension, the token cache) each broke a test;
**1005 with every server up, twice, none skipped**. Not committed.

### 2026-09-24 — Phase 10l: RabbitMQ

Started by the user ("2 --> start 10l"), after 10j and 10k were committed and pushed as
`4b44de9` and the website as `efc49ee` (both at the user's request).

- A scratchpad probe of `lapin` 4.12 against `rabbitmq:4.3-alpine` first: holding with
  `basic.get`, `nack` with requeue, a dropped connection, a missing queue, a wrong password,
  a missing vhost (hangs), and TLS with our `rustls` configuration.
- `crates/connectors` — `rabbitmq.rs` (`src.queue.rabbitmq`, `snk.queue.rabbitmq`) with 12
  tests (9 against RabbitMQ); `lapin`, `amq-protocol-tcp`, `async-rs`, and `tokio`'s
  `rt-multi-thread`.
- `samples/pipelines/rabbitmq_orders.json`; `verified.rs` — 4 tests, through the management
  API.
- `scripts/test-services.ps1` — RabbitMQ 4.3 with plain, TLS and management listeners
  (57672, 57671, 57673); `gate.yml` — 72 components; the registry test.
- Docs: `connectors.md` (RabbitMQ), the plan's as-built notes, `learnings.md`,
  `assignments.md` (A55-A56).

**1021 Rust tests with every server up, twice, none skipped; 143 frontend; fmt and clippy
clean; four mutations each caught.** CI run 35959734855 (10j and 10k) green meanwhile. Not
committed.

### 2026-09-24 — Phases 10m–10u planned

After 10l was pushed (`44d1aaa`), the user asked to target MongoDB, Redis, Elasticsearch, the
site's other databases, Snowflake and BigQuery one by one. Research: each client crate's
version, Rust version and TLS features (crates.io), each test image's size (Docker Hub).
Answered all as recommended except Elasticsearch, dropped for its memory. Plan written as
Phases 10m–10u (decisions 71–81; open question 15 for SQL Server). No code.

### 2026-09-24 — Phase 10m: MongoDB

Started by the user ("pls start 10m").

- A scratchpad probe of `mongodb` 3.9 against `mongo:8.0`: types, Extended JSON filters, a
  missing collection (silent), the `update` command upserting a batch, duplicate keys in an
  unordered insert, a wrong password, an unreachable server; and its TLS source.
- `crates/connectors` — `mongo.rs` (`src.db.mongodb`, `snk.db.mongodb`) with 10 tests (7
  against MongoDB); `mongodb` added. `kinesis/tests.rs`: one test made clock-proof.
- `samples/pipelines/mongodb_orders.json`; `verified.rs` — 4 tests (both transports with
  three incremental runs, a failed run saving no position, preview); `mongodb` a
  dev-dependency of the engine crate.
- `scripts/test-services.ps1` — MongoDB 8.0 (57017, plain and TLS, 1 GB); `gate.yml` — 74
  components; the registry test.
- Docs: `connectors.md` (MongoDB; Kinesis's clock), the plan's as-built notes,
  `learnings.md`, `assignments.md` (A57-A58).

**1035 Rust tests with every server up, twice, none skipped; 146 frontend; fmt and clippy
clean; six mutations each caught.**

### 2026-09-24 — 10n (Redis) dropped

The user: "i dont to implement 10n, can you update in md file". Plan: 10n marked not built,
its design kept for later; the later phases' component counts lowered by four. Tracker:
decision 82, status row, next phase **10o, BigQuery**. No code touched.

### 2026-09-24 — Phase 10o: BigQuery

Started by the user ("can we start building 10o BigQuery: parallely") while 10m's CI ran;
the same session dropped 10n (Redis) from the plan at the user's request.

- A probe of `ghcr.io/goccy/bigquery-emulator:0.8.1` first: `jobs.query` with named
  parameters, typed results, paging, load jobs by multipart upload (append and truncate),
  a missing table, `jobs.insert` (unsupported), and every value type's text.
- `crates/connectors` — `bigquery.rs` (`src.warehouse.bigquery`, `snk.warehouse.bigquery`)
  with 16 tests (3 against the emulator); `http.rs`: `Settings::signed`.
- `samples/pipelines/bigquery_orders.json`; `verified.rs` — 4 tests (both transports with
  three incremental runs, a failed run saving no position, preview).
- `scripts/test-services.ps1` — the emulator (59050, 1 GB); `gate.yml` — 76 components; the
  registry test; `frontend/src/icons.ts` — `warehouse`.
- Docs: `connectors.md` (BigQuery), the plan's as-built notes, `learnings.md`,
  `assignments.md` (A59-A60).
- Meanwhile 10m's CI went green (35972853098), and the website marked MongoDB working
  (`b035bd9`).

**1055 Rust tests with every server up, twice, none skipped; 149 frontend; fmt and clippy
clean; seven mutations each caught.**

### 2026-09-24 — Phase 10p: Snowflake

Started by the user ("pls start 10p").

- `openssl` (Git for Windows' 3.5.7) computed RFC 7515's key's fingerprint the way
  Snowflake's documentation does, as the test's expected value.
- `crates/connectors` — `snowflake.rs` (`src.warehouse.snowflake`,
  `snk.warehouse.snowflake`) with 12 tests against the fixture; `bigquery::fingerprint` made
  crate-visible; `gcp::tests::rfc_key_pem` for other tests.
- `samples/pipelines/snowflake_orders.json` (for a real account; `etl validate` passes);
  `gate.yml` — 78 components; the registry test. No test service: none exists.
- Docs: `connectors.md` (Snowflake), the plan's as-built notes, `learnings.md`,
  `assignments.md` (A61-A62).

**1067 Rust tests with every server up, twice, none skipped; 152 frontend; fmt and clippy
clean; seven mutations each caught.** Not checked against real Snowflake.

### 2026-09-24 — Phase 10q: MariaDB

Started by the user ("pls start 10q").

- A probe: `mariadb:11.8` read and written through DuckDB's mysql extension; its types read
  cleanly; a table created by `CREATE TABLE ... AS SELECT` lost a timestamp's microseconds.
  The same probe on `mysql:8.4` showed the same, and that a `DATETIME(6)` table keeps them.
- `verified.rs` — `round_trip` takes a label; 5 tests (MariaDB's round trip, its types, a
  masked wrong password, and the timestamp behaviour on MySQL and on MariaDB).
- `scripts/test-services.ps1` — MariaDB 11.8 (53307, 1 GB); `gate.yml`'s note.
- Docs: `connectors.md` (MySQL and MariaDB), the plan's as-built notes, `learnings.md`,
  `assignments.md` (A63), open question 16.

**1072 Rust tests with every server up, twice, none skipped; 152 frontend; fmt and clippy
clean.**

### 2026-09-24 — Question 16 fixed; Phase 10r: ClickHouse

The user: questions 16 and 17 as recommended, "start 10r, do not run CI for 10r"; earlier
"Stop CI runs" (10q's run cancelled).

- **Question 16**: a probe on both servers of `SET VARIABLE` + `mysql_execute` + a run-time
  `ALTER ... MODIFY ... DATETIME(6)`; then `builders.rs`' `sink_mysql`, a builder test, and
  `verified.rs`' timestamp test turned round (created tables keep fractions; an owner's
  `DATETIME` is left alone). Two mutations each caught.
- **10r**: a probe of ClickHouse 25.8's HTTP interface (types, parameters, a missing table, a
  wrong password, an error after a 200, the deduplication token); `clickhouse.rs` with 9
  tests; the sample; `verified.rs` 4 tests; ClickHouse in the test services (58123, 1 GB);
  80 components. Six mutations, one missed and fixed.
- The website: MariaDB and ClickHouse marked working (question 17, decision 71).

**1086 Rust tests with every server up, twice, none skipped; 155 frontend.** Pushed with
`[skip ci]`.

### 2026-09-24 — Resumed; 10s and 10t removed from the plan

The user: "run ./scripts/test-services.ps1 … cargo test --workspace and validate", then
"let us not implement 10s and 10t. let us remove from the plan itself".

- **Test servers**: MinIO's port 59000 was refused: Windows now reserves 58921–59020 (after a
  restart). Moved to **57900** in `test-services.ps1`, which is the only place that uses it;
  CI starts its own services.
- **Checks**: 1086 Rust tests with every server up, none failed or ignored; 80 components;
  155 frontend.
- **Plan**: the 10s and 10t rows and sections deleted; the component counts corrected
  (10o–10r's planned counts still included Redis: now 76, 78, 78, 80; 82 after 10u). Decision
  86, status rows, next phase **10u, SQL Server**, after open question 15. No code touched.

The user then answered: question 15 (c), a fixture only (decision 87); the website drops
Cassandra and Neo4j (decision 88); commit and push these docs and the services script with
`[skip ci]`. The plan's 10u section rewritten for the fixture.

Then, all as recommended: the website also drops Redis and Elasticsearch, committed and
pushed (`475ef5d`, decision 88); CI runs for 10u (decision 89). Next: "start 10u".

### 2026-09-24 — Phase 10u: SQL Server

Started by the user ("pls start 10u"); CI runs for it (decision 89).

- **No probe against a server** (decision 87). Instead `tiberius` 0.12.3's source was read
  for what the fixture must answer: PRELOGIN, TLS inside PRELOGIN packets, LOGIN7, the
  tokens and each type's encoding. Found there: its `On` encryption panics against a server
  that refuses (so the connector asks for `Required`), its rustls TLS trusts one CA file,
  everything, or the machine's store (so decision 42 is kept by requiring one of the first
  two), and it panics (`todo!`) on `sql_variant` and CLR types (so the connector catches the
  panic and names a `CAST`).
- `crates/connectors` — `sqlserver.rs` (`src.db.sqlserver`, `snk.db.sqlserver`),
  `sqlserver/fixture.rs` (a TDS server: TLS, sign-in refusals 18456 and 4060, batches,
  `sp_executesql`, twenty types, errors, Azure SQL's routing), `sqlserver/tests.rs` with 31
  tests; `tiberius` (`tds73`, `rustls`) and `tokio-util` (`compat`) added;
  `tests/fixtures/sqlserver/` — a test CA, a `localhost` certificate it signed, and an
  unrelated CA, made with OpenSSL 3.5.7 (README says how).
- `samples/pipelines/sqlserver_orders.json` (for a real server; `etl validate --param
  sqlserver_password=...` passes); `gate.yml` — 82 components; the registry test.
- Nine mutations on the incremental and write paths, each caught once the failed-batch test
  failed on a full batch rather than the last partial one.
- Docs: `connectors.md` (SQL Server), the plan's as-built notes, `learnings.md`,
  `assignments.md` (A66-A67).

**1117 Rust tests with every server up, twice, none skipped; 158 frontend; fmt and clippy
clean.** Not checked against real SQL Server.
