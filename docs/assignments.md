# Assignments

Hands-on exercises drawn from what each phase built. Each one has a goal, a hint, and a way to
check the answer. Mark one done by changing `[ ]` to `[x]` and adding the date.

Phases 0–9 were back-filled on 2026-09-23. **Every exercise below runs with the prebuilt
`target\debug\etl.exe`** unless it is marked 🦀, which means it needs the Rust toolchain
(installed on this machine on 2026-09-23, with the MSVC Build Tools it links with).

Work from the repo root. Copy a sample before changing it, so the committed ones stay intact:

```powershell
New-Item -ItemType Directory -Force samples\out\scratch | Out-Null   # git-ignored, like all of samples/out
Copy-Item samples\pipelines\orders_enriched.json samples\out\scratch\
```

---

## Phase 0–1 — The document and the graph

- [ ] **A1. Break the graph three ways.**
  *Goal:* see what validation catches before anything runs.
  *Do:* in a copy of `orders_enriched.json`, (a) add an edge that closes a loop, (b) point an
  edge at a node id that does not exist, (c) add `"disabled": true` to the `data` of
  `filter_recent`.
  *Hint:* `etl validate <file>` touches nothing, so it is safe to run repeatedly.
  *Check:* (a) and (b) exit **2** and the message names the nodes involved. (c) succeeds with
  warnings naming what was dropped *downstream* of the disabled node, not just the node itself.

- [ ] **A2. Forward compatibility.**
  *Goal:* confirm unknown keys survive.
  *Do:* add `"reviewedBy": "you"` to a node's `data`, and `"owner": "team-x"` at the top level.
  *Check:* `etl validate` still passes. Then open the file in the desktop app, move a node, save,
  and confirm both keys are still there.

## Phase 2 — SQL lowering

- [ ] **A3. Read the SQL before trusting it.**
  *Goal:* understand what a pipeline actually becomes.
  *Do:* `etl plan samples\pipelines\orders_enriched.json --script`
  *Hint:* every non-sink node is a `CREATE VIEW`.
  *Check:* answer in one sentence each. Which statement does the real work? Why would a per-stage
  timing here be misleading? (Compare with *Phase 2* in [learnings.md](learnings.md).)

- [ ] **A4. Exit codes.**
  *Goal:* know what a script calling `etl` can rely on.
  *Do:* produce exit 0, 1, 2 and 3 on purpose. For 3, point a source's `path` at a file that
  does not exist.
  *Hint:* read `$LASTEXITCODE` straight after the command, **not through a pipe**. A pipe gives
  you the last command's code (the Phase 9 mistake).
  *Check:* four commands, four codes, matching `etl --help`.

## Phase 3–4 — Components

- [ ] **A5. Find a component's contract.**
  *Do:* `etl components --manifest` and find `xf.pivot`.
  *Check:* explain why its `values` property is **required**, when no other pivot UI asks for it.
  (The answer is in the tracker, under *From Phase 4*.)

- [ ] **A6. 🦀 Add a component.**
  *Goal:* follow [adding_a_component.md](adding_a_component.md) end to end.
  *Do:* add `xf.head` — the first *n* rows, like `xf.limit` but with a different name and a
  default `n` of 10.
  *Check:* `cargo test --workspace` fails in exactly one place (the registry inventory) until
  you update it, then passes. `etl components` lists one more than before (59, with the 58 there
  are after Phase 10b), and the desktop palette shows it
  **with no frontend change**.

## Phase 5 — Parameters, contexts, secrets

- [ ] **A7. Same file, two places.**
  *Do:*
  ```powershell
  etl run samples\pipelines\orders_by_context.json --contexts samples\contexts.json
  etl run samples\pipelines\orders_by_context.json --contexts samples\contexts.json --context prod
  ```
  *Check:* the output lands in `samples/out/dev` and then `samples/out/prod`, and the pipeline
  file was not edited. Then try `--context prd` and explain why it is an error rather than a
  fallback.

- [ ] **A8. Precedence.**
  *Do:* run `csv_to_parquet.json` with no `--param since=...`, then with one.
  *Check:* write down the order in which a value is looked for, without looking it up. Then
  verify it against *Pipelines are portable* in the tracker.

- [ ] **A9. Secrets are masked.**
  *Do:* `etl secret init` and `etl secret set demo_pw --stdin`. Put `${SECRET:demo_pw}` into a
  property of a scratch pipeline, then run `etl plan <file> --script`.
  *Check:* the value appears as `********`, and `.etl/secrets.json` contains no plaintext.
  Explain why renaming the entry inside `secrets.json` breaks decryption.

## Phase 6 — Quality and control flow

- [ ] **A10. Dead letters.**
  *Do:* run `orders_checked.json`, then open the two small CSVs it writes.
  *Check:* 10 + 2 = 12 and 9 + 1 = 10. Then remove the edge from a quality node's `rejected` port
  and run again. Where did those rows go?

- [ ] **A11. The second transport.**
  *Do:* add `"policy": { "retryAttempts": 1 }` to the `data` of `filter_recent` in a copy of
  `orders_enriched.json`, and run it.
  *Check:* the unmodified sample prints no timings at all; the copy prints one beside the sink.
  Say why one policy on one transform changes that. Then open the copy in the desktop app: the
  Plan tab names the transport as **session** and says which stage asked for it. (`etl plan` in
  the terminal does not show the transport; only the Plan tab does.)

## Phase 8 — Runner, scheduler, console

- [ ] **A12. Incremental.**
  *Do:* run `orders_incremental.json` twice, then run `etl state list`, then
  `etl state forget orders_incremental --node read_orders`, then run it again.
  *Check:* 12 rows, then 0, then 12. Explain why the comparison is `>` and not `>=`.

- [ ] **A13. History and lineage.**
  *Do:* `etl runs list --limit 5`, then `etl runs show <id>`, then
  `etl lineage samples\pipelines\orders_checked.json`.
  *Check:* find the `[rejected]` marking in the lineage. Explain why `columns` is *absent* in
  `--json` rather than `[]`.

- [ ] **A14. The console's two roles.**
  *Do:* `etl serve --schedules samples\schedules.json --contexts samples\contexts.json`, then
  open the viewer link and the operator link.
  *Check:* only the operator sees a Run button. Then run
  `curl "http://127.0.0.1:8087/api/pipelines?token=<viewer token>"` and explain why it is
  refused even though the token is correct. Stop the console with Ctrl-C.

## Phase 9 — Standalone export

- [ ] **A15. Bake and move it.**
  *Do:* `etl build samples\pipelines\orders_checked.json -o samples\out\scratch\checked.exe`, then copy
  `checked.exe` to a directory **outside** the repo that holds a `samples\data\orders.csv`, and
  run it from there.
  *Check:* it runs with no DuckDB installed and reads the *new* directory's data.
  `checked.exe --info` shows what is inside. Explain why `${workspace}` must not be resolved at
  build time (the Phase 9c bug).

- [ ] **A16. The secret refusal.**
  *Do:* try `etl build` on the pipeline from A9, first without `--allow-secrets`, then with it.
  Then search the built file for the secret's value.
  *Check:* the first attempt is refused. After the second, the plaintext **is** in the file,
  and `--info` says so.

- [ ] **A17. Read the first CI run.**
  *Do:* `gh auth login`, then `gh run list --limit 5` and `gh run view <id>`.
  *Check:* for each of the jobs (`gate` × 2, `artifact` × 2, `cross-build-script`,
  `frontend`), write down pass or fail. For any failure, find the first error line with
  `gh run view <id> --log-failed`. Before fixing anything, predict which local check *should*
  have caught it, and why it did not.

## Phase 10a — Native components

- [ ] **A18. Watch the bridge.**
  *Goal:* see the staging file that sits between a connector and DuckDB.
  *Do:* `etl plan samples\pipelines\orders_xml.json`, then run it. While the plan is in front
  of you, find the path the XML source's view reads from.
  *Check:* the view reads `.etl/tmp/native/read_orders.jsonl`, and after the run that
  directory is empty. Explain why the file is gone, and why it is named after the node id and
  not after the pipeline.

- [ ] **A19. Break the XML on purpose.**
  *Do:* copy `samples/data/orders.xml` to `samples/out/scratch/`, point a copy of the pipeline
  at it, and make three separate changes: nest a `<city>` inside `<customer_id>`; give one
  order two `<status>` elements; put the word `loose` directly inside an `<order>`.
  *Check:* each run fails naming the element and a byte position. Say for each one why
  refusing is better than guessing, and what a user should do instead.

- [ ] **A20. Types in and out.**
  *Do:* remove the `columns` block from a copy of `orders_xml.json`, and change the sink to a
  Parquet file. Then `DESCRIBE` the Parquet file with DuckDB.
  *Hint:* `.\tools\duckdb\duckdb.exe -c "DESCRIBE SELECT * FROM 'samples/out/x.parquet'"`.
  *Check:* `order_ts` is a `TIMESTAMP` and `amount` is text. Explain why the two differ, and
  why the committed sample declares `columns` anyway.

- [ ] **A21. Nothing half-delivered.**
  *Do:* add a second branch to a copy of `orders_xml.json` that fails (an `xf.filter` on a
  column that does not exist), put `"policy": {"continueOnFailure": true}` on it, and run.
  *Check:* the run ends failed (exit 3), `orders_2026.xml` is not written, and the report says
  `nothing delivered, because the run failed`. Which settled decision and which older rule does
  this match?

- [ ] **A22. 🦀 Write a connector.**
  *Goal:* follow the native section of [adding_a_component.md](adding_a_component.md).
  *Do:* add `src.file.lines`: one record per line of a text file, with a single column `line`,
  and optionally `skip_blank`.
  *Check:* the engine needs no edit beyond the inventory test; `etl components` lists 59; a
  pipeline reading this very file into CSV runs; and you have written its delivery semantics in
  [connectors.md](connectors.md).

## Phase 10b — REST

- [ ] **A23. Read the retry rules off the wire.**
  *Goal:* see which failures are retried, and how the wait grows.
  *Do:* run `cargo test -p etl-connectors rest:: -- --nocapture` and read
  `a_5xx_is_retried_with_backoff`, `a_4xx_other_than_429_is_not_retried_and_says_why` and
  `a_retry_after_too_long_to_honour_is_an_error_not_a_hang`.
  *Check:* explain, in one sentence each, why a 503 is retried, why a 401 is not, and why a
  `Retry-After: 86400` is an error rather than a wait.

- [ ] **A24. A real API, read-only.**
  *Do:* copy `samples/pipelines/rest_orders.json` to `samples/out/scratch/`, and change the
  source to read `https://api.github.com/repos/duckdb/duckdb/releases` with
  `"query": {"per_page": "5"}`, no auth, `records` empty, `pagination` = `link`,
  `"max_pages": 2`, and `columns` of `tag_name` and `published_at`. Replace the filter and the
  REST sink with one `snk.file.csv`.
  *Check:* the run fails with `reached max_pages (2)`. Explain why that is the behaviour you
  want, then set `max_pages` so it succeeds and count the releases.

- [ ] **A25. Where the credential goes, and where it does not.**
  *Do:* `etl secret set api_token --stdin`, then run `etl plan` and `etl lineage` on your copy
  from A24 after adding `"auth": "bearer", "token": "${SECRET:api_token}"`.
  *Check:* the token appears in neither output. Put `?api_key=x` on the end of the URL and run
  `etl lineage` again. Explain why the query string is dropped there.

- [ ] **A26. At-least-once, in your own words.**
  *Do:* read *Delivery semantics* for `snk.saas.rest` in [connectors.md](connectors.md).
  *Check:* describe a failure where a batch arrives twice, and one where a row is never sent.
  For each, say what the report tells you and what you would do next.

- [ ] **A27. 🦀 A sixth pagination style.**
  *Do:* add `pagination = "token_header"`: the next page's token arrives in a response header
  named by `token_header` and is sent back in a query parameter.
  *Hint:* `Reply` would need the header; `Pagination::advance` decides what comes next.
  *Check:* a fixture test for it, `check` refusing it without `token_header`, and
  [connectors.md](connectors.md)'s table updated.

## Phase 10c — Verification against real systems

- [ ] **A28. Start the servers, run the suite.**
  *Do:* start Docker, run `./scripts/test-services.ps1`, set the three variables it prints,
  then `cargo test -p etl-duckdb-engine --test verified -- --nocapture`.
  *Check:* 9 pass and nothing says `skipping`. Run it again without the variables and
  explain why it still says 9 passed.

- [ ] **A29. Read an Iceberg table as it was.**
  *Do:* write a pipeline reading
  `crates/duckdb-engine/tests/fixtures/lake/orders_iceberg` with `allow_moved_paths` and
  `version` set to the `00001-…` metadata file's name (without `.metadata.json`), into CSV.
  *Check:* 7 rows. Set `version` to the `00002-…` file and get 12. Then point `path` at the
  metadata file itself and read the refusal. Why is that combination refused?

- [ ] **A30. Two S3 accounts.**
  *Do:* with the servers up, write a pipeline with two `src.cloud.s3` nodes on two different
  buckets and `etl plan` it.
  *Check:* two `CREATE OR REPLACE SECRET` statements, each `SCOPE`d to its own bucket. Explain
  what would go wrong if two nodes gave different credentials for the *same* bucket.

- [ ] **A31. The MySQL bug, by hand.**
  *Do:* in DuckDB, `ATTACH` the test MySQL, make a view over a table, and `SELECT count(*)`
  from the view. Then `SET mysql_aggregate_pushdown_enabled=false` and try again.
  *Check:* the first fails with an internal error and the second answers. Explain what the
  setting trades away.

## Phase 10d — SaaS GraphQL

- [ ] **A32. A 200 that is a failure.**
  *Do:* run `samples/pipelines/graphql_orders.json`'s end-to-end test, then change the
  fixture in `crates/duckdb-engine/tests/native.rs` to answer the *second* page with
  `{"data": {...}, "errors": [...]}`.
  *Check:* the run fails naming `page 2`, and nothing reaches the mutation. Explain why the
  first page's rows are not loaded either.

- [ ] **A33. Read a real API.**
  *Do:* write a pipeline with `src.saas.graphql` against `https://countries.trevorblades.com/graphql`
  reading `continents { code name countries { code } }` into Parquet.
  *Check:* 7 rows. Then look at the type of `countries` in the Parquet file and explain how a
  nested list of objects arrived as a DuckDB `LIST` of `STRUCT`.

- [ ] **A34. Throttling, by hand.**
  *Do:* in `graphql/tests.rs`, write a test where the first answer is
  `{"errors": [{"message": "slow", "extensions": {"code": "THROTTLED"}}]}` with
  `Retry-After: 1`, and the second is a page.
  *Hint:* `rate_limited_by_type_is_retried_and_retry_after_is_honoured` is nearly this.
  *Check:* it passes and takes about a second. Set `retry_codes` to `[]` and explain the
  failure you get instead.

- [ ] **A35. 🦀 `userErrors`.**
  *Do:* Shopify-style mutations report row failures in `data.<mutation>.userErrors` rather
  than in `errors`. Add an optional `user_errors` property to `snk.saas.graphql`: a JSON
  pointer, and a non-empty array there fails the batch.
  *Hint:* the sink's `judge` call is where a reply is accepted; it would need the pointer.
  *Check:* a fixture test for each of: empty array (success), non-empty (fails with the
  messages), pointer unset (unchanged). Update [connectors.md](connectors.md)'s *Not inspected*.

## Phase 10e — Checkpoints and Kafka

- [ ] **A36. Watch a position move.**
  *Do:* start the servers, make the topic and produce the twelve orders as in the tracker's
  session log, then run `samples/pipelines/kafka_orders.json` twice and `etl state list`.
  *Check:* 12 then 0 rows, and the listing shows one offset per partition summing to 12. Run
  `etl state forget kafka_orders --node read_orders` and explain what the next run reads.

- [ ] **A37. A failed run reads again.**
  *Do:* copy the sample, break its filter (`no_such_column > 1`), run it, fix it, run again.
  *Check:* the broken run saves no position (`etl state list`), and the fixed run reads all
  twelve. Which two pieces of code make sure of that, and why is there more than one?

- [ ] **A38. Make a gap.**
  *Do:* read part of a topic with `max_records`, then delete its first records with
  `kafka-delete-records.sh` in the container, then run again.
  *Check:* the run fails, names the offsets and counts them. Explain why failing is better
  here than carrying on from what is left.

- [ ] **A39. 🦀 Read the headers.**
  *Do:* Kafka records carry headers, which the source ignores. Add them as a `_headers` column
  (a JSON object of name to text, or base64 when not UTF-8).
  *Hint:* `row` builds each record; `rskafka::record::Record::headers` is a `BTreeMap`. Keep
  `METADATA_COLUMNS` and the clash check in step.
  *Check:* a broker test producing a header and reading it back, and `connectors.md` updated.

## Phase 10f — Kafka sink and security

- [ ] **A40. Same key, same partition.**
  *Do:* create a six-partition topic, write the same keys through `etl` (`snk.stream.kafka`
  with `key_column`) and through Kafka's console producer (`--property parse.key=true`), then
  consume both with `--property print.partition=true --property print.key=true`.
  *Check:* every key is on the same partition in both. Change one key's case and explain why it
  moves.

- [ ] **A41. Sign in three ways.**
  *Do:* with the services up, read the sample topic over `sasl_plaintext` with each of `plain`,
  `scram-sha-256` and `scram-sha-512`, then over `sasl_ssl` with `ca_cert` set to
  `target/test-services/kafka-ca.pem`. Keep the password in `etl secret`.
  *Check:* all four read. Then remove `ca_cert` and read the error; then set a wrong password
  and read that one. Which one would a user see if their cluster's CA were private?

- [ ] **A42. 🦀 Headers on the way out.**
  *Do:* add a `headers_column` to the sink: a column holding a JSON object of name to text,
  sent as the record's headers.
  *Hint:* `assign` builds each `rskafka::record::Record`; its `headers` is a `BTreeMap<String,
  Vec<u8>>`. Pair it with A39 (headers on the way in) and round-trip them.
  *Check:* a broker test writing and reading headers back, and `connectors.md` updated.

## Phase 10g — NATS JetStream

- [ ] **A43. A filter and its position.**
  *Do:* with the services up, make a stream capturing `shop.>`, publish to `shop.eu` and
  `shop.us` alternately, and read it with `filter_subject: shop.eu` twice.
  *Check:* the first run reads only `shop.eu` messages, and `etl state list` shows `next` past
  the stream's last sequence. Explain why it is not just past the last `shop.eu` message.

- [ ] **A44. A re-run without copies.**
  *Do:* run `samples/pipelines/nats_orders.json`, then `etl state forget nats_orders --node
  read_orders`, and run it again.
  *Check:* the second run reads all twelve again, but the large-orders stream does not grow,
  and the report says how many JetStream dropped. What would happen after the stream's
  duplicate window had passed?

- [ ] **A45. 🦀 A durable consumer, as an option.**
  *Do:* sketch (in prose, then in code if you like) a `consumer: durable` option that
  acknowledges messages only after a successful run, instead of saving a sequence.
  *Hint:* the hard part is that the connection must stay open until the engine says the run
  succeeded; today `read` returns before DuckDB starts. What would the SDK need?
  *Check:* a written answer to "what does the engine have to call, and when", compared with
  Settled decision 38.
