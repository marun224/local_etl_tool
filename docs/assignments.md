# Assignments

Hands-on exercises drawn from what each phase built. Each one has a goal, a hint, and a way to
check the answer. Mark one done by changing `[ ]` to `[x]` and adding the date.

Phases 0–9 were back-filled on 2026-09-23. **Every exercise below runs with the prebuilt
`target\debug\etl.exe`** unless it is marked 🦀, which means it needs the Rust toolchain. That
toolchain is not installed on this machine yet.

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
  you update it, then passes. `etl components` lists 55, and the desktop palette shows it
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
