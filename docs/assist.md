# `etl assist` — a pipeline from words, on this machine

Added in Phase 11b (2026-09-25). `etl assist "<request>"` asks a small coding model running
**on this machine** for a pipeline document, checks it as `etl validate` would, and prints it
or writes it. Nothing leaves the machine: the model runs under llama.cpp's `llama-server`,
started for the one request on a loopback port and stopped after (Settled decision 95).

```bash
./scripts/fetch-model.ps1                     # once: llama-server and the model into tools/
etl assist "read this Postgres table, dedupe, write Parquet"              # to stdout
etl assist "read orders.csv, keep 2026's orders, write JSON" -o pipelines/recent.json
etl validate pipelines/recent.json            # it already passed this before it was written
```

## What it needs

`scripts/fetch-model.ps1` fetches, into the git-ignored `tools/`:

| What | Where | Pinned to |
|---|---|---|
| llama.cpp's `llama-server` (CPU build) and its libraries | `tools/llama/` | release `b11173` |
| Qwen2.5-Coder-1.5B-Instruct, Q4_K_M | `tools/models/qwen2.5-coder-1.5b-instruct-q4_k_m.gguf` (1.04 GB) | Hugging Face revision `f86cb2c`, SHA256 checked |

**The model is a setting**: `--model FILE` or `ETL_ASSIST_MODEL` runs any GGUF chat model;
`--llama-server FILE` or `ETL_LLAMA_SERVER` another server. A path named either way must
exist; it is never quietly swapped for the vendored one. Without either, `tools/` is looked
for above the workspace, then above the `etl` being run.

## Options

| Option | |
|---|---|
| `-o, --out FILE` | Write the pipeline here (folders made) rather than to stdout |
| `--overwrite` | Replace `--out` if it exists; otherwise an existing file is kept |
| `--model FILE`, `--llama-server FILE` | As above |
| `--seed N` | The sampling seed, printed with every result, so an answer can be had again |
| `--workspace`, `--context`, `--param` | As every pipeline command takes them; used to check the result |

Exit codes as the other commands: 0 written, 1 usage (no model, an existing `--out`), 2 the
model's pipeline does not validate (the errors and what it wrote go to stderr, and nothing is
written), 3 the model or server failed (its log is named).

## How it works

1. **Pick the components** the request's words point at (`crates/assistant/src/pick.rs`): a
   word counts most in a component's id (`postgres` in `src.db.postgres`), less in its label,
   least in its description; a few synonyms (`duplicates` → `dedup`, `pg` → `postgres`). At
   most eight, always a source and a sink, never control flow or code.
2. **The prompt** describes those components and their properties, and shows one example.
3. **The grammar**: the request carries the manifest's JSON Schema (the one MCP's
   `get_schema` returns, `etl_metadata::schema`), narrowed to the picked components, as
   `response_format.json_schema`. `llama-server` turns it into a GBNF grammar, so every
   token keeps the output a document of that shape: only those components, only their
   properties, each value of its type (decision 96).
4. **The check**: an optional property written as empty text is dropped (the engine would
   read `""` as a value), then the document is resolved and compiled as `etl validate` does.
   Only a valid pipeline is printed or written.

The grammar makes the output well formed; the check makes it valid (edges between real
nodes, values the component accepts). Values the request did not give (a connection string,
a key column) are the model's placeholders: read the result before running it.

## In the desktop app (Phase 11c)

**Assistant** in the header opens the panel, in the inspector's place. Ask in words (Enter
asks, Shift+Enter is a new line); while the model writes, the panel counts the seconds and
offers **Cancel**. The answer appears **on the canvas as a draft**, in place of the pipeline
and under a banner, its nodes dashed:

- **Accept** makes the draft the pipeline, unsaved, as any edit is.
- **Discard** brings the pipeline back exactly as it was, unsaved changes and all.
- **Try again** asks the same request with a new seed.

An invalid draft is shown too, with the canvas's usual red box on the node at fault, and can
be accepted and fixed by hand (decision 103). Edits made while a draft is shown change the
draft. **Save, Open and Run wait until the draft is accepted or discarded**; Plan and
Preview work on the draft.

The model starts with the first request and stays up while the app is open (decision 100),
so a second request takes about 25 seconds rather than 55; about 1.2 GB of memory while it
does. Cancel stops it, and the next request starts it again. Closing the app stops it. The
panel finds `tools/` as `etl assist` does, from the workspace (the directory the app was
started in) and then from beside the app.

## How well it does

On this machine (an i7-8665U, 4 cores, 16 GB, no GPU), 2026-09-25:

- **"read this Postgres table, dedupe, write Parquet" validated on the first try in 10 of 10
  runs, twice**: as `etl assist` ten times (about 55 seconds each, the server started every
  time), and as the test below with one server for all ten (3m47s, about 23 seconds each once
  the prompt is read). The plan asked for 9 of 10.
- Every run chose `src.db.postgres` → `xf.dedup` → `snk.file.parquet`, with two edges.
  Before blank options were dropped, 7 of those 10 wrote `"schema": ""`: valid to `validate`,
  broken SQL at run time. That is why the step exists.
- "read orders.csv, keep orders over 100, sort by amount, write JSON" gives CSV → filter
  (`amount > 100`) → sort → JSON. Before the picker learned that "over" means filter, the
  filter was not offered, and the model left it out: **a component the picker misses is one
  the model cannot use**. The picker is the part most worth improving.

The checks: `cargo test -p etl-assistant --test model` (skips without the model; about 4
minutes) and `cargo test -p etl-cli --test assist` (one real request, about a minute). CI
runs neither with a model; it checks the schema against every sample pipeline, broken
documents refused, and the picker and prompt (`crates/assistant/tests/schema.rs`).
