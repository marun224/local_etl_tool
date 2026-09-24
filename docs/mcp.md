# `etl mcp` — the workspace over MCP

Added in Phase 11a (2026-09-24). An agent such as Claude Code starts `etl mcp` as a
subprocess and talks to it over stdin and stdout (the Model Context Protocol). Through it the
agent finds components, writes a pipeline, checks it, runs it, reads what happened and builds
an executable, with the same engine, history and secrets `etl` uses. **Nothing listens on a
port** (Settled decision 92).

## Connecting Claude Code

Add the server to the project's `.mcp.json` (or with `claude mcp add`), pointing at the
workspace:

```json
{
  "mcpServers": {
    "etl": {
      "command": "E:/workspace_09212026/ETL_Local_Tool/target/debug/etl.exe",
      "args": ["mcp", "--workspace", "."]
    }
  }
}
```

`etl mcp` takes the options every pipeline command takes: `--workspace`, `--context`,
`--param name=value` (bound for every call), `--contexts`, and `run`'s `--duckdb` and
`--no-counts`. It says which workspace it serves on stderr, and nothing else: stdout is the
protocol's.

## The tools

| Tool | What it does | Changes anything |
|---|---|---|
| `list_components` | Every component, or one namespace's: id, label, required properties | no |
| `get_component` | One component in full: properties, types, defaults, help, handles | no |
| `get_schema` | The JSON Schema every pipeline document must match (below) | no |
| `list_pipelines` | The workspace's pipelines: name, path, stages, why one does not compile | no |
| `validate_pipeline` | Check a document, or a file, without running or writing anything | no |
| `create_pipeline` | Check a document, then write it; an invalid one is not written, an existing file is kept unless `overwrite` | writes a `.json` |
| `plan_pipeline` | The stages in order and the SQL each runs, secrets masked | no |
| `get_lineage` | Where the data comes from and goes, node by node | no |
| `run_pipeline` | Run and wait; the run's record | runs it |
| `list_runs` | Recent runs, newest first | no |
| `get_run_log` | One run in full: rows and time per stage, watermarks, failures | no |
| `build_executable` | Bake a pipeline into one executable, this platform or another | writes the file |
| `list_connections` | Contexts (variable names) and secrets (names), never a secret's value | no |

Pipeline tools take `params` (name to value) and `context`, as `etl run` takes `--param` and
`--context`.

**The agent's own permission prompts are the gate** (decision 93): Claude Code asks before
each tool call unless told not to.

## What it will not do

- **Leave the workspace.** Every path is read relative to the workspace, and one that leads
  outside it (`../`, or an absolute path elsewhere) is refused.
- **Hand over a secret.** `list_connections` names secrets and shows how to refer to one
  (`${SECRET:name}`); nothing returns a value. Plans, lineage, errors and run records are
  masked (`********`). **`build_executable` refuses a pipeline that resolves a secret**,
  because baking it would write the secret into the file and no one over MCP would see the
  warning; `etl build --allow-secrets` is the way to do that on purpose.
- **Treat a failure as a crash.** An invalid document, a run that failed, a run id that does
  not exist: each comes back as a result marked as an error, which the agent reads and acts
  on.
- **Run two pipelines at once.** Runs are one at a time, as the console's are, so two never
  race on a watermark.

## The schema

`get_schema` returns a JSON Schema (draft 2020-12) generated from the component registry
(`etl_metadata::schema`): each node is tied to one component, its `componentId` a constant,
its `type` the canvas kind (`source`, `transform` or `sink`), and its `properties` exactly
that component's, typed, with required ones required. A value may be a `${...}` reference
instead, since documents are checked before parameters are resolved. It is stricter than the
engine, which only warns about an unknown property: it describes what should be written.
Phase 11b constrains a local model with the same schema (decision 96).
