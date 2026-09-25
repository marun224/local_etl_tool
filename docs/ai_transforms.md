# The `xf.ai.*` transforms

Phase 11d, in three parts (Settled decisions 106–115):

| Component | What | Built in | Needs |
|---|---|---|---|
| `xf.ai.chunk` | Split text into overlapping chunks | 11d1 (2026-09-25) | nothing: plain SQL |
| `xf.ai.redact` | Replace personal data found by its shape | 11d1 (2026-09-25) | nothing: plain SQL |
| `xf.ai.embed` | A vector for each text, from a local model | 11d2 (2026-09-25) | a 37 MB local model |
| `xf.ai.prompt`, `xf.ai.classify`, `xf.ai.extract` | Ask a model about each row | 11d3 (2026-09-25) | an OpenAI-compatible endpoint, or the local model |

`samples/pipelines/tickets_for_retrieval.json` runs the first two: support tickets in, personal
data out, chunks written to Parquet. `samples/pipelines/tickets_for_search.json` adds
`xf.ai.embed` after them, for a vector per chunk. `samples/pipelines/tickets_triaged.json`
labels each ticket, pulls facts out of it and summarises it, with the local model.

## `xf.ai.chunk`

| Property | Default | |
|---|---|---|
| `column` | (required) | The text to split |
| `size` | 1000 | Most characters in a chunk |
| `overlap` | 100 | Characters a chunk repeats from the one before; smaller than `size` |
| `output` | `chunk` | The chunk's column; its position is `<output>_index`, from 0 |

- **One row per chunk.** The text column is replaced by the chunk; every other column is kept,
  so a chunk still carries its row's id.
- **A chunk ends at the last whitespace before `size`**, so words are not cut, unless that would
  leave the chunk no longer than the overlap. A word longer than `size` is then cut where it
  must be, and the next chunk still moves on.
- **The overlap starts at a word** when the overlap holds a word boundary, so the repeated part
  is whole words. With a small overlap and long words it may start mid-word.
- **A row with no text** (null, empty or only whitespace) **gives no chunks**, and no chunk is
  ever empty.
- Measured in characters, not a model's tokens (decision 108). As a rough guide, English runs
  about four characters to a token.

It is one recursive SQL query, so it runs in DuckDB with the rest of the pipeline, on the
one-script path.

## `xf.ai.redact`

| Property | Default | |
|---|---|---|
| `columns` | (required) | The text columns to redact |
| `kinds` | all five | Any of `email`, `phone`, `credit_card`, `ssn`, `ip` |
| `replacement` | `token` | `token` writes `[EMAIL]`; `hash` writes `[EMAIL:<12 hex>]` |

**What it finds, by shape** (decision 107):

| Kind | Found | Not found |
|---|---|---|
| `email` | `name@domain.tld`, subdomains, `+tags` | obfuscated forms (`name at domain dot com`) |
| `ip` | IPv4; IPv6 written in full | compressed IPv6 (`2001:db8::1`) |
| `credit_card` | 13–19 digits, spaces or dashes allowed, **passing the Luhn check** | a number failing Luhn, such as an order number |
| `ssn` | US form `123-45-6789` | nine digits with no dashes |
| `phone` | 10–15 digits with at least one separator, `+` country codes, `(area)` codes | numbers without an area code; a date such as `2026-09-25`; `10 000 000` |

**Names and street addresses are not found.** Nothing here reads language, so it catches what
has a shape. For anything that must not leave, check a sample of the output.

- **The kinds are looked for in a fixed order** (email, IP, card, SSN, phone), whatever order
  they are listed in, so an IP address is not mistaken for three groups of a phone number.
- **`hash`** is the first 12 hex characters of SHA-256 over what identifies the value: the
  digits of a number, and the lower-cased address of an email. So `4111 1111 1111 1111` and
  `4111111111111111` hash alike, and redacted rows still join on it. **A hash is pseudonymous,
  not anonymous**: anyone holding a candidate value can hash it and compare.
- Null stays null; other columns are untouched; a redacted column becomes text.

## `xf.ai.embed`

| Property | Default | |
|---|---|---|
| `column` | (required) | The text to embed |
| `output` | `embedding` | The vector's column, `FLOAT[dimensions]` |
| `dimensions` | 384 | The model's vector length; change it only with another model |

**What it needs**: `./scripts/fetch-model.ps1` fetches **bge-small-en-v1.5** (Q8_0, 37 MB,
pinned and hash-checked, decision 109) into `tools/models/`, beside the `llama-server` that
`etl assist` uses. `ETL_EMBED_MODEL` points at another GGUF embedding model (then set
`dimensions` to its length). Nothing leaves the machine.

- **One vector per row**, in a `FLOAT[384]` column (decision 110), L2-normalised, so
  `array_cosine_similarity(a, b)` (or a dot product) compares two. A row with no text gets
  null. The other columns are untouched and keep their types.
- **At most 512 tokens a text**, about 2,000 characters of English; a longer one fails the
  stage with a message saying to split it with `xf.ai.chunk` first, which is what the sample
  does.
- **The model runs for the length of the stage**: started, texts sent 32 at a time, stopped.
  The four sample tickets' seven chunks take about 0.7 s, loading included.
- **Parquet has no fixed-length array**, so a vector written to Parquet reads back as a list
  (`FLOAT[]`). Cast it back to compare: `embedding::FLOAT[384]`.
- **`etl build` refuses a pipeline with `xf.ai.embed`** (decision 114): the model is a file on
  this machine, which a built executable does not carry. Run it with `etl run`, the scheduler,
  the console or the desktop app.
- Nearest neighbours with DuckDB alone:

  ```sql
  SELECT ticket_id, chunk,
         array_cosine_similarity(embedding::FLOAT[384], (SELECT embedding::FLOAT[384]
           FROM 'samples/out/ticket_vectors.parquet' WHERE ticket_id = 102 LIMIT 1)) AS score
  FROM 'samples/out/ticket_vectors.parquet' ORDER BY score DESC LIMIT 3;
  ```

  DuckDB's `vss` extension would index them for large tables; it is not vendored yet.

### How it runs: a native transform

`xf.ai.embed` is the first component that runs Rust **between** two DuckDB stages (native
sources run before DuckDB and native sinks after it). Its stage takes the driven path:

1. **The feed** numbers the input rows into a temp table and writes the row number and the
   text column to `.etl/tmp/native/<node>.in.jsonl`.
2. **The transform** reads that file and writes the row number and the vector to
   `.etl/tmp/native/<node>.jsonl`.
3. **The view** joins the vectors back onto the numbered rows (`LEFT JOIN`, in row order), so
   no other column makes the round trip through JSON.

`etl plan --script` shows all three. Preview runs them too.

## Asking a model: `xf.ai.prompt`, `xf.ai.classify`, `xf.ai.extract`

Each sends one chat request per row and adds what came back as columns. The same native
transform machinery as `xf.ai.embed`: only the columns they read cross into Rust, and the
rest keep their types.

| Component | Reads | Adds |
|---|---|---|
| `xf.ai.prompt` | the columns its `prompt` names, e.g. `Summarise: {body}` (`{{` `}}` for a brace) | `output` (default `answer`), text |
| `xf.ai.classify` | `column` | `output` (default `label`), always one of `labels` |
| `xf.ai.extract` | `column` | one column per `fields` entry: `text`, `integer`, `number`, `boolean` or `date` |

`xf.ai.prompt` also takes `system` and `max_tokens`; `xf.ai.classify` and `xf.ai.extract`
take `instructions` (a sentence on what the labels or fields mean) and `json_schema`.

### Where the rows go

| Property | Default | |
|---|---|---|
| `base_url` | (none) | An OpenAI-compatible API, e.g. `https://api.openai.com/v1`; `/chat/completions` is added |
| `model` | | The endpoint's model name; required with `base_url` |
| `api_key` | | Sent as a bearer token; write `${SECRET:name}`, never the key |

- **With `base_url`, each row's text leaves this machine** for that endpoint. The help on
  every one of the three says so. The endpoint's host shows in lineage; the key shows nowhere:
  not in the plan, the lineage, or a message (the engine masks a resolved secret everywhere).
- **Without `base_url`, the local model answers** (decision 112): the Qwen2.5-Coder-1.5B that
  `etl assist` runs, started for the stage with one slot per call at once, so all three work
  offline. It is small: on the sample tickets it labelled every ticket from the list and typed
  every field, but called a failing export "billing" and took a date for an order number. For
  judgement that matters, point `base_url` at a larger model.
- **`etl build`** builds a pipeline that names a `base_url` (the key must be a secret, and the
  build then needs `--allow-secrets`, as for any secret), and refuses one that uses the local
  model (decision 114).

### Guardrails (decision 113)

| Property | Default | |
|---|---|---|
| `max_rows` | 1000 | An input with more rows is **refused before any call**: each row is a call, and a call can cost money |
| `concurrency` | 4 | Calls at once; answers are written back in row order |
| `timeout_seconds` | 60 | For one call |
| `retries` | 2 | Extra attempts after a 429 (honouring `Retry-After`), a 5xx or a network failure; never after another 4xx |

**A call that still fails fails the stage**, naming the row (`row 3: ...`), and the calls not
yet made are not made. There is no partial result: rerun it once the endpoint is back.

### What the answer must be

- **`xf.ai.classify`'s answer is always one of `labels`.** With `json_schema` (the default)
  the request carries a JSON Schema whose `label` is an `enum` of them, so a model that honours
  `response_format` cannot answer anything else. Either way the answer is checked: a bare
  `Bug.` matches `bug`, and `banana` fails the row, saying what the model said.
- **`xf.ai.extract`'s fields are typed.** The schema asks for each field as its type or null;
  an answer is then read as that type where it can be (`"3"` as 3, `"yes"` as true, a
  `YYYY-MM-DD` date), and is null where it cannot. An answer that is not a JSON object at all
  fails the row. A code fence around it is allowed.
- **Switch `json_schema` off** for an endpoint that refuses `response_format`; the prompt then
  says what shape to answer in, and the check above still applies.
- Requests go out at `temperature` 0, so a rerun asks the same question the same way.
