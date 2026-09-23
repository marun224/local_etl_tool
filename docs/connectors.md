# Native connectors

Components written in Rust, for data DuckDB cannot reach. This file is the half of each
connector's "done" that a test cannot show: **what it promises, and what it does not.** Read
it before relying on a connector for anything that matters.

How they work in general, and how to add one, is in
[adding_a_component.md](adding_a_component.md#native-components-written-in-rust). The design
is in [PLAN_duckle_parity.md](PLAN_duckle_parity.md) under *Phase 10: split and design*.

## What every native connector shares

These come from the bridge, not from any one connector, so they hold for all of them.

**A source runs before DuckDB.** It reads everything, writes it to
`.etl/tmp/native/<node_id>.jsonl`, and the node becomes an ordinary view over that file. So
a native source reads its **whole input on every run**, including in `preview`. The
`incremental` filter narrows what reaches the rest of the pipeline, but not what the
connector reads. No connector pushes a watermark into its own query yet; one that does will
say so in its section below. (For REST, a `query` parameter bound to `${since}` gets most of
the way by hand.)

**A sink delivers after DuckDB, and only after a run that succeeded.** DuckDB writes the rows
to the staging file, and the connector delivers them once the whole run has finished without
a failure. That includes `continueOnFailure`: a run that carried on past a failure still
failed, and **its native sinks deliver nothing**. The report says so, one line per sink. This
is the same rule watermarks follow.

**Two native sinks are two deliveries, not one transaction.** If a pipeline has two and the
second fails, the first has already delivered. The run fails, naming the stage that failed.

**Types.** Records cross as JSON, which carries strings, numbers, booleans and null.

- **Reading, no `columns`:** DuckDB infers the types, as it does for `src.file.json`. An ISO
  date or timestamp inside a string becomes `DATE` or `TIMESTAMP`, and everything else stays
  as the connector wrote it. Inference also normalises a timestamp's text (`T` becomes a
  space).
- **Reading, with `columns`:** exactly those columns, cast to those types. This is the way to
  be certain.
- **Writing:** DuckDB writes each value in its JSON form. A `DECIMAL(10,2)` of `72.40` arrives
  as the number `72.4`, and a timestamp as `"2026-01-04 10:05:00"`. To keep a particular text
  form, cast to `VARCHAR` before the sink.
- **Non-finite doubles** (`1/0`, `NaN`) are written by DuckDB as bare `Infinity`/`NaN`, which
  is not JSON. A native sink receiving one fails and says that is the likely cause. Filter
  or cast them first. (Known gap, recorded in the tracker.)

**Secrets.** A connector's error text is masked the same way DuckDB's is, because connectors
quote paths and URLs back just as `ATTACH` quotes connection strings.

**Scratch.** Staging files are deleted after the run on every path, success or failure.
They are named after the node id, so two runs of the same pipeline in the same workspace at
the same moment would share them. The scheduler never does that; a hand-run `etl run`
beside a scheduler is the known unguarded case, as it is for spills.

## `src.file.xml` and `snk.file.xml`

Added in Phase 10a (2026-09-23). Parser and writer: `quick-xml` 0.41, pure Rust.

### The shape

One element is one row, and `record` names it. The rest of the document is ignored, so the
rows can sit at any depth.

| XML | Column |
|---|---|
| `<order id="7">` | `@id` = `"7"` |
| `<amount>12.5</amount>` inside the record | `amount` = `"12.5"` |
| `<amount currency="EUR">` | `amount@currency` = `"EUR"` |
| `<note/>` or `<note></note>` | `note` = `""` (empty, not null) |
| no `<note>` at all | `note` absent, read as `NULL` |

Namespace prefixes are ignored for matching and naming: `<x:order>` matches `record = order`.
Entities (`&amp;`, `&#65;`), CDATA, and attribute normalisation follow the XML 1.0
specification.

**Refused, by name and byte position, rather than guessed at:**

- an element inside a column (`<address><city>…`): only one level of children is read;
- the same child twice in one record (`<tag>a</tag><tag>b</tag>`): that is a list;
- text directly inside a record beside its children: mixed content has no column to go in;
- an undeclared entity such as `&nbsp;`;
- a document that is not well-formed.

**Zero matching elements is not an error.** It reads 0 rows, and the report says
`no <order> elements in …`, because a misspelt `record` looks exactly like an empty feed.

### Delivery semantics

**Reading** is a snapshot of the file when the source runs. A file being rewritten at that
moment can be read half-old, half-new; the source has no way to know. Use a file-watch
schedule, which waits for a file to stop changing before it fires.

**Writing** is all-or-nothing per file. The document is written to `<path>.partial` beside
the target and renamed into place, so a failure partway (a bad column name, a disk error)
leaves the previous file exactly as it was, with no `.partial` left behind. `mode` is
`overwrite` (the default) or `error_if_exists`. The second is checked before DuckDB starts,
as it is for every file sink, so a refusal writes nothing at all.

**The writer is the reader in reverse.** A column `@x` becomes an attribute of the record, and
`child@x` an attribute of that child. A null leaves the element out. A list or struct value,
or a column name that is not a valid XML name, is refused by name; `xf.rename` first. A flat
document read and written again is byte-identical. There is an end-to-end test that goes CSV
to XML, then XML to XML through DuckDB, and compares the bytes.

**Layout.** UTF-8, two-space indentation, `\n` line ends on every platform, so the same rows
give the same bytes wherever they are written.

## `src.saas.rest` and `snk.saas.rest`

Added in Phase 10b (2026-09-23). HTTP is `ureq` 3.2 (blocking, no async runtime) with
`rustls`, `ring` (Settled decision 16) and bundled `webpki-roots` certificates, so a built
artifact carries its own trust store. Proxies come from `ALL_PROXY`, `HTTPS_PROXY` or
`HTTP_PROXY`, and `NO_PROXY` is honoured (ureq's default, checked in its source).

### Authentication

`auth` is `none`, `bearer` (`Authorization: Bearer <token>`), `header` (`<auth_header>:
<token>`, default `X-API-Key`), or `basic` (`username`, `password`). **Put the credential in a
secret** (`"token": "${SECRET:api_token}"`), never in the document. It is masked in every
error, including an API's own error text if it quotes the credential back, and `etl build`
refuses to bake it without `--allow-secrets`. Lineage shows the endpoint without
`user:pass@` or a query string, because either can hold a credential.

### Reading

`records` is a JSON pointer to the array of rows in each response (`/data`); empty means the
response *is* the array. Every row must be a JSON object. Field values keep their JSON types,
and nested objects and arrays reach DuckDB as `STRUCT` and `LIST`. Declare `columns` to take
only what you need, typed.

| `pagination` | Asks for | Stops when |
|---|---|---|
| `none` | one request | after it |
| `page` | `page_param` = `page_start`, +1 each time (and `size_param` = `page_size` if both set) | a page is empty, or shorter than `page_size` |
| `offset` | `offset_param` = rows so far, `size_param` (default `limit`) = `page_size` (default 100) | a page is shorter than `page_size` |
| `cursor` | `cursor_param` = the value at `cursor_path` in the previous response | that value is missing, null or empty |
| `link` | the `rel="next"` URL in the `Link` header | there is none |

**`max_pages` (default 1000) is an error when reached, not a quiet stop.** A load that stopped
at the cap would look complete. A cursor that comes back unchanged is refused at once, rather
than 1,000 requests later.

### Retries and pacing

- **Retried:** HTTP 429, any 5xx, and failures to connect or read. `retries` (default 3) extra
  attempts, waiting `retry_backoff_ms` (default 500) and doubling each time. A `Retry-After`
  given in seconds replaces the backoff. One over 300 seconds is refused with an error
  rather than obeyed, because a run that sleeps for an hour looks hung.
- **Never retried:** any other 4xx. A 401 or a 400 will not get better by being sent again.
  The error quotes the start of the response body, since that is usually the explanation.
- `timeout_ms` (default 30,000) limits each request; `min_interval_ms` spaces requests out
  for rate-limited APIs.

### Delivery semantics

**The source is a snapshot per run, and not transactional.** Pages are read one after
another, and nothing stops the API changing between them: a row inserted before page 3 is
read may shift page 4, which can show one row twice or skip one. Offset and page pagination
suffer from this most; cursor and link pagination, where the API holds the position, least.
If exactly-once matters, deduplicate downstream (`xf.dedup`) or read a snapshot endpoint.
Like every native source, it reads everything on every run; see *What every native connector
shares*.

**The sink is at-least-once per batch.** Rows go in `batch_size` requests (default 100): one
JSON object each for a batch size of 1, otherwise a JSON array, including a last batch of one,
so the shape never depends on the count. `wrap` nests the body under a key. Any 2xx is
success. **A failure partway leaves the earlier batches delivered**, and the error says how
many: `batch 3 failed after 2 batch(es) (200 record(s)) were delivered`. A retried batch can
arrive twice if the API processed it and then failed to answer, so an API with idempotency
keys or upsert semantics is the safe target. A sink never uses GET: a GET has no body, and
one would deliver nothing while reporting success.
