# Native connectors

Components written in Rust, for data DuckDB cannot reach. This file is the half of each
connector's "done" that a test cannot show: **what it promises, and what it does not.** Read
it before relying on a connector for anything that matters.

How they work in general, and how to add one, is in
[adding_a_component.md](adding_a_component.md#native-components-written-in-rust). The design
is in [PLAN_duckle_parity.md](PLAN_duckle_parity.md) under *Phase 10: split and design*.

## What every native connector shares

These come from the bridge, not from any one connector, so they hold for all of them.

**A source runs before DuckDB.** It reads, writes what it read to
`.etl/tmp/native/<node_id>.jsonl`, and the node becomes an ordinary view over that file. Most
native sources read their **whole input on every run**, including in `preview`; the
`incremental` filter narrows what reaches the rest of the pipeline, but not what the
connector reads. (For REST, a `query` parameter bound to `${since}` gets most of the way by
hand.)

**A streaming source keeps a position instead** (Phase 10e; Kafka is the first). It is handed
where the last *successful* run stopped, reads on from there, and hands back where it got to.
That **checkpoint** is saved in `.etl/state/<pipeline>.json` beside the watermarks, in the
same atomic write, and **only when the whole run succeeds**: a failed run, including one that
carried on under `continueOnFailure`, saves no position, so the next run reads the same
records again. `etl state list` shows positions, and `etl state forget <pipeline> --node
<id>` makes a node start over. A built artifact keeps the same file in the directory it runs
in (or `--workspace`). `preview` reads from the saved position and saves nothing.

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

## `src.saas.graphql` and `snk.saas.graphql`

Added in Phase 10d (2026-09-23). They share REST's HTTP layer (`crates/connectors/src/http.rs`),
so **authentication, retries, pacing, timeouts and proxies are exactly as described for
REST above**, and so is the masking of credentials. Every request is a `POST` of
`{"query": ..., "variables": ...}` as JSON; there is no `method` property.

### The rule GraphQL adds: a 200 can be a failure

A GraphQL server answers a bad field, a permission problem or a throttle with **HTTP 200 and
an `errors` array**, sometimes beside partial `data`. So after every 2xx:

- **Any error fails the read or the batch, even when `data` came back.** Loading the rows
  around a hole as if they were whole is the partial load `max_pages` exists to prevent
  (Settled decision 20). The message quotes up to three errors with their code and `path`:
  `page 1: the API answered with 1 error(s): "Cannot query field "nosuch" on type
  "Country"." [GRAPHQL_VALIDATION_FAILED]`.
- **Except throttling** (Settled decision 21). When *every* error's `extensions.code` or `type`
  is in `retry_codes` (default `THROTTLED`, Shopify's, and `RATE_LIMITED`, GitHub's), the
  request is retried as a 429 would be: the same backoff, the same `retries` budget, and a
  `Retry-After` header honoured if the server sends one. Throttling mixed with a real error is
  a failure. `retry_codes: []` turns this off.
- `data` null or missing with no `errors` is a failure too.

A non-2xx status is handled by the shared layer exactly as for REST: 429 and 5xx retried,
other 4xx not.

### Reading

`query` is the query and `variables` a JSON object sent with it (`${...}` parameters work
inside it). `records` is a JSON pointer to the rows, e.g. `/data/orders/nodes`; it is
required, because a GraphQL response is never itself the array. Rows must be objects: point
at `nodes` rather than `edges` where the API has both, or each row arrives as one `node`
struct column.

| `pagination` | Sends | Stops when |
|---|---|---|
| `none` | the query once | after it |
| `relay` | `$after` (`cursor_variable`) = the last `pageInfo.endCursor`, null on the first page | `hasNextPage` is false |
| `offset` | `$offset` (`offset_variable`) = rows so far, `$limit` (`limit_variable`) = `page_size` (default 100) | a page is shorter than `page_size` |

For `relay`, `pageInfo` is looked for **beside the records** (`/data/orders/pageInfo` for
records at `/data/orders/nodes` or `/edges`); set `page_info` if the API keeps it elsewhere.
A missing `pageInfo`, a missing `hasNextPage`, or `hasNextPage` true with no `endCursor` is an
error naming what to ask for. A cursor that comes back unchanged is refused at once.
**`max_pages` (default 1000) is an error when reached**, as for REST.

**Checked before anything runs** (`etl validate`, the canvas): the query is not blank,
`variables` is a JSON object, `records` is a pointer, and the query mentions the variables its
pagination sends (`$after`, or `$offset` and `$limit`). `variables` may not set those
itself. The check is textual and deliberately loose: it catches a query that never mentions
the variable and cannot reject a valid one. The server is the authority on the rest.

### Delivery semantics

**The source is a snapshot per run, and not transactional**, as REST's: pages are read one
after another and the data can change between them. Relay cursors, which the server holds,
suffer from this least; offsets most. Like every native source, it reads everything on
every run.

**The sink is at-least-once per batch.** `mutation` runs once per `batch_size` rows (default
100), with the rows in `$rows` (`rows_variable`) **always as a list**, even a last batch of one,
because the variable is typed as a list in the mutation. `variables` are merged in beside it,
and may not set it. The mutation must mention `$rows`. **A batch whose reply has `errors`
fails the write**, and the error says how many batches had already landed:
`batch 3 failed after 2 batch(es) (200 record(s)) were delivered`. A retried batch can arrive
twice if the server applied it and then failed to answer.

**Not inspected:** mutations that report failure *inside* `data` rather than in `errors`, such
as Shopify's `userErrors`. A reply like that is a success as far as this sink can tell. Check
the target API's convention before relying on it.

Real TLS was checked by hand once, against the public countries API
(`countries.trevorblades.com`): 27 Oceania countries through a `$continent` variable, and an
unknown field reported as above. The suite itself never leaves 127.0.0.1.

## `src.stream.kafka`

Added in Phase 10e (2026-09-23). Client: `rskafka` 0.6, on a single-threaded `tokio` runtime
that exists only for the length of one read (Settled decision 26). Verified against Apache
Kafka 4.1 in KRaft mode. **Plaintext only for now**; TLS and SASL, and the sink, are Phase 10f.

### What it is, and what it is not

**It is not continuous streaming.** Each run is one **bounded micro-batch**:

1. When the run starts, it records each partition's latest offset. That is the end of this
   batch; anything that arrives while it reads belongs to the next run.
2. It reads from the saved position (or from `start` for a partition with none) up to those
   ends, or until `max_records` (default 100,000), whichever comes first. Partitions take
   turns, one fetch each, so a backlog in one does not starve the rest.
3. It saves where it got to, **only if the whole run succeeds**.

So how fresh the data is depends on how often the pipeline runs: schedule it every minute
and it is a minute behind. Reaching `max_records` is a normal stop, not an error, because
nothing is lost: the position is saved exactly where reading stopped, and the report says
roughly how many are left for the next run.

**The position is not a Kafka consumer group** (Settled decision 27). It lives in this
project's state file, so Kafka's own tools (`kafka-consumer-groups.sh`, lag dashboards) do not
see this pipeline, and two workspaces reading one topic are independent readers.

### Rows

| `value_format` | Row |
|---|---|
| `json` (default) | the value's JSON object, one column per field |
| `text` | a `value` column, the value as UTF-8 text |
| `bytes` | a `value` column, the value base64-encoded |

Every row also has `_topic`, `_partition`, `_offset`, `_timestamp` (UTC, milliseconds, as
`YYYY-MM-DD HH:MM:SS.mmm`) and `_key` (text, or base64 if the key is not UTF-8, or null).
Declare `columns` to type them; a declared `columns` map reads only the columns it names, so
include the underscore ones you want.

- **A value `json` cannot take fails the read**, naming the partition and offset: not JSON,
  not an object, or an object with a field named like an underscore column. A value that
  starts with a UTF-8 byte-order mark is not JSON either, and the error says so (Windows
  tools add one). Use `text` to read such values and unpack them downstream.
- **A tombstone** (a record with a key and no value) is a row with only the underscore
  columns under `json`, and a null `value` under `text` and `bytes`. A deletion is
  information, so it is kept.
- **Headers are not read** yet.

### Delivery semantics

**At-least-once into the pipeline.** The position is saved after the sinks have written. If a
run fails after its sinks wrote, or the save itself fails (the run then fails, saying so), the
next run reads those records again. Downstream, deduplicate on `_partition` and `_offset`, or
write to a target that upserts, if duplicates matter.

**Gaps are errors, not skips.** If retention (or `delete_records`) removed records this
pipeline never read, the next run **fails** and says which offsets and how many:
`partition 1: offsets 20 to 24 were deleted before this pipeline read them (5 record(s)
lost ...)`. Nothing is read. To carry on from what the topic still holds, run `etl state
forget` for the node. That restarts **every** partition of it from `start`, so partitions that
had no gap are read again too: deliberate, because choosing what to lose is a person's call.

**Also refused:** a saved position past the end of a partition (the topic was probably
deleted and made again), and a topic that does not exist; this connector never creates one.
A saved position for a different topic (the node's `topic` was edited) is set aside with a
note, and the new topic starts from `start`. A partition added since the last run starts
from `start`, with a note.

**Offsets that hold nothing readable** below the recorded end (transaction markers, records
compacted away) are stepped over, with a note, rather than asked for again forever.

**Two runs of one pipeline at once** read the same records, and the second to finish saves its
position over the first's. The scheduler never does this; a hand-run `etl run` beside a
running scheduler is the known unguarded case, as it is for watermarks.

**Timeouts.** `timeout_ms` (default 30,000) bounds each step, retries included: connecting,
listing the topic, and each fetch. `rskafka`'s own retries have no deadline, so without this a
mistyped broker address would hang a run; with it, the run fails naming what it was doing.

### A micro-batch into a file

Each run writes its own batch, so a file sink with `mode: overwrite` holds **only the latest
batch**, and a run that read nothing leaves an empty file. That is right for "the latest
changes", and wrong for "everything so far". For the second, write to a database sink that
appends, or put the date in the path (`${date}`). There is no per-run built-in for a file
name yet.
