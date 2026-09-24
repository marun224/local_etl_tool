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

**A queue source holds its messages instead** (Phase 10j; SQS is the first, Pub/Sub the
second, RabbitMQ the third). A queue keeps no
position to come back to: it hands a message out, hides it, and deletes it only when told to.
So a queue source saves nothing in `.etl/state/`; it **holds** what it received and hands the
engine a *receipt*, which the engine settles exactly once:

- **acknowledged** (the messages are deleted) after the run **fully succeeded and its native
  sinks delivered**, the moment a streaming source's position would be saved;
- **released** (the messages come back at once) on every other path: a failed stage, a
  `continueOnFailure` run with failures, a sink that failed, `preview`, or any error. A
  receipt nobody settled is released when it is dropped.

The report has a line for each (*"12 message(s) deleted from queue 'orders'"*). While the run
goes on, the hold is kept alive (see each connector), so a long run does not see its messages
handed to someone else. **If acknowledging fails after the sinks delivered**, the run still
succeeds and the report gets a **warning**, printed with ⚠ and kept in run history: those
messages will be delivered again. That is duplication, not loss. Delivery from a queue is
**at-least-once**: a message comes again after a failed run, a failed acknowledgement, or a
hold that ran out, and is never deleted before the run that read it succeeded.

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

## `src.stream.kafka` and `snk.stream.kafka`

The source was added in Phase 10e and the sink, TLS and SASL in 10f (both 2026-09-23).
Client: `rskafka` 0.6, on a single-threaded `tokio` runtime that exists only for the length of
one read or write (Settled decision 26). Verified against Apache Kafka 4.1 in KRaft mode, on
plaintext, SSL, SASL_PLAINTEXT and SASL_SSL listeners.

### Connecting

Both directions take the same connection properties.

| `security` | What it does | Also needs |
|---|---|---|
| `plaintext` (default) | nothing: for a broker on a trusted network | |
| `ssl` | encrypts, and checks the broker's certificate | `ca_cert` for a private CA |
| `sasl_plaintext` | signs in, unencrypted | `username`, `password` |
| `sasl_ssl` | both: what Confluent Cloud, MSK and Aiven expect | all of the above |

- **`sasl_mechanism`** is `plain` (default), `scram-sha-256` or `scram-sha-512`. OAUTHBEARER and
  Kerberos (GSSAPI) are not supported.
- **Put the password in a secret** (`"password": "${SECRET:kafka_password}"`). It is never in an
  error: a connection is described as `brokers (sasl_ssl, SCRAM-SHA-512 as 'etl')`, and every
  connector error is masked as well. `etl build` refuses to bake a secret without
  `--allow-secrets`.
- **`ca_cert`** is a PEM file of the certificate authority to trust, relative to the workspace.
  Unset, the bundled public roots are trusted (the same `webpki-roots` HTTPS uses), which is
  right for hosted Kafka and wrong for a private CA: that fails with `invalid peer
  certificate: UnknownIssuer`. Client certificates (mutual TLS) are not supported.
- **A setting that cannot work is refused before anything runs**, by `etl validate` and the
  canvas: SASL without a username or password, a username or password without SASL, or a
  `ca_cert` without TLS.
- **A failed sign-in says why.** `rskafka` retries a failed sign-in as if it were a network
  blip, until any timeout wins. So when connecting times out, the connector makes one more
  attempt with retries off and reports what it says, e.g. `Sasl handshake failed: API error:
  SaslAuthenticationFailed`. This takes about `timeout_ms` plus a few seconds.

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
listing the topic, each fetch and each send. A step that stalls is given five seconds more,
so that when retries run out their reason arrives rather than a bare "no answer".
`rskafka`'s own retries have no deadline, so without this a mistyped broker address would
hang a run; with it, the run fails naming what it was doing.

### Writing: `snk.stream.kafka`

Each row becomes **one record whose value is the row as a JSON object**, key column
included. Types go as DuckDB writes JSON (see *Types* above): a decimal as a number, a
timestamp as text.

- **`key_column`** names the column whose value is the record's key: text as UTF-8, a number or
  boolean as written, anything nested as its JSON. A null key, or no `key_column`, sends the
  row without one. A `key_column` the rows do not have fails the write, naming it.
- **Partitioning is Java's.** A keyed record goes to `murmur2(key) & 0x7fffffff` modulo the
  partition count, bit for bit as Kafka's Java producer does. Checked two ways: against the
  values Kafka's own test suite pins, and by writing 30 keys through `etl` and through Kafka's
  console producer into two six-partition topics, which put every key on the same partition.
  So a consumer that relies on "one key, one partition, in order" sees our records where it
  sees everyone else's. **Keyless rows** go to one partition per batch, taking turns across
  batches.
- **`batch_size`** (default 500) rows per batch. A batch goes to each of its partitions in turn,
  and each send waits for **every in-sync replica** to acknowledge (`acks=all`).
- **`compression`** is `none` (default), `gzip`, `snappy`, `lz4` or `zstd`, per batch. Each is
  verified by writing with it and reading back through the source.
- **The topic must exist.** This connector never creates one.

**Delivery is at-least-once per batch.** If a batch fails, the batches before it are
delivered, and **part of the failing batch may be too**, since it goes to each partition
separately. The error says so: `batch 3 failed after 2 batch(es) (4 record(s)) were
delivered, and part of batch 3 may have landed too: ...`. A send that timed out may also have
landed. So a re-run can write some records twice. There is no idempotent producer or
transaction, so if duplicates matter, key the records and deduplicate downstream, or read with
a consumer that tolerates them.

**Like every native sink, it delivers only after a run that fully succeeded** (see *What every
native connector shares*), and never in `preview`.

### A micro-batch into a file

Each run writes its own batch, so a file sink with `mode: overwrite` holds **only the latest
batch**, and a run that read nothing leaves an empty file. That is right for "the latest
changes", and wrong for "everything so far". For the second, write to a database sink that
appends, or put the date in the path (`${date}`). There is no per-run built-in for a file
name yet.

## `src.stream.nats` and `snk.stream.nats`

Added in Phase 10g (2026-09-23). Client: `async-nats` 0.50 (JetStream, `ring`, NKeys), on a
single-threaded `tokio` runtime that exists for one read or write (Settled decision 43).
Verified against NATS 2.11 servers: open, user and password, token, TLS, and operator mode
with a `.creds` file.

**JetStream only.** Core NATS keeps nothing: a message goes to whoever is listening at that
moment, so there is nothing to read back in a batch. The source reads a JetStream **stream**;
the sink publishes to a **subject** that a stream captures, and waits for JetStream's
acknowledgement.

### Connecting

- **`url`**: `nats://host:4222`, comma-separated for a cluster. `tls://` turns TLS on, as does
  `tls: true`.
- **`auth`**: `none`; `user_password` (`username`, `password`); `token` (`token`); `creds`
  (`creds_file`, a `.creds` file holding a JWT and an NKey seed, which is how Synadia Cloud and
  other operator-mode deployments sign in). Put passwords and tokens in secrets; a `.creds`
  file holds a secret key, so keep it out of version control.
- **TLS** trusts `ca_cert` if given and the bundled public roots if not, not the operating
  system's store (Settled decision 42), exactly as for Kafka. Client certificates are not
  supported.
- **A setting that would be ignored is refused** before anything runs: a token with
  `auth: none`, a password with `auth: token`, a `ca_cert` without TLS.
- **Errors say why and never quote a secret**: `connecting to nats://host:4222 (plaintext,
  user_password as 'etl'): authorization violation`, or `invalid peer certificate:
  UnknownIssuer`.

### Reading: `src.stream.nats`

The same bounded micro-batch as Kafka (see its section): when the run starts it records the
stream's last sequence, reads from the saved position up to it or `max_records`, and saves
where it got to **only if the whole run succeeds**. It reads through an **ephemeral ordered
consumer**: nothing is left on the server, no durable consumer exists, and NATS's own tools do
not show this pipeline as a consumer (Settled decision 38). `etl state forget` replays.

- **`filter_subject`** reads only matching subjects (`orders.eu.>`). The position still moves
  past the end of the stream, so messages that did not match are not looked at again.
  Changing the stream or the filter starts that node from `start`, with a note.
- **Rows** are Kafka's `value_format` (`json`, `text`, `bytes`, with the same refusals), plus
  `_stream`, `_subject`, `_sequence`, `_timestamp` (UTC, milliseconds) and `_headers` (a JSON
  object of name to value, a repeated name as a list, null when there are none). An empty
  payload under `json` is a row of only the underscore columns.
- **Messages discarded before they were read fail the run**, with the count: a stream's limits
  (`max_age`, `max_msgs`, `max_bytes`) drop the oldest messages, and a saved position older
  than the stream's first sequence means some were never read. The fix is `etl state forget`.
- **Messages deleted from the middle are not a gap.** `max_msgs_per_subject`, or a delete by
  hand, removes messages inside a stream; they are simply not there to read, and no error is
  raised, because JetStream does this in normal operation.
- A saved position past the end means the stream was probably deleted and made again: an
  error, with the same fix.

### Writing: `snk.stream.nats`

Each row is published as **one JSON message** to `subject`, which must be one exact subject (no
wildcards) that a stream captures; publishing where no stream listens fails. A batch
(`batch_size`, default 500) is published whole and then every acknowledgement is awaited,
so a batch costs one round trip's wait, not one per message.

- **`msg_id_column`** sets each message's `Nats-Msg-Id` from a column. **JetStream then drops a
  message whose ID it has already stored within the stream's duplicate window** (two minutes
  by default, set per stream), and the report says how many it dropped. So **a re-run within
  that window adds no copies**: the one sink in this project with that property. Outside the
  window, or without `msg_id_column`, delivery is at-least-once per batch.
- **A failed batch** reports the batches already delivered, and that part of the failing
  batch may have landed, as for Kafka.
- **Like every native sink, it publishes only after a run that fully succeeded**, and never in
  `preview`.

## `src.stream.kinesis` and `snk.stream.kinesis`

The source was added in Phase 10h, the sink in 10i (both 2026-09-24). Kinesis is a JSON-over-HTTPS API, so
it goes through the same blocking `ureq` layer as REST and GraphQL: no `tokio`.
**Verified against `kinesis-mock` 0.4.13, not against real AWS** (Settled decision 56): no AWS
account has been used. The request signing is this project's own and is proved by AWS's
published SigV4 test suite (all 38 cases, byte for byte); the test server does not check
signatures. Until someone reads a real stream with it, treat real-AWS use as unverified.

### Credentials and region

The first source that has them wins (Settled decision 48):

1. The node's `access_key_id` and `secret_access_key` (and `session_token` for temporary
   credentials). Put them in secrets, or better, leave them out.
2. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`.
3. A named profile (`profile`, else `AWS_PROFILE`, else `default`) in `~/.aws/credentials` or
   `~/.aws/config`, or the files `AWS_SHARED_CREDENTIALS_FILE` and `AWS_CONFIG_FILE` name.

**Not yet:** roles on EC2 (instance metadata), EKS (IRSA) and ECS, and `credential_process`
or SSO profiles. A missing credential says which of the sources above were tried. The region
comes from `region`, else `AWS_REGION` or `AWS_DEFAULT_REGION`, else the profile's.
Credentials are looked up when the connector runs, so an artifact takes them from the machine
it runs on and bakes in nothing unless a property holds them. The report says where they
came from, never what they are. `endpoint` overrides
`https://kinesis.<region>.amazonaws.com`, for a VPC endpoint or a test server.

### Reading

- **Each run reads each shard until Kinesis reports it caught up** (`MillisBehindLatest` 0),
  or the shard ends, or `max_records` is reached, shards taking turns. So a batch is "up to
  now" rather than, as for Kafka and NATS, "up to the end recorded when the run started":
  Kinesis has no cheap way to ask a shard for its newest sequence number. Records arriving
  during a run may be read by it or by the next; never twice, never skipped.
- **Resharding keeps order.** A child shard is read only after its parents are finished, so a
  partition key's records come out in the order they went in, across a split or a merge.
  A parent that has aged out of the stream counts as finished.
- **The position** is one entry per shard: the last sequence number read (as text: they are
  128-bit), "done" for a closed shard read to its end, "start" for a shard not yet reached,
  or, after a `latest` first run that read nothing, the time that run started, so the next
  run reads from then. That time is **this machine's clock**, compared with Kinesis's arrival
  times: a clock running behind makes records put just before the run count as new (found
  in 10m, when Docker Desktop's clock ran 150 ms ahead of Windows'). Keep the clock synced.
  `GetRecords` is paced to Kinesis's five calls a second per shard, and
  throttling (`ProvisionedThroughputExceededException`, a call rate exceeded) is retried
  with backoff; a real limit, such as an account's shard limit, fails at once.
- **Rows** are the other brokers' `value_format` (`json`, `text`, `bytes`) plus `_stream`,
  `_shard`, `_sequence`, `_timestamp` (arrival, UTC) and `_partition_key`.
- **Nothing is registered with Kinesis**: no consumer, no enhanced fan-out. The position lives
  in this project's state file; `etl state forget` replays.

### Expiry

Kinesis keeps records for the stream's retention (24 hours by default). When a run finds the
last record it read **no longer held**, records after it may have expired unread, and
**Kinesis's sequence numbers leave gaps, so the count cannot be known**. With `on_expired:
fail` (the default) the run fails, saying exactly that; nothing is read. With `on_expired:
continue` it carries on from the oldest record held and notes that some may have been lost.

One false alarm is known: a stream that received **nothing** for longer than its retention
loses nothing, but its last record has expired all the same, so the default still fails. A
pipeline that runs less often than the retention period, on a quiet stream, is the case for
`continue`.

### Delivery

At-least-once into the pipeline, as for Kafka and NATS: the position is saved only after a
fully successful run, so a failed run reads the same records again.

### Writing: `snk.stream.kinesis`

Each row is put as **one JSON record**, up to `batch_size` (default and most 500) to a
`PutRecords` call, and each call is also kept under Kinesis's 5 MiB. The stream must exist.

- **`partition_key_column`** makes each row's key its value in that column (numbers as text),
  so one key's records go to one shard, in order. A null or missing key fails the run, naming
  the row: Kinesis needs a key for every record. **Unset, the key is the row number**, which
  spreads rows across shards.
- **A record over 1 MiB** (the row as JSON plus its key), or a key outside 1 to 256
  characters, fails the run before it is sent, naming the row.
- **A call can put some records and refuse others.** Those refused for throughput
  (`ProvisionedThroughputExceededException`, `InternalFailure`, `KMSThrottlingException`) are
  sent again on their own, with backoff, up to `retries` times; the report counts them.
  Because only they are sent again, **a resent record lands after later records of the same
  call**, and so may come after a later record with the same key. Any other refusal fails at
  once.
- **A failure says how many records were put before it**; they stay in the stream. Delivery
  is at-least-once, as for the other brokers: a re-run puts everything again, and Kinesis has
  no duplicate window like NATS's.
- **Like every native sink, it puts only after a run that fully succeeded**, and never in
  `preview`.

## `src.queue.sqs` and `snk.queue.sqs`

Added in Phase 10j (2026-09-24). SQS speaks AWS's JSON protocol, so it goes through the same
signed `ureq` client as Kinesis: no `tokio`. Credentials, region and `endpoint` work exactly as
[for Kinesis](#credentials-and-region). **Verified against ElasticMQ 1.7.1, not against real
AWS** (Settled decision 70): the signing is proved by AWS's published SigV4 suite, and the test
server accepts any signature.

**Which queue:** `queue_url`, or `queue` (a name, looked up with `GetQueueUrl`, with
`queue_owner` for another account's queue). A FIFO queue's name ends in `.fifo`.

### Receiving: `src.queue.sqs`

- **A batch** receives up to ten messages a call until `max_records` (default **10,000**), a
  receive with a one-second wait comes back empty, or `max_wait_ms` (default 30,000) has
  passed. The one-second wait makes "empty" mean empty: a wait asks every server the queue
  lives on, where an instant answer can miss messages that are there.
- **Held, not deleted.** Each message is hidden for `visibility_seconds` (default 300, at
  most SQS's twelve hours). A **lease keeper** thread extends every held message by that much
  again every half-period until the run's outcome is known, then stops. Acknowledging is
  `DeleteMessageBatch`; releasing is `ChangeMessageVisibilityBatch` to 0, so the next run sees
  them at once, not after the timeout. If an extension fails, the settle line says so: some
  messages may have gone to another consumer meanwhile.
- **Rows** are the brokers' `value_format` (`json`, `text`, `bytes`) over the message body,
  plus `_queue` (the name), `_message_id`, `_sent_timestamp` (UTC), `_receive_count` (1 the
  first time; more means it came back), `_group_id` (FIFO) and `_attributes` (message
  attributes as `{name: value}`, a binary one as base64).
- **Order:** none on a standard queue. A FIFO queue keeps each group's order; while a
  group's messages are held, SQS hands out no more of that group, so one run takes at most
  one receive's worth (up to ten) from each group, and the next run takes the next.
- **Nothing is saved in `.etl/state/`** and `etl state forget` has nothing to forget: the
  queue holds the state.

### Sending: `snk.queue.sqs`

Each row is sent as **one JSON message**, ten to a `SendMessageBatch` and under 1 MiB a call.
A row over 1 MiB as JSON fails the run before it is sent, naming the row.

- **FIFO queues** need `message_group_id_column` (each group is kept in order), and either
  `deduplication_id_column` or content-based deduplication switched on for the queue. A null
  group or ID fails, naming the row.
- **`delay_seconds`** (standard queues only, up to 900) hides each message that long.
- **A call can send some messages and refuse others.** Those refused on SQS's side
  (`SenderFault` false) are sent again on their own, with backoff, up to `retries`; a refusal
  that is ours to fix fails at once. A failure says how many messages were sent before it;
  they stay in the queue.
- **At-least-once**, as the other sinks: a re-run sends everything again, except that a FIFO
  queue drops a repeated deduplication ID within its five-minute window.
- **Like every native sink, it sends only after a run that fully succeeded**, and never in
  `preview`.

## `src.queue.pubsub` and `snk.queue.pubsub`

Added in Phase 10k (2026-09-24). Pub/Sub's REST API (`v1`), through the shared `ureq` layer:
no `tokio`, and none of Google's own crates. **Verified against Google's Pub/Sub emulator
(`google-cloud-cli:586.0.0-emulators`), not against real Google Cloud** (Settled decision 70).
The emulator checks no sign-in, so signing in is proved separately: RS256 by RFC 7515's own
example, byte for byte, and the token exchange against a local test server.

**Which subscription or topic:** a name with `project`, or a full path,
`projects/<project>/subscriptions/<name>` (or `.../topics/<name>`), which needs no `project`.

### Signing in

The first of these that has credentials wins:

1. `credentials_file`: a service account's JSON key file, or a gcloud login file. It is a
   path, not the key itself, so the key stays a file only its owner can read.
2. `GOOGLE_APPLICATION_CREDENTIALS`, naming such a file.
3. gcloud's application-default login (`gcloud auth application-default login`):
   `%APPDATA%\gcloud\application_default_credentials.json` on Windows,
   `~/.config/gcloud/...` elsewhere, or under `CLOUDSDK_CONFIG`.

A **service account** signs a JWT with its key (RS256) and trades it at the key's
`token_uri`; a **person's login** trades its refresh token. Either way the access token is
cached until five minutes before it expires, and shared with the lease keeper. The metadata
server on GCE and GKE, and `external_account` (workload identity federation) files, are not
read yet. The report names the service account or login and where it came from, never the
key.

**`endpoint`**: unset, `PUBSUB_EMULATOR_HOST` if it is set, else
`https://pubsub.googleapis.com`. A regional endpoint (`https://europe-west1-pubsub.googleapis.com`)
works the same. **A plain `http://` endpoint is an emulator: nothing is signed, and a token
is never sent over it.**

### Pulling: `src.queue.pubsub`

- **A batch** pulls up to 1,000 messages a call until `max_records` (default **10,000**), a
  pull comes back empty, or `max_wait_ms` (default 30,000) has passed. A pull asks for an
  immediate answer (`returnImmediately`), so an empty subscription ends the run at once
  rather than after a server-side wait of unstated length. Google warns that such a pull
  can come back empty while messages are waiting; then the batch ends early and the next run
  takes them. Nothing is lost. Not yet seen against real Pub/Sub.
- **Held, not acknowledged.** A pull holds messages for the *subscription's* ack deadline,
  10 seconds unless it was set longer, so each pull's messages are extended at once to
  `ack_deadline_seconds` (default 60, at most 600). A **lease keeper** extends everything held
  by that much again every half-period until the run's outcome is known, then stops.
  Acknowledging is `:acknowledge`; releasing is `:modifyAckDeadline` to 0, so the next run
  gets them at once. If an extension fails, the settle line says so.
- **Rows** are the brokers' `value_format` (`json`, `text`, `bytes`) over the message data,
  plus `_subscription` (the name), `_message_id`, `_publish_time` (UTC, to the microsecond),
  `_ordering_key` (null without one), `_attributes` (`{name: value}`) and
  `_delivery_attempt`. **Pub/Sub counts deliveries only for a subscription with a dead-letter
  policy**; otherwise `_delivery_attempt` is null, and a repeat is spotted by `_message_id`.
  A message with attributes and no data is a row of the underscore columns alone.
- **Order:** none, unless the subscription has message ordering on and messages were
  published with ordering keys; then each key's messages come in order. A released message
  comes back before the key's later ones.
- **Nothing is saved in `.etl/state/`**: the subscription holds the state.

### Publishing: `snk.queue.pubsub`

Each row is published as **one JSON message**, up to 1,000 to a `:publish` call and under
10 MB once base64-encoded. A row too large for one call fails the run before it is sent,
naming the row.

- **`ordering_key_column`**: each message's ordering key; a null value publishes without one.
- **`attributes_column`**: a column holding an object, whose entries become the message's
  attributes as text. A null entry is left out; a value that is not an object fails, naming
  the row. The row, that column included, is still the message's data.
- **A publish is taken whole or not at all.** A 429 or 5xx is retried; a call retried after
  it had in fact landed publishes its messages twice. A failure says how many messages were
  published before it; they stay on the topic.
- **At-least-once**, as the other sinks: a re-run publishes everything again. Pub/Sub has no
  duplicate window for publishing.
- **Like every native sink, it publishes only after a run that fully succeeded**, and never
  in `preview`.

## `src.queue.rabbitmq` and `snk.queue.rabbitmq`

Added in Phase 10l (2026-09-24). AMQP 0-9-1 through `lapin`, on a small `tokio` runtime of the
connector's own. **Verified against RabbitMQ 4.3 itself**, plain and over TLS, classic and
quorum queues.

**Where:** `url`, `amqp://host[:port][/vhost]` or `amqps://` for TLS (ports 5672 and 5671 by
default). `username`, `password` and `vhost` fill in or override the URL's, so the password
can be a `${SECRET:...}` rather than part of a URL. The report and every error name
`host:port` and the vhost, never the password. **TLS** trusts the bundled public roots, or
`ca_cert`, exactly as Kafka and NATS do: `lapin` is handed the same `rustls` configuration
through its own connect function. **`timeout_ms`** (default 30,000) bounds every call to the
broker. It matters: a connect to a **vhost that does not exist** is refused by RabbitMQ at
once, but the refusal never reaches the client, so the run fails after `timeout_ms` saying
the vhost is the first thing to check.

### Receiving: `src.queue.rabbitmq`

- **A batch** is `basic.get` without acknowledging, one message at a time, until
  `max_records` (default **10,000**), the queue answers empty, or `max_wait_ms` (default
  30,000) has passed. The queue must already exist.
- **Held by the open channel.** RabbitMQ keeps what it handed out for as long as the channel
  stays open, so the receipt owns the connection and the channel. Nothing needs extending:
  `lapin` sends heartbeats on a thread of its own while the run goes on. Acknowledging is one
  `basic.ack` with `multiple`, up to the last delivery; releasing is one `basic.nack` with
  `multiple` and `requeue`. Then the channel and connection are closed.
- **A connection lost while holding gives everything back**: the broker requeues whatever
  was unacknowledged. The acknowledgement then fails, and the run warns that those messages
  will come again. The broker's own limit on a hold is `consumer_timeout`, **30 minutes by
  default**; a run holding messages longer than that ends the same way.
- **Rows** are the brokers' `value_format` (`json`, `text`, `bytes`) over the body, plus
  `_queue`, `_exchange` (null for the default exchange), `_routing_key`, `_redelivered`
  (true when it was handed out before), `_message_id` and `_timestamp` (the publisher's, if
  set) and `_headers` (as JSON). **A quorum queue counts hand-outs in the
  `x-acquired-count` header**; RabbitMQ 4's `x-delivery-count` counts only deliveries that
  failed.
- **Order:** a queue's own. A released message goes back to its place in a classic queue;
  a quorum queue may put it behind later ones.
- **Nothing is saved in `.etl/state/`**: the queue holds the state.

### Publishing: `snk.queue.rabbitmq`

Each row is published as **one JSON message** (`content_type` `application/json`) to
`exchange`, the default exchange when unset, with `routing_key` or each row's
`routing_key_column` (a null key fails, naming the row). The default exchange routes by
queue name, so it needs a routing key.

- **`persistent`** (default on) marks messages persistent, so a durable queue keeps them over
  a broker restart.
- **Publisher confirms**, awaited every 1,000 messages and at the end. Messages are published
  `mandatory`, so **a message no queue receives fails the run**, naming the row and its
  routing key, rather than vanishing, which an exchange with no matching binding otherwise
  does in silence. A failure says how many messages had been confirmed before it; they stay
  published.
- **At-least-once**: a re-run publishes everything again.
- **Like every native sink, it publishes only after a run that fully succeeded**, and never
  in `preview`.

## `src.db.mongodb` and `snk.db.mongodb`

Added in Phase 10m (2026-09-24). MongoDB's own driver (`mongodb` 3.9, its blocking API).
**Verified against MongoDB 8.0 itself**, plain and over TLS.

**Where:** `uri` (`mongodb://host[:port][,host...]/?options` or `mongodb+srv://...`),
`database`, `collection`. `username`, `password` and `auth_source` fill in or override the
URI's, so the password can be a `${SECRET:...}`. **TLS**: `ca_cert` turns it on and trusts
that file alone; otherwise the URI's `tls=true` trusts the bundled public roots, the same
rule as every other connector. `timeout_ms` (default 30,000) bounds finding a server and
each operation. Reports and errors name the hosts and the database, never the password.

### Reading: `src.db.mongodb`

- **`filter`, `projection`, `sort`**: JSON documents, as MongoDB takes them, in **Extended
  JSON** where a value is not plain JSON: `{"at": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}}`,
  `{"_id": {"$oid": "..."}}`. `batch_size` (default 1,000) documents a round trip;
  `max_records` caps a run.
- **Rows**: each top-level field is a column. What is not plain JSON is made plain: an
  `ObjectId` is its 24-character hex, a date a UTC timestamp, a `Decimal128` its exact text
  (which DuckDB casts to `DECIMAL` without loss), binary base64, nested documents and arrays
  JSON with the same rules inside. List the columns you want in `columns`, as for every
  native source.
- **A collection that does not exist is an error**, naming it. MongoDB itself answers a read
  of a missing collection with nothing, which would look like an empty one.
- **Only what is new**: `incremental_field` (e.g. `updatedAt`, `order_id`, or `_id`, whose
  `ObjectId`s rise with time) makes each run read only documents whose value there is above
  the last successful run's highest, in that field's order, with `start` (Extended JSON)
  for the first run. The highest value read is saved **only if the whole run succeeds**, as
  a checkpoint (`etl state list`, `etl state forget`), keeping its type: a date stays a
  date. A position saved for another collection or field is set aside and the report says
  so. **Two things are the data's to guarantee**: a document without the field is never
  read this way, and a field that can go *down*, or be set to a value below the saved one
  later, skips documents. `max_records` with it reads the oldest new documents first and the
  next run carries on.

### Writing: `snk.db.mongodb`

- **`mode: insert`** (the default): `insertMany`, unordered, 1,000 documents a call. A
  refused document (a duplicate `_id` or unique key) fails the run naming the row, **after**
  the rest of its batch has landed; the message says how many documents were written.
- **`mode: upsert`** with `key_fields`: each row **replaces** the document whose key fields
  match, or is added, through the `update` command, 1,000 to a call. A re-run adds nothing
  twice. A row without a value for a key field fails, naming it. It works on every server
  version (the driver's `bulkWrite` would need MongoDB 8).
- **Types**: a row's JSON becomes BSON as Extended JSON says, so a column holding
  `{"$date": "..."}` is stored as a date; a timestamp column from DuckDB arrives as text and is
  stored as text.
- **Like every native sink, it writes only after a run that fully succeeded**, and never in
  `preview`. Insert is at-least-once; upsert is idempotent by its keys.

## `src.warehouse.bigquery` and `snk.warehouse.bigquery`

Added in Phase 10o (2026-09-24). BigQuery's REST API through the shared `ureq` layer, signed
in as Pub/Sub is (a service account, gcloud's login, or nothing for a plain-`http://`
endpoint). **Verified against goccy's BigQuery emulator (0.8.1), not against real Google
Cloud** (Settled decision 78); the sign-in is 10k's, proved separately.

**Which table:** `project` (the project that runs the jobs) with `dataset` and `table`;
`dataset` may be `other-project.dataset` for another project's. `location` names where the
dataset lives; unset, BigQuery works it out and the connector keeps what the first job
reports. `timeout_ms` (default 120,000) bounds each request.

### Reading: `src.warehouse.bigquery`

- **A table, or a `query`** (GoogleSQL, not legacy SQL). Either becomes one **query job**
  (`jobs.query`), polled while it runs, then read **page by page** (10,000 rows a page)
  until the last. The report names the job and the bytes BigQuery says it processed, which
  is what it bills: **a table read is a full scan** unless the query narrows it.
- **Rows are typed by the result's schema.** INT64 is a number; NUMERIC and BIGNUMERIC their
  exact text (cast to `DECIMAL` without loss); TIMESTAMP a UTC timestamp to the microsecond
  (read from BigQuery's text exactly, never through a double); DATETIME, DATE and TIME
  their text; RECORD an object and REPEATED an array, with the same rules inside; JSON and
  GEOGRAPHY their text; FLOAT64's NaN and infinities stay text, as JSON has no such numbers.
- **Only what is new**: `incremental_column` wraps the read as
  `SELECT * FROM (<read>) WHERE col > @etl_after ORDER BY col`, the last successful run's
  highest value passed as a **typed query parameter**, never pasted into the SQL. The first
  run starts at `start`, a GoogleSQL literal you write (`TIMESTAMP '2026-01-01 00:00:00'`,
  `1000`), or at the beginning. The highest value read is saved as a checkpoint only if the
  whole run succeeds; a position saved for another table, query or column is set aside and
  the report says so. The column must be a scalar that can be compared (INT64, NUMERIC,
  FLOAT64, TIMESTAMP, DATETIME, DATE, STRING) and **must only go up**: a row loaded later
  with a lower value is never read. The wrapping still scans what the inner read scans;
  partition the table by that column to make incremental reads cheap.
- `max_records` stops mid-page and asks for no more pages.

### Writing: `snk.warehouse.bigquery`

- **Load jobs**, not streaming inserts: each run's rows as newline-delimited JSON, **4 MB to
  a job** (a multipart upload, meant for bodies small enough to send again whole), each job
  polled until done. Load jobs are free; streaming inserts are billed. BigQuery allows 1,500
  load jobs a table a day, so a run of more than a few gigabytes needs another way in.
- **The table must exist** (`CREATE_NEVER`), with columns matching the rows; a load that
  does not fit fails naming BigQuery's reason and the row it points at.
- **`mode: append`** (the default) adds the rows. **`mode: truncate`** replaces the table's
  rows with the first job and appends with the rest, so **a failure part-way leaves the rows
  loaded until then**, and the message says how many. A truncate with no rows still empties
  the table.
- Timestamps arrive as DuckDB's text (`2026-01-04 10:05:00`), which BigQuery's JSON loader
  reads as UTC.
- **Like every native sink, it loads only after a run that fully succeeded**, and never in
  `preview`. At-least-once: a re-run appends again.

## `src.warehouse.snowflake` and `snk.warehouse.snowflake`

Added in Phase 10p (2026-09-24). Snowflake's **SQL API** (`/api/v2/statements`) through the
shared `ureq` layer. **Not checked against real Snowflake**: there is no emulator, and no
account is used (Settled decisions 77, 78). What is proved, against the local fixture: the
key's fingerprint against `openssl`'s, the JWT's claims and signature, values typed as
Snowflake's documentation writes them, bind variables, and the flow of statements, polls
and partitions. A first run against a real account is the check still owed.

### Signing in

**By key pair only**, which is what the SQL API takes for a program: `account` (the account
identifier, `orgname-accountname` or a locator such as `xy12345.eu-west-1`), `user`, and the
user's private key, as `private_key_file` (PKCS#8 PEM, unencrypted, relative to the
workspace) or as `private_key` (the PEM text, as a `${SECRET:...}`). Register the public key
first: `ALTER USER <user> SET RSA_PUBLIC_KEY='...'`. Each request carries a fresh JWT signed
RS256 (10k's signing), naming the account, the user and the key's SHA-256 fingerprint, the
same value `DESC USER` shows as `RSA_PUBLIC_KEY_FP`. An encrypted key is refused with how to
decrypt it; passwords, OAuth and programmatic access tokens are not read yet. `role`,
`warehouse`, `database` and `schema` are sent with every statement; unset, the user's
defaults. `endpoint` replaces `https://<account>.snowflakecomputing.com` for a private link.

### Reading: `src.warehouse.snowflake`

- **A `table`** (`name`, `schema.name` or `database.schema.name`; a name Snowflake would read
  bare stays bare, so it is upper-cased as Snowflake stored it) **or a `query`**. One
  statement, submitted with a request ID so a retried submission is not run twice; a
  `202` (still running) is polled until done; then each **result partition** after the first
  is fetched. `max_records` stops without fetching later partitions.
- **Every statement runs with the session time zone UTC.**
- **Rows are typed by the result's `rowType`**: NUMBER with scale 0 a number (its text beyond
  64 bits), with a scale its exact text; FLOAT a number; BOOLEAN; DATE and TIME their text;
  TIMESTAMP_NTZ, _LTZ and _TZ a UTC timestamp to the microsecond; VARIANT, OBJECT and ARRAY
  parsed JSON; BINARY its hex; TEXT itself.
- **Only what is new**: `incremental_column` wraps the read as `SELECT * FROM (<read>) WHERE
  col > CAST(? AS <type>) ORDER BY col`, the last successful run's highest value a **bind
  variable**, never pasted into the SQL; the first run starts at `start`, a SQL literal you
  write (`'2026-01-01'::TIMESTAMP_NTZ`, `1000`). Saved only when the whole run succeeds; set
  aside, and said so, when the table, query or column changed. The column must only go up.
  **The warehouse runs, and bills, the whole wrapped query**: cluster the table by that
  column to keep it cheap.

### Writing: `snk.warehouse.snowflake`

- **Batched `INSERT`s**: `INSERT INTO <table> (<columns>) VALUES (?, ...)` with each column
  bound to an array of up to 1,000 values, so a statement inserts a thousand rows. The SQL API
  has no `PUT` or `COPY`; for millions of rows a stage and `COPY INTO` are the better way, and
  are not offered yet.
- **Every value is bound as text** and Snowflake converts it to the column's type; a nested
  value arrives as its JSON text, which a VARIANT column stores as a string, not parsed. The
  table must exist, and every row must have the first row's columns.
- **`mode: truncate`** runs `TRUNCATE TABLE` first, then the inserts: not one transaction, so
  a failure part-way leaves the table with what was inserted until then, which the error
  says.
- **Like every native sink, it inserts only after a run that fully succeeded**, and never in
  `preview`. At-least-once: a re-run inserts again.
