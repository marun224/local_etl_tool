# Decision — the execution model for control flow

**Status:** **chosen — option A, dual path.** Decided 2026-09-16. **Unblocks:** Phase 6b.

Phase 6b needs a stage to be runnable on its own. Everything built so far assumes the opposite,
and this is the record of what was measured and what was chosen instead.

---

## The problem

`exec::run` hands the whole plan to DuckDB as one `-c` invocation. That was a deliberate Phase 2
choice and it is written into the module: temp views live in a session, every invocation is a
fresh process, so stage-at-a-time execution would discard every view between stages. Fail-fast
comes free, because a failed statement aborts the batch.

Phase 6b's contents do not fit inside it:

| What 6b wants | Why one script cannot do it |
|---|---|
| `ctl.foreach` | The body runs N times with a different binding each time. N is not known when the script is generated. |
| `ctl.if` / `ctl.branch` | Which stages run depends on a value only known once the run is under way. |
| `ctl.wait`, `ctl.throttle` | Need the driver to hold between statements. |
| `ctl.run_pipeline` | A whole nested plan, with its own session. |
| `retry_attempts` | Re-running one stage means addressing one stage. |
| `continue_on_failure` | The batch aborting is exactly what this has to prevent. |
| `qa.row_count`, `qa.schema_match` | Deferred from 6a: they fail a run rather than partition rows, which needs the same machinery. |

Every one of these needs the same thing: send some SQL, see what happened, decide what to send
next, with the session still holding the temp views from before.

---

## What was measured

Against the vendored `tools/duckdb/duckdb.exe` v1.5.5 on Windows, 2026-09-16. These are
observations, not estimates; re-run them before trusting this document if the DuckDB version
moves. Probe scripts are not committed — they were throwaway, and the numbers below are the
part worth keeping.

**1. The CLI can be driven as a read-eval loop.** Statements sent on stdin are answered
incrementally — it does not buffer until EOF. A driver can read a result and branch on it before
sending the next statement. Verified by creating a view, reading a count, branching on the value,
and then reading the original view again from the same session.

**2. A warm session is two orders of magnitude cheaper than a spawn.**

| | per statement |
|---|---|
| Round trip on a persistent session | **0.54 ms** |
| Same, reading a 12-row CSV | 5.3 ms |
| Fresh process per statement | **41.5 ms** |

A spawn costs **77×** a round trip. A `foreach` over 1,000 values is about half a second of
overhead on a persistent session and about 40 seconds of it on process-per-call.

**3. A failed statement does not kill a driven session.** The error goes to stderr; the session
keeps its temp views, and new views can still be created afterwards. This is what makes
`continue_on_failure` implementable at all.

**4. `.bail on|off` is a per-statement toggle for fail-fast.** With `.bail off` (the default) a
failure prints and the session continues. With `.bail on` a failure **terminates the session** —
not just the batch — so it cannot be used to get fail-fast while keeping the session. Fail-fast
therefore has to be the driver's decision rather than the shell's, which is where per-stage
`continue_on_failure` needs it anyway.

**5. Failure is detectable on stdout alone, without racing stderr.** A failing statement emits no
JSON array. Since every stage already emits a count probe after it (Phase 2), a stage that
succeeded produces a count and a stage that failed produces nothing — the existing probes double
as success signals, with a sentinel marking the boundary. stderr is then only needed for the
*message*, not for the *verdict*, which matters because the two streams have no ordering
guarantee between them.

---

## The options

### A. Drive the CLI as a persistent session

Keep the subprocess and the vendored binary. Open it once per run with stdin held open, send
statements, read results against a sentinel, decide what to send next.

**For.** Keeps everything Phase 9 depends on — the vendored binary, the vendored extensions, the
air-gapped `LOAD`-only prelude — unchanged. No new dependency, no change to build time or binary
size. 0.54 ms per round trip is cheap enough that per-stage execution is affordable for every
plan, not only ones with control flow. The risk section's instruction to keep the invocation
behind one function is honoured rather than cashed in.

**Against.** The executor grows a wire protocol: sentinels, stream correlation, and a timeout for
a statement that never answers. Today a hung DuckDB is the OS's problem; with a held-open pipe it
becomes ours, and a deadlock is a hang rather than a crash. Errors stay text to be parsed — this
buys control flow, not type safety. Redaction must now run over a stream rather than one buffer.
Windows pipe buffering needs care, and the tests need to cover the protocol as well as the SQL.

### B. Link `duckdb-rs` and drop the subprocess

**For.** Real error types, typed results, no protocol to invent, no process to supervise. The
honest long-term destination.

**Against.** It is a different project. DuckDB's amalgamation is a very large C++ build, which
lands on every contributor's machine and on CI. It changes the extension story that Phase 4 and
Phase 9 are both built on — extensions are currently vendored files a CLI is pointed at. It
contradicts the plan's own risk note, which put an embedded engine down as *a later option rather
than a rewrite*. Doing it here means Phase 6b stops being about control flow and becomes about
build systems.

### C. Split the plan into script segments at control boundaries

Run each segment as its own `-c` invocation, as today.

**For.** The smallest change to `exec.rs`. No protocol, no held pipe.

**Against.** Temp views do not survive between segments, so every boundary has to materialise —
changing both semantics and performance at exactly the points a user added a control node, for
reasons that have nothing to do with what they asked for. At 41.5 ms a spawn, a `foreach` pays
per iteration. And it does not actually deliver `retry_attempts` or `continue_on_failure` at
stage granularity without segmenting at *every* stage, at which point it is option A with a worse
constant factor.

---

## Recommendation: A

B is where this ends up eventually and the code should keep pointing that way — one function
owning the invocation, no DuckDB knowledge leaking outward — but doing it now spends Phase 6b on
a build-system migration and takes the vendored-extension design with it. C is cheaper to write
and worse to own: it makes a user's control-flow node silently change how their data pipeline is
materialised.

A is the one that buys what 6b needs, costs a protocol rather than an architecture, and leaves
every later option open.

**If A is chosen, the shape is:**

- A `Session` type owning the child process, with `execute(sql) -> Result<Vec<JsonValue>>` and a
  timeout. The one function the risk note asked for.
- `.bail off` at open; the driver decides fail-fast from per-stage policy.
- Success is "the expected count arrived before the sentinel"; stderr supplies the message.
- The existing one-script path stays for plans with no control nodes, so 40 components' worth of
  behaviour and every existing test keep the execution path they were written against. A plan
  earns the session only when it needs one.

That last point is what keeps the change additive rather than a rewrite.

---

## The decision

**Option A, with the dual path.** Chosen 2026-09-16.

A plan runs through the persistent session **only when it needs one** — when it holds a control
node, or a stage carrying a retry or `continue_on_failure` policy. Every other plan takes the
existing one-script path, unchanged.

The reasoning for earning the session rather than moving everything onto it: 47 components and
300 tests were written against the one-script path, and that path is not broken. Moving them
wholesale would mean a bug in a newly written protocol is a bug in everything, instead of a bug
in the new features only. The cost is two execution paths to keep honest, and the mitigation is
that they share the plan, the SQL, and the count probes — only the transport differs.

Revisit if the two paths start disagreeing about anything other than transport. That is the
signal that the dual path has stopped paying for itself, and the answer then is to move
everything onto the session, not to add a third.
