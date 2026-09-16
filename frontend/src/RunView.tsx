/**
 * The three things the bottom panel shows: what happened, what will run, and
 * what the rows look like.
 *
 * Extracted from `App.tsx` so that file stays about owning the document. The
 * rule these all share is that a number is shown only when it means what it
 * appears to mean — a blank is a smaller lie than a zero, and this panel is
 * where someone decides whether a pipeline is working.
 */

import { Sql } from "./sql-highlight";
import type { PipelineDoc } from "./document";
import type { PlanView, PreviewResult, RunResult, Validation } from "./ipc";

/** Milliseconds, at a precision that does not imply more than was measured. */
export function duration(ms: number): string {
  if (ms < 1) return "<1 ms";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)} s`;
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

export function StatusTab({
  document,
  validation,
  run,
}: {
  document: PipelineDoc;
  validation: Validation | null;
  run: RunResult | null;
}) {
  if (document.nodes.length === 0) {
    return <p className="muted">Drag a component from the left to begin.</p>;
  }

  return (
    <>
      {validation && !validation.valid && validation.error && (
        <p className="error">
          <strong>{validation.error.stage}</strong>
          {validation.error.nodeId ? ` · ${validation.error.nodeId}` : ""} —{" "}
          {validation.error.message}
        </p>
      )}

      {validation?.valid && !run && (
        <p className="ok">
          Valid — {validation.stageCount} stage(s), {validation.sinkCount} sink(s).
        </p>
      )}

      {validation?.warnings.map((warning) => (
        <p key={warning} className="warn">
          {warning}
        </p>
      ))}

      {run && <RunTable run={run} />}
    </>
  );
}

function RunTable({ run }: { run: RunResult }) {
  // The footnote earns its place only when there is something blank to
  // explain, which is almost always — but a plan of sinks and control nodes
  // times every stage, and then the note would be answering nothing.
  const anyBlank = run.stages.some((stage) => stage.elapsedMs === null && !stage.skipped);

  return (
    <>
      <table className="run">
        <thead>
          <tr>
            <th>Stage</th>
            <th className="right">Rows</th>
            <th className="right">Rejected</th>
            <th className="right">Time</th>
            <th>Component</th>
          </tr>
        </thead>
        <tbody>
          {run.stages.map((stage) => (
            <tr key={stage.nodeId} className={stage.skipped ? "is-skipped" : ""}>
              <td>{stage.label}</td>
              <td className="right">
                {stage.skipped ? (
                  <span className="muted">{stage.skipped}</span>
                ) : (
                  (stage.rows?.toLocaleString() ?? "—")
                )}
              </td>
              {/* Zero rejects is the answer someone ran the check to see, so a
                  quality node shows it; a node that cannot reject shows
                  nothing, which is a different statement. */}
              <td className="right">
                {stage.rejected === null ? (
                  ""
                ) : (
                  <span className={stage.rejected > 0 ? "warn" : "muted"}>
                    {stage.rejected.toLocaleString()}
                  </span>
                )}
              </td>
              <td className="right">{stage.elapsedMs === null ? "" : duration(stage.elapsedMs)}</td>
              <td className="muted mono">{stage.componentId}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {anyBlank && (
        <p className="muted small note">
          A blank time is a stage whose work happens somewhere else: a lazy view costs
          microseconds to declare and is computed later by the sink that reads it. Timing it
          would credit the wrong stage.
        </p>
      )}

      {run.notes.map((note) => (
        <p key={note} className="muted">
          · {note}
        </p>
      ))}

      {run.failures.map((failure) => (
        <p key={failure} className="error">
          ! {failure}
        </p>
      ))}

      <p className={run.failed ? "error" : "ok"}>
        {run.failed ? "Run failed after" : "Ran"} {duration(run.elapsedMs)} —{" "}
        {run.stages.length} stage(s).
      </p>
    </>
  );
}

// ---------------------------------------------------------------------------
// Plan
// ---------------------------------------------------------------------------

/**
 * What will run, stage by stage, in the order it will run.
 *
 * Per stage rather than as one script, because the question this tab answers is
 * "what is this node doing", and a single blob makes the reader find the node
 * themselves. The whole script is one click away for when the question is
 * "what exactly gets sent".
 */
export function PlanTab({
  plan,
  whole,
  onToggleWhole,
  onSelect,
}: {
  plan: PlanView | null;
  whole: boolean;
  onToggleWhole: () => void;
  onSelect: (nodeId: string) => void;
}) {
  if (!plan) return <p className="muted">Press Plan to compile without running.</p>;

  return (
    <>
      <p className="plan-head">
        <span>{plan.stages.length} stage(s)</span>

        {plan.extensions.length > 0 && (
          <span className="muted">loads {plan.extensions.join(", ")}</span>
        )}

        {/* Which transport a plan earned is not a detail: it is why a retry or
            a branch is possible at all, and it is invisible in the SQL. */}
        <span className={plan.needsSession ? "warn" : "muted"}>
          {plan.needsSession ? `session — ${plan.sessionReasons.join(", ")}` : "one script"}
        </span>

        <span className="grow" />

        <button className="link" onClick={onToggleWhole}>
          {whole ? "By stage" : "Whole script"}
        </button>
      </p>

      {plan.warnings.map((warning) => (
        <p key={warning} className="warn">
          {warning}
        </p>
      ))}

      {whole ? (
        <Sql sql={plan.script} />
      ) : (
        <ol className="plan">
          {plan.stages.map((stage) => (
            <li key={stage.nodeId}>
              <p className="plan-stage">
                <button className="link" onClick={() => onSelect(stage.nodeId)}>
                  {stage.label}
                </button>
                <span className="muted mono">{stage.componentId}</span>
                {stage.splits && <span className="warn small">splits</span>}
              </p>
              <Sql sql={stage.sql} />
            </li>
          ))}
        </ol>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

export function DataTab({ preview }: { preview: PreviewResult | null }) {
  if (!preview) {
    return <p className="muted">Select a node and press Preview to read its rows.</p>;
  }

  if (preview.columns.length === 0) {
    return <p className="muted">{preview.nodeId} produces no columns.</p>;
  }

  return (
    <>
      <p className="muted small note">
        <strong className="mono">{preview.nodeId}</strong> — {preview.rows.length} row(s)
        {preview.truncated && ", and there are more"}. Reading is safe: only the stages this
        node depends on were run, and the sinks were dropped.
      </p>

      {preview.rows.length === 0 ? (
        <p className="muted">No rows.</p>
      ) : (
        <div className="grid">
          <table>
            <thead>
              <tr>
                <th className="row-number" />
                {preview.columns.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {preview.rows.map((row, index) => (
                <tr key={index}>
                  <td className="row-number muted">{index + 1}</td>
                  {preview.columns.map((column) => (
                    <Cell key={column} value={row[column]} />
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

/**
 * One cell.
 *
 * Null is a value and must not render as an empty cell — the two mean different
 * things, and a grid that conflates them is one you cannot debug a quality
 * check with.
 */
function Cell({ value }: { value: unknown }) {
  if (value === null || value === undefined) {
    return (
      <td className="muted null" title="null">
        null
      </td>
    );
  }

  if (typeof value === "object") {
    return <td className="mono">{JSON.stringify(value)}</td>;
  }

  return <td className={typeof value === "number" ? "right mono" : ""}>{String(value)}</td>;
}
