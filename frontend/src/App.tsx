/**
 * The shell, for Phase 7a.
 *
 * This is a harness, not the product: it exists to prove the five IPC commands
 * work end to end against the real engine, and to be the thing 7b's canvas is
 * dropped into. Everything here that looks like UI — the JSON textarea, the
 * node picker — is scaffolding the canvas replaces.
 *
 * What is *not* scaffolding: the component manifest arrives from the engine and
 * is rendered generically. Nothing in this file knows what a `src.file.csv` is.
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  asIpcError,
  compilePipeline,
  inDesktopShell,
  listComponents,
  previewNode,
  runPipeline,
  validatePipeline,
  type IpcError,
  type Manifest,
  type PlanView,
  type PreviewResult,
  type RunResult,
  type Validation,
} from "./ipc";

const SAMPLE = `{
  "formatVersion": 1,
  "nodes": [
    {
      "id": "orders",
      "type": "source",
      "position": { "x": 0, "y": 0 },
      "data": {
        "label": "Orders CSV",
        "componentId": "src.file.csv",
        "properties": { "path": "samples/data/orders.csv" }
      }
    },
    {
      "id": "large",
      "type": "transform",
      "position": { "x": 260, "y": 0 },
      "data": {
        "label": "Large orders",
        "componentId": "xf.filter",
        "properties": { "predicate": "amount > 100" }
      }
    }
  ],
  "edges": [
    {
      "id": "e1",
      "source": "orders",
      "target": "large",
      "sourceHandle": "main",
      "targetHandle": "in"
    }
  ]
}`;

type Panel =
  | { kind: "idle" }
  | { kind: "busy"; what: string }
  | { kind: "error"; error: IpcError }
  | { kind: "validation"; value: Validation }
  | { kind: "plan"; value: PlanView }
  | { kind: "run"; value: RunResult }
  | { kind: "preview"; value: PreviewResult };

export default function App() {
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [manifestError, setManifestError] = useState<IpcError | null>(null);
  const [document, setDocument] = useState(SAMPLE);
  const [nodeId, setNodeId] = useState("large");
  const [panel, setPanel] = useState<Panel>({ kind: "idle" });

  useEffect(() => {
    listComponents().then(setManifest, (thrown) => setManifestError(asIpcError(thrown)));
  }, []);

  /** Every command follows the same shape, so the busy and error handling is written once. */
  const perform = useCallback(
    async <T,>(what: string, work: () => Promise<T>, show: (value: T) => Panel) => {
      setPanel({ kind: "busy", what });
      try {
        setPanel(show(await work()));
      } catch (thrown) {
        setPanel({ kind: "error", error: asIpcError(thrown) });
      }
    },
    [],
  );

  const byNamespace = useMemo(() => {
    const counts = new Map<string, number>();
    for (const component of manifest?.components ?? []) {
      counts.set(component.namespace, (counts.get(component.namespace) ?? 0) + 1);
    }
    return [...counts.entries()].sort(([a], [b]) => a.localeCompare(b));
  }, [manifest]);

  if (!inDesktopShell()) {
    return (
      <main className="shell">
        <h1>ETL Local Tool</h1>
        <p className="notice">
          This page is open in a plain browser, so there is no engine behind it. Start the
          desktop shell instead:
        </p>
        <pre>cd apps/desktop &amp;&amp; npm --prefix ../../frontend run build &amp;&amp; cargo run</pre>
      </main>
    );
  }

  return (
    <main className="shell">
      <header>
        <h1>ETL Local Tool</h1>
        <p className="subtitle">Phase 7a — the shell and its five commands, against the real engine.</p>
      </header>

      <section>
        <h2>Components</h2>
        {manifestError ? (
          <p className="error">{manifestError.message}</p>
        ) : manifest ? (
          <p>
            <strong>{manifest.components.length}</strong> registered:{" "}
            {byNamespace.map(([namespace, count], index) => (
              <span key={namespace}>
                {index > 0 ? ", " : ""}
                {count} {namespace}
              </span>
            ))}
          </p>
        ) : (
          <p className="muted">loading…</p>
        )}
      </section>

      <section>
        <h2>Pipeline</h2>
        <textarea
          value={document}
          onChange={(event) => setDocument(event.target.value)}
          spellCheck={false}
          rows={18}
        />

        <div className="controls">
          <button onClick={() => perform("validating", () => validatePipeline(document), (value) => ({ kind: "validation", value }))}>
            Validate
          </button>
          <button onClick={() => perform("compiling", () => compilePipeline(document), (value) => ({ kind: "plan", value }))}>
            Compile
          </button>
          <button onClick={() => perform("running", () => runPipeline(document), (value) => ({ kind: "run", value }))}>
            Run
          </button>
          <span className="spacer" />
          <label>
            node
            <input value={nodeId} onChange={(event) => setNodeId(event.target.value)} size={12} />
          </label>
          <button onClick={() => perform("previewing", () => previewNode(document, nodeId), (value) => ({ kind: "preview", value }))}>
            Preview
          </button>
        </div>
      </section>

      <section>
        <h2>Result</h2>
        <Result panel={panel} />
      </section>
    </main>
  );
}

function Result({ panel }: { panel: Panel }) {
  switch (panel.kind) {
    case "idle":
      return <p className="muted">Nothing yet.</p>;

    case "busy":
      return <p className="muted">{panel.what}…</p>;

    case "error":
      return (
        <div className="error">
          <p>
            <strong>{panel.error.stage}</strong>
            {panel.error.nodeId ? ` · ${panel.error.nodeId}` : ""}
          </p>
          <pre>{panel.error.message}</pre>
        </div>
      );

    case "validation":
      return panel.value.valid ? (
        <div>
          <p className="ok">
            Valid — {panel.value.stageCount} stage(s), {panel.value.sinkCount} sink(s).
          </p>
          <Warnings items={panel.value.warnings} />
        </div>
      ) : (
        <div className="error">
          <p>
            <strong>{panel.value.error?.stage}</strong>
            {panel.value.error?.nodeId ? ` · ${panel.value.error.nodeId}` : ""}
          </p>
          <pre>{panel.value.error?.message}</pre>
        </div>
      );

    case "plan":
      return (
        <div>
          <p>
            {panel.value.stages.length} stage(s)
            {panel.value.extensions.length > 0 && ` · needs ${panel.value.extensions.join(", ")}`}
            {panel.value.needsSession &&
              ` · runs through a session (${panel.value.sessionReasons.join(", ")})`}
          </p>
          <Warnings items={panel.value.warnings} />
          <pre className="sql">{panel.value.script}</pre>
        </div>
      );

    case "run":
      return (
        <div>
          <table>
            <thead>
              <tr>
                <th>Stage</th>
                <th>Rows</th>
                <th>Component</th>
              </tr>
            </thead>
            <tbody>
              {panel.value.stages.map((stage) => (
                <tr key={stage.nodeId}>
                  <td>{stage.label}</td>
                  <td className="numeric">
                    {stage.skipped ?? stage.rows ?? "—"}
                    {stage.rejected !== null && ` (+${stage.rejected} rejected)`}
                  </td>
                  <td className="muted">{stage.componentId}</td>
                </tr>
              ))}
            </tbody>
          </table>

          {panel.value.notes.map((note) => (
            <p key={note} className="muted">
              · {note}
            </p>
          ))}
          {panel.value.failures.map((failure) => (
            <p key={failure} className="error">
              ! {failure}
            </p>
          ))}

          <p className={panel.value.failed ? "error" : "ok"}>
            {panel.value.failed ? "Run failed" : "Ran"} in {panel.value.elapsedMs}ms
          </p>
        </div>
      );

    case "preview": {
      const { columns, rows, truncated } = panel.value;

      if (rows.length === 0) {
        return <p className="muted">No rows.</p>;
      }

      return (
        <div>
          <table>
            <thead>
              <tr>
                {columns.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={index}>
                  {columns.map((column) => (
                    <td key={column}>{formatCell(row[column])}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          <p className="muted">
            {rows.length} row(s){truncated && ", and there are more"}
          </p>
        </div>
      );
    }
  }
}

function Warnings({ items }: { items: string[] }) {
  if (items.length === 0) return null;

  return (
    <ul className="warnings">
      {items.map((warning) => (
        <li key={warning}>{warning}</li>
      ))}
    </ul>
  );
}

/** Null is a value a cell can hold, and it must not render as blank. */
function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
