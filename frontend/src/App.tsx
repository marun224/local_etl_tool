/**
 * The studio.
 *
 * Owns the document and nothing else owns any part of it. The canvas draws it,
 * the palette adds to it, the engine is asked about it — but there is one copy,
 * and it is the same shape the CLI reads. That is what makes the round-trip
 * promise structural rather than something to remember.
 *
 * Phase 7b. The property panel on the right is 7c's and shows a node's current
 * values read-only until then; the run view is 7d's.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { open as openDialog, save as saveDialog } from "@tauri-apps/plugin-dialog";

import { Palette } from "./Palette";
import { PipelineCanvas } from "./PipelineCanvas";
import {
  emptyDocument,
  parseDocument,
  serializeDocument,
  specsById,
  propertiesOf,
  type PipelineDoc,
} from "./document";
import {
  asIpcError,
  compilePipeline,
  inDesktopShell,
  listComponents,
  previewNode,
  readPipeline,
  runPipeline,
  validatePipeline,
  writePipeline,
  type IpcError,
  type Manifest,
  type PreviewResult,
  type RunResult,
  type StageResult,
  type Validation,
} from "./ipc";

type Tab = "problems" | "sql" | "data";

export default function App() {
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [document, setDocument] = useState<PipelineDoc>(emptyDocument);
  const [path, setPath] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);

  const [selected, setSelected] = useState<string | null>(null);
  const [validation, setValidation] = useState<Validation | null>(null);
  const [run, setRun] = useState<RunResult | null>(null);
  const [preview, setPreview] = useState<PreviewResult | null>(null);
  const [sql, setSql] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("problems");

  const specs = useMemo(() => specsById(manifest), [manifest]);

  useEffect(() => {
    listComponents().then(setManifest, (thrown) => setToast(asIpcError(thrown).message));
  }, []);

  const edit = useCallback((next: PipelineDoc) => {
    setDocument(next);
    setDirty(true);
  }, []);

  // Validation follows editing, debounced. A canvas that only tells you
  // something is wrong when you press a button is a canvas you build mistakes
  // in for ten minutes first.
  const timer = useRef<number | undefined>(undefined);

  useEffect(() => {
    if (!inDesktopShell() || document.nodes.length === 0) {
      setValidation(null);
      return;
    }

    window.clearTimeout(timer.current);
    timer.current = window.setTimeout(() => {
      validatePipeline(serializeDocument(document)).then(setValidation, () => setValidation(null));
    }, 350);

    return () => window.clearTimeout(timer.current);
  }, [document]);

  /** Node id → what the engine said about it, for the red boxes. */
  const problems = useMemo(() => {
    const found = new Map<string, string>();

    const blamed = validation?.error;
    if (blamed?.nodeId) found.set(blamed.nodeId, blamed.message);

    for (const stage of run?.stages ?? []) {
      if (stage.skipped) found.set(stage.nodeId, stage.skipped);
    }

    return found;
  }, [validation, run]);

  const results = useMemo(() => {
    const found = new Map<string, StageResult>();
    for (const stage of run?.stages ?? []) found.set(stage.nodeId, stage);
    return found;
  }, [run]);

  const act = useCallback(
    async (what: string, work: () => Promise<void>) => {
      setBusy(what);
      setToast(null);
      try {
        await work();
      } catch (thrown) {
        const error: IpcError = asIpcError(thrown);
        setToast(error.message);
      } finally {
        setBusy(null);
      }
    },
    [],
  );

  const onOpen = () =>
    act("opening", async () => {
      const picked = await openDialog({
        multiple: false,
        filters: [{ name: "Pipeline", extensions: ["json"] }],
      });
      if (typeof picked !== "string") return;

      const text = await readPipeline(picked);
      setDocument(parseDocument(text));
      setPath(picked);
      setDirty(false);
      setRun(null);
      setPreview(null);
      setSql(null);
      setSelected(null);
    });

  const onSave = (as: boolean) =>
    act("saving", async () => {
      let target = path;

      if (as || target === null) {
        const picked = await saveDialog({
          defaultPath: target ?? "pipeline.json",
          filters: [{ name: "Pipeline", extensions: ["json"] }],
        });
        if (typeof picked !== "string") return;
        target = picked;
      }

      await writePipeline(target, serializeDocument(document));
      setPath(target);
      setDirty(false);
      setToast(`Saved to ${target}`);
    });

  const onRun = () =>
    act("running", async () => {
      const result = await runPipeline(serializeDocument(document));
      setRun(result);
      setTab("problems");
    });

  const onCompile = () =>
    act("compiling", async () => {
      const plan = await compilePipeline(serializeDocument(document));
      setSql(plan.script);
      setTab("sql");
    });

  const onPreview = () =>
    act("previewing", async () => {
      if (!selected) return;
      setPreview(await previewNode(serializeDocument(document), selected, 100));
      setTab("data");
    });

  if (!inDesktopShell()) {
    return (
      <main className="outside">
        <h1>ETL Local Tool</h1>
        <p>
          This page is open in a plain browser, so there is no engine behind it. Start the
          desktop shell instead:
        </p>
        <pre>npm --prefix frontend run dev{"\n"}cargo run -p etl-desktop</pre>
      </main>
    );
  }

  const selectedNode = document.nodes.find((node) => node.id === selected) ?? null;

  return (
    <div className="studio">
      <header className="bar">
        <strong>ETL Local Tool</strong>
        <span className="file">
          {path ?? "untitled"}
          {dirty && <span className="dot" title="unsaved changes" />}
        </span>

        <span className="grow" />

        <button onClick={onOpen}>Open</button>
        <button onClick={() => onSave(false)}>Save</button>
        <button onClick={() => onSave(true)}>Save as…</button>
        <span className="sep" />
        <button onClick={onCompile} disabled={document.nodes.length === 0}>
          SQL
        </button>
        <button onClick={onPreview} disabled={!selected}>
          Preview
        </button>
        <button className="primary" onClick={onRun} disabled={document.nodes.length === 0}>
          Run
        </button>
      </header>

      <div className="body">
        <Palette manifest={manifest} />

        <PipelineCanvas
          document={document}
          specs={specs}
          problems={problems}
          results={results}
          selected={selected}
          onChange={edit}
          onSelect={setSelected}
          onRefused={setToast}
        />

        <aside className="inspector">
          <h3>Node</h3>
          {selectedNode === null ? (
            <p className="muted">Nothing selected.</p>
          ) : (
            <>
              <div className="field">
                <span>id</span>
                <code>{selectedNode.id}</code>
              </div>
              <div className="field">
                <span>component</span>
                <code>{selectedNode.data.componentId}</code>
              </div>

              {/* 7c replaces this with generated inputs. Until then it shows
                  what the node holds, so a loaded file is legible. */}
              <h3>Properties</h3>
              {propertiesOf(selectedNode, specs).length === 0 ? (
                <p className="muted">This component takes none.</p>
              ) : (
                propertiesOf(selectedNode, specs).map((property) => (
                  <div className="field" key={property.name}>
                    <span title={property.help}>
                      {property.label}
                      {property.required && <b className="req">*</b>}
                    </span>
                    <code>{format(selectedNode.data.properties?.[property.name])}</code>
                  </div>
                ))
              )}
              <p className="muted small">Editing arrives in 7c.</p>
            </>
          )}
        </aside>
      </div>

      <section className="panel">
        <nav className="tabs">
          <button className={tab === "problems" ? "on" : ""} onClick={() => setTab("problems")}>
            Status
          </button>
          <button className={tab === "sql" ? "on" : ""} onClick={() => setTab("sql")}>
            SQL
          </button>
          <button className={tab === "data" ? "on" : ""} onClick={() => setTab("data")}>
            Data
          </button>
          <span className="grow" />
          {busy && <span className="muted">{busy}…</span>}
        </nav>

        <div className="panel-body">
          {toast && <p className="error">{toast}</p>}

          {tab === "problems" && (
            <Status document={document} validation={validation} run={run} />
          )}

          {tab === "sql" &&
            (sql ? <pre className="sql">{sql}</pre> : <p className="muted">Press SQL.</p>)}

          {tab === "data" && <Data preview={preview} />}
        </div>
      </section>
    </div>
  );
}

function Status({
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

      {validation?.valid && (
        <p className="ok">
          Valid — {validation.stageCount} stage(s), {validation.sinkCount} sink(s).
        </p>
      )}

      {validation?.warnings.map((warning) => (
        <p key={warning} className="warn">
          {warning}
        </p>
      ))}

      {run && (
        <>
          <table>
            <thead>
              <tr>
                <th>Stage</th>
                <th>Rows</th>
                <th>Component</th>
              </tr>
            </thead>
            <tbody>
              {run.stages.map((stage) => (
                <tr key={stage.nodeId}>
                  <td>{stage.label}</td>
                  <td>
                    {stage.skipped ?? stage.rows?.toLocaleString() ?? "—"}
                    {stage.rejected !== null && ` (+${stage.rejected} rejected)`}
                  </td>
                  <td className="muted">{stage.componentId}</td>
                </tr>
              ))}
            </tbody>
          </table>

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
            {run.failed ? "Run failed" : "Ran"} in {run.elapsedMs}ms
          </p>
        </>
      )}
    </>
  );
}

function Data({ preview }: { preview: PreviewResult | null }) {
  if (!preview) return <p className="muted">Select a node and press Preview.</p>;
  if (preview.rows.length === 0) return <p className="muted">No rows.</p>;

  return (
    <>
      <table>
        <thead>
          <tr>
            {preview.columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {preview.rows.map((row, index) => (
            <tr key={index}>
              {preview.columns.map((column) => (
                <td key={column}>{format(row[column])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted">
        {preview.nodeId} — {preview.rows.length} row(s)
        {preview.truncated && ", and there are more"}
      </p>
    </>
  );
}

/** Null is a value, and must not render as an empty cell. */
function format(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
