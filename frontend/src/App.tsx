/**
 * The studio.
 *
 * Owns the document and nothing else owns any part of it. The canvas draws it,
 * the palette adds to it, the engine is asked about it — but there is one copy,
 * and it is the same shape the CLI reads. That is what makes the round-trip
 * promise structural rather than something to remember.
 *
 * The bottom panel lives in `RunView.tsx`; this file owns the state it reads.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { open as openDialog, save as saveDialog } from "@tauri-apps/plugin-dialog";

import { Inspector } from "./Inspector";
import { Palette } from "./Palette";
import { DataTab, PlanTab, StatusTab } from "./RunView";
import { PipelineCanvas, nextPosition } from "./PipelineCanvas";
import {
  addNode,
  emptyDocument,
  newNode,
  parseDocument,
  serializeDocument,
  specsById,
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
  type PlanView,
  type PreviewResult,
  type RunResult,
  type StageResult,
  type Validation,
} from "./ipc";

type Tab = "status" | "plan" | "data";

export default function App() {
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [document, setDocument] = useState<PipelineDoc>(emptyDocument);
  const [path, setPath] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);

  const [selected, setSelected] = useState<string | null>(null);
  const [validation, setValidation] = useState<Validation | null>(null);
  const [run, setRun] = useState<RunResult | null>(null);
  const [preview, setPreview] = useState<PreviewResult | null>(null);
  const [plan, setPlan] = useState<PlanView | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("status");
  // Whether the Plan tab shows one script or one block per stage.
  const [wholeScript, setWholeScript] = useState(false);

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
      setPlan(null);
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
      setTab("status");
    });

  const onCompile = () =>
    act("compiling", async () => {
      setPlan(await compilePipeline(serializeDocument(document)));
      setTab("plan");
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
          Plan
        </button>
        <button onClick={onPreview} disabled={!selected}>
          Preview
        </button>
        <button className="primary" onClick={onRun} disabled={document.nodes.length === 0}>
          Run
        </button>
      </header>

      <div className="body">
        <Palette
          manifest={manifest}
          onAdd={(componentId) => {
            const spec = specs.get(componentId);
            if (!spec) return;

            const node = newNode(
              spec,
              nextPosition(document.nodes.length),
              document.nodes.map((existing) => existing.id),
            );

            edit(addNode(document, node));
            setSelected(node.id);
          }}
        />

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

        <Inspector
          document={document}
          node={selectedNode}
          specs={specs}
          onChange={edit}
          onRenamed={setSelected}
          onError={setToast}
        />
      </div>

      <section className="panel">
        <nav className="tabs">
          <button className={tab === "status" ? "on" : ""} onClick={() => setTab("status")}>
            Status
          </button>
          <button className={tab === "plan" ? "on" : ""} onClick={() => setTab("plan")}>
            Plan
          </button>
          <button className={tab === "data" ? "on" : ""} onClick={() => setTab("data")}>
            Data
          </button>
          <span className="grow" />
          {busy && <span className="muted">{busy}…</span>}
        </nav>

        <div className="panel-body">
          {toast && <p className="error">{toast}</p>}

          {tab === "status" && (
            <StatusTab document={document} validation={validation} run={run} />
          )}

          {tab === "plan" && (
            <PlanTab
              plan={plan}
              whole={wholeScript}
              onToggleWhole={() => setWholeScript((was) => !was)}
              onSelect={setSelected}
            />
          )}

          {tab === "data" && <DataTab preview={preview} />}
        </div>
      </section>
    </div>
  );
}
