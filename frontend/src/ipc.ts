/**
 * The bridge to the engine.
 *
 * Every type here mirrors a `#[derive(Serialize)]` struct in
 * `apps/desktop/src/main.rs`. That file is the source of truth; this one exists
 * so the rest of the app never calls `invoke` directly and never spells a
 * command name twice.
 *
 * There is deliberately no component list, no property schema, and no SQL
 * knowledge on this side. All of it arrives from `listComponents()`, because
 * with ~400 components to reach, a second copy in TypeScript would be a second
 * thing to keep right.
 */

import { invoke } from "@tauri-apps/api/core";

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/** Which step failed, so the UI can say what kind of problem this is. */
export type ErrorStage = "read" | "resolve" | "compile" | "run";

export interface IpcError {
  message: string;
  /** The node at fault, when the engine could name one. */
  nodeId: string | null;
  stage: ErrorStage;
}

export interface StageView {
  nodeId: string;
  componentId: string;
  label: string;
  kind: string;
  sql: string;
  from: string | null;
  /** True for a quality node, which has a second `rejected` output. */
  splits: boolean;
  needsSession: boolean;
}

export interface PlanView {
  stages: StageView[];
  warnings: string[];
  extensions: string[];
  script: string;
  needsSession: boolean;
  sessionReasons: string[];
}

export interface Validation {
  valid: boolean;
  error: IpcError | null;
  stageCount: number;
  sinkCount: number;
  warnings: string[];
}

export interface StageResult {
  nodeId: string;
  label: string;
  componentId: string;
  rows: number | null;
  /** Rows a quality node sent to its dead-letter output. */
  rejected: number | null;
  /** Why this stage did not run, if it did not. */
  skipped: string | null;
  /**
   * How long the stage took, when the engine was willing to say.
   *
   * `null` for most stages on most runs, and that is the engine being honest
   * rather than incomplete: a lazy view is registered in microseconds and
   * computed later by whatever reads it, so a `0 ms` beside it would credit
   * the wrong stage. Render nothing for `null` — not a zero, not a dash.
   */
  elapsedMs: number | null;
}

export interface RunResult {
  stages: StageResult[];
  elapsedMs: number;
  notes: string[];
  failures: string[];
  /** A run can reach the end and still have failed. */
  failed: boolean;
  script: string;
}

export interface PreviewResult {
  nodeId: string;
  columns: string[];
  rows: Record<string, unknown>[];
  truncated: boolean;
}

export interface PortSpec {
  name: string;
  label: string;
  help?: string;
}

export type PropertyType =
  | "text"
  | "path"
  | "sql"
  | "code"
  | "bool"
  | "integer"
  | "number"
  | "string_list"
  | "map"
  | "enum";

export interface PropertySpec {
  name: string;
  label: string;
  type: PropertyType;
  required?: boolean;
  default?: unknown;
  help?: string;
  options?: string[];
}

export type Namespace = "source" | "transform" | "sink" | "quality" | "control" | "code";

export interface ComponentSpec {
  id: string;
  namespace: Namespace;
  label: string;
  description?: string;
  icon?: string;
  inputs: PortSpec[];
  outputs: PortSpec[];
  properties: PropertySpec[];
  requiresExtensions?: string[];
}

export interface Manifest {
  formatVersion: number;
  components: ComponentSpec[];
}

/**
 * Where relative paths resolve from, which context is active, and what the
 * parameters are bound to — the same three knobs the CLI takes.
 */
export interface Settings {
  workspace?: string;
  context?: string;
  params?: [string, string][];
}

// ---------------------------------------------------------------------------
// Calling
// ---------------------------------------------------------------------------

/**
 * Whether we are running inside the desktop shell.
 *
 * `npm run dev` in an ordinary browser has no Tauri to talk to. Detecting it
 * lets the app say so, rather than rendering an empty page and leaving someone
 * to work out why nothing loads.
 */
export function inDesktopShell(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

/** Anything thrown by a command, normalised to the shape the UI renders. */
export function asIpcError(thrown: unknown): IpcError {
  if (typeof thrown === "object" && thrown !== null && "message" in thrown) {
    const candidate = thrown as Partial<IpcError>;
    return {
      message: String(candidate.message),
      nodeId: candidate.nodeId ?? null,
      stage: candidate.stage ?? "run",
    };
  }

  return { message: String(thrown), nodeId: null, stage: "run" };
}

function call<T>(command: string, args: Record<string, unknown>): Promise<T> {
  if (!inDesktopShell()) {
    return Promise.reject({
      message:
        "This page is not running inside the desktop shell, so there is no engine to call. " +
        "Start it with `npm run tauri dev` from apps/desktop.",
      nodeId: null,
      stage: "read",
    } satisfies IpcError);
  }

  return invoke<T>(command, args);
}

/** Every component the engine knows. The palette and property panels are generated from this. */
export function listComponents(): Promise<Manifest> {
  return call<Manifest>("list_components", {});
}

/** Compile without running: touches no files and spawns nothing. */
export function compilePipeline(document: string, settings: Settings = {}): Promise<PlanView> {
  return call<PlanView>("compile_pipeline", { document, settings });
}

/**
 * Check a document. Note this resolves rather than rejects when the document is
 * invalid — a pipeline under construction is invalid most of the time, and that
 * is not an exceptional condition.
 */
export function validatePipeline(document: string, settings: Settings = {}): Promise<Validation> {
  return call<Validation>("validate_pipeline", { document, settings });
}

/** Run the pipeline for real. */
export function runPipeline(document: string, settings: Settings = {}): Promise<RunResult> {
  return call<RunResult>("run_pipeline", { document, settings });
}

/** Read a pipeline document off disk. The path comes from the dialog plugin. */
export function readPipeline(path: string): Promise<string> {
  return call<string>("read_pipeline", { path });
}

/** Write a pipeline document to disk. Refuses anything that will not load back. */
export function writePipeline(path: string, document: string): Promise<void> {
  return call<void>("write_pipeline", { path, document });
}

/** Read the rows one node produces, without running the rest or writing anything. */
export function previewNode(
  document: string,
  nodeId: string,
  limit = 50,
  settings: Settings = {},
): Promise<PreviewResult> {
  return call<PreviewResult>("preview_node", { document, nodeId, limit, settings });
}
