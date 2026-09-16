/**
 * The pipeline document, as the canvas holds it.
 *
 * These types mirror `crates/metadata/src/lib.rs`. The rule that matters more
 * than any of them: **a document is the user's file, not our scratch space.**
 * Every struct in the Rust model carries an `extra` catch-all so a field written
 * by a newer version survives a load/save cycle, and this side has to honour the
 * same promise — so nodes and edges are edited by copying and replacing named
 * fields, never by rebuilding them from a known list of keys.
 *
 * That is what makes "the saved JSON round-trips through the CLI unchanged"
 * achievable rather than aspirational.
 */

import type { ComponentSpec, Manifest, PropertySpec } from "./ipc";

/** The relation-name suffix the engine reserves for a quality node's rejects. */
export const REJECT_SUFFIX = "__rejected";

/** The handle name of a quality node's dead-letter output. */
export const REJECTED_PORT = "rejected";

/** The handle every other output uses. */
export const MAIN_PORT = "main";

export interface Position {
  x: number;
  y: number;
}

export interface NodePolicy {
  retryAttempts?: number;
  retryBackoffMs?: number;
  continueOnFailure?: boolean;
  memoryLimitMb?: number;
}

export interface NodeData {
  label: string;
  componentId?: string;
  properties?: Record<string, unknown>;
  disabled?: boolean;
  materialize?: string;
  alias?: string;
  policy?: NodePolicy;
  /** Anything a newer version wrote. Preserved untouched. */
  [key: string]: unknown;
}

export interface PipelineNode {
  id: string;
  type?: string;
  position: Position;
  data: NodeData;
  [key: string]: unknown;
}

export interface PipelineEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
  targetHandle?: string;
  [key: string]: unknown;
}

export interface PipelineDoc {
  formatVersion: number;
  name?: string;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  [key: string]: unknown;
}

export function emptyDocument(): PipelineDoc {
  return { formatVersion: 1, nodes: [], edges: [] };
}

// ---------------------------------------------------------------------------
// Naming
// ---------------------------------------------------------------------------

/**
 * The canvas node type for a namespace.
 *
 * Purely how the canvas draws it — what a node *does* is its `componentId`.
 * Kept in the document because the Rust model has the field and round-tripping
 * means not dropping it.
 */
export function flowTypeFor(namespace: ComponentSpec["namespace"]): string {
  switch (namespace) {
    case "source":
      return "source";
    case "sink":
      return "sink";
    default:
      return "transform";
  }
}

/**
 * A readable, unique node id derived from a component.
 *
 * `src.file.csv` becomes `csv`, then `csv_2` if that is taken. Readable because
 * the id is what appears in the generated SQL — `FROM "csv"` reads better than
 * `FROM "n_1a2b3c"`, and the Plan tab shows that SQL to a person.
 *
 * The reserved suffix is refused outright: the engine rejects a node id ending
 * in `__rejected`, because it would collide with the relation a quality node
 * gives its dead-letter rows.
 */
export function newNodeId(componentId: string, taken: Iterable<string>): string {
  const used = new Set(taken);

  const base =
    componentId
      .split(".")
      .pop()
      ?.replace(/[^a-z0-9_]/gi, "_") || "node";

  const safe = base.endsWith(REJECT_SUFFIX) ? `${base}_node` : base;

  if (!used.has(safe)) return safe;

  for (let n = 2; ; n += 1) {
    const candidate = `${safe}_${n}`;
    if (!used.has(candidate)) return candidate;
  }
}

/** Whether an id is one the engine will refuse. */
export function isReservedId(id: string): boolean {
  return id.endsWith(REJECT_SUFFIX);
}

// ---------------------------------------------------------------------------
// Building
// ---------------------------------------------------------------------------

/**
 * The properties a new node starts with: every default the spec declares.
 *
 * Defaults are copied in rather than left absent so the document says what it
 * will do. A property that is required and has no default is deliberately left
 * out — the engine will name it, which is a better prompt than a blank string
 * that looks filled in.
 */
export function defaultProperties(spec: ComponentSpec): Record<string, unknown> {
  const properties: Record<string, unknown> = {};

  for (const property of spec.properties) {
    if (property.default !== undefined && property.default !== null) {
      properties[property.name] = property.default;
    }
  }

  return properties;
}

export function newNode(
  spec: ComponentSpec,
  position: Position,
  taken: Iterable<string>,
): PipelineNode {
  return {
    id: newNodeId(spec.id, taken),
    type: flowTypeFor(spec.namespace),
    position,
    data: {
      label: spec.label,
      componentId: spec.id,
      properties: defaultProperties(spec),
    },
  };
}

// ---------------------------------------------------------------------------
// Wiring rules
// ---------------------------------------------------------------------------

export interface Connection {
  source: string;
  target: string;
  sourceHandle?: string | null;
  targetHandle?: string | null;
}

/** Why a connection was refused, in words the canvas can show. */
export type Refusal = string | null;

/**
 * Whether an edge may be drawn, and if not, why.
 *
 * This deliberately mirrors what the engine checks rather than inventing
 * stricter rules of its own. Anything refused here would have been an error at
 * compile time; the point is to say so while the mouse is still down, not to
 * have a second opinion about what a valid pipeline is.
 */
export function refuseConnection(
  connection: Connection,
  document: PipelineDoc,
  specs: Map<string, ComponentSpec>,
): Refusal {
  const { source, target } = connection;

  if (source === target) {
    return "A node cannot feed itself.";
  }

  const sourceNode = document.nodes.find((node) => node.id === source);
  const targetNode = document.nodes.find((node) => node.id === target);

  if (!sourceNode || !targetNode) return "That node is not on the canvas.";

  const sourceSpec = specs.get(sourceNode.data.componentId ?? "");
  const targetSpec = specs.get(targetNode.data.componentId ?? "");

  if (!sourceSpec || !targetSpec) return "That component is not in the registry.";

  const sourceHandle = connection.sourceHandle ?? MAIN_PORT;
  const targetHandle = connection.targetHandle ?? targetSpec.inputs[0]?.name;

  if (sourceSpec.outputs.length === 0) {
    return `${sourceSpec.label} writes data out; it has nothing to pass on.`;
  }

  if (targetSpec.inputs.length === 0) {
    return `${targetSpec.label} reads its own data; it takes no input.`;
  }

  if (!sourceSpec.outputs.some((port) => port.name === sourceHandle)) {
    return `${sourceSpec.label} has no '${sourceHandle}' output.`;
  }

  if (!targetHandle || !targetSpec.inputs.some((port) => port.name === targetHandle)) {
    return `${targetSpec.label} has no '${targetHandle}' input.`;
  }

  // One edge per input port. The engine counts inputs against the declared
  // ports, so a second edge into the same port is a WrongInputCount later.
  const occupied = document.edges.some(
    (edge) =>
      edge.target === target &&
      (edge.targetHandle ?? targetSpec.inputs[0]?.name) === targetHandle,
  );

  if (occupied) {
    return `The '${targetHandle}' input of ${targetSpec.label} is already wired.`;
  }

  if (wouldCycle(source, target, document)) {
    return "That would make a loop, and data has to flow one way.";
  }

  return null;
}

/** Whether adding source → target closes a cycle. */
function wouldCycle(source: string, target: string, document: PipelineDoc): boolean {
  // Walk forward from the proposed target; reaching the source means a loop.
  const seen = new Set<string>();
  const frontier = [target];

  while (frontier.length > 0) {
    const current = frontier.pop();
    if (current === undefined || seen.has(current)) continue;
    seen.add(current);

    if (current === source) return true;

    for (const edge of document.edges) {
      if (edge.source === current) frontier.push(edge.target);
    }
  }

  return false;
}

// ---------------------------------------------------------------------------
// Editing, without losing what we do not understand
// ---------------------------------------------------------------------------

export function addNode(document: PipelineDoc, node: PipelineNode): PipelineDoc {
  return { ...document, nodes: [...document.nodes, node] };
}

export function removeNodes(document: PipelineDoc, ids: Set<string>): PipelineDoc {
  return {
    ...document,
    nodes: document.nodes.filter((node) => !ids.has(node.id)),
    // An edge to a node that is gone would be an UnknownEdgeEndpoint.
    edges: document.edges.filter((edge) => !ids.has(edge.source) && !ids.has(edge.target)),
  };
}

export function moveNode(document: PipelineDoc, id: string, position: Position): PipelineDoc {
  return {
    ...document,
    nodes: document.nodes.map((node) => (node.id === id ? { ...node, position } : node)),
  };
}

export function addEdge(document: PipelineDoc, connection: Connection): PipelineDoc {
  const edge: PipelineEdge = {
    id: newEdgeId(document),
    source: connection.source,
    target: connection.target,
    sourceHandle: connection.sourceHandle ?? MAIN_PORT,
    ...(connection.targetHandle ? { targetHandle: connection.targetHandle } : {}),
  };

  return { ...document, edges: [...document.edges, edge] };
}

export function removeEdges(document: PipelineDoc, ids: Set<string>): PipelineDoc {
  return { ...document, edges: document.edges.filter((edge) => !ids.has(edge.id)) };
}

/** Replace one node's data, keeping every key we did not set. */
export function updateNodeData(
  document: PipelineDoc,
  id: string,
  change: Partial<NodeData>,
): PipelineDoc {
  return {
    ...document,
    nodes: document.nodes.map((node) =>
      node.id === id ? { ...node, data: { ...node.data, ...change } } : node,
    ),
  };
}

function newEdgeId(document: PipelineDoc): string {
  const used = new Set(document.edges.map((edge) => edge.id));

  for (let n = 1; ; n += 1) {
    const candidate = `e${n}`;
    if (!used.has(candidate)) return candidate;
  }
}

// ---------------------------------------------------------------------------
// Reading and writing
// ---------------------------------------------------------------------------

/** Parse a document, with a message worth showing if it is not one. */
export function parseDocument(text: string): PipelineDoc {
  const value: unknown = JSON.parse(text);

  if (typeof value !== "object" || value === null) {
    throw new Error("A pipeline document is a JSON object.");
  }

  const doc = value as Partial<PipelineDoc>;

  if (!Array.isArray(doc.nodes) || !Array.isArray(doc.edges)) {
    throw new Error("A pipeline document needs a 'nodes' array and an 'edges' array.");
  }

  return { formatVersion: 1, ...doc, nodes: doc.nodes, edges: doc.edges };
}

/**
 * The document as it goes to disk.
 *
 * Two spaces and a trailing newline, matching the samples in the repo, so a
 * file saved from the canvas and one written by hand do not differ by
 * whitespace in every diff.
 */
export function serializeDocument(document: PipelineDoc): string {
  return `${JSON.stringify(document, null, 2)}\n`;
}

export function specsById(manifest: Manifest | null): Map<string, ComponentSpec> {
  return new Map((manifest?.components ?? []).map((spec) => [spec.id, spec]));
}

/** The property schema for a node, for the panel 7c generates. */
export function propertiesOf(
  node: PipelineNode,
  specs: Map<string, ComponentSpec>,
): PropertySpec[] {
  return specs.get(node.data.componentId ?? "")?.properties ?? [];
}
