/**
 * The canvas.
 *
 * Holds no pipeline state of its own: the document lives in `App`, and this
 * turns it into what xyflow draws and turns xyflow's events back into edits.
 * Keeping the document as the single source of truth is what makes "the saved
 * JSON round-trips through the CLI unchanged" true by construction — there is
 * no second model that could drift from it.
 */

import { useCallback, useMemo, useRef } from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Edge,
  type Node,
  type NodeChange,
  type EdgeChange,
  type Connection as FlowConnection,
  type IsValidConnection,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { ComponentNode, type ComponentNodeData } from "./ComponentNode";
import {
  addEdge,
  addNode,
  moveNode,
  newNode,
  refuseConnection,
  removeEdges,
  removeNodes,
  REJECTED_PORT,
  type PipelineDoc,
} from "./document";
import type { ComponentSpec, StageResult } from "./ipc";

const NODE_TYPES = { component: ComponentNode };

export interface CanvasProps {
  document: PipelineDoc;
  specs: Map<string, ComponentSpec>;
  /** Node ids the last validation or run blamed, with what it said. */
  problems: Map<string, string>;
  /** What the last run reported, so the boxes can show it. */
  results: Map<string, StageResult>;
  selected: string | null;
  onChange: (next: PipelineDoc) => void;
  onSelect: (nodeId: string | null) => void;
  onRefused: (why: string) => void;
}

export function PipelineCanvas(props: CanvasProps) {
  return (
    <ReactFlowProvider>
      <Canvas {...props} />
    </ReactFlowProvider>
  );
}

function Canvas({
  document,
  specs,
  problems,
  results,
  selected,
  onChange,
  onSelect,
  onRefused,
}: CanvasProps) {
  const wrapper = useRef<HTMLDivElement>(null);
  const { screenToFlowPosition } = useReactFlow();

  const nodes: Node[] = useMemo(
    () =>
      document.nodes.map((node) => {
        const result = results.get(node.id);

        return {
          id: node.id,
          type: "component",
          position: node.position,
          selected: node.id === selected,
          data: {
            label: node.data.label,
            componentId: node.data.componentId,
            spec: specs.get(node.data.componentId ?? ""),
            disabled: node.data.disabled === true,
            problem: problems.get(node.id) ?? null,
            rows: result?.rows ?? null,
            rejected: result?.rejected ?? null,
          } satisfies ComponentNodeData,
        };
      }),
    [document.nodes, specs, problems, results, selected],
  );

  const edges: Edge[] = useMemo(
    () =>
      document.edges.map((edge) => {
        const fromReject = edge.sourceHandle === REJECTED_PORT;

        return {
          id: edge.id,
          source: edge.source,
          target: edge.target,
          sourceHandle: edge.sourceHandle ?? null,
          targetHandle: edge.targetHandle ?? null,
          // A dead-letter edge is drawn differently because it carries the rows
          // that failed, and mistaking one for the main flow is the kind of
          // misreading that costs an afternoon.
          //
          // Spread rather than `className: undefined`, which `tsconfig`'s
          // exactOptionalPropertyTypes refuses: absent and present-but-undefined
          // are different things, and xyflow's type says absent.
          ...(fromReject ? { className: "edge-reject" } : {}),
          animated: fromReject,
        };
      }),
    [document.edges],
  );

  /** xyflow asks this while the mouse is down, so a bad wire never snaps. */
  const isValidConnection: IsValidConnection = useCallback(
    (connection) =>
      refuseConnection(
        {
          source: connection.source,
          target: connection.target,
          sourceHandle: connection.sourceHandle ?? null,
          targetHandle: connection.targetHandle ?? null,
        },
        document,
        specs,
      ) === null,
    [document, specs],
  );

  const onConnect = useCallback(
    (connection: FlowConnection) => {
      const why = refuseConnection(
        {
          source: connection.source,
          target: connection.target,
          sourceHandle: connection.sourceHandle ?? null,
          targetHandle: connection.targetHandle ?? null,
        },
        document,
        specs,
      );

      if (why !== null) {
        onRefused(why);
        return;
      }

      onChange(
        addEdge(document, {
          source: connection.source,
          target: connection.target,
          sourceHandle: connection.sourceHandle ?? null,
          targetHandle: connection.targetHandle ?? null,
        }),
      );
    },
    [document, specs, onChange, onRefused],
  );

  /**
   * Only position and removal are taken from xyflow.
   *
   * Selection is handled separately and everything else — labels, properties,
   * wiring — is an edit to the document. Letting xyflow own any part of the
   * document would be the second model this design exists to avoid.
   */
  const onNodesChange = useCallback(
    (changes: NodeChange[]) => {
      let next = document;
      const removed = new Set<string>();

      for (const change of changes) {
        if (change.type === "position" && change.position) {
          next = moveNode(next, change.id, change.position);
        } else if (change.type === "remove") {
          removed.add(change.id);
        } else if (change.type === "select" && change.selected) {
          onSelect(change.id);
        }
      }

      if (removed.size > 0) {
        next = removeNodes(next, removed);
        if (selected && removed.has(selected)) onSelect(null);
      }

      if (next !== document) onChange(next);
    },
    [document, onChange, onSelect, selected],
  );

  const onEdgesChange = useCallback(
    (changes: EdgeChange[]) => {
      const removed = new Set(
        changes.flatMap((change) => (change.type === "remove" ? [change.id] : [])),
      );

      if (removed.size > 0) onChange(removeEdges(document, removed));
    },
    [document, onChange],
  );

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      const componentId = event.dataTransfer.getData("application/etl-component");
      const spec = specs.get(componentId);
      if (!spec) return;

      const position = screenToFlowPosition({ x: event.clientX, y: event.clientY });
      const taken = document.nodes.map((node) => node.id);

      const node = newNode(spec, { x: Math.round(position.x), y: Math.round(position.y) }, taken);

      onChange(addNode(document, node));
      onSelect(node.id);
    },
    [document, specs, screenToFlowPosition, onChange, onSelect],
  );

  return (
    <div className="canvas" ref={wrapper}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={NODE_TYPES}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        isValidConnection={isValidConnection}
        onPaneClick={() => onSelect(null)}
        onDrop={onDrop}
        onDragOver={(event) => {
          event.preventDefault();
          event.dataTransfer.dropEffect = "copy";
        }}
        fitView
        proOptions={{ hideAttribution: false }}
        deleteKeyCode={["Delete", "Backspace"]}
      >
        <Background gap={16} size={1} />
        <Controls showInteractive={false} />
        <MiniMap pannable zoomable />
      </ReactFlow>
    </div>
  );
}
