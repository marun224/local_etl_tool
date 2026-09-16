/**
 * One node on the canvas.
 *
 * Every handle it draws comes from the component's spec, so a quality node gets
 * its second `rejected` output and a join gets its two named inputs without
 * this file knowing either exists. There is no `if (componentId === ...)`
 * anywhere here, and there must not be: with ~400 components to reach, a node
 * that renders per-component is a node nobody can add to.
 */

import { Handle, Position, type NodeProps } from "@xyflow/react";
import { iconFor } from "./icons";
import { REJECTED_PORT } from "./document";
import type { ComponentSpec } from "./ipc";

export interface ComponentNodeData extends Record<string, unknown> {
  label: string;
  spec: ComponentSpec | undefined;
  componentId: string | undefined;
  disabled: boolean;
  /** Set when the last validation blamed this node. */
  problem: string | null;
  /** Rows the last run reported, so the canvas can show what happened. */
  rows: number | null;
  rejected: number | null;
}

function Glyph({ name }: { name: string | undefined }) {
  const Icon = iconFor(name);
  return <Icon size={14} strokeWidth={2} aria-hidden />;
}

export function ComponentNode({ data, selected }: NodeProps) {
  const node = data as ComponentNodeData;
  const spec = node.spec;

  const inputs = spec?.inputs ?? [];
  const outputs = spec?.outputs ?? [];

  const classes = [
    "node",
    `node-${spec?.namespace ?? "unknown"}`,
    selected ? "is-selected" : "",
    node.disabled ? "is-disabled" : "",
    node.problem ? "has-problem" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={classes} title={node.problem ?? spec?.description ?? ""}>
      {inputs.map((port, index) => (
        <Handle
          key={port.name}
          type="target"
          position={Position.Left}
          id={port.name}
          className="port port-in"
          style={{ top: portOffset(index, inputs.length) }}
          title={port.label}
        />
      ))}

      <div className="node-head">
        <Glyph name={spec?.icon} />
        <span className="node-label">{node.label}</span>
      </div>

      <div className="node-sub">{node.componentId ?? "no component"}</div>

      {(node.rows !== null || node.rejected !== null) && (
        <div className="node-rows">
          {node.rows !== null && <span>{node.rows.toLocaleString()} rows</span>}
          {node.rejected !== null && (
            <span className="node-rejected">{node.rejected.toLocaleString()} rejected</span>
          )}
        </div>
      )}

      {outputs.map((port, index) => (
        <Handle
          key={port.name}
          type="source"
          position={Position.Right}
          id={port.name}
          className={`port port-out ${port.name === REJECTED_PORT ? "port-reject" : ""}`}
          style={{ top: portOffset(index, outputs.length) }}
          title={port.label}
        />
      ))}

      {/* A second output needs saying which is which; one does not. */}
      {outputs.length > 1 && (
        <div className="port-labels">
          {outputs.map((port, index) => (
            <span
              key={port.name}
              className={port.name === REJECTED_PORT ? "reject" : ""}
              style={{ top: portOffset(index, outputs.length) }}
            >
              {port.name}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

/** Spread `count` handles down the side of the node. */
function portOffset(index: number, count: number): string {
  if (count <= 1) return "50%";
  return `${((index + 1) / (count + 1)) * 100}%`;
}
