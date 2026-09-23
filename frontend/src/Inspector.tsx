/**
 * The property panel.
 *
 * Generated entirely from the component's property schema: there are nine
 * renderers here, one per `PropertyType`, and no knowledge of any component.
 * Adding a component to the registry gives it a working form with no change to
 * this file — which is the point, because there are ~400 of them to reach and a
 * per-component panel is a panel nobody can add to.
 *
 * The node's own fields — name, label, alias, materialize, disabled — sit above
 * the properties, and the stage policy sits below them. They are how a node is
 * *run* rather than what its component does, and every component has them, so
 * they are still not per-component.
 */

import { useEffect, useState } from "react";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { Plus, Trash2 } from "lucide-react";

import { propertiesOf, type PipelineDoc, type PipelineNode } from "./document";
import {
  asList,
  asPairs,
  fromList,
  fromPairs,
  fromText,
  isMissing,
  isSet,
  MATERIALIZE,
  POLICY_FIELDS,
  renameNode,
  setNodeField,
  setPolicyField,
  setProperty,
  toText,
  valueOf,
  type Pairs,
} from "./properties";
import type { ComponentSpec, PropertySpec } from "./ipc";

export interface InspectorProps {
  document: PipelineDoc;
  node: PipelineNode | null;
  specs: Map<string, ComponentSpec>;
  onChange: (next: PipelineDoc) => void;
  onRenamed: (id: string) => void;
  onError: (message: string) => void;
}

export function Inspector({
  document,
  node,
  specs,
  onChange,
  onRenamed,
  onError,
}: InspectorProps) {
  if (node === null) {
    return (
      <aside className="inspector">
        <p className="muted">Select a node to edit it.</p>
      </aside>
    );
  }

  const spec = specs.get(node.data.componentId ?? "");
  const properties = propertiesOf(node, specs);

  const set = (name: string, value: unknown) =>
    onChange(setProperty(document, node.id, name, value));

  const setField = (field: string, value: unknown) =>
    onChange(setNodeField(document, node.id, field, value));

  const setPolicy = (field: (typeof POLICY_FIELDS)[number], value: number | boolean | undefined) =>
    onChange(setPolicyField(document, node.id, field, value));

  return (
    <aside className="inspector">
      <h3>Node</h3>

      <NameField
        value={node.id}
        onCommit={(next) => {
          const result = renameNode(document, node.id, next);

          if ("error" in result) {
            onError(result.error);
            return false;
          }

          onChange(result.document);
          onRenamed(next.trim());
          return true;
        }}
      />

      <Text
        label="Label"
        help="What the box says on the canvas. The name above is what the SQL uses."
        value={node.data.label}
        onChange={(text) => setField("label", text === "" ? node.id : text)}
      />

      <Text
        label="Alias"
        help="A friendlier relation name, so a raw SQL node downstream can say FROM orders."
        value={toText(node.data.alias)}
        onChange={(text) => setField("alias", text.trim() === "" ? undefined : text.trim())}
      />

      {spec && spec.namespace !== "sink" && (
        <Choice
          label="Materialize"
          help={MATERIALIZE.find((mode) => mode.value === (node.data.materialize ?? "auto"))?.help}
          value={toText(node.data.materialize) || "auto"}
          options={MATERIALIZE.map((mode) => ({ value: mode.value, label: mode.label }))}
          onChange={(value) => setField("materialize", value === "auto" ? undefined : value)}
        />
      )}

      <Toggle
        label="Disabled"
        help="Switched-off nodes are skipped, and so is anything downstream of them."
        value={node.data.disabled === true}
        onChange={(on) => setField("disabled", on ? true : undefined)}
      />

      <h3>{spec?.label ?? node.data.componentId ?? "Unknown component"}</h3>

      {spec?.description && <p className="muted small">{spec.description}</p>}

      {spec === undefined ? (
        <p className="error small">
          This component is not in the registry, so there is nothing to configure.
        </p>
      ) : properties.length === 0 ? (
        <p className="muted small">This component takes no properties.</p>
      ) : (
        properties.map((property) => (
          <Field
            key={property.name}
            spec={property}
            properties={node.data.properties}
            onChange={(value) => set(property.name, value)}
          />
        ))
      )}

      <Policy node={node} onChange={setPolicy} />
    </aside>
  );
}

/**
 * What this stage does when it fails.
 *
 * Kept below the properties because it is about failure rather than about what
 * the node does, and most nodes never need it. The note at the bottom is not
 * decoration: setting any of these moves the whole pipeline onto the session
 * transport, which is a real change in how it runs, and finding that out from
 * the Plan tab afterwards is finding out too late.
 */
function Policy({
  node,
  onChange,
}: {
  node: PipelineNode;
  onChange: (
    field: (typeof POLICY_FIELDS)[number],
    value: number | boolean | undefined,
  ) => void;
}) {
  const policy = node.data.policy ?? {};
  const set = POLICY_FIELDS.some((field) => policy[field] !== undefined);

  return (
    <>
      <h3>
        When it fails
        {set && <span className="badge">session</span>}
      </h3>

      <Number
        label="Retry attempts"
        help="Extra attempts after the first. Left unset, the stage runs once."
        value={policy.retryAttempts}
        min={0}
        onChange={(value) => onChange("retryAttempts", value)}
      />

      <Number
        label="Retry backoff (ms)"
        help="How long to wait before the first retry, doubling each time after."
        value={policy.retryBackoffMs}
        min={0}
        onChange={(value) => onChange("retryBackoffMs", value)}
      />

      <Toggle
        label="Continue on failure"
        help="The rest of the run goes on; stages reading this one are skipped. The run still ends failed."
        value={policy.continueOnFailure === true}
        onChange={(on) => onChange("continueOnFailure", on ? true : undefined)}
      />

      <Number
        label="Memory limit (MB)"
        help="A ceiling set around this stage and put back afterwards."
        value={policy.memoryLimitMb}
        min={1}
        onChange={(value) => onChange("memoryLimitMb", value)}
      />

      {set && (
        <p className="muted small">
          Any of these puts the whole pipeline on the session transport: one DuckDB process
          held open, stages sent one at a time. That is what makes a retry possible, and it is
          also what lets the run report per-stage timings.
        </p>
      )}
    </>
  );
}

/**
 * A whole number, or nothing.
 *
 * Clearing the box removes the field rather than writing a zero, and a value
 * that will not parse is not written at all — the same rule the generated
 * fields follow, for the same reason: a typo must never become a value.
 */
function Number({
  label,
  help,
  value,
  min,
  onChange,
}: {
  label: string;
  help: string;
  value: number | undefined;
  min: number;
  onChange: (value: number | undefined) => void;
}) {
  return (
    <div className="field">
      <span title={help}>{label}</span>
      <input
        type="number"
        min={min}
        value={value ?? ""}
        onChange={(event) => {
          const text = event.target.value.trim();
          if (text === "") return onChange(undefined);

          const parsed = globalThis.Number(text);
          if (!globalThis.Number.isInteger(parsed) || parsed < min) return;

          onChange(parsed);
        }}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// One property, by type
// ---------------------------------------------------------------------------

function Field({
  spec,
  properties,
  onChange,
}: {
  spec: PropertySpec;
  properties: Record<string, unknown> | undefined;
  onChange: (value: unknown) => void;
}) {
  const value = valueOf(properties, spec);
  const missing = isMissing(properties, spec);

  // A value that is only there because the spec says so is worth marking: it
  // reads the same as one somebody chose, and the difference matters when you
  // are working out why a run did what it did.
  const defaulted = !isSet(properties, spec) && spec.default !== undefined;

  const label = (
    <span title={spec.help}>
      {spec.label}
      {spec.required && <b className="req">*</b>}
      {defaulted && <em className="default-tag">default</em>}
    </span>
  );

  return (
    <div className={`field ${missing ? "is-missing" : ""}`}>
      {label}
      <Control spec={spec} value={value} onChange={onChange} />
      {spec.help && <p className="help">{spec.help}</p>}
      {missing && <p className="error small">Required.</p>}
    </div>
  );
}

function Control({
  spec,
  value,
  onChange,
}: {
  spec: PropertySpec;
  value: unknown;
  onChange: (value: unknown) => void;
}) {
  switch (spec.type) {
    case "bool":
      return (
        <input
          type="checkbox"
          className="check"
          checked={value === true}
          onChange={(event) => onChange(event.target.checked)}
        />
      );

    case "enum":
      return (
        <select value={toText(value)} onChange={(event) => onChange(event.target.value)}>
          {/* An optional enum needs a way back to unset, which is not one of
              its own options. */}
          {!spec.required && <option value="">—</option>}
          {(spec.options ?? []).map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      );

    case "sql":
    case "code":
      return (
        <textarea
          className="code"
          rows={3}
          spellCheck={false}
          value={toText(value)}
          onChange={(event) => onChange(fromText(spec.type, event.target.value))}
        />
      );

    case "path":
      return <PathField value={toText(value)} onChange={(text) => onChange(fromText("path", text))} />;

    case "integer":
    case "number":
      return (
        <input
          type="number"
          step={spec.type === "integer" ? 1 : "any"}
          value={toText(value)}
          onChange={(event) => onChange(fromText(spec.type, event.target.value))}
        />
      );

    case "string_list":
      return <ListField items={asList(value)} onChange={(items) => onChange(fromList(items))} />;

    case "map":
      return <MapField pairs={asPairs(value)} onChange={(pairs) => onChange(fromPairs(pairs))} />;

    default:
      return (
        <input
          value={toText(value)}
          onChange={(event) => onChange(fromText(spec.type, event.target.value))}
        />
      );
  }
}

// ---------------------------------------------------------------------------
// The controls that need more than an input
// ---------------------------------------------------------------------------

function PathField({ value, onChange }: { value: string; onChange: (text: string) => void }) {
  return (
    <div className="row">
      <input value={value} onChange={(event) => onChange(event.target.value)} />
      <button
        className="tiny"
        title="Pick a file"
        onClick={async () => {
          const picked = await openDialog({ multiple: false });
          if (typeof picked === "string") onChange(picked);
        }}
      >
        …
      </button>
    </div>
  );
}

/**
 * A list of names — the columns a validator checks, the keys a join uses.
 *
 * Edited as a list rather than as comma-separated text so a value containing a
 * comma is possible, and so the order is visibly the order.
 */
function ListField({
  items,
  onChange,
}: {
  items: string[];
  onChange: (items: string[]) => void;
}) {
  const replace = (index: number, text: string) =>
    onChange(items.map((item, at) => (at === index ? text : item)));

  return (
    <div className="list">
      {items.map((item, index) => (
        <div className="row" key={index}>
          <input value={item} onChange={(event) => replace(index, event.target.value)} />
          <button
            className="tiny"
            title="Remove"
            onClick={() => onChange(items.filter((_, at) => at !== index))}
          >
            <Trash2 size={12} />
          </button>
        </div>
      ))}

      <button className="tiny wide" onClick={() => onChange([...items, ""])}>
        <Plus size={12} /> add
      </button>
    </div>
  );
}

/**
 * Ordered name/value pairs — a rename map, a cast map.
 *
 * Kept in local state while being edited, because a half-typed key would
 * otherwise vanish from the document the moment it was blank, taking its value
 * with it.
 */
function MapField({ pairs, onChange }: { pairs: Pairs; onChange: (pairs: Pairs) => void }) {
  const [draft, setDraft] = useState<Pairs>(pairs);

  // Follow the document when it changes underneath — a different node selected,
  // or a file opened — without fighting the person typing into this one.
  useEffect(() => {
    setDraft((held) => (sameShape(held, pairs) ? held : pairs));
  }, [pairs]);

  const commit = (next: Pairs) => {
    setDraft(next);
    onChange(next);
  };

  return (
    <div className="list">
      {draft.map(([key, value], index) => (
        <div className="row" key={index}>
          <input
            className="key"
            placeholder="from"
            value={key}
            onChange={(event) =>
              commit(draft.map((pair, at) => (at === index ? [event.target.value, pair[1]] : pair)))
            }
          />
          <input
            placeholder="to"
            value={value}
            onChange={(event) =>
              commit(draft.map((pair, at) => (at === index ? [pair[0], event.target.value] : pair)))
            }
          />
          <button
            className="tiny"
            title="Remove"
            onClick={() => commit(draft.filter((_, at) => at !== index))}
          >
            <Trash2 size={12} />
          </button>
        </div>
      ))}

      <button className="tiny wide" onClick={() => commit([...draft, ["", ""]])}>
        <Plus size={12} /> add
      </button>
    </div>
  );
}

function sameShape(a: Pairs, b: Pairs): boolean {
  return (
    a.length === b.length &&
    a.every((pair, index) => pair[0] === b[index]?.[0] && pair[1] === b[index]?.[1])
  );
}

// ---------------------------------------------------------------------------
// Node-level fields
// ---------------------------------------------------------------------------

/**
 * The node's name.
 *
 * Committed on blur or Enter rather than on every keystroke, because a rename
 * rewrites every edge that refers to it and doing that per character would
 * churn the document and lose the half-typed name to a collision check.
 */
function NameField({
  value,
  onCommit,
}: {
  value: string;
  onCommit: (next: string) => boolean;
}) {
  const [draft, setDraft] = useState(value);

  useEffect(() => setDraft(value), [value]);

  const commit = () => {
    if (draft === value) return;
    if (!onCommit(draft)) setDraft(value);
  };

  return (
    <div className="field">
      <span title="The relation name in the generated SQL, and what every edge refers to.">
        Name
      </span>
      <input
        className="code"
        value={draft}
        onChange={(event) => setDraft(event.target.value)}
        onBlur={commit}
        onKeyDown={(event) => {
          if (event.key === "Enter") event.currentTarget.blur();
          if (event.key === "Escape") setDraft(value);
        }}
      />
    </div>
  );
}

function Text({
  label,
  help,
  value,
  onChange,
}: {
  label: string;
  help?: string;
  value: string;
  onChange: (text: string) => void;
}) {
  return (
    <div className="field">
      <span title={help}>{label}</span>
      <input value={value} onChange={(event) => onChange(event.target.value)} />
    </div>
  );
}

function Choice({
  label,
  help,
  value,
  options,
  onChange,
}: {
  label: string;
  help?: string | undefined;
  value: string;
  options: { value: string; label: string }[];
  onChange: (value: string) => void;
}) {
  return (
    <div className="field">
      <span title={help}>{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
      {help && <p className="help">{help}</p>}
    </div>
  );
}

function Toggle({
  label,
  help,
  value,
  onChange,
}: {
  label: string;
  help?: string;
  value: boolean;
  onChange: (on: boolean) => void;
}) {
  return (
    <div className="field row-field">
      <span title={help}>{label}</span>
      <input
        type="checkbox"
        className="check"
        checked={value}
        onChange={(event) => onChange(event.target.checked)}
      />
    </div>
  );
}
