/**
 * Turning what a person typed into what the document holds.
 *
 * The panel that uses this is generated from each component's property schema,
 * so this file is where "the user typed something" becomes "the document says
 * something" for all nine property types — and it is kept apart from the React
 * so the rules can be tested without rendering anything.
 *
 * One idea runs through it: **absent and empty are different.** A property that
 * is not in the document takes the spec's default; a property set to `""` is an
 * empty string the engine will complain about by name. Clearing a field
 * therefore removes the key rather than writing a blank, which is what makes
 * "leave it unset and the default applies" work as written.
 */

import type { PipelineDoc } from "./document";
import type { PropertySpec, PropertyType } from "./ipc";

/** An ordered key/value list, which is how a `map` property is edited. */
export type Pairs = [string, string][];

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/**
 * What a node holds for a property, or the spec's default when it holds
 * nothing.
 *
 * The distinction matters for display: a field showing `true` because that is
 * the default reads the same as one showing `true` because someone chose it,
 * and both are honest — the run will use that value either way.
 */
export function valueOf(
  properties: Record<string, unknown> | undefined,
  spec: PropertySpec,
): unknown {
  const held = properties?.[spec.name];
  return held === undefined ? spec.default : held;
}

/** Whether the node itself sets this, rather than falling back to the default. */
export function isSet(
  properties: Record<string, unknown> | undefined,
  spec: PropertySpec,
): boolean {
  return properties?.[spec.name] !== undefined;
}

/** A property the engine will refuse for being absent. */
export function isMissing(
  properties: Record<string, unknown> | undefined,
  spec: PropertySpec,
): boolean {
  if (!spec.required) return false;

  const value = valueOf(properties, spec);

  if (value === undefined || value === null) return true;
  if (typeof value === "string") return value.trim() === "";
  if (Array.isArray(value)) return value.length === 0;

  return false;
}

/** Every required property a node has not answered, in schema order. */
export function missingProperties(
  properties: Record<string, unknown> | undefined,
  specs: PropertySpec[],
): string[] {
  return specs.filter((spec) => isMissing(properties, spec)).map((spec) => spec.label);
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

/**
 * Set one property, or remove it when `value` is `undefined`.
 *
 * Removing rather than writing a blank is the whole point: the engine fills an
 * absent property from the spec's default, and a `""` left behind by a cleared
 * field would override that default with something nobody chose.
 */
export function setProperty(
  document: PipelineDoc,
  nodeId: string,
  name: string,
  value: unknown,
): PipelineDoc {
  return {
    ...document,
    nodes: document.nodes.map((node) => {
      if (node.id !== nodeId) return node;

      const properties = { ...(node.data.properties ?? {}) };

      if (value === undefined) {
        delete properties[name];
      } else {
        properties[name] = value;
      }

      return { ...node, data: { ...node.data, properties } };
    }),
  };
}

/**
 * Set a field on the node itself — label, alias, materialize — or remove it.
 *
 * Separate from properties because these are how a node is *run* rather than
 * what its component does, and every component has them.
 */
export function setNodeField(
  document: PipelineDoc,
  nodeId: string,
  field: string,
  value: unknown,
): PipelineDoc {
  return {
    ...document,
    nodes: document.nodes.map((node) => {
      if (node.id !== nodeId) return node;

      const data = { ...node.data };

      if (value === undefined) {
        delete data[field];
      } else {
        data[field] = value;
      }

      return { ...node, data };
    }),
  };
}

/**
 * Rename a node, carrying its edges with it.
 *
 * The id is not cosmetic: it is the relation name in the generated SQL and the
 * thing every edge refers to, so renaming has to move both or the document
 * stops compiling. Returns `null` when the new name cannot be used, so the
 * caller can say why rather than silently doing nothing.
 */
export function renameNode(
  document: PipelineDoc,
  from: string,
  to: string,
): { document: PipelineDoc } | { error: string } {
  const trimmed = to.trim();

  if (trimmed === "") return { error: "A node needs a name." };
  if (trimmed === from) return { document };

  if (document.nodes.some((node) => node.id === trimmed)) {
    return { error: `There is already a node called '${trimmed}'.` };
  }

  if (trimmed.endsWith("__rejected")) {
    return {
      error: "Names ending in '__rejected' are reserved for a quality node's rejected rows.",
    };
  }

  return {
    document: {
      ...document,
      nodes: document.nodes.map((node) => (node.id === from ? { ...node, id: trimmed } : node)),
      edges: document.edges.map((edge) => ({
        ...edge,
        ...(edge.source === from ? { source: trimmed } : {}),
        ...(edge.target === from ? { target: trimmed } : {}),
      })),
    },
  };
}

// ---------------------------------------------------------------------------
// Coercion
// ---------------------------------------------------------------------------

/**
 * What a text field's contents mean for a property of this type.
 *
 * `undefined` means "remove it" — an empty field is an unanswered one. A number
 * that will not parse is also `undefined` rather than `NaN`, because `NaN`
 * serialises to `null` and would turn a typo into a value.
 */
export function fromText(type: PropertyType, text: string): unknown {
  const trimmed = text.trim();

  if (trimmed === "") return undefined;

  switch (type) {
    case "integer": {
      // Reject `1.5` for an integer rather than rounding it. Silently changing
      // a number someone typed is worse than refusing it.
      if (!/^[+-]?\d+$/.test(trimmed)) return undefined;

      const parsed = Number(trimmed);
      return Number.isSafeInteger(parsed) ? parsed : undefined;
    }

    case "number": {
      const parsed = Number(trimmed);
      return Number.isFinite(parsed) ? parsed : undefined;
    }

    case "bool":
      return trimmed === "true";

    // Text, path, sql and enum are all kept as written. Notably `sql` is not
    // trimmed or escaped here: it is user-written SQL by definition, and the
    // engine's own quoting rules are what protect the generated statement.
    default:
      return text;
  }
}

/** How a value is shown in a text field. */
export function toText(value: unknown): string {
  if (value === undefined || value === null) return "";
  if (typeof value === "string") return value;
  return String(value);
}

/** A `string_list` property's value, whatever shape the document holds. */
export function asList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string");
}

/**
 * A `map` property's value as ordered pairs.
 *
 * Order is load-bearing — `xf.rename` and `xf.cast` turn their entries into SQL
 * in the order they were written — so the editor works in pairs rather than in
 * an object, and only converts back on write.
 *
 * One caveat this cannot fix: JSON object keys that look like integers are
 * reordered ahead of the rest by every JavaScript engine, so a column literally
 * named `1` will not keep its place. The document format would have to change
 * to a pair array to solve it, which is not 7c's to do.
 */
export function asPairs(value: unknown): Pairs {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return [];

  return Object.entries(value as Record<string, unknown>).map(([key, held]) => [
    key,
    typeof held === "string" ? held : String(held),
  ]);
}

/** Pairs back to the object the document holds, dropping unnamed rows. */
export function fromPairs(pairs: Pairs): Record<string, string> | undefined {
  const named = pairs.filter(([key]) => key.trim() !== "");

  if (named.length === 0) return undefined;

  const out: Record<string, string> = {};
  for (const [key, value] of named) out[key.trim()] = value;

  return out;
}

/** A list back to what the document holds, dropping blank entries. */
export function fromList(items: string[]): string[] | undefined {
  const kept = items.map((item) => item.trim()).filter((item) => item !== "");
  return kept.length === 0 ? undefined : kept;
}

// ---------------------------------------------------------------------------
// Node-level choices
// ---------------------------------------------------------------------------

/** The materialisation modes the engine knows, with what each one costs. */
export const MATERIALIZE: { value: string; label: string; help: string }[] = [
  { value: "auto", label: "Auto", help: "Let the engine decide. Today that means a view." },
  { value: "view", label: "View", help: "Lazy. Nothing computes until a sink pulls." },
  { value: "memory", label: "Memory", help: "A temp table: computed once, held in memory." },
  { value: "disk", label: "Disk", help: "Spilled to a temporary Parquet file and read back." },
];
