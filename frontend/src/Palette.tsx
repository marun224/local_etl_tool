/**
 * The component palette.
 *
 * Built entirely from the manifest the engine serves — this file holds no list
 * of components and no knowledge of what any of them do. Adding a component to
 * the registry puts it here with no frontend change at all, which is the
 * property the whole registry design exists to buy.
 */

import { useMemo, useState } from "react";
import { iconFor } from "./icons";
import type { ComponentSpec, Manifest, Namespace } from "./ipc";

/** Namespaces in the order a pipeline reads: in, through, out, then the rest. */
const ORDER: Namespace[] = ["source", "transform", "quality", "control", "sink", "code"];

const TITLES: Record<Namespace, string> = {
  source: "Sources",
  transform: "Transforms",
  quality: "Quality",
  control: "Control",
  sink: "Sinks",
  code: "Code",
};

function Glyph({ name }: { name: string | undefined }) {
  const Icon = iconFor(name);
  return <Icon size={14} strokeWidth={2} aria-hidden />;
}

export function Palette({ manifest }: { manifest: Manifest | null }) {
  const [search, setSearch] = useState("");

  const groups = useMemo(() => {
    const needle = search.trim().toLowerCase();

    const matches = (spec: ComponentSpec) =>
      needle === "" ||
      spec.id.toLowerCase().includes(needle) ||
      spec.label.toLowerCase().includes(needle) ||
      (spec.description ?? "").toLowerCase().includes(needle);

    const byNamespace = new Map<Namespace, ComponentSpec[]>();

    for (const spec of manifest?.components ?? []) {
      if (!matches(spec)) continue;
      const list = byNamespace.get(spec.namespace) ?? [];
      list.push(spec);
      byNamespace.set(spec.namespace, list);
    }

    return ORDER.flatMap((namespace) => {
      const list = byNamespace.get(namespace);
      return list && list.length > 0 ? [{ namespace, list }] : [];
    });
  }, [manifest, search]);

  const total = manifest?.components.length ?? 0;
  const shown = groups.reduce((sum, group) => sum + group.list.length, 0);

  return (
    <aside className="palette">
      <input
        className="palette-search"
        placeholder={`Search ${total} components…`}
        value={search}
        onChange={(event) => setSearch(event.target.value)}
      />

      {manifest === null && <p className="muted pad">loading…</p>}

      {manifest !== null && shown === 0 && (
        <p className="muted pad">Nothing matches “{search}”.</p>
      )}

      {groups.map(({ namespace, list }) => (
        <section key={namespace} className="palette-group">
          <h3>{TITLES[namespace]}</h3>

          {list.map((spec) => (
            <div
              key={spec.id}
              className={`palette-item palette-${namespace}`}
              draggable
              title={spec.description ?? spec.id}
              onDragStart={(event) => {
                // The component id is all the canvas needs; it looks the rest
                // up in the same manifest.
                event.dataTransfer.setData("application/etl-component", spec.id);
                event.dataTransfer.effectAllowed = "copy";
              }}
            >
              <Glyph name={spec.icon} />
              <span className="palette-label">{spec.label}</span>
              {(spec.requiresExtensions?.length ?? 0) > 0 && (
                <span className="palette-ext" title={`needs ${spec.requiresExtensions?.join(", ")}`}>
                  ext
                </span>
              )}
            </div>
          ))}
        </section>
      ))}
    </aside>
  );
}
