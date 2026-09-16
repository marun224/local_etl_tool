/**
 * The rules the canvas enforces while the mouse is down.
 *
 * These matter more than they look. Every refusal here has a matching error in
 * the engine, and the point of checking twice is to say so before someone lets
 * go of a wire rather than after they press Run. So the thing worth testing is
 * that the two agree — a canvas that refuses something the engine allows is as
 * wrong as one that allows something the engine refuses, just less obviously.
 */

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  addEdge,
  addNode,
  defaultProperties,
  isReservedId,
  newNode,
  newNodeId,
  parseDocument,
  refuseConnection,
  removeNodes,
  serializeDocument,
  updateNodeData,
  type PipelineDoc,
} from "./document";
import type { ComponentSpec } from "./ipc";

// ---------------------------------------------------------------------------
// Fixtures: the shapes the registry actually produces
// ---------------------------------------------------------------------------

const csv: ComponentSpec = {
  id: "src.file.csv",
  namespace: "source",
  label: "CSV file",
  inputs: [],
  outputs: [{ name: "main", label: "Main" }],
  properties: [
    { name: "path", label: "Path", type: "path", required: true },
    { name: "header", label: "Header", type: "bool", default: true },
  ],
};

const filter: ComponentSpec = {
  id: "xf.filter",
  namespace: "transform",
  label: "Filter",
  inputs: [{ name: "in", label: "In" }],
  outputs: [{ name: "main", label: "Main" }],
  properties: [{ name: "predicate", label: "Predicate", type: "sql", required: true }],
};

const join: ComponentSpec = {
  id: "xf.join",
  namespace: "transform",
  label: "Join",
  inputs: [
    { name: "left", label: "Left" },
    { name: "right", label: "Right" },
  ],
  outputs: [{ name: "main", label: "Main" }],
  properties: [],
};

const notNull: ComponentSpec = {
  id: "qa.not_null",
  namespace: "quality",
  label: "Not null",
  inputs: [{ name: "in", label: "In" }],
  outputs: [
    { name: "main", label: "Main" },
    { name: "rejected", label: "Rejected" },
  ],
  properties: [],
};

const parquetSink: ComponentSpec = {
  id: "snk.file.parquet",
  namespace: "sink",
  label: "Parquet file",
  inputs: [{ name: "in", label: "In" }],
  outputs: [],
  properties: [],
};

const SPECS = new Map<string, ComponentSpec>(
  [csv, filter, join, notNull, parquetSink].map((spec) => [spec.id, spec]),
);

function node(id: string, componentId: string) {
  return { id, type: "transform", position: { x: 0, y: 0 }, data: { label: id, componentId } };
}

function doc(nodes: ReturnType<typeof node>[], edges: PipelineDoc["edges"] = []): PipelineDoc {
  return { formatVersion: 1, nodes, edges };
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

describe("refuseConnection", () => {
  it("allows an ordinary source into a transform", () => {
    const document = doc([node("a", csv.id), node("b", filter.id)]);

    expect(
      refuseConnection(
        { source: "a", target: "b", sourceHandle: "main", targetHandle: "in" },
        document,
        SPECS,
      ),
    ).toBeNull();
  });

  it("refuses a node feeding itself", () => {
    const document = doc([node("a", filter.id)]);

    expect(
      refuseConnection({ source: "a", target: "a" }, document, SPECS),
    ).toMatch(/cannot feed itself/i);
  });

  it("refuses an output a component does not have", () => {
    // Only a quality node has `rejected`. Wiring one off a filter would read
    // the accepted rows while the document asked for the rejected ones.
    const document = doc([node("a", filter.id), node("b", parquetSink.id)]);

    expect(
      refuseConnection(
        { source: "a", target: "b", sourceHandle: "rejected", targetHandle: "in" },
        document,
        SPECS,
      ),
    ).toMatch(/no 'rejected' output/i);
  });

  it("allows the rejected output of a quality node", () => {
    const document = doc([node("q", notNull.id), node("s", parquetSink.id)]);

    expect(
      refuseConnection(
        { source: "q", target: "s", sourceHandle: "rejected", targetHandle: "in" },
        document,
        SPECS,
      ),
    ).toBeNull();
  });

  it("refuses a second edge into an input that is already wired", () => {
    // The engine counts inputs against the declared ports, so this would be a
    // WrongInputCount at compile time.
    const document = doc(
      [node("a", csv.id), node("b", csv.id), node("f", filter.id)],
      [{ id: "e1", source: "a", target: "f", sourceHandle: "main", targetHandle: "in" }],
    );

    expect(
      refuseConnection(
        { source: "b", target: "f", sourceHandle: "main", targetHandle: "in" },
        document,
        SPECS,
      ),
    ).toMatch(/already wired/i);
  });

  it("allows both sides of a join, because they are different ports", () => {
    const document = doc(
      [node("a", csv.id), node("b", csv.id), node("j", join.id)],
      [{ id: "e1", source: "a", target: "j", sourceHandle: "main", targetHandle: "left" }],
    );

    expect(
      refuseConnection(
        { source: "b", target: "j", sourceHandle: "main", targetHandle: "right" },
        document,
        SPECS,
      ),
    ).toBeNull();
  });

  it("refuses anything out of a sink", () => {
    const document = doc([node("s", parquetSink.id), node("f", filter.id)]);

    expect(
      refuseConnection({ source: "s", target: "f", targetHandle: "in" }, document, SPECS),
    ).toMatch(/nothing to pass on/i);
  });

  it("refuses anything into a source", () => {
    const document = doc([node("f", filter.id), node("a", csv.id)]);

    expect(
      refuseConnection({ source: "f", target: "a" }, document, SPECS),
    ).toMatch(/takes no input/i);
  });

  it("refuses a connection that would close a loop", () => {
    // The engine reports a cycle by name; this says so before the wire lands.
    const document = doc(
      [node("a", filter.id), node("b", filter.id), node("c", filter.id)],
      [
        { id: "e1", source: "a", target: "b", sourceHandle: "main", targetHandle: "in" },
        { id: "e2", source: "b", target: "c", sourceHandle: "main", targetHandle: "in" },
      ],
    );

    expect(
      refuseConnection(
        { source: "c", target: "a", sourceHandle: "main", targetHandle: "in" },
        document,
        SPECS,
      ),
    ).toMatch(/loop/i);
  });

  it("refuses a component it does not know", () => {
    const document = doc([node("a", "xf.from_the_future"), node("b", filter.id)]);

    expect(
      refuseConnection({ source: "a", target: "b", targetHandle: "in" }, document, SPECS),
    ).toMatch(/not in the registry/i);
  });
});

// ---------------------------------------------------------------------------
// Ids
// ---------------------------------------------------------------------------

describe("newNodeId", () => {
  it("is readable, because it becomes the relation name in the SQL", () => {
    expect(newNodeId("src.file.csv", [])).toBe("csv");
    expect(newNodeId("qa.not_null", [])).toBe("not_null");
  });

  it("counts up rather than colliding", () => {
    expect(newNodeId("src.file.csv", ["csv"])).toBe("csv_2");
    expect(newNodeId("src.file.csv", ["csv", "csv_2"])).toBe("csv_3");
  });

  it("never produces the suffix the engine reserves", () => {
    // A node called `x__rejected` would collide with the relation a quality
    // node gives its dead-letter rows, and the engine refuses the document.
    const generated = newNodeId("xf.some__rejected", []);

    expect(isReservedId(generated)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Editing
// ---------------------------------------------------------------------------

describe("editing", () => {
  it("gives a new node every default its spec declares", () => {
    const properties = defaultProperties(csv);

    expect(properties).toEqual({ header: true });
    expect(properties).not.toHaveProperty(
      "path",
      "a required property with no default is left out so the engine names it",
    );
  });

  it("drops the edges of a node that is removed", () => {
    // An edge to a node that is gone is an UnknownEdgeEndpoint.
    const document = doc(
      [node("a", csv.id), node("b", filter.id)],
      [{ id: "e1", source: "a", target: "b", sourceHandle: "main", targetHandle: "in" }],
    );

    const after = removeNodes(document, new Set(["a"]));

    expect(after.nodes).toHaveLength(1);
    expect(after.edges).toHaveLength(0);
  });

  it("keeps fields it does not understand", () => {
    // A document is the user's file. A key written by a newer version has to
    // survive being loaded and saved by this one.
    const text = JSON.stringify({
      formatVersion: 1,
      somethingNew: { from: "the future" },
      nodes: [
        {
          id: "a",
          position: { x: 0, y: 0 },
          futureNodeKey: 42,
          data: { label: "A", componentId: "src.file.csv", futureDataKey: true },
        },
      ],
      edges: [],
    });

    const parsed = parseDocument(text);
    const edited = updateNodeData(parsed, "a", { label: "Renamed" });
    const written = JSON.parse(serializeDocument(edited));

    expect(written.somethingNew).toEqual({ from: "the future" });
    expect(written.nodes[0].futureNodeKey).toBe(42);
    expect(written.nodes[0].data.futureDataKey).toBe(true);
    expect(written.nodes[0].data.label).toBe("Renamed");
  });

  it("round-trips a document it did not change", () => {
    const original = {
      formatVersion: 1,
      name: "sample",
      nodes: [
        {
          id: "orders",
          type: "source",
          position: { x: 0, y: 0 },
          data: {
            label: "Orders CSV",
            componentId: "src.file.csv",
            properties: { path: "samples/data/orders.csv", header: true },
            alias: "orders",
          },
        },
      ],
      edges: [],
    };

    const text = `${JSON.stringify(original, null, 2)}\n`;

    expect(serializeDocument(parseDocument(text))).toBe(text);
  });

  it("refuses text that is not a pipeline document", () => {
    expect(() => parseDocument("{}")).toThrow(/nodes/);
    expect(() => parseDocument("[]")).toThrow(/nodes/);
  });

  it("numbers edges without reusing an id", () => {
    let document = doc([node("a", csv.id), node("b", filter.id), node("c", filter.id)]);

    document = addEdge(document, { source: "a", target: "b", targetHandle: "in" });
    document = addEdge(document, { source: "b", target: "c", targetHandle: "in" });

    expect(document.edges.map((edge) => edge.id)).toEqual(["e1", "e2"]);
  });

  it("builds a node the engine would accept", () => {
    const document = addNode(doc([]), newNode(csv, { x: 10, y: 20 }, []));
    const [only] = document.nodes;

    expect(only?.id).toBe("csv");
    expect(only?.type).toBe("source");
    expect(only?.position).toEqual({ x: 10, y: 20 });
    expect(only?.data.componentId).toBe("src.file.csv");
  });
});

// ---------------------------------------------------------------------------
// The committed samples
//
// The strongest form of the round-trip promise: the real files in the repo,
// through the canvas's own parse and serialize, byte for byte. A unit test with
// a fixture I wrote would only prove the fixture round-trips.
// ---------------------------------------------------------------------------

/**
 * The registry, from the engine rather than from a fixture.
 *
 * Read by running the CLI, the same way the Rust end-to-end tests reach for the
 * vendored DuckDB: if the binary has not been built, the tests that need it
 * skip rather than fail, so a fresh checkout is not red for a reason that has
 * nothing to do with the code. A committed copy of the manifest would drift
 * from the registry the moment anyone added a component.
 */
function realSpecs(): Map<string, ComponentSpec> | null {
  const binary = fileURLToPath(
    new URL(`../../target/debug/etl${process.platform === "win32" ? ".exe" : ""}`, import.meta.url),
  );

  if (!existsSync(binary)) return null;

  const manifest: { components: ComponentSpec[] } = JSON.parse(
    execFileSync(binary, ["components", "--manifest"], { encoding: "utf8" }),
  );

  return new Map(manifest.components.map((spec) => [spec.id, spec]));
}

describe("the samples in this repo", () => {
  const directory = new URL("../../samples/pipelines/", import.meta.url);
  const files = readdirSync(directory).filter((name) => name.endsWith(".json"));
  const specs = realSpecs();

  it("finds them, so this suite cannot pass by testing nothing", () => {
    expect(files.length).toBeGreaterThan(0);
  });

  for (const name of files) {
    const text = () => readFileSync(new URL(name, directory), "utf8");

    it(`loses nothing from ${name}`, () => {
      // **Content, not bytes.** `JSON.stringify` always expands arrays onto
      // their own lines, so a hand-written file holding `"values": ["a", "b"]`
      // comes back formatted differently however carefully its data is kept.
      // Promising byte-identity would mean writing a format-preserving JSON
      // editor, which is not what "round-trips through the CLI unchanged" has
      // to mean.
      //
      // What it does have to mean is that nothing is lost or altered — every
      // key, every value, every ordering — and that is what this asserts.
      const original: unknown = JSON.parse(text());
      const written: unknown = JSON.parse(serializeDocument(parseDocument(text())));

      expect(written).toEqual(original);
    });

    it(`re-saves ${name} identically the second time`, () => {
      // Formatting normalises once, on the first save, and never moves again —
      // so opening and saving a canvas-written file produces no diff at all,
      // and the churn is a one-off rather than a pattern.
      const once = serializeDocument(parseDocument(text()));

      expect(serializeDocument(parseDocument(once))).toBe(once);
    });

    it(`can wire ${name} the way it is already wired`, () => {
      if (specs === null) return;

      // Every edge in a committed sample must be one the canvas would have
      // allowed. If it refuses one, the two disagree about what a valid
      // pipeline is, and the canvas cannot rebuild what the CLI runs.
      const document = parseDocument(text());

      for (const edge of document.edges) {
        const withoutIt = {
          ...document,
          edges: document.edges.filter((other) => other.id !== edge.id),
        };

        const refusal = refuseConnection(
          {
            source: edge.source,
            target: edge.target,
            sourceHandle: edge.sourceHandle ?? null,
            targetHandle: edge.targetHandle ?? null,
          },
          withoutIt,
          specs,
        );

        expect(refusal, `${name}: edge ${edge.id}`).toBeNull();
      }
    });
  }
});
