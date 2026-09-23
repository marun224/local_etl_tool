/**
 * What typing into a generated form does to the document.
 *
 * The risky part of a form built from a schema is not the rendering — it is the
 * edges: an emptied field, a half-typed number, a value that matches the
 * default. Each of those has one right answer and several plausible wrong ones,
 * so they are pinned here rather than discovered later by a run that did
 * something nobody asked for.
 */

import { describe, expect, it } from "vitest";
import {
  asList,
  asPairs,
  fromList,
  fromPairs,
  fromText,
  isMissing,
  isSet,
  missingProperties,
  renameNode,
  setNodeField,
  setPolicyField,
  setProperty,
  toText,
  valueOf,
} from "./properties";
import type { PipelineDoc } from "./document";
import type { PropertySpec } from "./ipc";

const path: PropertySpec = { name: "path", label: "Path", type: "path", required: true };
const header: PropertySpec = { name: "header", label: "Header", type: "bool", default: true };
const limit: PropertySpec = { name: "limit", label: "Limit", type: "integer" };
const columns: PropertySpec = {
  name: "columns",
  label: "Columns",
  type: "string_list",
  required: true,
};

function doc(): PipelineDoc {
  return {
    formatVersion: 1,
    nodes: [
      {
        id: "orders",
        type: "source",
        position: { x: 0, y: 0 },
        data: {
          label: "Orders",
          componentId: "src.file.csv",
          properties: { path: "in.csv" },
        },
      },
      {
        id: "large",
        type: "transform",
        position: { x: 200, y: 0 },
        data: { label: "Large", componentId: "xf.filter", properties: {} },
      },
    ],
    edges: [
      { id: "e1", source: "orders", target: "large", sourceHandle: "main", targetHandle: "in" },
    ],
  };
}

// ---------------------------------------------------------------------------
// Absent is not empty
// ---------------------------------------------------------------------------

describe("absent versus empty", () => {
  it("falls back to the spec's default when the node says nothing", () => {
    expect(valueOf({}, header)).toBe(true);
    expect(isSet({}, header)).toBe(false);
  });

  it("distinguishes a chosen value from a defaulted one", () => {
    // Both run the same way; the difference is what the panel tells you, and
    // it matters when working out why a run did what it did.
    expect(valueOf({ header: true }, header)).toBe(true);
    expect(isSet({ header: true }, header)).toBe(true);
  });

  it("removes a property rather than writing a blank over it", () => {
    // This is the whole reason `fromText` returns undefined for "": a `""`
    // left behind by a cleared field would override the spec's default with
    // something nobody chose.
    const before = setProperty(doc(), "orders", "delimiter", ",");
    expect(before.nodes[0]?.data.properties).toHaveProperty("delimiter");

    const after = setProperty(before, "orders", "delimiter", undefined);
    expect(after.nodes[0]?.data.properties).not.toHaveProperty("delimiter");
  });

  it("treats an emptied field as unanswered", () => {
    expect(fromText("text", "")).toBeUndefined();
    expect(fromText("text", "   ")).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Numbers
// ---------------------------------------------------------------------------

describe("numbers", () => {
  it("refuses a decimal for an integer rather than rounding it", () => {
    // Silently changing a number someone typed is worse than ignoring it.
    expect(fromText("integer", "1.5")).toBeUndefined();
    expect(fromText("integer", "10")).toBe(10);
    expect(fromText("integer", "-3")).toBe(-3);
  });

  it("refuses an integer past what JSON can carry exactly", () => {
    expect(fromText("integer", "9007199254740993")).toBeUndefined();
    expect(fromText("integer", "9007199254740991")).toBe(9007199254740991);
  });

  it("never produces NaN, which would serialise to null", () => {
    // `null` in a property is a value the engine would have to interpret; a
    // typo should simply not be written.
    expect(fromText("number", "abc")).toBeUndefined();
    expect(fromText("number", "1e999")).toBeUndefined();
    expect(fromText("number", "2.5")).toBe(2.5);
  });
});

// ---------------------------------------------------------------------------
// SQL and text
// ---------------------------------------------------------------------------

describe("text", () => {
  it("keeps SQL exactly as written, including its whitespace", () => {
    // A `sql` property is user-written SQL by definition. Trimming or escaping
    // it here would break legitimate statements; the engine's quoting is what
    // protects the generated script.
    const written = "  amount > 100\n  AND status = 'paid'  ";

    expect(fromText("sql", written)).toBe(written);
  });

  it("keeps code exactly as written too", () => {
    const written = "query ($after: String) {\n  orders(after: $after) { id }\n}\n";

    expect(fromText("code", written)).toBe(written);
  });

  it("shows a missing value as an empty field rather than 'undefined'", () => {
    expect(toText(undefined)).toBe("");
    expect(toText(null)).toBe("");
    expect(toText(false)).toBe("false");
    expect(toText(0)).toBe("0");
  });
});

// ---------------------------------------------------------------------------
// Lists and maps
// ---------------------------------------------------------------------------

describe("lists", () => {
  it("drops blank rows on the way out but keeps them while editing", () => {
    // A row you have just added is empty, and removing it from the document
    // immediately would delete the box you are about to type into.
    expect(fromList(["a", "", "b"])).toEqual(["a", "b"]);
    expect(fromList(["", ""])).toBeUndefined();
  });

  it("reads a list out of whatever the document holds", () => {
    expect(asList(["a", "b"])).toEqual(["a", "b"]);
    expect(asList(undefined)).toEqual([]);
    expect(asList("a,b")).toEqual([]);
  });
});

describe("maps", () => {
  it("keeps the order the pairs were written in", () => {
    // `xf.rename` and `xf.cast` turn their entries into SQL in order, so a map
    // editor that reordered them would change the generated statement.
    const pairs: [string, string][] = [
      ["z", "1"],
      ["a", "2"],
      ["m", "3"],
    ];

    expect(Object.keys(fromPairs(pairs) ?? {})).toEqual(["z", "a", "m"]);
  });

  it("round-trips through the document shape", () => {
    const pairs: [string, string][] = [
      ["old", "new"],
      ["other", "thing"],
    ];

    expect(asPairs(fromPairs(pairs))).toEqual(pairs);
  });

  it("drops rows with no name, and an empty map entirely", () => {
    expect(fromPairs([["", "orphan"]])).toBeUndefined();
    expect(fromPairs([["kept", "yes"], ["", "no"]])).toEqual({ kept: "yes" });
  });
});

// ---------------------------------------------------------------------------
// Required
// ---------------------------------------------------------------------------

describe("required properties", () => {
  it("counts an absent, blank or empty value as missing", () => {
    expect(isMissing({}, path)).toBe(true);
    expect(isMissing({ path: "   " }, path)).toBe(true);
    expect(isMissing({ columns: [] }, columns)).toBe(true);

    expect(isMissing({ path: "in.csv" }, path)).toBe(false);
    expect(isMissing({ columns: ["a"] }, columns)).toBe(false);
  });

  it("is never missing when it is not required", () => {
    expect(isMissing({}, limit)).toBe(false);
  });

  it("lists what a node still owes, in schema order", () => {
    expect(missingProperties({}, [path, header, columns])).toEqual(["Path", "Columns"]);
  });
});

// ---------------------------------------------------------------------------
// Renaming
// ---------------------------------------------------------------------------

describe("renameNode", () => {
  it("carries the edges with it", () => {
    // The id is the relation name in the SQL and what every edge refers to, so
    // renaming without moving the edges would stop the document compiling.
    const result = renameNode(doc(), "orders", "sales");

    expect("document" in result).toBe(true);
    if (!("document" in result)) return;

    expect(result.document.nodes.map((node) => node.id)).toEqual(["sales", "large"]);
    expect(result.document.edges[0]?.source).toBe("sales");
    expect(result.document.edges[0]?.target).toBe("large");
  });

  it("refuses a name another node already has", () => {
    const result = renameNode(doc(), "orders", "large");

    expect("error" in result && result.error).toMatch(/already a node/i);
  });

  it("refuses the suffix the engine reserves", () => {
    const result = renameNode(doc(), "orders", "check__rejected");

    expect("error" in result && result.error).toMatch(/reserved/i);
  });

  it("refuses an empty name", () => {
    expect("error" in renameNode(doc(), "orders", "  ")).toBe(true);
  });

  it("is a no-op when the name has not changed", () => {
    const result = renameNode(doc(), "orders", "orders");

    expect("document" in result).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Node-level fields
// ---------------------------------------------------------------------------

describe("node fields", () => {
  it("removes a field rather than writing an empty one", () => {
    // An `alias: ""` would be a relation named nothing.
    const withAlias = setNodeField(doc(), "orders", "alias", "sales");
    expect(withAlias.nodes[0]?.data.alias).toBe("sales");

    const without = setNodeField(withAlias, "orders", "alias", undefined);
    expect(without.nodes[0]?.data).not.toHaveProperty("alias");
  });

  it("leaves other nodes alone", () => {
    const after = setNodeField(doc(), "orders", "label", "Renamed");

    expect(after.nodes[1]?.data.label).toBe("Large");
  });

  it("keeps fields it does not understand", () => {
    const before = doc();
    before.nodes[0]!.data.futureKey = { kept: true };

    const after = setNodeField(before, "orders", "label", "Renamed");

    expect(after.nodes[0]?.data.futureKey).toEqual({ kept: true });
  });
});

// ---------------------------------------------------------------------------
// Stage policy
// ---------------------------------------------------------------------------

describe("setPolicyField", () => {
  /** The policy on `orders` after applying a sequence of edits. */
  function after(...edits: [Parameters<typeof setPolicyField>[2], number | boolean | undefined][]) {
    let document = doc();
    for (const [field, value] of edits) {
      document = setPolicyField(document, "orders", field, value);
    }
    return document.nodes[0]?.data.policy;
  }

  it("writes a value, and leaves the other knobs alone", () => {
    expect(after(["retryAttempts", 2])).toEqual({ retryAttempts: 2 });
    expect(after(["retryAttempts", 2], ["retryBackoffMs", 100])).toEqual({
      retryAttempts: 2,
      retryBackoffMs: 100,
    });
  });

  it("removes a cleared knob rather than writing a zero", () => {
    // `retryAttempts: 0` and no `retryAttempts` compile the same, but the
    // first reads as a decision someone took.
    expect(after(["retryAttempts", 2], ["retryBackoffMs", 100], ["retryAttempts", undefined])).toEqual(
      { retryBackoffMs: 100 },
    );
  });

  it("removes the policy entirely once nothing is left in it", () => {
    // An empty `"policy": {}` is noise in a file people read and diff, and it
    // is one keystroke from looking like a policy that got lost.
    expect(after(["continueOnFailure", true], ["continueOnFailure", undefined])).toBeUndefined();
  });

  it("does not touch a node it was not asked about", () => {
    const document = setPolicyField(doc(), "orders", "retryAttempts", 1);
    expect(document.nodes[1]?.data.policy).toBeUndefined();
  });

  it("keeps zero as a value where zero is meaningful", () => {
    // Zero backoff is "retry immediately", which is a real choice and not the
    // same as leaving it unset.
    expect(after(["retryBackoffMs", 0])).toEqual({ retryBackoffMs: 0 });
  });
});
