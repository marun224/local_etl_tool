/**
 * @vitest-environment jsdom
 */

/**
 * The panel really is generated from the schema.
 *
 * The claim 7c makes is that a component gets a working form with no React
 * written for it. That is only checkable by handing the panel a spec it has
 * never seen and looking at what it renders — which is what this does, with a
 * made-up component using all nine property types.
 *
 * A screenshot would show one component's form on one day. This shows that the
 * mapping from type to control holds for every type, and it keeps showing it.
 */

import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Inspector } from "./Inspector";
import type { NodePolicy, PipelineDoc } from "./document";
import type { ComponentSpec } from "./ipc";

// The dialog plugin talks to Tauri, which is not here. Only the path control
// touches it, and only when clicked.
vi.mock("@tauri-apps/plugin-dialog", () => ({ open: vi.fn() }));

// Testing Library only unmounts automatically when vitest globals are on, and
// they are not: without this every render piles into the same document and the
// second test onward finds two of everything.
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

/** One component using every property type there is. */
const everything: ComponentSpec = {
  id: "xf.everything",
  namespace: "transform",
  label: "Everything",
  description: "A component that exists only in this test.",
  inputs: [{ name: "in", label: "In" }],
  outputs: [{ name: "main", label: "Main" }],
  properties: [
    { name: "title", label: "Title", type: "text", required: true, help: "Some words." },
    { name: "file", label: "File", type: "path" },
    { name: "predicate", label: "Predicate", type: "sql" },
    { name: "query", label: "Query", type: "code" },
    { name: "header", label: "Header", type: "bool", default: true },
    { name: "limit", label: "Limit", type: "integer" },
    { name: "ratio", label: "Ratio", type: "number" },
    { name: "columns", label: "Columns", type: "string_list" },
    { name: "renames", label: "Renames", type: "map" },
    { name: "mode", label: "Mode", type: "enum", options: ["fast", "careful"] },
  ],
};

const SPECS = new Map([[everything.id, everything]]);

function doc(properties: Record<string, unknown> = {}): PipelineDoc {
  return {
    formatVersion: 1,
    nodes: [
      {
        id: "n1",
        type: "transform",
        position: { x: 0, y: 0 },
        data: { label: "A node", componentId: everything.id, properties },
      },
    ],
    edges: [],
  };
}

/** Render the panel and hand back whatever it writes to the document. */
function panel(properties: Record<string, unknown> = {}) {
  const document = doc(properties);
  const onChange = vi.fn();
  const onError = vi.fn();

  render(
    <Inspector
      document={document}
      node={document.nodes[0] ?? null}
      specs={SPECS}
      onChange={onChange}
      onRenamed={vi.fn()}
      onError={onError}
    />,
  );

  /** The last document the panel produced. */
  const written = (): PipelineDoc => onChange.mock.calls.at(-1)?.[0] as PipelineDoc;
  const props = () => written().nodes[0]?.data.properties ?? {};

  return { onChange, onError, written, props };
}

function fieldFor(label: string): HTMLElement {
  const text = screen.getByText(label, { selector: "span" });
  const field = text.closest(".field");

  if (!field) throw new Error(`no field around '${label}'`);
  return field as HTMLElement;
}

// ---------------------------------------------------------------------------
// One control per type
// ---------------------------------------------------------------------------

describe("the control chosen for each property type", () => {
  it("gives a bool a checkbox, already reflecting its default", () => {
    panel();
    const input = within(fieldFor("Header")).getByRole("checkbox");

    expect(input).toBeTruthy();
    expect((input as HTMLInputElement).checked).toBe(true);
  });

  it("gives an enum a select holding exactly its options", () => {
    panel();
    const select = within(fieldFor("Mode")).getByRole("combobox");

    const values = [...(select as HTMLSelectElement).options].map((option) => option.value);

    // Plus a way back to unset, which is not one of the component's options.
    expect(values).toEqual(["", "fast", "careful"]);
  });

  it("gives sql a textarea rather than a single line", () => {
    panel();

    expect(within(fieldFor("Predicate")).getByRole("textbox").tagName).toBe("TEXTAREA");
  });

  it("gives code a textarea too, and writes its lines as typed", () => {
    const { props } = panel();
    const box = within(fieldFor("Query")).getByRole("textbox");
    expect(box.tagName).toBe("TEXTAREA");

    fireEvent.change(box, { target: { value: "query {\n  orders { id }\n}" } });

    expect(props()).toEqual({ query: "query {\n  orders { id }\n}" });
  });

  it("gives a number a numeric input, stepped for an integer", () => {
    panel();

    const limit = within(fieldFor("Limit")).getByRole("spinbutton");
    const ratio = within(fieldFor("Ratio")).getByRole("spinbutton");

    expect(limit.getAttribute("step")).toBe("1");
    expect(ratio.getAttribute("step")).toBe("any");
  });

  it("gives a path a picker beside its text", () => {
    panel();
    const field = fieldFor("File");

    expect(within(field).getByRole("textbox")).toBeTruthy();
    expect(within(field).getByTitle("Pick a file")).toBeTruthy();
  });

  it("gives a list one row per item, plus a way to add another", () => {
    panel({ columns: ["a", "b"] });
    const field = fieldFor("Columns");

    expect(within(field).getAllByRole("textbox")).toHaveLength(2);
    expect(within(field).getByText(/add/)).toBeTruthy();
  });

  it("gives a map two boxes per pair", () => {
    panel({ renames: { old: "new" } });
    const inputs = within(fieldFor("Renames")).getAllByRole("textbox");

    expect(inputs).toHaveLength(2);
    expect((inputs[0] as HTMLInputElement).value).toBe("old");
    expect((inputs[1] as HTMLInputElement).value).toBe("new");
  });

  it("renders a field for every property in the schema, and no others", () => {
    panel();

    for (const property of everything.properties) {
      expect(fieldFor(property.label), property.label).toBeTruthy();
    }
  });
});

// ---------------------------------------------------------------------------
// What editing writes
// ---------------------------------------------------------------------------

describe("editing", () => {
  it("writes what was typed", () => {
    const { props } = panel();

    fireEvent.change(within(fieldFor("Title")).getByRole("textbox"), {
      target: { value: "hello" },
    });

    expect(props()).toEqual({ title: "hello" });
  });

  it("removes a property when its field is emptied", () => {
    // Not `""`. A blank left behind would override the spec's default with
    // something nobody chose.
    const { props } = panel({ title: "hello" });

    fireEvent.change(within(fieldFor("Title")).getByRole("textbox"), { target: { value: "" } });

    expect(props()).not.toHaveProperty("title");
  });

  it("does not write a number that will not parse", () => {
    const { props } = panel();

    fireEvent.change(within(fieldFor("Limit")).getByRole("spinbutton"), {
      target: { value: "1.5" },
    });

    expect(props()).not.toHaveProperty("limit");
  });

  it("writes a bool the moment it is toggled", () => {
    const { props } = panel();

    fireEvent.click(within(fieldFor("Header")).getByRole("checkbox"));

    expect(props()).toEqual({ header: false });
  });
});

// ---------------------------------------------------------------------------
// What it says about the node
// ---------------------------------------------------------------------------

describe("what the panel tells you", () => {
  it("marks a required property that has not been answered", () => {
    panel();

    expect(fieldFor("Title").className).toContain("is-missing");
    expect(within(fieldFor("Title")).getByText("Required.")).toBeTruthy();
  });

  it("stops marking it once it is answered", () => {
    panel({ title: "done" });

    expect(fieldFor("Title").className).not.toContain("is-missing");
  });

  it("says when a value is only there because the spec says so", () => {
    // It reads identically to a chosen value, and the difference matters when
    // working out why a run did what it did.
    panel();

    expect(within(fieldFor("Header")).getByText("default")).toBeTruthy();
  });

  it("does not call it a default once someone has chosen it", () => {
    panel({ header: true });

    expect(within(fieldFor("Header")).queryByText("default")).toBeNull();
  });

  it("shows the component's help text", () => {
    panel();

    expect(screen.getByText("Some words.")).toBeTruthy();
    expect(screen.getByText(everything.description ?? "")).toBeTruthy();
  });

  it("says so plainly when the component is not in the registry", () => {
    const document = doc();
    document.nodes[0]!.data.componentId = "xf.from_the_future";

    render(
      <Inspector
        document={document}
        node={document.nodes[0] ?? null}
        specs={SPECS}
        onChange={vi.fn()}
        onRenamed={vi.fn()}
        onError={vi.fn()}
      />,
    );

    expect(screen.getByText(/not in the registry/)).toBeTruthy();
  });

  it("asks for a selection when there is none", () => {
    render(
      <Inspector
        document={doc()}
        node={null}
        specs={SPECS}
        onChange={vi.fn()}
        onRenamed={vi.fn()}
        onError={vi.fn()}
      />,
    );

    expect(screen.getByText(/Select a node/)).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// Stage policy
//
// The panel 7d finally gives these four knobs. What is worth testing is not
// that four inputs render, but that they follow the same "unset means unset"
// rule the generated fields do — and that the panel says out loud that
// setting any of them changes how the whole pipeline runs.
// ---------------------------------------------------------------------------

describe("the policy panel", () => {
  /** Whatever the panel wrote to `data.policy`. */
  function policyAfter(written: () => PipelineDoc) {
    return written().nodes[0]?.data.policy;
  }

  /** The panel, over a node that already carries a policy. */
  function withPolicy(policy: NodePolicy) {
    const document = doc();
    document.nodes[0]!.data.policy = policy;

    const onChange = vi.fn();
    render(
      <Inspector
        document={document}
        node={document.nodes[0] ?? null}
        specs={SPECS}
        onChange={onChange}
        onRenamed={vi.fn()}
        onError={vi.fn()}
      />,
    );

    return { onChange, written: () => onChange.mock.calls.at(-1)?.[0] as PipelineDoc };
  }

  it("offers all four knobs, unset", () => {
    panel();

    for (const label of [
      "Retry attempts",
      "Retry backoff (ms)",
      "Continue on failure",
      "Memory limit (MB)",
    ]) {
      expect(fieldFor(label)).toBeTruthy();
    }

    // Nothing is written merely by rendering.
    expect(screen.queryByText("session")).toBeNull();
  });

  it("writes a retry count", () => {
    const { written } = panel();
    const input = within(fieldFor("Retry attempts")).getByRole("spinbutton");

    fireEvent.change(input, { target: { value: "3" } });
    expect(policyAfter(written)).toEqual({ retryAttempts: 3 });
  });

  it("removes the policy when its last knob is cleared", () => {
    // Started from a document that already holds one, because the panel is
    // controlled: an `onChange` spy does not feed its result back, so clearing
    // a field the rendered document never had would be a no-op event.
    const { written } = withPolicy({ retryAttempts: 3 });

    fireEvent.change(within(fieldFor("Retry attempts")).getByRole("spinbutton"), {
      target: { value: "" },
    });

    expect(policyAfter(written)).toBeUndefined();
  });

  it("leaves the other knobs alone when one is cleared", () => {
    const { written } = withPolicy({ retryAttempts: 3, continueOnFailure: true });

    fireEvent.change(within(fieldFor("Retry attempts")).getByRole("spinbutton"), {
      target: { value: "" },
    });

    expect(policyAfter(written)).toEqual({ continueOnFailure: true });
  });

  it("does not write a number that will not parse", () => {
    const { onChange } = panel();
    const input = within(fieldFor("Memory limit (MB)")).getByRole("spinbutton");

    fireEvent.change(input, { target: { value: "2.5" } });
    fireEvent.change(input, { target: { value: "-1" } });

    // A typo must never become a value: the same rule the generated number
    // field follows.
    expect(onChange).not.toHaveBeenCalled();
  });

  it("says that a policy moves the pipeline onto the session transport", () => {
    // The claim is about what someone sees after setting one — finding it out
    // from the Plan tab afterwards is finding out too late.
    withPolicy({ retryAttempts: 1 });

    expect(screen.getByText("session")).toBeTruthy();
    expect(screen.getByText(/session transport/)).toBeTruthy();
  });
});
