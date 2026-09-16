/**
 * @vitest-environment jsdom
 */

/**
 * The run view's job is to say what happened without saying more than the
 * engine measured.
 *
 * Almost everything here is about the same rule from a different angle: a
 * stage with no timing must render *nothing* in that column. A zero, a dash or
 * a `0 ms` all read as a measurement, and the one place that matters is
 * exactly where someone is deciding which node made their pipeline slow.
 */

import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { DataTab, PlanTab, StatusTab, duration } from "./RunView";
import type { PipelineDoc } from "./document";
import type { PlanView, PreviewResult, RunResult, StageResult } from "./ipc";

afterEach(cleanup);

const document: PipelineDoc = {
  formatVersion: 1,
  nodes: [
    {
      id: "orders",
      type: "source",
      position: { x: 0, y: 0 },
      data: { label: "Orders", componentId: "src.file.csv" },
    },
  ],
  edges: [],
};

function stage(over: Partial<StageResult> = {}): StageResult {
  return {
    nodeId: "orders",
    label: "Orders",
    componentId: "src.file.csv",
    rows: 12,
    rejected: null,
    skipped: null,
    elapsedMs: null,
    ...over,
  };
}

function runOf(stages: StageResult[], over: Partial<RunResult> = {}): RunResult {
  return {
    stages,
    elapsedMs: 220,
    notes: [],
    failures: [],
    failed: false,
    script: "SELECT 1",
    ...over,
  };
}

/** The cells of the one data row, in order. */
function cells(): string[] {
  const row = screen.getAllByRole("row")[1];
  return within(row!)
    .getAllByRole("cell")
    .map((cell) => cell.textContent ?? "");
}

describe("duration", () => {
  it("never rounds a real measurement down to nothing", () => {
    // Sub-millisecond is the case that matters: the honest answer is "too
    // small to state", not "0 ms", which reads as free.
    expect(duration(0.4)).toBe("<1 ms");
    expect(duration(0)).toBe("<1 ms");
  });

  it("changes unit rather than growing digits", () => {
    expect(duration(26)).toBe("26 ms");
    expect(duration(999)).toBe("999 ms");
    expect(duration(1500)).toBe("1.50 s");
    expect(duration(64_000)).toBe("64.0 s");
  });
});

describe("StatusTab", () => {
  it("leaves the time cell empty for a stage the engine would not time", () => {
    render(<StatusTab document={document} validation={null} run={runOf([stage()])} />);

    // Position matters: Stage, Rows, Rejected, Time, Component.
    expect(cells()[3]).toBe("");
    // And nothing anywhere in the row claims a zero.
    expect(screen.queryByText("0 ms")).toBeNull();
  });

  it("shows a time when there is one", () => {
    render(
      <StatusTab document={document} validation={null} run={runOf([stage({ elapsedMs: 35 })])} />,
    );

    expect(cells()[3]).toBe("35 ms");
  });

  it("explains the blanks only when there are blanks to explain", () => {
    const note = /blank time is a stage whose work happens somewhere else/i;

    render(<StatusTab document={document} validation={null} run={runOf([stage()])} />);
    expect(screen.getByText(note)).toBeTruthy();

    cleanup();

    // Every stage timed: the note would be answering a question nobody asked.
    render(
      <StatusTab document={document} validation={null} run={runOf([stage({ elapsedMs: 9 })])} />,
    );
    expect(screen.queryByText(note)).toBeNull();
  });

  it("distinguishes a node that cannot reject from one that rejected nothing", () => {
    render(
      <StatusTab
        document={document}
        validation={null}
        run={runOf([stage({ rejected: null })])}
      />,
    );
    expect(cells()[2]).toBe("");

    cleanup();

    // Zero rejects is the result someone ran the check to see.
    render(
      <StatusTab document={document} validation={null} run={runOf([stage({ rejected: 0 })])} />,
    );
    expect(cells()[2]).toBe("0");
  });

  it("says why a stage was skipped instead of showing counts it does not have", () => {
    render(
      <StatusTab
        document={document}
        validation={null}
        run={runOf([stage({ rows: null, skipped: "upstream 'load' failed" })])}
      />,
    );

    expect(cells()[1]).toBe("upstream 'load' failed");
    expect(cells()[3]).toBe("");
  });

  it("reports a run that reached the end and still failed as failed", () => {
    render(
      <StatusTab
        document={document}
        validation={null}
        run={runOf([stage()], { failed: true, failures: ["Load (load): no such file"] })}
      />,
    );

    expect(screen.getByText(/Run failed after/)).toBeTruthy();
    expect(screen.getByText(/no such file/)).toBeTruthy();
  });
});

describe("PlanTab", () => {
  const plan: PlanView = {
    stages: [
      {
        nodeId: "orders",
        componentId: "src.file.csv",
        label: "Orders",
        kind: "Source",
        sql: "CREATE OR REPLACE TEMP VIEW orders AS SELECT 1",
        from: null,
        splits: false,
        needsSession: false,
      },
    ],
    warnings: [],
    extensions: ["httpfs"],
    script: "-- the whole thing\nSELECT 1",
    needsSession: true,
    sessionReasons: ["gate"],
  };

  it("names the transport and what asked for it", () => {
    render(<PlanTab plan={plan} whole={false} onToggleWhole={vi.fn()} onSelect={vi.fn()} />);

    // Invisible in the SQL, and the reason a retry or a branch is possible.
    expect(screen.getByText(/session — gate/)).toBeTruthy();
    expect(screen.getByText(/loads httpfs/)).toBeTruthy();
  });

  it("shows a stage's own SQL by stage, and the script when asked", () => {
    // Read through `textContent` rather than `getByText`: the highlighter
    // splits SQL into one span per token, so no single element holds a phrase.
    // That the text survives the split is itself the thing worth asserting.
    const sql = () =>
      Array.from(window.document.querySelectorAll("pre.sql"))
        .map((pre) => pre.textContent)
        .join("\n");

    const { rerender } = render(
      <PlanTab plan={plan} whole={false} onToggleWhole={vi.fn()} onSelect={vi.fn()} />,
    );

    expect(sql()).toContain("CREATE OR REPLACE TEMP VIEW orders AS SELECT 1");
    expect(sql()).not.toContain("the whole thing");

    rerender(<PlanTab plan={plan} whole onToggleWhole={vi.fn()} onSelect={vi.fn()} />);
    expect(sql()).toContain("-- the whole thing");
  });

  it("asks for a plan rather than rendering an empty one", () => {
    render(<PlanTab plan={null} whole={false} onToggleWhole={vi.fn()} onSelect={vi.fn()} />);
    expect(screen.getByText(/Press Plan/)).toBeTruthy();
  });
});

describe("DataTab", () => {
  function preview(over: Partial<PreviewResult> = {}): PreviewResult {
    return {
      nodeId: "orders",
      columns: ["id", "note"],
      rows: [{ id: 1, note: null }],
      truncated: false,
      ...over,
    };
  }

  it("renders null as null rather than as an empty cell", () => {
    render(<DataTab preview={preview()} />);

    const row = screen.getAllByRole("row")[1];
    const [, , note] = within(row!).getAllByRole("cell");

    expect(note?.textContent).toBe("null");
    expect(note?.className).toContain("null");
  });

  it("separates no rows from no preview", () => {
    render(<DataTab preview={null} />);
    expect(screen.getByText(/Select a node/)).toBeTruthy();

    cleanup();

    render(<DataTab preview={preview({ rows: [] })} />);
    expect(screen.getByText("No rows.")).toBeTruthy();
  });

  it("says when there are more rows than it fetched", () => {
    render(<DataTab preview={preview({ truncated: true })} />);
    expect(screen.getByText(/and there are more/)).toBeTruthy();
  });
});
