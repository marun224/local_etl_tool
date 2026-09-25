/**
 * @vitest-environment jsdom
 */

/**
 * The assistant panel, with the engine mocked: it asks, waits with Cancel,
 * shows what came back as text, and hands every answer's document on, valid
 * or not.
 */

import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { AssistResult } from "./ipc";

const ipc = vi.hoisted(() => ({
  assistPipeline: vi.fn(),
  cancelAssist: vi.fn(),
}));

vi.mock("./ipc", async (actual) => ({
  ...(await actual<typeof import("./ipc")>()),
  assistPipeline: ipc.assistPipeline,
  cancelAssist: ipc.cancelAssist,
}));

import { AssistPanel, DraftBanner, useAssistant } from "./AssistPanel";
import type { PipelineDoc } from "./document";

afterEach(cleanup);
beforeEach(() => {
  ipc.assistPipeline.mockReset();
  ipc.cancelAssist.mockReset().mockResolvedValue(undefined);
});

const DOCUMENT = JSON.stringify({
  formatVersion: 1,
  nodes: [
    {
      id: "pg",
      type: "source",
      position: { x: 0, y: 0 },
      data: { label: "Orders", componentId: "src.db.postgres" },
    },
  ],
  edges: [],
});

function answer(over: Partial<AssistResult> = {}): AssistResult {
  return {
    document: DOCUMENT,
    offered: ["src.db.postgres", "xf.dedup", "snk.file.parquet"],
    seed: 42,
    elapsedMs: 23_000,
    validation: { valid: true, error: null, stageCount: 3, sinkCount: 1, warnings: [] },
    ...over,
  };
}

/** A promise the test settles when it chooses. */
function pending<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

function Harness({ onDraft }: { onDraft: (doc: PipelineDoc, request: string) => void }) {
  const assistant = useAssistant(onDraft);
  return <AssistPanel assistant={assistant} onClose={() => undefined} />;
}

function ask(text: string) {
  fireEvent.change(screen.getByLabelText("Request"), { target: { value: text } });
  fireEvent.click(screen.getByText("Ask"));
}

describe("the assistant panel", () => {
  it("asks, waits with Cancel, then shows the answer and hands the draft on", async () => {
    const reply = pending<AssistResult>();
    ipc.assistPipeline.mockReturnValue(reply.promise);
    const onDraft = vi.fn();
    render(<Harness onDraft={onDraft} />);

    ask("read this Postgres table, dedupe, write Parquet");

    expect(ipc.assistPipeline).toHaveBeenCalledWith(
      "read this Postgres table, dedupe, write Parquet",
    );
    expect(screen.getByText("Cancel")).toBeTruthy();
    expect(screen.queryByLabelText("Request")).toBeNull();

    await act(async () => reply.resolve(answer()));

    expect(screen.getByText("Valid: 3 stages, on the canvas.")).toBeTruthy();
    expect(screen.getByText(/seed 42/).textContent).toContain(
      "src.db.postgres, xf.dedup, snk.file.parquet",
    );
    expect(onDraft).toHaveBeenCalledTimes(1);
    expect(onDraft.mock.calls[0]?.[0].nodes[0]?.id).toBe("pg");
    expect(onDraft.mock.calls[0]?.[1]).toBe("read this Postgres table, dedupe, write Parquet");
    expect(screen.getByLabelText("Request")).toBeTruthy();
  });

  it("hands on an invalid draft too, and says why it is not valid", async () => {
    ipc.assistPipeline.mockResolvedValue(
      answer({
        validation: {
          valid: false,
          error: { message: "'pg' needs a table", nodeId: "pg", stage: "compile" },
          stageCount: 0,
          sinkCount: 0,
          warnings: [],
        },
      }),
    );
    const onDraft = vi.fn();
    render(<Harness onDraft={onDraft} />);

    await act(async () => ask("postgres to parquet"));

    expect(screen.getByText("On the canvas, not valid: 'pg' needs a table")).toBeTruthy();
    expect(onDraft).toHaveBeenCalledTimes(1);
  });

  it("shows an error as text, markup and all", async () => {
    ipc.assistPipeline.mockRejectedValue({
      message: "the model was not found <b>here</b>. Fetch it with ./scripts/fetch-model.ps1.",
      nodeId: null,
      stage: "assist",
    });
    const onDraft = vi.fn();
    const { container } = render(<Harness onDraft={onDraft} />);

    await act(async () => ask("csv to parquet"));

    expect(screen.getByText(/fetch-model\.ps1/).textContent).toContain("<b>here</b>");
    expect(container.querySelector("b")).toBeNull();
    expect(onDraft).not.toHaveBeenCalled();
  });

  it("Cancel asks the engine to stop, and a cancelled request says so", async () => {
    const reply = pending<AssistResult>();
    ipc.assistPipeline.mockReturnValue(reply.promise);
    render(<Harness onDraft={vi.fn()} />);

    ask("csv to parquet");
    fireEvent.click(screen.getByText("Cancel"));
    expect(ipc.cancelAssist).toHaveBeenCalledTimes(1);

    await act(async () =>
      reply.reject({ message: "Cancelled.", nodeId: null, stage: "cancelled" }),
    );

    expect(screen.getByText("Cancelled.")).toBeTruthy();
    expect(screen.getByLabelText("Request")).toBeTruthy();
  });

  it("will not ask for nothing", () => {
    render(<Harness onDraft={vi.fn()} />);

    fireEvent.change(screen.getByLabelText("Request"), { target: { value: "   " } });

    expect((screen.getByText("Ask") as HTMLButtonElement).disabled).toBe(true);
    fireEvent.keyDown(screen.getByLabelText("Request"), { key: "Enter" });
    expect(ipc.assistPipeline).not.toHaveBeenCalled();
  });

  it("Enter asks; Shift+Enter is a new line", () => {
    ipc.assistPipeline.mockReturnValue(pending<AssistResult>().promise);
    render(<Harness onDraft={vi.fn()} />);
    const box = screen.getByLabelText("Request");

    fireEvent.change(box, { target: { value: "csv to parquet" } });
    fireEvent.keyDown(box, { key: "Enter", shiftKey: true });
    expect(ipc.assistPipeline).not.toHaveBeenCalled();

    fireEvent.keyDown(box, { key: "Enter" });
    expect(ipc.assistPipeline).toHaveBeenCalledWith("csv to parquet");
  });
});

describe("the draft banner", () => {
  it("offers Accept, Discard and Try again, and says whether the draft is valid", () => {
    const onAccept = vi.fn();
    const onDiscard = vi.fn();
    const onRetry = vi.fn();
    render(
      <DraftBanner
        request="csv to parquet"
        valid={false}
        busy={false}
        onAccept={onAccept}
        onDiscard={onDiscard}
        onRetry={onRetry}
      />,
    );

    expect(screen.getByRole("status").textContent).toContain("not valid");
    fireEvent.click(screen.getByText("Accept"));
    fireEvent.click(screen.getByText("Discard"));
    fireEvent.click(screen.getByText("Try again"));
    expect(onAccept).toHaveBeenCalledTimes(1);
    expect(onDiscard).toHaveBeenCalledTimes(1);
    expect(onRetry).toHaveBeenCalledTimes(1);
  });

  it("holds its buttons while the assistant is writing", () => {
    render(
      <DraftBanner
        request="csv to parquet"
        valid={true}
        busy={true}
        onAccept={vi.fn()}
        onDiscard={vi.fn()}
        onRetry={vi.fn()}
      />,
    );

    for (const name of ["Accept", "Discard", "Try again"]) {
      expect((screen.getByText(name) as HTMLButtonElement).disabled).toBe(true);
    }
  });
});
