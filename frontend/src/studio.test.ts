/**
 * A draft stands in for the document without touching it (decision 102), so
 * Discard is exact and Accept is one edit.
 */

import { describe, expect, it } from "vitest";

import type { PipelineDoc } from "./document";
import type { Validation } from "./ipc";
import {
  acceptDraft,
  discardDraft,
  editShown,
  problemsOf,
  showDraft,
  shown,
  type Studio,
} from "./studio";

function pipeline(id: string): PipelineDoc {
  return {
    formatVersion: 1,
    nodes: [
      {
        id,
        type: "source",
        position: { x: 0, y: 0 },
        data: { label: id, componentId: "src.file.csv", properties: { path: "in.csv" } },
      },
    ],
    edges: [],
  };
}

const mine: Studio = { document: pipeline("mine"), dirty: true, draft: null };
const drafted = showDraft(mine, { document: pipeline("drafted"), request: "csv to parquet" });

describe("a draft", () => {
  it("is what the canvas shows, and the document is untouched", () => {
    expect(shown(drafted).nodes[0]?.id).toBe("drafted");
    expect(drafted.document).toBe(mine.document);
    expect(drafted.dirty).toBe(true);
  });

  it("once accepted is the document, unsaved", () => {
    const accepted = acceptDraft(drafted);

    expect(accepted.draft).toBeNull();
    expect(accepted.document.nodes[0]?.id).toBe("drafted");
    expect(accepted.dirty).toBe(true);

    const clean = acceptDraft(showDraft({ ...mine, dirty: false }, drafted.draft!));
    expect(clean.dirty).toBe(true);
  });

  it("once discarded leaves the document exactly as it was, unsaved changes and all", () => {
    const discarded = discardDraft(drafted);

    expect(discarded).toEqual(mine);
    expect(discarded.document).toBe(mine.document);
  });

  it("takes the edits made while it is shown, and the document does not", () => {
    const edited = editShown(drafted, pipeline("edited"));

    expect(shown(edited).nodes[0]?.id).toBe("edited");
    expect(edited.document).toBe(mine.document);
    expect(edited.draft?.request).toBe("csv to parquet");
  });

  it("does not stop an ordinary edit marking the document unsaved", () => {
    const edited = editShown({ ...mine, dirty: false }, pipeline("edited"));

    expect(edited.document.nodes[0]?.id).toBe("edited");
    expect(edited.dirty).toBe(true);
  });

  it("accepting with no draft changes nothing", () => {
    expect(acceptDraft(mine)).toBe(mine);
  });
});

describe("problems", () => {
  it("put an invalid draft's error on the node it names", () => {
    const validation: Validation = {
      valid: false,
      error: { message: "'drafted' needs a path", nodeId: "drafted", stage: "compile" },
      stageCount: 0,
      sinkCount: 0,
      warnings: [],
    };

    expect(problemsOf(validation).get("drafted")).toBe("'drafted' needs a path");
  });

  it("are none for a valid document", () => {
    const validation: Validation = {
      valid: true,
      error: null,
      stageCount: 1,
      sinkCount: 0,
      warnings: [],
    };

    expect(problemsOf(validation).size).toBe(0);
    expect(problemsOf(null).size).toBe(0);
  });
});
