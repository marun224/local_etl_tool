/**
 * The studio's document, and the assistant's draft beside it (decision 102).
 *
 * A draft is shown in place of the document without touching it: Discard is
 * then nothing but forgetting the draft, and the document comes back exactly
 * as it was, unsaved changes and all. Accept makes the draft the document,
 * unsaved, as any edit is. Edits while a draft is shown change the draft.
 */

import type { PipelineDoc } from "./document";
import type { StageResult, Validation } from "./ipc";

export interface Draft {
  document: PipelineDoc;
  /** What was asked, for *Try again*. */
  request: string;
}

export interface Studio {
  document: PipelineDoc;
  dirty: boolean;
  draft: Draft | null;
}

/** What the canvas draws and the engine is asked about. */
export function shown(studio: Studio): PipelineDoc {
  return studio.draft?.document ?? studio.document;
}

export function showDraft(studio: Studio, draft: Draft): Studio {
  return { ...studio, draft };
}

export function acceptDraft(studio: Studio): Studio {
  if (!studio.draft) return studio;
  return { document: studio.draft.document, dirty: true, draft: null };
}

export function discardDraft(studio: Studio): Studio {
  return { ...studio, draft: null };
}

/** An edit from the canvas or the inspector, to whatever is shown. */
export function editShown(studio: Studio, next: PipelineDoc): Studio {
  if (studio.draft) return { ...studio, draft: { ...studio.draft, document: next } };
  return { ...studio, document: next, dirty: true };
}

/** Node id → what the engine said about it, for the red boxes. */
export function problemsOf(
  validation: Validation | null,
  stages: StageResult[] = [],
): Map<string, string> {
  const found = new Map<string, string>();

  const blamed = validation?.error;
  if (blamed?.nodeId) found.set(blamed.nodeId, blamed.message);

  for (const stage of stages) {
    if (stage.skipped) found.set(stage.nodeId, stage.skipped);
  }

  return found;
}
