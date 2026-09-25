/**
 * The assistant: ask for a pipeline in words, get a draft on the canvas.
 *
 * The model runs on this machine (Phase 11b) and takes a minute or so, so the
 * panel says how long it has been waiting and offers Cancel. Each message asks
 * for a new pipeline (decision 101); *Try again* asks the same with a new
 * seed. Everything here is text the model or the engine wrote, and it goes in
 * as text: React escapes it, and nothing here sets HTML.
 */

import { useCallback, useEffect, useRef, useState } from "react";

import { parseDocument, type PipelineDoc } from "./document";
import { asIpcError, assistPipeline, cancelAssist, type AssistResult } from "./ipc";

export interface Exchange {
  id: number;
  request: string;
  status: "waiting" | "answered" | "failed" | "cancelled";
  answer?: AssistResult;
  error?: string;
}

export interface Assistant {
  exchanges: Exchange[];
  /** When the request under way started, or null. */
  waitingSince: number | null;
  ask: (request: string) => void;
  cancel: () => void;
}

/**
 * The conversation and the request under way. `onDraft` gets each answer's
 * document, valid or not (decision 103).
 */
export function useAssistant(
  onDraft: (document: PipelineDoc, request: string, answer: AssistResult) => void,
): Assistant {
  const [exchanges, setExchanges] = useState<Exchange[]>([]);
  const [waitingSince, setWaitingSince] = useState<number | null>(null);
  const next = useRef(1);
  const draft = useRef(onDraft);
  draft.current = onDraft;

  const settle = (id: number, change: Partial<Exchange>) =>
    setExchanges((all) => all.map((one) => (one.id === id ? { ...one, ...change } : one)));

  const ask = useCallback(
    (request: string) => {
      const text = request.trim();
      if (!text || waitingSince !== null) return;

      const id = next.current++;
      setExchanges((all) => [...all, { id, request: text, status: "waiting" }]);
      setWaitingSince(Date.now());

      assistPipeline(text)
        .then((answer) => {
          const document = parseDocument(answer.document);
          settle(id, { status: "answered", answer });
          draft.current(document, text, answer);
        })
        .catch((thrown) => {
          const error = asIpcError(thrown);
          if (error.stage === "cancelled") settle(id, { status: "cancelled" });
          else settle(id, { status: "failed", error: error.message });
        })
        .finally(() => setWaitingSince(null));
    },
    [waitingSince],
  );

  const cancel = useCallback(() => {
    cancelAssist().catch(() => undefined);
  }, []);

  return { exchanges, waitingSince, ask, cancel };
}

function seconds(since: number, now: number): string {
  return `${Math.max(0, Math.round((now - since) / 1000))} s`;
}

export interface AssistPanelProps {
  assistant: Assistant;
  onClose: () => void;
}

export function AssistPanel({ assistant, onClose }: AssistPanelProps) {
  const [request, setRequest] = useState("");
  const [now, setNow] = useState(Date.now());
  const waiting = assistant.waitingSince !== null;

  useEffect(() => {
    if (!waiting) return;
    const tick = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(tick);
  }, [waiting]);

  const submit = () => {
    if (!request.trim() || waiting) return;
    assistant.ask(request);
    setRequest("");
  };

  return (
    <aside className="inspector assist" aria-label="Assistant">
      <div className="assist-head">
        <h3>Assistant</h3>
        <span className="grow" />
        <button className="small" onClick={onClose}>
          Close
        </button>
      </div>

      <p className="muted small">
        A small model on this machine writes the pipeline; nothing leaves it. About a minute
        on a CPU. Read what it wrote before running it: values you did not give are its
        guesses.
      </p>

      <ol className="assist-log">
        {assistant.exchanges.map((exchange) => (
          <li key={exchange.id}>
            <p className="assist-request">{exchange.request}</p>
            <Outcome exchange={exchange} />
          </li>
        ))}
      </ol>

      {waiting ? (
        <div className="assist-waiting">
          <span className="muted">Writing… {seconds(assistant.waitingSince ?? now, now)}</span>
          <button onClick={assistant.cancel}>Cancel</button>
        </div>
      ) : (
        <div className="assist-ask">
          <textarea
            aria-label="Request"
            rows={3}
            placeholder="read this Postgres table, dedupe, write Parquet"
            value={request}
            onChange={(event) => setRequest(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter" && !event.shiftKey) {
                event.preventDefault();
                submit();
              }
            }}
          />
          <button className="primary" onClick={submit} disabled={!request.trim()}>
            Ask
          </button>
        </div>
      )}
    </aside>
  );
}

function Outcome({ exchange }: { exchange: Exchange }) {
  switch (exchange.status) {
    case "waiting":
      return <p className="muted small">Writing…</p>;
    case "cancelled":
      return <p className="muted small">Cancelled.</p>;
    case "failed":
      return <p className="error small">{exchange.error}</p>;
    case "answered": {
      const answer = exchange.answer!;
      const took = `${Math.round(answer.elapsedMs / 1000)} s`;
      return (
        <div className="small">
          {answer.validation.valid ? (
            <p className="ok">Valid: {answer.validation.stageCount} stages, on the canvas.</p>
          ) : (
            <p className="error">
              On the canvas, not valid: {answer.validation.error?.message ?? "unknown"}
            </p>
          )}
          <p className="muted">
            {took}, seed {answer.seed}. Offered: {answer.offered.join(", ")}
          </p>
        </div>
      );
    }
  }
}

export interface DraftBannerProps {
  request: string;
  valid: boolean | null;
  busy: boolean;
  onAccept: () => void;
  onDiscard: () => void;
  onRetry: () => void;
}

/** Above the canvas while a draft is shown in place of the pipeline. */
export function DraftBanner(props: DraftBannerProps) {
  const verdict =
    props.valid === null ? "checking" : props.valid ? "valid" : "not valid; fix it or try again";

  return (
    <div className="draft-banner" role="status">
      <span>
        Draft from the assistant for “{props.request}” ({verdict})
      </span>
      <span className="grow" />
      <button className="primary" onClick={props.onAccept} disabled={props.busy}>
        Accept
      </button>
      <button onClick={props.onDiscard} disabled={props.busy}>
        Discard
      </button>
      <button onClick={props.onRetry} disabled={props.busy}>
        Try again
      </button>
    </div>
  );
}
