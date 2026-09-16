/**
 * The property that matters most here is not "are keywords blue" but "is every
 * character still there". A highlighter that drops or reorders input is
 * showing something other than what will run.
 */

import { describe, expect, it } from "vitest";
import { tokenize } from "./sql-highlight";

/** Reassembling the tokens must give back exactly what went in. */
function roundTrip(sql: string): string {
  return tokenize(sql)
    .map((token) => token.text)
    .join("");
}

describe("tokenize", () => {
  it("loses nothing, whatever the input", () => {
    const samples = [
      "SELECT * FROM orders",
      "-- a comment\nSELECT 1",
      "SELECT 'it''s quoted', \"odd column\" FROM t",
      "COPY (SELECT 1) TO 'out.parquet' (FORMAT parquet)",
      "",
      "   ",
      "SELECT 1.5, 42",
      "unterminated 'string",
      'unterminated "identifier',
      "¬∆˚ unicode and émojis 🎈",
    ];

    for (const sql of samples) {
      expect(roundTrip(sql)).toBe(sql);
    }
  });

  it("colours keywords regardless of case, and only whole words", () => {
    const kinds = new Map(tokenize("select selected FROM from_table").map((t) => [t.text, t.kind]));

    expect(kinds.get("select")).toBe("keyword");
    expect(kinds.get("FROM")).toBe("keyword");
    // `selected` and `from_table` merely start with a keyword.
    expect(kinds.get("select selected ")).toBeUndefined();
    expect(tokenize("selected")[0]).toEqual({ kind: "plain", text: "selected" });
    expect(tokenize("from_table")[0]).toEqual({ kind: "plain", text: "from_table" });
  });

  it("treats a doubled quote as an escape rather than the end", () => {
    const tokens = tokenize("SELECT 'it''s here' AS x");
    const string = tokens.find((token) => token.kind === "string");

    expect(string?.text).toBe("'it''s here'");
    // The `AS` after it is still found, which it would not be if the string
    // had been cut short at the doubled quote.
    expect(tokens.some((token) => token.kind === "keyword" && token.text === "AS")).toBe(true);
  });

  it("does not let a keyword inside a string or comment escape it", () => {
    expect(tokenize("'select from where'")).toEqual([
      { kind: "string", text: "'select from where'" },
    ]);

    const [comment, newline] = tokenize("-- select everything\n");
    expect(comment).toEqual({ kind: "comment", text: "-- select everything" });
    // The newline stays outside the comment, so spacing survives.
    expect(newline).toEqual({ kind: "plain", text: "\n" });
  });

  it("never emits an empty token", () => {
    const sql = "SELECT a, b FROM t WHERE x = 'y' -- note\n";
    expect(tokenize(sql).every((token) => token.text.length > 0)).toBe(true);
  });

  it("keeps markup as text rather than as a token boundary", () => {
    // The reason this file exists instead of Prism: a path like this reaches
    // the panel through the generated SQL, and must render as characters.
    const sql = "COPY t TO '<img src=x onerror=alert(1)>.parquet'";
    expect(roundTrip(sql)).toBe(sql);
    expect(tokenize(sql).some((token) => token.kind === "string")).toBe(true);
  });
});
