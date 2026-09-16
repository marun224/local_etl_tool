/**
 * Syntax highlighting for the SQL the engine generates.
 *
 * **Why this is not Prism, which the plan named.** Prism highlights by handing
 * back a string of HTML, which a React app renders with
 * `dangerouslySetInnerHTML`. The strings we would be feeding it are not ours
 * alone: a node's file path, a raw `xf.sql` body and a node id all end up
 * inside the generated SQL, so a pipeline document would be choosing what HTML
 * this window renders. That is a poor trade in a webview holding IPC commands
 * that read and write files, and it buys a dialect-generic highlighter for SQL
 * that this engine generates itself.
 *
 * Tokenising to React elements instead makes the hole structurally impossible:
 * every token is a text child, and React escapes text children. The cost is
 * this file — small, because the input is one known dialect rather than
 * whatever a user pastes.
 */

import { type ReactElement } from "react";

/** What a token is, which is also its CSS class. */
export type TokenKind = "keyword" | "string" | "number" | "comment" | "plain";

export interface Token {
  kind: TokenKind;
  text: string;
}

/**
 * The words worth colouring.
 *
 * Deliberately the words this engine emits rather than every word DuckDB
 * accepts: a keyword list that highlights things the generated SQL never
 * contains is a list nobody can tell is wrong.
 */
const KEYWORDS = new Set([
  "all", "and", "as", "asc", "attach", "between", "by", "case", "cast", "copy",
  "create", "cross", "database", "delimiter", "desc", "detach", "distinct",
  "drop", "else", "end", "except", "exists", "false", "format", "from", "full",
  "group", "having", "header", "in", "inner", "insert", "install", "intersect",
  "into", "is", "join", "left", "like", "limit", "load", "not", "null",
  "nulls", "offset", "on", "or", "order", "outer", "over", "partition",
  "pivot", "qualify", "replace", "right", "select", "set", "table", "temp",
  "then", "true", "union", "unpivot", "using", "values", "view", "when",
  "where", "window", "with",
]);

/**
 * Split SQL into coloured tokens.
 *
 * Every character of the input lands in exactly one token, so joining the
 * tokens back together reproduces the input byte for byte. That property is
 * what makes this safe to render: nothing is dropped, nothing is invented, and
 * a construct the tokeniser does not understand degrades to `plain` rather
 * than swallowing the rest of the statement.
 */
export function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  let plain = "";

  const flush = () => {
    if (plain) {
      tokens.push({ kind: "plain", text: plain });
      plain = "";
    }
  };

  const push = (kind: TokenKind, text: string) => {
    flush();
    tokens.push({ kind, text });
  };

  let at = 0;

  while (at < sql.length) {
    const rest = sql.slice(at);

    // A line comment runs to the newline, which stays outside it so blank
    // lines between statements keep their spacing.
    if (rest.startsWith("--")) {
      const end = rest.indexOf("\n");
      const text = end === -1 ? rest : rest.slice(0, end);
      push("comment", text);
      at += text.length;
      continue;
    }

    // A single-quoted literal, in which '' is an escaped quote rather than an
    // empty string followed by another one.
    if (rest.startsWith("'")) {
      const text = readQuoted(rest, "'");
      push("string", text);
      at += text.length;
      continue;
    }

    // A double-quoted identifier. Coloured as a string because that is what it
    // looks like, and because the alternative is colouring every quoted column
    // name as a keyword it is not.
    if (rest.startsWith('"')) {
      const text = readQuoted(rest, '"');
      push("string", text);
      at += text.length;
      continue;
    }

    const word = /^[A-Za-z_][A-Za-z0-9_]*/.exec(rest);
    if (word) {
      const text = word[0];
      if (KEYWORDS.has(text.toLowerCase())) push("keyword", text);
      else plain += text;
      at += text.length;
      continue;
    }

    const number = /^\d+(\.\d+)?/.exec(rest);
    if (number) {
      push("number", number[0]);
      at += number[0].length;
      continue;
    }

    plain += sql[at];
    at += 1;
  }

  flush();
  return tokens;
}

/**
 * Read a quoted run, including both quotes.
 *
 * An unterminated quote returns the rest of the input rather than throwing:
 * the SQL shown in this panel may be mid-edit or may have come back with an
 * error in it, and a highlighter that refuses to render malformed SQL hides
 * the thing someone opened the tab to read.
 */
function readQuoted(rest: string, quote: string): string {
  let at = 1;

  while (at < rest.length) {
    if (rest[at] === quote) {
      // A doubled quote is an escaped one, so step over both and carry on.
      if (rest[at + 1] === quote) {
        at += 2;
        continue;
      }
      return rest.slice(0, at + 1);
    }
    at += 1;
  }

  return rest;
}

/** The generated SQL, coloured. */
export function Sql({ sql }: { sql: string }): ReactElement {
  return (
    <pre className="sql">
      {tokenize(sql).map((token, index) =>
        token.kind === "plain" ? (
          token.text
        ) : (
          <span key={index} className={`tok-${token.kind}`}>
            {token.text}
          </span>
        ),
      )}
    </pre>
  );
}
