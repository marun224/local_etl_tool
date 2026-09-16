//! SQL quoting.
//!
//! Every generated statement passes through here. The rules are small but
//! load-bearing: an unquoted identifier or literal is how a column named
//! `order` or a path containing an apostrophe turns into a syntax error — or,
//! with attacker-controlled input, into something worse.
//!
//! DuckDB's escape in both positions is **doubling**: `"` inside a quoted
//! identifier, `'` inside a string literal. Single-quoted literals do no
//! backslash processing at all, which is why Windows paths need no special
//! handling here — `D:\data\x.csv` is already a valid literal.

/// Quote an identifier: a table, view, or column name.
///
/// Always quotes, even when the name looks safe. Unconditional quoting means
/// there is no "is this a reserved word" judgement call to get wrong, and
/// `SELECT "order"` works where `SELECT order` does not.
pub fn quote_identifier(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');

    for character in name.chars() {
        if character == '"' {
            out.push('"');
        }
        out.push(character);
    }

    out.push('"');
    out
}

/// Quote a string literal.
pub fn quote_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');

    for character in value.chars() {
        if character == '\'' {
            out.push('\'');
        }
        out.push(character);
    }

    out.push('\'');
    out
}

/// Quote a filesystem path as a literal, normalising separators on the way.
///
/// Backslashes are rewritten to forward slashes purely for legibility — the
/// generated SQL is shown to the user on the plan view, and `D:/data/x.csv`
/// reads better than `D:\data\x.csv`. Both forms resolve identically on
/// Windows, so this changes appearance and not behaviour.
pub fn quote_path(path: &str) -> String {
    quote_literal(&path.replace('\\', "/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_always_quoted() {
        assert_eq!(quote_identifier("orders"), r#""orders""#);
        assert_eq!(quote_identifier("order"), r#""order""#);
        assert_eq!(quote_identifier("n1"), r#""n1""#);
    }

    #[test]
    fn a_double_quote_in_an_identifier_is_doubled() {
        assert_eq!(quote_identifier(r#"we"ird"#), r#""we""ird""#);
        assert_eq!(quote_identifier(r#"""#), r#""""""#);
    }

    #[test]
    fn a_single_quote_in_a_literal_is_doubled() {
        assert_eq!(quote_literal("O'Brien"), "'O''Brien'");
        assert_eq!(quote_literal("'"), "''''");
    }

    #[test]
    fn a_quote_injection_attempt_stays_inside_the_literal() {
        // The classic: without doubling this would close the literal and start
        // a new statement.
        let hostile = "x'; DROP TABLE orders; --";

        assert_eq!(
            quote_literal(hostile),
            "'x''; DROP TABLE orders; --'",
            "the payload must remain one literal"
        );
    }

    #[test]
    fn backslashes_are_left_alone_in_literals() {
        // DuckDB does not process escapes in single-quoted strings, so a
        // Windows path is already valid and must not be doubled.
        assert_eq!(
            quote_literal(r"D:\data\orders.csv"),
            r"'D:\data\orders.csv'"
        );
    }

    #[test]
    fn paths_are_normalised_to_forward_slashes_for_readability() {
        assert_eq!(
            quote_path(r"D:\workspace\orders.csv"),
            "'D:/workspace/orders.csv'"
        );
        assert_eq!(
            quote_path("samples/data/orders.csv"),
            "'samples/data/orders.csv'"
        );
    }

    #[test]
    fn a_quote_in_a_path_is_still_escaped() {
        assert_eq!(
            quote_path(r"D:\Bob's Files\x.csv"),
            "'D:/Bob''s Files/x.csv'"
        );
    }

    #[test]
    fn unicode_survives_both_forms() {
        assert_eq!(quote_identifier("größe"), r#""größe""#);
        assert_eq!(quote_literal("café ☕"), "'café ☕'");
        assert_eq!(quote_path("data/日本/x.csv"), "'data/日本/x.csv'");
    }

    #[test]
    fn empty_input_produces_an_empty_quoted_token() {
        assert_eq!(quote_identifier(""), r#""""#);
        assert_eq!(quote_literal(""), "''");
    }
}
