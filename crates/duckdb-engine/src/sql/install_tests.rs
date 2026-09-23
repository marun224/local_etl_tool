//! What counts as an `INSTALL`, and what only looks like one.
//!
//! Both directions matter and they fail differently. A miss lets a pipeline
//! reach for the network from inside an artifact that has none; a false alarm
//! refuses somebody's legitimate SQL, and a check that cries wolf is a check
//! people route around. The second is the larger risk here, because the first
//! needs somebody to be trying.

use super::contains_install;

// ---------------------------------------------------------------------------
// Caught
// ---------------------------------------------------------------------------

#[test]
fn a_plain_install_is_caught() {
    assert!(contains_install("INSTALL httpfs;"));
}

#[test]
fn case_does_not_help() {
    assert!(contains_install("install httpfs;"));
    assert!(contains_install("InStAlL httpfs;"));
}

#[test]
fn an_install_after_a_harmless_statement_is_caught() {
    assert!(contains_install("SELECT 1;\nINSTALL httpfs;\nLOAD httpfs;"));
}

#[test]
fn force_install_and_install_from_are_caught() {
    assert!(contains_install("FORCE INSTALL spatial;"));
    assert!(contains_install("INSTALL h3 FROM community;"));
}

#[test]
fn an_install_hidden_after_a_comment_is_caught() {
    // The comment ends at the newline; what follows it does not get to inherit
    // the exemption.
    assert!(contains_install("-- nothing to see\nINSTALL httpfs;"));
    assert!(contains_install("/* nor here */ INSTALL httpfs;"));
}

#[test]
fn an_install_after_a_string_that_looks_unterminated_is_still_caught() {
    // Doubling is the escape, so `'it''s'` is one literal and the scanner has to
    // come out the other side of it still scanning.
    assert!(contains_install("SELECT 'it''s fine'; INSTALL httpfs;"));
}

#[test]
fn an_install_after_a_dollar_quoted_block_is_caught() {
    assert!(contains_install("SELECT $$ anything $$; INSTALL httpfs;"));
    assert!(contains_install("SELECT $tag$ x $tag$; INSTALL httpfs;"));
}

// ---------------------------------------------------------------------------
// Not caught, and must not be
// ---------------------------------------------------------------------------

#[test]
fn the_word_inside_a_string_literal_is_not_an_install() {
    // The check that fires on this is the check somebody learns to work around.
    assert!(!contains_install("SELECT 'preinstall' AS stage"));
    assert!(!contains_install("SELECT * FROM t WHERE note = 'INSTALL'"));
}

#[test]
fn the_word_inside_a_quoted_identifier_is_not_an_install() {
    assert!(!contains_install(r#"SELECT "install" FROM t"#));
}

#[test]
fn the_word_inside_a_comment_is_not_an_install() {
    assert!(!contains_install(
        "-- remember to INSTALL httpfs one day\nSELECT 1"
    ));
    assert!(!contains_install("/* INSTALL httpfs */ SELECT 1"));
}

#[test]
fn the_word_inside_a_dollar_quoted_block_is_not_an_install() {
    assert!(!contains_install("SELECT $$ INSTALL httpfs $$"));
}

#[test]
fn a_longer_word_that_merely_contains_it_is_not_an_install() {
    // The keyword has to be a word. These are the ones a plain `contains` gets
    // wrong, and every one of them is a plausible column name.
    assert!(!contains_install(
        "SELECT installed FROM duckdb_extensions()"
    ));
    assert!(!contains_install("SELECT preinstall_count FROM t"));
    assert!(!contains_install("SELECT install_date FROM t"));
    assert!(!contains_install("SELECT reinstall FROM t"));
}

#[test]
fn ordinary_generated_sql_passes() {
    // A sample of what the builders actually emit. If any of this tripped the
    // check, every pipeline in the project would stop compiling.
    for sql in [
        "CREATE OR REPLACE TEMP VIEW \"orders\" AS SELECT * FROM read_csv('data/orders.csv')",
        "COPY \"joined\" TO 'out/orders.parquet' (FORMAT parquet, COMPRESSION zstd)",
        "LOAD httpfs;",
        "ATTACH 'x.db' AS src (TYPE sqlite); SELECT * FROM src.orders",
        "SET extension_directory='C:/tools/duckdb/extensions';",
    ] {
        assert!(!contains_install(sql), "refused generated SQL: {sql}");
    }
}

#[test]
fn an_unterminated_literal_or_comment_does_not_hang_or_panic() {
    // Malformed SQL reaches here — it is checked before DuckDB ever sees it, so
    // this must terminate on input that will later be a syntax error.
    for sql in [
        "SELECT 'unterminated",
        "SELECT \"unterminated",
        "/* unterminated",
        "SELECT $$ unterminated",
        "SELECT $tag$ unterminated",
        "$",
        "",
    ] {
        let _ = contains_install(sql);
    }
}
