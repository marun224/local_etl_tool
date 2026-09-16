//! Tokens and roles. The first code in this project that decides whether a
//! stranger gets in, so the tests are about what it refuses.

use super::*;

fn tokens() -> Tokens {
    Tokens::build(
        Some("operator-token".to_string()),
        Some("viewer-token".to_string()),
    )
}

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

#[test]
fn an_operator_may_do_what_a_viewer_may() {
    assert!(Role::Operator.allows(Role::Viewer));
    assert!(Role::Operator.allows(Role::Operator));
}

#[test]
fn a_viewer_may_not_do_what_an_operator_may() {
    // The whole point of having two roles.
    assert!(Role::Viewer.allows(Role::Viewer));
    assert!(!Role::Viewer.allows(Role::Operator));
}

#[test]
fn the_right_token_gets_the_right_role() {
    let tokens = tokens();

    assert_eq!(tokens.role_for("operator-token"), Some(Role::Operator));
    assert_eq!(tokens.role_for("viewer-token"), Some(Role::Viewer));
}

#[test]
fn a_wrong_token_gets_nothing() {
    let tokens = tokens();

    for attempt in [
        "",
        " ",
        "nope",
        "operator-toke",   // one short
        "operator-token ", // one long
        "OPERATOR-TOKEN",  // tokens are not case-insensitive
        "operator-tokenX",
    ] {
        assert_eq!(tokens.role_for(attempt), None, "{attempt:?} was let in");
    }
}

#[test]
fn an_empty_environment_variable_is_not_an_empty_token() {
    // `ETL_CONSOLE_VIEWER_TOKEN=` in a shell script is somebody clearing it.
    // Accepting "" would open the console to anyone sending no token at all.
    let tokens = Tokens::build(Some(String::new()), Some("   ".to_string()));

    assert_eq!(tokens.role_for(""), None);
    assert_eq!(tokens.role_for("   "), None);

    // And it minted real ones instead.
    assert_eq!(tokens.operator_source(), Source::Minted);
    assert_eq!(tokens.viewer_source(), Source::Minted);
    assert_eq!(tokens.operator().len(), 64);
}

#[test]
fn two_roles_sharing_a_token_is_detected() {
    // It silently promotes every viewer to an operator, which is the one
    // mistake here that looks like it is working.
    let same = Tokens::build(Some("same".to_string()), Some("same".to_string()));
    assert!(same.roles_collide());

    assert!(!tokens().roles_collide());
}

#[test]
fn a_collision_grants_the_higher_role_rather_than_the_lower() {
    // If it is allowed through at all, it must not be the surprise that a
    // token someone believes is an operator's turns out to be a viewer's.
    let same = Tokens::build(Some("same".to_string()), Some("same".to_string()));

    assert_eq!(same.role_for("same"), Some(Role::Operator));
}

// ---------------------------------------------------------------------------
// Minting
// ---------------------------------------------------------------------------

#[test]
fn minted_tokens_are_long_random_and_different_from_each_other() {
    let tokens = Tokens::build(None, None);

    assert_eq!(tokens.operator().len(), 64, "32 bytes of hex");
    assert_eq!(tokens.viewer().len(), 64);
    assert_ne!(tokens.operator(), tokens.viewer());
    assert!(!tokens.roles_collide());
}

#[test]
fn a_minted_token_is_marked_for_printing_and_an_environment_one_is_not() {
    // A minted token exists nowhere else, so it has to be shown. A stable one
    // from the environment is somebody's secret and must not end up in the
    // terminal scrollback or a CI log.
    let mixed = Tokens::build(Some("from-env".to_string()), None);

    assert_eq!(mixed.operator_source(), Source::Environment);
    assert_eq!(mixed.viewer_source(), Source::Minted);
}

#[test]
fn every_minted_console_gets_its_own_tokens() {
    // Restarting must invalidate yesterday's link.
    let first = Tokens::build(None, None);
    let second = Tokens::build(None, None);

    assert_ne!(first.operator(), second.operator());
    assert_ne!(first.viewer(), second.viewer());
}

// ---------------------------------------------------------------------------
// Constant-time comparison
// ---------------------------------------------------------------------------

#[test]
fn constant_time_eq_is_still_a_correct_comparison() {
    // Constant time is worth nothing if it gets the answer wrong.
    assert!(constant_time_eq(b"", b""));
    assert!(constant_time_eq(b"abc", b"abc"));
    assert!(!constant_time_eq(b"abc", b"abd"));
    assert!(!constant_time_eq(b"abc", b"ab"));
    assert!(!constant_time_eq(b"", b"a"));
    // A difference in the first byte and in the last are both differences.
    assert!(!constant_time_eq(b"xbc", b"abc"));
    assert!(!constant_time_eq(b"abx", b"abc"));
}

#[test]
fn constant_time_eq_does_not_stop_at_the_first_difference() {
    // Checked by construction rather than by timing, which would be flaky on
    // a shared machine: a byte that differs early must not mask one that
    // differs late, and the accumulator is what guarantees that.
    assert!(!constant_time_eq(b"\x01\x00", b"\x00\x01"));
    assert!(!constant_time_eq(b"\xff\xff", b"\x00\x00"));
}

// ---------------------------------------------------------------------------
// Reading the header
// ---------------------------------------------------------------------------

#[test]
fn a_bearer_header_yields_its_token() {
    assert_eq!(bearer_of("Bearer abc123"), Some("abc123"));
    // Clients differ on the scheme's case.
    assert_eq!(bearer_of("bearer abc123"), Some("abc123"));
    assert_eq!(bearer_of("BEARER abc123"), Some("abc123"));
    assert_eq!(bearer_of("  Bearer   abc123  "), Some("abc123"));
}

#[test]
fn anything_that_is_not_a_bearer_header_yields_nothing() {
    for header in [
        "",
        "abc123",       // no scheme
        "Basic abc123", // a scheme we do not take
        "Bearer",       // no token
        "Bearer ",      // no token
        "Bearertoken",  // no space
    ] {
        assert_eq!(bearer_of(header), None, "{header:?} was accepted");
    }
}
