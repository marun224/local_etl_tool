//! Session protocol tests.
//!
//! The pure ones run anywhere. The rest need the vendored DuckDB and skip
//! without it, the same bargain the end-to-end tests make.

use super::*;
use crate::exec::{locate_duckdb, locate_extension_dir, RunOptions};

// ---------------------------------------------------------------------------
// The framing, without a process
// ---------------------------------------------------------------------------

#[test]
fn a_marker_line_yields_its_sequence_number() {
    assert_eq!(
        marker_sequence(r#"[{"__etl_mark":"__etl_mark_1__"}]"#),
        Some(1)
    );
    assert_eq!(
        marker_sequence(r#"[{"__etl_mark":"__etl_mark_4207__"}]"#),
        Some(4207)
    );
}

#[test]
fn ordinary_output_is_not_mistaken_for_a_marker() {
    assert_eq!(marker_sequence(r#"[{"n":12}]"#), None);
    assert_eq!(marker_sequence(""), None);
    assert_eq!(marker_sequence("__etl_mark_"), None);

    // A row that merely mentions the prefix without the closing delimiter is
    // data, not framing.
    assert_eq!(marker_sequence(r#"[{"note":"__etl_mark_x"}]"#), None);
}

#[test]
fn concatenated_arrays_are_read_as_separate_values() {
    let values = parse_values("[{\"n\":12}]\n[{\"n\":7}]\n").expect("parses");

    assert_eq!(values.len(), 2);
    assert_eq!(values[0][0]["n"], 12);
    assert_eq!(values[1][0]["n"], 7);
}

#[test]
fn an_array_spanning_several_lines_is_one_value() {
    // What DuckDB actually prints for a multi-row result.
    let text = "[{\"n\":1},\n{\"n\":2},\n{\"n\":3}]\n";
    let values = parse_values(text).expect("parses");

    assert_eq!(values.len(), 1, "three rows, one result");
    assert_eq!(values[0].as_array().unwrap().len(), 3);
}

#[test]
fn nothing_at_all_parses_to_no_values() {
    // What a failed statement leaves behind, and the signal the driver reads.
    assert!(parse_values("").expect("parses").is_empty());
    assert!(parse_values("\n\n").expect("parses").is_empty());
}

#[test]
fn an_error_marker_line_yields_its_sequence_number() {
    // Exactly what DuckDB prints for `SELECT error('__etl_errmark_3__')`.
    assert_eq!(
        error_marker_sequence("Invalid Input Error: __etl_errmark_3__"),
        Some(3)
    );
    assert_eq!(
        error_marker_sequence("Catalog Error: Table with name nope does not exist!"),
        None
    );
}

#[test]
fn the_two_markers_are_never_mistaken_for_each_other() {
    assert_eq!(marker_sequence(&error_marker_for(5)), None);
    assert_eq!(error_marker_sequence(&marker_for(5)), None);
    assert_eq!(marker_sequence(&marker_for(5)), Some(5));
    assert_eq!(error_marker_sequence(&error_marker_for(5)), Some(5));
}

// ---------------------------------------------------------------------------
// Attribution, with the timing forced
// ---------------------------------------------------------------------------

/// Two channels standing in for DuckDB's stdout and stderr.
fn pipes() -> (
    mpsc::Sender<String>,
    Receiver<String>,
    mpsc::Sender<String>,
    Receiver<String>,
) {
    let (out, lines) = mpsc::channel();
    let (err, errors) = mpsc::channel();
    (out, lines, err, errors)
}

#[test]
fn a_message_that_arrives_after_the_rows_still_belongs_to_its_statement() {
    // The race CI's first Linux run lost, made deterministic: statement 1's
    // marker is back on stdout well before its message reaches stderr.
    let (out, lines, err, errors) = pipes();

    std::thread::spawn(move || {
        out.send(marker_for(1)).unwrap();
        std::thread::sleep(Duration::from_millis(150));
        err.send("Catalog Error: Table with name nope does not exist!".into())
            .unwrap();
        err.send(format!("Invalid Input Error: {}", error_marker_for(1)))
            .unwrap();

        out.send(r#"[{"n":1}]"#.into()).unwrap();
        out.send(marker_for(2)).unwrap();
        err.send(format!("Invalid Input Error: {}", error_marker_for(2)))
            .unwrap();
    });

    let timeout = Duration::from_secs(5);

    let (_, first) = read_answer(&lines, &errors, 1, timeout).expect("statement 1");
    assert!(
        first.contains("nope"),
        "the late message is statement 1's: {first:?}"
    );

    let (rows, second) = read_answer(&lines, &errors, 2, timeout).expect("statement 2");
    assert!(rows.contains(r#""n":1"#));
    assert!(
        second.is_empty(),
        "and none of it reaches statement 2: {second:?}"
    );
}

#[test]
fn an_error_marker_from_another_statement_is_out_of_step() {
    let (out, lines, err, errors) = pipes();

    out.send(marker_for(1)).unwrap();
    err.send(format!("Invalid Input Error: {}", error_marker_for(2)))
        .unwrap();

    assert_eq!(
        read_answer(&lines, &errors, 1, Duration::from_secs(5)),
        Err(Wait::OutOfStep(2))
    );
}

#[test]
fn a_missing_error_marker_times_out_rather_than_hanging() {
    let (out, lines, _err, errors) = pipes();

    out.send(marker_for(1)).unwrap();

    assert_eq!(
        read_answer(&lines, &errors, 1, Duration::from_millis(50)),
        Err(Wait::TimedOut)
    );
}

// ---------------------------------------------------------------------------
// Against a real DuckDB
// ---------------------------------------------------------------------------

fn session() -> Option<Session> {
    let options = RunOptions::default();
    let binary = locate_duckdb(&options).ok()?;
    let extensions = locate_extension_dir(&options);

    Some(
        Session::open(&binary, None, extensions.as_deref(), &[])
            .expect("a session opens once the binary is found"),
    )
}

#[test]
fn a_statement_is_answered_with_its_own_output() {
    let Some(mut session) = session() else {
        return;
    };

    let answer = session.execute("SELECT 42 AS n;").expect("runs");

    assert_eq!(answer.values.len(), 1);
    assert_eq!(answer.values[0][0]["n"], 42);
    assert!(!answer.has_message());
}

#[test]
fn state_survives_between_statements() {
    // The whole reason this module exists: the one-script path cannot do this
    // across two invocations, and control flow needs it.
    let Some(mut session) = session() else {
        return;
    };

    session
        .execute("CREATE OR REPLACE TEMP VIEW v AS SELECT 7 AS n;")
        .expect("creates");

    let answer = session.execute("SELECT n FROM v;").expect("reads");

    assert_eq!(answer.values[0][0]["n"], 7);
}

#[test]
fn a_failed_statement_produces_no_values_and_a_message() {
    let Some(mut session) = session() else {
        return;
    };

    let answer = session.execute("SELECT * FROM nope;").expect("answers");

    assert!(
        answer.values.is_empty(),
        "no rows is the verdict: {:?}",
        answer.values
    );
    assert!(
        answer.stderr.contains("nope"),
        "and stderr carries the message: {}",
        answer.stderr
    );
}

#[test]
fn the_session_is_still_usable_after_a_failure() {
    // What `continue_on_failure` rests on. If a bad stage cost the session,
    // one failure would take the rest of the run with it.
    let Some(mut session) = session() else {
        return;
    };

    session
        .execute("CREATE OR REPLACE TEMP VIEW v AS SELECT 1 AS n;")
        .expect("creates");

    let _ = session.execute("SELECT * FROM nope;").expect("answers");

    let answer = session.execute("SELECT n FROM v;").expect("still works");
    assert_eq!(answer.values[0][0]["n"], 1, "the view outlived the error");

    let fresh = session
        .execute("CREATE OR REPLACE TEMP VIEW w AS SELECT 2 AS n; SELECT n FROM w;")
        .expect("still works");
    assert_eq!(
        fresh.values[0][0]["n"], 2,
        "and new state can still be made"
    );
}

#[test]
fn an_error_message_does_not_leak_into_the_next_statement() {
    let Some(mut session) = session() else {
        return;
    };

    let _ = session.execute("SELECT * FROM nope;").expect("answers");
    let answer = session.execute("SELECT 1 AS n;").expect("runs");

    assert!(
        !answer.has_message(),
        "the previous failure's message was reported once, not twice: {}",
        answer.stderr
    );
}

#[test]
fn alternating_failures_never_lend_their_message_to_a_neighbour() {
    // The CI failure as a stress test against the real process. Before stderr
    // was framed this passed on a quiet machine and failed on a busy one.
    let Some(mut session) = session() else {
        return;
    };

    for i in 0..200 {
        let failed = session.execute("SELECT * FROM nope;").expect("answers");
        assert!(
            failed.stderr.contains("nope"),
            "round {i}: a failure carries its own message: {:?}",
            failed.stderr
        );

        let fine = session.execute("SELECT 1 AS n;").expect("runs");
        assert!(
            !fine.has_message(),
            "round {i}: a success carries none: {:?}",
            fine.stderr
        );
    }
}

#[test]
fn a_prelude_that_fails_says_why() {
    // The prelude's LOADs are sent raw, ahead of the first statement, so their
    // message has to be attributed to it rather than dropped.
    let options = RunOptions::default();
    let Ok(binary) = locate_duckdb(&options) else {
        return;
    };
    let extensions = locate_extension_dir(&options);

    let outcome = Session::open(&binary, None, extensions.as_deref(), &["no_such_extension"]);

    match outcome {
        Err(SessionError::BadOutput(message)) => assert!(
            message.contains("no_such_extension"),
            "the message names the extension: {message}"
        ),
        Err(other) => panic!("expected the prelude to fail, got {other}"),
        Ok(_) => panic!("expected the prelude to fail"),
    }
}

#[test]
fn many_statements_stay_in_step() {
    // The marker carries a sequence number so a desync is caught rather than
    // silently misattributing one statement's rows to another. This would trip
    // it if the framing ever slipped.
    let Some(mut session) = session() else {
        return;
    };

    for i in 0..50 {
        let answer = session
            .execute(&format!("SELECT {i} AS n;"))
            .unwrap_or_else(|e| panic!("statement {i}: {e}"));

        assert_eq!(answer.values[0][0]["n"], i);
    }
}

#[test]
fn a_multi_row_result_comes_back_whole() {
    let Some(mut session) = session() else {
        return;
    };

    let answer = session
        .execute("SELECT * FROM (VALUES (1),(2),(3)) t(n);")
        .expect("runs");

    assert_eq!(answer.values.len(), 1);
    assert_eq!(answer.values[0].as_array().unwrap().len(), 3);
}

#[test]
fn a_statement_that_never_answers_times_out_rather_than_hanging() {
    let Some(session) = session() else {
        return;
    };

    // The failure mode the one-script path never had. A generous default would
    // make this test take ten minutes, so it is turned right down.
    let mut session = session.with_timeout(Duration::from_millis(400));

    let outcome = session.execute("SELECT count(*) FROM range(100000000000);");

    match outcome {
        Err(SessionError::Timeout { .. }) => {}
        Err(other) => panic!("expected a timeout, got {other}"),
        Ok(answer) => panic!("expected a timeout, got {} value(s)", answer.values.len()),
    }
}
