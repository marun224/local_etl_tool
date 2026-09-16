//! Intervals, and what they are counted from.

use super::*;

fn seconds_of(text: &str) -> u64 {
    Interval::parse(text).expect("parses").seconds()
}

#[test]
fn a_number_and_a_unit_parses() {
    assert_eq!(seconds_of("30s"), 30);
    assert_eq!(seconds_of("5m"), 300);
    assert_eq!(seconds_of("1h"), 3_600);
    assert_eq!(seconds_of("1d"), 86_400);
}

#[test]
fn units_add_up_in_any_order() {
    assert_eq!(seconds_of("2h30m"), 9_000);
    assert_eq!(seconds_of("30m2h"), 9_000);
    assert_eq!(seconds_of("1h30m15s"), 5_415);
    // The same interval written two ways is the same interval.
    assert_eq!(seconds_of("90m"), seconds_of("1h30m"));
}

#[test]
fn case_and_spacing_do_not_matter() {
    assert_eq!(seconds_of("1H"), 3_600);
    assert_eq!(seconds_of(" 2h 30m "), 9_000);
}

#[test]
fn a_bare_number_is_refused_and_the_message_offers_both_readings() {
    // The whole reason this is an error: "60" is a minute to half the people
    // who write it and an hour to the other half.
    let error = Interval::parse("60").expect_err("no unit");

    assert_eq!(
        error.to_string(),
        "'60' has no unit: write 60s for seconds or 60m for minutes"
    );
}

#[test]
fn an_unknown_unit_lists_the_ones_that_work() {
    let error = Interval::parse("5w").expect_err("weeks are not a unit here");

    assert!(
        matches!(error, IntervalError::UnknownUnit { .. }),
        "{error}"
    );
    assert!(error.to_string().contains("s (seconds)"));
}

#[test]
fn zero_is_refused() {
    // A zero interval is a busy loop wearing a schedule's clothes.
    assert_eq!(Interval::parse("0s"), Err(IntervalError::Zero));
    assert_eq!(Interval::parse("0h0m"), Err(IntervalError::Zero));
}

#[test]
fn nonsense_is_refused() {
    assert!(Interval::parse("").is_err());
    assert!(Interval::parse("hm").is_err());
    assert!(Interval::parse("h5").is_err());
    assert!(Interval::parse("every hour").is_err());
}

#[test]
fn an_absurd_interval_is_treated_as_a_typo() {
    let error = Interval::parse("500d").expect_err("longer than the guard");

    assert!(matches!(error, IntervalError::TooLong { .. }), "{error}");
    assert!(Interval::parse("399d").is_ok());
}

#[test]
fn a_number_too_large_to_hold_does_not_panic() {
    assert!(matches!(
        Interval::parse("99999999999999999999999d"),
        Err(IntervalError::Overflow { .. })
    ));
}

#[test]
fn it_echoes_back_what_was_written() {
    // `90m` must not come back as `1h30m`: the person reading the list is
    // checking it against the file they wrote.
    assert_eq!(Interval::parse("90m").unwrap().to_string(), "90m");
    assert_eq!(Interval::parse(" 2h30m ").unwrap().to_string(), "2h30m");
}

// ---------------------------------------------------------------------------
// Counting from the anchor
// ---------------------------------------------------------------------------

#[test]
fn the_next_tick_is_strictly_after_the_moment_given() {
    let hourly = Interval::parse("1h").expect("parses");

    // Asked at exactly one tick, the answer is the next one — otherwise a
    // schedule that just fired would fire again immediately.
    assert_eq!(hourly.next_after(0, 3_600), 7_200);
    assert_eq!(hourly.next_after(0, 3_599), 3_600);
}

#[test]
fn ticks_are_counted_from_the_anchor_not_from_now() {
    // The anchor is the last recorded run. An hourly pipeline that last ran
    // at 00:00 is due at 01:00, however many times the scheduler restarted
    // in between — that is the whole reason the anchor comes from history.
    let hourly = Interval::parse("1h").expect("parses");
    let last_run = 1_000;

    assert_eq!(hourly.next_after(last_run, last_run), last_run + 3_600);
    assert_eq!(hourly.next_after(last_run, last_run + 10), last_run + 3_600);
}

#[test]
fn an_anchor_in_the_future_is_the_next_tick_itself() {
    // A run recorded ahead of the clock — a machine whose time was corrected
    // backwards. Firing at the anchor rather than immediately is the quiet
    // direction: it waits rather than running in a loop until the clock
    // catches up.
    let hourly = Interval::parse("1h").expect("parses");

    assert_eq!(hourly.next_after(5_000, 1_000), 5_000);
}

#[test]
fn a_long_gap_lands_on_a_tick_boundary_rather_than_now() {
    // Down for three and a half hours on an hourly schedule: the next tick is
    // the next whole hour from the anchor, not "three and a half hours from
    // the anchor".
    let hourly = Interval::parse("1h").expect("parses");
    let anchor = 0;

    assert_eq!(hourly.next_after(anchor, 12_600), 14_400);
}

#[test]
fn missed_ticks_are_counted_but_not_queued() {
    let ten_minutes = Interval::parse("10m").expect("parses");
    let scheduled = 1_000;

    assert_eq!(ten_minutes.missed_between(scheduled, scheduled), 0);
    assert_eq!(ten_minutes.missed_between(scheduled, scheduled - 5), 0);
    // A run that overran by 25 minutes passed two further ticks.
    assert_eq!(ten_minutes.missed_between(scheduled, scheduled + 1_500), 2);
    assert_eq!(ten_minutes.missed_between(scheduled, scheduled + 600), 1);
}
