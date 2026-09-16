//! Cron, checked against answers a person can verify against a calendar.

use super::*;

/// A timestamp from civil parts, so the tests read as dates rather than
/// epoch seconds.
fn at(year: i64, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
    DateTime {
        year,
        month,
        day,
        hour,
        minute,
        second: 0,
    }
    .to_unix()
}

/// The next fire after a given moment, as `YYYY-MM-DDTHH:MM:SSZ`.
fn next(expression: &str, from: i64) -> String {
    Cron::parse(expression)
        .expect("parses")
        .next_after(from)
        .map(|fire| DateTime::from_unix(fire).to_rfc3339())
        .unwrap_or_else(|| "never".to_string())
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

#[test]
fn every_minute_parses() {
    assert!(Cron::parse("* * * * *").is_ok());
}

#[test]
fn a_field_count_that_is_not_five_is_named_in_the_error() {
    let error = Cron::parse("0 3 * *").expect_err("four fields is not a cron expression");

    assert!(
        matches!(error, CronError::WrongFieldCount { found: 4, .. }),
        "{error}"
    );
    // The message says what the five fields are, because the usual mistake is
    // a six-field expression copied from a system that has a seconds column.
    assert!(error.to_string().contains("minute, hour, day-of-month"));
}

#[test]
fn a_six_field_expression_is_refused_rather_than_misread() {
    // Quartz and some other schedulers put seconds first. Silently reading
    // that as minute-hour-dom-month-dow would run it at the wrong time.
    let error = Cron::parse("0 0 3 * * *").expect_err("six fields is not this grammar");

    assert!(matches!(error, CronError::WrongFieldCount { found: 6, .. }));
}

#[test]
fn out_of_range_values_name_the_field_and_the_range() {
    let error = Cron::parse("60 * * * *").expect_err("there is no minute 60");

    assert_eq!(
        error.to_string(),
        "in the minute field, 60 is out of range (0-59)"
    );

    let error = Cron::parse("* 24 * * *").expect_err("there is no hour 24");
    assert!(error.to_string().contains("hour"));

    let error = Cron::parse("* * 32 * *").expect_err("no month has 32 days");
    assert!(error.to_string().contains("day-of-month"));

    let error = Cron::parse("* * * 13 *").expect_err("there is no month 13");
    assert!(error.to_string().contains("month"));

    let error = Cron::parse("* * * * 8").expect_err("there is no day 8");
    assert!(error.to_string().contains("day-of-week"));
}

#[test]
fn a_backwards_range_is_an_error_rather_than_an_empty_schedule() {
    // The alternative is a schedule that parses and never fires, which is the
    // failure mode this whole module is trying to avoid.
    let error = Cron::parse("* 17-9 * * *").expect_err("17-9 runs backwards");

    assert!(matches!(
        error,
        CronError::BackwardsRange {
            low: 17,
            high: 9,
            ..
        }
    ));
}

#[test]
fn a_step_of_zero_is_an_error() {
    let error = Cron::parse("*/0 * * * *").expect_err("a step of zero matches nothing");

    assert!(matches!(error, CronError::ZeroStep { field: "minute" }));
}

#[test]
fn nonsense_in_a_field_is_named() {
    let error = Cron::parse("banana * * * *").expect_err("not a minute");

    assert!(matches!(error, CronError::Malformed { .. }), "{error}");
}

#[test]
fn shorthands_expand() {
    // The expanded form must match field for field. `text` deliberately does
    // not — a shorthand echoes back as what was written, not as what it
    // became, so `etl schedule list` shows the file's own spelling.
    let pairs = [
        ("@hourly", "0 * * * *"),
        ("@daily", "0 0 * * *"),
        ("@midnight", "0 0 * * *"),
        ("@weekly", "0 0 * * 0"),
        ("@monthly", "0 0 1 * *"),
        ("@yearly", "0 0 1 1 *"),
        ("@annually", "0 0 1 1 *"),
    ];

    for (shorthand, expanded) in pairs {
        let short = Cron::parse(shorthand).expect("parses");
        let long = Cron::parse(expanded).expect("parses");

        assert_eq!(short.minutes, long.minutes, "{shorthand}");
        assert_eq!(short.hours, long.hours, "{shorthand}");
        assert_eq!(short.days_of_month, long.days_of_month, "{shorthand}");
        assert_eq!(short.months, long.months, "{shorthand}");
        assert_eq!(short.days_of_week, long.days_of_week, "{shorthand}");
        assert_eq!(short.to_string(), shorthand);
    }

    // Case does not matter, because `@Daily` is what somebody will write.
    assert!(Cron::parse("@DAILY").is_ok());
}

#[test]
fn unknown_shorthands_are_refused() {
    let error = Cron::parse("@fortnightly").expect_err("not a shorthand");

    assert!(matches!(error, CronError::UnknownShorthand { .. }));
}

#[test]
fn names_are_accepted_for_months_and_weekdays() {
    // jan is month 1 and sun is weekday 0 — two different bases, which is the
    // thing `name_base` exists to keep straight.
    let by_name = Cron::parse("0 0 1 jan *").expect("parses");
    let by_number = Cron::parse("0 0 1 1 *").expect("parses");
    assert_eq!(by_name.months, by_number.months);

    let by_name = Cron::parse("0 0 * * sun").expect("parses");
    let by_number = Cron::parse("0 0 * * 0").expect("parses");
    assert_eq!(by_name.days_of_week, by_number.days_of_week);

    // And a full name, by prefix.
    let monday = Cron::parse("0 0 * * monday").expect("parses");
    assert!(monday.days_of_week.contains(1));
}

#[test]
fn a_name_range_works() {
    let weekdays = Cron::parse("0 9 * * mon-fri").expect("parses");

    for day in 1..=5 {
        assert!(weekdays.days_of_week.contains(day), "weekday {day}");
    }
    assert!(!weekdays.days_of_week.contains(0));
    assert!(!weekdays.days_of_week.contains(6));
}

#[test]
fn an_unknown_name_is_named() {
    let error = Cron::parse("0 0 * * funday").expect_err("not a day");

    assert!(matches!(error, CronError::UnknownName { .. }), "{error}");
}

#[test]
fn seven_is_sunday_and_is_folded_onto_zero() {
    // Both spellings exist in the wild; they must mean the same thing here.
    let seven = Cron::parse("0 0 * * 7").expect("parses");
    let zero = Cron::parse("0 0 * * 0").expect("parses");

    assert_eq!(seven.days_of_week, zero.days_of_week);
    assert!(seven.days_of_week.contains(0));
    assert!(!seven.days_of_week.contains(7));
}

#[test]
fn lists_and_steps_and_ranges_combine() {
    let every_quarter_hour = Cron::parse("0,15,30,45 * * * *").expect("parses");
    let by_step = Cron::parse("*/15 * * * *").expect("parses");

    assert_eq!(every_quarter_hour.minutes, by_step.minutes);

    // `5/15` reads as "from 5 onwards, every 15" — 5, 20, 35, 50.
    let from_five = Cron::parse("5/15 * * * *").expect("parses");
    for minute in [5, 20, 35, 50] {
        assert!(from_five.minutes.contains(minute), "minute {minute}");
    }
    assert!(!from_five.minutes.contains(0));

    // A range with a step stays inside the range.
    let business = Cron::parse("0 9-17/4 * * *").expect("parses");
    for hour in [9, 13, 17] {
        assert!(business.hours.contains(hour), "hour {hour}");
    }
    assert!(!business.hours.contains(21));
}

// ---------------------------------------------------------------------------
// Next fire
// ---------------------------------------------------------------------------

#[test]
fn the_next_fire_is_strictly_after_the_moment_given() {
    // Asked at exactly 03:00, the next daily-at-three is tomorrow, not now.
    // Otherwise a schedule that fired at 03:00 would immediately fire again.
    let three_am = at(2026, 9, 16, 3, 0);

    assert_eq!(next("0 3 * * *", three_am), "2026-09-17T03:00:00Z");
    assert_eq!(next("0 3 * * *", three_am - 1), "2026-09-16T03:00:00Z");
}

#[test]
fn seconds_are_ignored_and_the_fire_is_minute_aligned() {
    // Cron has no seconds. Asked at 02:59:30, the answer is 03:00:00.
    let almost = at(2026, 9, 16, 2, 59) + 30;

    assert_eq!(next("0 3 * * *", almost), "2026-09-16T03:00:00Z");
}

#[test]
fn a_daily_schedule_rolls_over_a_month_end() {
    assert_eq!(
        next("0 3 * * *", at(2026, 9, 30, 12, 0)),
        "2026-10-01T03:00:00Z"
    );
    assert_eq!(
        next("0 3 * * *", at(2026, 12, 31, 12, 0)),
        "2027-01-01T03:00:00Z"
    );
}

#[test]
fn a_monthly_schedule_skips_whole_months() {
    // The 1st of every month at midnight, asked in the middle of one.
    assert_eq!(
        next("@monthly", at(2026, 9, 16, 12, 0)),
        "2026-10-01T00:00:00Z"
    );
}

#[test]
fn a_weekly_schedule_lands_on_the_right_weekday() {
    // 2026-09-16 is a Wednesday, so the next Monday 09:00 is the 21st.
    assert_eq!(
        next("0 9 * * mon", at(2026, 9, 16, 12, 0)),
        "2026-09-21T09:00:00Z"
    );

    // And the next Sunday is the 20th.
    assert_eq!(
        next("0 0 * * sun", at(2026, 9, 16, 12, 0)),
        "2026-09-20T00:00:00Z"
    );
}

#[test]
fn february_29th_is_found_in_a_leap_year() {
    // Asked in 2024 — a leap year — the next 29 February is that year's.
    assert_eq!(
        next("0 0 29 2 *", at(2024, 1, 1, 0, 0)),
        "2024-02-29T00:00:00Z"
    );

    // Asked just after it, the next is four years later.
    assert_eq!(
        next("0 0 29 2 *", at(2024, 3, 1, 0, 0)),
        "2028-02-29T00:00:00Z"
    );
}

#[test]
fn a_day_that_never_comes_reports_never_rather_than_hanging() {
    // There is no 30th of February, and the search has to end rather than
    // loop. `never` is what the caller turns into a warning.
    assert_eq!(next("0 0 30 2 *", at(2026, 1, 1, 0, 0)), "never");
}

#[test]
fn the_two_day_fields_are_ored_when_both_are_restricted() {
    // The classic rule: `0 0 13 * fri` is the 13th OR any Friday, which is
    // what every Unix cron does. 2026-09-16 is a Wednesday, so the next fire
    // is Friday the 18th rather than the 13th of October.
    assert_eq!(
        next("0 0 13 * fri", at(2026, 9, 16, 12, 0)),
        "2026-09-18T00:00:00Z"
    );

    // And from just after that Friday, the next is the 13th of October —
    // reached by the day-of-month half of the OR.
    let after_friday = at(2026, 9, 18, 0, 1);
    let fire = next("0 0 13 * fri", after_friday);
    assert!(
        fire == "2026-09-25T00:00:00Z",
        "the next Friday comes first, but got {fire}"
    );
}

#[test]
fn only_one_restricted_day_field_is_consulted_alone() {
    // `0 0 13 * *` is the 13th and nothing else — the unrestricted
    // day-of-week must not OR in every day of the week.
    assert_eq!(
        next("0 0 13 * *", at(2026, 9, 16, 12, 0)),
        "2026-10-13T00:00:00Z"
    );

    // `0 0 * * fri` is every Friday and nothing else.
    assert_eq!(
        next("0 0 * * fri", at(2026, 9, 16, 12, 0)),
        "2026-09-18T00:00:00Z"
    );
}

#[test]
fn an_hour_with_no_matching_minute_left_moves_to_the_next_hour() {
    // At 09:30 with `0-15 9-10 * * *`, this hour's minutes are spent, so the
    // answer is 10:00 rather than nothing.
    assert_eq!(
        next("0-15 9-10 * * *", at(2026, 9, 16, 9, 30)),
        "2026-09-16T10:00:00Z"
    );

    // And with only hour 9 available, it rolls to tomorrow.
    assert_eq!(
        next("0-15 9 * * *", at(2026, 9, 16, 9, 30)),
        "2026-09-17T09:00:00Z"
    );
}

#[test]
fn every_minute_fires_a_minute_later() {
    assert_eq!(
        next("* * * * *", at(2026, 9, 16, 9, 30)),
        "2026-09-16T09:31:00Z"
    );
}

#[test]
fn matches_agrees_with_next_after() {
    // Whatever `next_after` picks must be a moment `matches` accepts. This is
    // the invariant that would break first if the skipping logic were wrong.
    let expressions = [
        "* * * * *",
        "0 3 * * *",
        "*/15 * * * *",
        "0 9 * * mon-fri",
        "0 0 1 * *",
        "30 2 13 * fri",
        "0 0 29 2 *",
    ];

    for expression in expressions {
        let cron = Cron::parse(expression).expect("parses");
        let mut probe = at(2026, 1, 1, 0, 0);

        for _ in 0..50 {
            let Some(fire) = cron.next_after(probe) else {
                break;
            };

            assert!(
                cron.matches(DateTime::from_unix(fire)),
                "{expression} fired at {} which it does not match",
                DateTime::from_unix(fire).to_rfc3339()
            );
            assert!(fire > probe, "{expression} did not move forward");

            probe = fire;
        }
    }
}

#[test]
fn no_minute_between_two_fires_is_missed() {
    // Walk a quarter-hourly schedule minute by minute for a day and check
    // that `matches` is true exactly on the minutes `next_after` returns.
    let cron = Cron::parse("*/15 9-17 * * mon-fri").expect("parses");

    let start = at(2026, 9, 16, 0, 0); // a Wednesday
    let mut expected = Vec::new();

    for minute in 0..(24 * 60) {
        let probe = start + minute * 60;
        if cron.matches(DateTime::from_unix(probe)) {
            expected.push(probe);
        }
    }

    let mut found = Vec::new();
    let mut probe = start - 1;
    while let Some(fire) = cron.next_after(probe) {
        if fire >= start + 24 * 3_600 {
            break;
        }
        found.push(fire);
        probe = fire;
    }

    assert_eq!(found, expected);
    // 9 through 17 inclusive, four times an hour.
    assert_eq!(found.len(), 9 * 4);
}
