//! Dates whose answers are not in doubt.
//!
//! Every expected value here is one that can be checked by hand or against a
//! calendar, which is the whole justification for not taking a date crate.

use super::*;

#[test]
fn the_epoch_is_the_epoch() {
    assert_eq!(to_rfc3339(0), "1970-01-01T00:00:00Z");
}

#[test]
fn a_known_timestamp_converts() {
    // 2026-09-16T12:34:56Z, checked against a calendar.
    let when = DateTime {
        year: 2026,
        month: 9,
        day: 16,
        hour: 12,
        minute: 34,
        second: 56,
    };

    assert_eq!(to_rfc3339(when.to_unix()), "2026-09-16T12:34:56Z");
}

#[test]
fn a_round_trip_returns_the_same_instant() {
    // A spread of instants: the epoch, before it, a leap day, a year end, and
    // a far-future date well outside any range this product will see.
    for seconds in [
        0,
        -1,
        -86_400,
        1_000_000_000,
        1_767_225_599, // 2025-12-31T23:59:59Z
        1_772_323_200, // 2026-02-29 does not exist; this is 2026-03-01
        4_102_444_800, // 2100-01-01
        253_402_300_799,
    ] {
        assert_eq!(
            DateTime::from_unix(seconds).to_unix(),
            seconds,
            "{seconds} did not survive a round trip"
        );
    }
}

#[test]
fn days_from_civil_inverts_civil_from_days() {
    // Every day for eight years across two leap years, both directions.
    for days in -1_000..2_000 {
        let (year, month, day) = civil_from_days(days);
        assert_eq!(
            days_from_civil(year, month, day),
            days,
            "{year:04}-{month:02}-{day:02} did not invert"
        );
    }
}

#[test]
fn a_leap_day_is_a_real_day() {
    // 2024-02-29 existed; 2023-02-29 did not, and 2024-03-01 is the day after.
    let leap_day = DateTime {
        year: 2024,
        month: 2,
        day: 29,
        hour: 0,
        minute: 0,
        second: 0,
    };

    assert_eq!(to_rfc3339(leap_day.to_unix()), "2024-02-29T00:00:00Z");
    assert_eq!(
        to_rfc3339(leap_day.to_unix() + SECONDS_PER_DAY),
        "2024-03-01T00:00:00Z"
    );
}

#[test]
fn the_century_rule_is_applied() {
    // 2000 was a leap year, 1900 and 2100 were not. This is the rule a
    // hand-rolled conversion gets wrong if it only checks divisibility by 4.
    assert!(is_leap_year(2000));
    assert!(!is_leap_year(1900));
    assert!(!is_leap_year(2100));
    assert!(is_leap_year(2024));
    assert!(!is_leap_year(2023));

    assert_eq!(days_in_month(2000, 2), 29);
    assert_eq!(days_in_month(1900, 2), 28);
    assert_eq!(days_in_month(2100, 2), 28);
}

#[test]
fn month_lengths_are_right() {
    let lengths = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    for (index, expected) in lengths.iter().enumerate() {
        assert_eq!(days_in_month(2026, index as u32 + 1), *expected);
    }
}

#[test]
fn weekdays_are_sunday_based() {
    // 1970-01-01 was a Thursday. 2026-09-16 is a Wednesday. Both checkable
    // against any calendar, which is the point.
    let thursday = DateTime::from_unix(0);
    assert_eq!(thursday.weekday(), 4);

    let wednesday = DateTime {
        year: 2026,
        month: 9,
        day: 16,
        hour: 0,
        minute: 0,
        second: 0,
    };
    assert_eq!(wednesday.weekday(), 3);
}

#[test]
fn weekdays_advance_by_one_a_day_and_wrap_at_seven() {
    let start = DateTime::from_unix(0).weekday();

    for offset in 0..30 {
        let day = DateTime::from_unix(offset * SECONDS_PER_DAY);
        assert_eq!(day.weekday(), (start + offset as u32) % 7);
    }
}

#[test]
fn dates_before_the_epoch_do_not_go_negative_in_the_weekday() {
    // `%` would give a negative answer here and index out of a day-name table;
    // `rem_euclid` is why this passes.
    for offset in 1..400 {
        let day = DateTime::from_unix(-offset * SECONDS_PER_DAY);
        assert!(day.weekday() < 7, "weekday was {}", day.weekday());
    }
}

#[test]
fn now_is_after_this_was_written() {
    // A clock sanity check rather than a date one: if this fails the machine's
    // clock is wrong, and every timestamp this crate writes is wrong with it.
    assert!(now_unix() > 1_757_980_800); // 2026-09-16
}

// ---------------------------------------------------------------------------
// Reading a timestamp back
// ---------------------------------------------------------------------------

#[test]
fn what_we_write_is_what_we_read() {
    // The only property that matters: a record written by this crate is
    // readable by the scheduler that has to count an interval from it.
    for seconds in [0, 1_000_000_000, 1_774_000_000, 4_102_444_800] {
        assert_eq!(from_rfc3339(&to_rfc3339(seconds)), Some(seconds));
    }
}

#[test]
fn a_timestamp_with_an_offset_is_refused_rather_than_read_as_utc() {
    // Reading `+05:30` as UTC would put a run five and a half hours out. This
    // build carries no timezone handling, so it says so.
    assert_eq!(from_rfc3339("2026-09-16T03:00:00+05:30"), None);
    assert_eq!(from_rfc3339("2026-09-16T03:00:00-08:00"), None);
    // And with no zone at all.
    assert_eq!(from_rfc3339("2026-09-16T03:00:00"), None);
}

#[test]
fn nonsense_is_none_rather_than_a_wrong_instant() {
    for text in [
        "",
        "Z",
        "not a date",
        "2026-13-01T00:00:00Z", // month 13
        "2026-02-30T00:00:00Z", // February never has 30 days
        "2026-00-01T00:00:00Z", // month 0
        "2026-09-16T24:00:00Z", // hour 24
        "2026-09-16T00:60:00Z", // minute 60
        "2026-09-16T00:00:00:00Z",
        "2026-09T00:00:00Z",
    ] {
        assert_eq!(from_rfc3339(text), None, "{text} should not parse");
    }
}

#[test]
fn the_forgiving_parts_are_forgiving() {
    let expected = DateTime {
        year: 2026,
        month: 9,
        day: 16,
        hour: 3,
        minute: 0,
        second: 0,
    }
    .to_unix();

    // Seconds omitted, a lowercase separator, a space, and surrounding space.
    assert_eq!(from_rfc3339("2026-09-16T03:00Z"), Some(expected));
    assert_eq!(from_rfc3339("2026-09-16t03:00:00z"), Some(expected));
    assert_eq!(from_rfc3339("2026-09-16 03:00:00Z"), Some(expected));
    assert_eq!(from_rfc3339("  2026-09-16T03:00:00Z  "), Some(expected));
    // Fractional seconds are dropped rather than refused.
    assert_eq!(from_rfc3339("2026-09-16T03:00:00.123Z"), Some(expected));
}

#[test]
fn a_leap_second_does_not_roll_the_minute_over() {
    // Unix time has no room for :60. Reading it as the second before is a
    // second out; rolling it into the next minute would be a minute out.
    let sixty = from_rfc3339("2016-12-31T23:59:60Z").expect("reads");
    let fifty_nine = from_rfc3339("2016-12-31T23:59:59Z").expect("reads");

    assert_eq!(sixty, fifty_nine);
}

#[test]
fn a_leap_day_reads_only_in_a_leap_year() {
    assert!(from_rfc3339("2024-02-29T00:00:00Z").is_some());
    assert_eq!(from_rfc3339("2023-02-29T00:00:00Z"), None);
}
