//! Civil dates in UTC, and nothing else.
//!
//! Hand-rolled for the reason this workspace hand-rolled its topological sort:
//! the conversion is short, well understood, and testable against dates whose
//! answers are not in doubt. A **timezone database** would be a different
//! argument entirely — it changes several times a year and a stale copy is
//! wrong *silently* — which is why nothing here knows about local time, and
//! why a schedule carrying a `tz` field is refused rather than approximated.
//!
//! This module exists because the conversion had been written twice: once
//! here for run timestamps and once in the engine for `${date}`. A third copy
//! for the scheduler would have settled the question the wrong way. The
//! engine's copy is still its own — it does not depend on this crate, and
//! making it do so is a change to the engine rather than to the scheduler.
//!
//! The algorithms are Howard Hinnant's `civil_from_days` and `days_from_civil`,
//! which are exact over the whole range of `i64` days and have no branches for
//! leap years beyond the era arithmetic.

use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds in a day. Every day, because this is UTC and there are no leap
/// seconds in Unix time.
pub const SECONDS_PER_DAY: i64 = 86_400;

/// A calendar date and time of day, UTC.
///
/// Fields are plain numbers in their natural ranges — `month` is 1–12 and
/// `day` is 1–31, not the 0-based forms the era arithmetic uses internally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DateTime {
    pub year: i64,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
}

impl DateTime {
    /// Break a Unix timestamp into its civil parts.
    pub fn from_unix(seconds: i64) -> Self {
        let days = seconds.div_euclid(SECONDS_PER_DAY);
        let within_day = seconds.rem_euclid(SECONDS_PER_DAY);

        let (year, month, day) = civil_from_days(days);

        DateTime {
            year,
            month: month as u32,
            day: day as u32,
            hour: (within_day / 3_600) as u32,
            minute: ((within_day % 3_600) / 60) as u32,
            second: (within_day % 60) as u32,
        }
    }

    /// Back to a Unix timestamp.
    pub fn to_unix(self) -> i64 {
        days_from_civil(self.year, i64::from(self.month), i64::from(self.day)) * SECONDS_PER_DAY
            + i64::from(self.hour) * 3_600
            + i64::from(self.minute) * 60
            + i64::from(self.second)
    }

    /// The day of the week, 0 = Sunday through 6 = Saturday.
    ///
    /// Sunday-based because that is what cron's fifth field means, and this is
    /// the only thing in the product that asks.
    pub fn weekday(self) -> u32 {
        let days = days_from_civil(self.year, i64::from(self.month), i64::from(self.day));

        // 1970-01-01 was a Thursday, so shift by 4 to land Sunday on 0.
        // `rem_euclid` rather than `%` so dates before the epoch stay in range.
        (days + 4).rem_euclid(7) as u32
    }

    /// `YYYY-MM-DDTHH:MM:SSZ`.
    pub fn to_rfc3339(self) -> String {
        let DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
        } = self;

        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
    }
}

/// Now, as a Unix timestamp.
///
/// A clock that is before the epoch reads as 0 rather than failing. Nothing
/// this product does is improved by refusing to run because the machine's
/// clock is absurd, and every caller here is stamping a record or comparing
/// two times it took from this same function.
pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or(0)
}

/// A Unix timestamp as `YYYY-MM-DDTHH:MM:SSZ`.
pub fn to_rfc3339(seconds: i64) -> String {
    DateTime::from_unix(seconds).to_rfc3339()
}

/// Read back what [`to_rfc3339`] wrote.
///
/// Deliberately narrow: exactly the shape this product writes, UTC, with the
/// `Z`. It is not a general RFC 3339 parser and does not pretend to be — an
/// offset would need the timezone handling this build has decided not to
/// carry, so a timestamp bearing one is refused rather than silently read as
/// UTC. The only caller is the scheduler, reading back a `started` field that
/// this crate itself wrote.
///
/// `None` rather than an error: the one thing a caller can do with an
/// unreadable timestamp is treat that run as unknown, and every caller here
/// has a sensible answer for that.
pub fn from_rfc3339(text: &str) -> Option<i64> {
    let text = text.trim();
    let body = text.strip_suffix('Z').or_else(|| text.strip_suffix('z'))?;

    let (date, clock) = body.split_once(['T', 't', ' '])?;

    let mut parts = date.split('-');
    let year: i64 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;

    if parts.next().is_some() || !(1..=12).contains(&month) || day < 1 {
        return None;
    }

    if day > days_in_month(year, month) {
        return None;
    }

    // Fractional seconds are dropped rather than refused: nothing here writes
    // them, but a hand-edited file carrying them is still a readable instant.
    let clock = clock.split('.').next()?;

    let mut parts = clock.split(':');
    let hour: u32 = parts.next()?.parse().ok()?;
    let minute: u32 = parts.next()?.parse().ok()?;
    // Seconds are optional, so `2026-09-16T03:00Z` reads.
    let second: u32 = match parts.next() {
        Some(text) => text.parse().ok()?,
        None => 0,
    };

    if parts.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    Some(
        DateTime {
            year,
            month,
            day,
            hour,
            minute,
            // A leap second, which Unix time has no room for, reads as the
            // second before it rather than rolling the minute over.
            second: second.min(59),
        }
        .to_unix(),
    )
}

/// Days since 1970-01-01 to a calendar date. Howard Hinnant's
/// `civil_from_days`.
pub fn civil_from_days(days: i64) -> (i64, i64, i64) {
    // Shift the epoch to 0000-03-01, so leap days land at the end of the cycle.
    let shifted = days + 719_468;

    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;

    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153; // [0, 11], March-based

    let day = day_of_year - (153 * month_position + 2) / 5 + 1;
    let month = if month_position < 10 {
        month_position + 3
    } else {
        month_position - 9
    };

    (year + i64::from(month <= 2), month, day)
}

/// A calendar date to days since 1970-01-01. Howard Hinnant's
/// `days_from_civil`, the exact inverse of [`civil_from_days`].
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    // March-based again: shift the year back so February's leap day is the
    // last day of the year rather than a hole in the middle of it.
    let year = year - i64::from(month <= 2);

    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400; // [0, 399]

    let month_position = if month > 2 { month - 3 } else { month + 9 }; // [0, 11]
    let day_of_year = (153 * month_position + 2) / 5 + day - 1; // [0, 365]
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year; // [0, 146096]

    era * 146_097 + day_of_era - 719_468
}

/// The number of days in a month, leap years included.
pub fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        // Not reachable from a parsed cron, whose month field is checked
        // against 1..=12 before it gets here. Zero rather than a panic,
        // because a scheduler that aborts the process over an impossible
        // month is worse than one that finds no matching day and says so.
        _ => 0,
    }
}

/// Whether a year is a leap year in the proleptic Gregorian calendar.
pub fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[cfg(test)]
mod tests;
