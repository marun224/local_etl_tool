//! `"cron": "0 3 * * *"` — five fields, UTC, parsed by hand.
//!
//! The *expression* is a short, well-understood grammar, so it is parsed here
//! for the same reason the topological sort and the civil-date conversion are
//! hand-rolled: it is small, it is testable against answers nobody disputes,
//! and a dependency for it would be a dependency forever.
//!
//! The **timezone database** is emphatically not in that category. It changes
//! several times a year, and a stale copy is wrong *silently*, at 2am, twice a
//! year. So there is no local time here at all: every expression is UTC, and a
//! `tz` field on a trigger is [refused](crate::TriggerError::TimezoneRefused)
//! rather than accepted and approximated. A schedule that quietly runs an hour
//! off is worse than one that will not start.
//!
//! # The grammar
//!
//! ```text
//! ┌───────────── minute        0–59
//! │ ┌─────────── hour          0–23
//! │ │ ┌───────── day of month  1–31
//! │ │ │ ┌─────── month         1–12, or jan–dec
//! │ │ │ │ ┌───── day of week   0–6 (0 = Sunday), 7 also Sunday, or sun–sat
//! │ │ │ │ │
//! * * * * *
//! ```
//!
//! Each field is a comma-separated list of `*`, `n`, `a-b`, `*/step` or
//! `a-b/step`. `@hourly`, `@daily`, `@midnight`, `@weekly`, `@monthly` and
//! `@yearly` are accepted as shorthands for the obvious expressions.
//!
//! # The day-of-month and day-of-week rule
//!
//! When **both** day fields are restricted, a day matches if **either** does —
//! they are OR'd, not AND'd. `0 0 13 * fri` is "the 13th, and every Friday",
//! not "Friday the 13th". This is what every Unix cron has done since Vixie's,
//! and inverting it here would make expressions copied from a crontab fire on
//! days they were never meant to. When only one is restricted, only that one
//! is consulted, which is the case that makes the rule invisible most of the
//! time.

use etl_state::time::{days_in_month, DateTime};
use std::fmt;
use thiserror::Error;

/// How far ahead [`Cron::next_after`] will look before giving up.
///
/// Five years covers every expression that fires at all, including
/// `0 0 29 2 *`, which waits up to eight years across a century boundary —
/// so that one *does* return `None` here, and the caller says so out loud
/// rather than a schedule silently never running. An expression that cannot
/// fire within five years is one somebody should be told about.
const SEARCH_YEARS: i64 = 5;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CronError {
    #[error(
        "'{text}' has {found} field(s), and a cron expression has five: \
         minute, hour, day-of-month, month, day-of-week"
    )]
    WrongFieldCount { text: String, found: usize },

    #[error("'{text}' is not a cron expression: it is empty")]
    Empty { text: String },

    #[error("'{shorthand}' is not a shorthand: use @hourly, @daily, @weekly, @monthly or @yearly")]
    UnknownShorthand { shorthand: String },

    #[error("in the {field} field, '{part}' is not a number, a range, or *")]
    Malformed { field: &'static str, part: String },

    #[error("in the {field} field, {value} is out of range ({low}-{high})")]
    OutOfRange {
        field: &'static str,
        value: i64,
        low: u32,
        high: u32,
    },

    #[error("in the {field} field, the range {low}-{high} runs backwards")]
    BackwardsRange {
        field: &'static str,
        low: u32,
        high: u32,
    },

    #[error("in the {field} field, a step of zero would match nothing")]
    ZeroStep { field: &'static str },

    #[error("in the {field} field, '{name}' is not a name I know")]
    UnknownName { field: &'static str, name: String },
}

/// Which values one cron field matches, as a bitmask.
///
/// A `u64` covers every field — minutes are the widest at 0–59 — and makes
/// "does this match" one shift and one mask rather than a scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct FieldSet {
    bits: u64,
}

impl FieldSet {
    fn contains(self, value: u32) -> bool {
        value < 64 && self.bits & (1 << value) != 0
    }

    fn insert(&mut self, value: u32) {
        if value < 64 {
            self.bits |= 1 << value;
        }
    }

    /// The smallest matching value at or above `from`, if there is one.
    fn next_from(self, from: u32) -> Option<u32> {
        if from >= 64 {
            return None;
        }

        let remaining = self.bits >> from;

        if remaining == 0 {
            None
        } else {
            Some(from + remaining.trailing_zeros())
        }
    }
}

/// One field's shape, for parsing and for error messages.
struct FieldKind {
    name: &'static str,
    low: u32,
    high: u32,
    names: &'static [&'static str],
    /// What the first name in `names` stands for: 1 for January, 0 for Sunday.
    name_base: u32,
}

const MINUTE: FieldKind = FieldKind {
    name: "minute",
    name_base: 0,
    low: 0,
    high: 59,
    names: &[],
};

const HOUR: FieldKind = FieldKind {
    name: "hour",
    name_base: 0,
    low: 0,
    high: 23,
    names: &[],
};

const DAY_OF_MONTH: FieldKind = FieldKind {
    name: "day-of-month",
    name_base: 0,
    low: 1,
    high: 31,
    names: &[],
};

const MONTH: FieldKind = FieldKind {
    name: "month",
    low: 1,
    high: 12,
    names: &[
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ],
    name_base: 1,
};

const DAY_OF_WEEK: FieldKind = FieldKind {
    name: "day-of-week",
    // 7 is accepted and folded to 0, the way every Unix cron does, so an
    // expression copied out of a crontab means the same thing here.
    low: 0,
    high: 7,
    names: &["sun", "mon", "tue", "wed", "thu", "fri", "sat"],
    name_base: 0,
};

/// A parsed cron expression, in UTC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cron {
    minutes: FieldSet,
    hours: FieldSet,
    days_of_month: FieldSet,
    months: FieldSet,
    days_of_week: FieldSet,

    /// Whether each day field was written as something other than `*`.
    ///
    /// Needed because the OR rule depends on *how the field was written*, not
    /// on what it matches: `*` in day-of-week matches every day, and so would
    /// make the OR match every day too if the distinction were not kept.
    day_of_month_restricted: bool,
    day_of_week_restricted: bool,

    text: String,
}

impl Cron {
    /// Parse a five-field expression, or one of the `@` shorthands.
    pub fn parse(text: &str) -> Result<Self, CronError> {
        let trimmed = text.trim();

        if trimmed.is_empty() {
            return Err(CronError::Empty {
                text: text.to_string(),
            });
        }

        let expanded = if let Some(shorthand) = trimmed.strip_prefix('@') {
            match shorthand.to_ascii_lowercase().as_str() {
                "hourly" => "0 * * * *",
                "daily" | "midnight" => "0 0 * * *",
                "weekly" => "0 0 * * 0",
                "monthly" => "0 0 1 * *",
                "yearly" | "annually" => "0 0 1 1 *",
                _ => {
                    return Err(CronError::UnknownShorthand {
                        shorthand: trimmed.to_string(),
                    })
                }
            }
        } else {
            trimmed
        };

        let fields: Vec<&str> = expanded.split_whitespace().collect();

        if fields.len() != 5 {
            return Err(CronError::WrongFieldCount {
                text: trimmed.to_string(),
                found: fields.len(),
            });
        }

        let mut days_of_week = parse_field(fields[4], &DAY_OF_WEEK)?;

        // Fold 7 onto 0 so the rest of the code has one spelling of Sunday.
        if days_of_week.contains(7) {
            days_of_week.insert(0);
            days_of_week.bits &= !(1 << 7);
        }

        Ok(Cron {
            minutes: parse_field(fields[0], &MINUTE)?,
            hours: parse_field(fields[1], &HOUR)?,
            days_of_month: parse_field(fields[2], &DAY_OF_MONTH)?,
            months: parse_field(fields[3], &MONTH)?,
            days_of_week,
            day_of_month_restricted: fields[2] != "*",
            day_of_week_restricted: fields[4] != "*",
            text: trimmed.to_string(),
        })
    }

    /// Whether this expression matches a given instant, to the minute.
    pub fn matches(&self, when: DateTime) -> bool {
        self.months.contains(when.month)
            && self.day_matches(when)
            && self.hours.contains(when.hour)
            && self.minutes.contains(when.minute)
    }

    /// The day rule: see the module docs. Either field matches when both are
    /// restricted; otherwise only the restricted one is consulted.
    fn day_matches(&self, when: DateTime) -> bool {
        let by_month_day = self.days_of_month.contains(when.day);
        let by_week_day = self.days_of_week.contains(when.weekday());

        match (self.day_of_month_restricted, self.day_of_week_restricted) {
            (true, true) => by_month_day || by_week_day,
            (true, false) => by_month_day,
            (false, true) => by_week_day,
            // Both `*`: every day.
            (false, false) => true,
        }
    }

    /// The first minute strictly after `after` that this expression matches.
    ///
    /// `None` when nothing matches within [`SEARCH_YEARS`], which is how an
    /// expression like `0 0 30 2 *` — the 30th of February — reports that it
    /// will never fire, rather than being accepted and then silently doing
    /// nothing forever.
    pub fn next_after(&self, after: i64) -> Option<i64> {
        // Strictly after, and minute-aligned: cron has no concept of seconds,
        // and a schedule that fired at 03:00:30 would fire again at 03:00:59.
        let mut probe = (after.div_euclid(60) + 1) * 60;
        let limit = probe + SEARCH_YEARS * 366 * 86_400;

        while probe < limit {
            let when = DateTime::from_unix(probe);

            // Skip whole months and whole days rather than minutes, so the
            // worst case is a few thousand cheap checks rather than the two
            // million a minute-by-minute scan would take.
            if !self.months.contains(when.month) {
                probe = start_of_next_month(when);
                continue;
            }

            if !self.day_matches(when) {
                probe = start_of_next_day(when);
                continue;
            }

            let Some(hour) = self.hours.next_from(when.hour) else {
                probe = start_of_next_day(when);
                continue;
            };

            // A later hour in the same day starts its minutes from zero; the
            // current hour carries on from the current minute.
            let from_minute = if hour == when.hour { when.minute } else { 0 };

            match self.minutes.next_from(from_minute) {
                Some(minute) => {
                    return Some(
                        DateTime {
                            hour,
                            minute,
                            second: 0,
                            ..when
                        }
                        .to_unix(),
                    )
                }
                None => {
                    // No minute left in this hour: try the next one.
                    probe = DateTime {
                        hour,
                        minute: 59,
                        second: 0,
                        ..when
                    }
                    .to_unix()
                        + 60;
                }
            }
        }

        None
    }
}

impl fmt::Display for Cron {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.text)
    }
}

/// Midnight on the first of the following month.
fn start_of_next_month(when: DateTime) -> i64 {
    let (year, month) = if when.month == 12 {
        (when.year + 1, 1)
    } else {
        (when.year, when.month + 1)
    };

    DateTime {
        year,
        month,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
    }
    .to_unix()
}

/// Midnight at the start of the following day.
fn start_of_next_day(when: DateTime) -> i64 {
    if when.day < days_in_month(when.year, when.month) {
        DateTime {
            day: when.day + 1,
            hour: 0,
            minute: 0,
            second: 0,
            ..when
        }
        .to_unix()
    } else {
        start_of_next_month(when)
    }
}

/// Parse one comma-separated field.
fn parse_field(text: &str, kind: &FieldKind) -> Result<FieldSet, CronError> {
    let mut set = FieldSet::default();

    for part in text.split(',') {
        let part = part.trim();

        if part.is_empty() {
            return Err(CronError::Malformed {
                field: kind.name,
                part: text.to_string(),
            });
        }

        // A step splits the part into a range and how far to skip within it.
        let (range, step) = match part.split_once('/') {
            Some((range, step)) => {
                let step: u32 = step.trim().parse().map_err(|_| CronError::Malformed {
                    field: kind.name,
                    part: part.to_string(),
                })?;

                if step == 0 {
                    return Err(CronError::ZeroStep { field: kind.name });
                }

                (range.trim(), step)
            }
            None => (part, 1),
        };

        let (low, high) = if range == "*" {
            (kind.low, kind.high)
        } else if let Some((start, end)) = split_range(range) {
            let low = parse_value(start, kind)?;
            let high = parse_value(end, kind)?;

            if low > high {
                return Err(CronError::BackwardsRange {
                    field: kind.name,
                    low,
                    high,
                });
            }

            (low, high)
        } else {
            let single = parse_value(range, kind)?;

            // `5/15` means "from 5 to the end of the field, every 15" — the
            // same reading as `5-59/15`. A bare `5` with no step is just 5,
            // which falls out of low == high.
            if step > 1 {
                (single, kind.high)
            } else {
                (single, single)
            }
        };

        let mut value = low;
        while value <= high {
            set.insert(value);
            value += step;
        }
    }

    Ok(set)
}

/// Split `a-b`, leaving a leading `-` alone so a negative number reports as
/// out of range rather than as an empty range bound.
fn split_range(text: &str) -> Option<(&str, &str)> {
    let index = text.char_indices().skip(1).find(|(_, c)| *c == '-')?.0;

    Some((&text[..index], &text[index + 1..]))
}

/// A single number, or a three-letter name where the field has them.
fn parse_value(text: &str, kind: &FieldKind) -> Result<u32, CronError> {
    let text = text.trim();

    if !kind.names.is_empty() && text.chars().next().is_some_and(|c| c.is_ascii_alphabetic()) {
        let wanted = text.to_ascii_lowercase();

        // Three letters is the whole name for these; `monday` is accepted by
        // prefix so nobody has to remember which form we take.
        let found = kind
            .names
            .iter()
            .position(|name| wanted.starts_with(name))
            .ok_or_else(|| CronError::UnknownName {
                field: kind.name,
                name: text.to_string(),
            })?;

        return Ok(found as u32 + kind.name_base);
    }

    let value: i64 = text.parse().map_err(|_| CronError::Malformed {
        field: kind.name,
        part: text.to_string(),
    })?;

    if value < i64::from(kind.low) || value > i64::from(kind.high) {
        return Err(CronError::OutOfRange {
            field: kind.name,
            value,
            low: kind.low,
            high: kind.high,
        });
    }

    Ok(value as u32)
}

#[cfg(test)]
mod tests;
