//! `"every": "90m"` — an interval, parsed.
//!
//! A duration written the way people write one: a number and a unit, repeated,
//! largest first. `30s`, `5m`, `1h`, `2h30m`, `1d`. Plain seconds with no unit
//! are refused, because `"every": "60"` reads as a minute to half the people
//! who write it and an hour to the other half.
//!
//! **What an interval is measured from** is not this module's business — it is
//! the scheduler's, and the answer is the last recorded run rather than the
//! moment the scheduler started. See [`crate::run`].

use std::fmt;
use thiserror::Error;

/// The longest interval that means anything. A year, give or take.
///
/// Not a technical limit — it is a typo guard. `"every": "500d"` is far more
/// likely to be a slip than a schedule somebody wants, and a schedule that
/// will next fire after the heat death of the project is indistinguishable
/// from one that is broken.
const MAX_SECONDS: u64 = 400 * 86_400;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IntervalError {
    #[error("'{text}' is not an interval: write a number and a unit, like 30s, 5m, 1h or 2h30m")]
    Malformed { text: String },

    #[error("'{text}' has no unit: write {text}s for seconds or {text}m for minutes")]
    NoUnit { text: String },

    #[error("'{unit}' is not a unit: use s (seconds), m (minutes), h (hours) or d (days)")]
    UnknownUnit { unit: String },

    #[error("an interval of zero would run without stopping")]
    Zero,

    #[error("'{text}' is longer than {max} days, which is more likely a typo than a schedule")]
    TooLong { text: String, max: u64 },

    #[error("'{text}' is too large to be a duration")]
    Overflow { text: String },
}

/// How often something repeats, in seconds.
///
/// The text it was written as is kept alongside, so `etl schedule list` can
/// echo `90m` back rather than the `1h30m` a normalising formatter would
/// produce — the person reading the list is checking it against the file they
/// wrote, and a value that is right but spelled differently costs them a
/// second look every time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval {
    seconds: u64,
    text: String,
}

impl Interval {
    /// Parse `30s`, `5m`, `1h`, `2h30m`, `1d`.
    ///
    /// Units may repeat in any order and are added together, so `90m` and
    /// `1h30m` are the same interval. Repeating a unit (`1h1h`) is accepted
    /// and summed, because refusing it would be a rule with no victim.
    pub fn parse(text: &str) -> Result<Self, IntervalError> {
        let trimmed = text.trim();

        if trimmed.is_empty() {
            return Err(IntervalError::Malformed {
                text: text.to_string(),
            });
        }

        let mut seconds: u64 = 0;
        let mut digits = String::new();
        let mut saw_a_unit = false;

        for character in trimmed.chars() {
            if character.is_ascii_digit() {
                digits.push(character);
                continue;
            }

            if character.is_whitespace() || character == '_' {
                continue;
            }

            // A unit with no number in front of it: `hm`, or `5mh`.
            if digits.is_empty() {
                return Err(IntervalError::Malformed {
                    text: trimmed.to_string(),
                });
            }

            let multiplier = match character.to_ascii_lowercase() {
                's' => 1,
                'm' => 60,
                'h' => 3_600,
                'd' => 86_400,
                other => {
                    return Err(IntervalError::UnknownUnit {
                        unit: other.to_string(),
                    })
                }
            };

            let count: u64 = digits.parse().map_err(|_| IntervalError::Overflow {
                text: trimmed.to_string(),
            })?;

            seconds = count
                .checked_mul(multiplier)
                .and_then(|part| seconds.checked_add(part))
                .ok_or_else(|| IntervalError::Overflow {
                    text: trimmed.to_string(),
                })?;

            digits.clear();
            saw_a_unit = true;
        }

        // Trailing digits with nothing after them. `"every": "60"` is the
        // common case, and the message names both readings rather than
        // picking one.
        if !digits.is_empty() {
            return Err(IntervalError::NoUnit {
                text: digits.clone(),
            });
        }

        if !saw_a_unit {
            return Err(IntervalError::Malformed {
                text: trimmed.to_string(),
            });
        }

        if seconds == 0 {
            return Err(IntervalError::Zero);
        }

        if seconds > MAX_SECONDS {
            return Err(IntervalError::TooLong {
                text: trimmed.to_string(),
                max: MAX_SECONDS / 86_400,
            });
        }

        Ok(Interval {
            seconds,
            text: trimmed.to_string(),
        })
    }

    /// The interval in seconds.
    pub fn seconds(&self) -> u64 {
        self.seconds
    }

    /// The first tick strictly after `after`, counting from `anchor`.
    ///
    /// Strictly after, so a schedule cannot fire twice for the same instant.
    /// The anchor is where counting starts — the last recorded run, normally,
    /// so that restarting the scheduler does not restart the clock.
    pub fn next_after(&self, anchor: i64, after: i64) -> i64 {
        let step = self.seconds as i64;

        if anchor > after {
            return anchor;
        }

        // How many whole intervals have elapsed since the anchor, plus one.
        // `div_euclid` so an anchor in the future of the epoch's sign does not
        // round the wrong way.
        let elapsed = after - anchor;
        anchor + (elapsed.div_euclid(step) + 1) * step
    }

    /// How many ticks were missed between `scheduled` and `now`.
    ///
    /// Used only to say so out loud. The scheduler does not run them: for a
    /// watermarked pipeline each run already reads everything new since the
    /// last mark, so five catch-up runs do what one does.
    pub fn missed_between(&self, scheduled: i64, now: i64) -> u64 {
        if now <= scheduled {
            return 0;
        }

        ((now - scheduled) / self.seconds as i64) as u64
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.text)
    }
}

#[cfg(test)]
mod tests;
