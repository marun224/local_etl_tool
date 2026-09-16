//! The loop: what is due, what ran, what was missed.
//!
//! Every timing rule lives here and none of them touch a clock directly —
//! [`Clock`] is a trait, so the tests drive a whole day of scheduling in
//! microseconds and no test anywhere has to sleep to find out what happens
//! when a run overruns its next tick.
//!
//! What executes a pipeline is a closure the caller supplies. This crate has
//! no idea what a pipeline is; it knows only that running one takes time and
//! comes back having either worked or not.
//!
//! # One at a time
//!
//! Runs are sequential. A schedule that comes due while another run is going
//! waits for it, and if its own tick passes in the meantime that tick is
//! missed rather than queued — see [`Tick::missed`]. Sequential execution is
//! what keeps `crates/state`'s single-writer assumption true rather than
//! merely hoped for, and it is why this crate needs no locking of its own
//! beyond [`crate::lock`], which keeps a *second scheduler* out.

use crate::watch::{Poll, WatchState};
use crate::{Schedule, Trigger};
use etl_state::time;
use std::path::{Path, PathBuf};

/// Where time comes from.
///
/// A trait so the loop can be tested. `SystemClock` is the only
/// implementation outside the tests.
pub trait Clock {
    /// Now, as a Unix timestamp.
    fn now(&self) -> i64;

    /// Wait. May return early — the loop re-checks rather than trusting it,
    /// which is also what makes a fake clock that never really waits work.
    fn sleep(&self, seconds: u64);
}

/// The real clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        time::now_unix()
    }

    fn sleep(&self, seconds: u64) {
        std::thread::sleep(std::time::Duration::from_secs(seconds));
    }
}

/// How a run turned out, as far as the scheduler needs to know.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Succeeded,
    Failed,
    /// The pipeline could not be loaded or compiled at all.
    ///
    /// Distinct from `Failed` because it is a standing condition rather than
    /// a bad run: a schedule pointing at a file that is not there will do
    /// this every tick until somebody fixes it, and the loop says so once
    /// rather than on every tick.
    Broken,
}

impl Outcome {
    pub fn name(self) -> &'static str {
        match self {
            Outcome::Succeeded => "ok",
            Outcome::Failed => "failed",
            Outcome::Broken => "broken",
        }
    }

    pub fn is_ok(self) -> bool {
        matches!(self, Outcome::Succeeded)
    }
}

/// One schedule firing.
#[derive(Debug, Clone)]
pub struct Tick {
    /// Which schedule.
    pub name: String,

    /// When it was due.
    pub due: i64,

    /// When it actually started, which is later if another run was going.
    pub started: i64,

    /// How the run turned out.
    pub outcome: Outcome,

    /// How long the run took, in seconds.
    pub took: i64,

    /// Ticks that passed **while this run was going**, because it overran.
    ///
    /// Measured from when the run started rather than from when it was due,
    /// so a first run that was overdue by eight hours reports nothing here.
    /// Being behind because the scheduler was *down* is a different thing and
    /// is reported separately, as [`Entry::behind`] — conflating them turns
    /// "you were switched off all night" into "your pipeline is too slow",
    /// which sends somebody looking in the wrong place.
    ///
    /// Reported, never queued. For a watermarked pipeline each run already
    /// reads everything new since the last mark, so running the backlog would
    /// do the same work several times over.
    pub missed: u64,
}

/// What the loop reports at the end.
#[derive(Debug, Clone, Default)]
pub struct Summary {
    pub ticks: Vec<Tick>,
}

impl Summary {
    pub fn ran(&self) -> usize {
        self.ticks.len()
    }

    pub fn failed(&self) -> usize {
        self.ticks
            .iter()
            .filter(|tick| !tick.outcome.is_ok())
            .count()
    }

    pub fn missed(&self) -> u64 {
        self.ticks.iter().map(|tick| tick.missed).sum()
    }
}

/// A schedule, plus where it has got to.
#[derive(Debug, Clone)]
pub struct Entry {
    pub schedule: Schedule,

    /// When this is next due. `None` for a watch, which is not due at a time,
    /// and for a cron expression that will never match.
    pub next: Option<i64>,

    /// The last run this schedule knows about, from history at startup and
    /// from its own runs after that. What an interval counts from.
    pub last_run: Option<i64>,

    /// How many ticks have been missed to overrun, over the scheduler's life.
    pub missed: u64,

    /// How many whole intervals passed between this pipeline's last recorded
    /// run and the moment the scheduler started.
    ///
    /// Downtime, in other words — the scheduler was not running, or this
    /// schedule was only just added to a pipeline that had been run by hand.
    /// Worth saying once at startup and never again: it is a fact about the
    /// past, not about how the scheduler is behaving now.
    pub behind: u64,

    /// The resolved path being watched, and what it looked like last poll.
    watch: Option<(PathBuf, WatchState)>,

    /// When the watch is next polled.
    next_poll: i64,
}

impl Entry {
    /// A one-line description of when this next happens, for `list`.
    ///
    /// Takes `now` so that a tick already in the past reads as *overdue*
    /// rather than as a timestamp the reader has to compare against their own
    /// sense of the time. A pipeline that has been down since yesterday shows
    /// a next fire of yesterday, which looks like a bug until you work out
    /// that it means "immediately".
    pub fn describe_next(&self, now: i64) -> String {
        match &self.schedule.trigger {
            Trigger::Watch(watch) => format!("when {} changes", watch.path().display()),
            _ => match self.next {
                Some(next) if next <= now => "due now".to_string(),
                Some(next) => time::to_rfc3339(next),
                None => "never".to_string(),
            },
        }
    }
}

/// The schedules of one workspace, and where each has got to.
#[derive(Debug)]
pub struct Scheduler {
    entries: Vec<Entry>,
    workspace: PathBuf,
}

impl Scheduler {
    /// Build a scheduler over the enabled schedules.
    ///
    /// `last_run` is asked, for each schedule, when that pipeline last ran —
    /// from `.etl/runs/`. That is what an interval is counted from, so that
    /// restarting the scheduler does not restart the clock and an hourly
    /// pipeline stays hourly across a reboot.
    pub fn new(
        schedules: impl IntoIterator<Item = Schedule>,
        workspace: impl Into<PathBuf>,
        now: i64,
        mut last_run: impl FnMut(&Schedule) -> Option<i64>,
    ) -> Self {
        let workspace = workspace.into();

        let entries = schedules
            .into_iter()
            .map(|schedule| {
                let previous = last_run(&schedule);

                let (next, watch, next_poll) = match &schedule.trigger {
                    Trigger::Every(interval) => {
                        // Counted from the last run when there was one. With
                        // no history, the anchor is now — so a new schedule
                        // waits one interval rather than firing the instant
                        // the scheduler starts, which is what somebody
                        // adding a schedule at 4pm expects of "every 1h".
                        let anchor = previous.unwrap_or(now);

                        // One interval past the anchor, and deliberately not
                        // `next_after`: a pipeline that last ran three hours
                        // ago on an hourly schedule is *overdue*, and this
                        // leaves `next` in the past so it runs at once.
                        // `next_after` would skip to the next whole hour and
                        // leave the data sitting for another fifty minutes,
                        // which is the opposite of what a scheduler coming
                        // back up should do.
                        (Some(anchor + interval.seconds() as i64), None, 0)
                    }

                    Trigger::Cron(cron) => (cron.next_after(now), None, 0),

                    Trigger::Watch(watch) => {
                        let path = watch.resolve(&workspace);
                        // Polled immediately, which takes the baseline. A
                        // baseline is never a fire; see `watch`.
                        (None, Some((path, WatchState::default())), now)
                    }
                };

                // Only meaningful for an interval that has run before: a cron
                // expression has no grid to fall behind, and a schedule that
                // has never run is not behind, it is new.
                let behind = match (&schedule.trigger, previous) {
                    (Trigger::Every(interval), Some(previous)) => {
                        interval.missed_between(previous + interval.seconds() as i64, now)
                    }
                    _ => 0,
                };

                Entry {
                    schedule,
                    next,
                    last_run: previous,
                    missed: 0,
                    behind,
                    watch,
                    next_poll,
                }
            })
            .collect();

        Scheduler { entries, workspace }
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn workspace(&self) -> &Path {
        &self.workspace
    }

    /// The earliest moment anything needs attention.
    ///
    /// `None` when nothing ever will — every schedule is a cron expression
    /// that cannot match, which the caller should say out loud rather than
    /// sit in a loop over.
    pub fn wake_at(&self) -> Option<i64> {
        self.entries
            .iter()
            .filter_map(|entry| match entry.watch {
                Some(_) => Some(entry.next_poll),
                None => entry.next,
            })
            .min()
    }

    /// Which schedules are due at `now`, in file order.
    ///
    /// Polls watches as a side effect, because whether a watch is due is only
    /// answerable by looking at the filesystem.
    fn due_now(&mut self, now: i64) -> Vec<usize> {
        let mut due = Vec::new();

        for (index, entry) in self.entries.iter_mut().enumerate() {
            if let Some((path, state)) = entry.watch.as_mut() {
                if now < entry.next_poll {
                    continue;
                }

                let poll_seconds = match &entry.schedule.trigger {
                    Trigger::Watch(watch) => watch.poll_seconds(),
                    _ => crate::watch::DEFAULT_POLL_SECONDS,
                };

                entry.next_poll = now + poll_seconds as i64;

                if state.poll(path) == Poll::Settled {
                    due.push(index);
                }

                continue;
            }

            if entry.next.is_some_and(|next| next <= now) {
                due.push(index);
            }
        }

        due
    }

    /// Run one pass: everything due at `now`, one at a time.
    ///
    /// The runner is called once per due schedule and returns how it went.
    /// Time is read from the clock again after each run, because a run takes
    /// time and the next schedule's due-ness depends on the clock as it is
    /// now rather than as it was when the pass began.
    pub fn pass(
        &mut self,
        clock: &impl Clock,
        runner: &mut impl FnMut(&Schedule) -> Outcome,
    ) -> Vec<Tick> {
        let mut ticks = Vec::new();
        let due = self.due_now(clock.now());

        for index in due {
            let started = clock.now();
            let was_due = self.entries[index].next.unwrap_or(started);

            let outcome = runner(&self.entries[index].schedule);
            let finished = clock.now();

            let entry = &mut self.entries[index];

            // From `started`, not from `was_due`: this counts ticks the run
            // itself ran through, not ones that passed before it got going.
            let missed = match &entry.schedule.trigger {
                Trigger::Every(interval) => interval.missed_between(started, finished),
                Trigger::Cron(_) | Trigger::Watch(_) => 0,
            };

            entry.missed += missed;
            entry.last_run = Some(started);
            entry.next = advance(&entry.schedule.trigger, was_due, finished);

            ticks.push(Tick {
                name: entry.schedule.name.clone(),
                due: was_due,
                started,
                outcome,
                took: (finished - started).max(0),
                missed,
            });
        }

        ticks
    }

    /// Run until `should_stop` says otherwise, sleeping between passes.
    ///
    /// `max_passes` bounds the number of passes for `--once` and for tests.
    /// A pass with nothing due still counts, so `--once` means "look once"
    /// rather than "wait until something happens".
    pub fn run(
        &mut self,
        clock: &impl Clock,
        runner: &mut impl FnMut(&Schedule) -> Outcome,
        max_passes: Option<u64>,
        should_stop: &mut impl FnMut() -> bool,
    ) -> Summary {
        let mut summary = Summary::default();
        let mut passes = 0_u64;

        loop {
            if should_stop() {
                break;
            }

            summary.ticks.extend(self.pass(clock, runner));
            passes += 1;

            if max_passes.is_some_and(|limit| passes >= limit) {
                break;
            }

            if should_stop() {
                break;
            }

            let Some(wake) = self.wake_at() else {
                // Nothing will ever be due. Sitting in a loop over that would
                // burn a core to no purpose; the caller reports it instead.
                break;
            };

            let now = clock.now();

            if wake > now {
                // Capped so a schedule an hour away still notices a stop
                // request, and a clock that jumps is re-examined rather than
                // slept through.
                clock.sleep(((wake - now) as u64).min(MAX_SLEEP_SECONDS));
            }
        }

        summary
    }
}

/// The longest the loop sleeps in one go.
///
/// Not a timing requirement — a wake-up costs nothing. It bounds how long the
/// process can take to notice that it has been asked to stop, and it means a
/// clock that jumps forward is re-examined within the minute rather than
/// having been slept through.
const MAX_SLEEP_SECONDS: u64 = 30;

/// When a trigger is next due, after a run that was due at `due` and finished
/// at `finished`.
fn advance(trigger: &Trigger, due: i64, finished: i64) -> Option<i64> {
    match trigger {
        // Counted from when the run was *due*, not from when it started or
        // finished, so an hourly pipeline stays on the hour rather than
        // drifting later by however long each run takes. Skipped forward past
        // `finished`, which is where the missed ticks went: a run that
        // overran its next tick does not immediately fire again.
        Trigger::Every(interval) => Some(interval.next_after(due, finished)),

        Trigger::Cron(cron) => cron.next_after(finished),

        // A watch is not due at a time.
        Trigger::Watch(_) => None,
    }
}

#[cfg(test)]
mod tests;
