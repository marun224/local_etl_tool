//! The loop, driven by a fake clock.
//!
//! Nothing here sleeps. A run that "takes" twenty-five minutes advances a
//! counter, which is the only reason it is possible to test what happens when
//! a run overruns its next tick at all.

use super::*;
use crate::{Interval, Trigger};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// A clock the test moves by hand.
struct FakeClock {
    now: Cell<i64>,
    slept: Cell<u64>,
}

impl FakeClock {
    fn at(now: i64) -> Self {
        FakeClock {
            now: Cell::new(now),
            slept: Cell::new(0),
        }
    }

    fn advance(&self, seconds: i64) {
        self.now.set(self.now.get() + seconds);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> i64 {
        self.now.get()
    }

    /// Jumps rather than waits, so a day of scheduling costs microseconds.
    fn sleep(&self, seconds: u64) {
        self.slept.set(self.slept.get() + seconds);
        self.advance(seconds as i64);
    }
}

fn schedule(name: &str, trigger: Trigger) -> Schedule {
    Schedule {
        name: name.to_string(),
        pipeline: PathBuf::from("pipeline.json"),
        trigger,
        enabled: true,
        context: None,
        params: BTreeMap::new(),
        extra: BTreeMap::new(),
    }
}

fn every(text: &str) -> Trigger {
    Trigger::Every(Interval::parse(text).expect("parses"))
}

fn cron(text: &str) -> Trigger {
    Trigger::Cron(crate::Cron::parse(text).expect("parses"))
}

/// A scheduler over one schedule with no history.
fn one(trigger: Trigger, now: i64) -> Scheduler {
    Scheduler::new(vec![schedule("job", trigger)], ".", now, |_| None)
}

// ---------------------------------------------------------------------------
// When things are due
// ---------------------------------------------------------------------------

#[test]
fn a_new_interval_schedule_waits_one_interval_rather_than_firing_at_once() {
    // Adding "every 1h" at 4pm means 5pm, not this instant. The other reading
    // makes every scheduler restart a run.
    let scheduler = one(every("1h"), 1_000);

    assert_eq!(scheduler.entries()[0].next, Some(1_000 + 3_600));
}

#[test]
fn an_interval_is_counted_from_the_last_recorded_run() {
    // The reason 8c needs 8b's history: restarting the scheduler must not
    // restart the clock.
    let last_run = 10_000;
    let now = last_run + 600; // ten minutes later, scheduler restarts

    let scheduler = Scheduler::new(vec![schedule("job", every("1h"))], ".", now, |_| {
        Some(last_run)
    });

    // Due an hour after the last run, not an hour after the restart.
    assert_eq!(scheduler.entries()[0].next, Some(last_run + 3_600));
}

#[test]
fn a_schedule_whose_interval_already_elapsed_is_due_immediately() {
    // Down for three hours on an hourly schedule: it should run on start.
    let last_run = 0;
    let now = 3 * 3_600 + 30;

    let scheduler = Scheduler::new(vec![schedule("job", every("1h"))], ".", now, |_| {
        Some(last_run)
    });

    let next = scheduler.entries()[0].next.expect("is due");
    assert!(next <= now, "next {next} should be at or before now {now}");
}

#[test]
fn nothing_runs_before_it_is_due() {
    let clock = FakeClock::at(1_000);
    let mut scheduler = one(every("1h"), clock.now());
    let mut ran = 0;

    // Half an hour of passes on an hourly schedule.
    for _ in 0..30 {
        clock.advance(60);
        scheduler.pass(&clock, &mut |_| {
            ran += 1;
            Outcome::Succeeded
        });
    }

    assert_eq!(ran, 0);
}

#[test]
fn it_runs_when_it_comes_due() {
    let clock = FakeClock::at(1_000);
    let mut scheduler = one(every("10m"), clock.now());
    let mut ran = 0;

    for _ in 0..30 {
        clock.advance(60);
        scheduler.pass(&clock, &mut |_| {
            ran += 1;
            Outcome::Succeeded
        });
    }

    // Thirty minutes at ten-minute intervals.
    assert_eq!(ran, 3);
}

// ---------------------------------------------------------------------------
// Missed ticks
// ---------------------------------------------------------------------------

#[test]
fn a_run_that_overruns_its_tick_skips_to_now_and_counts_what_it_missed() {
    // The decision this phase turns on. A ten-minute schedule whose run takes
    // twenty-five minutes has passed two further ticks; it runs once more,
    // not three times.
    let clock = FakeClock::at(0);
    let mut scheduler = one(every("10m"), clock.now());

    clock.advance(600); // the first tick is due

    let ticks = scheduler.pass(&clock, &mut |_| {
        clock.advance(1_500); // the run takes 25 minutes
        Outcome::Succeeded
    });

    assert_eq!(ticks.len(), 1, "it ran once");
    assert_eq!(ticks[0].missed, 2, "two ticks passed while it ran");
    assert_eq!(ticks[0].took, 1_500);

    // And the next tick is ahead of the clock rather than immediately due,
    // which is what "skip to now" means.
    let next = scheduler.entries()[0].next.expect("has a next");
    assert!(
        next > clock.now(),
        "next {next} must be after now {}",
        clock.now()
    );
}

#[test]
fn a_run_that_finishes_inside_its_interval_misses_nothing() {
    let clock = FakeClock::at(0);
    let mut scheduler = one(every("10m"), clock.now());

    clock.advance(600);

    let ticks = scheduler.pass(&clock, &mut |_| {
        clock.advance(30);
        Outcome::Succeeded
    });

    assert_eq!(ticks[0].missed, 0);
}

#[test]
fn ticks_stay_on_their_grid_rather_than_drifting_by_the_run_time() {
    // Each run takes a minute. After ten runs an hourly schedule should still
    // be on the hour, not ten minutes late — which is what anchoring the next
    // tick on the due time rather than on the finish buys.
    let clock = FakeClock::at(0);
    let mut scheduler = one(every("1h"), clock.now());
    let mut fired_at = Vec::new();

    for _ in 0..600 {
        clock.advance(60);

        let ticks = scheduler.pass(&clock, &mut |_| {
            clock.advance(60);
            Outcome::Succeeded
        });

        for tick in ticks {
            fired_at.push(tick.due);
        }
    }

    assert!(
        fired_at.len() >= 9,
        "expected several runs, got {fired_at:?}"
    );

    for (index, due) in fired_at.iter().enumerate() {
        assert_eq!(
            *due,
            3_600 * (index as i64 + 1),
            "run {index} drifted off the hour"
        );
    }
}

#[test]
fn a_cron_schedule_does_not_report_missed_ticks() {
    // A cron expression's next fire is computed from the expression, so there
    // is no grid to fall behind in the way an interval has.
    let clock = FakeClock::at(0);
    let mut scheduler = one(cron("* * * * *"), clock.now());

    clock.advance(120);

    let ticks = scheduler.pass(&clock, &mut |_| {
        clock.advance(600);
        Outcome::Succeeded
    });

    assert_eq!(ticks.len(), 1);
    assert_eq!(ticks[0].missed, 0);
}

// ---------------------------------------------------------------------------
// One at a time
// ---------------------------------------------------------------------------

#[test]
fn two_schedules_due_together_run_one_after_the_other() {
    // Sequential execution is what keeps the state crate's single-writer
    // assumption true, so this is worth pinning rather than assuming.
    let clock = FakeClock::at(0);
    let mut scheduler = Scheduler::new(
        vec![
            schedule("first", every("10m")),
            schedule("second", every("10m")),
        ],
        ".",
        clock.now(),
        |_| None,
    );

    clock.advance(600);

    let mut in_flight = 0;
    let mut most_at_once = 0;
    let mut order = Vec::new();

    let ticks = scheduler.pass(&clock, &mut |schedule| {
        in_flight += 1;
        most_at_once = most_at_once.max(in_flight);
        order.push(schedule.name.clone());
        clock.advance(60);
        in_flight -= 1;
        Outcome::Succeeded
    });

    assert_eq!(most_at_once, 1, "runs overlapped");
    assert_eq!(order, ["first", "second"]);
    assert_eq!(ticks.len(), 2);

    // The second started after the first finished.
    assert!(ticks[1].started >= ticks[0].started + ticks[0].took);
}

#[test]
fn a_schedule_delayed_by_another_run_still_reports_the_tick_it_was_due_at() {
    // `due` is the grid time, `started` is when it actually got to run. The
    // gap between them is the thing somebody debugging a late load needs.
    let clock = FakeClock::at(0);
    let mut scheduler = Scheduler::new(
        vec![
            schedule("slow", every("10m")),
            schedule("waiting", every("10m")),
        ],
        ".",
        clock.now(),
        |_| None,
    );

    clock.advance(600);

    let ticks = scheduler.pass(&clock, &mut |schedule| {
        if schedule.name == "slow" {
            clock.advance(300);
        }
        Outcome::Succeeded
    });

    let waiting = &ticks[1];
    assert_eq!(waiting.due, 600, "it was due on the grid");
    assert_eq!(waiting.started, 900, "it started after the slow one");
}

// ---------------------------------------------------------------------------
// The loop
// ---------------------------------------------------------------------------

#[test]
fn a_single_pass_looks_once_and_returns() {
    // `--once` means "look now", not "wait until something happens".
    let clock = FakeClock::at(0);
    let mut scheduler = one(every("1h"), clock.now());

    let summary = scheduler.run(&clock, &mut |_| Outcome::Succeeded, Some(1), &mut || false);

    assert_eq!(summary.ran(), 0);
    assert_eq!(clock.slept.get(), 0, "one pass must not sleep");
}

#[test]
fn the_loop_sleeps_until_the_next_thing_is_due() {
    let clock = FakeClock::at(0);
    let mut scheduler = one(every("10m"), clock.now());

    let summary = scheduler.run(&clock, &mut |_| Outcome::Succeeded, Some(40), &mut || false);

    assert!(summary.ran() >= 1, "nothing ran in forty passes");
    assert!(clock.slept.get() > 0, "the loop never slept");
}

#[test]
fn a_stop_request_ends_the_loop() {
    let clock = FakeClock::at(0);
    let mut scheduler = one(every("1s"), clock.now());

    let mut passes = 0;
    let summary = scheduler.run(&clock, &mut |_| Outcome::Succeeded, None, &mut || {
        passes += 1;
        passes > 5
    });

    assert!(summary.ran() < 10, "the loop did not stop when asked");
}

#[test]
fn the_loop_ends_rather_than_spinning_when_nothing_can_ever_be_due() {
    // Every schedule is a cron expression that never matches. Sitting in a
    // loop over that would burn a core to no purpose.
    let clock = FakeClock::at(0);
    let mut scheduler = one(cron("0 0 30 2 *"), clock.now());

    assert_eq!(scheduler.entries()[0].next, None);
    assert_eq!(scheduler.wake_at(), None);

    let summary = scheduler.run(&clock, &mut |_| Outcome::Succeeded, None, &mut || false);

    assert_eq!(summary.ran(), 0);
}

#[test]
fn sleeping_is_capped_so_a_stop_is_noticed() {
    // A schedule a day away must not make the process take a day to notice
    // that it has been asked to stop.
    let clock = FakeClock::at(0);
    let mut scheduler = one(every("1d"), clock.now());

    scheduler.run(&clock, &mut |_| Outcome::Succeeded, Some(2), &mut || false);

    assert!(
        clock.slept.get() <= MAX_SLEEP_SECONDS,
        "slept {} seconds in one go",
        clock.slept.get()
    );
}

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

#[test]
fn a_failed_run_does_not_stop_the_schedule() {
    // A pipeline that failed at 3am must still be tried at 4am. The
    // alternative is one bad night silently ending the schedule.
    let clock = FakeClock::at(0);
    let mut scheduler = one(every("10m"), clock.now());
    let mut runs = 0;

    for _ in 0..30 {
        clock.advance(60);
        scheduler.pass(&clock, &mut |_| {
            runs += 1;
            Outcome::Failed
        });
    }

    assert_eq!(runs, 3);
}

#[test]
fn a_broken_pipeline_keeps_its_place_in_the_rota() {
    let clock = FakeClock::at(0);
    let mut scheduler = one(every("10m"), clock.now());
    let mut runs = 0;

    for _ in 0..30 {
        clock.advance(60);
        scheduler.pass(&clock, &mut |_| {
            runs += 1;
            Outcome::Broken
        });
    }

    assert_eq!(runs, 3);
}

#[test]
fn the_summary_counts_runs_failures_and_missed_ticks() {
    let clock = FakeClock::at(0);
    let mut scheduler = Scheduler::new(
        vec![schedule("ok", every("10m")), schedule("bad", every("10m"))],
        ".",
        clock.now(),
        |_| None,
    );

    clock.advance(600);

    let mut summary = Summary::default();
    summary
        .ticks
        .extend(scheduler.pass(&clock, &mut |schedule| {
            if schedule.name == "bad" {
                Outcome::Failed
            } else {
                Outcome::Succeeded
            }
        }));

    assert_eq!(summary.ran(), 2);
    assert_eq!(summary.failed(), 1);
    assert_eq!(summary.missed(), 0);
}

#[test]
fn a_watch_is_described_by_what_it_watches_rather_than_by_a_time() {
    let scheduler = one(Trigger::Watch(crate::Watch::new("data/inbox", None)), 1_000);

    let described = scheduler.entries()[0].describe_next(1_000);

    assert!(described.contains("inbox"), "{described}");
    assert_eq!(scheduler.entries()[0].next, None);
}

#[test]
fn a_tick_already_in_the_past_reads_as_overdue_rather_than_as_a_stale_time() {
    // A pipeline down since yesterday shows a next fire of yesterday, which
    // looks like a bug until you work out it means "immediately".
    let last_run = 0;
    let now = 10 * 3_600;

    let scheduler = Scheduler::new(vec![schedule("job", every("1h"))], ".", now, |_| {
        Some(last_run)
    });

    assert_eq!(scheduler.entries()[0].describe_next(now), "due now");
}

#[test]
fn a_cron_that_can_never_fire_says_never() {
    let scheduler = one(cron("0 0 30 2 *"), 1_000);

    assert_eq!(scheduler.entries()[0].describe_next(1_000), "never");
}

// ---------------------------------------------------------------------------
// Behind, versus missed
// ---------------------------------------------------------------------------

#[test]
fn downtime_is_reported_as_behind_rather_than_as_missed() {
    // The distinction worth keeping: "you were switched off all night" must
    // not read as "your pipeline is too slow". A ten-minute schedule whose
    // last run was two hours ago is twelve intervals behind, and the run it
    // then does misses nothing.
    let clock = FakeClock::at(7_200);
    let mut scheduler = Scheduler::new(
        vec![schedule("job", every("10m"))],
        ".",
        clock.now(),
        |_| Some(0),
    );

    assert_eq!(scheduler.entries()[0].behind, 11);

    let ticks = scheduler.pass(&clock, &mut |_| {
        clock.advance(2);
        Outcome::Succeeded
    });

    assert_eq!(ticks.len(), 1, "the overdue run happens");
    assert_eq!(
        ticks[0].missed, 0,
        "a quick run that was merely overdue misses nothing"
    );
}

#[test]
fn a_schedule_that_has_never_run_is_new_rather_than_behind() {
    let scheduler = one(every("10m"), 1_000_000);

    assert_eq!(scheduler.entries()[0].behind, 0);
}

#[test]
fn a_cron_schedule_is_never_behind() {
    // There is no grid to fall behind: the next fire comes from the
    // expression, not from the last run.
    let scheduler = Scheduler::new(
        vec![schedule("job", cron("0 3 * * *"))],
        ".",
        7_200_000,
        |_| Some(0),
    );

    assert_eq!(scheduler.entries()[0].behind, 0);
}

#[test]
fn overrun_is_still_counted_as_missed() {
    // The other half of the distinction: a run that genuinely overran its own
    // tick does report it, and this is measured from when the run started.
    let clock = FakeClock::at(0);
    let mut scheduler = one(every("10m"), clock.now());

    clock.advance(600);

    let ticks = scheduler.pass(&clock, &mut |_| {
        clock.advance(1_500);
        Outcome::Succeeded
    });

    assert_eq!(ticks[0].missed, 2);
    assert_eq!(scheduler.entries()[0].behind, 0, "it was not down");
}
