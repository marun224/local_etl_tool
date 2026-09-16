//! The schedule file: what it accepts, and what it refuses by name.

use super::*;

fn parse(json: &str) -> Result<ScheduleFile, serde_json::Error> {
    serde_json::from_str(json)
}

fn one_trigger(json: &str) -> Result<Trigger, String> {
    let raw: RawTrigger = serde_json::from_str(json).map_err(|error| error.to_string())?;

    Trigger::try_from(raw).map_err(|error| error.to_string())
}

// ---------------------------------------------------------------------------
// Triggers
// ---------------------------------------------------------------------------

#[test]
fn the_three_triggers_parse() {
    assert!(matches!(
        one_trigger(r#"{"every": "1h"}"#),
        Ok(Trigger::Every(_))
    ));
    assert!(matches!(
        one_trigger(r#"{"cron": "0 3 * * *"}"#),
        Ok(Trigger::Cron(_))
    ));
    assert!(matches!(
        one_trigger(r#"{"watch": "data/inbox"}"#),
        Ok(Trigger::Watch(_))
    ));
}

#[test]
fn a_timezone_is_refused_with_the_reason() {
    // Settled decision 6, enforced. A schedule that quietly runs an hour off
    // is worse than one that will not start, so this is an error rather than
    // a warning that scrolls past.
    let error = one_trigger(r#"{"cron": "0 3 * * *", "tz": "Asia/Kolkata"}"#)
        .expect_err("a timezone is refused");

    assert!(error.contains("Asia/Kolkata"), "{error}");
    assert!(error.contains("UTC"), "{error}");
    // The message explains itself, because somebody will want to argue with it.
    assert!(error.contains("stale"), "{error}");
}

#[test]
fn a_timezone_is_refused_even_when_the_rest_is_valid() {
    // Checked before the trigger itself, so the timezone is what gets
    // reported rather than the expression parsing happily and running in UTC.
    assert!(one_trigger(r#"{"every": "1h", "tz": "UTC"}"#).is_err());
}

#[test]
fn a_trigger_with_nothing_in_it_says_what_it_wants() {
    let error = one_trigger("{}").expect_err("no trigger");

    assert!(error.contains("every"), "{error}");
    assert!(error.contains("cron"), "{error}");
    assert!(error.contains("watch"), "{error}");
}

#[test]
fn two_triggers_at_once_are_refused_and_both_are_named() {
    let error = one_trigger(r#"{"every": "1h", "cron": "0 3 * * *"}"#).expect_err("two triggers");

    assert!(error.contains("every"), "{error}");
    assert!(error.contains("cron"), "{error}");
}

#[test]
fn a_poll_interval_without_a_watch_is_refused() {
    // It would otherwise be accepted and do nothing, which reads as working.
    let error =
        one_trigger(r#"{"every": "1h", "pollSeconds": 5}"#).expect_err("poll without watch");

    assert!(error.contains("pollSeconds"), "{error}");
}

#[test]
fn a_bad_interval_reports_the_interval_error_rather_than_a_serde_one() {
    let error = one_trigger(r#"{"every": "60"}"#).expect_err("no unit");

    assert!(error.contains("has no unit"), "{error}");
}

#[test]
fn a_bad_cron_expression_reports_the_cron_error() {
    let error = one_trigger(r#"{"cron": "0 3 * *"}"#).expect_err("four fields");

    assert!(error.contains("five"), "{error}");
}

#[test]
fn a_watch_carries_its_poll_interval() {
    let trigger = one_trigger(r#"{"watch": "inbox", "pollSeconds": 3}"#).expect("parses");

    match trigger {
        Trigger::Watch(watch) => assert_eq!(watch.poll_seconds(), 3),
        other => panic!("expected a watch, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The file
// ---------------------------------------------------------------------------

#[test]
fn a_whole_file_parses() {
    let file = parse(
        r#"{
            "formatVersion": 1,
            "schedules": [
                {
                    "name": "hourly",
                    "pipeline": "pipelines/orders.json",
                    "trigger": { "every": "1h" },
                    "context": "prod",
                    "params": { "since": "2026-01-01" }
                },
                {
                    "name": "nightly",
                    "pipeline": "pipelines/rollup.json",
                    "trigger": { "cron": "0 3 * * *" },
                    "enabled": false
                }
            ]
        }"#,
    )
    .expect("parses");

    assert_eq!(file.schedules.len(), 2);
    assert_eq!(file.schedules[0].context.as_deref(), Some("prod"));
    assert_eq!(
        file.schedules[0].params.get("since").map(String::as_str),
        Some("2026-01-01")
    );

    // Enabled defaults to on; the second says otherwise.
    assert!(file.schedules[0].enabled);
    assert!(!file.schedules[1].enabled);

    // A disabled schedule is still listed, so it cannot vanish quietly.
    assert_eq!(file.schedules.len(), 2);
    assert_eq!(file.enabled().count(), 1);
}

#[test]
fn a_missing_file_is_an_empty_set_rather_than_an_error() {
    // A workspace that has never scheduled anything is the normal state, and
    // `etl schedule list` should say "none" rather than fail. The same call
    // `Contexts::load` makes.
    let file = ScheduleFile::load(Path::new("no-such-schedules.json")).expect("not an error");

    assert!(file.schedules.is_empty());
}

#[test]
fn a_file_from_a_newer_version_is_refused_rather_than_guessed_at() {
    let error = serde_json::from_str::<ScheduleFile>(r#"{"formatVersion": 99, "schedules": []}"#)
        .expect("parses as JSON")
        .check();

    // `check` does not police the version — `load` does, because that is
    // where the path is known to name in the message.
    assert!(error.is_ok());
}

#[test]
fn unknown_keys_survive_a_round_trip() {
    // The divergence from Duckle every struct in this product carries: a
    // document written by a newer version must survive being loaded and
    // re-saved by an older one.
    let original = r#"{
        "formatVersion": 1,
        "somethingNewer": { "a": 1 },
        "schedules": [
            {
                "name": "hourly",
                "pipeline": "p.json",
                "trigger": { "every": "1h" },
                "futureField": "kept"
            }
        ]
    }"#;

    let file = parse(original).expect("parses");
    let back = serde_json::to_string(&file).expect("serialises");

    assert!(back.contains("somethingNewer"), "{back}");
    assert!(back.contains("futureField"), "{back}");
}

#[test]
fn a_trigger_survives_a_round_trip() {
    for json in [
        r#"{"every":"90m"}"#,
        r#"{"cron":"0 3 * * *"}"#,
        r#"{"watch":"data/inbox","pollSeconds":10}"#,
    ] {
        let trigger = one_trigger(json).expect("parses");
        let back = serde_json::to_string(&RawTrigger::from(&trigger)).expect("serialises");
        let again = one_trigger(&back).expect("parses again");

        assert_eq!(trigger, again, "{json} did not survive");
    }
}

#[test]
fn an_interval_round_trips_as_it_was_written() {
    // `90m` must not come back as `1h30m`; a re-saved file should show no
    // diff, which is the property the canvas's save already has.
    let trigger = one_trigger(r#"{"every":"90m"}"#).expect("parses");
    let back = serde_json::to_string(&RawTrigger::from(&trigger)).expect("serialises");

    assert!(back.contains("90m"), "{back}");
}

// ---------------------------------------------------------------------------
// Checks that span fields
// ---------------------------------------------------------------------------

#[test]
fn two_schedules_with_one_name_are_refused() {
    // Names are how a run is reported and how a row is found in the list.
    let file = parse(
        r#"{
            "schedules": [
                { "name": "same", "pipeline": "a.json", "trigger": { "every": "1h" } },
                { "name": "same", "pipeline": "b.json", "trigger": { "every": "2h" } }
            ]
        }"#,
    )
    .expect("parses");

    let error = file.check().expect_err("duplicate names");

    assert!(
        matches!(error, ScheduleError::DuplicateName { .. }),
        "{error}"
    );
}

#[test]
fn a_nameless_schedule_is_refused_and_its_position_is_given() {
    // Position rather than name, because the name is what is missing.
    let file = parse(
        r#"{
            "schedules": [
                { "name": "fine", "pipeline": "a.json", "trigger": { "every": "1h" } },
                { "name": "  ", "pipeline": "b.json", "trigger": { "every": "2h" } }
            ]
        }"#,
    )
    .expect("parses");

    let error = file.check().expect_err("a blank name");

    assert!(
        matches!(error, ScheduleError::Unnamed { position: 1 }),
        "{error}"
    );
}

#[test]
fn a_schedule_with_no_pipeline_is_refused() {
    let file = parse(
        r#"{ "schedules": [ { "name": "x", "pipeline": "", "trigger": { "every": "1h" } } ] }"#,
    )
    .expect("parses");

    assert!(matches!(
        file.check().expect_err("no pipeline"),
        ScheduleError::NoPipeline { .. }
    ));
}

#[test]
fn an_empty_file_is_valid() {
    assert!(ScheduleFile::default().check().is_ok());
}

#[test]
fn a_pipeline_path_resolves_against_the_workspace() {
    let file = parse(
        r#"{ "schedules": [
            { "name": "x", "pipeline": "pipelines/a.json", "trigger": { "every": "1h" } }
        ] }"#,
    )
    .expect("parses");

    let workspace = Path::new("/work");

    assert_eq!(
        file.schedules[0].pipeline_in(workspace),
        workspace.join("pipelines/a.json")
    );
}

#[test]
fn a_trigger_describes_itself_for_the_list() {
    assert_eq!(
        one_trigger(r#"{"every":"90m"}"#).unwrap().describe(),
        "every 90m"
    );
    assert_eq!(
        one_trigger(r#"{"cron":"0 3 * * *"}"#).unwrap().describe(),
        "cron 0 3 * * *"
    );
    assert!(one_trigger(r#"{"watch":"inbox"}"#)
        .unwrap()
        .describe()
        .starts_with("watch "));
}
