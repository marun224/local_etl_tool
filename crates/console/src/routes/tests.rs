//! Every route, and every way of being refused, without binding a port.

use super::*;
use crate::auth::Tokens;
use crate::workspace::{PipelineSummary, ScheduleSummary};
use etl_state::{Outcome, RunRecord};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A workspace that answers from memory and counts what it was asked to run.
struct Fake {
    started: AtomicUsize,
    /// When set, every call fails with this, so the failure paths are reachable.
    broken: Option<Failure>,
}

impl Fake {
    fn new() -> Self {
        Fake {
            started: AtomicUsize::new(0),
            broken: None,
        }
    }

    fn broken(failure: Failure) -> Self {
        Fake {
            started: AtomicUsize::new(0),
            broken: Some(failure),
        }
    }

    fn check(&self) -> Result<(), Failure> {
        match &self.broken {
            Some(failure) => Err(failure.clone()),
            None => Ok(()),
        }
    }
}

fn record(id: &str, pipeline: &str) -> RunRecord {
    RunRecord {
        format_version: 1,
        id: id.to_string(),
        pipeline: pipeline.to_string(),
        path: None,
        started: "2026-09-16T12:00:00Z".to_string(),
        elapsed_ms: 120,
        outcome: Outcome::Succeeded,
        stages: Vec::new(),
        notes: Vec::new(),
        warnings: Vec::new(),
        failures: Vec::new(),
        watermarks: Vec::new(),
        extra: Default::default(),
    }
}

impl Workspace for Fake {
    fn label(&self) -> String {
        "test-workspace".to_string()
    }

    fn pipelines(&self) -> Result<Vec<PipelineSummary>, Failure> {
        self.check()?;

        Ok(vec![PipelineSummary {
            name: "orders".to_string(),
            path: "pipelines/orders.json".to_string(),
            stages: Some(3),
            problem: None,
            last_outcome: Some("succeeded".to_string()),
            last_run: Some("2026-09-16T12:00:00Z".to_string()),
        }])
    }

    fn lineage(&self, name: &str) -> Result<serde_json::Value, Failure> {
        self.check()?;

        if name != "orders" {
            return Err(Failure::not_found(format!("no pipeline called '{name}'")));
        }

        Ok(json!({ "pipeline": "orders", "sources": [], "sinks": [] }))
    }

    fn runs(&self, pipeline: Option<&str>, limit: usize) -> Result<Vec<RunRecord>, Failure> {
        self.check()?;

        // Echoed back so the tests can see what the route decoded.
        Ok(vec![record(
            &format!("limit={limit}"),
            pipeline.unwrap_or("<all>"),
        )])
    }

    fn run(&self, id: &str) -> Result<RunRecord, Failure> {
        self.check()?;

        if id == "known" {
            Ok(record("known", "orders"))
        } else {
            Err(Failure::not_found(format!("no run called '{id}'")))
        }
    }

    fn schedules(&self) -> Result<Vec<ScheduleSummary>, Failure> {
        self.check()?;

        Ok(vec![ScheduleSummary {
            name: "nightly".to_string(),
            pipeline: "orders".to_string(),
            trigger: "cron 0 3 * * *".to_string(),
            enabled: true,
            next: Some("2026-09-17T03:00:00Z".to_string()),
            last_run: None,
        }])
    }

    fn start(&self, name: &str) -> Result<RunRecord, Failure> {
        self.check()?;
        self.started.fetch_add(1, Ordering::SeqCst);

        Ok(record("new-run", name))
    }
}

fn tokens() -> Tokens {
    Tokens::build(Some("op".to_string()), Some("view".to_string()))
}

/// A request as a caller with `token` would make it.
fn ask(method: &str, target: &str, token: Option<&str>, workspace: &dyn Workspace) -> Outgoing {
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (target, ""),
    };

    let header = token.map(|token| format!("Bearer {token}"));

    handle(
        &Incoming {
            method,
            path,
            query,
            authorization: header.as_deref(),
        },
        &tokens(),
        workspace,
    )
}

fn body_of(response: &Outgoing) -> serde_json::Value {
    serde_json::from_str(&response.body).expect("a JSON body")
}

// ---------------------------------------------------------------------------
// The page and health
// ---------------------------------------------------------------------------

#[test]
fn the_page_is_served_without_a_token() {
    // The printed link has to open something, and the page itself is what
    // reads the token out of the URL.
    let response = ask("GET", "/", None, &Fake::new());

    assert_eq!(response.status, 200);
    assert!(response.content_type.starts_with("text/html"));
    assert!(response.body.contains("etl console"));
}

#[test]
fn health_needs_no_token_and_gives_nothing_away() {
    // A process manager must be able to ask without a credential, and the
    // answer must not help anybody decide this host is worth attacking.
    let response = ask("GET", "/api/health", None, &Fake::new());

    assert_eq!(response.status, 200);
    assert_eq!(body_of(&response), json!({ "status": "ok" }));

    // No workspace path, no pipeline names, no version.
    assert!(!response.body.contains("test-workspace"));
}

#[test]
fn health_is_a_get() {
    assert_eq!(ask("POST", "/api/health", None, &Fake::new()).status, 405);
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

#[test]
fn the_api_refuses_a_request_with_no_token() {
    for target in [
        "/api/pipelines",
        "/api/runs",
        "/api/schedules",
        "/api/runs/known",
        "/api/pipelines/orders/lineage",
    ] {
        let response = ask("GET", target, None, &Fake::new());

        assert_eq!(response.status, 401, "{target} was served without a token");
    }
}

#[test]
fn the_api_refuses_a_wrong_token() {
    let response = ask("GET", "/api/pipelines", Some("guess"), &Fake::new());

    assert_eq!(response.status, 401);
}

#[test]
fn a_401_says_what_kind_of_credential_without_hinting_whether_one_was_close() {
    let response = ask("GET", "/api/pipelines", Some("guess"), &Fake::new());

    assert!(response
        .headers
        .iter()
        .any(|(name, value)| *name == "WWW-Authenticate" && value == "Bearer"));

    // The message must not differ between "no token" and "wrong token" in a
    // way that tells an attacker their token exists.
    let missing = ask("GET", "/api/pipelines", None, &Fake::new());
    assert_eq!(missing.body, response.body);
}

#[test]
fn a_token_in_the_query_string_does_not_work_on_the_api() {
    // This is what stops a console link pasted into a chat from being a usable
    // API credential, and stops another site's form from posting one for you.
    let response = ask("GET", "/api/pipelines?token=op", None, &Fake::new());

    assert_eq!(response.status, 401);
}

#[test]
fn a_token_in_the_query_string_does_not_work_for_a_post_either() {
    let fake = Fake::new();
    let response = ask("POST", "/api/runs?pipeline=orders&token=op", None, &fake);

    assert_eq!(response.status, 401);
    assert_eq!(
        fake.started.load(Ordering::SeqCst),
        0,
        "a run was started by a URL-only credential"
    );
}

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

#[test]
fn a_viewer_may_read_everything() {
    for target in [
        "/api/pipelines",
        "/api/runs",
        "/api/schedules",
        "/api/runs/known",
        "/api/pipelines/orders/lineage",
    ] {
        let response = ask("GET", target, Some("view"), &Fake::new());

        assert_eq!(response.status, 200, "{target}: {}", response.body);
    }
}

#[test]
fn a_viewer_may_not_start_a_run() {
    // The reason there are two roles at all.
    let fake = Fake::new();
    let response = ask("POST", "/api/runs?pipeline=orders", Some("view"), &fake);

    assert_eq!(response.status, 403);
    assert_eq!(
        fake.started.load(Ordering::SeqCst),
        0,
        "a viewer started a run"
    );
}

#[test]
fn a_refused_role_is_403_rather_than_401() {
    // 401 would send somebody looking for a better token when what they need
    // is a different one.
    let response = ask(
        "POST",
        "/api/runs?pipeline=orders",
        Some("view"),
        &Fake::new(),
    );

    assert_eq!(response.status, 403);
    assert!(body_of(&response)["error"]
        .as_str()
        .expect("a message")
        .contains("operator"));
}

#[test]
fn an_operator_may_start_a_run() {
    let fake = Fake::new();
    let response = ask("POST", "/api/runs?pipeline=orders", Some("op"), &fake);

    assert_eq!(response.status, 200, "{}", response.body);
    assert_eq!(fake.started.load(Ordering::SeqCst), 1);
    assert_eq!(body_of(&response)["run"]["pipeline"], "orders");
}

#[test]
fn every_authenticated_response_states_the_role() {
    // How the page knows whether to draw a Run button, without guessing it
    // from an error message.
    let role_of = |token: &str| {
        ask("GET", "/api/pipelines", Some(token), &Fake::new())
            .headers
            .iter()
            .find(|(name, _)| *name == "X-Etl-Role")
            .map(|(_, value)| value.clone())
    };

    assert_eq!(role_of("op"), Some("operator".to_string()));
    assert_eq!(role_of("view"), Some("viewer".to_string()));
}

#[test]
fn a_refusal_states_the_role_it_did_have() {
    let response = ask(
        "POST",
        "/api/runs?pipeline=orders",
        Some("view"),
        &Fake::new(),
    );

    assert!(response
        .headers
        .iter()
        .any(|(name, value)| *name == "X-Etl-Role" && value == "viewer"));
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

#[test]
fn pipelines_are_listed() {
    let response = ask("GET", "/api/pipelines", Some("view"), &Fake::new());

    let body = body_of(&response);
    assert_eq!(body["pipelines"][0]["name"], "orders");
    assert_eq!(body["pipelines"][0]["stages"], 3);
}

#[test]
fn runs_take_a_pipeline_and_a_limit() {
    let response = ask(
        "GET",
        "/api/runs?pipeline=orders&limit=7",
        Some("view"),
        &Fake::new(),
    );

    let body = body_of(&response);
    assert_eq!(body["runs"][0]["pipeline"], "orders");
    assert_eq!(body["runs"][0]["id"], "limit=7");
}

#[test]
fn a_listing_with_no_limit_has_a_default() {
    let response = ask("GET", "/api/runs", Some("view"), &Fake::new());

    assert_eq!(body_of(&response)["runs"][0]["id"], "limit=50");
    assert_eq!(body_of(&response)["runs"][0]["pipeline"], "<all>");
}

#[test]
fn an_enormous_limit_is_capped_rather_than_honoured() {
    // A year of history serialised into memory to answer one request is a way
    // to take the console down with a single URL.
    let response = ask(
        "GET",
        "/api/runs?limit=100000000",
        Some("view"),
        &Fake::new(),
    );

    assert_eq!(body_of(&response)["runs"][0]["id"], "limit=1000");
}

#[test]
fn a_limit_of_zero_becomes_one_rather_than_nothing() {
    let response = ask("GET", "/api/runs?limit=0", Some("view"), &Fake::new());

    assert_eq!(body_of(&response)["runs"][0]["id"], "limit=1");
}

#[test]
fn a_limit_that_is_not_a_number_is_refused_by_name() {
    let response = ask("GET", "/api/runs?limit=lots", Some("view"), &Fake::new());

    assert_eq!(response.status, 422);
    assert!(body_of(&response)["error"]
        .as_str()
        .expect("a message")
        .contains("lots"));
}

#[test]
fn one_run_can_be_fetched_and_a_missing_one_is_404() {
    assert_eq!(
        ask("GET", "/api/runs/known", Some("view"), &Fake::new()).status,
        200
    );

    let missing = ask("GET", "/api/runs/nope", Some("view"), &Fake::new());
    assert_eq!(missing.status, 404);
}

#[test]
fn lineage_is_served_for_a_pipeline_that_exists() {
    let response = ask(
        "GET",
        "/api/pipelines/orders/lineage",
        Some("view"),
        &Fake::new(),
    );

    assert_eq!(response.status, 200);
    assert_eq!(body_of(&response)["pipeline"], "orders");
}

#[test]
fn a_pipeline_name_is_never_joined_onto_a_path() {
    // The traversal attempt reaches the workspace as a *name*, which resolves
    // it against the pipelines it knows about and finds nothing. It must never
    // become part of a filesystem path here.
    let response = ask(
        "GET",
        "/api/pipelines/..%2F..%2Fetc%2Fpasswd/lineage",
        Some("view"),
        &Fake::new(),
    );

    assert_eq!(response.status, 404);
}

#[test]
fn schedules_are_listed() {
    let response = ask("GET", "/api/schedules", Some("view"), &Fake::new());

    assert_eq!(body_of(&response)["schedules"][0]["name"], "nightly");
}

// ---------------------------------------------------------------------------
// Failures from the workspace
// ---------------------------------------------------------------------------

#[test]
fn a_workspace_failure_keeps_its_own_status() {
    // Only the workspace knows whether a name it could not find is a missing
    // file or a pipeline that will not compile.
    let broken = Fake::broken(Failure::invalid("orders.json will not compile"));
    let response = ask("GET", "/api/pipelines", Some("view"), &broken);

    assert_eq!(response.status, 422);
    assert_eq!(body_of(&response)["error"], "orders.json will not compile");
}

#[test]
fn a_run_that_cannot_start_reports_why() {
    let broken = Fake::broken(Failure::conflict("a scheduler holds this workspace"));
    let response = ask("POST", "/api/runs?pipeline=orders", Some("op"), &broken);

    assert_eq!(response.status, 409);
}

#[test]
fn a_post_with_no_pipeline_says_which_pipeline() {
    let response = ask("POST", "/api/runs", Some("op"), &Fake::new());

    assert_eq!(response.status, 422);
    assert!(body_of(&response)["error"]
        .as_str()
        .expect("a message")
        .contains("pipeline"));
}

// ---------------------------------------------------------------------------
// Methods and paths
// ---------------------------------------------------------------------------

#[test]
fn an_unknown_path_is_404() {
    assert_eq!(
        ask("GET", "/api/nope", Some("op"), &Fake::new()).status,
        404
    );
    assert_eq!(ask("GET", "/nope", Some("op"), &Fake::new()).status, 404);
}

#[test]
fn a_known_path_with_the_wrong_method_is_405() {
    assert_eq!(
        ask("DELETE", "/api/pipelines", Some("op"), &Fake::new()).status,
        405
    );
    assert_eq!(
        ask("PUT", "/api/runs", Some("op"), &Fake::new()).status,
        405
    );
}

#[test]
fn a_write_method_on_an_unknown_path_still_needs_a_token() {
    // The refusal must come from the token check, not from the router, so a
    // probe cannot map the API without one.
    assert_eq!(
        ask("DELETE", "/api/anything", None, &Fake::new()).status,
        401
    );
}

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

#[test]
fn every_response_is_hardened_including_the_refusals() {
    let responses = [
        ask("GET", "/", None, &Fake::new()),
        ask("GET", "/api/health", None, &Fake::new()),
        ask("GET", "/api/pipelines", None, &Fake::new()),
        ask("GET", "/api/pipelines", Some("view"), &Fake::new()),
        ask("GET", "/api/nope", Some("op"), &Fake::new()),
    ];

    for response in &responses {
        let named = |name: &str| response.headers.iter().any(|(key, _)| *key == name);

        assert!(named("Content-Security-Policy"), "{}", response.status);
        assert!(named("X-Content-Type-Options"), "{}", response.status);
        assert!(named("Referrer-Policy"), "{}", response.status);
        // The page URL carries a token; a cached copy would outlive it.
        assert!(named("Cache-Control"), "{}", response.status);
    }
}

#[test]
fn the_policy_forbids_framing_and_third_party_loads() {
    let response = ask("GET", "/", None, &Fake::new());

    let policy = response
        .headers
        .iter()
        .find(|(name, _)| *name == "Content-Security-Policy")
        .map(|(_, value)| value.clone())
        .expect("a policy");

    assert!(policy.contains("default-src 'none'"), "{policy}");
    assert!(policy.contains("frame-ancestors 'none'"), "{policy}");
    assert!(policy.contains("form-action 'none'"), "{policy}");
}

// ---------------------------------------------------------------------------
// Query parsing
// ---------------------------------------------------------------------------

#[test]
fn parameters_are_read_and_decoded() {
    assert_eq!(parameter("a=1&b=2", "b"), Some("2".to_string()));
    assert_eq!(
        parameter("name=my%20pipeline", "name"),
        Some("my pipeline".to_string())
    );
    assert_eq!(
        parameter("name=my+pipeline", "name"),
        Some("my pipeline".to_string())
    );
    assert_eq!(parameter("a=1", "missing"), None);
    assert_eq!(parameter("", "a"), None);
}

#[test]
fn an_empty_value_is_the_same_as_absent() {
    // `?pipeline=` is somebody leaving it blank, not asking for a pipeline
    // whose name is the empty string.
    assert_eq!(parameter("pipeline=", "pipeline"), None);
    assert_eq!(parameter("pipeline", "pipeline"), None);
}

#[test]
fn the_first_occurrence_of_a_name_wins() {
    // Parameter smuggling: two values for one name must not be ambiguous.
    assert_eq!(
        parameter("a=first&a=second", "a"),
        Some("first".to_string())
    );
}

#[test]
fn a_broken_escape_is_kept_rather_than_dropped() {
    // Silently deleting bytes is how a check gets bypassed by something that
    // was not what it was read as.
    assert_eq!(parameter("a=100%", "a"), Some("100%".to_string()));
    assert_eq!(parameter("a=%zz", "a"), Some("%zz".to_string()));
    assert_eq!(parameter("a=%4", "a"), Some("%4".to_string()));
}

#[test]
fn a_decoded_name_matches() {
    assert_eq!(
        parameter("pipe%6Cine=orders", "pipeline"),
        Some("orders".to_string())
    );
}
