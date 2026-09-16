//! The contexts file.

use super::*;
use crate::params::Resolver;

const TWO: &str = r#"{
  "formatVersion": 1,
  "active": "dev",
  "contexts": {
    "dev":  { "description": "This machine", "variables": { "root": "D:/data/dev" } },
    "prod": { "variables": { "root": "s3://analytics/prod", "region": "eu-west-1" } }
  }
}"#;

/// A directory of this test's own, so tests cannot tread on each other.
fn scratch(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join("etl-context-tests").join(name);

    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("scratch directory");

    directory
}

// ---------------------------------------------------------------------------
// The file
// ---------------------------------------------------------------------------

#[test]
fn a_contexts_file_parses() {
    let contexts = Contexts::from_json(TWO).expect("parses");

    assert_eq!(contexts.format_version, CURRENT_FORMAT_VERSION);
    assert_eq!(contexts.active.as_deref(), Some("dev"));
    assert_eq!(contexts.names(), ["dev", "prod"]);
    assert_eq!(contexts.contexts["prod"].variables["region"], "eu-west-1");
    assert_eq!(
        contexts.contexts["dev"].description.as_deref(),
        Some("This machine")
    );
}

#[test]
fn a_contexts_file_round_trips() {
    let once = Contexts::from_json(TWO).unwrap();
    let text = once.to_json_pretty().unwrap();

    assert_eq!(Contexts::from_json(&text).unwrap(), once);
}

#[test]
fn unknown_keys_survive_a_round_trip() {
    // The same rule as the pipeline document: a file written by a newer version
    // must not lose anything by being loaded and saved by an older one.
    let future = r#"{
      "formatVersion": 2,
      "unknownTopLevel": {"a": 1},
      "contexts": { "dev": { "variables": {}, "unknownContextKey": true } }
    }"#;

    let contexts = Contexts::from_json(future).unwrap();
    let out: serde_json::Value = serde_json::from_str(&contexts.to_json_pretty().unwrap()).unwrap();

    assert_eq!(out["unknownTopLevel"]["a"], 1);
    assert_eq!(out["contexts"]["dev"]["unknownContextKey"], true);
    assert_eq!(contexts.format_version, 2);
}

#[test]
fn a_missing_file_is_an_empty_set_rather_than_an_error() {
    // Most workspaces never define a context, and a pipeline that needs none
    // should not have to create a file to say so.
    let contexts = Contexts::load(&scratch("missing").join("contexts.json")).expect("loads");

    assert!(contexts.is_empty());
    assert_eq!(contexts.active, None);
}

#[test]
fn a_malformed_file_names_the_file() {
    let directory = scratch("malformed");
    let path = directory.join("contexts.json");
    std::fs::write(&path, "{ not json").unwrap();

    let error = Contexts::load(&path).expect_err("should not parse");

    assert!(matches!(error, ContextError::Malformed { .. }), "{error:?}");
    assert!(error.to_string().contains("contexts.json"), "{error}");
}

#[test]
fn an_active_context_that_is_not_defined_is_caught_on_load() {
    // Otherwise every run silently uses no context at all, which looks like the
    // pipeline being broken rather than the workspace being misconfigured.
    let directory = scratch("active_missing");
    let path = directory.join("contexts.json");
    std::fs::write(
        &path,
        r#"{"active": "staging", "contexts": {"dev": {"variables": {}}}}"#,
    )
    .unwrap();

    let error = Contexts::load(&path).expect_err("should refuse");

    assert!(
        matches!(&error, ContextError::ActiveMissing { name, .. } if name == "staging"),
        "{error:?}"
    );
}

#[test]
fn the_file_is_found_under_dot_etl_in_the_workspace() {
    let path = Contexts::path_in(Path::new("D:/work"));

    assert!(path.ends_with("contexts.json"));
    assert_eq!(
        path.to_string_lossy().replace('\\', "/"),
        "D:/work/.etl/contexts.json"
    );
}

#[test]
fn a_workspace_with_a_contexts_file_loads_it() {
    let workspace = scratch("workspace");
    std::fs::create_dir_all(workspace.join(".etl")).unwrap();
    std::fs::write(Contexts::path_in(&workspace), TWO).unwrap();

    let contexts = Contexts::load_from_workspace(&workspace).expect("loads");

    assert_eq!(contexts.names(), ["dev", "prod"]);
}

// ---------------------------------------------------------------------------
// Applying one
// ---------------------------------------------------------------------------

fn resolver() -> Resolver {
    Resolver::new("D:/work").with_today("2026-09-15")
}

#[test]
fn the_files_active_context_is_used_when_the_command_line_says_nothing() {
    let contexts = Contexts::from_json(TWO).unwrap();
    let resolver = contexts.apply(resolver(), None).expect("applies");

    assert_eq!(resolver.active_context(), Some("dev"));
}

#[test]
fn an_explicit_choice_overrides_the_files_active_context() {
    let contexts = Contexts::from_json(TWO).unwrap();
    let resolver = contexts.apply(resolver(), Some("prod")).expect("applies");

    assert_eq!(resolver.active_context(), Some("prod"));
}

#[test]
fn choosing_a_context_that_does_not_exist_fails_rather_than_falling_back() {
    // Running against dev because "prod" was misspelled is the worst outcome
    // available, so a name that is not there is an error.
    let contexts = Contexts::from_json(TWO).unwrap();
    let error = contexts
        .apply(resolver(), Some("prd"))
        .expect_err("should refuse");

    let message = error.to_string();
    assert!(matches!(error, ContextError::Unknown { .. }), "{error:?}");
    assert!(
        message.contains("dev") && message.contains("prod"),
        "{message}"
    );
}

#[test]
fn every_context_is_loaded_not_only_the_active_one() {
    // `${prod.root}` names a context explicitly and has to reach it even while
    // dev is the active one.
    let contexts = Contexts::from_json(TWO).unwrap();
    let resolver = contexts.apply(resolver(), Some("dev")).expect("applies");

    assert!(resolver.knows_context("dev"));
    assert!(resolver.knows_context("prod"));
    assert!(!resolver.knows_context("staging"));
}

#[test]
fn an_empty_contexts_file_activates_nothing() {
    let resolver = Contexts::default()
        .apply(resolver(), None)
        .expect("applies");

    assert_eq!(resolver.active_context(), None);
}
