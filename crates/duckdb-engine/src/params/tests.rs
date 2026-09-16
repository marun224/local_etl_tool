//! Parameter resolution and `${...}` interpolation.

use super::*;
use crate::plan::tests_support::{document, node};
use serde_json::json;

/// A one-node document whose `src.file.csv` carries the given properties.
fn doc(properties: JsonValue) -> PipelineDoc {
    document(vec![node("n", "src.file.csv", properties)], vec![])
}

/// A resolver with a fixed workspace and a fixed clock, so nothing in these
/// tests depends on where they run or when.
fn resolver() -> Resolver {
    Resolver::new("D:/work").with_today("2026-09-15")
}

/// The resolved value of a one-node document's `path`.
fn path_of(document: &PipelineDoc, resolver: &Resolver) -> String {
    resolve(document, resolver)
        .expect("resolves")
        .document
        .nodes[0]
        .data
        .properties
        .as_ref()
        .unwrap()["path"]
        .as_str()
        .expect("path is text")
        .to_string()
}

// ---------------------------------------------------------------------------
// Substitution
// ---------------------------------------------------------------------------

#[test]
fn an_explicit_binding_is_substituted() {
    let document = doc(json!({ "path": "${root}/orders.csv" }));
    let resolver = resolver().bind("root", "D:/data");

    assert_eq!(path_of(&document, &resolver), "D:/data/orders.csv");
}

#[test]
fn several_references_in_one_string_are_all_replaced() {
    let document = doc(json!({ "path": "${root}/${folder}/orders.csv" }));
    let resolver = resolver().bind("root", "D:/data").bind("folder", "raw");

    assert_eq!(path_of(&document, &resolver), "D:/data/raw/orders.csv");
}

#[test]
fn the_workspace_built_in_is_the_workspace_root() {
    let document = doc(json!({ "path": "${workspace}/samples/orders.csv" }));

    assert_eq!(
        path_of(&document, &resolver()),
        "D:/work/samples/orders.csv"
    );
}

#[test]
fn a_windows_workspace_is_written_with_forward_slashes() {
    // The value almost always lands inside a path literal, and `quote_path`
    // would normalise it anyway; doing it here keeps the two consistent.
    let document = doc(json!({ "path": "${workspace}/orders.csv" }));
    let resolver = Resolver::new(r"D:\work\project").with_today("2026-09-15");

    assert_eq!(path_of(&document, &resolver), "D:/work/project/orders.csv");
}

#[test]
fn the_date_built_in_is_todays_date() {
    let document = doc(json!({ "path": "runs/${date}/orders.csv" }));

    assert_eq!(
        path_of(&document, &resolver()),
        "runs/2026-09-15/orders.csv"
    );
}

#[test]
fn a_context_qualified_reference_reads_that_context() {
    let document = doc(json!({ "path": "${prod.root}/orders.csv" }));
    let resolver = resolver()
        .context("dev", [("root".into(), "D:/dev".into())].into())
        .context("prod", [("root".into(), "s3://bucket".into())].into())
        .activate(Some("dev"));

    // Explicitly named, so the active context does not come into it.
    assert_eq!(path_of(&document, &resolver), "s3://bucket/orders.csv");
}

#[test]
fn an_unqualified_reference_reads_the_active_context() {
    let document = doc(json!({ "path": "${root}/orders.csv" }));
    let resolver = resolver()
        .context("dev", [("root".into(), "D:/dev".into())].into())
        .context("prod", [("root".into(), "s3://bucket".into())].into());

    assert_eq!(
        path_of(&document, &resolver.clone().activate(Some("dev"))),
        "D:/dev/orders.csv"
    );
    assert_eq!(
        path_of(&document, &resolver.activate(Some("prod"))),
        "s3://bucket/orders.csv"
    );
}

#[test]
fn an_environment_reference_reads_the_environment() {
    // PATH rather than a variable this test sets: mutating the environment from
    // one test races every other test in the process.
    let document = doc(json!({ "path": "${ENV:PATH}" }));

    assert!(!path_of(&document, &resolver()).is_empty());
}

#[test]
fn interpolation_reaches_into_lists_and_nested_objects() {
    let document = doc(json!({
        "path": "in.csv",
        "columns": ["${first}", "plain"],
        "nested": { "deep": "${first}" }
    }));

    let resolved = resolve(&document, &resolver().bind("first", "order_id"))
        .expect("resolves")
        .document;
    let properties = resolved.nodes[0].data.properties.as_ref().unwrap();

    assert_eq!(properties["columns"][0], "order_id");
    assert_eq!(properties["columns"][1], "plain");
    assert_eq!(properties["nested"]["deep"], "order_id");
}

#[test]
fn non_string_values_are_left_exactly_as_they_were() {
    let document = doc(json!({ "path": "in.csv", "header": true, "skip": 3 }));

    let resolved = resolve(&document, &resolver()).expect("resolves").document;
    let properties = resolved.nodes[0].data.properties.as_ref().unwrap();

    assert_eq!(properties["header"], json!(true));
    assert_eq!(properties["skip"], json!(3));
}

#[test]
fn only_properties_are_interpolated() {
    // A label is what a person reads on the canvas, not configuration.
    let mut document = doc(json!({ "path": "${root}/x.csv" }));
    document.nodes[0].data.label = "${root}".to_string();

    let resolved = resolve(&document, &resolver().bind("root", "D:/data"))
        .expect("resolves")
        .document;

    assert_eq!(resolved.nodes[0].data.label, "${root}");
}

// ---------------------------------------------------------------------------
// The awkward strings
// ---------------------------------------------------------------------------

#[test]
fn a_doubled_dollar_is_a_literal_dollar() {
    let document = doc(json!({ "path": "$${root}/orders.csv" }));

    assert_eq!(
        path_of(&document, &resolver().bind("root", "unused")),
        "${root}/orders.csv"
    );
}

#[test]
fn a_lone_dollar_is_left_alone() {
    // `$1` in a predicate, a currency symbol in a literal.
    let document = doc(json!({ "path": "cost $5 and $1 more" }));

    assert_eq!(path_of(&document, &resolver()), "cost $5 and $1 more");
}

#[test]
fn a_substituted_value_is_not_itself_expanded() {
    // Single-pass, deliberately: it rules out runaway expansion, and stops a
    // value supplied on the command line from reaching the environment.
    let document = doc(json!({ "path": "${outer}" }));
    let resolver = resolver()
        .bind("outer", "${ENV:PATH}")
        .bind("ENV:PATH", "should never be consulted");

    assert_eq!(path_of(&document, &resolver), "${ENV:PATH}");
}

#[test]
fn multi_byte_text_survives_interpolation() {
    let document = doc(json!({ "path": "données/${root}/café.csv" }));

    assert_eq!(
        path_of(&document, &resolver().bind("root", "日本")),
        "données/日本/café.csv"
    );
}

#[test]
fn surrounding_whitespace_in_a_reference_is_ignored() {
    let document = doc(json!({ "path": "${ root }/orders.csv" }));

    assert_eq!(
        path_of(&document, &resolver().bind("root", "D:/data")),
        "D:/data/orders.csv"
    );
}

// ---------------------------------------------------------------------------
// Precedence
// ---------------------------------------------------------------------------

#[test]
fn an_explicit_binding_beats_the_context_the_default_and_the_built_in() {
    let mut document = doc(json!({ "path": "${root}" }));
    document.parameters.insert(
        "root".to_string(),
        ParameterSpec {
            param_type: Some("string".into()),
            required: None,
            default: Some(json!("from-default")),
            description: None,
            extra: Default::default(),
        },
    );

    let resolver = resolver()
        .bind("root", "from-command-line")
        .context("dev", [("root".into(), "from-context".into())].into())
        .activate(Some("dev"));

    assert_eq!(path_of(&document, &resolver), "from-command-line");
}

#[test]
fn the_context_beats_the_declared_default() {
    let mut document = doc(json!({ "path": "${root}" }));
    document.parameters.insert(
        "root".to_string(),
        ParameterSpec {
            param_type: None,
            required: None,
            default: Some(json!("from-default")),
            description: None,
            extra: Default::default(),
        },
    );

    let resolver = resolver()
        .context("dev", [("root".into(), "from-context".into())].into())
        .activate(Some("dev"));

    assert_eq!(path_of(&document, &resolver), "from-context");
}

#[test]
fn a_declared_parameter_shadows_a_built_in_and_says_so() {
    let mut document = doc(json!({ "path": "${workspace}" }));
    document.parameters.insert(
        "workspace".to_string(),
        ParameterSpec {
            param_type: None,
            required: None,
            default: Some(json!("elsewhere")),
            description: None,
            extra: Default::default(),
        },
    );

    let resolved = resolve(&document, &resolver()).expect("resolves");

    assert_eq!(
        resolved.warnings,
        [ParamWarning::ShadowsBuiltIn {
            name: "workspace".to_string()
        }]
    );
    assert_eq!(path_of(&document, &resolver()), "elsewhere");
}

#[test]
fn a_non_string_default_is_substituted_as_its_text() {
    let mut document = doc(json!({ "path": "rows-${limit}" }));
    document.parameters.insert(
        "limit".to_string(),
        ParameterSpec {
            param_type: Some("integer".into()),
            required: None,
            default: Some(json!(500)),
            description: None,
            extra: Default::default(),
        },
    );

    assert_eq!(path_of(&document, &resolver()), "rows-500");
}

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

fn err(document: &PipelineDoc, resolver: &Resolver) -> ParamError {
    resolve(document, resolver).expect_err("expected this to fail")
}

#[test]
fn an_unresolved_reference_names_the_node_and_the_property() {
    let error = err(&doc(json!({ "path": "${nowhere}" })), &resolver());

    assert!(
        matches!(&error, ParamError::Unresolved { node, property, reference, .. }
            if node == "n" && property == "path" && reference == "nowhere"),
        "{error:?}"
    );

    let message = error.to_string();
    assert!(message.contains("node 'n'"), "{message}");
    assert!(message.contains("property 'path'"), "{message}");
    // The built-ins are always available, so the hint is never empty.
    assert!(message.contains("workspace"), "{message}");
    assert_eq!(error.node_id(), Some("n"));
}

#[test]
fn an_unclosed_reference_is_reported_rather_than_passed_through() {
    let error = err(&doc(json!({ "path": "${root/orders.csv" })), &resolver());

    assert!(
        matches!(error, ParamError::Unterminated { .. }),
        "{error:?}"
    );
}

#[test]
fn an_empty_reference_is_rejected() {
    let error = err(&doc(json!({ "path": "${}" })), &resolver());

    assert!(
        matches!(error, ParamError::EmptyReference { .. }),
        "{error:?}"
    );
}

#[test]
fn a_required_parameter_with_no_value_fails_by_name() {
    let mut document = doc(json!({ "path": "in.csv" }));
    document.parameters.insert(
        "since".to_string(),
        ParameterSpec {
            param_type: None,
            required: Some(true),
            default: None,
            description: None,
            extra: Default::default(),
        },
    );

    let error = err(&document, &resolver());

    assert_eq!(
        error,
        ParamError::MissingRequired {
            name: "since".to_string()
        }
    );
    assert!(error.to_string().contains("'since'"), "{error}");
}

#[test]
fn a_required_parameter_is_satisfied_by_its_default() {
    // Unlike a component property, where required-plus-default is forbidden: a
    // required parameter also tells the canvas to prompt, which is meaningful
    // even when there is something to prompt with.
    let mut document = doc(json!({ "path": "${since}" }));
    document.parameters.insert(
        "since".to_string(),
        ParameterSpec {
            param_type: None,
            required: Some(true),
            default: Some(json!("2026-01-01")),
            description: None,
            extra: Default::default(),
        },
    );

    assert_eq!(path_of(&document, &resolver()), "2026-01-01");
}

#[test]
fn an_empty_value_for_a_required_parameter_is_rejected() {
    // `--param since=` is a slip, not a deliberate empty string. A required
    // component property is refused the same way.
    let mut document = doc(json!({ "path": "${since}" }));
    document.parameters.insert(
        "since".to_string(),
        ParameterSpec {
            param_type: None,
            required: Some(true),
            default: Some(json!("2026-01-01")),
            description: None,
            extra: Default::default(),
        },
    );

    let error = err(&document, &resolver().bind("since", "   "));

    assert_eq!(
        error,
        ParamError::EmptyRequired {
            name: "since".to_string()
        }
    );
}

#[test]
fn an_empty_value_for_an_optional_parameter_is_allowed() {
    // Only `required` makes emptiness suspicious; an optional prefix or suffix
    // is a perfectly ordinary thing to set to nothing.
    let mut document = doc(json!({ "path": "orders${suffix}.csv" }));
    document.parameters.insert(
        "suffix".to_string(),
        ParameterSpec {
            param_type: None,
            required: None,
            default: None,
            description: None,
            extra: Default::default(),
        },
    );

    assert_eq!(
        path_of(&document, &resolver().bind("suffix", "")),
        "orders.csv"
    );
}

#[test]
fn a_value_of_the_wrong_declared_type_is_rejected() {
    let mut document = doc(json!({ "path": "in.csv" }));
    document.parameters.insert(
        "limit".to_string(),
        ParameterSpec {
            param_type: Some("integer".into()),
            required: None,
            default: None,
            description: None,
            extra: Default::default(),
        },
    );

    let error = err(&document, &resolver().bind("limit", "ten"));

    assert!(
        matches!(&error, ParamError::WrongType { name, .. } if name == "limit"),
        "{error:?}"
    );
    assert!(error.to_string().contains("not an integer"), "{error}");
}

#[test]
fn every_declared_type_accepts_what_it_should_and_refuses_what_it_should_not() {
    let cases = [
        ("string", "anything at all", true),
        ("integer", "42", true),
        ("integer", "4.2", false),
        ("number", "4.2", true),
        ("number", "x", false),
        ("boolean", "true", true),
        ("boolean", "yes", false),
        ("date", "2026-01-01", true),
        ("date", "2026-01-01 10:00:00", true),
        ("date", "last tuesday", false),
        // An unrecognised type is carried, not checked: it is what a document
        // from a newer version looks like.
        ("colour", "chartreuse", true),
    ];

    for (declared, value, acceptable) in cases {
        let mut document = doc(json!({ "path": "in.csv" }));
        document.parameters.insert(
            "p".to_string(),
            ParameterSpec {
                param_type: Some(declared.into()),
                required: None,
                default: None,
                description: None,
                extra: Default::default(),
            },
        );

        let outcome = resolve(&document, &resolver().bind("p", value));

        assert_eq!(
            outcome.is_ok(),
            acceptable,
            "{declared} should {} '{value}'",
            if acceptable { "accept" } else { "refuse" }
        );
    }
}

#[test]
fn a_reference_to_an_undefined_context_lists_the_ones_there_are() {
    let document = doc(json!({ "path": "${staging.root}" }));
    let resolver = resolver()
        .context("dev", BTreeMap::new())
        .context("prod", BTreeMap::new());

    let error = err(&document, &resolver);
    let message = error.to_string();

    assert!(
        matches!(error, ParamError::UnknownContext { .. }),
        "{error:?}"
    );
    assert!(
        message.contains("dev") && message.contains("prod"),
        "{message}"
    );
}

#[test]
fn a_missing_variable_in_a_real_context_is_not_reported_as_a_missing_context() {
    let document = doc(json!({ "path": "${dev.nowhere}" }));
    let resolver = resolver().context("dev", [("root".into(), "D:/dev".into())].into());

    let error = err(&document, &resolver);

    assert!(matches!(error, ParamError::Unresolved { .. }), "{error:?}");
    assert!(error.to_string().contains("root"), "{error}");
}

#[test]
fn a_missing_environment_variable_names_the_variable() {
    let document = doc(json!({ "path": "${ENV:ETL_DEFINITELY_NOT_SET_ANYWHERE}" }));

    let error = err(&document, &resolver());

    assert!(
        matches!(&error, ParamError::MissingEnvironment { name, .. }
            if name == "ETL_DEFINITELY_NOT_SET_ANYWHERE"),
        "{error:?}"
    );
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

#[test]
fn what_was_substituted_is_reported() {
    let document = doc(json!({ "path": "${root}/${date}/orders.csv" }));
    let resolved = resolve(&document, &resolver().bind("root", "D:/data")).expect("resolves");

    assert_eq!(
        resolved.used,
        [
            ("date".to_string(), "2026-09-15".to_string()),
            ("root".to_string(), "D:/data".to_string()),
        ]
        .into()
    );
}

#[test]
fn a_value_supplied_for_a_parameter_the_document_does_not_declare_warns() {
    let resolved = resolve(
        &doc(json!({ "path": "in.csv" })),
        &resolver().bind("typo", "x"),
    )
    .expect("resolves");

    assert_eq!(
        resolved.warnings,
        [ParamWarning::Undeclared {
            name: "typo".to_string()
        }]
    );
}

#[test]
fn a_document_with_nothing_to_substitute_comes_back_unchanged() {
    let document = doc(json!({ "path": "samples/data/orders.csv", "header": true }));
    let resolved = resolve(&document, &resolver()).expect("resolves");

    assert_eq!(resolved.document, document);
    assert!(resolved.used.is_empty());
    assert!(resolved.warnings.is_empty());
}

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

/// A workspace with a key and the given secrets already in it.
fn store_with(name: &str, secrets: &[(&str, &str)]) -> etl_secrets::SecretStore {
    let root = std::env::temp_dir()
        .join("etl-param-secret-tests")
        .join(name);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("workspace");

    let mut store = etl_secrets::SecretStore::open(&root).expect("opens");
    for (key, value) in secrets {
        store.set(key, value, None).expect("encrypts");
    }

    store
}

#[test]
fn a_secret_reference_is_substituted() {
    let document = doc(json!({ "path": "host=db password=${SECRET:pg}" }));
    let resolver = resolver().secrets(store_with("substituted", &[("pg", "hunter2")]));

    assert_eq!(path_of(&document, &resolver), "host=db password=hunter2");
}

#[test]
fn a_secrets_value_is_masked_in_what_was_used() {
    // `used` is printed by `etl validate` and carried in the run report, so it
    // must never hold the plaintext.
    let document = doc(json!({ "path": "${SECRET:pg}" }));
    let resolver = resolver().secrets(store_with("masked_used", &[("pg", "hunter2")]));

    let resolved = resolve(&document, &resolver).expect("resolves");

    assert_eq!(resolved.used["SECRET:pg"], REDACTED);
    assert!(
        !format!("{:?}", resolved.used).contains("hunter2"),
        "{:?}",
        resolved.used
    );
}

#[test]
fn redact_masks_the_value_wherever_it_appears() {
    let document = doc(json!({ "path": "${SECRET:pg}" }));
    let resolver = resolver().secrets(store_with("redacts", &[("pg", "hunter2")]));
    let resolved = resolve(&document, &resolver).expect("resolves");

    assert!(resolved.uses_secrets());
    assert_eq!(resolved.secret_values(), ["hunter2"]);
    assert_eq!(
        resolved.redact("ATTACH 'password=hunter2' AS db; -- hunter2"),
        format!("ATTACH 'password={REDACTED}' AS db; -- {REDACTED}")
    );
}

#[test]
fn a_pipeline_with_no_secrets_has_nothing_to_redact() {
    let resolved = resolve(&doc(json!({ "path": "in.csv" })), &resolver()).expect("resolves");

    assert!(!resolved.uses_secrets());
    assert!(resolved.secret_values().is_empty());
    assert_eq!(resolved.redact("anything at all"), "anything at all");
}

#[test]
fn an_empty_secret_does_not_mask_the_whole_string() {
    // A naive `replace("", ...)` inserts the mask between every character.
    let document = doc(json!({ "path": "x${SECRET:blank}y" }));
    let resolver = resolver().secrets(store_with("empty_secret", &[("blank", "")]));
    let resolved = resolve(&document, &resolver).expect("resolves");

    assert_eq!(resolved.redact("untouched"), "untouched");
}

#[test]
fn a_secret_reference_without_a_store_says_so() {
    // Rather than "nothing provides SECRET:pg", which would send someone
    // looking for a parameter that was never the problem.
    let error = err(&doc(json!({ "path": "${SECRET:pg}" })), &resolver());

    assert!(
        matches!(&error, ParamError::NoSecretStore { name, .. } if name == "pg"),
        "{error:?}"
    );
}

#[test]
fn a_secret_that_is_not_in_the_store_lists_the_ones_that_are() {
    let resolver = resolver().secrets(store_with(
        "missing_secret",
        &[("pg_password", "x"), ("api_token", "y")],
    ));

    let error = err(&doc(json!({ "path": "${SECRET:typo}" })), &resolver);
    let message = error.to_string();

    assert!(
        matches!(error, ParamError::MissingSecret { .. }),
        "{error:?}"
    );
    assert!(
        message.contains("api_token") && message.contains("pg_password"),
        "{message}"
    );
}

#[test]
fn a_parameter_cannot_shadow_the_secret_prefix() {
    // `SECRET:` is checked before the ordinary lookup, so binding a parameter
    // of that name cannot make the resolver read something else.
    let document = doc(json!({ "path": "${SECRET:pg}" }));
    let resolver = resolver()
        .bind("SECRET:pg", "not the secret")
        .secrets(store_with("no_shadowing", &[("pg", "the real one")]));

    assert_eq!(path_of(&document, &resolver), "the real one");
}

#[test]
fn a_secret_is_not_expanded_a_second_time() {
    // Substitution is single-pass everywhere, and a secret whose value happens
    // to contain `${...}` must not become a way to read something else.
    let document = doc(json!({ "path": "${SECRET:pg}" }));
    let resolver = resolver().secrets(store_with("single_pass_secret", &[("pg", "${ENV:PATH}")]));

    assert_eq!(path_of(&document, &resolver), "${ENV:PATH}");
}

// ---------------------------------------------------------------------------
// The clock
// ---------------------------------------------------------------------------

#[test]
fn days_since_the_epoch_convert_to_the_right_calendar_date() {
    // Dates whose answers are not in doubt, including the leap-year cases the
    // algorithm exists to get right.
    let cases = [
        (0, (1970, 1, 1)),
        (1, (1970, 1, 2)),
        (-1, (1969, 12, 31)),
        (59, (1970, 3, 1)),
        (10_957, (2000, 1, 1)),
        (11_016, (2000, 2, 29)), // 2000 is a leap year: divisible by 400
        (11_017, (2000, 3, 1)),
        (19_723, (2024, 1, 1)),
        (19_782, (2024, 2, 29)), // 2024 is a leap year
        (20_788, (2026, 12, 1)),
    ];

    for (days, expected) in cases {
        assert_eq!(
            civil_from_days(days),
            (expected.0, expected.1, expected.2),
            "day {days}"
        );
    }
}

#[test]
fn a_non_leap_century_has_no_twenty_ninth_of_february() {
    // 1900 was not a leap year: divisible by 100 but not by 400. So the day
    // after 1900-02-28 must be March, with no 29th in between.
    assert_eq!(civil_from_days(-25_509), (1900, 2, 28));
    assert_eq!(civil_from_days(-25_508), (1900, 3, 1));

    // The contrast: 2000 was divisible by 400, so it does have a 29th.
    assert_eq!(civil_from_days(11_015), (2000, 2, 28));
    assert_eq!(civil_from_days(11_016), (2000, 2, 29));
}

#[test]
fn todays_date_is_shaped_like_a_date() {
    let today = today_utc();

    assert!(looks_like_a_date(&today), "{today}");
    assert_eq!(today.len(), 10, "{today}");
}
