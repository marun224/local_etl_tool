//! Signing, proved by AWS's own test suite; the credential sources and their
//! order, with stand-ins for the environment and the home directory.

use super::*;
use serde_json::json;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// AWS's SigV4 test suite
// ---------------------------------------------------------------------------

fn suite() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sigv4")
}

/// A raw request as the suite writes it: request line, headers (a line
/// starting with whitespace continues the header before it), blank line, body.
fn parse(raw: &str) -> (String, String, Vec<(String, String)>, Vec<u8>) {
    let (head, body) = match raw.split_once("\n\n") {
        Some((head, body)) => (head, body.as_bytes().to_vec()),
        None => (raw.trim_end_matches('\n'), Vec::new()),
    };
    let mut lines = head.lines();
    let request_line = lines.next().expect("a request line");
    let method = request_line.split(' ').next().unwrap().to_string();
    let target = request_line
        .strip_prefix(&format!("{method} "))
        .unwrap()
        .rsplit_once(" HTTP/")
        .unwrap()
        .0
        .to_string();

    let mut headers: Vec<(String, String)> = Vec::new();
    for line in lines {
        if line.starts_with([' ', '\t']) {
            // A folded continuation: one more piece of the last header's
            // value, joined with a space as the suite's expected output shows.
            let last = headers.last_mut().expect("a header to continue");
            last.1 = format!("{} {}", last.1, line.trim());
        } else if let Some((name, value)) = line.split_once(':') {
            headers.push((name.to_string(), value.to_string()));
        }
    }
    (method, target, headers, body)
}

#[test]
fn every_case_in_awss_sigv4_suite_signs_byte_for_byte() {
    let mut cases: Vec<PathBuf> = std::fs::read_dir(suite())
        .expect("the suite is vendored")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.join("context.json").is_file())
        .collect();
    cases.sort();
    assert_eq!(cases.len(), 38, "the whole suite, not part of it");

    let mut failures = Vec::new();
    for case in &cases {
        let name = case.file_name().unwrap().to_string_lossy().to_string();
        let read = |file: &str| std::fs::read_to_string(case.join(file)).unwrap();
        let context: JsonValue = serde_json::from_str(&read("context.json")).unwrap();
        let (method, target, headers, body) = parse(&read("request.txt"));

        let credentials = Credentials {
            access_key_id: context["credentials"]["access_key_id"]
                .as_str()
                .unwrap()
                .to_string(),
            secret_access_key: context["credentials"]["secret_access_key"]
                .as_str()
                .unwrap()
                .to_string(),
            session_token: context["credentials"]["token"].as_str().map(str::to_string),
            source: "the suite".to_string(),
        };
        let timestamp = context["timestamp"].as_str().unwrap(); // 2015-08-30T12:36:00Z
        let amz_date = timestamp.replace(['-', ':'], "");
        let signer = Signer {
            credentials: &credentials,
            region: context["region"].as_str().unwrap(),
            service: context["service"].as_str().unwrap(),
            amz_date: &amz_date,
            normalize: context["normalize"].as_bool().unwrap(),
            sign_body: context["sign_body"].as_bool().unwrap(),
            omit_session_token: context["omit_session_token"].as_bool().unwrap_or(false),
        };

        let signed = sign(
            &Unsigned {
                method: &method,
                target: &target,
                headers: &headers,
                body: &body,
            },
            &signer,
        );

        for (stage, got, file) in [
            (
                "canonical request",
                &signed.canonical_request,
                "header-canonical-request.txt",
            ),
            (
                "string to sign",
                &signed.string_to_sign,
                "header-string-to-sign.txt",
            ),
            ("signature", &signed.signature, "header-signature.txt"),
        ] {
            let expected = read(file);
            if got.trim_end() != expected.trim_end() {
                failures.push(format!(
                    "{name}: {stage}\n--- expected\n{expected}\n--- got\n{got}\n"
                ));
                break;
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn the_authorization_header_names_the_key_scope_and_signed_headers() {
    let credentials = Credentials {
        access_key_id: "AKIDEXAMPLE".into(),
        secret_access_key: "secret".into(),
        session_token: Some("tok".into()),
        source: "a test".into(),
    };
    let headers = vec![
        (
            "Host".to_string(),
            "kinesis.eu-west-1.amazonaws.com".to_string(),
        ),
        (
            "X-Amz-Target".to_string(),
            "Kinesis_20131202.ListShards".to_string(),
        ),
    ];
    let signed = sign(
        &Unsigned {
            method: "POST",
            target: "/",
            headers: &headers,
            body: b"{}",
        },
        &Signer {
            credentials: &credentials,
            region: "eu-west-1",
            service: "kinesis",
            amz_date: "20260924T101500Z",
            normalize: true,
            sign_body: false,
            omit_session_token: false,
        },
    );

    let authorization = &signed.headers.last().unwrap().1;
    assert!(
        authorization.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260924/eu-west-1/kinesis/aws4_request, \
             SignedHeaders=host;x-amz-date;x-amz-security-token;x-amz-target, Signature="
        ),
        "{authorization}"
    );
    assert!(signed
        .headers
        .iter()
        .any(|(name, value)| name == "x-amz-security-token" && value == "tok"));
    assert!(
        !format!("{credentials:?}").contains("secret"),
        "Debug hides the secret"
    );
}

#[test]
fn now_is_written_the_way_aws_wants_it() {
    let now = amz_date_now();
    assert_eq!(now.len(), 16, "{now}");
    assert_eq!(&now[8..9], "T");
    assert!(now.ends_with('Z'));
}

// ---------------------------------------------------------------------------
// Credentials and region
// ---------------------------------------------------------------------------

/// A home directory of its own, with `.aws/credentials` and `.aws/config`.
fn home(name: &str, credentials: &str, config: &str) -> PathBuf {
    let home = std::env::temp_dir().join(format!("etl-aws-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(home.join(".aws")).unwrap();
    std::fs::write(home.join(".aws/credentials"), credentials).unwrap();
    std::fs::write(home.join(".aws/config"), config).unwrap();
    home
}

fn with_env<'a>(
    vars: &'a [(&'a str, &'a str)],
    home: Option<PathBuf>,
) -> impl Fn() -> Sources<'a> + 'a {
    move || Sources {
        var: Box::leak(Box::new(move |name: &str| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        })),
        home: home.clone(),
    }
}

const FILES_CREDENTIALS: &str = "\
[default]
aws_access_key_id = FROMFILE
aws_secret_access_key = file-secret

[work]
aws_access_key_id=WORKKEY
aws_secret_access_key=work-secret
aws_session_token=work-token
";

const FILES_CONFIG: &str = "\
[default]
region = eu-west-1

[profile work]
region = ap-south-1

[profile config-only]
aws_access_key_id = CONFIGKEY
aws_secret_access_key = config-secret
region = us-west-2
";

#[test]
fn properties_win_then_the_environment_then_the_default_profile() {
    let home = home("order", FILES_CREDENTIALS, FILES_CONFIG);
    let env = [
        ("AWS_ACCESS_KEY_ID", "FROMENV"),
        ("AWS_SECRET_ACCESS_KEY", "env-secret"),
    ];

    let sources = with_env(&env, Some(home.clone()))();
    let from_properties = credentials(
        &json!({ "access_key_id": "FROMPROPS", "secret_access_key": "p" }),
        &sources,
    )
    .unwrap();
    assert_eq!(from_properties.access_key_id, "FROMPROPS");

    let from_env = credentials(&json!({}), &sources).unwrap();
    assert_eq!(from_env.access_key_id, "FROMENV");
    assert_eq!(
        from_env.source,
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
    );

    let files_only = with_env(&[], Some(home))();
    let from_file = credentials(&json!({}), &files_only).unwrap();
    assert_eq!(from_file.access_key_id, "FROMFILE");
    assert!(
        from_file.source.starts_with("profile 'default' in "),
        "{}",
        from_file.source
    );
}

#[test]
fn a_named_profile_comes_from_the_property_or_aws_profile_with_its_token() {
    let home = home("named", FILES_CREDENTIALS, FILES_CONFIG);

    let by_property = credentials(
        &json!({ "profile": "work" }),
        &with_env(&[], Some(home.clone()))(),
    )
    .unwrap();
    assert_eq!(by_property.access_key_id, "WORKKEY");
    assert_eq!(by_property.session_token.as_deref(), Some("work-token"));

    let env = [("AWS_PROFILE", "work")];
    let by_env = credentials(&json!({}), &with_env(&env, Some(home.clone()))()).unwrap();
    assert_eq!(by_env.access_key_id, "WORKKEY");

    // Credentials may live in `config` too, under `[profile name]`.
    let config_only = credentials(
        &json!({ "profile": "config-only" }),
        &with_env(&[], Some(home))(),
    )
    .unwrap();
    assert_eq!(config_only.access_key_id, "CONFIGKEY");
}

#[test]
fn the_region_comes_from_the_property_the_environment_or_the_profile() {
    let home = home("region", FILES_CREDENTIALS, FILES_CONFIG);
    let none = with_env(&[], Some(home.clone()));
    assert_eq!(
        region(&json!({ "region": "sa-east-1" }), &none()).unwrap(),
        "sa-east-1"
    );
    assert_eq!(
        region(&json!({}), &none()).unwrap(),
        "eu-west-1",
        "the default profile's"
    );
    assert_eq!(
        region(&json!({ "profile": "work" }), &none()).unwrap(),
        "ap-south-1"
    );

    let env = [("AWS_DEFAULT_REGION", "ca-central-1")];
    assert_eq!(
        region(&json!({}), &with_env(&env, Some(home))()).unwrap(),
        "ca-central-1"
    );
}

#[test]
fn nothing_found_names_every_place_it_looked() {
    let empty = std::env::temp_dir().join(format!("etl-aws-empty-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&empty);
    let sources = with_env(&[], Some(empty))();

    let error = credentials(&json!({}), &sources).unwrap_err().to_string();
    assert!(error.contains("AWS_ACCESS_KEY_ID"), "{error}");
    assert!(error.contains("profile 'default'"), "{error}");
    assert!(
        error.contains("Roles on EC2, EKS and ECS are not read yet"),
        "{error}"
    );

    let half = credentials(&json!({ "access_key_id": "K" }), &sources)
        .unwrap_err()
        .to_string();
    assert!(half.starts_with("property 'secret_access_key'"), "{half}");

    assert!(region(&json!({}), &sources)
        .unwrap_err()
        .to_string()
        .starts_with("property 'region'"));
}
