//! Signing in to Google Cloud, without Google: RS256 against RFC 7515's own
//! example, byte for byte, and the token exchange against the local fixture,
//! which records what was sent and answers as told.

use super::*;
use crate::fixture::{self, Fixture};
use ring::signature::{UnparsedPublicKey, RSA_PKCS1_2048_8192_SHA256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};

// ---------------------------------------------------------------------------
// The RFC's key, in the forms key files hold
// ---------------------------------------------------------------------------

fn rfc() -> JsonValue {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rfc7515/a2.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn from_base64url(text: &str) -> Vec<u8> {
    let mut standard = text.replace('-', "+").replace('_', "/");
    while !standard.len().is_multiple_of(4) {
        standard.push('=');
    }
    base64_decode(&standard).expect("base64url")
}

/// DER: a tag, a length, and the content.
fn der(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    match content.len() {
        n if n < 0x80 => out.push(n as u8),
        n if n < 0x100 => out.extend([0x81, n as u8]),
        n => out.extend([0x82, (n >> 8) as u8, n as u8]),
    }
    out.extend_from_slice(content);
    out
}

/// A DER INTEGER from big-endian bytes, which are never negative here.
fn integer(bytes: &[u8]) -> Vec<u8> {
    let mut content = bytes.to_vec();
    if content.first().is_some_and(|byte| byte & 0x80 != 0) {
        content.insert(0, 0);
    }
    der(0x02, &content)
}

/// PKCS#1's RSAPrivateKey, from the JWK's parts.
fn pkcs1(jwk: &JsonValue) -> Vec<u8> {
    let mut content = integer(&[0]);
    for part in ["n", "e", "d", "p", "q", "dp", "dq", "qi"] {
        content.extend(integer(&from_base64url(jwk[part].as_str().unwrap())));
    }
    der(0x30, &content)
}

/// PKCS#8's PrivateKeyInfo around it, as Google's key files hold.
fn pkcs8(jwk: &JsonValue) -> Vec<u8> {
    let rsa_encryption = der(
        0x06,
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01],
    );
    let algorithm = der(0x30, &[rsa_encryption, vec![0x05, 0x00]].concat());
    let content = [integer(&[0]), algorithm, der(0x04, &pkcs1(jwk))].concat();
    der(0x30, &content)
}

fn pem(label: &str, der: &[u8]) -> String {
    let body = base64_bytes(der);
    let lines: Vec<&str> = body
        .as_bytes()
        .chunks(64)
        .map(|line| std::str::from_utf8(line).unwrap())
        .collect();
    format!(
        "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
        lines.join("\n")
    )
}

// ---------------------------------------------------------------------------
// RS256
// ---------------------------------------------------------------------------

#[test]
fn rs256_signs_rfc_7515s_example_byte_for_byte() {
    let rfc = rfc();
    let key = RsaKeyPair::from_der(&pkcs1(&rfc["jwk"])).expect("the RFC's key");

    let header = base64url(rfc["protected_header"].as_str().unwrap().as_bytes());
    let payload = base64url(rfc["payload"].as_str().unwrap().as_bytes());
    let input = format!("{header}.{payload}");
    assert_eq!(input, rfc["signing_input"].as_str().unwrap());

    let signature = rs256(&key, input.as_bytes()).unwrap();
    let expected: Vec<u8> = rfc["signature"]
        .as_array()
        .unwrap()
        .iter()
        .map(|byte| byte.as_u64().unwrap() as u8)
        .collect();
    assert_eq!(signature, expected);
    assert_eq!(
        format!("{input}.{}", base64url(&signature)),
        rfc["jws"].as_str().unwrap()
    );
}

#[test]
fn a_key_is_read_from_either_pem_form_and_signs_the_same() {
    let rfc = rfc();
    let input = rfc["signing_input"].as_str().unwrap().as_bytes();
    let expected = rs256(&RsaKeyPair::from_der(&pkcs1(&rfc["jwk"])).unwrap(), input).unwrap();

    let from_pkcs8 = rsa_key(&pem("PRIVATE KEY", &pkcs8(&rfc["jwk"]))).unwrap();
    assert_eq!(rs256(&from_pkcs8, input).unwrap(), expected);
    // A key file's private_key has its newlines escaped, and they come back
    // as "\n" once the JSON is read; with CRLF from a Windows editor too.
    let crlf = pem("RSA PRIVATE KEY", &pkcs1(&rfc["jwk"])).replace('\n', "\r\n");
    assert_eq!(rs256(&rsa_key(&crlf).unwrap(), input).unwrap(), expected);

    assert_eq!(
        rsa_key("not a key").unwrap_err(),
        "no PEM private key in it"
    );
    let broken = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----";
    assert!(rsa_key(broken)
        .unwrap_err()
        .starts_with("the key is not a usable RSA key"));
}

// ---------------------------------------------------------------------------
// Key files, and where they are looked for
// ---------------------------------------------------------------------------

/// A directory for one test, removed when it ends.
pub(crate) struct Scratch(pub(crate) PathBuf);

impl Scratch {
    pub(crate) fn new(test: &str) -> Self {
        static COUNT: AtomicU32 = AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "etl-gcp-{test}-{}-{}",
            std::process::id(),
            COUNT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Scratch(path)
    }

    pub(crate) fn write(&self, name: &str, content: &JsonValue) -> PathBuf {
        let path = self.0.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, content.to_string()).unwrap();
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// RFC 7515's example key as a PKCS#8 PEM, for other connectors' tests.
pub(crate) fn rfc_key_pem() -> String {
    pem("PRIVATE KEY", &pkcs8(&rfc()["jwk"]))
}

pub(crate) fn service_account(token_uri: &str) -> JsonValue {
    json!({
        "type": "service_account",
        "project_id": "etl-test",
        "private_key_id": "k1",
        "private_key": pem("PRIVATE KEY", &pkcs8(&rfc()["jwk"])),
        "client_email": "etl@etl-test.iam.gserviceaccount.com",
        "token_uri": token_uri,
    })
}

fn user(token_uri: &str) -> JsonValue {
    json!({
        "type": "authorized_user",
        "client_id": "client-1.apps.googleusercontent.com",
        "client_secret": "s3cr3t/+=",
        "refresh_token": "1//refresh",
        "token_uri": token_uri,
    })
}

/// Look up credentials with `variables` as the environment and `home` as the
/// home directory, and say where they came from.
fn found(
    properties: JsonValue,
    variables: &[(&str, String)],
    home: &Path,
) -> Result<String, String> {
    let variables: BTreeMap<String, String> = variables
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect();
    let var = move |name: &str| variables.get(name).cloned();
    let sources = Sources {
        var: &var,
        home: Some(home.to_path_buf()),
    };
    credentials(&properties, &sources)
        .map(|credentials| credentials.source)
        .map_err(|error| error.to_string())
}

#[test]
fn credentials_come_from_the_first_place_that_has_them() {
    let scratch = Scratch::new("order");
    let account = scratch.write("account.json", &service_account("http://unused"));
    let person = scratch.write("person.json", &user("http://unused"));
    scratch.write(
        ".config/gcloud/application_default_credentials.json",
        &user("http://unused"),
    );
    let gcloud = scratch
        .0
        .join(".config")
        .join("gcloud")
        .join("application_default_credentials.json");
    let home = &scratch.0;
    let path = |file: &PathBuf| file.display().to_string();

    // The node's own file first.
    assert_eq!(
        found(
            json!({ "credentials_file": path(&account) }),
            &[("GOOGLE_APPLICATION_CREDENTIALS", path(&person))],
            home
        )
        .unwrap(),
        format!(
            "service account etl@etl-test.iam.gserviceaccount.com, from credentials_file {}",
            path(&account)
        )
    );
    // Then the variable.
    assert_eq!(
        found(
            json!({}),
            &[("GOOGLE_APPLICATION_CREDENTIALS", path(&person))],
            home
        )
        .unwrap(),
        format!(
            "gcloud's user login, from GOOGLE_APPLICATION_CREDENTIALS {}",
            path(&person)
        )
    );
    // Then gcloud's file, under the home directory...
    assert_eq!(
        found(json!({}), &[], home).unwrap(),
        format!("gcloud's user login, from {}", path(&gcloud))
    );
    // ...or where CLOUDSDK_CONFIG says gcloud keeps it.
    scratch.write(
        "elsewhere/application_default_credentials.json",
        &service_account("http://unused"),
    );
    let elsewhere = scratch
        .0
        .join("elsewhere")
        .join("application_default_credentials.json");
    assert!(found(
        json!({}),
        &[("CLOUDSDK_CONFIG", path(&scratch.0.join("elsewhere")))],
        home
    )
    .unwrap()
    .ends_with(&path(&elsewhere)));

    // Nowhere: every place looked is named.
    let empty = Scratch::new("none");
    let error = found(json!({}), &[], &empty.0).unwrap_err();
    assert!(
        error.starts_with("property 'credentials_file': no Google credentials found"),
        "{error}"
    );
    assert!(error.contains("GOOGLE_APPLICATION_CREDENTIALS"), "{error}");
    assert!(
        error.contains("application_default_credentials.json"),
        "{error}"
    );
}

#[test]
fn a_key_file_that_will_not_do_says_why() {
    let scratch = Scratch::new("bad");
    let home = &scratch.0;
    let error = |content: JsonValue| {
        let file = scratch.write("key.json", &content);
        found(
            json!({ "credentials_file": file.display().to_string() }),
            &[],
            home,
        )
        .unwrap_err()
    };

    let external = error(json!({ "type": "external_account" }));
    assert!(
        external.contains("is a 'external_account' credential"),
        "{external}"
    );
    let mut no_email = service_account("http://unused");
    no_email.as_object_mut().unwrap().remove("client_email");
    assert!(error(no_email).ends_with("has no \"client_email\""));
    let mut bad_key = service_account("http://unused");
    bad_key["private_key"] = json!("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----");
    assert!(error(bad_key).contains("private_key: the key is not a usable RSA key"));

    let missing = found(
        json!({ "credentials_file": "C:/no/such/key.json" }),
        &[],
        home,
    )
    .unwrap_err();
    assert!(
        missing.starts_with("property 'credentials_file': cannot read"),
        "{missing}"
    );
}

// ---------------------------------------------------------------------------
// The token exchange
// ---------------------------------------------------------------------------

/// A token endpoint handing out `token-<n>`, each good for `lasts` seconds.
pub(crate) fn token_server(lasts: u64) -> Fixture {
    fixture::serve(move |index, _| {
        fixture::ok(json!({
            "access_token": format!("token-{index}"),
            "expires_in": lasts,
            "token_type": "Bearer",
        }))
    })
}

pub(crate) fn tokens_from(file: &Path) -> Tokens {
    let sources = Sources {
        var: &|_| None,
        home: None,
    };
    let credentials = credentials(
        &json!({ "credentials_file": file.display().to_string() }),
        &sources,
    )
    .unwrap();
    Tokens::new(
        credentials,
        "https://example.com/scope",
        Duration::from_secs(5),
        1,
    )
}

/// A form body as name to value, decoded.
fn form(body: &str) -> BTreeMap<String, String> {
    let decode = |text: &str| {
        let bytes = text.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' {
                out.push(u8::from_str_radix(&text[i + 1..i + 3], 16).unwrap());
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(out).unwrap()
    };
    body.split('&')
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap();
            (decode(name), decode(value))
        })
        .collect()
}

#[test]
fn a_service_account_trades_a_signed_jwt_for_a_token_and_keeps_it() {
    let server = token_server(3600);
    let scratch = Scratch::new("jwt");
    let file = scratch.write("key.json", &service_account(&server.url("/token")));
    let tokens = tokens_from(&file);

    let before = etl_state::time::now_unix();
    assert_eq!(tokens.authorization().unwrap().unwrap(), "Bearer token-0");
    assert_eq!(
        tokens.clone().authorization().unwrap().unwrap(),
        "Bearer token-0",
        "cached, and shared by a clone"
    );
    let seen = server.seen();
    assert_eq!(seen.len(), 1, "one exchange");
    let request = &seen[0];
    assert_eq!(request.path(), "/token");
    assert_eq!(
        request.header("Content-Type"),
        Some("application/x-www-form-urlencoded")
    );

    let form = form(&request.body);
    assert_eq!(
        form["grant_type"],
        "urn:ietf:params:oauth:grant-type:jwt-bearer"
    );
    let parts: Vec<&str> = form["assertion"].split('.').collect();
    assert_eq!(parts.len(), 3);
    let header: JsonValue = serde_json::from_slice(&from_base64url(parts[0])).unwrap();
    assert_eq!(header, json!({ "alg": "RS256", "typ": "JWT", "kid": "k1" }));
    let claims: JsonValue = serde_json::from_slice(&from_base64url(parts[1])).unwrap();
    assert_eq!(claims["iss"], "etl@etl-test.iam.gserviceaccount.com");
    assert_eq!(claims["scope"], "https://example.com/scope");
    assert_eq!(claims["aud"], server.url("/token"));
    let issued = claims["iat"].as_i64().unwrap();
    assert!((before..=before + 5).contains(&issued), "{claims}");
    assert_eq!(claims["exp"].as_i64().unwrap(), issued + 3600);

    // Signed by the key, as Google will check it: with the public half.
    let key = rsa_key(&pem("PRIVATE KEY", &pkcs8(&rfc()["jwk"]))).unwrap();
    UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, key.public().as_ref())
        .verify(
            format!("{}.{}", parts[0], parts[1]).as_bytes(),
            &from_base64url(parts[2]),
        )
        .expect("a valid RS256 signature");
}

#[test]
fn a_token_close_to_expiring_is_fetched_again() {
    // Sixty seconds is inside the five-minute margin: every request asks anew.
    let server = token_server(60);
    let scratch = Scratch::new("expiry");
    let file = scratch.write("key.json", &service_account(&server.url("/token")));
    let tokens = tokens_from(&file);
    assert_eq!(tokens.authorization().unwrap().unwrap(), "Bearer token-0");
    assert_eq!(tokens.authorization().unwrap().unwrap(), "Bearer token-1");
    assert_eq!(server.seen().len(), 2);
}

#[test]
fn a_person_trades_their_refresh_token() {
    let server = token_server(3600);
    let scratch = Scratch::new("user");
    let file = scratch.write("user.json", &user(&server.url("/token")));
    assert_eq!(
        tokens_from(&file).authorization().unwrap().unwrap(),
        "Bearer token-0"
    );
    let form = form(&server.seen()[0].body);
    assert_eq!(form["grant_type"], "refresh_token");
    assert_eq!(form["client_id"], "client-1.apps.googleusercontent.com");
    assert_eq!(form["client_secret"], "s3cr3t/+=", "encoded, and back");
    assert_eq!(form["refresh_token"], "1//refresh");
    assert!(
        server.seen()[0]
            .body
            .contains("client_secret=s3cr3t%2F%2B%3D"),
        "{}",
        server.seen()[0].body
    );
}

#[test]
fn a_refused_sign_in_names_the_credentials_and_is_not_retried() {
    let server = fixture::serve(|_, _| {
        fixture::status(
            400,
            r#"{"error":"invalid_grant","error_description":"Invalid JWT Signature."}"#,
        )
    });
    let scratch = Scratch::new("refused");
    let file = scratch.write("key.json", &service_account(&server.url("/token")));
    let error = tokens_from(&file).authorization().unwrap_err().to_string();
    assert!(
        error.starts_with(
            "Google sign-in with service account etl@etl-test.iam.gserviceaccount.com"
        ),
        "{error}"
    );
    assert!(error.contains("HTTP 400"), "{error}");
    assert!(error.contains("Invalid JWT Signature."), "{error}");
    assert_eq!(server.seen().len(), 1);
}

#[test]
fn an_emulator_is_sent_no_token() {
    let tokens = Tokens::new(
        Credentials::nobody(),
        "https://example.com/scope",
        Duration::from_secs(1),
        0,
    );
    assert_eq!(tokens.authorization().unwrap(), None);
}
