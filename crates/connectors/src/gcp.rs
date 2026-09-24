//! Google Cloud: signing in, and where the credentials come from.
//!
//! **Signing in is ours** (Settled decision 64), as AWS's signing is: Google's
//! own Rust crates want a newer Rust and an async runtime, and what signing in
//! takes is small. A service account signs a JWT with its RSA key (RS256,
//! `ring`'s `RsaKeyPair`, already in the tree for TLS) and trades it at the
//! key's `token_uri` for an access token; a person signed in with
//! `gcloud auth application-default login` trades their refresh token for one.
//! Either way the token is sent as `Authorization: Bearer`, **cached** until
//! shortly before it expires, and shared by every thread of one node, a lease
//! keeper included. RS256 is proved by **RFC 7515's own example** (appendix
//! A.2), signed byte for byte, under `tests/fixtures/rfc7515/`; the emulator
//! does not check sign-in, so the exchange is tested against the local fixture.
//!
//! **Where credentials come from**, the first that has them winning:
//! `credentials_file` (a path, so a secret file rather than a secret value),
//! then `GOOGLE_APPLICATION_CREDENTIALS`, then gcloud's application-default
//! file (`%APPDATA%\gcloud` on Windows, `~/.config/gcloud` elsewhere, or
//! `CLOUDSDK_CONFIG`). The metadata server on GCE and GKE is not asked yet, as
//! AWS roles are not.
//!
//! **No sign-in at all** for an endpoint that is plain `http://`: that is an
//! emulator, and a token is never sent where it could be read on the wire.

use crate::aws::Sources;
use crate::http::{base64_bytes, base64_decode, snippet, Client, Extra, Judged, Settings};
use etl_plugin_sdk::ConnectorError;
use ring::rand::SystemRandom;
use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};
use serde_json::{json, Value as JsonValue};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(test)]
pub(crate) mod tests;

/// Where tokens are traded when a key file does not say.
const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";

/// A token is fetched again once it has less than this left, so a request
/// retried after a backoff never carries one that expired on the way.
const REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// How long a service account's JWT asks to be good for: Google's most.
const ASSERTION_SECONDS: i64 = 3600;

// ---------------------------------------------------------------------------
// RS256 and JWTs
// ---------------------------------------------------------------------------

/// Base64url without padding, as JWTs are written (RFC 7515, section 2).
pub(crate) fn base64url(bytes: &[u8]) -> String {
    base64_bytes(bytes)
        .trim_end_matches('=')
        .replace('+', "-")
        .replace('/', "_")
}

/// An RSA key from a PEM block: PKCS#8 (`BEGIN PRIVATE KEY`, what Google's key
/// files hold) or PKCS#1 (`BEGIN RSA PRIVATE KEY`).
pub(crate) fn rsa_key(pem: &str) -> Result<RsaKeyPair, String> {
    let body = |label: &str| {
        let begin = format!("-----BEGIN {label}-----");
        let end = format!("-----END {label}-----");
        let start = pem.find(&begin)? + begin.len();
        let stop = start + pem[start..].find(&end)?;
        Some(pem[start..stop].split_whitespace().collect::<String>())
    };
    let (der, pkcs8) = match (body("PRIVATE KEY"), body("RSA PRIVATE KEY")) {
        (Some(text), _) => (base64_decode(&text), true),
        (None, Some(text)) => (base64_decode(&text), false),
        (None, None) => return Err("no PEM private key in it".to_string()),
    };
    let der = der.ok_or("its PEM block is not valid base64")?;
    let key = if pkcs8 {
        RsaKeyPair::from_pkcs8(&der)
    } else {
        RsaKeyPair::from_der(&der)
    };
    key.map_err(|rejected| format!("the key is not a usable RSA key ({rejected})"))
}

/// RSASSA-PKCS1-v1_5 with SHA-256 over `input`: JWS's `RS256`. The same
/// input and key always give the same signature, which is what lets RFC
/// 7515's example check it byte for byte.
pub(crate) fn rs256(key: &RsaKeyPair, input: &[u8]) -> Result<Vec<u8>, String> {
    let mut signature = vec![0; key.public().modulus_len()];
    key.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        input,
        &mut signature,
    )
    .map_err(|_| "RS256 signing failed".to_string())?;
    Ok(signature)
}

/// `header.claims.signature`, each part base64url.
pub(crate) fn jwt(
    key: &RsaKeyPair,
    header: &JsonValue,
    claims: &JsonValue,
) -> Result<String, String> {
    let input = format!(
        "{}.{}",
        base64url(header.to_string().as_bytes()),
        base64url(claims.to_string().as_bytes())
    );
    let signature = rs256(key, input.as_bytes())?;
    Ok(format!("{input}.{}", base64url(&signature)))
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

/// How a node signs in.
pub(crate) enum Login {
    /// An emulator: nothing is sent.
    Nobody,
    ServiceAccount {
        email: String,
        key_id: Option<String>,
        key: Arc<RsaKeyPair>,
        token_uri: String,
    },
    /// gcloud's application-default login: a person's refresh token.
    User {
        client_id: String,
        client_secret: String,
        refresh_token: String,
        token_uri: String,
    },
}

/// A way to sign in, and where it came from, for the report. Never the secret.
pub(crate) struct Credentials {
    pub(crate) login: Login,
    pub(crate) source: String,
}

impl Credentials {
    pub(crate) fn nobody() -> Self {
        Credentials {
            login: Login::Nobody,
            source: "no sign-in (an emulator over plain http)".to_string(),
        }
    }
}

/// gcloud's application-default file, where it would be.
fn gcloud_file(sources: &Sources) -> Option<PathBuf> {
    let directory = sources
        .get("CLOUDSDK_CONFIG")
        .map(PathBuf::from)
        .or_else(|| {
            sources
                .get("APPDATA")
                .map(|app| Path::new(&app).join("gcloud"))
        })
        .or_else(|| {
            sources
                .home
                .as_ref()
                .map(|home| home.join(".config").join("gcloud"))
        })?;
    Some(directory.join("application_default_credentials.json"))
}

/// The credentials for a node, from the first place that has them.
pub(crate) fn credentials(
    properties: &JsonValue,
    sources: &Sources,
) -> Result<Credentials, ConnectorError> {
    if let Some(file) = crate::http::text(properties, "credentials_file") {
        let file = file.trim();
        return from_file(Path::new(file), &format!("credentials_file {file}"))
            .map_err(|error| ConnectorError::property("credentials_file", error));
    }
    if let Some(file) = sources.get("GOOGLE_APPLICATION_CREDENTIALS") {
        return from_file(
            Path::new(&file),
            &format!("GOOGLE_APPLICATION_CREDENTIALS {file}"),
        )
        .map_err(|error| {
            ConnectorError::property(
                "credentials_file",
                format!("GOOGLE_APPLICATION_CREDENTIALS names a file that will not do: {error}"),
            )
        });
    }
    let gcloud = gcloud_file(sources);
    if let Some(file) = gcloud.as_ref().filter(|file| file.is_file()) {
        return from_file(file, &file.display().to_string())
            .map_err(|error| ConnectorError::property("credentials_file", error));
    }
    Err(ConnectorError::property(
        "credentials_file",
        format!(
            "no Google credentials found. Looked in: the node's credentials_file; \
             GOOGLE_APPLICATION_CREDENTIALS; gcloud's application-default login{}. Run `gcloud \
             auth application-default login`, or give a service account's key file. The metadata \
             server on GCE and GKE is not read yet",
            gcloud.map_or(String::new(), |file| format!(" ({})", file.display()))
        ),
    ))
}

/// A key file, by its `type`.
fn from_file(file: &Path, source: &str) -> Result<Credentials, String> {
    let text = std::fs::read_to_string(file)
        .map_err(|error| format!("cannot read {}: {error}", file.display()))?;
    let document: JsonValue = serde_json::from_str(text.trim_start_matches('\u{feff}'))
        .map_err(|error| format!("{} is not JSON: {error}", file.display()))?;
    let field = |name: &str| {
        document[name]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("{} has no \"{name}\"", file.display()))
    };
    let token_uri = document["token_uri"]
        .as_str()
        .unwrap_or(DEFAULT_TOKEN_URI)
        .to_string();

    let login = match document["type"].as_str() {
        Some("service_account") => {
            let email = field("client_email")?;
            let key = rsa_key(&field("private_key")?)
                .map_err(|error| format!("{}: private_key: {error}", file.display()))?;
            Login::ServiceAccount {
                key_id: document["private_key_id"].as_str().map(str::to_string),
                email,
                key: Arc::new(key),
                token_uri,
            }
        }
        Some("authorized_user") => Login::User {
            client_id: field("client_id")?,
            client_secret: field("client_secret")?,
            refresh_token: field("refresh_token")?,
            token_uri,
        },
        Some(other) => {
            return Err(format!(
                "{} is a '{other}' credential; service_account and authorized_user are read, \
                 and others (external_account, impersonation) not yet",
                file.display()
            ))
        }
        None => return Err(format!("{} has no \"type\"", file.display())),
    };
    let who = match &login {
        Login::ServiceAccount { email, .. } => format!("service account {email}"),
        _ => "gcloud's user login".to_string(),
    };
    Ok(Credentials {
        login,
        source: format!("{who}, from {source}"),
    })
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

/// Access tokens for one node's credentials, fetched when needed and cached.
/// Cloned for another thread, it shares the cache.
#[derive(Clone)]
pub(crate) struct Tokens {
    credentials: Arc<Credentials>,
    cache: Arc<Mutex<Option<(String, Instant)>>>,
    timeout: Duration,
    retries: u32,
    scope: &'static str,
}

impl Tokens {
    pub(crate) fn new(
        credentials: Credentials,
        scope: &'static str,
        timeout: Duration,
        retries: u32,
    ) -> Self {
        Tokens {
            credentials: Arc::new(credentials),
            cache: Arc::new(Mutex::new(None)),
            timeout,
            retries,
            scope,
        }
    }

    pub(crate) fn source(&self) -> &str {
        &self.credentials.source
    }

    /// The `Authorization` value for a request, or `None` for an emulator.
    pub(crate) fn authorization(&self) -> Result<Option<String>, ConnectorError> {
        if let Login::Nobody = self.credentials.login {
            return Ok(None);
        }
        let mut cache = self.cache.lock().unwrap();
        if let Some((token, until)) = cache.as_ref() {
            if Instant::now() + REFRESH_MARGIN < *until {
                return Ok(Some(format!("Bearer {token}")));
            }
        }
        let (token, lasts) = self.fetch().map_err(|error| {
            ConnectorError::Data(format!(
                "Google sign-in with {}: {error}",
                self.credentials.source
            ))
        })?;
        *cache = Some((token.clone(), Instant::now() + lasts));
        Ok(Some(format!("Bearer {token}")))
    }

    /// Trade what the credentials hold for an access token, and how long it lasts.
    fn fetch(&self) -> Result<(String, Duration), ConnectorError> {
        let (token_uri, form) = match &self.credentials.login {
            Login::Nobody => unreachable!("an emulator asks for no token"),
            Login::ServiceAccount {
                email,
                key_id,
                key,
                token_uri,
            } => {
                let now = etl_state::time::now_unix();
                let mut header = json!({ "alg": "RS256", "typ": "JWT" });
                if let Some(id) = key_id {
                    header["kid"] = json!(id);
                }
                let claims = json!({
                    "iss": email,
                    "scope": self.scope,
                    "aud": token_uri,
                    "iat": now,
                    "exp": now + ASSERTION_SECONDS,
                });
                let assertion = jwt(key, &header, &claims).map_err(ConnectorError::Data)?;
                (
                    token_uri,
                    vec![
                        ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                        ("assertion", assertion.as_str()),
                    ]
                    .into_iter()
                    .map(|(k, v)| (k, v.to_string()))
                    .collect::<Vec<_>>(),
                )
            }
            Login::User {
                client_id,
                client_secret,
                refresh_token,
                token_uri,
            } => (
                token_uri,
                vec![
                    ("grant_type", "refresh_token".to_string()),
                    ("client_id", client_id.clone()),
                    ("client_secret", client_secret.clone()),
                    ("refresh_token", refresh_token.clone()),
                ],
            ),
        };

        let body = form_encode(&form);
        let mut client = Client::new(Settings::signed_post(
            token_uri.clone(),
            self.timeout,
            self.retries,
        ));
        let extra = Extra {
            headers: &Vec::<(String, String)>::new,
            content_type: "application/x-www-form-urlencoded",
            throttled: &|_, _| false,
        };
        let answer: JsonValue = client.send_with(
            token_uri,
            &[],
            Some(body.as_bytes()),
            Some(&extra),
            |reply| match serde_json::from_str(&reply.body) {
                Ok(value) => Judged::Accept(value),
                Err(_) => Judged::Fail(ConnectorError::Data(format!(
                    "the token answer is not JSON: {}",
                    snippet(&reply.body)
                ))),
            },
        )?;
        let token = answer["access_token"].as_str().ok_or_else(|| {
            ConnectorError::Data(format!(
                "the token answer has no access_token: {}",
                snippet(&answer.to_string())
            ))
        })?;
        let lasts = answer["expires_in"].as_u64().unwrap_or(3600);
        Ok((token.to_string(), Duration::from_secs(lasts)))
    }
}

/// `application/x-www-form-urlencoded`.
fn form_encode(pairs: &[(&str, String)]) -> String {
    let encode = |text: &str| {
        let mut out = String::with_capacity(text.len());
        for &byte in text.as_bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(byte as char)
                }
                other => out.push_str(&format!("%{other:02X}")),
            }
        }
        out
    };
    pairs
        .iter()
        .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}
