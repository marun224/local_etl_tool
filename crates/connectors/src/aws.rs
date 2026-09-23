//! AWS: Signature Version 4, and where credentials and the region come from.
//!
//! **Signing** is written here rather than taken from `aws-sigv4`, which
//! declares Rust 1.94.1 against this project's 1.88 and brings AWS's "smithy"
//! crates with it (Settled decision 47). What is written is formatting -- the
//! canonical request, the string to sign, the derived key -- and the
//! cryptography under it is `ring`'s HMAC-SHA256 and SHA-256, already in the
//! tree for TLS. It is proved against **AWS's own SigV4 test suite**, 38 cases
//! copied under `tests/fixtures/sigv4/`, each compared byte for byte at all
//! three stages; the Kinesis test server does not check signatures, so those
//! vectors are the proof.
//!
//! **Credentials** (Settled decision 48), the first source that has them
//! winning: the node's properties (`access_key_id`, `secret_access_key`,
//! `session_token`, which should be secrets), then `AWS_ACCESS_KEY_ID`,
//! `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`, then a named profile in the
//! shared files (`~/.aws/credentials`, `~/.aws/config`). Roles on EC2, EKS and
//! ECS are not read yet. **The region**: `region`, then `AWS_REGION` or
//! `AWS_DEFAULT_REGION`, then the profile's. Both are looked up when the
//! connector runs, so an artifact takes them from where it runs, and bakes
//! nothing in unless a property holds it.

use etl_plugin_sdk::ConnectorError;
use ring::{digest, hmac};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Signing
// ---------------------------------------------------------------------------

/// What signs a request: who, where, and when.
#[derive(Debug, Clone)]
pub(crate) struct Signer<'a> {
    pub(crate) credentials: &'a Credentials,
    pub(crate) region: &'a str,
    pub(crate) service: &'a str,
    /// `YYYYMMDDTHHMMSSZ`, UTC.
    pub(crate) amz_date: &'a str,
    /// Remove dot segments and repeated slashes from the path. True for every
    /// service but S3.
    pub(crate) normalize: bool,
    /// Add and sign `x-amz-content-sha256`. S3 wants it; Kinesis does not.
    pub(crate) sign_body: bool,
    /// Send the session token unsigned. Only the test suite asks for this.
    pub(crate) omit_session_token: bool,
}

/// A request as it will be sent, before signing.
#[derive(Debug, Clone)]
pub(crate) struct Unsigned<'a> {
    pub(crate) method: &'a str,
    /// The path and query as they will appear on the request line.
    pub(crate) target: &'a str,
    /// Every header that will be sent, `Host` included, in order. Repeated
    /// names are allowed.
    pub(crate) headers: &'a [(String, String)],
    pub(crate) body: &'a [u8],
}

/// What signing produced: the headers to add, and the intermediate steps,
/// which the test suite checks one by one.
#[derive(Debug, Clone)]
#[cfg_attr(not(test), allow(dead_code))] // the steps are read by the SigV4 suite
pub(crate) struct Signed {
    pub(crate) canonical_request: String,
    pub(crate) string_to_sign: String,
    pub(crate) signature: String,
    /// `x-amz-date`, the token if there is one, `x-amz-content-sha256` if
    /// asked for, and `Authorization`.
    pub(crate) headers: Vec<(String, String)>,
}

pub(crate) fn sign(request: &Unsigned, signer: &Signer) -> Signed {
    let date = &signer.amz_date[..8];
    let body_hash = hex(digest::digest(&digest::SHA256, request.body).as_ref());

    // The headers the signature covers: the request's own, plus the ones
    // signing adds before it signs.
    let mut headers: Vec<(String, String)> = request.headers.to_vec();
    let mut added = vec![("x-amz-date".to_string(), signer.amz_date.to_string())];
    if signer.sign_body {
        added.push(("x-amz-content-sha256".to_string(), body_hash.clone()));
    }
    let token = signer.credentials.session_token.as_deref();
    if let (Some(token), false) = (token, signer.omit_session_token) {
        added.push(("x-amz-security-token".to_string(), token.to_string()));
    }
    headers.extend(added.iter().cloned());

    let (path, query) = request
        .target
        .split_once('?')
        .unwrap_or((request.target, ""));
    let (canonical_headers, signed_headers) = canonical_headers(&headers);

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        canonical_path(path, signer.normalize),
        canonical_query(query),
        canonical_headers,
        signed_headers,
        body_hash
    );

    let scope = format!("{date}/{}/{}/aws4_request", signer.region, signer.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        signer.amz_date,
        hex(digest::digest(&digest::SHA256, canonical_request.as_bytes()).as_ref())
    );

    let key = [date, signer.region, signer.service, "aws4_request"]
        .iter()
        .fold(
            format!("AWS4{}", signer.credentials.secret_access_key).into_bytes(),
            |key, part| hmac_sha256(&key, part.as_bytes()),
        );
    let signature = hex(&hmac_sha256(&key, string_to_sign.as_bytes()));

    if let (Some(token), true) = (token, signer.omit_session_token) {
        added.push(("x-amz-security-token".to_string(), token.to_string()));
    }
    added.push((
        "Authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, \
             Signature={signature}",
            signer.credentials.access_key_id
        ),
    ));

    Signed {
        canonical_request,
        string_to_sign,
        signature,
        headers: added,
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
        .as_ref()
        .to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Percent-encode everything but RFC 3986's unreserved characters, and `/`
/// when `keep_slash`.
fn uri_encode(text: &[u8], keep_slash: bool) -> String {
    let mut out = String::with_capacity(text.len());
    for &byte in text {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            b'/' if keep_slash => out.push('/'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// Undo percent-encoding, leaving anything that is not a valid escape as it
/// was, so a value is never double-encoded.
fn uri_decode(text: &str) -> Vec<u8> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let escape = (bytes[i] == b'%' && i + 2 < bytes.len())
            .then(|| std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok())
            .flatten()
            .and_then(|pair| u8::from_str_radix(pair, 16).ok());
        match escape {
            Some(byte) => {
                out.push(byte);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    out
}

fn canonical_path(path: &str, normalize: bool) -> String {
    let path = if path.is_empty() { "/" } else { path };
    if !normalize {
        return uri_encode(path.as_bytes(), true);
    }

    // RFC 3986's dot-segment removal, with empty segments dropped as well:
    // `//a//b/` is `/a/b/`.
    let mut segments: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            other => segments.push(other),
        }
    }
    let trailing = path.ends_with('/') || path.ends_with("/.") || path.ends_with("/..");
    let mut normalized = format!("/{}", segments.join("/"));
    if trailing && !normalized.ends_with('/') {
        normalized.push('/');
    }
    uri_encode(normalized.as_bytes(), true)
}

fn canonical_query(query: &str) -> String {
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (
                uri_encode(&uri_decode(key), false),
                uri_encode(&uri_decode(value), false),
            )
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// The canonical header block and the signed-header list. Names lower-cased and
/// sorted; each value trimmed with runs of spaces made one, including inside
/// quotes, as the test suite's `get-header-value-trim` requires; a repeated
/// name's values joined by commas in the order sent.
fn canonical_headers(headers: &[(String, String)]) -> (String, String) {
    let mut by_name: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in headers {
        let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
        by_name
            .entry(name.trim().to_ascii_lowercase())
            .or_default()
            .push(value);
    }
    let block: String = by_name
        .iter()
        .map(|(name, values)| format!("{name}:{}\n", values.join(",")))
        .collect();
    let signed = by_name.keys().cloned().collect::<Vec<_>>().join(";");
    (block, signed)
}

/// Now as `YYYYMMDDTHHMMSSZ`, which is both the `x-amz-date` header and, in its
/// first eight characters, the credential scope's date.
pub(crate) fn amz_date_now() -> String {
    let now = etl_state::time::now_unix();
    let (year, month, day) = etl_state::time::civil_from_days(now.div_euclid(86_400));
    let within = now.rem_euclid(86_400);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        within / 3600,
        within / 60 % 60,
        within % 60
    )
}

// ---------------------------------------------------------------------------
// Credentials and region
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Credentials {
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
    pub(crate) session_token: Option<String>,
    /// Where they came from, for errors and the report. Never the secret.
    pub(crate) source: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

/// Where the lookups look: the environment and the home directory, passed in
/// so tests can stand in for both.
pub(crate) struct Sources<'a> {
    pub(crate) var: &'a dyn Fn(&str) -> Option<String>,
    pub(crate) home: Option<PathBuf>,
}

impl Sources<'_> {
    /// The process's own environment and home directory.
    pub(crate) fn process() -> Sources<'static> {
        fn var(name: &str) -> Option<String> {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        }
        Sources {
            var: &var,
            home: std::env::var_os("USERPROFILE")
                .or_else(|| std::env::var_os("HOME"))
                .map(PathBuf::from),
        }
    }

    fn get(&self, name: &str) -> Option<String> {
        (self.var)(name).filter(|value| !value.trim().is_empty())
    }

    fn profile_name(&self, properties: &JsonValue) -> String {
        text(properties, "profile")
            .map(str::to_string)
            .or_else(|| self.get("AWS_PROFILE"))
            .unwrap_or_else(|| "default".to_string())
    }

    fn file(&self, variable: &str, default: &str) -> Option<PathBuf> {
        self.get(variable).map(PathBuf::from).or_else(|| {
            self.home
                .as_ref()
                .map(|home| home.join(".aws").join(default))
        })
    }
}

/// The credentials for a node, from the first source that has them.
pub(crate) fn credentials(
    properties: &JsonValue,
    sources: &Sources,
) -> Result<Credentials, ConnectorError> {
    let key = text(properties, "access_key_id");
    let secret = text(properties, "secret_access_key");
    match (key, secret) {
        (Some(key), Some(secret)) => {
            return Ok(Credentials {
                access_key_id: key.to_string(),
                secret_access_key: secret.to_string(),
                session_token: text(properties, "session_token").map(str::to_string),
                source: "the node's properties".to_string(),
            })
        }
        (Some(_), None) => {
            return Err(ConnectorError::property(
                "secret_access_key",
                "is required when access_key_id is set",
            ))
        }
        (None, Some(_)) => {
            return Err(ConnectorError::property(
                "access_key_id",
                "is required when secret_access_key is set",
            ))
        }
        (None, None) => {}
    }

    if let (Some(key), Some(secret)) = (
        sources.get("AWS_ACCESS_KEY_ID"),
        sources.get("AWS_SECRET_ACCESS_KEY"),
    ) {
        return Ok(Credentials {
            access_key_id: key,
            secret_access_key: secret,
            session_token: sources.get("AWS_SESSION_TOKEN"),
            source: "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY".to_string(),
        });
    }

    let profile = sources.profile_name(properties);
    let from_files = [
        sources.file("AWS_SHARED_CREDENTIALS_FILE", "credentials"),
        sources.file("AWS_CONFIG_FILE", "config"),
    ];
    for (file, is_config) in from_files.iter().zip([false, true]) {
        let Some(file) = file else { continue };
        let Some(section) = profile_section(file, &profile, is_config)? else {
            continue;
        };
        if let (Some(key), Some(secret)) = (
            section.get("aws_access_key_id"),
            section.get("aws_secret_access_key"),
        ) {
            return Ok(Credentials {
                access_key_id: key.clone(),
                secret_access_key: secret.clone(),
                session_token: section.get("aws_session_token").cloned(),
                source: format!("profile '{profile}' in {}", file.display()),
            });
        }
    }

    Err(ConnectorError::property(
        "access_key_id",
        format!(
            "no AWS credentials found. Looked in: the node's access_key_id and \
             secret_access_key; AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY; profile '{profile}' \
             in {}. Roles on EC2, EKS and ECS are not read yet",
            from_files
                .iter()
                .flatten()
                .map(|file| file.display().to_string())
                .collect::<Vec<_>>()
                .join(" and ")
        ),
    ))
}

/// The region for a node, from the first source that has it.
pub(crate) fn region(properties: &JsonValue, sources: &Sources) -> Result<String, ConnectorError> {
    if let Some(region) = text(properties, "region") {
        return Ok(region.to_string());
    }
    if let Some(region) = sources
        .get("AWS_REGION")
        .or_else(|| sources.get("AWS_DEFAULT_REGION"))
    {
        return Ok(region);
    }
    let profile = sources.profile_name(properties);
    if let Some(config) = sources.file("AWS_CONFIG_FILE", "config") {
        if let Some(region) = profile_section(&config, &profile, true)?
            .and_then(|section| section.get("region").cloned())
        {
            return Ok(region);
        }
    }
    Err(ConnectorError::property(
        "region",
        format!("no AWS region found. Set region, or AWS_REGION, or region in profile '{profile}'"),
    ))
}

/// One profile's keys from an AWS shared file, or `None` when the file or the
/// profile is not there. In `config` every profile but `default` is written
/// `[profile name]`; in `credentials` it is plain `[name]`.
fn profile_section(
    file: &std::path::Path,
    profile: &str,
    is_config: bool,
) -> Result<Option<BTreeMap<String, String>>, ConnectorError> {
    let text = match std::fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ConnectorError::io(file, error)),
    };
    let wanted = if is_config && profile != "default" {
        format!("profile {profile}")
    } else {
        profile.to_string()
    };

    let mut current: Option<String> = None;
    let mut found: Option<BTreeMap<String, String>> = None;
    for line in text.lines() {
        let line = line.trim().trim_start_matches('\u{feff}');
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            let name = name.split_whitespace().collect::<Vec<_>>().join(" ");
            current = Some(name.clone());
            if name == wanted {
                found.get_or_insert_with(BTreeMap::new);
            }
            continue;
        }
        if current.as_deref() == Some(wanted.as_str()) {
            if let Some((key, value)) = line.split_once('=') {
                found
                    .get_or_insert_with(BTreeMap::new)
                    .insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
            }
        }
    }
    Ok(found)
}

fn text<'a>(properties: &'a JsonValue, key: &str) -> Option<&'a str> {
    properties
        .get(key)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}
