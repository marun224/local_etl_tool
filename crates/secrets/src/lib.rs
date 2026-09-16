//! Connection secrets, encrypted at rest inside the workspace.
//!
//! A pipeline that needs a database password has to get it from somewhere, and
//! the two easy answers are both wrong: in the document, where it reaches
//! version control, or in the environment, where it is fine for one machine and
//! nothing else. This crate is the third answer — encrypted in the workspace,
//! next to the pipelines, with the key beside it.
//!
//! **What this protects against, and what it does not.** The key lives in the
//! same `.etl/` directory as the secrets it opens, so anyone who can read the
//! whole directory can read the secrets. That is deliberate and is the same
//! trade a local-first tool always makes: the workspace is a folder you copy
//! about, and Phase 9 wants it to keep working when copied to a machine with no
//! network and no keychain. What it does buy is real: `secrets.json` on its own
//! is useless, so the file that gets pasted into an issue, committed by
//! accident, or synced to a backup does not leak anything. Treat `.etl/keys/`
//! the way you would an SSH private key.
//!
//! AES-256-GCM, a fresh nonce per value, and the secret's own name as
//! associated data — so an entry cannot be renamed or swapped with another
//! inside the file without the decryption failing.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce};

/// The format version this crate writes.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

/// Where the workspace key lives, relative to the workspace root.
pub const KEY_PATH: &str = ".etl/keys/workspace.key";

/// Where the encrypted values live, relative to the workspace root.
pub const STORE_PATH: &str = ".etl/secrets.json";

/// AES-256: 32 bytes.
const KEY_BYTES: usize = 32;

#[derive(Debug, Error)]
pub enum SecretError {
    #[error(
        "this workspace has no key at {path}. Run `etl secret init` to create one, or copy the \
         key from the workspace these secrets came from."
    )]
    NoKey { path: String },

    #[error("could not read {path}: {source}")]
    Unreadable {
        path: String,
        #[source]
        source: io::Error,
    },

    #[error("could not write {path}: {source}")]
    Unwritable {
        path: String,
        #[source]
        source: io::Error,
    },

    #[error("{path} is not a valid key: {reason}")]
    KeyMalformed { path: String, reason: String },

    #[error("{path} is not valid JSON: {source}")]
    StoreMalformed {
        path: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("{path} holds '{name}' in a form this version does not understand: {reason}")]
    EntryMalformed {
        path: String,
        name: String,
        reason: String,
    },

    #[error(
        "'{name}' could not be decrypted with this workspace's key. Either the key is not the \
         one it was encrypted with, or the entry has been altered."
    )]
    Undecryptable { name: String },

    #[error("'{name}' could not be encrypted")]
    Unencryptable { name: String },

    #[error("'{name}' decrypted to something that is not text")]
    NotText { name: String },

    #[error("a secret's name must not be empty")]
    EmptyName,
}

/// One encrypted value. Hex rather than base64 so the file stays greppable and
/// diffable, and so no encoding dependency is needed to read it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Entry {
    nonce: String,
    ciphertext: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

/// The on-disk shape of `.etl/secrets.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct StoreFile {
    #[serde(default, rename = "formatVersion", skip_serializing_if = "is_zero")]
    format_version: u32,
    #[serde(default)]
    secrets: BTreeMap<String, Entry>,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

/// A workspace's secrets, and the key that opens them.
pub struct SecretStore {
    workspace: PathBuf,
    key: [u8; KEY_BYTES],
    file: StoreFile,
}

impl std::fmt::Debug for SecretStore {
    /// Deliberately hand-written: a derived `Debug` would print the key.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretStore")
            .field("workspace", &self.workspace)
            .field("key", &"<redacted>")
            .field("secrets", &self.names())
            .finish()
    }
}

impl SecretStore {
    pub fn key_path(workspace: &Path) -> PathBuf {
        workspace.join(KEY_PATH)
    }

    pub fn store_path(workspace: &Path) -> PathBuf {
        workspace.join(STORE_PATH)
    }

    /// Whether this workspace has a key yet.
    pub fn has_key(workspace: &Path) -> bool {
        Self::key_path(workspace).is_file()
    }

    /// Create a key for this workspace, failing if one is already there.
    ///
    /// Refusing rather than overwriting is the whole point: a new key does not
    /// replace the old one, it makes every existing secret permanently
    /// unreadable.
    pub fn initialise(workspace: &Path) -> Result<PathBuf, SecretError> {
        let path = Self::key_path(workspace);

        if path.exists() {
            return Ok(path);
        }

        let key = Aes256Gcm::generate_key(&mut OsRng);
        write_new(&path, &format!("{}\n", to_hex(&key)))?;

        Ok(path)
    }

    /// Open a workspace's secrets, creating the key if there is none.
    pub fn open(workspace: &Path) -> Result<Self, SecretError> {
        Self::initialise(workspace)?;
        Self::open_existing(workspace)
    }

    /// Open a workspace's secrets, refusing if it has no key.
    ///
    /// This is what a *run* uses: a run that silently minted a key would then
    /// fail to decrypt everything, and "wrong key" is a much clearer thing to
    /// be told than "your secret is not there".
    pub fn open_existing(workspace: &Path) -> Result<Self, SecretError> {
        let key_path = Self::key_path(workspace);

        if !key_path.is_file() {
            return Err(SecretError::NoKey {
                path: key_path.display().to_string(),
            });
        }

        let key = read_key(&key_path)?;
        let store_path = Self::store_path(workspace);

        let file = match std::fs::read_to_string(&store_path) {
            Ok(text) => {
                serde_json::from_str(&text).map_err(|source| SecretError::StoreMalformed {
                    path: store_path.display().to_string(),
                    source,
                })?
            }
            // No store yet is an empty store, not a failure.
            Err(source) if source.kind() == io::ErrorKind::NotFound => StoreFile::default(),
            Err(source) => {
                return Err(SecretError::Unreadable {
                    path: store_path.display().to_string(),
                    source,
                })
            }
        };

        Ok(Self {
            workspace: workspace.to_path_buf(),
            key,
            file,
        })
    }

    /// The names held here, sorted. Names are not secret; values are.
    pub fn names(&self) -> Vec<&str> {
        self.file.secrets.keys().map(String::as_str).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.file.secrets.is_empty()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.file.secrets.contains_key(name)
    }

    pub fn description(&self, name: &str) -> Option<&str> {
        self.file
            .secrets
            .get(name)
            .and_then(|entry| entry.description.as_deref())
    }

    /// Encrypt a value under this workspace's key, replacing any previous one.
    pub fn set(
        &mut self,
        name: &str,
        value: &str,
        description: Option<&str>,
    ) -> Result<(), SecretError> {
        if name.trim().is_empty() {
            return Err(SecretError::EmptyName);
        }

        let cipher = self.cipher();
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        // The name travels as associated data, so an entry cannot be renamed or
        // swapped with another inside the file without decryption failing.
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: value.as_bytes(),
                    aad: name.as_bytes(),
                },
            )
            .map_err(|_| SecretError::Unencryptable {
                name: name.to_string(),
            })?;

        self.file.format_version = CURRENT_FORMAT_VERSION;
        self.file.secrets.insert(
            name.to_string(),
            Entry {
                nonce: to_hex(&nonce),
                ciphertext: to_hex(&ciphertext),
                description: description.map(str::to_string),
            },
        );

        Ok(())
    }

    /// Decrypt one value. `None` when there is no such secret.
    ///
    /// A secret that is present but will not decrypt is an error rather than a
    /// `None`: failing closed is the whole point, and "not found" would send
    /// someone looking for the wrong problem.
    pub fn get(&self, name: &str) -> Result<Option<String>, SecretError> {
        let Some(entry) = self.file.secrets.get(name) else {
            return Ok(None);
        };

        let store_path = Self::store_path(&self.workspace).display().to_string();

        let nonce = from_hex(&entry.nonce).ok_or_else(|| SecretError::EntryMalformed {
            path: store_path.clone(),
            name: name.to_string(),
            reason: "its nonce is not hexadecimal".to_string(),
        })?;

        if nonce.len() != 12 {
            return Err(SecretError::EntryMalformed {
                path: store_path,
                name: name.to_string(),
                reason: format!("its nonce is {} bytes, not 12", nonce.len()),
            });
        }

        let ciphertext =
            from_hex(&entry.ciphertext).ok_or_else(|| SecretError::EntryMalformed {
                path: store_path,
                name: name.to_string(),
                reason: "its ciphertext is not hexadecimal".to_string(),
            })?;

        let plaintext = self
            .cipher()
            .decrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &ciphertext,
                    aad: name.as_bytes(),
                },
            )
            .map_err(|_| SecretError::Undecryptable {
                name: name.to_string(),
            })?;

        String::from_utf8(plaintext)
            .map(Some)
            .map_err(|_| SecretError::NotText {
                name: name.to_string(),
            })
    }

    /// Forget a secret. Returns whether there was one.
    pub fn remove(&mut self, name: &str) -> bool {
        self.file.secrets.remove(name).is_some()
    }

    /// Write the store back out.
    pub fn save(&self) -> Result<(), SecretError> {
        let path = Self::store_path(&self.workspace);

        let text = serde_json::to_string_pretty(&self.file)
            .expect("the store is plain data and always serialises");

        create_parent(&path)?;

        std::fs::write(&path, format!("{text}\n")).map_err(|source| SecretError::Unwritable {
            path: path.display().to_string(),
            source,
        })
    }

    fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.key))
    }
}

// ---------------------------------------------------------------------------
// The key file
// ---------------------------------------------------------------------------

fn read_key(path: &Path) -> Result<[u8; KEY_BYTES], SecretError> {
    let text = std::fs::read_to_string(path).map_err(|source| SecretError::Unreadable {
        path: path.display().to_string(),
        source,
    })?;

    let bytes = from_hex(text.trim()).ok_or_else(|| SecretError::KeyMalformed {
        path: path.display().to_string(),
        reason: "it is not hexadecimal".to_string(),
    })?;

    <[u8; KEY_BYTES]>::try_from(bytes.as_slice()).map_err(|_| SecretError::KeyMalformed {
        path: path.display().to_string(),
        reason: format!(
            "it is {} bytes, and an AES-256 key is {KEY_BYTES}",
            bytes.len()
        ),
    })
}

fn create_parent(path: &Path) -> Result<(), SecretError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };

    if parent.as_os_str().is_empty() || parent.exists() {
        return Ok(());
    }

    std::fs::create_dir_all(parent).map_err(|source| SecretError::Unwritable {
        path: parent.display().to_string(),
        source,
    })
}

/// Write a file that must not already exist.
///
/// `create_new` rather than a check-then-write, so two processes racing to
/// initialise the same workspace cannot both believe they made the key.
fn write_new(path: &Path, contents: &str) -> Result<(), SecretError> {
    create_parent(path)?;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| SecretError::Unwritable {
            path: path.display().to_string(),
            source,
        })?;

    use std::io::Write;
    file.write_all(contents.as_bytes())
        .map_err(|source| SecretError::Unwritable {
            path: path.display().to_string(),
            source,
        })
}

// ---------------------------------------------------------------------------
// Random tokens
// ---------------------------------------------------------------------------

/// A fresh random token, hex, `bytes` bytes of entropy.
///
/// Here rather than in the console crate because this is where the project
/// keeps its cryptography: `OsRng` is already a dependency of the AES that
/// Settled decision 4 chose, and hex is already written below. A console
/// minting its own tokens from a second source of randomness would be two
/// answers to one question.
///
/// **From the operating system, never from a pseudo-random generator seeded by
/// the clock.** A bearer token that can be guessed is not a token. `OsRng`
/// reads the platform CSPRNG, and a failure to do so panics rather than
/// silently returning something weaker — which is the correct trade here,
/// because the alternative is a console that looks locked and is not.
pub fn random_token(bytes: usize) -> String {
    use aes_gcm::aead::rand_core::RngCore;

    let mut buffer = vec![0_u8; bytes];
    OsRng.fill_bytes(&mut buffer);

    to_hex(&buffer)
}

// ---------------------------------------------------------------------------
// Hex
// ---------------------------------------------------------------------------

/// Hex rather than base64: it keeps the store greppable, it has no padding
/// rules to get wrong, and it is ten lines rather than a dependency.
pub(crate) fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        out.push(char::from_digit((byte >> 4) as u32, 16).expect("a nibble is a hex digit"));
        out.push(char::from_digit((byte & 0x0f) as u32, 16).expect("a nibble is a hex digit"));
    }

    out
}

fn from_hex(text: &str) -> Option<Vec<u8>> {
    // `usize::is_multiple_of` would read better but is stable only in 1.87,
    // and the workspace declares 1.80.
    if text.len() % 2 != 0 {
        return None;
    }

    let digits: Vec<u8> = text
        .chars()
        .map(|character| character.to_digit(16).map(|value| value as u8))
        .collect::<Option<Vec<_>>>()?;

    Some(
        digits
            .chunks(2)
            .map(|pair| (pair[0] << 4) | pair[1])
            .collect(),
    )
}

#[cfg(test)]
mod tests;
