//! A pipeline baked into an executable, and the format that puts it there.
//!
//! Phase 9's artifact is one file you can copy to a server that has no Rust, no
//! DuckDB and no workspace, and run. This crate is both halves of that: the
//! format itself, and the [`etl-runner`](../etl_runner/index.html) binary that
//! reads it back out of its own executable.
//!
//! # Why an appended payload rather than a compile
//!
//! The obvious way to bake a pipeline in is to generate a Rust source file and
//! compile it. That would make exporting require a Rust toolchain on whichever
//! machine is doing the exporting — which is fine here, on a developer's
//! laptop, and wrong for the thing this is for: a **Build Pipeline** button in
//! a shipped desktop app, pressed by somebody who has never installed cargo.
//!
//! So a build is a **copy and an append**: take the runner binary that was
//! compiled once, copy it, and stick the pipeline on the end. No compiler, no
//! linker, and building for another OS is choosing a different runner to copy
//! (Phase 9c) rather than cross-compiling at the moment somebody clicks a
//! button.
//!
//! It also means the runner is an ordinary binary that happens to look at its
//! own tail. A runner with nothing appended is not broken — it says it has no
//! pipeline and exits, which is what makes [`Payload::read_from`] returning
//! `Ok(None)` a case rather than an error.
//!
//! # The format
//!
//! Everything is appended after the executable's own bytes, and everything is
//! found by reading **backwards** from the end — the one anchor that does not
//! depend on knowing how long the executable was:
//!
//! ```text
//! [ runner executable                      ]
//! [ blob region        ] blob_len bytes      <- 9b: DuckDB, extensions
//! [ header JSON        ] header_len bytes
//! [ header_len  u64 LE ] 8 bytes           \
//! [ blob_len    u64 LE ] 8 bytes            > the 24-byte trailer
//! [ MAGIC       u64 LE ] 8 bytes           /
//! ```
//!
//! The magic goes **last** so that finding it is a single seek to `end - 8`,
//! and so that appending to a file that already has a payload is detectable
//! rather than silently producing a binary with two.
//!
//! Both operating systems tolerate this. A PE file's headers give the size of
//! the image and the loader ignores anything after it; ELF is the same. This is
//! the same trick self-extracting archives have used for thirty years. What it
//! does break is a **signed** binary, on both platforms — appending invalidates
//! the signature. That is a Phase 9d packaging problem and is recorded as one.
//!
//! # What is in the header
//!
//! The **resolved** document, not the source one: `${...}` references are
//! substituted before baking, because the machine this runs on will not have
//! the workspace's contexts, its `.etl/` directory or its secret key. That is
//! also why [`Payload::pipeline`] can hold secret *values*, and why `etl build`
//! refuses to bake one without being told to in as many words.
//!
//! `files` is empty in 9a and is what 9b fills: the DuckDB CLI and whichever
//! extensions this pipeline's components asked for, addressed by offset into
//! the blob region. Declaring it now costs nothing and means 9b adds bytes
//! rather than a second format.

use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

pub mod extract;

#[cfg(test)]
mod tests;

/// The last eight bytes of a built runner: `"ETLBAKE1"` as little-endian u64.
///
/// Spelled as bytes rather than a hex constant so that `strings` on a built
/// artifact shows something a person can search for.
pub const MAGIC: u64 = u64::from_le_bytes(*b"ETLBAKE1");

/// How long the fixed trailer is: three little-endian `u64`s.
pub const TRAILER_LEN: u64 = 24;

/// The payload format this build of the runner writes and understands.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum PayloadError {
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

    #[error("could not find this executable: {source}")]
    NoSelf {
        #[source]
        source: io::Error,
    },

    #[error(
        "{path} carries a payload written in format {found}, but this runner understands {CURRENT_FORMAT_VERSION}. \
         Rebuild it with a matching version of `etl build`."
    )]
    Incompatible { path: String, found: u32 },

    #[error("{path} has a payload trailer but its header is not readable: {source}")]
    Corrupt {
        path: String,
        #[source]
        source: serde_json::Error,
    },

    #[error(
        "{path} has a payload trailer claiming {claimed} bytes, which does not fit in a file of {actual}"
    )]
    Truncated {
        path: String,
        claimed: u64,
        actual: u64,
    },

    #[error("{path} already has a pipeline baked into it; build from a plain runner instead")]
    AlreadyBaked { path: String },

    #[error(
        "this artifact carries a file called '{name}', which is not a plain filename. Refusing to extract it."
    )]
    UnsafeName { name: String },
}

/// What an embedded file is for, which is what decides where it is extracted.
///
/// DuckDB insists on finding an extension at
/// `<extension_directory>/<version>/<platform>/<name>.duckdb_extension`, so the
/// runner has to rebuild that shape rather than drop everything in one
/// directory. Saying which file is which here means the layout is reconstructed
/// from the payload rather than guessed from the filename.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// The DuckDB CLI itself.
    #[default]
    Engine,
    /// A `.duckdb_extension`, or the small `.info` file beside it.
    Extension,
}

/// One file carried inside the binary, addressed by offset into the blob region.
///
/// Empty in 9a. 9b puts the DuckDB CLI and the extensions this pipeline needs
/// here, which is what makes the artifact runnable on a machine that has
/// neither.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddedFile {
    /// What to call it once it is on disk. A bare filename, never a path: this
    /// string chooses a name inside a directory the runner made, and letting it
    /// contain a separator would let a built artifact write elsewhere.
    pub name: String,

    /// Where it goes once extracted.
    #[serde(default)]
    pub role: Role,

    /// Where this file starts, counted from the beginning of the blob region
    /// rather than from the beginning of the file. The executable's own length
    /// is not known to whoever reads the header, and does not need to be.
    pub offset: u64,

    pub length: u64,

    /// Whether this file has to be executable once extracted. True for the
    /// DuckDB CLI and false for an extension, which DuckDB only ever reads.
    #[serde(default, skip_serializing_if = "is_false")]
    pub executable: bool,

    /// Anything a newer version wrote, kept across a read and a write.
    #[serde(flatten, default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// What a built runner carries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Payload {
    #[serde(rename = "formatVersion")]
    pub format_version: u32,

    /// What this pipeline is called, for `--info` and for the run's own output.
    pub name: String,

    /// When it was built, UTC, as `etl-state` writes timestamps.
    #[serde(rename = "builtAt")]
    pub built_at: String,

    /// The **resolved** pipeline document: every `${...}` already substituted,
    /// because the machine this runs on has none of what would resolve them.
    pub pipeline: etl_metadata::PipelineDoc,

    /// Files carried in the blob region. Empty until 9b.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<EmbeddedFile>,

    /// The DuckDB version the embedded extensions were built for, as
    /// `"v1.5.5"`. Part of the path DuckDB looks them up by, and the reason an
    /// artifact cannot mix an engine with extensions from another release.
    #[serde(
        rename = "duckdbVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub duckdb_version: String,

    /// The DuckDB platform triple the embedded files are for, as
    /// `"windows_amd64"`. Recorded rather than detected at run time, because
    /// what matters is what was *put in* — a mismatch should be a clear
    /// refusal, not a confusing failure to load.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub platform: String,

    /// A digest of the blob region, set by [`Payload::write_built`].
    ///
    /// Without it the cache key cannot tell two builds apart whose embedded
    /// files happen to be the same *lengths* — the header records offsets and
    /// lengths, not contents — and the second artifact would run the first
    /// one's engine out of a shared directory. Computed once here, on the
    /// machine doing the building, rather than over hundreds of megabytes on
    /// every run.
    #[serde(rename = "blobDigest", default, skip_serializing_if = "is_zero_u64")]
    pub blob_digest: u64,

    /// Whether baking this pipeline substituted a secret into it.
    ///
    /// Recorded rather than inferred, so `--info` can say so on a machine that
    /// has no way to check. A built artifact that carries a password is a
    /// credential, and somebody holding one should be able to find that out
    /// without reading the bytes.
    #[serde(default, skip_serializing_if = "is_false")]
    pub carries_secrets: bool,

    #[serde(flatten, default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl Payload {
    /// A payload for a resolved document.
    pub fn new(
        name: impl Into<String>,
        built_at: impl Into<String>,
        pipeline: etl_metadata::PipelineDoc,
    ) -> Self {
        Payload {
            format_version: CURRENT_FORMAT_VERSION,
            name: name.into(),
            built_at: built_at.into(),
            pipeline,
            files: Vec::new(),
            duckdb_version: String::new(),
            platform: String::new(),
            blob_digest: 0,
            carries_secrets: false,
            extra: serde_json::Map::new(),
        }
    }

    /// Read the payload out of this running executable, if it has one.
    ///
    /// `Ok(None)` is a plain runner: built, never baked. That is a state worth
    /// distinguishing from a failure, because it is exactly what `cargo run -p
    /// etl-runner` produces and the message for it is "nothing is baked in",
    /// not "your file is corrupt".
    pub fn read_from_self() -> Result<Option<(Self, Blobs)>, PayloadError> {
        let path = std::env::current_exe().map_err(|source| PayloadError::NoSelf { source })?;

        Self::read_from(&path)
    }

    /// Read the payload out of a file, if it has one.
    pub fn read_from(path: &Path) -> Result<Option<(Self, Blobs)>, PayloadError> {
        let unreadable = |source: io::Error| PayloadError::Unreadable {
            path: path.display().to_string(),
            source,
        };

        let mut file = File::open(path).map_err(unreadable)?;
        let total = file.metadata().map_err(unreadable)?.len();

        if total < TRAILER_LEN {
            return Ok(None);
        }

        file.seek(SeekFrom::End(-(TRAILER_LEN as i64)))
            .map_err(unreadable)?;

        let mut trailer = [0_u8; TRAILER_LEN as usize];
        file.read_exact(&mut trailer).map_err(unreadable)?;

        let header_len = u64::from_le_bytes(trailer[0..8].try_into().expect("8 bytes"));
        let blob_len = u64::from_le_bytes(trailer[8..16].try_into().expect("8 bytes"));
        let magic = u64::from_le_bytes(trailer[16..24].try_into().expect("8 bytes"));

        if magic != MAGIC {
            return Ok(None);
        }

        // Checked before seeking rather than after failing to: a trailer that
        // claims more than the file holds is a truncated download, and saying
        // so beats an unexpected-end-of-file from somewhere deeper.
        let claimed = TRAILER_LEN
            .checked_add(header_len)
            .and_then(|sum| sum.checked_add(blob_len))
            .ok_or_else(|| PayloadError::Truncated {
                path: path.display().to_string(),
                claimed: u64::MAX,
                actual: total,
            })?;

        if claimed > total {
            return Err(PayloadError::Truncated {
                path: path.display().to_string(),
                claimed,
                actual: total,
            });
        }

        let header_at = total - TRAILER_LEN - header_len;
        file.seek(SeekFrom::Start(header_at)).map_err(unreadable)?;

        let mut header = vec![0_u8; header_len as usize];
        file.read_exact(&mut header).map_err(unreadable)?;

        // The version is read before the rest of the header is trusted, so a
        // format this runner does not know is a clear refusal rather than a
        // confusing deserialisation failure about some field that moved.
        let probe: FormatProbe =
            serde_json::from_slice(&header).map_err(|source| PayloadError::Corrupt {
                path: path.display().to_string(),
                source,
            })?;

        if probe.format_version != CURRENT_FORMAT_VERSION {
            return Err(PayloadError::Incompatible {
                path: path.display().to_string(),
                found: probe.format_version,
            });
        }

        let payload: Payload =
            serde_json::from_slice(&header).map_err(|source| PayloadError::Corrupt {
                path: path.display().to_string(),
                source,
            })?;

        Ok(Some((
            payload,
            Blobs {
                path: path.to_path_buf(),
                start: total - TRAILER_LEN - header_len - blob_len,
                length: blob_len,
                key: fnv1a(&header),
            },
        )))
    }

    /// Copy `runner` to `destination` and append this payload to the copy.
    ///
    /// The blob region is written from `blobs`, whose offsets must already
    /// agree with `self.files`. In 9a that is always empty.
    pub fn write_built(
        &mut self,
        runner: &Path,
        destination: &Path,
        blobs: &[u8],
    ) -> Result<(), PayloadError> {
        // Refused rather than appended to. Appending twice leaves two trailers,
        // and the file would run as whichever was last — which is the sort of
        // thing that works in testing and ships the wrong pipeline.
        if Self::read_from(runner)?.is_some() {
            return Err(PayloadError::AlreadyBaked {
                path: runner.display().to_string(),
            });
        }

        // Set here rather than by the caller: a digest that can be forgotten is
        // a digest that will be, and the consequence is two builds quietly
        // sharing one extracted engine.
        self.blob_digest = fnv1a(blobs);

        let header = serde_json::to_vec(self).expect("a payload serialises");

        std::fs::copy(runner, destination).map_err(|source| PayloadError::Unwritable {
            path: destination.display().to_string(),
            source,
        })?;

        let unwritable = |source: io::Error| PayloadError::Unwritable {
            path: destination.display().to_string(),
            source,
        };

        let mut out = File::options()
            .append(true)
            .open(destination)
            .map_err(unwritable)?;

        out.write_all(blobs).map_err(unwritable)?;
        out.write_all(&header).map_err(unwritable)?;
        out.write_all(&(header.len() as u64).to_le_bytes())
            .map_err(unwritable)?;
        out.write_all(&(blobs.len() as u64).to_le_bytes())
            .map_err(unwritable)?;
        out.write_all(&MAGIC.to_le_bytes()).map_err(unwritable)?;
        out.flush().map_err(unwritable)?;

        drop(out);

        make_executable(destination)?;

        Ok(())
    }
}

/// Just enough of the header to decide whether the rest can be trusted.
#[derive(Deserialize)]
struct FormatProbe {
    #[serde(rename = "formatVersion", default)]
    format_version: u32,
}

/// Where the blob region is, so a file can be pulled out of it on demand.
///
/// Held as a location rather than as bytes: 9b's blob region is the DuckDB CLI
/// and its extensions, which is hundreds of megabytes, and reading all of it
/// into memory to extract one file would be the wrong shape from the start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blobs {
    path: PathBuf,
    start: u64,
    length: u64,
    key: u64,
}

impl Blobs {
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn len(&self) -> u64 {
        self.length
    }

    /// A stable name for *this* build's extracted files.
    ///
    /// Taken over the header, which is a few hundred bytes, rather than over the
    /// blob region, which is a few hundred megabytes — this is computed on every
    /// run in order to decide whether extraction can be skipped, so it has to be
    /// cheap.
    ///
    /// That works only because the header carries [`Payload::blob_digest`].
    /// Without it the header would distinguish embedded files by *length*
    /// alone, and two builds whose engines happened to be the same size would
    /// share a directory — the second one silently running the first one's
    /// engine. A test pins exactly that case.
    pub fn key(&self) -> u64 {
        self.key
    }

    /// Read one embedded file's bytes.
    pub fn read(&self, file: &EmbeddedFile) -> Result<Vec<u8>, PayloadError> {
        let unreadable = |source: io::Error| PayloadError::Unreadable {
            path: self.path.display().to_string(),
            source,
        };

        let end = file
            .offset
            .checked_add(file.length)
            .filter(|end| *end <= self.length)
            .ok_or_else(|| PayloadError::Truncated {
                path: self.path.display().to_string(),
                claimed: file.offset.saturating_add(file.length),
                actual: self.length,
            })?;

        let _ = end;

        let mut handle = File::open(&self.path).map_err(unreadable)?;
        handle
            .seek(SeekFrom::Start(self.start + file.offset))
            .map_err(unreadable)?;

        let mut bytes = vec![0_u8; file.length as usize];
        handle.read_exact(&mut bytes).map_err(unreadable)?;

        Ok(bytes)
    }
}

/// FNV-1a, 64-bit.
///
/// Hand-rolled, like the topological sort and the cron grammar, and for the
/// same reason: it is a dozen lines of well-understood arithmetic and the
/// alternative is a dependency. It is **not** a cryptographic hash and is not
/// used as one — nothing here decides whether to trust bytes, only whether a
/// directory extracted earlier belongs to this build.
fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x1000_0000_01b3;

    bytes.iter().fold(OFFSET, |hash, byte| {
        (hash ^ *byte as u64).wrapping_mul(PRIME)
    })
}

/// Mark a built artifact executable.
///
/// A no-op on Windows, where the extension decides. It matters for 9c: a Linux
/// binary cross-built here arrives with whatever mode `std::fs::copy` gave it,
/// and a file somebody cannot run is a confusing way to deliver a working
/// pipeline.
#[cfg(unix)]
fn make_executable(path: &Path) -> Result<(), PayloadError> {
    use std::os::unix::fs::PermissionsExt;

    let unwritable = |source: io::Error| PayloadError::Unwritable {
        path: path.display().to_string(),
        source,
    };

    let mut permissions = std::fs::metadata(path).map_err(unwritable)?.permissions();
    permissions.set_mode(permissions.mode() | 0o111);
    std::fs::set_permissions(path, permissions).map_err(unwritable)
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<(), PayloadError> {
    Ok(())
}
