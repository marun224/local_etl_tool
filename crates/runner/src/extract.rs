//! Turning the blob region back into files DuckDB can use.
//!
//! The engine shells out to a DuckDB *binary* and DuckDB loads extensions from
//! a *directory*, so both have to exist on disk before a run. An artifact
//! carries them inside itself, which means the first thing it does is put them
//! somewhere.
//!
//! # Extracted once, not once per run
//!
//! Writing 37 MB of engine — and, for a pipeline that reads Postgres, another
//! 28 MB of extension — before every run would make a scheduled pipeline spend
//! most of its time copying itself. So extraction is keyed on the payload's
//! [`Blobs::key`] and skipped when that directory is already complete. A
//! different build gets a different key and its own directory; the same build
//! run a thousand times extracts once.
//!
//! # Two processes at once
//!
//! A scheduler firing two artifacts at the same moment, or a person running one
//! twice, must not have them writing the same files over each other. Extraction
//! therefore goes to a **private directory** named for the process, and the
//! last step is a single [`rename`](std::fs::rename) into place. Two processes
//! racing produce one winner and one loser; the loser notices the destination
//! now exists, throws its own work away and uses the winner's. That is the same
//! posture the scheduler's lock file takes, and it needs no lock at all.
//!
//! # Where
//!
//! The system temp directory, not beside the executable: an artifact is
//! routinely dropped somewhere read-only, and `/opt` or `C:\Program Files` is
//! exactly where somebody would put one. `ETL_RUNNER_CACHE` overrides it, which
//! is what a locked-down deployment with no writable temp needs.

use crate::{Blobs, Payload, PayloadError, Role};
use std::path::{Path, PathBuf};

#[cfg(test)]
#[path = "extract/tests.rs"]
mod tests;

/// The environment variable that chooses where an artifact unpacks itself.
pub const CACHE_ENV: &str = "ETL_RUNNER_CACHE";

/// Where the engine and its extensions ended up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extracted {
    /// The DuckDB binary to run, if one was embedded.
    pub duckdb_bin: Option<PathBuf>,

    /// What to hand `SET extension_directory`, if any extension was embedded.
    ///
    /// The directory *above* `<version>/<platform>/`, because that is the shape
    /// DuckDB expects to be pointed at.
    pub extension_dir: Option<PathBuf>,

    /// The directory everything was written into.
    pub root: PathBuf,

    /// Whether this run did the extracting, as opposed to finding it done.
    /// Reported by `--info` and useful when a first run looks slow.
    pub freshly_extracted: bool,
}

/// A name that cannot escape the directory it is written into.
///
/// A built artifact is *data*: somebody can hand you one, and its header is
/// whatever they wrote. A name of `../../../etc/cron.d/anything` would
/// otherwise be written wherever the relative path led. Checked rather than
/// sanitised, because a name that needs fixing is a name worth refusing.
fn is_safe_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains('\0')
        // A Windows drive-relative name like `C:evil` is not a separator and is
        // still not a bare filename.
        && !name.contains(':')
}

impl Payload {
    /// Put the embedded engine and extensions on disk, and say where.
    ///
    /// Does nothing and reports nothing when the payload carries no files,
    /// which is every artifact 9a built: those still find DuckDB the way the
    /// CLI does.
    pub fn extract(&self, blobs: &Blobs) -> Result<Extracted, PayloadError> {
        self.extract_into(blobs, &cache_base())
    }

    /// [`extract`](Self::extract), into a base directory of the caller's
    /// choosing.
    ///
    /// The seam exists because the base is otherwise read from the environment,
    /// and a test that sets an environment variable sets it for every other test
    /// in the process. Taking it as an argument is what lets these run in
    /// parallel, and it is the honest shape anyway: where to unpack is an input.
    pub fn extract_into(&self, blobs: &Blobs, base: &Path) -> Result<Extracted, PayloadError> {
        let root = self.cache_dir_in(base, blobs);

        if self.files.is_empty() {
            return Ok(Extracted {
                duckdb_bin: None,
                extension_dir: None,
                root,
                freshly_extracted: false,
            });
        }

        for file in &self.files {
            if !is_safe_name(&file.name) {
                return Err(PayloadError::UnsafeName {
                    name: file.name.clone(),
                });
            }
        }

        // The marker is written last, inside the private directory, so a
        // directory that exists but was interrupted before the rename never
        // looks complete. Anything half-written is in a `.partial-*` nobody
        // reads.
        let complete = root.join(COMPLETE_MARKER);
        let freshly_extracted = !complete.is_file();

        if freshly_extracted {
            self.unpack_into_place(blobs, &root)?;
        }

        Ok(Extracted {
            duckdb_bin: self.engine_at(&root),
            extension_dir: self.extension_root(&root),
            root,
            freshly_extracted,
        })
    }

    /// Where this build unpacks to.
    pub fn cache_dir(&self, blobs: &Blobs) -> PathBuf {
        self.cache_dir_in(&cache_base(), blobs)
    }

    /// Where this build unpacks to, under a given base.
    pub fn cache_dir_in(&self, base: &Path, blobs: &Blobs) -> PathBuf {
        // The key alone would be enough; the name is there so that somebody
        // looking at a temp directory full of these can tell which is which.
        base.join("etl-runner")
            .join(format!("{}-{:016x}", sanitise(&self.name), blobs.key()))
    }

    fn unpack_into_place(&self, blobs: &Blobs, root: &Path) -> Result<(), PayloadError> {
        let parent = root.parent().unwrap_or(root);
        let private = parent.join(format!(
            ".partial-{}-{}",
            root.file_name().unwrap_or_default().to_string_lossy(),
            std::process::id()
        ));

        let _ = std::fs::remove_dir_all(&private);

        let unwritable = |path: &Path| {
            let path = path.display().to_string();
            move |source| PayloadError::Unwritable {
                path: path.clone(),
                source,
            }
        };

        std::fs::create_dir_all(&private).map_err(unwritable(&private))?;

        for file in &self.files {
            let destination = match file.role {
                Role::Engine => private.join(&file.name),
                Role::Extension => {
                    // `<root>/extensions/<version>/<platform>/<name>`, which is
                    // where DuckDB resolves an extension to once it has been
                    // pointed at `<root>/extensions`.
                    let directory = self.extension_dir_for(&private);
                    std::fs::create_dir_all(&directory).map_err(unwritable(&directory))?;
                    directory.join(&file.name)
                }
            };

            std::fs::write(&destination, blobs.read(file)?).map_err(unwritable(&destination))?;

            if file.executable {
                make_runnable(&destination)?;
            }
        }

        std::fs::write(private.join(COMPLETE_MARKER), self.built_at.as_bytes())
            .map_err(unwritable(&private))?;

        // An earlier run that died between creating the directory and writing
        // the marker leaves one that `rename` will not land on — Windows refuses
        // to rename onto a non-empty directory, and POSIX refuses too. Nothing
        // reads a directory without a marker, so clearing it is safe.
        if root.exists() && !root.join(COMPLETE_MARKER).is_file() {
            let _ = std::fs::remove_dir_all(root);
        }

        // The one step that publishes the work. Everything above happened where
        // nothing else was looking.
        match std::fs::rename(&private, root) {
            Ok(()) => Ok(()),

            Err(_) if root.join(COMPLETE_MARKER).is_file() => {
                // Another process got there first with the same key, which means
                // the same bytes. Its copy is as good as ours, so ours goes in
                // the bin rather than over the top of a file something may
                // already have open.
                let _ = std::fs::remove_dir_all(&private);
                Ok(())
            }

            Err(source) => {
                let _ = std::fs::remove_dir_all(&private);
                Err(PayloadError::Unwritable {
                    path: root.display().to_string(),
                    source,
                })
            }
        }
    }

    /// The embedded engine's path under `root`, if one was embedded.
    fn engine_at(&self, root: &Path) -> Option<PathBuf> {
        self.files
            .iter()
            .find(|file| file.role == Role::Engine)
            .map(|file| root.join(&file.name))
    }

    /// The directory to hand `SET extension_directory`, if anything needs one.
    ///
    /// `<root>/extensions`, with the files themselves a further
    /// `<version>/<platform>/` down, because that is the layout DuckDB resolves
    /// against and it is not ours to choose.
    fn extension_root(&self, root: &Path) -> Option<PathBuf> {
        self.files
            .iter()
            .any(|file| file.role == Role::Extension)
            .then(|| root.join(EXTENSIONS_DIR))
    }

    /// The directory an extension file itself has to sit in, which is a
    /// version and a platform below what DuckDB is pointed at.
    pub fn extension_dir_for(&self, root: &Path) -> PathBuf {
        root.join(EXTENSIONS_DIR)
            .join(&self.duckdb_version)
            .join(&self.platform)
    }
}

/// Where artifacts unpack unless told otherwise.
fn cache_base() -> PathBuf {
    std::env::var_os(CACHE_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

/// The directory name DuckDB is pointed at.
pub const EXTENSIONS_DIR: &str = "extensions";

/// Written last, and the only evidence a directory is finished.
pub const COMPLETE_MARKER: &str = ".complete";

/// A pipeline name that is safe as a directory component.
fn sanitise(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => character,
            _ => '_',
        })
        .collect();

    if cleaned.is_empty() {
        "pipeline".to_string()
    } else {
        cleaned
    }
}

/// Make an extracted engine runnable.
#[cfg(unix)]
fn make_runnable(path: &Path) -> Result<(), PayloadError> {
    use std::os::unix::fs::PermissionsExt;

    let unwritable = |source| PayloadError::Unwritable {
        path: path.display().to_string(),
        source,
    };

    let mut permissions = std::fs::metadata(path).map_err(unwritable)?.permissions();
    permissions.set_mode(permissions.mode() | 0o755);
    std::fs::set_permissions(path, permissions).map_err(unwritable)
}

#[cfg(not(unix))]
fn make_runnable(_path: &Path) -> Result<(), PayloadError> {
    Ok(())
}
