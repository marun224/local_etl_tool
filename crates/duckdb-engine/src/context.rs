//! Named sets of variables, so one document runs in several environments.
//!
//! A context is the answer to "the same pipeline, but pointing at prod". It
//! lives in the workspace rather than in the document, because which
//! environment you are in is a property of the machine you are on, not of the
//! pipeline — committing `prod` into the pipeline file is exactly the thing
//! this avoids.
//!
//! The file is `.etl/contexts.json` under the workspace root. `.etl/` is
//! workspace state and is git-ignored, which is the right default for a file
//! holding hostnames and paths; a team that wants to share one can point at it
//! explicitly instead.

use crate::params::Resolver;
use etl_metadata::Extra;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// The format version this crate writes.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

/// Where the contexts file sits, relative to the workspace root.
pub const CONTEXTS_PATH: &str = ".etl/contexts.json";

#[derive(Debug, Error)]
pub enum ContextError {
    #[error("could not read {path}: {source}")]
    Unreadable {
        path: String,
        #[source]
        source: io::Error,
    },

    #[error("{path} is not valid JSON: {source}")]
    Malformed {
        path: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("there is no context named '{name}'{}", available(.known))]
    Unknown { name: String, known: Vec<String> },

    #[error(
        "'{name}' is the active context in {path}, but no context of that name is defined there"
    )]
    ActiveMissing { name: String, path: String },
}

fn available(known: &[String]) -> String {
    if known.is_empty() {
        ". This workspace defines none".to_string()
    } else {
        format!(". Defined: {}", known.join(", "))
    }
}

/// One environment's variables.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Context {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: Extra,
}

/// The whole `.etl/contexts.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Contexts {
    #[serde(default, rename = "formatVersion", skip_serializing_if = "is_zero")]
    pub format_version: u32,
    /// Which context is used when the command line does not say. Absent means
    /// no context, which is a legitimate state rather than a misconfiguration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<String>,
    #[serde(default)]
    pub contexts: BTreeMap<String, Context>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: Extra,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

impl Contexts {
    pub fn from_json(text: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(text)
    }

    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Read the contexts file, or return an empty set when there is none.
    ///
    /// A missing file is not an error: most workspaces never define a context,
    /// and a pipeline that needs none should not have to create one.
    pub fn load(path: &Path) -> Result<Self, ContextError> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(source) => {
                return Err(ContextError::Unreadable {
                    path: path.display().to_string(),
                    source,
                })
            }
        };

        let loaded = Self::from_json(&text).map_err(|source| ContextError::Malformed {
            path: path.display().to_string(),
            source,
        })?;

        // An `active` naming a context that is not there means every run picks
        // up the wrong environment silently. Better to say so on load.
        if let Some(active) = &loaded.active {
            if !loaded.contexts.contains_key(active) {
                return Err(ContextError::ActiveMissing {
                    name: active.clone(),
                    path: path.display().to_string(),
                });
            }
        }

        Ok(loaded)
    }

    /// The contexts file for a workspace, whether or not it exists.
    pub fn path_in(workspace: &Path) -> PathBuf {
        workspace.join(CONTEXTS_PATH)
    }

    /// Read the contexts file for a workspace.
    pub fn load_from_workspace(workspace: &Path) -> Result<Self, ContextError> {
        Self::load(&Self::path_in(workspace))
    }

    pub fn names(&self) -> Vec<&str> {
        self.contexts.keys().map(String::as_str).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.contexts.is_empty()
    }

    /// Load every context into a resolver and choose the active one.
    ///
    /// All of them are loaded, not only the active one, because
    /// `${Other.VARIABLE}` names a context explicitly and has to reach it.
    ///
    /// `requested` overrides the file's own `active`. `Some(name)` for a
    /// context that is not defined is an error rather than a silent fallback:
    /// running against dev because prod was misspelled is the worst outcome
    /// available.
    pub fn apply(
        &self,
        mut resolver: Resolver,
        requested: Option<&str>,
    ) -> Result<Resolver, ContextError> {
        for (name, context) in &self.contexts {
            resolver = resolver.context(name, context.variables.clone());
        }

        let chosen = match requested {
            Some(name) => {
                if !self.contexts.contains_key(name) {
                    return Err(ContextError::Unknown {
                        name: name.to_string(),
                        known: self.contexts.keys().cloned().collect(),
                    });
                }
                Some(name)
            }
            None => self.active.as_deref(),
        };

        Ok(resolver.activate(chosen))
    }
}

#[cfg(test)]
mod tests;
