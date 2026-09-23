//! The contract between the engine and a connector written in Rust.
//!
//! Everything else in this workspace reaches data through DuckDB: a component
//! is a spec and a builder that turns a node into SQL. A **native** component
//! is for the data DuckDB cannot reach -- a format with no reader, an API with
//! no extension. It is still registered as a spec, so the canvas, validation
//! and lineage treat it like any other component; what differs is that rows
//! cross between it and DuckDB through a staging file.
//!
//! - A [`Source`] is handed a [`RecordWriter`] and writes the records it reads.
//!   The engine puts them in a JSON Lines file before DuckDB starts, and the
//!   node's SQL is an ordinary view over that file.
//! - A [`Sink`] is handed a [`RecordReader`] over the file DuckDB wrote for it,
//!   after a run that succeeded, and delivers the records wherever they go.
//!
//! A record is a JSON object because that is what a line of JSON Lines holds.
//! Types travel as far as JSON carries them; a source that knows better declares
//! `columns` (see [`columns_property`]) and DuckDB casts on the way in.
//!
//! This crate does no I/O. The connectors live in `etl-connectors`, and the
//! staging files belong to the engine.

use etl_metadata::{ComponentSpec, PropertySpec};
use serde_json::{Map, Value as JsonValue};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[cfg(test)]
mod tests;

/// One row, as it crosses between a connector and DuckDB.
pub type Record = Map<String, JsonValue>;

/// Why a connector could not do its job.
///
/// Every message is shown to a person, so each variant names what it is about:
/// the property, the file, or the record. The engine masks secret values out of
/// the text before anyone sees it, the same as it does for DuckDB's errors.
#[derive(Debug, Error)]
pub enum ConnectorError {
    /// The node's configuration cannot work, found before or while reading.
    #[error("property '{property}': {reason}")]
    Property { property: String, reason: String },

    /// The data is not what the connector can take: a malformed document, a
    /// value that has no representation in the target.
    #[error("{0}")]
    Data(String),

    /// The outside world refused: a file that will not open, a write that fails.
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// The staging file between the connector and DuckDB could not be read or
    /// written. Raised by the engine's side of the bridge, not by connectors.
    #[error("staging: {0}")]
    Staging(String),
}

impl ConnectorError {
    pub fn property(property: &str, reason: impl Into<String>) -> Self {
        Self::Property {
            property: property.to_string(),
            reason: reason.into(),
        }
    }

    pub fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.display().to_string(),
            source,
        }
    }
}

/// Where a source puts the records it reads.
pub trait RecordWriter {
    fn write(&mut self, record: Record) -> Result<(), ConnectorError>;
}

/// Where a sink takes the records it delivers from.
pub trait RecordReader {
    /// The next record, or `None` when there are no more.
    fn read(&mut self) -> Result<Option<Record>, ConnectorError>;
}

/// Collecting into memory. For tests, and for anything small enough.
impl RecordWriter for Vec<Record> {
    fn write(&mut self, record: Record) -> Result<(), ConnectorError> {
        self.push(record);
        Ok(())
    }
}

/// Records from memory, for tests.
pub struct Records<I>(pub I);

impl<I: Iterator<Item = Record>> RecordReader for Records<I> {
    fn read(&mut self) -> Result<Option<Record>, ConnectorError> {
        Ok(self.0.next())
    }
}

/// What a connector needs to know about the run it is part of.
#[derive(Debug, Clone, Default)]
pub struct Context {
    /// Where relative paths resolve from: the workspace, as it does for every
    /// other component. `None` means the current directory.
    pub working_dir: Option<PathBuf>,
}

impl Context {
    /// A path from a node's properties, against the workspace.
    pub fn resolve(&self, path: &str) -> PathBuf {
        match &self.working_dir {
            Some(directory) => directory.join(path),
            None => PathBuf::from(path),
        }
    }
}

/// What a connector did, for the run report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    /// Records read or delivered.
    pub records: u64,
    /// One line for the report, in the connector's own words: where the records
    /// came from or went, and anything worth knowing about how it went.
    pub detail: String,
}

/// A component that reads records in from somewhere DuckDB cannot reach.
pub trait Source: Send + Sync {
    /// The spec, in the `src.*` namespace. It should include
    /// [`columns_property`], because the engine reads `columns` to type the
    /// staged records.
    fn spec(&self) -> ComponentSpec;

    /// Refuse a configuration that cannot work, before anything runs.
    ///
    /// The spec checks each property on its own -- present, the right type,
    /// one of the options. This is for rules that span properties, such as
    /// "cursor pagination needs `cursor_path`". The engine calls it while
    /// compiling, so `etl validate` and the canvas catch the mistake instead
    /// of the first page of a run. Touch nothing: no files, no network.
    fn check(&self, _properties: &JsonValue) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Read everything and write each record to `out`.
    ///
    /// `properties` are resolved: parameters, contexts and secrets substituted,
    /// defaults applied, and each value checked against the spec's type.
    fn read(
        &self,
        properties: &JsonValue,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError>;
}

/// A component that delivers records somewhere DuckDB cannot write.
pub trait Sink: Send + Sync {
    /// The spec, in the `snk.*` namespace. A `path` property, if it has one, is
    /// where the engine checks `mode` and creates the parent directory, as it
    /// does for every file sink.
    fn spec(&self) -> ComponentSpec;

    /// Refuse a configuration that cannot work, before anything runs. The
    /// same contract as [`Source::check`].
    fn check(&self, _properties: &JsonValue) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Deliver every record `input` holds. Called only after the run that
    /// produced them succeeded.
    fn write(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        context: &Context,
    ) -> Result<Summary, ConnectorError>;
}

/// A native component, either way round.
#[derive(Clone, Copy)]
pub enum Connector {
    Source(&'static dyn Source),
    Sink(&'static dyn Sink),
}

impl Connector {
    pub fn spec(&self) -> ComponentSpec {
        match self {
            Connector::Source(source) => source.spec(),
            Connector::Sink(sink) => sink.spec(),
        }
    }

    pub fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        match self {
            Connector::Source(source) => source.check(properties),
            Connector::Sink(sink) => sink.check(properties),
        }
    }
}

/// The `columns` property every native source should offer.
///
/// JSON carries strings, numbers and booleans and nothing finer, and most of
/// what a native source reads -- XML, most APIs -- is text. Left unset, DuckDB
/// infers types the way it does for every other source: ISO dates and
/// timestamps become typed, and the rest stays as the connector wrote it. Set,
/// DuckDB casts each named column on the way in, and only the named columns
/// are read.
pub fn columns_property() -> PropertySpec {
    PropertySpec::map("columns").help(
        "Column name to SQL type, e.g. amount = DOUBLE. Unset, DuckDB infers types: ISO dates \
         and timestamps are recognised and everything else stays text. Set, only these columns \
         are read, cast to these types.",
    )
}
