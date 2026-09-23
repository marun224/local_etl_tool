//! XML, both ways.
//!
//! DuckDB has no core XML reader; the only route is a community extension,
//! which sits badly with an air-gapped, vendored extension set. Phase 4 deferred
//! XML here for that reason, and it is the connector that proves the bridge:
//! no network, no container, and a format everyone has.
//!
//! **The shape is flat, on purpose.** One element is one row -- `record` names
//! it -- and each of its children is a column. An attribute on the record is a
//! column named `@name`; an attribute on a child is `child@name`. Anything
//! deeper than one level of children is refused, by name and position, rather
//! than flattened by a rule somebody would have to guess. The writer produces
//! exactly the shape the reader takes, so a round trip is lossless for anything
//! that was flat to begin with.
//!
//! Every value read is text, because XML has no other kind. `columns` on the
//! node is where types come from.

use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    columns_property, ConnectorError, Context, Record, RecordReader, RecordWriter, Sink, Source,
    Summary,
};
use quick_xml::escape::{escape, resolve_predefined_entity};
use quick_xml::events::{BytesStart, Event};
use quick_xml::{Reader, XmlVersion};
use serde_json::Value as JsonValue;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

/// `src.file.xml`.
pub struct XmlSource;

/// `snk.file.xml`.
pub struct XmlSink;

impl Source for XmlSource {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("src.file.xml", "XML file")
            .description(
                "Read an XML file where one element is one row and its children are the columns.",
            )
            .icon("file-code")
            .properties(vec![
                PropertySpec::path("path")
                    .required()
                    .help("The file to read."),
                PropertySpec::text("record").required().help(
                    "The element that is one row, e.g. order. Its children become columns, its \
                     attributes columns named @name.",
                ),
                columns_property(),
            ])
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        check_name("record", required(properties, "record")?)
    }

    fn read(
        &self,
        properties: &JsonValue,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let path = required(properties, "path")?;
        let record = required(properties, "record")?;
        check_name("record", record)?;

        let resolved = context.resolve(path);
        let file = File::open(&resolved).map_err(|error| ConnectorError::io(&resolved, error))?;

        let records = read_records(BufReader::new(file), record, out)?;

        let detail = if records == 0 {
            // Not an error: an empty feed is a real thing. But a misspelt
            // `record` looks exactly like one, so it is said out loud.
            format!("no <{record}> elements in {path}; 0 records read")
        } else {
            format!("{records} record(s) read from {path}")
        };

        Ok(Summary::new(records, detail))
    }
}

impl Sink for XmlSink {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("snk.file.xml", "XML file")
            .description("Write rows as an XML file: one element per row, one child per column.")
            .icon("file-code")
            .properties(vec![
                PropertySpec::path("path")
                    .required()
                    .help("The file to write."),
                PropertySpec::text("root")
                    .default(JsonValue::String("records".into()))
                    .help("The element that wraps every row."),
                PropertySpec::text("record")
                    .default(JsonValue::String("record".into()))
                    .help("The element each row is written as."),
                PropertySpec::enumerated("mode", &["overwrite", "error_if_exists"])
                    .default(JsonValue::String("overwrite".into()))
                    .help("What to do when the file already exists."),
            ])
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        check_name("root", required(properties, "root")?)?;
        check_name("record", required(properties, "record")?)
    }

    fn write(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let path = required(properties, "path")?;
        let root = required(properties, "root")?;
        let record = required(properties, "record")?;
        check_name("root", root)?;
        check_name("record", record)?;

        let target = context.resolve(path);
        let records = write_atomically(&target, |out| write_records(out, root, record, input))?;

        Ok(Summary::new(
            records,
            format!("{records} record(s) written to {path}"),
        ))
    }
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// Where the reader is, relative to the element being read.
enum Position {
    /// Outside any record.
    Outside,
    /// Directly inside a record, between its children.
    InRecord,
    /// Inside one child of a record, collecting its text.
    InField { name: String, text: String },
}

/// Read every `record` element and write it out. Returns how many there were.
pub(crate) fn read_records<R: std::io::BufRead>(
    source: R,
    record: &str,
    out: &mut dyn RecordWriter,
) -> Result<u64, ConnectorError> {
    let mut reader = Reader::from_reader(source);
    let mut buffer = Vec::new();

    let mut position = Position::Outside;
    let mut current = Record::new();
    let mut count = 0u64;

    loop {
        let at = reader.buffer_position();
        let event = reader
            .read_event_into(&mut buffer)
            .map_err(|error| malformed(reader.error_position(), error))?;

        match (&mut position, event) {
            (_, Event::Eof) => break,

            // A record starts. Its attributes are columns straight away.
            (Position::Outside, Event::Start(element)) if local(&element) == record => {
                current = Record::new();
                take_attributes(&element, None, &mut current, at)?;
                position = Position::InRecord;
            }

            // A record with no children at all: `<order id="1"/>`.
            (Position::Outside, Event::Empty(element)) if local(&element) == record => {
                let mut only = Record::new();
                take_attributes(&element, None, &mut only, at)?;
                out.write(only)?;
                count += 1;
            }

            // Anything else outside a record is the document around the rows.
            (Position::Outside, _) => {}

            // A column starts.
            (Position::InRecord, Event::Start(element)) => {
                let name = local(&element);
                claim(&current, &name, record, at)?;
                take_attributes(&element, Some(&name), &mut current, at)?;
                position = Position::InField {
                    name,
                    text: String::new(),
                };
            }

            // An empty column: `<note/>` is an empty string, not a missing one.
            (Position::InRecord, Event::Empty(element)) => {
                let name = local(&element);
                claim(&current, &name, record, at)?;
                take_attributes(&element, Some(&name), &mut current, at)?;
                current.insert(name, JsonValue::String(String::new()));
            }

            // The record ends: write it.
            (Position::InRecord, Event::End(_)) => {
                out.write(std::mem::take(&mut current))?;
                count += 1;
                position = Position::Outside;
            }

            // Text directly inside a record, between its children. Whitespace is
            // indentation; anything else is mixed content, which has no column
            // to go in.
            (Position::InRecord, Event::Text(text)) => {
                let content = text.xml10_content().map_err(|e| malformed(at, e))?;
                if !content.trim().is_empty() {
                    return Err(ConnectorError::Data(format!(
                        "<{record}> at byte {at} holds text of its own beside its child elements; \
                         only one level of child elements can be read as columns"
                    )));
                }
            }

            (Position::InRecord, Event::CData(_)) | (Position::InRecord, Event::GeneralRef(_)) => {
                return Err(ConnectorError::Data(format!(
                    "<{record}> at byte {at} holds text of its own beside its child elements; \
                     only one level of child elements can be read as columns"
                )));
            }

            (Position::InRecord, _) => {}

            // Inside a column: collect its text, entity by entity.
            (Position::InField { text, .. }, Event::Text(part)) => {
                text.push_str(&part.xml10_content().map_err(|e| malformed(at, e))?);
            }

            (Position::InField { text, .. }, Event::CData(part)) => {
                text.push_str(&part.decode().map_err(|e| malformed(at, e))?);
            }

            (Position::InField { text, .. }, Event::GeneralRef(reference)) => {
                if let Some(character) =
                    reference.resolve_char_ref().map_err(|e| malformed(at, e))?
                {
                    text.push(character);
                } else {
                    let name = reference.decode().map_err(|e| malformed(at, e))?;
                    match resolve_predefined_entity(&name) {
                        Some(value) => text.push_str(value),
                        None => {
                            return Err(ConnectorError::Data(format!(
                                "unknown entity &{name}; at byte {at}"
                            )))
                        }
                    }
                }
            }

            // The column ends.
            (Position::InField { name, text }, Event::End(_)) => {
                current.insert(
                    std::mem::take(name),
                    JsonValue::String(std::mem::take(text)),
                );
                position = Position::InRecord;
            }

            // An element inside a column: one level too deep.
            (Position::InField { name, .. }, Event::Start(inner))
            | (Position::InField { name, .. }, Event::Empty(inner)) => {
                return Err(ConnectorError::Data(format!(
                    "<{name}> inside <{record}> at byte {at} contains <{}>; only one level of child \
                     elements can be read as columns",
                    local(&inner)
                )));
            }

            (Position::InField { .. }, _) => {}
        }

        buffer.clear();
    }

    Ok(count)
}

/// A column name may appear once per record. A second `<item>` in one record is
/// a list, and a list has no single column to go in.
fn claim(current: &Record, name: &str, record: &str, at: u64) -> Result<(), ConnectorError> {
    if current.contains_key(name) {
        return Err(ConnectorError::Data(format!(
            "<{name}> appears more than once in one <{record}> (at byte {at}); a repeated element \
             is a list, and only single values can be read as columns"
        )));
    }
    Ok(())
}

/// An element's attributes, as columns: `@key` on the record itself, and
/// `child@key` on one of its children.
fn take_attributes(
    element: &BytesStart<'_>,
    child: Option<&str>,
    into: &mut Record,
    at: u64,
) -> Result<(), ConnectorError> {
    for attribute in element.attributes() {
        let attribute = attribute.map_err(|e| malformed(at, e))?;
        let key = String::from_utf8_lossy(attribute.key.local_name().as_ref()).into_owned();
        // Normalised as the XML 1.0 specification says an attribute value is:
        // entities resolved, and tabs and line breaks read as spaces.
        let value = attribute
            .normalized_value(XmlVersion::Implicit1_0)
            .map_err(|e| malformed(at, e))?;

        let column = match child {
            None => format!("@{key}"),
            Some(child) => format!("{child}@{key}"),
        };
        into.insert(column, JsonValue::String(value.into_owned()));
    }
    Ok(())
}

/// An element's name without its namespace prefix: `<ns:order>` is `order`.
fn local(element: &BytesStart<'_>) -> String {
    String::from_utf8_lossy(element.local_name().as_ref()).into_owned()
}

fn malformed(at: u64, error: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::Data(format!("not well-formed XML at byte {at}: {error}"))
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

/// Write to a temporary file beside the target and rename it into place, so a
/// write that fails partway never leaves half a document where a whole one was.
fn write_atomically<F>(target: &Path, write: F) -> Result<u64, ConnectorError>
where
    F: FnOnce(&mut dyn Write) -> Result<u64, ConnectorError>,
{
    let partial = partial_path(target);

    let outcome = (|| {
        let file = File::create(&partial).map_err(|e| ConnectorError::io(&partial, e))?;
        let mut out = BufWriter::new(file);
        let records = write(&mut out)?;
        out.flush().map_err(|e| ConnectorError::io(&partial, e))?;
        drop(out);
        std::fs::rename(&partial, target).map_err(|e| ConnectorError::io(target, e))?;
        Ok(records)
    })();

    if outcome.is_err() {
        let _ = std::fs::remove_file(&partial);
    }

    outcome
}

fn partial_path(target: &Path) -> PathBuf {
    let mut name = target.file_name().unwrap_or_default().to_os_string();
    name.push(".partial");
    target.with_file_name(name)
}

/// Write the document. Returns how many records went in.
///
/// Laid out with two-space indentation and `\n` line ends on every platform, so
/// the same rows produce the same bytes wherever they are written.
pub(crate) fn write_records(
    out: &mut dyn Write,
    root: &str,
    record: &str,
    input: &mut dyn RecordReader,
) -> Result<u64, ConnectorError> {
    let io = |error: std::io::Error| ConnectorError::Data(format!("writing XML: {error}"));

    writeln!(out, r#"<?xml version="1.0" encoding="UTF-8"?>"#).map_err(io)?;
    writeln!(out, "<{root}>").map_err(io)?;

    let mut count = 0u64;

    while let Some(row) = input.read()? {
        count += 1;

        // The reader's naming, in reverse: `@key` is an attribute of the
        // record, `child@key` an attribute of that child, anything else a child.
        let mut attributes = String::new();
        let mut children: Vec<Child> = Vec::new();

        for (column, value) in &row {
            // A null is a column with no value, and the honest way to say that
            // in XML is to leave it out. Reading it back gives a NULL.
            let Some(text) = text_of(column, value, count)? else {
                continue;
            };
            let text = escape(text.as_str()).into_owned();

            if let Some(attribute) = column.strip_prefix('@') {
                check_column(attribute, column)?;
                attributes.push_str(&format!(" {attribute}=\"{text}\""));
            } else if let Some((child, attribute)) = column.split_once('@') {
                check_column(child, column)?;
                check_column(attribute, column)?;
                child_named(&mut children, child)
                    .attributes
                    .push_str(&format!(" {attribute}=\"{text}\""));
            } else {
                check_column(column, column)?;
                child_named(&mut children, column).text = Some(text);
            }
        }

        if children.is_empty() {
            writeln!(out, "  <{record}{attributes}/>").map_err(io)?;
            continue;
        }

        writeln!(out, "  <{record}{attributes}>").map_err(io)?;

        for child in &children {
            match &child.text {
                Some(text) => writeln!(
                    out,
                    "    <{name}{attributes}>{text}</{name}>",
                    name = child.name,
                    attributes = child.attributes
                ),
                // Only its attributes had values.
                None => writeln!(
                    out,
                    "    <{name}{attributes}/>",
                    name = child.name,
                    attributes = child.attributes
                ),
            }
            .map_err(io)?;
        }

        writeln!(out, "  </{record}>").map_err(io)?;
    }

    writeln!(out, "</{root}>").map_err(io)?;

    Ok(count)
}

/// One child element of a record being written, escaped and ready.
struct Child {
    name: String,
    text: Option<String>,
    attributes: String,
}

/// The child with this name, added at the position its first column appeared.
fn child_named<'a>(children: &'a mut Vec<Child>, name: &str) -> &'a mut Child {
    let index = match children.iter().position(|child| child.name == name) {
        Some(index) => index,
        None => {
            children.push(Child {
                name: name.to_string(),
                text: None,
                attributes: String::new(),
            });
            children.len() - 1
        }
    };
    &mut children[index]
}

/// A value as element text, or `None` for a null.
fn text_of(column: &str, value: &JsonValue, row: u64) -> Result<Option<String>, ConnectorError> {
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::String(text) => Ok(Some(text.clone())),
        JsonValue::Number(number) => Ok(Some(number.to_string())),
        JsonValue::Bool(flag) => Ok(Some(flag.to_string())),
        JsonValue::Array(_) | JsonValue::Object(_) => Err(ConnectorError::Data(format!(
            "column '{column}' in row {row} is a list or a struct, which has no flat XML form; \
             select or flatten it before this sink"
        ))),
    }
}

fn check_column(name: &str, column: &str) -> Result<(), ConnectorError> {
    if is_xml_name(name) {
        Ok(())
    } else {
        Err(ConnectorError::Data(format!(
            "column '{column}' is not a valid XML element or attribute name; rename it before this \
             sink (xf.rename)"
        )))
    }
}

fn check_name(property: &str, name: &str) -> Result<(), ConnectorError> {
    if is_xml_name(name) {
        Ok(())
    } else {
        Err(ConnectorError::property(
            property,
            format!("'{name}' is not a valid XML element name"),
        ))
    }
}

/// Whether text can be used as an element or attribute name.
///
/// The practical subset of XML's `Name` production: a letter or underscore
/// first, then letters, digits, `_`, `-` and `.`. No colon, because a prefix
/// would need a namespace declaration this writer does not make.
pub(crate) fn is_xml_name(name: &str) -> bool {
    let mut characters = name.chars();

    let Some(first) = characters.next() else {
        return false;
    };

    (first.is_alphabetic() || first == '_')
        && characters.all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

fn required<'a>(properties: &'a JsonValue, key: &str) -> Result<&'a str, ConnectorError> {
    match properties.get(key).and_then(JsonValue::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(ConnectorError::property(key, "is required")),
    }
}
