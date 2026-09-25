//! `xf.ai.embed`: a vector for each text, from a small embedding model on this
//! machine (Phase 11d2; Settled decisions 109 and 110).
//!
//! The model is bge-small-en-v1.5, fetched by `scripts/fetch-model.ps1`, run by
//! the same `llama-server` as `etl assist` with `--embeddings`, for the length
//! of the stage. Texts go in batches; the vectors come back L2-normalised, so
//! a dot product is their cosine.

use etl_assistant::{locate_embedder, locate_server, Server};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    ConnectorError, Context, Record, RecordReader, RecordWriter, Summary, Transform, ROW_KEY,
};
use serde_json::{json, Value as JsonValue};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Texts sent to the model in one request.
const BATCH: usize = 32;

/// bge-small's context: an input longer than this many tokens is refused.
const MODEL_TOKENS: &str = "512";

pub struct EmbedTransform;

struct Settings {
    column: String,
    output: String,
    dimensions: usize,
}

impl Settings {
    fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let text = |key: &str| {
            properties[key]
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| ConnectorError::property(key, "must not be empty"))
        };
        let output = text("output")?;
        if output == ROW_KEY {
            return Err(ConnectorError::property(
                "output",
                format!("{ROW_KEY} is the engine's"),
            ));
        }
        let dimensions = properties["dimensions"]
            .as_u64()
            .filter(|dimensions| *dimensions >= 1)
            .ok_or_else(|| ConnectorError::property("dimensions", "must be at least 1"))?;
        Ok(Settings {
            column: text("column")?,
            output,
            dimensions: dimensions as usize,
        })
    }
}

impl Transform for EmbedTransform {
    fn spec(&self) -> ComponentSpec {
        ComponentSpec::new("xf.ai.embed", "Embed text")
            .description(
                "A vector for each text, from a small embedding model on this machine, for \
                 similarity search.",
            )
            .icon("sparkles")
            .properties(vec![
                PropertySpec::text("column").required().help(
                    "The text to embed. A row with no text gets a null vector. At most 512 \
                     tokens, about 2,000 characters of English: split longer text with \
                     xf.ai.chunk first.",
                ),
                PropertySpec::text("output")
                    .default(json!("embedding"))
                    .help(
                        "The vector's column, FLOAT[dimensions]: array_cosine_similarity compares \
                         two, and DuckDB's vss extension can index them.",
                    ),
                PropertySpec::integer("dimensions")
                    .default(json!(384))
                    .help(
                    "The model's vector length: 384 for the vendored bge-small-en-v1.5. Change \
                     it only with another model (ETL_EMBED_MODEL).",
                ),
            ])
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        Settings::from(properties).map(|_| ())
    }

    fn reads(&self, properties: &JsonValue) -> Vec<String> {
        properties["column"]
            .as_str()
            .map(str::to_string)
            .into_iter()
            .collect()
    }

    fn adds(&self, properties: &JsonValue) -> Vec<(String, String)> {
        let output = properties["output"].as_str().unwrap_or("embedding");
        let dimensions = properties["dimensions"].as_u64().unwrap_or(384);
        vec![(output.to_string(), format!("FLOAT[{dimensions}]"))]
    }

    /// The model is a file on this machine, which a built executable does not
    /// carry (decision 114).
    fn portable(&self, _properties: &JsonValue) -> bool {
        false
    }

    fn transform(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let settings = Settings::from(properties)?;
        let started = Instant::now();
        let (server, model) = tools(context)?;

        let log = std::env::temp_dir().join(format!("etl-embed-{}.log", std::process::id()));
        let running = Server::spawn_with(
            &server,
            &model,
            &log,
            &[
                "--embeddings",
                "--parallel",
                "1",
                "--ctx-size",
                MODEL_TOKENS,
                "--batch-size",
                MODEL_TOKENS,
                "--ubatch-size",
                MODEL_TOKENS,
            ],
        )
        .map_err(ConnectorError::Data)?;
        running.wait_until_loaded().map_err(ConnectorError::Data)?;

        let mut batch: Vec<(JsonValue, String)> = Vec::with_capacity(BATCH);
        let (mut embedded, mut without) = (0u64, 0u64);
        while let Some(record) = input.read()? {
            match text_of(&record, &settings.column) {
                Some(text) => batch.push((record.get(ROW_KEY).cloned().unwrap_or_default(), text)),
                None => without += 1,
            }
            if batch.len() == BATCH {
                embedded += embed(&running, &mut batch, &settings, out)?;
            }
        }
        embedded += embed(&running, &mut batch, &settings, out)?;
        drop(running);
        let _ = std::fs::remove_file(&log);

        let name = model.file_name().unwrap_or_default().to_string_lossy();
        let mut detail = format!(
            "{embedded} text(s) embedded with {name} in {:.1}s",
            started.elapsed().as_secs_f64()
        );
        if without > 0 {
            detail.push_str(&format!("; {without} row(s) had no text"));
        }
        Ok(Summary::new(embedded, detail))
    }
}

/// `llama-server` and the model, from the workspace or from beside this
/// executable.
fn tools(context: &Context) -> Result<(PathBuf, PathBuf), ConnectorError> {
    let mut starts: Vec<PathBuf> = Vec::new();
    starts.push(
        context
            .working_dir
            .clone()
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from(".")),
    );
    if let Some(beside) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf))
    {
        starts.push(beside);
    }

    let find = |locate: fn(Option<&Path>, &Path) -> Result<PathBuf, String>| {
        let mut first = None;
        for start in &starts {
            match locate(None, start) {
                Ok(found) => return Ok(found),
                Err(message) => {
                    first.get_or_insert(message);
                }
            }
        }
        Err(ConnectorError::Data(first.unwrap_or_default()))
    };
    Ok((find(locate_server)?, find(locate_embedder)?))
}

/// The text to embed, or `None` for a row with nothing to embed.
fn text_of(record: &Record, column: &str) -> Option<String> {
    let text = match record.get(column)? {
        JsonValue::Null => return None,
        JsonValue::String(text) => text.clone(),
        other => other.to_string(),
    };
    (!text.trim().is_empty()).then_some(text)
}

/// Embed a batch and write each vector with its row's key. Empties the batch.
fn embed(
    server: &Server,
    batch: &mut Vec<(JsonValue, String)>,
    settings: &Settings,
    out: &mut dyn RecordWriter,
) -> Result<u64, ConnectorError> {
    if batch.is_empty() {
        return Ok(0);
    }
    let texts: Vec<&str> = batch.iter().map(|(_, text)| text.as_str()).collect();
    let answer = server
        .post("/v1/embeddings", &json!({ "input": texts }))
        .map_err(|message| {
            let hint = if message.contains("too large") {
                " A text is longer than the model takes (512 tokens, about 2,000 characters of \
                 English); split it with xf.ai.chunk first."
            } else {
                ""
            };
            ConnectorError::Data(format!("{message}{hint}"))
        })?;

    let mut vectors: Vec<(usize, JsonValue)> = answer["data"]
        .as_array()
        .ok_or_else(|| {
            ConnectorError::Data(format!("llama-server's answer has no data: {answer}"))
        })?
        .iter()
        .map(|item| {
            (
                item["index"].as_u64().unwrap_or(0) as usize,
                item["embedding"].clone(),
            )
        })
        .collect();
    vectors.sort_by_key(|(index, _)| *index);
    if vectors.len() != batch.len() {
        return Err(ConnectorError::Data(format!(
            "asked for {} vectors and got {}",
            batch.len(),
            vectors.len()
        )));
    }

    for ((key, _), (_, vector)) in batch.iter().zip(vectors) {
        let length = vector.as_array().map(Vec::len).unwrap_or(0);
        if length != settings.dimensions {
            return Err(ConnectorError::property(
                "dimensions",
                format!(
                    "is {}, but the model gives vectors of {length}; set it to {length}",
                    settings.dimensions
                ),
            ));
        }
        let mut record = Record::new();
        record.insert(ROW_KEY.to_string(), key.clone());
        record.insert(settings.output.clone(), vector);
        out.write(record)?;
    }
    let count = batch.len() as u64;
    batch.clear();
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties() -> JsonValue {
        json!({ "column": "body", "output": "embedding", "dimensions": 384 })
    }

    #[test]
    fn it_reads_its_column_and_adds_a_typed_vector() {
        assert_eq!(EmbedTransform.reads(&properties()), ["body"]);
        assert_eq!(
            EmbedTransform.adds(&properties()),
            [("embedding".to_string(), "FLOAT[384]".to_string())]
        );
        assert!(
            !EmbedTransform.portable(&properties()),
            "a built executable has no model"
        );
    }

    #[test]
    fn a_configuration_that_cannot_work_is_refused_before_anything_runs() {
        let refused = |change: JsonValue| {
            let mut properties = properties();
            for (key, value) in change.as_object().unwrap() {
                properties[key] = value.clone();
            }
            EmbedTransform.check(&properties).unwrap_err().to_string()
        };

        assert!(refused(json!({ "dimensions": 0 })).contains("dimensions"));
        assert!(refused(json!({ "output": ROW_KEY })).contains("engine's"));
        assert!(refused(json!({ "column": " " })).contains("column"));
    }

    #[test]
    fn a_row_with_no_text_is_not_sent() {
        let record = |value: JsonValue| {
            let mut record = Record::new();
            record.insert("body".into(), value);
            record
        };

        assert_eq!(
            text_of(&record(json!("hello")), "body").as_deref(),
            Some("hello")
        );
        assert_eq!(text_of(&record(json!(42)), "body").as_deref(), Some("42"));
        assert_eq!(text_of(&record(json!(null)), "body"), None);
        assert_eq!(text_of(&record(json!("  ")), "body"), None);
        assert_eq!(text_of(&Record::new(), "body"), None);
    }
}
