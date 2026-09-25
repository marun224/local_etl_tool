//! `xf.ai.prompt`, `xf.ai.classify` and `xf.ai.extract`: ask a model about
//! each row (Phase 11d3; Settled decisions 111–113).
//!
//! Any OpenAI-compatible endpoint (`base_url` + `/chat/completions`), or,
//! with no `base_url`, the model `etl assist` runs, started for the stage, so
//! all three work offline. What is shared lives here: the guardrails (a row cap
//! refused before any call, a number of calls at once, a timeout and retries
//! per call through the web connectors' [`Client`]), and a call that still
//! fails failing the stage, naming its row.
//!
//! **Rows leave the machine** when `base_url` names another one; each
//! component's help says so. The key is a `${SECRET:...}` reference, resolved
//! by the engine and masked in every message.

use crate::http::{positive, text, whole, Client, Settings as HttpSettings};
use etl_assistant::{locate_model, locate_server, Server};
use etl_metadata::{ComponentSpec, PropertySpec};
use etl_plugin_sdk::{
    ConnectorError, Context, Record, RecordReader, RecordWriter, Summary, Transform, ROW_KEY,
};
use serde_json::{json, Map, Value as JsonValue};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

// ---------------------------------------------------------------------------
// What the three share
// ---------------------------------------------------------------------------

/// The endpoint and the guardrails, which every one of the three offers.
fn endpoint_properties() -> Vec<PropertySpec> {
    vec![
        PropertySpec::text("base_url").help(
            "An OpenAI-compatible API, e.g. https://api.openai.com/v1; /chat/completions is \
             added. Set, each row's text is sent there and leaves this machine. Unset, the \
             local model etl assist uses answers (scripts/fetch-model.ps1), and nothing leaves.",
        ),
        PropertySpec::text("model").help("The endpoint's model name. Required with base_url."),
        PropertySpec::text("api_key")
            .help("Sent as a bearer token. Use ${SECRET:name} rather than the key itself."),
        PropertySpec::integer("max_rows").default(json!(1000)).help(
            "Refuse, before any call, an input with more rows than this: each row is one call, \
             and a call can cost money.",
        ),
        PropertySpec::integer("concurrency")
            .default(json!(4))
            .help("Calls at once."),
        PropertySpec::integer("timeout_seconds")
            .default(json!(60))
            .help("How long one call may take."),
        PropertySpec::integer("retries").default(json!(2)).help(
            "Extra attempts after a 429, a 5xx or a network failure. A call that still fails \
             fails the stage.",
        ),
    ]
}

struct Endpoint {
    base_url: Option<String>,
    model: String,
    api_key: Option<String>,
    max_rows: usize,
    concurrency: usize,
    timeout_seconds: u64,
    retries: u64,
}

impl Endpoint {
    fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let base_url =
            text(properties, "base_url").map(|url| url.trim_end_matches('/').to_string());
        if let Some(url) = &base_url {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(ConnectorError::property(
                    "base_url",
                    "must start with http:// or https://",
                ));
            }
        }
        let model = match (text(properties, "model"), &base_url) {
            (Some(model), _) => model.to_string(),
            (None, None) => "local".to_string(),
            (None, Some(_)) => {
                return Err(ConnectorError::property(
                    "model",
                    "is required with base_url",
                ))
            }
        };
        Ok(Endpoint {
            base_url,
            model,
            api_key: text(properties, "api_key").map(str::to_string),
            max_rows: positive(properties, "max_rows", 1000)? as usize,
            concurrency: positive(properties, "concurrency", 4)? as usize,
            timeout_seconds: positive(properties, "timeout_seconds", 60)?,
            retries: whole(properties, "retries", 2)?,
        })
    }

    /// The web connectors' client, set up for `url`: bearer auth when there is
    /// a key, and their retry rules.
    fn client(&self, url: &str) -> Result<Client, ConnectorError> {
        let mut properties = json!({
            "url": url,
            "timeout_ms": self.timeout_seconds * 1000,
            "retries": self.retries,
        });
        if let Some(key) = &self.api_key {
            properties["auth"] = json!("bearer");
            properties["token"] = json!(key);
        }
        Ok(Client::new(HttpSettings::posting(&properties)?))
    }
}

/// What one of the three asks and makes of the answer.
trait Question: Sync {
    /// The chat messages for one row.
    fn messages(&self, row: &Record) -> Vec<JsonValue>;
    /// The JSON Schema the answer must match, if it has one to match.
    fn schema(&self) -> Option<JsonValue>;
    /// The columns the answer's text becomes.
    fn answer(&self, content: &str) -> Result<Record, String>;
    /// How long an answer may be.
    fn max_tokens(&self) -> u64 {
        512
    }
}

/// Ask `question` of every row: all read first, so the cap is refused before
/// any call, then `concurrency` at a time, written back in row order.
fn ask_rows(
    endpoint: &Endpoint,
    question: &dyn Question,
    input: &mut dyn RecordReader,
    out: &mut dyn RecordWriter,
    context: &Context,
) -> Result<Summary, ConnectorError> {
    let started = Instant::now();
    let mut rows = Vec::new();
    while let Some(row) = input.read()? {
        if rows.len() == endpoint.max_rows {
            return Err(ConnectorError::property(
                "max_rows",
                format!(
                    "the input has more than {} rows, and each is a call to the model; nothing \
                     was sent. Raise max_rows to send them all.",
                    endpoint.max_rows
                ),
            ));
        }
        rows.push(row);
    }
    if rows.is_empty() {
        return Ok(Summary::new(0, "no rows to ask about"));
    }

    // The local model, for as long as this stage needs it.
    let local = match &endpoint.base_url {
        Some(_) => None,
        None => Some(local_model(context, endpoint.concurrency)?),
    };
    let base = match (&endpoint.base_url, &local) {
        (Some(url), _) => url.clone(),
        (None, Some(server)) => format!("{}/v1", server.base_url()),
        (None, None) => unreachable!("one or the other is set above"),
    };
    let url = format!("{base}/chat/completions");

    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let answers: Vec<Mutex<Option<Result<Record, String>>>> =
        rows.iter().map(|_| Mutex::new(None)).collect();

    std::thread::scope(|scope| {
        for _ in 0..endpoint.concurrency.min(rows.len()) {
            scope.spawn(|| {
                let mut client = match endpoint.client(&url) {
                    Ok(client) => client,
                    Err(error) => {
                        stop.store(true, Ordering::SeqCst);
                        *answers[0].lock().unwrap() = Some(Err(error.to_string()));
                        return;
                    }
                };
                loop {
                    let index = next.fetch_add(1, Ordering::SeqCst);
                    if index >= rows.len() || stop.load(Ordering::SeqCst) {
                        return;
                    }
                    let answer = ask(&mut client, &url, endpoint, question, &rows[index]);
                    if answer.is_err() {
                        stop.store(true, Ordering::SeqCst);
                    }
                    *answers[index].lock().unwrap() = Some(answer);
                }
            });
        }
    });
    drop(local);

    // The first failure by row, so the message names the earliest bad row.
    for (index, answer) in answers.iter().enumerate() {
        if let Some(Err(message)) = &*answer.lock().unwrap() {
            return Err(ConnectorError::Data(format!(
                "row {}: {message}",
                index + 1
            )));
        }
    }
    for (row, answer) in rows.iter().zip(answers) {
        let Some(Ok(mut columns)) = answer.into_inner().unwrap() else {
            continue;
        };
        columns.insert(
            ROW_KEY.to_string(),
            row.get(ROW_KEY).cloned().unwrap_or_default(),
        );
        out.write(columns)?;
    }

    let from = match &endpoint.base_url {
        Some(url) => host_of(url),
        None => "the local model".to_string(),
    };
    Ok(Summary::new(
        rows.len() as u64,
        format!(
            "{} row(s) answered by {from} in {:.1}s",
            rows.len(),
            started.elapsed().as_secs_f64()
        ),
    ))
}

/// One call, and what its answer becomes.
fn ask(
    client: &mut Client,
    url: &str,
    endpoint: &Endpoint,
    question: &dyn Question,
    row: &Record,
) -> Result<Record, String> {
    let mut body = json!({
        "model": endpoint.model,
        "messages": question.messages(row),
        "temperature": 0,
        "max_tokens": question.max_tokens(),
    });
    if let Some(schema) = question.schema() {
        body["response_format"] = json!({
            "type": "json_schema",
            "json_schema": { "name": "answer", "strict": true, "schema": schema }
        });
    }

    let reply = client
        .send(url, &[], Some(body.to_string().as_bytes()))
        .map_err(|error| error.to_string())?;
    let answer: JsonValue = serde_json::from_str(&reply.body)
        .map_err(|error| format!("the endpoint's answer is not JSON: {error}"))?;
    let content = answer["choices"][0]["message"]["content"]
        .as_str()
        .ok_or_else(|| {
            format!(
                "the endpoint's answer has no message: {}",
                crate::http::snippet(&reply.body)
            )
        })?;
    question.answer(content)
}

/// The local model, started with a slot for each call at once.
fn local_model(context: &Context, concurrency: usize) -> Result<Server, ConnectorError> {
    let mut starts: Vec<PathBuf> = vec![context
        .working_dir
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."))];
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
        Err(ConnectorError::Data(format!(
            "{} (or set base_url to use an endpoint)",
            first.unwrap_or_default()
        )))
    };
    let (server, model) = (find(locate_server)?, find(locate_model)?);

    let log = std::env::temp_dir().join(format!("etl-ask-{}.log", std::process::id()));
    let slots = concurrency.to_string();
    // Each slot gets 2048 tokens: a row's prompt and its answer.
    let context_size = (2048 * concurrency).to_string();
    let running = Server::spawn_with(
        &server,
        &model,
        &log,
        &["--parallel", &slots, "--ctx-size", &context_size],
    )
    .map_err(ConnectorError::Data)?;
    running.wait_until_loaded().map_err(ConnectorError::Data)?;
    Ok(running)
}

/// Scheme, host and port: what lineage and the report may show of an endpoint.
fn host_of(url: &str) -> String {
    let rest = url.split("://").nth(1).unwrap_or(url);
    let host = rest.split('/').next().unwrap_or(rest);
    let host = host.rsplit('@').next().unwrap_or(host);
    host.to_string()
}

/// A row's value as text for a prompt: empty for null, JSON for anything that
/// is not already text.
fn as_text(value: Option<&JsonValue>) -> String {
    match value {
        None | Some(JsonValue::Null) => String::new(),
        Some(JsonValue::String(text)) => text.clone(),
        Some(other) => other.to_string(),
    }
}

/// An answer that should be JSON, perhaps in a code fence a model added.
fn json_in(content: &str) -> Option<JsonValue> {
    let trimmed = content.trim();
    let unfenced = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|inner| inner.strip_suffix("```"))
        .unwrap_or(trimmed);
    serde_json::from_str(unfenced.trim()).ok()
}

fn column_property(help: &str) -> PropertySpec {
    PropertySpec::text("column").required().help(help)
}

fn output_name(properties: &JsonValue, default: &str) -> String {
    text(properties, "output").unwrap_or(default).to_string()
}

fn check_output(properties: &JsonValue) -> Result<(), ConnectorError> {
    if text(properties, "output") == Some(ROW_KEY) {
        return Err(ConnectorError::property(
            "output",
            format!("{ROW_KEY} is the engine's"),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// xf.ai.prompt
// ---------------------------------------------------------------------------

/// A prompt template, cut into text and `{column}` placeholders. `{{` and
/// `}}` are a literal brace.
#[derive(Debug, PartialEq)]
enum Piece {
    Text(String),
    Column(String),
}

fn template(source: &str) -> Result<Vec<Piece>, ConnectorError> {
    let mut pieces = Vec::new();
    let mut literal = String::new();
    let mut chars = source.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                literal.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                literal.push('}');
            }
            '{' => {
                let mut name = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some('{') | None => {
                            return Err(ConnectorError::property(
                                "prompt",
                                "has a { with no closing }; write {{ for a literal brace",
                            ))
                        }
                        Some(other) => name.push(other),
                    }
                }
                let name = name.trim().to_string();
                if name.is_empty() {
                    return Err(ConnectorError::property("prompt", "has an empty {}"));
                }
                if !literal.is_empty() {
                    pieces.push(Piece::Text(std::mem::take(&mut literal)));
                }
                pieces.push(Piece::Column(name));
            }
            '}' => {
                return Err(ConnectorError::property(
                    "prompt",
                    "has a } with no opening {; write }} for a literal brace",
                ))
            }
            other => literal.push(other),
        }
    }
    if !literal.is_empty() {
        pieces.push(Piece::Text(literal));
    }
    Ok(pieces)
}

pub struct PromptTransform;

struct Prompt {
    pieces: Vec<Piece>,
    system: Option<String>,
    output: String,
    max_tokens: u64,
}

impl Question for Prompt {
    fn messages(&self, row: &Record) -> Vec<JsonValue> {
        let prompt: String = self
            .pieces
            .iter()
            .map(|piece| match piece {
                Piece::Text(text) => text.clone(),
                Piece::Column(name) => as_text(row.get(name)),
            })
            .collect();
        let mut messages = Vec::new();
        if let Some(system) = &self.system {
            messages.push(json!({ "role": "system", "content": system }));
        }
        messages.push(json!({ "role": "user", "content": prompt }));
        messages
    }

    fn schema(&self) -> Option<JsonValue> {
        None
    }

    fn answer(&self, content: &str) -> Result<Record, String> {
        let mut record = Record::new();
        record.insert(self.output.clone(), json!(content.trim()));
        Ok(record)
    }

    fn max_tokens(&self) -> u64 {
        self.max_tokens
    }
}

impl Prompt {
    fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let source = text(properties, "prompt")
            .ok_or_else(|| ConnectorError::property("prompt", "is required"))?;
        Ok(Prompt {
            pieces: template(source)?,
            system: text(properties, "system").map(str::to_string),
            output: output_name(properties, "answer"),
            max_tokens: positive(properties, "max_tokens", 512)?,
        })
    }
}

impl Transform for PromptTransform {
    fn spec(&self) -> ComponentSpec {
        let mut properties = vec![
            PropertySpec::code("prompt").required().help(
                "Sent for each row, with {column} replaced by that row's value, e.g. \
                 Summarise in one sentence: {body}. {{ and }} are a literal brace.",
            ),
            PropertySpec::text("system").help("Instructions sent before each prompt."),
            PropertySpec::text("output")
                .default(json!("answer"))
                .help("The answer's column, as text."),
            PropertySpec::integer("max_tokens")
                .default(json!(512))
                .help("How long an answer may be."),
        ];
        properties.extend(endpoint_properties());
        ComponentSpec::new("xf.ai.prompt", "Ask a model")
            .description(
                "Send a prompt for each row, made from its columns, and keep the answer. \
                 With base_url set, the rows leave this machine.",
            )
            .icon("message-square")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        Prompt::from(properties)?;
        check_output(properties)?;
        Endpoint::from(properties).map(|_| ())
    }

    fn reads(&self, properties: &JsonValue) -> Vec<String> {
        let mut columns = Vec::new();
        if let Ok(prompt) = Prompt::from(properties) {
            for piece in prompt.pieces {
                if let Piece::Column(name) = piece {
                    if !columns.contains(&name) {
                        columns.push(name);
                    }
                }
            }
        }
        columns
    }

    fn adds(&self, properties: &JsonValue) -> Vec<(String, String)> {
        vec![(output_name(properties, "answer"), "VARCHAR".into())]
    }

    fn portable(&self, properties: &JsonValue) -> bool {
        text(properties, "base_url").is_some()
    }

    fn transform(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let endpoint = Endpoint::from(properties)?;
        ask_rows(&endpoint, &Prompt::from(properties)?, input, out, context)
    }
}

// ---------------------------------------------------------------------------
// xf.ai.classify
// ---------------------------------------------------------------------------

pub struct ClassifyTransform;

struct Classify {
    column: String,
    labels: Vec<String>,
    instructions: Option<String>,
    output: String,
    schema: bool,
}

impl Classify {
    fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let labels: Vec<String> = properties["labels"]
            .as_array()
            .map(|labels| {
                labels
                    .iter()
                    .filter_map(|label| label.as_str().map(str::trim).map(str::to_string))
                    .filter(|label| !label.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        if labels.len() < 2 {
            return Err(ConnectorError::property("labels", "must list at least two"));
        }
        Ok(Classify {
            column: text(properties, "column")
                .ok_or_else(|| ConnectorError::property("column", "is required"))?
                .to_string(),
            labels,
            instructions: text(properties, "instructions").map(str::to_string),
            output: output_name(properties, "label"),
            schema: properties["json_schema"].as_bool().unwrap_or(true),
        })
    }
}

impl Question for Classify {
    fn messages(&self, row: &Record) -> Vec<JsonValue> {
        let mut system = format!(
            "Classify the text. Answer with exactly one of these labels: {}.",
            self.labels.join(", ")
        );
        if let Some(instructions) = &self.instructions {
            system.push(' ');
            system.push_str(instructions);
        }
        if self.schema {
            system.push_str(" Answer as JSON: {\"label\": \"...\"}.");
        }
        vec![
            json!({ "role": "system", "content": system }),
            json!({ "role": "user", "content": as_text(row.get(&self.column)) }),
        ]
    }

    fn schema(&self) -> Option<JsonValue> {
        self.schema.then(|| {
            json!({
                "type": "object",
                "properties": { "label": { "enum": self.labels } },
                "required": ["label"],
                "additionalProperties": false
            })
        })
    }

    /// Held to the list whatever the endpoint did with the schema: a JSON
    /// `label`, or the bare text, matched without regard to case or quotes.
    fn answer(&self, content: &str) -> Result<Record, String> {
        let said = json_in(content)
            .and_then(|answer| answer["label"].as_str().map(str::to_string))
            .unwrap_or_else(|| content.trim().trim_matches(['"', '\'', '.']).to_string());
        let label = self
            .labels
            .iter()
            .find(|label| label.eq_ignore_ascii_case(said.trim()))
            .ok_or_else(|| {
                format!(
                    "the model answered {said:?}, which is not one of {}",
                    self.labels.join(", ")
                )
            })?;
        let mut record = Record::new();
        record.insert(self.output.clone(), json!(label));
        Ok(record)
    }

    fn max_tokens(&self) -> u64 {
        64
    }
}

impl Transform for ClassifyTransform {
    fn spec(&self) -> ComponentSpec {
        let mut properties =
            vec![
            column_property("The text to classify."),
            PropertySpec::string_list("labels")
                .required()
                .help("The labels to choose from, at least two. The answer is always one of them."),
            PropertySpec::text("instructions")
                .help("What the labels mean, or how to choose, in a sentence or two."),
            PropertySpec::text("output")
                .default(json!("label"))
                .help("The label's column."),
            PropertySpec::boolean("json_schema").default(json!(true)).help(
                "Send the answer's JSON Schema, so the model can only pick a label. Switch off \
                 for an endpoint that refuses response_format; the answer is checked either way.",
            ),
        ];
        properties.extend(endpoint_properties());
        ComponentSpec::new("xf.ai.classify", "Classify with a model")
            .description(
                "Give each row one of your labels, chosen by a model from a text column. With \
                 base_url set, the rows leave this machine.",
            )
            .icon("tags")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        Classify::from(properties)?;
        check_output(properties)?;
        Endpoint::from(properties).map(|_| ())
    }

    fn reads(&self, properties: &JsonValue) -> Vec<String> {
        text(properties, "column")
            .map(str::to_string)
            .into_iter()
            .collect()
    }

    fn adds(&self, properties: &JsonValue) -> Vec<(String, String)> {
        vec![(output_name(properties, "label"), "VARCHAR".into())]
    }

    fn portable(&self, properties: &JsonValue) -> bool {
        text(properties, "base_url").is_some()
    }

    fn transform(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let endpoint = Endpoint::from(properties)?;
        ask_rows(&endpoint, &Classify::from(properties)?, input, out, context)
    }
}

// ---------------------------------------------------------------------------
// xf.ai.extract
// ---------------------------------------------------------------------------

/// The types a field can be, with its SQL type and JSON Schema type.
const FIELD_TYPES: [(&str, &str, &str); 5] = [
    ("text", "VARCHAR", "string"),
    ("integer", "BIGINT", "integer"),
    ("number", "DOUBLE", "number"),
    ("boolean", "BOOLEAN", "boolean"),
    ("date", "DATE", "string"),
];

pub struct ExtractTransform;

struct Extract {
    column: String,
    fields: Vec<(String, &'static str)>,
    instructions: Option<String>,
    schema: bool,
}

impl Extract {
    fn from(properties: &JsonValue) -> Result<Self, ConnectorError> {
        let Some(declared) = properties["fields"]
            .as_object()
            .filter(|map| !map.is_empty())
        else {
            return Err(ConnectorError::property(
                "fields",
                "must name at least one field",
            ));
        };
        let mut fields = Vec::new();
        for (name, kind) in declared {
            if name == ROW_KEY {
                return Err(ConnectorError::property(
                    "fields",
                    format!("{ROW_KEY} is the engine's"),
                ));
            }
            let kind = kind.as_str().unwrap_or("").trim();
            let Some((kind, _, _)) = FIELD_TYPES.iter().find(|(known, _, _)| *known == kind) else {
                return Err(ConnectorError::property(
                    "fields",
                    format!(
                        "'{name}' is {kind:?}, not one of {}",
                        FIELD_TYPES.map(|(known, _, _)| known).join(", ")
                    ),
                ));
            };
            fields.push((name.clone(), *kind));
        }
        Ok(Extract {
            column: text(properties, "column")
                .ok_or_else(|| ConnectorError::property("column", "is required"))?
                .to_string(),
            fields,
            instructions: text(properties, "instructions").map(str::to_string),
            schema: properties["json_schema"].as_bool().unwrap_or(true),
        })
    }
}

fn field_type(kind: &str) -> (&'static str, &'static str) {
    FIELD_TYPES
        .iter()
        .find(|(known, _, _)| *known == kind)
        .map(|(_, sql, json)| (*sql, *json))
        .unwrap_or(("VARCHAR", "string"))
}

/// A value as its field's type, or null when it is not one.
fn typed(value: Option<&JsonValue>, kind: &str) -> JsonValue {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return JsonValue::Null;
    };
    match kind {
        "integer" => value
            .as_i64()
            .or_else(|| {
                value
                    .as_f64()
                    .filter(|f| f.fract() == 0.0)
                    .map(|f| f as i64)
            })
            .or_else(|| value.as_str().and_then(|s| s.trim().parse().ok()))
            .map_or(JsonValue::Null, |n| json!(n)),
        "number" => value
            .as_f64()
            .or_else(|| value.as_str().and_then(|s| s.trim().parse().ok()))
            .map_or(JsonValue::Null, |n| json!(n)),
        "boolean" => value
            .as_bool()
            .or_else(|| {
                match value
                    .as_str()
                    .map(|s| s.trim().to_ascii_lowercase())
                    .as_deref()
                {
                    Some("true" | "yes") => Some(true),
                    Some("false" | "no") => Some(false),
                    _ => None,
                }
            })
            .map_or(JsonValue::Null, |b| json!(b)),
        // A date DuckDB cannot read would fail the stage, so only its own
        // shape is passed on.
        "date" => value
            .as_str()
            .map(str::trim)
            .filter(|s| {
                s.len() == 10
                    && s.chars().enumerate().all(|(i, c)| {
                        if i == 4 || i == 7 {
                            c == '-'
                        } else {
                            c.is_ascii_digit()
                        }
                    })
            })
            .map_or(JsonValue::Null, |s| json!(s)),
        _ => match value {
            JsonValue::String(text) => json!(text),
            other => json!(other.to_string()),
        },
    }
}

impl Question for Extract {
    fn messages(&self, row: &Record) -> Vec<JsonValue> {
        let listed: Vec<String> = self
            .fields
            .iter()
            .map(|(name, kind)| {
                let kind = if *kind == "date" {
                    "date as YYYY-MM-DD"
                } else {
                    kind
                };
                format!("{name} ({kind})")
            })
            .collect();
        let mut system = format!(
            "Extract these fields from the text and answer as a JSON object: {}. Use null for \
             a field the text does not give.",
            listed.join(", ")
        );
        if let Some(instructions) = &self.instructions {
            system.push(' ');
            system.push_str(instructions);
        }
        vec![
            json!({ "role": "system", "content": system }),
            json!({ "role": "user", "content": as_text(row.get(&self.column)) }),
        ]
    }

    fn schema(&self) -> Option<JsonValue> {
        self.schema.then(|| {
            let properties: Map<String, JsonValue> = self
                .fields
                .iter()
                .map(|(name, kind)| {
                    let (_, json_type) = field_type(kind);
                    (name.clone(), json!({ "type": [json_type, "null"] }))
                })
                .collect();
            json!({
                "type": "object",
                "properties": properties,
                "required": self.fields.iter().map(|(name, _)| name).collect::<Vec<_>>(),
                "additionalProperties": false
            })
        })
    }

    fn answer(&self, content: &str) -> Result<Record, String> {
        let answer = json_in(content)
            .filter(JsonValue::is_object)
            .ok_or_else(|| {
                format!(
                    "the model's answer is not a JSON object: {}",
                    crate::http::snippet(content)
                )
            })?;
        Ok(self
            .fields
            .iter()
            .map(|(name, kind)| (name.clone(), typed(answer.get(name), kind)))
            .collect())
    }
}

impl Transform for ExtractTransform {
    fn spec(&self) -> ComponentSpec {
        let mut properties =
            vec![
            column_property("The text to read the fields from."),
            PropertySpec::map("fields").required().help(
                "Each field's name, mapped to its type: text, integer, number, boolean or date. \
                 One column each; null where the text does not give it or the answer is not \
                 that type.",
            ),
            PropertySpec::text("instructions")
                .help("What the fields mean, or where to find them, in a sentence or two."),
            PropertySpec::boolean("json_schema").default(json!(true)).help(
                "Send the answer's JSON Schema, so the model can only answer with these fields. \
                 Switch off for an endpoint that refuses response_format.",
            ),
        ];
        properties.extend(endpoint_properties());
        ComponentSpec::new("xf.ai.extract", "Extract with a model")
            .description(
                "Read named, typed fields out of a text column, one column each. With base_url \
                 set, the rows leave this machine.",
            )
            .icon("list-tree")
            .properties(properties)
    }

    fn check(&self, properties: &JsonValue) -> Result<(), ConnectorError> {
        Extract::from(properties)?;
        Endpoint::from(properties).map(|_| ())
    }

    fn reads(&self, properties: &JsonValue) -> Vec<String> {
        text(properties, "column")
            .map(str::to_string)
            .into_iter()
            .collect()
    }

    fn adds(&self, properties: &JsonValue) -> Vec<(String, String)> {
        Extract::from(properties)
            .map(|extract| {
                extract
                    .fields
                    .iter()
                    .map(|(name, kind)| (name.clone(), field_type(kind).0.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn portable(&self, properties: &JsonValue) -> bool {
        text(properties, "base_url").is_some()
    }

    fn transform(
        &self,
        properties: &JsonValue,
        input: &mut dyn RecordReader,
        out: &mut dyn RecordWriter,
        context: &Context,
    ) -> Result<Summary, ConnectorError> {
        let endpoint = Endpoint::from(properties)?;
        ask_rows(&endpoint, &Extract::from(properties)?, input, out, context)
    }
}

#[cfg(test)]
mod tests;
