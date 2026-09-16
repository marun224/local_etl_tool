//! `${...}` interpolation and the parameter contract.
//!
//! A pipeline that hard-codes `D:/data/orders.csv` runs on one machine. The
//! point of this module is that the same document, unedited, runs in dev and in
//! prod — which is the whole of Phase 5's goal.
//!
//! Resolution happens **before compilation**, as a document-to-document
//! transform. That keeps [`compile`](crate::compile) pure and means `validate`
//! can report an unresolved parameter without generating a line of SQL.
//!
//! Three deliberate properties:
//!
//! * **Substitution is single-pass.** A value that is substituted in is never
//!   itself scanned for `${...}`. That rules out both runaway expansion and the
//!   uglier case: a parameter value supplied on the command line that contains
//!   `${ENV:AWS_SECRET_ACCESS_KEY}` and would otherwise read the environment of
//!   the process that ran it.
//! * **Only node properties are interpolated.** Labels, ids and positions are
//!   left alone — a parameter belongs in the configuration of a node, not in
//!   its name on the canvas.
//! * **Every failure names the node and the property.** An error that says only
//!   "unresolved parameter" leaves someone hunting a canvas for it.

use etl_metadata::{ParameterSpec, PipelineDoc};
use etl_secrets::SecretStore;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// The names the resolver answers on its own.
pub const BUILT_INS: [&str; 2] = ["workspace", "date"];

/// What a secret's value is replaced with anywhere it would be displayed.
///
/// A secret has to reach the generated SQL — DuckDB needs the real password —
/// but it must not reach the plan view, the run report, or a terminal. The
/// value is therefore masked everywhere except in the script handed to DuckDB.
pub const REDACTED: &str = "********";

/// Everything that can go wrong resolving a document's parameters.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParamError {
    #[error("node '{node}', property '{property}': nothing provides '{reference}'{}", suggestion(.known, .reference))]
    Unresolved {
        node: String,
        property: String,
        reference: String,
        known: Vec<String>,
    },

    #[error("node '{node}', property '{property}': '{text}' has a '${{' that is never closed")]
    Unterminated {
        node: String,
        property: String,
        text: String,
    },

    #[error("node '{node}', property '{property}': '${{}}' names nothing")]
    EmptyReference { node: String, property: String },

    #[error("parameter '{name}' is required and has no value and no default")]
    MissingRequired { name: String },

    #[error("parameter '{name}' is required, but the value given for it is empty")]
    EmptyRequired { name: String },

    #[error(
        "parameter '{name}' is declared as {declared} but '{value}' is not {article} {declared}"
    )]
    WrongType {
        name: String,
        declared: String,
        value: String,
        article: String,
    },

    #[error("node '{node}', property '{property}': there is no context named '{name}'{}", suggestion(.known, .name))]
    UnknownContext {
        node: String,
        property: String,
        name: String,
        known: Vec<String>,
    },

    #[error("node '{node}', property '{property}': the environment variable '{name}' is not set")]
    MissingEnvironment {
        node: String,
        property: String,
        name: String,
    },

    #[error(
        "node '{node}', property '{property}': this pipeline needs the secret '{name}', but no \
         secret store was opened for this workspace"
    )]
    NoSecretStore {
        node: String,
        property: String,
        name: String,
    },

    #[error("node '{node}', property '{property}': there is no secret named '{name}'{}", suggestion(.known, .name))]
    MissingSecret {
        node: String,
        property: String,
        name: String,
        known: Vec<String>,
    },

    #[error(
        "node '{node}', property '{property}': the secret '{name}' could not be read: {reason}"
    )]
    UnreadableSecret {
        node: String,
        property: String,
        name: String,
        reason: String,
    },
}

/// `. Known values are: a, b, c` — or nothing when there are none to list.
fn suggestion(known: &[String], _asked: &str) -> String {
    if known.is_empty() {
        String::new()
    } else {
        format!(". Known: {}", known.join(", "))
    }
}

impl ParamError {
    /// The node this error is about, so the canvas can mark the right box.
    pub fn node_id(&self) -> Option<&str> {
        match self {
            ParamError::Unresolved { node, .. }
            | ParamError::Unterminated { node, .. }
            | ParamError::EmptyReference { node, .. }
            | ParamError::UnknownContext { node, .. }
            | ParamError::MissingEnvironment { node, .. }
            | ParamError::NoSecretStore { node, .. }
            | ParamError::MissingSecret { node, .. }
            | ParamError::UnreadableSecret { node, .. } => Some(node),
            ParamError::MissingRequired { .. }
            | ParamError::EmptyRequired { .. }
            | ParamError::WrongType { .. } => None,
        }
    }
}

/// Something worth saying that does not stop the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParamWarning {
    /// A value was supplied for a parameter the pipeline does not declare.
    /// Usually a typo; also what a newer document looks like, so not an error.
    Undeclared { name: String },
    /// A declared parameter or context variable hides a built-in of the same
    /// name. Legal, and occasionally wanted, but worth saying out loud.
    ShadowsBuiltIn { name: String },
}

/// What resolution substituted, gathered as it goes.
#[derive(Debug, Clone, Default)]
struct Trace {
    used: BTreeMap<String, String>,
    secrets: BTreeSet<String>,
}

/// A document with every `${...}` replaced.
#[derive(Debug, Clone)]
pub struct Resolved {
    /// The document to compile. Identical to the input but for its properties.
    pub document: PipelineDoc,
    pub warnings: Vec<ParamWarning>,
    /// What each reference resolved to, for the run report and for `--explain`.
    /// Sorted, so two runs of the same pipeline print the same list. A secret's
    /// entry reads [`REDACTED`], never its value.
    pub used: BTreeMap<String, String>,
    /// The plaintext of every secret that was substituted, so callers can mask
    /// it out of anything they display. Not printed by anything itself.
    secrets: BTreeSet<String>,
}

impl Resolved {
    /// Mask every secret value this resolution substituted.
    ///
    /// Call it on anything a person will see — the plan view, the run report,
    /// a log line. The only text that should escape unmasked is the script
    /// handed to DuckDB.
    pub fn redact(&self, text: &str) -> String {
        let mut out = text.to_string();

        for secret in &self.secrets {
            // An empty secret would match everywhere and mask nothing useful.
            if !secret.is_empty() {
                out = out.replace(secret.as_str(), REDACTED);
            }
        }

        out
    }

    /// Whether any secret was substituted at all.
    pub fn uses_secrets(&self) -> bool {
        !self.secrets.is_empty()
    }

    /// The secret values, for handing to [`crate::RunOptions::redact`].
    pub fn secret_values(&self) -> Vec<String> {
        self.secrets.iter().cloned().collect()
    }
}

/// Where values come from, and in what order.
///
/// Precedence, highest first: an explicit binding (`--param`), then the active
/// context, then the parameter's own declared default, then a built-in. Built-
/// ins come last so that what a pipeline declares always wins over what the
/// tool assumes — with a warning, because the shadowing is worth knowing about.
#[derive(Debug, Clone)]
pub struct Resolver {
    explicit: BTreeMap<String, String>,
    contexts: BTreeMap<String, BTreeMap<String, String>>,
    active: Option<String>,
    workspace: PathBuf,
    today: String,
    /// Opened lazily by the caller: a pipeline with no `${SECRET:...}` in it
    /// must not need a workspace key to run.
    secrets: Option<Arc<SecretStore>>,
}

impl Resolver {
    /// A resolver with no bindings, rooted at `workspace`.
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        Self {
            explicit: BTreeMap::new(),
            contexts: BTreeMap::new(),
            active: None,
            workspace: workspace.into(),
            today: today_utc(),
            secrets: None,
        }
    }

    /// Bind a parameter explicitly, as `--param name=value` does.
    pub fn bind(mut self, name: &str, value: &str) -> Self {
        self.explicit.insert(name.to_string(), value.to_string());
        self
    }

    /// Add a named context's variables.
    pub fn context(mut self, name: &str, variables: BTreeMap<String, String>) -> Self {
        self.contexts.insert(name.to_string(), variables);
        self
    }

    /// Choose which context unqualified `${NAME}` references read from.
    pub fn activate(mut self, name: Option<&str>) -> Self {
        self.active = name.map(str::to_string);
        self
    }

    /// Give the resolver somewhere to read `${SECRET:name}` from.
    pub fn secrets(mut self, store: SecretStore) -> Self {
        self.secrets = Some(Arc::new(store));
        self
    }

    /// Pin `${date}`, so a test does not change answer at midnight.
    pub fn with_today(mut self, today: &str) -> Self {
        self.today = today.to_string();
        self
    }

    pub fn workspace(&self) -> &Path {
        &self.workspace
    }

    pub fn active_context(&self) -> Option<&str> {
        self.active.as_deref()
    }

    /// Whether a context of this name was loaded.
    pub fn knows_context(&self, name: &str) -> bool {
        self.contexts.contains_key(name)
    }

    fn context_names(&self) -> Vec<String> {
        self.contexts.keys().cloned().collect()
    }

    /// A built-in's value.
    fn built_in(&self, name: &str) -> Option<String> {
        match name {
            // Forward slashes: the value is usually pasted into a path literal,
            // and `quote_path` would normalise it anyway.
            "workspace" => Some(self.workspace.to_string_lossy().replace('\\', "/")),
            "date" => Some(self.today.clone()),
            _ => None,
        }
    }

    /// Resolve an unqualified `${name}`, in precedence order.
    fn lookup(&self, name: &str, declared: &BTreeMap<String, ParameterSpec>) -> Option<String> {
        if let Some(value) = self.explicit.get(name) {
            return Some(value.clone());
        }

        if let Some(active) = &self.active {
            if let Some(value) = self.contexts.get(active).and_then(|vars| vars.get(name)) {
                return Some(value.clone());
            }
        }

        if let Some(default) = declared.get(name).and_then(|spec| spec.default.as_ref()) {
            return Some(scalar(default));
        }

        self.built_in(name)
    }

    /// Every name this resolver could answer, for an error message that helps.
    fn known(&self, declared: &BTreeMap<String, ParameterSpec>) -> Vec<String> {
        let mut names: BTreeSet<String> = BTreeSet::new();

        names.extend(self.explicit.keys().cloned());
        names.extend(declared.keys().cloned());
        names.extend(BUILT_INS.iter().map(|b| b.to_string()));

        if let Some(active) = &self.active {
            if let Some(variables) = self.contexts.get(active) {
                names.extend(variables.keys().cloned());
            }
        }

        names.into_iter().collect()
    }
}

/// A JSON scalar as the text that will be substituted in.
///
/// Objects and arrays have no sensible spelling inside a string, so they become
/// their JSON form rather than something lossy.
fn scalar(value: &JsonValue) -> String {
    match value {
        JsonValue::String(text) => text.clone(),
        JsonValue::Null => String::new(),
        other => other.to_string(),
    }
}

/// Resolve a document's parameters and interpolate its properties.
///
/// Runs the contract check first, so a missing required parameter is reported
/// as that rather than as an unresolved reference somewhere downstream.
pub fn resolve(document: &PipelineDoc, resolver: &Resolver) -> Result<Resolved, ParamError> {
    let mut warnings = Vec::new();
    check_contract(document, resolver, &mut warnings)?;

    let mut trace = Trace::default();
    let mut resolved = document.clone();

    for node in &mut resolved.nodes {
        let Some(properties) = node.data.properties.take() else {
            continue;
        };

        let interpolated = walk(
            properties,
            &node.id,
            "properties",
            resolver,
            &document.parameters,
            &mut trace,
        )?;

        node.data.properties = Some(interpolated);
    }

    Ok(Resolved {
        document: resolved,
        warnings,
        used: trace.used,
        secrets: trace.secrets,
    })
}

/// Check the declared contract before substituting anything.
fn check_contract(
    document: &PipelineDoc,
    resolver: &Resolver,
    warnings: &mut Vec<ParamWarning>,
) -> Result<(), ParamError> {
    for (name, spec) in &document.parameters {
        if BUILT_INS.contains(&name.as_str()) {
            warnings.push(ParamWarning::ShadowsBuiltIn { name: name.clone() });
        }

        let value = resolver.lookup(name, &document.parameters);

        // A default satisfies `required`. Unlike a component property — where
        // required-with-a-default is meaningless and the tests forbid it — a
        // required parameter also tells the canvas to prompt for it, which is
        // worth saying even when there is something to prompt with.
        match value {
            None if spec.required.unwrap_or(false) => {
                return Err(ParamError::MissingRequired { name: name.clone() })
            }
            None => {}

            // An empty value for a required parameter is almost always a
            // mistake — `--param since=` rather than a deliberate empty
            // string — and a required *property* is already rejected the same
            // way, so the two stay consistent.
            Some(value) if spec.required.unwrap_or(false) && value.trim().is_empty() => {
                return Err(ParamError::EmptyRequired { name: name.clone() })
            }

            Some(value) => check_type(name, spec, &value)?,
        }
    }

    for name in resolver.explicit.keys() {
        if !document.parameters.contains_key(name) {
            warnings.push(ParamWarning::Undeclared { name: name.clone() });
        }
    }

    Ok(())
}

/// Check a bound value against its declared type.
///
/// The declared type is carried as free text on the wire so a document from a
/// newer version keeps whatever it says. An unrecognised type is therefore not
/// an error — it is simply not checked.
fn check_type(name: &str, spec: &ParameterSpec, value: &str) -> Result<(), ParamError> {
    let Some(declared) = spec.param_type.as_deref() else {
        return Ok(());
    };

    let acceptable = match declared {
        "string" | "text" => true,
        "integer" | "int" => value.trim().parse::<i64>().is_ok(),
        "number" | "float" => value.trim().parse::<f64>().is_ok(),
        "boolean" | "bool" => matches!(value.trim(), "true" | "false"),
        "date" => looks_like_a_date(value.trim()),
        _ => true,
    };

    if acceptable {
        return Ok(());
    }

    Err(ParamError::WrongType {
        name: name.to_string(),
        declared: declared.to_string(),
        value: value.to_string(),
        article: if declared.starts_with(['a', 'e', 'i', 'o', 'u']) {
            "an".to_string()
        } else {
            "a".to_string()
        },
    })
}

/// `YYYY-MM-DD`, optionally with a time after it.
///
/// Deliberately a shape check rather than a calendar one: the value is going
/// into SQL, where DuckDB is the authority on whether it is a real date, and a
/// second opinion here would eventually disagree with it.
fn looks_like_a_date(value: &str) -> bool {
    let date = value.split(['T', ' ']).next().unwrap_or_default();
    let parts: Vec<&str> = date.split('-').collect();

    parts.len() == 3
        && parts[0].len() == 4
        && parts.iter().all(|part| {
            !part.is_empty() && part.chars().all(|character| character.is_ascii_digit())
        })
}

/// Walk a property value, interpolating every string inside it.
fn walk(
    value: JsonValue,
    node: &str,
    property: &str,
    resolver: &Resolver,
    declared: &BTreeMap<String, ParameterSpec>,
    trace: &mut Trace,
) -> Result<JsonValue, ParamError> {
    Ok(match value {
        JsonValue::String(text) => JsonValue::String(interpolate(
            &text, node, property, resolver, declared, trace,
        )?),

        JsonValue::Array(items) => JsonValue::Array(
            items
                .into_iter()
                .map(|item| walk(item, node, property, resolver, declared, trace))
                .collect::<Result<Vec<_>, _>>()?,
        ),

        JsonValue::Object(fields) => {
            let mut out = serde_json::Map::with_capacity(fields.len());

            for (key, field) in fields {
                // The key names the property, so errors point at `path` rather
                // than at the whole `properties` object.
                let field = walk(field, node, &key, resolver, declared, trace)?;
                out.insert(key, field);
            }

            JsonValue::Object(out)
        }

        scalar => scalar,
    })
}

/// Replace every `${...}` in one string.
///
/// `$$` is a literal `$`, which is how a string that genuinely needs to contain
/// `${` is written.
fn interpolate(
    text: &str,
    node: &str,
    property: &str,
    resolver: &Resolver,
    declared: &BTreeMap<String, ParameterSpec>,
    trace: &mut Trace,
) -> Result<String, ParamError> {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut at = 0;

    while at < bytes.len() {
        if bytes[at] != b'$' {
            // Push whole characters, not bytes, so multi-byte text survives.
            let character = text[at..].chars().next().expect("index is on a boundary");
            out.push(character);
            at += character.len_utf8();
            continue;
        }

        match bytes.get(at + 1) {
            Some(b'$') => {
                out.push('$');
                at += 2;
            }

            Some(b'{') => {
                let Some(close) = text[at + 2..].find('}') else {
                    return Err(ParamError::Unterminated {
                        node: node.to_string(),
                        property: property.to_string(),
                        text: text.to_string(),
                    });
                };

                let reference = &text[at + 2..at + 2 + close];
                let value = resolve_one(reference, node, property, resolver, declared, trace)?;

                // The value goes into the SQL; what is *recorded* about it is
                // masked, so a secret cannot escape through the run report.
                let recorded = if trace.secrets.contains(&value) {
                    REDACTED.to_string()
                } else {
                    value.clone()
                };
                trace.used.insert(reference.to_string(), recorded);

                out.push_str(&value);
                at += 2 + close + 1;
            }

            // A lone `$` is just a dollar sign: `$1` in a predicate, a currency
            // symbol in a literal.
            _ => {
                out.push('$');
                at += 1;
            }
        }
    }

    Ok(out)
}

/// Resolve the text between `${` and `}`.
fn resolve_one(
    reference: &str,
    node: &str,
    property: &str,
    resolver: &Resolver,
    declared: &BTreeMap<String, ParameterSpec>,
    trace: &mut Trace,
) -> Result<String, ParamError> {
    let reference = reference.trim();

    if reference.is_empty() {
        return Err(ParamError::EmptyReference {
            node: node.to_string(),
            property: property.to_string(),
        });
    }

    // Secrets before anything else: the prefix is reserved, so a parameter
    // called `SECRET:x` cannot shadow the store.
    if let Some(name) = reference.strip_prefix("SECRET:") {
        let name = name.trim();

        let Some(store) = &resolver.secrets else {
            return Err(ParamError::NoSecretStore {
                node: node.to_string(),
                property: property.to_string(),
                name: name.to_string(),
            });
        };

        let value = store
            .get(name)
            .map_err(|error| ParamError::UnreadableSecret {
                node: node.to_string(),
                property: property.to_string(),
                name: name.to_string(),
                reason: error.to_string(),
            })?
            .ok_or_else(|| ParamError::MissingSecret {
                node: node.to_string(),
                property: property.to_string(),
                name: name.to_string(),
                known: store.names().into_iter().map(str::to_string).collect(),
            })?;

        // Remembered so it can be masked out of anything displayed later.
        trace.secrets.insert(value.clone());

        return Ok(value);
    }

    if let Some(key) = reference.strip_prefix("ENV:") {
        return std::env::var(key.trim()).map_err(|_| ParamError::MissingEnvironment {
            node: node.to_string(),
            property: property.to_string(),
            name: key.trim().to_string(),
        });
    }

    // A dot means a context-qualified variable. Parameter names therefore may
    // not contain one, which is the price of telling the two apart without a
    // second sigil.
    if let Some((context, variable)) = reference.split_once('.') {
        let Some(variables) = resolver.contexts.get(context) else {
            return Err(ParamError::UnknownContext {
                node: node.to_string(),
                property: property.to_string(),
                name: context.to_string(),
                known: resolver.context_names(),
            });
        };

        return variables
            .get(variable)
            .cloned()
            .ok_or_else(|| ParamError::Unresolved {
                node: node.to_string(),
                property: property.to_string(),
                reference: reference.to_string(),
                known: variables.keys().cloned().collect(),
            });
    }

    resolver
        .lookup(reference, declared)
        .ok_or_else(|| ParamError::Unresolved {
            node: node.to_string(),
            property: property.to_string(),
            reference: reference.to_string(),
            known: resolver.known(declared),
        })
}

// ---------------------------------------------------------------------------
// The clock
// ---------------------------------------------------------------------------

/// Today's date in UTC, as `YYYY-MM-DD`.
///
/// Hand-rolled rather than pulling in a date crate, which is the same call made
/// for the topological sort: the algorithm is short, well known, and tested
/// here against dates whose answers are not in doubt.
fn today_utc() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or(0);

    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));

    format!("{year:04}-{month:02}-{day:02}")
}

/// Days since 1970-01-01 to a calendar date. Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    // Shift the epoch to 0000-03-01, so leap days land at the end of the cycle.
    let shifted = days + 719_468;

    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;

    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153; // [0, 11], March-based

    let day = day_of_year - (153 * month_position + 2) / 5 + 1;
    let month = if month_position < 10 {
        month_position + 3
    } else {
        month_position - 9
    };

    (year + i64::from(month <= 2), month, day)
}

#[cfg(test)]
mod tests;
