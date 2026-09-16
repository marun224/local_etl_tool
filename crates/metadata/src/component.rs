//! What a component is.
//!
//! A [`ComponentSpec`] is the single description of a component that everything
//! else reads: the engine validates a node's properties against it, the CLI
//! lists it, and the canvas generates its property panel from it. There is no
//! second copy of this knowledge in the frontend — that is the whole point.
//! With ~400 components to reach, anything that must be written twice will
//! eventually disagree with itself.
//!
//! The specs live in this crate rather than in the engine so the desktop app
//! can depend on them without depending on SQL generation.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// The six component namespaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Namespace {
    /// `src.*` — reads data in.
    Source,
    /// `xf.*` — reshapes data.
    Transform,
    /// `snk.*` — writes data out.
    Sink,
    /// `qa.*` — validates rows.
    Quality,
    /// `ctl.*` — control flow and side effects.
    Control,
    /// `code.*` — user-supplied code.
    Code,
}

impl Namespace {
    /// The prefix this namespace uses in a component id.
    pub fn prefix(self) -> &'static str {
        match self {
            Namespace::Source => "src",
            Namespace::Transform => "xf",
            Namespace::Sink => "snk",
            Namespace::Quality => "qa",
            Namespace::Control => "ctl",
            Namespace::Code => "code",
        }
    }

    /// Parse the prefix of a component id.
    pub fn from_prefix(prefix: &str) -> Option<Self> {
        match prefix {
            "src" => Some(Namespace::Source),
            "xf" => Some(Namespace::Transform),
            "snk" => Some(Namespace::Sink),
            "qa" => Some(Namespace::Quality),
            "ctl" => Some(Namespace::Control),
            "code" => Some(Namespace::Code),
            _ => None,
        }
    }

    /// Every namespace, for listings.
    pub fn all() -> [Namespace; 6] {
        [
            Namespace::Source,
            Namespace::Transform,
            Namespace::Sink,
            Namespace::Quality,
            Namespace::Control,
            Namespace::Code,
        ]
    }
}

/// How a property is entered, which decides the control the canvas renders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertyType {
    /// A single line of text.
    Text,
    /// A file or object-store path — the canvas offers a file picker.
    Path,
    /// A SQL expression or statement — the canvas offers a code editor.
    Sql,
    Bool,
    Integer,
    Number,
    /// A list of strings, typically column names.
    StringList,
    /// Ordered name/value pairs, both strings — a rename map, a column-to-type
    /// map. Ordered because the pairs become SQL in the order they were
    /// entered, and a plan that reorders between runs is not reviewable.
    Map,
    /// One of [`PropertySpec::options`].
    Enum,
}

impl PropertyType {
    /// Whether a JSON value is acceptable for this type.
    pub fn accepts(self, value: &JsonValue) -> bool {
        match self {
            PropertyType::Text | PropertyType::Path | PropertyType::Sql | PropertyType::Enum => {
                value.is_string()
            }
            PropertyType::Bool => value.is_boolean(),
            PropertyType::Integer => value.is_i64() || value.is_u64(),
            PropertyType::Number => value.is_number(),
            PropertyType::StringList => value
                .as_array()
                .is_some_and(|items| items.iter().all(JsonValue::is_string)),
            PropertyType::Map => value
                .as_object()
                .is_some_and(|pairs| pairs.values().all(JsonValue::is_string)),
        }
    }

    /// How to describe the expected shape in an error message.
    pub fn expectation(self) -> &'static str {
        match self {
            PropertyType::Text | PropertyType::Path | PropertyType::Sql => "must be text",
            PropertyType::Enum => "must be one of the listed values",
            PropertyType::Bool => "must be true or false",
            PropertyType::Integer => "must be a whole number",
            PropertyType::Number => "must be a number",
            PropertyType::StringList => "must be a list of names",
            PropertyType::Map => "must be a set of name/value pairs",
        }
    }
}

/// One configurable property of a component.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertySpec {
    pub name: String,
    pub label: String,
    #[serde(rename = "type")]
    pub property_type: PropertyType,
    #[serde(default)]
    pub required: bool,
    /// Applied before lowering when the node leaves the property out, so a
    /// builder never has to carry its own idea of the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    /// The permitted values, for [`PropertyType::Enum`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
}

impl PropertySpec {
    fn new(name: &str, property_type: PropertyType) -> Self {
        Self {
            name: name.to_string(),
            label: title_case(name),
            property_type,
            required: false,
            default: None,
            help: None,
            options: Vec::new(),
        }
    }

    pub fn text(name: &str) -> Self {
        Self::new(name, PropertyType::Text)
    }

    pub fn path(name: &str) -> Self {
        Self::new(name, PropertyType::Path)
    }

    pub fn sql(name: &str) -> Self {
        Self::new(name, PropertyType::Sql)
    }

    pub fn boolean(name: &str) -> Self {
        Self::new(name, PropertyType::Bool)
    }

    pub fn integer(name: &str) -> Self {
        Self::new(name, PropertyType::Integer)
    }

    pub fn number(name: &str) -> Self {
        Self::new(name, PropertyType::Number)
    }

    pub fn string_list(name: &str) -> Self {
        Self::new(name, PropertyType::StringList)
    }

    pub fn map(name: &str) -> Self {
        Self::new(name, PropertyType::Map)
    }

    pub fn enumerated(name: &str, options: &[&str]) -> Self {
        Self {
            options: options.iter().map(|o| o.to_string()).collect(),
            ..Self::new(name, PropertyType::Enum)
        }
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn label(mut self, label: &str) -> Self {
        self.label = label.to_string();
        self
    }

    pub fn default(mut self, value: JsonValue) -> Self {
        self.default = Some(value);
        self
    }

    pub fn help(mut self, help: &str) -> Self {
        self.help = Some(help.to_string());
        self
    }
}

/// One input or output port.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortSpec {
    /// The handle name an edge uses to reach this port.
    pub name: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
}

impl PortSpec {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            label: title_case(name),
            help: None,
        }
    }

    pub fn help(mut self, help: &str) -> Self {
        self.help = Some(help.to_string());
        self
    }

    /// The single unnamed output most components have.
    pub fn main() -> Self {
        Self::new("main")
    }

    /// The single unnamed input most components have.
    pub fn input() -> Self {
        Self::new("in")
    }

    /// The dead-letter output of a quality node: the rows that failed its
    /// check. Wired to a sink it is an error report; left unwired the rows are
    /// simply dropped, which is the common case and must not be an error.
    pub fn rejected() -> Self {
        Self::new(REJECTED_PORT).help("Rows that did not pass the check.")
    }
}

/// The handle name of a quality node's dead-letter output.
///
/// One constant rather than a literal in the spec, the planner and the engine:
/// this string is the contract between an edge drawn on the canvas and the
/// relation the engine creates, and three copies of it is three chances for
/// one to drift.
pub const REJECTED_PORT: &str = "rejected";

/// The handle name of the ordinary output every producing component has.
pub const MAIN_PORT: &str = "main";

/// Everything that describes one component.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComponentSpec {
    /// The namespaced id, e.g. `src.file.csv`.
    pub id: String,
    pub namespace: Namespace,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// An icon name for the canvas palette.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    #[serde(default)]
    pub inputs: Vec<PortSpec>,
    #[serde(default)]
    pub outputs: Vec<PortSpec>,
    #[serde(default)]
    pub properties: Vec<PropertySpec>,
    /// DuckDB extensions this component needs loaded before it can run.
    ///
    /// Declared here rather than inferred from the generated SQL for two
    /// reasons: the canvas can warn that a pipeline needs `postgres` before
    /// someone starts a run rather than during one, and Phase 9's air-gapped
    /// packaging needs to know exactly which extension files to vendor.
    #[serde(
        default,
        rename = "requiresExtensions",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub requires_extensions: Vec<String>,
}

impl ComponentSpec {
    /// Start a spec. The namespace is derived from the id, so the two cannot
    /// drift apart.
    pub fn new(id: &str, label: &str) -> Self {
        let namespace = id
            .split('.')
            .next()
            .and_then(Namespace::from_prefix)
            .unwrap_or_else(|| panic!("component id '{id}' has no known namespace prefix"));

        // Sensible shapes by namespace; any component can override.
        let (inputs, outputs) = match namespace {
            Namespace::Source => (vec![], vec![PortSpec::main()]),
            Namespace::Sink => (vec![PortSpec::input()], vec![]),
            Namespace::Control => (vec![PortSpec::input()], vec![PortSpec::main()]),
            // A validator splits rather than filters: the rows that passed
            // leave by `main`, the ones that did not by `rejected`.
            Namespace::Quality => (
                vec![PortSpec::input()],
                vec![PortSpec::main(), PortSpec::rejected()],
            ),
            _ => (vec![PortSpec::input()], vec![PortSpec::main()]),
        };

        Self {
            id: id.to_string(),
            namespace,
            label: label.to_string(),
            description: None,
            icon: None,
            inputs,
            outputs,
            properties: Vec::new(),
            requires_extensions: Vec::new(),
        }
    }

    pub fn description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }

    pub fn icon(mut self, icon: &str) -> Self {
        self.icon = Some(icon.to_string());
        self
    }

    pub fn inputs(mut self, inputs: Vec<PortSpec>) -> Self {
        self.inputs = inputs;
        self
    }

    pub fn outputs(mut self, outputs: Vec<PortSpec>) -> Self {
        self.outputs = outputs;
        self
    }

    pub fn properties(mut self, properties: Vec<PropertySpec>) -> Self {
        self.properties = properties;
        self
    }

    /// Declare a DuckDB extension this component needs. Call it more than once
    /// for a component that needs several — reading Iceberg over S3 needs both
    /// `iceberg` and `httpfs`.
    pub fn requires_extension(mut self, extension: &str) -> Self {
        let extension = extension.to_string();
        if !self.requires_extensions.contains(&extension) {
            self.requires_extensions.push(extension);
        }
        self
    }

    pub fn property(&self, name: &str) -> Option<&PropertySpec> {
        self.properties.iter().find(|p| p.name == name)
    }

    /// How many upstream connections this component expects.
    pub fn input_count(&self) -> usize {
        self.inputs.len()
    }

    /// Whether this component declares an output with this handle name.
    ///
    /// An edge leaving an unnamed handle means `main`, which is what a canvas
    /// emits for a component with one output.
    pub fn has_output(&self, handle: Option<&str>) -> bool {
        let wanted = handle.unwrap_or(MAIN_PORT);
        self.outputs.iter().any(|port| port.name == wanted)
    }

    /// The handle names of this component's outputs, for an error message that
    /// can say what the alternatives were.
    pub fn output_names(&self) -> Vec<String> {
        self.outputs.iter().map(|port| port.name.clone()).collect()
    }

    /// Whether this component has a dead-letter output.
    pub fn has_reject_port(&self) -> bool {
        self.has_output(Some(REJECTED_PORT))
    }
}

/// `order_ts` → `Order Ts`. Good enough for a default label; specs that want
/// something better set one.
fn title_case(name: &str) -> String {
    name.split(['_', '-'])
        .filter(|word| !word.is_empty())
        .map(|word| {
            let mut characters = word.chars();
            match characters.next() {
                Some(first) => first.to_uppercase().chain(characters).collect::<String>(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_namespace_round_trips_through_its_prefix() {
        for namespace in Namespace::all() {
            assert_eq!(Namespace::from_prefix(namespace.prefix()), Some(namespace));
        }
    }

    #[test]
    fn an_unknown_prefix_is_not_a_namespace() {
        assert_eq!(Namespace::from_prefix("weird"), None);
    }

    #[test]
    fn the_namespace_comes_from_the_id() {
        assert_eq!(
            ComponentSpec::new("snk.file.csv", "CSV file").namespace,
            Namespace::Sink
        );
    }

    #[test]
    fn a_source_has_no_inputs_and_a_sink_has_no_outputs() {
        assert!(ComponentSpec::new("src.file.csv", "CSV").inputs.is_empty());
        assert!(ComponentSpec::new("snk.file.csv", "CSV").outputs.is_empty());
    }

    #[test]
    fn property_types_accept_only_their_own_shape() {
        assert!(PropertyType::Text.accepts(&json!("x")));
        assert!(!PropertyType::Text.accepts(&json!(1)));

        assert!(PropertyType::Bool.accepts(&json!(true)));
        assert!(!PropertyType::Bool.accepts(&json!("true")));

        assert!(PropertyType::Integer.accepts(&json!(7)));
        assert!(!PropertyType::Integer.accepts(&json!(7.5)));
        assert!(PropertyType::Number.accepts(&json!(7.5)));

        assert!(PropertyType::StringList.accepts(&json!(["a", "b"])));
        assert!(!PropertyType::StringList.accepts(&json!(["a", 2])));
        assert!(!PropertyType::StringList.accepts(&json!("a")));

        assert!(PropertyType::Map.accepts(&json!({"a": "b"})));
        assert!(PropertyType::Map.accepts(&json!({})));
        assert!(!PropertyType::Map.accepts(&json!({"a": 2})));
        assert!(!PropertyType::Map.accepts(&json!(["a"])));
    }

    #[test]
    fn labels_default_to_a_readable_form_of_the_name() {
        assert_eq!(PropertySpec::text("path").label, "Path");
        assert_eq!(PropertySpec::text("order_ts").label, "Order Ts");
        assert_eq!(PropertySpec::text("").label, "");
    }

    #[test]
    fn a_spec_round_trips_through_json() {
        let spec = ComponentSpec::new("src.file.csv", "CSV file")
            .description("Read a delimited text file.")
            .icon("file-text")
            .properties(vec![
                PropertySpec::path("path")
                    .required()
                    .help("The file to read."),
                PropertySpec::boolean("header").default(json!(true)),
            ]);

        let text = serde_json::to_string(&spec).unwrap();
        let parsed: ComponentSpec = serde_json::from_str(&text).unwrap();

        assert_eq!(parsed, spec);
    }
}
