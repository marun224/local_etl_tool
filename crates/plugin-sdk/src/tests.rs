use super::*;
use serde_json::json;

fn record(value: JsonValue) -> Record {
    value.as_object().expect("an object").clone()
}

#[test]
fn a_vec_collects_what_a_source_writes() {
    let mut out: Vec<Record> = Vec::new();
    let writer: &mut dyn RecordWriter = &mut out;

    writer.write(record(json!({"id": "1"}))).unwrap();
    writer.write(record(json!({"id": "2"}))).unwrap();

    assert_eq!(out.len(), 2);
    assert_eq!(out[1]["id"], "2");
}

#[test]
fn records_are_read_back_in_order_and_then_end() {
    let mut input = Records(vec![record(json!({"n": 1})), record(json!({"n": 2}))].into_iter());

    assert_eq!(input.read().unwrap().unwrap()["n"], 1);
    assert_eq!(input.read().unwrap().unwrap()["n"], 2);
    assert!(input.read().unwrap().is_none());
    assert!(input.read().unwrap().is_none(), "and stays ended");
}

#[test]
fn a_relative_path_resolves_against_the_workspace() {
    let context = Context {
        working_dir: Some(PathBuf::from("ws")),
        ..Context::default()
    };

    assert_eq!(
        context.resolve("data/a.xml"),
        PathBuf::from("ws").join("data/a.xml")
    );
    assert_eq!(Context::default().resolve("a.xml"), PathBuf::from("a.xml"));
}

#[test]
fn an_error_names_what_it_is_about() {
    let error = ConnectorError::property("record", "must be an element name");
    assert_eq!(
        error.to_string(),
        "property 'record': must be an element name"
    );

    let error = ConnectorError::io(
        Path::new("data/missing.xml"),
        std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
    );
    assert!(error.to_string().starts_with("data/missing.xml: "));
}

#[test]
fn the_columns_property_is_a_map() {
    let spec = columns_property();
    assert_eq!(spec.name, "columns");
    assert_eq!(spec.property_type, etl_metadata::PropertyType::Map);
    assert!(!spec.required, "unset means DuckDB infers the types");
}
