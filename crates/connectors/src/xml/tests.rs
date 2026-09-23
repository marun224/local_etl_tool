use super::*;
use etl_plugin_sdk::Records;
use serde_json::json;

fn read(text: &str, record: &str) -> Result<Vec<Record>, ConnectorError> {
    let mut out: Vec<Record> = Vec::new();
    read_records(text.as_bytes(), record, &mut out)?;
    Ok(out)
}

fn record(value: JsonValue) -> Record {
    value.as_object().expect("an object").clone()
}

fn written(rows: Vec<JsonValue>, root: &str, name: &str) -> Result<String, ConnectorError> {
    let mut out = Vec::new();
    let mut input = Records(rows.into_iter().map(record));
    write_records(&mut out, root, name, &mut input)?;
    Ok(String::from_utf8(out).expect("utf-8"))
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

#[test]
fn each_record_element_is_a_row_and_its_children_are_columns() {
    let rows = read(
        r#"<?xml version="1.0"?>
        <orders>
          <order id="1001"><customer>C001</customer><amount>12.5</amount></order>
          <order id="1002"><customer>C002</customer><amount>7</amount></order>
        </orders>"#,
        "order",
    )
    .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        record(json!({"@id": "1001", "customer": "C001", "amount": "12.5"}))
    );
    assert_eq!(
        rows[1]["amount"], "7",
        "text, always: types come from columns"
    );
}

#[test]
fn entities_character_references_and_cdata_become_the_text_they_stand_for() {
    let rows = read(
        "<r><row><a>fish &amp; chips</a><b>&lt;tag&gt; &#65;&#x42;</b>\
         <c><![CDATA[1 < 2 && raw]]></c><d>it&apos;s &quot;q&quot;</d></row></r>",
        "row",
    )
    .unwrap();

    assert_eq!(
        rows[0]["a"], "fish & chips",
        "spaces around an entity survive"
    );
    assert_eq!(rows[0]["b"], "<tag> AB");
    assert_eq!(rows[0]["c"], "1 < 2 && raw");
    assert_eq!(rows[0]["d"], "it's \"q\"");
}

#[test]
fn an_empty_element_is_an_empty_string_and_a_missing_one_is_absent() {
    let rows = read("<r><row><a/><b></b></row><row><b>x</b></row></r>", "row").unwrap();

    assert_eq!(rows[0]["a"], "");
    assert_eq!(rows[0]["b"], "");
    assert!(
        !rows[1].contains_key("a"),
        "absent, which DuckDB reads as NULL -- not an empty string"
    );
}

#[test]
fn attributes_on_a_child_are_named_after_it() {
    let rows = read(
        r#"<r><row><amount currency="EUR">12</amount></row></r>"#,
        "row",
    )
    .unwrap();

    assert_eq!(rows[0]["amount"], "12");
    assert_eq!(rows[0]["amount@currency"], "EUR");
}

#[test]
fn a_record_with_only_attributes_is_still_a_row() {
    let rows = read(
        r#"<r><row id="1" name="a &amp; b"/><row id="2"/></r>"#,
        "row",
    )
    .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["@name"], "a & b");
    assert_eq!(rows[1], record(json!({"@id": "2"})));
}

#[test]
fn namespace_prefixes_are_ignored_when_matching_and_naming() {
    let rows = read(
        r#"<x:feed xmlns:x="urn:x"><x:item><x:title>Hi</x:title></x:item></x:feed>"#,
        "item",
    )
    .unwrap();

    assert_eq!(rows, vec![record(json!({"title": "Hi"}))]);
}

#[test]
fn records_are_found_at_any_depth() {
    let rows = read(
        "<export><meta><n>2</n></meta><data><page><row><a>1</a></row></page>\
         <page><row><a>2</a></row></page></data></export>",
        "row",
    )
    .unwrap();

    assert_eq!(rows.len(), 2, "the <n> in <meta> is not a row");
    assert_eq!(rows[1]["a"], "2");
}

#[test]
fn no_matching_elements_is_zero_rows_not_an_error() {
    assert!(read("<r><row><a>1</a></row></r>", "rows")
        .unwrap()
        .is_empty());
}

#[test]
fn nesting_deeper_than_one_level_is_refused_by_name() {
    let error = read(
        "<r><row><address><city>Oslo</city></address></row></r>",
        "row",
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("<address>"), "{error}");
    assert!(error.contains("<city>"), "{error}");
    assert!(error.contains("one level"), "{error}");
}

#[test]
fn a_repeated_child_is_a_list_and_is_refused() {
    let error = read("<r><row><tag>a</tag><tag>b</tag></row></r>", "row")
        .unwrap_err()
        .to_string();

    assert!(error.contains("<tag> appears more than once"), "{error}");
}

#[test]
fn text_beside_child_elements_is_refused() {
    let error = read("<r><row>loose<a>1</a></row></r>", "row")
        .unwrap_err()
        .to_string();

    assert!(error.contains("text of its own"), "{error}");
}

#[test]
fn a_document_that_is_not_well_formed_says_where() {
    let error = read("<r><row><a>1</b></row></r>", "row")
        .unwrap_err()
        .to_string();

    assert!(error.contains("not well-formed XML at byte"), "{error}");
}

#[test]
fn an_undeclared_entity_is_refused_rather_than_kept_literally() {
    let error = read("<r><row><a>&nbsp;</a></row></r>", "row")
        .unwrap_err()
        .to_string();

    assert!(error.contains("&nbsp;"), "{error}");
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

#[test]
fn a_row_is_written_as_one_element_with_a_child_per_column() {
    let text = written(
        vec![json!({"id": 1, "amount": 12.5, "paid": true, "note": "a<b & \"c\""})],
        "orders",
        "order",
    )
    .unwrap();

    assert_eq!(
        text,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <orders>\n\
         \x20 <order>\n\
         \x20   <id>1</id>\n\
         \x20   <amount>12.5</amount>\n\
         \x20   <paid>true</paid>\n\
         \x20   <note>a&lt;b &amp; &quot;c&quot;</note>\n\
         \x20 </order>\n\
         </orders>\n"
    );
}

#[test]
fn a_null_is_left_out_rather_than_written_as_empty() {
    let text = written(vec![json!({"a": "x", "b": null})], "r", "row").unwrap();

    assert!(text.contains("<a>x</a>"));
    assert!(!text.contains("<b"), "{text}");
}

#[test]
fn at_columns_become_attributes_again() {
    let text = written(
        vec![json!({"@id": "7", "amount": "12", "amount@currency": "EUR"})],
        "r",
        "row",
    )
    .unwrap();

    assert!(text.contains(r#"<row id="7">"#), "{text}");
    assert!(
        text.contains(r#"<amount currency="EUR">12</amount>"#),
        "{text}"
    );
}

#[test]
fn no_rows_is_an_empty_root_not_an_error() {
    let text = written(vec![], "orders", "order").unwrap();
    assert!(text.ends_with("<orders>\n</orders>\n"), "{text}");
}

#[test]
fn a_column_that_cannot_be_an_element_name_is_refused_by_name() {
    let error = written(vec![json!({"order id": 1})], "r", "row")
        .unwrap_err()
        .to_string();

    assert!(error.contains("'order id'"), "{error}");
    assert!(error.contains("xf.rename"), "and says what to do: {error}");
}

#[test]
fn a_list_or_struct_value_is_refused_by_column_and_row() {
    let error = written(vec![json!({"a": 1}), json!({"a": [1, 2]})], "r", "row")
        .unwrap_err()
        .to_string();

    assert!(error.contains("'a' in row 2"), "{error}");
}

#[test]
fn xml_names_follow_the_practical_subset() {
    for good in ["order", "_x", "a1", "a-b", "a.b", "résumé"] {
        assert!(is_xml_name(good), "{good}");
    }
    for bad in ["", "1a", "a b", "a:b", "-a", "a@b", "a<b"] {
        assert!(!is_xml_name(bad), "{bad}");
    }
}

// ---------------------------------------------------------------------------
// Both ways
// ---------------------------------------------------------------------------

#[test]
fn writing_what_was_read_reproduces_the_bytes() {
    // The property the sink exists to have: a flat document read and written
    // again comes back identical, entities, attributes and empties included.
    let original = written(
        vec![
            json!({"@id": "1", "name": "fish & chips", "note": "", "amount@currency": "EUR", "amount": "12.5"}),
            json!({"@id": "2", "name": "<b>", "amount": "7"}),
        ],
        "orders",
        "order",
    )
    .unwrap();

    let rows = read(&original, "order").unwrap();
    let again = written(
        rows.into_iter().map(JsonValue::Object).collect(),
        "orders",
        "order",
    )
    .unwrap();

    assert_eq!(again, original);
}

// ---------------------------------------------------------------------------
// Through the traits, against files
// ---------------------------------------------------------------------------

fn scratch(name: &str) -> PathBuf {
    let directory =
        std::env::temp_dir().join(format!("etl-connectors-xml-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    directory
}

#[test]
fn the_sink_writes_the_file_and_the_source_reads_it_back() {
    let directory = scratch("roundtrip");
    let context = Context {
        working_dir: Some(directory.clone()),
        ..Context::default()
    };

    let rows = vec![record(json!({"id": 1, "city": "Oslo"}))];
    let summary = XmlSink
        .write(
            &json!({"path": "out/cities.xml", "root": "cities", "record": "city_row"}),
            &mut Records(rows.into_iter()),
            &context,
        )
        .expect_err("the directory does not exist yet");

    // The engine creates a sink's parent directory before a run; called
    // directly, the connector reports the path it could not write.
    assert!(summary.to_string().contains("cities.xml"), "{summary}");

    std::fs::create_dir_all(directory.join("out")).unwrap();
    let rows = vec![record(json!({"id": 1, "city": "Oslo"}))];
    let summary = XmlSink
        .write(
            &json!({"path": "out/cities.xml", "root": "cities", "record": "city_row"}),
            &mut Records(rows.into_iter()),
            &context,
        )
        .unwrap();
    assert_eq!(summary.records, 1);

    let mut back: Vec<Record> = Vec::new();
    let summary = XmlSource
        .read(
            &json!({"path": "out/cities.xml", "record": "city_row"}),
            &mut back,
            &context,
        )
        .unwrap();

    assert_eq!(summary.records, 1);
    assert_eq!(back[0], record(json!({"id": "1", "city": "Oslo"})));
    assert!(
        !directory.join("out/cities.xml.partial").exists(),
        "the temporary file is renamed away"
    );

    let _ = std::fs::remove_dir_all(&directory);
}

#[test]
fn a_write_that_fails_partway_leaves_the_old_file_untouched() {
    let directory = scratch("atomic");
    let target = directory.join("keep.xml");
    std::fs::write(&target, "the previous good file").unwrap();

    let context = Context {
        working_dir: Some(directory.clone()),
        ..Context::default()
    };
    let rows = vec![record(json!({"ok": 1})), record(json!({"bad column": 2}))];

    let error = XmlSink
        .write(
            &json!({"path": "keep.xml", "root": "r", "record": "row"}),
            &mut Records(rows.into_iter()),
            &context,
        )
        .unwrap_err();

    assert!(error.to_string().contains("'bad column'"), "{error}");
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "the previous good file"
    );
    assert!(
        !directory.join("keep.xml.partial").exists(),
        "and no debris"
    );

    let _ = std::fs::remove_dir_all(&directory);
}

#[test]
fn a_missing_file_names_the_path() {
    let mut out: Vec<Record> = Vec::new();
    let error = XmlSource
        .read(
            &json!({"path": "no/such/file.xml", "record": "row"}),
            &mut out,
            &Context::default(),
        )
        .unwrap_err()
        .to_string();

    assert!(error.contains("file.xml"), "{error}");
}

#[test]
fn a_record_name_that_is_not_an_element_name_is_a_property_error() {
    let mut out: Vec<Record> = Vec::new();
    let error = XmlSource
        .read(
            &json!({"path": "x.xml", "record": "my row"}),
            &mut out,
            &Context::default(),
        )
        .unwrap_err()
        .to_string();

    assert!(error.starts_with("property 'record'"), "{error}");
}
