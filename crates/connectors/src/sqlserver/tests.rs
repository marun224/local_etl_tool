//! Against the local TDS fixture only (Settled decision 87): no SQL Server
//! runs here. What SQL Server itself does with the SQL sent is recorded as
//! not yet checked, in `docs/connectors.md`.

use super::fixture::{
    results, rows, serve, Cell, Kind, Options, Param, Reply, Seen, Tds, ENCRYPT_OFF, ENCRYPT_ON,
    ENCRYPT_REQ,
};
use super::*;
use etl_plugin_sdk::Record;
use std::time::Instant;

fn days(year: i64, month: i64, day: i64) -> u32 {
    (etl_state::time::days_from_civil(year, month, day) + DAYS_FROM_0001) as u32
}

fn days_1900(year: i64, month: i64, day: i64) -> i64 {
    etl_state::time::days_from_civil(year, month, day) + DAYS_FROM_1900
}

fn certificate(name: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/sqlserver")
        .join(name)
        .display()
        .to_string()
}

/// A connection to `tds` with `extra` on top: unencrypted unless it says.
fn base(tds: &Tds, extra: JsonValue) -> JsonValue {
    let mut properties = json!({
        "host": "127.0.0.1", "port": tds.port, "username": "etl", "password": "etl-secret",
        "database": "sales", "encryption": "none", "timeout_ms": 5000,
    });
    for (key, value) in extra.as_object().unwrap() {
        properties[key] = value.clone();
    }
    properties
}

fn read_with(
    properties: &JsonValue,
    checkpoint: Option<JsonValue>,
) -> Result<(Vec<Record>, Summary), ConnectorError> {
    let mut rows: Vec<Record> = Vec::new();
    let context = Context {
        checkpoint,
        ..Context::default()
    };
    let summary = SqlserverSource.read(properties, &mut rows, &context)?;
    Ok((rows, summary))
}

fn write(properties: &JsonValue, values: Vec<JsonValue>) -> Result<Summary, ConnectorError> {
    SqlserverSink.write(
        properties,
        &mut crate::fixture::records(values),
        &Context::default(),
    )
}

fn failure(result: Result<impl std::fmt::Debug, ConnectorError>) -> String {
    result.expect_err("should fail").to_string()
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

#[test]
fn every_type_arrives_as_the_other_sources_give_it() {
    let kinds = [
        ("id", Kind::Int(4)),
        ("big", Kind::Int(8)),
        ("tiny", Kind::Int(1)),
        ("flag", Kind::Bit),
        ("ratio", Kind::Float(4)),
        ("score", Kind::Float(8)),
        ("amount", Kind::Decimal(10, 2)),
        ("refund", Kind::Decimal(10, 2)),
        ("whole", Kind::Decimal(18, 0)),
        ("huge", Kind::Decimal(38, 0)),
        ("name", Kind::NVarChar(Some(50))),
        ("notes", Kind::NVarChar(None)),
        ("blob", Kind::VarBinary(Some(10))),
        ("row_guid", Kind::Guid),
        ("day", Kind::Date),
        ("at", Kind::Time(7)),
        ("updated_at", Kind::DateTime2(7)),
        ("placed_at", Kind::DateTimeOffset(7)),
        ("legacy", Kind::DateTime),
        ("minute", Kind::SmallDateTime),
        ("doc", Kind::Xml),
    ];
    let long = "x".repeat(5000);
    let full = vec![
        Cell::Int(1),
        Cell::Int(9_007_199_254_740_993),
        Cell::Int(255),
        Cell::Bit(true),
        Cell::Float(1.1),
        Cell::Float(2.5),
        Cell::Decimal(1234),
        Cell::Decimal(-5),
        Cell::Decimal(42),
        Cell::Decimal(10i128.pow(30)),
        Cell::Text("Zoë".into()),
        Cell::Text(long.clone()),
        Cell::Bytes(vec![0xDE, 0xAD]),
        Cell::Guid("6F9619FF-8B86-D011-B42D-00C04FC964FF".into()),
        Cell::Date(days(2026, 9, 24)),
        Cell::Time(495_301_234_567),
        Cell::DateTime2(days(2026, 9, 24), 495_301_234_567),
        // 13:45:30.1234567 at +05:30 is 08:15:30.1234567 UTC.
        Cell::DateTimeOffset(days(2026, 9, 24), 297_301_234_567, 330),
        Cell::DateTime(days_1900(2026, 9, 24) as i32, 49_530 * 300 + 1),
        Cell::SmallDateTime(days_1900(2026, 9, 24) as u16, 13 * 60 + 45),
        Cell::Text("<a>1</a>".into()),
    ];
    let empty = vec![Cell::Null; kinds.len()];
    let set = rows(&kinds, vec![full, empty]);
    let tds = serve(Options::default(), move |_, _| results(set.clone()));

    let (rows, summary) = read_with(&base(&tds, json!({"table": "dbo.orders"})), None).unwrap();

    assert_eq!(
        rows[0],
        *json!({
            "id": 1, "big": 9_007_199_254_740_993i64, "tiny": 255, "flag": true,
            "ratio": 1.1, "score": 2.5,
            "amount": "12.34", "refund": "-0.05", "whole": 42,
            "huge": "1000000000000000000000000000000",
            "name": "Zoë", "notes": long, "blob": "dead",
            "row_guid": "6F9619FF-8B86-D011-B42D-00C04FC964FF",
            "day": "2026-09-24", "at": "13:45:30.123456",
            "updated_at": "2026-09-24 13:45:30.123456",
            "placed_at": "2026-09-24 08:15:30.123456",
            "legacy": "2026-09-24 13:45:30.003333",
            "minute": "2026-09-24 13:45:00.000000",
            "doc": "<a>1</a>",
        })
        .as_object()
        .unwrap()
    );
    assert!(rows[1].values().all(JsonValue::is_null), "{:?}", rows[1]);
    assert_eq!(summary.records, 2);
    assert_eq!(
        summary.detail,
        format!(
            "2 row(s) from table [dbo].[orders] at 127.0.0.1:{}/sales as etl",
            tds.port
        )
    );

    let seen = tds.seen();
    let Seen::Login {
        user,
        password,
        database,
        app,
        encrypted,
    } = &seen[0]
    else {
        panic!("{seen:?}")
    };
    assert_eq!(
        (
            user.as_str(),
            password.as_str(),
            database.as_str(),
            *encrypted
        ),
        ("etl", "etl-secret", "sales", false)
    );
    assert!(app.starts_with("etl "), "{app}");
    assert_eq!(seen[1].sql(), "SELECT * FROM [dbo].[orders]");
    assert!(seen[1].params().is_empty());
}

#[test]
fn a_query_is_sent_as_written_and_max_records_stops_reading() {
    let set = rows(
        &[("id", Kind::Int(4))],
        (1..=5).map(|id| vec![Cell::Int(id)]).collect(),
    );
    let tds = serve(Options::default(), move |_, _| results(set.clone()));
    let properties = base(
        &tds,
        json!({"query": "SELECT id FROM dbo.orders ORDER BY id;", "max_records": 2}),
    );

    let (rows, summary) = read_with(&properties, None).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        tds.statements()[0].sql(),
        "SELECT id FROM dbo.orders ORDER BY id"
    );
    assert!(
        summary
            .detail
            .ends_with("; stopped at max_records, with more for the next run"),
        "{}",
        summary.detail
    );
}

#[test]
fn a_table_read_with_max_records_asks_for_top() {
    let set = rows(&[("id", Kind::Int(4))], vec![vec![Cell::Int(1)]]);
    let tds = serve(Options::default(), move |_, _| results(set.clone()));

    read_with(
        &base(&tds, json!({"table": "orders", "max_records": 3})),
        None,
    )
    .unwrap();

    assert_eq!(tds.statements()[0].sql(), "SELECT TOP (3) * FROM [orders]");
}

#[test]
fn incremental_runs_bind_the_saved_position_and_save_it_at_full_precision() {
    let set = rows(
        &[("id", Kind::Int(4)), ("updated_at", Kind::DateTime2(7))],
        vec![
            vec![Cell::Int(1), Cell::DateTime2(days(2026, 9, 24), 1)],
            vec![
                Cell::Int(2),
                Cell::DateTime2(days(2026, 9, 24), 495_301_234_567),
            ],
        ],
    );
    let tds = serve(Options::default(), move |_, _| results(set.clone()));
    let properties = base(
        &tds,
        json!({"table": "dbo.orders", "incremental_column": "updated_at"}),
    );

    let (_, first) = read_with(&properties, None).unwrap();
    let saved = first.checkpoint.clone().unwrap();
    assert_eq!(
        saved,
        json!({
            "read": "table [dbo].[orders]", "column": "updated_at", "type": "datetime2(7)",
            "value": "2026-09-24 13:45:30.1234567",
        })
    );
    assert!(first
        .detail
        .ends_with("; read up to updated_at = 2026-09-24 13:45:30.1234567"));

    read_with(&properties, Some(saved)).unwrap();
    let other =
        json!({"read": "table [dbo].[orders]", "column": "id", "type": "bigint", "value": "9"});
    let (_, third) = read_with(&properties, Some(other)).unwrap();

    let statements = tds.statements();
    let inner = "SELECT * FROM (SELECT * FROM [dbo].[orders]) AS [etl_read]";
    assert_eq!(
        statements[0].sql(),
        format!("{inner} WHERE [updated_at] IS NOT NULL ORDER BY [updated_at]")
    );
    assert_eq!(
        statements[1].sql(),
        format!("{inner} WHERE [updated_at] > CAST(@P1 AS datetime2(7)) ORDER BY [updated_at]")
    );
    let Seen::Rpc {
        declared, params, ..
    } = &statements[1]
    else {
        panic!("{:?}", statements[1])
    };
    assert_eq!(declared, "@P1 nvarchar(4000)");
    assert_eq!(params, &[Param::Text("2026-09-24 13:45:30.1234567".into())]);
    // A position saved for another column is set aside, and said so.
    assert_eq!(statements[2].sql(), statements[0].sql());
    assert!(third.detail.ends_with(
        "; the saved position was for table [dbo].[orders] by 'id', so this read started over"
    ));
}

#[test]
fn start_is_where_a_first_incremental_run_begins() {
    let set = rows(&[("id", Kind::Int(8))], vec![]);
    let tds = serve(Options::default(), move |_, _| results(set.clone()));
    let properties = base(
        &tds,
        json!({"query": "SELECT id FROM dbo.orders", "incremental_column": "id", "start": "1000", "max_records": 50}),
    );

    let (rows, summary) = read_with(&properties, None).unwrap();

    assert!(rows.is_empty());
    assert_eq!(summary.checkpoint, None, "nothing read, nothing saved");
    assert!(
        summary.detail.ends_with("; nothing new by id"),
        "{}",
        summary.detail
    );
    assert_eq!(
        tds.statements()[0].sql(),
        "SELECT TOP (50) * FROM (SELECT id FROM dbo.orders) AS [etl_read] WHERE [id] > (1000) ORDER BY [id]"
    );
}

#[test]
fn a_saved_position_is_text_sql_server_casts_back_exactly() {
    use tiberius::numeric::Numeric;
    use tiberius::time::{Date, DateTime, DateTime2, DateTimeOffset, SmallDateTime};

    let day = Date::new(days(2026, 9, 24));
    let cases: Vec<(ColumnData<'static>, &str, &str)> = vec![
        (ColumnData::I32(Some(7)), "bigint", "7"),
        (
            ColumnData::Numeric(Some(Numeric::new_with_scale(-1234, 2))),
            "decimal(38, 2)",
            "-12.34",
        ),
        (ColumnData::F64(Some(0.5)), "float", "0.5"),
        (
            ColumnData::String(Some("B-7".into())),
            "nvarchar(max)",
            "B-7",
        ),
        (ColumnData::Date(Some(day)), "date", "2026-09-24"),
        (
            ColumnData::Time(Some(Time::new(49_530_123, 3))),
            "time(3)",
            "13:45:30.123",
        ),
        (
            ColumnData::DateTime2(Some(DateTime2::new(day, Time::new(49_530, 0)))),
            "datetime2(0)",
            "2026-09-24 13:45:30",
        ),
        (
            ColumnData::DateTimeOffset(Some(DateTimeOffset::new(
                DateTime2::new(day, Time::new(297_301_234_567, 7)),
                330,
            ))),
            "datetimeoffset(7)",
            "2026-09-24 08:15:30.1234567 +00:00",
        ),
        // A datetime's 300ths, as SQL Server shows them: .003, .007, .010.
        (
            ColumnData::DateTime(Some(DateTime::new(
                days_1900(2026, 9, 24) as i32,
                49_530 * 300 + 2,
            ))),
            "datetime",
            "2026-09-24 13:45:30.007",
        ),
        (
            ColumnData::SmallDateTime(Some(SmallDateTime::new(days_1900(2026, 9, 24) as u16, 825))),
            "smalldatetime",
            "2026-09-24 13:45:00",
        ),
    ];
    for (data, kind, text) in cases {
        assert_eq!(cast(&data).as_deref(), Some(kind), "{data:?}");
        assert_eq!(exact(&data).as_deref(), Some(text), "{data:?}");
    }
    assert_eq!(cast(&ColumnData::Bit(Some(true))), None);
    assert_eq!(cast(&ColumnData::Binary(Some(vec![1].into()))), None);
}

#[test]
fn a_column_that_cannot_be_compared_is_refused_as_incremental() {
    let set = rows(&[("flag", Kind::Bit)], vec![vec![Cell::Bit(true)]]);
    let tds = serve(Options::default(), move |_, _| results(set.clone()));

    let error = failure(read_with(
        &base(&tds, json!({"table": "t", "incremental_column": "FLAG"})),
        None,
    ));

    assert!(
        error.contains("incremental_column") && error.contains("'FLAG' is Bitn"),
        "{error}"
    );
}

#[test]
fn what_a_row_cannot_hold_is_refused_not_dropped() {
    let two = rows(&[("id", Kind::Int(4)), ("id", Kind::Int(4))], vec![]);
    let unnamed = rows(&[("", Kind::Int(4))], vec![]);
    let first = rows(&[("id", Kind::Int(4))], vec![vec![Cell::Int(1)]]);
    let tds = serve(Options::default(), move |index, _| match index {
        0 => results(two.clone()),
        1 => results(unnamed.clone()),
        _ => Reply::Results(vec![first.clone(), first.clone()]),
    });
    let properties = base(&tds, json!({"query": "EXEC dbo.report"}));

    let named_twice = failure(read_with(&properties, None));
    let no_name = failure(read_with(&properties, None));
    let two_sets = failure(read_with(&properties, None));

    assert!(
        named_twice.contains("two columns named 'id'"),
        "{named_twice}"
    );
    assert!(no_name.contains("column 1 has no name"), "{no_name}");
    assert!(two_sets.contains("more than one result set"), "{two_sets}");
}

#[test]
fn a_column_the_client_cannot_read_is_an_error_naming_the_way_round() {
    let set = rows(&[("v", Kind::Variant)], vec![]);
    let tds = serve(Options::default(), move |_, _| results(set.clone()));

    let error = failure(read_with(&base(&tds, json!({"table": "t"})), None));

    assert!(error.starts_with("table [t] at 127.0.0.1:"), "{error}");
    assert!(
        error.contains("could not read what the server sent (not yet implemented: not yet implemented for SSVariant)"),
        "{error}"
    );
    assert!(
        error.ends_with("is read by CASTing it in a query"),
        "{error}"
    );
}

#[test]
fn sql_server_errors_are_named_with_their_number_and_never_the_password() {
    let tds = serve(Options::default(), |_, _| {
        Reply::Error(208, "Invalid object name 'dbo.nope'.".into())
    });

    let error = failure(read_with(&base(&tds, json!({"table": "dbo.nope"})), None));

    assert_eq!(
        error,
        format!(
            "table [dbo].[nope] at 127.0.0.1:{}/sales as etl: SQL Server error 208: Invalid \
             object name 'dbo.nope'.",
            tds.port
        )
    );
    assert!(!error.contains("etl-secret"));
}

#[test]
fn an_error_after_some_rows_fails_the_read() {
    let set = rows(&[("id", Kind::Int(4))], vec![vec![Cell::Int(1)]]);
    let tds = serve(Options::default(), move |_, _| {
        Reply::RowsThenError(
            set.clone(),
            8134,
            "Divide by zero error encountered.".into(),
        )
    });

    let error = failure(read_with(
        &base(&tds, json!({"query": "SELECT 1/0 AS x"})),
        None,
    ));

    assert!(
        error.contains("SQL Server error 8134: Divide by zero"),
        "{error}"
    );
}

#[test]
fn an_order_by_in_an_incremental_query_is_explained() {
    let tds = serve(Options::default(), |_, _| {
        Reply::Error(
            1033,
            "The ORDER BY clause is invalid in views, inline functions, derived tables, \
             subqueries, and common table expressions, unless TOP, OFFSET or FOR XML is also \
             specified."
                .into(),
        )
    });
    let properties = base(
        &tds,
        json!({"query": "SELECT * FROM t ORDER BY id", "incremental_column": "id"}),
    );

    let error = failure(read_with(&properties, None));

    assert!(error.contains("SQL Server error 1033"), "{error}");
    assert!(error.ends_with("leave the ORDER BY out"), "{error}");
}

#[test]
fn a_wrong_password_and_a_missing_database_are_named_without_the_password() {
    let tds = serve(Options::default(), |_, _| Reply::Changed(0));

    let wrong = failure(read_with(
        &base(&tds, json!({"table": "t", "password": "not-it"})),
        None,
    ));
    let missing = failure(read_with(
        &base(&tds, json!({"table": "t", "database": "nope"})),
        None,
    ));

    assert_eq!(
        wrong,
        format!(
            "SQL Server at 127.0.0.1:{}/sales as etl: SQL Server error 18456: Login failed for \
             user 'etl'.",
            tds.port
        )
    );
    assert!(!wrong.contains("not-it"));
    assert!(
        missing.contains(
            "SQL Server error 4060: Cannot open database \"nope\" requested by the login."
        ),
        "{missing}"
    );
}

#[test]
fn a_server_that_says_nothing_meets_the_deadline() {
    let tds = serve(Options::default(), |_, _| Reply::Silent);
    let started = Instant::now();

    let error = failure(read_with(
        &base(&tds, json!({"table": "t", "timeout_ms": 300})),
        None,
    ));

    assert!(error.ends_with("no answer within 300 ms"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn a_redirect_from_the_gateway_is_followed_once() {
    let set = rows(&[("id", Kind::Int(4))], vec![vec![Cell::Int(7)]]);
    let behind = serve(Options::default(), move |_, _| results(set.clone()));
    let gateway = serve(
        Options {
            route_to: Some(("127.0.0.1".into(), behind.port)),
            ..Options::default()
        },
        |_, _| panic!("the gateway runs nothing"),
    );

    let (rows, _) = read_with(&base(&gateway, json!({"table": "t"})), None).unwrap();

    assert_eq!(rows[0]["id"], json!(7));
    assert_eq!(behind.statements().len(), 1);
    assert!(gateway.statements().is_empty());
}

// ---------------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------------

fn tls_server(encryption: u8) -> Tds {
    let set = rows(&[("id", Kind::Int(4))], vec![vec![Cell::Int(1)]]);
    serve(
        Options {
            encryption,
            tls: true,
            ..Options::default()
        },
        move |_, _| results(set.clone()),
    )
}

fn login_encrypted(tds: &Tds) -> bool {
    match &tds.seen()[0] {
        Seen::Login { encrypted, .. } => *encrypted,
        other => panic!("{other:?}"),
    }
}

#[test]
fn required_encryption_checks_the_server_against_ca_cert() {
    let tds = tls_server(ENCRYPT_ON);
    let properties = base(
        &tds,
        json!({"host": "localhost", "table": "t", "encryption": "required", "ca_cert": certificate("ca.pem")}),
    );

    let (rows, _) = read_with(&properties, None).unwrap();

    assert_eq!(rows.len(), 1);
    assert!(login_encrypted(&tds));
}

#[test]
fn a_server_another_authority_signed_is_refused() {
    let tds = tls_server(ENCRYPT_ON);
    let properties = base(
        &tds,
        json!({"host": "localhost", "table": "t", "encryption": "required", "ca_cert": certificate("other-ca.pem")}),
    );

    let error = failure(read_with(&properties, None));

    assert!(error.contains("certificate"), "{error}");
    assert!(tds.statements().is_empty());
}

#[test]
fn trust_server_certificate_encrypts_without_checking() {
    let tds = tls_server(ENCRYPT_REQ);
    let properties = base(
        &tds,
        json!({"host": "localhost", "table": "t", "encryption": "required", "trust_server_certificate": true}),
    );

    read_with(&properties, None).unwrap();

    assert!(login_encrypted(&tds));
}

#[test]
fn login_only_encrypts_the_sign_in_and_nothing_after() {
    let tds = tls_server(ENCRYPT_OFF);
    let properties = base(
        &tds,
        json!({"host": "localhost", "table": "t", "encryption": "login_only", "ca_cert": certificate("ca.pem")}),
    );

    let (rows, _) = read_with(&properties, None).unwrap();

    assert_eq!(
        rows.len(),
        1,
        "the statement went unencrypted and was answered"
    );
    assert!(login_encrypted(&tds));
}

#[test]
fn encryption_none_refuses_a_server_that_insists_rather_than_trust_this_machine() {
    let tds = tls_server(ENCRYPT_REQ);

    let error = failure(read_with(&base(&tds, json!({"table": "t"})), None));

    assert!(
        error.ends_with(
            "insists on encryption, and encryption is none: choose required, with ca_cert or \
             trust_server_certificate"
        ),
        "{error}"
    );
    assert!(tds.seen().is_empty(), "it never signed in");
}

#[test]
fn what_to_trust_is_settled_before_connecting() {
    let server = |extra: JsonValue| {
        let mut properties = json!({"host": "sql.local", "username": "etl"});
        for (key, value) in extra.as_object().unwrap() {
            properties[key] = value.clone();
        }
        Server::from(&properties)
    };
    let refused = |extra: JsonValue| server(extra).unwrap_err().to_string();

    assert!(refused(json!({}))
        .contains("property 'ca_cert': or trust_server_certificate is needed to encrypt"));
    assert!(
        refused(json!({"ca_cert": "ca.pem", "trust_server_certificate": true}))
            .contains("give one")
    );
    assert!(refused(json!({"encryption": "none", "ca_cert": "ca.pem"}))
        .contains("no certificate to check"));
    assert!(refused(json!({"host": "sql.local\\SALES"})).contains("give the instance's port"));
    assert!(refused(json!({"host": "sql.local:1433"})).contains("the port goes in port"));
    assert!(refused(json!({"encryption": "strict"})).contains("not one of required"));
    assert!(refused(json!({"port": 70000, "encryption": "none"})).contains("not a TCP port"));

    assert_eq!(
        server(json!({"encryption": "none"})).unwrap().trust,
        Trust::Refuse
    );
    let trusting = server(json!({"trust_server_certificate": true})).unwrap();
    assert_eq!(
        (trusting.trust, trusting.encryption, trusting.port),
        (Trust::All, EncryptionLevel::Required, 1433)
    );
    assert_eq!(
        server(json!({"encryption": "login_only", "ca_cert": " ca.pem "}))
            .unwrap()
            .trust,
        Trust::Ca("ca.pem".into())
    );
}

#[test]
fn ca_cert_must_be_one_certificate_in_a_file_the_client_reads() {
    let dir = std::env::temp_dir().join(format!("etl-sqlserver-ca-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ca = std::fs::read_to_string(certificate("ca.pem")).unwrap();
    let two = dir.join("two.pem");
    std::fs::write(&two, format!("{ca}{ca}")).unwrap();
    let key = dir.join("ca.key");
    std::fs::write(&key, &ca).unwrap();
    let tds = serve(Options::default(), |_, _| Reply::Changed(0));
    let with = |path: &Path| {
        failure(read_with(
            &base(
                &tds,
                json!({"table": "t", "encryption": "required", "ca_cert": path.display().to_string()}),
            ),
            None,
        ))
    };

    assert!(
        with(&two).contains("holds 2 certificates"),
        "{}",
        with(&two)
    );
    assert!(with(&key).contains("is not a .pem, .crt or .der file"));
    assert!(with(&dir.join("missing.pem")).starts_with("property 'ca_cert': "));
    assert!(tds.seen().is_empty(), "refused before connecting");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

#[test]
fn append_binds_text_in_batches_inside_the_parameter_limit() {
    let tds = serve(Options::default(), |_, _| Reply::Changed(0));
    let mut values: Vec<JsonValue> = (1..=1000)
        .map(|id| json!({"id": id, "name": format!("n{id}"), "amount": 12.5}))
        .collect();
    values[1] = json!({"id": 2, "name": null, "amount": true});
    values[2] = json!({"id": 3, "name": {"a": [1]}});

    let summary = write(&base(&tds, json!({"table": "dbo.orders"})), values).unwrap();

    let statements = tds.statements();
    // 3 columns: 666 rows (1,998 parameters) a statement.
    assert_eq!(statements.len(), 2);
    assert!(statements[0]
        .sql()
        .starts_with("INSERT INTO [dbo].[orders] ([id], [name], [amount]) VALUES (@P1, @P2, @P3), (@P4, @P5, @P6), "));
    assert!(statements[0].sql().ends_with("(@P1996, @P1997, @P1998)"));
    assert_eq!(statements[0].params().len(), 1998);
    assert_eq!(statements[1].params().len(), 334 * 3);
    assert_eq!(
        &statements[0].params()[..9],
        &[
            Param::Text("1".into()),
            Param::Text("n1".into()),
            Param::Text("12.5".into()),
            Param::Text("2".into()),
            Param::Null,
            Param::Text("true".into()),
            Param::Text("3".into()),
            Param::Text(r#"{"a":[1]}"#.into()),
            Param::Null,
        ]
    );
    assert_eq!(summary.records, 1000);
    assert_eq!(
        summary.detail,
        format!(
            "1000 row(s) appended to [dbo].[orders] at 127.0.0.1:{}/sales as etl in 2 INSERT(s)",
            tds.port
        )
    );
}

#[test]
fn truncate_empties_the_table_first() {
    let tds = serve(Options::default(), |_, _| Reply::Changed(1));

    let summary = write(
        &base(&tds, json!({"table": "orders", "mode": "truncate"})),
        vec![json!({"id": 1})],
    )
    .unwrap();

    let statements = tds.statements();
    assert_eq!(statements[0].sql(), "TRUNCATE TABLE [orders]");
    assert_eq!(
        statements[1].sql(),
        "INSERT INTO [orders] ([id]) VALUES (@P1)"
    );
    assert!(summary
        .detail
        .starts_with("1 row(s) replaced the rows of [orders]"));
}

#[test]
fn a_failed_batch_says_how_many_rows_stay() {
    let tds = serve(Options::default(), |index, _| match index {
        0 => Reply::Changed(1000),
        _ => Reply::Error(
            547,
            "The INSERT statement conflicted with the FOREIGN KEY constraint \"fk_customer\"."
                .into(),
        ),
    });
    let values = (1..=2500).map(|id| json!({"id": id})).collect();

    let error = failure(write(&base(&tds, json!({"table": "dbo.orders"})), values));

    assert!(error.contains("SQL Server error 547"), "{error}");
    assert!(
        error.ends_with("1000 row(s) had been inserted into [dbo].[orders] before this, and stay"),
        "{error}"
    );
}

#[test]
fn merge_stages_every_row_then_applies_one_merge() {
    let tds = serve(Options::default(), |_, seen| {
        if seen.sql().starts_with("MERGE") {
            Reply::Changed(2)
        } else {
            Reply::Changed(0)
        }
    });
    let properties = base(
        &tds,
        json!({"table": "dbo.customers", "mode": "merge", "key_columns": ["id"]}),
    );

    let summary = write(
        &properties,
        vec![
            json!({"id": 1, "name": "Ada"}),
            json!({"id": 2, "name": "Grace"}),
            json!({"id": 1, "name": "Ada L."}),
        ],
    )
    .unwrap();

    let statements = tds.statements();
    assert_eq!(statements.len(), 4, "{statements:?}");
    // The staging table is made by a batch: made through sp_executesql it
    // would be dropped when that call ended.
    assert!(
        matches!(statements[0], Seen::Batch(_)),
        "{:?}",
        statements[0]
    );
    assert_eq!(
        statements[0].sql(),
        "SELECT CAST(NULL AS bigint) AS [etl_row], [id], [name] INTO #etl_stage FROM \
         [dbo].[customers] WHERE 1 = 0 UNION ALL SELECT CAST(NULL AS bigint), [id], [name] FROM \
         [dbo].[customers] WHERE 1 = 0"
    );
    assert_eq!(
        statements[1].sql(),
        "INSERT INTO #etl_stage ([id], [name], [etl_row]) VALUES (@P1, @P2, @P3), (@P4, @P5, @P6), \
         (@P7, @P8, @P9)"
    );
    let order: Vec<Param> = statements[1]
        .params()
        .iter()
        .skip(2)
        .step_by(3)
        .cloned()
        .collect();
    assert_eq!(
        order,
        vec![
            Param::Text("1".into()),
            Param::Text("2".into()),
            Param::Text("3".into())
        ]
    );
    assert_eq!(
        statements[2].sql(),
        "MERGE [dbo].[customers] WITH (HOLDLOCK) AS [t] USING (SELECT [id], [name] FROM (SELECT \
         *, ROW_NUMBER() OVER (PARTITION BY [id] ORDER BY [etl_row] DESC) AS [etl_rank] FROM \
         #etl_stage) AS [r] WHERE [etl_rank] = 1) AS [s] ON [t].[id] = [s].[id] WHEN MATCHED \
         THEN UPDATE SET [t].[name] = [s].[name] WHEN NOT MATCHED BY TARGET THEN INSERT ([id], \
         [name]) VALUES ([s].[id], [s].[name]);"
    );
    assert!(matches!(statements[3], Seen::Batch(_)));
    assert_eq!(statements[3].sql(), "DROP TABLE #etl_stage");
    assert_eq!(summary.records, 3);
    assert!(
        summary.detail.ends_with(
            "on id: staged in 1 INSERT(s), then 2 row(s) inserted or updated by one MERGE"
        ),
        "{}",
        summary.detail
    );
}

#[test]
fn a_merge_on_every_column_only_inserts() {
    let sql = merge_statement("[t1]", &["a".into(), "b".into()], &["a".into(), "b".into()]);

    assert!(!sql.contains("WHEN MATCHED"), "{sql}");
    assert!(sql.contains("ON [t].[a] = [s].[a] AND [t].[b] = [s].[b] WHEN NOT MATCHED BY TARGET"));
}

#[test]
fn a_failed_merge_changes_nothing_and_says_so() {
    let tds = serve(Options::default(), |_, seen| {
        if seen.sql().starts_with("MERGE") {
            Reply::Error(
                2627,
                "Violation of PRIMARY KEY constraint 'pk_customers'.".into(),
            )
        } else {
            Reply::Changed(0)
        }
    });
    let properties = base(
        &tds,
        json!({"table": "customers", "mode": "merge", "key_columns": ["id"]}),
    );

    let error = failure(write(&properties, vec![json!({"id": 1})]));

    assert!(error.contains("SQL Server error 2627"), "{error}");
    assert!(error.ends_with("Nothing was merged into [customers]: MERGE is one statement"));
}

#[test]
fn what_the_sink_is_told_is_checked() {
    let settings = |extra: JsonValue| {
        let mut properties = json!({"table": "orders"});
        for (key, value) in extra.as_object().unwrap() {
            properties[key] = value.clone();
        }
        SinkSettings::from(&properties)
    };
    let refused = |extra: JsonValue| settings(extra).unwrap_err().to_string();

    assert!(refused(json!({"mode": "merge"}))
        .contains("property 'key_columns': is required with mode merge"));
    assert!(refused(json!({"key_columns": ["id"]})).contains("goes with mode merge"));
    assert!(refused(json!({"mode": "upsert"})).contains("not one of append, truncate, merge"));
    assert!(refused(json!({"query": "SELECT 1"})).contains("is for reading"));
    assert!(refused(json!({"table": "a.b.c.d"})).contains("database.schema.name"));
    assert_eq!(
        settings(json!({"mode": "merge", "key_columns": ["id", " region "]}))
            .unwrap()
            .mode,
        Mode::Merge(vec!["id".into(), "region".into()])
    );

    let tds = serve(Options::default(), |_, _| Reply::Changed(0));
    let missing_key = failure(write(
        &base(
            &tds,
            json!({"table": "t", "mode": "merge", "key_columns": ["code"]}),
        ),
        vec![json!({"id": 1})],
    ));
    let extra_column = failure(write(
        &base(&tds, json!({"table": "t"})),
        vec![json!({"id": 1}), json!({"id": 2, "late": true})],
    ));
    assert!(missing_key.contains("'code' is not a column of the rows written"));
    assert!(extra_column.contains("row 2 has a column 'late' the first row did not"));
}

#[test]
fn names_are_bracketed_as_sql_server_quotes_them() {
    assert_eq!(table_name("dbo.orders").unwrap(), "[dbo].[orders]");
    assert_eq!(
        table_name("[sales].[dbo].[my]]t]").unwrap(),
        "[sales].[dbo].[my]]t]"
    );
    assert_eq!(identifier("a]b"), "[a]]b]");
    assert!(table_name("dbo..orders").is_err());
}

#[test]
fn the_specs_say_what_each_side_takes() {
    let source = SqlserverSource.spec();
    let sink = SqlserverSink.spec();

    for name in [
        "host",
        "port",
        "database",
        "username",
        "password",
        "encryption",
        "ca_cert",
    ] {
        assert!(source.property(name).is_some(), "{name}");
        assert!(sink.property(name).is_some(), "{name}");
    }
    for name in [
        "table",
        "query",
        "incremental_column",
        "start",
        "max_records",
        "columns",
    ] {
        assert!(source.property(name).is_some(), "{name}");
    }
    assert!(sink.property("key_columns").is_some());
}
