//! A local server speaking enough TDS for the SQL Server connector's tests: no
//! container, no SQL Server (Settled decision 87). It records every sign-in
//! and statement, and answers each statement with whatever the test's handler
//! says, encoded as SQL Server encodes it (MS-TDS, TDS 7.4).
//!
//! It is also a check on itself: `tiberius`, a client used against real SQL
//! Servers, decodes everything it sends, so an answer encoded wrongly fails
//! the test that sent it.
//!
//! **TLS** is TDS's own arrangement: the handshake travels inside PRELOGIN
//! packets, then the connection is TLS throughout (`encryption: required`) or,
//! for a login-only encryption, only until the LOGIN7 message has been read.

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// What the fixture is told, and what it saw
// ---------------------------------------------------------------------------

/// PRELOGIN's encryption values.
pub(crate) const ENCRYPT_OFF: u8 = 0;
pub(crate) const ENCRYPT_ON: u8 = 1;
pub(crate) const ENCRYPT_NOT_SUP: u8 = 2;
pub(crate) const ENCRYPT_REQ: u8 = 3;

/// How the server behaves before any statement.
#[derive(Clone)]
pub(crate) struct Options {
    pub(crate) user: String,
    pub(crate) password: String,
    /// The databases a login may name. Unnamed means `master`.
    pub(crate) databases: Vec<String>,
    /// What PRELOGIN answers about encryption.
    pub(crate) encryption: u8,
    /// Whether it can do TLS at all, with `tests/fixtures/sqlserver/server.pem`.
    pub(crate) tls: bool,
    /// Where to send the client after signing it in, as Azure SQL's gateway
    /// does.
    pub(crate) route_to: Option<(String, u16)>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            user: "etl".into(),
            password: "etl-secret".into(),
            databases: vec!["master".into(), "sales".into()],
            encryption: ENCRYPT_NOT_SUP,
            tls: false,
            route_to: None,
        }
    }
}

/// A parameter of an `sp_executesql` call.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Param {
    Null,
    Text(String),
    Int(i64),
    Float(f64),
    Bit(bool),
}

impl Param {
    pub(crate) fn text(&self) -> Option<&str> {
        match self {
            Param::Text(text) => Some(text),
            _ => None,
        }
    }
}

/// One thing the fixture received.
#[derive(Clone, Debug)]
pub(crate) enum Seen {
    Login {
        user: String,
        password: String,
        database: String,
        app: String,
        /// Whether the LOGIN7 message came over TLS.
        encrypted: bool,
    },
    /// SQL sent as a batch: text, no parameters.
    Batch(String),
    /// SQL sent through `sp_executesql`, with its declarations and values.
    Rpc {
        sql: String,
        declared: String,
        params: Vec<Param>,
    },
}

impl Seen {
    /// The SQL of a statement; empty for a sign-in.
    pub(crate) fn sql(&self) -> &str {
        match self {
            Seen::Batch(sql) | Seen::Rpc { sql, .. } => sql,
            Seen::Login { .. } => "",
        }
    }

    pub(crate) fn params(&self) -> &[Param] {
        match self {
            Seen::Rpc { params, .. } => params,
            _ => &[],
        }
    }
}

/// A column's type, as COLMETADATA describes it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Kind {
    /// `tinyint`, `smallint`, `int`, `bigint`: 1, 2, 4 or 8 bytes.
    Int(u8),
    Bit,
    /// `real` or `float`: 4 or 8 bytes.
    Float(u8),
    /// Precision and scale.
    Decimal(u8, u8),
    /// `nvarchar(n)`; `None` is `nvarchar(max)`.
    NVarChar(Option<u16>),
    /// `varbinary(n)`; `None` is `varbinary(max)`.
    VarBinary(Option<u16>),
    Guid,
    Date,
    /// With its scale.
    Time(u8),
    DateTime2(u8),
    DateTimeOffset(u8),
    DateTime,
    SmallDateTime,
    Xml,
    /// `sql_variant`, which `tiberius` cannot read.
    Variant,
}

/// One value of a row.
#[derive(Clone, Debug)]
pub(crate) enum Cell {
    Null,
    Int(i64),
    Bit(bool),
    Float(f64),
    /// The unscaled value: 12.34 in a scale-2 column is 1234.
    Decimal(i128),
    Text(String),
    Bytes(Vec<u8>),
    /// As SQL Server shows it: `6F9619FF-8B86-D011-B42D-00C04FC964FF`.
    Guid(String),
    /// Days since 0001-01-01.
    Date(u32),
    /// Increments of the column's scale since midnight.
    Time(u64),
    DateTime2(u32, u64),
    /// Days and increments in UTC, and the offset in minutes.
    DateTimeOffset(u32, u64, i16),
    /// Days since 1900-01-01 and 300ths of a second.
    DateTime(i32, u32),
    /// Days since 1900-01-01 and minutes.
    SmallDateTime(u16, u16),
}

/// One result set.
#[derive(Clone, Debug)]
pub(crate) struct Rows {
    pub(crate) columns: Vec<(String, Kind)>,
    pub(crate) rows: Vec<Vec<Cell>>,
}

pub(crate) fn rows(columns: &[(&str, Kind)], rows: Vec<Vec<Cell>>) -> Rows {
    Rows {
        columns: columns
            .iter()
            .map(|(name, kind)| (name.to_string(), *kind))
            .collect(),
        rows,
    }
}

/// How a statement is answered.
#[derive(Clone, Debug)]
pub(crate) enum Reply {
    /// Result sets, one after another.
    Results(Vec<Rows>),
    /// Rows changed and no result.
    Changed(u64),
    /// SQL Server's error number and message.
    Error(u32, String),
    /// Some rows, then an error part-way.
    RowsThenError(Rows, u32, String),
    /// Nothing, ever: for a deadline.
    Silent,
}

pub(crate) fn results(set: Rows) -> Reply {
    Reply::Results(vec![set])
}

/// The server, listening on 127.0.0.1.
pub(crate) struct Tds {
    pub(crate) port: u16,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Tds {
    /// Everything received, sign-ins included.
    pub(crate) fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// The statements received, without sign-ins.
    pub(crate) fn statements(&self) -> Vec<Seen> {
        self.seen()
            .into_iter()
            .filter(|seen| !matches!(seen, Seen::Login { .. }))
            .collect()
    }
}

/// Serve `handler`, told how many statements came before this one.
pub(crate) fn serve<F>(options: Options, handler: F) -> Tds
where
    F: Fn(usize, &Seen) -> Reply + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    let handler = Arc::new(handler);
    let statements = Arc::new(AtomicUsize::new(0));
    let tls = options.tls.then(server_tls);
    std::thread::spawn(move || {
        for tcp in listener.incoming().flatten() {
            let (options, log, handler, statements, tls) = (
                options.clone(),
                Arc::clone(&log),
                Arc::clone(&handler),
                Arc::clone(&statements),
                tls.clone(),
            );
            std::thread::spawn(move || {
                let _ = connection(tcp, &options, tls, &log, &*handler, &statements);
            });
        }
    });
    Tds { port, seen }
}

fn server_tls() -> Arc<rustls::ServerConfig> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sqlserver");
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(dir.join("server.pem"))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let key = PrivateKeyDer::from_pem_file(dir.join("server.key")).unwrap();
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .unwrap();
    // A TLS 1.3 ticket would arrive after the handshake, which TDS does not
    // wrap; SQL Server sends none either.
    config.send_tls13_tickets = 0;
    Arc::new(config)
}

// ---------------------------------------------------------------------------
// Packets
// ---------------------------------------------------------------------------

const PRELOGIN: u8 = 0x12;
const REPLY: u8 = 0x04;
const BATCH: u8 = 0x01;
const RPC: u8 = 0x03;
const PACKET: usize = 4096;

/// The connection, plain or inside TLS.
enum Wire {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ServerConnection, TcpStream>>),
}

impl Read for Wire {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Wire::Plain(tcp) => tcp.read(buf),
            Wire::Tls(tls) => tls.read(buf),
        }
    }
}

impl Write for Wire {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Wire::Plain(tcp) => tcp.write(buf),
            Wire::Tls(tls) => tls.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Wire::Plain(tcp) => tcp.flush(),
            Wire::Tls(tls) => tls.flush(),
        }
    }
}

/// One packet: its type, whether it ends the message, its payload.
fn read_packet(wire: &mut impl Read) -> io::Result<(u8, bool, Vec<u8>)> {
    let mut header = [0u8; 8];
    wire.read_exact(&mut header)?;
    let length = usize::from(u16::from_be_bytes([header[2], header[3]]));
    let mut payload = vec![0u8; length.saturating_sub(8)];
    wire.read_exact(&mut payload)?;
    Ok((header[0], header[1] & 0x01 == 0x01, payload))
}

/// One message, however many packets it spans.
fn read_message(wire: &mut impl Read) -> io::Result<(u8, Vec<u8>)> {
    let mut message = Vec::new();
    loop {
        let (kind, last, payload) = read_packet(wire)?;
        message.extend(payload);
        if last {
            return Ok((kind, message));
        }
    }
}

fn write_message(wire: &mut impl Write, kind: u8, payload: &[u8]) -> io::Result<()> {
    let chunks: Vec<&[u8]> = if payload.is_empty() {
        vec![&[]]
    } else {
        payload.chunks(PACKET - 8).collect()
    };
    let count = chunks.len();
    for (index, chunk) in chunks.into_iter().enumerate() {
        let length = (chunk.len() + 8) as u16;
        let status = u8::from(index + 1 == count);
        let mut packet = vec![kind, status];
        packet.extend(length.to_be_bytes());
        packet.extend([0, 0, (index + 1) as u8, 0]);
        packet.extend(chunk);
        wire.write_all(&packet)?;
    }
    wire.flush()
}

// ---------------------------------------------------------------------------
// A connection
// ---------------------------------------------------------------------------

fn connection(
    mut tcp: TcpStream,
    options: &Options,
    tls: Option<Arc<rustls::ServerConfig>>,
    log: &Mutex<Vec<Seen>>,
    handler: &(dyn Fn(usize, &Seen) -> Reply + Send + Sync),
    statements: &AtomicUsize,
) -> io::Result<()> {
    // PRELOGIN: the client's wish, and the server's answer.
    let (_, prelogin) = read_message(&mut tcp)?;
    let wanted = prelogin_encryption(&prelogin).unwrap_or(ENCRYPT_NOT_SUP);
    write_message(&mut tcp, REPLY, &prelogin_answer(options.encryption))?;

    // What `tiberius` then does: nothing, TLS for the sign-in, or TLS for all.
    let encrypt = match (wanted, options.encryption) {
        (ENCRYPT_NOT_SUP, ENCRYPT_NOT_SUP) => None,
        (ENCRYPT_OFF, ENCRYPT_OFF) => Some(false),
        _ => Some(true),
    };
    let mut wire = match (encrypt, tls) {
        (None, _) => Wire::Plain(tcp),
        // The client will start a handshake this server cannot answer.
        (Some(_), None) => return Ok(()),
        (Some(_), Some(config)) => {
            let session = handshake(&mut tcp, config)?;
            Wire::Tls(Box::new(rustls::StreamOwned::new(session, tcp)))
        }
    };

    let (_, login) = read_message(&mut wire)?;
    let login = parse_login(&login, encrypt.is_some());
    if encrypt == Some(false) {
        // Encrypt=false: only the sign-in went over TLS.
        wire = match wire {
            Wire::Tls(tls) => Wire::Plain(tls.sock),
            plain => plain,
        };
    }
    let Seen::Login {
        user,
        password,
        database,
        ..
    } = &login
    else {
        unreachable!()
    };
    let refusal = if user != &options.user || password != &options.password {
        Some(vec![(
            18456,
            14,
            format!("Login failed for user '{user}'."),
        )])
    } else if !database.is_empty() && !options.databases.contains(database) {
        Some(vec![
            (
                4060,
                11,
                format!(
                    "Cannot open database \"{database}\" requested by the login. The login failed."
                ),
            ),
            (18456, 14, format!("Login failed for user '{user}'.")),
        ])
    } else {
        None
    };
    let database = if database.is_empty() {
        "master".to_string()
    } else {
        database.clone()
    };
    log.lock().unwrap().push(login);
    let mut tokens = Vec::new();
    match refusal {
        Some(errors) => {
            for (number, class, message) in errors {
                error_token(&mut tokens, number, class, &message);
            }
            done(&mut tokens, 0xFD, 0x02, 0);
            write_message(&mut wire, REPLY, &tokens)?;
            return Ok(());
        }
        None => {
            login_ack(&mut tokens);
            env_database(&mut tokens, &database);
            if let Some((host, port)) = &options.route_to {
                env_routing(&mut tokens, host, *port);
            }
            done(&mut tokens, 0xFD, 0x00, 0);
            write_message(&mut wire, REPLY, &tokens)?;
        }
    }

    loop {
        let (kind, body) = read_message(&mut wire)?;
        let seen = match kind {
            BATCH => Seen::Batch(parse_batch(&body)),
            RPC => parse_rpc(&body),
            _ => continue,
        };
        log.lock().unwrap().push(seen.clone());
        let index = statements.fetch_add(1, Ordering::SeqCst);
        let reply = handler(index, &seen);
        if let Reply::Silent = reply {
            // Hold the connection open and say nothing until the client goes.
            let mut sink = [0u8; 1024];
            while wire.read(&mut sink)? > 0 {}
            return Ok(());
        }
        write_message(&mut wire, REPLY, &answer(&reply, kind == RPC))?;
    }
}

/// The TLS handshake, each flight inside PRELOGIN packets.
fn handshake(
    tcp: &mut TcpStream,
    config: Arc<rustls::ServerConfig>,
) -> io::Result<rustls::ServerConnection> {
    let mut session = rustls::ServerConnection::new(config)
        .map_err(|error| io::Error::other(error.to_string()))?;
    let flush = |session: &mut rustls::ServerConnection, tcp: &mut TcpStream| -> io::Result<()> {
        let mut out = Vec::new();
        while session.wants_write() {
            session.write_tls(&mut out)?;
        }
        if !out.is_empty() {
            write_message(tcp, PRELOGIN, &out)?;
        }
        Ok(())
    };
    while session.is_handshaking() {
        flush(&mut session, tcp)?;
        if !session.is_handshaking() {
            break;
        }
        let (_, _, payload) = read_packet(tcp)?;
        let mut slice = payload.as_slice();
        while !slice.is_empty() {
            session.read_tls(&mut slice)?;
            session
                .process_new_packets()
                .map_err(|error| io::Error::other(error.to_string()))?;
        }
    }
    flush(&mut session, tcp)?;
    Ok(session)
}

// ---------------------------------------------------------------------------
// Reading what the client sent
// ---------------------------------------------------------------------------

fn prelogin_encryption(message: &[u8]) -> Option<u8> {
    let mut at = 0;
    while at < message.len() && message[at] != 0xFF {
        let token = message[at];
        let offset = usize::from(u16::from_be_bytes([message[at + 1], message[at + 2]]));
        if token == 1 {
            return message.get(offset).copied();
        }
        at += 5;
    }
    None
}

fn prelogin_answer(encryption: u8) -> Vec<u8> {
    // VERSION (16.0.4000.0), ENCRYPTION, INSTOPT, THREADID (empty), MARS.
    let options: [(u8, Vec<u8>); 5] = [
        (0, vec![16, 0, 0x0F, 0xA0, 0, 0]),
        (1, vec![encryption]),
        (2, vec![0]),
        (3, vec![]),
        (4, vec![0]),
    ];
    let mut head = Vec::new();
    let mut data: Vec<u8> = Vec::new();
    let mut offset = options.len() * 5 + 1;
    for (token, value) in &options {
        head.push(*token);
        head.extend((offset as u16).to_be_bytes());
        head.extend((value.len() as u16).to_be_bytes());
        offset += value.len();
        data.extend(value);
    }
    head.push(0xFF);
    head.extend(data);
    head
}

fn ucs2(bytes: &[u8]) -> String {
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
        .collect();
    String::from_utf16_lossy(&units)
}

fn parse_login(message: &[u8], encrypted: bool) -> Seen {
    // Offsets and lengths (in characters) from byte 36: host, user, password,
    // app, server, extension, client interface, language, database.
    let field = |index: usize, password: bool| -> String {
        let at = 36 + index * 4;
        let offset = usize::from(u16::from_le_bytes([message[at], message[at + 1]]));
        let length = usize::from(u16::from_le_bytes([message[at + 2], message[at + 3]])) * 2;
        let mut bytes = message[offset..offset + length].to_vec();
        if password {
            for byte in &mut bytes {
                let plain = *byte ^ 0xA5;
                *byte = plain.rotate_left(4);
            }
        }
        ucs2(&bytes)
    };
    Seen::Login {
        user: field(1, false),
        password: field(2, true),
        app: field(3, false),
        database: field(8, false),
        encrypted,
    }
}

fn skip_all_headers(body: &[u8]) -> &[u8] {
    let length = u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
    &body[length..]
}

fn parse_batch(body: &[u8]) -> String {
    ucs2(skip_all_headers(body))
}

/// A cursor over a message.
struct Bytes<'a>(&'a [u8]);

impl Bytes<'_> {
    fn take(&mut self, count: usize) -> &[u8] {
        let (head, rest) = self.0.split_at(count);
        self.0 = rest;
        head
    }
    fn u8(&mut self) -> u8 {
        self.take(1)[0]
    }
    fn u16(&mut self) -> u16 {
        let b = self.take(2);
        u16::from_le_bytes([b[0], b[1]])
    }
    fn u32(&mut self) -> u32 {
        let b = self.take(4);
        u32::from_le_bytes([b[0], b[1], b[2], b[3]])
    }
    fn u64(&mut self) -> u64 {
        let mut b = [0u8; 8];
        b.copy_from_slice(self.take(8));
        u64::from_le_bytes(b)
    }
}

fn parse_rpc(body: &[u8]) -> Seen {
    let mut bytes = Bytes(skip_all_headers(body));
    assert_eq!(bytes.u16(), 0xFFFF, "an RPC by procedure id");
    assert_eq!(bytes.u16(), 10, "sp_executesql");
    bytes.u16(); // option flags
    let mut values = Vec::new();
    while !bytes.0.is_empty() {
        let name_length = usize::from(bytes.u8()) * 2;
        bytes.take(name_length);
        bytes.u8(); // status flags
        values.push(parse_param(&mut bytes));
    }
    let mut values = values.into_iter();
    let sql = values
        .next()
        .and_then(|v| v.text().map(str::to_string))
        .unwrap_or_default();
    let declared = values
        .next()
        .and_then(|v| v.text().map(str::to_string))
        .unwrap_or_default();
    Seen::Rpc {
        sql,
        declared,
        params: values.collect(),
    }
}

fn parse_param(bytes: &mut Bytes) -> Param {
    match bytes.u8() {
        0x1F => Param::Null,
        0xE7 => {
            let max = bytes.u16();
            bytes.take(5); // collation
            if max == 0xFFFF {
                match bytes.u64() {
                    u64::MAX => Param::Null,
                    _ => {
                        let mut data = Vec::new();
                        loop {
                            let chunk = bytes.u32() as usize;
                            if chunk == 0 {
                                break;
                            }
                            data.extend_from_slice(bytes.take(chunk));
                            if bytes.0.is_empty() {
                                break;
                            }
                        }
                        Param::Text(ucs2(&data))
                    }
                }
            } else {
                match bytes.u16() {
                    0xFFFF => Param::Null,
                    length => Param::Text(ucs2(bytes.take(usize::from(length)))),
                }
            }
        }
        0x26 => {
            bytes.u8();
            match bytes.u8() {
                0 => Param::Null,
                length => {
                    let mut b = [0u8; 8];
                    b[..usize::from(length)].copy_from_slice(bytes.take(usize::from(length)));
                    let shift = 64 - u32::from(length) * 8;
                    Param::Int(i64::from_le_bytes(b) << shift >> shift)
                }
            }
        }
        0x6D => {
            bytes.u8();
            match bytes.u8() {
                0 => Param::Null,
                4 => {
                    let b = bytes.take(4);
                    Param::Float(f64::from(f32::from_le_bytes([b[0], b[1], b[2], b[3]])))
                }
                _ => Param::Float(f64::from_bits(bytes.u64())),
            }
        }
        0x68 => {
            bytes.u8();
            match bytes.u8() {
                0 => Param::Null,
                _ => Param::Bit(bytes.u8() != 0),
            }
        }
        other => panic!("the fixture does not read parameters of type {other:#x}"),
    }
}

// ---------------------------------------------------------------------------
// Writing the answer
// ---------------------------------------------------------------------------

fn b_varchar(out: &mut Vec<u8>, text: &str) {
    let units: Vec<u16> = text.encode_utf16().collect();
    out.push(units.len() as u8);
    for unit in units {
        out.extend(unit.to_le_bytes());
    }
}

fn us_varchar(out: &mut Vec<u8>, text: &str) {
    let units: Vec<u16> = text.encode_utf16().collect();
    out.extend((units.len() as u16).to_le_bytes());
    for unit in units {
        out.extend(unit.to_le_bytes());
    }
}

fn utf16(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

/// A token with a two-byte length after its type.
fn sized(out: &mut Vec<u8>, kind: u8, body: Vec<u8>) {
    out.push(kind);
    out.extend((body.len() as u16).to_le_bytes());
    out.extend(body);
}

fn login_ack(out: &mut Vec<u8>) {
    let mut body = vec![1];
    body.extend(0x7400_0004u32.to_be_bytes());
    b_varchar(&mut body, "Microsoft SQL Server");
    body.extend([16, 0, 0x0F, 0xA0]);
    sized(out, 0xAD, body);
}

fn env_database(out: &mut Vec<u8>, database: &str) {
    let mut body = vec![1];
    b_varchar(&mut body, database);
    b_varchar(&mut body, "master");
    sized(out, 0xE3, body);
}

fn env_routing(out: &mut Vec<u8>, host: &str, port: u16) {
    let units: Vec<u16> = host.encode_utf16().collect();
    let mut body = vec![20];
    body.extend(((1 + 2 + 2 + units.len() * 2) as u16).to_le_bytes());
    body.push(0); // TCP
    body.extend(port.to_le_bytes());
    body.extend((units.len() as u16).to_le_bytes());
    for unit in units {
        body.extend(unit.to_le_bytes());
    }
    body.extend(0u16.to_le_bytes()); // no old value
    sized(out, 0xE3, body);
}

fn error_token(out: &mut Vec<u8>, number: u32, class: u8, message: &str) {
    let mut body = Vec::new();
    body.extend(number.to_le_bytes());
    body.push(1); // state
    body.push(class);
    us_varchar(&mut body, message);
    b_varchar(&mut body, "fixture");
    b_varchar(&mut body, "");
    body.extend(1u32.to_le_bytes()); // line
    sized(out, 0xAA, body);
}

/// DONE (0xFD), DONEPROC (0xFE) or DONEINPROC (0xFF).
fn done(out: &mut Vec<u8>, kind: u8, status: u16, rows: u64) {
    out.push(kind);
    out.extend(status.to_le_bytes());
    out.extend(0u16.to_le_bytes());
    out.extend(rows.to_le_bytes());
}

fn time_length(scale: u8) -> usize {
    match scale {
        0..=2 => 3,
        3..=4 => 4,
        _ => 5,
    }
}

fn decimal_length(precision: u8) -> u8 {
    match precision {
        0..=9 => 5,
        10..=19 => 9,
        20..=28 => 13,
        _ => 17,
    }
}

/// Latin1_General_CI_AS.
const COLLATION: [u8; 5] = [0x09, 0x04, 0xD0, 0x00, 0x34];

fn type_info(out: &mut Vec<u8>, kind: Kind) {
    match kind {
        Kind::Int(bytes) => out.extend([0x26, bytes]),
        Kind::Bit => out.extend([0x68, 1]),
        Kind::Float(bytes) => out.extend([0x6D, bytes]),
        Kind::Decimal(precision, scale) => {
            out.extend([0x6A, decimal_length(precision), precision, scale])
        }
        Kind::NVarChar(length) => {
            out.push(0xE7);
            out.extend(length.map_or(0xFFFF, |chars| chars * 2).to_le_bytes());
            out.extend(COLLATION);
        }
        Kind::VarBinary(length) => {
            out.push(0xA5);
            out.extend(length.unwrap_or(0xFFFF).to_le_bytes());
        }
        Kind::Guid => out.extend([0x24, 16]),
        Kind::Date => out.push(0x28),
        Kind::Time(scale) => out.extend([0x29, scale]),
        Kind::DateTime2(scale) => out.extend([0x2A, scale]),
        Kind::DateTimeOffset(scale) => out.extend([0x2B, scale]),
        Kind::DateTime => out.extend([0x6F, 8]),
        Kind::SmallDateTime => out.extend([0x6F, 4]),
        Kind::Xml => out.extend([0xF1, 0]),
        Kind::Variant => {
            out.push(0x62);
            out.extend(8009u32.to_le_bytes());
        }
    }
}

fn plp(out: &mut Vec<u8>, data: Option<&[u8]>) {
    match data {
        None => out.extend(u64::MAX.to_le_bytes()),
        Some(data) => {
            out.extend((data.len() as u64).to_le_bytes());
            // Two chunks when there is anything to split, as a server may send.
            let (first, second) = data.split_at(data.len() / 2);
            for chunk in [first, second] {
                if !chunk.is_empty() {
                    out.extend((chunk.len() as u32).to_le_bytes());
                    out.extend(chunk);
                }
            }
            out.extend(0u32.to_le_bytes());
        }
    }
}

fn guid_bytes(text: &str) -> [u8; 16] {
    let hex: String = text.chars().filter(char::is_ascii_hexdigit).collect();
    let mut bytes = [0u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).unwrap();
    }
    // The first three groups go little-endian on the wire.
    bytes[0..4].reverse();
    bytes[4..6].reverse();
    bytes[6..8].reverse();
    bytes
}

fn cell(out: &mut Vec<u8>, kind: Kind, value: &Cell) {
    let long = matches!(
        kind,
        Kind::NVarChar(None) | Kind::VarBinary(None) | Kind::Xml
    );
    match (kind, value) {
        (Kind::NVarChar(Some(_)) | Kind::VarBinary(Some(_)), Cell::Null) => {
            out.extend(0xFFFFu16.to_le_bytes())
        }
        (_, Cell::Null) if long => plp(out, None),
        (_, Cell::Null) => out.push(0),
        (Kind::Int(bytes), Cell::Int(v)) => {
            out.push(bytes);
            out.extend(&v.to_le_bytes()[..usize::from(bytes)]);
        }
        (Kind::Bit, Cell::Bit(v)) => out.extend([1, u8::from(*v)]),
        (Kind::Float(4), Cell::Float(v)) => {
            out.push(4);
            out.extend((*v as f32).to_le_bytes());
        }
        (Kind::Float(_), Cell::Float(v)) => {
            out.push(8);
            out.extend(v.to_le_bytes());
        }
        (Kind::Decimal(precision, _), Cell::Decimal(v)) => {
            let length = decimal_length(precision);
            out.push(length);
            out.push(u8::from(*v >= 0));
            out.extend(&v.unsigned_abs().to_le_bytes()[..usize::from(length - 1)]);
        }
        (Kind::NVarChar(Some(_)), Cell::Text(v)) => {
            let data = utf16(v);
            out.extend((data.len() as u16).to_le_bytes());
            out.extend(data);
        }
        (Kind::NVarChar(None) | Kind::Xml, Cell::Text(v)) => plp(out, Some(&utf16(v))),
        (Kind::VarBinary(Some(_)), Cell::Bytes(v)) => {
            out.extend((v.len() as u16).to_le_bytes());
            out.extend(v);
        }
        (Kind::VarBinary(None), Cell::Bytes(v)) => plp(out, Some(v)),
        (Kind::Guid, Cell::Guid(v)) => {
            out.push(16);
            out.extend(guid_bytes(v));
        }
        (Kind::Date, Cell::Date(days)) => {
            out.push(3);
            out.extend(&days.to_le_bytes()[..3]);
        }
        (Kind::Time(scale), Cell::Time(increments)) => {
            let length = time_length(scale);
            out.push(length as u8);
            out.extend(&increments.to_le_bytes()[..length]);
        }
        (Kind::DateTime2(scale), Cell::DateTime2(days, increments)) => {
            let length = time_length(scale);
            out.push((length + 3) as u8);
            out.extend(&increments.to_le_bytes()[..length]);
            out.extend(&days.to_le_bytes()[..3]);
        }
        (Kind::DateTimeOffset(scale), Cell::DateTimeOffset(days, increments, offset)) => {
            let length = time_length(scale);
            out.push((length + 5) as u8);
            out.extend(&increments.to_le_bytes()[..length]);
            out.extend(&days.to_le_bytes()[..3]);
            out.extend(offset.to_le_bytes());
        }
        (Kind::DateTime, Cell::DateTime(days, ticks)) => {
            out.push(8);
            out.extend(days.to_le_bytes());
            out.extend(ticks.to_le_bytes());
        }
        (Kind::SmallDateTime, Cell::SmallDateTime(days, minutes)) => {
            out.push(4);
            out.extend(days.to_le_bytes());
            out.extend(minutes.to_le_bytes());
        }
        (kind, value) => panic!("the fixture cannot send {value:?} as {kind:?}"),
    }
}

fn result_set(out: &mut Vec<u8>, set: &Rows) {
    out.push(0x81);
    out.extend((set.columns.len() as u16).to_le_bytes());
    for (name, kind) in &set.columns {
        out.extend(0u32.to_le_bytes()); // user type
        out.extend(0x0001u16.to_le_bytes()); // nullable
        type_info(out, *kind);
        b_varchar(out, name);
    }
    for row in &set.rows {
        out.push(0xD1);
        for ((_, kind), value) in set.columns.iter().zip(row) {
            cell(out, *kind, value);
        }
    }
}

/// The tokens for a reply: as SQL Server ends an `sp_executesql` call
/// (DONEINPROC, RETURNSTATUS, DONEPROC) or a batch (DONE).
fn answer(reply: &Reply, rpc: bool) -> Vec<u8> {
    let statement_done = if rpc { 0xFF } else { 0xFD };
    let mut out = Vec::new();
    let mut failed = false;
    match reply {
        Reply::Results(sets) => {
            for (index, set) in sets.iter().enumerate() {
                result_set(&mut out, set);
                let more = if index + 1 < sets.len() || rpc {
                    0x01
                } else {
                    0
                };
                done(&mut out, statement_done, 0x10 | more, set.rows.len() as u64);
            }
        }
        Reply::Changed(count) => {
            done(&mut out, statement_done, 0x10 | u16::from(rpc), *count);
        }
        Reply::Error(number, message) => {
            error_token(&mut out, *number, 16, message);
            done(&mut out, statement_done, 0x02 | u16::from(rpc), 0);
            failed = true;
        }
        Reply::RowsThenError(set, number, message) => {
            result_set(&mut out, set);
            error_token(&mut out, *number, 16, message);
            done(&mut out, statement_done, 0x02 | u16::from(rpc), 0);
            failed = true;
        }
        Reply::Silent => unreachable!(),
    }
    if rpc {
        out.push(0x79);
        out.extend(0i32.to_le_bytes());
        done(&mut out, 0xFE, if failed { 0x02 } else { 0 }, 0);
    }
    out
}
