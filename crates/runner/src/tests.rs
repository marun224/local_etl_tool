//! What the payload format promises, asserted against real files on disk.
//!
//! The interesting cases are all the ones where a file is *not* a built runner:
//! a plain binary, a file too short to hold a trailer, one whose last bytes
//! happen not to be the magic, and one that was cut off mid-download. Each has
//! a plausible wrong answer — treat it as baked, read past the end, or report a
//! corrupt payload when there is simply no payload — so each is pinned.

use super::*;
use etl_metadata::PipelineDoc;

/// A directory of this test's own, cleaned up by the OS.
fn dir(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("etl-runner-tests/{name}"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("temp directory");

    root
}

/// Stand-in for a compiled runner: bytes that are not a payload.
///
/// The format never parses the executable in front of it, so any bytes do. That
/// is the property being relied on, and using arbitrary ones here says so.
fn fake_runner(at: &Path) -> PathBuf {
    let path = at.join("etl-runner.bin");
    std::fs::write(&path, b"MZ\x90\x00 not really a PE, and it does not matter").expect("write");

    path
}

fn document() -> PipelineDoc {
    PipelineDoc::from_json(
        r#"{
  "formatVersion": 1,
  "name": "orders",
  "nodes": [
    { "id": "read", "type": "source", "position": { "x": 0, "y": 0 },
      "data": { "label": "Orders", "componentId": "src.file.csv",
                "properties": { "path": "data/orders.csv", "header": true } } }
  ],
  "edges": []
}"#,
    )
    .expect("a document")
}

fn payload() -> Payload {
    Payload::new("orders", "2026-09-17T09:00:00Z", document())
}

// ---------------------------------------------------------------------------
// The round trip
// ---------------------------------------------------------------------------

#[test]
fn a_built_runner_reads_back_the_payload_that_was_written() {
    let at = dir("round-trip");
    let runner = fake_runner(&at);
    let built = at.join("orders.exe");

    let mut written = payload();
    written.write_built(&runner, &built, &[]).expect("builds");

    let (read, blobs) = Payload::read_from(&built)
        .expect("readable")
        .expect("has a payload");

    // Compared against the payload *as written*, which now carries the blob
    // digest `write_built` recorded. That the two agree is the round trip.
    assert_eq!(read, written);
    assert_eq!(read.name, "orders");
    assert_eq!(read.pipeline.nodes.len(), 1);
    assert!(blobs.is_empty(), "9a bakes no files");
}

#[test]
fn the_runner_in_front_of_the_payload_is_left_exactly_as_it_was() {
    let at = dir("prefix-intact");
    let runner = fake_runner(&at);
    let built = at.join("orders.exe");

    let original = std::fs::read(&runner).expect("read");
    let mut payload = payload();
    payload.write_built(&runner, &built, &[]).expect("builds");

    let after = std::fs::read(&built).expect("read");

    // The whole trick rests on this: the operating system loads the executable
    // by its own headers and never looks at what follows, so what precedes the
    // payload has to come through untouched.
    assert_eq!(&after[..original.len()], &original[..]);
    assert!(after.len() > original.len());
}

#[test]
fn a_payload_survives_a_document_with_keys_this_version_does_not_know() {
    let at = dir("round-trip-extra");
    let runner = fake_runner(&at);
    let built = at.join("orders.exe");

    let document = PipelineDoc::from_json(
        r#"{
  "formatVersion": 1,
  "name": "orders",
  "somethingNewer": { "kept": true },
  "nodes": [
    { "id": "read", "type": "source", "position": { "x": 0, "y": 0 },
      "data": { "label": "Orders", "componentId": "src.file.csv" } } ],
  "edges": []
}"#,
    )
    .expect("a document");

    Payload::new("orders", "2026-09-17T09:00:00Z", document)
        .write_built(&runner, &built, &[])
        .expect("builds");

    let (read, _) = Payload::read_from(&built)
        .expect("readable")
        .expect("has a payload");

    // The same `#[serde(flatten)] extra` posture the rest of the project takes:
    // a document written by a newer version survives being baked by an older
    // one rather than being quietly stripped on the way in.
    assert_eq!(read.pipeline.extra["somethingNewer"]["kept"], true);
}

// ---------------------------------------------------------------------------
// Files that are not built runners
// ---------------------------------------------------------------------------

#[test]
fn a_plain_runner_has_no_payload_rather_than_a_broken_one() {
    let at = dir("plain");
    let runner = fake_runner(&at);

    // This is `cargo run -p etl-runner`, and it is a state rather than a
    // failure: the message for it is "nothing is baked in".
    assert_eq!(Payload::read_from(&runner).expect("readable"), None);
}

#[test]
fn a_file_too_short_to_hold_a_trailer_has_no_payload() {
    let at = dir("tiny");
    let path = at.join("tiny.bin");
    std::fs::write(&path, b"short").expect("write");

    // Read before the length check, this seeks to a negative offset.
    assert_eq!(Payload::read_from(&path).expect("readable"), None);
}

#[test]
fn an_empty_file_has_no_payload() {
    let at = dir("empty");
    let path = at.join("empty.bin");
    std::fs::write(&path, b"").expect("write");

    assert_eq!(Payload::read_from(&path).expect("readable"), None);
}

#[test]
fn a_file_that_ends_in_something_else_has_no_payload() {
    let at = dir("wrong-magic");
    let path = at.join("other.bin");
    // Long enough for a trailer, and ending in bytes that are not the magic.
    std::fs::write(&path, vec![7_u8; 512]).expect("write");

    assert_eq!(Payload::read_from(&path).expect("readable"), None);
}

// ---------------------------------------------------------------------------
// Files that claim to be built runners and are not
// ---------------------------------------------------------------------------

/// A trailer claiming `header_len` and `blob_len`, appended to `body`.
fn with_trailer(at: &Path, name: &str, body: &[u8], header_len: u64, blob_len: u64) -> PathBuf {
    let path = at.join(name);
    let mut bytes = body.to_vec();
    bytes.extend_from_slice(&header_len.to_le_bytes());
    bytes.extend_from_slice(&blob_len.to_le_bytes());
    bytes.extend_from_slice(&MAGIC.to_le_bytes());
    std::fs::write(&path, bytes).expect("write");

    path
}

#[test]
fn a_trailer_claiming_more_than_the_file_holds_is_refused() {
    let at = dir("truncated");
    let path = with_trailer(&at, "cut.bin", b"not much here", 9_000, 0);

    // What a half-finished download looks like. Reported as truncation rather
    // than surfacing as an unexpected end-of-file from somewhere deeper.
    match Payload::read_from(&path) {
        Err(PayloadError::Truncated {
            claimed, actual, ..
        }) => {
            assert!(claimed > actual, "{claimed} should exceed {actual}");
        }
        other => panic!("expected truncation, got {other:?}"),
    }
}

#[test]
fn a_trailer_whose_lengths_overflow_is_refused_rather_than_wrapping() {
    let at = dir("overflow");
    let path = with_trailer(&at, "overflow.bin", b"body", u64::MAX, 8);

    // `total - TRAILER_LEN - header_len` would wrap and seek somewhere
    // arbitrary. The addition is checked for exactly this.
    assert!(matches!(
        Payload::read_from(&path),
        Err(PayloadError::Truncated { .. })
    ));
}

#[test]
fn a_payload_whose_header_is_not_json_is_refused() {
    let at = dir("corrupt");
    let header = b"{ this is not json";
    let mut body = b"runner".to_vec();
    body.extend_from_slice(header);

    let path = with_trailer(&at, "corrupt.bin", &body, header.len() as u64, 0);

    assert!(matches!(
        Payload::read_from(&path),
        Err(PayloadError::Corrupt { .. })
    ));
}

#[test]
fn a_payload_from_a_format_this_runner_does_not_know_is_refused_by_version() {
    let at = dir("incompatible");
    let header = br#"{"formatVersion":99,"whatever":true}"#;
    let mut body = b"runner".to_vec();
    body.extend_from_slice(header);

    let path = with_trailer(&at, "future.bin", &body, header.len() as u64, 0);

    // The version is read before the rest of the header is trusted, so this is
    // a clear refusal rather than a confusing complaint about a missing field.
    match Payload::read_from(&path) {
        Err(PayloadError::Incompatible { found, .. }) => assert_eq!(found, 99),
        other => panic!("expected an incompatible payload, got {other:?}"),
    }
}

#[test]
fn building_from_an_already_built_runner_is_refused() {
    let at = dir("already-baked");
    let runner = fake_runner(&at);
    let once = at.join("once.exe");
    let twice = at.join("twice.exe");

    payload().write_built(&runner, &once, &[]).expect("builds");

    // Appending again would leave two trailers, and the file would run as
    // whichever was last — which works in testing and ships the wrong pipeline.
    assert!(matches!(
        payload().write_built(&once, &twice, &[]),
        Err(PayloadError::AlreadyBaked { .. })
    ));
}

// ---------------------------------------------------------------------------
// The blob region, which 9b fills
// ---------------------------------------------------------------------------

#[test]
fn an_embedded_file_reads_back_the_bytes_it_was_given() {
    let at = dir("blobs");
    let runner = fake_runner(&at);
    let built = at.join("orders.exe");

    let first = b"the duckdb binary, supposedly".to_vec();
    let second = b"an extension".to_vec();

    let mut blobs = Vec::new();
    let duckdb = EmbeddedFile {
        name: "duckdb".to_string(),
        role: Role::Engine,
        offset: blobs.len() as u64,
        length: first.len() as u64,
        executable: true,
        extra: Default::default(),
    };
    blobs.extend_from_slice(&first);

    let extension = EmbeddedFile {
        name: "httpfs.duckdb_extension".to_string(),
        role: Role::Extension,
        offset: blobs.len() as u64,
        length: second.len() as u64,
        executable: false,
        extra: Default::default(),
    };
    blobs.extend_from_slice(&second);

    let mut payload = payload();
    payload.files = vec![duckdb.clone(), extension.clone()];

    payload
        .write_built(&runner, &built, &blobs)
        .expect("builds");

    let (read, region) = Payload::read_from(&built)
        .expect("readable")
        .expect("has a payload");

    assert_eq!(read.files.len(), 2);
    assert_eq!(region.len(), blobs.len() as u64);
    // Offsets are relative to the blob region, so reading the second one back
    // proves the region's own start was found rather than guessed.
    assert_eq!(region.read(&duckdb).expect("reads"), first);
    assert_eq!(region.read(&extension).expect("reads"), second);
}

#[test]
fn an_embedded_file_reaching_past_the_region_is_refused() {
    let at = dir("blob-overrun");
    let runner = fake_runner(&at);
    let built = at.join("orders.exe");

    let bytes = b"twelve bytes".to_vec();
    let honest = EmbeddedFile {
        name: "duckdb".to_string(),
        role: Role::Engine,
        offset: 0,
        length: bytes.len() as u64,
        executable: true,
        extra: Default::default(),
    };

    let mut payload = payload();
    payload.files = vec![honest];
    payload
        .write_built(&runner, &built, &bytes)
        .expect("builds");

    let (_, region) = Payload::read_from(&built)
        .expect("readable")
        .expect("has a payload");

    // A header that claims more than the region holds would otherwise read into
    // the header itself, and then into the trailer.
    let lying = EmbeddedFile {
        name: "duckdb".to_string(),
        role: Role::Engine,
        offset: 0,
        length: 9_000,
        executable: true,
        extra: Default::default(),
    };

    assert!(matches!(
        region.read(&lying),
        Err(PayloadError::Truncated { .. })
    ));
}

// ---------------------------------------------------------------------------
// The magic itself
// ---------------------------------------------------------------------------

#[test]
fn the_magic_is_searchable_text() {
    // Spelled as bytes so `strings` on a built artifact finds it. A hex
    // constant would work identically and tell nobody anything.
    assert_eq!(MAGIC.to_le_bytes(), *b"ETLBAKE1");
}
