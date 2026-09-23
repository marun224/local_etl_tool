//! What extraction promises, against a real filesystem.
//!
//! The two that matter most are the ones nothing else would catch: a header
//! naming a file that would be written outside the cache directory, and two
//! processes unpacking the same artifact at the same moment. Both are cases
//! where the obvious implementation works perfectly until the day it does not.

use crate::extract::{is_safe_name, COMPLETE_MARKER, EXTENSIONS_DIR};
use crate::{Blobs, EmbeddedFile, Payload, PayloadError, Role};
use etl_metadata::PipelineDoc;
use std::path::{Path, PathBuf};

fn dir(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("etl-extract-tests/{name}"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("temp directory");

    root
}

fn fake_runner(at: &Path) -> PathBuf {
    let path = at.join("etl-runner.bin");
    std::fs::write(&path, b"not really an executable").expect("write");

    path
}

fn document() -> PipelineDoc {
    PipelineDoc::from_json(
        r#"{"nodes":[{"id":"read","type":"source","position":{"x":0,"y":0},
           "data":{"label":"Orders","componentId":"src.file.csv"}}],"edges":[]}"#,
    )
    .expect("a document")
}

/// A payload carrying an engine and one extension, plus the blob bytes.
fn with_files(engine: &[u8], extension: &[u8]) -> (Payload, Vec<u8>) {
    let mut payload = Payload::new("orders", "2026-09-17T09:00:00Z", document());
    payload.duckdb_version = "v1.5.5".to_string();
    payload.platform = "windows_amd64".to_string();

    let mut blobs = Vec::new();

    payload.files.push(EmbeddedFile {
        name: "duckdb.exe".to_string(),
        role: Role::Engine,
        offset: 0,
        length: engine.len() as u64,
        executable: true,
        extra: Default::default(),
    });
    blobs.extend_from_slice(engine);

    payload.files.push(EmbeddedFile {
        name: "excel.duckdb_extension".to_string(),
        role: Role::Extension,
        offset: blobs.len() as u64,
        length: extension.len() as u64,
        executable: false,
        extra: Default::default(),
    });
    blobs.extend_from_slice(extension);

    (payload, blobs)
}

/// Build an artifact and read its payload back, so the `Blobs` under test is a
/// real one pointing into a real file.
fn built(at: &Path, payload: &Payload, blobs: &[u8]) -> (Payload, Blobs) {
    let runner = fake_runner(at);
    let artifact = at.join("built.exe");
    payload
        .clone()
        .write_built(&runner, &artifact, blobs)
        .expect("builds");

    Payload::read_from(&artifact)
        .expect("readable")
        .expect("has a payload")
}

// ---------------------------------------------------------------------------
// Putting the files where DuckDB looks for them
// ---------------------------------------------------------------------------

#[test]
fn the_engine_and_the_extension_land_where_duckdb_expects_them() {
    let at = dir("layout");
    let cache = at.join("cache");

    let (payload, blobs) = with_files(b"engine bytes", b"extension bytes");
    let (payload, region) = built(&at, &payload, &blobs);

    let extracted = payload.extract_into(&region, &cache).expect("extracts");

    let engine = extracted.duckdb_bin.expect("an engine");
    assert_eq!(std::fs::read(&engine).expect("read"), b"engine bytes");

    // DuckDB resolves an extension to <dir>/<version>/<platform>/<name>, so
    // what it is handed has to be the directory above those two.
    let dir_for_duckdb = extracted.extension_dir.expect("an extension directory");
    assert!(
        dir_for_duckdb.ends_with(EXTENSIONS_DIR),
        "{dir_for_duckdb:?}"
    );

    let file = dir_for_duckdb
        .join("v1.5.5")
        .join("windows_amd64")
        .join("excel.duckdb_extension");

    assert!(file.is_file(), "expected {}", file.display());
    assert_eq!(std::fs::read(&file).expect("read"), b"extension bytes");
}

#[test]
fn an_artifact_with_no_embedded_files_extracts_nothing() {
    let at = dir("nothing-embedded");
    let cache = at.join("cache");

    let payload = Payload::new("orders", "2026-09-17T09:00:00Z", document());
    let (payload, region) = built(&at, &payload, &[]);

    let extracted = payload.extract_into(&region, &cache).expect("extracts");

    // Every artifact 9a built is this. It still finds DuckDB the way the CLI
    // does, so reporting nothing is the whole of the correct behaviour.
    assert_eq!(extracted.duckdb_bin, None);
    assert_eq!(extracted.extension_dir, None);
    assert!(!extracted.freshly_extracted);
}

#[test]
fn an_artifact_with_an_engine_and_no_extensions_asks_for_no_extension_directory() {
    let at = dir("engine-only");
    let cache = at.join("cache");

    let mut payload = Payload::new("orders", "2026-09-17T09:00:00Z", document());
    payload.files.push(EmbeddedFile {
        name: "duckdb.exe".to_string(),
        role: Role::Engine,
        offset: 0,
        length: 6,
        executable: true,
        extra: Default::default(),
    });

    let (payload, region) = built(&at, &payload, b"engine");
    let extracted = payload.extract_into(&region, &cache).expect("extracts");

    assert!(extracted.duckdb_bin.is_some());
    // Pointing DuckDB at an empty directory would be harmless and misleading.
    assert_eq!(extracted.extension_dir, None);
}

// ---------------------------------------------------------------------------
// Doing it once
// ---------------------------------------------------------------------------

#[test]
fn a_second_run_reuses_what_the_first_extracted() {
    let at = dir("cached");
    let cache = at.join("cache");

    let (payload, blobs) = with_files(b"engine bytes", b"extension bytes");
    let (payload, region) = built(&at, &payload, &blobs);

    let first = payload.extract_into(&region, &cache).expect("extracts");
    assert!(first.freshly_extracted, "the first run does the work");

    let second = payload.extract_into(&region, &cache).expect("extracts");
    assert!(!second.freshly_extracted, "the second run does not");
    assert_eq!(first.root, second.root);
}

#[test]
fn two_builds_of_the_same_pipeline_do_not_share_a_directory() {
    let at = dir("distinct-keys");
    let cache = at.join("cache");

    let (first, first_blobs) = with_files(b"engine one", b"extension bytes");
    let first_at = at.join("a");
    std::fs::create_dir_all(&first_at).expect("create");
    let (first, first_region) = built(&first_at, &first, &first_blobs);

    let (second, second_blobs) = with_files(b"engine two", b"extension bytes");
    let second_at = at.join("b");
    std::fs::create_dir_all(&second_at).expect("create");
    let (second, second_region) = built(&second_at, &second, &second_blobs);

    // Same name, same build time, different contents. Sharing a directory would
    // have the second artifact silently run the first one's engine.
    assert_ne!(
        first.cache_dir_in(&cache, &first_region),
        second.cache_dir_in(&cache, &second_region)
    );
}

#[test]
fn an_interrupted_extraction_is_not_mistaken_for_a_finished_one() {
    let at = dir("interrupted");
    let cache = at.join("cache");

    let (payload, blobs) = with_files(b"engine bytes", b"extension bytes");
    let (payload, region) = built(&at, &payload, &blobs);

    let root = payload.cache_dir_in(&cache, &region);

    // A directory that exists, holds a plausible engine, and was never finished
    // — which is what a machine losing power halfway through leaves behind.
    std::fs::create_dir_all(&root).expect("create");
    std::fs::write(root.join("duckdb.exe"), b"truncated").expect("write");

    let extracted = payload.extract_into(&region, &cache).expect("extracts");

    // The marker is the only evidence, and it was not there.
    assert!(extracted.freshly_extracted, "it should redo the work");
    assert!(root.join(COMPLETE_MARKER).is_file());
    assert_eq!(
        std::fs::read(root.join("duckdb.exe")).expect("read"),
        b"engine bytes"
    );
}

#[test]
fn losing_the_race_to_another_process_is_not_an_error() {
    let at = dir("raced");
    let cache = at.join("cache");

    let (payload, blobs) = with_files(b"engine bytes", b"extension bytes");
    let (payload, region) = built(&at, &payload, &blobs);

    let root = payload.cache_dir_in(&cache, &region);

    // Stand in for the winner: a complete directory already in place. The rename
    // will fail, and the loser has to notice why and carry on rather than
    // writing over files the winner may already have open.
    std::fs::create_dir_all(&root).expect("create");
    std::fs::write(root.join("duckdb.exe"), b"engine bytes").expect("write");
    std::fs::write(root.join(COMPLETE_MARKER), b"2026-09-17T09:00:00Z").expect("write");

    let extracted = payload.extract_into(&region, &cache).expect("no error");

    assert!(!extracted.freshly_extracted);
    assert_eq!(extracted.duckdb_bin, Some(root.join("duckdb.exe")));
}

#[test]
fn nothing_is_left_behind_in_the_cache_after_a_successful_extraction() {
    let at = dir("no-litter");
    let cache = at.join("cache");

    let (payload, blobs) = with_files(b"engine bytes", b"extension bytes");
    let (payload, region) = built(&at, &payload, &blobs);

    payload.extract_into(&region, &cache).expect("extracts");

    let leftovers: Vec<String> = std::fs::read_dir(cache.join("etl-runner"))
        .expect("read")
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".partial-"))
        .collect();

    assert!(leftovers.is_empty(), "left behind: {leftovers:?}");
}

// ---------------------------------------------------------------------------
// A header is data somebody else wrote
// ---------------------------------------------------------------------------

#[test]
fn a_name_that_would_escape_the_cache_directory_is_refused() {
    let at = dir("traversal");
    let cache = at.join("cache");

    for name in [
        "../../../etc/cron.d/anything",
        "..\\..\\Windows\\System32\\evil.dll",
        "/etc/passwd",
        "C:\\Windows\\win.ini",
        "..",
        ".",
        "",
        "sub/dir",
    ] {
        let mut payload = Payload::new("orders", "2026-09-17T09:00:00Z", document());
        payload.files.push(EmbeddedFile {
            name: name.to_string(),
            role: Role::Engine,
            offset: 0,
            length: 4,
            executable: true,
            extra: Default::default(),
        });

        let case = at.join(format!("case-{}", name.len()));
        std::fs::create_dir_all(&case).expect("create");
        let (payload, region) = built(&case, &payload, b"evil");

        // Refused, not sanitised: a name that needs fixing is a name worth
        // refusing, and an artifact is data somebody can hand you.
        assert!(
            matches!(
                payload.extract_into(&region, &cache),
                Err(PayloadError::UnsafeName { .. })
            ),
            "'{name}' was not refused"
        );
    }
}

#[test]
fn a_plain_filename_is_accepted_and_everything_else_is_not() {
    assert!(is_safe_name("duckdb.exe"));
    assert!(is_safe_name("excel.duckdb_extension"));
    assert!(is_safe_name("excel.duckdb_extension.info"));

    assert!(!is_safe_name(""));
    assert!(!is_safe_name("."));
    assert!(!is_safe_name(".."));
    assert!(!is_safe_name("a/b"));
    assert!(!is_safe_name("a\\b"));
    assert!(!is_safe_name("C:evil"));
}

#[test]
fn nothing_is_written_when_a_name_is_refused() {
    let at = dir("refused-writes-nothing");
    let cache = at.join("cache");

    let mut payload = Payload::new("orders", "2026-09-17T09:00:00Z", document());
    // A good file first, so a check that ran per-file mid-write would already
    // have put this one on disk before reaching the bad one.
    payload.files.push(EmbeddedFile {
        name: "duckdb.exe".to_string(),
        role: Role::Engine,
        offset: 0,
        length: 6,
        executable: true,
        extra: Default::default(),
    });
    payload.files.push(EmbeddedFile {
        name: "../escape".to_string(),
        role: Role::Engine,
        offset: 6,
        length: 4,
        executable: false,
        extra: Default::default(),
    });

    let (payload, region) = built(&at, &payload, b"engineevil");

    assert!(matches!(
        payload.extract_into(&region, &cache),
        Err(PayloadError::UnsafeName { .. })
    ));

    // Every name is checked before any byte is written, which is why this holds.
    assert!(!payload.cache_dir_in(&cache, &region).exists());
}
