//! The secret store.

use super::*;

/// A workspace of this test's own.
fn workspace(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join("etl-secret-tests").join(name);

    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("workspace directory");

    directory
}

// ---------------------------------------------------------------------------
// The round trip
// ---------------------------------------------------------------------------

#[test]
fn a_secret_round_trips_through_the_file() {
    let root = workspace("round_trip");

    let mut store = SecretStore::open(&root).expect("opens");
    store
        .set("pg_password", "hunter2", Some("The analytics database"))
        .expect("encrypts");
    store.save().expect("saves");

    // A second open reads it back from disk, not from memory.
    let reopened = SecretStore::open_existing(&root).expect("reopens");

    assert_eq!(
        reopened.get("pg_password").expect("decrypts"),
        Some("hunter2".to_string())
    );
    assert_eq!(
        reopened.description("pg_password"),
        Some("The analytics database")
    );
    assert_eq!(reopened.names(), ["pg_password"]);
}

#[test]
fn the_plaintext_is_nowhere_in_the_file() {
    // The point of the exercise. If this ever fails, nothing else matters.
    let root = workspace("no_plaintext");

    let mut store = SecretStore::open(&root).expect("opens");
    store
        .set("token", "super-secret-value", None)
        .expect("encrypts");
    store.save().expect("saves");

    let written = std::fs::read_to_string(SecretStore::store_path(&root)).expect("readable");

    assert!(
        !written.contains("super-secret-value"),
        "the plaintext is in the file:\n{written}"
    );
    // The name is not secret, and being able to see which secrets exist is
    // what makes the file useful to a person.
    assert!(written.contains("token"), "{written}");
}

#[test]
fn the_same_value_encrypts_differently_every_time() {
    // A fresh nonce per write. Without it, equal values would be visibly equal
    // in the file, which leaks more than it looks like it does.
    let root = workspace("fresh_nonce");
    let mut store = SecretStore::open(&root).expect("opens");

    store.set("a", "identical", None).expect("encrypts");
    store.set("b", "identical", None).expect("encrypts");
    store.save().expect("saves");

    let written = std::fs::read_to_string(SecretStore::store_path(&root)).expect("readable");
    let file: StoreFile = serde_json::from_str(&written).expect("parses");

    assert_ne!(file.secrets["a"].nonce, file.secrets["b"].nonce);
    assert_ne!(file.secrets["a"].ciphertext, file.secrets["b"].ciphertext);
    assert_eq!(store.get("a").unwrap(), store.get("b").unwrap());
}

#[test]
fn an_empty_and_a_unicode_value_both_survive() {
    let root = workspace("awkward_values");
    let mut store = SecretStore::open(&root).expect("opens");

    store.set("empty", "", None).expect("encrypts");
    store
        .set("unicode", "pÿ†hôn 日本 🔐", None)
        .expect("encrypts");

    assert_eq!(store.get("empty").unwrap(), Some(String::new()));
    assert_eq!(
        store.get("unicode").unwrap(),
        Some("pÿ†hôn 日本 🔐".to_string())
    );
}

#[test]
fn setting_a_secret_again_replaces_it() {
    let root = workspace("replace");
    let mut store = SecretStore::open(&root).expect("opens");

    store.set("p", "old", None).expect("encrypts");
    store.set("p", "new", None).expect("encrypts");

    assert_eq!(store.get("p").unwrap(), Some("new".to_string()));
    assert_eq!(store.names(), ["p"]);
}

#[test]
fn a_secret_that_is_not_there_is_none_rather_than_an_error() {
    let root = workspace("absent");
    let store = SecretStore::open(&root).expect("opens");

    assert_eq!(store.get("nothing").unwrap(), None);
    assert!(store.is_empty());
    assert!(!store.contains("nothing"));
}

#[test]
fn a_secret_can_be_forgotten() {
    let root = workspace("remove");
    let mut store = SecretStore::open(&root).expect("opens");

    store.set("p", "x", None).expect("encrypts");
    assert!(store.remove("p"));
    assert!(!store.remove("p"));
    assert!(store.is_empty());
}

#[test]
fn a_secret_must_be_named() {
    let root = workspace("empty_name");
    let mut store = SecretStore::open(&root).expect("opens");

    assert!(matches!(
        store.set("  ", "x", None),
        Err(SecretError::EmptyName)
    ));
}

// ---------------------------------------------------------------------------
// Failing closed
// ---------------------------------------------------------------------------

#[test]
fn the_wrong_key_fails_closed() {
    // Phase 5's stated acceptance check. A wrong key must produce an error,
    // never plausible-looking rubbish.
    let original = workspace("wrong_key_source");
    let mut store = SecretStore::open(&original).expect("opens");
    store.set("p", "hunter2", None).expect("encrypts");
    store.save().expect("saves");

    // The same store, a different workspace's key.
    let imposter = workspace("wrong_key_target");
    SecretStore::initialise(&imposter).expect("makes a different key");
    std::fs::copy(
        SecretStore::store_path(&original),
        SecretStore::store_path(&imposter),
    )
    .expect("copies the store but not the key");

    let store = SecretStore::open_existing(&imposter).expect("opens");
    let error = store.get("p").expect_err("must not decrypt");

    assert!(
        matches!(&error, SecretError::Undecryptable { name } if name == "p"),
        "{error:?}"
    );
    assert!(error.to_string().contains("key"), "{error}");
}

#[test]
fn an_altered_ciphertext_fails_closed() {
    // GCM authenticates as well as encrypts, so a tampered entry must not
    // decrypt to anything at all.
    let root = workspace("tampered");
    let mut store = SecretStore::open(&root).expect("opens");
    store.set("p", "hunter2", None).expect("encrypts");

    // Flip the last hex digit of the ciphertext.
    let entry = store.file.secrets.get_mut("p").unwrap();
    let mut characters: Vec<char> = entry.ciphertext.chars().collect();
    let last = characters.len() - 1;
    characters[last] = if characters[last] == '0' { '1' } else { '0' };
    entry.ciphertext = characters.into_iter().collect();

    assert!(matches!(
        store.get("p"),
        Err(SecretError::Undecryptable { .. })
    ));
}

#[test]
fn renaming_an_entry_inside_the_file_fails_closed() {
    // The name is the associated data, so moving a value to another name
    // breaks it. Without that, someone could swap the dev and prod passwords
    // by editing the JSON, and every check would still pass.
    let root = workspace("renamed");
    let mut store = SecretStore::open(&root).expect("opens");
    store
        .set("dev_password", "dev-value", None)
        .expect("encrypts");

    let entry = store.file.secrets.remove("dev_password").unwrap();
    store
        .file
        .secrets
        .insert("prod_password".to_string(), entry);

    assert!(matches!(
        store.get("prod_password"),
        Err(SecretError::Undecryptable { .. })
    ));
}

#[test]
fn a_run_refuses_to_mint_a_key_it_was_not_given() {
    // `open_existing` is what a run uses: silently making a key would then fail
    // to decrypt everything, and "no key" is far clearer than "not found".
    let root = workspace("no_key");

    let error = SecretStore::open_existing(&root).expect_err("must refuse");

    assert!(matches!(error, SecretError::NoKey { .. }), "{error:?}");
    assert!(error.to_string().contains("etl secret init"), "{error}");
    assert!(!SecretStore::has_key(&root));
}

#[test]
fn initialising_twice_keeps_the_first_key() {
    // A second key would not replace the first, it would make every existing
    // secret permanently unreadable.
    let root = workspace("init_twice");

    SecretStore::initialise(&root).expect("first");
    let key = std::fs::read_to_string(SecretStore::key_path(&root)).unwrap();

    SecretStore::initialise(&root).expect("second is a no-op");

    assert_eq!(
        std::fs::read_to_string(SecretStore::key_path(&root)).unwrap(),
        key,
        "the key was replaced"
    );
}

#[test]
fn a_key_of_the_wrong_length_is_refused() {
    let root = workspace("short_key");
    std::fs::create_dir_all(SecretStore::key_path(&root).parent().unwrap()).unwrap();
    std::fs::write(SecretStore::key_path(&root), "abcdef\n").unwrap();

    let error = SecretStore::open_existing(&root).expect_err("must refuse");

    assert!(
        matches!(error, SecretError::KeyMalformed { .. }),
        "{error:?}"
    );
    assert!(error.to_string().contains("32"), "{error}");
}

#[test]
fn a_key_that_is_not_hexadecimal_is_refused() {
    let root = workspace("bad_key");
    std::fs::create_dir_all(SecretStore::key_path(&root).parent().unwrap()).unwrap();
    std::fs::write(SecretStore::key_path(&root), "not a key at all\n").unwrap();

    assert!(matches!(
        SecretStore::open_existing(&root),
        Err(SecretError::KeyMalformed { .. })
    ));
}

#[test]
fn a_malformed_store_names_the_file() {
    let root = workspace("bad_store");
    SecretStore::initialise(&root).expect("key");
    std::fs::write(SecretStore::store_path(&root), "{ not json").unwrap();

    let error = SecretStore::open_existing(&root).expect_err("must refuse");

    assert!(
        matches!(error, SecretError::StoreMalformed { .. }),
        "{error:?}"
    );
    assert!(error.to_string().contains("secrets.json"), "{error}");
}

#[test]
fn a_nonce_of_the_wrong_size_is_reported_as_a_broken_entry() {
    let root = workspace("bad_nonce");
    let mut store = SecretStore::open(&root).expect("opens");
    store.set("p", "x", None).expect("encrypts");
    store.file.secrets.get_mut("p").unwrap().nonce = "aabb".to_string();

    let error = store.get("p").expect_err("must refuse");

    assert!(
        matches!(error, SecretError::EntryMalformed { .. }),
        "{error:?}"
    );
    assert!(error.to_string().contains("12"), "{error}");
}

// ---------------------------------------------------------------------------
// Not leaking by accident
// ---------------------------------------------------------------------------

#[test]
fn debug_does_not_print_the_key() {
    // `Debug` is derived almost everywhere in this workspace; here it is not,
    // precisely so a stray `{:?}` in a log cannot print the key.
    let root = workspace("debug_redaction");
    let mut store = SecretStore::open(&root).expect("opens");
    store.set("p", "hunter2", None).expect("encrypts");

    let printed = format!("{store:?}");

    assert!(printed.contains("<redacted>"), "{printed}");
    assert!(!printed.contains("hunter2"), "{printed}");
    assert!(
        !printed.contains(&to_hex(&store.key)),
        "the key is in the Debug output"
    );
}

// ---------------------------------------------------------------------------
// Hex
// ---------------------------------------------------------------------------

#[test]
fn hex_round_trips_every_byte() {
    let all: Vec<u8> = (0..=255).collect();

    assert_eq!(from_hex(&to_hex(&all)).unwrap(), all);
    assert_eq!(to_hex(&[0x00, 0x0f, 0xff]), "000fff");
    assert_eq!(from_hex("").unwrap(), Vec::<u8>::new());
}

#[test]
fn hex_refuses_what_is_not_hex() {
    assert_eq!(from_hex("abc"), None, "odd length");
    assert_eq!(from_hex("zz"), None, "not a hex digit");
    assert_eq!(from_hex("ab cd"), None, "spaces are not hex digits");
}
