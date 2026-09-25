//! Which components a request is likeliest to need, by its words.
//!
//! The manifest is 156 KB, far more than a small model's prompt can carry, so
//! the prompt describes a handful of components and the grammar allows only
//! those. A word counts most when it names a component's id ("postgres" in
//! `src.db.postgres`), less in its label, least in its description.

use etl_metadata::{ComponentSpec, Namespace};

/// At most this many components are offered.
pub const MAX_PICKED: usize = 8;

/// Offered when the request names no source or no sink of its own.
const FALLBACK_SOURCES: [&str; 2] = ["src.file.csv", "src.file.parquet"];
const FALLBACK_SINKS: [&str; 2] = ["snk.file.parquet", "snk.file.csv"];

/// Words that say nothing about which component is meant.
const STOPWORDS: &[&str] = &[
    "a", "an", "and", "all", "as", "at", "by", "each", "every", "file", "files", "for", "from",
    "get", "in", "into", "it", "its", "load", "make", "me", "of", "on", "one", "out", "please",
    "put", "read", "rows", "save", "send", "table", "tables", "take", "that", "the", "them",
    "then", "these", "this", "those", "to", "with", "write",
];

/// Words people use for a component whose id and label say it differently.
const SYNONYMS: &[(&str, &str)] = &[
    ("duplicate", "dedup"),
    ("duplicates", "dedup"),
    ("pg", "postgres"),
    ("mssql", "sqlserver"),
    ("azure", "sqlserver"),
    // "Keep" is in six transforms' descriptions; a comparison says filter.
    ("where", "filter"),
    ("only", "filter"),
    ("over", "filter"),
    ("above", "filter"),
    ("under", "filter"),
    ("below", "filter"),
    ("greater", "filter"),
    ("less", "filter"),
    ("group", "aggregate"),
    ("sum", "aggregate"),
    ("count", "aggregate"),
    ("order", "sort"),
    ("rename", "rename"),
    ("columns", "select"),
    ("top", "limit"),
    ("first", "limit"),
    ("jsonlines", "jsonl"),
    ("ndjson", "jsonl"),
    ("xlsx", "excel"),
    ("bucket", "s3"),
];

/// The components to offer for this request, sources first, sinks last.
pub fn pick<'a>(request: &str, specs: &'a [ComponentSpec]) -> Vec<&'a ComponentSpec> {
    let words = words(request);

    let mut scored: Vec<(u32, usize)> = specs
        .iter()
        .enumerate()
        .filter(|(_, spec)| offered(spec.namespace))
        .map(|(index, spec)| (score(&words, spec), index))
        .filter(|(score, _)| *score >= 2)
        .collect();
    // Highest first; the registry's order breaks ties, so the same request
    // always gets the same prompt.
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));

    let mut picked: Vec<&ComponentSpec> = scored
        .into_iter()
        .take(MAX_PICKED)
        .map(|(_, index)| &specs[index])
        .collect();

    for (namespace, fallback) in [
        (Namespace::Source, FALLBACK_SOURCES),
        (Namespace::Sink, FALLBACK_SINKS),
    ] {
        if picked.iter().any(|spec| spec.namespace == namespace) {
            continue;
        }
        if picked.len() + fallback.len() > MAX_PICKED {
            picked.truncate(MAX_PICKED - fallback.len());
        }
        picked.extend(
            specs
                .iter()
                .filter(|spec| fallback.contains(&spec.id.as_str())),
        );
    }

    picked.sort_by_key(|spec| match spec.namespace {
        Namespace::Source => 0,
        Namespace::Sink => 2,
        _ => 1,
    });
    picked
}

/// Control flow and user code need a plan of their own; a model this small is
/// not asked to write them.
fn offered(namespace: Namespace) -> bool {
    !matches!(namespace, Namespace::Control | Namespace::Code)
}

/// The request's words, lowercased, stopwords gone, synonyms added.
pub fn words(request: &str) -> Vec<String> {
    let mut words: Vec<String> = Vec::new();
    for word in request
        .split(|c: char| !c.is_alphanumeric())
        .map(str::to_lowercase)
        .filter(|word| !word.is_empty() && !STOPWORDS.contains(&word.as_str()))
    {
        for (said, meant) in SYNONYMS {
            if word == *said && !words.iter().any(|w| w == meant) {
                words.push(meant.to_string());
            }
        }
        if !words.contains(&word) {
            words.push(word);
        }
    }
    words
}

fn score(words: &[String], spec: &ComponentSpec) -> u32 {
    // The id's segments after the namespace: `db`, `postgres`.
    let id: Vec<String> = spec.id.split('.').skip(1).map(str::to_lowercase).collect();
    let label = words_of(&spec.label);
    let description = spec
        .description
        .as_deref()
        .map(words_of)
        .unwrap_or_default();

    words
        .iter()
        .map(|word| {
            if id.iter().any(|key| matches(word, key)) {
                3
            } else if label.iter().any(|key| matches(word, key)) {
                2
            } else if description.iter().any(|key| matches(word, key)) {
                1
            } else {
                0
            }
        })
        .sum()
}

fn words_of(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .map(str::to_lowercase)
        .filter(|word| !word.is_empty() && !STOPWORDS.contains(&word.as_str()))
        .collect()
}

/// The same word, or one a longer form of the other: `dedupe` and `dedup`,
/// `postgres` and `postgresql`. Short words must match exactly, or `db` would
/// match everything.
fn matches(word: &str, key: &str) -> bool {
    if word == key {
        return true;
    }
    let (short, long) = if word.len() <= key.len() {
        (word, key)
    } else {
        (key, word)
    };
    short.len() >= 4 && long.starts_with(short)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs() -> Vec<ComponentSpec> {
        [
            ("src.file.csv", "CSV file", "Read a CSV file."),
            ("src.file.parquet", "Parquet file", "Read Parquet files."),
            (
                "src.db.postgres",
                "PostgreSQL table",
                "Read a table from a PostgreSQL database.",
            ),
            (
                "src.db.mysql",
                "MySQL table",
                "Read a table from a MySQL database.",
            ),
            ("xf.dedup", "Deduplicate", "Keep one row per key."),
            (
                "xf.filter",
                "Filter",
                "Keep the rows a predicate is true for.",
            ),
            ("xf.sort", "Sort", "Order rows."),
            ("ctl.log", "Log", "Write a message to the run's log."),
            ("snk.file.csv", "CSV file", "Write a CSV file."),
            ("snk.file.parquet", "Parquet file", "Write a Parquet file."),
            (
                "snk.db.postgres",
                "PostgreSQL table",
                "Write to a PostgreSQL table.",
            ),
        ]
        .into_iter()
        .map(|(id, label, description)| ComponentSpec::new(id, label).description(description))
        .collect()
    }

    fn ids(picked: &[&ComponentSpec]) -> Vec<String> {
        picked.iter().map(|spec| spec.id.clone()).collect()
    }

    #[test]
    fn the_verify_request_gets_its_three_components() {
        let specs = specs();
        let picked = ids(&pick(
            "read this Postgres table, dedupe, write Parquet",
            &specs,
        ));

        for wanted in ["src.db.postgres", "xf.dedup", "snk.file.parquet"] {
            assert!(
                picked.contains(&wanted.to_string()),
                "{wanted} in {picked:?}"
            );
        }
        assert!(!picked.contains(&"src.db.mysql".to_string()));
        assert!(!picked.contains(&"xf.sort".to_string()));
        // Sources first, sinks last, for the prompt to read in order.
        assert!(picked.first().unwrap().starts_with("src."));
        assert!(picked.last().unwrap().starts_with("snk."));
    }

    #[test]
    fn a_source_and_a_sink_are_always_offered() {
        let specs = specs();
        let picked = ids(&pick("dedupe it", &specs));

        assert!(picked.contains(&"xf.dedup".to_string()));
        assert!(picked.contains(&"src.file.csv".to_string()));
        assert!(picked.contains(&"snk.file.parquet".to_string()));
    }

    #[test]
    fn control_nodes_are_never_offered() {
        let specs = specs();
        assert!(!ids(&pick("log the csv", &specs)).contains(&"ctl.log".to_string()));
    }

    #[test]
    fn words_drop_the_filler_and_add_what_a_synonym_means() {
        assert_eq!(
            words("Read the duplicates from PG, then write CSV"),
            vec!["dedup", "duplicates", "postgres", "pg", "csv"]
        );
    }

    #[test]
    fn a_longer_form_matches_and_a_short_word_only_exactly() {
        assert!(matches("dedupe", "dedup"));
        assert!(matches("postgres", "postgresql"));
        assert!(!matches("db", "dbt"));
        assert!(!matches("sql", "sqlite"));
    }

    #[test]
    fn the_same_request_picks_the_same_components() {
        let specs = specs();
        let request = "csv to parquet sorted";
        assert_eq!(ids(&pick(request, &specs)), ids(&pick(request, &specs)));
    }
}
