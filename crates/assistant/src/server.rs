//! `llama-server` as a subprocess, for as long as one request needs it.

use std::fs::File;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const SERVER_ENV: &str = "ETL_LLAMA_SERVER";
pub const MODEL_ENV: &str = "ETL_ASSIST_MODEL";
pub const EMBED_MODEL_ENV: &str = "ETL_EMBED_MODEL";

/// The embedding model `scripts/fetch-model.ps1` fetches (Settled decision 109).
pub const DEFAULT_EMBEDDER: &str = "bge-small-en-v1.5-q8_0.gguf";

/// The model `scripts/fetch-model.ps1` fetches (Settled decision 95).
pub const DEFAULT_MODEL: &str = "qwen2.5-coder-1.5b-instruct-q4_k_m.gguf";

/// What a request ends with when the server was stopped under it.
pub const STOPPED: &str = "stopped before it answered";

/// Room for the prompt, a handful of component descriptions and the answer.
const CONTEXT: &str = "8192";

/// Loading a 1 GB model from a cold disk is the slow part of starting.
const STARTUP: Duration = Duration::from_secs(180);

/// A pipeline takes a minute or two on a laptop's CPU; this is the ceiling.
const GENERATION: Duration = Duration::from_secs(900);

/// Find `llama-server`: an explicit path, then `ETL_LLAMA_SERVER`, then
/// `tools/llama/` searching upward from `start`, as DuckDB is found.
pub fn locate_server(explicit: Option<&Path>, start: &Path) -> Result<PathBuf, String> {
    let name = if cfg!(windows) {
        "llama-server.exe"
    } else {
        "llama-server"
    };
    locate(
        explicit,
        SERVER_ENV,
        start,
        &["llama", name],
        "llama-server",
    )
}

/// Find the model: an explicit path, then `ETL_ASSIST_MODEL`, then
/// `tools/models/` searching upward from `start`.
pub fn locate_model(explicit: Option<&Path>, start: &Path) -> Result<PathBuf, String> {
    locate(
        explicit,
        MODEL_ENV,
        start,
        &["models", DEFAULT_MODEL],
        "the model",
    )
}

/// Find the embedding model `xf.ai.embed` runs: an explicit path, then
/// `ETL_EMBED_MODEL`, then `tools/models/` searching upward from `start`.
pub fn locate_embedder(explicit: Option<&Path>, start: &Path) -> Result<PathBuf, String> {
    locate(
        explicit,
        EMBED_MODEL_ENV,
        start,
        &["models", DEFAULT_EMBEDDER],
        "the embedding model",
    )
}

fn locate(
    explicit: Option<&Path>,
    variable: &str,
    start: &Path,
    vendored: &[&str],
    what: &str,
) -> Result<PathBuf, String> {
    // A path someone named is the one they meant: a typo must not quietly run
    // the vendored model instead.
    let named = explicit
        .map(|path| (path.to_path_buf(), path.display().to_string()))
        .or_else(|| {
            std::env::var_os(variable).map(|value| {
                let path = PathBuf::from(value);
                let shown = format!("{variable}={}", path.display());
                (path, shown)
            })
        });
    if let Some((path, shown)) = named {
        return if path.is_file() {
            Ok(path)
        } else {
            Err(format!("{what} was not found at {shown}"))
        };
    }

    let relative: PathBuf = std::iter::once("tools")
        .chain(vendored.iter().copied())
        .collect();
    start
        .ancestors()
        .map(|directory| directory.join(&relative))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            format!(
                "{what} was not found in {} or above it. Fetch it with ./scripts/fetch-model.ps1.",
                start.join(&relative).display()
            )
        })
}

/// A running `llama-server`, stopped when this is dropped.
///
/// Shareable across threads: one thread can wait on an answer while another
/// calls [`Server::stop`], which is how the desktop app cancels.
pub struct Server {
    child: Mutex<Child>,
    stopped: AtomicBool,
    base: String,
    log: PathBuf,
    agent: ureq::Agent,
}

impl Server {
    /// Start `server` with `model` on a loopback port of its own, and wait
    /// until the model has loaded. Its output goes to `log`.
    pub fn start(server: &Path, model: &Path, log: &Path) -> Result<Server, String> {
        let running = Server::spawn(server, model, log)?;
        running.wait_until_loaded()?;
        Ok(running)
    }

    /// Start `server` without waiting for the model to load, so the caller
    /// can hold it (and stop it) while it does.
    pub fn spawn(server: &Path, model: &Path, log: &Path) -> Result<Server, String> {
        // One slot, so the whole context is this request's.
        Server::spawn_with(
            server,
            model,
            log,
            &["--ctx-size", CONTEXT, "--parallel", "1"],
        )
    }

    /// [`Server::spawn`], with the model's own settings: `--embeddings` for an
    /// embedding model, a context size.
    pub fn spawn_with(
        server: &Path,
        model: &Path,
        log: &Path,
        settings: &[&str],
    ) -> Result<Server, String> {
        let port = free_port().map_err(|error| format!("no free local port: {error}"))?;
        let output = File::create(log).map_err(|error| format!("{}: {error}", log.display()))?;
        let errors = output
            .try_clone()
            .map_err(|error| format!("{}: {error}", log.display()))?;

        let child = Command::new(server)
            .arg("--model")
            .arg(model)
            .args([
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--no-webui",
            ])
            .args(settings)
            .stdin(Stdio::null())
            .stdout(output)
            .stderr(errors)
            .spawn()
            .map_err(|error| format!("{} could not be started: {error}", server.display()))?;

        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(GENERATION))
            .build()
            .new_agent();

        Ok(Server {
            child: Mutex::new(child),
            stopped: AtomicBool::new(false),
            base: format!("http://127.0.0.1:{port}"),
            log: log.to_path_buf(),
            agent,
        })
    }

    /// Where it listens, e.g. `http://127.0.0.1:52011`: its OpenAI-compatible
    /// API is under `/v1`.
    pub fn base_url(&self) -> &str {
        &self.base
    }

    /// Wait until the model has loaded, the server has died, or it was stopped.
    pub fn wait_until_loaded(&self) -> Result<(), String> {
        let started = Instant::now();
        let url = format!("{}/health", self.base);
        loop {
            if self.was_stopped() {
                return Err(STOPPED.to_string());
            }
            if let Some(status) = self.exited() {
                return Err(format!(
                    "llama-server stopped while loading the model ({status}).{}",
                    self.log_tail()
                ));
            }
            // 503 while the model loads, 200 once it is ready.
            if let Ok(response) = self.agent.get(&url).call() {
                if response.status() == 200 {
                    return Ok(());
                }
            }
            if started.elapsed() > STARTUP {
                return Err(format!(
                    "llama-server did not load the model within {} seconds.{}",
                    STARTUP.as_secs(),
                    self.log_tail()
                ));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    /// Stop the server, from any thread. An answer being waited on ends with
    /// [`STOPPED`]; the server cannot be used again.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        let mut child = self.child.lock().unwrap_or_else(|held| held.into_inner());
        let _ = child.kill();
        let _ = child.wait();
    }

    /// Whether this server can still answer: not stopped, not exited.
    pub fn is_alive(&self) -> bool {
        !self.was_stopped() && self.exited().is_none()
    }

    fn was_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    fn exited(&self) -> Option<std::process::ExitStatus> {
        let mut child = self.child.lock().unwrap_or_else(|held| held.into_inner());
        child.try_wait().ok().flatten()
    }

    /// POST `body` to `path` and return the JSON answer. A refusal is an
    /// error carrying the server's own message.
    pub fn post(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        let url = format!("{}{path}", self.base);
        let mut response = self
            .agent
            .post(&url)
            .header("Content-Type", "application/json")
            .send(body.to_string())
            .map_err(|error| {
                if self.was_stopped() {
                    STOPPED.to_string()
                } else {
                    format!("llama-server did not answer: {error}.{}", self.log_tail())
                }
            })?;

        let status = response.status();
        let text = response
            .body_mut()
            .with_config()
            .limit(64 * 1024 * 1024)
            .read_to_string()
            .map_err(|error| format!("llama-server's answer could not be read: {error}"))?;
        let answer: Result<serde_json::Value, _> = serde_json::from_str(&text);
        if status != 200 {
            let said = answer
                .ok()
                .and_then(|answer| answer["error"]["message"].as_str().map(str::to_string))
                .unwrap_or(text);
            return Err(format!(
                "llama-server refused the request ({status}): {said}"
            ));
        }
        answer.map_err(|error| format!("llama-server's answer is not JSON: {error}"))
    }

    /// Send one chat completion and return the model's text.
    pub fn complete(&self, body: &serde_json::Value) -> Result<String, String> {
        let answer = self.post("/v1/chat/completions", body)?;
        let text = answer.to_string();
        let choice = &answer["choices"][0];
        if choice["finish_reason"] == "length" {
            return Err(format!(
                "the model had not finished after {} tokens",
                crate::prompt::MAX_TOKENS
            ));
        }
        choice["message"]["content"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("llama-server's answer has no message: {text}"))
    }

    /// The last lines the server wrote, for an error to end with.
    fn log_tail(&self) -> String {
        let Ok(text) = std::fs::read_to_string(&self.log) else {
            return String::new();
        };
        let lines: Vec<&str> = text.lines().collect();
        let tail = lines[lines.len().saturating_sub(8)..].join("\n");
        if tail.is_empty() {
            String::new()
        } else {
            format!(" Its log ({}) ends:\n{tail}", self.log.display())
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A port nothing is listening on. The listener is dropped before the server
/// binds it; something else taking it in between fails the start, loudly.
fn free_port() -> std::io::Result<u16> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("etl-assistant-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn the_vendored_model_is_found_from_below_it() {
        let root = scratch("vendored");
        std::fs::create_dir_all(root.join("tools/models")).unwrap();
        std::fs::write(root.join("tools/models").join(DEFAULT_MODEL), b"gguf").unwrap();
        let below = root.join("crates/cli");
        std::fs::create_dir_all(&below).unwrap();

        let found = locate(
            None,
            "ETL_TEST_UNSET_VARIABLE",
            &below,
            &["models", DEFAULT_MODEL],
            "the model",
        );
        assert_eq!(
            found.unwrap(),
            root.join("tools/models").join(DEFAULT_MODEL)
        );
    }

    #[test]
    fn an_explicit_path_wins_and_a_missing_one_is_named() {
        let root = scratch("explicit");
        let model = root.join("other.gguf");
        std::fs::write(&model, b"gguf").unwrap();

        assert_eq!(locate_model(Some(&model), &root).unwrap(), model);

        // Even with the vendored model there to fall back on.
        std::fs::create_dir_all(root.join("tools/models")).unwrap();
        std::fs::write(root.join("tools/models").join(DEFAULT_MODEL), b"gguf").unwrap();
        let missing = root.join("missing.gguf");
        let error = locate(
            Some(&missing),
            "ETL_TEST_UNSET_VARIABLE",
            &root,
            &["models", DEFAULT_MODEL],
            "the model",
        )
        .unwrap_err();
        assert!(error.contains("missing.gguf"), "{error}");
    }

    #[test]
    fn nothing_found_says_how_to_fetch_it() {
        let root = scratch("nothing");
        let error = locate(
            None,
            "ETL_TEST_UNSET_VARIABLE",
            &root,
            &["llama", "llama-server"],
            "llama-server",
        )
        .unwrap_err();
        assert!(error.contains("fetch-model.ps1"), "{error}");
    }
}
