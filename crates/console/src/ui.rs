//! The console page: one file, no build step, no network.
//!
//! Served as a string from this binary rather than from a `frontend/` bundle.
//! The canvas in Phase 7 is a real application and earns Vite; this is four
//! tables and a button, and giving it a build step would mean the headless
//! runner could not serve its own console without one.
//!
//! # Two rules the page keeps
//!
//! **Everything from the workspace goes in through `textContent`.** Pipeline
//! names, error messages and file paths are all data somebody else wrote, and
//! `innerHTML` would make a pipeline named `<img onerror=...>` into script
//! running with the operator's token in hand. There is no `innerHTML` in this
//! file and there should never be one.
//!
//! **The token leaves the URL immediately.** The printed link carries it as a
//! query parameter because that is what makes a link work at all; the page
//! moves it into session storage and rewrites the address bar on load, so it
//! does not sit in the bar to be shoulder-read, copied into a chat, or handed
//! to another origin by a `Referer`. From then on it travels in a header,
//! which is the only form the API accepts.

/// The console page, with the workspace's label in the title.
pub fn page(label: &str) -> String {
    // The label comes from a path on this machine, so it is not hostile — but
    // it is interpolated into HTML, so it is escaped anyway. The one place in
    // this file where server-side data meets markup deserves the same care
    // the client side takes.
    let label = escape(label);

    format!(
        r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="referrer" content="no-referrer">
<title>{label} — etl console</title>
<style>
  :root {{
    --bg: #fbfbfa; --panel: #fff; --ink: #1a1a18; --muted: #6b6b66;
    --line: #e4e4e0; --accent: #2f6f4f; --bad: #a33; --warn: #8a6d1f;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --bg: #17171a; --panel: #1f1f23; --ink: #e8e8e4; --muted: #9a9a94;
      --line: #32323a; --accent: #7fc4a0; --bad: #e08585; --warn: #d6b45c;
    }}
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; background: var(--bg); color: var(--ink);
    font: 14px/1.5 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
  }}
  header {{
    padding: 16px 20px; border-bottom: 1px solid var(--line);
    display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap;
  }}
  h1 {{ font-size: 15px; margin: 0; font-weight: 600; }}
  .muted {{ color: var(--muted); }}
  .role {{
    font-size: 11px; text-transform: uppercase; letter-spacing: .06em;
    border: 1px solid var(--line); border-radius: 99px; padding: 2px 9px;
  }}
  main {{ padding: 20px; display: grid; gap: 20px; max-width: 1100px; }}
  section {{
    background: var(--panel); border: 1px solid var(--line);
    border-radius: 8px; overflow: hidden;
  }}
  h2 {{
    font-size: 12px; text-transform: uppercase; letter-spacing: .07em;
    margin: 0; padding: 11px 14px; border-bottom: 1px solid var(--line);
    color: var(--muted); font-weight: 600;
  }}
  table {{ width: 100%; border-collapse: collapse; }}
  td, th {{
    padding: 9px 14px; text-align: left; border-bottom: 1px solid var(--line);
    vertical-align: top;
  }}
  th {{ font-weight: 500; color: var(--muted); font-size: 12px; }}
  tr:last-child td {{ border-bottom: none; }}
  code {{ font-family: ui-monospace, "Cascadia Code", Consolas, monospace; font-size: 12.5px; }}
  button {{
    font: inherit; font-size: 12px; padding: 4px 11px; cursor: pointer;
    border: 1px solid var(--line); border-radius: 5px;
    background: var(--panel); color: var(--ink);
  }}
  button:hover:enabled {{ border-color: var(--accent); color: var(--accent); }}
  button:disabled {{ opacity: .45; cursor: default; }}
  .ok {{ color: var(--accent); }}
  .bad {{ color: var(--bad); }}
  .warn {{ color: var(--warn); }}
  .empty {{ padding: 14px; color: var(--muted); }}
  #banner {{
    display: none; margin: 0; padding: 11px 20px;
    border-bottom: 1px solid var(--line); background: var(--panel); color: var(--bad);
  }}
  #banner.on {{ display: block; }}
</style>
</head>
<body>
<header>
  <h1>{label}</h1>
  <span class="muted">etl console</span>
  <span class="role" id="role">…</span>
  <span class="muted" id="clock"></span>
</header>
<p id="banner"></p>
<main>
  <section><h2>Pipelines</h2><div id="pipelines" class="empty">Loading…</div></section>
  <section><h2>Schedules</h2><div id="schedules" class="empty">Loading…</div></section>
  <section><h2>Recent runs</h2><div id="runs" class="empty">Loading…</div></section>
</main>
<script>
(function () {{
  "use strict";

  // The token arrives in the link and is moved out of the URL at once: it must
  // not sit in the address bar to be shoulder-read, pasted into a chat, or
  // handed to another origin. From here it lives in session storage, which
  // dies with the tab, and travels only in a header.
  var KEY = "etl-console-token";
  var url = new URL(window.location.href);
  var fromLink = url.searchParams.get("token");

  if (fromLink) {{
    try {{ sessionStorage.setItem(KEY, fromLink); }} catch (e) {{ /* private mode */ }}
    url.searchParams.delete("token");
    window.history.replaceState({{}}, "", url.pathname + url.search);
  }}

  var token = fromLink;
  if (!token) {{
    try {{ token = sessionStorage.getItem(KEY); }} catch (e) {{ token = null; }}
  }}

  var role = null;

  function say(message) {{
    var banner = document.getElementById("banner");
    banner.textContent = message || "";
    banner.className = message ? "on" : "";
  }}

  function api(path, options) {{
    var settings = options || {{}};
    settings.headers = {{ "Authorization": "Bearer " + (token || "") }};
    settings.cache = "no-store";

    return fetch(path, settings).then(function (response) {{
      // The server states the role on every authenticated response, so the
      // page never has to guess it from an error message.
      var stated = response.headers.get("X-Etl-Role");
      if (stated && stated !== role) {{
        role = stated;
        document.getElementById("role").textContent = role;
      }}

      return response.json().catch(function () {{ return {{}}; }}).then(function (body) {{
        if (!response.ok) {{
          throw new Error(body.error || ("HTTP " + response.status));
        }}
        return body;
      }});
    }});
  }}

  // Every cell goes through textContent. A pipeline named `<img onerror=…>` is
  // a string, not markup, and this is the only reason that is true.
  function cell(text, className) {{
    var td = document.createElement("td");
    td.textContent = text === null || text === undefined ? "—" : String(text);
    if (className) {{ td.className = className; }}
    return td;
  }}

  function table(into, columns, rows, build) {{
    var host = document.getElementById(into);
    host.textContent = "";

    if (!rows.length) {{
      host.className = "empty";
      host.textContent = "None.";
      return;
    }}

    host.className = "";

    var element = document.createElement("table");
    var head = document.createElement("tr");

    columns.forEach(function (name) {{
      var th = document.createElement("th");
      th.textContent = name;
      head.appendChild(th);
    }});

    element.appendChild(head);

    rows.forEach(function (row) {{
      var tr = document.createElement("tr");
      build(tr, row);
      element.appendChild(tr);
    }});

    host.appendChild(element);
  }}

  function outcomeClass(outcome) {{
    if (outcome === "succeeded") {{ return "ok"; }}
    if (outcome === "failed") {{ return "bad"; }}
    return "warn";
  }}

  function loadPipelines() {{
    return api("/api/pipelines").then(function (body) {{
      table("pipelines", ["Pipeline", "Stages", "Last run", "", ""], body.pipelines,
        function (tr, pipeline) {{
          tr.appendChild(cell(pipeline.name));
          tr.appendChild(cell(pipeline.problem ? "—" : pipeline.stages));
          tr.appendChild(cell(pipeline.lastRun, pipeline.lastOutcome
            ? outcomeClass(pipeline.lastOutcome) : null));

          var note = cell(pipeline.problem || "", pipeline.problem ? "bad" : null);
          tr.appendChild(note);

          var action = document.createElement("td");

          // The button exists only for an operator, and only for a pipeline
          // that compiles. The server refuses either way — this is so the page
          // does not offer something it knows will be turned down.
          if (role === "operator" && !pipeline.problem) {{
            var button = document.createElement("button");
            button.textContent = "Run";
            button.addEventListener("click", function () {{
              button.disabled = true;
              button.textContent = "Running…";
              say("");

              api("/api/runs?pipeline=" + encodeURIComponent(pipeline.name), {{ method: "POST" }})
                .then(function () {{ return refresh(); }})
                .catch(function (error) {{ say(error.message); }})
                .then(function () {{
                  button.disabled = false;
                  button.textContent = "Run";
                }});
            }});
            action.appendChild(button);
          }}

          tr.appendChild(action);
        }});
    }});
  }}

  function loadSchedules() {{
    return api("/api/schedules").then(function (body) {{
      table("schedules", ["Schedule", "Pipeline", "Trigger", "Next"], body.schedules,
        function (tr, schedule) {{
          tr.appendChild(cell(schedule.name));
          tr.appendChild(cell(schedule.pipeline));
          tr.appendChild(cell(schedule.trigger));
          tr.appendChild(cell(schedule.enabled ? (schedule.next || "on change") : "off",
            schedule.enabled ? null : "muted"));
        }});
    }});
  }}

  function loadRuns() {{
    return api("/api/runs?limit=25").then(function (body) {{
      table("runs", ["Started", "Pipeline", "Outcome", "Took", "Rows"], body.runs,
        function (tr, run) {{
          tr.appendChild(cell(run.started));
          tr.appendChild(cell(run.pipeline));
          tr.appendChild(cell(run.outcome, outcomeClass(run.outcome)));
          tr.appendChild(cell(run.elapsedMs === undefined
            ? null : (run.elapsedMs / 1000).toFixed(2) + "s"));

          var rows = null;
          if (run.stages && run.stages.length) {{
            for (var i = run.stages.length - 1; i >= 0; i--) {{
              if (run.stages[i].rows !== undefined && run.stages[i].rows !== null) {{
                rows = run.stages[i].rows;
                break;
              }}
            }}
          }}
          tr.appendChild(cell(rows));
        }});
    }});
  }}

  function refresh() {{
    return Promise.all([loadPipelines(), loadSchedules(), loadRuns()])
      .then(function () {{
        document.getElementById("clock").textContent =
          "updated " + new Date().toISOString().replace(/\.\d+Z$/, "Z");
      }})
      .catch(function (error) {{ say(error.message); }});
  }}

  if (!token) {{
    say("No token. Open the link `etl serve` printed, which carries one.");
    ["pipelines", "schedules", "runs"].forEach(function (id) {{
      document.getElementById(id).textContent = "—";
    }});
    return;
  }}

  // The first request establishes the role from `X-Etl-Role`, and the
  // pipelines table is drawn again once it is known — so a Run button appears
  // for an operator without the page having guessed anything.
  refresh().then(function () {{
    if (role === "operator") {{ loadPipelines(); }}
  }});

  setInterval(refresh, 10000);
}})();
</script>
</body>
</html>
"##
    )
}

/// Escape the five characters that matter in HTML text and attributes.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());

    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }

    out
}

#[cfg(test)]
mod tests;
