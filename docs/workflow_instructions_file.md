# Project Workflow

How work runs in this repo — for Claude, and for anyone picking the project up later.
Project-agnostic: copy this file into a new project as-is.

## Ground rules

| Rule | Detail |
|---|---|
| Questions, then plan, then build | Ask clarifying questions first. Nothing is implemented until the plan file is agreed. |
| Questions as a written list | List every question at once, numbered, each with its options and a recommended answer. Never ask one at a time through interactive prompts. The user replies to all of them in one message. Unanswered ones go in the tracker as open decisions. |
| One phase at a time | When a phase is done, stop and ask. Start the next phase only after the user confirms. |
| Commands run on the user's machine | Claude runs them through PowerShell. No GUI or system-level control of the machine. |
| Ask before consequential actions | Writing outside the project, installing globally, deleting, or publishing needs approval first. Ordinary edits and test runs inside the project do not. |
| Sandbox first | Try exploratory or risky changes in Claude's scratchpad (or a temp copy) before applying them to the project. |
| Git only on "start" | No `add`, `commit`, `push` or tag until the user says **start**. |
| Log every command | Append each executed command, including failures, to [commands.md](commands.md). |

## Planning

- Write the plan to `PLAN_<topic>.md`, in the project's docs folder.
- Split it into **phases**, each small enough to finish in one day.
- Each phase states: goal, files touched, how it is verified, and what "done" means.
- Assume context is lost between phases. The plan and tracker must be enough to resume.

## Tracking

[task_tracker.md](task_tracker.md) holds the **state**; the plan holds the **design**.
It must answer, without reading any code:

1. Which phases and tasks are **done**, and on what date.
2. What is **deferred**, and why.
3. Which phase is **next**, plus any blockers or open decisions.

Update it before stopping each session. It is the first file to read on resume.

## Learnings and assignments

Each project keeps two more files in its docs folder. Create them in the first phase.

| File | Holds |
|---|---|
| `learnings.md` | What each phase taught: the concepts used, decisions and why, and mistakes worth not repeating. One section per phase, dated. |
| `assignments.md` | Hands-on exercises for the user, drawn from what the phase built. Each has a goal, a hint, and how to check the answer. Mark them done when completed. |

Add to both at the end of every phase, before asking to start the next.

## Python projects

- Package manager: **uv**. The virtual environment lives in the project (`.venv`).
- Every dependency goes in `pyproject.toml`. Add them with `uv add`, never `pip install`.
- Always work inside the project venv, for installs, tests and validation alike.

```powershell
uv init                           # new project
uv venv                           # create .venv
uv add <package>                  # runtime dependency
uv add --dev ipykernel pytest     # dev dependencies (ipykernel is always included)
uv sync                           # install from pyproject.toml / uv.lock
uv run pytest                     # run tests inside the venv
uv run python <script>.py         # run code inside the venv
.\.venv\Scripts\Activate.ps1      # interactive shell inside the venv
```

Registering the kernel with Jupyter writes outside the project, so ask first:
`uv run python -m ipykernel install --user --name <project>`

## Rust projects

Dependencies go in `Cargo.toml` via `cargo add`. The gate is the last three lines below.

```powershell
cargo add <crate> -p <package>                        # add a dependency
cargo build --workspace                               # build
cargo fmt --all --check                               # gate: formatting
cargo clippy --workspace --all-targets -- -D warnings # gate: lints
cargo test --workspace                                # gate: tests
```

## Daily loop

1. Read [task_tracker.md](task_tracker.md). Confirm the next phase with the user.
2. Read that phase in the plan. Test in the sandbox where it helps, then implement.
3. Run the gate until green.
4. Update the tracker: done, deferred, and newly found blockers. Add the phase to `learnings.md` and `assignments.md`.
5. Check that commands are logged. Report, then ask before starting the next phase.
