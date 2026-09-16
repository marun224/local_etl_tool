# Project Workflow

How work runs in this repo — for Claude, and for anyone picking the project up later.
Project-agnostic: copy this file into a new project as-is.

## Ground rules

| Rule | Detail |
|---|---|
| Plan before building | Nothing is implemented until a plan file exists and is agreed. |
| Ask before consequential actions | Writing outside the project, installing globally, publishing, or anything touching git needs approval first. Ordinary edits and test runs inside the project do not. |
| Never commit unasked | No `add`, `commit`, `push`, or tag without a direct instruction. |
| Log every command | Append each executed command to [commands.md](commands.md). |
| PowerShell | Commands are written for PowerShell on Windows. |

```

## Planning

Work is written to a plan file before implementation starts.

- Name it for the work, not generically — `PLAN_<topic>.md`.
- Split into **phases**, each finishable in a single sitting.
- Each phase states: goal, files touched, how it is verified, what "done" means.
- Phases are executed on different days. Assume the context is lost between them.

## Resuming

Status lives in [task_tracker.md](task_tracker.md), not in the plan. The plan file holds
the **design**; the tracker holds the **state**.

Without reading any code, the tracker must answer:

1. Which phases are **done**, and on what date.
2. Which phase is **next**.
3. What is in progress, and any known gaps, blockers or open decisions.

Update the tracker at the end of every session — before stopping, not afterwards. It is
the first file to read when picking the project back up.

## Daily loop

1. Read [task_tracker.md](task_tracker.md). Confirm the next phase.
2. Read that phase in the plan file. Implement it.
3. Run the gate until green.
4. Update the tracker: mark the phase done, record anything discovered or newly blocked.
5. Check commands are logged. Stop. Do not commit unless asked.
