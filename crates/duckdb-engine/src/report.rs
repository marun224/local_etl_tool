//! How a [`RunReport`](crate::RunReport) reads.
//!
//! Lines, not printing: the caller decides where they go. `etl run` sends them
//! to stdout, and so does the standalone runner Phase 9 builds — and that is
//! the reason this lives here rather than in either of them. The formatting has
//! real judgement in it (which timings are honest, when a rejected count is
//! worth showing, how a skipped stage differs from an uncounted one), and two
//! copies of that judgement would drift the first time one was touched.
//!
//! It is also what makes the rules testable without a process, a terminal or a
//! captured stdout.

use crate::RunReport;

#[cfg(test)]
mod tests;

/// The per-stage table, the failures, and the notes — in that order.
///
/// Without the trailing "Ran N stage(s)" summary, which callers word for
/// themselves: the CLI has a run that may have been recorded in history, and
/// the standalone runner does not.
pub fn report_lines(report: &RunReport) -> Vec<String> {
    let width = report
        .stages
        .iter()
        .map(|stage| stage.label.chars().count())
        .max()
        .unwrap_or(0);

    let mut lines = Vec::new();

    for stage in &report.stages {
        let rows = match (&stage.skipped, stage.rows) {
            // A stage that did not run says why, rather than showing a dash
            // that reads the same as "no counts were collected".
            (Some(reason), _) => reason.describe(),
            (None, Some(rows)) => format!("{rows} rows"),
            (None, None) => "-".to_string(),
        };

        // A quality node's rejected count is shown even when it is zero. Zero
        // rejects is the result someone ran the check to see, and hiding it
        // would make a passing check look like a node that did nothing.
        let rejected = match stage.rejected {
            Some(rejected) => format!("  {rejected} rejected"),
            None => String::new(),
        };

        // Most stages have no timing and must not be padded into a column of
        // blanks; the ones that do have earned it. See `StageOutcome::elapsed`
        // for which those are and why.
        let took = match stage.elapsed {
            Some(elapsed) => format!("  {:.0}ms", elapsed.as_secs_f64() * 1000.0),
            None => String::new(),
        };

        lines.push(format!(
            "  {:width$}  {:>12}  {}{}{}",
            stage.label, rows, stage.component_id, rejected, took
        ));
    }

    for failure in &report.failures {
        lines.push(format!(
            "  ! {} ({}): {}",
            failure.label, failure.node_id, failure.message
        ));
    }

    for note in &report.notes {
        lines.push(format!("  · {note}"));
    }

    for warning in &report.warnings {
        lines.push(format!("  ⚠ {warning}"));
    }

    lines
}
