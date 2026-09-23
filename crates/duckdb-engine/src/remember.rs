//! What a workspace remembers, applied to one run of one pipeline.
//!
//! Two halves, used by every caller that runs a pipeline for real -- `etl run`,
//! the scheduler, the console and a built artifact -- so that none of them can
//! drift from the others on the one rule that matters: **state advances only
//! after a run that fully succeeded.**
//!
//! - [`compile_options`] turns stored state into what `compile_with` needs:
//!   each incremental source's watermark, and each native source's checkpoint.
//!   A stored value that no longer fits its node is dropped, with a warning to
//!   say so, and that node reads from its start.
//! - [`remember`] folds a finished run's report into the stored state.
//!
//! Neither reads or writes a file. The callers own the store, because they
//! differ in where it is and in how loudly a failure to save should be said.

use crate::{CompileOptions, RunReport};
use etl_metadata::PipelineDoc;
use etl_state::PipelineState;

#[cfg(test)]
mod tests;

/// What stored state means for compiling `document`.
#[derive(Debug, Clone, Default)]
pub struct Remembering {
    pub options: CompileOptions,
    /// Stored values that were set aside, one sentence each, for the caller to
    /// print as warnings.
    pub warnings: Vec<String>,
}

/// The watermarks and checkpoints in `stored` that still apply to `document`.
///
/// A node whose incremental column has changed since its watermark was taken,
/// or whose component has changed since its checkpoint was saved, starts over:
/// comparing next run's `order_id` against last run's `order_ts`, or handing a
/// connector a position in some other connector's terms, would be silently
/// wrong. Whether a checkpoint still fits a node's *properties* -- Kafka's
/// topic, say -- is the connector's to judge, since only it can read one.
pub fn compile_options(document: &PipelineDoc, stored: &PipelineState) -> Remembering {
    let mut remembering = Remembering::default();

    for node in &document.nodes {
        if let (Some(declared), Some(watermark)) =
            (node.data.incremental.as_ref(), stored.watermark(&node.id))
        {
            if watermark.matches_column(&declared.column) {
                remembering
                    .options
                    .watermarks
                    .insert(node.id.clone(), watermark.value.clone());
            } else {
                remembering.warnings.push(format!(
                    "'{}' now watches '{}' but its watermark was taken from '{}'; reading from \
                     the start",
                    node.id, declared.column, watermark.column
                ));
            }
        }

        if let Some(checkpoint) = stored.checkpoint(&node.id) {
            let component = node.data.component_id.as_deref().unwrap_or_default();
            if checkpoint.component == component {
                remembering
                    .options
                    .checkpoints
                    .insert(node.id.clone(), checkpoint.value.clone());
            } else {
                remembering.warnings.push(format!(
                    "'{}' is now '{component}' but its saved position came from '{}'; reading \
                     from the start",
                    node.id, checkpoint.component
                ));
            }
        }
    }

    remembering
}

/// What [`remember`] changed, for the caller to report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Remembered {
    /// Node id and the watermark it moved to.
    pub advanced: Vec<(String, String)>,
    /// Incremental sources that loaded nothing, and so kept the mark they had:
    /// the ordinary state of an incremental pipeline with nothing new to do.
    pub nothing_new: Vec<String>,
    /// Native sources whose position was saved.
    pub positions: Vec<String>,
}

impl Remembered {
    /// Whether the stored state changed and needs writing.
    pub fn changed(&self) -> bool {
        !self.advanced.is_empty() || !self.positions.is_empty()
    }
}

/// Fold a finished run into `stored`.
///
/// **A failed run changes nothing**, checked here as well as by the callers:
/// a report from `continue_on_failure` reaches this far, and saving from it
/// would skip whatever the failed run never finished with. The engine already
/// leaves a failed report's checkpoints empty; this does not rely on it.
pub fn remember(report: &RunReport, stored: &mut PipelineState) -> Remembered {
    let mut remembered = Remembered::default();

    if report.failed() {
        return remembered;
    }

    for watermark in &report.watermarks {
        match &watermark.value {
            Some(value) => {
                stored.advance(&watermark.node_id, &watermark.column, value.clone());
                remembered
                    .advanced
                    .push((watermark.node_id.clone(), value.clone()));
            }
            None => remembered.nothing_new.push(watermark.node_id.clone()),
        }
    }

    // Saved even when it did not move: the connector returned it, so it is
    // where this run's read ended under this configuration.
    for checkpoint in &report.checkpoints {
        stored.record_checkpoint(
            &checkpoint.node_id,
            &checkpoint.component_id,
            checkpoint.value.clone(),
        );
        remembered.positions.push(checkpoint.node_id.clone());
    }

    remembered
}
