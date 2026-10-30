use std::fmt;

use dolos::cli::{ArchiveSummary, DataSummary, StateSummary, WalSummary};
use dolos::storage::Access;
use dolos_core::{
    config::RootConfig,
    wal::{verify_integrity_with, IntegrityFailure, IntegrityReport},
    BlockHash, BlockSlot,
};
use miette::{Context, IntoDiagnostic};
use serde::Serialize;

use super::Positions;
use crate::feedback::Feedback;
use crate::output::{emit, FormatArgs, Unhealthy};

#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    format: FormatArgs,
}

/// What `sync status` found. The cursors keep the shape `data summary`
/// printed.
#[derive(Debug, Serialize)]
struct Status {
    #[serde(flatten)]
    cursors: DataSummary,
    archive_kept: bool,
    /// What keeps the stores from standing at the WAL tip.
    disagreements: Vec<String>,
    /// How far back `sync rollback` reaches; none while the WAL is empty.
    rollback_window: Option<RollbackWindow>,
    integrity: IntegrityReport,
    healthy: bool,
}

#[derive(Debug, Serialize)]
struct RollbackWindow {
    /// Where the WAL's unbroken history starts, the oldest point a rollback
    /// reaches: 0 when it runs from Origin.
    from_slot: BlockSlot,
    to_slot: BlockSlot,
    /// The blocks a rollback to `from_slot` undoes.
    blocks: u64,
}

impl Status {
    /// Why the status is unhealthy; none when it is not.
    fn problems(&self) -> Vec<String> {
        let mut found = Vec::new();

        if !self.disagreements.is_empty() {
            found.push("the stores disagree".to_string());
        }

        if !self.integrity.complete {
            found.push("the WAL check stopped at a block it cannot decode".to_string());
        } else if !self.integrity.failures.is_empty() {
            found.push(format!(
                "the WAL has {} block(s) that do not chain",
                self.integrity.failures.len()
            ));
        }

        found
    }
}

fn hash(hash: &Option<BlockHash>) -> String {
    hash.as_ref().map_or("none".to_string(), hex::encode)
}

fn slot(slot: Option<BlockSlot>) -> String {
    slot.map_or("empty".to_string(), |slot| format!("slot {slot}"))
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.cursors.wal.start_slot, self.cursors.wal.tip_slot) {
            (Some(start), Some(tip)) => writeln!(f, "wal        slot {start} to slot {tip}")?,
            _ => writeln!(f, "wal        empty")?,
        }

        writeln!(f, "state      {}", slot(self.cursors.state.tip_slot))?;

        if self.archive_kept {
            writeln!(f, "archive    {}", slot(self.cursors.archive.tip_slot))?;
        } else {
            writeln!(f, "archive    not kept")?;
        }

        match &self.rollback_window {
            Some(window) => writeln!(
                f,
                "rollback   back to slot {}, undoing up to {} block(s)",
                window.from_slot, window.blocks
            )?,
            None => writeln!(f, "rollback   none: the WAL is empty")?,
        }

        let scan = if self.integrity.complete {
            "complete"
        } else {
            "stopped"
        };
        writeln!(
            f,
            "integrity  {} block(s) checked, scan {scan}",
            self.integrity.blocks
        )?;

        for failure in &self.integrity.failures {
            match failure {
                IntegrityFailure::Discontinuity {
                    slot,
                    block_hash,
                    expected_prev,
                    got_prev,
                } => writeln!(
                    f,
                    "           discontinuity at slot {slot}: block {}, expected prev {}, got prev {}",
                    hash(block_hash),
                    hash(expected_prev),
                    hash(got_prev),
                )?,
                IntegrityFailure::Undecodable { slot, error } => {
                    writeln!(f, "           undecodable block at slot {slot}: {error}")?
                }
            }
        }

        if self.disagreements.is_empty() {
            writeln!(f, "agreement  the stores stand at the WAL tip")?;
        }

        for disagreement in &self.disagreements {
            writeln!(f, "agreement  {disagreement}")?;
        }

        write!(f, "{}", if self.healthy { "healthy" } else { "unhealthy" })
    }
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    crate::common::setup_tracing_warnings()?;

    let stores = crate::common::open_existing_stores(config, Access::Observe)?;
    let positions = Positions::read(config, &stores)?;

    let progress = feedback.slot_progress_bar();
    progress.set_message("checking the WAL");
    progress.set_length(positions.wal_tip.as_ref().map_or(0, |point| point.slot()));

    let integrity = verify_integrity_with(&stores.wal, |slot| progress.set_position(slot))
        .into_diagnostic()
        .context("checking the WAL")?;

    progress.finish_and_clear();
    drop(stores);

    let rollback_window = positions.wal_tip.as_ref().map(|tip| RollbackWindow {
        from_slot: positions.wal_history.unwrap_or(0),
        to_slot: tip.slot(),
        blocks: integrity.blocks,
    });

    let disagreements = positions.disagreements();
    let healthy = disagreements.is_empty() && integrity.is_intact();

    let status = Status {
        cursors: DataSummary {
            wal: WalSummary {
                start_slot: positions.wal_start.as_ref().map(|point| point.slot()),
                tip_slot: positions.wal_tip.as_ref().map(|point| point.slot()),
            },
            archive: ArchiveSummary {
                tip_slot: positions.archive,
            },
            state: StateSummary {
                tip_slot: positions.state.as_ref().map(|point| point.slot()),
            },
        },
        archive_kept: positions.archive_kept,
        disagreements,
        rollback_window,
        integrity,
        healthy,
    };

    emit(&status, &args.format)?;

    match status.problems() {
        problems if problems.is_empty() => Ok(()),
        problems => Err(Unhealthy(problems.join("; ")).into()),
    }
}
