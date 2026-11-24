//! `sync`: pull the chain from the upstream peer, and inspect, recover and
//! roll back the WAL it writes.

use clap::{Parser, Subcommand};
use dolos_core::{
    config::{RootConfig, StoreType},
    BlockSlot, ChainPoint,
};
use miette::{Context, IntoDiagnostic};

use dolos::prelude::*;

use crate::common::Stores;
use crate::feedback::Feedback;

mod catchup;
#[cfg(feature = "utils")]
mod dump;
mod recover;
mod rollback;
mod status;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Pull blocks from the upstream peer up to its tip, or on with `--follow`
    Catchup(catchup::Args),

    /// Where the WAL, the state and the archive stand, and whether the WAL's
    /// blocks chain; reads only
    Status(status::Args),

    /// Bring the state and the archive up to the WAL tip, as a crash left them
    Recover(recover::Args),

    /// Print the WAL's entries
    #[cfg(feature = "utils")]
    Dump(dump::Args),

    /// Roll the WAL, the state and the archive back to a block the WAL holds
    Rollback(rollback::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    match &args.command {
        Command::Catchup(x) => catchup::run(config, x),
        Command::Status(x) => status::run(config, x, feedback),
        Command::Recover(x) => recover::run(config, x),
        #[cfg(feature = "utils")]
        Command::Dump(x) => dump::run(config, x),
        Command::Rollback(x) => rollback::run(config, x),
    }
}

/// Whether the dataset keeps an archive between runs: `history.mode` keeps
/// one, on a store that is not ephemeral. An ephemeral archive holds only
/// what the running node refilled from the WAL, which no other process sees.
fn archive_kept(config: &RootConfig) -> bool {
    config.keeps_archive() && config.storage.archive.kind != StoreType::Ephemeral
}

/// Where each store stands, as read without changing any.
#[derive(Debug, Clone)]
struct Positions {
    wal_start: Option<ChainPoint>,
    wal_tip: Option<ChainPoint>,
    /// The slot from which the WAL holds every block: a reset point's, or the
    /// first block's in a pruned WAL; none when it holds every block from
    /// Origin, or nothing.
    wal_history: Option<BlockSlot>,
    /// The oldest point a rollback reaches; none while the WAL holds nothing
    /// past Origin.
    rollback_floor: Option<RollbackFloor>,
    state: Option<ChainPoint>,
    /// The blocks the WAL holds after another at their slot, the main block
    /// of a Byron boundary pair, with how many of the slot's blocks each
    /// applies; read at the WAL tip's and the state's slots.
    stacked: Vec<(ChainPoint, usize)>,
    /// The archive's tip slot: `None` while it is empty, or not kept.
    archive: Option<BlockSlot>,
    /// How many blocks the archive holds at its tip slot.
    archive_blocks: usize,
    /// Whether the dataset keeps an archive; see [`archive_kept`].
    archive_kept: bool,
    /// Whether the archive keeps only the latest blocks
    /// (`history.mode = "rolling"`).
    archive_rolling: bool,
}

/// The first WAL entry past Origin, the oldest point a rollback reaches.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RollbackFloor {
    point: ChainPoint,
    /// Whether the entry holds a block, which a rollback to it keeps; a reset
    /// point holds none.
    is_block: bool,
}

impl Positions {
    fn read(config: &RootConfig, stores: &Stores) -> miette::Result<Self> {
        let wal_start = stores
            .wal
            .find_start()
            .into_diagnostic()
            .context("reading the WAL start")?
            .map(|(point, _)| point);

        let mut wal_tip = stores
            .wal
            .find_tip()
            .into_diagnostic()
            .context("reading the WAL tip")?
            .map(|(point, _)| point);

        let (wal_history, rollback_floor) = history_start(&stores.wal)
            .into_diagnostic()
            .context("reading the WAL start")?;

        let state = stores
            .state
            .read_cursor()
            .into_diagnostic()
            .context("reading the state cursor")?;

        let archive = stores
            .archive
            .get_tip()
            .into_diagnostic()
            .context("reading the archive tip")?
            .map(|(slot, _)| slot);

        let archive_blocks = match archive {
            Some(slot) => stores
                .archive
                .get_blocks_by_slot(&slot)
                .into_diagnostic()
                .context("reading the archive tip")?
                .len(),
            None => 0,
        };

        let slots: std::collections::BTreeSet<_> = [&wal_tip, &state]
            .into_iter()
            .flatten()
            .map(|point| point.slot())
            .collect();

        let mut stacked = Vec::new();
        for slot in slots {
            let chain = dolos_core::wal::chain_at_slot(&stores.wal, slot)
                .into_diagnostic()
                .context("reading the WAL")?;

            // Keys sort a slot's entries by hash, so the last key there can be
            // a boundary block the chain puts first.
            if chain.len() > 1 && wal_tip.as_ref().is_some_and(|tip| tip.slot() == slot) {
                wal_tip = chain.last().cloned();
            }

            stacked.extend(
                chain
                    .into_iter()
                    .enumerate()
                    .skip(1)
                    .map(|(at, point)| (point, at + 1)),
            );
        }

        Ok(Self {
            wal_start,
            wal_tip,
            wal_history,
            rollback_floor,
            state,
            stacked,
            archive,
            archive_blocks,
            archive_kept: archive_kept(config),
            archive_rolling: config.history.max_slots().is_some(),
        })
    }

    /// Whether the archive is a kept rolling one, left empty: it keeps only
    /// the latest blocks, so it is whole again with whatever blocks reach it
    /// next, replayed from the WAL or synced, as after a bootstrap.
    fn archive_refills(&self) -> bool {
        self.archive_kept && self.archive_rolling && self.archive.is_none()
    }

    /// How far the WAL tip has applied the chain.
    fn wal(&self) -> Option<Applied> {
        self.applied(self.wal_tip.as_ref())
    }

    /// How far the state has applied the chain.
    fn state(&self) -> Option<Applied> {
        self.applied(self.state.as_ref())
    }

    /// How far the archive has applied the chain: through the blocks at its
    /// tip.
    fn archive(&self) -> Option<Applied> {
        self.archive.map(|slot| Applied {
            slot,
            blocks: self.archive_blocks.max(1),
        })
    }

    /// [`applied`], counting the blocks before `point` at its slot.
    fn applied(&self, point: Option<&ChainPoint>) -> Option<Applied> {
        let mut at = applied(point)?;

        if let Some((_, blocks)) = self.stacked.iter().find(|(held, _)| Some(held) == point) {
            at.blocks = *blocks;
        }

        Some(at)
    }

    /// What keeps the state, and the archive when it is kept, from standing at
    /// the WAL tip; nothing when they do.
    fn disagreements(&self) -> Vec<String> {
        let wal = self.wal();
        let mut found = Vec::new();

        if self.state() != wal {
            found.push(relative_to_wal("state", self.state(), wal));
        }

        if self.archive_kept && self.archive() != wal {
            found.push(relative_to_wal("archive", self.archive(), wal));
        }

        found
    }

    /// Whether the archive is the one store away from the WAL tip, and ahead
    /// of it: only a sync past the archive tip brings the others to it.
    fn only_archive_ahead(&self) -> bool {
        self.state() == self.wal() && self.archive_kept && self.archive() > self.wal()
    }
}

/// How far a store has applied the chain, ordered as the chain is: through
/// the blocks at `slot`, or, for a cursor that holds only a slot (ESTART's,
/// at an epoch's first block), up to them and not through them. A Byron
/// boundary block and the main block chained onto it share a slot, and the
/// main block comes second.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Applied {
    slot: BlockSlot,
    /// How many of the blocks at `slot` are applied.
    blocks: usize,
}

impl std::fmt::Display for Applied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.blocks {
            0 => write!(f, "slot {} (its block not applied)", self.slot),
            1 => write!(f, "slot {}", self.slot),
            n => write!(f, "slot {} (its first {n} blocks applied)", self.slot),
        }
    }
}

/// `at`, or `empty` for a store that has applied nothing.
fn position(at: Option<Applied>) -> String {
    at.map_or("empty".to_string(), |at| at.to_string())
}

/// Where the WAL's unbroken history starts (see [`Positions::wal_history`]),
/// and the oldest point a rollback reaches. `reset_to` keeps only Origin
/// before the entry it writes, so a reset point is the first entry past it.
fn history_start<W: WalStore>(
    wal: &W,
) -> Result<(Option<BlockSlot>, Option<RollbackFloor>), WalError> {
    let mut entries = wal.iter_blocks(None, None)?;
    let mut first = entries.next();

    let from_origin = matches!(first, Some((ChainPoint::Origin, _)));

    if from_origin {
        first = entries.next();
    }

    let history = match &first {
        Some((point, block)) if block.is_empty() => Some(point.slot()),
        Some((point, _)) if !from_origin => Some(point.slot()),
        _ => None,
    };

    let floor = first.map(|(point, block)| RollbackFloor {
        point,
        is_block: !block.is_empty(),
    });

    Ok((history, floor))
}

/// How far a store at `point` has applied the chain: none for no position, or
/// for Origin, where nothing is applied yet.
fn applied(point: Option<&ChainPoint>) -> Option<Applied> {
    match point {
        None | Some(ChainPoint::Origin) => None,
        Some(point) => Some(Applied {
            slot: point.slot(),
            blocks: usize::from(point.is_fully_defined()),
        }),
    }
}

/// `store`, at `at`, said against the WAL tip at `wal`.
fn relative_to_wal(store: &str, at: Option<Applied>, wal: Option<Applied>) -> String {
    let store = match at {
        Some(at) => format!("the {store}, at {at},"),
        None => format!("the {store}, empty,"),
    };

    let relation = if at < wal { "behind" } else { "ahead of" };

    match wal {
        Some(wal) => format!("{store} is {relation} the WAL tip at {wal}"),
        None => format!("{store} is {relation} the WAL, which is empty"),
    }
}
