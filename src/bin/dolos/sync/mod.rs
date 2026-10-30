//! `sync`: pull the chain from the upstream peer, and inspect, recover and
//! roll back the WAL it writes.

use clap::{Parser, Subcommand};
use dolos_core::{config::RootConfig, BlockSlot, ChainPoint};
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

/// Where each store stands, as read without changing any.
#[derive(Debug, Clone)]
struct Positions {
    wal_start: Option<ChainPoint>,
    wal_tip: Option<ChainPoint>,
    /// The slot from which the WAL holds every block: a reset point's, or the
    /// first block's in a pruned WAL; none when it holds every block from
    /// Origin, or nothing.
    wal_history: Option<BlockSlot>,
    state: Option<ChainPoint>,
    /// The archive's tip slot: `None` while it is empty, or not kept.
    archive: Option<BlockSlot>,
    /// Whether `history.mode` keeps an archive at all.
    archive_kept: bool,
}

impl Positions {
    fn read(config: &RootConfig, stores: &Stores) -> miette::Result<Self> {
        let wal_start = stores
            .wal
            .find_start()
            .into_diagnostic()
            .context("reading the WAL start")?
            .map(|(point, _)| point);

        let wal_tip = stores
            .wal
            .find_tip()
            .into_diagnostic()
            .context("reading the WAL tip")?
            .map(|(point, _)| point);

        let wal_history = history_start(&stores.wal)
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

        Ok(Self {
            wal_start,
            wal_tip,
            wal_history,
            state,
            archive,
            archive_kept: config.keeps_archive(),
        })
    }

    /// The slot the WAL tip has applied the chain through.
    fn wal(&self) -> Option<BlockSlot> {
        applied(self.wal_tip.as_ref())
    }

    /// The slot the state has applied the chain through.
    fn state(&self) -> Option<BlockSlot> {
        applied(self.state.as_ref())
    }

    /// What keeps the state, and the archive when it is kept, from standing at
    /// the WAL tip; nothing when they do.
    fn disagreements(&self) -> Vec<String> {
        let wal = self.wal();
        let mut found = Vec::new();

        if self.state() != wal {
            found.push(relative_to_wal("state", self.state(), wal));
        }

        if self.archive_kept && self.archive != wal {
            found.push(relative_to_wal("archive", self.archive, wal));
        }

        found
    }
}

/// Where the WAL's unbroken history starts. `reset_to` keeps only Origin
/// before the entry it writes, so a reset point is the first entry past it.
fn history_start<W: WalStore>(wal: &W) -> Result<Option<BlockSlot>, WalError> {
    let mut entries = wal.iter_blocks(None, None)?;
    let mut first = entries.next();

    let from_origin = matches!(first, Some((ChainPoint::Origin, _)));

    if from_origin {
        first = entries.next();
    }

    Ok(match first {
        Some((point, block)) if block.is_empty() => Some(point.slot()),
        Some((point, _)) if !from_origin => Some(point.slot()),
        _ => None,
    })
}

/// The slot a store has applied the chain through: none for no position, or
/// for Origin, where nothing is applied yet.
fn applied(point: Option<&ChainPoint>) -> Option<BlockSlot> {
    match point {
        None | Some(ChainPoint::Origin) => None,
        Some(point) => Some(point.slot()),
    }
}

/// `store`, at `at`, said against the WAL tip at `wal`.
fn relative_to_wal(store: &str, at: Option<BlockSlot>, wal: Option<BlockSlot>) -> String {
    let store = match at {
        Some(slot) => format!("the {store}, at slot {slot},"),
        None => format!("the {store}, empty,"),
    };

    let relation = if at < wal { "behind" } else { "ahead of" };

    match wal {
        Some(slot) => format!("{store} is {relation} the WAL tip at slot {slot}"),
        None => format!("{store} is {relation} the WAL, which is empty"),
    }
}
