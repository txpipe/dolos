//! Build the slot-ordered live-UTxO tags on a store that predates them.
//!
//! A store this binary built — from genesis, a stele or `doctor
//! rebuild-state` — positions every UTxO as it enters the set, and the routes
//! that page by chain position (`/assets/{asset}/utxos`) rely on that. A store
//! an earlier binary built carries the plain tags only and reports itself
//! unpositioned (`StateStore::utxos_positioned`); so does one a binary
//! without the ordered tags wrote to since. This repair builds the positions
//! for such a store from what it has: the UTxO set, and the archive's exact
//! index for the producing slot of each.
//!
//! What it does, in order:
//!
//! - clears whatever positions the store holds, so a rebuild cut short leaves a
//!   store that still reports itself unpositioned rather than half so;
//! - walks the UTxO set once for the refs, then positions them in chunks: the
//!   tags come from the bodies, the slot from the archive, and both go through
//!   the same state writer the sync uses, one commit per chunk;
//! - marks the store positioned in its last commit.
//!
//! A UTxO whose producing block the archive no longer holds — pruned by
//! `sync.max_history`, or never there on a ledger-only node — gets no position.
//! A node that synced it keeps one regardless, since the roll positions from
//! the batch and nothing prunes the positions; the difference is not visible,
//! because the routes that page by chain position start at the oldest block
//! held and serve such a UTxO on neither node. The refs are held in memory
//! between the two passes, about 40 bytes per live UTxO, so no state-store
//! iterator is open while the store is written.
//!
//! It refuses to run while the WAL is ahead of the state: a WAL entry written
//! before the positions existed names no slot for the inputs it consumes, so
//! replaying it after this repair would leave their ordered entries behind.
//! `dolos daemon` catches the state up on start; run this once it has.

use dolos_cardano::indexes::utxo_index_delta_with_slots;
use dolos_core::{
    archive::position_utxos, config::RootConfig, StateStore as _, StateWriter as _, TxoRef,
    UtxoSetDelta, WalStore as _,
};
use miette::{Context as _, IntoDiagnostic as _};

use dolos::adapters::ArchiveStoreBackend;

use crate::feedback::Feedback;

/// UTxOs positioned per state commit.
const CHUNK: usize = 10_000;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// rebuild the positions of a store that already reports itself
    /// positioned
    #[arg(long, action)]
    force: bool,
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    // errors only, like `rebuild-state`: the progress bar and the summary
    // below are the command's output
    crate::common::setup_tracing_error_only()?;

    let state = crate::common::open_state_store(config)
        .map_err(|e| miette::miette!("{e}"))
        .context("opening the state store")?;
    let archive = crate::common::open_archive_store(config)
        .map_err(|e| miette::miette!("{e}"))
        .context("opening the archive store")?;
    let wal = crate::common::open_wal_store(config)
        .map_err(|e| miette::miette!("{e}"))
        .context("opening the wal")?;

    let cursor = state
        .read_cursor()
        .into_diagnostic()
        .context("reading the state cursor")?;

    let Some(cursor) = cursor else {
        miette::bail!("the state store has no cursor; bootstrap the node first");
    };

    let wal_tip = wal
        .find_tip()
        .into_diagnostic()
        .context("reading the wal tip")?
        .map(|(point, _)| point);

    if let Some(tip) = wal_tip {
        if tip.slot() > cursor.slot() {
            miette::bail!(
                help = "start `dolos daemon` once so it catches the state up, then run this again",
                "the wal is ahead of the state ({tip} > {cursor}); the entries still to replay \
                 would leave their inputs' positions behind"
            );
        }
    }

    if matches!(archive, ArchiveStoreBackend::NoOp(_)) {
        println!(
            "the archive is `no_op`: no utxo has a block to be positioned by, so none will be"
        );
    }

    let positioned = state
        .utxos_positioned()
        .into_diagnostic()
        .context("reading whether the utxos are positioned")?;

    if positioned && !args.force {
        println!("this store is already positioned; re-run with --force to rebuild its positions");
        return Ok(());
    }

    state
        .clear_utxo_positions()
        .into_diagnostic()
        .context("clearing the positions")?;

    let refs: Vec<TxoRef> = state
        .iter_utxos()
        .into_diagnostic()
        .context("iterating the utxo set")?
        .map(|entry| entry.map(|(txo, _)| txo))
        .collect::<Result<_, _>>()
        .into_diagnostic()
        .context("iterating the utxo set")?;

    let progress = feedback.count_progress_bar("utxos");
    progress.set_length(refs.len() as u64);
    progress.set_message("positioning");

    let mut positioned = 0usize;

    for chunk in refs.chunks(CHUNK) {
        let bodies = state
            .get_utxos(chunk.to_vec())
            .into_diagnostic()
            .context("reading a chunk of utxos")?;

        let slots = position_utxos(&archive, bodies.keys())
            .into_diagnostic()
            .context("looking up the producing slots")?;

        positioned += slots.len();

        let delta = UtxoSetDelta {
            produced_utxo: bodies,
            ..Default::default()
        };

        let writer = state.start_writer().into_diagnostic()?;
        writer
            .apply_utxo_tags(&utxo_index_delta_with_slots(&delta, &slots))
            .into_diagnostic()
            .context("writing a chunk of positions")?;
        writer.commit().into_diagnostic()?;

        progress.inc(chunk.len() as u64);
    }

    let writer = state.start_writer().into_diagnostic()?;
    writer.mark_utxos_positioned().into_diagnostic()?;
    writer
        .commit()
        .into_diagnostic()
        .context("marking the store positioned")?;

    progress.finish_and_clear();

    println!(
        "positioned {positioned} of {} live utxos; the rest have no block the archive holds",
        refs.len()
    );

    Ok(())
}
