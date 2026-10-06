//! Repair the DRep sightings of a state store built before `DRepSeen`
//! existed.
//!
//! `/governance/dreps` lists every DRep any certificate ever referenced, in
//! the order of its first sighting. A store an older binary built recorded no
//! sighting: it has no row for a DRep that only ever appeared as a
//! vote-delegation target, and its other rows fall back to their lifecycle
//! stamps. A sync forward records sightings from then on, but never the ones
//! behind it — an existing instance needs this.
//!
//! The command replays the certificates of the archived Conway blocks, up to
//! the state cursor, through the same derivation the roll uses
//! (`DRepSightings`), and writes only the rows that differ: a new row per
//! missing DRep, and the earliest sighting per row that records none or a
//! later one. It reads the archive and writes the state store, and touches
//! nothing else, so it is a no-op on a store a fixed binary synced, and safe to
//! re-run.
//!
//! It refuses to run on an archive pruned past the first Conway block: the
//! first sightings it would need are gone, and a partial answer would freeze a
//! later one in place. Such an instance rebuilds its state from a snapshot.

use dolos_cardano::roll::dreps::DRepSightings;
use dolos_core::config::RootConfig;
use miette::Context as _;
use pallas::ledger::traverse::MultiEraBlock;

use dolos::adapters::DomainAdapter;
use dolos::prelude::*;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// write the repaired rows; without it the command only reports what it
    /// would write
    #[arg(long, action)]
    execute: bool,
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let state = crate::common::open_state_store(config)
        .map_err(|e| miette::miette!("{e}"))
        .context("opening the state store")?;

    let archive = crate::common::open_archive_store(config)
        .map_err(|e| miette::miette!("{e}"))
        .context("opening the archive store")?;

    let Some(cursor) = state
        .read_cursor()
        .map_err(|e| miette::miette!("{e}"))
        .context("reading the state cursor")?
    else {
        println!("this store has not synced anything yet; nothing to do");
        return Ok(());
    };

    let Some(start) = DRepSightings::scan_start::<DomainAdapter>(&state)
        .map_err(|e| miette::miette!("{e}"))
        .context("reading the era summary")?
    else {
        println!("this store has not reached conway yet; no certificate names a DRep");
        return Ok(());
    };

    let tip = cursor.slot();

    if start > tip {
        println!("this store has not reached conway yet; no certificate names a DRep");
        return Ok(());
    }

    let earliest = archive
        .get_range(None, None)
        .map_err(|e| miette::miette!("{e}"))
        .context("reading the archive")?
        .next()
        .map(|(slot, _)| slot);

    if earliest.is_none_or(|earliest| earliest > start) {
        miette::bail!(
            "the archive does not reach back to the first conway slot ({start}); the first \
             sightings it would need are gone, so rebuild this instance's state from a snapshot \
             instead"
        );
    }

    let progress = feedback.slot_progress_bar();
    progress.set_message("scanning conway certificates");
    progress.set_length(tip);
    progress.set_position(start);

    let blocks = archive
        .get_range(Some(start), Some(tip + 1))
        .map_err(|e| miette::miette!("{e}"))
        .context("reading the archive")?;

    let mut sightings = DRepSightings::default();

    for (slot, body) in blocks {
        let block = MultiEraBlock::decode(&body)
            .map_err(|e| miette::miette!("{e}"))
            .with_context(|| format!("decoding the block at slot {slot}"))?;

        sightings.visit_block(&block);
        progress.set_position(slot);
    }

    progress.finish_and_clear();

    let repairs = sightings
        .repairs::<DomainAdapter>(&state)
        .map_err(|e| miette::miette!("{e}"))
        .context("reading the dreps")?;

    println!("DREPS SIGHTED : {}", sightings.len());
    println!("ROWS TO WRITE : {}", repairs.len());

    if repairs.is_empty() {
        println!("nothing to write; this store is already repaired");
        return Ok(());
    }

    if !args.execute {
        println!("dry run; re-run with --execute to write them");
        return Ok(());
    }

    DRepSightings::apply::<DomainAdapter>(&state, &repairs)
        .map_err(|e| miette::miette!("{e}"))
        .context("writing the dreps")?;

    println!("wrote {} dreps", repairs.len());

    Ok(())
}
