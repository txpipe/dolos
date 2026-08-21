//! Repair a store built before the Shelley→Allegra AVVM reclamation deleted
//! its UTxOs.
//!
//! Every mainnet store any released dolos built carries the unredeemed Byron
//! AVVM outputs the real chain destroyed in December 2020: the boundary moved
//! their value from the `utxos` pot to `reserves` and left the rows in place.
//! The fixed binary does both halves, but only for a store synced from genesis
//! after the fix — an existing instance needs this.
//!
//! What it does and does not touch is the whole of its correctness:
//!
//! - It deletes the still-unspent AVVM refs from the state store and drops them
//!   from the UTxO filter indexes.
//! - It leaves the **pots alone**. They already had the reclamation applied;
//!   adjusting them here would break the half that is currently right and turn
//!   a one-sided overcount into an inconsistency nothing checks.
//! - It derives the ref set from the Byron genesis on every run, through the
//!   same `AvvmReclamation::genesis_refs` the boundary uses. A recorded census
//!   is a record of one tip, never an input.
//!
//! It is a no-op on a store the fixed binary built (nothing left unspent), a
//! no-op on preprod and preview (their `avvmDistr` is empty), and refuses to
//! run on a store that has not yet crossed the boundary — there the reclamation
//! is still ahead of it and deleting anything would pre-empt the ledger.

use dolos_cardano::estart::AvvmReclamation;
use dolos_core::config::RootConfig;
use miette::Context as _;
use pallas::ledger::traverse::MultiEraOutput;

use dolos::adapters::{storage, DomainAdapter};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// apply the deletion; without it the command only reports what it would
    /// delete
    #[arg(long, action)]
    execute: bool,

    /// print every ref it would delete, one `{tx_hash}#{index} {lovelace}`
    /// per line
    #[arg(long, action)]
    list: bool,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    // State and indexes only: the repair touches neither the WAL nor the
    // archive, and on a mainnet instance those are the expensive opens.
    let state = crate::common::open_state_store(config)
        .map_err(|e| miette::miette!("{e}"))
        .context("opening the state store")?;
    let indexes = storage::open_index_store(config)
        .map_err(|e| miette::miette!("{e}"))
        .context("opening the index store")?;

    let derived = AvvmReclamation::genesis_refs(&genesis).len();

    if derived == 0 {
        println!("this network's byron genesis distributes nothing through AVVM; nothing to do");
        return Ok(());
    }

    let crossed = AvvmReclamation::boundary_crossed::<DomainAdapter>(&state)
        .map_err(|e| miette::miette!("{e}"))
        .context("reading the shelley era summary")?;

    if !crossed {
        miette::bail!(
            "this store has not crossed the shelley→allegra boundary yet; the reclamation is \
             still ahead of it and deleting its utxos now would pre-empt the ledger"
        );
    }

    let reclamation = AvvmReclamation::unredeemed::<DomainAdapter>(&state, &genesis)
        .map_err(|e| miette::miette!("{e}"))
        .context("reading the unredeemed AVVM utxos")?;

    println!("AVVM REFS DERIVED : {derived}");
    println!("STILL UNSPENT     : {}", reclamation.utxos.len());
    println!("LOVELACE PRESENT  : {}", reclamation.total);

    if args.list {
        // Sorted, so the output diffs cleanly against a recorded census.
        let mut listed: Vec<String> = reclamation
            .utxos
            .iter()
            .map(|(txo, body)| {
                let lovelace = MultiEraOutput::try_from(body.as_ref())
                    .map(|x| x.value().coin())
                    .unwrap_or_default();

                format!("{}#{} {lovelace}", txo.0, txo.1)
            })
            .collect();

        listed.sort();

        for line in listed {
            println!("{line}");
        }
    }

    if reclamation.is_empty() {
        println!("nothing to delete; this store is already repaired");
        return Ok(());
    }

    if !args.execute {
        println!("dry run; re-run with --execute to delete them");
        return Ok(());
    }

    reclamation
        .apply_deletion::<DomainAdapter>(&state, &indexes)
        .map_err(|e| miette::miette!("{e}"))
        .context("deleting the AVVM utxos")?;

    println!(
        "deleted {} utxos; the pots were left alone",
        reclamation.utxos.len()
    );

    Ok(())
}
