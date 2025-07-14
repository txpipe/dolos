use miette::{Context, IntoDiagnostic};
use pallas::crypto::hash::Hash;
use std::str::FromStr;

use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// slot of the block to find
    #[arg(long)]
    slot: u64,

    /// hash of the block to find as a hex string
    #[arg(long)]
    hash: String,
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(config)?;

    let dolos::adapters::WalAdapter::Redb(wal) = crate::common::open_wal_store(config)?;

    let hash = Hash::from_str(&args.hash)
        .into_diagnostic()
        .context("error parsing hash")?;

    let point = ChainPoint::Specific(args.slot, hash);

    let seq = wal
        .find_intersect(&[point])
        .into_diagnostic()
        .context("finding intersect")?;

    match seq {
        Some((seq, _)) => println!("{seq}"),
        None => println!("can't find block"),
    };

    Ok(())
}
