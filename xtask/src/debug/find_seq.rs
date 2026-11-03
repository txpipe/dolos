use std::str::FromStr;

use anyhow::{Context, Result};
use dolos::storage::Access;
use dolos_core::{config::RootConfig, BlockSlot, ChainPoint, WalStore as _};
use pallas::crypto::hash::Hash;

use super::ConfigArgs;

#[derive(clap::Args)]
pub struct Args {
    #[command(flatten)]
    config: ConfigArgs,

    /// slot of the block to find
    #[arg(long)]
    slot: u64,

    /// hash of the block to find as a hex string
    #[arg(long)]
    hash: String,
}

pub fn run(args: &Args) -> Result<()> {
    let hash = Hash::from_str(&args.hash).context("error parsing hash")?;

    match find_seq(&args.config.load()?, args.slot, hash)? {
        Some(point) => println!("{point}"),
        None => println!("can't find block"),
    };

    Ok(())
}

/// The key of the WAL entry holding the block at `slot` with `hash`, if the
/// WAL holds it.
pub fn find_seq(
    config: &RootConfig,
    slot: BlockSlot,
    hash: Hash<32>,
) -> Result<Option<ChainPoint>> {
    let stores = super::open(config, Access::Observe)?;

    let found = stores
        .wal
        .find_intersect(&[ChainPoint::Specific(slot, hash)])
        .context("finding intersect")?;

    Ok(found.map(|(point, _)| point))
}
