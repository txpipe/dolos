use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use dolos::storage::{Access, WalStoreBackend};
use dolos_core::{
    config::{RedbWalConfig, RootConfig},
    BlockSlot, WalStore as _,
};
use itertools::Itertools;

use super::ConfigArgs;

#[derive(clap::Args)]
pub struct Args {
    #[command(flatten)]
    config: ConfigArgs,

    /// path to the new WAL db to create
    #[arg(long)]
    output: PathBuf,

    /// start copying from this slot
    #[arg(long)]
    since: Option<BlockSlot>,

    /// stop copying at this slot
    #[arg(long)]
    until: Option<BlockSlot>,
}

pub fn run(args: &Args) -> Result<()> {
    copy_wal(&args.config.load()?, &args.output, args.since, args.until)
}

/// Append the node's WAL entries from `since` to `until` to the WAL at
/// `output`, creating it if it is not there.
pub fn copy_wal(
    config: &RootConfig,
    output: &Path,
    since: Option<BlockSlot>,
    until: Option<BlockSlot>,
) -> Result<()> {
    let source = super::open(config, Access::Observe)?.wal;

    let target = WalStoreBackend::<dolos_cardano::CardanoDelta>::open_redb(
        output,
        &RedbWalConfig::default(),
    )
    .context("opening target WAL")?;

    let since = match since {
        Some(slot) => source.locate_point(slot).context("finding initial slot")?,
        None => None,
    };

    let until = match until {
        Some(slot) => source.locate_point(slot).context("finding final slot")?,
        None => None,
    };

    let reader = source
        .iter_logs(since, until)
        .context("iterating over logs")?;

    for chunk in reader.chunks(100).into_iter() {
        target
            .append_entries(chunk.collect())
            .context("appending to target")?;
    }

    Ok(())
}
