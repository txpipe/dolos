use dolos_core::config::{RedbWalConfig, RootConfig};
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use std::path::PathBuf;

use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
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

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let source = crate::common::open_wal_store(config)?;

    let target = dolos_redb3::wal::RedbWalStore::open(&args.output, &RedbWalConfig::default())
        .into_diagnostic()
        .context("opening target WAL")?;

    let since = match args.since {
        Some(slot) => source
            .locate_point(slot)
            .into_diagnostic()
            .context("finding initial slot")?,
        None => None,
    };

    let until = match args.until {
        Some(slot) => source
            .locate_point(slot)
            .into_diagnostic()
            .context("finding final slot")?,
        None => None,
    };

    let reader = source
        .iter_logs(since, until)
        .into_diagnostic()
        .context("iterating over logs")?;

    for chunk in reader.chunks(100).into_iter() {
        let entries: Vec<_> = chunk.collect();

        target
            .append_entries(&entries)
            .into_diagnostic()
            .context("appending to target")?;
    }

    Ok(())
}
