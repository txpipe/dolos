use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use std::path::PathBuf;

use dolos::wal::{BlockSlot, LogSeq, WalReader, WalWriter};

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

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (source, _) = crate::common::open_data_stores(config).context("opening data stores")?;

    let mut target = dolos::wal::redb::WalStore::open(&args.output, None)
        .into_diagnostic()
        .context("opening target WAL")?;

    let since = match args.since {
        Some(slot) => source
            .approximate_slot(slot, 100)
            .into_diagnostic()
            .context("finding initial slot")?,
        None => None,
    };

    let until = match args.until {
        Some(slot) => source
            .approximate_slot(slot, 100)
            .into_diagnostic()
            .context("finding final slot")?,
        None => None,
    };

    let reader = match (since, until) {
        (Some(since), Some(until)) => source
            .crawl_range(since, until)
            .into_diagnostic()
            .context("crawling range")?,
        _ => source
            .crawl_from(since)
            .into_diagnostic()
            .context("crawling from")?,
    };

    for chunk in reader.chunks(100).into_iter() {
        let entries = chunk.map(|(_, v)| v);

        target
            .append_entries(entries)
            .into_diagnostic()
            .context("appending to target")?;
    }

    Ok(())
}
