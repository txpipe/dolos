use miette::{bail, Context, IntoDiagnostic};
use tracing::debug;

use crate::common::open_data_stores;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// number of slots back from tip to insert into WAL
    #[arg(long)]
    slot_retrace: u64,
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    let (mut wal, chain, _) = open_data_stores(config).context("opening data stores")?;

    let (tip, _) = chain
        .find_tip()
        .into_diagnostic()
        .context("finding chain tip")?
        .ok_or(miette::miette!("chain is empty"))?;

    if tip < args.slot_retrace {
        bail!("slot retrace is larger than available chain");
    }

    // TODO: apply real formula for volatile safe margin
    let start = tip - args.slot_retrace;

    let volatile = chain.crawl_after(Some(start));

    for block in volatile {
        let (slot, hash) = block.into_diagnostic()?;

        debug!(slot, "filling up wal");

        let body = chain
            .get_block(hash)
            .into_diagnostic()?
            .ok_or(miette::miette!("block not found"))?;

        wal.roll_forward(slot, hash, body).into_diagnostic()?;
    }

    Ok(())
}
