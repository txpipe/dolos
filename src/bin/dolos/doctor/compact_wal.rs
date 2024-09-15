use dolos::wal::{ChainPoint, WalReader as _};
use miette::{Context, IntoDiagnostic};

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &crate::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (mut wal, _) = crate::common::open_data_stores(config).context("opening data stores")?;

    let (_, tip) = wal
        .find_tip()
        .into_diagnostic()
        .context("finding tip")?
        .ok_or(miette::miette!("no tip"))?;

    let tip = match tip {
        ChainPoint::Origin => 0,
        ChainPoint::Specific(slot, _) => slot,
    };

    println!("tip is slot {}", tip);

    // TODO: make this configurable
    let min_slot = tip - 1000;

    println!("removing blocks before slot {}", min_slot);

    wal.remove_before(min_slot)
        .into_diagnostic()
        .context("removing range from WAL")?;

    let db = wal.db_mut().unwrap();

    while db.compact().into_diagnostic()? {
        println!("wal compaction round");
    }

    println!("wal segment trimmed");

    Ok(())
}
