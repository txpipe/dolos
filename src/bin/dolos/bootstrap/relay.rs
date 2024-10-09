use dolos::wal::redb::WalStore;
use miette::{bail, Context, IntoDiagnostic};
use tracing::info;

use crate::{common::storage_is_empty, feedback::Feedback};

#[derive(Debug, clap::Args, Default)]
pub struct Args {
    /// Skip the bootstrap if there's already data in the stores
    #[arg(long, action)]
    skip_if_not_empty: bool,
}

fn open_empty_wal(config: &crate::Config) -> miette::Result<WalStore> {
    let wal = crate::common::open_wal(config)?;

    let is_empty = wal.is_empty().into_diagnostic()?;

    if !is_empty {
        bail!("can't continue with data already available");
    }

    Ok(wal)
}

pub fn run(config: &crate::Config, args: &Args, _feedback: &Feedback) -> miette::Result<()> {
    if args.skip_if_not_empty && !storage_is_empty(config) {
        info!("Skipping bootstrap because storage is not empty.");
        return Ok(());
    }

    let mut wal = open_empty_wal(config).context("opening WAL")?;

    wal.initialize_from_origin()
        .into_diagnostic()
        .context("initializing WAL")?;

    Ok(())
}
