use dolos::wal::redb::WalStore;
use miette::{bail, Context, IntoDiagnostic};

use crate::feedback::Feedback;

#[derive(Debug, clap::Args, Default)]
pub struct Args {}

fn open_empty_wal(config: &crate::Config) -> miette::Result<WalStore> {
    let wal = crate::common::open_wal(config)?;

    let is_empty = wal.is_empty().into_diagnostic()?;

    if !is_empty {
        bail!("can't continue with data already available");
    }

    Ok(wal)
}

pub fn run(config: &crate::Config, _args: &Args, _feedback: &Feedback) -> miette::Result<()> {
    let mut wal = open_empty_wal(config).context("opening WAL")?;

    wal.initialize_from_origin()
        .into_diagnostic()
        .context("initializing WAL")?;

    Ok(())
}
