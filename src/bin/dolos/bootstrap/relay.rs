use miette::{bail, Context, IntoDiagnostic};

use dolos::prelude::*;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args, Default, Clone)]
pub struct Args {}

fn open_empty_wal(config: &crate::Config) -> miette::Result<dolos_redb::wal::RedbWalStore> {
    let dolos::adapters::WalAdapter::Redb(wal) = crate::common::open_wal_store(config)?;

    let is_empty = wal.is_empty().map_err(WalError::from).into_diagnostic()?;

    if !is_empty {
        bail!("can't continue with data already available");
    }

    Ok(wal)
}

pub fn run(config: &crate::Config, _args: &Args, _feedback: &Feedback) -> miette::Result<()> {
    let wal = open_empty_wal(config).context("opening WAL")?;

    wal.initialize_from_origin()
        .map_err(WalError::from)
        .into_diagnostic()
        .context("initializing WAL")?;

    println!("Data initialized to sync from origin");

    Ok(())
}
