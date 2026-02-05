use dolos_core::config::RootConfig;
use miette::{bail, Context, IntoDiagnostic};

use crate::feedback::Feedback;

#[derive(Debug, clap::Args, Default, Clone)]
pub struct Args {}

fn ensure_empty_wal(config: &RootConfig) -> miette::Result<()> {
    let wal = crate::common::open_wal_store(config)?;

    let is_empty = wal.is_empty().into_diagnostic()?;

    if !is_empty {
        bail!("can't continue with data already available");
    }

    Ok(())
}

pub fn run(config: &RootConfig, _args: &Args, _feedback: &Feedback) -> miette::Result<()> {
    ensure_empty_wal(config).context("opening WAL")?;

    println!("Data initialized to sync from origin");

    Ok(())
}
