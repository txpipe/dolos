use clap::{Parser, Subcommand};

use crate::feedback::Feedback;

mod rebuild_stores;
mod wal_integrity;

#[cfg(feature = "utils")]
mod reset_genesis;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// rebuilds ledger and chain from WAL
    RebuildStores(rebuild_stores::Args),
    /// checks the integrity of the WAL records
    WalIntegrity(wal_integrity::Args),

    #[cfg(feature = "utils")]
    /// resets the genesis files with well-known values
    ResetGenesis(reset_genesis::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &super::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    match &args.command {
        Command::RebuildStores(x) => rebuild_stores::run(config, x, feedback)?,
        Command::WalIntegrity(x) => wal_integrity::run(config, x)?,

        #[cfg(feature = "utils")]
        Command::ResetGenesis(x) => reset_genesis::run(config, x)?,
    }

    Ok(())
}

pub fn run_rebuild_stores(config: &super::Config, feedback: &Feedback) -> miette::Result<()> {
    rebuild_stores::run(config, &rebuild_stores::Args { chunk: 500 }, feedback)
}
