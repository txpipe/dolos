use clap::{Parser, Subcommand};

use crate::feedback::Feedback;

mod catchup_stores;
mod reset_wal;
mod wal_integrity;

#[cfg(feature = "utils")]
mod reset_genesis;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// catch up store data from WAL records
    CatchupStores(catchup_stores::Args),

    // Reset WAL position using state cursor
    ResetWal(reset_wal::Args),

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
        Command::CatchupStores(x) => catchup_stores::run(config, x, feedback)?,
        Command::ResetWal(x) => reset_wal::run(config, x, feedback)?,
        Command::WalIntegrity(x) => wal_integrity::run(config, x)?,

        #[cfg(feature = "utils")]
        Command::ResetGenesis(x) => reset_genesis::run(config, x)?,
    }

    Ok(())
}
