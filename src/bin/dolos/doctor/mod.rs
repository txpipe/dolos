use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;

use crate::feedback::Feedback;

mod catchup_stores;
mod reset_wal;
mod rollback;
mod update_entity;
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

    /// rolls back the node to a specific slot and hash
    Rollback(rollback::Args),

    /// manually updates an entity in the state
    UpdateEntity(update_entity::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    match &args.command {
        Command::CatchupStores(x) => catchup_stores::run(config, x, feedback)?,
        Command::ResetWal(x) => reset_wal::run(config, x, feedback)?,
        Command::WalIntegrity(x) => wal_integrity::run(config, x)?,
        Command::Rollback(x) => rollback::run(config, x)?,
        Command::UpdateEntity(x) => update_entity::run(config, x)?,

        #[cfg(feature = "utils")]
        Command::ResetGenesis(x) => reset_genesis::run(config, x)?,
    }

    Ok(())
}
