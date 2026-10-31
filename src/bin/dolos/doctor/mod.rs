use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;

use crate::feedback::Feedback;

mod catchup_stores;
mod check;
mod rebuild_state;
mod reset_wal;
mod rollback;
mod wal_integrity;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// validates configuration and data by loading the domain
    Check(check::Args),

    /// catch up store data from WAL records
    CatchupStores(catchup_stores::Args),

    /// rebuild the state store by replaying the local archive
    RebuildState(rebuild_state::Args),

    // Reset WAL position using state cursor
    ResetWal(reset_wal::Args),

    /// checks the integrity of the WAL records
    WalIntegrity(wal_integrity::Args),

    /// rolls back the node to a specific slot and hash
    Rollback(rollback::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    match &args.command {
        Command::Check(x) => check::run(config, x)?,
        Command::CatchupStores(x) => catchup_stores::run(config, x, feedback)?,
        Command::RebuildState(x) => rebuild_state::run(config, x, feedback)?,
        Command::ResetWal(x) => reset_wal::run(config, x, feedback)?,
        Command::WalIntegrity(x) => wal_integrity::run(config, x)?,
        Command::Rollback(x) => rollback::run(config, x)?,
    }

    Ok(())
}
