use clap::{Parser, Subcommand};

use crate::feedback::Feedback;

mod rebuild_ledger;
mod trim_wal;
mod wal_integrity;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// rebuilds the whole ledger from chain data
    RebuildLedger(rebuild_ledger::Args),
    /// checks the integrity of the WAL records
    WalIntegrity(wal_integrity::Args),
    /// remove parts of the WAL
    TrimWal(trim_wal::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &super::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    match &args.command {
        Command::RebuildLedger(x) => rebuild_ledger::run(config, x, feedback)?,
        Command::WalIntegrity(x) => wal_integrity::run(config, x)?,
        Command::TrimWal(x) => trim_wal::run(config, x)?,
    }

    Ok(())
}

pub fn run_rebuild_ledger(config: &super::Config, feedback: &Feedback) -> miette::Result<()> {
    rebuild_ledger::run(config, &rebuild_ledger::Args, feedback)
}
