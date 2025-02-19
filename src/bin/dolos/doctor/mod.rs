use clap::{Parser, Subcommand};

use crate::feedback::Feedback;

mod rebuild;
mod wal_integrity;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// rebuilds ledger and chain from WAL
    Rebuild(rebuild::Args),
    /// checks the integrity of the WAL records
    WalIntegrity(wal_integrity::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &super::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    match &args.command {
        Command::Rebuild(x) => rebuild::run(config, x, feedback)?,
        Command::WalIntegrity(x) => wal_integrity::run(config, x)?,
    }

    Ok(())
}

pub fn run_rebuild(config: &super::Config, feedback: &Feedback) -> miette::Result<()> {
    rebuild::run(config, &rebuild::Args, feedback)
}
