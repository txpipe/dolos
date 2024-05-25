use clap::{Parser, Subcommand};

mod rebuild_ledger;
mod wal_integrity;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// rebuilds the whole ledger from chain data
    RebuildLedger(rebuild_ledger::Args),
    WalIntegrity(wal_integrity::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &super::Config, args: &Args) -> miette::Result<()> {
    match &args.command {
        Command::RebuildLedger(x) => rebuild_ledger::run(config, x)?,
        Command::WalIntegrity(x) => wal_integrity::run(config, x)?,
    }

    Ok(())
}
