use clap::{Parser, Subcommand};

mod rebuild_ledger;
mod rebuild_wal;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// rebuilds the whole ledger from chain data
    RebuildLedger(rebuild_ledger::Args),
    /// rebuilds a segment of the wal from chain data
    RebuildWal(rebuild_wal::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &super::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    match &args.command {
        Command::RebuildLedger(x) => rebuild_ledger::run(config, x)?,
        Command::RebuildWal(x) => rebuild_wal::run(config, x)?,
    }

    Ok(())
}
