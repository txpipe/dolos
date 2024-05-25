use clap::{Parser, Subcommand};

mod dump_wal;
mod summary;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// shows a summary of managed data
    Summary(summary::Args),
    /// dumps data from the WAL
    DumpWal(dump_wal::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &super::Config, args: &Args) -> miette::Result<()> {
    match &args.command {
        Command::Summary(x) => summary::run(config, x)?,
        Command::DumpWal(x) => dump_wal::run(config, x)?,
    }

    Ok(())
}
