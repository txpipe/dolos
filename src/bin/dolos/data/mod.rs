use clap::{Parser, Subcommand};

mod dump_wal;
mod export;
mod find_seq;
mod summary;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// shows a summary of managed data
    Summary(summary::Args),
    /// dumps data from the WAL
    DumpWal(dump_wal::Args),
    /// finds the WAL seq for a block
    FindSeq(find_seq::Args),
    /// exports a snapshot from the current data
    Export(export::Args),
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
        Command::FindSeq(x) => find_seq::run(config, x)?,
        Command::Export(x) => export::run(config, x)?,
    }

    Ok(())
}
