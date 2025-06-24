use clap::{Parser, Subcommand};

mod copy_ledger;
mod copy_wal;
mod dump_wal;
mod export;
mod find_seq;
mod prune_chain;
mod prune_wal;
mod stats;
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
    /// copies a range of slots from the WAL into a new db
    CopyWal(copy_wal::Args),
    /// copies a range of slots from the WAL into a new db
    CopyLedger(copy_ledger::Args),
    /// removes blocks from the WAL before a given slot
    PruneWal(prune_wal::Args),
    /// removes blocks from the chain before a given slot
    PruneChain(prune_chain::Args),
    /// shows statistics about the data for Redb stores
    Stats(stats::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(
    config: &super::Config,
    args: &Args,
    feedback: &super::feedback::Feedback,
) -> miette::Result<()> {
    match &args.command {
        Command::Summary(x) => summary::run(config, x)?,
        Command::DumpWal(x) => dump_wal::run(config, x)?,
        Command::FindSeq(x) => find_seq::run(config, x)?,
        Command::Export(x) => export::run(config, x, feedback)?,
        Command::CopyWal(x) => copy_wal::run(config, x)?,
        Command::CopyLedger(x) => copy_ledger::run(config, x, feedback)?,
        Command::PruneWal(x) => prune_wal::run(config, x)?,
        Command::PruneChain(x) => prune_chain::run(config, x)?,
        Command::Stats(x) => stats::run(config, x)?,
    }

    Ok(())
}
