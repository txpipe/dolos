use clap::{Parser, Subcommand};

mod compute_spdd;
mod copy_wal;
mod dump_state;
mod dump_wal;
mod export;
mod find_seq;
mod housekeeping;
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
    /// dumps data from the state
    DumpState(dump_state::Args),
    /// computes the SPDD for the current epoch
    ComputeSpdd(compute_spdd::Args),
    /// finds the WAL seq for a block
    FindSeq(find_seq::Args),
    /// exports a snapshot from the current data
    Export(export::Args),
    /// copies a range of slots from the WAL into a new db
    CopyWal(copy_wal::Args),
    /// removes blocks from the WAL before a given slot
    PruneWal(prune_wal::Args),
    /// removes blocks from the chain before a given slot
    PruneChain(prune_chain::Args),
    /// shows statistics about the data for Redb stores
    Stats(stats::Args),
    /// shows statistics about the data for Redb stores
    Housekeeping(housekeeping::Args),
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
        Command::DumpState(x) => dump_state::run(config, x)?,
        Command::ComputeSpdd(x) => compute_spdd::run(config, x)?,
        Command::FindSeq(x) => find_seq::run(config, x)?,
        Command::Export(x) => export::run(config, x, feedback)?,
        Command::CopyWal(x) => copy_wal::run(config, x)?,
        Command::PruneWal(x) => prune_wal::run(config, x)?,
        Command::PruneChain(x) => prune_chain::run(config, x)?,
        Command::Stats(x) => stats::run(config, x)?,
        Command::Housekeeping(x) => housekeeping::run(config, x)?,
    }

    Ok(())
}
