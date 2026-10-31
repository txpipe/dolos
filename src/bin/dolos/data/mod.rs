use clap::ValueEnum;
use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;

mod check;
pub(crate) mod cleanup;
mod clear_state;
mod compute_nonce;
mod compute_spdd;
mod copy_wal;
mod dump_blocks;
mod dump_entity;
mod dump_logs;
mod dump_state;
mod find_seq;
mod housekeeping;
mod prune_chain;
mod prune_wal;
mod stats;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Default,
    Dbsync,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// checks the consistency of the managed data
    Check(check::Args),
    /// dumps data from the state
    DumpState(dump_state::Args),
    /// dumps an entity from the state
    DumpEntity(dump_entity::Args),
    /// dumps data from the logs
    DumpLogs(dump_logs::Args),
    /// dumps data from the blocks
    DumpBlocks(dump_blocks::Args),
    /// clears data from the state
    ClearState(clear_state::Args),
    /// clears all data managed by this node
    Cleanup(cleanup::Args),
    /// computes the SPDD for the current epoch
    ComputeSpdd(compute_spdd::Args),
    /// computes the nonce for a epoch
    ComputeNonce(compute_nonce::Args),
    /// finds the WAL seq for a block
    FindSeq(find_seq::Args),
    /// copies a range of slots from the WAL into a new db
    CopyWal(copy_wal::Args),
    /// removes blocks from the WAL before a given slot
    PruneWal(prune_wal::Args),
    /// removes blocks from the chain before a given slot
    PruneChain(prune_chain::Args),
    /// shows storage footprint statistics about the data
    Stats(stats::Args),
    /// runs housekeeping tasks
    Housekeeping(housekeeping::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(
    config: &RootConfig,
    args: &Args,
    feedback: &super::feedback::Feedback,
) -> miette::Result<()> {
    match &args.command {
        Command::Check(x) => check::run(config, x, feedback)?,
        Command::DumpState(x) => dump_state::run(config, x)?,
        Command::DumpEntity(x) => dump_entity::run(config, x)?,
        Command::DumpLogs(x) => dump_logs::run(config, x)?,
        Command::DumpBlocks(x) => dump_blocks::run(config, x)?,
        Command::ClearState(x) => clear_state::run(config, x)?,
        Command::Cleanup(x) => cleanup::run(config, x)?,
        Command::ComputeSpdd(x) => compute_spdd::run(config, x)?,
        Command::ComputeNonce(x) => compute_nonce::run(config, x)?,
        Command::FindSeq(x) => find_seq::run(config, x)?,
        Command::CopyWal(x) => copy_wal::run(config, x)?,
        Command::PruneWal(x) => prune_wal::run(config, x)?,
        Command::PruneChain(x) => prune_chain::run(config, x)?,
        Command::Stats(x) => stats::run(config, x)?,
        Command::Housekeeping(x) => housekeeping::run(config, x)?,
    }

    Ok(())
}
