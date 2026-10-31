use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;

mod check;
mod clear_state;
mod compute_nonce;
mod compute_spdd;
mod copy_wal;
mod dump;
mod find_seq;
mod housekeeping;
mod inspect;
mod rebuild;
mod reset;
mod stats;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// shows storage footprint statistics about the data
    Stats(stats::Args),
    /// prints one entity from the state
    Inspect(inspect::Args),
    /// prints records from the state, the logs or the blocks
    Dump(dump::Args),
    /// checks the consistency of the managed data
    Check(check::Args),
    /// runs housekeeping tasks, applying the configured history policy
    Housekeeping(housekeeping::Args),
    /// removes every store under the storage path, keeping the configuration
    Reset(reset::Args),
    /// rebuilds the state store by replaying the local archive
    Rebuild(rebuild::Args),
    /// clears data from the state
    ClearState(clear_state::Args),
    /// computes the SPDD for the current epoch
    ComputeSpdd(compute_spdd::Args),
    /// computes the nonce for a epoch
    ComputeNonce(compute_nonce::Args),
    /// finds the WAL seq for a block
    FindSeq(find_seq::Args),
    /// copies a range of slots from the WAL into a new db
    CopyWal(copy_wal::Args),
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
        Command::Stats(x) => stats::run(config, x)?,
        Command::Inspect(x) => inspect::run(config, x)?,
        Command::Dump(x) => dump::run(config, x)?,
        Command::Check(x) => check::run(config, x, feedback)?,
        Command::Housekeeping(x) => housekeeping::run(config, x)?,
        Command::Reset(x) => reset::run(config, x)?,
        Command::Rebuild(x) => rebuild::run(config, x, feedback)?,
        Command::ClearState(x) => clear_state::run(config, x)?,
        Command::ComputeSpdd(x) => compute_spdd::run(config, x)?,
        Command::ComputeNonce(x) => compute_nonce::run(config, x)?,
        Command::FindSeq(x) => find_seq::run(config, x)?,
        Command::CopyWal(x) => copy_wal::run(config, x)?,
    }

    Ok(())
}
