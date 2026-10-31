use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;

use crate::feedback::Feedback;

mod rebuild_state;
mod reclaim_avvm;
mod update_entity;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// rebuild the state store by replaying the local archive
    RebuildState(rebuild_state::Args),

    /// delete the unredeemed Byron AVVM utxos a pre-fix binary left behind
    ReclaimAvvm(reclaim_avvm::Args),

    /// manually updates an entity in the state
    UpdateEntity(update_entity::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    match &args.command {
        Command::RebuildState(x) => rebuild_state::run(config, x, feedback)?,
        Command::ReclaimAvvm(x) => reclaim_avvm::run(config, x)?,
        Command::UpdateEntity(x) => update_entity::run(config, x)?,
    }

    Ok(())
}
