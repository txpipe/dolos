use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;

mod reclaim_avvm;
mod update_entity;

#[derive(Debug, Subcommand)]
pub enum Command {
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

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    match &args.command {
        Command::ReclaimAvvm(x) => reclaim_avvm::run(config, x)?,
        Command::UpdateEntity(x) => update_entity::run(config, x)?,
    }

    Ok(())
}
