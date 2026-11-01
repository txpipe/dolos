use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;

use crate::feedback::Feedback;

mod rebuild_state;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// rebuild the state store by replaying the local archive
    RebuildState(rebuild_state::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    match &args.command {
        Command::RebuildState(x) => rebuild_state::run(config, x, feedback)?,
    }

    Ok(())
}
