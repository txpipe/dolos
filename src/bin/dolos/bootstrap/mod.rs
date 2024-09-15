use clap::{Parser, Subcommand};
use miette::IntoDiagnostic;

use crate::feedback::Feedback;

mod mithril;
mod relay;
mod snapshot;

#[derive(Debug, Subcommand)]
pub enum Command {
    Relay(relay::Args),
    Mithril(mithril::Args),
    Snapshot(snapshot::Args),
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Snapshot(_) => write!(f, "Dolos snapshot (a few mins, trust me bro)"),
            Command::Mithril(_) => write!(f, "Mithril snapshot (a few hours, trust Mithril SPOs)"),
            Command::Relay(_) => write!(f, "Relay chain-sync (several days, trust your relay)"),
        }
    }
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

fn inquire_command() -> miette::Result<Command> {
    inquire::Select::new(
        "which bootstrap method would you like to use?",
        vec![
            Command::Snapshot(snapshot::Args::default()),
            Command::Mithril(mithril::Args::default()),
            Command::Relay(relay::Args::default()),
        ],
    )
    .prompt()
    .into_diagnostic()
}

pub fn run(config: &crate::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    let command = if let Some(command) = &args.command {
        command
    } else {
        &inquire_command()?
    };

    match command {
        Command::Relay(args) => relay::run(config, args, feedback),
        Command::Mithril(args) => mithril::run(config, args, feedback),
        Command::Snapshot(args) => snapshot::run(config, args, feedback),
    }
}
