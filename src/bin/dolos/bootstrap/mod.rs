use clap::{Parser, Subcommand};
use miette::IntoDiagnostic;

use crate::feedback::Feedback;

mod mithril;
mod snapshot;

#[derive(Debug, Subcommand)]
pub enum Command {
    Mithril(mithril::Args),
    Snapshot(snapshot::Args),
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Mithril(_) => write!(f, "Secure Mithril snapshot (slow but secure)"),
            Command::Snapshot(_) => write!(f, "Fast Dolos snapshot (trust me bro)"),
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
            Command::Mithril(mithril::Args::default()),
            Command::Snapshot(snapshot::Args::default()),
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
        Command::Mithril(args) => mithril::run(config, &args, feedback),
        Command::Snapshot(args) => snapshot::run(config, &args, feedback),
    }
}
