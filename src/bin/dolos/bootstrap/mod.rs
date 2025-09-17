use clap::{Parser, Subcommand};
use inquire::list_option::ListOption;
use miette::IntoDiagnostic;

use crate::feedback::Feedback;


mod mithril;
mod relay;
mod snapshot;

#[derive(Debug, Subcommand, Clone)]
pub enum Command {
    Relay(relay::Args),
    Mithril(mithril::Args),
    Snapshot(snapshot::Args),
}

impl Command {
    pub fn inquire() -> miette::Result<Self> {
        let cmd = inquire::Select::new(
            "which bootstrap method would you like to use?",
            vec![
                ListOption::new(0, "Dolos snapshot (a few mins, trust me bro)"),
                ListOption::new(1, "Mithril snapshot (a few hours, trust Mithril SPOs)"),
                ListOption::new(2, "Relay chain-sync (several days, trust your relay)"),
            ],
        )
        .prompt()
        .into_diagnostic()?;

        match cmd.index {
            0 => Ok(Command::Snapshot(snapshot::Args::inquire()?)),
            1 => Ok(Command::Mithril(mithril::Args::default())),
            2 => Ok(Command::Relay(relay::Args::default())),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Parser, Default)]
pub struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

pub fn run(config: &crate::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    let command = match args.command.clone() {
        Some(x) => x,
        None => Command::inquire()?,
    };

    match command {
        Command::Relay(args) => relay::run(config, &args, feedback),
        Command::Mithril(args) => mithril::run(config, &args, feedback),
        Command::Snapshot(args) => snapshot::run(config, &args, feedback),
    }
}
