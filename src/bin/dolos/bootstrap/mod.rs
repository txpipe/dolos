use clap::{Parser, Subcommand};
use inquire::list_option::ListOption;
use miette::{bail, Context, IntoDiagnostic};
use tracing::info;

use crate::feedback::Feedback;
use dolos_core::StateStore;

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
    /// Clear existing data before bootstrapping
    #[arg(long)]
    force: bool,

    /// Skip bootstrap if data already exists (exit 0)
    #[arg(long, alias = "skip-if-not-empty")]
    skip_if_data: bool,

    /// Continue bootstrap even if data exists, trusting the subcommand to handle resumption
    #[arg(long, alias = "resume")]
    r#continue: bool,

    /// Enable verbose logging output
    #[arg(long, action)]
    verbose: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

use dolos_core::config::RootConfig;

fn has_existing_data(config: &RootConfig) -> miette::Result<bool> {
    let state = crate::common::open_state_store(config)?;
    let cursor = state
        .read_cursor()
        .into_diagnostic()
        .context("reading state cursor")?;

    Ok(cursor.is_some())
}

fn clear_storage(config: &RootConfig) -> miette::Result<()> {
    info!("existing data detected, clearing storage due to --force");

    let storage_path = &config.storage.path;

    std::fs::remove_dir_all(storage_path)
        .into_diagnostic()
        .context("removing existing storage")?;

    std::fs::create_dir_all(storage_path)
        .into_diagnostic()
        .context("recreating storage directory")?;

    Ok(())
}

/// Checks existing data in storage and decides how to proceed based on flags.
/// Returns `Ok(true)` if bootstrap should continue, `Ok(false)` if it should be skipped.
fn handle_existing_data(config: &RootConfig, args: &Args) -> miette::Result<bool> {
    if args.r#continue {
        return Ok(true);
    }

    if !has_existing_data(config)? {
        return Ok(true);
    }

    if args.skip_if_data {
        info!("existing data detected, skipping bootstrap");
        return Ok(false);
    }

    if args.force {
        clear_storage(config)?;
        return Ok(true);
    }

    bail!("existing data detected in storage. Use --force to clear and re-bootstrap, --skip-if-data to skip, or --continue to resume");
}

fn dispatch(config: &RootConfig, command: &Command, feedback: &Feedback) -> miette::Result<()> {
    match command {
        Command::Relay(args) => relay::run(config, args, feedback),
        Command::Mithril(args) => mithril::run(config, args, feedback),
        Command::Snapshot(args) => snapshot::run(config, args, feedback),
    }
}

fn setup_tracing(config: &RootConfig, verbose: bool) -> miette::Result<()> {
    if verbose {
        crate::common::setup_tracing(&config.logging, &config.telemetry)?;
    } else {
        crate::common::setup_tracing_error_only()?;
    }

    Ok(())
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    setup_tracing(config, args.verbose)?;

    if !handle_existing_data(config, args)? {
        return Ok(());
    }

    let command = match args.command.clone() {
        Some(x) => x,
        None => Command::inquire()?,
    };

    dispatch(config, &command, feedback)
}
