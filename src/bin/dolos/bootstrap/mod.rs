use std::io::IsTerminal;

use clap::{Parser, Subcommand};
use inquire::list_option::ListOption;
use miette::{bail, Context, IntoDiagnostic};
use tracing::info;

use crate::feedback::Feedback;
use dolos_core::{StateStore, WalStore};

mod mithril;
mod ranged;
mod relay;
mod snapshot;
mod stelae;

#[derive(Debug, Subcommand, Clone)]
pub enum Command {
    Relay(relay::Args),
    Mithril(mithril::Args),
    Snapshot(snapshot::Args),
    Stelae(stelae::Args),
}

impl Command {
    /// Ask the operator which bootstrap method to use.
    ///
    /// Refused outright when there is no terminal to ask on, *before* the
    /// prompt rather than through it. `inquire` reads the console directly on
    /// Windows rather than this process's stdin, so a redirected or closed
    /// stdin does not make the prompt fail there — it waits for a keypress a
    /// script will never send. Checking here is what makes "a machine with no
    /// terminal" the same clean refusal on every platform, which is what
    /// `inspect_existing_data` below is written to assume.
    pub fn inquire() -> miette::Result<Self> {
        if !std::io::stdin().is_terminal() {
            bail!("no bootstrap method given and no terminal to ask on; pass a subcommand instead (see `dolos bootstrap --help`)");
        }

        let cmd = inquire::Select::new(
            "which bootstrap method would you like to use?",
            vec![
                ListOption::new(0, "Dolos snapshot (a few mins, trust me bro)"),
                ListOption::new(
                    1,
                    "Stelae snapshot (a few mins, from a stele you can verify)",
                ),
                ListOption::new(2, "Mithril snapshot (a few hours, trust Mithril SPOs)"),
                ListOption::new(3, "Relay chain-sync (several days, trust your relay)"),
            ],
        )
        .prompt()
        .into_diagnostic()?;

        match cmd.index {
            0 => Ok(Command::Snapshot(snapshot::Args::inquire()?)),
            1 => Ok(Command::Stelae(stelae::Args::inquire()?)),
            2 => Ok(Command::Mithril(mithril::Args::default())),
            3 => Ok(Command::Relay(relay::Args::default())),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Parser, Default)]
pub struct Args {
    /// Clear existing data before bootstrapping
    #[arg(long, global = true)]
    force: bool,

    /// Skip bootstrap if data already exists (exit 0)
    #[arg(long, alias = "skip-if-not-empty", global = true)]
    skip_if_data: bool,

    /// Continue bootstrap even if data exists, trusting the subcommand to
    /// handle resumption
    #[arg(long, alias = "resume", global = true)]
    r#continue: bool,

    /// Enable verbose logging output
    #[arg(long, action, global = true)]
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

/// What to do about data already in storage.
///
/// Deciding is separated from doing because only one of the outcomes is
/// destructive, and it must not happen until the run is fully known. An
/// interactive bootstrap asks which method to use — and, for a stele, where the
/// stele is — *after* the flags are parsed, so a `--force` that cleared first
/// would take a working node away on a typo, a cancel, or a machine with no
/// terminal, and hand back nothing in its place.
enum Existing {
    /// Data is there and `--skip-if-data` says to leave it alone.
    Skip,
    /// Go ahead, but clear storage first.
    Clear,
    /// Go ahead as things are.
    Proceed,
}

/// Read what is in storage and decide, without touching any of it.
///
/// The refusals happen here — a skip, and the bail for existing data with no
/// flag saying what to do about it — so neither is a question asked of an
/// operator whose answer is then thrown away.
fn inspect_existing_data(config: &RootConfig, args: &Args) -> miette::Result<Existing> {
    if args.r#continue {
        return Ok(Existing::Proceed);
    }

    if !has_existing_data(config)? {
        return Ok(Existing::Proceed);
    }

    if args.skip_if_data {
        return Ok(Existing::Skip);
    }

    if args.force {
        return Ok(Existing::Clear);
    }

    bail!("existing data detected in storage. Use --force to clear and re-bootstrap, --skip-if-data to skip, or --continue to resume");
}

fn dispatch(config: &RootConfig, command: &Command, feedback: &Feedback) -> miette::Result<()> {
    match command {
        Command::Relay(args) => relay::run(config, args, feedback),
        Command::Mithril(args) => mithril::run(config, args, feedback),
        Command::Snapshot(args) => snapshot::run(config, args, feedback),
        // No `feedback`: a stele restore has no progress reporting yet, which
        // is a stated gap rather than an oversight — see the follow-up plan.
        Command::Stelae(args) => stelae::run(config, args),
    }
}

/// Seed the WAL from the state cursor so that `find_intersect` works after
/// bootstrap.
///
/// Some bootstrap mechanisms skip WAL commits for performance, leaving it
/// empty. This ensures the WAL tip matches the state cursor regardless of which
/// bootstrap method was used.
fn seed_wal_from_state(config: &RootConfig) -> miette::Result<()> {
    let state = crate::common::open_state_store(config)?;
    let wal = crate::common::open_wal_store(config)?;

    let cursor = state
        .read_cursor()
        .into_diagnostic()
        .context("reading state cursor")?;

    let Some(cursor) = cursor else {
        info!("no state cursor after bootstrap, skipping WAL seed");
        return Ok(());
    };

    if !cursor.is_fully_defined() {
        return Err(miette::miette!(
            "state cursor at slot {} has no block hash, cannot seed WAL",
            cursor.slot(),
        ));
    }

    wal.reset_to(&cursor)
        .into_diagnostic()
        .context("seeding WAL from state cursor")?;

    info!(%cursor, "seeded WAL from state cursor");

    Ok(())
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

    let existing = inspect_existing_data(config, args)?;

    if matches!(existing, Existing::Skip) {
        info!("existing data detected, skipping bootstrap");
        return Ok(());
    }

    let command = match args.command.clone() {
        Some(x) => x,
        None => Command::inquire()?,
    };

    // The first destructive step, and deliberately the last one before the run:
    // everything that could still refuse — the flags, the prompts, the source a
    // stele restore was given — has already had its say.
    if matches!(existing, Existing::Clear) {
        clear_storage(config)?;
    }

    dispatch(config, &command, feedback)?;

    // Reset WAL after any successful bootstrap so that `find_intersect` works.
    // Some bootstrap mechanisms skip WAL commits for performance, leaving it empty
    // or stale. This ensures the WAL tip matches the state cursor regardless of
    // which bootstrap method was used.
    if let Err(e) = seed_wal_from_state(config) {
        tracing::error!("failed to seed WAL from state: {}", e);
    }

    Ok(())
}
