use std::io::IsTerminal;

use clap::{Parser, Subcommand};
use inquire::list_option::ListOption;
use miette::{bail, IntoDiagnostic};
use tracing::info;

use crate::feedback::Feedback;
use dolos::storage::{Existing, ExistingDataPolicy};
use dolos_core::{seed_wal_from_state, WalSeed};

pub(crate) mod mithril;
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

/// What to do about data already in storage, and the flags that say it.
///
/// The decision is [`dolos::storage::inspect_existing_data`]'s — a wipe is
/// storage lifecycle rather than anything a bootstrap method knows about — and
/// what is here is the three flags that reach it.
fn inspect_existing_data(config: &RootConfig, args: &Args) -> miette::Result<Existing> {
    let policy = ExistingDataPolicy {
        force: args.force,
        skip_if_data: args.skip_if_data,
        r#continue: args.r#continue,
    };

    dolos::storage::inspect_existing_data(config, policy).into_diagnostic()
}

fn dispatch(
    config: &RootConfig,
    command: &Command,
    feedback: &Feedback,
    resume: bool,
) -> miette::Result<()> {
    match command {
        Command::Relay(args) => relay::run(config, args, feedback),
        Command::Mithril(args) => mithril::run(config, args, feedback),
        Command::Snapshot(args) => snapshot::run(config, args, feedback),
        // `resume` is `--continue`, and this is the only subcommand that does
        // anything with it. For the others it has always meant no more than
        // "proceed even though there is data here"; for a stele restore it is
        // also the instruction to read the progress file an interrupted attempt
        // left behind. Passing it rather than re-reading `args` keeps that one
        // flag with one meaning per subcommand instead of two spellings.
        Command::Stelae(args) => stelae::run(config, args, feedback, resume),
    }
}

/// Seed the WAL from the state cursor so that `find_intersect` works after
/// bootstrap.
///
/// The seed itself is [`dolos_core::seed_wal_from_state`], beside the bulk
/// import that leaves the gap; what is here is opening the two stores and
/// saying what happened.
fn seed_wal(config: &RootConfig) -> miette::Result<()> {
    let state = crate::common::open_state_store(config)?;
    let wal = crate::common::open_wal_store(config)?;

    match seed_wal_from_state(&state, &wal).into_diagnostic()? {
        WalSeed::NoCursor => info!("no state cursor after bootstrap, skipping WAL seed"),
        WalSeed::Seeded(cursor) => info!(%cursor, "seeded WAL from state cursor"),
    }

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
        info!("existing data detected, clearing storage due to --force");

        dolos::storage::clear_storage(&config.storage.path).into_diagnostic()?;
    }

    dispatch(config, &command, feedback, args.r#continue)?;

    // Reset WAL after any successful bootstrap so that `find_intersect` works.
    // Some bootstrap mechanisms skip WAL commits for performance, leaving it empty
    // or stale. This ensures the WAL tip matches the state cursor regardless of
    // which bootstrap method was used.
    if let Err(e) = seed_wal(config) {
        tracing::error!("failed to seed WAL from state: {}", e);
    }

    Ok(())
}
