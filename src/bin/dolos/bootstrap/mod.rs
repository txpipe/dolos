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

/// Empty the storage directory.
///
/// A `remove_dir_all` of the whole path and not a per-store wipe, which matters
/// for one thing that is not a store: a stele restore's progress file lives
/// inside `storage.path`, and a progress file that outlived the stores it
/// describes would tell the next `--continue` to skip layers whose data is
/// gone. Anything that clears storage has to clear that too, and taking the
/// directory is how this does it without having to remember.
fn clear_storage(config: &RootConfig) -> miette::Result<()> {
    info!("existing data detected, clearing storage due to --force");

    clear_storage_path(&config.storage.path)
}

fn clear_storage_path(storage_path: &std::path::Path) -> miette::Result<()> {
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
        // No `feedback`: a stele restore has no progress reporting yet, which
        // is a stated gap rather than an oversight — see the follow-up plan.
        //
        // `resume` is `--continue`, and this is the only subcommand that does
        // anything with it. For the others it has always meant no more than
        // "proceed even though there is data here"; for a stele restore it is
        // also the instruction to read the progress file an interrupted attempt
        // left behind. Passing it rather than re-reading `args` keeps that one
        // flag with one meaning per subcommand instead of two spellings.
        Command::Stelae(args) => stelae::run(config, args, resume),
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

    dispatch(config, &command, feedback, args.r#continue)?;

    // Reset WAL after any successful bootstrap so that `find_intersect` works.
    // Some bootstrap mechanisms skip WAL commits for performance, leaving it empty
    // or stale. This ensures the WAL tip matches the state cursor regardless of
    // which bootstrap method was used.
    if let Err(e) = seed_wal_from_state(config) {
        tracing::error!("failed to seed WAL from state: {}", e);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use dolos_snapshot::restore::Checkpoint;

    /// A `--force` wipe takes the progress file with the data it describes.
    ///
    /// The hazard this rules out is specific and quiet. `--continue` reads the
    /// progress file and skips every layer it names; `--force` clears the
    /// stores. A progress file that survived a wipe would therefore tell the
    /// next run that layers it has no data for are already done, and the node
    /// that came out would be missing a slice of chain with nothing reporting
    /// it.
    ///
    /// Asserted against [`clear_storage`]'s actual behaviour rather than
    /// against the fact that it happens to call `remove_dir_all`: what has to
    /// stay true is the outcome, however the wipe is later spelled.
    #[test]
    fn clearing_storage_removes_a_restore_in_progress() {
        let temp = tempfile::tempdir().unwrap();
        let storage = temp.path().join("data");

        std::fs::create_dir_all(&storage).unwrap();

        let progress = Checkpoint::path_in(&storage);
        std::fs::write(&progress, b"{}").unwrap();

        // A stand-in for the stores the progress file describes, so the
        // assertion is about a directory that had a node in it.
        std::fs::write(storage.join("state"), b"a store").unwrap();

        assert!(progress.exists());

        super::clear_storage_path(&storage).unwrap();

        assert!(
            !progress.exists(),
            "a progress file outlived the stores it describes"
        );

        assert!(
            storage.is_dir(),
            "the storage directory itself has to come back, empty"
        );

        assert_eq!(
            std::fs::read_dir(&storage).unwrap().count(),
            0,
            "and come back empty"
        );
    }

    /// The progress file is *inside* the storage path.
    ///
    /// The other half of the test above, and the half that would fail first if
    /// the file were ever moved: a wipe of `storage.path` only takes it while
    /// it lives there.
    #[test]
    fn the_progress_file_lives_inside_the_storage_path() {
        let storage = std::path::Path::new("/var/lib/dolos/data");

        assert!(Checkpoint::path_in(storage).starts_with(storage));
    }
}
