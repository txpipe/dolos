use std::io::IsTerminal;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use miette::{bail, IntoDiagnostic};
use tracing::info;

use crate::feedback::Feedback;
use dolos::storage::{Existing, ExistingDataPolicy};
use dolos_core::config::RootConfig;
use dolos_core::{seed_wal_from_state, WalSeed};

mod immutable;
mod immutabledb;
#[cfg(feature = "mithril")]
pub(crate) mod mithril;
mod stelae;

#[derive(Debug, Subcommand, Clone)]
pub enum Command {
    /// Download a Mithril-certified ImmutableDB and replay it
    #[cfg(feature = "mithril")]
    Mithril(mithril::Args),

    /// Restore a Stelae snapshot
    Stelae(stelae::Args),

    /// Replay the ImmutableDB of a local Cardano node database
    Immutabledb(immutabledb::Args),
}

impl Command {
    /// `--continue`, which only the replays take.
    fn resumes(&self) -> bool {
        match self {
            #[cfg(feature = "mithril")]
            Command::Mithril(args) => args.replay.resume,
            Command::Stelae(_) => false,
            Command::Immutabledb(args) => args.replay.resume,
        }
    }

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

        Self::prompt()
    }

    #[cfg(feature = "utils")]
    fn prompt() -> miette::Result<Self> {
        let mut methods = vec![Method::Stelae];
        #[cfg(feature = "mithril")]
        methods.push(Method::Mithril);
        methods.push(Method::Immutabledb);

        let method = inquire::Select::new("which bootstrap method would you like to use?", methods)
            .prompt()
            .into_diagnostic()?;

        match method {
            Method::Stelae => Ok(Command::Stelae(stelae::Args::inquire()?)),
            #[cfg(feature = "mithril")]
            Method::Mithril => Ok(Command::Mithril(from_environment()?)),
            Method::Immutabledb => Ok(Command::Immutabledb(immutabledb::Args::inquire()?)),
        }
    }

    #[cfg(not(feature = "utils"))]
    fn prompt() -> miette::Result<Self> {
        bail!("no bootstrap method given and this build has no prompt; pass a subcommand instead (see `dolos bootstrap --help`)");
    }
}

/// What the prompt offers.
#[cfg(feature = "utils")]
#[derive(Debug, Clone, Copy)]
enum Method {
    Stelae,
    #[cfg(feature = "mithril")]
    Mithril,
    Immutabledb,
}

#[cfg(feature = "utils")]
impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Method::Stelae => "Stelae snapshot (a few mins, from a stele you can verify)",
            #[cfg(feature = "mithril")]
            Method::Mithril => "Mithril snapshot (a few hours, trust Mithril SPOs)",
            Method::Immutabledb => {
                "Local ImmutableDB (a few hours, from a Cardano node database you hold)"
            }
        })
    }
}

/// A subcommand's arguments as an invocation naming none parses them, so
/// that the prompted path still reads the environment inputs.
#[cfg(feature = "utils")]
fn from_environment<T: clap::Args>() -> miette::Result<T> {
    #[derive(Parser)]
    struct Environment<T: clap::Args> {
        #[command(flatten)]
        args: T,
    }

    Environment::<T>::try_parse_from(["dolos"])
        .map(|parsed| parsed.args)
        .into_diagnostic()
}

#[derive(Debug, Parser, Default)]
pub struct Args {
    /// Replace any data already in storage
    #[arg(long, global = true)]
    force: bool,

    /// Exit 0 without bootstrapping when storage already holds data; a store
    /// this release refuses is an error instead
    #[arg(long, global = true)]
    skip_if_data: bool,

    /// Enable verbose logging output
    #[arg(long, action, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

impl Args {
    /// What to do about data already in storage, as the flags say it.
    ///
    /// `--force`, `--skip-if-data` and `--continue` each answer that question
    /// differently, so two of them together is a usage error. Checked here
    /// rather than by clap, which does not compare a group's global flags
    /// with a subcommand's.
    fn policy(&self) -> Result<ExistingDataPolicy, clap::Error> {
        let policy = ExistingDataPolicy {
            force: self.force,
            skip_if_data: self.skip_if_data,
            r#continue: self.command.as_ref().is_some_and(Command::resumes),
        };

        let given: Vec<_> = [
            (policy.force, "--force"),
            (policy.skip_if_data, "--skip-if-data"),
            (policy.r#continue, "--continue"),
        ]
        .into_iter()
        .filter_map(|(given, flag)| given.then_some(flag))
        .collect();

        if given.len() > 1 {
            return Err(clap::Error::raw(
                clap::error::ErrorKind::ArgumentConflict,
                format!("{} cannot be used together\n", given.join(" and ")),
            ));
        }

        Ok(policy)
    }
}

/// A command with its inputs resolved: what it defaults from the network, the
/// credentials it reads with, the source it was pointed at.
///
/// Resolved once, before `--force` clears anything, so that a missing or bad
/// input is refused with the node intact and the run uses exactly what was
/// checked. A credentials file can be a pipe that reads only once.
enum Resolved<'a> {
    #[cfg(feature = "mithril")]
    Mithril(&'a mithril::Args, dolos_mithril::MithrilSource),
    Stelae(&'a stelae::Args, stelae::Inputs),
    Immutabledb(&'a immutabledb::Args, PathBuf),
}

fn resolve<'a>(config: &RootConfig, command: &'a Command) -> miette::Result<Resolved<'a>> {
    let network = dolos_cardano::networks::KnownNetwork::by_name(&config.chain.network);

    let resolved = match command {
        #[cfg(feature = "mithril")]
        Command::Mithril(args) => Resolved::Mithril(args, mithril::mithril_source(args, network)?),
        Command::Stelae(args) => Resolved::Stelae(args, stelae::resolve(args, network)?),
        Command::Immutabledb(args) => {
            Resolved::Immutabledb(args, immutabledb::resolve(config, args)?)
        }
    };

    Ok(resolved)
}

fn dispatch(
    config: &RootConfig,
    resolved: Resolved<'_>,
    feedback: &Feedback,
) -> miette::Result<()> {
    match resolved {
        #[cfg(feature = "mithril")]
        Resolved::Mithril(args, source) => mithril::run(config, args, &source, feedback),
        Resolved::Stelae(args, inputs) => stelae::run(config, args, inputs, feedback),
        Resolved::Immutabledb(args, immutable) => {
            immutabledb::run(config, args, &immutable, feedback)
        }
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
        crate::common::setup_tracing(&config.logging, config.telemetry.as_ref())?;
    } else {
        crate::common::setup_tracing_error_only()?;
    }

    Ok(())
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    let policy = args.policy().unwrap_or_else(|error| error.exit());

    setup_tracing(config, args.verbose)?;

    // The decision is the storage lifecycle's rather than any source's: one
    // policy over every managed store, whichever source fills them.
    let existing = dolos::storage::inspect_existing_data(config, policy).into_diagnostic()?;

    if matches!(existing, Existing::Skip) {
        info!("existing data detected, skipping bootstrap");
        return Ok(());
    }

    let command = match args.command.clone() {
        Some(x) => x,
        None => Command::inquire()?,
    };

    let resolved = resolve(config, &command)?;

    // The first destructive step, and deliberately the last one before the run:
    // everything that could still refuse — the flags, the prompts, the source
    // the command was given — has already had its say.
    if matches!(existing, Existing::Clear) {
        info!("existing data detected, clearing storage due to --force");

        dolos::storage::clear_storage(&config.storage.path).into_diagnostic()?;
    }

    dispatch(config, resolved, feedback)?;

    // Reset WAL after any successful bootstrap so that `find_intersect` works.
    // Some bootstrap mechanisms skip WAL commits for performance, leaving it empty
    // or stale. This ensures the WAL tip matches the state cursor regardless of
    // which bootstrap method was used.
    if let Err(e) = seed_wal(config) {
        tracing::error!("failed to seed WAL from state: {}", e);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(flags: &[&str]) -> Result<Args, clap::Error> {
        Args::try_parse_from(std::iter::once("bootstrap").chain(flags.iter().copied()))
    }

    #[test]
    fn continue_belongs_to_the_replays() {
        let replays = [
            #[cfg(feature = "mithril")]
            &["mithril", "--continue"][..],
            &["immutabledb", "--source", "db", "--continue"][..],
        ];

        for flags in replays {
            let policy = parsed(flags).unwrap().policy().unwrap();
            assert!(policy.r#continue, "{flags:?}");
        }

        for flags in [
            &["--continue", "stelae"][..],
            &["stelae", "--continue"][..],
            &["--continue"][..],
        ] {
            assert_eq!(parsed(flags).unwrap_err().exit_code(), 2, "{flags:?}");
        }
    }

    #[test]
    fn two_answers_about_existing_data_are_a_usage_error() {
        for flags in [
            &["--force", "--skip-if-data", "stelae"][..],
            &["--force", "stelae", "--skip-if-data"][..],
            &["--force", "immutabledb", "--source", "db", "--continue"][..],
            &[
                "immutabledb",
                "--source",
                "db",
                "--continue",
                "--skip-if-data",
            ][..],
        ] {
            let error = parsed(flags).unwrap().policy().unwrap_err();
            assert_eq!(error.exit_code(), 2, "{flags:?}");
        }

        for flags in [
            &["--force", "stelae"][..],
            &["stelae", "--skip-if-data"][..],
            &["immutabledb", "--source", "db"][..],
        ] {
            assert!(parsed(flags).unwrap().policy().is_ok(), "{flags:?}");
        }
    }
}
