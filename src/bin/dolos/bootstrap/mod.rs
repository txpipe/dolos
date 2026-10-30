use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use miette::{bail, Context, IntoDiagnostic};
use tracing::info;

use crate::feedback::Feedback;
use dolos::storage::{Existing, ExistingDataPolicy, Removal};
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

    /// Exit 0 without bootstrapping when storage already holds a node, as an
    /// interrupted replay that has committed leaves one for `--continue` to
    /// resume; data without one, as a bootstrap interrupted before it commits
    /// a cursor leaves, and a store this release refuses are errors instead
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
    Mithril(&'a mithril::Args, dolos_mithril::MithrilSource, PathBuf),
    Stelae(&'a stelae::Args, stelae::Inputs),
    Immutabledb(&'a immutabledb::Args, PathBuf),
}

/// `clears` says that `--force` is to clear storage once the command is
/// resolved, so that a source the clear would take is refused here.
fn resolve<'a>(
    config: &RootConfig,
    command: &'a Command,
    clears: bool,
) -> miette::Result<Resolved<'a>> {
    let network = dolos_cardano::networks::KnownNetwork::by_name(&config.chain.network);

    let resolved = match command {
        #[cfg(feature = "mithril")]
        Command::Mithril(args) => {
            let (source, download_dir) = mithril::resolve(config, args, network, clears)?;
            Resolved::Mithril(args, source, download_dir)
        }
        Command::Stelae(args) => {
            Resolved::Stelae(args, stelae::resolve(config, args, network, clears)?)
        }
        Command::Immutabledb(args) => {
            Resolved::Immutabledb(args, immutabledb::resolve(config, args, clears)?)
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
        Resolved::Mithril(args, source, download_dir) => {
            mithril::run(config, args, &source, &download_dir, feedback)
        }
        Resolved::Stelae(args, inputs) => {
            stelae::run(config, args, inputs, feedback)?;

            // A restore writes the state and the archive, never the WAL, so
            // the WAL is seeded at the restored cursor for `find_intersect`.
            // A replay needs no seed: finishing it reconciles the WAL with
            // the state, and keeps the window of a node it had nothing to
            // add to.
            if let Err(e) = seed_wal(config) {
                tracing::error!("failed to seed WAL from state: {}", e);
            }

            Ok(())
        }
        Resolved::Immutabledb(args, immutable) => {
            immutabledb::run(config, args, &immutable, feedback)
        }
    }
}

/// What clearing the storage root, as `--force` does before a run, would
/// take of the source at `dir`, of which the run reads `reads`: `dir`
/// itself, when it lies there or the way to it passes a directory or link
/// there, or else the first of `reads`, or of the directories and links
/// under them, through the links under them, that the clear takes. Each
/// directory is read once, whatever names it, and nothing else in `dir` is
/// read.
///
/// A directory that can't be searched, as a non-root user's `lost+found`, or
/// one gone by the time it is read, holds nothing the run reads through, and
/// nor does a link whose way passes one. One that can be searched but not
/// listed is refused: the OS reaches what it holds by name, and what that is
/// can't be told.
pub(super) fn the_clear_takes(
    config: &RootConfig,
    dir: &Path,
    reads: &[PathBuf],
) -> miette::Result<Option<PathBuf>> {
    use std::io::ErrorKind;

    let root = &config.storage.path;
    let clear = Removal::of(root)
        .into_diagnostic()
        .with_context(|| format!("reading the storage root {}", root.display()))?;

    let reading = |path: &Path| format!("reading {}", path.display());
    let takes = |path: &Path| {
        clear
            .takes(path)
            .into_diagnostic()
            .with_context(|| reading(path))
    };
    let directory = |path: &Path| match std::fs::metadata(path) {
        Ok(meta) => Ok(meta.is_dir()),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::NotFound | ErrorKind::PermissionDenied
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error).into_diagnostic().with_context(|| reading(path)),
    };

    if takes(dir)? {
        return Ok(Some(dir.to_path_buf()));
    }

    let mut pending = Vec::new();

    for path in reads {
        if takes(path)? {
            return Ok(Some(path.clone()));
        }

        if directory(path)? {
            pending.push(path.clone());
        }
    }

    let mut listed = std::collections::HashSet::new();

    while let Some(next) = pending.pop() {
        match identity(&next) {
            Ok(id) => {
                if !listed.insert(id) {
                    continue;
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(error).into_diagnostic().with_context(|| reading(&next)),
        }

        let entries = match std::fs::read_dir(&next) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) if error.kind() == ErrorKind::PermissionDenied => {
                if !searched(&next) {
                    continue;
                }

                return Err(error).into_diagnostic().with_context(|| {
                    format!(
                        "reading {}, which can be searched but not listed, so what is reached \
                         through it can't be told: make it readable to run with --force",
                        next.display()
                    )
                });
            }
            Err(error) => return Err(error).into_diagnostic().with_context(|| reading(&next)),
        };

        for entry in entries {
            let entry = entry.into_diagnostic().with_context(|| reading(&next))?;
            let path = entry.path();
            let kind = entry
                .file_type()
                .into_diagnostic()
                .with_context(|| reading(&path))?;

            if !kind.is_symlink() && !kind.is_dir() {
                continue;
            }

            match clear.takes(&path) {
                Ok(true) => return Ok(Some(path)),
                Ok(false) => {}
                Err(error) if error.kind() == ErrorKind::PermissionDenied => continue,
                Err(error) => return Err(error).into_diagnostic().with_context(|| reading(&path)),
            }

            if directory(&path)? {
                pending.push(path);
            }
        }
    }

    Ok(None)
}

/// What the directory at `path` is, through any link, whatever it is called:
/// its device and inode.
#[cfg(unix)]
fn identity(path: &Path) -> std::io::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;

    let meta = std::fs::metadata(path)?;

    Ok((meta.dev(), meta.ino()))
}

/// What the directory at `path` is, through any link, whatever it is called:
/// its final path.
#[cfg(not(unix))]
fn identity(path: &Path) -> std::io::Result<PathBuf> {
    std::fs::canonicalize(path)
}

/// Whether the OS can pass through the directory at `dir` to what is in it.
fn searched(dir: &Path) -> bool {
    std::fs::symlink_metadata(dir.join(".")).is_ok()
}

/// Refuse `what`, the source at `dir`, when the clear `--force` runs before
/// `before` would take it, or what the run reads of it, `reads`, naming
/// `remedy`.
pub(super) fn refuse_what_the_clear_takes(
    config: &RootConfig,
    dir: &Path,
    reads: &[PathBuf],
    what: &str,
    before: &str,
    remedy: &str,
) -> miette::Result<()> {
    let root = config.storage.path.display();

    let taken = the_clear_takes(config, dir, reads).with_context(|| {
        format!(
            "checking {what} at {} against the storage root {root}, which `--force` clears \
             before {before}",
            dir.display()
        )
    })?;

    match taken {
        None => Ok(()),
        Some(taken) if taken == dir => bail!(
            "{what} at {} lies in the storage root {root}, or is reached through it, which \
             `--force` clears before {before}: {remedy}",
            dir.display()
        ),
        Some(entry) => {
            let kind = match std::fs::symlink_metadata(&entry) {
                Ok(meta) if meta.is_dir() => "directory",
                Ok(meta) if meta.is_file() => "file",
                _ => "link",
            };

            bail!(
                "the {kind} {} in {what} at {} leads into the storage root {root}, which \
                 `--force` clears before {before}: {remedy}",
                entry.display(),
                dir.display()
            )
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

    let clears = matches!(existing, Existing::Clear);

    let resolved = resolve(config, &command, clears)?;

    // The first destructive step, and deliberately the last one before the run:
    // everything that could still refuse — the flags, the prompts, the source
    // the command was given — has already had its say.
    if clears {
        info!("existing data detected, clearing storage due to --force");

        dolos::storage::clear_storage(&config.storage.path).into_diagnostic()?;
    }

    dispatch(config, resolved, feedback)
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
