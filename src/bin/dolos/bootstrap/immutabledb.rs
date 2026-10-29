//! `dolos bootstrap immutabledb` — replay a local Cardano node's ImmutableDB.
//!
//! The replay is the one `bootstrap mithril` runs after its download; what
//! only this command has is the trust story. Nothing certifies a local
//! database, so the one check is that it belongs to the configured network:
//! the magic cardano-node and a Mithril download record beside `immutable/`
//! must be the configuration's.

use std::path::{Path, PathBuf};

use dolos_core::config::{Protocol, RootConfig};
use miette::{Context as _, IntoDiagnostic as _};

use super::immutable::{ReplayOptions, Replayed};
use crate::feedback::Feedback;

/// The file cardano-node and a Mithril download record the network magic in.
const PROTOCOL_MAGIC_ID: &str = "protocolMagicId";

#[derive(Debug, clap::Args, Clone)]
pub struct Args {
    /// the Cardano node database directory to replay: the one holding
    /// `immutable/` and `protocolMagicId`, as cardano-node and a Mithril
    /// download leave it. Only read, never written
    #[arg(long, value_name = "PATH")]
    pub source: PathBuf,

    #[command(flatten)]
    pub replay: ReplayOptions,
}

impl Args {
    #[cfg(feature = "utils")]
    pub fn inquire() -> miette::Result<Self> {
        let source = inquire::Text::new("where is the Cardano node database?")
            .with_help_message("the directory holding `immutable/` and `protocolMagicId`")
            .with_placeholder("/var/lib/cardano/db")
            .prompt()
            .into_diagnostic()?;

        Ok(Self {
            source: source.into(),
            replay: ReplayOptions::default(),
        })
    }
}

/// The ImmutableDB `args` names, once it is known to be readable, to belong
/// to the configured network, and to reach the stop epoch.
///
/// Resolved before `--force` clears anything, which `clears` says it is to,
/// so that a database for another network, a directory that isn't one, or
/// one the clear would take costs the operator nothing.
pub(super) fn resolve(config: &RootConfig, args: &Args, clears: bool) -> miette::Result<PathBuf> {
    let expected = match config.chain.protocol {
        Protocol::Cardano => u64::from(crate::common::open_genesis(config)?.network_magic()),
    };

    let immutable =
        super::immutable::check_database(config, &args.source, args.replay.resume, clears)?;

    let found = network_magic(&args.source)?;

    if found != expected {
        miette::bail!(
            "the ImmutableDB at {} is for network magic {found}, and this node's configuration \
             is for network magic {expected}",
            args.source.display()
        );
    }

    super::immutable::check_stop_epoch(config, &args.replay)?;

    Ok(immutable)
}

/// The network magic the database at `db` records.
fn network_magic(db: &Path) -> miette::Result<u64> {
    let path = db.join(PROTOCOL_MAGIC_ID);

    let recorded = std::fs::read_to_string(&path)
        .into_diagnostic()
        .with_context(|| {
            format!(
                "reading {}, which tells the network the ImmutableDB is for",
                path.display()
            )
        })?;

    recorded
        .trim()
        .parse()
        .into_diagnostic()
        .with_context(|| format!("{} holds no network magic", path.display()))
}

pub(super) fn run(
    config: &RootConfig,
    args: &Args,
    immutable: &Path,
    feedback: &Feedback,
) -> miette::Result<()> {
    match super::immutable::replay_immutabledb(config, immutable, args.replay, feedback)? {
        Replayed::Tip => tracing::info!("bootstrap complete, run `dolos daemon` to start the node"),
        Replayed::StopEpoch => {
            tracing::info!("stop epoch reached, `--continue` resumes the replay")
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_magic_is_read_from_the_database_directory() {
        let db = tempfile::tempdir().unwrap();

        std::fs::write(db.path().join(PROTOCOL_MAGIC_ID), "2\n").unwrap();
        assert_eq!(network_magic(db.path()).unwrap(), 2);

        std::fs::write(db.path().join(PROTOCOL_MAGIC_ID), "preview").unwrap();
        let message = format!("{:?}", network_magic(db.path()).unwrap_err());
        assert!(message.contains("holds no network magic"), "{message}");
    }

    #[test]
    fn a_database_without_its_magic_is_refused() {
        let db = tempfile::tempdir().unwrap();

        let message = format!("{:?}", network_magic(db.path()).unwrap_err());
        assert!(message.contains(PROTOCOL_MAGIC_ID), "{message}");
    }
}
