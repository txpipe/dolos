//! `dolos bootstrap mithril` — the interactive fetch-and-import.
//!
//! Parse and render: the download, its plan and its verification are
//! [`dolos_mithril`]'s, and the import is the ImmutableDB replay
//! `bootstrap immutabledb` runs too. What is here is the operator's
//! arguments, the runtime the async fetch is driven on and the progress bars
//! it reports through.

use dolos_cardano::networks::KnownNetwork;
use dolos_core::config::RootConfig;
use dolos_mithril::{fetch_snapshot, Fetch, MithrilSource};
use miette::{Context, IntoDiagnostic};
use std::{path::Path, sync::Arc};
use tracing::{info, warn};

use super::immutable::ReplayOptions;
use crate::feedback::{Feedback, MithrilFeedback};

#[derive(Debug, clap::Args, Clone)]
pub struct Args {
    /// the Mithril aggregator to download from; defaults to the configured
    /// network's, and is required for a custom network
    #[arg(long, env = "DOLOS_MITHRIL_AGGREGATOR", value_name = "URL")]
    pub(crate) aggregator: Option<String>,

    /// the aggregator's genesis verification key, JSON-hex encoded; defaults
    /// to the configured network's, and is required for a custom network
    #[arg(long, env = "DOLOS_MITHRIL_GENESIS_VKEY", value_name = "KEY")]
    pub(crate) genesis_vkey: Option<String>,

    #[arg(long, default_value = "./snapshot")]
    pub(crate) download_dir: String,

    /// Skip the Mithril certificate validation
    #[arg(long, action)]
    pub(crate) skip_validation: bool,

    /// Assume the snapshot is already available in the download dir
    #[arg(long, action)]
    pub(crate) skip_download: bool,

    /// Retain downloaded snapshot instead of deleting it
    #[arg(long, action)]
    pub(crate) retain_snapshot: bool,

    /// Start downloading from this immutable file number (inclusive)
    #[arg(long)]
    pub(crate) download_start: Option<u64>,

    /// Download up to this immutable file number (inclusive)
    #[arg(long)]
    pub(crate) download_end: Option<u64>,

    #[command(flatten)]
    pub(crate) replay: ReplayOptions,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            aggregator: None,
            genesis_vkey: None,
            download_dir: "./snapshot".to_string(),
            skip_validation: Default::default(),
            skip_download: Default::default(),
            retain_snapshot: Default::default(),
            download_start: None,
            download_end: None,
            replay: ReplayOptions::default(),
        }
    }
}

/// The aggregator to trust: what the invocation names, else the known
/// network's.
pub(crate) fn mithril_source(
    args: &Args,
    network: Option<&KnownNetwork>,
) -> miette::Result<MithrilSource> {
    let catalog = network.map(|network| &network.mithril);

    let aggregator = args
        .aggregator
        .clone()
        .or_else(|| catalog.map(|mithril| mithril.aggregator.to_owned()))
        .ok_or_else(|| {
            miette::miette!(
                "a custom network has no default Mithril aggregator; pass --aggregator (or set \
                 DOLOS_MITHRIL_AGGREGATOR)"
            )
        })?;

    let genesis_vkey = args
        .genesis_vkey
        .clone()
        .or_else(|| catalog.map(|mithril| mithril.genesis_vkey.to_owned()))
        .ok_or_else(|| {
            miette::miette!(
                "a custom network has no default Mithril genesis key; pass --genesis-vkey (or set \
                 DOLOS_MITHRIL_GENESIS_VKEY)"
            )
        })?;

    Ok(MithrilSource {
        aggregator,
        genesis_vkey,
    })
}

pub fn run(
    config: &RootConfig,
    args: &Args,
    source: &MithrilSource,
    feedback: &Feedback,
) -> miette::Result<()> {
    let target_directory = Path::new(&args.download_dir);

    if !target_directory.exists() {
        std::fs::create_dir_all(target_directory)
            .map_err(|err| miette::miette!(err.to_string()))
            .context(format!(
                "Failed to create directory: {}",
                target_directory.display()
            ))?;
    }

    if !args.skip_download {
        // Spawn a temporary Tokio runtime just for the async download
        let rt = tokio::runtime::Runtime::new()
            .into_diagnostic()
            .context("creating tokio runtime for download")?;

        let fetch = Fetch {
            download_dir: target_directory,
            skip_validation: args.skip_validation,
            download_start: args.download_start,
            download_end: args.download_end,
        };

        let receiver = Arc::new(MithrilFeedback::new(feedback));

        rt.block_on(fetch_snapshot(&fetch, source, Some(receiver)))
            .map_err(|err| miette::miette!(err.to_string()))
            .context("fetching and validating mithril snapshot")?;
    } else {
        warn!("skipping download, assuming download dir has snapshot and it's validated")
    }

    let immutable_path = Path::new(&args.download_dir).join("immutable");

    super::immutable::replay_immutabledb(config, &immutable_path, args.replay, feedback)?;

    if !args.retain_snapshot {
        info!("deleting downloaded snapshot");

        std::fs::remove_dir_all(Path::new(&args.download_dir))
            .into_diagnostic()
            .context("removing downloaded snapshot")?;
    }

    info!("bootstrap complete, run `dolos daemon` to start the node");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_cardano::networks;

    #[test]
    fn a_known_network_defaults_the_aggregator_and_key() {
        let source = mithril_source(&Args::default(), Some(&networks::PREPROD)).unwrap();

        assert_eq!(source.aggregator, networks::PREPROD.mithril.aggregator);
        assert_eq!(source.genesis_vkey, networks::PREPROD.mithril.genesis_vkey);
    }

    #[test]
    fn explicit_inputs_win_over_the_catalog() {
        let args = Args {
            aggregator: Some("https://aggregator.example".to_owned()),
            genesis_vkey: Some("5b31".to_owned()),
            ..Default::default()
        };

        assert_eq!(
            mithril_source(&args, Some(&networks::MAINNET)).unwrap(),
            MithrilSource {
                aggregator: "https://aggregator.example".to_owned(),
                genesis_vkey: "5b31".to_owned(),
            }
        );
    }

    #[test]
    fn a_custom_network_without_inputs_names_the_missing_flag() {
        let message = mithril_source(&Args::default(), None)
            .unwrap_err()
            .to_string();
        assert!(message.contains("--aggregator"), "{message}");

        let args = Args {
            aggregator: Some("https://aggregator.example".to_owned()),
            ..Default::default()
        };
        let message = mithril_source(&args, None).unwrap_err().to_string();
        assert!(message.contains("--genesis-vkey"), "{message}");
    }

    /// The prompted path parses no arguments and still reads the
    /// environment, with every other flag at its default.
    #[cfg(feature = "utils")]
    #[test]
    fn the_environment_reaches_the_flags() {
        let _guard = crate::common::ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        std::env::set_var("DOLOS_MITHRIL_AGGREGATOR", "https://aggregator.example");
        std::env::set_var("DOLOS_MITHRIL_GENESIS_VKEY", "5b31");

        let parsed = super::super::from_environment::<Args>();

        std::env::remove_var("DOLOS_MITHRIL_AGGREGATOR");
        std::env::remove_var("DOLOS_MITHRIL_GENESIS_VKEY");

        let args = parsed.unwrap();
        assert_eq!(
            args.aggregator.as_deref(),
            Some("https://aggregator.example")
        );
        assert_eq!(args.genesis_vkey.as_deref(), Some("5b31"));
        assert_eq!(args.download_dir, Args::default().download_dir);
        assert_eq!(args.replay.chunk_size, Args::default().replay.chunk_size);
    }
}
