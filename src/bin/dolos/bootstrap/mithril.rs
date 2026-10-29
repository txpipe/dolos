//! `dolos bootstrap mithril` — the interactive fetch-and-import.
//!
//! Parse and render: the download, its plan and its verification are
//! [`dolos_mithril`]'s, and the import is the ImmutableDB replay
//! `bootstrap immutabledb` runs too. What is here is the operator's
//! arguments, the runtime the async fetch is driven on and the progress bars
//! it reports through.

use dolos::storage::Removal;
use dolos_cardano::networks::KnownNetwork;
use dolos_core::config::RootConfig;
use dolos_mithril::{fetch_snapshot, Fetch, MithrilSource};
use miette::{Context, IntoDiagnostic};
use pallas::interop::hardano::storage::immutable;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::{info, warn};

use super::immutable::{ReplayOptions, Replayed};
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

    #[arg(
        long,
        default_value = "./snapshot",
        value_parser = clap::builder::NonEmptyStringValueParser::new()
    )]
    pub(crate) download_dir: String,

    /// Skip the Mithril certificate validation
    #[arg(long, action)]
    pub(crate) skip_validation: bool,

    /// Assume the snapshot is already available in the download dir
    #[arg(long, action)]
    pub(crate) skip_download: bool,

    /// Retain downloaded snapshot instead of deleting it. A replay that stops
    /// at `--stop-epoch` keeps it either way, for `--continue --skip-download`
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

/// The aggregator to trust and the download directory, once the run is
/// known to reach its stop epoch, and the directory to lie apart from what
/// the bootstrap writes and, when `clears`, clears. The run deletes that
/// directory at the end.
///
/// Under `--skip-download` the database already there must be one the
/// replay reads, as `bootstrap immutabledb` checks its source; a replay from
/// the origin over a download that starts at a later immutable file must
/// find the files before it there.
pub(crate) fn resolve(
    config: &RootConfig,
    args: &Args,
    network: Option<&KnownNetwork>,
    clears: bool,
) -> miette::Result<(MithrilSource, PathBuf)> {
    let source = mithril_source(args, network)?;

    super::immutable::check_stop_epoch(config, &args.replay)?;

    let download_dir = dolos::storage::resolve_parent_dirs(Path::new(&args.download_dir))
        .into_diagnostic()
        .context("resolving --download-dir")?;

    if args.skip_download {
        super::immutable::check_database(config, &download_dir, args.replay.resume, clears)?;
    } else {
        check_download_dir(config, &download_dir, clears)?;
        check_download_start(args, &download_dir)?;
    }

    Ok((source, download_dir))
}

/// Refuse a directory to download into that the bootstrap would write under
/// or, when `clears`, that clearing the storage root takes.
///
/// The directory and the storage root are made before the second look, so
/// that a second name for one of them, another case on a case-insensitive
/// volume, which only their file identity tells, is refused before the
/// download rather than found at the delete that ends the run.
fn check_download_dir(config: &RootConfig, dir: &Path, clears: bool) -> miette::Result<()> {
    let root = &config.storage.path;

    super::immutable::check_placement(config, &[dir])?;

    if clears
        && Removal::of(root)
            .into_diagnostic()?
            .takes(dir)
            .into_diagnostic()?
    {
        miette::bail!(
            "the download directory {} lies in the storage root {}, or is reached through it, \
             which `--force` clears before the download: download outside the storage root",
            dir.display(),
            root.display()
        );
    }

    std::fs::create_dir_all(dir)
        .into_diagnostic()
        .with_context(|| format!("creating the download directory {}", dir.display()))?;

    crate::common::ensure_storage_path(config)
        .into_diagnostic()
        .context("creating the storage directory")?;

    super::immutable::check_placement(config, &[dir])
}

/// Refuse a download that starts at a later immutable file for a replay from
/// the origin, unless the download directory already holds every file before
/// it: the download skips them, and the replay reads from the first. A
/// download given no range starts at the highest file already there.
fn check_download_start(args: &Args, download_dir: &Path) -> miette::Result<()> {
    if args.replay.resume {
        return Ok(());
    }

    let immutable = download_dir.join("immutable");

    let (start, why) = match (args.download_start, args.download_end) {
        (Some(start), _) => (start, format!("--download-start {start} skips")),
        (None, None) => match dolos_mithril::highest_existing_immutable(&immutable) {
            Some(highest) => (
                highest,
                format!(
                    "the download resumes at immutable file {highest}, the highest there, so it \
                     skips"
                ),
            ),
            None => return Ok(()),
        },
        (None, Some(_)) => return Ok(()),
    };

    for number in 0..start {
        let name = format!("{number:05}");

        if let Err(error) = immutable::chunk::read_blocks(&immutable, &name) {
            miette::bail!(
                "{why} the immutable files before {start}, and immutable file {number} in {} \
                 fails to open ({error}), which a replay from the origin reads: pass \
                 --download-start 0 to download them, or --continue to resume a node that holds \
                 them",
                immutable.display()
            );
        }
    }

    Ok(())
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
    download_dir: &Path,
    feedback: &Feedback,
) -> miette::Result<()> {
    if !download_dir.exists() {
        std::fs::create_dir_all(download_dir)
            .map_err(|err| miette::miette!(err.to_string()))
            .context(format!(
                "Failed to create directory: {}",
                download_dir.display()
            ))?;
    }

    if !args.skip_download {
        // Spawn a temporary Tokio runtime just for the async download
        let rt = tokio::runtime::Runtime::new()
            .into_diagnostic()
            .context("creating tokio runtime for download")?;

        let fetch = Fetch {
            download_dir,
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

    let immutable_path = download_dir.join("immutable");

    let replayed =
        super::immutable::replay_immutabledb(config, &immutable_path, args.replay, feedback)?;

    // A stop leaves the rest of the snapshot to replay, so it stays for the
    // resume whether or not it was to be retained.
    if replayed == Replayed::StopEpoch {
        info!(
            snapshot = %download_dir.display(),
            "stop epoch reached, keeping the snapshot for `--continue --skip-download`"
        );

        return Ok(());
    }

    if !args.retain_snapshot {
        delete_snapshot(config, download_dir)?;
    }

    info!("bootstrap complete, run `dolos daemon` to start the node");

    Ok(())
}

/// Delete the snapshot at `download_dir`, unless that would take the storage
/// root or a store: checked now, when every one of them exists, so that no
/// name for them that the checks before the download missed costs the node.
/// A snapshot that holds one is kept, and the operator told.
fn delete_snapshot(config: &RootConfig, download_dir: &Path) -> miette::Result<()> {
    if let Some((name, store)) = super::immutable::store_in(config, download_dir)? {
        eprintln!(
            "kept the snapshot at {}: deleting it would take the {name} at {}. Move the node \
             out of it before you remove it",
            download_dir.display(),
            store.display()
        );

        return Ok(());
    }

    info!("deleting downloaded snapshot");

    std::fs::remove_dir_all(download_dir)
        .into_diagnostic()
        .context("removing downloaded snapshot")
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

    fn config_over(root: &Path) -> RootConfig {
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = root.join("data");
        config
    }

    /// A downloading run into `dir`.
    fn downloading(dir: &Path) -> Args {
        Args {
            aggregator: Some("https://aggregator.invalid".to_owned()),
            genesis_vkey: Some("5b31".to_owned()),
            download_dir: dir.display().to_string(),
            ..Default::default()
        }
    }

    /// A download directory that holds the storage root, which the run would
    /// delete the node with at the end, is refused before the download
    /// whatever is to be cleared. One beside the stores in the root is
    /// refused only when `--force` is to clear the root, so that the resume
    /// over it a stop recommends is accepted as the download was.
    #[test]
    fn the_download_directory_is_checked_before_the_download() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let root = config.storage.path.clone();

        for dir in [root.clone(), temp.path().to_path_buf()] {
            for clears in [false, true] {
                let message = resolve(&config, &downloading(&dir), None, clears)
                    .unwrap_err()
                    .to_string();
                assert!(message.contains("lies in the database"), "{message}");
            }
        }

        let inside = root.join("snapshot");
        let (_, resolved) = resolve(&config, &downloading(&inside), None, false).unwrap();
        assert_eq!(resolved, inside);

        let message = resolve(&config, &downloading(&inside), None, true)
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("the download directory")
                && message.contains("which `--force` clears"),
            "{message}"
        );
    }

    /// The download directory and the storage root are made before they are
    /// compared, so that another case of the root's name on a
    /// case-insensitive volume is refused before the download.
    #[test]
    fn a_second_name_for_the_root_is_refused_before_the_download() {
        let temp = tempfile::tempdir().unwrap();
        let probe = temp.path().join("probe");
        std::fs::create_dir_all(&probe).unwrap();

        if !temp.path().join("PROBE").is_dir() {
            return;
        }

        let mut config = config_over(temp.path());
        config.storage.path = temp.path().join("cardano").join("dolos");

        let message = resolve(
            &config,
            &downloading(&temp.path().join("Cardano")),
            None,
            false,
        )
        .unwrap_err()
        .to_string();
        assert!(message.contains("lies in the database"), "{message}");
    }

    /// Write immutable files `numbers`, each holding no blocks.
    fn immutable_files(immutable: &Path, numbers: impl IntoIterator<Item = u64>) {
        std::fs::create_dir_all(immutable).unwrap();

        for number in numbers {
            std::fs::write(
                immutable.join(format!("{number:05}.primary")),
                [1, 0, 0, 0, 0],
            )
            .unwrap();
            std::fs::write(immutable.join(format!("{number:05}.secondary")), []).unwrap();
            std::fs::write(immutable.join(format!("{number:05}.chunk")), []).unwrap();
        }
    }

    /// A download from a later immutable file leaves the files before it to
    /// the download directory, so a replay from the origin is refused until
    /// each of them opens there, as a download given no range resumes at the
    /// highest file there. A resume reads on from its committed position.
    #[test]
    fn a_later_download_start_needs_the_files_before_it() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let dir = temp.path().join("snapshot");
        let immutable = dir.join("immutable");

        let mut args = downloading(&dir);
        args.download_start = Some(2);

        let refused = |args: &Args| resolve(&config, args, None, false).unwrap_err().to_string();

        let message = refused(&args);
        assert!(
            message.contains("--download-start 2 skips")
                && message.contains("immutable file 0 in")
                && message.contains("fails to open"),
            "{message}"
        );

        std::fs::create_dir_all(&immutable).unwrap();
        std::fs::write(immutable.join("00000.chunk"), []).unwrap();
        assert!(refused(&args).contains("immutable file 0 in"));

        immutable_files(&immutable, [0]);
        assert!(refused(&args).contains("immutable file 1 in"));

        immutable_files(&immutable, [1]);
        assert!(resolve(&config, &args, None, false).is_ok());

        std::fs::remove_dir_all(&dir).unwrap();
        immutable_files(&immutable, [1, 2]);

        args.download_start = None;
        let message = refused(&args);
        assert!(
            message.contains("resumes at immutable file 2")
                && message.contains("immutable file 0 in"),
            "{message}"
        );

        args.download_end = Some(4);
        assert!(resolve(&config, &args, None, false).is_ok());

        args.download_end = None;
        args.replay.resume = true;
        assert!(resolve(&config, &args, None, false).is_ok());

        args.replay.resume = false;
        args.download_start = Some(0);
        assert!(resolve(&config, &args, None, false).is_ok());
    }

    /// The end of a run deletes the snapshot, unless the storage root or a
    /// store lies in it, which the delete would take: that snapshot is kept.
    #[test]
    fn the_snapshot_is_kept_when_it_holds_a_store() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = config_over(temp.path());
        let dir = temp.path().join("snapshot");

        std::fs::create_dir_all(dir.join("immutable")).unwrap();
        config.storage.state.path = Some(dir.join("state"));
        std::fs::create_dir_all(dir.join("state")).unwrap();

        delete_snapshot(&config, &dir).unwrap();
        assert!(
            dir.join("state").is_dir(),
            "the delete took the state store"
        );

        config.storage.state.path = None;
        delete_snapshot(&config, &dir).unwrap();
        assert!(!dir.exists(), "the snapshot outlived the delete");
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
