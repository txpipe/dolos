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
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::{info, warn};

use super::immutable::{Placed, ReplayOptions, Replayed};
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
/// find the files before it there, whole.
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
        check_download_start(args, &download_dir)?;

        if clears {
            check_past_the_download_end(args, &download_dir)?;
        }

        check_download_dir(config, &download_dir, clears)?;
    }

    Ok((source, download_dir))
}

/// Refuse a directory to download into that the bootstrap would write under
/// or, when `clears`, that clearing the storage root takes, with what is
/// already in its `immutable` directory, the one the download writes and
/// the replay reads: they read and write through the links there.
///
/// The directory and the storage root are made before the second look, so
/// that a second name for one of them, another case on a case-insensitive
/// volume, which only their file identity tells, is refused before the
/// download rather than found at the delete that ends the run. What a
/// refused run made is removed again.
fn check_download_dir(config: &RootConfig, dir: &Path, clears: bool) -> miette::Result<()> {
    let check = || {
        super::immutable::check_placement(config, &[dir], Placed::Download)?;

        if clears {
            super::refuse_what_the_clear_takes(
                config,
                dir,
                &[dir.join("immutable")],
                "the download directory",
                "the download",
                "download outside the storage root, or resume the replay a stop left there with \
                 --continue --skip-download, without --force",
            )?;
        }

        miette::Result::Ok(())
    };

    check()?;

    let made = make_dirs(&[dir, &config.storage.path])?;

    check().inspect_err(|_| remove_made(&made))
}

/// Create the directories at `dirs`, and return those made that weren't
/// there, each before the ones made inside it. A directory that fails to be
/// made removes those made before it.
fn make_dirs(dirs: &[&Path]) -> miette::Result<Vec<PathBuf>> {
    let mut made = Vec::new();

    for dir in dirs {
        if let Err(error) = make_dir(dir, &mut made) {
            remove_made(&made);
            return Err(error);
        }
    }

    Ok(made)
}

/// Create the directory at `dir` a part at a time, as the OS walks it, and
/// add those made to `made`: a `..` is the OS's to take, so the directory a
/// path names through one that is missing is made once, and one already
/// there is never taken for made.
fn make_dir(dir: &Path, made: &mut Vec<PathBuf>) -> miette::Result<()> {
    use std::io::ErrorKind;
    use std::path::Component;

    let creating = || format!("creating {}", dir.display());
    let mut reached = PathBuf::new();

    for component in std::path::absolute(dir)
        .into_diagnostic()
        .with_context(creating)?
        .components()
    {
        reached.push(component);

        if !matches!(component, Component::Normal(_)) {
            continue;
        }

        match std::fs::symlink_metadata(&reached) {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                std::fs::create_dir(&reached)
                    .into_diagnostic()
                    .with_context(creating)?;

                made.push(reached.clone());
            }
            Err(error) => return Err(error).into_diagnostic().with_context(creating),
        }
    }

    // A link there that leads nowhere is no directory.
    std::fs::create_dir_all(dir)
        .into_diagnostic()
        .with_context(creating)
}

/// Remove the directories [`make_dirs`] made, each before the one it was
/// made in.
fn remove_made(made: &[PathBuf]) {
    for dir in made.iter().rev() {
        let _ = std::fs::remove_dir(dir);
    }
}

/// Refuse a download that starts at a later immutable file for a replay from
/// the origin, unless the download directory already holds every file before
/// it, whole: the download skips them, and the replay reads from the first.
/// A download given no range starts at the highest file already there.
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
        super::immutable::check_whole(&immutable, &format!("{number:05}")).with_context(|| {
            format!(
                "{why} the immutable files before {start}, and immutable file {number} in {} \
                 doesn't read whole, which a replay from the origin reads: pass \
                 --download-start 0 to download them, or --continue to resume a node that holds \
                 them",
                immutable.display()
            )
        })?;
    }

    Ok(())
}

/// Refuse a download that ends before immutable files already in the
/// download directory, unless each of them the replay reads reads whole: the
/// download leaves them as they are, and the replay reads on into them, to
/// every file but the last. Checked under a clear, so that a cut one is
/// found before the node goes, as one before a later start is.
fn check_past_the_download_end(args: &Args, download_dir: &Path) -> miette::Result<()> {
    let Some(end) = args.download_end else {
        return Ok(());
    };

    let immutable = download_dir.join("immutable");

    let Some(last) = super::immutable::last_chunk(&immutable) else {
        return Ok(());
    };

    for number in end.saturating_add(1)..last {
        super::immutable::check_whole(&immutable, &format!("{number:05}")).with_context(|| {
            format!(
                "--download-end {end} leaves the immutable files after it as they are, and \
                 immutable file {number} in {} doesn't read whole, which the replay reads on \
                 into up to file {last}, the last there: remove the files after {end}, or pass \
                 a later --download-end to download them",
                immutable.display()
            )
        })?;
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
    /// whatever is to be cleared, and before anything is made. One beside the
    /// stores in the root is refused only when `--force` is to clear the
    /// root, so that the resume over it a stop recommends is accepted as the
    /// download was.
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
                assert!(
                    message.contains("lies in the download directory")
                        && message.contains("--download-dir"),
                    "{message}"
                );
                assert!(!root.exists(), "the refusal made the storage root");
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
                && message.contains("which `--force` clears")
                && message.contains("download outside the storage root")
                && message.contains("--continue --skip-download, without --force"),
            "{message}"
        );
    }

    /// The download directory and the storage root are made before they are
    /// compared, so that another case of the root's name on a
    /// case-insensitive volume is refused before the download. The refusal
    /// removes what it made, one level or several deep.
    #[test]
    fn a_second_name_for_the_root_is_refused_before_the_download() {
        let cases: [(&[&str], &[&str]); 2] = [
            (&["Cardano"], &["cardano", "dolos"]),
            (&["Cardano", "preview"], &["cardano", "preview", "dolos"]),
        ];

        for (download, root) in cases {
            let temp = tempfile::tempdir().unwrap();
            let probe = temp.path().join("probe");
            std::fs::create_dir_all(&probe).unwrap();

            if !temp.path().join("PROBE").is_dir() {
                return;
            }

            let mut config = config_over(temp.path());
            config.storage.path = root
                .iter()
                .fold(temp.path().to_path_buf(), |p, c| p.join(c));
            let dir = download
                .iter()
                .fold(temp.path().to_path_buf(), |p, c| p.join(c));

            let message = resolve(&config, &downloading(&dir), None, false)
                .unwrap_err()
                .to_string();
            assert!(
                message.contains("lies in the download directory"),
                "{download:?}: {message}"
            );
            assert!(
                !temp.path().join("cardano").exists(),
                "{download:?}: the refusal left what it made"
            );
        }
    }

    /// What is made through a `..` out of a directory that is missing is
    /// the missing directory alone: one already there that the path then
    /// names is no more made than it is removed again.
    #[test]
    fn a_directory_already_there_is_never_taken_for_made() {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let through = temp.path().join("missing").join("..").join("data");

        let made = make_dirs(&[&through]).unwrap();
        assert!(made.iter().all(|dir| !dir.ends_with("data")), "{made:?}");

        remove_made(&made);
        assert!(
            data.is_dir(),
            "removing what was made took {}",
            data.display()
        );
        assert!(!temp.path().join("missing").exists());
    }

    /// A directory that fails to be made, as a storage root that is a link
    /// leading nowhere does, removes those made before it.
    #[cfg(unix)]
    #[test]
    fn a_directory_that_fails_to_be_made_removes_those_made_before_it() {
        let temp = tempfile::tempdir().unwrap();
        let dangling = temp.path().join("data");
        std::os::unix::fs::symlink(temp.path().join("nowhere"), &dangling).unwrap();

        let mut config = config_over(temp.path());
        config.storage.path = dangling;
        let dir = temp.path().join("newdl");

        assert!(resolve(&config, &downloading(&dir), None, false).is_err());
        assert!(!dir.exists(), "the failure left {}", dir.display());
    }

    /// A download directory named through `..` out of the storage root is
    /// the directory beside it, which `--force` doesn't take.
    #[test]
    fn a_download_directory_named_through_the_root_lies_beside_it() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        std::fs::create_dir_all(&config.storage.path).unwrap();

        let through = config.storage.path.join("..").join("snapshot");
        let (_, resolved) = resolve(&config, &downloading(&through), None, true).unwrap();

        assert_eq!(
            std::fs::canonicalize(resolved).unwrap(),
            std::fs::canonicalize(temp.path().join("snapshot")).unwrap()
        );
    }

    /// Under `--force`, the download directory is read through the links
    /// already in it, which the download reads and writes through: one whose
    /// `immutable` directory, or a file in it, links into the storage root is
    /// refused before the clear, naming the link, and so is one whose
    /// `immutable` directory links to a directory elsewhere whose files link
    /// there, with a range and without. Without a clear, it lies beside the
    /// stores. A link back to the directory it is in is read once.
    #[cfg(unix)]
    #[test]
    fn under_a_clear_the_download_directory_is_read_through_its_links() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let kept = config.storage.path.join("snapshot").join("immutable");
        immutable_files(&kept, [0, 1]);

        let linked = temp.path().join("linked");
        std::fs::create_dir_all(&linked).unwrap();
        symlink(&kept, linked.join("immutable")).unwrap();

        let link_each_file = |dir: &Path| {
            std::fs::create_dir_all(dir).unwrap();
            for entry in std::fs::read_dir(&kept).unwrap() {
                let file = entry.unwrap().path();
                symlink(&file, dir.join(file.file_name().unwrap())).unwrap();
            }
        };

        let each = temp.path().join("each");
        link_each_file(&each.join("immutable"));

        let nested = temp.path().join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        link_each_file(&temp.path().join("outside").join("imm"));
        symlink(
            temp.path().join("outside").join("imm"),
            nested.join("immutable"),
        )
        .unwrap();

        for (dir, link) in [
            (&linked, linked.join("immutable")),
            (&each, each.join("immutable")),
            (&nested, nested.join("immutable")),
        ] {
            for start in [Some(2), None] {
                let mut args = downloading(dir);
                args.download_start = start;

                let message = resolve(&config, &args, None, true).unwrap_err().to_string();
                assert!(
                    message.contains(&format!("the link {}", link.display()))
                        && message.contains("which `--force` clears"),
                    "{start:?}: {message}"
                );

                assert!(resolve(&config, &args, None, false).is_ok(), "{start:?}");
            }
        }

        let looped = temp.path().join("looped");
        immutable_files(&looped.join("immutable"), [0, 1]);
        symlink(
            looped.join("immutable"),
            looped.join("immutable").join("loop"),
        )
        .unwrap();
        resolve(&config, &downloading(&looped), None, true).unwrap();
    }

    /// Under `--force`, only the download directory's `immutable` directory
    /// is read, the one the download writes and the replay reads: what lies
    /// beside it, as a volume's `lost+found` or `.Trashes`, is never read,
    /// whatever links it holds. In it, a directory that can't be searched
    /// holds nothing the run reads, and nor does a link through one, so both
    /// are passed over. One that can be searched but not listed is refused,
    /// since what is reached through it can't be told, and so is a download
    /// directory that can't be listed whose `immutable` directory links into
    /// the root. Any other error reading the source is named as it is.
    #[cfg(unix)]
    #[test]
    fn under_a_clear_a_directory_that_cant_be_listed_is_refused_unless_it_cant_be_searched() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let kept = config.storage.path.join("snapshot").join("immutable");
        immutable_files(&kept, [0, 1]);

        let volume = temp.path().join("volume");
        std::fs::create_dir_all(volume.join("immutable")).unwrap();
        let lost = volume.join("lost+found");
        std::fs::create_dir_all(&lost).unwrap();
        let trashes = volume.join(".Trashes");
        std::fs::create_dir_all(&trashes).unwrap();
        symlink(&kept, trashes.join("kept")).unwrap();

        let unsearched = temp.path().join("unsearched");
        let locked = unsearched.join("immutable").join("lost+found");
        std::fs::create_dir_all(&locked).unwrap();

        let through = temp.path().join("through");
        std::fs::create_dir_all(through.join("immutable")).unwrap();
        let sealed = temp.path().join("outside").join("sealed");
        let tree = temp.path().join("outside").join("tree");
        std::fs::create_dir_all(&sealed).unwrap();
        std::fs::create_dir_all(&tree).unwrap();
        symlink(sealed.join("kept"), tree.join("kept")).unwrap();
        symlink(&tree, through.join("immutable").join("extra")).unwrap();

        let searched = temp.path().join("searched");
        std::fs::create_dir_all(&searched).unwrap();
        symlink(&kept, searched.join("immutable")).unwrap();

        let unlisted = temp.path().join("unlisted").join("immutable");
        std::fs::create_dir_all(&unlisted).unwrap();
        symlink(kept.join("00000.chunk"), unlisted.join("00000.chunk")).unwrap();

        let mode = |dir: &Path, mode| {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).unwrap()
        };

        let modes = [
            (&lost, 0o000),
            (&trashes, 0o1311),
            (&locked, 0o000),
            (&sealed, 0o000),
            (&searched, 0o311),
            (&unlisted, 0o311),
        ];

        for (dir, bits) in modes {
            mode(dir, bits);
        }

        // Permissions don't bind a superuser, who lists them all anyway.
        let bound = std::fs::read_dir(&lost).is_err();

        let passed = [&volume, &unsearched, &through]
            .map(|dir| resolve(&config, &downloading(dir), None, true).map(drop));
        let linked = resolve(&config, &downloading(&searched), None, true).map(drop);
        let refused = resolve(
            &config,
            &downloading(unlisted.parent().unwrap()),
            None,
            true,
        )
        .map(drop);

        for (dir, _) in modes {
            mode(dir, 0o755);
        }

        let said = |result: miette::Result<()>| {
            result
                .unwrap_err()
                .chain()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(": ")
        };

        let message = said(linked);
        assert!(
            message.contains("which `--force` clears")
                && message.contains(&format!(
                    "the link {}",
                    searched.join("immutable").display()
                )),
            "{message}"
        );

        let file = temp.path().join("snapfile");
        std::fs::write(&file, []).unwrap();
        let message = said(resolve(&config, &downloading(&file), None, true).map(drop));
        assert!(
            message.contains(&format!("reading {}", file.join("immutable").display()))
                && !message.contains("can be searched"),
            "{message}"
        );

        if !bound {
            return;
        }

        for result in passed {
            result.unwrap();
        }

        let message = said(refused);
        assert!(
            message.contains("which `--force` clears")
                && message.contains("can be searched but not listed")
                && message.contains(&unlisted.display().to_string())
                && message.contains("make it readable"),
            "{message}"
        );
    }

    /// Under `--force`, the immutable files a download that ends early
    /// leaves as they are must read whole, every one the replay reads on
    /// into, which is all but the last file there: a cut or missing one is
    /// refused before anything is cleared, with a range and with only an
    /// end, while the last, here the empty chunk a node is still writing,
    /// is never read. Without a clear, the replay finds out.
    #[test]
    fn under_a_clear_the_files_past_the_download_end_read_whole() {
        use dolos_testing::{
            blocks::write_immutable_chunks,
            synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
        };

        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let dir = temp.path().join("snapshot");
        let immutable = dir.join("immutable");
        std::fs::create_dir_all(&immutable).unwrap();

        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 4,
            slot: 0,
            start_block: 0,
            ..Default::default()
        });
        let write = || {
            write_immutable_chunks(
                &immutable,
                &[&blocks[..1], &blocks[1..2], &blocks[2..3], &blocks[3..]],
            )
        };

        let cut = |number: u64| {
            let path = immutable.join(format!("{number:05}.chunk"));
            let bytes = std::fs::read(&path).unwrap();
            std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
        };

        let ending = |start: Option<u64>, end: u64| {
            let mut args = downloading(&dir);
            args.download_start = start;
            args.download_end = Some(end);
            args
        };

        let refused = |args: &Args| {
            let report = resolve(&config, args, None, true).unwrap_err();
            report
                .chain()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(": ")
        };

        for args in [ending(Some(0), 1), ending(None, 1)] {
            write();
            assert!(resolve(&config, &args, None, true).is_ok());

            cut(2);
            let message = refused(&args);
            assert!(
                message.contains("--download-end 1 leaves")
                    && message.contains("immutable file 2 in")
                    && message.contains("doesn't read whole")
                    && message.contains("up to file 4"),
                "{message}"
            );
            assert!(resolve(&config, &args, None, false).is_ok());

            assert!(resolve(&config, &ending(args.download_start, 2), None, true).is_ok());
        }

        write();
        cut(1);
        assert!(refused(&ending(None, 0)).contains("immutable file 1 in"));

        write();
        for extension in ["chunk", "primary", "secondary"] {
            std::fs::remove_file(immutable.join(format!("00002.{extension}"))).unwrap();
        }
        assert!(refused(&ending(None, 1)).contains("immutable file 2 in"));

        write();
        cut(3);
        assert!(refused(&ending(None, 1)).contains("immutable file 3 in"));
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
    /// highest file there, and before anything is made. A resume reads on
    /// from its committed position.
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
                && message.contains("doesn't read whole"),
            "{message}"
        );
        assert!(
            !dir.exists() && !config.storage.path.exists(),
            "the refusal made directories"
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

        std::fs::remove_dir_all(&dir).unwrap();
        immutable_files(&immutable, [0, 2]);
        let message = refused(&args);
        assert!(
            message.contains("resumes at immutable file 2")
                && message.contains("immutable file 1 in"),
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

    /// The files a later download start skips are read to their ends, so
    /// that one cut short, as an overwritten or interrupted file leaves it,
    /// is refused before `--force` clears anything rather than found by the
    /// replay after it: a chunk cut with its index files intact, and a cut
    /// secondary index, through either way of starting later.
    #[test]
    fn the_files_before_a_later_start_read_whole() {
        use dolos_testing::{
            blocks::write_immutable_chunks,
            synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
        };

        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let dir = temp.path().join("snapshot");
        let immutable = dir.join("immutable");
        std::fs::create_dir_all(&immutable).unwrap();

        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 4,
            slot: 0,
            start_block: 0,
            ..Default::default()
        });
        let write = || write_immutable_chunks(&immutable, &[&blocks[..2], &blocks[2..]]);

        let refused = |args: &Args| {
            let report = resolve(&config, args, None, true).unwrap_err();
            report
                .chain()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(": ")
        };

        let cut = |file: &str, to: usize| {
            let path = immutable.join(file);
            let bytes = std::fs::read(&path).unwrap();
            std::fs::write(&path, &bytes[..to]).unwrap();
        };

        let mut later = downloading(&dir);
        later.download_start = Some(2);
        let resumed = downloading(&dir);

        for args in [&later, &resumed] {
            write();
            assert!(resolve(&config, args, None, true).is_ok());

            cut("00001.chunk", blocks[2].len() / 2);
            let message = refused(args);
            assert!(
                message.contains("immutable file 1 in")
                    && message.contains("doesn't read whole")
                    && message.contains("before its last block"),
                "{message}"
            );

            write();
            cut("00001.secondary", 60);
            let message = refused(args);
            assert!(
                message.contains("immutable file 1 in")
                    && message.contains("00001.secondary holds 60 bytes"),
                "{message}"
            );
        }
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
