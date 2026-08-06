//! `dolos bootstrap stelae` — rebuild a node from a Stelae snapshot.
//!
//! A sibling of [`super::snapshot`] rather than a mode of it. The two share a
//! goal and nothing else: one unpacks a gzip tar of the storage engines' own
//! files over the data directory, the other reads a set of deterministic CBOR
//! layers through the store traits. Their flags, their failure modes and their
//! trust stories are all different, so folding the second into the first would
//! have meant a command whose options only make sense in pairs.
//!
//! Everything below this module is `dolos_snapshot::restore`, which is generic
//! over the store traits. What only this module knows is the node: its network
//! magic, which comes from genesis and never from a file an operator can edit,
//! and its `sync.max_history`, which bounds how much chain history a restore
//! bothers to read.
//!
//! ## The two sources are one restore
//!
//! `file://DIR` and `oci://HOST/PATH` differ in where the bytes come from and in
//! nothing else: the same plan, the same refusals, the same store writes, the
//! same checkpoints. Only two things are the registry's alone — `--point`,
//! which names which stele in a repository to read, and `--insecure`, which is
//! for a registry on a loopback address.
//!
//! ## `--continue` is the resume
//!
//! `bootstrap`'s `--continue` already meant "go ahead even though there is data
//! here, the subcommand knows how to resume". For a stele restore that is now
//! literally true: it is what makes the run consult the progress file an
//! interrupted attempt left in the storage directory, so the layers that
//! attempt committed are not fetched again.
//!
//! Without it a restore starts over, and it starts over *properly* — a progress
//! file it did not ask to honour is overwritten rather than obeyed.
//! `dolos_snapshot::restore::Checkpoint` states why: a progress file that
//! outlived the stores it describes would otherwise skip layers onto nothing.

use std::path::PathBuf;

use dolos_core::config::RootConfig;
use miette::{Context as _, IntoDiagnostic as _};

use crate::repo::RepoRef;

#[derive(Debug, clap::Args, Clone)]
pub struct Args {
    /// Where to restore from, as a URL: `file://DIR` naming a stele directory,
    /// or `oci://HOST/PATH` naming a repository in a registry.
    #[arg(long)]
    pub source: Source,

    /// which stele in the repository to restore: `latest`, or `epoch-N` for the
    /// stele published at the end of epoch N. Registry sources only.
    #[arg(long, value_name = "POINT", default_value = "latest")]
    pub point: Point,

    /// talk to the repository over plaintext HTTP rather than HTTPS; for a
    /// registry on a loopback address or a mirror inside a cluster, and for
    /// nothing reachable from outside one
    #[arg(long, action)]
    pub insecure: bool,
}

impl Args {
    pub fn inquire() -> miette::Result<Self> {
        let source = inquire::Text::new("where is the stele?")
            .with_help_message(
                "a directory written by `dolos snapshot publish --output-dir`, or an OCI \
                 repository",
            )
            .with_placeholder("oci://ghcr.io/txpipe/dolos-snapshots/mainnet")
            .prompt()
            .into_diagnostic()?;

        Ok(Self {
            source: source.parse().map_err(|e: String| miette::miette!("{e}"))?,
            point: Point::default(),
            insecure: false,
        })
    }
}

/// Where a `--source` points.
///
/// The scheme is what selects a restore path, which is why this is parsed
/// rather than sniffed: a directory that happens to look like a stele and a URL
/// that says it is one are different claims, and only the second is the
/// operator's.
///
/// Parsed by clap rather than inside [`run`], so an unusable source is refused
/// before `--force` has cleared anything. The flags that decide what to do with
/// existing data are handled a layer above this command, and a source rejected
/// any later would have cost the operator the node they still had.
#[derive(Debug, Clone)]
pub enum Source {
    /// A stele directory on this filesystem.
    Dir(PathBuf),
    /// A stele repository in an OCI registry.
    Repo(RepoRef),
}

impl std::str::FromStr for Source {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // `file:///abs/path` is the spelled-out form and leaves a leading slash
        // behind, which is the absolute path. `file://relative/path` is the one
        // an operator actually types, and leaves a relative one. Both work, and
        // neither is guessed at: what follows the scheme is the path.
        if let Some(path) = raw.strip_prefix("file://") {
            if path.is_empty() {
                return Err(format!("{raw:?} names no directory"));
            }

            return Ok(Self::Dir(PathBuf::from(path)));
        }

        if raw.starts_with("oci://") {
            return raw.parse::<RepoRef>().map(Self::Repo);
        }

        Err(format!(
            "{raw:?} is not a stele source; it is `file://DIR` or `oci://HOST/PATH`"
        ))
    }
}

/// Which stele in a repository to read.
///
/// A thin newtype over the profile's own [`dolos_snapshot::registry::Point`] in
/// a build that has one, and a parsed-but-unusable value in a build that does
/// not. The parse has to exist either way — clap needs a type for the flag, and
/// a flag that only parses under a feature would be a different command
/// depending on how it was built — so the refusal lands in [`run`], where it
/// can name the feature.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Point {
    #[default]
    Latest,
    Epoch(u64),
}

impl std::str::FromStr for Point {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if raw == "latest" {
            return Ok(Self::Latest);
        }

        if let Some(epoch) = raw.strip_prefix("epoch-") {
            return epoch
                .parse::<u64>()
                .map(Self::Epoch)
                .map_err(|e| format!("{raw:?} does not name an epoch: {e}"));
        }

        Err(format!(
            "{raw:?} is not a point in a stele repository; it is `latest` or `epoch-N`"
        ))
    }
}

impl std::fmt::Display for Point {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Latest => write!(f, "latest"),
            Self::Epoch(epoch) => write!(f, "epoch-{epoch}"),
        }
    }
}

/// What every restore needs before it can read a layer, whatever the source is.
struct Node {
    root: PathBuf,
    stores: crate::common::Stores,
    magic: u64,
    max_history: Option<u64>,
}

impl Node {
    fn open(config: &RootConfig) -> miette::Result<Self> {
        let root = crate::common::ensure_storage_path(config)
            .into_diagnostic()
            .context("creating the storage directory")?;

        let stores = crate::common::open_data_stores(config)
            .into_diagnostic()
            .context("opening the data stores")?;

        let genesis = crate::common::open_genesis_files(&config.genesis)?;

        Ok(Self {
            root,
            stores,
            magic: u64::from(genesis.network_magic()),
            max_history: config.sync.max_history,
        })
    }

    /// What this node knows about itself, as the restore driver takes it.
    fn restoring(&self, resume: bool) -> dolos_snapshot::restore::Restoring<'_> {
        dolos_snapshot::restore::Restoring {
            network_magic: self.magic,
            max_history: self.max_history,
            storage_path: &self.root,
            resume,
        }
    }

    /// Where the restore writes.
    fn target(
        &self,
    ) -> dolos_snapshot::restore::Target<
        '_,
        impl dolos_core::ArchiveStore,
        impl dolos_core::StateStore,
        impl dolos_core::IndexStore,
    > {
        dolos_snapshot::restore::Target::new(
            &self.stores.archive,
            &self.stores.state,
            &self.stores.indexes,
        )
    }
}

fn restore_dir(config: &RootConfig, dir: &std::path::Path, resume: bool) -> miette::Result<()> {
    let node = Node::open(config)?;

    let (plan, outlook, summary) =
        dolos_snapshot::restore::restore_dir(dir, node.restoring(resume), node.target())
            .into_diagnostic()
            .context("restoring the stele")?;

    report(&plan, &outlook, &summary);

    Ok(())
}

#[cfg(feature = "registry")]
fn restore_repo(
    config: &RootConfig,
    repo: &RepoRef,
    point: Point,
    insecure: bool,
    resume: bool,
) -> miette::Result<()> {
    use dolos_snapshot::registry;

    let node = Node::open(config)?;

    let registry = registry::open(&repo.host, &repo.repository, insecure)
        .into_diagnostic()
        .context("opening the repository")?;

    let point = match point {
        Point::Latest => registry::Point::Latest,
        Point::Epoch(epoch) => registry::Point::Epoch(epoch),
    };

    println!("source:   {repo} ({point})");

    let (plan, outlook, summary) =
        registry::restore_registry(&registry, point, node.restoring(resume), node.target())
            .into_diagnostic()
            .context("restoring the stele")?;

    report(&plan, &outlook, &summary);

    Ok(())
}

/// The same command in a build that has no registry client.
///
/// A clean refusal naming the feature, rather than a source that parses and
/// then does nothing. The `registry` feature is default-off because the TLS
/// stack it brings needs `cmake` to build.
#[cfg(not(feature = "registry"))]
fn restore_repo(
    _config: &RootConfig,
    repo: &RepoRef,
    _point: Point,
    _insecure: bool,
    _resume: bool,
) -> miette::Result<()> {
    miette::bail!(
        "cannot restore from {repo}: this build has no registry client; \
         rebuild dolos with `--features registry`",
    )
}

/// What the run did, in the numbers an operator checks.
///
/// The remaining-download figures are printed *after* the restore rather than
/// before it, and that is a gap rather than a choice: rendering progress while
/// it runs is the observer seam this command shares with `snapshot publish`,
/// and it belongs to whichever of the two follow-up slices lands first. What is
/// here is the arithmetic that seam will read, and a report that at least says
/// what a resumed run cost rather than what an unresumed one would have.
fn report(
    plan: &dolos_snapshot::restore::Plan,
    outlook: &dolos_snapshot::restore::Outlook,
    summary: &dolos_snapshot::restore::Summary,
) {
    println!(
        "network:  {} ({})",
        plan.position.network.name(),
        plan.position.network.magic()
    );
    println!("cursor:   {}", plan.position.point);
    println!("sequence: {}", plan.sequence);

    if plan.skipped_epochs > 0 {
        println!(
            "epochs:   {} restored, {} skipped by sync.max_history",
            plan.epochs.len(),
            plan.skipped_epochs,
        );
    } else {
        println!("epochs:   {}", plan.epochs.len());
    }

    if outlook.inherited > 0 {
        println!(
            "resumed:  {} layer(s) an earlier attempt had already committed",
            outlook.inherited,
        );
    }

    println!(
        "fetched:  {} layers ({} skipped), {} compressed bytes planned",
        summary.layers_fetched, summary.layers_skipped, outlook.remaining.compressed_bytes,
    );

    println!(
        "restored: {} blocks, {} logs, {} index records, {} entities, {} utxos",
        summary.blocks, summary.logs, summary.index_records, summary.entities, summary.utxos,
    );
}

pub fn run(config: &RootConfig, args: &Args, resume: bool) -> miette::Result<()> {
    match &args.source {
        Source::Dir(dir) => restore_dir(config, dir, resume),
        Source::Repo(repo) => restore_repo(config, repo, args.point, args.insecure, resume),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_source_names_a_directory() {
        for (raw, expected) in [
            ("file:///var/lib/dolos/stele", "/var/lib/dolos/stele"),
            ("file://stele", "stele"),
            ("file://./stele", "./stele"),
        ] {
            let Source::Dir(dir) = raw.parse::<Source>().unwrap() else {
                panic!("{raw:?} did not parse as a directory");
            };

            assert_eq!(dir, PathBuf::from(expected), "{raw:?}");
        }
    }

    #[test]
    fn an_oci_source_names_a_repository() {
        let Source::Repo(repo) = "oci://ghcr.io/txpipe/dolos-snapshots/mainnet"
            .parse::<Source>()
            .unwrap()
        else {
            panic!("an oci url did not parse as a repository");
        };

        assert_eq!(repo.host, "ghcr.io");
        assert_eq!(repo.repository, "txpipe/dolos-snapshots/mainnet");
    }

    /// A malformed repository is refused by the source parse, not carried to
    /// the registry — the whole reason `--source` is a parsed type.
    #[test]
    fn an_unusable_source_is_refused() {
        for raw in [
            "oci://ghcr.io",                    // no repository path
            "oci://ghcr.io/txpipe/dolos:v1",    // a tag; `--point` names steles
            "https://example.invalid/snapshot", // not a scheme this understands
            "/var/lib/dolos/stele",             // a path is not a URL
            "file://",
            "",
        ] {
            assert!(raw.parse::<Source>().is_err(), "{raw:?}");
        }
    }

    /// The CLI's point and the profile's agree on their spelling.
    ///
    /// Two types rather than one because the profile's lives behind the
    /// `registry` feature and this flag must parse in every build. That makes
    /// drift possible, so it is pinned: `--point epoch-500` here and
    /// `registry::Point::Epoch(500)` there have to print the same tag.
    #[test]
    fn a_point_names_latest_or_an_epoch() {
        assert_eq!("latest".parse::<Point>().unwrap(), Point::Latest);
        assert_eq!("epoch-500".parse::<Point>().unwrap(), Point::Epoch(500));
        assert_eq!(Point::default(), Point::Latest);

        assert_eq!(Point::Latest.to_string(), "latest");
        assert_eq!(Point::Epoch(500).to_string(), "epoch-500");

        for raw in ["epoch", "epoch-", "epoch-abc", "500", "Latest", ""] {
            assert!(raw.parse::<Point>().is_err(), "{raw:?}");
        }
    }

    #[cfg(feature = "registry")]
    #[test]
    fn the_two_points_render_the_same_tag() {
        use dolos_snapshot::registry;

        assert_eq!(
            Point::Latest.to_string(),
            registry::Point::Latest.to_string()
        );

        for epoch in [0, 1, 500] {
            assert_eq!(
                Point::Epoch(epoch).to_string(),
                registry::Point::Epoch(epoch).to_string(),
            );
        }
    }
}
