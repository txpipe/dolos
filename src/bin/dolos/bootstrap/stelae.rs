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

use dolos_snapshot::registry::{self, Point, Repository};

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

    /// directory to stage pulled layers in; defaults to
    /// `<storage.path>/scratch`. registry sources only — a `file://` restore
    /// stages nothing
    #[arg(long, value_name = "DIR")]
    pub scratch_dir: Option<PathBuf>,
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
            scratch_dir: None,
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
    Repo(Repository),
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

        if raw.starts_with(registry::SCHEME) {
            return raw
                .parse::<Repository>()
                .map(Self::Repo)
                .map_err(|e| e.to_string());
        }

        Err(format!(
            "{raw:?} is not a stele source; it is `file://DIR` or `{}HOST/PATH`",
            registry::SCHEME,
        ))
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

fn restore_repo(
    config: &RootConfig,
    repo: &Repository,
    point: Point,
    insecure: bool,
    scratch_dir: Option<&std::path::Path>,
    resume: bool,
) -> miette::Result<()> {
    // First: `Node::open` runs `ensure_storage_path`, so the default of
    // `<storage.path>/scratch` needs no special case on a host where the
    // storage directory does not exist yet.
    let node = Node::open(config)?;

    // Resolved here rather than inside the transport: which identity this node
    // reads a registry as is the node's policy, and `crate::common` is where
    // this program keeps its own. Where it stages comes from the same place.
    let auth = crate::common::stele_registry_auth(&config.stelae)?;

    let scratch = crate::common::stele_scratch_dir(&config.storage, scratch_dir);

    let registry = registry::open(repo, insecure, auth, scratch)
        .into_diagnostic()
        .context("opening the repository")?;

    println!("source:   {repo} ({point})");

    let (plan, outlook, summary) =
        registry::restore_registry(&registry, point, node.restoring(resume), node.target())
            .into_diagnostic()
            .context("restoring the stele")?;

    report(&plan, &outlook, &summary);

    Ok(())
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
        Source::Repo(repo) => restore_repo(
            config,
            repo,
            args.point,
            args.insecure,
            args.scratch_dir.as_deref(),
            resume,
        ),
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

        assert_eq!(repo.registry(), "ghcr.io");
        assert_eq!(repo.repository(), "txpipe/dolos-snapshots/mainnet");
    }

    /// A source this command cannot use is refused by the parse, not carried to
    /// the registry — the whole reason `--source` is a parsed type, and what
    /// makes the refusal land before `--force` clears anything.
    ///
    /// Only the scheme dispatch is this module's. What makes a *repository*
    /// usable is the transport's and is tested there; these are the two cases
    /// that get here either way, plus one that proves an unusable repository
    /// does propagate.
    #[test]
    fn an_unusable_source_is_refused() {
        for raw in [
            "https://example.invalid/snapshot", // not a scheme this understands
            "/var/lib/dolos/stele",             // a path is not a URL
            "file://",
            "",
            // And a repository the transport refuses is refused here too,
            // rather than being carried as far as a connection.
            "oci://ghcr.io",
            "oci://ghcr.io/txpipe/dolos:v1",
        ] {
            assert!(raw.parse::<Source>().is_err(), "{raw:?}");
        }
    }
}
