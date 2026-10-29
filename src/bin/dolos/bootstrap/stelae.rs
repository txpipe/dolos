//! `dolos bootstrap stelae` — rebuild a node from a Stelae snapshot.
//!
//! Everything below this module is `dolos_snapshot::restore`, which is generic
//! over the store traits. What only this module knows is the node: its network
//! magic, which comes from genesis and never from a file an operator can edit,
//! and its `history.window`, which bounds how much chain history a restore
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
//! ## A restore has no resume
//!
//! An interrupted restore leaves data in storage, which `bootstrap` refuses
//! until `--force` clears it, so a restore always starts from empty stores. The
//! progress file it writes as it goes is overwritten rather than obeyed:
//! `dolos_snapshot::restore::Checkpoint` states why a progress file that
//! outlived the stores it describes would otherwise skip layers onto nothing.

use std::path::PathBuf;

use dolos_cardano::networks::KnownNetwork;
use dolos_core::config::RootConfig;
use miette::{Context as _, IntoDiagnostic as _};

use dolos_snapshot::{
    facade::{self as snapshot, RestoreInput, SnapshotRepository},
    node,
    registry::{self, Auth, Point, Repository},
    restore::Source,
};

use crate::{
    feedback::{Feedback, SteleProgress},
    registry::CredentialArgs,
};

#[derive(Debug, clap::Args, Clone)]
pub struct Args {
    /// Where to restore from, as a URL: `file://DIR` naming a stele directory,
    /// or `oci://HOST/PATH` naming a repository in a registry. Defaults to the
    /// configured network's official repository, and is required for a custom
    /// network
    #[arg(long)]
    pub source: Option<Source>,

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

    /// skip the restore disk-space preflight and continue even when the
    /// stele's declared sizes exceed the available space
    #[arg(long, action)]
    pub skip_space_check: bool,

    #[command(flatten)]
    pub credentials: CredentialArgs,
}

impl Args {
    #[cfg(feature = "utils")]
    pub fn inquire() -> miette::Result<Self> {
        let source = inquire::Text::new("where is the stele?")
            .with_help_message(
                "a directory stele written by the profile encoder, or an OCI repository",
            )
            .with_placeholder("oci://ghcr.io/txpipe/dolos-snapshots/mainnet")
            .prompt()
            .into_diagnostic()?;

        Ok(Self {
            source: Some(source.parse().map_err(|e: String| miette::miette!("{e}"))?),
            point: Point::default(),
            insecure: false,
            scratch_dir: None,
            skip_space_check: false,
            credentials: super::from_environment()?,
        })
    }
}

/// The stele to restore: what the invocation names, else the known network's
/// official repository.
pub(super) fn source(args: &Args, network: Option<&KnownNetwork>) -> miette::Result<Source> {
    if let Some(source) = &args.source {
        return Ok(source.clone());
    }

    let network = network.ok_or_else(|| {
        miette::miette!("a custom network has no default stele repository; pass --source")
    })?;

    network
        .stelae
        .registry
        .parse()
        .map_err(|e: String| miette::miette!("{e}"))
}

/// Where a restore reads from, with the credentials a repository is read
/// with.
pub(super) enum Inputs {
    Dir(PathBuf),
    Repo(Repository, Auth),
}

/// Resolves the source and, for a repository, the credentials, so that
/// `--force` clears nothing for a restore that would refuse them, and the
/// restore reads with the credentials that were checked.
pub(super) fn resolve(args: &Args, network: Option<&KnownNetwork>) -> miette::Result<Inputs> {
    match source(args, network)? {
        Source::Dir(dir) => Ok(Inputs::Dir(dir)),
        Source::Repo(repo) => {
            let auth = args.credentials.auth(&repo, network)?;

            Ok(Inputs::Repo(repo, auth))
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

#[derive(Clone, Copy)]
struct RestoreOptions<'a> {
    feedback: &'a Feedback,
    skip_space_check: bool,
}

impl Node {
    fn open(config: &RootConfig) -> miette::Result<Self> {
        let root = crate::common::ensure_storage_path(config)
            .into_diagnostic()
            .context("creating the storage directory")?;

        let genesis = crate::common::open_genesis(config)?;

        let stores = crate::common::open_data_stores(config)
            .into_diagnostic()
            .context("opening the data stores")?;

        Ok(Self {
            root,
            stores,
            magic: u64::from(genesis.network_magic()),
            max_history: config.history.max_slots(),
        })
    }

    /// What this node knows about itself, as the restore driver takes it.
    fn restoring(&self, skip_space_check: bool) -> dolos_snapshot::restore::Restoring<'_> {
        dolos_snapshot::restore::Restoring {
            network_magic: self.magic,
            max_history: self.max_history,
            storage_path: &self.root,
            resume: false,
            skip_space_check,
        }
    }

    /// Where the restore writes.
    fn target(
        &self,
    ) -> dolos_snapshot::restore::Target<
        '_,
        impl dolos_core::ArchiveStore,
        impl dolos_core::StateStore,
    > {
        dolos_snapshot::restore::Target::new(&self.stores.archive, &self.stores.state)
    }
}

fn restore(
    config: &RootConfig,
    args: &Args,
    inputs: Inputs,
    options: RestoreOptions<'_>,
) -> miette::Result<()> {
    let node = Node::open(config)?;
    let progress = SteleProgress::restoring(options.feedback);
    let observer = progress.observer();
    let restoring = node.restoring(options.skip_space_check);

    let outcome = match inputs {
        Inputs::Dir(dir) => snapshot::restore(
            RestoreInput::Directory(&dir),
            restoring,
            node.target(),
            Some(&observer),
        ),
        Inputs::Repo(repo, auth) => {
            // Staging resolution remains node policy. The profile facade
            // receives the resolved inputs and never reads configuration or
            // environment on its own.
            let scratch = node::scratch_dir(&config.storage, args.scratch_dir.as_deref());
            let repository = SnapshotRepository::open(
                &repo,
                args.insecure,
                auth,
                scratch,
                registry::Tuning::default(),
            )
            .into_diagnostic()
            .context("opening the repository")?;

            println!("source:   {repo} ({})", args.point);

            snapshot::restore(
                RestoreInput::Repository {
                    repository: &repository,
                    point: args.point,
                },
                restoring,
                node.target(),
                Some(&observer),
            )
        }
    }
    .into_diagnostic()
    .context("restoring the stele")?;

    progress.finish();

    report(&outcome.plan, &outcome.outlook, &outcome.summary);

    Ok(())
}

/// What the run did, in the numbers an operator checks.
///
/// Still printed after the restore, and now that is a choice rather than a gap:
/// the run itself is drawn while it happens through the profile observer seam,
/// so what is left for the end is the arithmetic a bar cannot carry.
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
            "epochs:   {} restored, {} skipped by history.window",
            plan.epochs.len(),
            plan.skipped_epochs,
        );
    } else {
        println!("epochs:   {}", plan.epochs.len());
    }

    // Printed only when it happened, and never folded into the epoch line: an
    // epoch dropped by the history window is this node's own configuration, and
    // a layer dropped for a kind this build does not implement is a stele from
    // a publisher ahead of it. An operator acts on the second by upgrading.
    if !plan.skipped_unknown.is_empty() {
        println!(
            "skipped:  {} layer(s) this build has no kind for ({}); upgrade to restore them",
            plan.skipped_unknown.len(),
            plan.skipped_kinds().join(", "),
        );
    }

    // The one line about layers this restore deliberately did not read. A
    // dump is a past epoch's state, and this run is building a node that
    // stands at the stele's sequence — so what an operator learns here is what
    // the stele *carries*, which is the difference between a repository they
    // can restore an old epoch out of later and one they cannot.
    //
    // With the layer count when it is short of a whole one, because a dump may
    // be. Only the tip is checked for completeness — a publisher whose
    // predecessor did not carry every shard of an epoch warns and publishes the
    // short dump anyway — so an epoch listed bare here and an epoch missing
    // nine of its shards would otherwise read identically, and the second is
    // not a repository that restores that epoch.
    let dumps = plan.carried_dumps();

    if !dumps.is_empty() {
        let epochs: Vec<String> = dumps
            .iter()
            .map(|dump| match dump.is_whole() {
                true => dump.epoch.to_string(),
                false => format!(
                    "{} (partial: {} of {} layers)",
                    dump.epoch,
                    dump.carried,
                    dolos_snapshot::restore::CarriedDump::expected(),
                ),
            })
            .collect();

        println!(
            "dumps:    {} retained state dump(s) carried and not restored (epochs {})",
            dumps.len(),
            epochs.join(", "),
        );
    }

    println!(
        "fetched:  {} layers ({} skipped), {} compressed bytes planned",
        summary.layers_fetched, summary.layers_skipped, outlook.remaining.compressed_bytes,
    );

    println!(
        "restored: {} blocks, {} logs, {} index records, {} first-seen entries, {} entities, {} utxos",
        summary.blocks,
        summary.logs,
        summary.index_records,
        summary.entity_index,
        summary.entities,
        summary.utxos,
    );
}

pub(super) fn run(
    config: &RootConfig,
    args: &Args,
    inputs: Inputs,
    feedback: &Feedback,
) -> miette::Result<()> {
    let options = RestoreOptions {
        feedback,
        skip_space_check: args.skip_space_check,
    };

    restore(config, args, inputs, options)
}

#[cfg(test)]
mod progress_tests {
    use dolos_snapshot::progress::{Event, Outcome, Progress as _};

    use crate::feedback::{Feedback, SteleProgress};

    const PER_RESTORE: usize = 3 + 16;

    /// The wiring `restore_dir` and `restore_repo` use, driven over a
    /// fixture-scale restore that resumes.
    ///
    /// The resume is the case worth rendering and the one worth testing: a
    /// skipped layer moves no blob and no bytes, so a bar that advanced only on
    /// bytes would stall on exactly the layers a resumed run gets for free.
    #[test]
    fn the_restore_renderer_tracks_a_resumed_fixture_scale_run() {
        let progress = SteleProgress::restoring(&Feedback::hidden());

        let epoch = serde_json::json!({"networkMagic": 2, "epoch": 0});
        let shard = serde_json::json!({"networkMagic": 2, "epoch": 0, "shard": 0});

        // The layer an earlier attempt had already committed: announced, closed,
        // nothing pulled for it.
        progress.on(Event::LayerStarted {
            index: 0,
            total: PER_RESTORE,
            kind: "blocks",
            scope: &epoch,
        });

        progress.on(Event::LayerFinished {
            index: 0,
            total: PER_RESTORE,
            kind: "blocks",
            outcome: Outcome::Skipped,
        });

        assert_eq!(
            progress.layers_position(),
            1,
            "a skip still advances the run"
        );
        assert_eq!(progress.blob_position(), 0);

        for index in 1..PER_RESTORE {
            let kind = if index < 3 { "logs" } else { "state" };
            let scope = if index < 3 { &epoch } else { &shard };

            progress.on(Event::LayerStarted {
                index,
                total: PER_RESTORE,
                kind,
                scope,
            });

            // A registry reader pulls the whole blob before a record comes back
            // out of it, which is the stretch this bar exists for.
            progress.on(Event::Blob {
                moved: true,
                bytes: 8_192,
            });

            for _ in 0..8 {
                progress.on(Event::Bytes(1_024));
            }

            progress.on(Event::Records(2_000));

            progress.on(Event::LayerFinished {
                index,
                total: PER_RESTORE,
                kind,
                outcome: Outcome::Transferred,
            });
        }

        assert_eq!(progress.layers_position(), PER_RESTORE as u64);
        assert_eq!(progress.layers_length(), Some(PER_RESTORE as u64));

        // A running total over every blob the restore pulled, rather than the
        // last one's size — see `SteleProgress`.
        let pulled = (PER_RESTORE as u64 - 1) * 8_192;

        assert_eq!(progress.blob_position(), pulled);
        assert_eq!(progress.blob_length(), Some(pulled));

        assert_eq!(
            progress.records_position(),
            2_000 * (PER_RESTORE as u64 - 1),
            "the skipped layer contributed no records"
        );

        progress.finish();
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use dolos_cardano::networks;

    use super::*;

    fn args(source: Option<&str>) -> Args {
        Args {
            source: source.map(|raw| raw.parse().unwrap()),
            point: Point::default(),
            insecure: false,
            scratch_dir: None,
            skip_space_check: false,
            credentials: CredentialArgs::default(),
        }
    }

    fn repository(source: Source) -> String {
        match source {
            Source::Repo(repo) => format!("{}/{}", repo.registry(), repo.repository()),
            Source::Dir(dir) => panic!("expected a repository, got {}", dir.display()),
        }
    }

    #[test]
    fn a_known_network_restores_from_its_official_repository() {
        let resolved = source(&args(None), Some(&networks::PREVIEW)).unwrap();

        assert_eq!(
            format!("oci://{}", repository(resolved)),
            networks::PREVIEW.stelae.registry
        );
    }

    #[test]
    fn an_explicit_source_wins() {
        let resolved = source(&args(Some("file://stele")), Some(&networks::PREVIEW)).unwrap();
        assert!(matches!(resolved, Source::Dir(dir) if dir == Path::new("stele")));

        let resolved = source(
            &args(Some("oci://ghcr.io/txpipe/dolos-snapshots/preview")),
            Some(&networks::PREVIEW),
        )
        .unwrap();
        assert_eq!(
            repository(resolved),
            "ghcr.io/txpipe/dolos-snapshots/preview"
        );
    }

    #[test]
    fn a_custom_network_without_a_source_names_the_flag() {
        let message = source(&args(None), None).unwrap_err().to_string();

        assert!(message.contains("--source"), "{message}");
    }
}
