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

use dolos_snapshot::{
    node,
    registry::{self, Point, Repository},
    restore::Source,
};

use crate::feedback::{Feedback, SteleProgress};

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

fn restore_dir(
    config: &RootConfig,
    dir: &std::path::Path,
    feedback: &Feedback,
    resume: bool,
) -> miette::Result<()> {
    let node = Node::open(config)?;
    let progress = SteleProgress::restoring(feedback);

    let (plan, outlook, summary) = dolos_snapshot::restore::restore_dir(
        dir,
        node.restoring(resume),
        node.target(),
        &progress.observer(),
    )
    .into_diagnostic()
    .context("restoring the stele")?;

    progress.finish();

    report(&plan, &outlook, &summary);

    Ok(())
}

fn restore_repo(
    config: &RootConfig,
    repo: &Repository,
    point: Point,
    insecure: bool,
    scratch_dir: Option<&std::path::Path>,
    feedback: &Feedback,
    resume: bool,
) -> miette::Result<()> {
    // First: `Node::open` runs `ensure_storage_path`, so the default of
    // `<storage.path>/scratch` needs no special case on a host where the
    // storage directory does not exist yet.
    let node = Node::open(config)?;

    // Resolved here rather than inside the transport: which identity this node
    // reads a registry as is the node's policy, and `dolos_snapshot::node` is
    // where that policy lives. Where it stages comes from the same place.
    let auth = node::registry_auth(&config.stelae).into_diagnostic()?;

    let scratch = node::scratch_dir(&config.storage, scratch_dir);

    let registry = registry::open(repo, insecure, auth, scratch, registry::Tuning::default())
        .into_diagnostic()
        .context("opening the repository")?;

    println!("source:   {repo} ({point})");

    let progress = SteleProgress::restoring(feedback);

    let (plan, outlook, summary) = registry::restore_registry(
        &registry,
        point,
        node.restoring(resume),
        node.target(),
        &progress.observer(),
    )
    .into_diagnostic()
    .context("restoring the stele")?;

    progress.finish();

    report(&plan, &outlook, &summary);

    Ok(())
}

/// What the run did, in the numbers an operator checks.
///
/// Still printed after the restore, and now that is a choice rather than a gap:
/// the run itself is drawn while it happens, through the observer seam this
/// command shares with `snapshot publish`, so what is left for the end is the
/// arithmetic a bar cannot carry — what a resumed run cost rather than what an
/// unresumed one would have.
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

    // Printed only when it happened, and never folded into the epoch line: an
    // epoch dropped by `sync.max_history` is this node's own configuration, and
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

pub fn run(
    config: &RootConfig,
    args: &Args,
    feedback: &Feedback,
    resume: bool,
) -> miette::Result<()> {
    match &args.source {
        Source::Dir(dir) => restore_dir(config, dir, feedback, resume),
        Source::Repo(repo) => restore_repo(
            config,
            repo,
            args.point,
            args.insecure,
            args.scratch_dir.as_deref(),
            feedback,
            resume,
        ),
    }
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
