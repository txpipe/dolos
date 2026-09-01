use std::path::PathBuf;

use clap::Parser;
use dolos_core::config::RootConfig;
use miette::{Context as _, IntoDiagnostic as _};

use dolos_snapshot::{
    export, node,
    publisher::{Next, Publisher, RepositoryPublish},
    registry::{self, Repository},
};

use super::EpochRange;
use crate::feedback::{Feedback, SteleProgress};

/// Where a stele goes, and it goes to exactly one place.
///
/// An [`ArgGroup`] rather than a pair of `conflicts_with`/`required_unless`
/// attributes, because it is the only spelling whose *message* names both
/// options when neither is given. `--rebuild` and `--insecure` then say they
/// conflict with `--output-dir` rather than that they require `--repo`: a
/// boolean flag is always "present" to clap's `requires`, so the requirement
/// would never fire.
#[derive(Debug, Parser)]
#[command(group(
    clap::ArgGroup::new("destination").required(true).args(["output_dir", "repo"])
))]
pub struct Args {
    /// directory to write the stele into; must not already hold one
    #[arg(long)]
    output_dir: Option<PathBuf>,

    /// OCI repository to publish into, e.g.
    /// `oci://ghcr.io/txpipe/dolos-mainnet`
    #[arg(long, value_name = "OCI_URL")]
    repo: Option<Repository>,

    /// epochs to write layers for, e.g. `500..520`, `500..=520`, `500..`,
    /// `..520` or `500`; defaults to every epoch below the cursor
    #[arg(long, value_name = "RANGE")]
    epochs: Option<EpochRange>,

    /// rebuild every layer instead of carrying forward the ones a previous
    /// publish already put in the repository
    #[arg(long, action, conflicts_with = "output_dir")]
    rebuild: bool,

    /// talk to the repository over plaintext HTTP rather than HTTPS; for a
    /// registry on a loopback address or a mirror inside a cluster, and for
    /// nothing reachable from outside one
    #[arg(long, action, conflicts_with = "output_dir")]
    insecure: bool,

    /// directory to stage layers in while they are uploaded; defaults to
    /// `<storage.path>/scratch`
    #[arg(long, value_name = "DIR", conflicts_with = "output_dir")]
    scratch_dir: Option<PathBuf>,

    /// epochs whose index layers one traversal of the index store fills; a
    /// larger band trades resident memory for fewer traversals, and changes
    /// nothing about the stele it produces. Defaults to the measured value
    /// that keeps the index pass inside 1 GiB
    #[arg(long, value_name = "EPOCHS")]
    index_band: Option<std::num::NonZeroUsize>,

    /// how many layer producers run at once: the store traversals that fill,
    /// frame and compress layers, one of them reserved for the index bands.
    /// Changes nothing about the stele it produces. Defaults to 4; `1` is the
    /// strictly serial walk
    #[arg(long, value_name = "N")]
    producers: Option<std::num::NonZeroUsize>,

    /// how many layer round trips to run at once against the repository; a
    /// publish is a few hundred small transfers and its wall clock is their
    /// latency, not their bytes. Defaults to 8; `1` is the strictly serial
    /// path, for a registry that answers concurrency badly
    #[arg(long, value_name = "N", conflicts_with = "output_dir")]
    concurrency: Option<std::num::NonZeroUsize>,

    /// check that the repository still holds every layer carried forward from
    /// the previous stele, instead of trusting the manifest that names them.
    /// One round trip per carried layer, so the cost grows with the history
    /// behind the stele; for a repository whose blob retention you do not trust
    #[arg(long, action, conflicts_with = "output_dir")]
    verify_carried: bool,

    /// report what would be written and exit
    #[arg(long, action)]
    dry_run: bool,

    /// fail when the repository is already at this node's sequence, instead of
    /// reporting that there is nothing to publish and exiting zero; for an
    /// operator who ran this expecting a new stele, and against a job on a
    /// timer, for which "nothing has closed since last time" is the ordinary
    /// case
    #[arg(long, action, conflicts_with = "output_dir")]
    require_new: bool,
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    let stores = crate::common::open_data_stores(config)
        .into_diagnostic()
        .context("opening the data stores")?;

    let selection = super::Selection {
        epochs: args.epochs,
        index_band: args.index_band,
        producers: args.producers,
    };

    let plan = super::planned(config, &stores, &selection, "planning the publish")?;

    super::report_plan(&plan)?;

    match (&args.repo, &args.output_dir) {
        (Some(repo), _) => {
            let publish = RepositoryPublish {
                repo,
                insecure: args.insecure,
                scratch_dir: args.scratch_dir.as_deref(),
                rebuild: args.rebuild,
                dry_run: args.dry_run,
                require_new: args.require_new,
                tuning: registry::Tuning {
                    concurrency: args.concurrency,
                    verify_adopted: args.verify_carried,
                },
            };

            to_repository(config, &publish, &plan, &stores, feedback)
        }
        (None, Some(dir)) => to_directory(args, dir, &plan, &stores, feedback),
        // The required `destination` group already refuses this.
        (None, None) => unreachable!("one of --output-dir and --repo is required"),
    }
}

fn to_directory(
    args: &Args,
    dir: &std::path::Path,
    plan: &export::Plan,
    stores: &crate::common::Stores,
    feedback: &Feedback,
) -> miette::Result<()> {
    if args.dry_run {
        println!("dry run: nothing written to {}", dir.display());
        return Ok(());
    }

    // A directory publish moves no bytes over a network, so the blob bar stays
    // empty here — but the layer and record bars are the same hours of store
    // walking a repository publish pays, and they are what this reports.
    let progress = SteleProgress::publishing(feedback);

    let inscription = export::publish(
        dir,
        plan,
        &stores.archive,
        &stores.state,
        &stores.indexes,
        None,
        &progress.observer(),
    )
    .into_diagnostic()
    .context("exporting the stele")?;

    progress.finish();

    let digest = inscription.digest().into_diagnostic()?;

    println!("wrote {}", dir.display());
    println!("layers:   {}", inscription.layers.len());
    println!(
        "size:     {} uncompressed bytes",
        inscription.uncompressed_size()
    );
    println!("identity: {digest}");

    Ok(())
}

/// Publish into an OCI repository, chained to whatever is already in it.
///
/// Parse and render: [`Publisher`] is the lifecycle, [`Next`] is what each
/// reading of the repository means for it, and what is here is the order the
/// operator sees it in. The report is what a publisher wants to check rather
/// than trust: how much of this stele was inherited rather than built, and how
/// much of it moved. Both are numbers the code counted, not an inference from a
/// duration.
pub(super) fn to_repository(
    config: &RootConfig,
    publish: &RepositoryPublish,
    plan: &export::Plan,
    stores: &crate::common::Stores,
    feedback: &Feedback,
) -> miette::Result<()> {
    let repo = publish.repo;

    let auth = node::registry_auth(&config.stelae).into_diagnostic()?;

    let publisher = Publisher::open(config, publish, auth)
        .into_diagnostic()
        .context("opening the repository")?;

    let standing = publisher
        .standing(plan)
        .into_diagnostic()
        .context("reading the repository's latest stele")?;

    match Next::read(standing, plan.sequence, publish.require_new).into_diagnostic()? {
        Next::First => println!("follows:  nothing; this repository holds no stele"),
        Next::After { latest } => println!("follows:  sequence {latest}"),
        Next::Nothing(message) => {
            println!("{message}");
            return Ok(());
        }
    }

    publisher
        .preflight()
        .into_diagnostic()
        .context("sizing the staging directory")?;

    if publish.dry_run {
        let preview = publisher
            .preview(plan, &stores.archive)
            .into_diagnostic()
            .context("planning the publish")?;

        match preview.predecessor {
            Some((sequence, digest)) => println!("follows:  sequence {sequence} ({digest})"),
            None => println!("follows:  nothing; this repository holds no stele"),
        }

        println!("history:  {} entries", preview.history);
        println!("reuse:    {} layers carried forward", preview.layers_reused);
        println!("build:    {} layers", preview.layers_built);
        println!("dry run: nothing written to {repo}");

        return Ok(());
    }

    // Built here rather than above the dry run: the dry run prints and returns,
    // and a renderer for a publish that never happens would draw an empty bar
    // under the report.
    let progress = SteleProgress::publishing(feedback);

    let published = publisher
        .publish(
            plan,
            &stores.archive,
            &stores.state,
            &stores.indexes,
            &progress.observer(),
        )
        .into_diagnostic()
        .context("publishing the stele")?;

    progress.finish();

    let transfer = published.transfer;

    println!("wrote {repo}");
    println!("history:  {} entries", published.inscription.history.len());
    println!(
        "layers:   {} ({} built, {} carried forward)",
        published.inscription.layers.len(),
        published.layers_built,
        published.layers_reused,
    );
    println!(
        "uploaded: {} layers, {} compressed bytes",
        transfer.layers_uploaded, transfer.bytes_uploaded,
    );
    println!(
        "skipped:  {} layers already in the registry, {} compressed bytes",
        transfer.layers_skipped, transfer.bytes_skipped,
    );
    println!(
        "size:     {} uncompressed bytes",
        published.inscription.uncompressed_size()
    );
    println!("identity: {}", published.identity);

    Ok(())
}

#[cfg(test)]
mod tests {
    use dolos_snapshot::progress::{Event, Outcome, Progress as _};

    use crate::feedback::{Feedback, SteleProgress};

    /// One epoch's three layers plus the sixteen state shards — the fixture
    /// suites' `PER_PUBLISH`, and the smallest publish this profile makes.
    const PER_PUBLISH: usize = 3 + 16;

    /// The wiring `run` uses, driven over a fixture-scale publish.
    ///
    /// Named for what it is protecting: [#1191]'s squash dropped the CLI's
    /// rendering layer outright and nothing noticed, because nothing under
    /// `src/bin/dolos/` had anything to say about it. This exercises the same
    /// constructor `to_repository` and `to_directory` call, feeds it the event
    /// stream a one-epoch publish produces, and holds the bars to where that
    /// run actually ended.
    ///
    /// [#1191]: https://github.com/txpipe/dolos/pull/1191
    #[test]
    fn the_publish_renderer_tracks_a_fixture_scale_run() {
        let progress = SteleProgress::publishing(&Feedback::hidden());

        let epoch = serde_json::json!({"networkMagic": 2, "epoch": 0});
        let shard = serde_json::json!({"networkMagic": 2, "epoch": 0, "shard": 0});

        let mut moved = 0u64;

        for (index, kind) in ["blocks", "indexes", "logs"].into_iter().enumerate() {
            progress.on(Event::LayerStarted {
                index,
                total: PER_PUBLISH,
                kind,
                scope: &epoch,
            });

            progress.on(Event::Records(1_000));

            // The transport's half: staged, then uploaded in chunks.
            progress.on(Event::Blob {
                moved: true,
                bytes: 4_096,
            });

            for _ in 0..4 {
                progress.on(Event::Bytes(1_024));
                moved += 1_024;
            }

            progress.on(Event::LayerFinished {
                index,
                total: PER_PUBLISH,
                kind,
                outcome: Outcome::Transferred,
            });
        }

        // The state pass: sixteen layers open at once, closed in order, with one
        // record stream feeding all of them.
        for index in 3..PER_PUBLISH {
            progress.on(Event::LayerStarted {
                index,
                total: PER_PUBLISH,
                kind: "state",
                scope: &shard,
            });
        }

        progress.on(Event::Records(50_000));

        for index in 3..PER_PUBLISH {
            progress.on(Event::Blob {
                moved: false,
                bytes: 512,
            });

            progress.on(Event::LayerFinished {
                index,
                total: PER_PUBLISH,
                kind: "state",
                outcome: Outcome::Transferred,
            });
        }

        assert_eq!(progress.layers_position(), PER_PUBLISH as u64);
        assert_eq!(progress.layers_length(), Some(PER_PUBLISH as u64));
        assert_eq!(progress.records_position(), 53_000);

        // Everything the publish took on is accounted for: the three layers
        // that were uploaded, byte for byte, and the ones the registry already
        // held, counted done the moment they were announced.
        let skipped = (PER_PUBLISH as u64 - 3) * 512;

        assert_eq!(progress.blob_position(), 3 * 4_096 + skipped);
        assert_eq!(progress.blob_length(), Some(3 * 4_096 + skipped));

        assert_eq!(moved, 3 * 4_096);

        progress.finish();
    }
}
