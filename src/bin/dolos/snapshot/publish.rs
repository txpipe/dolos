use std::path::PathBuf;

use clap::Parser;
use dolos_core::config::RootConfig;
use dolos_snapshot::export;
use miette::{Context as _, IntoDiagnostic as _};

use dolos_snapshot::{
    export::Standing,
    registry::{self, Registry, Repository},
};

use super::EpochRange;

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

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let stores = crate::common::open_data_stores(config)
        .into_diagnostic()
        .context("opening the data stores")?;

    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    let plan = export::plan(&stores.state, u64::from(genesis.network_magic()))
        .into_diagnostic()
        .context("planning the publish")?;

    let plan = super::restrict(plan, args.epochs);

    super::report_plan(&plan)?;

    match (&args.repo, &args.output_dir) {
        (Some(repo), _) => to_repository(config, args, repo, &plan, &stores),
        (None, Some(dir)) => to_directory(args, dir, &plan, &stores),
        // The required `destination` group already refuses this.
        (None, None) => unreachable!("one of --output-dir and --repo is required"),
    }
}

fn to_directory(
    args: &Args,
    dir: &std::path::Path,
    plan: &export::Plan,
    stores: &crate::common::Stores,
) -> miette::Result<()> {
    if args.dry_run {
        println!("dry run: nothing written to {}", dir.display());
        return Ok(());
    }

    let inscription = export::publish(
        dir,
        plan,
        &stores.archive,
        &stores.state,
        &stores.indexes,
        None,
    )
    .into_diagnostic()
    .context("exporting the stele")?;

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
/// The report is what a publisher wants to check rather than trust: how much of
/// this stele was inherited rather than built, and how much of it moved. Both
/// are numbers the code counted, not an inference from a duration.
fn to_repository(
    config: &RootConfig,
    args: &Args,
    repo: &Repository,
    plan: &export::Plan,
    stores: &crate::common::Stores,
) -> miette::Result<()> {
    // A publisher's credentials come from `STELAE_REGISTRY_USER` /
    // `STELAE_REGISTRY_PASSWORD`, which override anything configured. The
    // configured user is still the fallback: it is read-only, so authenticating
    // with it fails the push at the registry rather than a step earlier — which
    // is the honest place for "these credentials cannot publish" to be said.
    let auth = crate::common::stele_registry_auth(&config.stelae)?;

    let registry = registry::open(repo, args.insecure, auth)
        .into_diagnostic()
        .context("opening the repository")?;

    // Before anything is built, and before the dry run too: a publisher asking
    // what a publish would do wants the same answer the publish gives.
    if !standing(&registry, plan, args)? {
        return Ok(());
    }

    if args.dry_run {
        // `None` here and `None` at the `publish` below are one decision: a dry
        // run describes the publish that follows it, so the two calls are
        // handed the same digest records or the number is about something else.
        let preview = registry::preview(&registry, plan, None, args.rebuild)
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

    let published = registry::publish(
        &registry,
        plan,
        &stores.archive,
        &stores.state,
        &stores.indexes,
        None,
        args.rebuild,
    )
    .into_diagnostic()
    .context("publishing the stele")?;

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

/// Read where this node stands against the repository, and report it.
///
/// Returns whether the publish should go on. Three of the four readings are
/// terminal here, and it is the *middle* one that this exists for:
///
/// - **nothing published, or exactly one sequence behind** — carry on.
/// - **the repository has already reached this node** — there is nothing to
///   publish. A job on a timer that runs more often than epochs close arrives
///   here every time it runs, and that is not a failure, so it is reported and
///   the process exits zero. `--require-new` makes the same case an error, for
///   an operator who ran it expecting a stele.
/// - **the node is further ahead than one sequence** — refused, and the refusal
///   stands: whether a deliberate gap ever gets a policy is not this command's
///   to invent. What is new is that the message names the distance alongside
///   both sequences, so "the publisher has been down for a day" and "the
///   publisher has been down for a month" do not read the same.
fn standing(registry: &Registry, plan: &export::Plan, args: &Args) -> miette::Result<bool> {
    let standing = registry::standing(registry, plan)
        .into_diagnostic()
        .context("reading the repository's latest stele")?;

    match standing {
        Standing::Empty => {
            println!("follows:  nothing; this repository holds no stele");
            Ok(true)
        }
        Standing::Next { latest } => {
            println!("follows:  sequence {latest}");
            Ok(true)
        }
        Standing::UpToDate { latest } => {
            let message = format!(
                "nothing to publish: this repository is at sequence {latest} and this node is at \
                 sequence {}",
                plan.sequence,
            );

            if args.require_new {
                return Err(miette::miette!("{message}"));
            }

            println!("{message}");
            Ok(false)
        }
        Standing::Ahead { latest, distance } => Err(miette::miette!(
            "this repository's latest stele is sequence {latest} and this node is at sequence \
             {}, {distance} sequences ahead: a publish must follow the repository's latest \
             stele, and this one would leave a gap no later stele could close",
            plan.sequence,
        )),
    }
}
