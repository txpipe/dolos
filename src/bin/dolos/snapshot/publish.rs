use std::path::PathBuf;

use clap::Parser;
use dolos_core::config::RootConfig;
use dolos_snapshot::export;
use miette::{Context as _, IntoDiagnostic as _};

use dolos_snapshot::registry::{self, Repository};

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
}

/// An epoch selection, in Rust's own range spellings.
///
/// Spelled the way a reader already knows how to read: `..` excludes its end,
/// `..=` includes it. Both are accepted because a publisher naming "epochs 500
/// through 519" and one naming "up to and including 519" are both natural, and
/// silently picking one of the two meanings is how an operator publishes an
/// epoch short.
#[derive(Debug, Clone, Copy)]
struct EpochRange {
    first: Option<u64>,
    last: Option<u64>,
}

impl std::str::FromStr for EpochRange {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let bad = |why: &str| format!("{raw:?} is not an epoch range: {why}");

        let parse = |part: &str| -> Result<Option<u64>, String> {
            match part.trim() {
                "" => Ok(None),
                value => value
                    .parse::<u64>()
                    .map(Some)
                    .map_err(|e| bad(&format!("{value:?}: {e}"))),
            }
        };

        // `..=` first: `..` is a prefix of it, so testing in the other order
        // would read `500..=520` as a range ending at `=520`.
        let (raw, inclusive) = match raw.split_once("..=") {
            Some(_) => (raw.replacen("..=", "..", 1), true),
            None => (raw.to_owned(), false),
        };

        let Some((start, end)) = raw.split_once("..") else {
            // A bare number: exactly that epoch.
            let only = parse(&raw)?.ok_or_else(|| bad("it is empty"))?;

            return Ok(Self {
                first: Some(only),
                last: Some(only),
            });
        };

        let first = parse(start)?;
        let end = parse(end)?;

        let last = match (end, inclusive) {
            (Some(end), false) => Some(
                end.checked_sub(1)
                    .ok_or_else(|| bad("an exclusive end of 0 selects nothing"))?,
            ),
            (None, true) => return Err(bad("`..=` needs an end")),
            (end, _) => end,
        };

        if let (Some(first), Some(last)) = (first, last) {
            if first > last {
                return Err(bad("it starts after it ends"));
            }
        }

        Ok(Self { first, last })
    }
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let stores = crate::common::open_data_stores(config)
        .into_diagnostic()
        .context("opening the data stores")?;

    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    let plan = export::plan(&stores.state, u64::from(genesis.network_magic()))
        .into_diagnostic()
        .context("planning the publish")?;

    let plan = match args.epochs {
        Some(range) => plan.restrict_epochs(range.first, range.last),
        None => plan,
    };

    let tag = plan.tag().into_diagnostic()?;

    println!(
        "network:  {} ({})",
        plan.network.name(),
        plan.network.magic()
    );
    println!("cursor:   {}", plan.cursor);
    println!("sequence: {} (tag {tag})", plan.sequence);

    match (plan.epochs.first(), plan.epochs.last()) {
        (Some(first), Some(last)) => println!(
            "epochs:   {}..={} ({} of them, slots {}..={})",
            first.epoch,
            last.epoch,
            plan.epochs.len(),
            first.start_slot,
            last.end_slot,
        ),
        // The state tip alone is a legitimate publish; say so rather than
        // printing an empty range and looking like a mistake.
        _ => println!("epochs:   none selected; the state tip only"),
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: &str) -> (Option<u64>, Option<u64>) {
        let range: EpochRange = raw.parse().unwrap();
        (range.first, range.last)
    }

    #[test]
    fn epoch_ranges_read_the_way_rust_ranges_do() {
        assert_eq!(parse("500..520"), (Some(500), Some(519)));
        assert_eq!(parse("500..=520"), (Some(500), Some(520)));
        assert_eq!(parse("500.."), (Some(500), None));
        assert_eq!(parse("..520"), (None, Some(519)));
        assert_eq!(parse("..=520"), (None, Some(520)));
        assert_eq!(parse(".."), (None, None));
        assert_eq!(parse("500"), (Some(500), Some(500)));
    }

    #[test]
    fn a_nonsensical_range_is_refused() {
        for raw in ["520..500", "..0", "abc", "", "500..abc", "500..=", "-1"] {
            assert!(raw.parse::<EpochRange>().is_err(), "{raw:?}");
        }
    }
}
