//! What a stele *would* be, computed from local stores and no registry.
//!
//! `publish` writes a stele and reports its identity. This command computes the
//! identity and writes nothing — the same walk over the same stores through the
//! same framing, hashing and zstd pipeline, into a writer that keeps nothing
//! instead of into a directory or a repository.
//!
//! ## Why it exists
//!
//! A publish into a repository *inherits* the layers of epochs that have
//! already closed rather than rebuilding them, which means the newest
//! inscription describes layers the publishing node never read out of its own
//! stores. Every stele published that way carries an attestation nothing but
//! the publisher has ever reproduced. This is the command that reproduces it:
//! run somewhere else, over another node's stores, it either arrives at the
//! same digest or it does not, and the arithmetic in between is a determinism
//! job rather than a person.
//!
//! ## History is an input, not an inference
//!
//! `history` lives inside the canonical document, so a stele's identity depends
//! on which predecessor it chained from — the same stores chained differently
//! are two different digests, deliberately. A reproduction therefore has to be
//! *told* the chain:
//!
//! - by default there is none, `history: []`, which is the first stele of a
//!   repository and what a publisher checks before it has ever published;
//! - `--chain-from FILE` reads a predecessor's canonical inscription off disk
//!   and extends it by the rule a publish extends it by, which is
//!   [`dolos_snapshot::export::Following`] and is one function shared with the
//!   registry publisher rather than a second implementation of contiguity.
//!
//! ## stdout is the document
//!
//! The canonical inscription goes to `stdout` and everything else — the plan,
//! the layer count, the identity — to `stderr`, so `dolos snapshot digest >
//! stele.json` is a file a verifier can hash and `diff`. `--output FILE` writes
//! the same bytes to a file instead, which is what a determinism job comparing
//! two runs wants.
//!
//! ## What it costs
//!
//! What a publish costs, minus the I/O and the upload. Every layer is
//! compressed, because a reproduction that skipped compression would not be
//! doing the work a publish does. On mainnet that is hours.

use std::path::PathBuf;

use clap::Parser;
use dolos_core::config::RootConfig;
use dolos_snapshot::export::{self, Following, Plan, Predecessor};
use miette::{Context as _, IntoDiagnostic as _};

use super::EpochRange;

#[derive(Debug, Parser)]
pub struct Args {
    /// epochs to build layers for, e.g. `500..520`, `500..=520`, `500..`,
    /// `..520` or `500`; defaults to every epoch below the cursor, and must
    /// match what the stele being reproduced was published with
    #[arg(long, value_name = "RANGE")]
    epochs: Option<EpochRange>,

    /// canonical inscription of the stele this one follows, as
    /// `inspect --json` or a published `inscription.json` holds it; without it
    /// the reproduction carries no history, which is a different digest
    #[arg(long, value_name = "FILE")]
    chain_from: Option<PathBuf>,

    /// write the canonical inscription to FILE instead of to stdout
    #[arg(long, value_name = "FILE")]
    output: Option<PathBuf>,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let stores = crate::common::open_data_stores(config)
        .into_diagnostic()
        .context("opening the data stores")?;

    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    let plan = export::plan(&stores.state, u64::from(genesis.network_magic()))
        .into_diagnostic()
        .context("planning the reproduction")?;

    let plan = super::restrict(plan, args.epochs);

    super::report_plan(&plan)?;

    // Read and chained before the stores are walked: a predecessor this plan
    // does not follow is an hour of compression the operator does not have to
    // spend to find out.
    let previous = args
        .chain_from
        .as_deref()
        .map(|path| chain_from(path, &plan))
        .transpose()?;

    let predecessor: &dyn Predecessor = match &previous {
        Some(following) => following,
        None => &export::First,
    };

    match previous.as_ref().map(Predecessor::history) {
        Some(history) => eprintln!("history:  {} entries", history.len()),
        None => eprintln!("history:  none; this reproduction starts a chain"),
    }

    let inscription = export::reproduce(
        &plan,
        &stores.archive,
        &stores.state,
        &stores.indexes,
        None,
        predecessor,
    )
    .into_diagnostic()
    .context("reproducing the stele")?;

    // The bytes, not a re-encoding of the content: they are what a verifier
    // hashes, so anything that pretty-printed them would carry a digest nobody
    // else computes. The identity is their sha256, which is what
    // `Inscription::digest` is.
    let canonical = inscription.canonicalize().into_diagnostic()?;
    let identity = inscription.digest().into_diagnostic()?;

    eprintln!("layers:   {}", inscription.layers.len());
    eprintln!(
        "size:     {} uncompressed bytes",
        inscription.uncompressed_size()
    );
    eprintln!("identity: {identity}");

    match args.output.as_deref() {
        Some(path) => {
            std::fs::write(path, &canonical)
                .into_diagnostic()
                .with_context(|| format!("writing {}", path.display()))?;

            eprintln!("wrote {}", path.display());
        }
        None => {
            use std::io::Write as _;

            // Written rather than printed, with no trailing newline: the
            // document is bytes, `digest > stele.json` has to hash to the
            // identity reported above, and `--chain-from` refuses anything
            // that is not the canonical encoding itself.
            std::io::stdout()
                .write_all(&canonical)
                .into_diagnostic()
                .context("writing the inscription to stdout")?;
        }
    }

    Ok(())
}

/// Read a predecessor's canonical inscription off disk and chain this plan onto
/// it.
///
/// Everything the bytes have to satisfy is [`Following::read`]'s: the profile
/// crate owns what makes a predecessor usable, and this adds only the path to
/// the message.
fn chain_from(path: &std::path::Path, plan: &Plan) -> miette::Result<Following> {
    let raw = std::fs::read(path)
        .into_diagnostic()
        .with_context(|| format!("reading {}", path.display()))?;

    Following::read(&raw, plan)
        .into_diagnostic()
        .with_context(|| format!("chaining onto {}", path.display()))
}
