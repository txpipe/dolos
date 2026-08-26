//! Check a published stele against what it claims — and, opted into, against
//! this node's own stores.
//!
//! Two checks, separable because a verifier with no stores can still run the
//! first:
//!
//! - **Transport, the default.** Pull the manifest and the inscription and
//!   check they agree, then stream every blob: its bytes must hash to the blob
//!   digest the manifest addresses it by, decompress within the size the
//!   descriptor claims, and hash to the layer's `diffId` with the record count
//!   the inscription states. The history chain's shape — contiguous back to its
//!   first entry — is checked on the way in; a gapped chain never parses.
//! - **`--reproduce`.** Rebuild every layer from the local stores through the
//!   same discarding pipeline `digest` uses and require the result to be
//!   byte-identical to the published document — descriptors compared kind by
//!   kind and scope by scope, then the whole inscription digest, which is what
//!   a Phase 5 signature will be over. This is the check that closes the
//!   incremental publish's trust gap: a layer inherited rather than rebuilt was
//!   attested without being reproduced, until something runs this.
//!
//! ## What a pass does not prove
//!
//! Digests, not signatures. The published `history` is an input — a verifier
//! cannot know a chain it did not publish — so a pass proves the layers and
//! the document around them, never the chain's provenance. Saying otherwise
//! would be worse than the gap; signatures are Phase 5 and `sign` does not
//! exist yet.
//!
//! ## The exit code is the deliverable
//!
//! Non-zero on any mismatch, with the offending layer named. A determinism job
//! is this command and nothing else.

use clap::Parser;
use dolos_core::config::RootConfig;
use dolos_snapshot::{
    export,
    registry::{self, Point, Repository},
};
use miette::{Context as _, IntoDiagnostic as _};

use super::EpochRange;

#[derive(Debug, Parser)]
pub struct Args {
    /// OCI repository holding the stele, e.g.
    /// `oci://ghcr.io/txpipe/dolos-mainnet`
    #[arg(long, value_name = "OCI_URL")]
    repo: Repository,

    /// which stele to verify: `latest`, or `epoch-N` for the stele published
    /// at the end of epoch N
    #[arg(long, value_name = "POINT", default_value = "latest")]
    point: Point,

    /// talk to the repository over plaintext HTTP rather than HTTPS; for a
    /// registry on a loopback address or a mirror inside a cluster, and for
    /// nothing reachable from outside one
    #[arg(long, action)]
    insecure: bool,

    /// also rebuild every layer from this node's stores and require the result
    /// to be byte-identical to the published inscription. Costs what a publish
    /// costs — hours on mainnet — which is why it is opt-in and does not
    /// belong in a pre-flight
    #[arg(long, action)]
    reproduce: bool,

    /// epochs whose index layers one traversal of the index store fills; a
    /// larger band trades resident memory for fewer traversals, and changes
    /// nothing about the stele it produces. Defaults to the measured value
    /// that keeps the index pass inside 1 GiB
    #[arg(long, value_name = "EPOCHS", requires = "reproduce")]
    index_band: Option<std::num::NonZeroUsize>,

    /// how many layer producers run at once: the store traversals that fill,
    /// frame and compress layers, one of them reserved for the index bands.
    /// Changes nothing about the reproduction. Defaults to 4; `1` is the
    /// strictly serial walk
    #[arg(long, value_name = "N", requires = "reproduce")]
    producers: Option<std::num::NonZeroUsize>,

    /// epochs the reproduction builds layers for, e.g. `500..520`, `500..=520`
    /// or `500`; defaults to every epoch below the cursor, and must match what
    /// the stele was published with
    #[arg(long, value_name = "RANGE", requires = "reproduce")]
    epochs: Option<EpochRange>,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let auth = crate::common::stele_registry_auth(&config.stelae)?;

    // A verification streams every blob through a staged file. No
    // `--scratch-dir` of its own: it reads what is already published rather
    // than moving a node's own data.
    let scratch = crate::common::stele_scratch_dir(&config.storage, None);

    let registry = registry::open(
        &args.repo,
        args.insecure,
        auth,
        scratch,
        registry::Tuning::default(),
    )
    .into_diagnostic()
    .context("opening the repository")?;

    let verified = registry::verify(&registry, args.point)
        .into_diagnostic()
        .context("verifying the stele")?;

    println!("verified {} at {}", args.repo, args.point);
    println!("sequence: {}", verified.inscription.sequence);

    match verified.inscription.history.as_slice() {
        [] => println!("history:  none; this stele starts a chain"),
        [first, ..] => println!(
            "history:  {} entries, contiguous back to sequence {}",
            verified.inscription.history.len(),
            first.sequence,
        ),
    }

    println!(
        "layers:   {} streamed and checked ({} compressed bytes)",
        verified.inscription.layers.len(),
        verified.compressed_bytes,
    );
    println!("identity: {}", verified.identity);

    // Where a reader meets the limit of what just passed: digests were
    // checked, and the history was an input rather than something this command
    // could vouch for.
    println!("checked:  digests only; signatures and chain provenance are phase 5");

    if !args.reproduce {
        return Ok(());
    }

    let stores = crate::common::open_data_stores(config)
        .into_diagnostic()
        .context("opening the data stores")?;

    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    let plan = export::plan(
        &stores.state,
        u64::from(genesis.network_magic()),
        super::retained_epochs(config)?,
    )
    .into_diagnostic()
    .context("planning the reproduction")?;

    let plan = super::restrict(plan, args.epochs);
    let plan = super::banded(plan, args.index_band);
    let plan = super::produced(plan, args.producers);

    super::report_plan(&plan)?;

    let reproduced = export::verify_reproduction(
        &verified.inscription,
        &plan,
        &stores.archive,
        &stores.state,
        &stores.indexes,
        None,
    )
    .into_diagnostic()
    .context("reproducing the published stele from the local stores")?;

    println!(
        "reproduced: {} layers rebuilt from the local stores; the documents are byte-identical \
         ({})",
        reproduced.layers.len(),
        verified.identity,
    );

    Ok(())
}
