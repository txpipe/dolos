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
//!   attested without being reproduced, until something runs this. It runs only
//!   over stores standing on the first block of the stele's epoch, the one
//!   state a stele is cut from (`crates/snapshot/PROFILE.md`, "Only the
//!   boundary cut is published").
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

use std::path::PathBuf;

use clap::Parser;
use dolos_core::config::RootConfig;
use dolos_snapshot::{
    export::Plan,
    facade::{SnapshotRepository, SnapshotSource as _, StoreSnapshot},
    node,
    registry::{self, Point, Repository},
};
use miette::{Context as _, IntoDiagnostic as _};

use super::EpochRange;
use crate::registry::CredentialArgs;

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
    /// to be byte-identical to the published inscription. The stores must
    /// stand on the first block of the stele's epoch, as a `stop_epoch` sync
    /// leaves them. Costs what a publish costs — hours on mainnet — which is
    /// why it is opt-in and does not belong in a pre-flight
    #[arg(long, action)]
    reproduce: bool,

    /// epochs whose index layers one traversal of the archive store fills; a
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

    /// the publisher's retained state-dump epochs the reproduction plans
    /// with, e.g. `208,236,290`; they are signed parameters, so they must
    /// match what the stele was published with. Omitted, the reproduction
    /// retains the tip alone
    #[arg(
        long,
        value_name = "EPOCHS",
        value_delimiter = ',',
        requires = "reproduce"
    )]
    state_epochs: Vec<u64>,

    /// read the retained state-dump epochs from FILE, separated by commas or
    /// newlines, in place of --state-epochs
    #[arg(
        long,
        value_name = "FILE",
        requires = "reproduce",
        conflicts_with = "state_epochs"
    )]
    state_epochs_file: Option<PathBuf>,

    #[command(flatten)]
    credentials: CredentialArgs,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    // Planned before the transport check, so that a list or a selection this
    // node cannot plan is refused before every blob has been streamed.
    let reproduction = args
        .reproduce
        .then(|| reproduction(config, args))
        .transpose()?;

    let auth = args.credentials.auth_for_chain(&args.repo, &config.chain)?;

    // A verification streams every blob through a staged file. No
    // `--scratch-dir` of its own: it reads what is already published rather
    // than moving a node's own data.
    let scratch = node::scratch_dir(&config.storage, None);

    let repository = SnapshotRepository::open(
        &args.repo,
        args.insecure,
        auth,
        scratch,
        registry::Tuning::default(),
    )
    .into_diagnostic()
    .context("opening the repository")?;

    let verified = repository
        .verify(args.point)
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

    let Some((stores, plan)) = reproduction else {
        return Ok(());
    };

    let reproduced = StoreSnapshot::new(&stores.archive, &stores.state)
        .verify_reproduction(&verified.inscription, &plan)
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

/// The stores and the reported plan `--reproduce` rebuilds the stele from.
fn reproduction(config: &RootConfig, args: &Args) -> miette::Result<(crate::common::Stores, Plan)> {
    let retained = super::retained_epochs(&args.state_epochs, args.state_epochs_file.as_deref())?;

    let stores = crate::common::open_data_stores(config)
        .into_diagnostic()
        .context("opening the data stores")?;

    let selection = super::Selection {
        epochs: args.epochs,
        index_band: args.index_band,
        producers: args.producers,
    };

    let (plan, boundary) = super::planned(
        config,
        &stores,
        &selection,
        retained,
        "planning the reproduction",
    )?;

    super::report_plan(&plan, &boundary)?;

    Ok((stores, plan))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args, clap::Error> {
        let fixed = [
            "verify",
            "--repo",
            "oci://ghcr.io/txpipe/dolos-snapshots/preview",
        ];

        Args::try_parse_from(fixed.into_iter().chain(args.iter().copied()))
    }

    #[test]
    fn the_state_epochs_are_a_comma_separated_list_for_a_reproduction() {
        let args = parse(&["--reproduce", "--state-epochs", "208,236,290"]).unwrap();
        assert_eq!(args.state_epochs, vec![208, 236, 290]);

        assert!(parse(&["--state-epochs", "208"]).is_err());
        assert!(parse(&["--state-epochs-file", "epochs.txt"]).is_err());
    }

    #[test]
    fn a_state_epochs_file_takes_the_place_of_the_list() {
        assert!(parse(&[
            "--reproduce",
            "--state-epochs",
            "208",
            "--state-epochs-file",
            "epochs.txt",
        ])
        .is_err());

        let args = parse(&["--reproduce", "--state-epochs-file", "epochs.txt"]).unwrap();
        assert_eq!(args.state_epochs_file, Some(PathBuf::from("epochs.txt")));
    }
}
