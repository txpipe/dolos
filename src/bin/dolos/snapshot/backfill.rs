//! `dolos snapshot backfill` — replay mithril history one epoch at a time,
//! publishing a stele at each boundary.
//!
//! Parse and process concerns. The daemon itself is
//! [`dolos_snapshot::backfill`], which owns the loop, the window arithmetic
//! and the import; what is here is what a binary owns and a library must not:
//! the operator's arguments, the tokio runtime the mithril calls are driven
//! on, the signal watcher that cancels them, the renderers, and the store and
//! domain constructions that reach into this crate's own backends.

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use dolos_core::config::RootConfig;
use dolos_snapshot::{
    backfill,
    export::Plan,
    registry::{self, Repository},
};
use indicatif::ProgressBar;
use miette::{bail, Context as _, IntoDiagnostic as _};
use tokio_util::sync::CancellationToken;

use crate::feedback::Feedback;
use dolos::adapters::{ArchiveStoreBackend, DomainAdapter, IndexStoreBackend, StateStoreBackend};

/// Where the mithril window lands when the operator names nowhere: beside the
/// stores, so the bytes stay on the data mount.
const DOWNLOAD_DIR: &str = "mithril";

#[derive(Debug, Parser)]
pub struct Args {
    /// OCI repository to publish into, e.g.
    /// `oci://ghcr.io/txpipe/dolos-mainnet`
    #[arg(long, value_name = "OCI_URL")]
    repo: Repository,

    /// talk to the repository over plaintext HTTP rather than HTTPS; for a
    /// registry on a loopback address or a mirror inside a cluster, and for
    /// nothing reachable from outside one
    #[arg(long, action)]
    insecure: bool,

    /// directory to stage layers in while they are uploaded; defaults to
    /// `<storage.path>/scratch`
    #[arg(long, value_name = "DIR")]
    scratch_dir: Option<PathBuf>,

    /// how many layer round trips to run at once against the repository; see
    /// `dolos snapshot publish --concurrency`. Defaults to 8
    #[arg(long, value_name = "N")]
    concurrency: Option<std::num::NonZeroUsize>,

    /// check that the repository still holds every layer carried forward from
    /// the previous stele; see `dolos snapshot publish --verify-carried`. One
    /// round trip per carried layer, every epoch
    #[arg(long, action)]
    verify_carried: bool,

    /// directory the mithril immutable files are downloaded into; defaults to
    /// `<storage.path>/mithril`
    #[arg(long, value_name = "DIR")]
    download_dir: Option<PathBuf>,

    /// immutable files fetched per download round
    #[arg(long, default_value = "40")]
    window: u64,

    /// stop after publishing this sequence; for smoke tests
    #[arg(long, value_name = "N")]
    until_epoch: Option<u64>,

    /// skip the mithril digest and merkle validation; the certificate chain
    /// is still verified. local smoke tests only
    #[arg(long, action)]
    skip_validation: bool,
}

/// SIGTERM/SIGINT as a token the synchronous loop polls between chunks.
///
/// The driver has no ambient tokio runtime for `hook_exit_token`, so the
/// signal wait gets a dedicated thread with a current-thread runtime of its
/// own.
fn spawn_exit_watcher() -> miette::Result<CancellationToken> {
    let cancel = CancellationToken::new();
    let hooked = cancel.clone();

    std::thread::Builder::new()
        .name("exit-signal".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("building the signal-wait runtime");

            runtime.block_on(async {
                crate::common::wait_for_exit_signal().await;
                tracing::warn!("shutdown requested; stopping at the next chunk");
                hooked.cancel();
            });
        })
        .into_diagnostic()
        .context("spawning the signal-wait thread")?;

    Ok(cancel)
}

/// The replay's bar, one per epoch round.
///
/// A cell rather than a bar held for the whole run: the daemon opens and
/// abandons a round per epoch boundary, which is the same bar-per-round the
/// loop drew before it moved into the library.
struct ReplayBar<'a> {
    feedback: &'a Feedback,
    bar: std::cell::RefCell<Option<ProgressBar>>,
}

impl<'a> ReplayBar<'a> {
    fn new(feedback: &'a Feedback) -> Self {
        Self {
            feedback,
            bar: std::cell::RefCell::new(None),
        }
    }
}

impl backfill::Replay for ReplayBar<'_> {
    fn round_started(&self) {
        let bar = self.feedback.slot_progress_bar();
        bar.set_message("replaying immutable blocks");
        *self.bar.borrow_mut() = Some(bar);
    }

    fn reached(&self, slot: dolos_core::BlockSlot) {
        if let Some(bar) = self.bar.borrow().as_ref() {
            bar.set_position(slot);
        }
    }

    fn round_finished(&self) {
        if let Some(bar) = self.bar.borrow_mut().take() {
            bar.abandon_with_message("replay round complete");
        }
    }
}

/// The repository arm, as `snapshot publish --repo` renders it.
struct RepositoryArm<'a> {
    config: &'a RootConfig,
    args: &'a Args,
    feedback: &'a Feedback,
}

impl RepositoryArm<'_> {
    fn settings(&self) -> dolos_snapshot::publisher::RepositoryPublish<'_> {
        dolos_snapshot::publisher::RepositoryPublish {
            repo: &self.args.repo,
            insecure: self.args.insecure,
            scratch_dir: self.args.scratch_dir.as_deref(),
            rebuild: false,
            dry_run: false,
            require_new: false,
            tuning: registry::Tuning {
                concurrency: self.args.concurrency,
                verify_adopted: self.args.verify_carried,
            },
        }
    }
}

impl backfill::Publish<DomainAdapter> for RepositoryArm<'_> {
    fn announce(&self, plan: &Plan) -> Result<(), backfill::Error> {
        super::report_plan(plan).map_err(backfill::Error::caller)
    }

    fn publish(
        &self,
        plan: &Plan,
        archive: &ArchiveStoreBackend,
        state: &StateStoreBackend,
        indexes: &IndexStoreBackend,
    ) -> Result<(), backfill::Error> {
        super::publish::to_repository(
            self.config,
            &self.settings(),
            plan,
            archive,
            state,
            indexes,
            self.feedback,
        )
        .map_err(backfill::Error::caller)
    }
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    if args.window == 0 {
        bail!("--window must be at least 1");
    }

    let Some(mithril) = config.mithril.as_ref() else {
        bail!("missing mithril config");
    };

    // Loaded here rather than at first use so a run whose genesis is missing
    // fails before it downloads anything.
    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    let download_dir = args
        .download_dir
        .clone()
        .unwrap_or_else(|| config.storage.path.join(DOWNLOAD_DIR));

    std::fs::create_dir_all(&download_dir)
        .into_diagnostic()
        .with_context(|| format!("creating the download dir {}", download_dir.display()))?;

    // For the async mithril calls only. The registry client owns a
    // current-thread runtime of its own and must never run inside this one, so
    // every publish stays on the plain thread.
    let runtime = tokio::runtime::Runtime::new()
        .into_diagnostic()
        .context("creating the tokio runtime for mithril downloads")?;

    let replay = ReplayBar::new(feedback);

    let publish = RepositoryArm {
        config,
        args,
        feedback,
    };

    let driver = backfill::Driver::<DomainAdapter> {
        config,
        genesis: &genesis,
        mithril,
        download_dir,
        window: args.window,
        until_epoch: args.until_epoch,
        skip_validation: args.skip_validation,
        runtime: runtime.handle().clone(),
        cancel: spawn_exit_watcher()?,
        // A receiver per round, so a window's bars are its own.
        mithril_feedback: &|| {
            Some(Arc::new(crate::feedback::MithrilFeedback::new(feedback))
                as Arc<
                    dyn dolos_mithril::mithril_client::feedback::FeedbackReceiver,
                >)
        },
        replay: &replay,
        open_stores: &|| {
            let stores = crate::common::open_data_stores(config)
                .into_diagnostic()
                .context("opening the data stores")
                .map_err(backfill::Error::caller)?;

            Ok(backfill::Stores::<DomainAdapter> {
                wal: stores.wal,
                state: stores.state,
                archive: stores.archive,
                indexes: stores.indexes,
            })
        },
        build_domain: &|target| {
            crate::common::setup_domain_with_stop_epoch(config, Some(target))
                .map_err(backfill::Error::caller)
        },
        shutdown_domain: &|domain: &DomainAdapter| {
            domain
                .shutdown()
                .map_err(|e| backfill::Error::caller(format!("shutting down the domain: {e}")))
        },
        publish: &publish,
    };

    match driver.run().into_diagnostic()? {
        backfill::Outcome::UntilEpoch { sequence } => {
            println!("sequence {sequence} published; stopping at --until-epoch");
        }
        backfill::Outcome::UpToDate => {
            println!("the repository is up to date with mithril; nothing left to backfill");
        }
    }

    Ok(())
}
