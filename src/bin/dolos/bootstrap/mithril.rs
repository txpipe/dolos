//! `dolos bootstrap mithril` — the interactive fetch-and-import.
//!
//! Parse and render: the download, its plan and its verification are
//! [`dolos_snapshot::mithril`]'s, shared with the backfill daemon. What is
//! here is the operator's arguments, the runtime the async fetch is driven
//! on, the progress bars it reports through, and the import into a domain the
//! root library assembles.

use dolos_core::config::RootConfig;
use dolos_core::ImportExt;
use dolos_snapshot::mithril::{fetch_snapshot, Fetch};
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use std::{path::Path, sync::Arc};
use tracing::{info, warn};

use crate::feedback::Feedback;
use dolos::prelude::*;

#[derive(Debug, clap::Args, Clone)]
pub struct Args {
    #[arg(long, default_value = "./snapshot")]
    pub(crate) download_dir: String,

    /// Skip the Mithril certificate validation
    #[arg(long, action)]
    pub(crate) skip_validation: bool,

    /// Assume the snapshot is already available in the download dir
    #[arg(long, action)]
    pub(crate) skip_download: bool,

    /// Retain downloaded snapshot instead of deleting it
    #[arg(long, action)]
    pub(crate) retain_snapshot: bool,

    /// Number of blocks to process in each chunk, more is faster but uses more
    /// memory
    #[arg(long, default_value = "500")]
    pub(crate) chunk_size: usize,

    #[arg(long)]
    pub(crate) start_from: Option<ChainPoint>,

    /// Start downloading from this immutable file number (inclusive)
    #[arg(long)]
    pub(crate) download_start: Option<u64>,

    /// Download up to this immutable file number (inclusive)
    #[arg(long)]
    pub(crate) download_end: Option<u64>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            download_dir: "./snapshot".to_string(),
            skip_validation: Default::default(),
            skip_download: Default::default(),
            retain_snapshot: Default::default(),
            chunk_size: 500,
            start_from: None,
            download_start: None,
            download_end: None,
        }
    }
}

pub(crate) struct MithrilFeedback {
    aggregate_pb: indicatif::ProgressBar,
    validate_pb: indicatif::ProgressBar,
}

impl MithrilFeedback {
    pub(crate) fn new(feedback: &Feedback) -> Self {
        let multi = feedback.multi_progress();

        let aggregate_pb = multi.add(indicatif::ProgressBar::hidden());
        aggregate_pb.set_style(
            indicatif::ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} files {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        aggregate_pb.set_message("downloading immutable files");

        let validate_pb = multi.add(indicatif::ProgressBar::new_spinner());
        validate_pb.set_style(
            indicatif::ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap(),
        );

        Self {
            aggregate_pb,
            validate_pb,
        }
    }
}

#[async_trait::async_trait]
impl mithril_client::feedback::FeedbackReceiver for MithrilFeedback {
    async fn handle_event(&self, event: mithril_client::feedback::MithrilEvent) {
        match event {
            mithril_client::feedback::MithrilEvent::CardanoDatabase(db_event) => match db_event {
                mithril_client::feedback::MithrilEventCardanoDatabase::Started {
                    total_immutable_files,
                    ..
                } => {
                    self.aggregate_pb
                        .set_draw_target(indicatif::ProgressDrawTarget::stderr());
                    self.aggregate_pb.set_length(total_immutable_files);
                    self.aggregate_pb.set_position(0);
                }
                mithril_client::feedback::MithrilEventCardanoDatabase::ImmutableDownloadCompleted {
                    ..
                } => {
                    self.aggregate_pb.inc(1);
                }
                mithril_client::feedback::MithrilEventCardanoDatabase::Completed { .. } => {
                    self.aggregate_pb.finish_with_message("download completed");
                }
                mithril_client::feedback::MithrilEventCardanoDatabase::DigestDownloadStarted {
                    size,
                    ..
                } => {
                    self.validate_pb.set_length(size);
                    self.validate_pb.set_position(0);
                    self.validate_pb.set_message("downloading digests");
                }
                mithril_client::feedback::MithrilEventCardanoDatabase::DigestDownloadProgress {
                    downloaded_bytes,
                    size,
                    ..
                } => {
                    self.validate_pb.set_length(size);
                    self.validate_pb.set_position(downloaded_bytes);
                    self.validate_pb.set_message("downloading digests");
                }
                mithril_client::feedback::MithrilEventCardanoDatabase::DigestDownloadCompleted {
                    ..
                } => {
                    self.validate_pb
                        .finish_with_message("digests downloaded");
                }
                _ => {
                    tracing::debug!("unhandled mithril event: {db_event:?}");
                }
            },
            mithril_client::feedback::MithrilEvent::CertificateChainValidationStarted {
                ..
            } => {
                self.validate_pb
                    .set_message("certificate chain validation started");
            }
            mithril_client::feedback::MithrilEvent::CertificateValidated {
                certificate_hash: hash,
                ..
            } => {
                self.validate_pb
                    .set_message(format!("validating cert: {hash}"));
            }
            mithril_client::feedback::MithrilEvent::CertificateChainValidated { .. } => {
                self.validate_pb.set_message("certificate chain validated");
            }
            mithril_client::feedback::MithrilEvent::CertificateFetchedFromCache { .. } => {
                self.validate_pb
                    .set_message("certificate fetched from cache");
            }
            x => {
                tracing::debug!("unhandled mithril event: {x:?}");
            }
        }
    }
}

fn define_starting_point(
    args: &Args,
    state: &dolos::storage::StateStoreBackend,
) -> Result<pallas::network::miniprotocols::Point, miette::Error> {
    use dolos_core::StateStore;

    if let Some(point) = &args.start_from {
        Ok(point.clone().try_into().unwrap())
    } else {
        let cursor = state
            .read_cursor()
            .into_diagnostic()
            .context("reading state cursor")?;

        let point = cursor
            .map(|c| c.try_into().unwrap())
            .unwrap_or(pallas::network::miniprotocols::Point::Origin);

        Ok(point)
    }
}

/// Inner import function that can return errors.
/// The outer function ensures shutdown is called regardless of success/failure.
fn do_import(
    domain: &dolos::adapters::DomainAdapter,
    args: &Args,
    immutable_path: &Path,
    feedback: &Feedback,
    chunk_size: usize,
) -> Result<(), miette::Error> {
    let tip = pallas::interop::hardano::storage::immutable::get_tip(immutable_path)
        .map_err(|err| miette::miette!(err.to_string()))
        .context("reading immutable db tip")?
        .ok_or(miette::miette!("immutable db has no tip"))?;

    let cursor = define_starting_point(args, domain.state())?;

    let mut iter = pallas::interop::hardano::storage::immutable::read_blocks_from_point(
        immutable_path,
        cursor.clone(),
    )
    .map_err(|err| miette::miette!(err.to_string()))
    .context("reading immutable db tip")?;

    // unless we're starting from the origin of the chain, we need to skip the first
    // result since the iterator will be standing in the last slot already
    // processed, we don't want to import it twice.
    if cursor != pallas::network::miniprotocols::Point::Origin {
        iter.next();
    }

    let progress = feedback.slot_progress_bar();

    progress.set_message("importing immutable db");
    progress.set_length(tip.slot_or_default());

    for batch in iter.chunks(chunk_size).into_iter() {
        let batch: Vec<_> = batch
            .try_collect()
            .into_diagnostic()
            .context("reading block data")?;

        // we need to wrap them on a ref counter since bytes are going to be shared
        // around throughout the pipeline
        let batch: Vec<_> = batch.into_iter().map(Arc::new).collect();

        let last = domain
            .import_blocks(batch)
            .map_err(|e| miette::miette!(e.to_string()))?;

        progress.set_position(last);
    }

    progress.abandon_with_message("immutable db import complete");

    Ok(())
}

fn import_hardano_into_domain(
    args: &Args,
    config: &RootConfig,
    immutable_path: &Path,
    feedback: &Feedback,
    chunk_size: usize,
) -> Result<(), miette::Error> {
    let domain = crate::common::setup_domain(config)?;

    let result = do_import(&domain, args, immutable_path, feedback, chunk_size);

    // Always shutdown the domain before it goes out of scope, regardless of
    // whether import succeeded or failed.
    if let Err(e) = domain.shutdown() {
        tracing::error!("error during domain shutdown: {}", e);
    }

    result
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    let mithril = config
        .mithril
        .as_ref()
        .ok_or(miette::miette!("missing mithril config"))?;

    let target_directory = Path::new(&args.download_dir);

    if !target_directory.exists() {
        std::fs::create_dir_all(target_directory)
            .map_err(|err| miette::miette!(err.to_string()))
            .context(format!(
                "Failed to create directory: {}",
                target_directory.display()
            ))?;
    }

    if !args.skip_download {
        // Spawn a temporary Tokio runtime just for the async download
        let rt = tokio::runtime::Runtime::new()
            .into_diagnostic()
            .context("creating tokio runtime for download")?;

        let fetch = Fetch {
            download_dir: target_directory,
            skip_validation: args.skip_validation,
            download_start: args.download_start,
            download_end: args.download_end,
        };

        let receiver = Arc::new(MithrilFeedback::new(feedback));

        rt.block_on(fetch_snapshot(&fetch, mithril, Some(receiver)))
            .map_err(|err| miette::miette!(err.to_string()))
            .context("fetching and validating mithril snapshot")?;
    } else {
        warn!("skipping download, assuming download dir has snapshot and it's validated")
    }

    let immutable_path = Path::new(&args.download_dir).join("immutable");

    // Import is now fully sync - no Tokio runtime needed
    import_hardano_into_domain(args, config, &immutable_path, feedback, args.chunk_size)?;

    if !args.retain_snapshot {
        info!("deleting downloaded snapshot");

        std::fs::remove_dir_all(Path::new(&args.download_dir))
            .into_diagnostic()
            .context("removing downloaded snapshot")?;
    }

    info!("bootstrap complete, run `dolos daemon` to start the node");

    Ok(())
}
