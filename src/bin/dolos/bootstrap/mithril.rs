use dolos_core::config::{MithrilConfig, RootConfig};
use dolos_core::ImportExt;
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use mithril_client::cardano_database_client::{DownloadUnpackOptions, ImmutableFileRange};
use mithril_client::{
    AggregatorDiscoveryType, ClientBuilder, MessageBuilder, MithrilError, MithrilResult,
};
use std::{path::Path, sync::Arc};
use tracing::{info, warn};

use crate::feedback::Feedback;
use dolos::prelude::*;

#[derive(Debug, clap::Args, Clone)]
pub struct Args {
    #[arg(long, default_value = "./snapshot")]
    download_dir: String,

    /// Skip the Mithril certificate validation
    #[arg(long, action)]
    skip_validation: bool,

    /// Assume the snapshot is already available in the download dir
    #[arg(long, action)]
    skip_download: bool,

    /// Retain downloaded snapshot instead of deleting it
    #[arg(long, action)]
    retain_snapshot: bool,

    /// Number of blocks to process in each chunk, more is faster but uses more
    /// memory
    #[arg(long, default_value = "500")]
    chunk_size: usize,

    #[arg(long)]
    start_from: Option<ChainPoint>,

    /// Start downloading from this immutable file number (inclusive)
    #[arg(long)]
    download_start: Option<u64>,

    /// Download up to this immutable file number (inclusive)
    #[arg(long)]
    download_end: Option<u64>,
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

struct MithrilFeedback {
    aggregate_pb: indicatif::ProgressBar,
    validate_pb: indicatif::ProgressBar,
}

impl MithrilFeedback {
    fn new(feedback: &Feedback) -> Self {
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

/// Scan the immutable directory for the highest immutable file number present.
fn highest_existing_immutable(immutable_dir: &Path) -> Option<u64> {
    let entries = std::fs::read_dir(immutable_dir).ok()?;
    let mut max: Option<u64> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(num_str) = name.split('.').next().and_then(|s| s.parse::<u64>().ok()) {
            max = Some(max.map_or(num_str, |m| m.max(num_str)));
        }
    }
    max
}

/// Ranges of immutable files to download and to verify.
struct DownloadPlan {
    /// Range to download, if any. `None` when files on disk already cover the
    /// snapshot.
    download: Option<ImmutableFileRange>,
    /// Range to verify against the certificate.
    verify: ImmutableFileRange,
}

/// Build the explicit range requested via CLI args, if any.
fn explicit_range(args: &Args) -> Option<ImmutableFileRange> {
    match (args.download_start, args.download_end) {
        (Some(start), Some(end)) => Some(ImmutableFileRange::Range(start, end)),
        (Some(start), None) => Some(ImmutableFileRange::From(start)),
        (None, Some(end)) => Some(ImmutableFileRange::UpTo(end)),
        (None, None) => None,
    }
}

/// Compute the download & verification plan based on CLI args, existing files
/// on disk and the snapshot's last immutable file number.
///
/// When an explicit range is given, both download and verification are scoped
/// to it. Otherwise the full range is verified; if immutables already exist
/// locally, the download resumes from the highest file present. The highest
/// file is re-fetched (not skipped) because an interrupted run may have left
/// it truncated, and it's never verified until this run completes.
fn plan_download(args: &Args, immutable_dir: &Path, last_immutable: u64) -> DownloadPlan {
    if let Some(verify) = explicit_range(args) {
        return DownloadPlan {
            download: explicit_range(args),
            verify,
        };
    }

    let download = match highest_existing_immutable(immutable_dir) {
        Some(highest) if highest > last_immutable => {
            info!(
                highest,
                last_immutable, "local immutable files already cover the snapshot"
            );
            None
        }
        Some(highest) => {
            info!(highest, "resuming download from immutable file {highest}");
            Some(ImmutableFileRange::From(highest))
        }
        None => Some(ImmutableFileRange::Full),
    };

    DownloadPlan {
        download,
        verify: ImmutableFileRange::Full,
    }
}

async fn fetch_snapshot(
    args: &Args,
    config: &MithrilConfig,
    feedback: &Feedback,
) -> MithrilResult<()> {
    let feedback = MithrilFeedback::new(feedback);

    let client = ClientBuilder::new(AggregatorDiscoveryType::Url(config.aggregator.clone()))
        .set_genesis_verification_key(mithril_client::GenesisVerificationKey::JsonHex(
            config.genesis_key.clone(),
        ))
        .add_feedback_receiver(Arc::new(feedback))
        .build()?;

    let db_client = client.cardano_database_v2();

    let snapshots = db_client.list().await?;

    let last_digest = snapshots
        .iter()
        .max_by_key(|s| s.beacon.immutable_file_number)
        .ok_or(MithrilError::msg("no snapshot available"))?
        .hash
        .as_str();

    let snapshot = db_client
        .get(last_digest)
        .await?
        .ok_or(MithrilError::msg("no snapshot available"))?;

    let certificate = client
        .certificate()
        .verify_chain(&snapshot.certificate_hash)
        .await?;

    let target_directory = Path::new(&args.download_dir);
    let immutable_dir = target_directory.join("immutable");

    let last_immutable = snapshot.beacon.immutable_file_number;
    let plan = plan_download(args, &immutable_dir, last_immutable);

    if let Some(immutable_range) = &plan.download {
        let download_opts = DownloadUnpackOptions {
            allow_override: true,
            include_ancillary: false,
            ..DownloadUnpackOptions::default()
        };

        db_client
            .download_unpack(&snapshot, immutable_range, target_directory, download_opts)
            .await?;

        let nb_files = immutable_range.length(last_immutable);

        if let Err(e) = db_client
            .add_statistics(
                *immutable_range == ImmutableFileRange::Full,
                false,
                nb_files,
            )
            .await
        {
            warn!("failed incrementing snapshot download statistics: {:?}", e);
        }
    }

    if !args.skip_validation {
        let verified_digests = db_client
            .download_and_verify_digests(&certificate, &snapshot)
            .await?;

        let merkle_proof = db_client
            .verify_cardano_database(
                &certificate,
                &snapshot,
                &plan.verify,
                false,
                target_directory,
                &verified_digests,
            )
            .await
            .map_err(|e| MithrilError::msg(format!("verification failed: {e:?}")))?;

        let message = MessageBuilder::new()
            .compute_cardano_database_message(&certificate, &merkle_proof)
            .await?;

        if !certificate.match_message(&message) {
            return Err(MithrilError::msg(
                "mithril certificate does not match the downloaded snapshot",
            ));
        }
    } else {
        warn!("skipping validation, assuming snapshot is already validated");
    }

    Ok(())
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

        rt.block_on(fetch_snapshot(args, mithril, feedback))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args_with_range(start: Option<u64>, end: Option<u64>) -> Args {
        Args {
            download_start: start,
            download_end: end,
            ..Default::default()
        }
    }

    fn touch_immutables(dir: &Path, numbers: impl IntoIterator<Item = u64>) {
        for n in numbers {
            for ext in ["chunk", "primary", "secondary"] {
                std::fs::write(dir.join(format!("{n:05}.{ext}")), []).unwrap();
            }
        }
    }

    #[test]
    fn plan_uses_explicit_range_for_download_and_verify() {
        let dir = tempfile::tempdir().unwrap();

        let plan = plan_download(&args_with_range(Some(5), Some(8)), dir.path(), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::Range(5, 8)));
        assert_eq!(plan.verify, ImmutableFileRange::Range(5, 8));

        let plan = plan_download(&args_with_range(Some(5), None), dir.path(), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::From(5)));
        assert_eq!(plan.verify, ImmutableFileRange::From(5));

        let plan = plan_download(&args_with_range(None, Some(8)), dir.path(), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::UpTo(8)));
        assert_eq!(plan.verify, ImmutableFileRange::UpTo(8));
    }

    #[test]
    fn plan_downloads_and_verifies_full_on_fresh_dir() {
        let dir = tempfile::tempdir().unwrap();

        let plan = plan_download(&args_with_range(None, None), dir.path(), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::Full));
        assert_eq!(plan.verify, ImmutableFileRange::Full);

        // a missing dir behaves like a fresh one
        let plan = plan_download(&args_with_range(None, None), &dir.path().join("nope"), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::Full));
    }

    #[test]
    fn plan_resumes_from_highest_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        touch_immutables(dir.path(), 0..=4);

        // the highest file is re-fetched (not skipped): an interrupted run may
        // have left it truncated
        let plan = plan_download(&args_with_range(None, None), dir.path(), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::From(4)));
        assert_eq!(plan.verify, ImmutableFileRange::Full);
    }

    #[test]
    fn plan_refetches_boundary_file_when_download_complete() {
        let dir = tempfile::tempdir().unwrap();
        touch_immutables(dir.path(), 0..=10);

        // highest == last must not produce an out-of-bounds range (From(11)
        // would make the mithril client fail with "invalid immutable file
        // range" when resuming after a crash during import)
        let plan = plan_download(&args_with_range(None, None), dir.path(), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::From(10)));
        assert_eq!(plan.verify, ImmutableFileRange::Full);
    }

    #[test]
    fn plan_skips_download_when_local_files_exceed_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        touch_immutables(dir.path(), 0..=11);

        let plan = plan_download(&args_with_range(None, None), dir.path(), 10);
        assert_eq!(plan.download, None);
        assert_eq!(plan.verify, ImmutableFileRange::Full);
    }

    #[test]
    fn highest_existing_ignores_non_numeric_files() {
        let dir = tempfile::tempdir().unwrap();
        touch_immutables(dir.path(), [0, 3]);
        std::fs::write(dir.path().join("lock"), []).unwrap();
        std::fs::write(dir.path().join("clean"), []).unwrap();

        assert_eq!(highest_existing_immutable(dir.path()), Some(3));
    }
}
