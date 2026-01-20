use miette::{Context, IntoDiagnostic};
use mithril_client::{ClientBuilder, MessageBuilder, MithrilError, MithrilResult};
use std::{path::Path, sync::Arc};
use tracing::warn;

use dolos::prelude::*;

use crate::feedback::Feedback;
use dolos_core::config::MithrilConfig;

use super::Args;

struct MithrilFeedback {
    download_pb: indicatif::ProgressBar,
    validate_pb: indicatif::ProgressBar,
}

#[async_trait::async_trait]
impl mithril_client::feedback::FeedbackReceiver for MithrilFeedback {
    async fn handle_event(&self, event: mithril_client::feedback::MithrilEvent) {
        match event {
            mithril_client::feedback::MithrilEvent::SnapshotDownloadStarted { .. } => {
                self.download_pb.set_message("snapshot download started")
            }
            mithril_client::feedback::MithrilEvent::SnapshotDownloadProgress {
                downloaded_bytes,
                size,
                ..
            } => {
                self.download_pb.set_length(size);
                self.download_pb.set_position(downloaded_bytes);
                self.download_pb.set_message("downloading Mithril snapshot");
            }
            mithril_client::feedback::MithrilEvent::SnapshotDownloadCompleted { .. } => {
                self.download_pb.set_message("snapshot download completed");
            }
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
                self.validate_pb.set_message(format!("{x:?}"));
            }
        }
    }
}

pub(crate) async fn fetch_snapshot(
    args: &Args,
    config: &MithrilConfig,
    feedback: &Feedback,
) -> MithrilResult<()> {
    let feedback = MithrilFeedback {
        download_pb: feedback.bytes_progress_bar(),
        validate_pb: feedback.indeterminate_progress_bar(),
    };

    let client = ClientBuilder::aggregator(&config.aggregator, &config.genesis_key)
        .add_feedback_receiver(Arc::new(feedback))
        .set_ancillary_verification_key(config.ancillary_key.clone())
        .build()?;

    let snapshots = client.cardano_database().list().await?;

    let last_digest = snapshots
        .first()
        .ok_or(MithrilError::msg("no snapshot available"))?
        .digest
        .as_ref();

    let snapshot = client
        .cardano_database()
        .get(last_digest)
        .await?
        .ok_or(MithrilError::msg("no snapshot available"))?;

    let certificate = client
        .certificate()
        .verify_chain(&snapshot.certificate_hash)
        .await?;

    let target_directory = Path::new(&args.download_dir);

    client
        .cardano_database()
        .download_unpack(&snapshot, target_directory)
        .await?;

    if let Err(e) = client.cardano_database().add_statistics(&snapshot).await {
        warn!("failed incrementing snapshot download statistics: {:?}", e);
    }

    if !args.skip_validation {
        warn!("skipping validation, assuming snapshot is already validated");
        return Ok(());
    }

    let message = MessageBuilder::new()
        .compute_snapshot_message(&certificate, target_directory)
        .await?;

    assert!(certificate.match_message(&message));

    Ok(())
}

pub(crate) fn define_starting_point(
    args: &Args,
    state: &dolos_redb3::state::StateStore,
) -> Result<pallas::network::miniprotocols::Point, miette::Error> {
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

pub(crate) fn define_archive_starting_point(
    args: &Args,
    archive: &impl ArchiveStore,
) -> Result<pallas::network::miniprotocols::Point, miette::Error> {
    if let Some(point) = &args.start_from {
        return Ok(point.clone().try_into().unwrap());
    }

    let tip = archive
        .get_tip()
        .into_diagnostic()
        .context("reading archive tip")?;

    let Some((slot, raw)) = tip else {
        return Ok(pallas::network::miniprotocols::Point::Origin);
    };

    let block = dolos_cardano::owned::OwnedMultiEraBlock::decode(Arc::new(raw))
        .into_diagnostic()
        .context("decoding archive tip")?;

    let point = ChainPoint::Specific(slot, block.hash());

    Ok(point.try_into().unwrap())
}
