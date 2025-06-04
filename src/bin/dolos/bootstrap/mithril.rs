use itertools::Itertools;
use miette::{bail, Context, IntoDiagnostic};
use mithril_client::{ClientBuilder, MessageBuilder, MithrilError, MithrilResult};
use pallas::ledger::traverse::MultiEraBlock;
use std::{path::Path, sync::Arc};
use tracing::{debug, info, warn};

use dolos::prelude::*;

use crate::{feedback::Feedback, MithrilConfig};

#[derive(Debug, clap::Args, Clone)]
pub struct Args {
    #[arg(long, default_value = "./snapshot")]
    download_dir: String,

    /// Skip the bootstrap if there's already data in the stores
    #[arg(long, action)]
    skip_if_not_empty: bool,

    /// Skip the Mithril certificate validation
    #[arg(long, action)]
    skip_validation: bool,

    /// Assume the snapshot is already available in the download dir
    #[arg(long, action)]
    skip_download: bool,

    /// Retain downloaded snapshot instead of deleting it
    #[arg(long, action)]
    retain_snapshot: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            download_dir: "./snapshot".to_string(),
            skip_if_not_empty: Default::default(),
            skip_validation: Default::default(),
            skip_download: Default::default(),
            retain_snapshot: Default::default(),
        }
    }
}

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
                self.validate_pb.set_message(format!("{:?}", x));
            }
        }
    }
}

async fn fetch_snapshot(
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

fn open_empty_wal(config: &crate::Config) -> miette::Result<dolos_redb::wal::RedbWalStore> {
    let wal = match crate::common::open_wal_store(config)? {
        dolos::adapters::WalAdapter::Redb(x) => x,
        _ => bail!("only redb wal adapter is supported"),
    };

    let is_empty = wal.is_empty().map_err(WalError::from).into_diagnostic()?;

    if !is_empty {
        bail!("can't continue with data already available");
    }

    Ok(wal)
}

fn import_hardano_into_wal(
    config: &crate::Config,
    immutable_path: &Path,
    feedback: &Feedback,
) -> Result<(), miette::Error> {
    let iter = pallas::storage::hardano::immutable::read_blocks(immutable_path)
        .into_diagnostic()
        .context("reading immutable db")?;

    let tip = pallas::storage::hardano::immutable::get_tip(immutable_path)
        .map_err(|err| miette::miette!(err.to_string()))
        .context("reading immutable db tip")?
        .ok_or(miette::miette!("immutable db has no tip"))?;

    let mut wal = open_empty_wal(config).context("opening WAL")?;

    wal.initialize_from_origin()
        .map_err(WalError::from)
        .into_diagnostic()
        .context("initializing WAL")?;

    let progress = feedback.slot_progress_bar();

    progress.set_message("importing immutable db from Haskell into WAL");
    progress.set_length(tip.slot_or_default());

    for chunk in iter.chunks(100).into_iter() {
        let bodies: Vec<_> = chunk
            .try_collect()
            .into_diagnostic()
            .context("reading block data")?;

        let blocks: Vec<_> = bodies
            .iter()
            .map(|b| {
                let blockd = MultiEraBlock::decode(b)
                    .into_diagnostic()
                    .context("decoding block cbor")?;

                progress.set_position(blockd.slot());
                debug!(slot = blockd.slot(), "importing block");

                miette::Result::Ok(RawBlock {
                    slot: blockd.slot(),
                    hash: blockd.hash(),
                    era: blockd.era(),
                    body: b.clone(),
                })
            })
            .try_collect::<_, _, miette::Report>()?;

        wal.roll_forward(blocks.into_iter())
            .into_diagnostic()
            .context("adding wal entries")?;
    }

    progress.abandon_with_message("WAL import complete");

    Ok(())
}

pub fn run(config: &crate::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    //crate::common::setup_tracing(&config.logging)?;

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
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(fetch_snapshot(args, mithril, feedback))
            .map_err(|err| miette::miette!(err.to_string()))
            .context("fetching and validating mithril snapshot")?;
    } else {
        warn!("skipping download, assuming download dir has snapshot and it's validated")
    }

    let immutable_path = Path::new(&args.download_dir).join("immutable");

    import_hardano_into_wal(config, &immutable_path, feedback)?;

    crate::doctor::run_rebuild_stores(config, feedback).context("rebuilding ledger and chain")?;

    if !args.retain_snapshot {
        info!("deleting downloaded snapshot");

        std::fs::remove_dir_all(Path::new(&args.download_dir))
            .into_diagnostic()
            .context("removing downloaded snapshot")?;
    }

    println!("bootstrap complete, run `dolos daemon` to start the node");

    Ok(())
}
