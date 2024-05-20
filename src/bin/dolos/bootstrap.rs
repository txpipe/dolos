use dolos::ledger;
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use itertools::Itertools;
use miette::{bail, Context, IntoDiagnostic};
use mithril_client::{ClientBuilder, MessageBuilder, MithrilError, MithrilResult};
use pallas::{
    ledger::{
        configs::{byron, shelley},
        traverse::MultiEraBlock,
    },
    storage::rolldb::{chain, wal},
};
use std::{path::Path, sync::Arc};
use tracing::{debug, info, warn};

use crate::{common::Stores, MithrilConfig};

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(long, default_value = "./snapshot")]
    download_dir: String,

    /// Skip the bootstrap if there's already data in the stores
    #[arg(long, action)]
    skip_if_not_empty: bool,

    /// Skip the Mithril certificate validation
    #[arg(long, action)]
    skip_validation: bool,

    /// Delete any existing data and continue with bootstrap
    #[arg(long, short, action)]
    force: bool,

    /// Assume the snapshot is already available in the download dir
    #[arg(long, action)]
    skip_download: bool,

    /// Retain downloaded snapshot instead of deleting it
    #[arg(long, action)]
    retain_snapshot: bool,
}

struct Feedback {
    multi: MultiProgress,
    download_pb: ProgressBar,
    validate_pb: ProgressBar,
    wal_pb: ProgressBar,
    ledger_pb: ProgressBar,
    chain_pb: ProgressBar,
}

impl Feedback {
    fn indeterminate_progress_bar(owner: &mut MultiProgress) -> ProgressBar {
        let pb = ProgressBar::new_spinner();

        pb.set_style(
            ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg}").unwrap(),
        );

        owner.add(pb)
    }

    fn slot_progress_bar(owner: &mut MultiProgress) -> ProgressBar {
        let pb = ProgressBar::new_spinner();

        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} slots (eta: {eta}) {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        owner.add(pb)
    }

    fn bytes_progress_bar(owner: &mut MultiProgress) -> ProgressBar {
        let pb = ProgressBar::new_spinner();

        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {bytes}/{total_bytes} (eta: {eta}) {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        owner.add(pb)
    }
}

impl Default for Feedback {
    fn default() -> Self {
        let mut multi = MultiProgress::new();

        Self {
            download_pb: Self::bytes_progress_bar(&mut multi),
            validate_pb: Self::indeterminate_progress_bar(&mut multi),
            wal_pb: Self::slot_progress_bar(&mut multi),
            ledger_pb: Self::slot_progress_bar(&mut multi),
            chain_pb: Self::slot_progress_bar(&mut multi),
            multi,
        }
    }
}

#[async_trait::async_trait]
impl mithril_client::feedback::FeedbackReceiver for Feedback {
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
                self.validate_pb.set_message("snapshot download completed");
            }
            mithril_client::feedback::MithrilEvent::CertificateChainValidationStarted {
                ..
            } => info!("certificate chain validation started"),
            mithril_client::feedback::MithrilEvent::CertificateValidated {
                certificate_chain_validation_id: id,
                certificate_hash: hash,
            } => {
                self.validate_pb
                    .set_message(format!("certificate validated {id}: {hash}"));
            }
            mithril_client::feedback::MithrilEvent::CertificateChainValidated { .. } => {
                info!("certificate chain validated")
            }
        }
    }
}

async fn fetch_snapshot(
    args: &Args,
    config: &MithrilConfig,
    feedback: Arc<Feedback>,
) -> MithrilResult<()> {
    let client = ClientBuilder::aggregator(&config.aggregator, &config.genesis_key)
        .add_feedback_receiver(feedback)
        .build()?;

    let snapshots = client.snapshot().list().await?;

    let last_digest = snapshots
        .first()
        .ok_or(MithrilError::msg("no snapshot available"))?
        .digest
        .as_ref();

    let snapshot = client
        .snapshot()
        .get(last_digest)
        .await?
        .ok_or(MithrilError::msg("no snapshot available"))?;

    let target_directory = Path::new(&args.download_dir);

    client
        .snapshot()
        .download_unpack(&snapshot, target_directory)
        .await?;

    if let Err(e) = client.snapshot().add_statistics(&snapshot).await {
        warn!("failed incrementing snapshot download statistics: {:?}", e);
    }

    let certificate = if args.skip_validation {
        client
            .certificate()
            .get(&snapshot.certificate_hash)
            .await?
            .ok_or(MithrilError::msg("certificate for snapshot not found"))?
    } else {
        client
            .certificate()
            .verify_chain(&snapshot.certificate_hash)
            .await?
    };

    let message = MessageBuilder::new()
        .compute_snapshot_message(&certificate, target_directory)
        .await?;

    assert!(certificate.match_message(&message));

    Ok(())
}

fn open_empty_stores(config: &super::Config, force: bool) -> miette::Result<Option<Stores>> {
    let mut stores = crate::common::open_data_stores(config)?;

    let empty = stores.0.is_empty() && stores.1.is_empty() && stores.2.is_empty();

    match (empty, force) {
        (true, _) => Ok(Some(stores)),
        (false, true) => {
            drop(stores);

            crate::common::destroy_data_stores(config)
                .context("destroying existing data stored")?;

            stores = crate::common::open_data_stores(config)?;
            Ok(Some(stores))
        }
        (false, false) => Ok(None),
    }
}

fn import_hardano_into_wal(
    immutable_path: &Path,
    feedback: &Feedback,
    wal: &mut pallas::storage::rolldb::wal::Store,
) -> Result<(), miette::Error> {
    let iter = pallas::storage::hardano::immutable::read_blocks(&immutable_path)
        .into_diagnostic()
        .context("reading immutable db")?;

    let tip = pallas::storage::hardano::immutable::get_tip(&immutable_path)
        .map_err(|err| miette::miette!(err.to_string()))
        .context("reading immutable db tip")?
        .ok_or(miette::miette!("immutable db has no tip"))?;

    feedback.wal_pb.set_message("importing WAL");
    feedback.wal_pb.set_length(tip.slot_or_default());

    for block in iter {
        let block = block.into_diagnostic().context("reading block")?;

        let blockd = MultiEraBlock::decode(&block)
            .into_diagnostic()
            .context("decoding block cbor")?;

        debug!(slot = blockd.slot(), "importing block");

        wal.roll_forward(blockd.slot(), blockd.hash(), block.clone())
            .into_diagnostic()
            .context("adding chain entry")?;

        feedback.wal_pb.set_position(blockd.slot());
    }

    Ok(())
}

fn rebuild_ledger_from_wal(
    feedback: &Feedback,
    wal: &wal::Store,
    ledger: &mut impl ledger::LedgerStore,
    byron: &byron::GenesisFile,
    shelley: &shelley::GenesisFile,
) -> miette::Result<()> {
    let delta = dolos::ledger::compute_origin_delta(&byron);

    ledger
        .apply(&[delta])
        .into_diagnostic()
        .context("applying origin utxos")?;

    let tip = wal
        .find_tip()
        .into_diagnostic()
        .context("finding WAL tip")?
        .ok_or(miette::miette!("no WAL tip found"))?;

    feedback.wal_pb.set_message("re-building ledger");
    feedback.ledger_pb.set_length(tip.0);

    let remaining = wal.crawl_after(None);

    for chunk in remaining.chunks(100).into_iter() {
        let blocks: Vec<_> = chunk
            .map_ok(|b| b.1)
            .filter_map_ok(|l| match l {
                pallas::storage::rolldb::wal::Log::Apply(_, _, body) => Some(body),
                _ => None,
            })
            .try_collect()
            .into_diagnostic()
            .context("fetching blocks")?;

        let blocks: Vec<_> = blocks
            .iter()
            .map(|body| MultiEraBlock::decode(body))
            .try_collect()
            .into_diagnostic()
            .context("decoding blocks")?;

        dolos::ledger::import_block_batch(&blocks, ledger, &byron, &shelley)
            .into_diagnostic()
            .context("importing blocks to ledger store")?;

        blocks
            .last()
            .inspect(|b| feedback.ledger_pb.set_position(b.slot()));
    }

    Ok(())
}

fn rebuild_chain_from_wal(
    feedback: &Feedback,
    wal: &wal::Store,
    chain: &mut chain::Store,
) -> miette::Result<()> {
    let (tip, _) = wal
        .find_tip()
        .into_diagnostic()
        .context("finding wal tip")?
        .ok_or(miette::miette!("wal is empty"))?;

    feedback.chain_pb.set_message("rebuilding chain from wal");
    feedback.chain_pb.set_length(tip);

    let remaining = wal.crawl_after(None);

    for block in remaining {
        let (slot, log) = block.into_diagnostic()?;

        debug!(slot, "filling up chain");

        match log {
            pallas::storage::rolldb::wal::Log::Apply(slot, hash, body) => {
                chain
                    .roll_forward(slot, hash, body)
                    .into_diagnostic()
                    .context("rolling chain forward")?;

                feedback.chain_pb.set_position(slot);
            }
            _ => (),
        };
    }

    Ok(())
}

pub fn run(config: &super::Config, args: &Args) -> miette::Result<()> {
    //crate::common::setup_tracing(&config.logging)?;
    let feedback = Arc::new(Feedback::default());

    let mithril = config
        .mithril
        .as_ref()
        .ok_or(miette::miette!("missing mithril config"))?;

    let empty_stores =
        open_empty_stores(config, args.force).context("opening empty data stored")?;

    if empty_stores.is_none() && args.skip_if_not_empty {
        warn!("data stores are not empty, skipping bootstrap");
        return Ok(());
    } else if empty_stores.is_none() {
        bail!("data stores must be empty to execute bootstrap");
    }

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
            .block_on(fetch_snapshot(args, mithril, feedback.clone()))
            .map_err(|err| miette::miette!(err.to_string()))
            .context("fetching and validating mithril snapshot")?;
    } else {
        warn!("skipping download, assuming download dir has snapshot and it's validated")
    }

    let immutable_path = Path::new(&args.download_dir).join("immutable");

    let (mut wal, mut chain, mut ledger) = empty_stores.unwrap();

    let (byron, shelley, _) = crate::common::open_genesis_files(&config.genesis)?;

    import_hardano_into_wal(&immutable_path, &feedback, &mut wal)?;

    rebuild_ledger_from_wal(&feedback, &wal, &mut ledger, &byron, &shelley)?;

    rebuild_chain_from_wal(&feedback, &wal, &mut chain)?;

    if !args.retain_snapshot {
        info!("deleting downloaded snapshot");

        std::fs::remove_dir_all(Path::new(&args.download_dir))
            .into_diagnostic()
            .context("removing downloaded snapshot")?;
    }

    println!("bootstrap complete, run `dolos daemon` to start the node");

    Ok(())
}
