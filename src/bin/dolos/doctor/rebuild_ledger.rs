use dolos::{
    ledger,
    wal::{self, RawBlock, ReadUtils, WalReader as _},
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use pallas::ledger::traverse::MultiEraBlock;
use tracing::debug;

struct Feedback {
    _multi: MultiProgress,
    global_pb: ProgressBar,
}

impl Default for Feedback {
    fn default() -> Self {
        let multi = MultiProgress::new();

        let global_pb = ProgressBar::new_spinner();
        let global_pb = multi.add(global_pb);
        global_pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} (eta: {eta}) {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        Self {
            _multi: multi,
            global_pb,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &crate::Config, _args: &Args) -> miette::Result<()> {
    //crate::common::setup_tracing(&config.logging)?;

    let feedback = Feedback::default();

    let (byron, shelley, _) = crate::common::open_genesis_files(&config.genesis)?;

    let (wal, mut ledger) =
        crate::common::open_data_stores(config).context("opening data stores")?;

    if ledger
        .is_empty()
        .into_diagnostic()
        .context("checking empty state")?
    {
        debug!("importing genesis");

        let delta = dolos::ledger::compute_origin_delta(&byron);

        ledger
            .apply(&[delta])
            .into_diagnostic()
            .context("applying origin utxos")?;
    }

    let (_, tip) = wal
        .find_tip()
        .into_diagnostic()
        .context("finding WAL tip")?
        .ok_or(miette::miette!("no WAL tip found"))?;

    match tip {
        wal::ChainPoint::Origin => feedback.global_pb.set_length(0),
        wal::ChainPoint::Specific(slot, _) => feedback.global_pb.set_length(slot),
    }

    let wal_seq = ledger
        .cursor()
        .into_diagnostic()
        .context("finding ledger cursor")?
        .map(|ledger::ChainPoint(s, h)| wal.assert_point(&wal::ChainPoint::Specific(s, h)))
        .transpose()
        .into_diagnostic()
        .context("locating wal sequence")?;

    let remaining = wal
        .crawl_from(wal_seq)
        .into_diagnostic()
        .context("crawling wal")?
        .filter_forward()
        .into_blocks()
        .flatten();

    for chunk in remaining.chunks(100).into_iter() {
        let bodies = chunk.map(|RawBlock { body, .. }| body).collect_vec();

        let blocks: Vec<_> = bodies
            .iter()
            .map(|b| MultiEraBlock::decode(b))
            .try_collect()
            .into_diagnostic()
            .context("decoding blocks")?;

        dolos::state::import_block_batch(&blocks, &mut ledger, &byron, &shelley)
            .into_diagnostic()
            .context("importing blocks to ledger store")?;

        blocks
            .last()
            .inspect(|b| feedback.global_pb.set_position(b.slot()));
    }

    Ok(())
}
