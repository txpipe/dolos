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

    let (wal, _, mut ledger) =
        crate::common::open_data_stores(config).context("opening data stores")?;

    if ledger.is_empty() {
        debug!("importing genesis");

        let delta = dolos::ledger::compute_origin_delta(&byron);

        ledger
            .apply(&[delta])
            .into_diagnostic()
            .context("applying origin utxos")?;
    }

    let tip = wal
        .find_tip()
        .into_diagnostic()
        .context("finding WAL tip")?
        .ok_or(miette::miette!("no WAL tip found"))?;

    feedback.global_pb.set_length(tip.0);

    let intersection: Vec<_> = ledger
        .cursor()
        .into_diagnostic()
        .context("finding ledger cursor")?
        .map(|p| (p.0, p.1))
        .into_iter()
        .collect();

    let wal_seq = wal
        .find_wal_seq(&intersection)
        .into_diagnostic()
        .context("finding WAL sequence")?;

    let remaining = wal.crawl_after(wal_seq);

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

        dolos::ledger::import_block_batch(&blocks, &mut ledger, &byron, &shelley)
            .into_diagnostic()
            .context("importing blocks to ledger store")?;

        blocks
            .last()
            .inspect(|b| feedback.global_pb.set_position(b.slot()));
    }

    Ok(())
}
