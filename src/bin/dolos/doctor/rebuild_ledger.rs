use dolos::{
    ledger,
    wal::{self, RawBlock, ReadUtils, WalReader as _},
};
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use pallas::ledger::traverse::MultiEraBlock;
use tracing::debug;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args;

pub fn run(config: &crate::Config, _args: &Args, feedback: &Feedback) -> miette::Result<()> {
    //crate::common::setup_tracing(&config.logging)?;

    let progress = feedback.slot_progress_bar();
    progress.set_message("rebuilding ledger");

    let (byron, shelley, _) = crate::common::open_genesis_files(&config.genesis)?;

    let wal = crate::common::open_wal(config).context("opening WAL store")?;

    let ledger = dolos::state::redb::LedgerStore::in_memory_v2_light()
        .into_diagnostic()
        .context("creating in-memory state store")?;

    let mut ledger = dolos::state::LedgerStore::Redb(ledger);

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
        wal::ChainPoint::Origin => progress.set_length(0),
        wal::ChainPoint::Specific(slot, _) => progress.set_length(slot),
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

        dolos::state::apply_block_batch(&blocks, &mut ledger, &byron, &shelley)
            .into_diagnostic()
            .context("importing blocks to ledger store")?;

        blocks.last().inspect(|b| progress.set_position(b.slot()));
    }

    let upgraded = {
        let pb = feedback.indeterminate_progress_bar();
        pb.set_message("creating indexes");
        let out = ledger.upgrade().unwrap();
        pb.abandon_with_message("indexes created");

        out
    };

    let disc_ledger = crate::common::open_ledger(&config).context("opening ledger")?;

    {
        let pb = feedback.indeterminate_progress_bar();
        pb.set_message("copying memory ledger into disc");

        upgraded
            .copy(&disc_ledger)
            .into_diagnostic()
            .context("copying from memory db into disc")?;

        pb.abandon_with_message("ledger copy to disk finished");
    }

    Ok(())
}
