use std::sync::Arc;

use dolos::cardano::mutable_slots;
use dolos::wal::{self, WalBlockReader, WalReader as _};
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use pallas::ledger::traverse::MultiEraBlock;
use tracing::debug;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(short, long, default_value_t = 500)]
    pub chunk: usize,
}

pub fn run(config: &crate::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    //crate::common::setup_tracing(&config.logging)?;

    let progress = feedback.slot_progress_bar();
    progress.set_message("rebuilding ledger");

    let wal = crate::common::open_wal_store(config)?;
    let genesis = Arc::new(crate::common::open_genesis_files(&config.genesis)?);

    let light = dolos::state::redb::LedgerStore::in_memory_v2_light()
        .into_diagnostic()
        .context("creating in-memory state store")?;

    let light = dolos::state::LedgerStore::Redb(light);

    if light
        .is_empty()
        .into_diagnostic()
        .context("checking empty state")?
    {
        debug!("importing genesis");

        let delta = dolos::cardano::compute_origin_delta(&genesis);

        light
            .apply(&[delta])
            .into_diagnostic()
            .context("applying origin utxos")?;
    }

    let root = crate::common::ensure_storage_path(config)?;

    let chain = crate::common::open_chain_store(config)?;

    let (_, tip) = wal
        .find_tip()
        .into_diagnostic()
        .context("finding WAL tip")?
        .ok_or(miette::miette!("no WAL tip found"))?;

    match tip {
        wal::ChainPoint::Origin => progress.set_length(0),
        wal::ChainPoint::Specific(slot, _) => progress.set_length(slot),
    }

    // Amount of slots until unmutability is guaranteed.
    let lookahead = mutable_slots(&genesis);
    let remaining = WalBlockReader::try_new(&wal, None, lookahead)
        .into_diagnostic()
        .context("creating wal block reader")?;

    for chunk in remaining.chunks(args.chunk).into_iter() {
        let collected = chunk.collect_vec();
        let blocks: Vec<_> = collected
            .iter()
            .map(|b| MultiEraBlock::decode(&b.body))
            .try_collect()
            .into_diagnostic()
            .context("decoding blocks")?;

        let deltas = dolos::state::calculate_block_batch_deltas(&blocks, &light)
            .into_diagnostic()
            .context("calculating batch deltas.")?;

        chain
            .apply(&deltas)
            .into_diagnostic()
            .context("applying deltas to chain")?;

        dolos::state::apply_delta_batch(
            deltas,
            &light,
            &genesis,
            config.storage.max_ledger_history,
        )
        .into_diagnostic()
        .context("importing blocks to ledger store")?;

        blocks.last().inspect(|b| progress.set_position(b.slot()));
    }

    let ledger_path = root.join("ledger");

    let disk = dolos::state::redb::LedgerStore::open_v2_light(ledger_path, None)
        .into_diagnostic()
        .context("opening ledger db")?;

    let disk = dolos::state::LedgerStore::Redb(disk);

    let pb = feedback.indeterminate_progress_bar();
    pb.set_message("copying memory ledger into disc");

    light
        .copy(&disk)
        .into_diagnostic()
        .context("copying from memory db into disc")?;

    pb.abandon_with_message("ledger copy to disk finished");

    let pb = feedback.indeterminate_progress_bar();
    pb.set_message("creating indexes");

    disk.upgrade()
        .into_diagnostic()
        .context("creating indexes")?;

    pb.abandon_with_message("indexes created");

    Ok(())
}
