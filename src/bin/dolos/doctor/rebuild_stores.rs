use dolos::adapters::StateAdapter;
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use pallas::ledger::traverse::MultiEraBlock;
use std::sync::Arc;
use tracing::debug;

use dolos::cardano::mutable_slots;
use dolos::prelude::*;

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

    let light = dolos_redb::state::LedgerStore::in_memory_v2_light()
        .map_err(StateError::from)
        .into_diagnostic()
        .context("creating in-memory state store")?;

    let light = StateAdapter::Redb(light);

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
        ChainPoint::Origin => progress.set_length(0),
        ChainPoint::Specific(slot, _) => progress.set_length(slot),
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

        let mut deltas = Vec::new();

        for block in blocks.iter() {
            let ledger_query = dolos_cardano::ChainLogic::ledger_query_for_block(block, &deltas)
                .into_diagnostic()?;

            let required_utxos = light
                .get_utxos(ledger_query.required_inputs)
                .into_diagnostic()
                .context("getting required utxos")?;

            let slice = LedgerSlice {
                resolved_inputs: [required_utxos, ledger_query.extra_inputs]
                    .into_iter()
                    .flatten()
                    .collect(),
            };

            let delta = dolos_cardano::ChainLogic::compute_apply_delta(slice, block)
                .into_diagnostic()
                .context("calculating batch deltas.")?;

            deltas.push(delta);
        }

        chain
            .apply(&deltas)
            .into_diagnostic()
            .context("applying deltas to chain")?;

        light
            .apply(&deltas)
            .into_diagnostic()
            .context("applying deltas to ledger")?;

        blocks.last().inspect(|b| progress.set_position(b.slot()));
    }

    let ledger_path = root.join("ledger");

    let disk = dolos_redb::state::LedgerStore::open_v2_light(ledger_path, None)
        .map_err(StateError::from)
        .into_diagnostic()
        .context("opening ledger db")?;

    let disk = StateAdapter::Redb(disk);

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
