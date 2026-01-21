use miette::{Context, IntoDiagnostic};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use itertools::Itertools;

use dolos::prelude::*;
use dolos_cardano::{owned::OwnedMultiEraOutput, roll::txs::_hack_collect_slot_tags_from_block};
use pallas::ledger::traverse::MultiEraBlock;

use crate::feedback::Feedback;
use dolos_core::config::RootConfig;

use super::helpers::define_archive_starting_point;
use super::Args;

#[derive(Default)]
struct UtxoCache {
    entries: HashMap<TxoRef, OwnedMultiEraOutput>,
}

impl UtxoCache {
    fn insert_block_outputs(&mut self, block: &MultiEraBlock) -> miette::Result<()> {
        for tx in block.txs() {
            let tx_hash = tx.hash();

            for (idx, output) in tx.produces() {
                let txo_ref = TxoRef(tx_hash, idx as u32);
                let eracbor: EraCbor = output.into();
                let resolved = OwnedMultiEraOutput::decode(Arc::new(eracbor)).into_diagnostic()?;

                self.entries.insert(txo_ref, resolved);
            }
        }

        Ok(())
    }

    fn remove_block_inputs(&mut self, block: &MultiEraBlock) {
        for tx in block.txs() {
            for input in tx.consumes() {
                let txo_ref: TxoRef = (&input).into();
                self.entries.remove(&txo_ref);
            }
        }
    }
}

pub(crate) async fn import_hardano_into_archive(
    args: &Args,
    config: &RootConfig,
    immutable_path: &Path,
    feedback: &Feedback,
    chunk_size: usize,
) -> Result<(), miette::Error> {
    let domain = crate::common::setup_domain(config).await?;

    let tip = pallas::storage::hardano::immutable::get_tip(immutable_path)
        .map_err(|err| miette::miette!(err.to_string()))
        .context("reading immutable db tip")?
        .ok_or(miette::miette!("immutable db has no tip"))?;

    let cursor = define_archive_starting_point(args, domain.archive())?;

    let iter = pallas::storage::hardano::immutable::read_blocks_from_point(immutable_path, cursor)
        .map_err(|err| miette::miette!(err.to_string()))
        .context("reading immutable db tip")?;

    let progress = feedback.slot_progress_bar();

    progress.set_message("importing immutable db (archive)");
    progress.set_length(tip.slot_or_default());

    let mut utxos = UtxoCache::default();

    for batch in iter.chunks(chunk_size).into_iter() {
        let batch: Vec<_> = batch
            .try_collect()
            .into_diagnostic()
            .context("reading block data")?;

        let writer = domain
            .archive()
            .start_writer()
            .map_err(|err| miette::miette!(format!("{err:?}")))
            .context("starting archive writer")?;

        let mut last_slot = None;

        for raw in batch {
            let raw: Arc<BlockBody> = Arc::new(raw);
            let block = dolos_cardano::owned::OwnedMultiEraBlock::decode(raw.clone())
                .into_diagnostic()
                .context("decoding block")?;
            let point = block.point();
            let view = block.view();

            utxos.insert_block_outputs(view)?;

            let mut tags = SlotTags::default();
            _hack_collect_slot_tags_from_block(view, &utxos.entries, &mut tags)
                .into_diagnostic()
                .context("computing block tags")?;

            writer
                .apply(&point, &raw, &tags)
                .map_err(|err| miette::miette!(format!("{err:?}")))
                .context("writing archive block")?;

            utxos.remove_block_inputs(view);

            last_slot = Some(point.slot());
        }

        writer
            .commit()
            .map_err(|err| miette::miette!(format!("{err:?}")))
            .context("committing archive batch")?;

        if let Some(last_slot) = last_slot {
            progress.set_position(last_slot);
        }
    }

    progress.abandon_with_message("immutable db archive import complete");

    Ok(())
}
