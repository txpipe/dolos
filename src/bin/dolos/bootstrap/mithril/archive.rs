use miette::{Context, IntoDiagnostic};
use std::path::Path;
use std::sync::Arc;

use itertools::Itertools;

use dolos::prelude::*;
use dolos_cardano::roll::txs::collect_slot_tags_from_block;

use crate::feedback::Feedback;
use dolos_core::config::RootConfig;

use super::helpers::define_archive_starting_point;
use super::utxo_cache::UtxoCache;
use super::Args;

pub(crate) async fn import_hardano(
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

    let utxos = UtxoCache::in_memory().context("initializing in-memory utxo cache")?;

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

            utxos
                .insert_block_outputs(view)
                .context("caching block outputs")?;

            let resolved_inputs = utxos
                .resolve_block_inputs(view)
                .context("resolving block inputs")?;

            let mut tags = SlotTags::default();
            collect_slot_tags_from_block(view, &resolved_inputs, &mut tags)
                .into_diagnostic()
                .context("computing block tags")?;

            writer
                .apply(&point, &raw, &tags)
                .map_err(|err| miette::miette!(format!("{err:?}")))
                .context("writing archive block")?;

            utxos
                .remove_block_inputs(view)
                .context("evicting block inputs")?;

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
