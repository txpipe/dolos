use miette::{Context, IntoDiagnostic};
use std::path::Path;
use std::sync::Arc;

use itertools::Itertools;

use dolos::prelude::*;

use crate::feedback::Feedback;
use dolos_core::config::RootConfig;

use super::helpers::define_starting_point;
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

    let cursor = define_starting_point(args, domain.state())?;

    let iter = pallas::storage::hardano::immutable::read_blocks_from_point(immutable_path, cursor)
        .map_err(|err| miette::miette!(err.to_string()))
        .context("reading immutable db tip")?;

    let progress = feedback.slot_progress_bar();

    progress.set_message("importing immutable db (state)");
    progress.set_length(tip.slot_or_default());

    for batch in iter.chunks(chunk_size).into_iter() {
        let batch: Vec<_> = batch
            .try_collect()
            .into_diagnostic()
            .context("reading block data")?;

        // we need to wrap them on a ref counter since bytes are going to be shared
        // around throughout the pipeline
        let batch: Vec<_> = batch.into_iter().map(Arc::new).collect();

        let last = dolos_core::facade::import_blocks_state_only(&domain, batch, false)
            .await
            .map_err(|e| miette::miette!(e.to_string()))?;

        progress.set_position(last);
    }

    import_result?;

    domain
        .state()
        .rebuild_utxo_indexes()
        .map_err(|err| miette::miette!(format!("{err:?}")))
        .context("rebuilding state indexes")?;

    progress.abandon_with_message("immutable db state import complete");

    Ok(())
}
