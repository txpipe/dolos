use std::collections::HashMap;
use std::sync::Arc;

use dolos_core::config::RootConfig;
use dolos_core::{
    ChainPoint, EraCbor, IndexStore as _, IndexWriter as _, StateStore as _, TxoRef, UtxoSetDelta,
};
use miette::{Context, IntoDiagnostic};

use dolos::adapters::DomainAdapter;
use dolos::prelude::*;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// UTxOs per applied index batch
    #[arg(short, long, default_value_t = 10_000)]
    pub chunk: usize,
}

/// Re-derive the live-UTxO filter indexes from the state store.
///
/// Sync builds these indexes incrementally, so a store that predates a
/// dimension never backfills it. This walks the UTxO set once and re-applies
/// every tag. Multimap inserts are idempotent, so tags that already exist are
/// unaffected.
pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    let progress = feedback.indeterminate_progress_bar();
    progress.set_message("rebuilding live-utxo indexes");

    let domain = crate::common::setup_domain(config)?;

    let cursor = domain
        .state()
        .read_cursor()
        .into_diagnostic()
        .context("reading state cursor")?
        .ok_or_else(|| miette::miette!("state store has no cursor to rebuild from"))?;

    // Applying a delta stamps its cursor as the index store's cursor, and
    // bootstrap trusts that cursor to decide how much WAL to replay into the
    // index store. Rebuilding against a lagging index would falsely mark it
    // caught up and skip the replay that restores its archive tags, so only
    // an index that already sits at the state cursor can rebuild in place.
    let index_cursor = domain
        .indexes()
        .cursor()
        .into_diagnostic()
        .context("reading index cursor")?;

    if index_cursor.as_ref() != Some(&cursor) {
        miette::bail!(
            help = "run `dolos doctor catchup-stores` first so the index store reaches the state cursor",
            "index cursor ({index_cursor:?}) does not match state cursor ({cursor:?}); refusing to rebuild",
        );
    }

    let utxos = domain
        .state()
        .iter_utxos()
        .into_diagnostic()
        .context("iterating the utxo set")?;

    let mut batch: HashMap<TxoRef, Arc<EraCbor>> = HashMap::new();
    let mut total = 0u64;

    for entry in utxos {
        let (txo, cbor) = entry.into_diagnostic().context("reading utxo entry")?;
        batch.insert(txo, Arc::new(cbor));

        if batch.len() >= args.chunk {
            total += batch.len() as u64;
            apply_batch(&domain, &cursor, std::mem::take(&mut batch))?;
            progress.set_message(format!("re-indexed {total} utxos"));
        }
    }

    if !batch.is_empty() {
        total += batch.len() as u64;
        apply_batch(&domain, &cursor, batch)?;
    }

    progress.finish_with_message(format!("re-indexed {total} utxos"));

    Ok(())
}

fn apply_batch(
    domain: &DomainAdapter,
    cursor: &ChainPoint,
    produced_utxo: HashMap<TxoRef, Arc<EraCbor>>,
) -> miette::Result<()> {
    let delta = UtxoSetDelta {
        produced_utxo,
        ..Default::default()
    };

    let delta = dolos_cardano::indexes::index_delta_from_utxo_delta(cursor.clone(), &delta);

    let writer = domain
        .indexes()
        .start_writer()
        .into_diagnostic()
        .context("starting index writer")?;

    writer
        .apply(&delta)
        .into_diagnostic()
        .context("applying index delta")?;

    writer
        .commit()
        .into_diagnostic()
        .context("committing index delta")
}
