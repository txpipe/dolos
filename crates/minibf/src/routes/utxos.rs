use axum::http::StatusCode;
use pallas::ledger::traverse::MultiEraOutput;
use std::collections::HashSet;

use dolos_cardano::indexes::AsyncCardanoQueryExt;
use dolos_core::async_query::{BlockMetaResolver, BlockRefMeta};
use dolos_core::{Domain, StateStore as _, TxHash, TxoIdx, TxoRef};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::{Order, Pagination},
    Facade,
};

/// Loads, sorts and paginates the page of UTxO models for `refs`.
///
/// `from` / `to` on the pagination are ignored: Blockfrost does not accept
/// them on the address and script UTxO endpoints. Use
/// [`load_utxo_models_in_height_range`] for the endpoints that do.
pub async fn load_utxo_models<D, T>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    load_utxo_models_filtered(domain, refs, pagination, |_| true).await
}

/// Like [`load_utxo_models`] but honours `from` / `to` as an inclusive block
/// height range, the way Blockfrost's `/assets/{asset}/utxos` does.
///
/// An output whose creation block was pruned by `sync.max_history` has no
/// known height, so it can never be proven to sit inside the range, and the
/// model it feeds requires the block hash, height and time. Such rows are
/// left out rather than reported with made-up block data.
pub async fn load_utxo_models_in_height_range<D, T>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let range = pagination.clone();

    load_utxo_models_filtered(domain, refs, pagination, move |meta| match meta {
        Some(meta) => !range.should_skip(meta.height, 0),
        None => false,
    })
    .await
}

/// The work here is proportional to the number of refs only for the cheap
/// part: resolving each distinct creating tx to its block position and
/// sorting. Output bytes are fetched and decoded for the requested page alone.
async fn load_utxo_models_filtered<D, T>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
    filter: impl Fn(Option<&BlockRefMeta>) -> bool,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let chain = domain.get_chain_summary()?;

    let mut block_meta = BlockMetaResolver::new(domain.query());
    let block_deps = block_meta
        .resolve_batch(refs.iter().map(|txo_ref| txo_ref.0))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut rows: Vec<_> = refs
        .into_iter()
        .map(|txo_ref| {
            let meta = block_deps.get(&txo_ref.0).cloned();
            (page_sort_key(meta.as_ref(), &txo_ref), txo_ref, meta)
        })
        .filter(|(_, _, meta)| filter(meta.as_ref()))
        .collect();

    rows.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
    if let Order::Desc = pagination.order {
        rows.reverse();
    }

    let page: Vec<_> = rows
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(_, txo_ref, meta)| (txo_ref, meta))
        .collect();

    if page.is_empty() {
        return Ok(vec![]);
    }

    let utxos = domain
        .state()
        .get_utxos(page.iter().map(|(txo_ref, _)| txo_ref.clone()).collect())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut out = Vec::with_capacity(page.len());
    for (txo_ref, meta) in page {
        let cbor = utxos
            .get(&txo_ref)
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let output = MultiEraOutput::try_from(cbor.as_ref())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let builder = UtxoOutputModelBuilder::from_output(txo_ref.0, txo_ref.1, output);
        let builder = match meta {
            Some(meta) => {
                let block_time = chain.slot_time(meta.slot);
                builder.with_block_data(meta).with_block_time(block_time)
            }
            None => builder,
        };

        let key: Vec<u8> = txo_ref.into();
        let consumed_by = domain
            .query()
            .tx_by_spent_txo(&key)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let builder = match consumed_by {
            Some(consumed_by) => builder.with_consumed_by(consumed_by),
            None => builder,
        };

        out.push(<UtxoOutputModelBuilder<'_> as IntoModel<T>>::into_model(
            builder,
        )?);
    }

    Ok(out)
}

/// The page order for a UTxO row: chain position first, `TxoRef` second.
///
/// Chain position is `None` for an output whose creation block was pruned by
/// `sync.max_history` — the block that carries its slot no longer exists, so
/// true chain order is unrecoverable for it. The `TxoRef` tie-breaker keeps
/// those rows in a deterministic order across requests; without it their
/// order came from `HashMap` iteration, and two page requests could slice two
/// different shufflings, duplicating or dropping rows.
///
/// `None` sorts before every known position, which approximates chain order:
/// a pruned creation block is older than every retained one.
fn page_sort_key(
    meta: Option<&BlockRefMeta>,
    txo_ref: &TxoRef,
) -> (Option<(u64, usize, u32)>, TxHash, TxoIdx) {
    let TxoRef(tx_hash, txo_idx) = txo_ref;

    (
        meta.map(|meta| (meta.slot, meta.tx_index, *txo_idx)),
        *tx_hash,
        *txo_idx,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas::crypto::hash::Hash;

    /// Pins the pruned-row ordering contract: no chain position means the
    /// `TxoRef` decides, deterministically, and the whole unknowable group
    /// sorts before any row with a known position.
    #[test]
    fn page_sort_key_orders_pruned_rows_by_txo_ref() {
        let low = TxoRef(Hash::from([0xaa; 32]), 1);
        let high = TxoRef(Hash::from([0xbb; 32]), 0);
        let low_later = TxoRef(Hash::from([0xaa; 32]), 2);

        // no block data: the TxoRef alone decides, tx hash before output index
        assert!(page_sort_key(None, &low) < page_sort_key(None, &high));
        assert!(page_sort_key(None, &low) < page_sort_key(None, &low_later));
        assert!(page_sort_key(None, &low_later) < page_sort_key(None, &high));

        // a known chain position sorts after the whole unknowable group,
        // regardless of its TxoRef
        let positioned = TxoRef(Hash::from([0x00; 32]), 0);
        let meta = BlockRefMeta {
            slot: 1,
            hash: Hash::from([0x11; 32]),
            height: 1,
            tx_hash: Hash::from([0x00; 32]),
            tx_index: 0,
        };

        assert!(page_sort_key(None, &high) < page_sort_key(Some(&meta), &positioned));
    }
}
