use axum::http::StatusCode;
use pallas::ledger::traverse::MultiEraOutput;
use std::{collections::HashSet, sync::Arc};

use dolos_cardano::indexes::AsyncCardanoQueryExt;
use dolos_core::async_query::{BlockMetaResolver, BlockRefMeta};
use dolos_core::{Domain, EraCbor, StateStore as _, TxHash, TxoIdx, TxoRef};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::{Order, Pagination},
    Facade,
};

/// The outputs that a UTxO listing returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UtxoFilter {
    /// Every output.
    All,
    /// Only outputs without native assets, the rows that Blockfrost lists for
    /// the `lovelace` asset.
    AdaOnly,
}

impl UtxoFilter {
    /// Returns `true` when the listing returns `output`.
    pub fn keeps(self, output: &MultiEraOutput<'_>) -> bool {
        match self {
            UtxoFilter::All => true,
            UtxoFilter::AdaOnly => output
                .value()
                .assets()
                .iter()
                .all(|policy| policy.assets().is_empty()),
        }
    }
}

/// UTxO refs read, decoded and filtered per blocking task. One chunk holds
/// its permit for a few milliseconds.
const UTXO_CHUNK: usize = 1024;

/// Loads one page of the UTxOs in `refs` that `filter` keeps. Only the last
/// page can have fewer than `count` rows.
pub async fn load_utxo_models<D, T>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
    filter: UtxoFilter,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let utxos = select_utxos(domain, refs, filter, UTXO_CHUNK).await?;

    let mut block_meta = BlockMetaResolver::new(domain.query());
    let block_deps = block_meta
        .resolve_batch(utxos.iter().map(|(txo_ref, _)| txo_ref.0))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut rows: Vec<_> = utxos
        .into_iter()
        .map(|(txo_ref, cbor)| {
            let block_data = block_deps.get(&txo_ref.0).cloned();
            (
                page_sort_key(&txo_ref, block_data.as_ref()),
                txo_ref,
                cbor,
                block_data,
            )
        })
        .collect();

    match pagination.order {
        Order::Asc => {
            rows.sort_by_key(|(sort_key, ..)| *sort_key);
        }
        Order::Desc => {
            rows.sort_by_key(|(sort_key, ..)| *sort_key);
            rows.reverse();
        }
    }

    let mut out = Vec::new();
    for (i, row) in rows.into_iter().enumerate() {
        let Some((_, txo_ref, cbor, block_data)) = pagination.as_included_item(i, row) else {
            continue;
        };

        let output = MultiEraOutput::try_from(cbor.as_ref())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let TxoRef(tx_hash, txo_idx) = txo_ref;
        let builder = UtxoOutputModelBuilder::from_output(tx_hash, txo_idx, output);

        let builder = if let Some(x) = block_data {
            builder.with_block_data(x)
        } else {
            builder
        };

        let key: Vec<u8> = builder.txo_ref().into();
        let consumed_by = domain
            .query()
            .tx_by_spent_txo(&key)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let builder = if let Some(consumed_by) = consumed_by {
            builder.with_consumed_by(consumed_by)
        } else {
            builder
        };

        out.push(<UtxoOutputModelBuilder<'_> as IntoModel<T>>::into_model(
            builder,
        )?);
    }

    Ok(out)
}

/// Read and decode the outputs behind `refs`, `chunk` refs per blocking
/// task, and keep those that `filter` keeps. Every output must decode.
async fn select_utxos<D>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    filter: UtxoFilter,
    chunk: usize,
) -> Result<Vec<(TxoRef, Arc<EraCbor>)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let mut refs: Vec<_> = refs.into_iter().collect();
    refs.sort();

    let mut selected = Vec::new();

    for refs in refs.chunks(chunk.max(1)) {
        let refs = refs.to_vec();

        let kept = domain
            .blocking(move |domain| {
                let utxos = domain
                    .state()
                    .get_utxos(refs)
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                let mut kept = Vec::with_capacity(utxos.len());
                for (txo_ref, cbor) in utxos {
                    let output = MultiEraOutput::try_from(cbor.as_ref())
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                    if filter.keeps(&output) {
                        kept.push((txo_ref, cbor));
                    }
                }

                Ok(kept)
            })
            .await?;

        selected.extend(kept);
    }

    Ok(selected)
}

/// The page order for a UTxO model: chain position first, `TxoRef` second.
/// The chain position is the sort key of the UTxO models.
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
    txo_ref: &TxoRef,
    block_data: Option<&BlockRefMeta>,
) -> (Option<(u64, usize, u32)>, TxHash, TxoIdx) {
    let TxoRef(tx_hash, txo_idx) = *txo_ref;

    (
        block_data.map(|data| (data.slot, data.tx_index, txo_idx)),
        tx_hash,
        txo_idx,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestApp;
    use dolos_cardano::indexes::CardanoStateIndexExt as _;
    use pallas::crypto::hash::Hash;

    /// Pins the pruned-row ordering contract: no chain position means the
    /// `TxoRef` decides, deterministically, and the whole unknowable group
    /// sorts before any row with a known position.
    #[test]
    fn page_sort_key_orders_pruned_rows_by_txo_ref() {
        let low = TxoRef(Hash::from([0xaa; 32]), 1);
        let high = TxoRef(Hash::from([0xbb; 32]), 0);
        let low_later = TxoRef(Hash::from([0xaa; 32]), 2);
        let key = |txo_ref: &TxoRef| page_sort_key(txo_ref, None);

        // no block data: the TxoRef alone decides, tx hash before output index
        assert!(key(&low) < key(&high));
        assert!(key(&low) < key(&low_later));
        assert!(key(&low_later) < key(&high));

        // a known chain position sorts after the whole unknowable group,
        // regardless of its TxoRef
        let block = BlockRefMeta {
            slot: 1,
            hash: Hash::from([0x11; 32]),
            height: 1,
            tx_hash: Hash::from([0x00; 32]),
            tx_index: 0,
        };
        let positioned = page_sort_key(&TxoRef(Hash::from([0x00; 32]), 0), Some(&block));

        assert!(key(&high) < positioned);
    }

    async fn selected(app: &TestApp, filter: UtxoFilter, chunk: usize) -> Vec<TxoRef> {
        let facade = app.facade();
        let address = pallas::ledger::addresses::Address::from_bech32(&app.vectors().address)
            .expect("vector address");
        let refs = facade
            .state()
            .utxos_by_address(&address.to_vec())
            .expect("address refs");

        let mut refs: Vec<_> = select_utxos(&facade, refs, filter, chunk)
            .await
            .expect("selected utxos")
            .into_iter()
            .map(|(txo_ref, _)| txo_ref)
            .collect();
        refs.sort();
        refs
    }

    #[tokio::test]
    async fn chunked_utxo_selection_matches_unchunked() {
        let app = TestApp::new();

        for filter in [UtxoFilter::All, UtxoFilter::AdaOnly] {
            let whole = selected(&app, filter, usize::MAX).await;
            let chunked = selected(&app, filter, 2).await;

            assert_eq!(chunked, whole);
        }

        assert!(
            selected(&app, UtxoFilter::All, usize::MAX).await.len() > 2,
            "too few utxos to split into chunks"
        );
    }
}
