use axum::http::StatusCode;
use futures::future::join_all;
use itertools::Itertools;
use pallas::ledger::traverse::MultiEraOutput;
use std::collections::{HashMap, HashSet};

use dolos_cardano::indexes::AsyncCardanoQueryExt;
use dolos_core::{async_query::BlockRefMeta, Domain, StateStore as _, TxHash, TxoIdx, TxoRef};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::{Order, Pagination},
    Facade,
};

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
    let utxos = domain
        .state()
        .get_utxos(refs.into_iter().collect())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // decoded
    let utxos: HashMap<_, _> = utxos
        .iter()
        .map(|(k, v)| MultiEraOutput::try_from(v.as_ref()).map(|x| (k, x)))
        .try_collect()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let tx_deps: Vec<_> = utxos.keys().map(|txoref| txoref.0).unique().collect();
    let block_deps: HashMap<TxHash, BlockRefMeta> = join_all(tx_deps.iter().map(|tx| {
        let tx = *tx;
        async move {
            match domain.query().block_meta_by_tx_hash(tx.to_vec()).await {
                Ok(Some(block_data)) => Some(Ok((tx, block_data))),
                Ok(None) => None,
                Err(_) => Some(Err(StatusCode::INTERNAL_SERVER_ERROR)),
            }
        }
    }))
    .await
    .into_iter()
    .flatten()
    .collect::<Result<_, _>>()?;

    let mut models: Vec<_> = utxos
        .into_iter()
        .map(|(TxoRef(tx_hash, txo_idx), txo)| {
            let builder = UtxoOutputModelBuilder::from_output(*tx_hash, *txo_idx, txo);
            let block_data = block_deps.get(tx_hash).cloned();

            if let Some(x) = block_data {
                builder.with_block_data(x)
            } else {
                builder
            }
        })
        .map(|x| (page_sort_key::<T>(&x), x))
        .collect();

    match pagination.order {
        Order::Asc => {
            models.sort_by_key(|(sort_key, _)| *sort_key);
        }
        Order::Desc => {
            models.sort_by_key(|(sort_key, _)| *sort_key);
            models.reverse();
        }
    }

    let mut out = Vec::new();
    for (i, builder) in models.into_iter().map(|(_, builder)| builder).enumerate() {
        let Some(builder) = pagination.as_included_item(i, builder) else {
            continue;
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

/// The page order for a UTxO model: chain position first, `TxoRef` second.
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
fn page_sort_key<T>(
    builder: &UtxoOutputModelBuilder<'_>,
) -> (Option<(u64, usize, u32)>, TxHash, TxoIdx)
where
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let TxoRef(tx_hash, txo_idx) = builder.txo_ref();

    (
        <UtxoOutputModelBuilder<'_> as IntoModel<T>>::sort_key(builder),
        tx_hash,
        txo_idx,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
    use dolos_core::async_query::BlockRefMeta;
    use pallas::codec::minicbor;
    use pallas::crypto::hash::Hash;
    use pallas::ledger::primitives::conway::{PostAlonzoTransactionOutput, Value};
    use pallas::ledger::traverse::Era;

    fn output_bytes() -> Vec<u8> {
        let output = PostAlonzoTransactionOutput {
            address: vec![0x60; 29].into(),
            value: Value::Coin(1_000_000),
            datum_option: None,
            script_ref: None,
        };

        minicbor::to_vec(&output).unwrap()
    }

    /// Pins the pruned-row ordering contract: no chain position means the
    /// `TxoRef` decides, deterministically, and the whole unknowable group
    /// sorts before any row with a known position.
    #[test]
    fn page_sort_key_orders_pruned_rows_by_txo_ref() {
        let bytes = output_bytes();
        fn output(b: &[u8]) -> MultiEraOutput<'_> {
            MultiEraOutput::decode(Era::Conway, b).unwrap()
        }
        let key = page_sort_key::<AddressUtxoContentInner>;

        let low = UtxoOutputModelBuilder::from_output(Hash::from([0xaa; 32]), 1, output(&bytes));
        let high = UtxoOutputModelBuilder::from_output(Hash::from([0xbb; 32]), 0, output(&bytes));
        let low_later =
            UtxoOutputModelBuilder::from_output(Hash::from([0xaa; 32]), 2, output(&bytes));

        // no block data: the TxoRef alone decides, tx hash before output index
        assert!(key(&low) < key(&high));
        assert!(key(&low) < key(&low_later));
        assert!(key(&low_later) < key(&high));

        // a known chain position sorts after the whole unknowable group,
        // regardless of its TxoRef
        let positioned =
            UtxoOutputModelBuilder::from_output(Hash::from([0x00; 32]), 0, output(&bytes))
                .with_block_data(BlockRefMeta {
                    slot: 1,
                    hash: Hash::from([0x11; 32]),
                    height: 1,
                    tx_hash: Hash::from([0x00; 32]),
                    tx_index: 0,
                });

        assert!(key(&high) < key(&positioned));
    }
}
