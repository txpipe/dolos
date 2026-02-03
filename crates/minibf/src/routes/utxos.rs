use axum::http::StatusCode;
use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
use itertools::Itertools;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use std::collections::{HashMap, HashSet};

use dolos_cardano::indexes::AsyncCardanoQueryExt;
use dolos_core::{Domain, StateStore as _, TxoRef};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::{Order, Pagination},
    Facade,
};

pub async fn load_utxo_models<D>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
) -> Result<Vec<AddressUtxoContentInner>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
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

    let block_deps = domain.get_block_with_tx_batch(tx_deps).await?;

    // decoded
    let blocks_deps: HashMap<_, _> = block_deps
        .iter()
        .map(|(k, (cbor, order))| MultiEraBlock::decode(cbor).map(|x| (k, (x, order))))
        .try_collect()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut models: Vec<_> = utxos
        .into_iter()
        .map(|(TxoRef(tx_hash, txo_idx), txo)| {
            let builder = UtxoOutputModelBuilder::from_output(*tx_hash, *txo_idx, txo);
            let block_data = blocks_deps.get(&tx_hash).cloned();

            if let Some((block, tx_order)) = block_data {
                builder.with_block_data(block, *tx_order)
            } else {
                builder
            }
        })
        .map(|x| {
            (
                <UtxoOutputModelBuilder<'_> as IntoModel<AddressUtxoContentInner>>::sort_key(&x),
                x,
            )
        })
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

        out.push(builder.into_model()?);
    }

    Ok(out)
}
