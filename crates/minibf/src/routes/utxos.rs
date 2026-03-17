use axum::http::StatusCode;
use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
use futures::future::join_all;
use itertools::Itertools;
use pallas::ledger::traverse::{Era, MultiEraBlock, MultiEraOutput};
use std::collections::{HashMap, HashSet};

use dolos_cardano::{indexes::AsyncCardanoQueryExt, CardanoError};
use dolos_core::{Domain, StateStore as _, TxHash, TxoRef};

use crate::{
    mapping::{IntoModel, UtxoBlockData, UtxoOutputModelBuilder},
    pagination::{Order, Pagination},
    Facade,
};

pub async fn load_utxo_models<D>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
) -> Result<Vec<AddressUtxoContentInner>, StatusCode>
where
    D: Domain<ChainSpecificError = CardanoError> + Clone + Send + Sync + 'static,
{
    let utxos = domain
        .state()
        .get_utxos(refs.into_iter().collect())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // decoded
    let utxos: HashMap<_, _> = utxos
        .iter()
        .map(|(k, v)| {
            let era = Era::try_from(v.0).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let output =
                MultiEraOutput::decode(era, &v.1).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            Ok::<_, StatusCode>((k, output))
        })
        .try_collect()
        .map_err(|e: StatusCode| e)?;

    let tx_deps: Vec<_> = utxos.keys().map(|txoref| txoref.0).unique().collect();
    let block_deps: HashMap<TxHash, UtxoBlockData> = join_all(tx_deps.iter().map(|tx| {
        let tx = *tx;
        async move {
            match domain.query().block_by_tx_hash(tx.as_slice().to_vec()).await {
                Ok(Some((cbor, txorder))) => {
                    let Ok(block) = MultiEraBlock::decode(&cbor) else {
                        return Some(Err(StatusCode::INTERNAL_SERVER_ERROR));
                    };
                    let block_data = match UtxoBlockData::try_from((block, txorder)) {
                        Ok(data) => data,
                        Err(err) => return Some(Err(err)),
                    };
                    Some(Ok((tx, block_data)))
                }
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

        let txo_ref = builder.txo_ref();
        let key = {
            let mut v = txo_ref.0.as_slice().to_vec();
            v.extend_from_slice(&txo_ref.1.to_be_bytes());
            v
        };
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
