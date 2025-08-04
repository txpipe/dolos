use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    pool_delegators_inner::PoolDelegatorsInner, pool_list_extended_inner::PoolListExtendedInner,
};
use dolos_cardano::model::{AccountState, PoolDelegator, PoolState};
use dolos_core::{Domain, Entity, State3Store as _};
use pallas::{crypto::hash::Hash, ledger::addresses::Network};
use serde_json::json;

use crate::{
    error::Error,
    mapping::{bech32_pool, rational_to_f64, IntoModel},
    pagination::{Pagination, PaginationParameters},
    Facade,
};

struct PoolModelBuilder {
    operator: Hash<28>,
    state: dolos_cardano::model::PoolState,
}

impl IntoModel<PoolListExtendedInner> for PoolModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<PoolListExtendedInner, StatusCode> {
        let pool_id = bech32_pool(self.operator)?;

        let out = PoolListExtendedInner {
            pool_id,
            hex: hex::encode(self.operator),
            active_stake: self.state.active_stake.to_string(),
            live_stake: self.state.live_stake.to_string(),
            live_saturation: self.state.live_saturation,
            blocks_minted: self.state.blocks_minted as i32,
            declared_pledge: self.state.declared_pledge.to_string(),
            margin_cost: rational_to_f64::<6>(&self.state.margin_cost),
            fixed_cost: self.state.fixed_cost.to_string(),
            metadata: self.state.metadata.map(|m| {
                let out = json!({
                    "url": m.url,
                    "hash": m.hash,
                });

                Box::new(out)
            }),
        };

        Ok(out)
    }
}

pub async fn all_extended<D: Domain>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListExtendedInner>>, Error> {
    let start = &[0u8; 28].as_slice();
    let end = &[255u8; 28].as_slice();

    let iter = domain
        .state3()
        .iter_entities_typed::<PoolState>(start..end)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let pagination = Pagination::try_from(params)?;
    let page: Vec<_> = iter
        .skip(pagination.skip())
        .take(pagination.count)
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mapped: Vec<_> = page
        .into_iter()
        .map(|(key, state)| {
            let builder = PoolModelBuilder {
                operator: Hash::<28>::from(key.as_slice()),
                state,
            };

            builder.into_model()
        })
        .collect::<Result<_, _>>()?;

    Ok(Json(mapped))
}

struct PoolDelegatorModelBuilder {
    delegator: PoolDelegator,
    account: Option<dolos_cardano::model::AccountState>,
    network: Network,
}

impl IntoModel<PoolDelegatorsInner> for PoolDelegatorModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<PoolDelegatorsInner, StatusCode> {
        let address = crate::mapping::stake_cred_to_address(&self.delegator.0, self.network)
            .to_bech32()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let live_stake = self
            .account
            .map(|a| a.controlled_amount.to_string())
            .unwrap_or_default();

        Ok(PoolDelegatorsInner {
            address,
            live_stake,
        })
    }
}

pub async fn by_id_delegators<D: Domain>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolDelegatorsInner>>, Error> {
    let (_, operator) = bech32::decode(&id).map_err(|_| StatusCode::BAD_REQUEST)?;

    let network = domain.get_network_id()?;

    let iter = domain
        .state3()
        .iter_entity_values(PoolDelegator::NS, operator.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let pagination = Pagination::try_from(params)?;

    let page: Vec<_> = iter
        .skip(pagination.skip())
        .take(pagination.count)
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mapped: Vec<_> = page
        .into_iter()
        .map(|delegator| {
            let account = domain
                .state3()
                .read_entity_typed::<AccountState>(delegator.as_slice())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let delegator = PoolDelegator::decode_value(delegator)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let builder = PoolDelegatorModelBuilder {
                delegator,
                account,
                network,
            };

            builder.into_model()
        })
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(mapped))
}
