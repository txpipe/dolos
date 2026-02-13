use crate::prelude::*;
use dolos_cardano::FixedNamespace as _;
use dolos_cardano::{PoolParams, PoolState};
use dolos_core::StateStore;
use pallas::codec::minicbor::{self, Decode, Encode};
use pallas::codec::utils::{AnyCbor, AnyUInt, Bytes, Nullable, TagWrap};
use pallas::ledger::primitives::Relay;
use pallas::network::miniprotocols::localstate::queries_v16 as q16;
use pallas::network::miniprotocols::localtxsubmission::SMaybe;
use std::collections::{BTreeMap, BTreeSet};
use std::iter::FromIterator;
use tracing::debug;

#[derive(Debug, Encode, Decode, PartialEq, Clone)]
pub struct LocalPState {
    #[n(0)]
    stake_pool_params: BTreeMap<Bytes, q16::PoolParams>,
    #[n(1)]
    future_stake_pool_params: BTreeMap<Bytes, q16::PoolParams>,
    #[n(2)]
    retiring: BTreeMap<Bytes, u32>,
    #[n(3)]
    deposits: BTreeMap<Bytes, q16::Coin>,
}

pub fn build_stake_pool_params_response<D: Domain>(
    domain: &D,
    pool_ids: &q16::Pools,
) -> Result<AnyCbor, Error> {
    let state = domain.state();
    let pools_iter = state
        .iter_entities_typed::<PoolState>(PoolState::NS, None)
        .map_err(|e| Error::server(format!("failed to iterate pools: {}", e)))?;

    let filter_set: BTreeSet<Vec<u8>> = pool_ids.0.iter().map(|p| p.to_vec()).collect();

    let mut result: BTreeMap<Bytes, q16::PoolParams> = BTreeMap::new();

    for record in pools_iter {
        let (_, pool) = record.map_err(|e| Error::server(format!("failed to read pool: {}", e)))?;

        let pool_id_bytes = pool.operator.to_vec();
        if !filter_set.contains(&pool_id_bytes) {
            continue;
        }

        let pool_id: Bytes = pool_id_bytes.into();

        let live_snapshot_opt = pool.snapshot.live();
        let live_snapshot = match live_snapshot_opt {
            Some(ls) => ls,
            None => continue,
        };

        result.insert(
            pool_id.clone(),
            convert_pool_params(pool_id.as_ref(), &live_snapshot.params),
        );
    }

    debug!(num_pools = result.len(), "returning stake pool params");

    Ok(AnyCbor::from_encode((result,)))
}

pub fn build_stake_pools_response<D: Domain>(domain: &D) -> Result<AnyCbor, Error> {
    let state = domain.state();
    let pools_iter = state
        .iter_entities_typed::<PoolState>(PoolState::NS, None)
        .map_err(|e| Error::server(format!("failed to iterate pools: {}", e)))?;

    let mut pool_ids: BTreeSet<Bytes> = BTreeSet::new();

    for record in pools_iter {
        let (_, pool) = record.map_err(|e| Error::server(format!("failed to read pool: {}", e)))?;

        let live_snapshot_opt = pool.snapshot.live();
        let live_snapshot = match live_snapshot_opt {
            Some(ls) => ls,
            None => continue,
        };

        if live_snapshot.is_retired {
            continue;
        }

        let pool_id: Bytes = pool.operator.to_vec().into();
        pool_ids.insert(pool_id);
    }

    debug!(num_pools = pool_ids.len(), "returning stake pools");

    let pools_response: q16::Pools = TagWrap(pool_ids);
    Ok(AnyCbor::from_encode((pools_response,)))
}

pub fn build_pool_state_response<D: Domain>(
    domain: &D,
    pools_filter: &SMaybe<q16::Pools>,
) -> Result<AnyCbor, Error> {
    let state = domain.state();
    let pools_iter = state
        .iter_entities_typed::<PoolState>(PoolState::NS, None)
        .map_err(|e| Error::server(format!("failed to iterate pools: {}", e)))?;

    let filter_set: Option<BTreeSet<Vec<u8>>> = match pools_filter {
        SMaybe::Some(pools) => {
            let set: BTreeSet<Vec<u8>> = pools.0.iter().map(|p| p.to_vec()).collect();
            Some(set)
        }
        SMaybe::None => None,
    };

    let mut stake_pool_params: BTreeMap<Bytes, q16::PoolParams> = BTreeMap::new();
    let mut future_stake_pool_params: BTreeMap<Bytes, q16::PoolParams> = BTreeMap::new();
    let mut retiring: BTreeMap<Bytes, u32> = BTreeMap::new();
    let mut deposits: BTreeMap<Bytes, q16::Coin> = BTreeMap::new();

    for record in pools_iter {
        let (_, pool) = record.map_err(|e| Error::server(format!("failed to read pool: {}", e)))?;

        let pool_id_bytes = pool.operator.to_vec();
        if let Some(ref filter) = filter_set {
            if !filter.contains(&pool_id_bytes) {
                continue;
            }
        }

        let pool_id: Bytes = pool_id_bytes.into();

        let live_snapshot_opt = pool.snapshot.live();
        let live_snapshot = match live_snapshot_opt {
            Some(ls) => ls,
            None => continue,
        };

        if live_snapshot.is_retired {
            continue;
        }

        stake_pool_params.insert(
            pool_id.clone(),
            convert_pool_params(pool_id.as_ref(), &live_snapshot.params),
        );

        if let Some(next_snapshot) = pool.snapshot.next() {
            future_stake_pool_params.insert(
                pool_id.clone(),
                convert_pool_params(pool_id.as_ref(), &next_snapshot.params),
            );
        }

        if let Some(retiring_epoch) = pool.retiring_epoch {
            retiring.insert(pool_id.clone(), retiring_epoch as u32);
        }

        deposits.insert(pool_id, AnyUInt::U64(pool.deposit));
    }

    debug!(
        num_pools = stake_pool_params.len(),
        num_future = future_stake_pool_params.len(),
        num_retiring = retiring.len(),
        "returning pool state"
    );

    let pstate = LocalPState {
        stake_pool_params,
        future_stake_pool_params,
        retiring,
        deposits,
    };

    let encoded = minicbor::to_vec(pstate)
        .map_err(|e| Error::server(format!("failed to encode pool state: {e}")))?;

    let wrapped = TagWrap::<Bytes, 24>(encoded.into());
    Ok(AnyCbor::from_encode(vec![wrapped]))
}

fn convert_pool_params(operator: &[u8], params: &PoolParams) -> q16::PoolParams {
    let relays: Vec<q16::Relay> = params
        .relays
        .iter()
        .map(|r| match r {
            Relay::SingleHostAddr(port, ipv4, ipv6) => {
                q16::Relay::SingleHostAddr((*port).into(), ipv4.clone().into(), ipv6.clone().into())
            }
            Relay::SingleHostName(port, dns) => {
                q16::Relay::SingleHostName((*port).into(), dns.clone())
            }
            Relay::MultiHostName(dns) => q16::Relay::MultiHostName(dns.clone()),
        })
        .collect();

    let pool_metadata: Nullable<q16::PoolMetadata> = match &params.pool_metadata {
        Some(metadata) => Nullable::Some(q16::PoolMetadata {
            url: metadata.url.clone(),
            hash: metadata.hash.to_vec().into(),
        }),
        None => Nullable::Null,
    };

    q16::PoolParams {
        operator: operator.to_vec().into(),
        vrf_keyhash: params.vrf_keyhash.to_vec().into(),
        pledge: AnyUInt::U64(params.pledge),
        cost: AnyUInt::U64(params.cost),
        margin: q16::UnitInterval {
            numerator: params.margin.numerator,
            denominator: params.margin.denominator,
        },
        reward_account: params.reward_account.to_vec().into(),
        pool_owners: BTreeSet::from_iter(
            params
                .pool_owners
                .iter()
                .map(|h| Bytes::from(h.to_vec()))
                .collect::<Vec<_>>(),
        )
        .into(),
        relays,
        pool_metadata,
    }
}
