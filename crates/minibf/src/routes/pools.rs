use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    pool::Pool, pool_calidus_key::PoolCalidusKey, pool_delegators_inner::PoolDelegatorsInner,
    pool_history_inner::PoolHistoryInner, pool_list_extended_inner::PoolListExtendedInner,
    PoolListExtendedInnerMetadata,
};
use dolos_cardano::{
    cip151,
    indexes::{AsyncCardanoQueryExt, SlotOrder},
    model::{AccountState, PoolState},
    pallas_extras, PoolDelegation, PoolHash, StakeLog,
};
use dolos_core::{BlockSlot, Domain, EntityKey};
use futures::{future::join_all, StreamExt};
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    ledger::{addresses::Network, primitives::StakeCredential, traverse::MultiEraBlock},
};
use rayon::prelude::*;

use crate::{
    error::Error,
    mapping::{
        bech32_calidus, bech32_pool, pool_offchain_metadata, rational_to_f64,
        stake_cred_to_address, vkey_to_stake_address, IntoModel,
    },
    pagination::{Pagination, PaginationParameters},
    Facade,
};

const ACCOUNT_SCAN_CHUNK_SIZE: usize = 4096;

#[derive(Default, Clone, Copy)]
struct PoolMetrics {
    live_stake: u64,
    active_stake: u64,
    live_delegators: u64,
    total_live_stake: u64,
    total_active_stake: u64,
    live_pledge: u64,
}

impl PoolMetrics {
    fn merge(mut self, other: Self) -> Self {
        self.live_stake += other.live_stake;
        self.active_stake += other.active_stake;
        self.live_delegators += other.live_delegators;
        self.total_live_stake += other.total_live_stake;
        self.total_active_stake += other.total_active_stake;
        self.live_pledge += other.live_pledge;
        self
    }
}

fn safe_ratio(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 / total as f64
    }
}

fn reduce_account_chunk(accounts: &[AccountState], target_pool: PoolHash) -> PoolMetrics {
    accounts
        .par_iter()
        .fold(PoolMetrics::default, |mut metrics, account| {
            let live_stake = account.live_stake();
            metrics.total_live_stake += live_stake;

            if let Some(hash) = account
                .delegated_pool_live()
                .or(account.retired_pool.as_ref())
            {
                if hash == &target_pool {
                    metrics.live_stake += live_stake;
                    metrics.live_delegators += 1;
                }
            }

            let active_stake = account.active_stake();
            metrics.total_active_stake += active_stake;

            if let Some(PoolDelegation::Pool(hash)) = account.pool.set() {
                if hash == &target_pool {
                    metrics.active_stake += active_stake;
                }
            }

            metrics
        })
        .reduce(PoolMetrics::default, |a, b| a.merge(b))
}

fn compute_live_pledge<D: Domain>(
    domain: &Facade<D>,
    target_pool: PoolHash,
    owners: &[pallas::crypto::hash::Hash<28>],
) -> Result<u64, StatusCode>
where
    Option<AccountState>: From<D::Entity>,
{
    owners.iter().try_fold(0u64, |acc, owner| {
        let credential = StakeCredential::AddrKeyhash(*owner);
        let key = minicbor::to_vec(&credential).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let Some(account) = domain.read_cardano_entity::<AccountState>(key)? else {
            return Ok(acc);
        };

        if account
            .delegated_pool_live()
            .or(account.retired_pool.as_ref())
            == Some(&target_pool)
        {
            Ok(acc + account.live_stake())
        } else {
            Ok(acc)
        }
    })
}

fn compute_pool_metrics_sync<D: Domain>(
    domain: Facade<D>,
    target_pool: PoolHash,
    owners: Vec<pallas::crypto::hash::Hash<28>>,
) -> Result<PoolMetrics, StatusCode>
where
    Option<AccountState>: From<D::Entity>,
{
    let mut accounts = domain.iter_cardano_entities::<AccountState>(None)?;
    let mut chunk = Vec::with_capacity(ACCOUNT_SCAN_CHUNK_SIZE);
    let mut metrics = PoolMetrics::default();

    loop {
        chunk.clear();

        for _ in 0..ACCOUNT_SCAN_CHUNK_SIZE {
            let Some(item) = accounts.next() else {
                break;
            };

            let (_, account) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            chunk.push(account);
        }

        if chunk.is_empty() {
            break;
        }

        metrics = metrics.merge(reduce_account_chunk(&chunk, target_pool));
    }

    metrics.live_pledge = compute_live_pledge(&domain, target_pool, &owners)?;

    Ok(metrics)
}

fn decode_pool_id(pool_id: &str) -> Result<Vec<u8>, Error> {
    if pool_id.starts_with("pool1") {
        let (_, operator) = bech32::decode(pool_id).map_err(|_| Error::InvalidPoolId)?;
        return Ok(operator);
    } else if pool_id.len() == 56 {
        return hex::decode(pool_id).map_err(|_| Error::InvalidPoolId);
    }

    Err(Error::InvalidPoolId)
}

fn scan_pool_cert_hashes_in_block(
    block: &MultiEraBlock,
    target_pool: &PoolHash,
) -> (Vec<String>, Vec<String>) {
    let mut registrations = Vec::new();
    let mut retirements = Vec::new();

    for tx in block.txs() {
        let mut has_registration = false;
        let mut has_retirement = false;

        for cert in tx.certs() {
            if pallas_extras::cert_as_pool_registration(&cert)
                .is_some_and(|cert| &cert.operator == target_pool)
            {
                has_registration = true;
            }

            if pallas_extras::cert_as_pool_retirement(&cert)
                .is_some_and(|cert| &cert.operator == target_pool)
            {
                has_retirement = true;
            }
        }

        if has_registration || has_retirement {
            let tx_hash = tx.hash().to_string();

            if has_registration {
                registrations.push(tx_hash.clone());
            }

            if has_retirement {
                retirements.push(tx_hash);
            }
        }
    }

    (registrations, retirements)
}

async fn load_pool_cert_hashes<D>(
    domain: &Facade<D>,
    pool: PoolHash,
) -> Result<(Vec<String>, Vec<String>), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let tip = domain.get_tip_slot()?;
    let stream =
        domain
            .query()
            .blocks_by_pool_certs_stream(pool.as_slice(), 0, tip, SlotOrder::Asc);

    let mut stream = Box::pin(stream);
    let mut registrations = Vec::new();
    let mut retirements = Vec::new();

    while let Some(item) = stream.next().await {
        let (_, block) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let Some(block) = block else {
            continue;
        };

        let block = MultiEraBlock::decode(block.as_slice())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let (mut block_registrations, mut block_retirements) =
            scan_pool_cert_hashes_in_block(&block, &pool);

        registrations.append(&mut block_registrations);
        retirements.append(&mut block_retirements);
    }

    Ok((registrations, retirements))
}

async fn load_pool_calidus_key<D>(
    domain: &Facade<D>,
    pool: PoolHash,
) -> Result<Option<PoolCalidusKey>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let tip = domain.get_tip_slot()?;
    let chain = domain.get_chain_summary()?;
    let stream = domain.query().blocks_by_metadata_stream(
        cip151::CIP151_METADATA_LABEL,
        0,
        tip,
        SlotOrder::Desc,
    );

    let mut stream = Box::pin(stream);

    while let Some(item) = stream.next().await {
        let (_, block) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let Some(block) = block else {
            continue;
        };

        let block = MultiEraBlock::decode(block.as_slice())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let block_height = block.number();
        let block_slot = block.slot();
        let block_time = chain.slot_time(block_slot);
        let epoch = chain.slot_epoch(block_slot).0;

        for tx in block.txs().into_iter().rev() {
            let Some(metadata) = cip151::cip151_metadata_for_tx(&tx) else {
                continue;
            };

            let Ok(registration) = cip151::parse_cip151_pool_registration(&metadata) else {
                continue;
            };

            if registration.pool_id != pool {
                continue;
            }

            if cip151::calidus_key_is_revoked(&registration.calidus_pub_key) {
                return Ok(None);
            }

            return Ok(Some(PoolCalidusKey {
                id: bech32_calidus(cip151::calidus_key_id_bytes(&registration.calidus_pub_key))?,
                pub_key: hex::encode(registration.calidus_pub_key),
                nonce: registration
                    .nonce
                    .try_into()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
                tx_hash: tx.hash().to_string(),
                block_height: block_height
                    .try_into()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
                block_time: block_time
                    .try_into()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
                epoch: epoch
                    .try_into()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            }));
        }
    }

    Ok(None)
}

pub async fn by_id<D>(
    Path(id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Pool>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
    Option<AccountState>: From<D::Entity>,
{
    let operator = decode_pool_id(&id)?;
    let pool = domain
        .read_cardano_entity::<PoolState>(operator.as_slice())?
        .ok_or(StatusCode::NOT_FOUND)?;

    let snapshot = pool
        .snapshot
        .live()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let params = &snapshot.params;

    let network = domain.get_network_id()?;
    let circulating_supply = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .initial_pots
        .circulating();
    let optimal = domain
        .get_current_effective_pparams()?
        .ensure_k()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let owners = params.pool_owners.clone();
    let operator = pool.operator;
    let domain_clone = domain.clone();
    let metrics = tokio::task::spawn_blocking(move || {
        compute_pool_metrics_sync(domain_clone, operator, owners)
    })
    .await
    .map_err(|err| {
        tracing::error!(error = ?err, "pool metrics task failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })??;

    let (registration, retirement) = load_pool_cert_hashes(&domain, pool.operator).await?;
    let calidus_key = load_pool_calidus_key(&domain, pool.operator).await?;

    let reward_account = pallas_extras::parse_reward_account(&params.reward_account)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
        .and_then(|cred| {
            stake_cred_to_address(&cred, network)
                .to_bech32()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        })?;

    let owners = params
        .pool_owners
        .iter()
        .map(|owner| {
            vkey_to_stake_address(*owner, network)
                .to_bech32()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let response = Pool {
        pool_id: bech32_pool(pool.operator)?,
        hex: hex::encode(pool.operator),
        vrf_key: params.vrf_keyhash.to_string(),
        blocks_minted: pool.blocks_minted_total as i32,
        blocks_epoch: snapshot.blocks_minted as i32,
        live_stake: metrics.live_stake.to_string(),
        live_size: safe_ratio(metrics.live_stake, metrics.total_live_stake),
        live_saturation: if circulating_supply == 0 {
            0.0
        } else {
            metrics.live_stake as f64 * optimal as f64 / circulating_supply as f64
        },
        live_delegators: metrics.live_delegators as f64,
        active_stake: metrics.active_stake.to_string(),
        active_size: safe_ratio(metrics.active_stake, metrics.total_active_stake),
        declared_pledge: params.pledge.to_string(),
        live_pledge: metrics.live_pledge.to_string(),
        margin_cost: rational_to_f64::<6>(&params.margin),
        fixed_cost: params.cost.to_string(),
        reward_account,
        owners,
        registration,
        retirement,
        calidus_key: calidus_key.map(Box::new),
    };

    Ok(Json(response))
}

pub async fn all_extended<D: Domain>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListExtendedInner>>, Error>
where
    Option<PoolState>: From<D::Entity>,
    Option<AccountState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let mut live_stake_map = HashMap::new();
    let mut active_stake_map = HashMap::new();
    for x in domain
        .iter_cardano_entities::<AccountState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        let (_, state) = x.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if let Some(PoolDelegation::Pool(hash)) = state.pool.live() {
            let stake = state.stake.live().map(|x| x.total()).unwrap_or(0);
            live_stake_map
                .entry(*hash)
                .and_modify(|entry| *entry += stake)
                .or_insert(stake);
        };
        if let Some(PoolDelegation::Pool(hash)) = state.pool.set() {
            let stake = state.stake.set().map(|x| x.total()).unwrap_or(0);
            active_stake_map
                .entry(*hash)
                .and_modify(|entry| *entry += stake)
                .or_insert(stake);
        };
    }
    let circulating_supply = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .initial_pots
        .circulating();
    let optimal = domain
        .get_current_effective_pparams()?
        .ensure_k()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let pools = domain
        .iter_cardano_entities::<PoolState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .flat_map(|x| {
            let Ok((key, state)) = x else {
                return Some(Err(StatusCode::INTERNAL_SERVER_ERROR));
            };
            if state.snapshot.live().map(|x| x.is_retired).unwrap_or(false) {
                return None;
            }
            Some(Ok((state.register_slot, (key, state))))
        })
        .collect::<Result<Vec<(BlockSlot, (EntityKey, PoolState))>, StatusCode>>()?
        .into_iter()
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .map(|(_, x)| x)
        .skip(pagination.skip())
        .take(pagination.count)
        .collect_vec();

    let metadata_futures: Vec<_> = pools
        .iter()
        .map(|(_, pool)| {
            let url = pool
                .snapshot
                .live()
                .and_then(|x| x.params.pool_metadata.as_ref())
                .map(|x| x.url.clone());
            async move {
                match url {
                    Some(u) => pool_offchain_metadata(&u).await,
                    None => None,
                }
            }
        })
        .collect();

    let metadata_results = join_all(metadata_futures).await;

    let mut out = vec![];

    for ((_, pool), fetched_metadata) in pools.into_iter().zip(metadata_results) {
        let poolhex = hex::encode(pool.operator);
        let pool_id = bech32_pool(pool.operator)?;
        let params = pool.snapshot.live().map(|x| x.params.clone());
        let metadata = match params.as_ref() {
            Some(x) => match x.pool_metadata.as_ref() {
                Some(onchain) => {
                    let out = match fetched_metadata {
                        Some(offchain) => PoolListExtendedInnerMetadata {
                            url: Some(onchain.url.clone()),
                            hash: Some(hex::encode(&*onchain.hash)),
                            error: None,
                            ticker: Some(offchain.ticker),
                            name: Some(offchain.name),
                            description: Some(offchain.description),
                            homepage: Some(offchain.homepage),
                        },
                        None => PoolListExtendedInnerMetadata {
                            url: Some(onchain.url.clone()),
                            hash: Some(hex::encode(&*onchain.hash)),
                            ..Default::default()
                        },
                    };

                    Some(Box::new(out))
                }
                None => None,
            },
            None => None,
        };

        let live = live_stake_map.get(&pool.operator).copied();
        let active = active_stake_map.get(&pool.operator).copied();

        out.push(PoolListExtendedInner {
            pool_id,
            hex: poolhex,
            live_stake: live.map(|x| x.to_string()).unwrap_or("0".to_string()),
            active_stake: active.map(|x| x.to_string()).unwrap_or("0".to_string()),
            live_saturation: live
                .map(|x| x as f64 * optimal as f64 / circulating_supply as f64)
                .unwrap_or_default(),
            blocks_minted: pool.blocks_minted_total as i32,
            declared_pledge: params
                .as_ref()
                .map(|x| x.pledge.to_string())
                .unwrap_or_default(),
            margin_cost: params
                .as_ref()
                .map(|x| rational_to_f64::<6>(&x.margin))
                .unwrap_or_default(),
            fixed_cost: params
                .as_ref()
                .map(|x| x.cost.to_string())
                .unwrap_or_default(),
            metadata,
        });
    }

    Ok(Json(out))
}

struct PoolDelegatorModelBuilder {
    delegator: StakeCredential,
    account: Option<dolos_cardano::model::AccountState>,
    network: Network,
}

impl IntoModel<PoolDelegatorsInner> for PoolDelegatorModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<PoolDelegatorsInner, StatusCode> {
        let address = crate::mapping::stake_cred_to_address(&self.delegator, self.network)
            .to_bech32()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let live_stake = self.account.map(|x| x.live_stake()).unwrap_or_default();

        Ok(PoolDelegatorsInner {
            address,
            live_stake: live_stake.to_string(),
        })
    }
}

pub async fn by_id_delegators<D: Domain>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolDelegatorsInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<PoolState>: From<D::Entity>,
{
    let operator = decode_pool_id(&id)?;
    if !domain.cardano_entity_exists::<PoolState>(operator.as_slice())? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let network = domain.get_network_id()?;

    let iter = domain
        .iter_cardano_entities::<AccountState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let filtered = iter.filter_ok(|(_, account)| {
        account
            .delegated_pool_live()
            .or(account.retired_pool.as_ref())
            .is_some_and(|f| f.as_slice() == operator.as_slice())
    });

    let pagination = Pagination::try_from(params)?;

    let page: Vec<_> = filtered
        .skip(pagination.skip())
        .take(pagination.count)
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mapped: Vec<_> = page
        .into_iter()
        .map(|(delegator, account)| {
            let delegator: StakeCredential = minicbor::decode(delegator.as_ref())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let builder = PoolDelegatorModelBuilder {
                delegator,
                account: Some(account),
                network,
            };

            builder.into_model()
        })
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(mapped))
}

// HACK: blockfrost dbsync version computes fees at the SQL query level using
// the formula: `FLOOR(fee + (rewards - fee) * margin)`.
//
// This is not strictly correct, as the operator share has much more involved
// formula. This method is a workaround to make the data compatible with the
// blockfrost dbsync version.
fn bf_compatible_fees(log: &StakeLog) -> u64 {
    let margin = log
        .margin_cost
        .as_ref()
        .map(
            |pallas::ledger::primitives::RationalNumber {
                 numerator,
                 denominator,
             }| num_rational::Rational64::new(*numerator as i64, *denominator as i64),
        )
        .unwrap_or(num_rational::Rational64::from_integer(0));

    let rewards = num_rational::Rational64::from_integer(log.total_rewards as i64);
    let fixed_cost = num_rational::Rational64::from_integer(log.fixed_cost as i64);

    let variable_fees = (rewards - fixed_cost) * margin;
    let fixed_fees = fixed_cost;

    let fees = variable_fees + fixed_fees;
    let fees = fees.to_integer() as u64;

    if fees > log.total_rewards {
        log.total_rewards
    } else {
        fees
    }
}

pub async fn by_id_history<D: Domain>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolHistoryInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
{
    let operator = decode_pool_id(&id)?;
    let pagination = Pagination::try_from(params)?;
    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (epoch, _) = summary.slot_epoch(tip);

    let mut entries = domain
        .iter_cardano_logs_per_epoch::<StakeLog>(operator.into(), 0..epoch)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Apply order before pagination
    if matches!(pagination.order, crate::pagination::Order::Desc) {
        entries.reverse()
    };

    let mapped: Vec<_> = entries
        .into_iter()
        .filter(|(_, log)| log.total_stake > 0)
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(epoch, log)| {
            Ok(PoolHistoryInner {
                epoch: epoch as i32,
                blocks: log.blocks_minted as i32,
                active_stake: log.total_stake.to_string(),
                active_size: log.relative_size,
                delegators_count: log.delegators_count as i32,
                rewards: log.total_rewards.to_string(),
                fees: bf_compatible_fees(&log).to_string(),
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(mapped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::{
        pool::Pool, pool_delegators_inner::PoolDelegatorsInner,
        pool_list_extended_inner::PoolListExtendedInner,
    };
    use dolos_cardano::cip151;
    use dolos_cardano::model::{DRepDelegation, EpochValue, Stake};
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::{
        codec::utils::Bytes,
        crypto::hash::Hash,
        ledger::primitives::{alonzo, Int, StakeCredential},
    };
    use serde_json::Value;

    fn md_int(value: i64) -> alonzo::Metadatum {
        alonzo::Metadatum::Int(Int::from(value))
    }

    fn md_map(items: Vec<(alonzo::Metadatum, alonzo::Metadatum)>) -> alonzo::Metadatum {
        alonzo::Metadatum::Map(items.into())
    }

    fn invalid_pool_id() -> &'static str {
        "not-a-pool"
    }

    fn missing_pool_id() -> &'static str {
        "pool1qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qursw2w89e2"
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    async fn assert_error_message(app: &TestApp, path: &str, expected: &str) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let payload: Value = serde_json::from_slice(&bytes).expect("failed to parse error body");
        assert_eq!(payload["message"], expected);
    }

    #[tokio::test]
    async fn pool_by_id_happy_path() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pool: Pool = serde_json::from_slice(&bytes).expect("failed to parse pool response");
        assert_eq!(pool.pool_id, pool_id);
        assert!(!pool.registration.is_empty());
        assert!(pool.retirement.is_empty());
        assert!(pool.calidus_key.is_none());
    }

    fn cip151_pool_metadata(
        pool_id: &str,
        nonce: i128,
        calidus_key: [u8; 32],
    ) -> alonzo::Metadatum {
        let operator = decode_pool_id(pool_id).expect("valid pool id");

        md_map(vec![
            (md_int(0), md_int(2)),
            (
                md_int(1),
                md_map(vec![
                    (
                        md_int(1),
                        alonzo::Metadatum::Array(vec![
                            md_int(1),
                            alonzo::Metadatum::Bytes(Bytes::from(operator)),
                        ]),
                    ),
                    (md_int(2), alonzo::Metadatum::Array(vec![])),
                    (md_int(3), alonzo::Metadatum::Array(vec![md_int(2)])),
                    (
                        md_int(4),
                        alonzo::Metadatum::Int(Int::try_from(nonce).expect("nonce fits")),
                    ),
                    (
                        md_int(7),
                        alonzo::Metadatum::Bytes(Bytes::from(calidus_key.to_vec())),
                    ),
                ]),
            ),
        ])
    }

    #[tokio::test]
    async fn pool_by_id_returns_calidus_key() {
        let default_cfg = SyntheticBlockConfig {
            pool_id: bech32_pool([9u8; 28]).expect("valid pool id"),
            ..Default::default()
        };
        let pool_id = default_cfg.pool_id.clone();
        let calidus_pub_key = [0x57; 32];
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            metadata_entries: vec![(
                cip151::CIP151_METADATA_LABEL,
                cip151_pool_metadata(&pool_id, 12345, calidus_pub_key),
            )],
            ..default_cfg
        });

        let path = format!("/pools/{pool_id}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pool: Pool = serde_json::from_slice(&bytes).expect("failed to parse pool response");
        let calidus = pool.calidus_key.expect("missing calidus key");
        assert_eq!(calidus.pub_key, hex::encode(calidus_pub_key));
        assert_eq!(calidus.nonce, 12345);
        assert!(calidus.id.starts_with("calidus1"));
    }

    #[tokio::test]
    async fn pool_by_id_treats_zero_calidus_key_as_revocation() {
        let default_cfg = SyntheticBlockConfig {
            pool_id: bech32_pool([9u8; 28]).expect("valid pool id"),
            ..Default::default()
        };
        let pool_id = default_cfg.pool_id.clone();
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            metadata_entries: vec![(
                cip151::CIP151_METADATA_LABEL,
                cip151_pool_metadata(&pool_id, 12345, [0u8; 32]),
            )],
            ..default_cfg
        });

        let path = format!("/pools/{pool_id}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pool: Pool = serde_json::from_slice(&bytes).expect("failed to parse pool response");
        assert!(pool.calidus_key.is_none());
    }

    #[tokio::test]
    async fn pool_by_id_bad_request() {
        let app = TestApp::new();
        let path = format!("/pools/{}", invalid_pool_id());
        assert_error_message(&app, &path, "Invalid or malformed pool id format.").await;
    }

    #[tokio::test]
    async fn pool_by_id_not_found() {
        let app = TestApp::new();
        let path = format!("/pools/{}", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn pool_by_id_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[test]
    fn pool_metrics_include_retired_pool_stake() {
        let target_pool = Hash::from([1u8; 28]);
        let account = AccountState {
            registered_at: None,
            stake: EpochValue::with_live(
                3,
                Stake {
                    utxo_sum: 42,
                    ..Default::default()
                },
            ),
            pool: EpochValue::with_live(3, PoolDelegation::NotDelegated),
            drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
            vote_delegated_at: None,
            deregistered_at: None,
            credential: StakeCredential::AddrKeyhash(Hash::from([2u8; 28])),
            retired_pool: Some(target_pool),
        };

        let metrics = reduce_account_chunk(&[account], target_pool);

        assert_eq!(metrics.live_stake, 42);
        assert_eq!(metrics.live_pledge, 0);
        assert_eq!(metrics.live_delegators, 1);
        assert_eq!(metrics.total_live_stake, 42);
        assert_eq!(metrics.active_stake, 0);
    }

    #[tokio::test]
    async fn pools_extended_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools/extended?page=999999").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool list extended");
    }

    #[tokio::test]
    async fn pools_extended_paginated() {
        let cfg = SyntheticBlockConfig {
            slot: 500_000,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg(cfg);
        let (status_1, bytes_1) = app.get_bytes("/pools/extended?page=1&count=1").await;
        let (status_2, bytes_2) = app.get_bytes("/pools/extended?page=2&count=1").await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse pool list extended page 1");
        let page_2: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse pool list extended page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 0);
    }
    #[tokio::test]
    async fn pools_extended_bad_request() {
        let app = TestApp::new();
        let path = "/pools/extended?count=invalid";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn pools_extended_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/pools/extended", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn pools_delegators_happy_path() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/delegators?page=999999");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<PoolDelegatorsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool delegators");
    }

    #[tokio::test]
    async fn pools_delegators_paginated() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path_page_1 = format!("/pools/{pool_id}/delegators?page=1&count=1");
        let path_page_2 = format!("/pools/{pool_id}/delegators?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<PoolDelegatorsInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse delegators page 1");
        let page_2: Vec<PoolDelegatorsInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse delegators page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 0);
    }
    #[tokio::test]
    async fn pools_delegators_bad_request() {
        let app = TestApp::new();
        let path = format!("/pools/{}/delegators", invalid_pool_id());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn pools_delegators_not_found() {
        let app = TestApp::new();
        let path = format!("/pools/{}/delegators", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn pools_delegators_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/delegators");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
