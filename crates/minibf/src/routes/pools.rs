use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    dreps_inner_metadata_error::Code,
    pool::Pool,
    pool_calidus_key::PoolCalidusKey,
    pool_delegators_inner::PoolDelegatorsInner,
    pool_history_inner::PoolHistoryInner,
    pool_list_extended_inner::PoolListExtendedInner,
    pool_list_retire_inner::PoolListRetireInner,
    pool_updates_inner::{Action, PoolUpdatesInner},
    pool_votes_inner::{self, PoolVotesInner},
    tx_content_pool_certs_inner_relays_inner::TxContentPoolCertsInnerRelaysInner,
    DrepsInnerMetadataError, PoolListExtendedInnerMetadata, PoolMetadata as PoolMetadataModel,
};
use dolos_cardano::{
    cip151,
    indexes::{AsyncCardanoQueryExt, CardanoArchiveIndexExt, SlotOrder},
    model::{AccountState, PoolState},
    pallas_extras, PoolDelegation, PoolHash, StakeLog,
};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain, EntityKey};
use futures::{future::join_all, StreamExt};
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    crypto::hash::Hasher,
    ledger::{
        addresses::Network,
        primitives::{
            conway::{Vote, Voter},
            StakeCredential,
        },
        traverse::MultiEraBlock,
    },
};
use rayon::prelude::*;
use serde::Serialize;

use crate::{
    error::Error,
    log_and_500,
    mapping::{
        bech32_calidus, bech32_pool, i32_or_500, pool_offchain_metadata, rational_to_f64,
        stake_cred_to_address, vkey_to_stake_address, IntoModel,
    },
    pagination::{Pagination, PaginationParameters},
    routes::governance::voter_casts,
    Facade,
};

const ACCOUNT_SCAN_CHUNK_SIZE: usize = 4096;

type ActivePools = HashSet<PoolHash>;

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

fn live_stake_pool(account: &AccountState) -> Option<PoolHash> {
    if !account.is_registered() {
        return None;
    }

    account
        .delegated_pool_live()
        .or(account.retired_pool.as_ref())
        .copied()
}

fn live_stake_denominator_pool(
    account: &AccountState,
    active_pools: &ActivePools,
) -> Option<PoolHash> {
    if !account.is_registered() {
        return None;
    }

    let pool = account.delegated_pool_live()?;

    active_pools.contains(pool).then_some(*pool)
}

fn active_stake_pool(account: &AccountState) -> Option<PoolHash> {
    match account.pool.set() {
        Some(PoolDelegation::Pool(hash)) => Some(*hash),
        _ => None,
    }
}

fn reduce_account_chunk(
    accounts: &[AccountState],
    target_pool: PoolHash,
    active_pools: &ActivePools,
) -> PoolMetrics {
    accounts
        .par_iter()
        .fold(PoolMetrics::default, |mut metrics, account| {
            let live_stake = account.live_stake();

            if live_stake_denominator_pool(account, active_pools).is_some() {
                metrics.total_live_stake += live_stake;
            }

            if live_stake_pool(account).is_some_and(|hash| hash == target_pool) {
                metrics.live_stake += live_stake;
            }

            if live_stake_pool(account).is_some_and(|hash| hash == target_pool) {
                metrics.live_delegators += 1;
            }

            if let Some(hash) = active_stake_pool(account) {
                let active_stake = account.active_stake();
                metrics.total_active_stake += active_stake;

                if hash == target_pool {
                    metrics.active_stake += active_stake;
                }
            }

            metrics
        })
        .reduce(PoolMetrics::default, |a, b| a.merge(b))
}

fn load_active_pools<D>(domain: &Facade<D>) -> Result<ActivePools, StatusCode>
where
    D: Domain,
    Option<PoolState>: From<D::Entity>,
{
    let mut active_pools = ActivePools::default();

    for item in domain
        .iter_cardano_entities::<PoolState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        let (_, pool) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if pool
            .snapshot
            .live()
            .is_some_and(|snapshot| !snapshot.is_retired)
        {
            active_pools.insert(pool.operator);
        }
    }

    Ok(active_pools)
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

        if !account.is_registered() {
            return Ok(acc);
        }

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
    active_pools: ActivePools,
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

        metrics = metrics.merge(reduce_account_chunk(&chunk, target_pool, &active_pools));
    }

    metrics.live_pledge = compute_live_pledge(&domain, target_pool, &owners)?;

    Ok(metrics)
}

const POOL_HASH_LEN: usize = 28;
const POOL_HRP: &str = "pool";
const POOL_HRP_UPPER: &str = "POOL";
const POOL_HEX_MAX_BYTES: usize = 49;
const POOL_ID_MIN_UNITS: usize = 8;
const POOL_ID_MAX_UNITS: usize = 1000;
const BECH32_CHARSET: &[u8; 32] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const BECH32_CHECKSUM_LEN: usize = 6;
const BECH32_CONST: u32 = 1;
const BECH32M_CONST: u32 = 0x2bc8_30a3;
const BECH32_GENERATOR: [u32; 5] = [
    0x3b6a_57b2,
    0x2650_8e6d,
    0x1ea1_19fa,
    0x3d42_33dd,
    0x2a14_62b3,
];

fn bech32_step(checksum: u32) -> u32 {
    let top = checksum >> 25;
    let mut next = (checksum & 0x01ff_ffff) << 5;
    for (bit, generator) in BECH32_GENERATOR.iter().enumerate() {
        if (top >> bit) & 1 == 1 {
            next ^= generator;
        }
    }
    next
}

fn bech32_residue(hrp: &[u8], values: &[u8]) -> u32 {
    let mut checksum = 1;
    for c in hrp {
        checksum = bech32_step(checksum) ^ u32::from(c >> 5);
    }
    checksum = bech32_step(checksum);
    for c in hrp {
        checksum = bech32_step(checksum) ^ u32::from(c & 0x1f);
    }
    for value in values {
        checksum = bech32_step(checksum) ^ u32::from(*value);
    }
    checksum
}

fn bech32_values(data: &str) -> Option<Vec<u8>> {
    data.bytes()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            BECH32_CHARSET
                .iter()
                .position(|&x| x == c)
                .map(|value| value as u8)
        })
        .collect()
}

fn pool_hash_from_bytes(bytes: &[u8]) -> Option<PoolHash> {
    <[u8; POOL_HASH_LEN]>::try_from(bytes)
        .ok()
        .map(PoolHash::from)
}

/// Gives the hash only for the canonical ID: the lowercase Bech32 text of 28
/// bytes.
///
/// An `EntityKey` has 32 bytes. `EntityKey::from(&[u8])` adds zero bytes to a
/// short value and cuts a long value to 32 bytes. Thus, a 29-byte value that
/// ends with `0x00` finds the pool of its first 28 bytes. The parsers return a
/// `PoolHash` for this reason.
fn canonical_pool_hash(input: &str) -> Option<PoolHash> {
    let (_, bytes) = bech32::decode(input).ok()?;
    let hash = pool_hash_from_bytes(&bytes)?;
    (bech32_pool(hash).ok()? == input).then_some(hash)
}

/// Parses a pool ID with the rules of `/pools/{pool_id}` and
/// `/pools/{pool_id}/history`.
///
/// Hex of even length gives its bytes, with no length limit.
/// Other text must be a Bech32 or Bech32m string with the `pool` prefix in one
/// letter case. Other input gives `Error::InvalidPoolId`.
///
/// The result is `None` for a valid ID that no pool can have.
/// Blockfrost finds a pool only from the canonical form of its ID.
/// Thus, a valid ID in a different form finds no pool.
pub(crate) fn parse_pool_id_unbounded(input: &str) -> Result<Option<PoolHash>, Error> {
    if let Ok(bytes) = hex::decode(input) {
        return Ok(pool_hash_from_bytes(&bytes));
    }

    let (hrp, data) = input.rsplit_once('1').ok_or(Error::InvalidPoolId)?;
    let upper = match hrp {
        POOL_HRP => false,
        POOL_HRP_UPPER => true,
        _ => return Err(Error::InvalidPoolId),
    };
    let wrong_case = |c: u8| {
        if upper {
            c.is_ascii_lowercase()
        } else {
            c.is_ascii_uppercase()
        }
    };
    if data.len() < BECH32_CHECKSUM_LEN || data.bytes().any(wrong_case) {
        return Err(Error::InvalidPoolId);
    }

    let values = bech32_values(data).ok_or(Error::InvalidPoolId)?;
    let residue = bech32_residue(POOL_HRP.as_bytes(), &values);
    if residue != BECH32_CONST && residue != BECH32M_CONST {
        return Err(Error::InvalidPoolId);
    }

    Ok(canonical_pool_hash(input))
}

/// Parses a pool ID with the rules of the other pool routes and the epoch pool
/// routes.
///
/// Hex gives its bytes. For hex of odd length, the parser ignores the last
/// digit. Hex of 50 bytes or more gives `Error::InvalidPoolId`.
/// Other text must be a Bech32 string of 8 to 1000 UTF-16 units with the
/// `pool` prefix. The text must be equal to its Unicode lowercase form or its
/// Unicode uppercase form.
///
/// The Kelvin sign U+212A has the lowercase form `k` and is its own uppercase
/// form. Thus, an uppercase ID with this sign is valid, but it finds no pool.
///
/// The result is `None` for a valid ID that no pool can have.
pub(crate) fn parse_pool_id_bounded(input: &str) -> Result<Option<PoolHash>, Error> {
    if !input.is_empty() && input.bytes().all(|c| c.is_ascii_hexdigit()) {
        let bytes = hex::decode(&input[..input.len() & !1]).map_err(|_| Error::InvalidPoolId)?;
        if bytes.len() > POOL_HEX_MAX_BYTES {
            return Err(Error::InvalidPoolId);
        }
        return Ok(pool_hash_from_bytes(&bytes));
    }

    let units = input.encode_utf16().count();
    if !(POOL_ID_MIN_UNITS..=POOL_ID_MAX_UNITS).contains(&units) {
        return Err(Error::InvalidPoolId);
    }

    let lower = input.to_lowercase();
    if input != lower && input != input.to_uppercase() {
        return Err(Error::InvalidPoolId);
    }

    let (hrp, data) = lower.rsplit_once('1').ok_or(Error::InvalidPoolId)?;
    if hrp != POOL_HRP || data.len() < BECH32_CHECKSUM_LEN {
        return Err(Error::InvalidPoolId);
    }

    let values = bech32_values(data).ok_or(Error::InvalidPoolId)?;
    if bech32_residue(POOL_HRP.as_bytes(), &values) != BECH32_CONST {
        return Err(Error::InvalidPoolId);
    }

    Ok(canonical_pool_hash(input))
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

fn scan_pool_updates_in_block(
    block: &MultiEraBlock,
    target_pool: &PoolHash,
) -> Vec<(String, i32, Action)> {
    let mut updates = Vec::new();

    for tx in block.txs() {
        let tx_hash = tx.hash().to_string();

        for (cert_index, cert) in tx.certs().iter().enumerate() {
            let action = if pallas_extras::cert_as_pool_registration(cert)
                .is_some_and(|cert| &cert.operator == target_pool)
            {
                Some(Action::Registered)
            } else if pallas_extras::cert_as_pool_retirement(cert)
                .is_some_and(|cert| &cert.operator == target_pool)
            {
                Some(Action::Deregistered)
            } else {
                None
            };

            if let Some(action) = action {
                updates.push((tx_hash.clone(), cert_index as i32, action));
            }
        }
    }

    updates
}

async fn load_pool_updates<D>(
    domain: &Facade<D>,
    pool: PoolHash,
    order: SlotOrder,
) -> Result<Vec<(String, i32, Action)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let tip = domain.get_tip_slot()?;
    let stream = domain
        .query()
        .blocks_by_pool_certs_stream(pool.as_slice(), 0, tip, order);

    let mut stream = Box::pin(stream);
    let mut updates = Vec::new();

    while let Some(item) = stream.next().await {
        let (_, block) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let Some(block) = block else {
            continue;
        };

        let block = MultiEraBlock::decode(block.as_slice())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let mut block_updates = scan_pool_updates_in_block(&block, &pool);

        // For descending order, the stream gives blocks from the tip backward.
        // Reverse the certs in each block. Then the full list follows the
        // chain position in reverse.
        if matches!(order, SlotOrder::Desc) {
            block_updates.reverse();
        }

        updates.append(&mut block_updates);
    }

    Ok(updates)
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
    let Some(hash) = parse_pool_id_unbounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    let pool = domain
        .read_cardano_entity::<PoolState>(hash)?
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

    let active_pools = load_active_pools(&domain)?;
    let owners = params.pool_owners.clone();
    let operator = pool.operator;
    let domain_clone = domain.clone();
    let metrics = tokio::task::spawn_blocking(move || {
        compute_pool_metrics_sync(domain_clone, operator, active_pools, owners)
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

#[derive(Serialize)]
#[serde(untagged)]
pub enum PoolMetadataResponse {
    Metadata(PoolMetadataModel),
    Empty(EmptyObject),
}

#[derive(Default, Serialize)]
pub struct EmptyObject {}

fn build_pool_extended_metadata(
    onchain: Option<&pallas::ledger::primitives::PoolMetadata>,
    offchain: Option<crate::mapping::PoolOffchainMetadata>,
) -> Option<Box<PoolListExtendedInnerMetadata>> {
    onchain.map(|onchain| {
        Box::new(match offchain {
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
        })
    })
}

fn build_pool_metadata_response(
    operator: impl AsRef<[u8]>,
    onchain: Option<&pallas::ledger::primitives::PoolMetadata>,
    offchain: Option<crate::mapping::PoolOffchainMetadata>,
    error: Option<DrepsInnerMetadataError>,
) -> Result<PoolMetadataResponse, StatusCode> {
    let operator = operator.as_ref();

    let Some(onchain) = onchain else {
        return Ok(PoolMetadataResponse::Empty(EmptyObject::default()));
    };

    Ok(PoolMetadataResponse::Metadata(PoolMetadataModel {
        pool_id: bech32_pool(operator)?,
        hex: hex::encode(operator),
        url: Some(onchain.url.clone()),
        hash: Some(hex::encode(&*onchain.hash)),
        error: error.map(Box::new),
        ticker: offchain.as_ref().map(|x| x.ticker.clone()),
        name: offchain.as_ref().map(|x| x.name.clone()),
        description: offchain.as_ref().map(|x| x.description.clone()),
        homepage: offchain.as_ref().map(|x| x.homepage.clone()),
    }))
}

fn hash_mismatch_error(
    url: &str,
    expected_hash: &[u8],
    actual_hash: &[u8],
) -> blockfrost_openapi::models::DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        Code::HashMismatch,
        format!(
            "Hash mismatch when fetching metadata from {url}. Expected \"{}\" but got \"{}\".",
            hex::encode(expected_hash),
            hex::encode(actual_hash),
        ),
    )
}

fn http_response_error(url: &str, status: StatusCode) -> DrepsInnerMetadataError {
    let reason = status.canonical_reason().unwrap_or("Unknown");

    DrepsInnerMetadataError::new(
        Code::HttpResponseError,
        format!(
            "Error Offchain Pool: HTTP Response error from {url} resulted in HTTP status code : {} \"{reason}\"",
            status.as_u16(),
        ),
    )
}

fn connection_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        Code::ConnectionError,
        format!("Error Offchain Pool: Connection failure error when fetching metadata from {url}."),
    )
}

async fn fetch_pool_offchain_metadata(
    pool: &PoolState,
) -> Option<crate::mapping::PoolOffchainMetadata> {
    let metadata = pool
        .snapshot
        .live()
        .and_then(|x| x.params.pool_metadata.as_ref());

    match metadata {
        Some(metadata) => {
            pool_offchain_metadata(&metadata.url, Some(metadata.hash.as_slice())).await
        }
        None => None,
    }
}

async fn fetch_pool_metadata_with_error(
    pool: &PoolState,
) -> (
    Option<crate::mapping::PoolOffchainMetadata>,
    Option<DrepsInnerMetadataError>,
) {
    let Some(metadata) = pool
        .snapshot
        .live()
        .and_then(|x| x.params.pool_metadata.as_ref())
    else {
        return (None, None);
    };

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::limited(3))
        .user_agent("Dolos MiniBF")
        .build()
    {
        Ok(client) => client,
        Err(_) => return (None, None),
    };

    let response = match client.get(&metadata.url).send().await {
        Ok(response) => response,
        Err(_) => return (None, Some(connection_error(&metadata.url))),
    };

    if response.status() != StatusCode::OK {
        return (
            None,
            Some(http_response_error(&metadata.url, response.status())),
        );
    }

    let body = match response.bytes().await {
        Ok(body) => body,
        Err(_) => return (None, None),
    };

    let actual_hash = Hasher::<256>::hash(body.as_ref());

    if actual_hash.as_ref() != metadata.hash.as_slice() {
        return (
            None,
            Some(hash_mismatch_error(
                &metadata.url,
                metadata.hash.as_slice(),
                actual_hash.as_ref(),
            )),
        );
    }

    match serde_json::from_slice(body.as_ref()) {
        Ok(offchain) => (Some(offchain), None),
        Err(_) => (None, None),
    }
}

fn select_pools(
    pools: impl IntoIterator<Item = (BlockSlot, PoolHash)>,
    pagination: &Pagination,
) -> Vec<PoolHash> {
    let mut pools: Vec<(BlockSlot, PoolHash)> = pools.into_iter().collect();

    pools.sort_unstable_by_key(|(slot, operator)| (*slot, *operator));

    if matches!(pagination.order, crate::pagination::Order::Desc) {
        pools.reverse();
    }

    pools
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(_, operator)| operator)
        .collect()
}

pub async fn all<D: Domain>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let pools = domain
        .iter_cardano_entities::<PoolState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .filter_map(|x| {
            let (_, state) = match x {
                Ok(item) => item,
                Err(_) => return Some(Err(StatusCode::INTERNAL_SERVER_ERROR)),
            };
            if state.snapshot.live().map(|x| x.is_retired).unwrap_or(false) {
                return None;
            }
            Some(Ok((state.register_slot, state.operator)))
        })
        .collect::<Result<Vec<(BlockSlot, PoolHash)>, StatusCode>>()?;

    let out = select_pools(pools, &pagination)
        .into_iter()
        .map(bech32_pool)
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
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
        if let Some(hash) = live_stake_pool(&state) {
            let stake = state.live_stake();
            live_stake_map
                .entry(hash)
                .and_modify(|entry| *entry += stake)
                .or_insert(stake);
        };
        if let Some(hash) = active_stake_pool(&state) {
            let stake = state.stake.set().map(|x| x.total()).unwrap_or(0);
            active_stake_map
                .entry(hash)
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

    let mut pools = domain
        .iter_cardano_entities::<PoolState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .flat_map(|x| {
            let Ok((key, state)) = x else {
                return Some(Err(StatusCode::INTERNAL_SERVER_ERROR));
            };
            if state.snapshot.live().map(|x| x.is_retired).unwrap_or(false) {
                return None;
            }
            Some(Ok(((state.register_slot, state.operator), (key, state))))
        })
        .collect::<Result<Vec<((BlockSlot, PoolHash), (EntityKey, PoolState))>, StatusCode>>()?;

    pools.sort_by(|a, b| Ord::cmp(&a.0, &b.0));

    if matches!(pagination.order, crate::pagination::Order::Desc) {
        pools.reverse();
    }

    let pools = pools
        .into_iter()
        .map(|(_, x)| x)
        .skip(pagination.skip())
        .take(pagination.count)
        .collect_vec();

    let metadata_futures: Vec<_> = pools
        .iter()
        .map(|(_, pool)| fetch_pool_offchain_metadata(pool))
        .collect();

    let metadata_results = join_all(metadata_futures).await;

    let mut out = vec![];

    for ((_, pool), fetched_metadata) in pools.into_iter().zip(metadata_results) {
        let poolhex = hex::encode(pool.operator);
        let pool_id = bech32_pool(pool.operator)?;
        let params = pool.snapshot.live().map(|x| x.params.clone());
        let metadata = params
            .as_ref()
            .and_then(|x| build_pool_extended_metadata(x.pool_metadata.as_ref(), fetched_metadata));

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

fn select_retiring_pools(
    pools: impl IntoIterator<Item = PoolState>,
    current_epoch: u64,
    pagination: &Pagination,
) -> Vec<(u64, PoolHash)> {
    let mut retiring: Vec<(u64, BlockSlot, PoolHash)> = pools
        .into_iter()
        .filter_map(|pool| {
            let retiring_epoch = pool.retiring_epoch?;
            (retiring_epoch > current_epoch).then_some((
                retiring_epoch,
                pool.register_slot,
                pool.operator,
            ))
        })
        .collect();

    retiring.sort_unstable_by_key(|(epoch, slot, operator)| (*epoch, *slot, *operator));

    if matches!(pagination.order, crate::pagination::Order::Desc) {
        retiring.reverse();
    }

    retiring
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(retiring_epoch, _, operator)| (retiring_epoch, operator))
        .collect()
}

pub async fn all_retiring<D: Domain>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListRetireInner>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (current_epoch, _) = summary.slot_epoch(tip);

    let pools = domain
        .iter_cardano_entities::<PoolState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|item| item.map(|(_, pool)| pool))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let out = select_retiring_pools(pools, current_epoch, &pagination)
        .into_iter()
        .map(|(retiring_epoch, operator)| {
            Ok(PoolListRetireInner {
                pool_id: bech32_pool(operator)?,
                epoch: retiring_epoch as i32,
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
}

fn select_retired_pools(
    pools: impl IntoIterator<Item = PoolState>,
    pagination: &Pagination,
) -> Vec<(u64, PoolHash)> {
    let mut retired: Vec<(u64, BlockSlot, PoolHash)> = pools
        .into_iter()
        .filter_map(|pool| {
            let is_retired = pool.snapshot.live().map(|x| x.is_retired).unwrap_or(false);
            let retiring_epoch = pool.retiring_epoch?;
            is_retired.then_some((retiring_epoch, pool.register_slot, pool.operator))
        })
        .collect();

    retired.sort_unstable_by_key(|(epoch, slot, operator)| (*epoch, *slot, *operator));

    if matches!(pagination.order, crate::pagination::Order::Desc) {
        retired.reverse();
    }

    retired
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(retiring_epoch, _, operator)| (retiring_epoch, operator))
        .collect()
}

pub async fn all_retired<D: Domain>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListRetireInner>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let pools = domain
        .iter_cardano_entities::<PoolState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|item| item.map(|(_, pool)| pool))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let out = select_retired_pools(pools, &pagination)
        .into_iter()
        .map(|(retiring_epoch, operator)| {
            Ok(PoolListRetireInner {
                pool_id: bech32_pool(operator)?,
                epoch: retiring_epoch as i32,
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
}

pub async fn by_id_metadata<D: Domain>(
    Path(id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<PoolMetadataResponse>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let Some(hash) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    let pool = domain
        .read_cardano_entity::<PoolState>(hash)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let onchain = pool
        .snapshot
        .live()
        .and_then(|x| x.params.pool_metadata.as_ref());
    let (offchain, error) = fetch_pool_metadata_with_error(&pool).await;

    Ok(Json(build_pool_metadata_response(
        hash, onchain, offchain, error,
    )?))
}

pub async fn by_id_relays<D: Domain>(
    Path(id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentPoolCertsInnerRelaysInner>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let Some(hash) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    let pool = domain
        .read_cardano_entity::<PoolState>(hash)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let relays = pool
        .snapshot
        .live()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
        .params
        .relays
        .clone();

    let out = relays
        .into_iter()
        .map(|relay| relay.into_model())
        .collect::<Result<Vec<_>, StatusCode>>()?;

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

/// Blocks minted by a pool, oldest first.
///
/// The `pool_blocks` archive dimension tags each block with its issuer pool,
/// so the page is a key-only slot scan plus one body read per listed block.
/// The cost does not grow with the page number, so deep pages stay cheap and
/// the scan budget does not apply.
pub async fn by_id_blocks<D>(
    Path(id): Path<String>,
    Query(mut params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    // Drop `from`/`to` before validation: Blockfrost never reads them here,
    // so a malformed or reversed window is ignored rather than rejected.
    params.from = None;
    params.to = None;

    let pagination = Pagination::try_from(params)?;
    let Some(pool) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let tip = domain.get_tip_slot()?;

    let inner = domain.inner.clone();
    let skip = pagination.skip();
    let count = pagination.count;
    let order = pagination.order;

    let (page, minted_any) =
        tokio::task::spawn_blocking(move || -> Result<(Vec<String>, bool), StatusCode> {
            let mut slots = inner
                .archive()
                .slots_by_pool_blocks(pool.as_slice(), 0, tip)
                .map_err(log_and_500("failed to read the pool blocks index"))?
                .peekable();

            // Whether the pool minted anything at all. The 404 rule below
            // needs it, and the slots are already in memory.
            let minted_any = slots.peek().is_some();

            // Page over the slots alone, so no block outside the page is read.
            let page_slots: Vec<BlockSlot> = match order {
                crate::pagination::Order::Asc => {
                    slots.skip(skip).take(count).collect::<Result<_, _>>()
                }
                crate::pagination::Order::Desc => {
                    slots.rev().skip(skip).take(count).collect::<Result<_, _>>()
                }
            }
            .map_err(log_and_500("failed to page the pool blocks index"))?;

            let mut page = Vec::with_capacity(page_slots.len());

            for slot in page_slots {
                let body = inner
                    .archive()
                    .get_block_by_slot(&slot)
                    .map_err(log_and_500("failed to read a block of a pool"))?;

                // A tagged slot always holds a block, and a Byron block is
                // never tagged. Skip either, rather than fail the page.
                let Some(body) = body else {
                    tracing::warn!(slot, "pool blocks index points at a missing block");
                    continue;
                };

                let Some(header) = super::epochs::decode_block_header(&body)? else {
                    tracing::warn!(slot, "pool blocks index points at a Byron block");
                    continue;
                };

                page.push(header.hash().to_string());
            }

            Ok((page, minted_any))
        })
        .await
        .map_err(log_and_500("pool blocks scan task failed"))??;

    // Blockfrost 404s a pool that db-sync never saw. The `PoolState` entity
    // covers every pool that registered on chain, and a pool must register
    // before it can mint. The index acts as a second proof of existence, for
    // any issuer that has no entity.
    if !minted_any && !domain.cardano_entity_exists::<PoolState>(pool)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    Ok(Json(page))
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
    let pagination = Pagination::try_from(params)?;
    let Some(hash) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    if !domain.cardano_entity_exists::<PoolState>(hash)? {
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
            .is_some_and(|f| *f == hash)
    });

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

/// A pool margin as db-sync stores it: a `float8` holding the ratio rounded
/// to the nearest double.
///
/// Two operands up to 2^53 convert exactly, so their quotient is that nearest
/// double. Larger ones go through a long decimal expansion, which `parse`
/// rounds correctly.
fn margin_f64(numerator: u64, denominator: u64) -> f64 {
    const EXACT: u64 = 1 << 53;

    if denominator == 0 {
        return 0.0;
    }

    if numerator <= EXACT && denominator <= EXACT {
        return numerator as f64 / denominator as f64;
    }

    let divisor = denominator as u128;
    let mut remainder = numerator as u128 % divisor;
    let mut text = format!("{}.", numerator as u128 / divisor);

    for _ in 0..60 {
        remainder *= 10;
        text.push(char::from(b'0' + (remainder / divisor) as u8));
        remainder %= divisor;
    }

    text.parse().unwrap_or_default()
}

// HACK: Blockfrost does not derive the operator share. Its SQL reports
// `FLOOR(fee + (rewards - fee) * margin)`, and `rewards` when they fall short
// of the fixed cost. db-sync keeps the margin as a `float8`, so Postgres runs
// the whole expression in `float8`; this does the same, operation for
// operation.
fn bf_compatible_fees(log: &StakeLog) -> u64 {
    let rewards = log.total_rewards;
    let fixed = log.fixed_cost;

    if rewards < fixed {
        return rewards;
    }

    let margin = log
        .margin_cost
        .as_ref()
        .map(|margin| margin_f64(margin.numerator, margin.denominator))
        .unwrap_or_default();

    (fixed as f64 + (rewards - fixed) as f64 * margin).floor() as u64
}

pub async fn by_id_history<D: Domain>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolHistoryInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<PoolState>: From<D::Entity>,
{
    let hash = parse_pool_id_unbounded(&id)?;
    let pagination = Pagination::try_from(params)?;
    let Some(hash) = hash else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    if !domain.cardano_entity_exists::<PoolState>(hash)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (epoch, _) = summary.slot_epoch(tip);

    let mut entries = domain
        .iter_cardano_logs_per_epoch::<StakeLog>(hash.into(), 0..epoch)
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

pub async fn by_id_updates<D>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolUpdatesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let Some(pool) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    if !domain.cardano_entity_exists::<PoolState>(pool)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let order = match pagination.order {
        crate::pagination::Order::Asc => SlotOrder::Asc,
        crate::pagination::Order::Desc => SlotOrder::Desc,
    };

    let updates = load_pool_updates(&domain, pool, order).await?;

    let out = updates
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(tx_hash, cert_index, action)| PoolUpdatesInner {
            tx_hash,
            cert_index,
            action,
        })
        .collect();

    Ok(Json(out))
}

fn pool_vote_model(vote: &Vote) -> pool_votes_inner::Vote {
    match vote {
        Vote::Yes => pool_votes_inner::Vote::Yes,
        Vote::No => pool_votes_inner::Vote::No,
        Vote::Abstain => pool_votes_inner::Vote::Abstain,
    }
}

/// `GET /pools/{pool_id}/votes` returns the votes of a stake pool. By default,
/// the list starts with the oldest vote.
///
/// `tx_hash` is the hash of the transaction that contains the vote.
/// `cert_index` is the index of the vote in the ballot of the pool in that
/// transaction. A repeated vote on the same proposal is a separate row.
///
/// If the pool never registered, the endpoint returns 404. The endpoint does
/// not return 404 for a retired pool. If the pool registered but has no votes,
/// the endpoint returns an empty list.
///
/// `max_scan_items` limits both the page depth and the number of blocks that
/// one request reads.
///
/// The endpoint does the checks in the same order as Blockfrost. The order is
/// the query string, then the pool ID, then the registration of the pool.
/// Blockfrost has no scan limit. Thus, the endpoint does the scan limit check
/// after the Blockfrost checks.
pub async fn by_id_votes<D>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolVotesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let Some(pool) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    if !domain.cardano_entity_exists::<PoolState>(pool)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let voter = Voter::StakePoolKey(pool);
    let tip = domain.get_tip_slot()?;
    let budget = domain.config.max_scan_items() as usize;

    let casts = domain
        .query()
        .run_blocking(move |domain| Ok(voter_casts(&domain, &voter, tip, &pagination, budget)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    let out = casts
        .into_iter()
        .map(|cast| {
            Ok(PoolVotesInner {
                tx_hash: hex::encode(cast.tx),
                cert_index: i32_or_500(cast.cert_index)?,
                vote: pool_vote_model(&cast.vote),
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        bech32_encode_values, bech32_values_from_bytes, pool_id_cases, PoolIdCase, TestApp,
        TestFault, REG_POOL_HEX, REG_POOL_ID,
    };
    use blockfrost_openapi::models::{
        pool::Pool, pool_delegators_inner::PoolDelegatorsInner,
        pool_list_extended_inner::PoolListExtendedInner,
        pool_list_retire_inner::PoolListRetireInner,
    };
    use dolos_cardano::cip151;
    use dolos_cardano::model::{DRepDelegation, EpochValue, PoolParams, PoolSnapshot, Stake};
    use dolos_testing::synthetic::{SyntheticBlockConfig, SyntheticProposalRef, SyntheticVote};
    use pallas::{
        codec::utils::Bytes,
        crypto::hash::Hash,
        ledger::primitives::{
            alonzo, conway::GovAction, Int, PoolMetadata, Relay, StakeCredential,
        },
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

    /// The payloads of these pool IDs do not have 28 bytes. The first payload
    /// is the hash of the registered pool without its last byte. The second
    /// payload is the hash with a `0x00` byte at the end. Without the length
    /// check, the registration check finds the pool for the second payload.
    fn wrong_length_pool_ids(app: &TestApp) -> [String; 2] {
        let mut payload = parse_pool_id_unbounded(&app.vectors().pool_id)
            .ok()
            .flatten()
            .expect("Cannot decode the pool ID.")
            .to_vec();
        let short = bech32_pool(&payload[..27]).expect("Cannot encode the short pool ID.");
        payload.push(0);
        let long = bech32_pool(&payload).expect("Cannot encode the long pool ID.");

        [short, long]
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
        let operator = parse_pool_id_unbounded(pool_id)
            .ok()
            .flatten()
            .expect("valid pool id")
            .to_vec();

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
    fn pool_metrics_exclude_unregistered_retired_pool_stake() {
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

        let metrics = reduce_account_chunk(&[account], target_pool, &ActivePools::default());

        assert_eq!(metrics.live_stake, 0);
        assert_eq!(metrics.live_pledge, 0);
        assert_eq!(metrics.live_delegators, 0);
        assert_eq!(metrics.total_live_stake, 0);
        assert_eq!(metrics.active_stake, 0);
    }

    #[test]
    fn pool_metrics_only_count_registered_accounts_with_pool_relation() {
        let target_pool = Hash::from([1u8; 28]);
        let other_active_pool = Hash::from([2u8; 28]);
        let retired_pool = Hash::from([3u8; 28]);

        let accounts = [
            AccountState {
                registered_at: Some(1),
                stake: EpochValue::with_live(
                    3,
                    Stake {
                        utxo_sum: 10,
                        ..Default::default()
                    },
                ),
                pool: EpochValue::with_live(3, PoolDelegation::Pool(target_pool)),
                drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
                vote_delegated_at: None,
                deregistered_at: None,
                credential: StakeCredential::AddrKeyhash(Hash::from([2u8; 28])),
                retired_pool: None,
            },
            AccountState {
                registered_at: Some(1),
                stake: EpochValue::with_live(
                    3,
                    Stake {
                        utxo_sum: 20,
                        ..Default::default()
                    },
                ),
                pool: EpochValue::with_live(3, PoolDelegation::Pool(other_active_pool)),
                drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
                vote_delegated_at: None,
                deregistered_at: None,
                credential: StakeCredential::AddrKeyhash(Hash::from([3u8; 28])),
                retired_pool: None,
            },
            AccountState {
                registered_at: Some(1),
                stake: EpochValue::with_live(
                    3,
                    Stake {
                        utxo_sum: 30,
                        ..Default::default()
                    },
                ),
                pool: EpochValue::with_live(3, PoolDelegation::Pool(retired_pool)),
                drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
                vote_delegated_at: None,
                deregistered_at: None,
                credential: StakeCredential::AddrKeyhash(Hash::from([4u8; 28])),
                retired_pool: Some(retired_pool),
            },
            AccountState {
                registered_at: Some(1),
                stake: EpochValue::with_live(
                    3,
                    Stake {
                        utxo_sum: 40,
                        ..Default::default()
                    },
                ),
                pool: EpochValue::with_live(3, PoolDelegation::NotDelegated),
                drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
                vote_delegated_at: None,
                deregistered_at: None,
                credential: StakeCredential::AddrKeyhash(Hash::from([5u8; 28])),
                retired_pool: None,
            },
            AccountState {
                registered_at: Some(1),
                stake: EpochValue::with_live(
                    3,
                    Stake {
                        utxo_sum: 50,
                        ..Default::default()
                    },
                ),
                pool: EpochValue::with_live(3, PoolDelegation::Pool(target_pool)),
                drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
                vote_delegated_at: None,
                deregistered_at: Some(2),
                credential: StakeCredential::AddrKeyhash(Hash::from([6u8; 28])),
                retired_pool: None,
            },
        ];

        let active_pools = ActivePools::from([target_pool, other_active_pool]);
        let metrics = reduce_account_chunk(&accounts, target_pool, &active_pools);

        assert_eq!(metrics.live_stake, 10);
        assert_eq!(metrics.live_pledge, 0);
        assert_eq!(metrics.live_delegators, 1);
        assert_eq!(metrics.total_live_stake, 30);
        assert_eq!(metrics.active_stake, 0);
        assert_eq!(metrics.total_active_stake, 0);
    }

    #[test]
    fn pool_metrics_include_retired_pool_stake_for_registered_accounts() {
        let target_pool = Hash::from([1u8; 28]);
        let account = AccountState {
            registered_at: Some(1),
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

        let metrics = reduce_account_chunk(&[account], target_pool, &ActivePools::default());

        assert_eq!(metrics.live_stake, 42);
        assert_eq!(metrics.total_live_stake, 0);
        assert_eq!(metrics.live_delegators, 1);
    }

    #[tokio::test]
    async fn pool_by_id_matches_extended_live_metrics() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let pool_path = format!("/pools/{pool_id}");
        let (pool_status, pool_bytes) = app.get_bytes(&pool_path).await;
        let (extended_status, extended_bytes) =
            app.get_bytes("/pools/extended?page=1&count=100").await;

        assert_eq!(pool_status, StatusCode::OK);
        assert_eq!(extended_status, StatusCode::OK);

        let pool: Pool =
            serde_json::from_slice(&pool_bytes).expect("failed to parse pool response");
        let extended: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&extended_bytes).expect("failed to parse pool list extended");

        let extended_pool = extended
            .into_iter()
            .find(|candidate| candidate.pool_id == pool_id)
            .expect("pool missing from extended response");

        assert_eq!(extended_pool.live_stake, pool.live_stake);
        assert_eq!(extended_pool.live_saturation, pool.live_saturation);
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
    async fn pools_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pools: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse pool list");

        let pool_id = app.vectors().pool_id.as_str();
        assert!(
            pools.iter().any(|candidate| candidate == pool_id),
            "expected pool {pool_id} in list, got {pools:?}"
        );
    }

    #[tokio::test]
    async fn pools_matches_extended_ids() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools?page=1&count=100").await;
        let (extended_status, extended_bytes) =
            app.get_bytes("/pools/extended?page=1&count=100").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(extended_status, StatusCode::OK);

        let pools: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse pool list");
        let extended: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&extended_bytes).expect("failed to parse pool list extended");

        let extended_ids: Vec<String> = extended.into_iter().map(|pool| pool.pool_id).collect();

        assert_eq!(pools, extended_ids);
    }

    #[tokio::test]
    async fn pools_order_desc_reverses_asc() {
        let app = TestApp::new();
        let (asc_status, asc_bytes) = app.get_bytes("/pools?order=asc&count=100").await;
        let (desc_status, desc_bytes) = app.get_bytes("/pools?order=desc&count=100").await;

        assert_eq!(asc_status, StatusCode::OK);
        assert_eq!(desc_status, StatusCode::OK);

        let asc: Vec<String> =
            serde_json::from_slice(&asc_bytes).expect("failed to parse asc pool list");
        let mut desc: Vec<String> =
            serde_json::from_slice(&desc_bytes).expect("failed to parse desc pool list");

        desc.reverse();
        assert_eq!(asc, desc);
    }

    #[tokio::test]
    async fn pools_matches_extended_ids_desc() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools?order=desc&count=100").await;
        let (extended_status, extended_bytes) =
            app.get_bytes("/pools/extended?order=desc&count=100").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(extended_status, StatusCode::OK);

        let pools: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse pool list");
        let extended: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&extended_bytes).expect("failed to parse pool list extended");

        let extended_ids: Vec<String> = extended.into_iter().map(|pool| pool.pool_id).collect();

        assert_eq!(pools, extended_ids);
    }

    #[tokio::test]
    async fn pools_empty_out_of_range_page() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools?page=694269").await;

        assert_eq!(status, StatusCode::OK);

        let pools: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse pool list");
        assert!(pools.is_empty());
    }

    #[tokio::test]
    async fn pools_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/pools?count=invalid", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn pools_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/pools", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn pools_retiring_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools/retiring").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let retiring: Vec<PoolListRetireInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool list retire");

        assert!(
            retiring.is_empty(),
            "synthetic ledger registers pools but never schedules a future retirement, so the list is expected to be empty"
        );
    }

    fn retiring_pool(
        operator: [u8; 28],
        register_slot: u64,
        retiring_epoch: Option<u64>,
    ) -> PoolState {
        let params = PoolParams {
            vrf_keyhash: Hash::from([0u8; 32]),
            pledge: 0,
            cost: 0,
            margin: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            reward_account: vec![0u8; 29],
            pool_owners: vec![],
            relays: vec![],
            pool_metadata: None,
        };
        let snapshot = PoolSnapshot {
            is_retired: false,
            blocks_minted: 0,
            params,
            is_new: false,
        };
        PoolState {
            operator: Hash::from(operator),
            snapshot: EpochValue::with_live(3, snapshot),
            blocks_minted_total: 0,
            register_slot,
            retiring_epoch,
            deposit: 0,
        }
    }

    #[test]
    fn select_pools_orders_and_paginates() {
        let pools = vec![
            (30, Hash::from([3u8; 28])),
            (10, Hash::from([1u8; 28])),
            (20, Hash::from([2u8; 28])),
            // same register slot as [2u8; 28]: stable tie-break on operator
            (20, Hash::from([9u8; 28])),
        ];

        let asc = select_pools(pools.clone(), &Pagination::default());
        assert_eq!(
            asc,
            vec![
                Hash::from([1u8; 28]),
                Hash::from([2u8; 28]),
                Hash::from([9u8; 28]),
                Hash::from([3u8; 28]),
            ]
        );

        let desc = Pagination {
            order: crate::pagination::Order::Desc,
            ..Pagination::default()
        };
        let ordered_desc = select_pools(pools.clone(), &desc);
        assert_eq!(
            ordered_desc,
            vec![
                Hash::from([3u8; 28]),
                Hash::from([9u8; 28]),
                Hash::from([2u8; 28]),
                Hash::from([1u8; 28]),
            ]
        );

        let paged = Pagination {
            count: 2,
            page: 2,
            ..Pagination::default()
        };
        let second_page = select_pools(pools, &paged);
        assert_eq!(
            second_page,
            vec![Hash::from([9u8; 28]), Hash::from([3u8; 28])]
        );
    }

    #[test]
    fn select_retiring_pools_filters_and_orders() {
        let pools = vec![
            // scheduled in the past/current: excluded
            retiring_pool([1u8; 28], 10, Some(5)),
            retiring_pool([2u8; 28], 20, Some(10)),
            // not retiring: excluded
            retiring_pool([3u8; 28], 30, None),
            // future retirements: included
            retiring_pool([4u8; 28], 40, Some(12)),
            retiring_pool([5u8; 28], 50, Some(11)),
            // same epoch as above, later register_slot -> stable tie-break
            retiring_pool([6u8; 28], 60, Some(11)),
        ];

        let pagination = Pagination::default();
        let selected = select_retiring_pools(pools.clone(), 10, &pagination);

        assert_eq!(
            selected,
            vec![
                (11, Hash::from([5u8; 28])),
                (11, Hash::from([6u8; 28])),
                (12, Hash::from([4u8; 28])),
            ]
        );

        // Descending order reverses the listing.
        let desc = Pagination {
            order: crate::pagination::Order::Desc,
            ..Pagination::default()
        };
        let selected_desc = select_retiring_pools(pools, 10, &desc);
        assert_eq!(
            selected_desc,
            vec![
                (12, Hash::from([4u8; 28])),
                (11, Hash::from([6u8; 28])),
                (11, Hash::from([5u8; 28])),
            ]
        );
    }

    #[test]
    fn select_retiring_pools_paginates() {
        let pools = vec![
            retiring_pool([1u8; 28], 10, Some(11)),
            retiring_pool([2u8; 28], 20, Some(12)),
            retiring_pool([3u8; 28], 30, Some(13)),
        ];

        let params = PaginationParameters {
            count: Some("1".to_string()),
            page: Some("2".to_string()),
            order: None,
            from: None,
            to: None,
        };
        let pagination = Pagination::try_from(params).expect("valid pagination");

        let selected = select_retiring_pools(pools, 10, &pagination);
        assert_eq!(selected, vec![(12, Hash::from([2u8; 28]))]);
    }

    #[tokio::test]
    async fn pools_retiring_bad_request() {
        let app = TestApp::new();
        assert_status(
            &app,
            "/pools/retiring?count=invalid",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn pools_retiring_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/pools/retiring", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn pools_retired_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools/retired").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let retired: Vec<PoolListRetireInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool list retire");

        assert!(
            retired.is_empty(),
            "the synthetic ledger registers pools but never retires them, so the list is empty"
        );
    }

    fn retired_pool(
        operator: [u8; 28],
        register_slot: u64,
        retiring_epoch: Option<u64>,
        is_retired: bool,
    ) -> PoolState {
        let mut pool = retiring_pool(operator, register_slot, retiring_epoch);
        pool.snapshot.unwrap_live_mut().is_retired = is_retired;
        pool
    }

    #[test]
    fn select_retired_pools_filters_and_orders() {
        let pools = vec![
            // These pools are retired. The sort key is
            // (retiring_epoch, register_slot, operator).
            retired_pool([1u8; 28], 10, Some(5), true),
            retired_pool([2u8; 28], 20, Some(10), true),
            // This pool has the same epoch but a later register_slot.
            // The register_slot makes the tie-break stable.
            retired_pool([6u8; 28], 60, Some(10), true),
            // This pool has a retirement epoch but is not retired. The scan
            // removes it.
            retired_pool([3u8; 28], 30, Some(12), false),
            // This pool has no retirement. The scan removes it.
            retired_pool([4u8; 28], 40, None, false),
        ];

        let pagination = Pagination::default();
        let selected = select_retired_pools(pools.clone(), &pagination);

        assert_eq!(
            selected,
            vec![
                (5, Hash::from([1u8; 28])),
                (10, Hash::from([2u8; 28])),
                (10, Hash::from([6u8; 28])),
            ]
        );

        // A descending order reverses the list.
        let desc = Pagination {
            order: crate::pagination::Order::Desc,
            ..Pagination::default()
        };
        let selected_desc = select_retired_pools(pools, &desc);
        assert_eq!(
            selected_desc,
            vec![
                (10, Hash::from([6u8; 28])),
                (10, Hash::from([2u8; 28])),
                (5, Hash::from([1u8; 28])),
            ]
        );
    }

    #[test]
    fn select_retired_pools_paginates() {
        let pools = vec![
            retired_pool([1u8; 28], 10, Some(11), true),
            retired_pool([2u8; 28], 20, Some(12), true),
            retired_pool([3u8; 28], 30, Some(13), true),
        ];

        let params = PaginationParameters {
            count: Some("1".to_string()),
            page: Some("2".to_string()),
            order: None,
            from: None,
            to: None,
        };
        let pagination = Pagination::try_from(params).expect("valid pagination");

        let selected = select_retired_pools(pools, &pagination);
        assert_eq!(selected, vec![(12, Hash::from([2u8; 28]))]);
    }

    #[tokio::test]
    async fn pools_retired_bad_request() {
        let app = TestApp::new();
        assert_status(
            &app,
            "/pools/retired?count=invalid",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn pools_retired_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/pools/retired", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[test]
    fn pool_metadata_response_empty_without_onchain_metadata() {
        let response =
            build_pool_metadata_response([1u8; 28], None, None, None).expect("pool metadata");

        assert!(matches!(response, PoolMetadataResponse::Empty(_)));
    }

    #[test]
    fn pool_metadata_response_includes_onchain_and_offchain_fields() {
        let operator = [1u8; 28];
        let onchain = PoolMetadata {
            url: "https://example.com/pool.json".to_string(),
            hash: vec![2u8; 32].into(),
        };
        let offchain = crate::mapping::PoolOffchainMetadata {
            ticker: "TICK".to_string(),
            name: "Pool Name".to_string(),
            description: "Pool Description".to_string(),
            homepage: "https://example.com".to_string(),
        };

        let response = build_pool_metadata_response(operator, Some(&onchain), Some(offchain), None)
            .expect("pool metadata");

        let PoolMetadataResponse::Metadata(response) = response else {
            panic!("expected metadata response");
        };

        assert_eq!(
            response.pool_id,
            bech32_pool(operator).expect("bech32 pool id")
        );
        let expected_hash = hex::encode([2u8; 32]);
        assert_eq!(response.hex, hex::encode(operator));
        assert_eq!(
            response.url.as_deref(),
            Some("https://example.com/pool.json")
        );
        assert_eq!(response.hash.as_deref(), Some(expected_hash.as_str()));
        assert_eq!(response.ticker.as_deref(), Some("TICK"));
        assert_eq!(response.name.as_deref(), Some("Pool Name"));
        assert_eq!(response.description.as_deref(), Some("Pool Description"));
        assert_eq!(response.homepage.as_deref(), Some("https://example.com"));
        assert!(response.error.is_none());
    }

    #[test]
    fn pool_metadata_response_includes_error_details() {
        let operator = [1u8; 28];
        let onchain = PoolMetadata {
            url: "https://tinyurl.com/39a7pnv5".to_string(),
            hash: vec![
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
                0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67,
                0x89, 0xab, 0xcd, 0xef,
            ]
            .into(),
        };
        let error = hash_mismatch_error(
            &onchain.url,
            onchain.hash.as_slice(),
            &[
                0xb1, 0x23, 0xea, 0x83, 0xd1, 0xf8, 0x7a, 0xfc, 0xb9, 0x5a, 0x76, 0x66, 0x1b, 0xe5,
                0x08, 0xb3, 0x7a, 0x09, 0x57, 0xc7, 0xfa, 0x95, 0xba, 0xa8, 0x83, 0xa8, 0x0d, 0x12,
                0x03, 0xff, 0xcd, 0x94,
            ],
        );

        let response = build_pool_metadata_response(operator, Some(&onchain), None, Some(error))
            .expect("pool metadata");

        let PoolMetadataResponse::Metadata(response) = response else {
            panic!("expected metadata response");
        };

        let error = response.error.expect("expected error");
        assert_eq!(error.code, Code::HashMismatch);
        assert!(error
            .message
            .contains("Hash mismatch when fetching metadata"));
    }

    #[test]
    fn pool_metadata_http_response_error_matches_expected_format() {
        let error =
            http_response_error("https://blockfrost.io/fakemetadata", StatusCode::NOT_FOUND);

        assert_eq!(error.code, Code::HttpResponseError);
        assert_eq!(
            error.message,
            "Error Offchain Pool: HTTP Response error from https://blockfrost.io/fakemetadata resulted in HTTP status code : 404 \"Not Found\""
        );
    }

    #[test]
    fn pool_metadata_connection_error_matches_expected_format() {
        let error =
            connection_error("http://localhost:23009/p/pool_clai_registration_metadata.json");

        assert_eq!(error.code, Code::ConnectionError);
        assert_eq!(
            error.message,
            "Error Offchain Pool: Connection failure error when fetching metadata from http://localhost:23009/p/pool_clai_registration_metadata.json."
        );
    }

    #[tokio::test]
    async fn pools_metadata_happy_path_returns_empty_object_when_unset() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/metadata");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let response: serde_json::Value =
            serde_json::from_slice(&bytes).expect("failed to parse pool metadata");
        assert_eq!(response, serde_json::json!({}));
    }

    #[tokio::test]
    async fn pools_metadata_bad_request() {
        let app = TestApp::new();
        let path = format!("/pools/{}/metadata", invalid_pool_id());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn pools_metadata_not_found() {
        let app = TestApp::new();
        let path = format!("/pools/{}/metadata", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn pools_metadata_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/metadata");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[test]
    fn relay_ipv6_words_swapped_to_network_order() {
        // On-chain bytes of a real preview relay (pool1drrylt...): the ledger
        // serializes relay IPv6 as four little-endian 32-bit words, so the
        // mapped address must match what dbsync/Blockfrost display.
        let onchain: [u8; 16] = [
            0x61, 0x08, 0x01, 0x20, 0x80, 0x2b, 0x00, 0x40, 0xff, 0x3e, 0x16, 0x02, 0x09, 0x90,
            0x05, 0xfe,
        ];

        let relay = Relay::SingleHostAddr(Some(3001), None, Some(Bytes::from(onchain.to_vec())));
        let model = relay.into_model().expect("relay model");

        assert_eq!(
            model.ipv6.as_deref(),
            Some("2001:861:4000:2b80:216:3eff:fe05:9009")
        );
        assert_eq!(model.ipv4, None);
        assert_eq!(model.port, 3001);
    }

    #[tokio::test]
    async fn pools_relays_happy_path() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_relays: vec![
                Relay::SingleHostAddr(Some(3001), Some(Bytes::from(vec![192, 168, 0, 1])), None),
                Relay::SingleHostName(Some(3002), "relay.example.com".to_string()),
                Relay::MultiHostName("_relays._tcp.example.com".to_string()),
            ],
            ..Default::default()
        });

        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/relays");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let relays: Vec<TxContentPoolCertsInnerRelaysInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool relays");

        assert_eq!(
            relays,
            vec![
                TxContentPoolCertsInnerRelaysInner {
                    ipv4: Some("192.168.0.1".to_string()),
                    ipv6: None,
                    dns: None,
                    dns_srv: None,
                    port: 3001,
                },
                TxContentPoolCertsInnerRelaysInner {
                    ipv4: None,
                    ipv6: None,
                    dns: Some("relay.example.com".to_string()),
                    dns_srv: None,
                    port: 3002,
                },
                TxContentPoolCertsInnerRelaysInner {
                    ipv4: None,
                    ipv6: None,
                    dns: None,
                    dns_srv: Some("_relays._tcp.example.com".to_string()),
                    port: 0,
                },
            ]
        );
    }

    #[tokio::test]
    async fn pools_relays_empty_without_registered_relays() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/relays");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let relays: Vec<TxContentPoolCertsInnerRelaysInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool relays");
        assert!(relays.is_empty());
    }

    #[tokio::test]
    async fn pools_relays_bad_request() {
        let app = TestApp::new();
        let path = format!("/pools/{}/relays", invalid_pool_id());
        assert_error_message(&app, &path, "Invalid or malformed pool id format.").await;
    }

    #[tokio::test]
    async fn pools_relays_not_found() {
        let app = TestApp::new();
        let path = format!("/pools/{}/relays", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn pools_relays_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/relays");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
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

    /// Every synthetic block is minted by the same fixed issuer key, so the
    /// pool derived from it owns the whole toy chain.
    fn toy_issuer_pool() -> String {
        bech32_pool(Hasher::<224>::hash(&[0x10, 0x11])).expect("valid pool id")
    }

    /// The hex form of the same pool id. Blockfrost accepts either form.
    fn toy_issuer_pool_hex() -> String {
        Hasher::<224>::hash(&[0x10, 0x11]).to_string()
    }

    #[tokio::test]
    async fn pools_blocks_happy_path() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();

        let (status, bytes) = app
            .get_bytes(&format!("/pools/{pool}/blocks?count=100"))
            .await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let hashes: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse hashes");

        assert!(!hashes.is_empty());

        for hash in &hashes {
            assert_eq!(hash.len(), 64);
            assert!(hash
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        }

        // The toy chain sits in one epoch, so the sibling epoch endpoint
        // lists exactly the same blocks.
        let epoch = app.tip_epoch();
        let (_, bytes) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}?count=100"))
            .await;
        let by_epoch: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse hashes");

        assert_eq!(hashes, by_epoch);
    }

    #[tokio::test]
    async fn pools_blocks_hex_id_matches_bech32() {
        let app = TestApp::new();

        let (_, bech32_bytes) = app
            .get_bytes(&format!("/pools/{}/blocks?count=100", toy_issuer_pool()))
            .await;
        let (status, hex_bytes) = app
            .get_bytes(&format!(
                "/pools/{}/blocks?count=100",
                toy_issuer_pool_hex()
            ))
            .await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&hex_bytes)
        );

        let from_bech32: Vec<String> = serde_json::from_slice(&bech32_bytes).unwrap();
        let from_hex: Vec<String> = serde_json::from_slice(&hex_bytes).unwrap();

        assert!(!from_hex.is_empty());
        assert_eq!(from_bech32, from_hex);
    }

    #[tokio::test]
    async fn pools_blocks_paginated() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();

        let (status_1, bytes_1) = app
            .get_bytes(&format!("/pools/{pool}/blocks?count=1&page=1"))
            .await;
        let (status_2, bytes_2) = app
            .get_bytes(&format!("/pools/{pool}/blocks?count=1&page=2"))
            .await;
        let (status_far, bytes_far) = app
            .get_bytes(&format!("/pools/{pool}/blocks?count=1&page=1000"))
            .await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);
        assert_eq!(status_far, StatusCode::OK);

        let page_1: Vec<String> = serde_json::from_slice(&bytes_1).unwrap();
        let page_2: Vec<String> = serde_json::from_slice(&bytes_2).unwrap();
        let page_far: Vec<String> = serde_json::from_slice(&bytes_far).unwrap();

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1, page_2);
        assert!(page_far.is_empty());
    }

    #[tokio::test]
    async fn pools_blocks_desc_is_reversed_asc() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();

        let (_, asc_bytes) = app
            .get_bytes(&format!("/pools/{pool}/blocks?order=asc&count=100"))
            .await;
        let (_, desc_bytes) = app
            .get_bytes(&format!("/pools/{pool}/blocks?order=desc&count=100"))
            .await;

        let asc: Vec<String> = serde_json::from_slice(&asc_bytes).unwrap();
        let mut desc: Vec<String> = serde_json::from_slice(&desc_bytes).unwrap();

        assert!(!asc.is_empty());

        desc.reverse();
        assert_eq!(asc, desc);
    }

    #[tokio::test]
    async fn pools_blocks_registered_pool_without_blocks_is_empty() {
        let app = TestApp::new();
        let pool = app.vectors().pool_id.clone();

        let (status, bytes) = app.get_bytes(&format!("/pools/{pool}/blocks")).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let hashes: Vec<String> = serde_json::from_slice(&bytes).unwrap();
        assert!(hashes.is_empty());
    }

    #[tokio::test]
    async fn pools_blocks_bad_request() {
        let app = TestApp::new();

        let path = format!("/pools/{}/blocks", invalid_pool_id());
        assert_error_message(&app, &path, "Invalid or malformed pool id format.").await;

        // The endpoint does the query string check before the pool ID check.
        let path = format!("/pools/{}/blocks?count=0", invalid_pool_id());
        assert_error_message(&app, &path, "querystring/count must be >= 1").await;
    }

    #[tokio::test]
    async fn pools_blocks_bad_pagination() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();

        assert_error_message(
            &app,
            &format!("/pools/{pool}/blocks?order=a"),
            "querystring/order must be equal to one of the allowed values",
        )
        .await;
        assert_error_message(
            &app,
            &format!("/pools/{pool}/blocks?page=0"),
            "querystring/page must be >= 1",
        )
        .await;
        assert_error_message(
            &app,
            &format!("/pools/{pool}/blocks?count=101"),
            "querystring/count must be <= 100",
        )
        .await;
    }

    /// Blockfrost does not declare `from`/`to` for this route, so a malformed
    /// or reversed window changes nothing.
    #[tokio::test]
    async fn pools_blocks_ignores_from_to() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();

        let (status, plain) = app
            .get_bytes(&format!("/pools/{pool}/blocks?count=100"))
            .await;
        assert_eq!(status, StatusCode::OK);

        let expected: Vec<String> = serde_json::from_slice(&plain).unwrap();
        assert!(!expected.is_empty());

        for window in ["from=bad", "from=999999999&to=1", "from=abc&to=xyz"] {
            let (status, bytes) = app
                .get_bytes(&format!("/pools/{pool}/blocks?count=100&{window}"))
                .await;

            assert_eq!(
                status,
                StatusCode::OK,
                "unexpected status {status} for {window} with body: {}",
                String::from_utf8_lossy(&bytes)
            );

            let hashes: Vec<String> = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(hashes, expected, "{window} changed the body");
        }
    }

    #[tokio::test]
    async fn pools_blocks_not_found() {
        let app = TestApp::new();
        let pool = bech32_pool([0xff; 28]).expect("valid pool id");
        let path = format!("/pools/{pool}/blocks");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        for pool_id in wrong_length_pool_ids(&app) {
            let path = format!("/pools/{pool_id}/blocks");
            assert_status(&app, &path, StatusCode::NOT_FOUND).await;
        }
    }

    #[tokio::test]
    async fn pools_blocks_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let path = format!("/pools/{}/blocks", toy_issuer_pool());
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn pools_updates_happy_path() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/updates");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let updates: Vec<PoolUpdatesInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool updates");

        // The synthetic chain registers the pool. The list has one
        // registration update or more.
        assert!(!updates.is_empty());
        assert!(updates
            .iter()
            .any(|update| matches!(update.action, Action::Registered)));

        for update in &updates {
            assert!(!update.tx_hash.is_empty());
            assert!(update.cert_index >= 0);
        }
    }

    #[tokio::test]
    async fn pools_updates_order_desc_reverses_asc() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();

        let (asc_status, asc_bytes) = app
            .get_bytes(&format!("/pools/{pool_id}/updates?order=asc&count=100"))
            .await;
        let (desc_status, desc_bytes) = app
            .get_bytes(&format!("/pools/{pool_id}/updates?order=desc&count=100"))
            .await;

        assert_eq!(asc_status, StatusCode::OK);
        assert_eq!(desc_status, StatusCode::OK);

        let asc: Vec<PoolUpdatesInner> =
            serde_json::from_slice(&asc_bytes).expect("failed to parse asc updates");
        let mut desc: Vec<PoolUpdatesInner> =
            serde_json::from_slice(&desc_bytes).expect("failed to parse desc updates");

        desc.reverse();
        assert_eq!(asc, desc);
    }

    #[tokio::test]
    async fn pools_updates_bad_request() {
        let app = TestApp::new();
        let path = format!("/pools/{}/updates", invalid_pool_id());
        assert_error_message(&app, &path, "Invalid or malformed pool id format.").await;
    }

    #[tokio::test]
    async fn pools_updates_not_found() {
        let app = TestApp::new();
        let path = format!("/pools/{}/updates", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        for pool_id in wrong_length_pool_ids(&app) {
            let path = format!("/pools/{pool_id}/updates");
            assert_status(&app, &path, StatusCode::NOT_FOUND).await;
        }
    }

    #[tokio::test]
    async fn pools_updates_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/updates");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    const VOTING_POOL: [u8; 28] = [9u8; 28];

    fn synthetic_vote(
        voter: &Voter,
        block: usize,
        tx: usize,
        action: u32,
        vote: Vote,
    ) -> SyntheticVote {
        SyntheticVote {
            voter: voter.clone(),
            proposal: SyntheticProposalRef { block, tx, action },
            vote,
        }
    }

    /// Transaction 0 of block 0 proposes three actions. In block 1,
    /// transaction 0 contains a ballot of the pool on actions 2 and 0. The
    /// same transaction also contains the ballots of a DRep and of a second
    /// pool. In block 1, transaction 1 contains a ballot of the pool on
    /// action 1. In block 2, transaction 0 contains a new vote of the pool on
    /// action 0.
    ///
    /// Each ballot is a map that sorts its votes by governance action ID. As a
    /// result, in transaction 0 of block 1, the vote of the pool on action 0
    /// has `cert_index` 0. The vote of the pool on action 2 has `cert_index` 1.
    /// In that transaction, the ballot of the DRep is before the ballot of the
    /// pool. The index starts at 0 for each voter. Thus, the ballot of the
    /// DRep does not change the index.
    ///
    /// Transaction 0 is before transaction 1. Thus, in chain order, the vote
    /// of the pool on action 2 is before the vote of the pool on action 1.
    fn pool_votes_config() -> SyntheticBlockConfig {
        let pool = Voter::StakePoolKey(Hash::from(VOTING_POOL));
        let drep = Voter::DRepKey(Hash::from([7u8; 28]));
        let other_pool = Voter::StakePoolKey(Hash::from([5u8; 28]));

        SyntheticBlockConfig {
            pool_id: bech32_pool(VOTING_POOL).expect("Cannot encode the pool ID."),
            block_count: 3,
            txs_per_block: 2,
            gov_actions_by_block: vec![
                vec![vec![
                    GovAction::Information,
                    GovAction::Information,
                    GovAction::Information,
                ]],
                vec![],
                vec![],
            ],
            votes_by_block: vec![
                vec![],
                vec![
                    vec![
                        synthetic_vote(&drep, 0, 0, 0, Vote::No),
                        synthetic_vote(&pool, 0, 0, 2, Vote::Abstain),
                        synthetic_vote(&pool, 0, 0, 0, Vote::Yes),
                        synthetic_vote(&other_pool, 0, 0, 1, Vote::No),
                    ],
                    vec![synthetic_vote(&pool, 0, 0, 1, Vote::No)],
                ],
                vec![vec![synthetic_vote(&pool, 0, 0, 0, Vote::No)]],
            ],
            ..Default::default()
        }
    }

    async fn get_pool_votes(app: &TestApp, pool_id: &str, query: &str) -> Vec<PoolVotesInner> {
        let path = format!("/pools/{pool_id}/votes{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "The request to {path} returned status {status}. The response body was {}.",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("The pool vote response did not contain valid JSON.")
    }

    fn pool_vote_row(
        tx_hash: &str,
        cert_index: i32,
        vote: pool_votes_inner::Vote,
    ) -> PoolVotesInner {
        PoolVotesInner {
            tx_hash: tx_hash.to_string(),
            cert_index,
            vote,
        }
    }

    fn expected_pool_votes(app: &TestApp) -> Vec<PoolVotesInner> {
        let blocks = &app.vectors().blocks;

        vec![
            pool_vote_row(&blocks[1].tx_hashes[0], 0, pool_votes_inner::Vote::Yes),
            pool_vote_row(&blocks[1].tx_hashes[0], 1, pool_votes_inner::Vote::Abstain),
            pool_vote_row(&blocks[1].tx_hashes[1], 0, pool_votes_inner::Vote::No),
            pool_vote_row(&blocks[2].tx_hashes[0], 0, pool_votes_inner::Vote::No),
        ]
    }

    #[tokio::test]
    async fn pools_votes_happy_path() {
        let app = TestApp::new_with_cfg(pool_votes_config());
        let expected = expected_pool_votes(&app);

        let rows = get_pool_votes(&app, &app.vectors().pool_id, "").await;
        assert_eq!(rows, expected);

        let rows = get_pool_votes(&app, &hex::encode(VOTING_POOL), "").await;
        assert_eq!(rows, expected);
    }

    #[tokio::test]
    async fn pools_votes_orders_and_paginates() {
        let app = TestApp::new_with_cfg(pool_votes_config());
        let pool_id = app.vectors().pool_id.clone();
        let expected = expected_pool_votes(&app);
        let reversed = expected.iter().rev().cloned().collect_vec();

        assert_eq!(
            get_pool_votes(&app, &pool_id, "?order=desc").await,
            reversed
        );
        assert_eq!(
            get_pool_votes(&app, &pool_id, "?order=desc&count=3").await,
            reversed[..3]
        );
        assert_eq!(
            get_pool_votes(&app, &pool_id, "?count=2").await,
            expected[..2]
        );

        // If `count` is 2, page 1 contains two of the three votes of the pool
        // in block 1. The first vote on page 2 is the third of these votes.
        assert_eq!(
            get_pool_votes(&app, &pool_id, "?count=2&page=2").await,
            expected[2..]
        );
        assert_eq!(
            get_pool_votes(&app, &pool_id, "?count=1&page=2").await,
            expected[1..2]
        );
        assert!(get_pool_votes(&app, &pool_id, "?page=2").await.is_empty());
    }

    #[tokio::test]
    async fn pools_votes_without_votes() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.clone();

        assert!(get_pool_votes(&app, &pool_id, "").await.is_empty());
        assert!(get_pool_votes(&app, &pool_id, "?order=desc")
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn pools_votes_bad_request() {
        let app = TestApp::new_with_cfg(pool_votes_config());
        let path = format!("/pools/{}/votes", invalid_pool_id());
        assert_error_message(&app, &path, "Invalid or malformed pool id format.").await;

        // The endpoint does the query string check before the pool ID check.
        let path = format!("/pools/{}/votes?count=0", invalid_pool_id());
        assert_error_message(&app, &path, "querystring/count must be >= 1").await;

        let path = format!("/pools/{}/votes?order=sideways", missing_pool_id());
        assert_error_message(
            &app,
            &path,
            "querystring/order must be equal to one of the allowed values",
        )
        .await;
    }

    #[tokio::test]
    async fn pools_votes_not_found() {
        let app = TestApp::new_with_cfg(pool_votes_config());
        let path = format!("/pools/{}/votes", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        // Block 1 contains a vote of the second pool, but the second pool
        // never registered.
        let path = format!("/pools/{}/votes", hex::encode([5u8; 28]));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        for pool_id in wrong_length_pool_ids(&app) {
            let path = format!("/pools/{pool_id}/votes");
            assert_status(&app, &path, StatusCode::NOT_FOUND).await;
        }
    }

    #[tokio::test]
    async fn pools_votes_rejects_deep_page() {
        let app = TestApp::new_with_scan_limit(pool_votes_config(), 3);
        let path = format!("/pools/{}/votes?count=2&page=2", app.vectors().pool_id);
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn pools_votes_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/votes");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    const COUNT_MESSAGE: &str = "querystring/count must be >= 1";
    const POOL_ID_MESSAGE: &str = "Invalid or malformed pool id format.";
    const NOT_FOUND_MESSAGE: &str = "The requested component has not been found.";

    fn registered_pool_app() -> TestApp {
        TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: REG_POOL_ID.to_string(),
            ..Default::default()
        })
    }

    fn pool_id_case(label: &str) -> PoolIdCase {
        pool_id_cases()
            .into_iter()
            .find(|case| case.label == label)
            .expect("Cannot find the pool ID case.")
    }

    /// Gives a text for each difference from the Blockfrost error body.
    async fn error_mismatch(
        app: &TestApp,
        path: &str,
        status: StatusCode,
        message: &str,
    ) -> Option<String> {
        let (actual, bytes) = app.get_bytes(path).await;
        let expected = serde_json::json!({
            "status_code": status.as_u16(),
            "error": status.canonical_reason(),
            "message": message,
        });
        let body = serde_json::from_slice::<Value>(&bytes).ok();

        (actual != status || body.as_ref() != Some(&expected)).then(|| {
            format!(
                "{path}: expected {status} {message:?}, got {actual} {}",
                String::from_utf8_lossy(&bytes)
            )
        })
    }

    fn fee_log(total_rewards: u64, fixed_cost: u64, margin: Option<(u64, u64)>) -> StakeLog {
        StakeLog {
            total_rewards,
            fixed_cost,
            margin_cost: margin.map(|(numerator, denominator)| {
                pallas::ledger::primitives::RationalNumber {
                    numerator,
                    denominator,
                }
            }),
            ..Default::default()
        }
    }

    #[test]
    fn bf_fees_fall_back_to_the_rewards_below_the_fixed_cost() {
        assert_eq!(
            bf_compatible_fees(&fee_log(0, 340_000_000, Some((1, 100)))),
            0
        );
        assert_eq!(
            bf_compatible_fees(&fee_log(1_000, 340_000_000, Some((1, 100)))),
            1_000
        );
        assert_eq!(
            bf_compatible_fees(&fee_log(25_816_995_192, 340_000_000, Some((0, 1)))),
            340_000_000
        );
        assert_eq!(bf_compatible_fees(&fee_log(500, 100, None)), 100);
    }

    /// Blockfrost runs the formula in `float8`, and so floors one lovelace
    /// below the exact `floor(940189878034 * 3 / 11) = 256415421282` here.
    #[test]
    fn bf_fees_round_like_float8() {
        assert_eq!(
            bf_compatible_fees(&fee_log(940_189_878_034, 0, Some((3, 11)))),
            256_415_421_281
        );
        assert_eq!(
            bf_compatible_fees(&fee_log(905_252_471_403, 500_000_000, Some((3, 11)))),
            247_250_674_018
        );
    }

    /// The `Rational64` arithmetic this replaced cast the margin into `i64`, so
    /// a denominator near `u64::MAX` came out negative or overflowed.
    #[test]
    fn bf_fees_take_any_margin() {
        let fees = bf_compatible_fees(&fee_log(
            10_000_000_000_000,
            340_000_000,
            Some((u64::MAX / 4, u64::MAX - 1)),
        ));
        assert_eq!(fees, 340_000_000 + (10_000_000_000_000 - 340_000_000) / 4);

        assert_eq!(margin_f64(u64::MAX / 4, u64::MAX - 1), 0.25);
        assert_eq!(margin_f64(1, 0), 0.0);
    }

    #[tokio::test]
    async fn pools_history_happy_path() {
        let app = registered_pool_app();
        let path = format!("/pools/{REG_POOL_ID}/history");
        assert_status(&app, &path, StatusCode::OK).await;
    }

    #[tokio::test]
    async fn pools_history_not_found() {
        let app = registered_pool_app();
        let path = format!("/pools/{}/history", missing_pool_id());
        let mismatch = error_mismatch(&app, &path, StatusCode::NOT_FOUND, NOT_FOUND_MESSAGE).await;
        assert!(mismatch.is_none(), "{mismatch:?}");
    }

    #[tokio::test]
    async fn pool_routes_check_pagination_in_blockfrost_order() {
        let app = registered_pool_app();
        let mut mismatches = Vec::new();

        for label in ["invalid", "bmissing", "b29", "h58"] {
            let id = pool_id_case(label).path_id();

            for suffix in ["delegators", "updates", "votes"] {
                let path = format!("/pools/{id}/{suffix}?count=0");
                let mismatch =
                    error_mismatch(&app, &path, StatusCode::BAD_REQUEST, COUNT_MESSAGE).await;
                mismatches.extend(mismatch);
            }

            // The `/history` route parses the pool ID format before the pagination.
            // The 404 for a valid ID comes after the pagination.
            let message = if label == "invalid" {
                POOL_ID_MESSAGE
            } else {
                COUNT_MESSAGE
            };
            let path = format!("/pools/{id}/history?count=0");
            mismatches.extend(error_mismatch(&app, &path, StatusCode::BAD_REQUEST, message).await);
        }

        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    /// Gives a text for a difference from the Blockfrost response.
    /// The text contains the case label, not the path, because some IDs have
    /// thousands of characters.
    async fn pool_id_case_mismatch(
        app: &TestApp,
        case: &PoolIdCase,
        route: &str,
        expected: u16,
    ) -> Option<String> {
        let (status, bytes) = app.get_bytes(&route.replace("{id}", &case.path_id())).await;
        let expected_body = match expected {
            400 => Some(("Bad Request", POOL_ID_MESSAGE)),
            404 => Some(("Not Found", NOT_FOUND_MESSAGE)),
            _ => None,
        }
        .map(|(error, message)| {
            serde_json::json!({ "status_code": expected, "error": error, "message": message })
        });
        let body = serde_json::from_slice::<Value>(&bytes).ok();
        let matched =
            status.as_u16() == expected && (expected_body.is_none() || body == expected_body);

        (!matched).then(|| {
            format!(
                "{} {route}: expected {expected}, got {status} {}",
                case.label,
                String::from_utf8_lossy(&bytes)
            )
        })
    }

    #[tokio::test]
    async fn pool_routes_match_blockfrost_for_each_pool_id() {
        let app = registered_pool_app();
        let mut mismatches = Vec::new();

        for case in pool_id_cases() {
            for suffix in [
                "",
                "/metadata",
                "/relays",
                "/delegators",
                "/history",
                "/updates",
                "/votes",
            ] {
                let expected = match suffix {
                    "" | "/history" => case.unbounded_status,
                    _ => case.bounded_status,
                };
                let route = format!("/pools/{{id}}{suffix}");
                mismatches.extend(pool_id_case_mismatch(&app, &case, &route, expected).await);
            }
        }

        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    fn parse_mismatches(
        parse: fn(&str) -> Result<Option<PoolHash>, Error>,
        expected: fn(&PoolIdCase) -> u16,
    ) -> Vec<String> {
        let registered = PoolHash::from(
            <[u8; 28]>::try_from(hex::decode(REG_POOL_HEX).expect("Cannot decode the pool hex."))
                .expect("The pool hex does not have 28 bytes."),
        );

        pool_id_cases()
            .iter()
            .filter_map(|case| {
                let result = parse(&case.id);
                let matched = match expected(case) {
                    400 => matches!(result, Err(Error::InvalidPoolId)),
                    200 => matches!(result, Ok(Some(hash)) if hash == registered),
                    404 => matches!(result, Ok(ref hash) if *hash != Some(registered)),
                    status => panic!("Unknown status {status} for case {}.", case.label),
                };
                (!matched).then(|| {
                    format!(
                        "{}: expected {}, got {result:?}.",
                        case.label,
                        expected(case)
                    )
                })
            })
            .collect()
    }

    #[test]
    fn parse_pool_id_unbounded_matches_blockfrost() {
        let mismatches = parse_mismatches(parse_pool_id_unbounded, |case| case.unbounded_status);
        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    #[test]
    fn parse_pool_id_bounded_matches_blockfrost() {
        let mismatches = parse_mismatches(parse_pool_id_bounded, |case| case.bounded_status);
        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    #[test]
    fn pool_id_cases_have_expected_lengths() {
        let cases = pool_id_cases();
        let expected = [
            ("b1000", 1000),
            ("b1001", 1001),
            ("b1002", 1002),
            ("b1023", 1023),
            ("b1024", 1024),
            ("b5000", 5007),
        ];

        for (label, len) in expected {
            let case = cases
                .iter()
                .find(|case| case.label == label)
                .expect("Cannot find the case.");
            assert_eq!(case.id.chars().count(), len, "Wrong length for {label}.");
        }
    }

    #[test]
    fn pool_id_case_paths_are_ascii() {
        for case in pool_id_cases() {
            assert!(
                case.path_id().is_ascii(),
                "The path for {} is not ASCII.",
                case.label
            );
        }
    }

    #[test]
    fn pool_id_encoder_matches_bech32_crate() {
        let registered = hex::decode(REG_POOL_HEX).expect("Cannot decode the pool hex.");
        let encode = |bytes: &[u8], constant| {
            bech32_encode_values("pool", &bech32_values_from_bytes(bytes), constant)
        };

        assert_eq!(REG_POOL_ID, encode(&registered, 1));
        assert_eq!(
            bech32_pool(&registered).expect("Cannot encode the pool ID."),
            encode(&registered, 1)
        );
        assert_eq!(
            bech32_pool([9u8; 28]).expect("Cannot encode the pool ID."),
            encode(&[9u8; 28], 1)
        );

        let hrp = bech32::Hrp::parse("pool").expect("Cannot parse the HRP.");
        assert_eq!(
            bech32::encode::<bech32::Bech32m>(hrp, &registered)
                .expect("Cannot encode the Bech32m ID."),
            encode(&registered, 0x2bc8_30a3)
        );
    }
}
