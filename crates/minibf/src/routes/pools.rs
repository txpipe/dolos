use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
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
    final_rows::newest_final_row,
    indexes::{AsyncCardanoQueryExt, CardanoArchiveIndexExt, SlotOrder},
    model::{AccountState, CalidusRegistration, PoolState},
    pallas_extras, pool_history, AccountKey, PoolEpochLog, PoolHash, PoolKey,
};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain, StateStore as _};
use futures::{future::join_all, StreamExt};
use itertools::Itertools;
use pallas::{
    crypto::hash::Hasher,
    ledger::{
        addresses::Network,
        primitives::{
            conway::{Vote, Voter},
            Epoch, StakeCredential,
        },
        traverse::MultiEraBlock,
    },
};
use serde::Serialize;

use crate::{
    error::Error,
    log_and_500,
    mapping::{
        bech32_calidus, bech32_pool, i32_or_500, pool_offchain_body, pool_offchain_metadata,
        rational_to_f64, stake_cred_to_address, vkey_to_stake_address, IntoModel,
        OffchainFetchError, MAX_POOL_METADATA_BYTES,
    },
    pagination::{Pagination, PaginationParameters},
    routes::governance::voter_casts,
    scan::{EntityScan, ENTITY_CHUNK},
    Facade,
};

type ActivePools = HashSet<PoolHash>;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct PoolMetrics {
    live_stake: u64,
    live_delegators: u64,
    total_live_stake: u64,
    live_pledge: u64,
}

impl PoolMetrics {
    fn merge(mut self, other: Self) -> Self {
        self.live_stake += other.live_stake;
        self.live_delegators += other.live_delegators;
        self.total_live_stake += other.total_live_stake;
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

fn reduce_account_chunk(
    accounts: &[AccountState],
    target_pool: PoolHash,
    active_pools: &ActivePools,
) -> PoolMetrics {
    accounts
        .iter()
        .fold(PoolMetrics::default(), |mut metrics, account| {
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

            metrics
        })
}

/// The pools whose live snapshot is not retired, `chunk` pools per blocking
/// task.
async fn load_active_pools<D>(domain: &Facade<D>, chunk: usize) -> Result<ActivePools, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let active = EntityScan::<PoolState>::all(chunk)
        .filter_map(domain, |_, pool| {
            pool.snapshot
                .live()
                .is_some_and(|snapshot| !snapshot.is_retired)
                .then_some(pool.operator)
        })
        .await?;

    Ok(active.into_iter().collect())
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
        let key = AccountKey(StakeCredential::AddrKeyhash(*owner));

        let Some(account) = domain.read_cardano_entity::<AccountState>(&key)? else {
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

/// The live stake metrics of `target_pool`, from a scan of every account,
/// `chunk` accounts per blocking task.
async fn compute_pool_metrics<D>(
    domain: &Facade<D>,
    target_pool: PoolHash,
    active_pools: ActivePools,
    owners: Vec<pallas::crypto::hash::Hash<28>>,
    chunk: usize,
) -> Result<PoolMetrics, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AccountState>: From<D::Entity>,
{
    let active_pools = Arc::new(active_pools);

    let mut metrics = EntityScan::<AccountState>::all(chunk)
        .fold(domain, PoolMetrics::default(), move |metrics, accounts| {
            let accounts: Vec<_> = accounts.into_iter().map(|(_, account)| account).collect();
            Ok(metrics.merge(reduce_account_chunk(&accounts, target_pool, &active_pools)))
        })
        .await?;

    metrics.live_pledge = domain
        .blocking(move |domain| compute_live_pledge(&domain, target_pool, &owners))
        .await?;

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
/// A pool is keyed by exactly its 28-byte hash, so the parsers return a
/// `PoolHash` rather than the bytes they decoded.
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

/// The `calidus_key` answer for the registration the pool's state carries.
/// Height, time and epoch come from the registering block, found by slot. A
/// revocation, a nonce the answer's `i32` cannot hold, or a block the archive
/// no longer holds, answers `None`.
fn pool_calidus_key<D>(
    domain: &Facade<D>,
    registration: &CalidusRegistration,
) -> Result<Option<PoolCalidusKey>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let Some(pub_key) = registration.pub_key else {
        return Ok(None);
    };

    let Ok(nonce) = registration.nonce.try_into() else {
        return Ok(None);
    };

    let Some(body) = domain
        .archive()
        .get_block_by_slot(&registration.slot)
        .map_err(log_and_500("failed to read a calidus registration block"))?
    else {
        return Ok(None);
    };

    let block = MultiEraBlock::decode(&body)
        .map_err(log_and_500("failed to decode a calidus registration block"))?;
    let chain = domain.get_chain_summary()?;

    Ok(Some(PoolCalidusKey {
        id: bech32_calidus(cip151::calidus_key_id_bytes(&pub_key))?,
        pub_key: hex::encode(pub_key),
        nonce,
        tx_hash: registration.tx_hash.to_string(),
        block_height: block
            .number()
            .try_into()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        block_time: chain
            .slot_time(registration.slot)
            .try_into()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        epoch: chain
            .slot_epoch(registration.slot)
            .0
            .try_into()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    }))
}

/// The retiring epoch the pool's newest certificate in a valid transaction
/// names, `None` when that certificate registers the pool or the archive holds
/// none of its certificates.
fn newest_retirement<D: Domain>(domain: &D, pool: &PoolHash) -> Result<Option<Epoch>, StatusCode> {
    let archive = domain.archive();

    let slot = archive
        .slots_by_pool_certs(pool.as_slice(), 0, BlockSlot::MAX)
        .map_err(log_and_500("failed to read the pool certs index"))?
        .next_back()
        .transpose()
        .map_err(log_and_500("failed to read the pool certs index"))?;

    let Some(slot) = slot else {
        return Ok(None);
    };

    let Some(body) = archive
        .get_block_by_slot(&slot)
        .map_err(log_and_500("failed to read a pool certs block"))?
    else {
        return Ok(None);
    };

    let block =
        MultiEraBlock::decode(&body).map_err(log_and_500("failed to decode a pool certs block"))?;

    let mut newest = None;

    for tx in block.txs().iter().filter(|tx| tx.is_valid()) {
        for cert in tx.certs() {
            if pallas_extras::cert_as_pool_registration(&cert)
                .is_some_and(|cert| &cert.operator == pool)
            {
                newest = None;
            }

            if let Some(cert) =
                pallas_extras::cert_as_pool_retirement(&cert).filter(|cert| &cert.operator == pool)
            {
                newest = Some(cert.epoch);
            }
        }
    }

    Ok(newest)
}

/// A pool's row: the state row, then its final row in the log.
///
/// ESTART commits a final row before it deletes the state row, so a state
/// miss read first finds a moved pool in the log. The final row is keyed at
/// the epoch [`PoolState::final_row_epoch`] derives from the retirement the
/// pool's newest certificate makes. Where that misses, a pruned archive
/// without the certificate for one, the log is scanned for the newest row.
pub(crate) fn read_pool_row<D: Domain>(
    domain: &D,
    pool: PoolHash,
) -> Result<Option<PoolState>, StatusCode> {
    let key = PoolKey(pool);

    let live = domain
        .state()
        .read_entity_typed::<PoolState>(&key)
        .map_err(log_and_500("failed to read pool state"))?;

    if live.is_some() {
        return Ok(live);
    }

    if let Some(retiring) = newest_retirement(domain, &pool)? {
        let chain = dolos_cardano::eras::load_era_summary::<D>(domain.state())
            .map_err(log_and_500("failed to load era summary"))?;
        let slot = chain.epoch_start(PoolState::final_row_epoch(retiring));

        let logged = domain
            .archive()
            .read_log_typed::<PoolState>(slot, &key)
            .map_err(log_and_500("failed to read the pools log"))?;

        if logged.is_some() {
            return Ok(logged);
        }
    }

    let logged = newest_final_row::<PoolState>(domain.archive(), &key)
        .map_err(log_and_500("failed to read the pools log"))?;

    Ok(logged.map(|(_, row)| row))
}

/// Whether the pool ever registered: it has a row in state or in the log.
pub(crate) fn pool_exists<D: Domain>(domain: &D, pool: PoolHash) -> Result<bool, StatusCode> {
    Ok(read_pool_row(domain, pool)?.is_some())
}

/// The newest final row of every pool in the log, in no particular order.
fn logged_pools<D: Domain>(domain: &D) -> Result<Vec<PoolState>, StatusCode> {
    let rows = domain
        .archive()
        .iter_logs_typed::<PoolState>(None)
        .map_err(log_and_500("failed to iterate the pools log"))?;

    let mut newest = HashMap::new();

    // the log is keyed slot first, so a later row of a pool replaces an
    // earlier one
    for row in rows {
        let (_, key, row) = row.map_err(log_and_500("failed to read the pools log"))?;
        newest.insert(key, row);
    }

    Ok(newest.into_values().collect())
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
    let pool = read_pool_row(&domain.inner, hash)?.ok_or(StatusCode::NOT_FOUND)?;

    let snapshot = pool
        .snapshot
        .live()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let params = &snapshot.params;

    let network = domain.get_network_id()?;
    let epoch = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let circulating_supply = epoch.initial_pots.circulating();
    let optimal = domain
        .get_current_effective_pparams()?
        .ensure_k()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // The active stake is the distribution ESTART froze two epochs back,
    // which `set` holds, against the epoch's total of it.
    let active_stake = pool
        .snapshot
        .set()
        .and_then(|x| x.stake)
        .unwrap_or_default();
    let total_active_stake = epoch.active_stake.unwrap_or_default();

    let active_pools = load_active_pools(&domain, ENTITY_CHUNK).await?;
    let owners = params.pool_owners.clone();
    let metrics =
        compute_pool_metrics(&domain, pool.operator, active_pools, owners, ENTITY_CHUNK).await?;

    let (registration, retirement) = load_pool_cert_hashes(&domain, pool.operator).await?;

    let chain = domain.get_chain_summary()?;
    let blocks_minted = pool_history::blocks_minted(&domain.inner, &chain, vec![pool.clone()])
        .await
        .map_err(log_and_500("failed to derive a pool's lifetime blocks"))?
        .pop()
        .unwrap_or_default();
    let calidus_key = match &pool.calidus {
        Some(registration) => pool_calidus_key(&domain, registration)?,
        None => None,
    };

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
        blocks_minted: blocks_minted as i32,
        blocks_epoch: snapshot.blocks_minted as i32,
        live_stake: metrics.live_stake.to_string(),
        live_size: safe_ratio(metrics.live_stake, metrics.total_live_stake),
        live_saturation: if circulating_supply == 0 {
            0.0
        } else {
            metrics.live_stake as f64 * optimal as f64 / circulating_supply as f64
        },
        live_delegators: metrics.live_delegators as f64,
        active_stake: active_stake.to_string(),
        active_size: safe_ratio(active_stake, total_active_stake),
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

fn size_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        Code::SizeExceeded,
        format!(
            "Error Offchain Pool: Size error, fetching metadata from {url} exceeded {MAX_POOL_METADATA_BYTES} bytes."
        ),
    )
}

fn pool_fetch_error(url: &str, error: OffchainFetchError) -> Option<DrepsInnerMetadataError> {
    match error {
        OffchainFetchError::Client | OffchainFetchError::Body => None,
        OffchainFetchError::Connection => Some(connection_error(url)),
        OffchainFetchError::HttpResponse(status) => Some(http_response_error(url, status)),
        OffchainFetchError::SizeExceeded => Some(size_error(url)),
    }
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

    let body = match pool_offchain_body(&metadata.url).await {
        Ok(body) => body,
        Err(error) => return (None, pool_fetch_error(&metadata.url, error)),
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

/// The listing order of `/pools` and `/pools/extended`: the slot of each
/// pool's first registration, as db-sync's `pool_hash.id` orders them, the
/// operator breaking ties. A pool the Shelley genesis stakes lists first, and
/// one whose registration the archive no longer holds last.
async fn listing_order<D: Domain>(
    domain: &Facade<D>,
    pools: &[PoolHash],
) -> Result<Vec<(BlockSlot, PoolHash)>, Error> {
    let positions = pool_history::first_registrations(&domain.inner, pools.to_vec())
        .await
        .map_err(log_and_500("failed to read the pools' first registrations"))?;

    Ok(positions
        .into_iter()
        .zip(pools)
        .map(|(at, operator)| (at.map_or(BlockSlot::MAX, |(slot, ..)| slot), *operator))
        .collect())
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

pub async fn all<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let pools: Vec<PoolHash> = load_listed_pools(&domain, ENTITY_CHUNK)
        .await?
        .into_iter()
        .map(|(_, pool)| pool.operator)
        .collect();

    let pools = listing_order(&domain, &pools).await?;

    let out = select_pools(pools, &pagination)
        .into_iter()
        .map(bech32_pool)
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
}

/// The live stake of every pool, from a scan of every account, `chunk`
/// accounts per blocking task.
async fn load_live_stakes<D>(
    domain: &Facade<D>,
    chunk: usize,
) -> Result<HashMap<PoolHash, u64>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AccountState>: From<D::Entity>,
{
    EntityScan::<AccountState>::all(chunk)
        .fold(domain, HashMap::new(), |mut stakes, accounts| {
            for (_, state) in &accounts {
                if let Some(hash) = live_stake_pool(state) {
                    *stakes.entry(hash).or_default() += state.live_stake();
                }
            }
            Ok(stakes)
        })
        .await
}

/// The pools whose live snapshot is not retired, in key order, `chunk` pools
/// per blocking task.
async fn load_listed_pools<D>(
    domain: &Facade<D>,
    chunk: usize,
) -> Result<Vec<(PoolKey, PoolState)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    EntityScan::<PoolState>::all(chunk)
        .filter_map(domain, |key, state| {
            if state.snapshot.live().map(|x| x.is_retired).unwrap_or(false) {
                return None;
            }
            Some((key, state))
        })
        .await
}

pub async fn all_extended<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListExtendedInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
    Option<AccountState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let live_stake_map = load_live_stakes(&domain, ENTITY_CHUNK).await?;

    let circulating_supply = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .initial_pots
        .circulating();
    let optimal = domain
        .get_current_effective_pparams()?
        .ensure_k()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut listed: HashMap<PoolHash, (PoolKey, PoolState)> =
        load_listed_pools(&domain, ENTITY_CHUNK)
            .await?
            .into_iter()
            .map(|(key, pool)| (pool.operator, (key, pool)))
            .collect();

    let operators: Vec<PoolHash> = listed.keys().copied().collect();
    let order = listing_order(&domain, &operators).await?;

    let pools = select_pools(order, &pagination)
        .into_iter()
        .filter_map(|operator| listed.remove(&operator))
        .collect_vec();

    let chain = domain.get_chain_summary()?;
    let blocks_minted = pool_history::blocks_minted(
        &domain.inner,
        &chain,
        pools.iter().map(|(_, pool)| pool.clone()).collect(),
    )
    .await
    .map_err(log_and_500("failed to derive the pools' lifetime blocks"))?;

    let metadata_futures: Vec<_> = pools
        .iter()
        .map(|(_, pool)| fetch_pool_offchain_metadata(pool))
        .collect();

    let metadata_results = join_all(metadata_futures).await;

    let mut out = vec![];

    for (((_, pool), fetched_metadata), blocks_minted) in
        pools.into_iter().zip(metadata_results).zip(blocks_minted)
    {
        let poolhex = hex::encode(pool.operator);
        let pool_id = bech32_pool(pool.operator)?;
        let params = pool.snapshot.live().map(|x| x.params.clone());
        let metadata = params
            .as_ref()
            .and_then(|x| build_pool_extended_metadata(x.pool_metadata.as_ref(), fetched_metadata));

        let live = live_stake_map.get(&pool.operator).copied();
        let active = pool.snapshot.set().and_then(|x| x.stake);

        out.push(PoolListExtendedInner {
            pool_id,
            hex: poolhex,
            live_stake: live.map(|x| x.to_string()).unwrap_or("0".to_string()),
            active_stake: active.map(|x| x.to_string()).unwrap_or("0".to_string()),
            live_saturation: live
                .map(|x| x as f64 * optimal as f64 / circulating_supply as f64)
                .unwrap_or_default(),
            blocks_minted: blocks_minted as i32,
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

/// The place of the latest retirement certificate of a pool on chain: the
/// slot, the transaction index in the block, and the certificate index in the
/// transaction.
type RetirementPosition = (BlockSlot, usize, usize);

fn retiring_candidates(
    pools: impl IntoIterator<Item = PoolState>,
    current_epoch: u64,
) -> Vec<(u64, PoolHash)> {
    pools
        .into_iter()
        .filter_map(|pool| {
            let retiring_epoch = pool.retiring_epoch?;
            (retiring_epoch > current_epoch).then_some((retiring_epoch, pool.operator))
        })
        .collect()
}

fn retired_candidates(pools: impl IntoIterator<Item = PoolState>) -> Vec<(u64, PoolHash)> {
    pools
        .into_iter()
        .filter_map(|pool| {
            let is_retired = pool.snapshot.live().map(|x| x.is_retired).unwrap_or(false);
            let retiring_epoch = pool.retiring_epoch?;
            is_retired.then_some((retiring_epoch, pool.operator))
        })
        .collect()
}

/// Returns the epochs that the page touches and that have more than one pool.
/// Only the pools of these epochs need a retirement position.
fn tied_epochs_on_page(list: &[(u64, PoolHash)], pagination: &Pagination) -> HashSet<u64> {
    let mut epochs: Vec<u64> = list.iter().map(|(epoch, _)| *epoch).collect();
    epochs.sort_unstable();

    if matches!(pagination.order, crate::pagination::Order::Desc) {
        epochs.reverse();
    }

    let sizes = epochs.iter().counts();

    epochs
        .iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .filter(|epoch| sizes[*epoch] > 1)
        .copied()
        .collect()
}

/// Reads the retirement position of each pool in `pools`.
///
/// A registration cancels an earlier retirement. So the newest certificate of
/// a retiring or retired pool is its latest retirement. The pool certificate
/// index gives the slot of that certificate without a block read. Pools with
/// the same slot also need the transaction index, so the function reads that
/// block once.
///
/// The lookup has no upper slot bound. So it also finds a certificate from a
/// block that the state store applied after the request started.
fn retirement_positions<D: Domain>(
    domain: &D,
    pools: &[PoolHash],
) -> Result<HashMap<PoolHash, RetirementPosition>, StatusCode> {
    let archive = domain.archive();
    let mut by_slot: HashMap<BlockSlot, Vec<PoolHash>> = HashMap::new();

    for pool in pools {
        let slot = archive
            .slots_by_pool_certs(pool.as_slice(), 0, BlockSlot::MAX)
            .map_err(log_and_500("failed to read the pool certs index"))?
            .next_back()
            .transpose()
            .map_err(log_and_500("failed to read the pool certs index"))?;

        // A pruned archive can lack the certificate. Then the pool gets no
        // position.
        if let Some(slot) = slot {
            by_slot.entry(slot).or_default().push(*pool);
        }
    }

    let mut out = HashMap::new();

    for (slot, shared) in by_slot {
        if let [pool] = shared.as_slice() {
            out.insert(*pool, (slot, 0, 0));
            continue;
        }

        let Some(body) = archive
            .get_block_by_slot(&slot)
            .map_err(log_and_500("failed to read a pool certs block"))?
        else {
            continue;
        };

        let block = MultiEraBlock::decode(&body)
            .map_err(log_and_500("failed to decode a pool certs block"))?;

        for (tx_index, tx) in block.txs().iter().enumerate() {
            for (cert_index, cert) in tx.certs().iter().enumerate() {
                let Some(cert) = pallas_extras::cert_as_pool_retirement(cert) else {
                    continue;
                };

                // A later certificate replaces an earlier one. So the map
                // keeps the latest retirement in the block.
                if shared.contains(&cert.operator) {
                    out.insert(cert.operator, (slot, tx_index, cert_index));
                }
            }
        }
    }

    Ok(out)
}

/// Sorts the list by epoch, then by retirement position, then by operator.
/// Returns one page of the result.
///
/// A pool without a position counts as the oldest retirement in its epoch.
/// A pool gets no position when a pruned archive lacks its certificate. Then
/// its retirement is older than the kept history.
fn order_page(
    list: Vec<(u64, PoolHash)>,
    positions: &HashMap<PoolHash, RetirementPosition>,
    pagination: &Pagination,
) -> Vec<(u64, PoolHash)> {
    let mut keyed: Vec<(u64, Option<RetirementPosition>, PoolHash)> = list
        .into_iter()
        .map(|(epoch, pool)| (epoch, positions.get(&pool).copied(), pool))
        .collect();

    keyed.sort_unstable();

    if matches!(pagination.order, crate::pagination::Order::Desc) {
        keyed.reverse();
    }

    keyed
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(epoch, _, pool)| (epoch, pool))
        .collect()
}

/// Orders the list like Blockfrost and returns one page.
///
/// Blockfrost sorts the pools by retirement epoch. Inside one epoch, it sorts
/// them by the transaction of the latest retirement certificate. Blockfrost
/// gives no order to pools that retire in one transaction. Here the
/// certificate index orders them.
fn page_retirements<D: Domain>(
    domain: &D,
    list: Vec<(u64, PoolHash)>,
    pagination: &Pagination,
) -> Result<Vec<(u64, PoolHash)>, StatusCode> {
    let tied = tied_epochs_on_page(&list, pagination);

    let pools: Vec<PoolHash> = list
        .iter()
        .filter(|(epoch, _)| tied.contains(epoch))
        .map(|(_, pool)| *pool)
        .collect();

    let positions = retirement_positions(domain, &pools)?;

    Ok(order_page(list, &positions, pagination))
}

fn retire_list_model(list: Vec<(u64, PoolHash)>) -> Result<Vec<PoolListRetireInner>, StatusCode> {
    list.into_iter()
        .map(|(retiring_epoch, operator)| {
            Ok(PoolListRetireInner {
                pool_id: bech32_pool(operator)?,
                epoch: retiring_epoch as i32,
            })
        })
        .collect()
}

/// Reads every pool state and returns one page of the pools that `select`
/// keeps. With `logged`, the newest final row of each pool the log holds and
/// state does not joins them: a pool live again is read from state only.
///
/// The pool states are read in chunks on the blocking pool. The page then
/// reads the log and the archive in one more blocking task. State is read
/// first, so a pool that moves between the two reads is in both, and is read
/// from state.
async fn load_retirement_page<D, F>(
    domain: Facade<D>,
    pagination: Pagination,
    logged: bool,
    select: F,
) -> Result<Vec<PoolListRetireInner>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
    F: FnOnce(Vec<PoolState>) -> Vec<(u64, PoolHash)> + Send + 'static,
{
    let mut pools = EntityScan::<PoolState>::all(ENTITY_CHUNK)
        .filter_map(&domain, |_, pool| Some(pool))
        .await?;

    domain
        .blocking(move |domain| {
            if logged {
                let live: HashSet<PoolHash> = pools.iter().map(|pool| pool.operator).collect();

                pools.extend(
                    logged_pools(&domain.inner)?
                        .into_iter()
                        .filter(|pool| !live.contains(&pool.operator)),
                );
            }

            let page = page_retirements(&domain.inner, select(pools), &pagination)?;

            retire_list_model(page)
        })
        .await
}

pub async fn all_retiring<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListRetireInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (current_epoch, _) = summary.slot_epoch(tip);

    // a pool in the log retired epochs ago, so only state holds retiring ones
    let out = load_retirement_page(domain, pagination, false, move |pools| {
        retiring_candidates(pools, current_epoch)
    })
    .await?;

    Ok(Json(out))
}

pub async fn all_retired<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListRetireInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let out = load_retirement_page(domain, pagination, true, retired_candidates).await?;

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
    let pool = read_pool_row(&domain.inner, hash)?.ok_or(StatusCode::NOT_FOUND)?;

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
    let pool = read_pool_row(&domain.inner, hash)?.ok_or(StatusCode::NOT_FOUND)?;

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

/// Pool block index slots read per blocking task while a page is located.
const POOL_BLOCKS_CHUNK: usize = 4096;

/// The slots of one page of the blocks `pool` minted up to `tip`, and whether
/// the pool minted any. The index is read `chunk` slots per blocking task; a
/// pool tags at most one block per slot, so each chunk resumes past the last
/// slot it read.
async fn pool_block_page_slots<D>(
    domain: &Facade<D>,
    pool: PoolHash,
    tip: BlockSlot,
    order: crate::pagination::Order,
    skip: usize,
    count: usize,
    chunk: usize,
) -> Result<(Vec<BlockSlot>, bool), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let (mut start, mut end) = (0, tip);
    let mut seen = 0usize;
    let mut page = Vec::new();
    let mut minted_any = false;

    loop {
        // Never read past the page.
        let want = (skip + count).saturating_sub(seen).clamp(1, chunk.max(1));

        let slots = domain
            .blocking(move |domain| {
                let slots = domain
                    .archive()
                    .slots_by_pool_blocks(pool.as_slice(), start, end)
                    .map_err(log_and_500("failed to read the pool blocks index"))?;

                match order {
                    crate::pagination::Order::Asc => {
                        slots.take(want).collect::<Result<Vec<_>, _>>()
                    }
                    crate::pagination::Order::Desc => {
                        slots.rev().take(want).collect::<Result<Vec<_>, _>>()
                    }
                }
                .map_err(log_and_500("failed to page the pool blocks index"))
            })
            .await?;

        minted_any |= !slots.is_empty();

        for slot in &slots {
            if seen >= skip && page.len() < count {
                page.push(*slot);
            }
            seen += 1;
        }

        let Some(&last) = slots.last() else {
            break;
        };

        if slots.len() < want || page.len() >= count {
            break;
        }

        match order {
            crate::pagination::Order::Asc => start = last + 1,
            crate::pagination::Order::Desc if last == 0 => break,
            crate::pagination::Order::Desc => end = last - 1,
        }
    }

    Ok((page, minted_any))
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

    let (page_slots, minted_any) = pool_block_page_slots(
        &domain,
        pool,
        tip,
        pagination.order,
        pagination.skip(),
        pagination.count,
        POOL_BLOCKS_CHUNK,
    )
    .await?;

    let page = domain
        .blocking(move |domain| {
            let mut page = Vec::with_capacity(page_slots.len());

            for slot in page_slots {
                let body = domain
                    .archive()
                    .get_block_by_slot(&slot)
                    .map_err(log_and_500("failed to read a block of a pool"))?;

                // A tagged slot always holds a block, and a Byron block is
                // never tagged. Skip either, rather than fail the page.
                let Some(body) = body else {
                    tracing::warn!(slot, "pool blocks index points at a missing block");
                    continue;
                };

                let Some(header) = crate::mapping::blocks::decode_block_header(&body)? else {
                    tracing::warn!(slot, "pool blocks index points at a Byron block");
                    continue;
                };

                page.push(header.hash().to_string());
            }

            Ok(page)
        })
        .await?;

    // Blockfrost 404s a pool that db-sync never saw. A `PoolState` row, in
    // state or in the final-row log, covers every pool that registered on
    // chain, and a pool must register before it can mint. The index acts as a second proof of existence, for
    // any issuer that has no entity.
    if !minted_any && !pool_exists(&domain.inner, pool)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    Ok(Json(page))
}

/// One page of the accounts whose live stake goes to `pool`, in key order,
/// `chunk` accounts per blocking task. The scan stops once the page is full.
async fn load_delegators_page<D>(
    domain: &Facade<D>,
    pool: PoolHash,
    skip: usize,
    count: usize,
    chunk: usize,
) -> Result<Vec<(AccountKey, AccountState)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AccountState>: From<D::Entity>,
{
    let mut accounts = EntityScan::<AccountState>::all(chunk);
    let mut seen = 0usize;
    let mut page = Vec::new();

    while page.len() < count {
        let Some(delegators) = accounts
            .next(domain, move |accounts| {
                accounts
                    .into_iter()
                    .filter(|(_, account)| {
                        account
                            .delegated_pool_live()
                            .or(account.retired_pool.as_ref())
                            .is_some_and(|f| *f == pool)
                    })
                    .collect::<Vec<_>>()
            })
            .await?
        else {
            break;
        };

        for delegator in delegators {
            if seen >= skip && page.len() < count {
                page.push(delegator);
            }
            seen += 1;
        }
    }

    Ok(page)
}

pub async fn by_id_delegators<D>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolDelegatorsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AccountState>: From<D::Entity>,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let Some(hash) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    if !pool_exists(&domain.inner, hash)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let network = domain.get_network_id()?;

    let page = load_delegators_page(
        &domain,
        hash,
        pagination.skip(),
        pagination.count,
        ENTITY_CHUNK,
    )
    .await?;

    let mapped: Vec<_> = page
        .into_iter()
        .map(|(delegator, account)| {
            let delegator = StakeCredential::from(delegator);

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
/// double. Larger ones go through a decimal expansion, which `parse` rounds
/// correctly as long as the cut leaves the quotient on its side of every
/// midpoint between doubles. 64 decimals do: a quotient that is a midpoint
/// has a power-of-two denominator below 2^64 and ends within 63 decimals, and
/// one that is not lies more than 2^-181 from any midpoint.
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

    for _ in 0..64 {
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
fn bf_compatible_fees(log: &PoolEpochLog) -> u64 {
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
    if !pool_exists(&domain.inner, hash)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (epoch, _) = summary.slot_epoch(tip);

    let mut entries = domain
        .iter_cardano_logs_per_epoch::<PoolEpochLog>(&PoolKey(hash), 0..epoch)
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

    if !pool_exists(&domain.inner, pool)? {
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

    if !pool_exists(&domain.inner, pool)? {
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
    use crate::pagination::Order;
    use crate::test_support::{
        bech32_encode_values, bech32_values_from_bytes, health_beside, pool_id_cases,
        replicated_app, PoolIdCase, TestApp, TestFault, REG_POOL_HEX, REG_POOL_ID, SLOW_READ,
    };
    use blockfrost_openapi::models::{
        block_content::BlockContent, pool::Pool, pool_delegators_inner::PoolDelegatorsInner,
        pool_list_extended_inner::PoolListExtendedInner,
        pool_list_retire_inner::PoolListRetireInner,
    };
    use dolos_cardano::model::{
        DRepDelegation, EpochValue, PoolDelegation, PoolParams, PoolSnapshot, Stake,
    };
    use dolos_core::StateWriter as _;
    use dolos_testing::synthetic::{SyntheticBlockConfig, SyntheticProposalRef, SyntheticVote};
    use pallas::{
        codec::utils::Bytes,
        crypto::hash::Hash,
        ledger::primitives::{alonzo, conway::GovAction, PoolMetadata, Relay, StakeCredential},
    };
    use serde_json::Value;
    use std::{io::ErrorKind, net::TcpListener};

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

    /// The cold key of the pool the calidus tests register.
    fn calidus_cold_key() -> pallas::crypto::key::ed25519::SecretKey {
        dolos_testing::cip151::cold_key(9)
    }

    fn calidus_pool_id() -> String {
        dolos_testing::cip151::pool_bech32(&calidus_cold_key())
    }

    /// A CIP-151 registration of `calidus_key` for the calidus tests' pool,
    /// signed by its cold key.
    fn cip151_registration(nonce: u64, calidus_key: [u8; 32]) -> Vec<(u64, alonzo::Metadatum)> {
        dolos_testing::cip151::signed_registration(&calidus_cold_key(), nonce, calidus_key)
    }

    /// Every synthetic block carries the same registration in its first
    /// transaction; the first one holds, since a later one with the same
    /// nonce does not exceed it. The chain starts at slot 500,000, so the
    /// registering block's slot and height differ.
    #[tokio::test]
    async fn pool_by_id_returns_calidus_key() {
        let default_cfg = SyntheticBlockConfig {
            pool_id: calidus_pool_id(),
            slot: 500_000,
            ..Default::default()
        };
        let pool_id = default_cfg.pool_id.clone();
        let calidus_pub_key = [0x57; 32];
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            metadata_entries: cip151_registration(12345, calidus_pub_key),
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

        let registering = &app.vectors().blocks[0];
        assert_ne!(registering.slot, registering.block_number);
        assert_eq!(calidus.tx_hash, registering.tx_hashes[0]);

        let (status, bytes) = app
            .get_bytes(&format!("/blocks/{}", registering.block_hash))
            .await;
        assert_eq!(status, StatusCode::OK);
        let block: BlockContent = serde_json::from_slice(&bytes).expect("block response");
        assert_eq!(Some(calidus.block_height), block.height);
        assert_eq!(calidus.block_time, block.time);
        assert_eq!(Some(calidus.epoch), block.epoch);
    }

    /// A registration whose block the archive no longer holds is served as
    /// no key, not as an error.
    #[tokio::test]
    async fn pool_by_id_serves_no_calidus_key_without_its_block() {
        let app = TestApp::new_with_cfg_and_setup(
            SyntheticBlockConfig {
                pool_id: calidus_pool_id(),
                ..Default::default()
            },
            |domain, vectors| {
                let operator = dolos_testing::cip151::pool_id(&calidus_cold_key());
                let mut pool = domain
                    .state()
                    .read_entity_typed::<PoolState>(&PoolKey(operator))
                    .expect("pool read")
                    .expect("pool state");
                let tip = vectors.blocks.last().expect("synthetic blocks");
                pool.calidus = Some(CalidusRegistration {
                    pub_key: Some([0x57; 32]),
                    nonce: 1,
                    tx_hash: [0xaa; 32].into(),
                    slot: tip.slot + 1_000,
                });

                let writer = domain.state().start_writer().expect("state writer");
                writer
                    .write_entity_typed(&PoolKey(operator), &pool)
                    .expect("pool write");
                writer.commit().expect("state commit");
            },
        );

        let pool_id = app.vectors().pool_id.clone();
        let (status, bytes) = app.get_bytes(&format!("/pools/{pool_id}")).await;

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
    async fn pool_by_id_treats_zero_calidus_key_as_revocation() {
        let pool_id = calidus_pool_id();
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: pool_id.clone(),
            metadata_entries: cip151_registration(12345, [0u8; 32]),
            ..Default::default()
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

    /// The answer's nonce is an `i32`: a registration with a larger one is
    /// served as no key, not as an error.
    #[tokio::test]
    async fn pool_by_id_serves_no_calidus_key_for_a_nonce_above_i32() {
        let pool_id = calidus_pool_id();
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: pool_id.clone(),
            metadata_entries: cip151_registration(i32::MAX as u64 + 1, [0x57; 32]),
            ..Default::default()
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

    /// A registration without the pool's cold-key signature is not served.
    #[tokio::test]
    async fn pool_by_id_ignores_an_unsigned_calidus_key() {
        let pool_id = calidus_pool_id();
        let pool = dolos_testing::cip151::pool_id(&calidus_cold_key());
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: pool_id.clone(),
            metadata_entries: dolos_testing::cip151::unsigned_registration(pool, 12345, [0x57; 32]),
            ..Default::default()
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

    /// A registration in a phase-2-invalid transaction registers nothing, so
    /// the key a valid one set before it is still the one served, though the
    /// invalid one carries a higher nonce.
    #[tokio::test]
    async fn pool_by_id_skips_an_invalid_transactions_calidus_key() {
        use dolos_core::import::ImportExt as _;

        let pool_id = calidus_pool_id();
        let registration = cip151_registration;
        let app = TestApp::new_with_cfg_and_setup(
            SyntheticBlockConfig {
                pool_id: pool_id.clone(),
                metadata_entries: registration(12345, [0x57; 32]),
                ..Default::default()
            },
            |domain, vectors| {
                let tip = vectors.blocks.last().expect("synthetic blocks");
                let (_, raw) = dolos_testing::blocks::make_conway_block_with_metadata_at(
                    tip.slot + 1,
                    tip.block_number + 1,
                    Some(tip.block_hash.parse().expect("block hash")),
                    registration(12346, [0x58; 32]),
                    false,
                );
                domain
                    .import_blocks(vec![raw])
                    .expect("import the invalid registration");
            },
        );

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
        assert_eq!(calidus.pub_key, hex::encode([0x57; 32]));
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

    /// `/pools/{id}` and `/pools/extended` read the active stake ESTART froze
    /// two epochs back: delegation and amount both from the pool's `set`
    /// snapshot, sized against the epoch's stored total. That is the
    /// distribution Blockfrost reads from `epoch_stake` for the current
    /// epoch.
    #[tokio::test]
    async fn pool_routes_read_the_frozen_active_stake() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let operator = parse_pool_id_bounded(&vectors.pool_id)
                .expect("pool id")
                .expect("pool hash");
            let mut pool = domain
                .state()
                .read_entity_typed::<PoolState>(&PoolKey(operator))
                .expect("pool read")
                .expect("pool state");

            // the live snapshot, frozen and rotated twice: `set` holds the
            // tally, `mark` and `live` hold none
            let mut frozen = pool.snapshot.live().expect("live snapshot").clone();
            frozen.stake = Some(300);
            frozen.delegators = Some(2);
            frozen.live_pledge = Some(0);

            let epoch = pool.snapshot.epoch().expect("pool epoch");
            let mut snapshot = EpochValue::with_live(epoch, frozen);
            snapshot.default_transition(epoch + 1);
            snapshot.default_transition(epoch + 2);
            pool.snapshot = snapshot;

            let mut state =
                dolos_cardano::load_epoch::<dolos_testing::toy_domain::ToyDomain>(domain.state())
                    .expect("epoch state");
            state.active_stake = Some(1_200);

            let writer = domain.state().start_writer().expect("state writer");
            writer
                .write_entity_typed(&PoolKey(operator), &pool)
                .expect("pool write");
            writer.write_entity_typed(&(), &state).expect("epoch write");
            writer.commit().expect("state commit");
        });

        let pool_id = app.vectors().pool_id.clone();
        let (status, bytes) = app.get_bytes(&format!("/pools/{pool_id}")).await;
        assert_eq!(status, StatusCode::OK);

        let pool: Pool = serde_json::from_slice(&bytes).expect("pool response");
        assert_eq!(pool.active_stake, "300");
        assert_eq!(pool.active_size, 0.25);

        let (status, bytes) = app.get_bytes("/pools/extended?count=100").await;
        assert_eq!(status, StatusCode::OK);

        let extended: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&bytes).expect("extended response");
        let listed = extended
            .into_iter()
            .find(|x| x.pool_id == pool_id)
            .expect("pool listed");
        assert_eq!(listed.active_stake, "300");
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

    /// A pool the Shelley genesis stakes registered before any certificate,
    /// so it lists before the pools the chain registers.
    #[tokio::test]
    async fn pools_list_a_genesis_pool_first() {
        const GENESIS: [u8; 28] = [0xff; 28];

        let app = TestApp::builder().with_genesis_pool(GENESIS).build();
        let (status, bytes) = app.get_bytes("/pools?count=100").await;
        assert_eq!(status, StatusCode::OK);

        let pools: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse pool list");

        assert!(pools.len() > 1, "the chain registers pools of its own");
        assert_eq!(pools[0], crate::mapping::bech32_pool(GENESIS).unwrap());
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

    fn retiring_pool(operator: [u8; 28], retiring_epoch: Option<u64>) -> PoolState {
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
            stake: None,
            delegators: None,
            live_pledge: None,
        };
        PoolState {
            operator: Hash::from(operator),
            snapshot: EpochValue::with_live(3, snapshot),
            retiring_epoch,
            deposit: 0,
            calidus: None,
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
    fn retiring_candidates_keep_future_retirements() {
        let pools = vec![
            // The retirement epoch is past or current, so the pool is left out.
            retiring_pool([1u8; 28], Some(5)),
            retiring_pool([2u8; 28], Some(10)),
            // The pool does not retire, so it is left out.
            retiring_pool([3u8; 28], None),
            // The retirement epoch is in the future, so the pool stays.
            retiring_pool([4u8; 28], Some(12)),
            retiring_pool([5u8; 28], Some(11)),
        ];

        let mut selected = retiring_candidates(pools, 10);
        selected.sort_unstable();

        assert_eq!(
            selected,
            vec![(11, Hash::from([5u8; 28])), (12, Hash::from([4u8; 28]))]
        );
    }

    #[test]
    fn tied_epochs_on_page_selects_tie_groups_in_the_window() {
        // Ascending epochs: 5, 10, 10, 10, 11, 12, 12.
        let list: Vec<(u64, PoolHash)> = [5, 10, 10, 10, 11, 12, 12]
            .into_iter()
            .enumerate()
            .map(|(i, epoch)| (epoch, Hash::from([i as u8; 28])))
            .collect();

        let page = |page: u64, count: usize, order: crate::pagination::Order| Pagination {
            page,
            count,
            order,
            ..Pagination::default()
        };
        let asc = crate::pagination::Order::Asc;
        let desc = crate::pagination::Order::Desc;

        assert_eq!(
            tied_epochs_on_page(&list, &Pagination::default()),
            HashSet::from([10, 12])
        );
        // The window holds epochs 5 and 10. Epoch 5 has one pool.
        assert_eq!(
            tied_epochs_on_page(&list, &page(1, 2, asc)),
            HashSet::from([10])
        );
        // The window holds epochs 11 and 12. Epoch 11 has one pool.
        assert_eq!(
            tied_epochs_on_page(&list, &page(3, 2, asc)),
            HashSet::from([12])
        );
        // In descending order, the first window holds epoch 12 only.
        assert_eq!(
            tied_epochs_on_page(&list, &page(1, 2, desc)),
            HashSet::from([12])
        );
        // The window holds epoch 5 only, which has one pool.
        assert!(tied_epochs_on_page(&list, &page(1, 1, asc)).is_empty());
    }

    #[test]
    fn order_page_sorts_an_epoch_by_retirement_position() {
        let pool = |byte: u8| Hash::from([byte; 28]);
        let list = vec![
            (10, pool(1)),
            (10, pool(2)),
            (10, pool(3)),
            (10, pool(4)),
            (10, pool(6)),
            (5, pool(5)),
        ];
        // Pool 4 has no position, and pool 5 is alone in its epoch.
        let positions = HashMap::from([
            (pool(1), (200, 0, 0)),
            (pool(2), (100, 1, 0)),
            (pool(3), (100, 0, 1)),
            (pool(6), (100, 0, 0)),
        ]);

        let asc = order_page(list.clone(), &positions, &Pagination::default());
        let expected = vec![
            (5, pool(5)),
            // No position sorts first.
            (10, pool(4)),
            // Slot 100, transaction 0: the certificate index decides.
            (10, pool(6)),
            (10, pool(3)),
            // Slot 100, transaction 1.
            (10, pool(2)),
            (10, pool(1)),
        ];
        assert_eq!(asc, expected);

        let desc = Pagination {
            order: crate::pagination::Order::Desc,
            ..Pagination::default()
        };
        let reversed: Vec<_> = expected.iter().rev().copied().collect();
        assert_eq!(order_page(list.clone(), &positions, &desc), reversed);

        let second_page = Pagination {
            count: 2,
            page: 2,
            ..Pagination::default()
        };
        assert_eq!(
            order_page(list, &positions, &second_page),
            vec![(10, pool(6)), (10, pool(3))]
        );
    }

    const RETIRE_ORDER_POOLS: [[u8; 28]; 4] = [[0x11; 28], [0x33; 28], [0x44; 28], [0x22; 28]];

    /// Builds a chain where four pools retire in epoch 100.
    ///
    /// Pools B and C retire in block 1, in transaction 0 and transaction 1.
    /// Pool A retires in block 0. Pool A then retires again in block 2, in the
    /// transaction where pool D retires, after pool D. So Blockfrost lists B,
    /// C, D, A. The pools are written in the order C, A, D, B, and the
    /// operator hashes give the order A, D, B, C. Neither order gives B, C,
    /// D, A in either direction. So only the retirement position gives the
    /// expected order.
    fn retirement_order_app(is_retired: bool) -> TestApp {
        use pallas::ledger::primitives::conway::Certificate;

        let [a, b, c, d] = RETIRE_ORDER_POOLS;
        let retire = |pool: [u8; 28]| Certificate::PoolRetirement(Hash::from(pool), 100);

        let cfg = SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 2,
            extra_certs_by_block: vec![
                vec![vec![retire(a)], vec![]],
                vec![vec![retire(b)], vec![retire(c)]],
                vec![vec![retire(d), retire(a)], vec![]],
            ],
            ..Default::default()
        };

        TestApp::new_with_cfg_and_setup(cfg, |domain, _| {
            let writer = domain.state().start_writer().expect("state writer");

            for operator in [c, a, d, b] {
                let pool = retired_pool(operator, Some(100), is_retired);
                writer
                    .write_entity_typed(&PoolKey(operator.into()), &pool)
                    .expect("failed to write pool state");
            }

            writer.commit().expect("failed to commit pool state");
        })
    }

    async fn assert_retirement_order(app: &TestApp, path: &str) {
        let [a, b, c, d] = RETIRE_ORDER_POOLS;
        let expected: Vec<PoolListRetireInner> = [b, c, d, a]
            .into_iter()
            .map(|operator| PoolListRetireInner {
                pool_id: bech32_pool(Hash::from(operator)).expect("bech32 pool id"),
                epoch: 100,
            })
            .collect();

        for (query, expected) in [
            ("", expected.clone()),
            ("?order=desc", expected.into_iter().rev().collect()),
        ] {
            let (status, bytes) = app.get_bytes(&format!("{path}{query}")).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "unexpected status {status} with body: {}",
                String::from_utf8_lossy(&bytes)
            );

            let rows: Vec<PoolListRetireInner> =
                serde_json::from_slice(&bytes).expect("failed to parse pool list retire");
            assert_eq!(rows, expected, "{path}{query}");
        }
    }

    #[tokio::test]
    async fn pools_retiring_orders_an_epoch_by_retirement_position() {
        let app = retirement_order_app(false);
        assert_retirement_order(&app, "/pools/retiring").await;
    }

    #[tokio::test]
    async fn pools_retired_orders_an_epoch_by_retirement_position() {
        let app = retirement_order_app(true);
        assert_retirement_order(&app, "/pools/retired").await;
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
        retiring_epoch: Option<u64>,
        is_retired: bool,
    ) -> PoolState {
        let mut pool = retiring_pool(operator, retiring_epoch);
        pool.snapshot.unwrap_live_mut().is_retired = is_retired;
        pool
    }

    #[test]
    fn retired_candidates_keep_retired_pools() {
        let pools = vec![
            // The pools are retired, so they stay.
            retired_pool([1u8; 28], Some(5), true),
            retired_pool([2u8; 28], Some(10), true),
            // The pool has a retirement epoch but is not retired yet, so it is
            // left out.
            retired_pool([3u8; 28], Some(12), false),
            // The pool does not retire, so it is left out.
            retired_pool([4u8; 28], None, false),
        ];

        let mut selected = retired_candidates(pools);
        selected.sort_unstable();

        assert_eq!(
            selected,
            vec![(5, Hash::from([1u8; 28])), (10, Hash::from([2u8; 28]))]
        );
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
    async fn pool_metadata_fetches_reject_loopback_without_connecting() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("The test cannot bind the TCP listener.");
        listener
            .set_nonblocking(true)
            .expect("The test cannot set the TCP listener to nonblocking mode.");
        let url = format!(
            "http://{}/pool.json",
            listener
                .local_addr()
                .expect("The test cannot read the local address.")
        );
        let mut pool = retiring_pool([1u8; 28], None);
        pool.snapshot.unwrap_live_mut().params.pool_metadata = Some(PoolMetadata {
            url: url.clone(),
            hash: vec![2u8; 32].into(),
        });

        let (metadata, error) = fetch_pool_metadata_with_error(&pool).await;

        assert!(metadata.is_none());
        assert_eq!(
            error,
            Some(DrepsInnerMetadataError::new(
                Code::ConnectionError,
                format!("Error Offchain Pool: Connection failure error when fetching metadata from {url}."),
            ))
        );
        assert_eq!(listener.accept().unwrap_err().kind(), ErrorKind::WouldBlock);

        assert!(fetch_pool_offchain_metadata(&pool).await.is_none());
        assert_eq!(listener.accept().unwrap_err().kind(), ErrorKind::WouldBlock);
    }

    #[test]
    fn pool_fetch_error_preserves_connection_and_http_errors() {
        let url = "https://example.com/pool.json";
        let connection = pool_fetch_error(url, OffchainFetchError::Connection)
            .expect("The connection error is missing.");
        assert_eq!(connection.code, Code::ConnectionError);
        assert_eq!(
            connection.message,
            "Error Offchain Pool: Connection failure error when fetching metadata from https://example.com/pool.json."
        );

        let http = pool_fetch_error(url, OffchainFetchError::HttpResponse(StatusCode::NOT_FOUND))
            .expect("The HTTP response error is missing.");
        assert_eq!(http.code, Code::HttpResponseError);
        assert_eq!(
            http.message,
            "Error Offchain Pool: HTTP Response error from https://example.com/pool.json resulted in HTTP status code : 404 \"Not Found\""
        );
    }

    #[test]
    fn pool_fetch_error_suppresses_client_and_body_errors() {
        for error in [OffchainFetchError::Client, OffchainFetchError::Body] {
            assert!(pool_fetch_error("https://example.com/pool.json", error).is_none());
        }
    }

    #[test]
    fn pool_fetch_error_maps_size_exceeded() {
        let error = pool_fetch_error(
            "https://example.com/pool.json",
            OffchainFetchError::SizeExceeded,
        )
        .expect("The size limit error is missing.");

        assert_eq!(error.code, Code::SizeExceeded);
        assert_eq!(
            error.message,
            "Error Offchain Pool: Size error, fetching metadata from https://example.com/pool.json exceeded 512 bytes."
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
            anchor: None,
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

    fn fee_log(total_rewards: u64, fixed_cost: u64, margin: Option<(u64, u64)>) -> PoolEpochLog {
        PoolEpochLog {
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
    /// a denominator near `u64::MAX` came out negative or overflowed. The
    /// ledger accepts an unreduced margin, and a 100% one past 2^53 keeps its
    /// integer part.
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

        let full = (1 << 60, 1 << 60);

        assert_eq!(margin_f64(full.0, full.1), 1.0);
        assert_eq!(
            bf_compatible_fees(&fee_log(1_000_000_000_000, 340_000_000, Some(full))),
            1_000_000_000_000
        );
    }

    /// Margins past 2^53 go through the decimal expansion. The first sits
    /// exactly on a midpoint between two doubles, which a 60-decimal cut
    /// rounded down, one lovelace short in `fees`. The second is a double
    /// away from the plain `f64` division of its operands, and so is the
    /// third, whose numerator is within 2^53 and whose odd denominator is just
    /// past it, down to 2^53 + 1.
    #[test]
    fn bf_fees_round_wide_margins_to_the_nearest_double() {
        let midpoint = (12_777_994_273_690_747, 1 << 63);

        assert_eq!(
            margin_f64(midpoint.0, midpoint.1),
            12777994273690747f64 / 9223372036854775808f64
        );
        assert_eq!(
            bf_compatible_fees(&fee_log(446_511_071_931, 340_000_000, Some(midpoint))),
            958_122_242
        );

        let (numerator, denominator) = (3_071_691_459_605_072_764, 15_394_439_036_360_229_302);

        assert_eq!(margin_f64(numerator, denominator), 0.19953253589494388);
        assert_ne!(numerator as f64 / denominator as f64, 0.19953253589494388);

        let narrow = (675_963_839_381_191, 9_007_199_254_746_239);

        assert_eq!(margin_f64(narrow.0, narrow.1), 0.07504706182945822);
        assert_ne!(narrow.0 as f64 / narrow.1 as f64, 0.07504706182945822);
        assert_eq!(
            bf_compatible_fees(&fee_log(73_496_656_964_163, 340_000_000, Some(narrow))),
            5_516_022_643_447
        );

        assert_eq!(margin_f64(narrow.0, (1 << 53) + 1), 0.07504706182950192);
        assert_ne!(
            narrow.0 as f64 / ((1u64 << 53) + 1) as f64,
            0.07504706182950192
        );
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

    #[tokio::test(flavor = "current_thread")]
    async fn slow_extended_listing_does_not_stall_health() {
        let app = TestApp::new_with_fault(Some(TestFault::SlowStateScans(SLOW_READ)));

        assert_eq!(health_beside(&app, "/pools/extended").await, StatusCode::OK);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_delegators_listing_does_not_stall_health() {
        let app = TestApp::new_with_fault(Some(TestFault::SlowStateScans(SLOW_READ)));
        let path = format!("/pools/{}/delegators", app.vectors().pool_id);

        assert_eq!(health_beside(&app, &path).await, StatusCode::OK);
    }

    fn vector_pool(app: &TestApp) -> PoolHash {
        parse_pool_id_bounded(&app.vectors().pool_id)
            .expect("pool id")
            .expect("pool hash")
    }

    #[tokio::test]
    async fn chunked_pool_scans_match_unchunked() {
        let app = replicated_app();
        let facade = app.facade();
        let operator = vector_pool(&app);
        let owners = facade
            .read_cardano_entity::<PoolState>(&PoolKey(operator))
            .expect("pool read")
            .expect("pool state")
            .snapshot
            .live()
            .expect("live snapshot")
            .params
            .pool_owners
            .clone();

        let active = load_active_pools(&facade, usize::MAX).await.expect("pools");
        assert!(active.len() > 2, "too few pools to split into chunks");

        let metrics = compute_pool_metrics(
            &facade,
            operator,
            active.clone(),
            owners.clone(),
            usize::MAX,
        )
        .await
        .expect("metrics");
        assert!(
            metrics.live_delegators > 2,
            "too few accounts to split into chunks"
        );

        let stakes = load_live_stakes(&facade, usize::MAX).await.expect("stakes");
        let listed: Vec<_> = load_listed_pools(&facade, usize::MAX)
            .await
            .expect("pools")
            .into_iter()
            .map(|(key, _)| key)
            .collect();

        for chunk in [1, 2] {
            assert_eq!(
                load_active_pools(&facade, chunk).await.expect("pools"),
                active
            );
            assert_eq!(
                compute_pool_metrics(&facade, operator, active.clone(), owners.clone(), chunk)
                    .await
                    .expect("metrics"),
                metrics
            );
            assert_eq!(
                load_live_stakes(&facade, chunk).await.expect("stakes"),
                stakes
            );

            let chunked: Vec<_> = load_listed_pools(&facade, chunk)
                .await
                .expect("pools")
                .into_iter()
                .map(|(key, _)| key)
                .collect();
            assert_eq!(chunked, listed);
        }
    }

    #[tokio::test]
    async fn chunked_delegator_pages_match_unchunked() {
        let app = replicated_app();
        let facade = app.facade();
        let operator = vector_pool(&app);

        let page = |skip, count, chunk| {
            let facade = facade.clone();
            async move {
                load_delegators_page(&facade, operator, skip, count, chunk)
                    .await
                    .expect("delegators")
                    .into_iter()
                    .map(|(key, _)| key)
                    .collect::<Vec<_>>()
            }
        };

        assert!(
            page(0, 100, usize::MAX).await.len() > 2,
            "too few delegators to split into chunks"
        );

        for (skip, count) in [(0, 100), (1, 2), (2, 1), (4, 3), (10, 5)] {
            let whole = page(skip, count, usize::MAX).await;

            for chunk in [1, 2] {
                assert_eq!(
                    page(skip, count, chunk).await,
                    whole,
                    "skip {skip}, count {count}"
                );
            }
        }
    }

    #[tokio::test]
    async fn chunked_pool_block_pages_match_unchunked() {
        let app = TestApp::new();
        let facade = app.facade();
        let pool = Hasher::<224>::hash(&[0x10, 0x11]);
        let tip = facade.get_tip_slot().expect("tip");

        let whole_asc = pool_block_page_slots(&facade, pool, tip, Order::Asc, 0, 100, usize::MAX)
            .await
            .expect("slots");
        assert!(whole_asc.0.len() > 2, "too few blocks to split into chunks");

        for order in [Order::Asc, Order::Desc] {
            for (skip, count) in [(0, 100), (1, 2), (2, 1), (3, 5), (10, 5)] {
                let whole =
                    pool_block_page_slots(&facade, pool, tip, order, skip, count, usize::MAX)
                        .await
                        .expect("slots");

                for chunk in [1, 2] {
                    let chunked =
                        pool_block_page_slots(&facade, pool, tip, order, skip, count, chunk)
                            .await
                            .expect("slots");
                    assert_eq!(chunked, whole, "{order:?}, skip {skip}, count {count}");
                }
            }
        }
    }
}

/// A pool in each phase of a retirement: retired in state, moved to the
/// final-row log as ESTART moves it, and registered again as a fresh row.
#[cfg(test)]
mod retired_pool_phases {
    use axum::http::StatusCode;
    use blockfrost_openapi::models::{pool::Pool, pool_list_retire_inner::PoolListRetireInner};
    use dolos_cardano::{
        final_rows::FinalRows, pallas_extras::MultiEraPoolRegistration, CalidusRegistration,
        PoolHash, PoolKey, PoolRegistration, PoolState,
    };
    use dolos_core::{
        ArchiveStore as _, ArchiveWriter as _, BlockSlot, Domain as _, EntityDelta as _,
        StateStore as _, StateWriter as _,
    };
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use pallas::{
        crypto::hash::Hash,
        ledger::{
            addresses::{Network, StakeAddress, StakePayload},
            primitives::conway::Certificate,
        },
    };
    use serde_json::Value;

    use crate::{mapping::bech32_pool, test_support::TestApp};

    /// The epoch the pool retires at: the synthetic chain's first, so it has
    /// retired by the tip.
    const RETIRING: u64 = 0;

    /// An owner only the retired pool's registration names: no account and no
    /// other pool knows it.
    const LOGGED_OWNER: [u8; 28] = [0x95; 28];

    /// The operator of the pool the chain registers.
    const OPERATOR: [u8; 28] = [0x71; 28];

    fn epoch_start(domain: &ToyDomain, epoch: u64) -> BlockSlot {
        dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
            .unwrap()
            .epoch_start(epoch)
    }

    /// The default chain's pool, retired by a certificate in the last
    /// transaction of block `retire_in`, and its state row as the EWRAP that
    /// retires it leaves it: `live` retired, also naming [`LOGGED_OWNER`],
    /// and with a calidus key. `then` moves it on from there.
    fn retired_pool_app(
        retire_in: usize,
        then: impl FnOnce(&ToyDomain, PoolHash, PoolState),
    ) -> TestApp {
        let pool = PoolHash::from(OPERATOR);
        let retire = Certificate::PoolRetirement(pool, RETIRING);

        let cfg = SyntheticBlockConfig {
            pool_id: bech32_pool(pool).unwrap(),
            block_count: 3,
            txs_per_block: 2,
            extra_certs_by_block: (0..3)
                .map(|block| match block == retire_in {
                    true => vec![vec![], vec![retire.clone()]],
                    false => vec![vec![], vec![]],
                })
                .collect(),
            ..Default::default()
        };

        TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let key = PoolKey(pool);
            let mut row = domain
                .state()
                .read_entity_typed::<PoolState>(&key)
                .unwrap()
                .expect("the synthetic chain registers the pool");

            row.retiring_epoch = Some(RETIRING);
            row.deposit = 0;
            row.calidus = Some(CalidusRegistration {
                pub_key: Some([0x57; 32]),
                nonce: 1,
                tx_hash: [0xaa; 32].into(),
                slot: vectors.blocks[0].slot,
            });

            let live = row.snapshot.unwrap_live_mut();
            live.is_retired = true;
            live.params.pool_owners.push(LOGGED_OWNER.into());

            let writer = domain.state().start_writer().unwrap();
            writer.write_entity_typed(&key, &row).unwrap();
            writer.commit().unwrap();

            then(domain, pool, row);
        })
    }

    /// Move the pool's row to the log at `slot`, as the ESTART finalize
    /// commit does: the archive commits first, then the state deletes it.
    fn move_to_log(domain: &ToyDomain, pool: PoolHash, row: &PoolState, slot: BlockSlot) {
        let mut finals = FinalRows::default();
        finals.push(slot, &PoolKey(pool), row);

        let archive = domain.archive().start_writer().unwrap();
        let state = domain.state().start_writer().unwrap();
        finals.write(&state, &archive).unwrap();
        archive.commit().unwrap();
        state.commit().unwrap();

        assert!(domain
            .state()
            .read_entity_typed::<PoolState>(&PoolKey(pool))
            .unwrap()
            .is_none());
    }

    /// Where ESTART keys the pool's final row.
    fn final_row_slot(domain: &ToyDomain) -> BlockSlot {
        epoch_start(domain, PoolState::final_row_epoch(RETIRING))
    }

    fn moved_app() -> TestApp {
        retired_pool_app(2, |domain, pool, row| {
            move_to_log(domain, pool, &row, final_row_slot(domain));
        })
    }

    fn logged_owner_address() -> String {
        StakeAddress::new(
            Network::Testnet,
            StakePayload::Stake(Hash::from(LOGGED_OWNER)),
        )
        .to_bech32()
        .unwrap()
    }

    /// The routes a pool answers, and an account only its registration names.
    fn paths(app: &TestApp) -> Vec<String> {
        let pool = &app.vectors().pool_id;
        let owner = logged_owner_address();

        vec![
            format!("/pools/{pool}"),
            format!("/pools/{pool}/metadata"),
            format!("/pools/{pool}/relays"),
            format!("/pools/{pool}/history"),
            format!("/pools/{pool}/delegators"),
            format!("/pools/{pool}/updates"),
            format!("/pools/{pool}/blocks"),
            format!("/pools/{pool}/votes"),
            format!("/epochs/1/stakes/{pool}"),
            format!("/epochs/1/blocks/{pool}"),
            "/pools/retired".to_string(),
            "/pools/retiring".to_string(),
            format!("/accounts/{owner}/addresses"),
        ]
    }

    async fn answers(app: &TestApp) -> Vec<(String, StatusCode, Value)> {
        let mut out = vec![];

        for path in paths(app) {
            let (status, bytes) = app.get_bytes(&path).await;
            let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            out.push((path, status, body));
        }

        out
    }

    /// Done criterion of the route tests: once moved to the log, the pool
    /// answers every route as it did from state, the account only its
    /// registration names stays known, and `/pools/retired` still lists it.
    #[tokio::test]
    async fn a_moved_pool_answers_as_it_did_from_state() {
        let retired = retired_pool_app(2, |_, _, _| ());
        let moved = moved_app();

        let before = answers(&retired).await;
        let after = answers(&moved).await;

        for ((path, status, body), (_, moved_status, moved_body)) in before.iter().zip(&after) {
            assert_eq!(status, moved_status, "{path}");
            assert_eq!(body, moved_body, "{path}");
        }

        let ok = |path: &str| {
            before
                .iter()
                .find(|(x, ..)| x.as_str() == path)
                .map(|(_, status, body)| (*status, body.clone()))
                .unwrap()
        };

        let pool_id = &moved.vectors().pool_id;
        let (status, body) = ok(&format!("/pools/{pool_id}"));
        assert_eq!(status, StatusCode::OK);
        let pool: Pool = serde_json::from_value(body).unwrap();
        assert!(!pool.retirement.is_empty());
        assert!(pool.calidus_key.is_some());

        let (status, body) = ok("/pools/retired");
        assert_eq!(status, StatusCode::OK);
        let retired: Vec<PoolListRetireInner> = serde_json::from_value(body).unwrap();
        assert_eq!(
            retired,
            vec![PoolListRetireInner {
                pool_id: pool_id.clone(),
                epoch: RETIRING as i32,
            }]
        );

        let (status, _) = ok(&format!("/accounts/{}/addresses", logged_owner_address()));
        assert_eq!(status, StatusCode::OK);
    }

    /// The point read keys the log at the epoch the newest retirement names,
    /// rather than scanning it: a decoy row keyed later, which a scan would
    /// take for the newest, is not served.
    #[tokio::test]
    async fn the_point_read_keys_the_log_by_the_newest_retirement() {
        let app = retired_pool_app(2, |domain, pool, row| {
            move_to_log(domain, pool, &row, final_row_slot(domain));

            let mut decoy = row.clone();
            decoy.snapshot.unwrap_live_mut().params.pledge = 999;

            let writer = domain.archive().start_writer().unwrap();
            writer
                .write_log_typed(final_row_slot(domain) + 1, &PoolKey(pool), &decoy)
                .unwrap();
            writer.commit().unwrap();
        });

        let (status, bytes) = app
            .get_bytes(&format!("/pools/{}", app.vectors().pool_id))
            .await;
        assert_eq!(status, StatusCode::OK);

        let pool: Pool = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(pool.declared_pledge, "0");
    }

    /// A row the newest retirement does not key, as in an archive pruned of
    /// the certificate, is found by a scan of the log.
    #[tokio::test]
    async fn a_row_off_the_retirement_key_is_found_by_a_scan() {
        let moved = moved_app();
        let elsewhere = retired_pool_app(2, |domain, pool, row| {
            move_to_log(domain, pool, &row, final_row_slot(domain) - 1);
        });

        let before = answers(&moved).await;
        let after = answers(&elsewhere).await;

        for ((path, status, body), (_, other_status, other_body)) in before.iter().zip(&after) {
            assert_eq!(status, other_status, "{path}");
            assert_eq!(body, other_body, "{path}");
        }
    }

    /// Registered again after the move: a fresh row in state, its deposit set
    /// and no calidus key, serves the pool, and `/pools/retired` drops it,
    /// while the log keeps the old row.
    #[tokio::test]
    async fn a_pool_registered_again_is_served_from_its_fresh_row() {
        let app = retired_pool_app(1, |domain, pool, row| {
            move_to_log(domain, pool, &row, final_row_slot(domain));

            let params = row.snapshot.live().unwrap().params.clone();
            let cert = MultiEraPoolRegistration {
                operator: pool,
                vrf_keyhash: params.vrf_keyhash,
                pledge: 7,
                cost: params.cost,
                margin: params.margin,
                reward_account: params.reward_account,
                pool_owners: params.pool_owners,
                relays: params.relays,
                pool_metadata: params.pool_metadata,
            };

            let epoch = dolos_cardano::load_epoch::<ToyDomain>(domain.state())
                .unwrap()
                .number;
            let mut fresh = None;
            PoolRegistration::new(cert, epoch_start(domain, epoch), epoch, 500).apply(&mut fresh);
            let fresh = fresh.unwrap();

            assert_eq!(fresh.deposit, 500);
            assert_eq!(fresh.calidus, None);

            let writer = domain.state().start_writer().unwrap();
            writer.write_entity_typed(&PoolKey(pool), &fresh).unwrap();
            writer.commit().unwrap();
        });

        let pool_id = &app.vectors().pool_id;
        let (status, bytes) = app.get_bytes(&format!("/pools/{pool_id}")).await;
        assert_eq!(status, StatusCode::OK);

        let pool: Pool = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(pool.declared_pledge, "7");
        assert!(pool.calidus_key.is_none());

        let (status, bytes) = app.get_bytes("/pools/retired").await;
        assert_eq!(status, StatusCode::OK);
        let retired: Vec<PoolListRetireInner> = serde_json::from_slice(&bytes).unwrap();
        assert!(retired.is_empty());
    }
}
