//! The `/pools` routes: the shared pool id parsing, the point read of a
//! pool's row, the live metrics and the listing order the endpoints share.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use axum::http::StatusCode;
use blockfrost_openapi::models::{
    pool_list_retire_inner::PoolListRetireInner, DrepsInnerMetadataError,
};
use dolos_cardano::{
    final_rows::newest_final_row,
    indexes::CardanoArchiveIndexExt,
    model::{AccountState, FixedNamespace as _, PoolState},
    pallas_extras, pool_history, AccountKey, PoolHash, PoolKey,
};
use dolos_core::{
    keys, ArchiveStore as _, BlockSlot, Domain, Entity as _, KeyRange, StateStore as _,
};
use itertools::Itertools;
use pallas::{
    crypto::hash::Hasher,
    ledger::{primitives::StakeCredential, traverse::MultiEraBlock},
};

use crate::{
    error::Error,
    log_and_500,
    mapping::{
        bech32_pool, pool_offchain_body, pool_offchain_metadata,
        pools::{hash_mismatch_error, pool_fetch_error, retire_list_model},
    },
    pagination::Pagination,
    scan::{EntityScan, ENTITY_CHUNK},
    Facade,
};

mod all;
mod blocks;
mod by_id;
mod delegators;
mod extended;
mod history;
mod metadata;
mod relays;
mod retired;
mod retiring;
mod updates;
mod votes;

pub use all::all;
pub use blocks::by_id_blocks;
pub use by_id::by_id;
pub use delegators::by_id_delegators;
pub use extended::all_extended;
pub use history::by_id_history;
pub use metadata::by_id_metadata;
pub use relays::by_id_relays;
pub use retired::all_retired;
pub use retiring::all_retiring;
pub use updates::by_id_updates;
pub use votes::by_id_votes;

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

/// The slot of the pool's newest certificate, `None` when the archive holds
/// none of them.
fn newest_cert_slot<D: Domain>(
    domain: &D,
    pool: &PoolHash,
) -> Result<Option<BlockSlot>, StatusCode> {
    domain
        .archive()
        .slots_by_pool_certs(pool.as_slice(), 0, BlockSlot::MAX)
        .map_err(log_and_500("failed to read the pool certs index"))?
        .next_back()
        .transpose()
        .map_err(log_and_500("failed to read the pool certs index"))
}

/// The log rows the point read passes over before it leaves the pool to a
/// scan. They are the rows of pools moved between the certificate and the
/// final row, at most `eMax` + 2 epochs of moves: a few dozen on mainnet.
const FORWARD_READ_ROWS: usize = 256;

/// The first final row of `key` among the [`FORWARD_READ_ROWS`] rows at or
/// after `from`, decoding only its rows.
fn first_final_row_from<D: Domain>(
    domain: &D,
    key: &PoolKey,
    from: BlockSlot,
) -> Result<Option<PoolState>, StatusCode> {
    let wanted = keys::encode(key);

    let rows = domain
        .archive()
        .iter_logs(PoolState::NS, KeyRange::slots(from..BlockSlot::MAX))
        .map_err(log_and_500("failed to iterate the pools log"))?;

    for row in rows.take(FORWARD_READ_ROWS) {
        let (log_key, value) = row.map_err(log_and_500("failed to read the pools log"))?;

        if log_key.entity() == wanted.as_slice() {
            let row = PoolState::decode_entity(PoolState::NS, &value)
                .map_err(log_and_500("failed to decode a pools log row"))?;

            return Ok(Some(row));
        }
    }

    Ok(None)
}

/// A pool's row from state, or from the log by its newest certificate.
fn point_read_pool_row<D: Domain>(
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

    match newest_cert_slot(domain, &pool)? {
        Some(slot) => first_final_row_from(domain, &key, slot),
        None => Ok(None),
    }
}

/// A pool's row: the state row, then its final row in the log.
///
/// ESTART commits a final row before it deletes the state row, so a state
/// miss read first finds a moved pool in the log. A moved pool's newest
/// certificate is the retirement that moved it, and its final row is the
/// pool's first one keyed after that certificate's slot, at the retiring
/// epoch + 2 ([`PoolState::final_row_epoch`]). So the log is read forward
/// from that slot to the pool's first row, with no block read; only the rows
/// of pools moved in between are passed over, by key. Where that misses, an
/// archive pruned of the certificate for one or a pool that never registered,
/// the log is scanned for the newest row, on the blocking pool under the query
/// limiter.
pub(crate) async fn read_pool_row<D>(
    domain: &Facade<D>,
    pool: PoolHash,
) -> Result<Option<PoolState>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    if let Some(row) = point_read_pool_row(&domain.inner, pool)? {
        return Ok(Some(row));
    }

    domain
        .blocking(move |domain| {
            let logged = newest_final_row::<PoolState>(domain.archive(), &PoolKey(pool))
                .map_err(log_and_500("failed to read the pools log"))?;

            Ok(logged.map(|(_, row)| row))
        })
        .await
}

/// Whether the pool ever registered: it has a row in state or in the log.
pub(crate) async fn pool_exists<D>(domain: &Facade<D>, pool: PoolHash) -> Result<bool, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    Ok(read_pool_row(domain, pool).await?.is_some())
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

/// The place of the latest retirement certificate of a pool on chain: the
/// slot, the transaction index in the block, and the certificate index in the
/// transaction.
type RetirementPosition = (BlockSlot, usize, usize);

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

#[cfg(test)]
pub(super) mod testing {
    use super::*;
    use crate::test_support::{TestApp, REG_POOL_ID};
    use blockfrost_openapi::models::pool_list_retire_inner::PoolListRetireInner;
    use dolos_cardano::model::{EpochValue, PoolParams, PoolSnapshot};
    use dolos_core::StateWriter as _;
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::crypto::hash::Hash;
    use serde_json::Value;

    pub fn invalid_pool_id() -> &'static str {
        "not-a-pool"
    }

    pub fn missing_pool_id() -> &'static str {
        "pool1qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qursw2w89e2"
    }

    /// The payloads of these pool IDs do not have 28 bytes. The first payload
    /// is the hash of the registered pool without its last byte. The second
    /// payload is the hash with a `0x00` byte at the end. Without the length
    /// check, the registration check finds the pool for the second payload.
    pub fn wrong_length_pool_ids(app: &TestApp) -> [String; 2] {
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

    pub async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    pub async fn assert_error_message(app: &TestApp, path: &str, expected: &str) {
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

    pub fn retiring_pool(operator: [u8; 28], retiring_epoch: Option<u64>) -> PoolState {
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

    pub const RETIRE_ORDER_POOLS: [[u8; 28]; 4] = [[0x11; 28], [0x33; 28], [0x44; 28], [0x22; 28]];

    /// Builds a chain where four pools retire in epoch 100.
    ///
    /// Pools B and C retire in block 1, in transaction 0 and transaction 1.
    /// Pool A retires in block 0. Pool A then retires again in block 2, in the
    /// transaction where pool D retires, after pool D. So Blockfrost lists B,
    /// C, D, A. The pools are written in the order C, A, D, B, and the
    /// operator hashes give the order A, D, B, C. Neither order gives B, C,
    /// D, A in either direction. So only the retirement position gives the
    /// expected order.
    pub fn retirement_order_app(is_retired: bool) -> TestApp {
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

    pub async fn assert_retirement_order(app: &TestApp, path: &str) {
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

    pub fn retired_pool(
        operator: [u8; 28],
        retiring_epoch: Option<u64>,
        is_retired: bool,
    ) -> PoolState {
        let mut pool = retiring_pool(operator, retiring_epoch);
        pool.snapshot.unwrap_live_mut().is_retired = is_retired;
        pool
    }

    pub const NOT_FOUND_MESSAGE: &str = "The requested component has not been found.";

    pub fn registered_pool_app() -> TestApp {
        TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: REG_POOL_ID.to_string(),
            ..Default::default()
        })
    }

    /// Gives a text for each difference from the Blockfrost error body.
    pub async fn error_mismatch(
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

    pub fn vector_pool(app: &TestApp) -> PoolHash {
        parse_pool_id_bounded(&app.vectors().pool_id)
            .expect("pool id")
            .expect("pool hash")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::pools::testing::{
            error_mismatch, registered_pool_app, vector_pool, NOT_FOUND_MESSAGE,
        },
        test_support::{
            bech32_encode_values, bech32_values_from_bytes, pool_id_cases, replicated_app,
            PoolIdCase, TestApp, REG_POOL_HEX, REG_POOL_ID,
        },
    };
    use blockfrost_openapi::models::{pool::Pool, pool_list_extended_inner::PoolListExtendedInner};
    use dolos_cardano::model::EpochValue;
    use dolos_core::StateWriter as _;
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::crypto::hash::Hash;
    use serde_json::Value;

    const COUNT_MESSAGE: &str = "querystring/count must be >= 1";

    const POOL_ID_MESSAGE: &str = "Invalid or malformed pool id format.";

    fn pool_id_case(label: &str) -> PoolIdCase {
        pool_id_cases()
            .into_iter()
            .find(|case| case.label == label)
            .expect("Cannot find the pool ID case.")
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
}

/// A pool in each phase of a retirement: retired in state, moved to the
/// final-row log as ESTART moves it, and registered again as a fresh row.
#[cfg(test)]
mod retired_pool_phases {
    use crate::{mapping::bech32_pool, test_support::TestApp};
    use axum::http::StatusCode;
    use blockfrost_openapi::models::{pool::Pool, pool_list_retire_inner::PoolListRetireInner};
    use dolos_cardano::{
        final_rows::FinalRows, indexes::CardanoArchiveIndexExt as _,
        pallas_extras::MultiEraPoolRegistration, CalidusRegistration, PoolHash, PoolKey,
        PoolRegistration, PoolState,
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

    /// The epoch the pool retires at: the one after the synthetic chain's
    /// (epoch 2), as a retirement certificate must name a later epoch.
    const RETIRING: u64 = 3;

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
            // not `/pools/retiring`: the synthetic tip is before the
            // retirement, where a chain is three epochs past it by the move
            "/pools/retired".to_string(),
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

    /// The point read reads the log forward from the newest certificate,
    /// rather than scanning it: a decoy row keyed later, which a scan would
    /// take for the newest, is not served.
    #[tokio::test]
    async fn the_point_read_starts_at_the_newest_certificate() {
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

    /// Write `row` to the pools log under `key` at `slot`.
    fn write_log_row(domain: &ToyDomain, slot: BlockSlot, key: &PoolKey, row: &PoolState) {
        let writer = domain.archive().start_writer().unwrap();
        writer.write_log_typed(slot, key, row).unwrap();
        writer.commit().unwrap();
    }

    /// The slots of the pool's certificates, oldest first.
    fn cert_slots(domain: &ToyDomain, pool: PoolHash) -> Vec<BlockSlot> {
        domain
            .archive()
            .slots_by_pool_certs(pool.as_slice(), 0, BlockSlot::MAX)
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// `row` with a pledge the served pool must not show.
    fn with_pledge(row: &PoolState, pledge: u64) -> PoolState {
        let mut row = row.clone();
        row.snapshot.unwrap_live_mut().params.pledge = pledge;
        row
    }

    async fn served_pledge(app: &TestApp) -> String {
        let (status, bytes) = app
            .get_bytes(&format!("/pools/{}", app.vectors().pool_id))
            .await;
        assert_eq!(status, StatusCode::OK);

        let pool: Pool = serde_json::from_slice(&bytes).unwrap();
        pool.declared_pledge
    }

    /// The forward read takes the pool's own row: another pool's row at the
    /// same slot, which sorts first, is passed over.
    #[tokio::test]
    async fn the_point_read_passes_over_another_pools_row() {
        let app = retired_pool_app(2, |domain, pool, row| {
            move_to_log(domain, pool, &row, final_row_slot(domain));

            let other = PoolKey(PoolHash::from([0x10; 28]));
            assert!(other.0 < pool);
            write_log_row(
                domain,
                final_row_slot(domain),
                &other,
                &with_pledge(&row, 999),
            );
        });

        assert_eq!(served_pledge(&app).await, "0");
    }

    /// Moved twice: the forward read starts at the newest certificate, past
    /// the older final row, which sits between the two certificates.
    #[tokio::test]
    async fn the_point_read_serves_the_newer_of_two_final_rows() {
        let app = retired_pool_app(2, |domain, pool, row| {
            let certs = cert_slots(domain, pool);
            assert!(certs.len() >= 2, "{certs:?}");
            assert!(certs[0] + 1 < certs[certs.len() - 1]);

            write_log_row(
                domain,
                certs[0] + 1,
                &PoolKey(pool),
                &with_pledge(&row, 111),
            );
            move_to_log(domain, pool, &row, final_row_slot(domain));
        });

        assert_eq!(served_pledge(&app).await, "0");
    }

    /// More rows of other pools than the forward read passes over sit between
    /// the certificate and the final row: the scan still finds it.
    #[tokio::test]
    async fn a_row_past_the_forward_read_is_found_by_a_scan() {
        let app = retired_pool_app(2, |domain, pool, row| {
            let from = *cert_slots(domain, pool).last().unwrap();
            let decoy = with_pledge(&row, 999);

            for i in 0..=super::FORWARD_READ_ROWS {
                let other = PoolKey(PoolHash::from([0x10; 28]));
                write_log_row(domain, from + 1 + i as BlockSlot, &other, &decoy);
            }

            let past = from + 2 + super::FORWARD_READ_ROWS as BlockSlot;
            move_to_log(domain, pool, &row, past);
        });

        assert_eq!(served_pledge(&app).await, "0");
    }

    /// A pool that never registered answers 404 on every pool route, after
    /// the scan of the log finds nothing.
    #[tokio::test]
    async fn a_pool_that_never_registered_is_not_found() {
        let app = moved_app();
        let unknown = bech32_pool(PoolHash::from([0x33; 28])).unwrap();

        for path in [
            format!("/pools/{unknown}"),
            format!("/pools/{unknown}/metadata"),
            format!("/pools/{unknown}/relays"),
            format!("/pools/{unknown}/history"),
            format!("/pools/{unknown}/delegators"),
            format!("/pools/{unknown}/updates"),
            format!("/pools/{unknown}/blocks"),
            format!("/pools/{unknown}/votes"),
            format!("/epochs/1/stakes/{unknown}"),
            format!("/epochs/1/blocks/{unknown}"),
        ] {
            let (status, _) = app.get_bytes(&path).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
        }
    }

    /// `/pools/retired` lists a pool moved twice once, at its newer final
    /// row's retiring epoch.
    #[tokio::test]
    async fn pools_retired_lists_the_newer_of_two_final_rows() {
        let app = retired_pool_app(2, |domain, pool, row| {
            let mut older = row.clone();
            older.retiring_epoch = Some(RETIRING - 1);

            write_log_row(domain, 0, &PoolKey(pool), &older);
            move_to_log(domain, pool, &row, final_row_slot(domain));
        });

        let (status, bytes) = app.get_bytes("/pools/retired").await;
        assert_eq!(status, StatusCode::OK);

        let retired: Vec<PoolListRetireInner> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            retired,
            vec![PoolListRetireInner {
                pool_id: app.vectors().pool_id.clone(),
                epoch: RETIRING as i32,
            }]
        );
    }

    /// A row keyed before the newest certificate, where the forward read
    /// cannot reach it, as when an archive pruned of the retirement holds a
    /// later certificate only, is found by a scan of the log.
    #[tokio::test]
    async fn a_row_before_the_newest_certificate_is_found_by_a_scan() {
        let moved = moved_app();
        let elsewhere = retired_pool_app(2, |domain, pool, row| {
            move_to_log(domain, pool, &row, 0);
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
