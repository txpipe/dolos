//! What minibf reads of a pool's history: where it first registered and the
//! blocks it minted over its life. Neither is in state. The archive rebuilds
//! both, and two cache namespaces (decision 0059) keep what sealed data gives
//! of them for the list routes.
//!
//! - [`POOL_REGISTRATIONS`] maps each pool to its first registration in a valid
//!   transaction, or the Shelley genesis, for every pool first registered
//!   before the immutable slot the entry records.
//! - [`POOL_BLOCKS`] maps each pool to the sum of its `pool-epochs` rows up to
//!   the epoch the entry records. A read adds the rows after it, the pool's
//!   snapshots and the blocks of the first Shelley epochs, which no row covers.

use std::{
    collections::{BTreeMap, HashSet},
    ops::Range,
};

use dolos_core::{
    config::HistoryMode, ArchiveStore as _, CacheEntry, CacheError, CacheNamespace,
    CacheStore as _, ChainError, Domain, DomainError, Entity as _, FixedNamespace as _, Genesis,
    LogRangeOf, StateStore as _,
};
use pallas::{codec::minicbor, ledger::primitives::Epoch};

use crate::{
    indexes::CardanoArchiveIndexExt as _, periods::read_sealed,
    rupd::loading::stake_snapshot_epoch, CertPosition, ChainSummary, PoolEpochLog, PoolHash,
    PoolKey, PoolState,
};

/// The cache namespace of first registrations: one entry, under [`ENTRY`].
pub const POOL_REGISTRATIONS: CacheNamespace = CacheNamespace::immutable("pool-registrations", 1);

/// The cache namespace of lifetime blocks: one entry, under [`ENTRY`].
pub const POOL_BLOCKS: CacheNamespace = CacheNamespace::immutable("pool-blocks", 1);

/// Each namespace holds one entry for every pool.
pub const ENTRY: &[u8] = b"";

/// Register both namespaces, unless rolling history prunes the archive they
/// are built from, or the cache is off and would keep no entry: there every
/// read goes to the archive, for the pools it asks about only. Answers
/// whether it registered.
pub fn register_pool_caches<D: Domain>(domain: &D) -> Result<bool, CacheError> {
    if domain.history_config().mode == HistoryMode::Rolling
        || !domain.storage_config().cache.enabled
    {
        return Ok(false);
    }

    domain.cache().register(POOL_REGISTRATIONS)?;
    domain.cache().register(POOL_BLOCKS)?;

    Ok(true)
}

/// Where the pools the Shelley genesis stakes register: before every block,
/// since no certificate registers them.
pub const GENESIS_POSITION: CertPosition = (0, 0, 0);

/// The pools the Shelley genesis stakes.
pub fn genesis_pools(genesis: &Genesis) -> HashSet<PoolHash> {
    let pools = genesis
        .shelley
        .staking
        .as_ref()
        .and_then(|staking| staking.pools.as_ref());

    pools
        .into_iter()
        .flat_map(|pools| pools.values())
        .filter_map(|pool| pool.public_key.parse().ok())
        .collect()
}

/// Where `pool` first registered: at [`GENESIS_POSITION`] for one of
/// `genesis`, else in its first valid registration the archive holds.
fn first_registration<D: Domain>(
    domain: &D,
    genesis: &HashSet<PoolHash>,
    pool: &PoolHash,
) -> Result<Option<CertPosition>, ChainError> {
    if genesis.contains(pool) {
        return Ok(Some(GENESIS_POSITION));
    }

    Ok(domain.archive().pool_first_registration(pool)?)
}

/// The last slot no rollback can undo with the tip at `tip`.
pub fn immutable_slot<D: Domain>(domain: &D, tip: u64) -> u64 {
    tip.saturating_sub(crate::mutable_slots(&domain.genesis()))
}

/// The last epoch whose `pool-epochs` rows no rollback can undo with the
/// ledger in `epoch`. RUPD writes an epoch's rows during the next one, so
/// that next epoch must be past the rollback window by the start of `epoch`.
pub fn pool_blocks_watermark(
    chain: &ChainSummary,
    mutable_slots: u64,
    epoch: Epoch,
) -> Option<Epoch> {
    (0..epoch)
        .rev()
        .find(|logged| crate::periods::is_sealable(chain, mutable_slots, logged + 1, epoch))
}

/// The first registrations of the pools first registered at or before
/// `watermark`, a slot.
#[derive(Debug, Clone, Default, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
pub struct RegistrationsEntry {
    #[n(0)]
    pub watermark: u64,

    /// Slot, transaction index and certificate index.
    #[n(1)]
    pub first: BTreeMap<PoolHash, (u64, u64, u64)>,
}

impl RegistrationsEntry {
    pub fn decode(value: &[u8]) -> Result<Self, ChainError> {
        Ok(minicbor::decode(value)?)
    }

    pub fn position(&self, pool: &PoolHash) -> Option<CertPosition> {
        self.first
            .get(pool)
            .map(|&(slot, tx, cert)| (slot, tx as usize, cert as usize))
    }

    fn insert(&mut self, pool: PoolHash, (slot, tx, cert): CertPosition) {
        self.first.insert(pool, (slot, tx as u64, cert as u64));
    }

    fn into_cache_entry(self, cost: u64) -> CacheEntry {
        CacheEntry::new(minicbor::to_vec(&self).expect("infallible encode"), cost)
    }
}

/// The registrations entry at `watermark`, built without the cache: one
/// forward seek for each pool in state.
pub fn build_registrations_entry<D: Domain>(
    domain: &D,
    watermark: u64,
) -> Result<CacheEntry, ChainError> {
    extend_registrations(domain, RegistrationsEntry::default(), watermark)
}

/// `entry` moved to `watermark`, seeking only the pools in state it lacks.
fn extend_registrations<D: Domain>(
    domain: &D,
    mut entry: RegistrationsEntry,
    watermark: u64,
) -> Result<CacheEntry, ChainError> {
    let genesis = genesis_pools(&domain.genesis());
    let mut seeks = 0;

    for pool in domain.state().iter_entities_typed::<PoolState>(None)? {
        let (PoolKey(pool), _) = pool?;

        if entry.first.contains_key(&pool) {
            continue;
        }

        seeks += 1;

        match first_registration(domain, &genesis, &pool)? {
            Some(at) if at.0 <= watermark => entry.insert(pool, at),
            _ => (),
        }
    }

    entry.watermark = watermark;

    Ok(entry.into_cache_entry(seeks))
}

/// Where each of `pools` first registered in a valid transaction, `None`
/// where the archive no longer holds it.
///
/// The pools of the cached entry come from it, and each other pool costs a
/// seek. The entry is extended in the background once a pool it lacks
/// registered at or before the immutable slot. With the namespace
/// unregistered, every pool costs a seek.
pub async fn first_registrations<D: Domain>(
    domain: &D,
    pools: Vec<PoolHash>,
) -> Result<Vec<Option<CertPosition>>, DomainError> {
    let tip = blocking(domain, |domain| tip_slot(domain)).await?;
    let watermark = immutable_slot(domain, tip);

    let fill = {
        let domain = domain.clone();
        move || build_registrations_entry(&domain, watermark)
    };

    let entry = match domain
        .cache()
        .get_or_fill(POOL_REGISTRATIONS.name, ENTRY, fill)
        .await
    {
        Ok(value) => Some(value),
        Err(CacheError::Fill(error)) => {
            return Err(DomainError::Internal(format!(
                "pool registrations: {error}"
            )))
        }
        Err(CacheError::UnknownNamespace(_)) => None,
        Err(error) => {
            tracing::warn!(%error, "pool registrations cache unavailable, seeking uncached");
            None
        }
    };

    let (positions, stale) = blocking(domain, move |domain| {
        let entry = entry
            .as_deref()
            .map(RegistrationsEntry::decode)
            .transpose()?;

        let genesis = genesis_pools(&domain.genesis());
        let mut stale = None;

        let positions = pools
            .iter()
            .map(|pool| {
                if let Some(at) = entry.as_ref().and_then(|entry| entry.position(pool)) {
                    return Ok(Some(at));
                }

                let at = first_registration(domain, &genesis, pool)?;

                if let (Some(entry), Some(at)) = (&entry, at) {
                    if at.0 <= watermark && entry.watermark < watermark {
                        stale = Some(entry.clone());
                    }
                }

                Ok(at)
            })
            .collect::<Result<Vec<_>, ChainError>>()?;

        Ok((positions, stale))
    })
    .await?;

    if let Some(stale) = stale {
        let extend = {
            let domain = domain.clone();
            move || extend_registrations(&domain, stale, watermark)
        };

        if let Err(error) = domain
            .cache()
            .refill(POOL_REGISTRATIONS.name, ENTRY, extend)
        {
            tracing::warn!(%error, "pool registrations cache entry not extended");
        }
    }

    Ok(positions)
}

/// The blocks each pool minted in the epochs up to `watermark`, by the sum
/// of its `pool-epochs` rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
pub struct BlocksEntry {
    #[n(0)]
    pub watermark: Epoch,

    #[n(1)]
    pub blocks: BTreeMap<PoolHash, u64>,
}

impl BlocksEntry {
    pub fn decode(value: &[u8]) -> Result<Self, ChainError> {
        Ok(minicbor::decode(value)?)
    }

    fn into_cache_entry(self, cost: u64) -> CacheEntry {
        CacheEntry::new(minicbor::to_vec(&self).expect("infallible encode"), cost)
    }
}

/// The blocks entry at `watermark`, built without the cache: one scan of the
/// `pool-epochs` rows up to it.
pub fn build_blocks_entry<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    watermark: Epoch,
) -> Result<CacheEntry, ChainError> {
    extend_blocks(domain, chain, None, watermark)
}

/// `entry` moved to `watermark`, scanning only the rows past its own.
fn extend_blocks<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    entry: Option<BlocksEntry>,
    watermark: Epoch,
) -> Result<CacheEntry, ChainError> {
    let from = match &entry {
        Some(entry) if entry.watermark <= watermark => chain.epoch_start(entry.watermark + 1),
        _ => 0,
    };

    let mut entry = entry
        .filter(|entry| entry.watermark <= watermark)
        .unwrap_or_default();

    let to = chain.epoch_start(watermark + 1);
    let range = LogRangeOf::<PoolKey>::slots(from..to);
    let mut rows = 0;

    for row in domain
        .archive()
        .iter_logs_typed::<PoolEpochLog>(Some(range))?
    {
        let (_, PoolKey(pool), log) = row?;

        *entry.blocks.entry(pool).or_default() += log.blocks_minted;
        rows += 1;
    }

    entry.watermark = watermark;

    Ok(entry.into_cache_entry(rows))
}

/// The Shelley epochs no `pool-epochs` row covers: RUPD logs an epoch only
/// once a stake snapshot exists for it ([`stake_snapshot_epoch`]).
pub fn unlogged_epochs(chain: &ChainSummary) -> Range<Epoch> {
    let first_shelley = chain.first_shelley_epoch();

    let first_logged = (first_shelley..)
        .find(|epoch| stake_snapshot_epoch(epoch + 1, first_shelley).is_some())
        .expect("a stake snapshot exists from some epoch on");

    first_shelley..first_logged
}

/// The blocks `pool` minted in the [`unlogged_epochs`] before `end`, by the
/// archive's `pool_blocks` tags.
fn unlogged_blocks<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    pool: &PoolHash,
    end: Epoch,
) -> Result<u64, ChainError> {
    let epochs = unlogged_epochs(chain);
    let end = end.min(epochs.end);

    if epochs.start >= end {
        return Ok(0);
    }

    let slots = domain.archive().slots_by_pool_blocks(
        pool.as_ref(),
        chain.epoch_start(epochs.start),
        chain.epoch_start(end) - 1,
    )?;

    let mut blocks = 0;

    for slot in slots {
        slot?;
        blocks += 1;
    }

    Ok(blocks)
}

/// The blocks `pool` minted from epoch `from` on: its `pool-epochs` rows up
/// to the epoch before its live snapshot, the `mark` snapshot's count while
/// that epoch has no row yet (between ESTART and RUPD), and the live count.
/// The [`unlogged_epochs`] neither snapshot holds come from the archive
/// whatever `from` is, since no entry sums them either.
pub fn unsealed_blocks<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    pool: &PoolState,
    from: Epoch,
) -> Result<u64, ChainError> {
    let Some(live_epoch) = pool.snapshot.epoch() else {
        return Ok(0);
    };

    let previous = live_epoch.checked_sub(1);
    let epochs: Vec<Epoch> = (from..live_epoch).chain(previous).collect();

    let rows = read_sealed::<_, PoolEpochLog>(domain.archive(), chain, epochs, |_| {
        PoolKey(pool.operator)
    })?;

    let mut logged = 0;

    for (epoch, raw) in &rows {
        if *epoch >= from {
            let log = PoolEpochLog::decode_entity(PoolEpochLog::NS, raw)?;
            logged += log.blocks_minted;
        }
    }

    let mark = match previous {
        Some(previous) if !rows.contains_key(&previous) => pool
            .snapshot
            .mark()
            .map(|snapshot| u64::from(snapshot.blocks_minted))
            .unwrap_or_default(),
        _ => 0,
    };

    let live = pool
        .snapshot
        .live()
        .map(|snapshot| u64::from(snapshot.blocks_minted))
        .unwrap_or_default();

    let unlogged = match previous {
        Some(previous) => unlogged_blocks(domain, chain, &pool.operator, previous)?,
        None => 0,
    };

    Ok(logged + mark + live + unlogged)
}

/// The blocks each of `pools` minted over its life.
///
/// The epochs up to the entry's watermark come from the cache, and the rest
/// from the pool's rows and snapshots. An entry behind the current watermark
/// is extended in the background. With the namespace unregistered, or no
/// epoch immutable yet, every row is read.
pub async fn blocks_minted<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    pools: Vec<PoolState>,
) -> Result<Vec<u64>, DomainError> {
    let watermark = blocking(domain, {
        let chain = chain.clone();
        move |domain| {
            let (epoch, _) = chain.slot_epoch(tip_slot(domain)?);
            let mutable_slots = crate::mutable_slots(&domain.genesis());
            Ok(pool_blocks_watermark(&chain, mutable_slots, epoch))
        }
    })
    .await?;

    let entry = match watermark {
        Some(watermark) => {
            let fill = {
                let (domain, chain) = (domain.clone(), chain.clone());
                move || build_blocks_entry(&domain, &chain, watermark)
            };

            match domain
                .cache()
                .get_or_fill(POOL_BLOCKS.name, ENTRY, fill)
                .await
            {
                Ok(value) => Some(value),
                Err(CacheError::Fill(error)) => {
                    return Err(DomainError::Internal(format!("pool blocks: {error}")))
                }
                Err(CacheError::UnknownNamespace(_)) => None,
                Err(error) => {
                    tracing::warn!(%error, "pool blocks cache unavailable, reading uncached");
                    None
                }
            }
        }
        None => None,
    };

    let (blocks, stale) = blocking(domain, {
        let chain = chain.clone();

        move |domain| {
            let entry = entry.as_deref().map(BlocksEntry::decode).transpose()?;

            // an entry past the current watermark is newer than the tip; read
            // every row rather than trust it
            let usable = entry
                .as_ref()
                .filter(|entry| watermark.is_some_and(|watermark| entry.watermark <= watermark));

            let blocks = pools
                .iter()
                .map(|pool| match usable {
                    Some(entry) => {
                        let sealed = entry.blocks.get(&pool.operator).copied().unwrap_or(0);
                        let rest = unsealed_blocks(domain, &chain, pool, entry.watermark + 1)?;
                        Ok(sealed + rest)
                    }
                    None => unsealed_blocks(domain, &chain, pool, 0),
                })
                .collect::<Result<Vec<_>, ChainError>>()?;

            let stale = entry.filter(|entry| watermark.is_some_and(|w| entry.watermark < w));

            Ok((blocks, stale))
        }
    })
    .await?;

    if let (Some(stale), Some(watermark)) = (stale, watermark) {
        let extend = {
            let (domain, chain) = (domain.clone(), chain.clone());
            move || extend_blocks(&domain, &chain, Some(stale), watermark)
        };

        if let Err(error) = domain.cache().refill(POOL_BLOCKS.name, ENTRY, extend) {
            tracing::warn!(%error, "pool blocks cache entry not extended");
        }
    }

    Ok(blocks)
}

fn tip_slot<D: Domain>(domain: &D) -> Result<u64, ChainError> {
    Ok(domain
        .state()
        .read_cursor()?
        .map(|point| point.slot())
        .unwrap_or_default())
}

/// Run `f` on the blocking pool under the domain's query limiter.
async fn blocking<D, T, F>(domain: &D, f: F) -> Result<T, DomainError>
where
    D: Domain,
    T: Send + 'static,
    F: FnOnce(&D) -> Result<T, ChainError> + Send + 'static,
{
    let inner = domain.clone();

    Ok(domain
        .query_limiter()
        .run_blocking(move || f(&inner))
        .await??)
}

#[cfg(test)]
mod tests {
    use dolos_core::{
        config::{CacheConfig, StorageConfig},
        ArchiveWriter as _, ChainPoint, StateWriter as _,
    };
    use dolos_testing::toy_domain::ToyDomain;
    use pallas::ledger::primitives::conway::RationalNumber;

    use super::*;
    use crate::{EpochValue, PoolParams, PoolSnapshot};

    const POOL: [u8; 28] = [0x77; 28];
    const TIP: Epoch = 400;

    fn snapshot(blocks_minted: u32) -> PoolSnapshot {
        PoolSnapshot {
            is_retired: false,
            blocks_minted,
            params: PoolParams {
                vrf_keyhash: [0; 32].into(),
                pledge: 0,
                cost: 0,
                margin: RationalNumber {
                    numerator: 0,
                    denominator: 1,
                },
                reward_account: vec![0xe0; 29],
                pool_owners: vec![],
                relays: vec![],
                pool_metadata: None,
            },
            is_new: false,
            stake: None,
            delegators: None,
            live_pledge: None,
        }
    }

    /// A store with its tip in [`TIP`], `POOL`'s live snapshot there with
    /// `live` blocks and its `mark` with `mark`, and a `pool-epochs` row of
    /// `epoch % 7` blocks for each of `logged`. Answers the pool's lifetime
    /// blocks.
    fn seeded(logged: std::ops::Range<Epoch>, mark: u32, live: u32) -> (ToyDomain, u64) {
        seeded_with(None, logged, mark, live)
    }

    /// [`seeded`] on a store configured by `storage`.
    fn seeded_with(
        storage: Option<StorageConfig>,
        logged: std::ops::Range<Epoch>,
        mark: u32,
        live: u32,
    ) -> (ToyDomain, u64) {
        let domain = ToyDomain::new(None, storage);
        let chain = crate::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();

        let pool = PoolState {
            operator: POOL.into(),
            snapshot: EpochValue::from_parts(
                TIP,
                Some(snapshot(live)),
                None,
                Some(snapshot(mark)),
                None,
                None,
            ),
            retiring_epoch: None,
            deposit: 0,
            calidus: None,
        };

        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&PoolKey(POOL.into()), &pool)
            .unwrap();
        writer
            .set_cursor(ChainPoint::Slot(chain.epoch_start(TIP) + 1))
            .unwrap();
        writer.commit().unwrap();

        let mut total = u64::from(live);

        let writer = domain.archive().start_writer().unwrap();
        for epoch in logged.clone() {
            let log = PoolEpochLog {
                blocks_minted: epoch % 7,
                ..Default::default()
            };

            writer
                .write_log_typed(chain.epoch_start(epoch), &PoolKey(POOL.into()), &log)
                .unwrap();

            total += epoch % 7;
        }
        writer.commit().unwrap();

        if logged.end < TIP {
            total += u64::from(mark);
        }

        (domain, total)
    }

    async fn read(domain: &ToyDomain) -> (u64, u64) {
        let chain = crate::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
        let pool = domain
            .state()
            .read_entity_typed::<PoolState>(&PoolKey(POOL.into()))
            .unwrap()
            .unwrap();

        let uncached = unsealed_blocks(domain, &chain, &pool, 0).unwrap();
        let cached = blocks_minted(domain, &chain, vec![pool]).await.unwrap()[0];

        (uncached, cached)
    }

    /// The `mark` count joins the rows while RUPD has not logged the epoch
    /// before the live one, and the cached entry answers as the rows do.
    #[tokio::test]
    async fn cached_lifetime_blocks_equal_the_rows_and_snapshots() {
        for logged in [0..TIP, 0..TIP - 1] {
            let (domain, total) = seeded(logged.clone(), 5, 3);
            register_pool_caches(&domain).unwrap();

            assert_eq!(read(&domain).await, (total, total), "rows {logged:?}, fill");
            assert_eq!(read(&domain).await, (total, total), "rows {logged:?}, hit");

            let entry = domain
                .cache()
                .get(POOL_BLOCKS.name, ENTRY)
                .expect("entry kept");
            let entry = BlocksEntry::decode(&entry).unwrap();
            let chain = crate::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
            let mutable_slots = crate::mutable_slots(&domain.genesis());

            assert_eq!(
                Some(entry.watermark),
                pool_blocks_watermark(&chain, mutable_slots, TIP)
            );
            assert!(entry.watermark < TIP - 1, "the tail is read from the rows");
        }
    }

    /// With the cache off no entry would be kept, so every request would
    /// rebuild one for every pool: the namespaces stay unregistered, and a
    /// read sums the pool's own rows.
    #[tokio::test]
    async fn the_namespaces_stay_unregistered_while_the_cache_is_off() {
        let storage = StorageConfig {
            cache: CacheConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let (domain, total) = seeded_with(Some(storage), 0..TIP - 1, 5, 3);

        assert!(!register_pool_caches(&domain).unwrap());
        assert_eq!(read(&domain).await, (total, total));
        assert!(domain.cache().get(POOL_BLOCKS.name, ENTRY).is_none());
    }

    /// An entry extends from its own watermark, scanning only the rows after
    /// it, to what a fresh build reaches.
    #[test]
    fn an_extended_entry_equals_a_fresh_one() {
        let (domain, _) = seeded(0..TIP, 5, 3);
        let chain = crate::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();

        let fresh = build_blocks_entry(&domain, &chain, TIP - 10).unwrap();

        let old = build_blocks_entry(&domain, &chain, TIP - 50).unwrap();
        let old = BlocksEntry::decode(&old.value).unwrap();
        let extended = extend_blocks(&domain, &chain, Some(old), TIP - 10).unwrap();

        assert_eq!(extended.value, fresh.value);
        assert_eq!(
            extended.cost, 40,
            "one row for each epoch past the old entry"
        );
    }
}
