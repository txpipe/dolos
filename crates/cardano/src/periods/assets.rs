//! Assets: what minibf reads of an asset — its supply, its mint and burn
//! count, its first mint and the tx that carries its metadata — derived from
//! the archive, since no state namespace keeps it.
//!
//! - Whether an asset exists, and the slot of its first mint, come from the
//!   archive's entity index for a sealed epoch and the staged `first-mints`
//!   rows for an unsealed one: the roll's first-mint witness (decision 0064).
//! - The rest is a fold over the asset's mints and burns: `asset-mint-chunks`
//!   where an epoch is sealed, the staged `asset-mints` rows where it is not.
//!   The newest mint whose tx's CIP-25 metadata names the asset is its metadata
//!   tx.
//! - A CIP-68 reference token (label 100) is also updated by the outputs that
//!   carry it with a datum, which are not mints, so its metadata tx comes from
//!   a newest-first walk of its `asset`-tagged blocks instead.
//!
//! The [`ASSETS`] cache namespace (decision 0059) keeps each asset's fold over
//! the epochs immutable when it was built; a read adds the epochs after them.

use std::{collections::BTreeMap, ops::RangeInclusive};

use dolos_core::{
    cache::Cost, config::HistoryMode, ArchiveStore as _, BlockSlot, CacheEntry, CacheError,
    CacheNamespace, CacheStore as _, ChainError, Domain, DomainError, Entity as _, RangeOf,
    StateStore as _,
};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
    ledger::{
        primitives::Epoch,
        traverse::{MultiEraBlock, MultiEraTx},
    },
};

use super::{is_sealable, stats::blocking, ChunkRow as _, GroupKey, CHUNK_LEN};
use crate::{
    cip25::{cip25_metadata_for_tx, cip25_mint_names_asset},
    cip68::{has_cip25_metadata, parse_cip67_label_from_asset_name},
    indexes::{archive_dimensions, entities},
    AssetKey, AssetMintChunk, AssetMintSet, ChainSummary, FirstMintSet, FixedNamespace as _,
    PolicyKey,
};

/// The cache namespace of asset folds: an asset's fold over its epochs up to
/// a watermark, keyed by its subject (`policy ‖ name`).
///
/// Keyed by subject rather than `AssetKey`: a fill walks a label-100 token's
/// `asset` tag, which is keyed by subject, and `dolos cache warm` rebuilds an
/// entry from its key alone.
///
/// Every entry is kept: a crawl reads each asset once, so a fold kept only
/// past a cost threshold would serve a crawl at the fold's cost.
pub const ASSETS: CacheNamespace = CacheNamespace::immutable("assets", 1);

/// Register [`ASSETS`], unless rolling history prunes the archive, whose
/// sealed mints it drops: there every read folds as without a cache. Answers
/// whether it registered.
pub fn register_assets_cache<D: Domain>(domain: &D) -> Result<bool, CacheError> {
    if domain.history_config().mode == HistoryMode::Rolling {
        return Ok(false);
    }

    domain.cache().register(ASSETS)?;

    Ok(true)
}

/// An asset's mints and burns folded over a run of epochs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub struct AssetFold {
    /// Mints less burns, big-endian.
    #[n(0)]
    #[cbor(with = "minicbor::bytes")]
    quantity: [u8; 16],

    /// Mints and burns.
    #[n(1)]
    pub mint_tx_count: u64,

    /// The first mint's tx.
    #[n(2)]
    pub initial_tx: Option<Hash<32>>,

    /// The newest tx that sets the asset's on-chain metadata.
    #[n(3)]
    pub metadata_tx: Option<Hash<32>>,
}

impl AssetFold {
    pub fn quantity(&self) -> i128 {
        i128::from_be_bytes(self.quantity)
    }

    fn add_mint(&mut self, mint: &AssetMintSet) {
        let quantity = self.quantity().saturating_add(i128::from(mint.amount));
        self.quantity = quantity.to_be_bytes();
        self.mint_tx_count += 1;
        self.initial_tx.get_or_insert(mint.tx_hash);

        if mint.names_asset {
            self.metadata_tx = Some(mint.tx_hash);
        }
    }

    /// This fold followed by `later`, the fold of the epochs after it.
    fn then(mut self, later: &AssetFold) -> Self {
        let quantity = self.quantity().saturating_add(later.quantity());
        self.quantity = quantity.to_be_bytes();
        self.mint_tx_count += later.mint_tx_count;
        self.initial_tx = self.initial_tx.or(later.initial_tx);
        self.metadata_tx = later.metadata_tx.or(self.metadata_tx);
        self
    }
}

/// What minibf reads of an asset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetSummary {
    /// The slot of the asset's first mint.
    pub initial_slot: BlockSlot,
    pub fold: AssetFold,
}

/// An `assets` entry: the fold over the asset's epochs from `start` up to
/// `watermark`.
///
/// Stored as the fold in its CBOR encoding, then the start, the watermark and
/// the cost, each a big-endian `u64`, so they read without decoding the fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetEntry {
    pub fold: AssetFold,
    /// The epoch of the first mint the fold started at. A read whose first
    /// mint is in another epoch, as after a rebuild corrects it, builds the
    /// entry afresh.
    pub start: Epoch,
    /// The last epoch the entry covers.
    pub watermark: Epoch,
    /// The sealed chunks and blocks the fold read.
    pub cost: Cost,
}

const TRAILER: usize = 24;

impl AssetEntry {
    fn into_cache_entry(self) -> CacheEntry {
        let mut value = minicbor::to_vec(&self.fold).expect("infallible encode");
        value.extend_from_slice(&self.start.to_be_bytes());
        value.extend_from_slice(&self.watermark.to_be_bytes());
        value.extend_from_slice(&self.cost.to_be_bytes());

        CacheEntry::new(value, self.cost)
    }

    pub fn decode(value: &[u8]) -> Result<Self, ChainError> {
        let (fold, [start, watermark, cost]) = split_trailer(value).ok_or_else(|| {
            minicbor::decode::Error::message("asset entry shorter than its trailer")
        })?;

        let mut d = minicbor::Decoder::new(fold);
        let decoded = AssetFold::decode(&mut d, &mut ())?;

        if d.position() != fold.len() {
            return Err(minicbor::decode::Error::message("trailing bytes in asset entry").into());
        }

        Ok(Self {
            fold: decoded,
            start,
            watermark,
            cost,
        })
    }

    /// The watermark of a stored entry, read from its trailer alone.
    pub fn watermark_of(value: &[u8]) -> Option<Epoch> {
        split_trailer(value).map(|(_, [_, watermark, _])| watermark)
    }

    /// A stored entry without its cost: what it answers. An entry extended
    /// across a label-100 update records the blocks of both walks, where one
    /// built afresh records those of the last.
    pub fn without_cost(value: &[u8]) -> Option<&[u8]> {
        split_trailer(value)?;
        Some(&value[..value.len() - 8])
    }
}

fn split_trailer(value: &[u8]) -> Option<(&[u8], [u64; 3])> {
    let at = value.len().checked_sub(TRAILER)?;
    let (fold, trailer) = value.split_at(at);
    let word = |i: usize| u64::from_be_bytes(trailer[i * 8..][..8].try_into().unwrap());

    Some((fold, [word(0), word(1), word(2)]))
}

/// The last epoch an `assets` entry may cover with the tip in epoch `tip`:
/// the newest epoch no rollback can reach. None while no epoch is past it.
///
/// Its mints are final whether or not their epoch has sealed yet: the fold
/// reads an epoch's sealed chunks where they exist and its staged rows where
/// they do not, so an entry never depends on how far the seal got.
pub fn assets_watermark<D: Domain>(domain: &D, chain: &ChainSummary, tip: Epoch) -> Option<Epoch> {
    let mutable_slots = crate::utils::mutable_slots(&domain.genesis());

    (0..tip)
        .rev()
        .find(|epoch| is_sealable(chain, mutable_slots, *epoch, tip))
}

/// The policy and key of a subject, `None` where it is too short to hold a
/// policy.
fn split_subject(subject: &[u8]) -> Option<(Hash<28>, AssetKey)> {
    let policy: [u8; 28] = subject.get(..28)?.try_into().ok()?;

    Some((policy.into(), AssetKey::of(subject)))
}

/// The slot of `subject`'s first mint, with the tip in epoch `tip`; `None`
/// for an asset never minted.
///
/// The entity index answers for a first mint in a sealed epoch, and the staged
/// `first-mints` rows for one in an epoch the last seal left staged
/// ([`super::staged_epochs`]). The seal writes the entry
/// before it deletes the row, so after a miss in the rows the index is read
/// again: an entry the seal committed between the two reads is found then.
pub fn first_mint_slot<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    tip: Epoch,
    subject: &[u8],
) -> Result<Option<BlockSlot>, ChainError> {
    let Some((policy, key)) = split_subject(subject) else {
        return Ok(None);
    };

    let indexed = || -> Result<Option<BlockSlot>, ChainError> {
        let slots = domain
            .archive()
            .entity_slots(entities::ASSET, &[key.0.as_slice()])?;

        Ok(slots.into_iter().next().flatten())
    };

    if let Some(slot) = indexed()? {
        return Ok(Some(slot));
    }

    let mutable_slots = crate::utils::mutable_slots(&domain.genesis());
    let ledger = crate::load_epoch::<D>(domain.state())?.number;

    let keys: Vec<GroupKey<PolicyKey, AssetKey>> =
        super::staged_epochs(chain, mutable_slots, ledger, tip)
            .map(|epoch| GroupKey {
                group: PolicyKey(policy),
                epoch,
                member: key,
            })
            .collect();

    let rows = domain
        .state()
        .read_entities_typed::<FirstMintSet>(&keys.iter().collect::<Vec<_>>())?;

    if let Some(row) = rows.into_iter().flatten().next() {
        return Ok(Some(row.slot));
    }

    indexed()
}

/// The asset's staged mints, by epoch, in chain order. Read before any sealed
/// chunk, so a seal running between the two reads leaves each epoch in one of
/// them.
fn read_staged_mints<D: Domain>(
    domain: &D,
    key: &AssetKey,
) -> Result<BTreeMap<Epoch, Vec<AssetMintSet>>, ChainError> {
    super::read_staged::<_, AssetMintSet>(domain.state(), RangeOf::prefix(key))
}

/// How far a fold walks a label-100 token's tagged blocks for its metadata
/// tx. A walk that finds none leaves the fold's metadata tx unset, so a merge
/// keeps the one before the walked epochs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Walk {
    /// No walk, for a read of the supply alone.
    Skip,
    /// At most this many blocks, for a read that answers a request.
    Upto(u64),
    /// Every block, for an entry the cache keeps.
    Whole,
}

/// `subject`'s fold over `epochs`, and the sealed chunks and blocks it read.
///
/// Per epoch the sealed chunks win, and `staged` holds the asset's staged
/// rows, read before them. A label-100 token takes its metadata tx from a walk
/// of the epochs' slots instead.
fn fold<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    subject: &[u8],
    epochs: RangeInclusive<Epoch>,
    staged: &BTreeMap<Epoch, Vec<AssetMintSet>>,
    walk: Walk,
) -> Result<(AssetFold, Cost), ChainError> {
    let mut out = AssetFold::default();

    if epochs.is_empty() {
        return Ok((out, 0));
    }

    let Some((policy, key)) = split_subject(subject) else {
        return Ok((out, 0));
    };

    let archive = domain.archive();
    let sealed =
        super::read_sealed::<_, AssetMintChunk>(archive, chain, epochs.clone(), |_| (key, 0))?;

    let mut cost = 0;

    for epoch in epochs.clone() {
        if let Some(first) = sealed.get(&epoch) {
            let first = AssetMintChunk::decode_entity(AssetMintChunk::NS, first)?;
            let chunks = first.count.div_ceil(CHUNK_LEN as u64) as u32;

            let mut rows = vec![first];
            rows.extend(super::load_chunks::<_, AssetMintChunk>(
                archive,
                chain,
                epoch,
                1..chunks,
                |i| (key, i),
            )?);

            cost += rows.len() as u64;

            for mint in rows.into_iter().flat_map(|row| row.into_items()) {
                out.add_mint(&mint);
            }
        } else if let Some(rows) = staged.get(&epoch) {
            for mint in rows {
                out.add_mint(mint);
            }
        }
    }

    let name = &subject[28..];

    if parse_cip67_label_from_asset_name(name) == Some(100) {
        let first = chain.epoch_start(*epochs.start());
        let last = chain.epoch_start(epochs.end() + 1) - 1;

        let (metadata_tx, blocks) =
            reference_metadata_tx(domain, subject, &policy, first..=last, walk)?;

        out.metadata_tx = metadata_tx;
        cost += blocks;
    }

    Ok((out, cost))
}

/// The newest tx in `slots` that sets a CIP-68 reference token's metadata, and
/// the blocks read to find it: an output carrying the token with a datum, or a
/// valid mint of it whose CIP-25 metadata names it. These are the roll's two
/// writers of an asset's metadata tx, so outputs of a phase-2-invalid tx are
/// its collateral return, as there.
fn reference_metadata_tx<D: Domain>(
    domain: &D,
    subject: &[u8],
    policy: &Hash<28>,
    slots: RangeInclusive<BlockSlot>,
    walk: Walk,
) -> Result<(Option<Hash<32>>, Cost), ChainError> {
    let limit = match walk {
        Walk::Skip => return Ok((None, 0)),
        Walk::Upto(limit) => limit,
        Walk::Whole => u64::MAX,
    };

    let archive = domain.archive();
    let name = &subject[28..];
    let mut blocks = 0;

    let tagged = archive.slots_by_tag(
        archive_dimensions::ASSET,
        subject,
        *slots.start(),
        *slots.end(),
    )?;

    for slot in tagged.rev() {
        if blocks == limit {
            tracing::warn!(
                subject = hex::encode(subject),
                blocks,
                "label-100 metadata walk stopped at its limit; the read keeps the metadata tx \
                 found before the walked epochs"
            );

            break;
        }

        let Some(body) = archive.get_block_by_slot(&slot?)? else {
            continue;
        };

        let block = MultiEraBlock::decode(&body).map_err(ChainError::DecodingError)?;
        blocks += 1;

        for tx in block.txs().iter().rev() {
            if sets_reference_metadata(tx, policy, name) {
                return Ok((Some(tx.hash()), blocks));
            }
        }
    }

    Ok((None, blocks))
}

fn sets_reference_metadata(tx: &MultiEraTx, policy: &Hash<28>, name: &[u8]) -> bool {
    let carried = tx.produces().iter().any(|(_, output)| {
        output.datum().is_some()
            && output.value().assets().iter().any(|assets| {
                assets.policy() == policy && assets.assets().iter().any(|x| x.name() == name)
            })
    });

    if carried {
        return true;
    }

    if !tx.is_valid() {
        return false;
    }

    let amounts: Vec<i64> = tx
        .mints()
        .iter()
        .filter(|mint| mint.policy() == policy)
        .flat_map(|mint| mint.assets())
        .filter(|asset| asset.name() == name)
        .map(|asset| asset.mint_coin().unwrap_or_default())
        .collect();

    if amounts.is_empty() {
        return false;
    }

    let metadata = if has_cip25_metadata(tx) {
        cip25_metadata_for_tx(tx)
    } else {
        None
    };

    amounts
        .into_iter()
        .any(|amount| cip25_mint_names_asset(metadata.as_ref(), policy, name, amount))
}

/// The `assets` entry of `subject` up to `watermark`, built without the cache,
/// from `start`, the epoch of its first mint. A miss builds through it, and so
/// does warming the cache.
pub fn build_asset_entry<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    subject: &[u8],
    start: Epoch,
    watermark: Epoch,
) -> Result<CacheEntry, ChainError> {
    let key = AssetKey::of(subject);
    let staged = read_staged_mints(domain, &key)?;
    let (fold, cost) = fold(
        domain,
        chain,
        subject,
        start..=watermark,
        &staged,
        Walk::Whole,
    )?;

    let entry = AssetEntry {
        fold,
        start,
        watermark,
        cost,
    };

    Ok(entry.into_cache_entry())
}

/// `value`, an entry of `subject`, extended to `watermark` by folding only the
/// epochs past its own. An entry past `watermark`, or one that started at
/// another epoch than `start`, is built afresh from `start`.
fn extend_asset_entry<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    subject: &[u8],
    value: &[u8],
    start: Epoch,
    watermark: Epoch,
) -> Result<CacheEntry, ChainError> {
    let entry = AssetEntry::decode(value)?;

    if entry.start != start || entry.watermark > watermark {
        return build_asset_entry(domain, chain, subject, start, watermark);
    }

    let key = AssetKey::of(subject);
    let staged = read_staged_mints(domain, &key)?;
    let from = entry.watermark + 1;
    let (rest, cost) = fold(
        domain,
        chain,
        subject,
        from..=watermark,
        &staged,
        Walk::Whole,
    )?;

    let entry = AssetEntry {
        fold: entry.fold.then(&rest),
        start,
        watermark,
        cost: entry.cost.saturating_add(cost),
    };

    Ok(entry.into_cache_entry())
}

/// What the first read of [`read`] learns, on the blocking pool.
struct Head {
    initial_slot: BlockSlot,
    start: Epoch,
    watermark: Option<Epoch>,
    staged: BTreeMap<Epoch, Vec<AssetMintSet>>,
    /// The fold past the watermark, or of every epoch where the first mint is
    /// past it.
    tail: AssetFold,
}

/// What minibf reads of `subject` with the tip in epoch `tip`, or `None` for
/// an asset never minted.
///
/// A label-100 token's metadata tx walks at most `walk` of the token's tagged
/// blocks past the cached epochs, newest first. If none of them sets it, the
/// read keeps the one the cached epochs found.
pub async fn asset_summary<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    tip: Epoch,
    subject: Vec<u8>,
    walk: u64,
) -> Result<Option<AssetSummary>, DomainError> {
    read(domain, chain, tip, subject, Walk::Upto(walk)).await
}

/// The supply of `subject` with the tip in epoch `tip`, or `None` for an
/// asset never minted: [`asset_summary`] without the label-100 walk, for the
/// lists, which report the supply alone.
pub async fn asset_quantity<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    tip: Epoch,
    subject: Vec<u8>,
) -> Result<Option<i128>, DomainError> {
    let summary = read(domain, chain, tip, subject, Walk::Skip).await?;

    Ok(summary.map(|x| x.fold.quantity()))
}

/// The epochs up to the entry's watermark come from the cache: a miss folds
/// them into the entry it offers. The epochs after it, the unsealed ones
/// included, fold as without a cache, and an entry behind the current
/// watermark is extended in the background. An entry that started at another
/// epoch than the first mint, or past the current watermark, is rebuilt in the
/// background, and the read folds as without a cache. With the namespace
/// unregistered, or the first mint past the watermark, the read folds as
/// without a cache. `walk` bounds every walk but the entry's.
///
/// Async, and never inside a blocking closure: a miss waits on a fill that
/// takes a query permit of its own.
async fn read<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    tip: Epoch,
    subject: Vec<u8>,
    walk: Walk,
) -> Result<Option<AssetSummary>, DomainError> {
    let head = blocking(domain, chain, {
        let subject = subject.clone();

        move |domain, chain| {
            let Some(initial_slot) = first_mint_slot(domain, chain, tip, &subject)? else {
                return Ok(None);
            };

            let (start, _) = chain.slot_epoch(initial_slot);
            let key = AssetKey::of(&subject);
            let staged = read_staged_mints(domain, &key)?;

            let watermark = assets_watermark(domain, chain, tip).filter(|w| *w >= start);
            let from = watermark.map_or(start, |w| w + 1);
            let (tail, _) = fold(domain, chain, &subject, from..=tip, &staged, walk)?;

            Ok(Some(Head {
                initial_slot,
                start,
                watermark,
                staged,
                tail,
            }))
        }
    })
    .await?;

    let Some(head) = head else {
        return Ok(None);
    };

    let summary = |fold| AssetSummary {
        initial_slot: head.initial_slot,
        fold,
    };

    let Some(watermark) = head.watermark else {
        return Ok(Some(summary(head.tail)));
    };

    let start = head.start;

    let fill = {
        let (domain, chain, subject) = (domain.clone(), chain.clone(), subject.clone());
        move || build_asset_entry(&domain, &chain, &subject, start, watermark)
    };

    let uncached = |staged: BTreeMap<Epoch, Vec<AssetMintSet>>, subject: Vec<u8>| {
        blocking(domain, chain, move |domain, chain| {
            Ok(fold(domain, chain, &subject, start..=tip, &staged, walk)?.0)
        })
    };

    let value = match domain
        .cache()
        .get_or_fill(ASSETS.name, &subject, fill)
        .await
    {
        Ok(value) => value,
        Err(CacheError::Fill(error)) => {
            return Err(DomainError::Internal(format!("asset fold: {error}")))
        }
        Err(CacheError::UnknownNamespace(_)) => {
            return Ok(Some(summary(uncached(head.staged, subject).await?)))
        }
        Err(error) => {
            tracing::warn!(%error, "assets cache unavailable, folding uncached");
            return Ok(Some(summary(uncached(head.staged, subject).await?)));
        }
    };

    let entry =
        AssetEntry::decode(&value).map_err(|error| DomainError::Internal(error.to_string()))?;

    let current = entry.start == start && entry.watermark == watermark;

    let fold = if current {
        entry.fold.then(&head.tail)
    } else if entry.start != start || entry.watermark > watermark {
        uncached(head.staged, subject.clone()).await?
    } else {
        let (staged, subject) = (head.staged, subject.clone());
        let entry = entry.clone();

        blocking(domain, chain, move |domain, chain| {
            let from = entry.watermark + 1;
            let (rest, _) = fold(domain, chain, &subject, from..=tip, &staged, walk)?;

            Ok(entry.fold.then(&rest))
        })
        .await?
    };

    if !current {
        let extend = {
            let (domain, chain, subject) = (domain.clone(), chain.clone(), subject.clone());
            move || extend_asset_entry(&domain, &chain, &subject, &value, start, watermark)
        };

        if let Err(error) = domain.cache().refill(ASSETS.name, &subject, extend) {
            tracing::warn!(%error, "assets cache entry not extended");
        }
    }

    Ok(Some(summary(fold)))
}

/// Every subject a sealed epoch first minted, in mint order: the keys a whole
/// `assets` namespace holds.
pub fn sealed_subjects<D: Domain>(domain: &D) -> Result<Vec<Vec<u8>>, ChainError> {
    let mut out = Vec::new();

    for row in domain
        .archive()
        .iter_logs_typed::<crate::FirstMintChunk>(None)?
    {
        let (_, _, chunk) = row?;
        out.extend(chunk.into_items());
    }

    Ok(out)
}

/// How many of `subjects` have no first mint in the entity index. A sealed
/// subject has one unless the seal that listed it predates the index.
pub fn count_unindexed<D: Domain>(domain: &D, subjects: &[Vec<u8>]) -> Result<usize, ChainError> {
    let mut missing = 0;

    for batch in subjects.chunks(1_000) {
        let keys: Vec<AssetKey> = batch.iter().map(|subject| AssetKey::of(subject)).collect();
        let keys: Vec<&[u8]> = keys.iter().map(|key| key.0.as_slice()).collect();

        missing += domain
            .archive()
            .entity_slots(entities::ASSET, &keys)?
            .iter()
            .filter(|slot| slot.is_none())
            .count();
    }

    Ok(missing)
}
