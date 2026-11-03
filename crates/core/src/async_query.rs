use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::Duration,
};

use futures_util::{stream, StreamExt as _};
use itertools::Itertools as _;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};

use crate::{
    archive::ArchiveStore, ArchiveError, BlockBody, BlockHash, BlockHeight, BlockSlot, ChainError,
    ChainPoint, Domain, DomainError, EraCbor, TagDimension, TxHash, TxOrder, UtxoPosition,
};

/// Lightweight block metadata for a transaction, extracted via a single decode.
///
/// Returned by `block_meta_by_tx_hash`. Callers that need the full block body
/// should use `block_by_tx_hash` instead.
#[derive(Debug, Clone)]
pub struct BlockRefMeta {
    pub slot: BlockSlot,
    pub hash: BlockHash,
    pub height: BlockHeight,
    pub tx_hash: TxHash,
    pub tx_index: TxOrder,
}

/// Maximum number of metadata entries, including misses, retained between
/// batches.
pub const MAX_BLOCK_META_DEPS: usize = 4096;

/// Resolves each distinct hash once per batch through one shared blocking
/// limiter.
///
/// Cached hits and misses avoid subsequent fetches until eviction. The memo is
/// bounded; batch results are owned and complete even when larger than the
/// memo. Eviction only costs a later re-fetch. This does not impose a
/// request-size cap.
pub struct BlockMetaResolver<D: Domain> {
    query: AsyncQueryFacade<D>,
    memo: HashMap<TxHash, Option<BlockRefMeta>>,
    cap: usize,
    fetches: usize,
    body_fetches: usize,
}

impl<D: Domain> BlockMetaResolver<D> {
    pub fn new(query: AsyncQueryFacade<D>) -> Self {
        Self::with_capacity(query, MAX_BLOCK_META_DEPS)
    }

    pub fn with_capacity(query: AsyncQueryFacade<D>, cap: usize) -> Self {
        Self {
            query,
            memo: HashMap::new(),
            cap: cap.max(1),
            fetches: 0,
            body_fetches: 0,
        }
    }

    /// Number of distinct uncached hashes submitted for fetching, including
    /// misses.
    pub fn fetches(&self) -> usize {
        self.fetches
    }

    /// Number of archive bodies read and decoded for metadata resolution.
    pub fn body_fetches(&self) -> usize {
        self.body_fetches
    }

    /// Return owned metadata for the requested hashes, omitting archive misses.
    ///
    /// Every hash is fetched at most once in this call, including batches
    /// larger than the memo capacity. Errors propagate without being cached
    /// as misses.
    pub async fn resolve_batch(
        &mut self,
        hashes: impl IntoIterator<Item = TxHash>,
    ) -> Result<HashMap<TxHash, BlockRefMeta>, DomainError> {
        let required: Vec<_> = hashes.into_iter().unique().collect();
        let mut resolved = HashMap::new();
        let mut missing = Vec::new();

        for hash in &required {
            if let Some(meta) = self.memo.get(hash) {
                if let Some(meta) = meta {
                    resolved.insert(*hash, meta.clone());
                }
            } else {
                missing.push(*hash);
            }
        }

        self.fetches += missing.len();
        let (fetched, body_fetches) = self.query.block_meta_by_tx_hashes(missing).await?;
        self.body_fetches += body_fetches;

        for (hash, meta) in fetched {
            if let Some(meta) = &meta {
                resolved.insert(hash, meta.clone());
            }
            if self.memo.len() == self.cap {
                self.memo.clear();
            }
            self.memo.insert(hash, meta);
        }

        tracing::debug!(
            deps = required.len(),
            held = self.memo.len(),
            fetches = self.fetches(),
            body_fetches = self.body_fetches(),
            "resolved block metadata"
        );

        Ok(resolved)
    }
}

/// Transaction hashes located per blocking task when a batch resolves its
/// slots. A heavy batch releases its permit between chunks, and a dropped one
/// stops at the next chunk.
pub const LOCATE_CHUNK: usize = 256;

/// Bounds the blocking query work of one process.
///
/// The domain carries one limiter and every [`AsyncQueryFacade`] built from it
/// runs its blocking closures under that limiter, whichever protocol started
/// them. A permit exists only inside a running blocking closure, so async code
/// never holds one while it waits for another.
///
/// A closure must never wait on async work: it would hold its permit while the
/// work it waits on may need another. The types cannot enforce this.
///
/// The limiter also admits heavy scans, the requests whose answer can read a
/// large part of a store: at most [`Self::heavy_limit`] at once across every
/// API, each waiting at most [`Self::heavy_queue`] for a slot. Work run inside
/// [`HeavyAdmission::scope`] holds at most [`Self::heavy_share`] permits in
/// all, so the rest stay free for every other request, and each blocking
/// closure it starts keeps the request's slot taken until it ends.
#[derive(Debug, Clone)]
pub struct QueryLimiter {
    permits: Arc<Semaphore>,
    cap: usize,
    heavy: Arc<HeavyScans>,
}

#[derive(Debug)]
struct HeavyScans {
    slots: Arc<Semaphore>,
    limit: usize,
    queue: Duration,
    share: Arc<Semaphore>,
    share_cap: usize,
}

tokio::task_local! {
    static HEAVY_SCAN: HeavyAdmission;
}

/// A heavy scan's slot, taken by [`QueryLimiter::admit_heavy`].
///
/// The slot is free again once the admission and every clone of it are
/// dropped. Each blocking closure started inside [`Self::scope`] holds a
/// clone, so a request dropped mid-scan (a deadline, a client gone) frees its
/// slot only when the work it started has ended.
#[derive(Debug, Clone)]
pub struct HeavyAdmission {
    slot: Arc<OwnedSemaphorePermit>,
}

impl HeavyAdmission {
    /// Run `f` as this heavy scan: its blocking query work counts against the
    /// heavy share and keeps the slot taken until it ends.
    pub async fn scope<F: Future>(self, f: F) -> F::Output {
        HEAVY_SCAN.scope(self, f).await
    }
}

fn limiter_closed() -> DomainError {
    DomainError::ArchiveError(ArchiveError::InternalError(
        "query limiter closed".to_string(),
    ))
}

impl QueryLimiter {
    pub const DEFAULT_CAP: usize = 32;
    pub const DEFAULT_HEAVY_LIMIT: usize = 8;
    pub const DEFAULT_HEAVY_QUEUE: Duration = Duration::from_secs(5);

    /// A limiter of `cap` permits, at least one, with the default heavy-scan
    /// admission.
    pub fn new(cap: usize) -> Self {
        Self::with_heavy_scans(cap, Self::DEFAULT_HEAVY_LIMIT, Self::DEFAULT_HEAVY_QUEUE)
    }

    /// A limiter of `cap` permits that admits `heavy_limit` heavy scans at
    /// once, at least one, each waiting at most `heavy_queue` for a slot.
    pub fn with_heavy_scans(cap: usize, heavy_limit: usize, heavy_queue: Duration) -> Self {
        let cap = cap.max(1);
        let heavy_limit = heavy_limit.max(1);
        let share_cap = (cap / 2).max(1);

        Self {
            permits: Arc::new(Semaphore::new(cap)),
            cap,
            heavy: Arc::new(HeavyScans {
                slots: Arc::new(Semaphore::new(heavy_limit)),
                limit: heavy_limit,
                queue: heavy_queue,
                share: Arc::new(Semaphore::new(share_cap)),
                share_cap,
            }),
        }
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Permits held by running closures.
    pub fn in_use(&self) -> usize {
        self.cap - self.permits.available_permits()
    }

    /// Blocking closures one request may run at once, so that a single
    /// request cannot take every permit.
    ///
    /// Inside a heavy scan of this limiter it is the heavy share divided
    /// among the heavy slots, at least one, so admitted scans together stay
    /// within the share.
    pub fn fan_out(&self) -> usize {
        let fan_out = (self.cap / 2).clamp(1, 16);

        if self.current_heavy_scan().is_some() {
            (self.heavy.share_cap / self.heavy.limit).clamp(1, fan_out)
        } else {
            fan_out
        }
    }

    /// Heavy scans admitted at once.
    pub fn heavy_limit(&self) -> usize {
        self.heavy.limit
    }

    /// The longest a heavy scan waits for a slot before it is refused.
    pub fn heavy_queue(&self) -> Duration {
        self.heavy.queue
    }

    /// Permits heavy scans hold at most, together: half the cap, at least
    /// one.
    pub fn heavy_share(&self) -> usize {
        self.heavy.share_cap
    }

    /// Heavy slots taken, by live requests and by blocking work that
    /// outlived its request.
    pub fn heavy_admitted(&self) -> usize {
        self.heavy.limit - self.heavy.slots.available_permits()
    }

    /// Permits held by heavy scans' running closures.
    pub fn heavy_in_use(&self) -> usize {
        self.heavy.share_cap - self.heavy.share.available_permits()
    }

    /// Take a heavy-scan slot, waiting at most [`Self::heavy_queue`] for one
    /// in arrival order; `None` once the wait runs out.
    pub async fn admit_heavy(&self) -> Option<HeavyAdmission> {
        let slots = self.heavy.slots.clone();

        let slot = if self.heavy.queue.is_zero() {
            slots.try_acquire_owned().ok()?
        } else {
            tokio::time::timeout(self.heavy.queue, slots.acquire_owned())
                .await
                .ok()?
                .ok()?
        };

        Some(HeavyAdmission {
            slot: Arc::new(slot),
        })
    }

    /// The heavy scan the current task runs under, when this limiter
    /// admitted it.
    fn current_heavy_scan(&self) -> Option<HeavyAdmission> {
        HEAVY_SCAN
            .try_with(HeavyAdmission::clone)
            .ok()
            .filter(|admission| Arc::ptr_eq(admission.slot.semaphore(), &self.heavy.slots))
    }

    /// Run `f` on the blocking pool once a permit is free. The permit moves
    /// into the closure and is returned when it ends, panics included.
    ///
    /// Inside a heavy scan the closure first takes a permit of the heavy
    /// share, and holds it and the scan's slot until it ends.
    pub async fn run_blocking<T, F>(&self, f: F) -> Result<T, DomainError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let heavy = self.current_heavy_scan();

        let share = match &heavy {
            Some(_) => Some(
                self.heavy
                    .share
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| limiter_closed())?,
            ),
            None => None,
        };

        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| limiter_closed())?;

        let handle = tokio::task::spawn_blocking(move || {
            let _held = (permit, share, heavy);
            f()
        });

        handle
            .await
            .map_err(|e| DomainError::ArchiveError(ArchiveError::InternalError(e.to_string())))
    }
}

impl Default for QueryLimiter {
    fn default() -> Self {
        Self::new(Self::DEFAULT_CAP)
    }
}

/// Pick the block a hash names out of the blocks recorded at one slot.
///
/// The index answers with a slot, and a slot usually holds one block, which is
/// then the answer without a decode. Where the chain put two blocks on one
/// slot — a Byron epoch-boundary block and the first main block of the epoch
/// it opens — the hash is what tells them apart.
///
/// A body that will not decode is not the block the hash names, so it is
/// passed over rather than failing the lookup: the caller asked for one block,
/// and the one it asked for may be the sibling.
fn pick_by_hash(candidates: Vec<BlockBody>, hash: &[u8]) -> Option<BlockBody> {
    if candidates.len() <= 1 {
        return candidates.into_iter().next();
    }

    candidates.into_iter().find(|body| {
        MultiEraBlock::decode(body).is_ok_and(|decoded| decoded.hash().as_ref() == hash)
    })
}

/// Async access to a domain's blocking queries, run under the domain's
/// [`QueryLimiter`].
#[derive(Clone)]
pub struct AsyncQueryFacade<D: Domain> {
    inner: D,
}

impl<D: Domain> AsyncQueryFacade<D>
where
    D: Clone + Send + Sync + 'static,
{
    pub fn new(inner: D) -> Self {
        Self { inner }
    }

    pub async fn run_blocking<T, F>(&self, f: F) -> Result<T, DomainError>
    where
        T: Send + 'static,
        F: FnOnce(D) -> Result<T, DomainError> + Send + 'static,
    {
        let inner = self.inner.clone();
        self.inner
            .query_limiter()
            .run_blocking(move || f(inner))
            .await?
    }

    pub async fn block_by_slot(&self, slot: BlockSlot) -> Result<Option<BlockBody>, DomainError> {
        self.run_blocking(move |domain| Ok(domain.archive().get_block_by_slot(&slot)?))
            .await
    }

    pub async fn block_by_hash(&self, hash: Vec<u8>) -> Result<Option<BlockBody>, DomainError> {
        self.run_blocking(move |domain| {
            let slot = domain.archive().slot_by_block_hash(&hash)?;
            match slot {
                Some(slot) => {
                    let candidates = domain.archive().get_blocks_by_slot(&slot)?;
                    Ok(pick_by_hash(candidates, &hash))
                }
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn block_by_number(&self, number: u64) -> Result<Option<BlockBody>, DomainError> {
        self.run_blocking(move |domain| {
            let slot = domain.archive().slot_by_block_number(number)?;
            match slot {
                Some(slot) => Ok(domain.archive().get_block_by_slot(&slot)?),
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn slot_by_number(&self, number: u64) -> Result<Option<BlockSlot>, DomainError> {
        self.run_blocking(move |domain| Ok(domain.archive().slot_by_block_number(number)?))
            .await
    }

    pub async fn block_by_tx_hash(
        &self,
        tx_hash: Vec<u8>,
    ) -> Result<Option<(BlockBody, TxOrder)>, DomainError> {
        let tx_hash_lookup = tx_hash.clone();
        let Some(raw) = self
            .run_blocking(move |domain| {
                let slot = domain.archive().slot_by_tx_hash(&tx_hash_lookup)?;
                let Some(slot) = slot else {
                    return Ok(None);
                };

                Ok(domain.archive().get_block_by_slot(&slot)?)
            })
            .await?
        else {
            return Ok(None);
        };

        let block = MultiEraBlock::decode(raw.as_slice())
            .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;
        if let Some((idx, _)) = block
            .txs()
            .iter()
            .enumerate()
            .find(|(_, tx)| tx.hash().to_vec() == tx_hash)
        {
            return Ok(Some((raw, idx)));
        }

        Ok(None)
    }

    /// Look up the block containing a given transaction hash and return only
    /// chain-point metadata, decoding the block once inside the blocking task.
    ///
    /// Prefer this over `block_by_tx_hash` when only the chain point is needed
    /// — it avoids a second `MultiEraBlock::decode` in the caller.
    pub async fn block_meta_by_tx_hash(
        &self,
        tx_hash: Vec<u8>,
    ) -> Result<Option<BlockRefMeta>, DomainError> {
        self.run_blocking(move |domain| {
            let Some(slot) = domain.archive().slot_by_tx_hash(&tx_hash)? else {
                return Ok(None);
            };
            let Some(raw) = domain.archive().get_block_by_slot(&slot)? else {
                return Ok(None);
            };
            let block = MultiEraBlock::decode(raw.as_slice())
                .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;
            let Some((tx_index, _)) = block
                .txs()
                .iter()
                .enumerate()
                .find(|(_, tx)| tx.hash().as_slice() == tx_hash.as_slice())
            else {
                return Ok(None);
            };
            Ok(Some(BlockRefMeta {
                slot: block.slot(),
                hash: block.hash(),
                height: block.number(),
                tx_hash: tx_hash.as_slice().into(),
                tx_index,
            }))
        })
        .await
    }

    /// Resolve transaction metadata while reading and decoding each selected
    /// archive body at most once.
    async fn block_meta_by_tx_hashes(
        &self,
        requested: Vec<TxHash>,
    ) -> Result<(Vec<(TxHash, Option<BlockRefMeta>)>, usize), DomainError> {
        self.extract_by_tx_hashes(requested, |block, tx_index, tx_hash, _| BlockRefMeta {
            slot: block.slot(),
            hash: block.hash(),
            height: block.number(),
            tx_hash,
            tx_index,
        })
        .await
    }

    /// Block metadata for transactions whose creation position is known,
    /// reading and decoding each slot's bodies once and no exact index.
    ///
    /// A transaction is left out when the archive no longer holds the block at
    /// its slot (pruned history), or holds none that carries it at its index.
    /// Slot groups run as one blocking task each, at most
    /// [`QueryLimiter::fan_out`] at a time.
    pub async fn block_meta_by_positions(
        &self,
        located: impl IntoIterator<Item = (TxHash, UtxoPosition)>,
    ) -> Result<HashMap<TxHash, BlockRefMeta>, DomainError> {
        let mut groups: HashMap<BlockSlot, HashSet<(TxHash, u16)>> = HashMap::new();
        for (tx_hash, position) in located {
            groups
                .entry(position.slot)
                .or_default()
                .insert((tx_hash, position.tx_index));
        }

        let query = self.clone();
        let tasks = stream::iter(groups.into_iter().map(move |(slot, txs)| {
            let query = query.clone();
            async move {
                query
                    .run_blocking(move |domain| {
                        let raws = domain.archive().get_blocks_by_slot(&slot)?;
                        let mut found = Vec::with_capacity(txs.len());
                        let mut decode_error = None;

                        for raw in raws {
                            let block = match MultiEraBlock::decode(raw.as_slice()) {
                                Ok(block) => block,
                                Err(error) => {
                                    decode_error.get_or_insert_with(|| {
                                        DomainError::ChainError(ChainError::DecodingError(error))
                                    });
                                    continue;
                                }
                            };

                            let block_txs = block.txs();

                            for (tx_hash, tx_index) in &txs {
                                let index = usize::from(*tx_index);
                                let carried = block_txs.get(index).map(MultiEraTx::hash);

                                if carried == Some(*tx_hash) {
                                    found.push(BlockRefMeta {
                                        slot: block.slot(),
                                        hash: block.hash(),
                                        height: block.number(),
                                        tx_hash: *tx_hash,
                                        tx_index: index,
                                    });
                                }
                            }
                        }

                        // A corrupt sibling cannot hide a transaction found in
                        // a valid body, but must not turn into a silent miss.
                        if found.len() != txs.len() {
                            if let Some(error) = decode_error {
                                return Err(error);
                            }
                        }

                        Ok(found)
                    })
                    .await
            }
        }))
        .buffer_unordered(self.inner.query_limiter().fan_out())
        .collect::<Vec<_>>()
        .await;

        // Await every started blocking task before propagating an error. A
        // dropped spawn_blocking handle does not cancel its underlying work.
        let mut resolved = HashMap::new();
        for task in tasks {
            for meta in task? {
                resolved.insert(meta.tx_hash, meta);
            }
        }

        Ok(resolved)
    }

    /// Fetch the CBOR of every requested transaction, reading and decoding
    /// each archive body that holds them at most once.
    ///
    /// Answers follow the order of the distinct requested hashes; a hash the
    /// archive does not hold answers `None`.
    pub async fn tx_cbor_batch(
        &self,
        tx_hashes: impl IntoIterator<Item = TxHash>,
    ) -> Result<Vec<(TxHash, Option<EraCbor>)>, DomainError> {
        let requested = tx_hashes.into_iter().unique().collect();
        let (fetched, _) = self
            .extract_by_tx_hashes(requested, |block, _, _, tx| {
                EraCbor(block.era().into(), tx.encode())
            })
            .await?;
        Ok(fetched)
    }

    /// Run `extract` over each requested transaction found in the archive,
    /// returning answers in the order of the distinct `requested` hashes along
    /// with the number of bodies read.
    ///
    /// Exact index lookups are retained for every distinct transaction hash,
    /// [`LOCATE_CHUNK`] hashes per blocking task, one chunk after another.
    /// Their slots are grouped after the last chunk, then each group uses one
    /// blocking task under the domain's limiter, at most
    /// [`QueryLimiter::fan_out`] at a time. Results are owned; decoded blocks
    /// and body buffers are dropped inside those bounded tasks.
    async fn extract_by_tx_hashes<T, F>(
        &self,
        requested: Vec<TxHash>,
        extract: F,
    ) -> Result<(Vec<(TxHash, Option<T>)>, usize), DomainError>
    where
        T: Clone + Send + 'static,
        F: Fn(&MultiEraBlock, TxOrder, TxHash, &MultiEraTx) -> T + Clone + Send + 'static,
    {
        let mut located = Vec::with_capacity(requested.len());
        for chunk in requested.chunks(LOCATE_CHUNK) {
            let chunk = chunk.to_vec();
            let slots = self
                .run_blocking(move |domain| {
                    chunk
                        .into_iter()
                        .map(|tx_hash| {
                            let slot = domain.archive().slot_by_tx_hash(tx_hash.as_slice())?;
                            Ok((tx_hash, slot))
                        })
                        .collect::<Result<Vec<_>, DomainError>>()
                })
                .await?;
            located.extend(slots);
        }

        let mut fetched = HashMap::new();
        let mut groups: HashMap<BlockSlot, Vec<TxHash>> = HashMap::new();
        for (tx_hash, slot) in located {
            if let Some(slot) = slot {
                groups.entry(slot).or_default().push(tx_hash);
            } else {
                fetched.insert(tx_hash, None);
            }
        }

        let mut body_fetches = 0;
        let query = self.clone();
        let tasks = stream::iter(groups.into_iter().map(move |(slot, tx_hashes)| {
            let query = query.clone();
            let extract = extract.clone();
            async move {
                query
                    .run_blocking(move |domain| {
                        let raws = domain.archive().get_blocks_by_slot(&slot)?;
                        let body_count = raws.len();
                        let mut found = HashMap::new();
                        let mut decode_error = None;
                        let requested: HashSet<_> = tx_hashes.iter().copied().collect();
                        for raw in raws {
                            let block = match MultiEraBlock::decode(raw.as_slice()) {
                                Ok(block) => block,
                                Err(error) => {
                                    decode_error.get_or_insert_with(|| {
                                        DomainError::ChainError(ChainError::DecodingError(error))
                                    });
                                    continue;
                                }
                            };
                            for (tx_index, tx) in block.txs().iter().enumerate() {
                                let tx_hash = tx.hash();
                                if requested.contains(&tx_hash) {
                                    found
                                        .entry(tx_hash)
                                        .or_insert_with(|| extract(&block, tx_index, tx_hash, tx));
                                }
                            }
                        }

                        // A corrupt sibling cannot hide a transaction found in
                        // a valid body, but an unresolved hash must not turn
                        // that corruption into a cached miss.
                        if found.len() != requested.len() {
                            if let Some(error) = decode_error {
                                return Err(error);
                            }
                        }

                        Ok((
                            tx_hashes
                                .into_iter()
                                .map(|tx_hash| {
                                    let value = found.get(&tx_hash).cloned();
                                    (tx_hash, value)
                                })
                                .collect::<Vec<_>>(),
                            body_count,
                        ))
                    })
                    .await
            }
        }))
        .buffer_unordered(self.inner.query_limiter().fan_out())
        .collect::<Vec<_>>()
        .await;

        // Await every started blocking task before propagating an error. A
        // dropped spawn_blocking handle does not cancel its underlying work.
        for task in tasks {
            let (entries, bodies) = task?;
            fetched.extend(entries);
            body_fetches += bodies;
        }

        let fetched = requested
            .into_iter()
            .map(|tx_hash| {
                let value = fetched.remove(&tx_hash).unwrap_or(None);
                (tx_hash, value)
            })
            .collect();
        Ok((fetched, body_fetches))
    }

    pub async fn tx_cbor(&self, tx_hash: Vec<u8>) -> Result<Option<EraCbor>, DomainError> {
        let tx_hash_lookup = tx_hash.clone();
        let Some(raw) = self
            .run_blocking(move |domain| {
                let slot = domain.archive().slot_by_tx_hash(&tx_hash_lookup)?;
                let Some(slot) = slot else {
                    return Ok(None);
                };

                Ok(domain.archive().get_block_by_slot(&slot)?)
            })
            .await?
        else {
            return Ok(None);
        };

        let block = MultiEraBlock::decode(raw.as_slice())
            .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;
        if let Some(tx) = block.txs().iter().find(|x| x.hash().to_vec() == tx_hash) {
            return Ok(Some(EraCbor(block.era().into(), tx.encode())));
        }

        Ok(None)
    }

    pub async fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: Vec<u8>,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, DomainError> {
        self.run_blocking(move |domain| {
            let slots = domain
                .archive()
                .slots_by_tag(dimension, &key, start_slot, end_slot)?
                .collect::<Result<Vec<_>, ArchiveError>>()?;
            Ok(slots)
        })
        .await
    }

    pub async fn find_intersect(
        &self,
        intersect: Vec<ChainPoint>,
    ) -> Result<Option<ChainPoint>, DomainError> {
        self.run_blocking(move |domain| Ok(domain.archive().find_intersect(&intersect)?))
            .await
    }
}

#[cfg(test)]
mod tests {
    use dolos_testing::blocks::{byron_ebb_slot, make_byron_ebb, make_conway_block_with_prev};

    use super::*;

    /// The case the plural read exists for: a Byron epoch-boundary block and
    /// the first main block of the epoch it opens share a slot, so resolving a
    /// hash through the index lands on both and only the hash tells them
    /// apart.
    #[test]
    fn a_shared_slot_is_resolved_by_hash() {
        let (ebb_point, ebb) = make_byron_ebb(1, pallas::crypto::hash::Hash::new([7u8; 32]));
        let (main_point, main) =
            make_conway_block_with_prev(byron_ebb_slot(1), ebb_point.hash(), 1);

        let candidates = vec![ebb.as_ref().clone(), main.as_ref().clone()];

        let ebb_hash = ebb_point.hash().unwrap();
        let main_hash = main_point.hash().unwrap();

        assert_eq!(
            pick_by_hash(candidates.clone(), ebb_hash.as_ref()),
            Some(ebb.as_ref().clone())
        );

        assert_eq!(
            pick_by_hash(candidates, main_hash.as_ref()),
            Some(main.as_ref().clone())
        );
    }

    /// A body that will not decode must not take its sibling down with it: the
    /// hash asked for here belongs to the block that is fine.
    #[test]
    fn an_undecodable_candidate_does_not_hide_its_sibling() {
        let (ebb_point, ebb) = make_byron_ebb(1, pallas::crypto::hash::Hash::new([7u8; 32]));
        let ebb_hash = ebb_point.hash().unwrap();

        let candidates = vec![b"not a block".to_vec(), ebb.as_ref().clone()];

        assert_eq!(
            pick_by_hash(candidates, ebb_hash.as_ref()),
            Some(ebb.as_ref().clone())
        );
    }

    #[test]
    fn a_limiter_keeps_at_least_one_permit() {
        assert_eq!(QueryLimiter::new(0).cap(), 1);
        assert_eq!(QueryLimiter::default().cap(), QueryLimiter::DEFAULT_CAP);
    }

    #[test]
    fn fan_out_is_half_the_cap_within_one_and_sixteen() {
        for (cap, fan_out) in [(1, 1), (2, 1), (32, 16), (64, 16)] {
            assert_eq!(QueryLimiter::new(cap).fan_out(), fan_out, "cap {cap}");
        }
    }

    #[tokio::test]
    async fn a_panicking_closure_returns_its_permit() {
        let limiter = QueryLimiter::new(1);

        let result = limiter.run_blocking(|| panic!("query panicked")).await;

        assert!(result.is_err());
        assert_eq!(limiter.in_use(), 0);
        assert_eq!(limiter.run_blocking(|| 7).await.unwrap(), 7);
    }

    const SHORT_QUEUE: Duration = Duration::from_millis(100);

    /// A blocking closure that runs until its gate opens, and tells when it
    /// has started.
    fn gated() -> (
        impl FnOnce() + Send + 'static,
        std::sync::mpsc::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (open, gate) = std::sync::mpsc::channel::<()>();
        let (started, running) = tokio::sync::oneshot::channel();

        let closure = move || {
            let _ = started.send(());
            let _ = gate.recv();
        };

        (closure, open, running)
    }

    #[test]
    fn heavy_defaults_are_eight_slots_a_five_second_queue_and_half_the_cap() {
        let limiter = QueryLimiter::default();

        assert_eq!(limiter.heavy_limit(), 8);
        assert_eq!(limiter.heavy_queue(), Duration::from_secs(5));
        assert_eq!(limiter.heavy_share(), 16);

        let small = QueryLimiter::with_heavy_scans(1, 0, Duration::ZERO);
        assert_eq!((small.heavy_limit(), small.heavy_share()), (1, 1));
    }

    #[tokio::test]
    async fn heavy_fan_out_divides_the_share_among_the_slots() {
        for (cap, heavy, light_fan_out, heavy_fan_out) in [
            (32, 8, 16, 2),
            (64, 4, 16, 8),
            (8, 8, 4, 1),
            (16, 2, 8, 4),
            (1, 1, 1, 1),
        ] {
            let limiter = QueryLimiter::with_heavy_scans(cap, heavy, SHORT_QUEUE);
            assert_eq!(limiter.fan_out(), light_fan_out, "cap {cap}");

            let admission = limiter.admit_heavy().await.expect("a free slot");
            let inside = admission.scope(async { limiter.fan_out() }).await;
            assert_eq!(inside, heavy_fan_out, "cap {cap}, heavy {heavy}");
        }
    }

    #[tokio::test]
    async fn another_limiters_heavy_scan_is_light_here() {
        let first = QueryLimiter::with_heavy_scans(8, 1, SHORT_QUEUE);
        let second = QueryLimiter::with_heavy_scans(8, 1, SHORT_QUEUE);

        let admission = first.admit_heavy().await.expect("a free slot");
        let fan_out = admission.scope(async { second.fan_out() }).await;

        assert_eq!(fan_out, 4);
    }

    #[tokio::test]
    async fn a_full_heavy_limit_refuses_after_its_queue() {
        let limiter = QueryLimiter::with_heavy_scans(32, 2, SHORT_QUEUE);
        let _held = [
            limiter.admit_heavy().await.expect("first slot"),
            limiter.admit_heavy().await.expect("second slot"),
        ];

        let started = std::time::Instant::now();
        assert!(limiter.admit_heavy().await.is_none());
        assert!(started.elapsed() >= SHORT_QUEUE, "{:?}", started.elapsed());
        assert_eq!(limiter.heavy_admitted(), 2);
    }

    #[tokio::test]
    async fn a_queued_heavy_scan_enters_when_a_slot_frees() {
        let limiter = QueryLimiter::with_heavy_scans(32, 1, Duration::from_secs(5));
        let held = limiter.admit_heavy().await.expect("first slot");

        let waiting = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.admit_heavy().await.is_some() }
        });

        tokio::time::sleep(SHORT_QUEUE).await;
        drop(held);

        assert!(waiting.await.unwrap());
    }

    #[tokio::test]
    async fn a_zero_queue_refuses_at_once() {
        let limiter = QueryLimiter::with_heavy_scans(32, 1, Duration::ZERO);
        let _held = limiter.admit_heavy().await.expect("first slot");

        assert!(limiter.admit_heavy().await.is_none());
    }

    /// A request dropped mid-scan (a deadline, a client gone) leaves its
    /// blocking closure running; the slot stays taken until it ends, so no
    /// replacement enters beside it.
    #[tokio::test]
    async fn a_dropped_heavy_scan_keeps_its_slot_until_its_closure_ends() {
        let limiter = QueryLimiter::with_heavy_scans(4, 1, SHORT_QUEUE);
        let (closure, open, running) = gated();

        let admission = limiter.admit_heavy().await.expect("a free slot");
        let request = tokio::spawn({
            let limiter = limiter.clone();
            admission.scope(async move { limiter.run_blocking(closure).await })
        });

        running.await.unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());

        assert_eq!(limiter.heavy_admitted(), 1);
        assert_eq!(limiter.heavy_in_use(), 1);
        assert_eq!(limiter.in_use(), 1);
        assert!(limiter.admit_heavy().await.is_none());

        open.send(()).unwrap();
        let replacement = limiter.admit_heavy().await;
        assert!(replacement.is_some());
        drop(replacement);

        assert_eq!(limiter.heavy_admitted(), 0);
        assert_eq!(limiter.heavy_in_use(), 0);
        assert_eq!(limiter.in_use(), 0);
    }

    /// Heavy scans that start more closures than their fan-out still hold at
    /// most the heavy share, and a light closure runs beside them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heavy_scans_hold_at_most_their_share() {
        for (cap, heavy) in [(8, 2), (32, 8), (6, 4)] {
            let limiter = QueryLimiter::with_heavy_scans(cap, heavy, SHORT_QUEUE);
            let share = limiter.heavy_share();
            let mut gates = Vec::new();
            let mut scans = Vec::new();

            for _ in 0..heavy {
                let admission = limiter.admit_heavy().await.expect("a free slot");
                let mut closures = Vec::new();

                for _ in 0..cap {
                    let (closure, open, _) = gated();
                    gates.push(open);
                    closures.push(closure);
                }

                let limiter = limiter.clone();
                scans.push(tokio::spawn(admission.scope(async move {
                    let runs = closures.into_iter().map(|c| limiter.run_blocking(c));
                    futures_util::future::join_all(runs).await
                })));
            }

            tokio::time::sleep(SHORT_QUEUE).await;
            assert_eq!(limiter.heavy_in_use(), share, "cap {cap}, heavy {heavy}");
            assert_eq!(limiter.in_use(), share, "cap {cap}, heavy {heavy}");

            let light = tokio::time::timeout(Duration::from_secs(5), limiter.run_blocking(|| 7));
            assert_eq!(light.await.expect("light closure waited").unwrap(), 7);

            for open in gates {
                let _ = open.send(());
            }

            for scan in scans {
                for result in scan.await.unwrap() {
                    result.unwrap();
                }
            }

            assert_eq!(limiter.heavy_in_use(), 0);
            assert_eq!(limiter.heavy_admitted(), 0);
        }
    }

    /// The ordinary slot holds one block and the index already named it, so
    /// the answer costs no decode — which an undecodable body is enough to
    /// show.
    #[test]
    fn a_lone_candidate_is_returned_undecoded() {
        let body = b"not a block".to_vec();

        assert_eq!(pick_by_hash(vec![body.clone()], &[0u8; 32]), Some(body));
    }
}
