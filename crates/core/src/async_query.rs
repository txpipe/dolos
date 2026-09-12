use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use futures_util::{stream, StreamExt as _};
use itertools::Itertools as _;

use tokio::sync::Semaphore;

use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    archive::ArchiveStore, ArchiveError, BlockBody, BlockHash, BlockHeight, BlockSlot, ChainError,
    ChainPoint, Domain, DomainError, EraCbor, TagDimension, TxHash, TxOrder,
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

#[derive(Debug, Clone)]
pub struct AsyncQueryOptions {
    pub max_blocking: usize,
}

impl Default for AsyncQueryOptions {
    fn default() -> Self {
        Self { max_blocking: 16 }
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

#[derive(Clone)]
pub struct AsyncQueryFacade<D: Domain> {
    inner: D,
    limiter: Arc<Semaphore>,
    options: AsyncQueryOptions,
}

impl<D: Domain> AsyncQueryFacade<D>
where
    D: Clone + Send + Sync + 'static,
{
    pub fn new(inner: D) -> Self {
        Self::with_options(inner, AsyncQueryOptions::default())
    }

    pub fn with_options(inner: D, options: AsyncQueryOptions) -> Self {
        let options = AsyncQueryOptions {
            max_blocking: options.max_blocking.max(1),
        };
        let limiter = Arc::new(Semaphore::new(options.max_blocking));
        Self {
            inner,
            limiter,
            options,
        }
    }

    pub fn options(&self) -> &AsyncQueryOptions {
        &self.options
    }

    pub async fn run_blocking<T, F>(&self, f: F) -> Result<T, DomainError>
    where
        T: Send + 'static,
        F: FnOnce(D) -> Result<T, DomainError> + Send + 'static,
    {
        let permit = self.limiter.clone().acquire_owned().await.map_err(|_| {
            DomainError::ArchiveError(ArchiveError::InternalError(
                "query limiter closed".to_string(),
            ))
        })?;
        let inner = self.inner.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(inner)
        });

        handle
            .await
            .map_err(|e| DomainError::ArchiveError(ArchiveError::InternalError(e.to_string())))?
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
    ///
    /// Exact index lookups are retained for every distinct transaction hash.
    /// Their slots are grouped before body work starts, then each group uses
    /// one blocking task under the facade's shared limiter. Results are owned;
    /// decoded blocks and body buffers are dropped inside those bounded tasks.
    async fn block_meta_by_tx_hashes(
        &self,
        tx_hashes: Vec<TxHash>,
    ) -> Result<(Vec<(TxHash, Option<BlockRefMeta>)>, usize), DomainError> {
        let requested = tx_hashes.clone();
        let located = self
            .run_blocking(move |domain| {
                tx_hashes
                    .into_iter()
                    .map(|tx_hash| {
                        let slot = domain.archive().slot_by_tx_hash(tx_hash.as_slice())?;
                        Ok((tx_hash, slot))
                    })
                    .collect::<Result<Vec<_>, DomainError>>()
            })
            .await?;

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
            async move {
                query
                    .run_blocking(move |domain| {
                        let raws = domain.archive().get_blocks_by_slot(&slot)?;
                        let body_count = raws.len();
                        let mut positions = HashMap::new();
                        let requested: HashSet<_> = tx_hashes.iter().copied().collect();
                        for raw in raws {
                            let block = MultiEraBlock::decode(raw.as_slice()).map_err(|error| {
                                DomainError::ChainError(ChainError::DecodingError(error))
                            })?;
                            for (tx_index, tx) in block.txs().iter().enumerate() {
                                let tx_hash = tx.hash();
                                if requested.contains(&tx_hash) {
                                    positions.entry(tx_hash).or_insert_with(|| BlockRefMeta {
                                        slot: block.slot(),
                                        hash: block.hash(),
                                        height: block.number(),
                                        tx_hash,
                                        tx_index,
                                    });
                                }
                            }
                        }

                        Ok((
                            tx_hashes
                                .into_iter()
                                .map(|tx_hash| {
                                    let meta = positions.get(&tx_hash).cloned();
                                    (tx_hash, meta)
                                })
                                .collect::<Vec<_>>(),
                            body_count,
                        ))
                    })
                    .await
            }
        }))
        .buffer_unordered(self.options.max_blocking)
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
                let meta = fetched.remove(&tx_hash).unwrap_or(None);
                (tx_hash, meta)
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

    /// The ordinary slot holds one block and the index already named it, so
    /// the answer costs no decode — which an undecodable body is enough to
    /// show.
    #[test]
    fn a_lone_candidate_is_returned_undecoded() {
        let body = b"not a block".to_vec();

        assert_eq!(pick_by_hash(vec![body.clone()], &[0u8; 32]), Some(body));
    }
}
