//! Request-scoped resolution of the chain position of UTxO-producing transactions.

use std::collections::HashMap;

use crate::{async_query::BlockRefMeta, AsyncQueryFacade, Domain, DomainError, TxHash};
use futures_util::future::join_all;
use itertools::Itertools as _;

/// Maximum number of metadata entries, including misses, retained between batches.
pub const MAX_BLOCK_META_DEPS: usize = 4096;

/// Resolves each distinct hash once per batch through one shared blocking limiter.
///
/// Cached hits and misses avoid subsequent fetches until eviction. The memo is
/// bounded; batch results are owned and complete even when larger than the memo.
/// Eviction only costs a later re-fetch. This does not impose a request-size cap.
pub struct BlockMetaResolver<D: Domain> {
    query: AsyncQueryFacade<D>,
    memo: HashMap<TxHash, Option<BlockRefMeta>>,
    cap: usize,
    fetches: usize,
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
        }
    }

    /// Number of distinct uncached hashes submitted for fetching, including misses.
    pub fn fetches(&self) -> usize {
        self.fetches
    }

    /// Return owned metadata for the requested hashes, omitting archive misses.
    ///
    /// Every hash is fetched at most once in this call, including batches larger
    /// than the memo capacity. Errors propagate without being cached as misses.
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
        let query = &self.query;
        let fetched = join_all(missing.into_iter().map(|hash| async move {
            query
                .block_meta_by_tx_hash(hash.to_vec())
                .await
                .map(|meta| (hash, meta))
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

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
            "resolved block metadata"
        );

        Ok(resolved)
    }
}
