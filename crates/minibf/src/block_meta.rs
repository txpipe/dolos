//! Request-scoped resolution of the chain position of UTxO-producing transactions.

use std::collections::HashMap;

use dolos_core::{async_query::BlockRefMeta, AsyncQueryFacade, Domain, DomainError, TxHash};
use futures::future::join_all;
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dolos_core::async_query::AsyncQueryOptions;
    use dolos_testing::{
        faults::{FaultyToyDomain, TestFault},
        synthetic::SyntheticBlockConfig,
        toy_domain::ToyDomain,
    };

    use crate::test_support::TestDomainBuilder;

    use super::*;

    #[tokio::test]
    async fn deduplicates_hits_and_misses_across_batches() {
        let (domain, vectors) =
            TestDomainBuilder::new_with_synthetic(SyntheticBlockConfig::default()).finish();
        let known: TxHash = vectors.tx_hash.parse().unwrap();
        let absent = TxHash::from([0xff; 32]);
        let expected = AsyncQueryFacade::new(domain.clone())
            .block_meta_by_tx_hash(known.to_vec())
            .await
            .unwrap()
            .unwrap();
        let mut resolver = BlockMetaResolver::new(AsyncQueryFacade::new(domain));

        for _ in 0..2 {
            let result = resolver
                .resolve_batch([known, absent, known, absent])
                .await
                .unwrap();
            assert_eq!(result.len(), 1);
            let actual = &result[&known];
            assert_eq!(actual.slot, expected.slot);
            assert_eq!(actual.hash, expected.hash);
            assert_eq!(actual.height, expected.height);
            assert_eq!(actual.tx_hash, expected.tx_hash);
            assert_eq!(actual.tx_index, expected.tx_index);
            assert_eq!(resolver.fetches(), 2);
        }
        assert!(resolver.resolve_batch([]).await.unwrap().is_empty());
        assert_eq!(resolver.fetches(), 2);
    }

    #[tokio::test]
    async fn oversized_batch_fetches_each_hash_once_and_bounds_memo() {
        let (domain, vectors) =
            TestDomainBuilder::new_with_synthetic(SyntheticBlockConfig::default()).finish();
        let known: TxHash = vectors.tx_hash.parse().unwrap();
        let absent = TxHash::from([0xff; 32]);
        let mut resolver = BlockMetaResolver::with_capacity(AsyncQueryFacade::new(domain), 1);

        let result = resolver
            .resolve_batch([known, absent, known, absent])
            .await
            .unwrap();
        assert_eq!(result[&known].tx_hash, known);
        assert_eq!(resolver.fetches(), 2);
        assert_eq!(resolver.memo.len(), 1);
        assert!(resolver.resolve_batch([absent]).await.unwrap().is_empty());
        assert_eq!(resolver.fetches(), 2);
        let refetched = resolver.resolve_batch([known]).await.unwrap();
        assert_eq!(refetched[&known].slot, result[&known].slot);
        assert_eq!(resolver.fetches(), 3);
        assert_eq!(resolver.memo.len(), 1);
    }

    #[tokio::test]
    async fn archive_errors_are_not_cached_as_misses() {
        let domain = FaultyToyDomain::new(ToyDomain::new(None, None), TestFault::ArchiveStoreError);
        let mut resolver = BlockMetaResolver::new(AsyncQueryFacade::new(domain));
        let hash = TxHash::from([0xff; 32]);

        for expected_fetches in 1..=2 {
            assert!(resolver.resolve_batch([hash, hash]).await.is_err());
            assert!(resolver.memo.is_empty());
            assert_eq!(resolver.fetches(), expected_fetches);
        }
    }

    #[tokio::test]
    async fn all_fetches_use_the_supplied_query_limiter() {
        let query = AsyncQueryFacade::with_options(
            ToyDomain::new(None, None),
            AsyncQueryOptions { max_blocking: 1 },
        );
        let mut resolver = BlockMetaResolver::new(query.clone());
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, wait_for_release) = std::sync::mpsc::channel();
        let holder = tokio::spawn(async move {
            query
                .run_blocking(move |_| {
                    entered.send(()).unwrap();
                    wait_for_release
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                    Ok(())
                })
                .await
                .unwrap();
        });
        started.await.unwrap();

        let pending = resolver.resolve_batch((1..=32).map(|value| TxHash::from([value; 32])));
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), pending.as_mut())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        assert!(pending.await.unwrap().is_empty());
        holder.await.unwrap();
    }
}
