//! Shared resolution of the source output behind a tx input.
//!
//! Several endpoints scan a range of blocks and, for every tx in them, need the
//! output each input consumes (`/addresses/{address}/transactions`,
//! `/addresses/{address}/txs`, `/addresses/{address}/total`,
//! `/assets/{subject}/transactions`). Resolving an input means fetching the
//! CBOR of the tx that produced it and reading the output at the input's index.
//!
//! Done naively that is one fetch plus one full decode per input, so a
//! dependency tx referenced by N inputs is paid for N times. This module keeps
//! that work to once per distinct dependency hash:
//!
//! - [`InputCache`] owns the fetched bytes for the whole request. Its
//!   [`prepare`](InputCache::prepare) batches the distinct dependency hashes of
//!   a block's txs into a single round of deduped fetches (via
//!   [`Facade::get_tx_batch`]) and only asks for hashes it does not already
//!   hold, so a dependency shared across blocks is fetched once per request.
//!   Misses (genesis or pruned txs) are cached too, so they are not retried.
//! - [`InputResolver`] borrows those bytes and memoizes the decoding, handing
//!   out transient [`MultiEraOutput`] borrows tied to `&mut self` — the same
//!   ownership constraint `TxModelBuilder` manages for the `/txs/{hash}/*`
//!   endpoints.
//!
//! The cache is request-scoped and bounded: it is pure memoization, so when it
//! would grow past [`MAX_CACHED_DEPS`] entries it is cleared and the entries
//! still needed are fetched again. That caps memory on a full-history scan of a
//! high-activity address.

use std::collections::HashMap;

use axum::http::StatusCode;
use itertools::Itertools as _;
use pallas::ledger::traverse::{MultiEraInput, MultiEraOutput, MultiEraTx};

use dolos_core::{Domain, EraCbor, TxHash};

use crate::{Facade, TxMap};

/// Maximum number of dependency txs held by an [`InputCache`] at once.
///
/// Reaching it clears the cache: entries are pure memoization, so eviction only
/// ever costs a re-fetch.
pub const MAX_CACHED_DEPS: usize = 4096;

/// Request-scoped, bounded store of the dependency txs consumed by the blocks
/// being scanned.
pub struct InputCache {
    txs: TxMap,
    cap: usize,
    fetches: usize,
}

impl Default for InputCache {
    fn default() -> Self {
        Self::with_capacity(MAX_CACHED_DEPS)
    }
}

impl InputCache {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            txs: TxMap::new(),
            cap: cap.max(1),
            fetches: 0,
        }
    }

    /// Number of dependency txs fetched from the domain so far.
    ///
    /// One per distinct hash that was not already cached — the dedup this
    /// module exists for is observable through this counter.
    pub fn fetches(&self) -> usize {
        self.fetches
    }

    /// Fetch whatever is missing to resolve the inputs of `txs`, then hand out
    /// a resolver over the cached bytes.
    ///
    /// The resolver borrows the cache, so it must be dropped before the next
    /// block is prepared; the fetched bytes survive it.
    pub async fn prepare<'a, 'b, 'tx, D>(
        &'a mut self,
        domain: &Facade<D>,
        txs: impl IntoIterator<Item = &'b MultiEraTx<'tx>>,
    ) -> Result<InputResolver<'a>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
        'tx: 'b,
    {
        let required: Vec<TxHash> = txs
            .into_iter()
            .flat_map(|tx| {
                tx.consumes()
                    .iter()
                    .map(|input| *input.hash())
                    .collect::<Vec<_>>()
            })
            .unique()
            .collect();

        let required_len = required.len();

        let mut missing: Vec<TxHash> = required
            .iter()
            .filter(|hash| !self.txs.contains_key(*hash))
            .copied()
            .collect();

        // the cache is pure memoization: dropping it all is always safe, as
        // long as everything this batch needs is fetched again right after.
        if !missing.is_empty() && self.txs.len() + missing.len() > self.cap {
            self.txs.clear();
            missing = required;
        }

        if !missing.is_empty() {
            self.fetches += missing.len();
            let fetched = domain.get_tx_batch(missing).await?;
            self.txs.extend(fetched);
        }

        tracing::debug!(
            deps = required_len,
            cached = self.txs.len(),
            fetches = self.fetches(),
            "prepared input dependencies"
        );

        Ok(InputResolver::new(&self.txs))
    }
}

/// Decodes each dependency tx at most once and resolves inputs against it.
pub struct InputResolver<'a> {
    txs: &'a TxMap,
    decoded: HashMap<TxHash, Option<MultiEraTx<'a>>>,
    decodes: usize,
}

impl<'a> InputResolver<'a> {
    fn new(txs: &'a TxMap) -> Self {
        Self {
            txs,
            decoded: HashMap::new(),
            decodes: 0,
        }
    }

    /// Number of dependency txs decoded so far — one per distinct hash
    /// actually resolved, however many inputs point at it.
    pub fn decodes(&self) -> usize {
        self.decodes
    }

    /// The output `input` consumes, or `None` when the source tx is not
    /// available (genesis or pruned) or has no output at that index.
    ///
    /// The borrow is transient: it ends before the next call.
    pub fn resolve(
        &mut self,
        input: &MultiEraInput<'_>,
    ) -> Result<Option<MultiEraOutput<'_>>, StatusCode> {
        let hash = *input.hash();

        if !self.decoded.contains_key(&hash) {
            let source = self.txs;

            let decoded = match source.get(&hash) {
                Some(Some(EraCbor(era, cbor))) => {
                    let era = (*era).try_into().map_err(|err| {
                        tracing::error!(error = ?err, "unknown era for dependency tx");
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?;

                    let tx = MultiEraTx::decode_for_era(era, cbor).map_err(|err| {
                        tracing::error!(error = ?err, "failed to decode dependency tx");
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?;

                    self.decodes += 1;

                    tracing::trace!(%hash, decodes = self.decodes(), "decoded dependency tx");

                    Some(tx)
                }
                // known miss: the source tx is not in the archive
                Some(None) => None,
                // not prepared: treated as a miss, but it means a caller
                // resolved inputs of a tx it never handed to `prepare`
                None => {
                    debug_assert!(false, "resolving an input that was never prepared");
                    tracing::warn!(%hash, "dependency tx was not prepared, treating as missing");
                    None
                }
            };

            self.decoded.insert(hash, decoded);
        }

        let output = self
            .decoded
            .get(&hash)
            .and_then(|tx| tx.as_ref())
            .and_then(|tx| tx.produces_at(input.index() as usize));

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use dolos_core::config::MinibfConfig;
    use dolos_testing::{
        synthetic::{build_synthetic_blocks, SyntheticBlockConfig, SyntheticVectors},
        toy_domain::ToyDomain,
    };
    use pallas::ledger::traverse::{Era, MultiEraBlock};

    use crate::test_support::TestDomainBuilder;

    use super::*;

    fn synthetic_cfg() -> SyntheticBlockConfig {
        SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 2,
            ..Default::default()
        }
    }

    fn vectors() -> SyntheticVectors {
        let (_, vectors, _) = build_synthetic_blocks(synthetic_cfg());
        vectors
    }

    fn facade() -> (Facade<ToyDomain>, SyntheticVectors) {
        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(synthetic_cfg()).finish();

        let facade = Facade {
            inner: domain,
            config: MinibfConfig::new("[::]:0".parse().expect("invalid listen address")),
            cache: crate::cache::CacheService::default(),
        };

        (facade, vectors)
    }

    fn dep_cbor(vectors: &SyntheticVectors) -> EraCbor {
        EraCbor(Era::Conway.into(), vectors.tx_cbor.clone())
    }

    #[test]
    fn decodes_each_dependency_once() {
        let vectors = vectors();

        let tx = MultiEraTx::decode_for_era(Era::Conway, &vectors.tx_cbor).expect("decodable tx");

        let inputs = tx.consumes();
        let input = inputs.first().expect("synthetic tx consumes an input");

        // pretend the source of the input is the synthetic tx itself
        let mut txs = TxMap::new();
        txs.insert(*input.hash(), Some(dep_cbor(&vectors)));

        let mut resolver = InputResolver::new(&txs);

        for _ in 0..5 {
            let output = resolver.resolve(input).expect("resolve failed");
            assert!(output.is_some(), "expected the source output");
        }

        assert_eq!(
            resolver.decodes(),
            1,
            "the dependency tx must be decoded exactly once"
        );
    }

    #[test]
    fn caches_misses() {
        let vectors = vectors();

        let tx = MultiEraTx::decode_for_era(Era::Conway, &vectors.tx_cbor).expect("decodable tx");

        let inputs = tx.consumes();
        let input = inputs.first().expect("synthetic tx consumes an input");

        let mut txs = TxMap::new();
        txs.insert(*input.hash(), None);

        let mut resolver = InputResolver::new(&txs);

        for _ in 0..5 {
            let output = resolver.resolve(input).expect("resolve failed");
            assert!(output.is_none(), "a missing source tx resolves to nothing");
        }

        assert_eq!(resolver.decodes(), 0, "nothing to decode for a known miss");
    }

    #[tokio::test]
    async fn fetches_once_per_distinct_dependency() {
        let (domain, vectors) = facade();

        let tx = MultiEraTx::decode_for_era(Era::Conway, &vectors.tx_cbor).expect("decodable tx");

        let mut cache = InputCache::default();

        // three txs consuming the same dependency: one fetch, not three
        let scanned = [&tx, &tx, &tx];
        let inputs: usize = scanned.iter().map(|tx| tx.consumes().len()).sum();
        assert_eq!(inputs, 3);

        {
            let _resolver = cache.prepare(&domain, scanned).await.expect("prepare");
        }

        assert_eq!(cache.fetches(), 1, "one fetch per distinct dependency hash");

        // a later block consuming an already-cached dependency: no new fetch
        {
            let _resolver = cache.prepare(&domain, [&tx]).await.expect("prepare");
        }

        assert_eq!(cache.fetches(), 1, "cached dependencies are not refetched");
    }

    #[tokio::test]
    async fn eviction_keeps_the_current_batch_resolvable() {
        let (domain, _) = facade();

        let (blocks, _, _) = build_synthetic_blocks(synthetic_cfg());
        let raw = blocks.first().expect("at least one synthetic block");
        let block = MultiEraBlock::decode(raw).expect("decodable block");

        let txs = block.txs();
        let (first, second) = (&txs[0], &txs[1]);

        let first_inputs = first.consumes();
        let second_inputs = second.consumes();

        assert_ne!(
            first_inputs[0].hash(),
            second_inputs[0].hash(),
            "the two txs must depend on different source txs"
        );

        // a cache too small to hold anything alongside the current batch
        let mut cache = InputCache::with_capacity(1);

        {
            let _resolver = cache.prepare(&domain, [first]).await.expect("prepare");
        }

        assert_eq!(cache.fetches(), 1);

        {
            // this batch needs a dependency the full cache does not hold, so
            // the cache is cleared and the batch fetched again
            let mut resolver = cache.prepare(&domain, [second]).await.expect("prepare");

            // whatever eviction did, every input of the prepared batch is
            // still answerable without hitting the unprepared path
            resolver.resolve(&second_inputs[0]).expect("resolve failed");
        }

        assert_eq!(cache.fetches(), 2, "the evicting batch is fetched again");
    }
}
