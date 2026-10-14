use std::{sync::atomic::Ordering, time::Duration};

use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    measured::MeasuredStores,
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::{MemoryStores, ToyDomain, ToyStores as _},
};

use dolos_core::{
    async_query::BlockMetaResolver, config::CardanoConfig, indexes::ArchiveIndexDelta,
    ArchiveStore as _, ArchiveWriter as _, AsyncQueryFacade, ChainPoint, Domain, QueryLimiter,
    TxHash, LOCATE_CHUNK,
};
use pallas::ledger::traverse::MultiEraBlock;

fn domain_with_block() -> (ToyDomain, TxHash) {
    let domain = ToyDomain::new(None, None);
    let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig::default());
    let raw = &blocks[0];
    let block = MultiEraBlock::decode(raw).unwrap();
    let known = block.txs()[0].hash();
    let writer = domain.archive().start_writer().unwrap();
    writer
        .apply(&ChainPoint::Specific(block.slot(), block.hash()), raw)
        .unwrap();
    writer
        .apply_index(&[ArchiveIndexDelta {
            slot: block.slot(),
            block_hash: block.hash().to_vec(),
            block_number: Some(block.number()),
            tx_hashes: block.txs().iter().map(|tx| tx.hash().to_vec()).collect(),
            tags: Vec::new(),
        }])
        .unwrap();
    writer.commit().unwrap();
    (domain, known)
}

fn domain_with_shared_blocks(
    block_count: usize,
    txs_per_block: usize,
) -> (ToyDomain, Vec<Vec<TxHash>>) {
    let domain = ToyDomain::new(None, None);
    let hashes = load_shared_blocks(&domain, block_count, txs_per_block);
    (domain, hashes)
}

fn load_shared_blocks<D: Domain>(
    domain: &D,
    block_count: usize,
    txs_per_block: usize,
) -> Vec<Vec<TxHash>> {
    let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count,
        txs_per_block,
        ..Default::default()
    });
    let writer = domain.archive().start_writer().unwrap();
    let mut hashes = Vec::new();
    let mut indexes = Vec::new();
    for raw in &blocks {
        let block = MultiEraBlock::decode(raw).unwrap();
        let tx_hashes: Vec<_> = block.txs().iter().map(|tx| tx.hash()).collect();
        writer
            .apply(&ChainPoint::Specific(block.slot(), block.hash()), raw)
            .unwrap();
        indexes.push(ArchiveIndexDelta {
            slot: block.slot(),
            block_hash: block.hash().to_vec(),
            block_number: Some(block.number()),
            tx_hashes: tx_hashes.iter().map(|hash| hash.to_vec()).collect(),
            tags: Vec::new(),
        });
        hashes.push(tx_hashes);
    }
    writer.apply_index(&indexes).unwrap();
    writer.commit().unwrap();
    hashes
}

#[tokio::test]
async fn deduplicates_hits_and_misses_across_batches() {
    let (domain, known) = domain_with_block();
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
        assert_eq!(resolver.body_fetches(), 1);
    }
    assert!(resolver.resolve_batch([]).await.unwrap().is_empty());
    assert_eq!(resolver.fetches(), 2);
}

#[tokio::test]
async fn shared_block_hashes_read_and_decode_once_per_slot() {
    let (domain, hashes) = domain_with_shared_blocks(3, 4);
    let all: Vec<_> = hashes.iter().flatten().copied().collect();
    let mut resolver = BlockMetaResolver::new(AsyncQueryFacade::new(domain));

    let resolved = resolver.resolve_batch(all.clone()).await.unwrap();
    assert_eq!(resolved.len(), all.len());
    assert_eq!(resolver.fetches(), 12);
    assert_eq!(resolver.body_fetches(), 3);
    for (expected_block, block_hashes) in hashes.iter().enumerate() {
        for (expected_index, hash) in block_hashes.iter().enumerate() {
            let meta = &resolved[hash];
            assert_eq!(meta.height as usize, expected_block + 1);
            assert_eq!(meta.tx_index, expected_index);
        }
    }

    let cached = resolver.resolve_batch(all).await.unwrap();
    assert_eq!(cached.len(), resolved.len());
    assert_eq!(resolver.body_fetches(), 3);
}

#[tokio::test]
async fn one_transaction_per_block_remains_one_body_fetch_per_slot() {
    let (domain, hashes) = domain_with_shared_blocks(3, 1);
    let requested: Vec<_> = hashes.iter().map(|hashes| hashes[0]).collect();
    let mut resolver = BlockMetaResolver::new(AsyncQueryFacade::new(domain));

    assert_eq!(resolver.resolve_batch(requested).await.unwrap().len(), 3);
    assert_eq!(resolver.fetches(), 3);
    assert_eq!(resolver.body_fetches(), 3);
}

#[tokio::test]
async fn hashes_indexed_to_one_slot_search_each_candidate_body_once() {
    let domain = ToyDomain::new(None, None);
    let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 2,
        txs_per_block: 1,
        ..Default::default()
    });
    let decoded: Vec<_> = blocks
        .iter()
        .map(|raw| MultiEraBlock::decode(raw).unwrap())
        .collect();
    let shared_slot = decoded[0].slot();
    let hashes: Vec<_> = decoded.iter().map(|block| block.txs()[0].hash()).collect();
    let writer = domain.archive().start_writer().unwrap();
    for (raw, block) in blocks.iter().zip(&decoded) {
        writer
            .apply(&ChainPoint::Specific(shared_slot, block.hash()), raw)
            .unwrap();
    }
    writer
        .apply_index(
            &decoded
                .iter()
                .map(|block| ArchiveIndexDelta {
                    slot: shared_slot,
                    block_hash: block.hash().to_vec(),
                    block_number: Some(block.number()),
                    tx_hashes: block.txs().iter().map(|tx| tx.hash().to_vec()).collect(),
                    tags: Vec::new(),
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
    writer.commit().unwrap();

    let mut resolver = BlockMetaResolver::new(AsyncQueryFacade::new(domain));
    let resolved = resolver.resolve_batch(hashes.clone()).await.unwrap();
    assert_eq!(resolved.len(), 2);
    assert_eq!(resolver.body_fetches(), 2);
    for hash in hashes {
        assert_eq!(resolved[&hash].tx_hash, hash);
    }
}

#[tokio::test]
async fn undecodable_sibling_does_not_hide_requested_transaction() {
    let domain = ToyDomain::new(None, None);
    let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig::default());
    let raw = &blocks[0];
    let block = MultiEraBlock::decode(raw).unwrap();
    let known = block.txs()[0].hash();
    let corrupt = std::sync::Arc::new(b"not a block".to_vec());
    let writer = domain.archive().start_writer().unwrap();
    writer
        .apply(
            &ChainPoint::Specific(block.slot(), [0xff; 32].into()),
            &corrupt,
        )
        .unwrap();
    writer
        .apply(&ChainPoint::Specific(block.slot(), block.hash()), raw)
        .unwrap();
    writer
        .apply_index(&[ArchiveIndexDelta {
            slot: block.slot(),
            block_hash: block.hash().to_vec(),
            block_number: Some(block.number()),
            tx_hashes: vec![known.to_vec()],
            tags: Vec::new(),
        }])
        .unwrap();
    writer.commit().unwrap();

    let mut resolver = BlockMetaResolver::new(AsyncQueryFacade::new(domain));
    let resolved = resolver.resolve_batch([known]).await.unwrap();
    assert_eq!(resolved[&known].tx_hash, known);
    assert_eq!(resolver.body_fetches(), 2);
}

#[tokio::test]
async fn undecodable_requested_body_remains_an_error() {
    let domain = ToyDomain::new(None, None);
    let hash = TxHash::from([0xff; 32]);
    let slot = 1;
    let corrupt = std::sync::Arc::new(b"not a block".to_vec());
    let writer = domain.archive().start_writer().unwrap();
    writer
        .apply(&ChainPoint::Specific(slot, [0xff; 32].into()), &corrupt)
        .unwrap();
    writer
        .apply_index(&[ArchiveIndexDelta {
            slot,
            block_hash: vec![0xff; 32],
            block_number: Some(1),
            tx_hashes: vec![hash.to_vec()],
            tags: Vec::new(),
        }])
        .unwrap();
    writer.commit().unwrap();

    let mut resolver = BlockMetaResolver::new(AsyncQueryFacade::new(domain));
    for expected_fetches in 1..=2 {
        assert!(resolver.resolve_batch([hash]).await.is_err());
        assert_eq!(resolver.fetches(), expected_fetches);
    }
}

#[tokio::test]
async fn oversized_batch_fetches_each_hash_once_and_bounds_memo() {
    let (domain, known) = domain_with_block();
    let absent = TxHash::from([0xff; 32]);
    let mut resolver = BlockMetaResolver::with_capacity(AsyncQueryFacade::new(domain), 1);

    let result = resolver
        .resolve_batch([known, absent, known, absent])
        .await
        .unwrap();
    assert_eq!(result[&known].tx_hash, known);
    assert_eq!(resolver.fetches(), 2);
    assert!(resolver.resolve_batch([absent]).await.unwrap().is_empty());
    assert_eq!(resolver.fetches(), 2);
    let refetched = resolver.resolve_batch([known]).await.unwrap();
    assert_eq!(refetched[&known].slot, result[&known].slot);
    assert_eq!(resolver.fetches(), 3);
}

#[tokio::test]
async fn archive_errors_are_not_cached_as_misses() {
    let domain = FaultyToyDomain::new(ToyDomain::new(None, None), TestFault::ArchiveStoreError);
    let mut resolver = BlockMetaResolver::new(AsyncQueryFacade::new(domain));
    let hash = TxHash::from([0xff; 32]);

    for expected_fetches in 1..=2 {
        assert!(resolver.resolve_batch([hash, hash]).await.is_err());
        assert_eq!(resolver.fetches(), expected_fetches);
    }
}

type MeasuredDomain = ToyDomain<MeasuredStores<MemoryStores>>;

fn measured_domain(limiter: QueryLimiter) -> MeasuredDomain {
    ToyDomain::with_stores(
        ToyDomain::new(None, None).genesis(),
        CardanoConfig::default(),
        None,
        None,
        MeasuredStores::new(MemoryStores::open()),
    )
    .with_query_limiter(limiter)
}

fn exact_lookups(domain: &MeasuredDomain) -> u64 {
    domain
        .archive()
        .counters
        .exact_lookups
        .load(Ordering::Relaxed)
}

/// Hashes no block holds, distinct from each other and from real ones.
fn absent(count: usize) -> impl Iterator<Item = TxHash> {
    (0..count as u32).map(|i| {
        let mut bytes = [0xee; 32];
        bytes[..4].copy_from_slice(&i.to_be_bytes());
        TxHash::from(bytes)
    })
}

/// A closure that holds its permit until released.
struct Held {
    release: std::sync::mpsc::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl Held {
    async fn through<D: Domain>(query: &AsyncQueryFacade<D>) -> Self {
        let query = query.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, wait_for_release) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
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
        Self { release, task }
    }

    async fn release(self) {
        self.release.send(()).unwrap();
        self.task.await.unwrap();
    }
}

/// Let every spawned task run until it waits, so it queues for a permit
/// before whatever the test spawns next.
async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn facades_built_from_one_domain_share_its_limiter() {
    let domain = ToyDomain::new(None, None).with_query_limiter(QueryLimiter::new(1));
    let held = Held::through(&AsyncQueryFacade::new(domain.clone())).await;
    assert_eq!(domain.query_limiter().in_use(), 1);

    let other = AsyncQueryFacade::new(domain.clone());
    let pending = other.slot_by_number(1);
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), pending.as_mut())
            .await
            .is_err()
    );

    held.release().await;
    pending.await.unwrap();
    assert_eq!(domain.query_limiter().in_use(), 0);
}

#[tokio::test]
async fn two_domains_do_not_share_a_limiter() {
    let first = ToyDomain::new(None, None).with_query_limiter(QueryLimiter::new(1));
    let second = ToyDomain::new(None, None).with_query_limiter(QueryLimiter::new(1));
    let held = Held::through(&AsyncQueryFacade::new(first.clone())).await;

    tokio::time::timeout(
        Duration::from_secs(5),
        AsyncQueryFacade::new(second.clone()).slot_by_number(1),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first.query_limiter().in_use(), 1);
    assert_eq!(second.query_limiter().in_use(), 0);

    held.release().await;
}

#[tokio::test]
async fn a_one_permit_domain_resolves_a_batch_of_many_chunks_and_slots() {
    let (domain, hashes) = domain_with_shared_blocks(3, 4);
    let domain = domain.with_query_limiter(QueryLimiter::new(1));
    let known: Vec<_> = hashes.iter().flatten().copied().collect();
    let filler: Vec<_> = absent(known.len() * LOCATE_CHUNK / 3).collect();
    let batch: Vec<_> = known
        .iter()
        .zip(filler.chunks(LOCATE_CHUNK / 3))
        .flat_map(|(hash, filler)| std::iter::once(*hash).chain(filler.iter().copied()))
        .collect();
    assert!(batch.len() > 3 * LOCATE_CHUNK);

    let mut resolver = BlockMetaResolver::new(AsyncQueryFacade::new(domain.clone()));
    let resolved = resolver.resolve_batch(batch.clone()).await.unwrap();

    assert_eq!(resolved.len(), known.len());
    assert_eq!(resolver.fetches(), batch.len());
    assert_eq!(resolver.body_fetches(), 3);
    for hash in &known {
        assert_eq!(resolved[hash].tx_hash, *hash);
    }
    assert_eq!(domain.query_limiter().in_use(), 0);
}

#[tokio::test]
async fn a_light_query_runs_between_the_locate_chunks_of_a_heavy_batch() {
    let domain = measured_domain(QueryLimiter::new(1));
    let query = AsyncQueryFacade::new(domain.clone());
    let held = Held::through(&query).await;

    let heavy = tokio::spawn({
        let query = query.clone();
        async move {
            BlockMetaResolver::new(query)
                .resolve_batch(absent(3 * LOCATE_CHUNK))
                .await
        }
    });
    settle().await;
    let light = tokio::spawn({
        let query = query.clone();
        let domain = domain.clone();
        async move {
            query
                .run_blocking(move |_| Ok(exact_lookups(&domain)))
                .await
        }
    });
    settle().await;
    held.release().await;

    assert_eq!(light.await.unwrap().unwrap(), LOCATE_CHUNK as u64);
    assert!(heavy.await.unwrap().unwrap().is_empty());
    assert_eq!(exact_lookups(&domain), 3 * LOCATE_CHUNK as u64);
}

#[tokio::test]
async fn a_dropped_batch_stops_at_the_next_chunk() {
    let domain = measured_domain(QueryLimiter::new(1));
    let query = AsyncQueryFacade::new(domain.clone());
    let held = Held::through(&query).await;

    let heavy = tokio::spawn({
        let query = query.clone();
        async move {
            BlockMetaResolver::new(query)
                .resolve_batch(absent(3 * LOCATE_CHUNK))
                .await
        }
    });
    settle().await;
    let (entered, between_chunks) = tokio::sync::oneshot::channel();
    let (release, wait_for_release) = std::sync::mpsc::channel::<()>();
    let gate = tokio::spawn({
        let query = query.clone();
        async move {
            query
                .run_blocking(move |_| {
                    entered.send(()).unwrap();
                    wait_for_release
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                    Ok(())
                })
                .await
        }
    });
    settle().await;
    held.release().await;

    between_chunks.await.unwrap();
    assert_eq!(exact_lookups(&domain), LOCATE_CHUNK as u64);
    heavy.abort();
    assert!(heavy.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    gate.await.unwrap().unwrap();

    assert_eq!(exact_lookups(&domain), LOCATE_CHUNK as u64);
    assert_eq!(domain.query_limiter().in_use(), 0);
}

#[tokio::test]
async fn tx_cbor_batch_answers_each_distinct_hash_like_tx_cbor() {
    let (domain, hashes) = domain_with_shared_blocks(3, 4);
    let query = AsyncQueryFacade::new(domain);
    let missing = TxHash::from([0xff; 32]);
    let mut requested: Vec<_> = hashes.iter().flatten().rev().copied().collect();
    requested.insert(5, missing);
    let distinct = requested.clone();
    requested.extend_from_slice(&distinct[..3]);

    let batch = query.tx_cbor_batch(requested).await.unwrap();

    assert_eq!(
        batch.iter().map(|(hash, _)| *hash).collect::<Vec<_>>(),
        distinct
    );
    for (hash, cbor) in batch {
        let single = query.tx_cbor(hash.to_vec()).await.unwrap();
        assert_eq!(cbor.is_some(), hash != missing);
        assert_eq!(cbor, single);
    }
}

#[tokio::test]
async fn tx_cbor_batch_reads_each_body_once_however_many_of_its_txs_are_asked() {
    let domain = measured_domain(QueryLimiter::default());
    let hashes = load_shared_blocks(&domain, 3, 4);
    let query = AsyncQueryFacade::new(domain.clone());
    let reads = || {
        domain
            .archive()
            .counters
            .block_reads
            .load(Ordering::Relaxed)
    };

    let before = reads();
    let batch = query
        .tx_cbor_batch(hashes.iter().flatten().copied())
        .await
        .unwrap();

    assert_eq!(batch.len(), 12);
    assert!(batch.iter().all(|(_, cbor)| cbor.is_some()));
    assert_eq!(reads() - before, 3);
    assert_eq!(domain.query_limiter().in_use(), 0);
}
