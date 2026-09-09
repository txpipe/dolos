use std::time::Duration;

use dolos_core::async_query::AsyncQueryOptions;
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::ToyDomain,
};

use dolos_core::{
    block_meta::BlockMetaResolver, indexes::ArchiveIndexDelta, ArchiveStore as _,
    ArchiveWriter as _, AsyncQueryFacade, ChainPoint, Domain as _, TxHash,
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
    }
    assert!(resolver.resolve_batch([]).await.unwrap().is_empty());
    assert_eq!(resolver.fetches(), 2);
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
