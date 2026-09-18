use dolos_core::{archive::Skippable, ArchiveStore, NamespaceType, StateSchema};
use dolos_testing::{
    archive::{populate_archive_batched, ArchiveShape},
    measured::MeasuredStores,
    toy_domain::{MemoryStores, ToyStores},
};

fn shape(blocks: u64) -> ArchiveShape {
    ArchiveShape {
        epochs: 2,
        blocks_per_epoch: blocks,
        log_rows_per_epoch: 17,
        slots_per_epoch: 100,
        seed: 42,
    }
}

fn schema() -> StateSchema {
    let mut schema = StateSchema::default();
    schema.insert("logs", NamespaceType::KeyValue);
    schema
}

#[test]
fn population_is_identical_across_batch_boundaries() {
    let first = dolos_core::builtin::MemoryArchiveStore::new(schema());
    let second = dolos_core::builtin::MemoryArchiveStore::new(schema());
    let shape = shape(7);
    populate_archive_batched(&first, "logs", &shape, 1).unwrap();
    populate_archive_batched(&second, "logs", &shape, 11).unwrap();
    assert_eq!(
        first.get_range(None, None).unwrap().collect::<Vec<_>>(),
        second.get_range(None, None).unwrap().collect::<Vec<_>>(),
    );
    let range = shape.log_key(0, 0)..shape.log_key(shape.epochs, 0);
    assert_eq!(
        first
            .iter_logs("logs", range.clone())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        second
            .iter_logs("logs", range)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
}

#[test]
fn logs_only_population_has_no_bodies_and_rejects_invalid_batch_size() {
    let store = dolos_core::builtin::MemoryArchiveStore::new(schema());
    let shape = shape(0);
    assert!(populate_archive_batched(&store, "logs", &shape, 0).is_err());
    populate_archive_batched(&store, "logs", &shape, 3).unwrap();
    assert!(store.get_tip().unwrap().is_none());
    assert_eq!(
        store
            .iter_logs("logs", shape.log_key(0, 0)..shape.log_key(2, 0))
            .unwrap()
            .count(),
        34
    );
}

#[test]
fn measurement_preserves_reverse_iteration_and_does_not_count_skips_as_reads() {
    let stores = MeasuredStores::new(MemoryStores::open());
    let archive = stores.archive();
    populate_archive_batched(archive, "account-epochs", &shape(8), 3).unwrap();
    archive.counters.reset();
    let mut blocks = archive.get_range(Some(0), Some(100)).unwrap();
    blocks.skip_forward(2);
    blocks.skip_backward(1);
    assert_eq!(archive.counters.snapshot().block_reads, 0);
    assert_eq!(blocks.next().unwrap().0, 24);
    assert_eq!(blocks.next_back().unwrap().0, 72);
    assert_eq!(archive.counters.snapshot().block_reads, 2);
    assert!(archive.counters.snapshot().decoded_bytes > 0);
    let log_range = shape(8).log_key(0, 0)..shape(8).log_key(1, 0);
    let mut logs = archive.iter_logs("account-epochs", log_range).unwrap();
    assert_eq!(archive.counters.snapshot().log_rows, 0);
    logs.next().unwrap().unwrap();
    assert_eq!(archive.counters.snapshot().log_rows, 1);
}

#[test]
fn persistent_wal_is_anchored_at_imported_tip_before_live_replay() {
    use dolos_core::{BootstrapExt, Domain, StateStore, SyncExt, WalStore};
    use dolos_testing::performance::{ApiFixture, FixtureShape};

    let fixture = ApiFixture::new(MemoryStores::open(), FixtureShape::default()).unwrap();
    let imported_tip = fixture.domain.state().read_cursor().unwrap().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let domain = fixture
        .domain
        .with_persistent_wal(directory.path().join("wal"))
        .unwrap();
    assert_eq!(domain.wal().find_tip().unwrap().unwrap().0, imported_tip);
    assert!(domain.wal().read_entry(&imported_tip).unwrap().is_some());
    for block in &fixture.tail[..2] {
        domain.roll_forward(block.clone()).unwrap();
    }
    domain.check_integrity().unwrap();
    domain.rollback(&imported_tip).unwrap();
    assert_eq!(domain.state().read_cursor().unwrap(), Some(imported_tip));
    domain.bootstrap().unwrap();
}

#[tokio::test]
async fn batched_live_replay_runs_wal_archive_state_and_notifications() {
    use dolos_core::{Domain, StateStore, SyncExt, TipEvent, TipSubscription, WalStore};
    use dolos_testing::performance::{ApiFixture, FixtureShape};

    let fixture = ApiFixture::new(MemoryStores::open(), FixtureShape::default()).unwrap();
    let imported_tip = fixture.domain.state().read_cursor().unwrap().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let domain = fixture
        .domain
        .with_persistent_wal(directory.path().join("wal"))
        .unwrap();
    let mut tips = domain.watch_tip(Some(imported_tip.clone())).unwrap();

    assert_eq!(domain.roll_forward_batch(Vec::new()).unwrap(), None);
    let replay = fixture.tail[..3].to_vec();
    let expected = replay
        .iter()
        .map(|raw| pallas::ledger::traverse::MultiEraBlock::decode(raw).unwrap())
        .map(|block| (block.slot(), block.hash()))
        .collect::<Vec<_>>();
    assert_eq!(
        domain.roll_forward_batch(replay).unwrap(),
        Some(expected.last().unwrap().0)
    );

    assert_eq!(
        domain.state().read_cursor().unwrap(),
        Some(dolos_core::ChainPoint::Specific(
            expected.last().unwrap().0,
            expected.last().unwrap().1,
        ))
    );
    assert_eq!(
        domain.archive().get_tip().unwrap().unwrap().0,
        expected.last().unwrap().0
    );
    assert_eq!(
        domain
            .wal()
            .iter_blocks(Some(imported_tip), None)
            .unwrap()
            .count(),
        4
    );
    for (slot, hash) in expected {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), tips.next_tip())
            .await
            .unwrap();
        assert!(
            matches!(event, TipEvent::Apply(point, _) if point == dolos_core::ChainPoint::Specific(slot, hash))
        );
    }
}

#[test]
fn batched_live_replay_propagates_writer_failures() {
    use dolos_core::SyncExt;
    use dolos_testing::{
        faults::{FaultyToyDomain, TestFault},
        synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
        toy_domain::ToyDomain,
    };
    use std::sync::Arc;

    let (blocks, _, config) = build_synthetic_blocks(SyntheticBlockConfig::default());
    let inner = ToyDomain::new_with_genesis_and_config(
        Arc::new(dolos_cardano::include::preview::load()),
        config,
        None,
        None,
    );
    let domain = FaultyToyDomain::new(inner, TestFault::ArchiveStoreError);

    assert!(domain.roll_forward_batch(blocks[..2].to_vec()).is_err());
}
