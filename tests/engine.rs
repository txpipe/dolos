mod node;

use std::io;
use std::sync::Arc;

use dolos::core::config::ArchiveStoreConfig;
use dolos::core::{
    ChainPoint, Domain as _, MempoolStore as _, MempoolTxStage, RawBlock, StateStore,
    StateWriter as _, SyncExt as _,
};
use dolos::engine::{
    BulkReplayRunError, BulkReplaySession, DomainBuildError, DomainBuilder, ReplayProgress,
    ReplayWorkspace,
};
use dolos_snapshot::source::SnapshotSource as _;
use dolos_testing::blocks::make_conway_block_with_prev;
use dolos_testing::mempool::make_test_mempool_tx;
use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
use pallas::ledger::traverse::MultiEraBlock;

fn fixture() -> (node::Node, Arc<dolos::core::Genesis>, Vec<RawBlock>) {
    let mut node = node::Node::new();
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let (blocks, _, chain) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 4,
        slot: 100,
        ..Default::default()
    });
    node.config.chain = node.custom_chain(2, chain.custom_utxos);
    (node, genesis, blocks)
}

#[test]
fn public_builder_cleanly_initializes_configured_stores() {
    let (node, genesis, _) = fixture();
    let domain = DomainBuilder::new(&node.config, genesis).build().unwrap();
    assert_eq!(domain.state().read_cursor().unwrap(), None);
    domain.shutdown().unwrap();
}

#[test]
fn the_builder_sizes_the_query_limiter_from_the_configuration() {
    let (mut node, genesis, _) = fixture();
    node.config.serve.max_concurrent_queries = 4;

    let domain = DomainBuilder::new(&node.config, genesis).build().unwrap();

    assert_eq!(domain.query_limiter().cap(), 4);
    domain.shutdown().unwrap();
}

/// Neither the builder nor a replay workspace opens a store for a
/// configuration it refuses, or for a custom magic its genesis does not match.
#[test]
fn a_refused_configuration_opens_no_store() {
    let (node, genesis, _) = fixture();

    let mut invalid = node.config.clone();
    invalid.serve.max_concurrent_queries = 0;

    let mut mismatched = node.config.clone();
    mismatched.chain = node.custom_chain(43, vec![]);

    // Only a configuration built in code can carry this: the file refuses it
    // as it is read.
    let mut ephemeral_with_a_path = node.config.clone();
    ephemeral_with_a_path.storage.archive = ArchiveStoreConfig {
        blocks_path: Some(node.config.storage.path.join("blocks")),
        ..ArchiveStoreConfig::ephemeral()
    };

    for config in [&invalid, &ephemeral_with_a_path] {
        assert!(matches!(
            DomainBuilder::new(config, genesis.clone()).build(),
            Err(DomainBuildError::Config(_))
        ));
    }
    assert!(matches!(
        DomainBuilder::new(&mismatched, genesis.clone()).build(),
        Err(DomainBuildError::Chain(_))
    ));

    for config in [&invalid, &mismatched, &ephemeral_with_a_path] {
        assert!(ReplayWorkspace::open(config, genesis.clone()).is_err());
    }

    assert!(!node.config.storage.path.exists());
}

#[test]
fn sync_finalizes_a_transaction_after_the_configured_confirmations() {
    let (mut node, genesis, blocks) = fixture();
    node.config.mempool.retain_confirmed_blocks = 2;

    let domain = DomainBuilder::new(&node.config, genesis).build().unwrap();

    let tx = MultiEraBlock::decode(&blocks[0]).unwrap().txs()[0].hash();
    let mempool = domain.mempool();
    mempool.receive(make_test_mempool_tx(tx)).unwrap();
    mempool.mark_inflight(&[tx]).unwrap();
    mempool.mark_acknowledged(&[tx]).unwrap();

    let finalized = || {
        mempool
            .dump_finalized(0, 100)
            .items
            .iter()
            .any(|logged| logged.hash == tx && logged.stage == MempoolTxStage::Finalized)
    };

    domain.roll_forward(blocks[0].clone()).unwrap();
    assert!(!finalized(), "one confirmation");

    domain.roll_forward(blocks[1].clone()).unwrap();
    assert!(finalized(), "two confirmations");

    domain.shutdown().unwrap();
}

#[test]
fn inspection_does_not_initialize_a_fresh_workspace() {
    let (node, genesis, _) = fixture();
    let workspace = ReplayWorkspace::open(&node.config, genesis).unwrap();
    assert_eq!(workspace.snapshot().committed_position().unwrap(), None);
    assert_eq!(workspace.snapshot().epoch().unwrap(), None);
    workspace.finish().unwrap();
}

#[test]
fn configured_boundary_survives_inspection_and_requires_explicit_advancement() {
    let node = node::Node::new();
    let mut genesis = dolos_cardano::include::preview::load();
    genesis.force_protocol = Some(9);
    let epoch_one = genesis.shelley.epoch_length.unwrap() as u64;
    let genesis = Arc::new(genesis);
    let (before, block_before) = make_conway_block_with_prev(epoch_one - 1, None, 1);
    let (boundary, block_boundary) = make_conway_block_with_prev(epoch_one, before.hash(), 2);
    let (after, block_after) = make_conway_block_with_prev(epoch_one + 1, boundary.hash(), 3);

    let mut session = BulkReplaySession::open(&node.config, genesis.clone(), Some(1)).unwrap();
    let progress = session
        .import_blocks(vec![block_before, block_boundary, block_after.clone()])
        .unwrap();
    assert_eq!(
        progress,
        ReplayProgress::Boundary {
            position: boundary.clone()
        }
    );
    assert_eq!(
        session.import_blocks(vec![block_after.clone()]).unwrap(),
        progress
    );
    assert!(session.prune_history().is_err());
    session.finish().unwrap();

    for _attempt in 0..2 {
        let workspace = ReplayWorkspace::open(&node.config, genesis.clone()).unwrap();
        let snapshot = workspace.snapshot();
        assert_eq!(
            snapshot.committed_position().unwrap(),
            Some(boundary.clone())
        );
        assert_eq!(snapshot.epoch().unwrap(), Some(1));
        let plan = snapshot
            .plan(
                u64::from(genesis.network_magic()),
                dolos_snapshot::planning::retained_epochs(&[]).unwrap(),
            )
            .unwrap();
        assert_eq!(plan.sequence, 1);
        drop(snapshot);
        workspace.finish().unwrap();
    }

    let workspace = ReplayWorkspace::open(&node.config, genesis).unwrap();
    let mut session = workspace.start(Some(2)).unwrap();
    assert_eq!(
        session.import_blocks(vec![block_after]).unwrap().position(),
        &after
    );
    session.finish().unwrap();
}

#[test]
fn unfinished_replay_resumes_from_its_committed_input() {
    let (node, genesis, blocks) = fixture();
    let mut session = BulkReplaySession::open(&node.config, genesis.clone(), None).unwrap();
    let committed = session
        .import_blocks(blocks[..2].to_vec())
        .unwrap()
        .position()
        .clone();
    drop(session);

    let workspace = ReplayWorkspace::open(&node.config, genesis.clone()).unwrap();
    assert_eq!(
        workspace.snapshot().committed_position().unwrap(),
        Some(committed.clone())
    );
    let mut resumed = workspace.start(None).unwrap();
    assert_eq!(
        resumed.committed_position().unwrap(),
        Some(committed.clone())
    );
    let final_position = resumed
        .import_blocks(blocks[2..].to_vec())
        .unwrap()
        .position()
        .clone();
    assert!(final_position.slot() > committed.slot());
    resumed.finish().unwrap();

    let domain = DomainBuilder::new(&node.config, genesis).build().unwrap();
    assert_eq!(domain.state().read_cursor().unwrap(), Some(final_position));
    domain.shutdown().unwrap();
}

#[test]
fn operation_failure_still_finalizes_for_normal_node_startup() {
    let (node, genesis, blocks) = fixture();
    let session = BulkReplaySession::open(&node.config, genesis.clone(), None).unwrap();
    let mut committed = None;
    let result = session.run(|session| {
        committed = Some(session.import_blocks(blocks).unwrap().position().clone());
        Err::<(), _>(io::Error::other("failure after import"))
    });
    assert!(matches!(result, Err(BulkReplayRunError::Operation(_))));
    let domain = DomainBuilder::new(&node.config, genesis).build().unwrap();
    assert_eq!(domain.state().read_cursor().unwrap(), committed);
    domain.shutdown().unwrap();
}

#[test]
fn success_finalizes_for_normal_node_startup() {
    let (node, genesis, blocks) = fixture();
    let session = BulkReplaySession::open(&node.config, genesis.clone(), None).unwrap();
    let committed = session
        .run(|session| session.import_blocks(blocks))
        .unwrap()
        .position()
        .clone();
    let domain = DomainBuilder::new(&node.config, genesis).build().unwrap();
    assert_eq!(domain.state().read_cursor().unwrap(), Some(committed));
    domain.shutdown().unwrap();
}

#[test]
fn empty_input_does_not_poison_a_session() {
    let (node, genesis, blocks) = fixture();
    let mut session = BulkReplaySession::open(&node.config, genesis, None).unwrap();
    assert!(session.import_blocks(vec![]).is_err());
    assert_eq!(session.committed_position().unwrap(), None);
    session.import_blocks(blocks).unwrap();
    session.finish().unwrap();
}

#[test]
fn import_failure_requires_reopening() {
    let (node, genesis, blocks) = fixture();
    let mut session = BulkReplaySession::open(&node.config, genesis.clone(), None).unwrap();
    assert!(session.import_blocks(vec![Arc::new(vec![0xff])]).is_err());
    assert!(session
        .import_blocks(blocks.clone())
        .unwrap_err()
        .to_string()
        .contains("finish and reopen"));
    session.finish().unwrap();
    let mut session = BulkReplaySession::open(&node.config, genesis, None).unwrap();
    session.import_blocks(blocks).unwrap();
    session.finish().unwrap();
}

#[test]
fn unanchored_position_is_not_silently_repaired_and_failed_start_releases_stores() {
    let (node, genesis, _) = fixture();
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
    let writer = stores.state.start_writer().unwrap();
    writer.set_cursor(ChainPoint::Slot(42)).unwrap();
    writer.commit().unwrap();
    stores.state.shutdown().unwrap();
    drop(stores);

    let workspace = ReplayWorkspace::open(&node.config, genesis.clone()).unwrap();
    assert!(workspace.snapshot().epoch().is_err());
    let error = workspace.start(None).err().unwrap();
    assert!(error.to_string().contains("no block hash"));

    let workspace = ReplayWorkspace::open(&node.config, genesis).unwrap();
    assert_eq!(
        workspace.snapshot().committed_position().unwrap(),
        Some(ChainPoint::Slot(42))
    );
    workspace.finish().unwrap();
}
