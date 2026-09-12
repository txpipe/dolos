mod node;

#[cfg(feature = "mithril")]
#[test]
fn backfill_publishes_before_advancing_and_releases_failed_publications() {
    use dolos_snapshot::{backfill, export::Plan, source::SnapshotSource};
    use std::cell::Cell;

    struct Publisher {
        expected: ChainPoint,
        fail: bool,
        called: Cell<bool>,
        cancel: tokio_util::sync::CancellationToken,
    }

    impl backfill::Publish for Publisher {
        fn publish(&self, plan: &Plan, source: &dyn SnapshotSource) -> Result<(), backfill::Error> {
            assert_eq!(plan.sequence, 1);
            assert_eq!(
                source.committed_position().unwrap(),
                Some(self.expected.clone())
            );
            self.called.set(true);
            if self.fail {
                self.cancel.cancel();
                Err(backfill::Error::caller(io::Error::other(
                    "publication failed",
                )))
            } else {
                Ok(())
            }
        }
    }

    let mut node = node::Node::new();
    let mut genesis = dolos_cardano::include::preview::load();
    genesis.force_protocol = Some(9);
    let epoch_one = genesis.shelley.epoch_length.unwrap() as u64;
    let genesis = Arc::new(genesis);
    let (before, block_before) = make_conway_block_with_prev(epoch_one - 1, None, 1);
    let (boundary, block_boundary) = make_conway_block_with_prev(epoch_one, before.hash(), 2);
    node.config.chain = ChainConfig::Cardano(Default::default());
    let mut session = BulkReplaySession::open(&node.config, genesis.clone(), Some(1)).unwrap();
    session
        .import_blocks(vec![block_before, block_boundary])
        .unwrap();
    session.finish().unwrap();

    let mithril = dolos::core::config::MithrilConfig {
        aggregator: "http://unused.invalid".to_owned(),
        genesis_key: String::new(),
        ancillary_key: None,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for fail in [true, false] {
        let cancel = tokio_util::sync::CancellationToken::new();
        let publish = Publisher {
            expected: boundary.clone(),
            fail,
            called: Cell::new(false),
            cancel: cancel.clone(),
        };
        let open_workspace = || {
            ReplayWorkspace::open(&node.config, genesis.clone()).map_err(backfill::Error::caller)
        };
        let driver = backfill::Driver {
            config: &node.config,
            genesis: &genesis,
            mithril: &mithril,
            download_dir: node.root.path().join("unused"),
            window: 1,
            until_epoch: Some(1),
            skip_validation: false,
            runtime: runtime.handle().clone(),
            cancel,
            mithril_feedback: &|| None,
            replay: &(),
            open_workspace: &open_workspace,
            publish: &publish,
        };
        let result = driver.run();
        assert_eq!(result.is_err(), fail);
        assert!(publish.called.get());
        let workspace = ReplayWorkspace::open(&node.config, genesis.clone()).unwrap();
        assert_eq!(
            workspace.snapshot().committed_position().unwrap(),
            Some(boundary.clone())
        );
        workspace.finish().unwrap();
    }
}

use std::io;
use std::sync::Arc;

use dolos::core::config::ChainConfig;
use dolos::core::{ChainPoint, Domain as _, RawBlock, StateStore, StateWriter as _};
use dolos::engine::{
    BulkReplayRunError, BulkReplaySession, DomainBuilder, ReplayProgress, ReplayWorkspace,
};
use dolos_snapshot::source::SnapshotSource as _;
use dolos_testing::blocks::make_conway_block_with_prev;
use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};

fn fixture() -> (node::Node, Arc<dolos::core::Genesis>, Vec<RawBlock>) {
    let mut node = node::Node::new();
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let (blocks, _, chain) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 4,
        slot: 100,
        ..Default::default()
    });
    node.config.chain = ChainConfig::Cardano(chain);
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
fn inspection_does_not_initialize_a_fresh_workspace() {
    let (node, genesis, _) = fixture();
    let workspace = ReplayWorkspace::open(&node.config, genesis).unwrap();
    assert_eq!(workspace.snapshot().committed_position().unwrap(), None);
    assert_eq!(workspace.snapshot().epoch().unwrap(), None);
    workspace.finish().unwrap();
}

#[test]
fn configured_boundary_survives_inspection_and_requires_explicit_advancement() {
    let mut node = node::Node::new();
    let mut genesis = dolos_cardano::include::preview::load();
    genesis.force_protocol = Some(9);
    let epoch_one = genesis.shelley.epoch_length.unwrap() as u64;
    let genesis = Arc::new(genesis);
    let (before, block_before) = make_conway_block_with_prev(epoch_one - 1, None, 1);
    let (boundary, block_boundary) = make_conway_block_with_prev(epoch_one, before.hash(), 2);
    let (after, block_after) = make_conway_block_with_prev(epoch_one + 1, boundary.hash(), 3);
    node.config.chain = ChainConfig::Cardano(Default::default());

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
                dolos_snapshot::planning::retained_epochs(&node.config).unwrap(),
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
