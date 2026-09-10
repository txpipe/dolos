mod node;

use std::io;
use std::sync::Arc;

use dolos::core::config::ChainConfig;
use dolos::core::{
    recover_bulk_checkpoint, BulkRecovery, BulkRecoveryError, ChainPoint, Domain as _, StateStore,
    StateWriter as _,
};
use dolos::engine::{BulkReplayRunError, BulkReplaySession, DomainBuilder, ReplayProgress};
use dolos_testing::blocks::make_conway_block_with_prev;
use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};

#[test]
fn public_builder_cleanly_initializes_configured_stores() {
    let node = node::Node::new();
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = DomainBuilder::new(&node.config, genesis).build().unwrap();

    assert_eq!(domain.state().read_cursor().unwrap(), None);

    domain.shutdown().unwrap();
}

#[test]
fn configured_stopping_epoch_reports_the_committed_boundary() {
    let mut node = node::Node::new();
    let mut genesis = dolos_cardano::include::preview::load();
    genesis.force_protocol = Some(9);
    let genesis = Arc::new(genesis);
    let epoch_one = genesis.shelley.epoch_length.unwrap() as u64;
    let (before, block_before) = make_conway_block_with_prev(epoch_one - 1, None, 1);
    let (boundary, block_boundary) = make_conway_block_with_prev(epoch_one, before.hash(), 2);
    let (_, block_after) = make_conway_block_with_prev(epoch_one + 1, boundary.hash(), 3);
    let blocks = vec![block_before, block_boundary, block_after];
    node.config.chain = ChainConfig::Cardano(Default::default());

    let session = BulkReplaySession::open(&node.config, genesis, Some(1)).unwrap();
    let progress = session.import_blocks(blocks).unwrap();

    assert!(
        matches!(progress, ReplayProgress::Boundary { .. }),
        "{progress:?}"
    );
    assert!(progress.position().is_fully_defined());
    assert_eq!(progress.position().slot(), epoch_one);

    session.close().unwrap();
}

#[test]
fn opening_a_session_recovers_state_left_ahead_of_wal() {
    let mut node = node::Node::new();
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let (blocks, _, chain) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 3,
        slot: 100,
        ..Default::default()
    });
    node.config.chain = ChainConfig::Cardano(chain);

    let session = BulkReplaySession::open(&node.config, genesis.clone(), None).unwrap();
    let expected = session.import_blocks(blocks).unwrap().position().clone();
    drop(session);

    let recovered = BulkReplaySession::open(&node.config, genesis, None).unwrap();

    assert_eq!(
        recovered.initial_recovery(),
        &BulkRecovery::WalSeeded {
            previous: None,
            position: expected,
        }
    );

    recovered.close().unwrap();
}

#[test]
fn run_closes_and_checkpoints_after_an_operation_failure() {
    let mut node = node::Node::new();
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let (blocks, _, chain) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 2,
        slot: 100,
        ..Default::default()
    });
    node.config.chain = ChainConfig::Cardano(chain);

    let session = BulkReplaySession::open(&node.config, genesis.clone(), None).unwrap();
    let result = session.run(|session| {
        session.import_blocks(blocks).unwrap();
        Err::<(), _>(io::Error::other("failure after import"))
    });

    assert!(matches!(result, Err(BulkReplayRunError::Operation(_))));

    let reopened = BulkReplaySession::open(&node.config, genesis, None).unwrap();
    assert!(matches!(
        reopened.initial_recovery(),
        BulkRecovery::Aligned { .. }
    ));
    reopened.close().unwrap();
}

#[test]
fn run_closes_and_checkpoints_after_success() {
    let mut node = node::Node::new();
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let (blocks, _, chain) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 2,
        slot: 100,
        ..Default::default()
    });
    node.config.chain = ChainConfig::Cardano(chain);

    let session = BulkReplaySession::open(&node.config, genesis.clone(), None).unwrap();
    let committed = session
        .run(|session| session.import_blocks(blocks))
        .unwrap()
        .position()
        .clone();

    let reopened = BulkReplaySession::open(&node.config, genesis, None).unwrap();
    assert_eq!(
        reopened.initial_recovery(),
        &BulkRecovery::Aligned {
            position: committed,
        }
    );
    reopened.close().unwrap();
}

#[test]
fn unanchored_state_is_not_silently_recovered() {
    let node = node::Node::new();
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let builder = DomainBuilder::new(&node.config, genesis);
    let stores = builder.open_stores().unwrap();
    let writer = stores.state.start_writer().unwrap();
    writer.set_cursor(ChainPoint::Slot(42)).unwrap();
    writer.commit().unwrap();

    let error = recover_bulk_checkpoint(&stores.state, &stores.wal).unwrap_err();
    assert!(matches!(
        error,
        BulkRecoveryError::UnanchoredState { slot: 42 }
    ));

    stores.wal.shutdown().unwrap();
    stores.state.shutdown().unwrap();
    stores.archive.shutdown().unwrap();
}
