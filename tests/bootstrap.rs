//! Integration test for bootstrap catch-up logic.
//!
//! Exercises the full Cardano pipeline: feeds blocks through the sync
//! lifecycle with partial commits (WAL + state only), then verifies that
//! `bootstrap()` recovers archive and index stores from WAL replay.

use std::sync::Arc;

use dolos_core::{
    BootstrapExt, ChainLogic, ChainPoint, Domain, IndexStore, StateStore, StateWriter, WalStore,
    WorkUnit,
};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::ToyDomain,
};

/// Helper: feed blocks into a domain with partial work-unit execution.
///
/// Runs load → compute → commit_wal → commit_state but **skips**
/// commit_archive and commit_indexes, simulating a crash between
/// the state commit and the archive/index commits.
fn feed_blocks_partial(domain: &ToyDomain, blocks: &[dolos_core::RawBlock]) {
    let mut chain = domain.write_chain();

    for block in blocks {
        if !chain.can_receive_block() {
            drain_partial(&mut chain, domain);
        }
        chain.receive_block(block.clone()).unwrap();
    }

    drain_partial(&mut chain, domain);
}

fn drain_partial(chain: &mut dolos_cardano::CardanoLogic, domain: &ToyDomain) {
    while let Some(mut work) =
        <dolos_cardano::CardanoLogic as ChainLogic>::pop_work::<ToyDomain>(chain, domain)
    {
        WorkUnit::<ToyDomain>::initialize(&mut work, domain).unwrap();
        let total_shards = WorkUnit::<ToyDomain>::total_shards(&work);
        let start_shard = WorkUnit::<ToyDomain>::start_shard(&work);
        for shard in start_shard..total_shards {
            WorkUnit::<ToyDomain>::load(&mut work, domain, shard).unwrap();
            WorkUnit::<ToyDomain>::compute(&mut work, shard).unwrap();
            WorkUnit::<ToyDomain>::commit_wal(&mut work, domain, shard).unwrap();
            WorkUnit::<ToyDomain>::commit_state(&mut work, domain, shard).unwrap();
            // Intentionally skip commit_archive and commit_indexes — and
            // intentionally skip finalize() to model "crash after state
            // commit", which is what the recovery test below exercises.
        }
    }
}

#[test]
fn test_catchup_recovers_archive_and_indexes() {
    let cfg = SyntheticBlockConfig::default();
    let (blocks, vectors, cardano_config) = build_synthetic_blocks(cfg);

    let genesis = Arc::new(dolos_cardano::include::devnet::load());
    let domain =
        ToyDomain::new_with_genesis_and_config(genesis, cardano_config, None, None);

    // Record baseline cursors — all stores are in sync after initial bootstrap.
    let baseline_state = domain.state().read_cursor().unwrap();
    let baseline_archive = domain.archive().get_tip().unwrap().map(|(s, _)| s);
    let baseline_index = domain.indexes().cursor().unwrap();

    // Feed synthetic blocks with partial execution (skip archive + indexes).
    feed_blocks_partial(&domain, &blocks);

    // State should have advanced.
    let state_cursor = domain.state().read_cursor().unwrap().unwrap();
    assert_ne!(
        Some(&state_cursor),
        baseline_state.as_ref(),
        "state should have advanced after feeding blocks"
    );

    // Archive and indexes should still be at the baseline.
    let archive_tip = domain.archive().get_tip().unwrap().map(|(s, _)| s);
    let index_cursor = domain.indexes().cursor().unwrap();
    assert_eq!(archive_tip, baseline_archive, "archive should not have advanced");
    assert_eq!(index_cursor, baseline_index, "indexes should not have advanced");

    // --- Run bootstrap (which calls catch_up_stores internally) ---
    domain.bootstrap().unwrap();

    // Archive tip should now match state cursor.
    let archive_tip_after = domain.archive().get_tip().unwrap().map(|(s, _)| s);
    assert_eq!(
        archive_tip_after,
        Some(state_cursor.slot()),
        "archive tip should match state cursor after catch-up"
    );

    // Index cursor should now match state cursor.
    let index_cursor_after = domain.indexes().cursor().unwrap();
    assert_eq!(
        index_cursor_after.as_ref(),
        Some(&state_cursor),
        "index cursor should match state cursor after catch-up"
    );

    // Verify index content: look up a synthetic tx hash to confirm
    // compute_catchup produced the correct index delta.
    let tx_hash_hex = &vectors.blocks[0].tx_hashes[0];
    let tx_hash_bytes = hex::decode(tx_hash_hex).unwrap();
    let slot = domain.indexes().slot_by_tx_hash(&tx_hash_bytes).unwrap();
    assert!(
        slot.is_some(),
        "tx hash {} should be found in index after catch-up",
        tx_hash_hex
    );
}

#[test]
fn test_bootstrap_origin_cursor_is_noop() {
    let genesis = Arc::new(dolos_cardano::include::devnet::load());
    let domain = ToyDomain::new_with_genesis(genesis, None, None);

    // Simulate relay bootstrap: state at Origin, WAL seeded at Origin
    let writer = domain.state().start_writer().unwrap();
    writer.set_cursor(ChainPoint::Origin).unwrap();
    writer.commit().unwrap();

    domain.wal().reset_to(&ChainPoint::Origin).unwrap();

    // bootstrap() should succeed without crashing on the empty Origin WAL entry.
    // Previously this would fail with UnknownCbor("") when catch_up_stores tried
    // to CBOR-decode the synthetic Origin entry's empty block bytes.
    domain.bootstrap().unwrap();

    // Archive should still be empty — no real blocks were processed.
    assert!(domain.archive().get_tip().unwrap().is_none());
}
