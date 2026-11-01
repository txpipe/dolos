#![cfg(not(windows))]

use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

#[path = "common.rs"]
mod common;

use common::*;

fn daemon_syncs(workspace: &ScenarioWorkspace) {
    println!("e2e sync start: {}", workspace.name());

    // No store exists before the daemon runs, so there is nothing to read
    // first: it starts from genesis.
    write_custom_genesis(workspace);

    let mut cmd = prepare_scenario_process(workspace);

    let handle = cmd
        .args(["daemon"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn process");

    let mut guard = ProcessGuard::new(handle);

    std::thread::sleep(Duration::from_secs(60));

    assert!(guard
        .try_wait()
        .expect("failed to query process status")
        .is_none());

    shutdown_gracefully(&mut guard);

    let after = fetch_summary(workspace);

    let after_tip = after
        .wal
        .tip_slot
        .into_iter()
        .chain(after.archive.tip_slot)
        .chain(after.state.tip_slot)
        .max()
        .unwrap_or_default();

    assert!(
        after_tip > 0,
        "expected tip to advance past genesis for {}, after={after_tip}",
        workspace.name()
    );
}

/// `dolos sync catchup --stop-epoch 1` from genesis ends once the ledger enters
/// epoch 1, so the state tip is past the first epoch boundary. Chain-sync
/// asks for one header at a time, so the run takes minutes over a remote
/// relay.
fn sync_passes_the_first_epoch_boundary(workspace: &ScenarioWorkspace) {
    write_custom_genesis(workspace);

    let mut cmd = prepare_scenario_process(workspace);

    let handle = cmd
        .args(["sync", "catchup", "--stop-epoch", "1"])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn process");

    let mut guard = ProcessGuard::new(handle);

    let status = wait_for_exit(&mut guard, Duration::from_secs(40 * 60));
    assert!(
        status.success(),
        "a sync that reached its stop epoch exited with {status} for {}",
        workspace.name()
    );

    let summary = fetch_summary(workspace);
    let epoch_length = dolos_cardano::networks::PREVIEW
        .genesis()
        .shelley
        .epoch_length
        .unwrap() as u64;

    assert!(
        summary
            .state
            .tip_slot
            .is_some_and(|tip| tip >= epoch_length),
        "state tip {:?} is not past the first epoch boundary at slot {epoch_length}",
        summary.state.tip_slot
    );
}

// ===========================================================================
// `sync catchup` against a local peer
//
// The test process serves a synthetic chain (`dolos_testing::peer`), so these
// need no relay and run in seconds.
// ===========================================================================

const PREVIEW_MAGIC: u64 = 2;

/// A peer on the port the scenario syncs from.
fn local_peer(workspace: &ScenarioWorkspace, options: PeerOptions) -> PeerFixture {
    PeerFixture::start(
        &format!("127.0.0.1:{}", workspace.port(0)),
        PREVIEW_MAGIC,
        options,
    )
    .expect("failed to start the local peer")
}

/// `dolos <args>`, its stderr written to `log` in the workspace, where no
/// pipe can fill and stall it.
fn spawn_dolos(workspace: &ScenarioWorkspace, args: &[&str], log: &str) -> ProcessGuard {
    let stderr = std::fs::File::create(workspace.path().join(log)).expect("failed to create log");

    let handle = prepare_scenario_process(workspace)
        .args(args)
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .expect("failed to spawn process");

    ProcessGuard::new(handle)
}

/// `dolos sync catchup <args>`, logged as [`spawn_dolos`] does.
fn spawn_catchup(workspace: &ScenarioWorkspace, args: &[&str], log: &str) -> ProcessGuard {
    let args = [&["sync", "catchup"], args].concat();
    spawn_dolos(workspace, &args, log)
}

/// The log, every run of whitespace one space, so a diagnostic the renderer
/// wrapped reads as one line.
fn read_log(workspace: &ScenarioWorkspace, log: &str) -> String {
    std::fs::read_to_string(workspace.path().join(log))
        .expect("failed to read log")
        .replace('│', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn wait_for_exit(guard: &mut ProcessGuard, limit: Duration) -> ExitStatus {
    let deadline = Instant::now() + limit;

    loop {
        if let Some(status) = guard.try_wait().expect("failed to query process status") {
            return status;
        }

        assert!(Instant::now() < deadline, "the process did not exit");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The slot of the target `sync catchup` printed: `target: slot N, block H`.
fn printed_target(log: &str) -> Option<u64> {
    let (_, rest) = log.split_once("target: slot ")?;
    rest.split(',').next()?.parse().ok()
}

/// `sync catchup` against a peer whose tip keeps advancing exits 0 once the
/// stores commit the tip it reached, which `sync status` then shows.
fn catchup_commits_the_tip_it_reaches(workspace: &ScenarioWorkspace) {
    let peer = local_peer(
        workspace,
        PeerOptions {
            initial_blocks: 100,
            produce_every: Some(Duration::from_millis(200)),
            ..Default::default()
        },
    );

    let mut guard = spawn_catchup(workspace, &[], "catchup.log");
    let status = wait_for_exit(&mut guard, Duration::from_secs(120));
    let log = read_log(workspace, "catchup.log");

    assert!(status.success(), "exited with {status}: {log}");

    let target = printed_target(&log).unwrap_or_else(|| panic!("no target printed: {log}"));
    assert!(
        target >= 100,
        "the target, slot {target}, is short of the chain"
    );
    assert!(log.contains("committed: slot "), "{log}");

    let summary = fetch_summary(workspace);

    for (store, tip) in [
        ("state", summary.state.tip_slot),
        ("WAL", summary.wal.tip_slot),
    ] {
        assert!(
            tip.is_some_and(|tip| tip >= target),
            "the {store} tip {tip:?} is short of the target, slot {target}"
        );
    }

    assert!(peer.tip().slot() >= target);
}

/// An upstream that stops mid-run, with no retries left, ends `sync catchup`
/// with a failure, short of any target.
fn catchup_fails_once_its_upstream_stops(workspace: &ScenarioWorkspace) {
    let peer = local_peer(
        workspace,
        PeerOptions {
            initial_blocks: 500,
            stop_after_headers: Some(50),
            ..Default::default()
        },
    );

    let mut guard = spawn_catchup(workspace, &[], "catchup.log");
    let status = wait_for_exit(&mut guard, Duration::from_secs(120));
    let log = read_log(workspace, "catchup.log");

    assert_eq!(status.code(), Some(1), "{log}");
    assert!(peer.is_stopped());
    assert_eq!(printed_target(&log), None, "{log}");
    assert!(
        log.contains("the sync ended before the stores committed the catch-up target"),
        "{log}"
    );

    // A catch-up runs no submit stage: the pull stage gave up on the peer.
    assert!(
        log.contains("the pull stage gave up on the upstream peer"),
        "{log}"
    );
}

/// The scenario, serving minibf on its second port as well.
fn serve_minibf(workspace: &ScenarioWorkspace) {
    let path = workspace.path().join("dolos.toml");
    let mut config = std::fs::read_to_string(&path).expect("failed to read the config");

    config.push_str(&format!(
        "\n[serve.minibf]\nlisten_address = \"127.0.0.1:{}\"\n",
        workspace.port(1)
    ));

    std::fs::write(&path, config).expect("failed to write the config");
}

/// A serving daemon whose sync gives up on its upstream exits 1 with the
/// cause, so that a supervisor restarts it.
fn daemon_fails_once_its_sync_gives_up(workspace: &ScenarioWorkspace) {
    serve_minibf(workspace);

    // Every session closes on its first request, delivering nothing.
    let peer = local_peer(
        workspace,
        PeerOptions {
            initial_blocks: 10,
            close_at_first_request: true,
            ..Default::default()
        },
    );

    let mut guard = spawn_dolos(workspace, &["daemon"], "daemon.log");
    wait_for_tcp_port(workspace, 1, Duration::from_secs(60));

    let status = wait_for_exit(&mut guard, Duration::from_secs(60));
    let log = read_log(workspace, "daemon.log");

    assert_eq!(status.code(), Some(1), "{log}");
    assert!(
        log.contains("the sync stopped: the pull stage gave up on the upstream peer"),
        "{log}"
    );
    assert!(peer.intersects() >= 1);
}

/// A daemon whose upstream refuses every connection exits 1 too.
fn daemon_fails_once_its_upstream_is_unreachable(workspace: &ScenarioWorkspace) {
    // Nothing listens on the scenario's upstream port.
    let mut guard = spawn_dolos(workspace, &["daemon"], "daemon.log");

    let status = wait_for_exit(&mut guard, Duration::from_secs(60));
    let log = read_log(workspace, "daemon.log");

    assert_eq!(status.code(), Some(1), "{log}");
    assert!(log.contains("gave up on the upstream peer"), "{log}");
    // The cause under pallas's own label.
    assert!(
        log.contains("connecting: error connecting bearer: Connection refused"),
        "{log}"
    );
}

/// `sync catchup --follow` whose peer drops every tx-submission session, and
/// nothing else, ends on the submit stage's give-up, which it names.
fn follow_fails_once_the_submit_stage_gives_up(workspace: &ScenarioWorkspace) {
    let peer = local_peer(
        workspace,
        PeerOptions {
            initial_blocks: 10,
            close_tx_submission: true,
            ..Default::default()
        },
    );

    let mut guard = spawn_catchup(workspace, &["--follow"], "follow.log");
    let status = wait_for_exit(&mut guard, Duration::from_secs(60));
    let log = read_log(workspace, "follow.log");

    assert_eq!(status.code(), Some(1), "{log}");
    assert!(
        log.contains(
            "the sync ended while following the chain: the submit stage gave up on the upstream peer"
        ),
        "{log}"
    );
    assert_eq!(peer.tx_submissions(), 1, "no retries are configured");
}

/// `sync catchup --follow` runs on past the target it commits, until Ctrl-C
/// ends it, which is not a success.
fn follow_runs_past_the_target(workspace: &ScenarioWorkspace) {
    let peer = local_peer(
        workspace,
        PeerOptions {
            initial_blocks: 100,
            produce_every: Some(Duration::from_millis(100)),
            ..Default::default()
        },
    );

    let mut guard = spawn_catchup(workspace, &["--follow"], "follow.log");

    let deadline = Instant::now() + Duration::from_secs(120);
    while !read_log(workspace, "follow.log").contains("target committed") {
        assert!(
            guard
                .try_wait()
                .expect("failed to query process status")
                .is_none(),
            "--follow exited before committing the target: {}",
            read_log(workspace, "follow.log")
        );
        assert!(Instant::now() < deadline, "the target was never committed");
        std::thread::sleep(Duration::from_millis(100));
    }

    let target = printed_target(&read_log(workspace, "follow.log")).expect("no target printed");

    std::thread::sleep(Duration::from_secs(3));
    assert!(
        guard
            .try_wait()
            .expect("failed to query process status")
            .is_none(),
        "--follow exited on its own"
    );

    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(guard.id() as i32),
        nix::sys::signal::Signal::SIGINT,
    )
    .expect("failed to interrupt the process");

    let status = wait_for_exit(&mut guard, Duration::from_secs(60));
    let log = read_log(workspace, "follow.log");
    assert_eq!(status.code(), Some(1), "{log}");
    assert!(
        log.contains("interrupted while following the chain"),
        "{log}"
    );

    let summary = fetch_summary(workspace);
    assert!(
        summary.state.tip_slot.is_some_and(|tip| tip > target),
        "the state tip {:?} did not move past the target, slot {target}",
        summary.state.tip_slot
    );

    drop(peer);
}

macro_rules! test_for_scenario {
    ($name:ident, $func:ident, $scenario:expr) => {
        #[test]
        #[ignore]
        fn $name() {
            $func(&ScenarioWorkspace::new(&SCENARIOS[$scenario]));
        }
    };
}

test_for_scenario!(daemon_syncs_for_preview_full_explicit, daemon_syncs, 0);
test_for_scenario!(daemon_syncs_for_preview_full_implicit, daemon_syncs, 1);
test_for_scenario!(daemon_syncs_for_preview_min_implicit, daemon_syncs, 2);
test_for_scenario!(daemon_syncs_for_mainnet_full_implicit, daemon_syncs, 3);
test_for_scenario!(daemon_syncs_for_preprod_full_implicit, daemon_syncs, 4);
test_for_scenario!(daemon_syncs_for_custom_network, daemon_syncs, 5);
test_for_scenario!(daemon_syncs_for_preview_ordinary, daemon_syncs, 6);
test_for_scenario!(
    sync_passes_the_first_epoch_boundary_for_preview_ordinary,
    sync_passes_the_first_epoch_boundary,
    6
);
test_for_scenario!(
    catchup_commits_the_tip_it_reaches_for_preview_local_peer,
    catchup_commits_the_tip_it_reaches,
    7
);
test_for_scenario!(
    catchup_fails_once_its_upstream_stops_for_preview_local_peer,
    catchup_fails_once_its_upstream_stops,
    7
);
test_for_scenario!(
    follow_runs_past_the_target_for_preview_local_peer,
    follow_runs_past_the_target,
    7
);
test_for_scenario!(
    daemon_fails_once_its_sync_gives_up_for_preview_local_peer,
    daemon_fails_once_its_sync_gives_up,
    7
);
test_for_scenario!(
    daemon_fails_once_its_upstream_is_unreachable_for_preview_local_peer,
    daemon_fails_once_its_upstream_is_unreachable,
    7
);
test_for_scenario!(
    follow_fails_once_the_submit_stage_gives_up_for_preview_local_peer,
    follow_fails_once_the_submit_stage_gives_up,
    7
);

// ===========================================================================
// Block-sequence integrity tests
//
// Home for sync-related hardening tests: continuity guards, rollback
// verification, body validation, etc. Current cohort covers the apply-side
// continuity guard (`check_continuity` in `RollWorkUnit::load`) that
// prevents silent state poisoning from non-contiguous block delivery —
// the root cause of the `input not found` incident at block 4,436,119.
// ===========================================================================

use dolos_cardano::consensus::ConsensusError;
use dolos_cardano::owned::OwnedMultiEraBlock;
use dolos_cardano::roll::{WorkBatch, WorkBlock};
use dolos_core::{ChainError, Domain as _, DomainError, StateStore as _, SyncExt as _};
use dolos_testing::{
    blocks::make_conway_block_with_prev,
    peer::{PeerFixture, PeerOptions},
    toy_domain::ToyDomain,
};
use pallas::crypto::hash::Hash;

/// A hash filled with a single byte value, for use as a "wrong" parent hash.
fn wrong_hash(n: u8) -> Hash<32> {
    Hash::new([n; 32])
}

/// Feeding a block whose `previous_hash` doesn't match the state cursor's
/// hash must fail with `BrokenContinuity` — not `InputNotFound` or any
/// other downstream symptom.
#[test]
fn apply_rejects_non_contiguous_block() {
    let domain = ToyDomain::new(None, None);

    // First block: prev_hash=None, cursor=Origin → both sides None → skip.
    let (point_a, block_a) = make_conway_block_with_prev(1, None, 0);
    domain
        .roll_forward(block_a)
        .expect("first block should apply");

    // Second block: prev_hash doesn't match cursor's hash (hash_A).
    let (_, block_c) = make_conway_block_with_prev(2, Some(wrong_hash(99)), 1);
    let err = domain.roll_forward(block_c).unwrap_err();

    let DomainError::ChainError(ChainError::Consensus(inner)) = &err else {
        panic!("expected consensus error, got {err:?}");
    };
    assert!(
        matches!(
            inner.downcast_ref::<ConsensusError>(),
            Some(ConsensusError::BrokenContinuity { .. })
        ),
        "expected BrokenContinuity, got {inner:?}"
    );

    // State cursor must be unchanged — the guard fires before commit_state.
    let cursor = domain.state().read_cursor().unwrap();
    assert_eq!(
        cursor,
        Some(point_a),
        "cursor must remain at the last good block after rejection"
    );
}

/// A block whose slot is less than the cursor's slot must fail with
/// `SlotNotIncreasing`, even if the parent hash is correct.
#[test]
fn apply_rejects_slot_regression() {
    let domain = ToyDomain::new(None, None);

    // First block at slot 10.
    let (point_a, block_a) = make_conway_block_with_prev(10, None, 0);
    domain
        .roll_forward(block_a)
        .expect("first block should apply");

    // Second block at slot 5 (regression) with correct prev_hash.
    let hash_a = point_a.hash().unwrap();
    let (_, block_b) = make_conway_block_with_prev(5, Some(hash_a), 1);
    let err = domain.roll_forward(block_b).unwrap_err();

    let DomainError::ChainError(ChainError::Consensus(inner)) = &err else {
        panic!("expected consensus error, got {err:?}");
    };
    assert!(
        matches!(
            inner.downcast_ref::<ConsensusError>(),
            Some(ConsensusError::SlotNotIncreasing { .. })
        ),
        "expected SlotNotIncreasing, got {inner:?}"
    );

    let cursor = domain.state().read_cursor().unwrap();
    assert_eq!(cursor, Some(point_a), "cursor must remain at slot 10");
}

/// Simulates the 4,436,119 incident scenario: apply an orphan, rollback,
/// then apply the canonical sibling. The continuity guard must check
/// against the post-rollback cursor (not a stale in-memory value) and
/// allow the canonical block to apply cleanly.
#[test]
fn fork_simulation_applies_canonical_after_rollback() {
    let domain = ToyDomain::new(None, None);

    // Block A: the common ancestor / fork point.
    let (point_a, block_a) = make_conway_block_with_prev(1, None, 0);
    domain.roll_forward(block_a).expect("block A should apply");
    let hash_a = point_a.hash().unwrap();

    // Orphan O': sibling that will be rolled back.
    let (point_o, block_o) = make_conway_block_with_prev(2, Some(hash_a), 1);
    domain
        .roll_forward(block_o)
        .expect("orphan O' should apply");

    // Confirm cursor advanced to O'.
    let cursor_before = domain.state().read_cursor().unwrap();
    assert_eq!(
        cursor_before,
        Some(point_o.clone()),
        "cursor should be at O'"
    );

    // Rollback to the fork point A.
    domain
        .rollback(&point_a)
        .expect("rollback to A should succeed");

    // Confirm cursor is restored to A.
    let cursor_after = domain.state().read_cursor().unwrap();
    assert_eq!(
        cursor_after,
        Some(point_a),
        "cursor should be at A after rollback"
    );

    // Canonical sibling C: same parent (A), same slot (2), different block_number
    // → different hash than O'.
    let (point_c, block_c) = make_conway_block_with_prev(2, Some(hash_a), 2);
    assert_ne!(
        point_c.hash(),
        point_o.hash(),
        "C and O' must have different hashes (siblings)"
    );

    // C must apply cleanly — the guard reads the cursor fresh from state
    // (which is A after rollback), not from a stale in-memory buffer.
    domain
        .roll_forward(block_c)
        .expect("canonical sibling C should apply after rollback");

    let cursor_final = domain.state().read_cursor().unwrap();
    assert_eq!(cursor_final, Some(point_c), "cursor should advance to C");
}

/// The first block after genesis (prev_hash=None, cursor=Origin) must be
/// accepted — both sides of the hash check are None, so the check is skipped.
#[test]
fn continuity_check_skipped_for_genesis() {
    let domain = ToyDomain::new(None, None);

    // First block with prev_hash=None. Cursor is Origin (no hash).
    // Both sides of the hash check are None → skip.
    let (point_a, block_a) = make_conway_block_with_prev(1, None, 0);
    domain
        .roll_forward(block_a)
        .expect("first block after genesis must be accepted");

    let cursor = domain.state().read_cursor().unwrap();
    assert_eq!(
        cursor,
        Some(point_a),
        "cursor should advance to first block"
    );
}

/// Same-slot blocks are allowed (Byron EBBs share the slot with the first
/// block of the epoch). The slot check uses `>=`, not `>`.
#[test]
fn continuity_check_allows_same_slot_ebb() {
    let domain = ToyDomain::new(None, None);

    // First block at slot 5.
    let (point_a, block_a) = make_conway_block_with_prev(5, None, 0);
    domain
        .roll_forward(block_a)
        .expect("first block should apply");
    let hash_a = point_a.hash().unwrap();

    // Second block at the same slot 5, with correct prev_hash.
    // This models the Byron EBB → first-block-of-epoch pattern.
    let (point_b, block_b) = make_conway_block_with_prev(5, Some(hash_a), 1);
    domain
        .roll_forward(block_b)
        .expect("same-slot block with correct parent must be accepted");

    let cursor = domain.state().read_cursor().unwrap();
    assert_eq!(
        cursor,
        Some(point_b),
        "cursor should advance to the same-slot block"
    );
}

/// A multi-block batch must validate internal chaining: each block's
/// prev_hash must match the previous block's hash, and slots must not
/// regress within the batch. This exercises `WorkBatch::check_continuity`
/// directly with 2+ blocks in a single batch — the path that
/// `SyncExt::roll_forward` (one block per call) cannot reach.
#[test]
fn batch_internal_continuity_validated() {
    let domain = ToyDomain::new(None, None);

    // Establish a cursor at block A via the normal apply path.
    let (point_a, block_a) = make_conway_block_with_prev(1, None, 0);
    domain.roll_forward(block_a).expect("block A should apply");
    let cursor = domain.state().read_cursor().unwrap();
    let hash_a = point_a.hash().unwrap();

    // Build a multi-block batch: B (prev=A) → C (prev=B), both contiguous.
    let (_, block_b_raw) = make_conway_block_with_prev(2, Some(hash_a), 1);
    let block_b = WorkBlock::new(OwnedMultiEraBlock::decode(block_b_raw).unwrap());
    let hash_b = block_b.block.view().hash();

    let (_, block_c_raw) = make_conway_block_with_prev(3, Some(hash_b), 2);
    let block_c = WorkBlock::new(OwnedMultiEraBlock::decode(block_c_raw).unwrap());

    let mut batch = WorkBatch::for_single_block(block_b);
    batch.add_work(block_c);
    batch.sort_by_slot();

    // Contiguous batch extending from cursor A → must pass.
    batch
        .check_continuity(cursor.as_ref())
        .expect("contiguous batch should pass check_continuity");

    // Now build a batch with a broken intra-batch link: B (prev=A) → D
    // (prev=wrong).
    let (_, block_b_raw) = make_conway_block_with_prev(2, Some(hash_a), 1);
    let block_b = WorkBlock::new(OwnedMultiEraBlock::decode(block_b_raw).unwrap());

    let (_, block_d_raw) = make_conway_block_with_prev(3, Some(wrong_hash(99)), 3);
    let block_d = WorkBlock::new(OwnedMultiEraBlock::decode(block_d_raw).unwrap());

    let mut bad_batch = WorkBatch::for_single_block(block_b);
    bad_batch.add_work(block_d);
    bad_batch.sort_by_slot();

    let err = bad_batch.check_continuity(cursor.as_ref()).unwrap_err();
    assert!(
        matches!(err, ConsensusError::BrokenContinuity { .. }),
        "expected BrokenContinuity for broken intra-batch link, got {err:?}"
    );

    // Note: intra-batch slot regression is unreachable after sort_by_slot
    // (slots are monotonic by construction). The SlotNotIncreasing check only
    // fires for the first block vs the cursor, which is covered by
    // `apply_rejects_slot_regression` above.
}
