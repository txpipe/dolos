//! The catch-up criterion and the upstream retries, over the real pull and
//! apply stages syncing from a local peer.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dolos_core::config::{ArchiveStoreConfig, ChainConfig, RetryConfig, StateStoreConfig};
use dolos_testing::blocks::make_conway_block_with_prev;
use dolos_testing::peer::{PeerFixture, PeerOptions};
use gasket::messaging::OutputPort;
use gasket::runtime::{StagePhase, Tether, TetherState};
use itertools::Itertools as _;

use super::*;
use crate::engine::DomainBuilder;

const PREVIEW_MAGIC: u64 = 2;

fn domain_over(root: &Path, retry: RetryConfig) -> (RootConfig, DomainAdapter) {
    let mut config = RootConfig::new(ChainConfig::known("preview"));
    config.storage.path = root.to_path_buf();
    config.storage.state = StateStoreConfig::ephemeral();
    config.storage.archive = ArchiveStoreConfig::ephemeral();
    config.sync.retry = retry;

    let genesis = dolos_cardano::networks::PREVIEW.genesis();
    let domain = DomainBuilder::new(&config, Arc::new(genesis))
        .build()
        .unwrap();

    (config, domain)
}

fn no_backoff(max_retries: usize) -> RetryConfig {
    RetryConfig {
        max_retries,
        backoff_unit_seconds: 0,
        ..Default::default()
    }
}

/// Retry `n` waits `unit × factor^n` seconds.
fn backing_off(max_retries: usize, unit: u64, factor: u32) -> RetryConfig {
    RetryConfig {
        max_retries,
        backoff_unit_seconds: unit,
        backoff_factor: factor,
        max_backoff_seconds: 60,
        dismissible: false,
    }
}

fn peer(options: PeerOptions) -> PeerFixture {
    PeerFixture::start("127.0.0.1:0", PREVIEW_MAGIC, options).unwrap()
}

/// A pipeline's stages, watched without a `Daemon`, whose signal hooks would
/// leave this test binary deaf to Ctrl-C.
struct Stages(Vec<Tether>);

impl Stages {
    fn ended(&self, name: &str) -> bool {
        self.0
            .iter()
            .any(|tether| tether.name() == name && is_ended(tether))
    }

    fn any_ended(&self) -> bool {
        self.0.iter().any(is_ended)
    }

    fn names(&self) -> Vec<&str> {
        self.0.iter().map(Tether::name).collect()
    }

    fn teardown(self) {
        for tether in &self.0 {
            let _ = tether.dismiss_stage();
        }

        for tether in self.0 {
            tether.join_stage();
        }
    }
}

fn is_ended(tether: &Tether) -> bool {
    !matches!(tether.check_state(), TetherState::Alive(phase) if phase != StagePhase::Ended)
}

fn start(
    config: &RootConfig,
    peer: &PeerFixture,
    limit: SyncLimit,
    domain: &DomainAdapter,
) -> (Arc<SyncProgress>, Stages) {
    let pipeline = sync(
        &config.sync,
        PREVIEW_MAGIC,
        peer.address().to_string(),
        limit,
        domain.clone(),
    )
    .unwrap();

    (pipeline.progress, Stages(pipeline.tethers))
}

/// Run a catch-up until a stage ends, or panic after a minute.
fn catch_up(
    config: &RootConfig,
    peer: &PeerFixture,
    domain: &DomainAdapter,
) -> (Arc<SyncProgress>, Stages) {
    let (progress, stages) = start(config, peer, SyncLimit::CatchUp, domain);

    wait_until("the catch-up ends", Duration::from_secs(60), || {
        stages.any_ended()
    });

    (progress, stages)
}

/// Wait until `done` holds, or panic naming `what` after `limit`.
fn wait_until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;

    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn state_cursor(domain: &DomainAdapter) -> Option<ChainPoint> {
    domain.state().read_cursor().unwrap()
}

fn wal_tip(domain: &DomainAdapter) -> Option<ChainPoint> {
    domain.wal().find_tip().unwrap().map(|(point, _)| point)
}

/// An apply stage on its own, catching up to whatever target `progress`
/// gets, fed through the returned port. Its housekeeping tick is an hour
/// apart, and the first is at once.
fn apply_alone(
    domain: &DomainAdapter,
    progress: &Arc<SyncProgress>,
) -> (OutputPort<PullEvent>, Stages) {
    let mut apply = apply::Stage::new(
        domain.clone(),
        SyncLimit::CatchUp,
        progress.clone(),
        Duration::from_secs(3600),
    );

    let mut feed = OutputPort::default();
    let (to_apply, from_feed) = gasket::messaging::tokio::mpsc_channel(50);
    feed.connect(to_apply);
    apply.upstream.connect(from_feed);

    let policy = define_gasket_policy(&no_backoff(0));
    let tether = gasket::runtime::spawn_stage(apply, policy);

    (feed, Stages(vec![tether]))
}

fn send_blocks(feed: &mut OutputPort<PullEvent>, blocks: Vec<RawBlock>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    runtime.block_on(async {
        for block in blocks {
            feed.send(PullEvent::RollForward(block).into())
                .await
                .unwrap();
        }
    });
}

fn point(slot: u64) -> ChainPoint {
    ChainPoint::Specific(slot, dolos_testing::blocks::slot_to_hash(slot))
}

#[test]
fn a_catch_up_ends_once_the_first_observed_tip_is_committed_while_the_tip_advances() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(0));

    let peer = peer(PeerOptions {
        // Long enough a drain for the peer to produce blocks during it.
        initial_blocks: 200,
        produce_every: Some(Duration::from_millis(10)),
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);

    // The apply stage ended it; the pull stage stopped pulling, but waited.
    assert!(stages.ended("apply"));
    assert!(!stages.ended("pull"));
    stages.teardown();

    assert!(progress.succeeded(SyncLimit::CatchUp));

    let target = progress.target().expect("the target was fixed");
    assert!(target.slot() >= 200, "{target}");

    // Nothing past the target was pulled, so it is where both stores stand.
    assert_eq!(state_cursor(&domain), Some(target.clone()));
    assert_eq!(wal_tip(&domain), Some(target.clone()));

    // Nor asked for: one request for the rollback to the intersection, one
    // per block through the target (block `n` sits at slot `n`), and the one
    // answered with an await.
    assert_eq!(peer.next_requests() as u64, target.slot() + 2);

    // The peer kept producing: its tip left the target behind, and the sync
    // succeeded anyway.
    wait_until("the peer's tip moves on", Duration::from_secs(5), || {
        peer.tip().slot() > target.slot()
    });
}

#[test]
fn a_catch_up_runs_no_submit_stage_and_a_follow_does() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(0));
    let peer = peer(PeerOptions::default());

    let (_, stages) = start(&config, &peer, SyncLimit::CatchUp, &domain);
    assert_eq!(stages.names(), ["pull", "apply"]);
    stages.teardown();

    let (_, stages) = start(&config, &peer, SyncLimit::Follow, &domain);
    assert_eq!(stages.names(), ["pull", "apply", "submit"]);
    stages.teardown();
}

#[test]
fn a_peer_that_stops_once_the_target_is_fixed_leaves_the_drain_to_finish() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(0));

    // Enough blocks that some are still queued for the apply stage when the
    // target is fixed.
    let mut peer = peer(PeerOptions {
        initial_blocks: 300,
        ..Default::default()
    });

    let (progress, stages) = start(&config, &peer, SyncLimit::CatchUp, &domain);

    let deadline = Instant::now() + Duration::from_secs(60);
    while progress.target().is_none() {
        assert!(Instant::now() < deadline, "the target was never fixed");
        assert!(!stages.any_ended(), "the catch-up ended short of a target");
        std::thread::sleep(Duration::from_micros(200));
    }

    let tip = peer.tip();
    peer.stop();

    wait_until("the catch-up ends", Duration::from_secs(60), || {
        stages.any_ended()
    });
    stages.teardown();

    assert!(progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(state_cursor(&domain), Some(tip));
}

#[test]
fn a_follow_commits_the_target_and_keeps_going() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(0));

    let peer = peer(PeerOptions {
        initial_blocks: 20,
        produce_every: Some(Duration::from_millis(20)),
        ..Default::default()
    });

    let (progress, stages) = start(&config, &peer, SyncLimit::Follow, &domain);

    wait_until("the target is committed", Duration::from_secs(60), || {
        progress.caught_up()
    });

    let target = progress.target().unwrap();

    wait_until(
        "the state passes the target",
        Duration::from_secs(10),
        || state_cursor(&domain).is_some_and(|cursor| cursor.slot() > target.slot() + 3),
    );

    assert!(!stages.any_ended(), "a follow never ends on its own");
    assert!(!progress.succeeded(SyncLimit::Follow));

    stages.teardown();
}

#[test]
fn a_catch_up_of_a_store_at_the_tip_ends_at_once() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(0));
    let peer = peer(PeerOptions {
        initial_blocks: 50,
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);
    stages.teardown();
    assert!(progress.succeeded(SyncLimit::CatchUp));

    // The session's opening rollback to the stores' tip reaches the apply
    // stage, usually after the target is fixed, so this does not pin the
    // target's own wake; the apply stage's own case below does.
    let started = Instant::now();
    let (progress, stages) = catch_up(&config, &peer, &domain);
    stages.teardown();

    assert!(progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(progress.target(), Some(peer.tip()));
    assert!(started.elapsed() < Duration::from_secs(20));
}

#[test]
fn a_catch_up_whose_peer_stops_ends_without_success_once_retries_run_out() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(0));

    let peer = peer(PeerOptions {
        initial_blocks: 50,
        stop_after_headers: Some(5),
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);

    // The pull stage gave up; the apply stage followed it.
    wait_until("the pull stage ends", Duration::from_secs(5), || {
        stages.ended("pull")
    });
    stages.teardown();

    assert!(peer.is_stopped());
    assert!(
        progress.target().is_none(),
        "the peer stopped short of its tip"
    );
    assert!(!progress.succeeded(SyncLimit::CatchUp));

    // The session ended on headers, before any block: the one attempt.
    let failure = progress
        .upstream_failure()
        .expect("the upstream was given up");
    assert!(failure.message.contains("chain-sync failed"), "{failure}");
}

#[test]
fn a_dropped_intersection_is_retried() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(1));

    let peer = peer(PeerOptions {
        initial_blocks: 20,
        close_at_intersects: vec![1],
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);
    stages.teardown();

    assert!(progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(peer.intersects(), 2);
}

#[test]
fn a_session_that_delivered_reconnects_without_counting_a_failure() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, domain) = domain_over(temp.path(), no_backoff(1));

    // Small batches, so that the first session delivers blocks before it
    // closes after five headers, uncounted; the reconnect's intersection is
    // dropped, the one failure allowed.
    config.sync.pull_batch_size = 2;

    let peer = peer(PeerOptions {
        initial_blocks: 50,
        close_first_session_after_headers: Some(5),
        close_at_intersects: vec![2],
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);
    stages.teardown();

    assert!(progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(peer.intersects(), 3);
}

#[test]
fn a_peer_that_fails_every_session_is_given_up_once_retries_run_out() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(2));

    let peer = peer(PeerOptions {
        initial_blocks: 20,
        close_at_first_request: true,
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);

    assert!(stages.ended("pull"));
    stages.teardown();

    assert!(!progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(peer.intersects(), 3, "one session and two retries");

    let failure = progress
        .upstream_failure()
        .expect("the upstream was given up");
    assert!(
        failure
            .message
            .starts_with("the pull stage gave up on the upstream peer"),
        "{failure}"
    );
    assert!(
        failure.message.contains("after 3 failed attempts in a row"),
        "{failure}"
    );
}

#[test]
fn a_peer_that_serves_headers_but_no_block_is_given_up() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(2));

    let peer = peer(PeerOptions {
        initial_blocks: 20,
        refuse_block_fetch: true,
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);

    assert!(stages.ended("pull"));
    stages.teardown();

    assert!(!progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(wal_tip(&domain), None);

    // Headers are not a delivery: every session counts.
    assert_eq!(peer.intersects(), 3, "one session and two retries");
    assert!(progress.upstream_failure().is_some());
}

#[test]
fn a_peer_whose_chain_grows_but_serves_no_block_is_given_up() {
    let temp = tempfile::tempdir().unwrap();

    // A second between attempts, so that every session gets a header no
    // earlier one did.
    let (config, domain) = domain_over(temp.path(), backing_off(2, 1, 1));

    let peer = peer(PeerOptions {
        initial_blocks: 5,
        produce_every: Some(Duration::from_millis(250)),
        refuse_block_fetch: true,
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);

    assert!(stages.ended("pull"));
    stages.teardown();

    assert!(!progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(wal_tip(&domain), None);
    assert_eq!(peer.intersects(), 3, "one session and two retries");
    assert!(progress.upstream_failure().is_some());
}

#[test]
fn a_delivering_session_starts_the_count_again() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, domain) = domain_over(temp.path(), no_backoff(1));
    config.sync.pull_batch_size = 2;

    // A dropped intersection, a session that delivers blocks and closes,
    // then another dropped intersection: with the count started again,
    // each failure is the first in a row, which one retry allows.
    let peer = peer(PeerOptions {
        initial_blocks: 50,
        close_at_intersects: vec![1, 3],
        close_first_session_after_headers: Some(5),
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);
    stages.teardown();

    assert!(
        progress.succeeded(SyncLimit::CatchUp),
        "{:?}",
        progress.upstream_failure()
    );
    assert_eq!(peer.intersects(), 4);
}

#[test]
fn every_failed_attempt_in_a_row_waits_its_own_backoff() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), backing_off(2, 1, 2));

    // The first session fails at its first request; the intersections after
    // it are dropped.
    let peer = peer(PeerOptions {
        initial_blocks: 20,
        close_at_first_request: true,
        close_at_intersects: vec![2, 3],
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);
    stages.teardown();

    assert!(!progress.succeeded(SyncLimit::CatchUp));

    // Failure `n` waits 2^n s: 2 s after the session, then 4 s after the
    // first dropped intersection, whatever failed before it. The third
    // failure gives up.
    let times = peer.intersection_times();
    assert_eq!(times.len(), 3);

    let gaps = times.windows(2).map(|w| w[1] - w[0]).collect_vec();
    let within = |gap: Duration, wait: u64| {
        gap >= Duration::from_secs(wait) && gap < Duration::from_secs(wait + 1)
    };

    assert!(within(gaps[0], 2), "{gaps:?}");
    assert!(within(gaps[1], 4), "{gaps:?}");
}

#[test]
fn a_dropped_intersection_with_no_retries_left_gives_up() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(0));

    let peer = peer(PeerOptions {
        initial_blocks: 20,
        close_at_intersects: vec![1],
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);

    assert!(stages.ended("pull"));
    stages.teardown();

    assert!(!progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(peer.intersects(), 1);

    let failure = progress
        .upstream_failure()
        .expect("the upstream was given up");
    assert!(
        failure.message.contains("finding the intersection"),
        "{failure}"
    );
}

#[test]
fn a_peer_that_drops_every_intersection_is_given_up() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(1));

    let peer = peer(PeerOptions {
        initial_blocks: 20,
        close_at_intersects: (1..=10).collect(),
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);

    assert!(stages.ended("pull"));
    stages.teardown();

    assert!(!progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(peer.intersects(), 2);

    let failure = progress
        .upstream_failure()
        .expect("the upstream was given up");
    assert!(
        failure.message.contains("after 2 failed attempts in a row"),
        "{failure}"
    );
}

#[test]
fn a_peer_that_holds_none_of_the_wal_is_given_up_once_retries_run_out() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(1));

    let ahead = peer(PeerOptions {
        initial_blocks: 50,
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &ahead, &domain);
    stages.teardown();
    assert!(progress.succeeded(SyncLimit::CatchUp));

    // A peer still syncing, behind every point the WAL offers.
    let behind = peer(PeerOptions {
        initial_blocks: 10,
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &behind, &domain);

    assert!(stages.ended("pull"));
    stages.teardown();

    assert!(!progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(behind.intersects(), 2, "one attempt and one retry");
    assert_eq!(state_cursor(&domain), Some(ahead.tip()));

    let failure = progress
        .upstream_failure()
        .expect("the upstream was given up");
    assert!(
        failure
            .message
            .contains("holds none of the WAL's recent points"),
        "{failure}"
    );
}

#[test]
fn a_peer_of_another_network_is_given_up_in_the_handshake() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(0));

    let mainnet = PeerFixture::start("127.0.0.1:0", 764824073, PeerOptions::default()).unwrap();

    let (progress, stages) = catch_up(&config, &mainnet, &domain);

    assert!(stages.ended("pull"));
    stages.teardown();

    let failure = progress
        .upstream_failure()
        .expect("the upstream was given up");
    assert!(failure.handshake, "{failure}");
    assert!(
        failure.message.contains("connecting: handshake"),
        "{failure}"
    );
}

#[test]
fn a_follow_whose_peer_closes_every_tx_submission_gives_up_on_it() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(1));

    let peer = peer(PeerOptions {
        initial_blocks: 20,
        close_tx_submission: true,
        ..Default::default()
    });

    let (progress, stages) = start(&config, &peer, SyncLimit::Follow, &domain);

    wait_until("the submit stage ends", Duration::from_secs(60), || {
        stages.ended("submit")
    });

    let pull_ended = stages.ended("pull");
    stages.teardown();

    assert!(!pull_ended, "only the submit stage's sessions were closed");
    assert_eq!(peer.tx_submissions(), 2, "one session and one retry");

    let failure = progress
        .upstream_failure()
        .expect("the upstream was given up");
    assert!(
        failure
            .message
            .starts_with("the submit stage gave up on the upstream peer"),
        "{failure}"
    );
    assert!(
        failure.message.contains("after 2 failed attempts in a row"),
        "{failure}"
    );
}

#[test]
fn a_block_that_fails_to_apply_ends_a_catch_up_without_success() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(0));

    let peer = peer(PeerOptions {
        initial_blocks: 50,
        corrupt_body: Some(10),
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);

    assert!(stages.ended("apply"));
    stages.teardown();

    assert!(!progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(progress.upstream_failure(), None);
    assert_eq!(state_cursor(&domain).map(|cursor| cursor.slot()), Some(9));
}

#[test]
fn the_apply_stage_ends_a_catch_up_once_a_target_it_holds_is_fixed() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), no_backoff(0));
    let peer = peer(PeerOptions {
        initial_blocks: 20,
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);
    stages.teardown();
    assert!(progress.succeeded(SyncLimit::CatchUp));

    // Past its first housekeeping tick, no event reaches it and the next
    // tick is an hour away: only the target's wake can end it.
    let progress = Arc::new(SyncProgress::default());
    let (_feed, stages) = apply_alone(&domain, &progress);

    std::thread::sleep(Duration::from_millis(500));
    assert!(!stages.any_ended());

    assert!(progress.fix_target(peer.tip()));

    wait_until("the apply stage ends", Duration::from_secs(10), || {
        stages.any_ended()
    });
    stages.teardown();

    assert!(progress.succeeded(SyncLimit::CatchUp));
}

/// Blocks at slots `1..n`, then a pair at slot `n`: the twin, like a Byron
/// epoch boundary block, and the tip chained on it, like the epoch's first
/// block.
fn chain_ending_in_twins(n: u64) -> (Vec<RawBlock>, ChainPoint, ChainPoint) {
    let mut blocks = Vec::new();
    let mut parent = None;

    for slot in 1..n {
        let (point, block) = make_conway_block_with_prev(slot, parent, slot);
        parent = point.hash();
        blocks.push(block);
    }

    let (twin, twin_block) = make_conway_block_with_prev(n, parent, n);
    let (tip, tip_block) = make_conway_block_with_prev(n, twin.hash(), n + 1);
    blocks.extend([twin_block, tip_block]);

    (blocks, twin, tip)
}

#[test]
fn a_target_whose_slot_twin_sorts_after_it_in_the_wal_is_committed() {
    let temp = tempfile::tempdir().unwrap();
    let (_, domain) = domain_over(temp.path(), no_backoff(0));

    let (blocks, twin, target) = (5..200)
        .map(chain_ending_in_twins)
        .find(|(_, twin, tip)| twin.clone().into_bytes() > tip.clone().into_bytes())
        .expect("a pair whose twin sorts after the tip");

    let progress = Arc::new(SyncProgress::default());
    progress.fix_target(target.clone());

    let (mut feed, stages) = apply_alone(&domain, &progress);
    send_blocks(&mut feed, blocks);

    wait_until("the target is committed", Duration::from_secs(30), || {
        stages.any_ended()
    });
    stages.teardown();

    assert!(progress.succeeded(SyncLimit::CatchUp));
    assert_eq!(state_cursor(&domain), Some(target));
    assert_eq!(
        wal_tip(&domain),
        Some(twin),
        "the WAL's last entry is the twin"
    );
}

#[test]
fn the_target_is_the_first_tip_fixed() {
    let progress = SyncProgress::default();

    assert!(progress.fix_target(point(10)));
    assert!(!progress.fix_target(point(12)));
    assert_eq!(progress.target(), Some(point(10)));
}

#[test]
fn the_target_is_committed_only_where_both_stores_stand_on_it() {
    let target = point(10);
    let at = |state: &ChainPoint, wal_tip: &ChainPoint, holds: bool| {
        committed(&target, Some(state), Some(wal_tip), holds)
    };

    assert!(at(&target, &target, true));

    // Short of it, past it, or on one store only.
    assert!(!at(&point(9), &point(9), false));
    assert!(!at(&point(11), &point(11), true));
    assert!(!at(&target, &point(11), true));
    assert!(!at(&point(9), &target, true));
    assert!(!committed(&target, None, None, false));

    // A rollback below the target moves the stores off it, the target stays.
    assert!(!at(&point(5), &point(5), false));

    // A twin in the target's slot, like a Byron epoch boundary block before
    // the epoch's first block, can be the WAL's last entry.
    let twin = ChainPoint::Specific(10, dolos_testing::blocks::slot_to_hash(1010));
    assert!(at(&target, &twin, true));
    assert!(!at(&twin, &twin, true), "the state stands on the twin");
    assert!(!at(&target, &twin, false), "the WAL lacks the target");

    // Origin, while no block is applied.
    let origin = ChainPoint::Origin;
    assert!(committed(&origin, None, None, false));
    assert!(committed(&origin, None, Some(&origin), true));
    assert!(!committed(&origin, Some(&target), Some(&target), false));
}

#[test]
fn retries_back_off_as_configured() {
    let retry = RetryConfig::default();

    assert_eq!(retry::backoff(&retry, 1), Duration::from_secs(4));
    assert_eq!(retry::backoff(&retry, 4), Duration::from_secs(32));
    assert_eq!(retry::backoff(&retry, 5), Duration::from_secs(60));
    assert_eq!(retry::backoff(&retry, 1000), Duration::from_secs(60));
}
