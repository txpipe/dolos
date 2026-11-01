//! The catch-up criterion and the upstream retries, over the real pull and
//! apply stages syncing from a local peer.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dolos_core::config::{ArchiveStoreConfig, ChainConfig, RetryConfig, StateStoreConfig};
use dolos_testing::peer::{PeerFixture, PeerOptions};
use gasket::runtime::{StagePhase, Tether, TetherState};

use super::*;
use crate::engine::DomainBuilder;

const PREVIEW_MAGIC: u64 = 2;

fn domain_over(root: &Path, max_retries: usize) -> (RootConfig, DomainAdapter) {
    let mut config = RootConfig::new(ChainConfig::known("preview"));
    config.storage.path = root.to_path_buf();
    config.storage.state = StateStoreConfig::ephemeral();
    config.storage.archive = ArchiveStoreConfig::ephemeral();
    config.sync.retry = RetryConfig {
        max_retries,
        backoff_unit_seconds: 0,
        ..Default::default()
    };

    let genesis = dolos_cardano::networks::PREVIEW.genesis();
    let domain = DomainBuilder::new(&config, Arc::new(genesis))
        .build()
        .unwrap();

    (config, domain)
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

fn point(slot: u64) -> ChainPoint {
    ChainPoint::Specific(slot, dolos_testing::blocks::slot_to_hash(slot))
}

#[test]
fn a_catch_up_ends_once_the_first_observed_tip_is_committed_while_the_tip_advances() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), 0);

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
    let (config, domain) = domain_over(temp.path(), 0);
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
    let (config, domain) = domain_over(temp.path(), 0);

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
    let (config, domain) = domain_over(temp.path(), 0);

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
    let (config, domain) = domain_over(temp.path(), 0);
    let peer = peer(PeerOptions {
        initial_blocks: 50,
        ..Default::default()
    });

    let (progress, stages) = catch_up(&config, &peer, &domain);
    stages.teardown();
    assert!(progress.succeeded(SyncLimit::CatchUp));

    // Nothing is left to apply, so only the target's own wake can end it
    // before the housekeeping tick, a minute away.
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
    let (config, domain) = domain_over(temp.path(), 0);

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

    let failure = progress
        .upstream_failure()
        .expect("the upstream was given up");
    assert!(failure.contains("connecting"), "{failure}");
}

#[test]
fn a_dropped_intersection_is_retried() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), 1);

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
    let (config, domain) = domain_over(temp.path(), 1);

    // The first session closes after five new headers, uncounted; the
    // reconnect's intersection is dropped, the one failure allowed.
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
    let (config, domain) = domain_over(temp.path(), 2);

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
        failure.contains("after 3 failed attempts in a row"),
        "{failure}"
    );
}

#[test]
fn a_peer_that_serves_headers_but_no_block_is_given_up() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), 2);

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

    // The first session's headers were new, so its failure is not counted;
    // every later session repeats them.
    assert_eq!(peer.intersects(), 4);
    assert!(progress.upstream_failure().is_some());
}

#[test]
fn a_block_that_fails_to_apply_ends_a_catch_up_without_success() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path(), 0);

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

    assert_eq!(pull::backoff(&retry, 1), Duration::from_secs(4));
    assert_eq!(pull::backoff(&retry, 4), Duration::from_secs(32));
    assert_eq!(pull::backoff(&retry, 5), Duration::from_secs(60));
    assert_eq!(pull::backoff(&retry, 1000), Duration::from_secs(60));
}
