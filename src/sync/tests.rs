//! The catch-up criterion, over the real pull and apply stages syncing from a
//! local peer whose tip keeps advancing.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dolos_core::config::{ArchiveStoreConfig, ChainConfig, RetryConfig, StateStoreConfig};
use dolos_testing::peer::{PeerFixture, PeerOptions};
use gasket::daemon::Daemon;

use super::*;
use crate::engine::DomainBuilder;

const PREVIEW_MAGIC: u64 = 2;

fn domain_over(root: &Path) -> (RootConfig, DomainAdapter) {
    let mut config = RootConfig::new(ChainConfig::known("preview"));
    config.storage.path = root.to_path_buf();
    config.storage.state = StateStoreConfig::ephemeral();
    config.storage.archive = ArchiveStoreConfig::ephemeral();
    config.sync.retry = RetryConfig {
        max_retries: 0,
        backoff_unit_seconds: 0,
        ..Default::default()
    };

    let genesis = dolos_cardano::networks::PREVIEW.genesis();
    let domain = DomainBuilder::new(&config, Arc::new(genesis))
        .build()
        .unwrap();

    (config, domain)
}

fn start(
    config: &RootConfig,
    peer: &PeerFixture,
    limit: SyncLimit,
    domain: &DomainAdapter,
) -> Pipeline {
    sync(
        &config.sync,
        PREVIEW_MAGIC,
        peer.address().to_string(),
        limit,
        domain.clone(),
    )
    .unwrap()
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

#[test]
fn a_catch_up_ends_once_the_first_observed_tip_is_committed_while_the_tip_advances() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path());

    let peer = PeerFixture::start(
        "127.0.0.1:0",
        PREVIEW_MAGIC,
        PeerOptions {
            // Long enough a drain for the peer to produce blocks during it.
            initial_blocks: 200,
            produce_every: Some(Duration::from_millis(10)),
            ..Default::default()
        },
    )
    .unwrap();

    let sync = start(&config, &peer, SyncLimit::CatchUp, &domain);
    let progress = sync.progress.clone();
    let daemon = Daemon::new(sync.tethers);

    wait_until("the catch-up ends", Duration::from_secs(60), || {
        daemon.has_ended()
    });
    daemon.teardown();

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
fn a_follow_commits_the_target_and_keeps_going() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path());

    let peer = PeerFixture::start(
        "127.0.0.1:0",
        PREVIEW_MAGIC,
        PeerOptions {
            initial_blocks: 20,
            produce_every: Some(Duration::from_millis(20)),
            ..Default::default()
        },
    )
    .unwrap();

    let sync = start(&config, &peer, SyncLimit::Follow, &domain);
    let progress = sync.progress.clone();
    let daemon = Daemon::new(sync.tethers);

    wait_until("the target is committed", Duration::from_secs(60), || {
        progress.caught_up()
    });

    let target = progress.target().unwrap();

    wait_until(
        "the state passes the target",
        Duration::from_secs(10),
        || state_cursor(&domain).is_some_and(|cursor| cursor.slot() > target.slot() + 3),
    );

    assert!(!daemon.has_ended(), "a follow never ends on its own");
    assert!(!progress.succeeded(SyncLimit::Follow));

    daemon.teardown();
}

#[test]
fn a_catch_up_whose_peer_stops_ends_without_success_once_retries_run_out() {
    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path());

    let peer = PeerFixture::start(
        "127.0.0.1:0",
        PREVIEW_MAGIC,
        PeerOptions {
            initial_blocks: 50,
            stop_after_headers: Some(5),
            ..Default::default()
        },
    )
    .unwrap();

    let sync = start(&config, &peer, SyncLimit::CatchUp, &domain);
    let progress = sync.progress.clone();
    let daemon = Daemon::new(sync.tethers);

    wait_until("the sync ends", Duration::from_secs(60), || {
        daemon.has_ended()
    });
    daemon.teardown();

    assert!(peer.is_stopped());
    assert!(
        progress.target().is_none(),
        "the peer stopped short of its tip"
    );
    assert!(!progress.succeeded(SyncLimit::CatchUp));
}

#[test]
fn the_target_is_the_first_tip_fixed() {
    let progress = SyncProgress::default();
    let first = ChainPoint::Specific(10, dolos_testing::blocks::slot_to_hash(10));
    let later = ChainPoint::Specific(12, dolos_testing::blocks::slot_to_hash(12));

    assert!(progress.fix_target(first.clone()));
    assert!(!progress.fix_target(later));
    assert_eq!(progress.target(), Some(first));
}

#[test]
fn the_target_is_committed_only_where_both_stores_stand_on_it() {
    let point = |slot| ChainPoint::Specific(slot, dolos_testing::blocks::slot_to_hash(slot));
    let target = point(10);

    assert!(committed(&target, Some(&target), Some(&target)));

    // Short of it, past it on another branch, or on one store only.
    assert!(!committed(&target, Some(&point(9)), Some(&point(9))));
    assert!(!committed(&target, Some(&point(11)), Some(&point(11))));
    assert!(!committed(&target, Some(&target), Some(&point(9))));
    assert!(!committed(&target, None, None));

    // A rollback below the target moves the stores off it, the target stays.
    let below = point(5);
    assert!(!committed(&target, Some(&below), Some(&below)));

    // Origin, while no block is applied.
    assert!(committed(&ChainPoint::Origin, None, None));
    assert!(committed(
        &ChainPoint::Origin,
        Some(&ChainPoint::Origin),
        None
    ));
    assert!(!committed(
        &ChainPoint::Origin,
        Some(&target),
        Some(&target)
    ));
}
