//! Integration tests for the housekeeping (WAL/archive pruning) stack.
//!
//! Covers the layers above the store-level `prune_history` unit tests:
//! `Domain::housekeeping` orchestration (which stores it prunes and how it
//! combines their `done` flags), the `drain_housekeeping` batching loop, and
//! that store errors propagate instead of being swallowed — the failure mode
//! that let the WAL slot-range endianness bug (PR #1045) grow a WAL to ~15×
//! its budget undetected.

use std::sync::Arc;

use dolos_core::{
    config::SyncConfig, ArchiveWriter, ChainPoint, Domain, LogEntry, LogValue, WalStore,
};
use dolos_cardano::CardanoDelta;
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    toy_domain::ToyDomain,
};

/// `SyncConfig` with only the pruning windows set (other fields default).
fn sync_config(max_rollback: Option<u64>, max_history: Option<u64>) -> SyncConfig {
    let mut cfg = SyncConfig::default();
    cfg.max_rollback = max_rollback;
    cfg.max_history = max_history;
    cfg
}

/// WAL entry at `slot` with a nonzero hash, so it survives `remove_before` at
/// its own slot (the zero-hash cutoff bound compares strictly below it).
fn wal_entry(slot: u64) -> LogEntry<CardanoDelta> {
    let mut hash = [1u8; 32];
    hash[0..8].copy_from_slice(&slot.to_be_bytes());
    (ChainPoint::Specific(slot, hash.into()), LogValue::origin())
}

fn seed_wal(domain: &ToyDomain, slots: std::ops::Range<u64>) {
    let entries: Vec<_> = slots.map(wal_entry).collect();
    domain.wal().append_entries(&entries).unwrap();
}

fn wal_start(domain: &impl Domain) -> Option<u64> {
    domain.wal().find_start().unwrap().map(|(p, _)| p.slot())
}

fn wal_tip(domain: &impl Domain) -> Option<u64> {
    domain.wal().find_tip().unwrap().map(|(p, _)| p.slot())
}

fn seed_archive(domain: &ToyDomain, slots: std::ops::Range<u64>) {
    let writer = domain.archive().start_writer().unwrap();
    for slot in slots {
        let point = ChainPoint::Specific(slot, [0u8; 32].into());
        let block = Arc::new(format!("block-{slot}").into_bytes());
        writer.apply(&point, &block).unwrap();
    }
    writer.commit().unwrap();
}

/// Block slots currently indexed by the archive, ascending.
fn archive_slots(domain: &ToyDomain) -> Vec<u64> {
    domain
        .archive()
        .get_range(None, None)
        .unwrap()
        .map(|(s, _)| s)
        .collect()
}

fn archive_tip(domain: &ToyDomain) -> Option<u64> {
    domain.archive().get_tip().unwrap().map(|(s, _)| s)
}

/// Both windows configured and within one batch: a single call prunes both
/// stores to `tip - window` and reports `done`.
#[test]
fn housekeeping_prunes_configured_stores() {
    let domain = ToyDomain::new(None, None).with_sync_config(sync_config(Some(100), Some(100)));
    seed_wal(&domain, 2000..2600); // tip 2599
    seed_archive(&domain, 2000..2600);

    let done = domain.housekeeping().unwrap();
    assert!(done, "backlog within one batch must finish");

    assert_eq!(wal_tip(&domain), Some(2599), "wal tip preserved");
    assert_eq!(wal_start(&domain), Some(2499), "wal pruned to tip - max_rollback");

    assert_eq!(archive_tip(&domain), Some(2599), "archive tip preserved");
    let archived = archive_slots(&domain);
    assert_eq!(archived.first(), Some(&2499), "archive pruned to tip - max_history");
    assert_eq!(archived.last(), Some(&2599));
}

/// `housekeeping` returns the `&&` of the two stores' `done`: with a WAL
/// backlog beyond one batch it reports `false` until the loop drains it.
#[test]
fn housekeeping_returns_false_until_converged() {
    let domain = ToyDomain::new(None, None).with_sync_config(sync_config(Some(1000), None));
    seed_wal(&domain, 1..15_000); // tip 14_999, excess ~14k > one 10k batch

    assert!(!domain.housekeeping().unwrap(), "backlog exceeds one batch");

    let mut rounds = 1;
    while !domain.housekeeping().unwrap() {
        rounds += 1;
        assert!(rounds < 100, "housekeeping did not converge");
    }

    assert_eq!(wal_tip(&domain), Some(14_999), "tip preserved");
    assert_eq!(wal_start(&domain), Some(13_999), "converges to tip - max_rollback");
}

/// Unconfigured windows (`None`) leave a store untouched, and `housekeeping`
/// still reports `done`.
#[test]
fn housekeeping_skips_unconfigured_stores() {
    let domain = ToyDomain::new(None, None); // default sync_config: both None
    seed_wal(&domain, 2000..2100);
    seed_archive(&domain, 2000..2100);

    let wal_before = (wal_start(&domain), wal_tip(&domain));
    let archive_before = archive_slots(&domain);

    assert!(domain.housekeeping().unwrap(), "nothing to prune → done");

    assert_eq!((wal_start(&domain), wal_tip(&domain)), wal_before, "wal untouched");
    assert_eq!(archive_slots(&domain), archive_before, "archive untouched");
}

/// Only the configured store is pruned; the other is left alone.
#[test]
fn housekeeping_only_prunes_configured_store() {
    let domain = ToyDomain::new(None, None).with_sync_config(sync_config(Some(100), None));
    seed_wal(&domain, 2000..2600); // tip 2599
    seed_archive(&domain, 2000..2600);

    let archive_before = archive_slots(&domain);

    assert!(domain.housekeeping().unwrap());

    assert_eq!(wal_start(&domain), Some(2499), "wal pruned to window");
    assert_eq!(archive_slots(&domain), archive_before, "archive untouched (max_history None)");
}

/// A WAL store error surfaces out of `housekeeping` instead of being swallowed.
#[test]
fn housekeeping_propagates_wal_error() {
    let inner = ToyDomain::new(None, None).with_sync_config(sync_config(Some(100), None));
    let domain = FaultyToyDomain::new(inner, TestFault::WalStoreError);

    assert!(domain.housekeeping().is_err(), "wal store error must surface");
}

/// An archive store error surfaces out of `housekeeping`.
#[test]
fn housekeeping_propagates_archive_error() {
    let inner = ToyDomain::new(None, None).with_sync_config(sync_config(None, Some(100)));
    let domain = FaultyToyDomain::new(inner, TestFault::ArchiveStoreError);

    assert!(domain.housekeeping().is_err(), "archive store error must surface");
}

/// `drain_housekeeping(None)` loops until the backlog is fully drained.
#[test]
fn drain_housekeeping_converges() {
    let domain = ToyDomain::new(None, None).with_sync_config(sync_config(Some(1000), None));
    seed_wal(&domain, 1..30_001); // tip 30_000, needs multiple 10k batches

    let rounds = domain.drain_housekeeping(None).unwrap();

    assert!(rounds > 1, "multi-batch backlog must take multiple rounds");
    assert_eq!(wal_tip(&domain), Some(30_000), "tip preserved");
    assert_eq!(wal_start(&domain), Some(29_000), "drained to tip - max_rollback");
}

/// `max_rounds` caps the loop: it stops after the budget with backlog left.
#[test]
fn drain_housekeeping_caps_at_max_rounds() {
    let domain = ToyDomain::new(None, None).with_sync_config(sync_config(Some(1000), None));
    seed_wal(&domain, 1..30_001); // tip 30_000

    let rounds = domain.drain_housekeeping(Some(1)).unwrap();

    assert_eq!(rounds, 1, "stops after the round budget");
    assert_eq!(wal_tip(&domain), Some(30_000), "tip preserved");
    assert!(
        wal_start(&domain).unwrap() < 29_000,
        "backlog remains: not yet drained to the window"
    );
}

/// `max_rounds` is an upper bound, not a fixed count: a converged run exits
/// early rather than spinning the remaining rounds.
#[test]
fn drain_housekeeping_stops_when_done() {
    let domain = ToyDomain::new(None, None).with_sync_config(sync_config(Some(100), None));
    seed_wal(&domain, 1..600); // small backlog, converges in one round

    let rounds = domain.drain_housekeeping(Some(10)).unwrap();

    assert_eq!(rounds, 1, "converged run stops early, not at max_rounds");
}

/// A zero budget is a genuine no-op: no rounds run, nothing is pruned.
#[test]
fn drain_housekeeping_zero_rounds_is_noop() {
    let domain = ToyDomain::new(None, None).with_sync_config(sync_config(Some(100), None));
    seed_wal(&domain, 2000..2600); // tip 2599, over the window

    let before = (wal_start(&domain), wal_tip(&domain));
    let rounds = domain.drain_housekeeping(Some(0)).unwrap();

    assert_eq!(rounds, 0, "zero budget runs no rounds");
    assert_eq!((wal_start(&domain), wal_tip(&domain)), before, "wal untouched");
}
