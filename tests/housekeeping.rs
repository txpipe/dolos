//! Integration tests for the housekeeping (WAL/archive pruning) stack.
//!
//! Covers the layers above the store-level `prune_history` unit tests:
//! `Domain::housekeeping` orchestration (which stores it prunes and how it
//! combines their `done` flags), the `drain_housekeeping` batching loop, and
//! that store errors propagate instead of being swallowed — the failure mode
//! that let the WAL slot-range endianness bug (PR #1045) grow a WAL to ~15×
//! its budget undetected.

use std::sync::Arc;

use dolos_cardano::CardanoDelta;
use dolos_core::{
    config::{HistoryConfig, HistoryWindow},
    ArchiveStore as _, ArchiveWriter, ChainPoint, Domain, LogEntry, LogValue, WalStore,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    toy_domain::ToyDomain,
};

fn rolling(slots: u64) -> HistoryConfig {
    HistoryConfig::rolling(HistoryWindow::slots(slots))
}

/// The protocol's rollback window, which the WAL is always pruned to.
fn rollback_window(domain: &impl Domain) -> u64 {
    dolos_cardano::mutable_slots(&domain.genesis())
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

/// `slots`, then a tip one rollback window past the last of them, so that
/// pruning keeps exactly the last.
fn seed_wal_behind_tip(domain: &ToyDomain, slots: std::ops::Range<u64>) -> u64 {
    let last = slots.end - 1;
    let tip = last + rollback_window(domain);
    seed_wal(domain, slots);
    seed_wal(domain, tip..tip + 1);
    tip
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

/// Within one batch, a single call prunes the archive to `tip - window` and
/// the WAL to `tip - rollback window`, and reports `done`.
#[test]
fn housekeeping_prunes_both_stores() {
    let domain = ToyDomain::new(None, None).with_history_config(rolling(100));
    let tip = seed_wal_behind_tip(&domain, 2000..2600);
    seed_archive(&domain, 2000..2600);

    let done = domain.housekeeping().unwrap();
    assert!(done, "backlog within one batch must finish");

    assert_eq!(wal_tip(&domain), Some(tip), "wal tip preserved");
    assert_eq!(
        wal_start(&domain),
        Some(2599),
        "wal pruned to tip - rollback window"
    );

    assert_eq!(archive_tip(&domain), Some(2599), "archive tip preserved");
    let archived = archive_slots(&domain);
    assert_eq!(
        archived.first(),
        Some(&2499),
        "archive pruned to tip - history window"
    );
    assert_eq!(archived.last(), Some(&2599));
}

/// `housekeeping` returns the `&&` of the two stores' `done`: with a WAL
/// backlog beyond one batch it reports `false` until the loop drains it.
#[test]
fn housekeeping_returns_false_until_converged() {
    let domain = ToyDomain::new(None, None);
    let tip = seed_wal_behind_tip(&domain, 1..15_000); // excess ~15k > one 10k batch

    assert!(!domain.housekeeping().unwrap(), "backlog exceeds one batch");

    let mut rounds = 1;
    while !domain.housekeeping().unwrap() {
        rounds += 1;
        assert!(rounds < 100, "housekeeping did not converge");
    }

    assert_eq!(wal_tip(&domain), Some(tip), "tip preserved");
    assert_eq!(
        wal_start(&domain),
        Some(14_999),
        "converges to tip - rollback window"
    );
}

/// Stores within their windows are left untouched, and `housekeeping` still
/// reports `done`.
#[test]
fn housekeeping_leaves_stores_within_their_windows() {
    let domain = ToyDomain::new(None, None).with_history_config(rolling(1_000));
    seed_wal(&domain, 2000..2100);
    seed_archive(&domain, 2000..2100);

    let wal_before = (wal_start(&domain), wal_tip(&domain));
    let archive_before = archive_slots(&domain);

    assert!(domain.housekeeping().unwrap(), "nothing to prune → done");

    assert_eq!(
        (wal_start(&domain), wal_tip(&domain)),
        wal_before,
        "wal untouched"
    );
    assert_eq!(archive_slots(&domain), archive_before, "archive untouched");
}

/// Full history leaves the archive alone while the WAL is still pruned.
#[test]
fn full_history_prunes_only_the_wal() {
    let domain = ToyDomain::new(None, None); // default history: full
    seed_wal_behind_tip(&domain, 2000..2600);
    seed_archive(&domain, 2000..2600);

    let archive_before = archive_slots(&domain);

    assert!(domain.housekeeping().unwrap());

    assert_eq!(wal_start(&domain), Some(2599), "wal pruned to window");
    assert_eq!(
        archive_slots(&domain),
        archive_before,
        "archive untouched (full history)"
    );
}

/// A WAL store error surfaces out of `housekeeping` instead of being swallowed.
#[test]
fn housekeeping_propagates_wal_error() {
    let inner = ToyDomain::new(None, None);
    let domain = FaultyToyDomain::new(inner, TestFault::WalStoreError);

    assert!(
        domain.housekeeping().is_err(),
        "wal store error must surface"
    );
}

/// An archive store error surfaces out of `housekeeping`.
#[test]
fn housekeeping_propagates_archive_error() {
    let inner = ToyDomain::new(None, None).with_history_config(rolling(100));
    let domain = FaultyToyDomain::new(inner, TestFault::ArchiveStoreError);

    assert!(
        domain.housekeeping().is_err(),
        "archive store error must surface"
    );
}

/// `drain_housekeeping(None)` loops until the backlog is fully drained.
#[test]
fn drain_housekeeping_converges() {
    let domain = ToyDomain::new(None, None);
    let tip = seed_wal_behind_tip(&domain, 1..30_001); // needs multiple 10k batches

    let rounds = domain.drain_housekeeping(None).unwrap();

    assert!(rounds > 1, "multi-batch backlog must take multiple rounds");
    assert_eq!(wal_tip(&domain), Some(tip), "tip preserved");
    assert_eq!(
        wal_start(&domain),
        Some(30_000),
        "drained to tip - rollback window"
    );
}

/// `max_rounds` caps the loop: it stops after the budget with backlog left.
#[test]
fn drain_housekeeping_caps_at_max_rounds() {
    let domain = ToyDomain::new(None, None);
    let tip = seed_wal_behind_tip(&domain, 1..30_001);

    let rounds = domain.drain_housekeeping(Some(1)).unwrap();

    assert_eq!(rounds, 1, "stops after the round budget");
    assert_eq!(wal_tip(&domain), Some(tip), "tip preserved");
    assert!(
        wal_start(&domain).unwrap() < 30_000,
        "backlog remains: not yet drained to the window"
    );
}

/// `max_rounds` is an upper bound, not a fixed count: a converged run exits
/// early rather than spinning the remaining rounds.
#[test]
fn drain_housekeeping_stops_when_done() {
    let domain = ToyDomain::new(None, None);
    seed_wal_behind_tip(&domain, 1..600); // small backlog, converges in one round

    let rounds = domain.drain_housekeeping(Some(10)).unwrap();

    assert_eq!(rounds, 1, "converged run stops early, not at max_rounds");
}

/// A zero budget is a genuine no-op: no rounds run, nothing is pruned.
#[test]
fn drain_housekeeping_zero_rounds_is_noop() {
    let domain = ToyDomain::new(None, None);
    seed_wal_behind_tip(&domain, 2000..2600); // over the window

    let before = (wal_start(&domain), wal_tip(&domain));
    let rounds = domain.drain_housekeeping(Some(0)).unwrap();

    assert_eq!(rounds, 0, "zero budget runs no rounds");
    assert_eq!(
        (wal_start(&domain), wal_tip(&domain)),
        before,
        "wal untouched"
    );
}
