//! The restore driver, against real store sets.
//!
//! Four properties, and the order they are stated in is the order they get
//! harder to satisfy:
//!
//! 1. **A stele for another chain is refused before anything is written.** The
//!    one refusal whose timing is part of the requirement: half a mainnet
//!    ledger under a preprod configuration is not a state a node recovers from.
//! 2. **A failed restore leaves no cursor**, so `has_existing_data()` reports
//!    an empty node rather than a half-restored one.
//! 3. **Roundtrip.** A node built by the harness, exported, and restored into
//!    an empty store set is the node it came from: same cursor, same entities,
//!    same UTxO set, same archive, same index records, same tag queries. And it
//!    re-exports to the same inscription — the check that a restore did not
//!    merely land something self-consistent.
//! 4. **Cross-check against a node that never saw the stele.** An independently
//!    replayed ledger, compared against the restored one. This is what catches
//!    a restore that agrees with its own export and is wrong: the roundtrip
//!    cannot, because it compares against the same bytes.
//!
//! Every property runs on both live backend bindings — the builtin memory pair
//! and the on-disk fjall pair — because `append_prehashed` and `apply_utxoset`
//! are backend code and the restore is the only caller that drives them from a
//! wire format.

mod node;

use dolos_cardano::indexes::{archive_dimensions, index_delta_from_utxo_delta};
use dolos_core::{
    ArchiveStore, ChainPoint, Domain as _, EntityKey, EraCbor, ExactRecord, IndexStore, LogKey,
    StateStore, TagRecord, TxoRef, UtxoSet, UtxoSetDelta,
};
use dolos_snapshot::{
    restore::{self, Budget},
    Error, NAMESPACES, STATE, STATE_SHARDS, UTXOS,
};
use dolos_testing::toy_domain::{FjallStores, MemoryStores, ToyDomain, ToyStores};
use node::{export_to, harness, Blank};
use stelae::{dir::SteleDir, frame::Limits};

/// Every layer read through a window far below one record and every write
/// batch committed after a single record.
///
/// Both loops the restore owns — the refill inside a layer, and the chunking
/// that bounds a write batch — only run more than once when the data is bigger
/// than the budget. On a fixture that fits in one chunk they would never
/// execute at all, so the budget is shrunk to the fixture rather than the
/// fixture grown to a mainnet epoch.
fn shredded() -> Budget {
    Budget {
        limits: Limits {
            window: 8,
            ..Limits::default()
        },
        commit_records: 1,
        commit_bytes: 1,
    }
}

fn magic_of<B: ToyStores>(domain: &ToyDomain<B>) -> u64 {
    u64::from(domain.genesis().network_magic())
}

/// Export `domain` into a fresh directory and restore it into an empty store
/// set.
fn round_trip<B: ToyStores>(budget: Budget) -> (ToyDomain<B>, Blank<B>, restore::Summary) {
    let domain: ToyDomain<B> = harness();

    let temp = tempfile::tempdir().unwrap();
    export_to(temp.path(), &domain);

    let blank = Blank::<B>::open();
    let summary = restore_into(temp.path(), magic_of(&domain), &blank, budget).unwrap();

    // The directory is dropped with the tempdir at the end of this function, so
    // nothing below can be reading from it: a restored store set stands on its
    // own.
    (domain, blank, summary)
}

fn restore_into<B: ToyStores>(
    root: &std::path::Path,
    magic: u64,
    blank: &Blank<B>,
    budget: Budget,
) -> Result<restore::Summary, Error> {
    let stele = SteleDir::open(root)?;

    let plan = restore::plan(&stele, magic, None)?;
    plan.preflight(root)?;

    let index = stele.blob_index()?;

    restore::restore(
        &stele,
        &index,
        &plan,
        &blank.archive,
        blank.state(),
        blank.indexes(),
        budget,
    )
}

// --------------------------------------------------------------------------
// 1. Refusals
// --------------------------------------------------------------------------

/// Done criterion 3, first half.
///
/// The stele is preview's; the node claims to be mainnet. Nothing may be
/// written, and "nothing" is checked against the stores rather than inferred
/// from the error.
#[test]
fn a_stele_for_another_network_is_refused_before_anything_is_written() {
    let domain: ToyDomain = harness();

    let temp = tempfile::tempdir().unwrap();
    export_to(temp.path(), &domain);

    let blank = Blank::<MemoryStores>::open();

    let err = restore_into(
        temp.path(),
        dolos_snapshot::MAINNET_MAGIC,
        &blank,
        Budget::default(),
    )
    .unwrap_err();

    assert!(
        matches!(
            err,
            Error::NetworkMismatch {
                expected: dolos_snapshot::MAINNET_MAGIC,
                ..
            }
        ),
        "{err:?}"
    );

    assert_untouched(&blank);
}

/// Done criterion 3, second half.
///
/// One state shard's blob is removed, so the restore fails partway through the
/// tip — after epochs and other shards have landed. `set_cursor` is the last
/// thing a restore does, so what it leaves behind is a store set with data in
/// it and no cursor, which is what `bootstrap`'s `has_existing_data()` reads.
#[test]
fn a_restore_that_fails_partway_leaves_no_cursor() {
    let domain: ToyDomain = harness();

    let temp = tempfile::tempdir().unwrap();
    let inscription = export_to(temp.path(), &domain);

    // The last shard, so the failure lands after fifteen others have committed.
    let last = inscription
        .layers_of_kind(STATE)
        .last()
        .expect("a stele has state shards");

    let stele = SteleDir::open(temp.path()).unwrap();
    let blob = stele
        .blob_index()
        .unwrap()
        .blob_for(&last.diff_id)
        .expect("the shard's blob");

    std::fs::remove_file(stele.blob_path(&blob)).unwrap();

    let blank = Blank::<MemoryStores>::open();
    let err = restore_into(temp.path(), magic_of(&domain), &blank, Budget::default()).unwrap_err();

    assert!(
        matches!(err, Error::Stelae(stelae::Error::LayerNotFound { .. })),
        "{err:?}"
    );

    assert!(
        blank.state().read_cursor().unwrap().is_none(),
        "a failed restore left a cursor behind"
    );

    // And it really did get far enough to write: an assertion about what is
    // *not* there is worth nothing if nothing was ever attempted.
    assert!(
        blank
            .archive
            .get_range(None, None)
            .unwrap()
            .next()
            .is_some(),
        "the restore failed before it wrote anything, so the cursor proves nothing"
    );
}

fn assert_untouched<B: ToyStores>(blank: &Blank<B>) {
    assert!(blank.state().read_cursor().unwrap().is_none(), "a cursor");
    assert!(
        blank
            .archive
            .get_range(None, None)
            .unwrap()
            .next()
            .is_none(),
        "a block"
    );
    assert!(
        blank.indexes().cursor().unwrap().is_none(),
        "an index cursor"
    );

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        assert!(
            blank
                .state()
                .iter_entities(ns, EntityKey::full_range())
                .unwrap()
                .next()
                .is_none(),
            "an entity under {ns}"
        );
    }

    assert!(
        blank.state().iter_utxos().unwrap().next().is_none(),
        "a utxo"
    );
}

// --------------------------------------------------------------------------
// 2. Roundtrip
// --------------------------------------------------------------------------

/// Done criterion 1.
fn roundtrip<B: ToyStores>() {
    let (domain, blank, summary) = round_trip::<B>(shredded());

    assert_stores_match(&blank, &domain);

    // The summary is what an operator reads, so it has to be the truth rather
    // than a count of loop iterations.
    assert_eq!(summary.blocks, blocks_of(domain.archive()).len() as u64);
    assert_eq!(summary.utxos, utxos_of(domain.state()).len() as u64);
    assert!(summary.entities > 0, "the fixture restored no entities");
    assert!(summary.logs > 0, "the fixture restored no logs");
    assert!(
        summary.index_records > 0,
        "the fixture restored no index records"
    );

    // And the restored node publishes the stele it was restored from. A restore
    // that landed something merely self-consistent moves this digest.
    let from_original = tempfile::tempdir().unwrap();
    let from_restored = tempfile::tempdir().unwrap();

    let original = export_to(from_original.path(), &domain);

    let restored = {
        let stele = SteleDir::create(from_restored.path()).unwrap();
        let plan = dolos_snapshot::export::plan(blank.state(), magic_of(&domain)).unwrap();

        dolos_snapshot::export::export(
            &stele,
            &plan,
            &blank.archive,
            blank.state(),
            blank.indexes(),
            None,
        )
        .unwrap()
    };

    assert_eq!(
        original.digest().unwrap(),
        restored.digest().unwrap(),
        "the restored node does not publish the stele it was restored from"
    );
}

#[test]
fn a_restored_node_is_the_node_it_came_from_on_memory() {
    roundtrip::<MemoryStores>();
}

#[test]
fn a_restored_node_is_the_node_it_came_from_on_fjall() {
    roundtrip::<FjallStores>();
}

// --------------------------------------------------------------------------
// 3. Cross-check
// --------------------------------------------------------------------------

/// Done criterion 2.
///
/// The comparison is against a node built by `ImportExt::import_blocks` over
/// the same blocks and seeded the same way — [`harness`] itself, run a second
/// time — which has never seen the stele. The roundtrip above compares a
/// restore against the export it came from and so cannot tell a faithful
/// restore from one that reproduced the export's own mistake; this can.
fn cross_check<B: ToyStores>() {
    let (_, blank, _) = round_trip::<B>(Budget::default());
    let replayed: ToyDomain<B> = harness();

    assert_stores_match(&blank, &replayed);
}

#[test]
fn a_restored_node_matches_a_replayed_one_on_memory() {
    cross_check::<MemoryStores>();
}

#[test]
fn a_restored_node_matches_a_replayed_one_on_fjall() {
    cross_check::<FjallStores>();
}

// --------------------------------------------------------------------------
// The comparison
// --------------------------------------------------------------------------

fn assert_stores_match<B: ToyStores>(restored: &Blank<B>, original: &ToyDomain<B>) {
    assert_state_matches(restored.state(), original.state());
    assert_archive_matches(&restored.archive, original.archive());
    assert_indexes_match(restored.indexes(), original.indexes(), original.state());
}

fn assert_state_matches<S: StateStore>(restored: &S, original: &S) {
    assert_eq!(
        restored.read_cursor().unwrap(),
        original.read_cursor().unwrap(),
        "cursor"
    );

    assert!(
        matches!(
            restored.read_cursor().unwrap(),
            Some(ChainPoint::Specific(..))
        ),
        "a restored cursor has to be anchored, or the WAL cannot be reseeded from it"
    );

    let mut any = false;

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        let left = entities_of(restored, ns);
        let right = entities_of(original, ns);

        any |= !right.is_empty();

        assert_eq!(left, right, "entities under {ns}");
    }

    assert!(any, "the fixture has no entities, so this proves nothing");

    let utxos = utxos_of(original);
    assert!(!utxos.is_empty(), "the fixture has no utxos");
    assert_eq!(utxos_of(restored), utxos, "the utxo set");
}

fn assert_archive_matches<A: ArchiveStore>(restored: &A, original: &A) {
    let blocks = blocks_of(original);
    assert!(!blocks.is_empty(), "the fixture archived no blocks");
    assert_eq!(blocks_of(restored), blocks, "blocks");

    let mut any = false;

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        let left = logs_of(restored, ns);
        let right = logs_of(original, ns);

        any |= !right.is_empty();

        assert_eq!(left, right, "logs under {ns}");
    }

    assert!(any, "the fixture wrote no logs, so this proves nothing");
}

/// Both halves of the index store: the archive records the layers carry, and
/// the live-UTxO dimensions they deliberately do not.
///
/// The second half is the point. `utxo::*` tags are never shipped — ADR-004's
/// Amendment 2 — so they exist in a restored node only because the restore
/// rebuilt them, and only a query proves it did.
fn assert_indexes_match<I: IndexStore, S: StateStore>(restored: &I, original: &I, state: &S) {
    assert_eq!(
        restored.cursor().unwrap(),
        original.cursor().unwrap(),
        "cursor"
    );

    let tags = tags_of(original);
    assert!(!tags.is_empty(), "the fixture produced no archive tags");
    assert_eq!(tags_of(restored), tags, "archive tags");

    let exact = exact_of(original);
    assert!(!exact.is_empty(), "the fixture produced no exact records");
    assert_eq!(exact_of(restored), exact, "exact records");

    // Every dimension the ledger tagged the restored UTxO set under, asked of
    // both stores.
    let delta = UtxoSetDelta {
        produced_utxo: utxos_of(state)
            .into_iter()
            .map(|(txo, value)| (txo, std::sync::Arc::new(value)))
            .collect(),
        ..Default::default()
    };

    let rebuilt = index_delta_from_utxo_delta(ChainPoint::Origin, &delta);
    let mut asked = 0usize;

    for (txo, tags) in &rebuilt.utxo.produced {
        for tag in tags {
            let left: UtxoSet = restored.utxos_by_tag(tag.dimension, &tag.key).unwrap();
            let right: UtxoSet = original.utxos_by_tag(tag.dimension, &tag.key).unwrap();

            assert!(
                left.contains(txo),
                "the rebuilt index lost {txo:?} under {}",
                tag.dimension
            );
            assert_eq!(left, right, "utxos under {}", tag.dimension);

            asked += 1;
        }
    }

    assert!(
        asked > 0,
        "the fixture's utxos carry no tags, so the rebuild proves nothing"
    );
}

fn entities_of<S: StateStore>(store: &S, ns: &'static str) -> Vec<(EntityKey, Vec<u8>)> {
    store
        .iter_entities(ns, EntityKey::full_range())
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn utxos_of<S: StateStore>(store: &S) -> std::collections::BTreeMap<TxoRef, EraCbor> {
    store.iter_utxos().unwrap().map(Result::unwrap).collect()
}

fn blocks_of<A: ArchiveStore>(store: &A) -> Vec<(u64, Vec<u8>)> {
    store.get_range(None, None).unwrap().collect()
}

fn logs_of<A: ArchiveStore>(store: &A, ns: &'static str) -> Vec<(LogKey, Vec<u8>)> {
    store
        .iter_logs(ns, LogKey::full_range())
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn tags_of<I: IndexStore>(store: &I) -> Vec<TagRecord> {
    let mut found: Vec<TagRecord> = store
        .iter_archive_tags(&archive_dimensions::ALL, 0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    found.sort();
    found
}

fn exact_of<I: IndexStore>(store: &I) -> Vec<ExactRecord> {
    let mut found: Vec<ExactRecord> = store
        .iter_exact_records(0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    found.sort_by_key(|record| (record.kind, record.key().to_vec()));
    found
}

// --------------------------------------------------------------------------
// Selection
// --------------------------------------------------------------------------

/// A stele always carries the whole tip, whatever history travels with it, so
/// `sync.max_history` never touches the state shards — only the epoch layers.
#[test]
fn max_history_selects_epochs_and_never_the_tip() {
    let domain: ToyDomain = harness();

    let temp = tempfile::tempdir().unwrap();
    export_to(temp.path(), &domain);

    let stele = SteleDir::open(temp.path()).unwrap();

    for max_history in [None, Some(0), Some(u64::MAX)] {
        let plan = restore::plan(&stele, magic_of(&domain), max_history).unwrap();

        assert_eq!(plan.state.len(), STATE_SHARDS as usize, "{max_history:?}");

        // The fixture stands inside epoch zero, so every window reaches the tip
        // and nothing is ever dropped — which is the assertion, not a shortcut:
        // the epoch the cursor stands in must survive any window.
        assert_eq!(plan.skipped_epochs, 0, "{max_history:?}");
        assert_eq!(plan.epochs.len(), 1, "{max_history:?}");
    }
}
