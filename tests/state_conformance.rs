//! Semantic conformance for the state store contract.
//!
//! Written once against the [`StateStore`] trait and run against every live
//! backend: fjall, the persistent one, and the builtin memory store. A backend
//! that only ever ran its own tests could drift from its peers without anything
//! failing — and the seam this pins, `iter_utxos`, feeds a snapshot layer, so a
//! backend that iterated a slightly different set would publish one.
//!
//! Distinct from `tests/memory.rs`, which measures *allocation* behaviour: the
//! disk backends promise lazy, O(1)-memory iteration, and the builtin memory
//! store deliberately does not (it materializes). That is a documented property
//! of each backend rather than a shared semantic, so it stays there.

use std::collections::HashMap;
use std::sync::Arc;

use dolos_core::{
    builtin::MemoryStateStore, config::FjallStateConfig, ChainPoint, EntityKey, EraCbor, TxoRef,
    UtxoSetDelta,
};
use dolos_core::{StateStore as CoreStateStore, StateWriter as CoreStateWriter};

const ACCOUNTS: &str = "accounts";
const POOLS: &str = "pools";

/// One state backend under test. The guard keeps whatever the backend needs
/// alive — a temp directory for the on-disk one, nothing for the other.
trait Backend {
    type Store: CoreStateStore;
    type Guard;

    fn open() -> (Self::Store, Self::Guard);
}

struct Fjall;

impl Backend for Fjall {
    type Store = dolos_fjall::StateStore;
    type Guard = tempfile::TempDir;

    fn open() -> (Self::Store, Self::Guard) {
        let dir = tempfile::tempdir().expect("failed to create tempdir");

        let config = FjallStateConfig {
            path: None,
            cache: Some(16),
            max_history: None,
            max_journal_size: None,
            flush_on_commit: Some(false),
            l0_threshold: None,
            worker_threads: Some(1),
            memtable_size_mb: None,
        };

        let store = dolos_fjall::StateStore::open(dir.path(), &config)
            .expect("failed to open fjall state store");

        (store, dir)
    }
}

struct Memory;

impl Backend for Memory {
    type Store = MemoryStateStore;
    type Guard = ();

    fn open() -> (Self::Store, Self::Guard) {
        (MemoryStateStore::new(), ())
    }
}

macro_rules! conformance_suite {
    ($module:ident, $backend:ty) => {
        mod $module {
            use super::*;

            #[test]
            fn cursor_starts_empty_and_survives_commit() {
                super::cursor_starts_empty_and_survives_commit::<$backend>();
            }

            #[test]
            fn entities_are_namespaced() {
                super::entities_are_namespaced::<$backend>();
            }

            #[test]
            fn entity_range_is_half_open_and_ordered() {
                super::entity_range_is_half_open_and_ordered::<$backend>();
            }

            #[test]
            fn writes_are_invisible_until_commit() {
                super::writes_are_invisible_until_commit::<$backend>();
            }

            #[test]
            fn iter_utxos_yields_the_whole_set_in_ref_order() {
                super::iter_utxos_yields_the_whole_set_in_ref_order::<$backend>();
            }

            #[test]
            fn utxo_delta_produces_consumes_recovers_and_undoes() {
                super::utxo_delta_produces_consumes_recovers_and_undoes::<$backend>();
            }

            #[test]
            fn consumption_wins_within_one_delta() {
                super::consumption_wins_within_one_delta::<$backend>();
            }
        }
    };
}

conformance_suite!(fjall, Fjall);
conformance_suite!(memory, Memory);

fn key(n: u8) -> EntityKey {
    EntityKey::from(&[n; 32])
}

fn txo(n: u8, idx: u32) -> TxoRef {
    TxoRef([n; 32].into(), idx)
}

fn utxo(n: u8) -> Arc<EraCbor> {
    Arc::new(EraCbor(6, vec![n; 8]))
}

fn produce(refs: &[(TxoRef, Arc<EraCbor>)]) -> UtxoSetDelta {
    UtxoSetDelta {
        produced_utxo: refs.iter().cloned().collect(),
        ..Default::default()
    }
}

fn apply_utxos<S: CoreStateStore>(store: &S, delta: &UtxoSetDelta) {
    let writer = store.start_writer().expect("start_writer failed");
    writer.apply_utxoset(delta).expect("apply_utxoset failed");
    writer.commit().expect("commit failed");
}

fn all_utxos<S: CoreStateStore>(store: &S) -> Vec<(TxoRef, EraCbor)> {
    store
        .iter_utxos()
        .expect("iter_utxos failed")
        .collect::<Result<Vec<_>, _>>()
        .expect("utxo iteration failed")
}

fn cursor_starts_empty_and_survives_commit<B: Backend>() {
    let (store, _guard) = B::open();

    assert_eq!(store.read_cursor().expect("read_cursor failed"), None);

    let writer = store.start_writer().expect("start_writer failed");
    writer
        .set_cursor(ChainPoint::Slot(42))
        .expect("set_cursor failed");
    writer.commit().expect("commit failed");

    assert_eq!(
        store.read_cursor().expect("read_cursor failed"),
        Some(ChainPoint::Slot(42))
    );
}

fn entities_are_namespaced<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().expect("start_writer failed");
    writer
        .write_entity(ACCOUNTS, &key(1), &vec![0xaa])
        .expect("write_entity failed");
    writer
        .write_entity(POOLS, &key(1), &vec![0xbb])
        .expect("write_entity failed");
    writer.commit().expect("commit failed");

    // The same key in two namespaces is two entities.
    assert_eq!(
        store.read_entities(ACCOUNTS, &[&key(1)]).unwrap(),
        vec![Some(vec![0xaa])]
    );
    assert_eq!(
        store.read_entities(POOLS, &[&key(1)]).unwrap(),
        vec![Some(vec![0xbb])]
    );

    // A missing key reads as a hole in the right position, not a short vector.
    assert_eq!(
        store
            .read_entities(ACCOUNTS, &[&key(0), &key(1), &key(2)])
            .unwrap(),
        vec![None, Some(vec![0xaa]), None]
    );

    // Deleting one namespace's entry leaves the other alone.
    let writer = store.start_writer().expect("start_writer failed");
    writer
        .delete_entity(ACCOUNTS, &key(1))
        .expect("delete_entity failed");
    writer.commit().expect("commit failed");

    assert_eq!(
        store.read_entities(ACCOUNTS, &[&key(1)]).unwrap(),
        vec![None]
    );
    assert_eq!(
        store.read_entities(POOLS, &[&key(1)]).unwrap(),
        vec![Some(vec![0xbb])]
    );
}

fn entity_range_is_half_open_and_ordered<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().expect("start_writer failed");
    for n in [1u8, 2, 3, 4] {
        writer
            .write_entity(ACCOUNTS, &key(n), &vec![n])
            .expect("write_entity failed");
    }
    // A neighbouring namespace whose keys must not show up in the scan.
    writer
        .write_entity(POOLS, &key(2), &vec![0xff])
        .expect("write_entity failed");
    writer.commit().expect("commit failed");

    let collect = |range| {
        store
            .iter_entities(ACCOUNTS, range)
            .expect("iter_entities failed")
            .collect::<Result<Vec<_>, _>>()
            .expect("entity iteration failed")
    };

    let all = collect(EntityKey::full_range());
    assert_eq!(
        all.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![key(1), key(2), key(3), key(4)],
        "entities come out in key order, and only from the requested namespace"
    );

    // Half-open: the start is included, the end is not.
    let middle = collect(key(2)..key(4));
    assert_eq!(
        middle.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![key(2), key(3)]
    );

    assert!(
        collect(key(2)..key(2)).is_empty(),
        "an empty range is empty"
    );

    // An inverted range matches nothing rather than blowing up: the disk
    // backends compare encoded bytes and simply find none, so an ordered-map
    // backend has to reach the same answer instead of panicking on the bound.
    assert!(
        collect(key(4)..key(2)).is_empty(),
        "an inverted range is empty"
    );
}

fn writes_are_invisible_until_commit<B: Backend>() {
    let (store, _guard) = B::open();

    apply_utxos(&store, &produce(&[(txo(1, 0), utxo(1))]));

    let writer = store.start_writer().expect("start_writer failed");
    writer
        .write_entity(ACCOUNTS, &key(9), &vec![0x09])
        .expect("write_entity failed");
    writer
        .apply_utxoset(&produce(&[(txo(2, 0), utxo(2))]))
        .expect("apply_utxoset failed");
    writer
        .set_cursor(ChainPoint::Slot(7))
        .expect("set_cursor failed");

    // Everything above is still pending: a reader sees the store as it was.
    assert_eq!(
        store.read_entities(ACCOUNTS, &[&key(9)]).unwrap(),
        vec![None]
    );
    assert_eq!(all_utxos(&store).len(), 1);
    assert_eq!(store.read_cursor().unwrap(), None);

    writer.commit().expect("commit failed");

    assert_eq!(
        store.read_entities(ACCOUNTS, &[&key(9)]).unwrap(),
        vec![Some(vec![0x09])]
    );
    assert_eq!(all_utxos(&store).len(), 2);
    assert_eq!(store.read_cursor().unwrap(), Some(ChainPoint::Slot(7)));
}

fn iter_utxos_yields_the_whole_set_in_ref_order<B: Backend>() {
    let (store, _guard) = B::open();

    // Written out of order, and with more than one index per tx hash, so the
    // ordering claim has something to bite on.
    let refs = [
        (txo(3, 1), utxo(31)),
        (txo(1, 2), utxo(12)),
        (txo(3, 0), utxo(30)),
        (txo(1, 0), utxo(10)),
        (txo(2, 0), utxo(20)),
    ];
    apply_utxos(&store, &produce(&refs));

    let iterated = all_utxos(&store);

    assert_eq!(
        iterated.len(),
        refs.len(),
        "iter_utxos yields the whole set"
    );

    let expected: HashMap<TxoRef, EraCbor> = refs
        .iter()
        .map(|(txo, value)| (txo.clone(), value.as_ref().clone()))
        .collect();
    assert_eq!(
        iterated.iter().cloned().collect::<HashMap<_, _>>(),
        expected,
        "iter_utxos yields exactly what was written, values included"
    );

    let order: Vec<TxoRef> = iterated.iter().map(|(txo, _)| txo.clone()).collect();
    let mut sorted = order.clone();
    sorted.sort();
    assert_eq!(
        order, sorted,
        "iter_utxos yields refs in (tx_hash, index) order — what every backend's \
         key encoding sorts by"
    );

    // The same set, asked for by ref.
    let by_ref = store
        .get_utxos(refs.iter().map(|(txo, _)| txo.clone()).collect())
        .expect("get_utxos failed");
    assert_eq!(by_ref.len(), refs.len());
    for (txo, value) in &refs {
        assert_eq!(by_ref.get(txo).map(|v| v.as_ref()), Some(value.as_ref()));
    }

    // A ref that was never produced is absent rather than an error.
    let missing = store
        .get_utxos(vec![txo(9, 9)])
        .expect("get_utxos failed for an unknown ref");
    assert!(missing.is_empty());
}

fn utxo_delta_produces_consumes_recovers_and_undoes<B: Backend>() {
    let (store, _guard) = B::open();

    apply_utxos(
        &store,
        &produce(&[(txo(1, 0), utxo(1)), (txo(2, 0), utxo(2))]),
    );

    // Consume one: it leaves the set.
    apply_utxos(
        &store,
        &UtxoSetDelta {
            consumed_utxo: [(txo(1, 0), utxo(1))].into_iter().collect(),
            ..Default::default()
        },
    );
    assert_eq!(
        all_utxos(&store)
            .into_iter()
            .map(|(txo, _)| txo)
            .collect::<Vec<_>>(),
        vec![txo(2, 0)]
    );

    // Roll that back: `recovered_stxi` puts a consumed UTxO back, value and
    // all, and `undone_utxo` takes a produced one away again.
    apply_utxos(
        &store,
        &UtxoSetDelta {
            recovered_stxi: [(txo(1, 0), utxo(1))].into_iter().collect(),
            undone_utxo: [(txo(2, 0), utxo(2))].into_iter().collect(),
            ..Default::default()
        },
    );

    let remaining = all_utxos(&store);
    assert_eq!(
        remaining,
        vec![(txo(1, 0), utxo(1).as_ref().clone())],
        "recovered UTxOs come back whole; undone ones are gone"
    );
}

/// A delta that both produces and consumes the same ref leaves it absent.
///
/// The backends order their write batches differently — fjall removes consumed
/// then undone, redb3 the other way round — but every one of them applies both
/// removal sets after both insertion sets, so the outcome is a property of the
/// delta rather than of the backend. Pinning it here keeps it that way.
fn consumption_wins_within_one_delta<B: Backend>() {
    let (store, _guard) = B::open();

    apply_utxos(
        &store,
        &UtxoSetDelta {
            produced_utxo: [(txo(1, 0), utxo(1)), (txo(2, 0), utxo(2))]
                .into_iter()
                .collect(),
            consumed_utxo: [(txo(1, 0), utxo(1))].into_iter().collect(),
            ..Default::default()
        },
    );

    assert_eq!(
        all_utxos(&store)
            .into_iter()
            .map(|(txo, _)| txo)
            .collect::<Vec<_>>(),
        vec![txo(2, 0)]
    );
}

/// The two live backends have to agree on the UTxO set the same delta produces,
/// and on the order they hand it over: `iter_utxos` feeds a snapshot layer, so
/// the sequence is the published content.
#[test]
fn backends_agree_on_the_iterated_utxo_set() {
    let (fjall, _guard) = Fjall::open();
    let (memory, _) = Memory::open();

    let delta = UtxoSetDelta {
        produced_utxo: [
            (txo(7, 3), utxo(73)),
            (txo(1, 0), utxo(10)),
            (txo(7, 0), utxo(70)),
            (txo(4, 9), utxo(49)),
        ]
        .into_iter()
        .collect(),
        consumed_utxo: [(txo(4, 9), utxo(49))].into_iter().collect(),
        ..Default::default()
    };

    apply_utxos(&fjall, &delta);
    apply_utxos(&memory, &delta);

    assert_eq!(all_utxos(&fjall), all_utxos(&memory));
}
