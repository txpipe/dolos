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
    builtin::MemoryStateStore, ChainPoint, EntityKey, EraCbor, KeyBytes, KeyRange, RangeOf, Tag,
    TagEntry, TagOrder, TagStart, TxoRef, UtxoIndexDelta, UtxoPosition, UtxoSet, UtxoSetDelta,
    UtxoValue,
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

        let store = dolos_fjall::StateStore::open(dir.path(), dolos_fjall::Preset::Standard)
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
            fn entities_of_mixed_widths_share_a_namespace() {
                super::entities_of_mixed_widths_share_a_namespace::<$backend>();
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

            #[test]
            fn apply_then_utxos_by_tag_finds_the_ref() {
                super::apply_then_utxos_by_tag_finds_the_ref::<$backend>();
            }

            #[test]
            fn undo_utxo_tags_removes_exactly_what_apply_added() {
                super::undo_utxo_tags_removes_exactly_what_apply_added::<$backend>();
            }

            #[test]
            fn undo_leaves_no_tag_for_an_output_its_block_spent() {
                super::undo_leaves_no_tag_for_an_output_its_block_spent::<$backend>();
            }

            #[test]
            fn consumed_refs_leave_the_tag() {
                super::consumed_refs_leave_the_tag::<$backend>();
            }

            #[test]
            fn tag_pages_walk_creation_order() {
                super::tag_pages_walk_creation_order::<$backend>();
            }

            #[test]
            fn tag_pages_resume_after_an_entry() {
                super::tag_pages_resume_after_an_entry::<$backend>();
            }

            #[test]
            fn tag_reads_skip_longer_lookup_keys() {
                super::tag_reads_skip_longer_lookup_keys::<$backend>();
            }

            #[test]
            fn utxos_with_tag_probes_the_full_key() {
                super::utxos_with_tag_probes_the_full_key::<$backend>();
            }
        }
    };
}

conformance_suite!(fjall, Fjall);
conformance_suite!(memory, Memory);

fn key(n: u8) -> EntityKey {
    EntityKey::of(&[n; 32])
}

/// `len` bytes of `n`.
fn key_of(n: u8, len: usize) -> EntityKey {
    EntityKey::from_raw(&vec![n; len])
}

fn range(start: &EntityKey, end: &EntityKey) -> KeyRange {
    KeyRange {
        start: KeyBytes::from_slice(start.as_ref()),
        end: Some(KeyBytes::from_slice(end.as_ref())),
    }
}

fn txo(n: u8, idx: u32) -> TxoRef {
    TxoRef([n; 32].into(), idx)
}

/// A UTxO whose value and creation position both derive from `n`.
fn utxo(n: u8) -> Arc<UtxoValue> {
    let position = UtxoPosition::new(u64::from(n) * 1_000, u16::from(n));
    Arc::new(UtxoValue::new(EraCbor(6, vec![n; 8]), Some(position)))
}

fn produce(refs: &[(TxoRef, Arc<UtxoValue>)]) -> UtxoSetDelta {
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

fn all_utxos<S: CoreStateStore>(store: &S) -> Vec<(TxoRef, UtxoValue)> {
    store
        .iter_utxos()
        .expect("iter_utxos failed")
        .collect::<Result<Vec<_>, _>>()
        .expect("utxo iteration failed")
}

const ADDRESS: &str = "address";
const POLICY: &str = "policy";

/// The tags of `txo(n, idx)`, created by the tx at index 0 of slot `n`.
fn tagged(
    n: u8,
    idx: u32,
    tags: &[(&'static str, u8)],
) -> (TxoRef, Option<UtxoPosition>, Vec<Tag>) {
    tagged_at(txo(n, idx), Some((u64::from(n), 0)), tags)
}

fn tagged_at(
    txo: TxoRef,
    position: Option<(u64, u16)>,
    tags: &[(&'static str, u8)],
) -> (TxoRef, Option<UtxoPosition>, Vec<Tag>) {
    let tags = tags
        .iter()
        .map(|(dimension, key)| Tag::new(dimension, vec![*key; 4]))
        .collect();

    let position = position.map(|(slot, tx_index)| UtxoPosition::new(slot, tx_index));

    (txo, position, tags)
}

fn apply_tags<S: CoreStateStore>(store: &S, delta: &UtxoIndexDelta) {
    let writer = store.start_writer().expect("start_writer failed");
    writer
        .apply_utxo_tags(delta)
        .expect("apply_utxo_tags failed");
    writer.commit().expect("commit failed");
}

fn by_tag<S: CoreStateStore>(store: &S, dimension: &'static str, key: u8) -> UtxoSet {
    store
        .utxos_by_tag(dimension, &[key; 4])
        .expect("utxos_by_tag failed")
}

fn refs(items: &[TxoRef]) -> UtxoSet {
    items.iter().cloned().collect()
}

fn entries<S: CoreStateStore>(
    store: &S,
    order: TagOrder,
    start: TagStart,
    count: usize,
) -> Vec<TagEntry> {
    store
        .utxos_by_tag_page(ADDRESS, &[1; 4], order, start, count)
        .expect("utxos_by_tag_page failed")
}

fn page<S: CoreStateStore>(store: &S, order: TagOrder, offset: usize, count: usize) -> Vec<TxoRef> {
    entries(store, order, TagStart::Skip(offset), count)
        .into_iter()
        .map(|entry| entry.txo)
        .collect()
}

/// The entry `tagged_at` gives `txo` at `position`.
fn entry_at(txo: TxoRef, position: Option<(u64, u16)>) -> TagEntry {
    let position = position.map(|(slot, tx_index)| UtxoPosition::new(slot, tx_index));
    TagEntry::new(position, txo)
}

/// The ordered read returns the refs `utxos_by_tag` returns, sorted by creation
/// position — no position first, then slot, then tx index — and then by ref,
/// which within one tx is the output index. A page is a window of that order,
/// or of its reverse.
fn tag_pages_walk_creation_order<B: Backend>() {
    let (store, _guard) = B::open();

    // hash order disagrees with position order on purpose
    let rows = [
        (txo(0xf0, 1), None),
        (txo(0x10, 0), None),
        (txo(0x05, 2), Some((900, 0))),
        (txo(0x05, 0), Some((900, 0))),
        (txo(0xee, 0), Some((900, 1))),
        (txo(0x01, 0), Some((1_000, 0))),
        (txo(0xaa, 3), Some((0, 0))),
        (txo(0x77, 0), Some((u64::from(u32::MAX) + 5, 2))),
    ];

    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: rows
                .iter()
                .map(|(txo, position)| tagged_at(txo.clone(), *position, &[(ADDRESS, 1)]))
                .collect(),
            consumed: vec![],
        },
    );

    let mut sorted = rows.to_vec();
    sorted.sort_by_key(|(txo, position)| (*position, txo.clone()));
    let asc: Vec<TxoRef> = sorted.into_iter().map(|(txo, _)| txo).collect();
    let desc: Vec<TxoRef> = asc.iter().rev().cloned().collect();

    assert_eq!(
        page(&store, TagOrder::Asc, 0, usize::MAX)
            .into_iter()
            .collect::<UtxoSet>(),
        by_tag(&store, ADDRESS, 1),
        "the pages cover the set and nothing else"
    );

    assert_eq!(page(&store, TagOrder::Asc, 0, usize::MAX), asc);
    assert_eq!(page(&store, TagOrder::Desc, 0, usize::MAX), desc);

    assert_eq!(page(&store, TagOrder::Asc, 2, 3), asc[2..5]);
    assert_eq!(page(&store, TagOrder::Desc, 2, 3), desc[2..5]);

    assert_eq!(
        page(&store, TagOrder::Asc, 6, 5),
        asc[6..],
        "the last page is partial"
    );
    assert_eq!(page(&store, TagOrder::Desc, 6, 5), desc[6..]);

    assert!(page(&store, TagOrder::Asc, rows.len(), 5).is_empty());
    assert!(page(&store, TagOrder::Desc, 100, 1).is_empty());
    assert!(page(&store, TagOrder::Asc, 0, 0).is_empty());

    // consumed at its position, a ref leaves the order
    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![],
            consumed: vec![tagged_at(txo(0x05, 0), Some((900, 0)), &[(ADDRESS, 1)])],
        },
    );

    let remaining: Vec<TxoRef> = asc.into_iter().filter(|x| *x != txo(0x05, 0)).collect();
    assert_eq!(page(&store, TagOrder::Asc, 0, usize::MAX), remaining);
}

/// A walk resumed just past an entry reads on from there in either order,
/// whether or not the tag still holds that entry, so a ref spent between two
/// reads shifts nothing.
fn tag_pages_resume_after_an_entry<B: Backend>() {
    let (store, _guard) = B::open();

    let rows = [
        (txo(0x40, 0), None),
        (txo(0x05, 0), Some((7, 0))),
        (txo(0x05, 1), Some((7, 0))),
        (txo(0x01, 0), Some((7, 3))),
        (txo(0xee, 0), Some((9, 0))),
    ];

    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: rows
                .iter()
                .map(|(txo, position)| tagged_at(txo.clone(), *position, &[(ADDRESS, 1)]))
                .collect(),
            consumed: vec![],
        },
    );

    let asc: Vec<TagEntry> = rows
        .iter()
        .map(|(txo, position)| entry_at(txo.clone(), *position))
        .collect();
    let desc: Vec<TagEntry> = asc.iter().rev().cloned().collect();

    assert_eq!(
        entries(&store, TagOrder::Asc, TagStart::Skip(0), usize::MAX),
        asc,
        "a page carries each entry's order bytes"
    );

    for i in 0..asc.len() {
        let after = |entry: &TagEntry| TagStart::After(entry.clone());

        assert_eq!(
            entries(&store, TagOrder::Asc, after(&asc[i]), usize::MAX),
            asc[i + 1..]
        );
        assert_eq!(
            entries(&store, TagOrder::Desc, after(&desc[i]), usize::MAX),
            desc[i + 1..]
        );
        assert_eq!(
            entries(&store, TagOrder::Asc, after(&asc[i]), 1),
            asc[i + 1..].iter().take(1).cloned().collect::<Vec<_>>()
        );
    }

    // the entry a walk stopped at is spent before the next read
    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![],
            consumed: vec![tagged_at(txo(0x05, 1), Some((7, 0)), &[(ADDRESS, 1)])],
        },
    );

    assert_eq!(
        entries(&store, TagOrder::Asc, TagStart::After(asc[2].clone()), 2),
        asc[3..5]
    );
    assert_eq!(
        entries(&store, TagOrder::Desc, TagStart::After(asc[2].clone()), 2),
        vec![asc[1].clone(), asc[0].clone()]
    );
}

/// A lookup key that extends another (an asset name that extends a shorter
/// one) shares its prefix on disk; neither read returns its refs.
fn tag_reads_skip_longer_lookup_keys<B: Backend>() {
    let (store, _guard) = B::open();

    let mut longer = tagged_at(txo(0x33, 0), None, &[]);
    longer.2.push(Tag::new(ADDRESS, vec![1; 5]));

    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![tagged(1, 0, &[(ADDRESS, 1)]), longer],
            consumed: vec![],
        },
    );

    assert_eq!(by_tag(&store, ADDRESS, 1), refs(&[txo(1, 0)]));
    assert_eq!(page(&store, TagOrder::Asc, 0, usize::MAX), vec![txo(1, 0)]);
    assert_eq!(page(&store, TagOrder::Desc, 0, usize::MAX), vec![txo(1, 0)]);
    assert_eq!(
        store
            .utxos_by_tag(ADDRESS, &[1; 5])
            .expect("utxos_by_tag failed"),
        refs(&[txo(0x33, 0)])
    );

    // nor does a walk resumed past the key's entries, or before them
    let start = entry_at(txo(1, 0), Some((1, 0)));
    let before = entry_at(txo(0, 0), None);

    assert!(entries(&store, TagOrder::Asc, TagStart::After(start), usize::MAX).is_empty());
    assert!(entries(&store, TagOrder::Desc, TagStart::After(before), usize::MAX).is_empty());
}

/// A probe answers which of the given entries the tag holds, in the order
/// given: the ref under that exact lookup key, at that exact position.
fn utxos_with_tag_probes_the_full_key<B: Backend>() {
    let (store, _guard) = B::open();

    let mut longer = tagged_at(txo(0x33, 0), None, &[]);
    longer.2.push(Tag::new(ADDRESS, vec![1; 5]));

    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![
                tagged(1, 0, &[(ADDRESS, 1)]),
                tagged(2, 0, &[(ADDRESS, 1)]),
                tagged(3, 0, &[(ADDRESS, 2)]),
                longer,
            ],
            consumed: vec![],
        },
    );

    let held_late = entry_at(txo(2, 0), Some((2, 0)));
    let held_early = entry_at(txo(1, 0), Some((1, 0)));
    let elsewhere = entry_at(txo(3, 0), Some((3, 0)));
    let moved = entry_at(txo(1, 0), Some((1, 1)));
    let extended = entry_at(txo(0x33, 0), None);

    let probed = store
        .utxos_with_tag(
            ADDRESS,
            &[1; 4],
            vec![
                held_late.clone(),
                elsewhere,
                moved,
                extended,
                held_early.clone(),
            ],
        )
        .expect("utxos_with_tag failed");

    assert_eq!(probed, vec![held_late, held_early]);

    assert!(store
        .utxos_with_tag(POLICY, &[1; 4], vec![entry_at(txo(1, 0), Some((1, 0)))])
        .expect("utxos_with_tag failed")
        .is_empty());
}

/// A produced ref answers under every tag it carries, and only there.
fn apply_then_utxos_by_tag_finds_the_ref<B: Backend>() {
    let (store, _guard) = B::open();

    assert!(
        by_tag(&store, ADDRESS, 1).is_empty(),
        "a fresh store holds no tags"
    );

    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![
                tagged(1, 0, &[(ADDRESS, 1), (POLICY, 9)]),
                tagged(1, 1, &[(ADDRESS, 1)]),
                tagged(2, 0, &[(ADDRESS, 2), (POLICY, 9)]),
            ],
            consumed: vec![],
        },
    );

    assert_eq!(
        by_tag(&store, ADDRESS, 1),
        refs(&[txo(1, 0), txo(1, 1)]),
        "every ref tagged under the key, and no other"
    );
    assert_eq!(by_tag(&store, ADDRESS, 2), refs(&[txo(2, 0)]));
    assert_eq!(
        by_tag(&store, POLICY, 9),
        refs(&[txo(1, 0), txo(2, 0)]),
        "dimensions are independent"
    );
    assert!(
        by_tag(&store, POLICY, 1).is_empty(),
        "the same key under another dimension is another tag"
    );
    assert!(by_tag(&store, ADDRESS, 3).is_empty());
}

/// A rollback has to leave the tags where they started: the refs the delta
/// produced are gone, the refs it consumed are back, and nothing else moved.
fn undo_utxo_tags_removes_exactly_what_apply_added<B: Backend>() {
    let (store, _guard) = B::open();

    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![
                tagged(1, 0, &[(ADDRESS, 1), (POLICY, 9)]),
                tagged(2, 0, &[(ADDRESS, 2)]),
            ],
            consumed: vec![],
        },
    );

    let block = UtxoIndexDelta {
        produced: vec![tagged(3, 0, &[(ADDRESS, 1), (POLICY, 9)])],
        consumed: vec![tagged(2, 0, &[(ADDRESS, 2)])],
    };

    apply_tags(&store, &block);

    assert_eq!(by_tag(&store, ADDRESS, 1), refs(&[txo(1, 0), txo(3, 0)]));
    assert!(by_tag(&store, ADDRESS, 2).is_empty());

    let writer = store.start_writer().expect("start_writer failed");
    writer
        .undo_utxo_tags(&block)
        .expect("undo_utxo_tags failed");
    writer.commit().expect("commit failed");

    assert_eq!(
        by_tag(&store, ADDRESS, 1),
        refs(&[txo(1, 0)]),
        "undo removed what the block produced, and only that"
    );
    assert_eq!(
        by_tag(&store, POLICY, 9),
        refs(&[txo(1, 0)]),
        "under every dimension the block tagged"
    );
    assert_eq!(
        by_tag(&store, ADDRESS, 2),
        refs(&[txo(2, 0)]),
        "undo restored what the block consumed"
    );
}

/// An output that a later tx of its own block spent is in both halves of the
/// block's delta, at the same position. Apply leaves it no tag, and so must
/// the undo: the tags end where they were before the block.
fn undo_leaves_no_tag_for_an_output_its_block_spent<B: Backend>() {
    let (store, _guard) = B::open();

    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![tagged(1, 0, &[(ADDRESS, 1)])],
            consumed: vec![],
        },
    );

    let block = UtxoIndexDelta {
        produced: vec![
            tagged_at(txo(5, 0), Some((5, 0)), &[(ADDRESS, 1), (POLICY, 9)]),
            tagged_at(txo(6, 0), Some((5, 1)), &[(ADDRESS, 1)]),
        ],
        consumed: vec![
            tagged(1, 0, &[(ADDRESS, 1)]),
            tagged_at(txo(5, 0), Some((5, 0)), &[(ADDRESS, 1), (POLICY, 9)]),
        ],
    };

    apply_tags(&store, &block);

    assert_eq!(by_tag(&store, ADDRESS, 1), refs(&[txo(6, 0)]));
    assert!(by_tag(&store, POLICY, 9).is_empty());

    let writer = store.start_writer().expect("start_writer failed");
    writer
        .undo_utxo_tags(&block)
        .expect("undo_utxo_tags failed");
    writer.commit().expect("commit failed");

    assert_eq!(
        by_tag(&store, ADDRESS, 1),
        refs(&[txo(1, 0)]),
        "the spent output left no ghost, the consumed one came back"
    );
    assert!(by_tag(&store, POLICY, 9).is_empty());
    assert_eq!(page(&store, TagOrder::Asc, 0, usize::MAX), vec![txo(1, 0)]);
}

/// Consuming a ref takes it out from under its tags — and nothing but its
/// own tags: a neighbour sharing the key stays.
fn consumed_refs_leave_the_tag<B: Backend>() {
    let (store, _guard) = B::open();

    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![
                tagged(1, 0, &[(ADDRESS, 1), (POLICY, 9)]),
                tagged(1, 1, &[(ADDRESS, 1)]),
            ],
            consumed: vec![],
        },
    );

    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![],
            consumed: vec![tagged(1, 0, &[(ADDRESS, 1), (POLICY, 9)])],
        },
    );

    assert_eq!(
        by_tag(&store, ADDRESS, 1),
        refs(&[txo(1, 1)]),
        "the consumed ref left, its neighbour under the same key stayed"
    );
    assert!(
        by_tag(&store, POLICY, 9).is_empty(),
        "a key with no refs left answers empty rather than erroring"
    );

    // Produced and consumed in one delta: consumption wins, as it does for
    // the UTxO set itself.
    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![tagged(5, 0, &[(ADDRESS, 5)])],
            consumed: vec![tagged(5, 0, &[(ADDRESS, 5)])],
        },
    );

    assert!(by_tag(&store, ADDRESS, 5).is_empty());
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

    let all = collect(KeyRange::all());
    assert_eq!(
        all.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![key(1), key(2), key(3), key(4)],
        "entities come out in key order, and only from the requested namespace"
    );

    // Half-open: the start is included, the end is not.
    let middle = collect(range(&key(2), &key(4)));
    assert_eq!(
        middle.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![key(2), key(3)]
    );

    assert!(
        collect(range(&key(2), &key(2))).is_empty(),
        "an empty range is empty"
    );

    // An inverted range matches nothing rather than blowing up: the disk
    // backends compare encoded bytes and simply find none, so an ordered-map
    // backend has to reach the same answer instead of panicking on the bound.
    assert!(
        collect(range(&key(4), &key(2))).is_empty(),
        "an inverted range is empty"
    );
}

/// Keys of any width share one namespace: the empty key of a singleton, a
/// one-byte key, hashes, and a composite past the old 32 bytes, all read back
/// and walked in byte order — a key of all `0xff` included, which the open end
/// of a range reaches and a fixed-width bound could not.
fn entities_of_mixed_widths_share_a_namespace<B: Backend>() {
    let (store, _guard) = B::open();

    let keys = [
        key_of(0x00, 0),
        key_of(0x00, 1),
        key_of(0x01, 1),
        key_of(0x01, 2),
        key_of(0x01, 28),
        key_of(0x01, 29),
        key_of(0x02, 36),
        key_of(0xff, 32),
        key_of(0xff, 68),
    ];

    let writer = store.start_writer().expect("start_writer failed");
    for (i, key) in keys.iter().enumerate().rev() {
        writer
            .write_entity(ACCOUNTS, key, &vec![i as u8])
            .expect("write_entity failed");
    }
    writer
        .write_entity(POOLS, &key_of(0x01, 28), &vec![0xee])
        .expect("write_entity failed");
    writer.commit().expect("commit failed");

    let read = store
        .read_entities(ACCOUNTS, &keys.iter().collect::<Vec<_>>())
        .expect("read_entities failed");
    assert_eq!(
        read,
        (0..keys.len())
            .map(|i| Some(vec![i as u8]))
            .collect::<Vec<_>>()
    );

    // a key and its own prefix are two rows, not one
    assert_eq!(
        store.read_entities(ACCOUNTS, &[&key_of(0x01, 3)]).unwrap(),
        vec![None]
    );

    let collect = |range: KeyRange| -> Vec<EntityKey> {
        store
            .iter_entities(ACCOUNTS, range)
            .expect("iter_entities failed")
            .map(|entry| entry.expect("entity iteration failed").0)
            .collect()
    };

    assert_eq!(collect(KeyRange::all()), keys.to_vec());

    // every key starting with 0x01, and none of its neighbours
    assert_eq!(collect(KeyRange::prefix(&[0x01])), keys[2..6].to_vec());

    // a typed prefix range over a composite key
    let prefixed = RangeOf::<(u8, [u8; 35])>::prefix(&0x02u8).into_raw();
    assert_eq!(collect(prefixed), keys[6..7].to_vec());

    // the prefix of all `0xff` has no successor, so its range is open
    assert_eq!(collect(KeyRange::prefix(&[0xff])), keys[7..].to_vec());
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

    let expected: HashMap<TxoRef, UtxoValue> = refs
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
