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
    builtin::MemoryStateStore, BlockSlot, ChainPoint, EntityKey, EraCbor, KeyBytes, KeyRange,
    RangeOf, SlotOrder, SlotScan, Tag, TxoRef, UtxoIndexDelta, UtxoSet, UtxoSetDelta,
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
            fn consumed_refs_leave_the_tag() {
                super::consumed_refs_leave_the_tag::<$backend>();
            }

            #[test]
            fn a_lookup_key_that_prefixes_another_is_its_own_tag() {
                super::a_lookup_key_that_prefixes_another_is_its_own_tag::<$backend>();
            }

            #[test]
            fn positioned_refs_are_listed_in_slot_order() {
                super::positioned_refs_are_listed_in_slot_order::<$backend>();
            }

            #[test]
            fn a_slot_walk_resumes_past_its_cursor() {
                super::a_slot_walk_resumes_past_its_cursor::<$backend>();
            }

            #[test]
            fn utxo_slots_follow_the_set() {
                super::utxo_slots_follow_the_set::<$backend>();
            }

            #[test]
            fn undo_regains_before_it_drops() {
                super::undo_regains_before_it_drops::<$backend>();
            }

            #[test]
            fn a_lookup_key_that_prefixes_another_has_its_own_slot_order() {
                super::a_lookup_key_that_prefixes_another_has_its_own_slot_order::<$backend>();
            }

            #[test]
            fn the_positioned_mark_is_set_cleared_and_kept() {
                super::the_positioned_mark_is_set_cleared_and_kept::<$backend>();
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

const ADDRESS: &str = "address";
const POLICY: &str = "policy";

fn tagged(n: u8, idx: u32, tags: &[(&'static str, u8)]) -> (TxoRef, Vec<Tag>) {
    let tags = tags
        .iter()
        .map(|(dimension, key)| Tag::new(dimension, vec![*key; 4]))
        .collect();

    (txo(n, idx), tags)
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

/// A tagged ref and its producing slot, as the two sides of a delta take
/// them.
type Positioned = ((TxoRef, Vec<Tag>), (TxoRef, BlockSlot));

/// A tagged ref with its producing slot.
fn at(slot: BlockSlot, tagged: (TxoRef, Vec<Tag>)) -> Positioned {
    let txo = tagged.0.clone();
    (tagged, (txo, slot))
}

/// A delta producing `rows`, each at its slot.
fn produced_at(rows: Vec<Positioned>) -> UtxoIndexDelta {
    let (produced, slots): (Vec<_>, HashMap<_, _>) = rows.into_iter().unzip();

    UtxoIndexDelta {
        produced,
        slots,
        ..Default::default()
    }
}

fn in_slots<S: CoreStateStore>(
    store: &S,
    dimension: &'static str,
    key: u8,
    scan: &SlotScan,
) -> Vec<(BlockSlot, TxoRef)> {
    store
        .utxos_by_tag_in_slots(dimension, &[key; 4], scan)
        .expect("utxos_by_tag_in_slots failed")
}

fn scan(slots: std::ops::RangeInclusive<BlockSlot>, order: SlotOrder) -> SlotScan {
    SlotScan {
        slots,
        order,
        after: None,
        limit: usize::MAX,
    }
}

fn slots_of<S: CoreStateStore>(store: &S, refs: &[TxoRef]) -> HashMap<TxoRef, BlockSlot> {
    store
        .get_utxo_slots(refs.to_vec())
        .expect("get_utxo_slots failed")
}

/// Every positioned ref of a tag comes back in slot order, then ref order
/// inside a slot, ascending or descending; the slot bounds are inclusive on
/// both ends; a ref without a slot is in the plain tag and nowhere here.
fn positioned_refs_are_listed_in_slot_order<B: Backend>() {
    let (store, _guard) = B::open();

    apply_tags(
        &store,
        &produced_at(vec![
            at(30, tagged(3, 0, &[(ADDRESS, 1)])),
            at(10, tagged(1, 1, &[(ADDRESS, 1), (POLICY, 9)])),
            at(10, tagged(1, 0, &[(ADDRESS, 1)])),
            at(20, tagged(2, 0, &[(ADDRESS, 1)])),
            at(20, tagged(9, 0, &[(ADDRESS, 2)])),
        ]),
    );

    // a genesis-like ref: tagged, never positioned
    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![tagged(7, 0, &[(ADDRESS, 1)])],
            ..Default::default()
        },
    );

    let asc = vec![
        (10, txo(1, 0)),
        (10, txo(1, 1)),
        (20, txo(2, 0)),
        (30, txo(3, 0)),
    ];
    let mut desc = asc.clone();
    desc.reverse();

    assert_eq!(
        in_slots(&store, ADDRESS, 1, &scan(0..=u64::MAX, SlotOrder::Asc)),
        asc
    );
    assert_eq!(
        in_slots(&store, ADDRESS, 1, &scan(0..=u64::MAX, SlotOrder::Desc)),
        desc
    );

    // inclusive bounds, both ends, both directions
    assert_eq!(
        in_slots(&store, ADDRESS, 1, &scan(10..=20, SlotOrder::Asc)),
        asc[..3]
    );
    assert_eq!(
        in_slots(&store, ADDRESS, 1, &scan(11..=30, SlotOrder::Desc)),
        desc[..2]
    );
    assert_eq!(
        in_slots(&store, ADDRESS, 1, &scan(21..=29, SlotOrder::Asc)),
        vec![]
    );
    assert_eq!(
        in_slots(
            &store,
            ADDRESS,
            1,
            &scan(std::ops::RangeInclusive::new(30, 10), SlotOrder::Asc)
        ),
        vec![]
    );

    // the plain tag has the unpositioned ref too, the other key only its own
    assert_eq!(
        by_tag(&store, ADDRESS, 1),
        refs(&[txo(1, 0), txo(1, 1), txo(2, 0), txo(3, 0), txo(7, 0)])
    );
    assert_eq!(
        in_slots(&store, ADDRESS, 2, &scan(0..=u64::MAX, SlotOrder::Asc)),
        vec![(20, txo(9, 0))]
    );
    assert_eq!(
        in_slots(&store, POLICY, 9, &scan(0..=u64::MAX, SlotOrder::Asc)),
        vec![(10, txo(1, 1))]
    );

    // a limit cuts the walk short, zero rows is zero rows
    let mut two = scan(0..=u64::MAX, SlotOrder::Asc);
    two.limit = 2;
    assert_eq!(in_slots(&store, ADDRESS, 1, &two), asc[..2]);
    two.limit = 0;
    assert_eq!(in_slots(&store, ADDRESS, 1, &two), vec![]);
}

/// A walk in chunks resumes strictly past its cursor in its direction,
/// across slot groups and inside one, and a cursor past the far end — or
/// before the near one — selects nothing or everything, never a panic.
fn a_slot_walk_resumes_past_its_cursor<B: Backend>() {
    let (store, _guard) = B::open();

    apply_tags(
        &store,
        &produced_at(vec![
            at(10, tagged(1, 0, &[(ADDRESS, 1)])),
            at(10, tagged(1, 1, &[(ADDRESS, 1)])),
            at(10, tagged(2, 0, &[(ADDRESS, 1)])),
            at(20, tagged(3, 0, &[(ADDRESS, 1)])),
            at(30, tagged(4, 0, &[(ADDRESS, 1)])),
        ]),
    );

    let whole = in_slots(&store, ADDRESS, 1, &scan(0..=u64::MAX, SlotOrder::Asc));
    assert_eq!(whole.len(), 5);

    for order in [SlotOrder::Asc, SlotOrder::Desc] {
        let mut expected = whole.clone();
        if order == SlotOrder::Desc {
            expected.reverse();
        }

        for limit in 1..=6 {
            let mut walked = Vec::new();
            let mut scan = scan(0..=u64::MAX, order);
            scan.limit = limit;

            loop {
                let chunk = in_slots(&store, ADDRESS, 1, &scan);
                let last = chunk.len() < limit;
                scan.after = chunk.last().cloned();
                walked.extend(chunk);
                if last {
                    break;
                }
            }

            assert_eq!(walked, expected, "{order:?} in chunks of {limit}");
        }
    }

    // a cursor outside the slot range
    let mut past = scan(10..=20, SlotOrder::Asc);
    past.after = Some((20, txo(3, 0)));
    assert_eq!(in_slots(&store, ADDRESS, 1, &past), vec![]);
    past.after = Some((25, txo(0, 0)));
    assert_eq!(in_slots(&store, ADDRESS, 1, &past), vec![]);
    past.after = Some((5, txo(0, 0)));
    assert_eq!(in_slots(&store, ADDRESS, 1, &past), whole[..4]);

    let mut past = scan(20..=30, SlotOrder::Desc);
    past.after = Some((20, txo(3, 0)));
    assert_eq!(in_slots(&store, ADDRESS, 1, &past), vec![]);
    past.after = Some((15, txo(0, 0)));
    assert_eq!(in_slots(&store, ADDRESS, 1, &past), vec![]);
    past.after = Some((35, txo(0, 0)));
    assert_eq!(
        in_slots(&store, ADDRESS, 1, &past),
        vec![(30, txo(4, 0)), (20, txo(3, 0))]
    );
}

/// The slot kept beside a ref answers while the ref is live and positioned,
/// and consuming the ref takes the slot and its ordered rows with it — when
/// the consuming delta names the slot, as the roll does from this very read.
fn utxo_slots_follow_the_set<B: Backend>() {
    let (store, _guard) = B::open();

    apply_tags(
        &store,
        &produced_at(vec![
            at(10, tagged(1, 0, &[(ADDRESS, 1), (POLICY, 9)])),
            at(20, tagged(2, 0, &[(ADDRESS, 1)])),
        ]),
    );
    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![tagged(7, 0, &[(ADDRESS, 1)])],
            ..Default::default()
        },
    );

    let slots = slots_of(&store, &[txo(1, 0), txo(2, 0), txo(7, 0), txo(8, 0)]);
    assert_eq!(
        slots,
        HashMap::from([(txo(1, 0), 10), (txo(2, 0), 20)]),
        "positioned live refs answer, the unpositioned and the unknown do not"
    );

    // consumed with its slot, as the roll hands it back
    apply_tags(
        &store,
        &UtxoIndexDelta {
            consumed: vec![tagged(1, 0, &[(ADDRESS, 1), (POLICY, 9)])],
            slots: slots.clone(),
            ..Default::default()
        },
    );

    assert_eq!(
        slots_of(&store, &[txo(1, 0), txo(2, 0)]),
        HashMap::from([(txo(2, 0), 20)])
    );
    assert_eq!(
        in_slots(&store, ADDRESS, 1, &scan(0..=u64::MAX, SlotOrder::Asc)),
        vec![(20, txo(2, 0))]
    );
    assert_eq!(
        in_slots(&store, POLICY, 9, &scan(0..=u64::MAX, SlotOrder::Asc)),
        vec![]
    );
    assert_eq!(by_tag(&store, ADDRESS, 1), refs(&[txo(2, 0), txo(7, 0)]));
}

/// Undoing a block puts the inputs it consumed back, with their slots, and
/// takes the outputs it produced out, with theirs. A ref on both sides — one
/// the block produced and consumed — ends up out, like it is out of the set:
/// the regained rows go in before the dropped ones go out.
fn undo_regains_before_it_drops<B: Backend>() {
    let (store, _guard) = B::open();

    // the input the block will consume, produced earlier at slot 10
    apply_tags(
        &store,
        &produced_at(vec![at(10, tagged(1, 0, &[(ADDRESS, 1)]))]),
    );

    // the block at slot 20: consumes 1#0, produces 2#0 and 3#0, consumes 3#0
    let block = UtxoIndexDelta {
        produced: vec![
            tagged(2, 0, &[(ADDRESS, 1)]),
            tagged(3, 0, &[(ADDRESS, 1), (POLICY, 9)]),
        ],
        consumed: vec![
            tagged(1, 0, &[(ADDRESS, 1)]),
            tagged(3, 0, &[(ADDRESS, 1), (POLICY, 9)]),
        ],
        slots: HashMap::from([(txo(1, 0), 10), (txo(2, 0), 20), (txo(3, 0), 20)]),
    };

    apply_tags(&store, &block);

    assert_eq!(
        in_slots(&store, ADDRESS, 1, &scan(0..=u64::MAX, SlotOrder::Asc)),
        vec![(20, txo(2, 0))]
    );
    assert_eq!(
        slots_of(&store, &[txo(1, 0), txo(2, 0), txo(3, 0)]),
        HashMap::from([(txo(2, 0), 20)])
    );

    let writer = store.start_writer().expect("start_writer failed");
    writer
        .undo_utxo_tags(&block)
        .expect("undo_utxo_tags failed");
    writer.commit().expect("commit failed");

    assert_eq!(by_tag(&store, ADDRESS, 1), refs(&[txo(1, 0)]));
    assert_eq!(by_tag(&store, POLICY, 9), refs(&[]));
    assert_eq!(
        in_slots(&store, ADDRESS, 1, &scan(0..=u64::MAX, SlotOrder::Asc)),
        vec![(10, txo(1, 0))]
    );
    assert_eq!(
        in_slots(&store, POLICY, 9, &scan(0..=u64::MAX, SlotOrder::Asc)),
        vec![]
    );
    assert_eq!(
        slots_of(&store, &[txo(1, 0), txo(2, 0), txo(3, 0)]),
        HashMap::from([(txo(1, 0), 10)])
    );
}

/// The ordered rows of a lookup key are its own: a key that prefixes another
/// does not list the longer key's rows, in either direction, and a prefix of
/// both lists nothing.
fn a_lookup_key_that_prefixes_another_has_its_own_slot_order<B: Backend>() {
    let (store, _guard) = B::open();

    let short = vec![1u8, 1];
    let long = vec![1u8, 1, 2];
    let longer = vec![1u8, 1, 2, 3];

    let rows: Vec<_> = [
        (txo(1, 0), &short, 10),
        (txo(2, 0), &long, 5),
        (txo(2, 1), &long, 15),
        (txo(3, 0), &longer, 10),
    ]
    .into_iter()
    .map(|(txo, key, slot)| at(slot, (txo, vec![Tag::new(ADDRESS, key.clone())])))
    .collect();

    apply_tags(&store, &produced_at(rows));

    let whole = |key: &[u8], order| {
        let scan = scan(0..=u64::MAX, order);
        store
            .utxos_by_tag_in_slots(ADDRESS, key, &scan)
            .expect("utxos_by_tag_in_slots failed")
    };

    for order in [SlotOrder::Asc, SlotOrder::Desc] {
        assert_eq!(whole(&short, order), vec![(10, txo(1, 0))]);
        assert_eq!(whole(&longer, order), vec![(10, txo(3, 0))]);
        assert_eq!(whole(&[1], order), vec![]);
    }
    assert_eq!(
        whole(&long, SlotOrder::Asc),
        vec![(5, txo(2, 0)), (15, txo(2, 1))]
    );
    assert_eq!(
        whole(&long, SlotOrder::Desc),
        vec![(15, txo(2, 1)), (5, txo(2, 0))]
    );
}

/// A fresh store is unpositioned; a writer that marks it, positioned; later
/// writers that move the cursor keep it so; clearing the positions unsets it
/// and takes the ordered rows and slots with it, leaving the plain tags.
fn the_positioned_mark_is_set_cleared_and_kept<B: Backend>() {
    let (store, _guard) = B::open();

    assert!(!store.utxos_positioned().expect("utxos_positioned failed"));

    apply_tags(
        &store,
        &produced_at(vec![at(10, tagged(1, 0, &[(ADDRESS, 1)]))]),
    );
    assert!(!store.utxos_positioned().expect("utxos_positioned failed"));

    // the mark follows the cursor: with none to follow, the commit fails
    let writer = store.start_writer().expect("start_writer failed");
    writer
        .mark_utxos_positioned()
        .expect("mark_utxos_positioned failed");
    assert!(writer.commit().is_err(), "a mark with no cursor commits");
    assert!(!store.utxos_positioned().expect("utxos_positioned failed"));

    let writer = store.start_writer().expect("start_writer failed");
    writer
        .mark_utxos_positioned()
        .expect("mark_utxos_positioned failed");
    writer
        .set_cursor(ChainPoint::Specific(10, [1; 32].into()))
        .expect("set_cursor failed");
    writer.commit().expect("commit failed");
    assert!(store.utxos_positioned().expect("utxos_positioned failed"));

    // a later cursor, with and without a mark of its own
    for slot in [20, 30] {
        let writer = store.start_writer().expect("start_writer failed");
        writer
            .set_cursor(ChainPoint::Specific(slot, [slot as u8; 32].into()))
            .expect("set_cursor failed");
        writer.commit().expect("commit failed");
        assert!(store.utxos_positioned().expect("utxos_positioned failed"));
    }

    store
        .clear_utxo_positions()
        .expect("clear_utxo_positions failed");

    assert!(!store.utxos_positioned().expect("utxos_positioned failed"));
    assert_eq!(
        in_slots(&store, ADDRESS, 1, &scan(0..=u64::MAX, SlotOrder::Asc)),
        vec![]
    );
    assert_eq!(slots_of(&store, &[txo(1, 0)]), HashMap::new());
    assert_eq!(by_tag(&store, ADDRESS, 1), refs(&[txo(1, 0)]));
    assert_eq!(
        store.read_cursor().expect("read_cursor failed"),
        Some(ChainPoint::Specific(30, [30; 32].into()))
    );

    // marking again, in a writer that sets no cursor, stamps the stored one
    let writer = store.start_writer().expect("start_writer failed");
    writer
        .mark_utxos_positioned()
        .expect("mark_utxos_positioned failed");
    writer.commit().expect("commit failed");
    assert!(store.utxos_positioned().expect("utxos_positioned failed"));
}

/// Lookup keys vary in length, and one may be a prefix of another: an asset
/// with an empty name is the prefix of every asset of its policy. Each is its
/// own tag on every backend.
fn a_lookup_key_that_prefixes_another_is_its_own_tag<B: Backend>() {
    let (store, _guard) = B::open();

    let short: Vec<u8> = vec![1, 1];
    let long: Vec<u8> = vec![1, 1, 2];
    let longer: Vec<u8> = vec![1, 1, 2, 3];

    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![
                (txo(1, 0), vec![Tag::new(ADDRESS, short.clone())]),
                (txo(2, 0), vec![Tag::new(ADDRESS, long.clone())]),
                (txo(2, 1), vec![Tag::new(ADDRESS, long.clone())]),
                (txo(3, 0), vec![Tag::new(ADDRESS, longer.clone())]),
            ],
            consumed: vec![],
            ..Default::default()
        },
    );

    let whole = |key: &[u8]| -> Vec<TxoRef> {
        let mut refs: Vec<TxoRef> = store
            .utxos_by_tag(ADDRESS, key)
            .expect("utxos_by_tag failed")
            .into_iter()
            .collect();
        refs.sort();
        refs
    };
    assert_eq!(whole(&short), vec![txo(1, 0)]);
    assert_eq!(whole(&long), vec![txo(2, 0), txo(2, 1)]);
    assert_eq!(whole(&longer), vec![txo(3, 0)]);
    assert!(whole(&[1]).is_empty(), "a prefix of every key is no tag");
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
            ..Default::default()
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
            ..Default::default()
        },
    );

    let block = UtxoIndexDelta {
        produced: vec![tagged(3, 0, &[(ADDRESS, 1), (POLICY, 9)])],
        consumed: vec![tagged(2, 0, &[(ADDRESS, 2)])],
        ..Default::default()
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
            ..Default::default()
        },
    );

    apply_tags(
        &store,
        &UtxoIndexDelta {
            produced: vec![],
            consumed: vec![tagged(1, 0, &[(ADDRESS, 1), (POLICY, 9)])],
            ..Default::default()
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
            ..Default::default()
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
