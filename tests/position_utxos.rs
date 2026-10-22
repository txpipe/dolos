//! `dolos doctor position-utxos`, end to end against real on-disk stores.
//!
//! The repair claim: a store whose positions were taken away — what a store
//! built before the slot-ordered tags existed looks like — gets them back from
//! nothing but its UTxO set and its archive, identical to the positions the
//! sync wrote, and reports itself positioned only once they are all there.

mod node;

use std::collections::{BTreeMap, HashMap};

use dolos_core::{
    ArchiveStore as _, BlockSlot, ChainPoint, LogValue, SlotOrder, SlotScan, StateStore as _,
    TagDimension, TxoRef, WalStore as _,
};
use node::{assert_ok, Node};

/// One tag: its dimension and lookup key.
type TagKey = (TagDimension, Vec<u8>);

/// One row of a tag in chain order.
type SlotRow = (BlockSlot, TxoRef);

/// Every position a state store holds, read through the backend enum the node
/// runs on: the slot beside each live UTxO, and every tag in slot order.
#[derive(Debug, PartialEq)]
struct Positions {
    slots: HashMap<TxoRef, BlockSlot>,
    ordered: BTreeMap<TagKey, Vec<SlotRow>>,
    positioned: bool,
}

impl Positions {
    fn read(state: &dolos::storage::StateStoreBackend) -> Self {
        let live: Vec<_> = state
            .iter_utxos()
            .unwrap()
            .map(|entry| entry.unwrap())
            .map(|(txo, value)| (txo, std::sync::Arc::new(value)))
            .collect();

        let slots = state
            .get_utxo_slots(live.iter().map(|(txo, _)| txo.clone()).collect())
            .unwrap();

        let tags =
            dolos_cardano::indexes::utxo_index_delta_from_utxo_delta(&dolos_core::UtxoSetDelta {
                produced_utxo: live.into_iter().collect(),
                ..Default::default()
            });

        let mut ordered = BTreeMap::new();
        for (_, tags) in &tags.produced {
            for tag in tags {
                ordered
                    .entry((tag.dimension, tag.key.clone()))
                    .or_insert_with(|| {
                        state
                            .utxos_by_tag_in_slots(
                                tag.dimension,
                                &tag.key,
                                &SlotScan {
                                    slots: 0..=u64::MAX,
                                    order: SlotOrder::Asc,
                                    after: None,
                                    limit: usize::MAX,
                                },
                            )
                            .unwrap()
                    });
            }
        }

        Self {
            slots,
            ordered,
            positioned: state.utxos_positioned().unwrap(),
        }
    }
}

fn position(node: &Node, extra: &[&str]) -> std::process::Output {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_dolos"));

    command
        .arg("--config")
        .arg(node.config_path())
        .args(["doctor", "position-utxos"])
        .args(extra);

    command.output().unwrap()
}

/// Positions taken away and rebuilt by the command come back identical to the
/// ones the sync wrote: every live UTxO a block produced at its block's slot,
/// the genesis ones at none, every tag in the same slot order; and the mark
/// follows.
#[test]
fn positions_rebuilt_from_the_set_and_the_archive_match_the_synced_ones() {
    let node = Node::new();
    node.sync();

    let synced = {
        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

        let synced = Positions::read(&stores.state);
        assert!(
            synced.positioned,
            "a synced store reports itself positioned"
        );
        assert!(!synced.slots.is_empty(), "the fixture positioned no utxo");

        for (txo, slot) in &synced.slots {
            assert_eq!(
                stores.archive.slot_by_tx_hash(txo.0.as_slice()).unwrap(),
                Some(*slot)
            );
        }

        // the chain's custom UTxOs are its genesis: tagged, never positioned
        let live = stores.state.iter_utxos().unwrap().count();
        assert!(
            synced.slots.len() < live,
            "the genesis utxos carry no position"
        );

        // what a store from before the ordered tags looks like
        stores.state.clear_utxo_positions().unwrap();
        let cleared = Positions::read(&stores.state);
        assert!(!cleared.positioned);
        assert!(cleared.slots.is_empty());
        assert!(cleared.ordered.values().all(|rows| rows.is_empty()));

        synced
    };

    let stdout = assert_ok(&position(&node, &[]));
    assert!(stdout.contains("positioned"), "{stdout}");

    let rebuilt = {
        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
        Positions::read(&stores.state)
    };
    assert_eq!(rebuilt, synced);

    // a positioned store is left alone without `--force`, rebuilt with it
    let stdout = assert_ok(&position(&node, &[]));
    assert!(stdout.contains("already positioned"), "{stdout}");

    assert_ok(&position(&node, &["--force"]));

    let forced = {
        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
        Positions::read(&stores.state)
    };
    assert_eq!(forced, synced);
}

/// A WAL ahead of the state is refused: the entries still to replay would
/// leave their inputs' positions behind, so the daemon has to catch the state
/// up first.
#[test]
fn a_wal_ahead_of_the_state_is_refused() {
    let node = Node::new();
    node.sync();

    {
        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

        let cursor = stores.state.read_cursor().unwrap().unwrap();
        let ahead = ChainPoint::Specific(cursor.slot() + 1, [7; 32].into());
        stores
            .wal
            .append_entries(vec![(ahead, LogValue::origin())])
            .unwrap();
    }

    let output = position(&node, &[]);
    assert!(
        !output.status.success(),
        "the doctor ran on a wal ahead of the state"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("ahead of the state"), "{stderr}");

    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
    assert!(
        Positions::read(&stores.state).positioned,
        "a refused run touched the store"
    );
}
