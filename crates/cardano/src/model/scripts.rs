//! Scripts: every script the chain has carried, in the order cardano-db-sync
//! first saw it (decision 0057).
//!
//! Nothing else enumerates scripts. They are not ledger state, and the archive
//! index keeps a hash of the script hash, so it answers "where is this script"
//! but not "which scripts are there". Three namespaces do:
//!
//! - `scripts`, the witness, keyed by script hash: where the script was first
//!   seen, which is what a block needs to tell a new script from a repeat and
//!   what `/scripts/{hash}` starts its lookup from.
//! - `first-scripts`, a `Set` keyed `epoch ‖ script hash`: the scripts an
//!   unsealed epoch saw first.
//! - `first-script-chunks`, the `Chunk` the set seals into: an epoch's scripts
//!   in first-seen order, at `epoch_start(e) ‖ chunk`.

use dolos_core::NsKey;
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
};
use serde::{Deserialize, Serialize};

use crate::periods::{ChunkRow, GroupKey, Ungrouped};

/// Where a script was first seen: its block's slot, its tx's index in the
/// block and its place among the scripts the tx carries.
///
/// Field order is listing order, so the derived ordering is db-sync's.
#[derive(
    Debug, Clone, Copy, Encode, Decode, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct ScriptState {
    #[n(0)]
    pub slot: u64,

    #[n(1)]
    pub tx_index: u32,

    #[n(2)]
    pub order: u32,
}

hash_key!(
    /// The hash of a script, the key of its `scripts` row.
    ScriptKey,
    Hash<28>,
    28
);

display_as_stored!(ScriptKey);

entity_boilerplate!(ScriptState, "scripts", state = ScriptKey);

/// A script an epoch saw first, held in state until the epoch seals into
/// [`FirstScriptChunk`]s. The value is the script's [`ScriptState`].
#[derive(
    Debug, Clone, Copy, Encode, Decode, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct FirstScriptSet {
    #[n(0)]
    pub slot: u64,

    #[n(1)]
    pub tx_index: u32,

    #[n(2)]
    pub order: u32,
}

/// The key of a `first-scripts` row: `epoch ‖ script hash`.
pub type FirstScriptKey = GroupKey<Ungrouped, ScriptKey>;

entity_boilerplate!(FirstScriptSet, "first-scripts", state = FirstScriptKey);

impl From<ScriptState> for FirstScriptSet {
    fn from(x: ScriptState) -> Self {
        Self {
            slot: x.slot,
            tx_index: x.tx_index,
            order: x.order,
        }
    }
}

/// One chunk of the scripts an epoch saw first, in first-seen order.
///
/// Sealed at `epoch_start(e) ‖ chunk (4 B)`. Every chunk carries the epoch's
/// total, so a reader learns how many scripts the epoch holds from its first
/// chunk.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Default)]
pub struct FirstScriptChunk {
    #[n(0)]
    pub count: u64,

    #[n(1)]
    pub hashes: Vec<Hash<28>>,
}

entity_boilerplate!(FirstScriptChunk, "first-script-chunks", log = u32);

impl ChunkRow for FirstScriptChunk {
    type Item = Hash<28>;

    fn new(count: u64, hashes: Vec<Hash<28>>) -> Self {
        Self { count, hashes }
    }

    fn into_items(self) -> Vec<Hash<28>> {
        self.hashes
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_script_state()(
            slot in root::any_slot(),
            tx_index in 0u32..500,
            order in 0u32..50,
        ) -> ScriptState {
            ScriptState { slot, tx_index, order }
        }
    }
}

// --- Deltas ---

/// Records where a script was first seen.
///
/// Emitted once per script, by the block that carries it first: the roll
/// tells new scripts from repeats ahead of applying, so an existing row is
/// left alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptFirstSeen {
    pub(crate) script_hash: Hash<28>,
    pub(crate) first: ScriptState,

    /// For undo: did this delta create the row?
    pub(crate) was_new: bool,
}

impl ScriptFirstSeen {
    pub fn new(script_hash: Hash<28>, first: ScriptState) -> Self {
        Self {
            script_hash,
            first,
            was_new: false,
        }
    }
}

impl dolos_core::EntityDelta for ScriptFirstSeen {
    type Entity = ScriptState;

    fn key(&self) -> NsKey {
        NsKey::of::<ScriptState>(&ScriptKey(self.script_hash))
    }

    fn apply(&mut self, entity: &mut Option<ScriptState>) {
        self.was_new = entity.is_none();

        if self.was_new {
            *entity = Some(self.first);
        }
    }

    fn undo(&self, entity: &mut Option<ScriptState>) {
        if self.was_new {
            *entity = None;
        }
    }
}

/// Stages a script in the `first-scripts` row of the epoch that saw it first.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageFirstScript {
    pub(crate) key: FirstScriptKey,
    pub(crate) row: FirstScriptSet,

    /// For undo: did this delta create the row?
    pub(crate) was_new: bool,
}

impl StageFirstScript {
    pub fn new(epoch: u64, script_hash: Hash<28>, first: ScriptState) -> Self {
        Self {
            key: GroupKey {
                group: Ungrouped,
                epoch,
                member: ScriptKey(script_hash),
            },
            row: first.into(),
            was_new: false,
        }
    }
}

impl dolos_core::EntityDelta for StageFirstScript {
    type Entity = FirstScriptSet;

    fn key(&self) -> NsKey {
        NsKey::of::<FirstScriptSet>(&self.key)
    }

    fn apply(&mut self, entity: &mut Option<FirstScriptSet>) {
        self.was_new = entity.is_none();

        if self.was_new {
            *entity = Some(self.row);
        }
    }

    fn undo(&self, entity: &mut Option<FirstScriptSet>) {
        if self.was_new {
            *entity = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::any_script_state;
    use super::*;
    use crate::model::testing::{
        self as root, assert_delta_roundtrip, assert_delta_serde_roundtrip,
    };
    use dolos_core::{EntityDelta as _, KeyCodec as _};
    use proptest::prelude::*;

    fn first(slot: u64) -> ScriptState {
        ScriptState {
            slot,
            tx_index: 1,
            order: 2,
        }
    }

    #[test]
    fn first_seen_records_a_new_script() {
        let mut delta = ScriptFirstSeen::new(Hash::new([1u8; 28]), first(42));
        let mut entity = None;

        delta.apply(&mut entity);

        assert!(delta.was_new);
        assert_eq!(entity, Some(first(42)));

        delta.undo(&mut entity);
        assert_eq!(entity, None);
    }

    #[test]
    fn first_seen_leaves_a_known_script_alone() {
        let mut delta = ScriptFirstSeen::new(Hash::new([1u8; 28]), first(42));
        let mut entity = Some(first(10));

        delta.apply(&mut entity);
        assert!(!delta.was_new);
        assert_eq!(entity, Some(first(10)));

        delta.undo(&mut entity);
        assert_eq!(entity, Some(first(10)));
    }

    #[test]
    fn positions_order_as_db_sync_lists() {
        let at = |slot, tx_index, order| ScriptState {
            slot,
            tx_index,
            order,
        };

        let mut positions = vec![at(2, 0, 0), at(1, 3, 0), at(1, 0, 5), at(1, 0, 1)];
        positions.sort();

        assert_eq!(
            positions,
            vec![at(1, 0, 1), at(1, 0, 5), at(1, 3, 0), at(2, 0, 0)]
        );
    }

    #[test]
    fn first_script_keys_are_epoch_then_hash() {
        let key = StageFirstScript::new(300, Hash::new([7u8; 28]), first(1)).key;
        let raw = dolos_core::keys::encode(&key);

        assert_eq!(raw.len(), 8 + 28);
        assert_eq!(raw[..8], 300u64.to_be_bytes());
        assert_eq!(raw[8..], [7u8; 28]);
        assert_eq!(FirstScriptKey::decode_key(&raw), Ok(key));
        assert!(FirstScriptKey::decode_key(&raw[..35]).is_err());
    }

    prop_compose! {
        fn any_script_first_seen()(
            script_hash in root::any_hash_28(),
            first in any_script_state(),
        ) -> ScriptFirstSeen {
            ScriptFirstSeen::new(script_hash, first)
        }
    }

    prop_compose! {
        fn any_stage_first_script()(
            epoch in 0u64..1000,
            script_hash in root::any_hash_28(),
            first in any_script_state(),
        ) -> StageFirstScript {
            StageFirstScript::new(epoch, script_hash, first)
        }
    }

    proptest! {
        #[test]
        fn script_first_seen_roundtrip(
            entity in prop::option::of(any_script_state()),
            delta in any_script_first_seen(),
        ) {
            assert_delta_roundtrip(entity, delta.clone());
            assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn stage_first_script_roundtrip(
            entity in prop::option::of(any_script_state().prop_map(FirstScriptSet::from)),
            delta in any_stage_first_script(),
        ) {
            assert_delta_roundtrip(entity, delta.clone());
            assert_delta_serde_roundtrip(entity, delta);
        }
    }
}
