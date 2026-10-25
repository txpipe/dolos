//! Scripts: every script the chain has carried, in the order cardano-db-sync
//! first saw it (decision 0057).
//!
//! Nothing else enumerates scripts. They are not ledger state, and the archive
//! index keeps a hash of the script hash, so it answers "where is this script"
//! but not "which scripts are there". The `script` tag follows phase-2
//! validity, so a script's first tag is its first sighting: the roll seeks it
//! to tell a new script from a repeat, and `/scripts/{hash}` reads the script
//! there. Two namespaces list them:
//!
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

hash_key!(
    /// The hash of a script, the member of its `first-scripts` key.
    ScriptKey,
    Hash<28>,
    28
);

display_as_stored!(ScriptKey);

/// A script an epoch saw first, held in state until the epoch seals into
/// [`FirstScriptChunk`]s. The value is where the script was first seen: its
/// block's slot, its tx's index in the block and its place among the scripts
/// the tx carries.
///
/// Field order is listing order, so the derived ordering is db-sync's.
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
        pub fn any_first_script_set()(
            slot in root::any_slot(),
            tx_index in 0u32..500,
            order in 0u32..50,
        ) -> FirstScriptSet {
            FirstScriptSet { slot, tx_index, order }
        }
    }
}

// --- Deltas ---

/// Stages a script in the `first-scripts` row of the epoch that saw it first.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageFirstScript {
    pub(crate) key: FirstScriptKey,
    pub(crate) row: FirstScriptSet,

    /// For undo: did this delta create the row?
    pub(crate) was_new: bool,
}

impl StageFirstScript {
    pub fn new(epoch: u64, script_hash: Hash<28>, first: FirstScriptSet) -> Self {
        Self {
            key: GroupKey {
                group: Ungrouped,
                epoch,
                member: ScriptKey(script_hash),
            },
            row: first,
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
    use super::testing::any_first_script_set;
    use super::*;
    use crate::model::testing::{
        self as root, assert_delta_roundtrip, assert_delta_serde_roundtrip,
    };
    use dolos_core::KeyCodec as _;
    use proptest::prelude::*;

    #[test]
    fn positions_order_as_db_sync_lists() {
        let at = |slot, tx_index, order| FirstScriptSet {
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
        let sighting = FirstScriptSet {
            slot: 1,
            tx_index: 1,
            order: 2,
        };
        let key = StageFirstScript::new(300, Hash::new([7u8; 28]), sighting).key;
        let raw = dolos_core::keys::encode(&key);

        assert_eq!(raw.len(), 8 + 28);
        assert_eq!(raw[..8], 300u64.to_be_bytes());
        assert_eq!(raw[8..], [7u8; 28]);
        assert_eq!(FirstScriptKey::decode_key(&raw), Ok(key));
        assert!(FirstScriptKey::decode_key(&raw[..35]).is_err());
    }

    prop_compose! {
        fn any_stage_first_script()(
            epoch in 0u64..1000,
            script_hash in root::any_hash_28(),
            first in any_first_script_set(),
        ) -> StageFirstScript {
            StageFirstScript::new(epoch, script_hash, first)
        }
    }

    proptest! {
        #[test]
        fn stage_first_script_roundtrip(
            entity in prop::option::of(any_first_script_set()),
            delta in any_stage_first_script(),
        ) {
            assert_delta_roundtrip(entity, delta.clone());
            assert_delta_serde_roundtrip(entity, delta);
        }
    }
}
