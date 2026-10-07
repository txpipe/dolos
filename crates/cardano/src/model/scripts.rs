//! The script registry: every script the chain has carried, in the order it
//! first showed up.
//!
//! Nothing else enumerates scripts. They are not ledger state, and the archive
//! index keeps a hash of the script hash, so it answers "where is this script"
//! but not "which scripts are there". The registry is two namespaces:
//!
//! - `scripts`, keyed by script hash: what a block needs to tell a new script
//!   from a repeat.
//! - `script_seqs`, keyed by a dense sequence number: the listing. Sequence
//!   numbers are handed out in the order cardano-db-sync numbers its `script`
//!   rows, so a page of the listing is a key range and its position from either
//!   end is arithmetic.

use dolos_core::{EntityKey, NsKey, StateError, StateStore};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
};
use serde::{Deserialize, Serialize};

use super::FixedNamespace as _;

/// A script the chain has carried, keyed by its hash.
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScriptState {
    /// Position in the listing, see [`ScriptSeqState`].
    #[n(0)]
    pub seq: u64,

    /// Slot of the block that carried it first.
    #[n(1)]
    pub first_slot: u64,
}

entity_boilerplate!(ScriptState, "scripts");

/// The script at one position of the listing, keyed by that position.
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScriptSeqState {
    #[n(0)]
    pub script_hash: Hash<28>,
}

entity_boilerplate!(ScriptSeqState, "script_seqs");

impl ScriptSeqState {
    /// Big-endian, so key order is listing order.
    pub fn key(seq: u64) -> EntityKey {
        EntityKey::from(seq.to_be_bytes().as_slice())
    }

    /// How many scripts the registry lists.
    ///
    /// Sequence numbers are dense (a rollback takes them off the top), so the
    /// count is the first number with no row: found by doubling a probe until
    /// it misses and halving the gap, a few dozen point reads instead of a
    /// counter that every new script would have to update.
    pub fn count<S: StateStore>(state: &S) -> Result<u64, StateError> {
        let exists = |seq: u64| -> Result<bool, StateError> {
            let found = state.read_entities(Self::NS, &[&Self::key(seq)])?;
            Ok(found.into_iter().next().flatten().is_some())
        };

        if !exists(0)? {
            return Ok(0);
        }

        // invariant: `present` has a row, `missing` has none
        let mut present = 0u64;
        let mut missing = 1u64;

        while exists(missing)? {
            present = missing;
            missing = missing.saturating_mul(2);
        }

        while missing - present > 1 {
            let middle = present + (missing - present) / 2;

            if exists(middle)? {
                present = middle;
            } else {
                missing = middle;
            }
        }

        Ok(missing)
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_script_state()(
            seq in any::<u64>(),
            first_slot in any::<u64>(),
        ) -> ScriptState {
            ScriptState { seq, first_slot }
        }
    }

    prop_compose! {
        pub fn any_script_seq_state()(
            script_hash in root::any_hash_28(),
        ) -> ScriptSeqState {
            ScriptSeqState { script_hash }
        }
    }
}

// --- Deltas ---

/// Registers a script under its hash.
///
/// Emitted once per script, by the block that carries it first. The roll
/// decides that ahead of applying (it needs the sequence number for
/// [`ScriptSeqAssigned`] too), so an existing row is left alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptFirstSeen {
    pub(crate) script_hash: Hash<28>,
    pub(crate) seq: u64,
    pub(crate) slot: u64,

    /// For undo: did this delta create the row?
    pub(crate) was_new: bool,
}

impl ScriptFirstSeen {
    pub fn new(script_hash: Hash<28>, seq: u64, slot: u64) -> Self {
        Self {
            script_hash,
            seq,
            slot,
            was_new: false,
        }
    }
}

impl dolos_core::EntityDelta for ScriptFirstSeen {
    type Entity = ScriptState;

    fn key(&self) -> NsKey {
        NsKey::from((ScriptState::NS, self.script_hash))
    }

    fn apply(&mut self, entity: &mut Option<ScriptState>) {
        self.was_new = entity.is_none();

        if self.was_new {
            *entity = Some(ScriptState {
                seq: self.seq,
                first_slot: self.slot,
            });
        }
    }

    fn undo(&self, entity: &mut Option<ScriptState>) {
        if self.was_new {
            *entity = None;
        }
    }
}

/// Puts a script at its position of the listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptSeqAssigned {
    pub(crate) seq: u64,
    pub(crate) script_hash: Hash<28>,

    /// For undo: whatever the position held before.
    pub(crate) prev: Option<ScriptSeqState>,
}

impl ScriptSeqAssigned {
    pub fn new(seq: u64, script_hash: Hash<28>) -> Self {
        Self {
            seq,
            script_hash,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for ScriptSeqAssigned {
    type Entity = ScriptSeqState;

    fn key(&self) -> NsKey {
        NsKey::from((ScriptSeqState::NS, ScriptSeqState::key(self.seq)))
    }

    fn apply(&mut self, entity: &mut Option<ScriptSeqState>) {
        self.prev = entity.take();

        *entity = Some(ScriptSeqState {
            script_hash: self.script_hash,
        });
    }

    fn undo(&self, entity: &mut Option<ScriptSeqState>) {
        *entity = self.prev.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{any_script_seq_state, any_script_state};
    use super::*;
    use crate::model::testing::{
        self as root, assert_delta_roundtrip, assert_delta_serde_roundtrip,
    };
    use dolos_core::EntityDelta as _;
    use proptest::prelude::*;

    #[test]
    fn first_seen_registers_a_new_script() {
        let mut delta = ScriptFirstSeen::new(Hash::new([1u8; 28]), 7, 42);
        let mut entity = None;

        delta.apply(&mut entity);

        assert!(delta.was_new);
        assert_eq!(
            entity,
            Some(ScriptState {
                seq: 7,
                first_slot: 42
            })
        );

        delta.undo(&mut entity);
        assert_eq!(entity, None);
    }

    #[test]
    fn first_seen_leaves_a_known_script_alone() {
        let known = ScriptState {
            seq: 3,
            first_slot: 10,
        };

        let mut delta = ScriptFirstSeen::new(Hash::new([1u8; 28]), 7, 42);
        let mut entity = Some(known.clone());

        delta.apply(&mut entity);
        assert!(!delta.was_new);
        assert_eq!(entity, Some(known.clone()));

        delta.undo(&mut entity);
        assert_eq!(entity, Some(known));
    }

    #[test]
    fn count_is_the_first_sequence_number_with_no_row() {
        use dolos_core::{builtin::MemoryStateStore, StateStore as _, StateWriter as _};

        let state = MemoryStateStore::new();
        assert_eq!(ScriptSeqState::count(&state).unwrap(), 0);

        let mut written = 0u64;

        // around every power of two the probe doubles over, and one big jump
        for total in [1u64, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 1_000] {
            let writer = state.start_writer().unwrap();

            for seq in written..total {
                let row = ScriptSeqState {
                    script_hash: Hash::new([seq as u8; 28]),
                };
                writer
                    .write_entity_typed(&ScriptSeqState::key(seq), &row)
                    .unwrap();
            }

            writer.commit().unwrap();
            written = total;

            assert_eq!(ScriptSeqState::count(&state).unwrap(), total);
        }
    }

    #[test]
    fn seq_keys_sort_in_listing_order() {
        let keys: Vec<_> = [0u64, 1, 255, 256, 65_536, u64::MAX]
            .into_iter()
            .map(ScriptSeqState::key)
            .collect();

        let mut sorted = keys.clone();
        sorted.sort();

        assert_eq!(keys, sorted);
    }

    prop_compose! {
        fn any_script_first_seen()(
            script_hash in root::any_hash_28(),
            seq in any::<u64>(),
            slot in any::<u64>(),
        ) -> ScriptFirstSeen {
            ScriptFirstSeen::new(script_hash, seq, slot)
        }
    }

    prop_compose! {
        fn any_script_seq_assigned()(
            seq in any::<u64>(),
            script_hash in root::any_hash_28(),
        ) -> ScriptSeqAssigned {
            ScriptSeqAssigned::new(seq, script_hash)
        }
    }

    proptest! {
        #[test]
        fn script_first_seen_roundtrip(
            entity in prop::option::of(any_script_state()),
            delta in any_script_first_seen(),
        ) {
            assert_delta_roundtrip(entity.clone(), delta.clone());
            assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn script_seq_assigned_roundtrip(
            entity in prop::option::of(any_script_seq_state()),
            delta in any_script_seq_assigned(),
        ) {
            assert_delta_roundtrip(entity.clone(), delta.clone());
            assert_delta_serde_roundtrip(entity, delta);
        }
    }
}
