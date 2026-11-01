use dolos_core::NsKey;
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
};
use serde::{Deserialize, Serialize};

/// Namespace for datum entities in the state store.
pub const DATUM_NS: &str = "datums";

/// State of a witness datum with reference counting.
///
/// Datums are keyed by their hash (32 bytes) and store:
/// - `refcount`: Number of live UTxOs whose datum option is this hash
/// - `bytes`: The raw CBOR-encoded datum bytes, empty until a transaction
///   producing one of those UTxOs witnesses the datum
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatumState {
    #[n(0)]
    pub refcount: u64,
    #[n(1)]
    #[cbor(with = "minicbor::bytes")]
    pub bytes: Vec<u8>,
}

impl DatumState {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { refcount: 1, bytes }
    }
}

hash_key!(
    /// A datum, keyed by its hash.
    DatumKey,
    Hash<32>,
    32
);

display_as_stored!(DatumKey);

entity_boilerplate!(DatumState, "datums", state = DatumKey);

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_datum_state()(
            refcount in 0u64..1000u64,
            bytes in prop::collection::vec(any::<u8>(), 0..64),
        ) -> DatumState {
            DatumState { refcount, bytes }
        }
    }
}

// --- Deltas ---

/// Delta for incrementing a datum's reference count.
///
/// Emitted for every UTxO produced with a `DatumOption::Hash`, carrying the
/// datum when the transaction witnesses it and empty bytes otherwise.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatumRefIncrement {
    pub(crate) datum_hash: Hash<32>,
    pub(crate) datum_bytes: Vec<u8>,

    /// For undo: was this datum newly created (true) or did it already exist
    /// (false)?
    pub(crate) was_new: bool,

    /// For undo: did this delta set the bytes of a row that had none?
    pub(crate) filled: bool,
}

impl DatumRefIncrement {
    pub fn new(datum_hash: Hash<32>, datum_bytes: Vec<u8>) -> Self {
        Self {
            datum_hash,
            datum_bytes,
            was_new: false,
            filled: false,
        }
    }
}

impl dolos_core::EntityDelta for DatumRefIncrement {
    type Entity = DatumState;

    fn key(&self) -> NsKey {
        NsKey::of::<DatumState>(&DatumKey(self.datum_hash))
    }

    fn apply(&mut self, entity: &mut Option<DatumState>) {
        match entity {
            Some(state) => {
                self.was_new = false;
                self.filled = state.bytes.is_empty() && !self.datum_bytes.is_empty();
                state.refcount = state.refcount.saturating_add(1);

                if self.filled {
                    state.bytes = self.datum_bytes.clone();
                }
            }
            None => {
                self.was_new = true;
                self.filled = false;
                *entity = Some(DatumState::new(self.datum_bytes.clone()));
            }
        }
    }

    fn undo(&self, entity: &mut Option<DatumState>) {
        if self.was_new {
            *entity = None;
        } else if let Some(state) = entity {
            state.refcount = state.refcount.saturating_sub(1);

            if self.filled {
                state.bytes.clear();
            }
        }
    }
}

/// Delta for decrementing a datum's reference count.
///
/// Emitted when a UTxO is consumed that had a `DatumOption::Hash`, which its
/// production counted.
/// If the refcount reaches zero, the datum is removed from the state store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatumRefDecrement {
    pub(crate) datum_hash: Hash<32>,

    /// For undo: the previous state before decrement (needed to restore if
    /// deleted)
    pub(crate) prev_state: Option<DatumState>,
}

impl DatumRefDecrement {
    pub fn new(datum_hash: Hash<32>) -> Self {
        Self {
            datum_hash,
            prev_state: None,
        }
    }
}

impl dolos_core::EntityDelta for DatumRefDecrement {
    type Entity = DatumState;

    fn key(&self) -> NsKey {
        NsKey::of::<DatumState>(&DatumKey(self.datum_hash))
    }

    fn apply(&mut self, entity: &mut Option<DatumState>) {
        if let Some(state) = entity {
            // Save for undo before modifying
            self.prev_state = Some(state.clone());

            if state.refcount <= 1 {
                // Refcount would become zero, delete the datum
                *entity = None;
            } else {
                state.refcount -= 1;
            }
        }
        // If entity is None, this is a no-op (datum doesn't exist)
    }

    fn undo(&self, entity: &mut Option<DatumState>) {
        *entity = self.prev_state.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::EntityDelta;

    #[test]
    fn test_datum_increment_new() {
        let hash = Hash::new([1u8; 32]);
        let bytes = vec![0x01, 0x02, 0x03];

        let mut delta = DatumRefIncrement::new(hash, bytes.clone());
        let mut entity: Option<DatumState> = None;

        delta.apply(&mut entity);

        assert!(delta.was_new);
        assert!(entity.is_some());
        let state = entity.as_ref().unwrap();
        assert_eq!(state.refcount, 1);
        assert_eq!(state.bytes, bytes);
    }

    #[test]
    fn test_datum_increment_existing() {
        let hash = Hash::new([1u8; 32]);
        let bytes = vec![0x01, 0x02, 0x03];

        let mut delta = DatumRefIncrement::new(hash, bytes.clone());
        let mut entity: Option<DatumState> = Some(DatumState {
            refcount: 5,
            bytes: bytes.clone(),
        });

        delta.apply(&mut entity);

        assert!(!delta.was_new);
        let state = entity.as_ref().unwrap();
        assert_eq!(state.refcount, 6);
    }

    #[test]
    fn test_datum_increment_unwitnessed_then_witnessed() {
        let hash = Hash::new([1u8; 32]);
        let bytes = vec![0x01, 0x02, 0x03];

        let mut unwitnessed = DatumRefIncrement::new(hash, vec![]);
        let mut entity: Option<DatumState> = None;
        unwitnessed.apply(&mut entity);
        let counted = entity.clone();
        assert_eq!(
            counted,
            Some(DatumState {
                refcount: 1,
                bytes: vec![]
            })
        );

        let mut witnessed = DatumRefIncrement::new(hash, bytes.clone());
        witnessed.apply(&mut entity);
        assert!(witnessed.filled);
        assert_eq!(entity, Some(DatumState { refcount: 2, bytes }));

        witnessed.undo(&mut entity);
        assert_eq!(entity, counted);
    }

    #[test]
    fn test_datum_increment_keeps_known_bytes() {
        let hash = Hash::new([1u8; 32]);
        let bytes = vec![0x01, 0x02, 0x03];
        let known = Some(DatumState {
            refcount: 1,
            bytes: bytes.clone(),
        });

        let mut unwitnessed = DatumRefIncrement::new(hash, vec![]);
        let mut entity = known.clone();
        unwitnessed.apply(&mut entity);
        assert!(!unwitnessed.filled);
        assert_eq!(entity, Some(DatumState { refcount: 2, bytes }));

        unwitnessed.undo(&mut entity);
        assert_eq!(entity, known);
    }

    #[test]
    fn test_datum_decrement_to_zero() {
        let hash = Hash::new([1u8; 32]);
        let bytes = vec![0x01, 0x02, 0x03];

        let mut delta = DatumRefDecrement::new(hash);
        let mut entity: Option<DatumState> = Some(DatumState {
            refcount: 1,
            bytes: bytes.clone(),
        });

        delta.apply(&mut entity);

        // Should be deleted
        assert!(entity.is_none());
    }

    #[test]
    fn test_datum_decrement_not_to_zero() {
        let hash = Hash::new([1u8; 32]);
        let bytes = vec![0x01, 0x02, 0x03];

        let mut delta = DatumRefDecrement::new(hash);
        let mut entity: Option<DatumState> = Some(DatumState {
            refcount: 3,
            bytes: bytes.clone(),
        });

        delta.apply(&mut entity);

        let state = entity.as_ref().unwrap();
        assert_eq!(state.refcount, 2);
    }

    use super::testing::any_datum_state;
    use crate::model::testing::{self as root, assert_delta_roundtrip};
    use proptest::prelude::*;

    prop_compose! {
        fn any_datum_ref_increment()(
            hash in root::any_hash_32(),
            bytes in prop::collection::vec(any::<u8>(), 0..64),
        ) -> DatumRefIncrement {
            DatumRefIncrement::new(hash, bytes)
        }
    }

    prop_compose! {
        fn any_datum_ref_decrement()(
            hash in root::any_hash_32(),
        ) -> DatumRefDecrement {
            DatumRefDecrement::new(hash)
        }
    }

    proptest! {
        #[test]
        fn datum_ref_increment_roundtrip(
            entity in prop::option::of(any_datum_state()),
            delta in any_datum_ref_increment(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn datum_ref_decrement_roundtrip(
            entity in prop::option::of(any_datum_state()),
            delta in any_datum_ref_decrement(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }
    }
}

#[cfg(test)]
mod key_tests {
    use dolos_core::{keys::encode, KeyCodec};
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn layout_is_pinned() {
        assert_eq!(
            encode(&DatumKey(Hash::new([0x13; 32]))).to_vec(),
            [0x13; 32]
        );
    }

    proptest! {
        #[test]
        fn keys_round_trip(hash in any::<[u8; 32]>()) {
            let key = DatumKey(hash.into());
            prop_assert_eq!(DatumKey::decode_key(&encode(&key)), Ok(key));
        }
    }
}
