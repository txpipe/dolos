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
/// - `refcount`: Number of UTxOs currently referencing this datum
/// - `bytes`: The raw CBOR-encoded datum bytes
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
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

entity_boilerplate!(DatumState, "datums");

// --- Deltas ---

/// Delta for incrementing a datum's reference count.
///
/// Emitted when a UTxO is produced with a `DatumOption::Hash` that references
/// a datum present in the transaction's witness set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatumRefIncrement {
    pub(crate) datum_hash: Hash<32>,
    pub(crate) datum_bytes: Vec<u8>,

    /// For undo: was this datum newly created (true) or did it already exist (false)?
    pub(crate) was_new: bool,
}

impl DatumRefIncrement {
    pub fn new(datum_hash: Hash<32>, datum_bytes: Vec<u8>) -> Self {
        Self {
            datum_hash,
            datum_bytes,
            was_new: false,
        }
    }
}

impl dolos_core::EntityDelta for DatumRefIncrement {
    type Entity = DatumState;

    fn key(&self) -> NsKey {
        NsKey::from((DATUM_NS, self.datum_hash))
    }

    fn apply(&mut self, entity: &mut Option<DatumState>) {
        match entity {
            Some(state) => {
                // Existing datum - just increment refcount
                self.was_new = false;
                state.refcount = state.refcount.saturating_add(1);
            }
            None => {
                // New datum - create with refcount=1
                self.was_new = true;
                *entity = Some(DatumState::new(self.datum_bytes.clone()));
            }
        }
    }

    fn undo(&self, _entity: &mut Option<DatumState>) {
        // no-op: undo not yet comprehensively implemented
    }
}

/// Delta for decrementing a datum's reference count.
///
/// Emitted when a UTxO is consumed that had a `DatumOption::Hash`.
/// If the refcount reaches zero, the datum is removed from the state store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatumRefDecrement {
    pub(crate) datum_hash: Hash<32>,

    /// For undo: the previous state before decrement (needed to restore if deleted)
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
        NsKey::from((DATUM_NS, self.datum_hash))
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

    fn undo(&self, _entity: &mut Option<DatumState>) {
        // no-op: undo not yet comprehensively implemented
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
}
