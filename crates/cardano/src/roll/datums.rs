//! Datum state tracking via entity-delta pattern.
//!
//! This module implements witness datum reference counting using the entity system.
//! Datums are stored with a reference count that tracks how many UTxOs reference them.
//! When a UTxO with a datum hash is produced, the refcount is incremented.
//! When consumed, it's decremented. When refcount reaches zero, the datum is removed.
//!
//! Only witness datums (those in the transaction witness set with `DatumOption::Hash`)
//! are tracked. Inline datums (`DatumOption::Data`) are not reference counted.

use std::collections::HashMap;

use dolos_core::{ChainError, NsKey, TxoRef};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::conway::DatumOption,
        traverse::{MultiEraBlock, MultiEraInput, MultiEraOutput, MultiEraTx, OriginalHash},
    },
};
use serde::{Deserialize, Serialize};

use super::{BlockVisitor, WorkDeltas};
use crate::model::{DatumState, DATUM_NS};
use crate::owned::OwnedMultiEraOutput;

/// Delta for incrementing a datum's reference count.
///
/// Emitted when a UTxO is produced with a `DatumOption::Hash` that references
/// a datum present in the transaction's witness set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatumRefIncrement {
    datum_hash: Hash<32>,
    datum_bytes: Vec<u8>,

    /// For undo: was this datum newly created (true) or did it already exist (false)?
    was_new: bool,
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

    fn undo(&self, entity: &mut Option<DatumState>) {
        if self.was_new {
            // Was newly created, so remove it entirely
            *entity = None;
        } else if let Some(state) = entity {
            // Was existing, decrement back
            state.refcount = state.refcount.saturating_sub(1);
        }
    }
}

/// Delta for decrementing a datum's reference count.
///
/// Emitted when a UTxO is consumed that had a `DatumOption::Hash`.
/// If the refcount reaches zero, the datum is removed from the state store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatumRefDecrement {
    datum_hash: Hash<32>,

    /// For undo: the previous state before decrement (needed to restore if deleted)
    prev_state: Option<DatumState>,
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

    fn undo(&self, entity: &mut Option<DatumState>) {
        // Restore the previous state
        *entity = self.prev_state.clone();
    }
}

/// Visitor that tracks witness datums and emits refcount deltas.
///
/// For each transaction:
/// 1. Collects witness datums from `tx.plutus_data()` into a temporary map
/// 2. When processing outputs with `DatumOption::Hash`, emits `DatumRefIncrement`
/// 3. When processing inputs with `DatumOption::Hash`, emits `DatumRefDecrement`
#[derive(Default)]
pub struct DatumVisitor {
    /// Witness datums collected from current transaction's plutus_data()
    witness_datums: HashMap<Hash<32>, Vec<u8>>,
}

impl BlockVisitor for DatumVisitor {
    fn visit_tx(
        &mut self,
        _deltas: &mut WorkDeltas,
        _block: &MultiEraBlock,
        tx: &MultiEraTx,
        _utxos: &HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Result<(), ChainError> {
        // Clear and collect witness datums for this transaction
        self.witness_datums.clear();

        for datum in tx.plutus_data() {
            let datum_hash = datum.original_hash();
            let datum_bytes = datum.raw_cbor().to_vec();
            self.witness_datums.insert(datum_hash, datum_bytes);
        }

        Ok(())
    }

    fn visit_output(
        &mut self,
        deltas: &mut WorkDeltas,
        _block: &MultiEraBlock,
        _tx: &MultiEraTx,
        _index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        // Check if this output references a datum by hash
        if let Some(DatumOption::Hash(datum_hash)) = output.datum() {
            // Only emit increment if the datum is in the witness set
            if let Some(datum_bytes) = self.witness_datums.get(&datum_hash) {
                deltas.add_for_entity(DatumRefIncrement::new(datum_hash, datum_bytes.clone()));
            }
        }

        Ok(())
    }

    fn visit_input(
        &mut self,
        deltas: &mut WorkDeltas,
        _block: &MultiEraBlock,
        _tx: &MultiEraTx,
        _input: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        // Check if the consumed UTxO had a datum hash
        if let Some(DatumOption::Hash(datum_hash)) = resolved.datum() {
            deltas.add_for_entity(DatumRefDecrement::new(datum_hash));
        }

        Ok(())
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

        // Undo should remove it
        delta.undo(&mut entity);
        assert!(entity.is_none());
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

        // Undo should decrement back
        delta.undo(&mut entity);
        let state = entity.as_ref().unwrap();
        assert_eq!(state.refcount, 5);
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

        // Undo should restore
        delta.undo(&mut entity);
        assert!(entity.is_some());
        let state = entity.as_ref().unwrap();
        assert_eq!(state.refcount, 1);
        assert_eq!(state.bytes, bytes);
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

        // Undo should restore
        delta.undo(&mut entity);
        let state = entity.as_ref().unwrap();
        assert_eq!(state.refcount, 3);
    }
}
