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

use dolos_core::{ChainError, TxoRef};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::conway::DatumOption,
        traverse::{MultiEraBlock, MultiEraInput, MultiEraOutput, MultiEraTx, OriginalHash},
    },
};

use super::{BlockVisitor, WorkDeltas};
use crate::owned::OwnedMultiEraOutput;
use crate::{DatumRefDecrement, DatumRefIncrement};

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
