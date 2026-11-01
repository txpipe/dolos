//! Datum state tracking via entity-delta pattern.
//!
//! This module implements witness datum reference counting using the entity
//! system. A datum's row counts the live UTxOs whose datum option is its hash:
//! producing one increments the count and consuming one decrements it, so the
//! two always pair up, and the row is removed when the count reaches zero.
//! The row holds the datum's bytes once a transaction producing one of those
//! UTxOs witnesses it, and empty bytes until then.
//!
//! Inline datums (`DatumOption::Data`) are not reference counted.

use std::collections::HashMap;

use dolos_core::{ChainError, TxOrder, TxoRef};
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
/// 2. For every output with `DatumOption::Hash`, emits `DatumRefIncrement`,
///    with the datum when the transaction witnesses it
/// 3. For every input with `DatumOption::Hash`, emits `DatumRefDecrement`
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
        _: &TxOrder,
        _index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        // Count every output that references a datum by hash, witnessed or
        // not, so that its spend's decrement pairs with this increment
        if let Some(DatumOption::Hash(datum_hash)) = output.datum() {
            let datum_bytes = self
                .witness_datums
                .get(&datum_hash)
                .cloned()
                .unwrap_or_default();

            deltas.add_for_entity(DatumRefIncrement::new(datum_hash, datum_bytes));
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
