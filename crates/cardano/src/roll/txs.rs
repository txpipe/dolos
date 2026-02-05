//! Transaction log visitor for extracting entity deltas.
//!
//! This module provides the `TxLogVisitor` which extracts entity deltas
//! during block traversal. Index tag extraction has been moved to
//! `CardanoIndexDeltaBuilder` in the indexes module.

use std::collections::HashMap;

use dolos_core::{ChainError, Genesis, TxoRef};
use pallas::{
    codec::utils::KeepRaw,
    ledger::{
        primitives::{conway::PlutusData, Epoch},
        traverse::{MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraRedeemer, MultiEraTx},
    },
};

use super::WorkDeltas;
use crate::{owned::OwnedMultiEraOutput, roll::BlockVisitor, PParamsSet};

/// Visitor that extracts entity deltas from block transactions.
///
/// Note: Index tag extraction has been moved to `CardanoIndexDeltaBuilder`.
/// This visitor now only handles entity delta extraction.
#[derive(Default, Clone)]
pub struct TxLogVisitor;

impl BlockVisitor for TxLogVisitor {
    fn visit_root(
        &mut self,
        _deltas: &mut WorkDeltas,
        _block: &MultiEraBlock,
        _: &Genesis,
        _: &PParamsSet,
        _: Epoch,
        _: u64,
        _: u16,
    ) -> Result<(), ChainError> {
        // Index tag extraction moved to CardanoIndexDeltaBuilder
        Ok(())
    }

    fn visit_tx(
        &mut self,
        _deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _tx: &MultiEraTx,
        _: &HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Result<(), ChainError> {
        // Index tag extraction moved to CardanoIndexDeltaBuilder
        Ok(())
    }

    fn visit_input(
        &mut self,
        _deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _input: &MultiEraInput,
        _resolved: &pallas::ledger::traverse::MultiEraOutput,
    ) -> Result<(), ChainError> {
        // Index tag extraction moved to CardanoIndexDeltaBuilder
        Ok(())
    }

    fn visit_output(
        &mut self,
        _deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: u32,
        _output: &pallas::ledger::traverse::MultiEraOutput,
    ) -> Result<(), ChainError> {
        // Index tag extraction moved to CardanoIndexDeltaBuilder
        Ok(())
    }

    fn visit_datums(
        &mut self,
        _deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _datum: &KeepRaw<'_, PlutusData>,
    ) -> Result<(), ChainError> {
        // Index tag extraction moved to CardanoIndexDeltaBuilder
        Ok(())
    }

    fn visit_cert(
        &mut self,
        _deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        // Index tag extraction moved to CardanoIndexDeltaBuilder
        Ok(())
    }

    fn visit_redeemers(
        &mut self,
        _deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _redeemer: &MultiEraRedeemer,
    ) -> Result<(), ChainError> {
        // Index tag extraction moved to CardanoIndexDeltaBuilder
        Ok(())
    }
}

#[cfg(test)]
mod test {
    // Tests for index tag extraction have moved to indexes/delta.rs
}
