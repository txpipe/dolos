//! The materialized views (`NamespaceRole::View`): what each block adds to
//! the period logs and stats the API serves, as one visitor set.
//!
//! The ledger never reads a view, so the set is skipped as a whole by a node
//! that materializes none (`SyncConfig::materialized_views`), and a new view
//! joins the roll by joining this set.

use std::collections::HashMap;

use dolos_core::{ChainError, Genesis, TxOrder, TxoRef};
use pallas::{
    codec::utils::KeepRaw,
    ledger::{
        primitives::{Epoch, PlutusData},
        traverse::{
            MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraPolicyAssets,
            MultiEraProposal, MultiEraRedeemer, MultiEraTx, MultiEraUpdate,
        },
    },
};

use super::{
    first_mints::FirstMintVisitor, stake_addresses::StakeAddressVisitor, stats::StatsVisitor,
    BlockVisitor, WorkDeltas,
};
use crate::{owned::OwnedMultiEraOutput, PParamsSet};

#[derive(Default)]
pub struct ViewVisitors {
    first_mints: FirstMintVisitor,
    stake_addresses: StakeAddressVisitor,
    stats: StatsVisitor,
}

/// Forward a visit to every view, in declaration order, stopping at the first
/// error.
macro_rules! each {
    ($self:ident . $method:ident ( $($arg:expr),* $(,)? )) => {{
        $self.first_mints.$method($($arg),*)?;
        $self.stake_addresses.$method($($arg),*)?;
        $self.stats.$method($($arg),*)
    }};
}

impl BlockVisitor for ViewVisitors {
    fn visit_root(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        genesis: &Genesis,
        pparams: &PParamsSet,
        epoch: Epoch,
        epoch_start: u64,
        protocol: u16,
    ) -> Result<(), ChainError> {
        each!(self.visit_root(
            deltas,
            block,
            genesis,
            pparams,
            epoch,
            epoch_start,
            protocol
        ))
    }

    fn visit_tx(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        utxos: &HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Result<(), ChainError> {
        each!(self.visit_tx(deltas, block, tx, utxos))
    }

    fn visit_input(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        input: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        each!(self.visit_input(deltas, block, tx, input, resolved))
    }

    fn visit_output(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        order: &TxOrder,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        each!(self.visit_output(deltas, block, tx, order, index, output))
    }

    fn visit_mint(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        order: &TxOrder,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), ChainError> {
        each!(self.visit_mint(deltas, block, tx, order, mint))
    }

    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        order: &TxOrder,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        each!(self.visit_cert(deltas, block, tx, order, cert))
    }

    fn visit_withdrawal(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        account: &[u8],
        amount: u64,
    ) -> Result<(), ChainError> {
        each!(self.visit_withdrawal(deltas, block, tx, account, amount))
    }

    fn visit_update(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: Option<&MultiEraTx>,
        update: &MultiEraUpdate,
    ) -> Result<(), ChainError> {
        each!(self.visit_update(deltas, block, tx, update))
    }

    fn visit_datums(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        data: &KeepRaw<'_, PlutusData>,
    ) -> Result<(), ChainError> {
        each!(self.visit_datums(deltas, block, tx, data))
    }

    fn visit_proposal(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        proposal: &MultiEraProposal,
        idx: usize,
    ) -> Result<(), ChainError> {
        each!(self.visit_proposal(deltas, block, tx, proposal, idx))
    }

    fn visit_redeemers(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        redeemer: &MultiEraRedeemer,
    ) -> Result<(), ChainError> {
        each!(self.visit_redeemers(deltas, block, tx, redeemer))
    }

    fn flush(&mut self, deltas: &mut WorkDeltas) -> Result<(), ChainError> {
        each!(self.flush(deltas))
    }
}
