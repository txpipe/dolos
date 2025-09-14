use dolos_core::{batch::WorkDeltas, ChainError};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};

use crate::{roll::BlockVisitor, CardanoLogic};

#[derive(Default)]
pub struct TxLogVisitor;

impl BlockVisitor for TxLogVisitor {
    fn visit_tx(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        tx: &MultiEraTx,
    ) -> Result<(), ChainError> {
        deltas.slot.tx_hashes.push(tx.hash().to_vec());

        Ok(())
    }
}
