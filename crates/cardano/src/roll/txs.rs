use dolos_core::{batch::WorkDeltas, State3Error};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};

use crate::{roll::BlockVisitor, CardanoLogic};

pub struct TxLogVisitor;

impl BlockVisitor for TxLogVisitor {
    fn visit_tx(
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        tx: &MultiEraTx,
    ) -> Result<(), State3Error> {
        deltas.slot.tx_hashes.push(tx.hash().to_vec());

        Ok(())
    }
}
