use std::borrow::Cow;

use dolos_core::{batch::WorkDeltas, NsKey, State3Error};
use pallas::ledger::traverse::MultiEraBlock;
use serde::{Deserialize, Serialize};

use crate::{
    model::{EpochState, FixedNamespace as _, EPOCH_KEY_MARK},
    roll::BlockVisitor,
    CardanoLogic,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochStatsUpdate {
    block_fees: u64,
}

impl dolos_core::EntityDelta for EpochStatsUpdate {
    type Entity = EpochState;

    fn key(&self) -> Cow<'_, NsKey> {
        Cow::Owned(NsKey::from((EpochState::NS, EPOCH_KEY_MARK)))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let entity = entity.get_or_insert_default();

        entity.gathered_fees += self.block_fees;
    }

    fn undo(&mut self, entity: &mut Option<EpochState>) {
        let entity = entity.get_or_insert_default();

        entity.gathered_fees -= self.block_fees;
    }
}

pub struct EpochStateVisitor;

impl BlockVisitor for EpochStateVisitor {
    fn visit_root(
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
    ) -> Result<(), State3Error> {
        let block_fees = block.txs().iter().filter_map(|tx| tx.fee()).sum::<u64>();

        deltas.add_for_entity(EpochStatsUpdate { block_fees });

        Ok(())
    }
}
