use std::{borrow::Cow, collections::HashMap};

use dolos_core::{NsKey, State3Error, State3Store, StateDelta};
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    model::{AccountState, CardanoDelta, EpochState, FixedNamespace as _, EPOCH_KEY_MARK},
    pallas_extras,
    roll::{BlockVisitor, DeltaBuilder},
};

#[derive(Debug, Clone)]
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

pub struct EpochStateVisitor<'a> {
    delta: &'a mut StateDelta<CardanoDelta>,
}

impl<'a> From<&'a mut StateDelta<CardanoDelta>> for EpochStateVisitor<'a> {
    fn from(delta: &'a mut StateDelta<CardanoDelta>) -> Self {
        Self { delta }
    }
}

impl<'a> BlockVisitor for EpochStateVisitor<'a> {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        let block_fees = block.txs().iter().filter_map(|tx| tx.fee()).sum::<u64>();

        self.delta.add_delta(EpochStatsUpdate { block_fees });

        Ok(())
    }
}
