use std::collections::HashMap;

use dolos_core::{State3Error, State3Store};
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    model::{AccountState, EpochState},
    pallas_extras,
    roll::{BlockVisitor, DeltaBuilder, SliceBuilder},
};

pub struct EpochStateVisitor<'a, T>(&'a mut T);

impl<'a, T> From<&'a mut T> for EpochStateVisitor<'a, T> {
    fn from(value: &'a mut T) -> Self {
        Self(value)
    }
}

impl<'a, S: State3Store> BlockVisitor for EpochStateVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        self.0
            .slice
            .ensure_loaded_typed::<EpochState>(crate::model::CURRENT_EPOCH_KEY, self.0.store)?;

        let cursor = self.0.store().get_cursor()?;

        let should_compute =
            pallas_extras::is_epoch_boundary(&self.0.chain_summary, cursor, block.slot());

        if should_compute {
            let mut by_pool = HashMap::<[u8; 28], u128>::new();

            let all_accounts = self.0.store().iter_entities_typed::<AccountState>(
                &[0u8; 32].as_slice()..&[255u8; 32].as_slice(),
            )?;

            for record in all_accounts {
                let (_, value) = record?;

                if let Some(pool_id) = value.pool_id {
                    let key = pool_id.try_into().unwrap();
                    let entry = by_pool.entry(key).or_insert(0);
                    *entry += value.controlled_amount as u128;
                }
            }
        }

        Ok(())
    }
}

impl<'a> BlockVisitor for EpochStateVisitor<'a, DeltaBuilder<'_>> {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        let current = self
            .0
            .slice()
            .get_entity_typed::<EpochState>(crate::model::CURRENT_EPOCH_KEY)?
            .unwrap_or_default();

        let block_fees = block.txs().iter().filter_map(|tx| tx.fee()).sum::<u64>();

        let new = EpochState {
            gathered_fees: Some(current.gathered_fees.unwrap_or_default() + block_fees),
            ..current
        };

        self.0
            .delta_mut()
            .override_entity(crate::model::CURRENT_EPOCH_KEY, new, None);

        Ok(())
    }
}
