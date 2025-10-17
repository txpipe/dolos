use crate::{EndStats, EpochState, FixedNamespace as _, CURRENT_EPOCH_KEY};
use dolos_core::{ChainError, NsKey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochWrapUp {
    stats: EndStats,
}

impl dolos_core::EntityDelta for EpochWrapUp {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        entity.end = Some(self.stats.clone());
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        entity.end = None;
    }
}

#[derive(Default)]
pub struct BoundaryVisitor;

fn define_new_pool_count(ctx: &super::BoundaryWork) -> usize {
    let rolling = ctx.ending_state.rolling.live();

    // we need to know which of the registered pools are actually new pools that
    // need deposit vs re-registration of existing pools.
    let repeated_pools = ctx
        .existing_pools
        .intersection(&rolling.registered_pools)
        .count();

    rolling.registered_pools.len() - repeated_pools
}

fn define_end_stats(ctx: &super::BoundaryWork) -> EndStats {
    EndStats {
        new_pools: define_new_pool_count(ctx) as u64,
        retired_pools: ctx.retiring_pools.clone(),
    }
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn flush(&mut self, ctx: &mut super::BoundaryWork) -> Result<(), ChainError> {
        let stats = define_end_stats(ctx);

        ctx.deltas.add_for_entity(EpochWrapUp { stats });

        Ok(())
    }
}
