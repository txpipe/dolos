use crate::{
    AccountState, CardanoDelta, EndStats, EpochState, FixedNamespace as _, PParamsSet, PoolHash,
    PoolState, CURRENT_EPOCH_KEY,
};
use dolos_core::{ChainError, NsKey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolWrapUp {
    pool_hash: PoolHash,
}

impl PoolWrapUp {
    pub fn new(pool_hash: PoolHash) -> Self {
        Self { pool_hash }
    }
}

impl dolos_core::EntityDelta for PoolWrapUp {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.pool_hash.as_slice()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        let entity = entity.as_mut().expect("existing pool");

        let snapshot = entity.snapshot.scheduled_or_default();

        snapshot.is_retired = true;
    }

    fn undo(&self, _entity: &mut Option<PoolState>) {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochWrapUp {
    stats: EndStats,
    migration: Option<PParamsSet>,
}

impl dolos_core::EntityDelta for EpochWrapUp {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        entity.rolling.scheduled_or_default();

        if let Some(migration) = &self.migration {
            entity.pparams.schedule_unchecked(Some(migration.clone()));
        } else {
            entity.pparams.scheduled_or_default();
        }

        entity.end = Some(self.stats.clone());
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        entity.end = None;
    }
}

fn define_pparams_migration(ctx: &super::BoundaryWork) -> Option<PParamsSet> {
    let transition = ctx.ending_state().pparams.era_transition()?;

    let migrated = crate::forks::migrate_pparams_version(
        transition.prev_version.into(),
        transition.new_version.into(),
        ctx.ending_state().pparams.next().unwrap(),
        &ctx.genesis,
    );

    Some(migrated)
}

#[derive(Default)]
pub struct BoundaryVisitor {
    deltas: Vec<CardanoDelta>,
}

impl BoundaryVisitor {
    fn change(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.push(delta.into());
    }
}

fn define_new_pool_count(ctx: &super::BoundaryWork) -> usize {
    let rolling = ctx.ending_state.rolling.unwrap_live();

    // we need to know which of the registered pools are actually new pools that
    // need deposit vs re-registration of existing pools.
    let repeated_pools = ctx
        .existing_pools
        .intersection(&rolling.registered_pools)
        .count();

    rolling.registered_pools.len() - repeated_pools
}

fn define_end_stats(ctx: &super::BoundaryWork) -> EndStats {
    let pool_refund_count = ctx
        .retiring_pools
        .values()
        .filter(|(_, account)| account.is_registered())
        .count();

    let pool_invalid_refund_count = ctx.retiring_pools.len() - pool_refund_count;

    EndStats {
        pool_deposit_count: define_new_pool_count(ctx) as u64,
        pool_refund_count: pool_refund_count as u64,
        pool_invalid_refund_count: pool_invalid_refund_count as u64,
    }
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_retiring_pool(
        &mut self,
        _: &mut super::BoundaryWork,
        pool_hash: PoolHash,
        _: &PoolState,
        _: &AccountState,
    ) -> Result<(), ChainError> {
        self.change(PoolWrapUp::new(pool_hash));

        Ok(())
    }

    fn flush(&mut self, ctx: &mut super::BoundaryWork) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        let stats = define_end_stats(ctx);
        let migration = define_pparams_migration(ctx);

        ctx.deltas.add_for_entity(EpochWrapUp { stats, migration });

        Ok(())
    }
}
