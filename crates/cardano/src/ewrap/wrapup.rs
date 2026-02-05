use crate::{
    AccountState, CardanoDelta, EndStats, EpochState, FixedNamespace as _, PoolHash, PoolState,
    CURRENT_EPOCH_KEY,
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
        // todo!()
        // Placeholder undo logic. Ensure this does not panic.
    }
}

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

        entity.rolling.scheduled_or_default();
        entity.pparams.scheduled_or_default();
        entity.end = Some(self.stats.clone());
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        entity.end = None;
    }
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

fn define_proposal_total_refunds(ctx: &super::BoundaryWork) -> u64 {
    let enacting_sum: u64 = ctx
        .enacting_proposals
        .values()
        .map(|(p, _)| p.deposit.unwrap_or_default())
        .sum();

    let dropping_sum: u64 = ctx
        .dropping_proposals
        .values()
        .map(|(p, _)| p.deposit.unwrap_or_default())
        .sum();

    enacting_sum + dropping_sum
}

fn define_proposal_valid_refunds(ctx: &super::BoundaryWork) -> u64 {
    let enacting_sum: u64 = ctx
        .enacting_proposals
        .values()
        .filter(|(_, a)| a.as_ref().is_some_and(|a| a.is_registered()))
        .map(|(p, _)| p.deposit.unwrap_or_default())
        .sum();

    let dropping_sum: u64 = ctx
        .dropping_proposals
        .values()
        .filter(|(_, a)| a.as_ref().is_some_and(|a| a.is_registered()))
        .map(|(p, _)| p.deposit.unwrap_or_default())
        .sum();

    enacting_sum + dropping_sum
}

fn define_end_stats(ctx: &super::BoundaryWork) -> EndStats {
    let pool_refund_count = ctx
        .retiring_pools
        .values()
        .filter(|(_, a)| a.as_ref().is_some_and(|a| a.is_registered()))
        .count();

    let pool_invalid_refund_count = ctx.retiring_pools.len() - pool_refund_count;

    let proposal_total_refunds = define_proposal_total_refunds(ctx);
    let proposal_valid_refunds = define_proposal_valid_refunds(ctx);
    let proposal_invalid_refunds = proposal_total_refunds - proposal_valid_refunds;

    let incentives = ctx.rewards.incentives();
    let treasury_mirs = ctx
        .ending_state()
        .rolling
        .unwrap_live()
        .treasury_mirs;

    EndStats {
        pool_deposit_count: ctx.new_pools.len() as u64,
        pool_refund_count: pool_refund_count as u64,
        pool_invalid_refund_count: pool_invalid_refund_count as u64,
        epoch_incentives: incentives.clone(),
        effective_rewards: ctx.rewards.applied_effective(),
        unspendable_to_treasury: ctx.rewards.applied_unspendable_to_treasury(),
        unspendable_to_reserves: ctx.rewards.applied_unspendable_to_reserves(),
        treasury_mirs,
        proposal_refunds: proposal_valid_refunds,
        proposal_invalid_refunds,
        // TODO: deprecate
        __drep_deposits: 0,
        __drep_refunds: 0,
    }
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_retiring_pool(
        &mut self,
        _: &mut super::BoundaryWork,
        pool_hash: PoolHash,
        _: &PoolState,
        _: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        self.change(PoolWrapUp::new(pool_hash));

        Ok(())
    }

    fn flush(&mut self, ctx: &mut super::BoundaryWork) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        let stats = define_end_stats(ctx);

        ctx.deltas.add_for_entity(EpochWrapUp { stats });

        Ok(())
    }
}
