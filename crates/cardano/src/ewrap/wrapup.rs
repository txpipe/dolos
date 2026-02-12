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

    // Use effective MIR amounts (only applied to registered accounts)
    // instead of total from rolling stats (which includes MIRs to unregistered accounts)
    let treasury_mirs = ctx.effective_treasury_mirs;
    let reserve_mirs = ctx.effective_reserve_mirs;
    let invalid_treasury_mirs = ctx.invalid_treasury_mirs;
    let invalid_reserve_mirs = ctx.invalid_reserve_mirs;

    // Log comparison with rolling stats for debugging
    let rolling_treasury_mirs = ctx
        .ending_state()
        .rolling
        .unwrap_live()
        .treasury_mirs;
    if treasury_mirs != rolling_treasury_mirs {
        tracing::info!(
            epoch = ctx.ending_state().number,
            %treasury_mirs,
            %rolling_treasury_mirs,
            %invalid_treasury_mirs,
            "treasury MIRs: effective != rolling (MIRs to unregistered accounts)"
        );
    }

    let effective = ctx.rewards.applied_effective();
    let to_treasury = ctx.rewards.applied_unspendable_to_treasury();
    let to_reserves = ctx.rewards.applied_unspendable_to_reserves();

    // Sum of applied_rewards should match effective
    let applied_rewards_sum: u64 = ctx.applied_rewards.iter().map(|r| r.amount).sum();

    assert!(
        effective == applied_rewards_sum,
        "EWRAP epoch {}: effective_rewards ({}) != applied_rewards_sum ({}), diff = {}",
        ctx.ending_state().number,
        effective,
        applied_rewards_sum,
        effective as i64 - applied_rewards_sum as i64
    );

    tracing::info!(
        epoch = ctx.ending_state().number,
        available_rewards = %incentives.available_rewards,
        %effective,
        %to_treasury,
        %to_reserves,
        consumed = %(effective + to_treasury),
        returned = %(incentives.available_rewards.saturating_sub(effective + to_treasury)),
        %applied_rewards_sum,
        applied_rewards_count = ctx.applied_rewards.len(),
        "EWRAP reward classification"
    );

    EndStats {
        pool_deposit_count: ctx.new_pools.len() as u64,
        pool_refund_count: pool_refund_count as u64,
        pool_invalid_refund_count: pool_invalid_refund_count as u64,
        epoch_incentives: incentives.clone(),
        effective_rewards: effective,
        unspendable_to_treasury: to_treasury,
        unspendable_to_reserves: to_reserves,
        treasury_mirs,
        reserve_mirs,
        invalid_treasury_mirs,
        invalid_reserve_mirs,
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
