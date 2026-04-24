use crate::{AccountState, CardanoDelta, EndStats, EpochEndInit, PoolHash, PoolState, PoolWrapUp};
use dolos_core::ChainError;

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
    let rolling_treasury_mirs = ctx.ending_state().rolling.unwrap_live().treasury_mirs;
    if treasury_mirs != rolling_treasury_mirs {
        tracing::info!(
            epoch = ctx.ending_state().number,
            %treasury_mirs,
            %rolling_treasury_mirs,
            %invalid_treasury_mirs,
            "treasury MIRs: effective != rolling (MIRs to unregistered accounts)"
        );
    }

    // Reward accumulators start at zero — shards add to them via
    // `EpochEndAccumulate` before `EwrapFinalize` emits `EpochWrapUp`.
    tracing::debug!(
        epoch = ctx.ending_state().number,
        available_rewards = %incentives.available_rewards,
        "EWRAP prepare: seeding EndStats with zero reward accumulators"
    );

    EndStats {
        pool_deposit_count: ctx.new_pools.len() as u64,
        pool_refund_count: pool_refund_count as u64,
        pool_invalid_refund_count: pool_invalid_refund_count as u64,
        epoch_incentives: incentives.clone(),
        effective_rewards: 0,
        unspendable_to_treasury: 0,
        unspendable_to_reserves: 0,
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

        // Emit the prepare-time EndStats seed. The reward accumulators are
        // zero here; shards will add their contributions via
        // `EpochEndAccumulate`, and `EwrapFinalize` will emit `EpochWrapUp`
        // against the accumulated `end`.
        let stats = define_end_stats(ctx);

        ctx.deltas.add_for_entity(EpochEndInit::new(stats));

        Ok(())
    }
}
