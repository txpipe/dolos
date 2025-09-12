use dolos_core::ChainError;
use pallas::ledger::primitives::RationalNumber;

use crate::{AccountState, EpochState, PoolState};

pub struct Pots {
    pub to_treasury: u64,
    pub to_distribute: u64,
}

pub fn compute_pure(
    previous_reserves: u64,
    gathered_fees: u64,
    decayed_deposits: u64,
    rho: RationalNumber,
    tau: RationalNumber,
) -> Pots {
    let rho = rho.numerator as f64 / rho.denominator as f64;
    let from_reserves = rho * (previous_reserves as f64);

    let reward_pot_f64 = (from_reserves.round() as u64 + gathered_fees + decayed_deposits) as f64;

    let tau = tau.numerator as f64 / tau.denominator as f64;
    let to_treasury_f64 = tau * reward_pot_f64;
    let to_distribute_f64 = (1.0 - tau) * reward_pot_f64;

    let to_treasury = to_treasury_f64.round() as u64;
    let to_distribute = to_distribute_f64.round() as u64;

    Pots {
        to_treasury,
        to_distribute,
    }
}

pub fn compute_for_epoch(epoch: &EpochState) -> Result<Pots, ChainError> {
    let rho = epoch.pparams.rho().ok_or(ChainError::PParamsNotFound)?;

    let tau = epoch.pparams.tau().ok_or(ChainError::PParamsNotFound)?;

    let pots = compute_pure(
        epoch.reserves,
        epoch.gathered_fees,
        epoch.decayed_deposits,
        rho,
        tau,
    );

    Ok(pots)
}

pub type TotalPoolReward = u64;
pub type OperatorShare = u64;

pub fn compute_pool_reward(
    total_rewards: u64,
    total_active_stake: u64,
    pool: &PoolState,
    k: u32,
    a0: RationalNumber,
) -> (TotalPoolReward, OperatorShare) {
    let z0 = 1.0 / k as f64;
    let sigma = pool.active_stake as f64 / total_active_stake as f64;
    let s = pool.declared_pledge as f64 / total_active_stake as f64;
    let sigma_prime = sigma.min(z0);

    let r = total_rewards as f64;
    let a0 = a0.numerator as f64 / a0.denominator as f64;
    let r_pool = r * (sigma_prime + s.min(sigma) * a0 * (sigma_prime - sigma));

    let r_pool_u64 = r_pool.round() as u64;
    let after_cost = r_pool_u64.saturating_sub(pool.fixed_cost);
    let pool_margin_cost = pool.margin_cost.numerator as f64 / pool.margin_cost.denominator as f64;
    let operator_share = pool.fixed_cost + ((after_cost as f64) * pool_margin_cost).round() as u64;

    (r_pool_u64, operator_share)
}

pub fn compute_delegator_reward(
    remaining: u64,
    total_delegated: u64,
    delegator: &AccountState,
) -> u64 {
    let share = (delegator.active_stake as f64 / total_delegated as f64) * remaining as f64;
    share.round() as u64
}
