use std::cmp::min;

use crate::{floor_int, ratio};

pub type Ratio = num_rational::BigRational;

/// Calculate the optimal rewards of the pool.
///
/// These are the theoretical rewards that a pool would receive if it has
/// perfect apparent performance.
///
/// # Arguments
///
/// * `epoch_rewards` - The total rewards for the epoch.
/// * `optimal_pool_count` - The optimal pool count.
/// * `influence` - The influence of the pool.
/// * `relative_stake_of_pool` - The relative stake of the pool.
/// * `relative_stake_of_pool_owner` - The relative stake of the pool owner.
///
/// # Returns
///
/// The optimal rewards of the pool.
fn optimal_pool_rewards(
    epoch_rewards: u64,
    optimal_pool_count: u32,
    influence: Ratio,
    relative_stake_of_pool: Ratio,
    relative_stake_of_pool_owner: Ratio,
) -> i128 {
    let epoch_rewards = ratio!(epoch_rewards);
    let size_of_saturated_pool = ratio!(1, optimal_pool_count);

    let capped_relative_stake = min(relative_stake_of_pool, size_of_saturated_pool.clone());
    let capped_relative_stake_of_pool_owner =
        min(relative_stake_of_pool_owner, size_of_saturated_pool.clone());

    // R / (1 + a0)
    let one = ratio!(1);
    let one_plus_influence = &one + &influence;
    let rewards_divided_by_one_plus_influence = epoch_rewards / one_plus_influence;

    // (z0 - sigma') / z0
    let size_of_saturated_minus_capped_relative_state =
        size_of_saturated_pool.clone() - capped_relative_stake.clone();
    let relative_stake_of_saturated_pool =
        size_of_saturated_minus_capped_relative_state / size_of_saturated_pool.clone();

    // (sigma' - s' * relativeStakeOfSaturatedPool) / z0
    let numer = capped_relative_stake.clone()
        - (capped_relative_stake_of_pool_owner.clone() * relative_stake_of_saturated_pool);
    let denom = size_of_saturated_pool;
    let saturated_pool_weight = numer / denom;

    // R / (1+a0) * (sigma' + s' * a0 * saturatedPoolWeight)
    let mult1 = rewards_divided_by_one_plus_influence;
    let mult2 = capped_relative_stake
        + (capped_relative_stake_of_pool_owner * influence * saturated_pool_weight);
    let out = mult1 * mult2;

    floor_int!(out, i128)
}

pub type TotalPoolReward = u64;

pub type OperatorShare = u64;

pub fn delegator_reward(available_rewards: u64, total_delegated: u64, delegator_stake: u64) -> u64 {
    let share = (delegator_stake as f64 / total_delegated as f64) * available_rewards as f64;
    share.round() as u64
}

/// Calculates a pool apparent performance
fn pool_apparent_performance(
    d: Ratio,                // Unit interval [0,1]; if d >= 0.8 => p̄ = 1
    pool_blocks: u32,        // n
    epoch_blocks: u32,       // blocksTotal (sum over pools)
    pool_stake: u64,         // E-2 pool stake
    total_active_stake: u64, // E-2 ACTIVE stake (σ_a denominator)
) -> Ratio {
    if total_active_stake == 0 {
        return ratio!(0);
    }

    let sigma_a = ratio!(pool_stake, total_active_stake);

    if sigma_a == ratio!(0) {
        return ratio!(0);
    }

    // β = n / max(1, N_total)
    let beta = ratio!(pool_blocks, std::cmp::max(epoch_blocks, 1));

    // if d < 0.8 then β/σ_a else 1
    let eight_tenths = ratio!(4, 5);

    if d < eight_tenths {
        beta / sigma_a
    } else {
        ratio!(1)
    }
}

/// Calculate the total rewards of the pool.
pub fn pool_rewards(
    epoch_rewards: u64,
    circulating_supply: u64,
    _total_stake: u64, // <-- E-2 total registered stake (NOT circulating)
    active_stake: u64, // E-2 active stake (σ_a denominator)
    pool_stake: u64,   // E-2 pool stake
    declared_pledge: u64,
    live_pledge: u64,
    k: u32,
    a0: Ratio,
    d: Ratio,
    pool_blocks: u32,
    epoch_blocks: u32,
) -> u64 {
    if live_pledge < declared_pledge {
        return 0;
    }

    assert!(k > 0, "k must be > 0");

    // σ (relative to total stake)
    // TODO: understand why we need to use circulating supply here instead of total
    // stake as specified in the spec
    let σ = ratio!(pool_stake, circulating_supply);

    // s (relative to total stake)
    // TODO: understand why we need to use circulating supply here instead of total
    // stake as specified in the spec
    let s = ratio!(declared_pledge, circulating_supply);

    let optimal = optimal_pool_rewards(epoch_rewards, k, a0, σ, s);

    let pbar = pool_apparent_performance(d, pool_blocks, epoch_blocks, pool_stake, active_stake);

    let out = ratio!(optimal) * pbar;

    floor_int!(out, u64)
}

/// Calculate the operator share of the pool rewards.
///
/// # Arguments
///
/// * `pool_rewards` - The total rewards of the pool.
/// * `fixed_cost` - The fixed cost of the pool.
/// * `margin_cost` - The margin cost of the pool.
/// * `pool_stake` - The stake of the pool.
/// * `live_pledge` - The live pledge of the pool.
/// * `circulating_supply` - The circulating supply of the pool.
///
/// # Returns
///
/// The operator share of the pool rewards.
pub fn pool_operator_share(
    pool_rewards: u64,
    fixed_cost: u64,
    margin_cost: Ratio,
    pool_stake: u64,
    live_pledge: u64,
    circulating_supply: u64,
) -> u64 {
    let c = fixed_cost;

    if pool_rewards <= c {
        return pool_rewards; // operator takes it all if rewards ≤ fixed cost
    }

    let after_cost = pool_rewards - c;

    let s = ratio!(live_pledge, circulating_supply);
    let σ = ratio!(pool_stake, circulating_supply);

    // s/σ — ratio of owner's pledge to pool stake (denominator cancels, so we can
    // use amounts)
    let s_over_sigma = s / σ;

    let one = ratio!(1);

    let m = margin_cost;

    // c + (f̂ − c) · ( m + (1 − m) · s/σ )
    let term = &m + (one - &m) * s_over_sigma;

    let after_cost = ratio!(after_cost);

    let variable = after_cost * term;

    let variable = floor_int!(variable, u64);

    c + variable
}

#[cfg(test)]
mod tests {
    use super::*;

    const POTENTIAL_RESERVES: [u64; 13] = [
        13262280841681299,
        13247093198353459,
        13230232787944838,
        13212986170770203,
        13195031638588164,
        13176528835451373,
        13157936081322000,
        13139088245733216,
        13120582265809833,
        13101550250680254,
        13082116350342059,
        13062655956639744,
        13042967905529920,
    ];

    #[test]
    #[ignore]
    fn test_pool_operator_share() {
        let pool_rewards = 545898007;
        let fixed_cost = 500000000;
        let margin_cost = ratio!(392699, 12500000);
        let pool_stake = 1019497437409;
        let live_pledge = 19497437409;

        for potential_reserve in POTENTIAL_RESERVES {
            let circulating_supply = 45_000_000_000_000_000 - potential_reserve;

            let operator_share = pool_operator_share(
                pool_rewards,
                fixed_cost,
                margin_cost.clone(),
                pool_stake,
                live_pledge,
                circulating_supply,
            );

            if operator_share == 501441928 {
                println!("match");
                return;
            }
        }

        panic!("no match");
    }
}
