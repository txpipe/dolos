use std::cmp::min;

use crate::{floor_int, ratio};

pub type Ratio = num_rational::BigRational;

/// Calculate the maximal (aka: optimal) rewards for a pool.
pub fn pool_maximal_rewards(
    r: Ratio,
    nopt: u32,
    a0: Ratio,
    relative_stake: Ratio,
    relative_pledge: Ratio,
) -> u64 {
    let z0 = ratio!(1, nopt);
    let s = min(relative_stake, z0.clone());
    let p = min(relative_pledge, z0.clone());

    // (z0 - σ') / z0
    let op = (&z0 - &s) / &z0;

    // ρ' * ((z0 - σ') / z0)
    let op = &p * &op;

    // σ' - ρ' * ((z0 - σ') / z0)
    let op = &s - &op;

    // (σ' - ρ' * ((z0 - σ') / z0)) / z0
    let op = op / z0;

    // ρ' * a0 * ((σ' - ρ' * ((z0 - σ') / z0)) / z0)
    let op = p * &a0 * op;

    // σ' + ρ' * a0 * ((σ' - ρ' * ((z0 - σ') / z0)) / z0)
    let op = s + op;

    // R / (1 + a0) * (σ' + ρ' * a0 * ((σ' - ρ' * ((z0 - σ') / z0)) / z0))
    let op = r / (ratio!(1) + &a0) * op;

    floor_int!(op, u64)
}

pub fn delegator_reward(available_rewards: u64, total_delegated: u64, delegator_stake: u64) -> u64 {
    let weight = ratio!(delegator_stake, total_delegated);

    let share = weight * ratio!(available_rewards);

    floor_int!(share, u64)
}

/// Calculates a pool apparent performance
pub fn pool_apparent_performance(
    d: Ratio,                // Unit interval [0,1]; if d >= 0.8 => p̄ = 1
    pool_blocks: u64,        // n
    epoch_blocks: u64,       // blocksTotal (sum over pools)
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

#[allow(clippy::too_many_arguments)]
/// Calculate the total rewards of the pool.
pub fn pool_rewards(
    epoch_rewards: u64,
    circulating_supply: u64,
    active_stake: u64, // E-2 active stake (σ_a denominator)
    pool_stake: u64,   // E-2 pool stake
    declared_pledge: u64,
    live_pledge: u64,
    nopt: u32,
    a0: Ratio,
    d: Ratio,
    pool_blocks: u64,
    epoch_blocks: u64,
) -> u64 {
    if live_pledge < declared_pledge {
        return 0;
    }

    let relative_stake = ratio!(pool_stake, circulating_supply);

    let relative_pledge = ratio!(declared_pledge, circulating_supply);

    let optimal = pool_maximal_rewards(
        ratio!(epoch_rewards),
        nopt,
        a0,
        relative_stake,
        relative_pledge,
    );

    let pbar = pool_apparent_performance(d, pool_blocks, epoch_blocks, pool_stake, active_stake);

    let out = ratio!(optimal) * &pbar;

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
    let theta = ratio!(pool_stake, circulating_supply);

    // s/σ — ratio of owner's pledge to pool stake (denominator cancels, so we can
    // use amounts)
    let s_over_sigma = s / theta;

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

    #[test]
    fn test_maximal_rewards() {
        let r: u64 = 35730783712305;
        let pool_stake: u64 = 100000000000000;
        let declared_pledge: u64 = 100000000000000;
        let circulating_supply: u64 = 30035967612278277;
        let nopt = 150;
        let a0 = ratio!(3, 10);

        let relative_stake = ratio!(pool_stake, circulating_supply);

        let relative_pledge = ratio!(declared_pledge, circulating_supply);

        let result = pool_maximal_rewards(ratio!(r), nopt, a0, relative_stake, relative_pledge);

        let expected = 98354332965;

        assert_eq!(result, expected);
    }

    #[test]
    fn test_well_known_apparent_performance() {
        let d = ratio!(0);
        let pool_blocks = 1514;
        let epoch_blocks = 4298;
        let pool_stake = 100000000000000;
        let total_active_stake = 300000000000000;

        let result =
            pool_apparent_performance(d, pool_blocks, epoch_blocks, pool_stake, total_active_stake);

        let expected = ratio!(2271, 2149);

        assert_eq!(result, expected);
    }
}
