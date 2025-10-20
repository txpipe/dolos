use pallas::codec::minicbor;
use pallas::codec::minicbor::{Decode, Encode};

use serde::{Deserialize, Serialize};

use crate::{floor_int, ratio, Lovelace};

pub type Ratio = num_rational::BigRational;
pub type PallasRatio = pallas::ledger::primitives::RationalNumber;

pub type Eta = Ratio;

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, Default)]
pub struct Pots {
    #[n(0)]
    pub reserves: Lovelace,

    #[n(1)]
    pub treasury: Lovelace,

    #[n(2)]
    pub utxos: Lovelace,

    #[n(3)]
    pub rewards: Lovelace,

    #[n(4)]
    pub fees: Lovelace,

    #[n(5)]
    pub pool_count: u64,

    #[n(6)]
    pub account_count: u64,

    #[n(7)]
    pub deposit_per_pool: u64,

    #[n(8)]
    pub deposit_per_account: u64,

    #[n(9)]
    pub nominal_deposits: u64,
}

impl Pots {
    pub fn obligations(&self) -> Lovelace {
        let pool_deposits = self.deposit_per_pool * self.pool_count;
        let account_deposits = self.deposit_per_account * self.account_count;

        Lovelace::from(self.nominal_deposits + pool_deposits + account_deposits)
    }

    pub fn max_supply(&self) -> Lovelace {
        self.reserves + self.treasury + self.utxos + self.rewards + self.fees + self.obligations()
    }

    pub fn circulating(&self) -> Lovelace {
        self.max_supply() - self.reserves
    }

    pub fn is_consistent(&self, expected_max_supply: Lovelace) -> bool {
        self.max_supply() == expected_max_supply
    }

    pub fn assert_consistency(&self, expected_max_supply: Lovelace) {
        assert_eq!(self.max_supply(), expected_max_supply);
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, Default)]
pub struct EpochIncentives {
    #[n(0)]
    pub total: u64,

    #[n(2)]
    pub treasury_tax: u64,

    #[n(3)]
    pub available_rewards: u64,

    #[n(4)]
    pub used_fees: u64,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, Default)]
pub struct PotDelta {
    #[n(0)]
    pub produced_utxos: Lovelace,

    #[n(1)]
    pub consumed_utxos: Lovelace,

    #[n(2)]
    pub gathered_fees: u64,

    #[n(3)]
    pub new_accounts: u64,

    #[n(4)]
    pub removed_accounts: u64,

    #[n(5)]
    pub new_pools: u64,

    #[n(6)]
    pub removed_pools: u64,

    #[n(7)]
    pub withdrawals: u64,

    #[n(8)]
    pub effective_rewards: u64,

    #[n(9)]
    pub unspendable_rewards: u64,
}

/// Calculate eta using the decentralisation parameter and the formula:
///
/// ```text
/// η(blocks, d) = {
///   1,                                    if d ≥ 0.8
///   min(1, blocks / expected_blocks),     otherwise
/// }
///
/// where:
///   blocks = total_blocks_in_epoch_by_pools
///   d = decentralization_parameter
///   expected_blocks = (1 - d) × slots_per_epoch × active_slot_coefficient
/// ```
///
/// This implements the eta calculation from the Cardano Shelley delegation
/// specification. Eta represents the ratio between actual blocks produced by
/// pools and the expected number of blocks under ideal conditions.
///
/// The number of expected blocks will be the number of slots per epoch times
/// the active slots coefficient See: Non-Updatable Parameters: https://cips.cardano.org/cips/cip9/
///
/// decentralizationParameter is the proportion of blocks that are expected to
/// be produced by stake pools instead of the OBFT (Ouroboros Byzantine Fault
/// Tolerance) nodes. It was introduced close before the Shelley era: https://github.com/input-output-hk/cardano-ledger/commit/c4f10d286faadcec9e4437411bce9c6c3b6e51c2
///
/// # Arguments
///
/// * `minted_blocks` - Total number of blocks produced by stake pools in the
///   epoch
/// * `d` - Decentralization parameter (proportion of blocks expected to be
///   produced by stake pools)
/// * `f` - Active slot coefficient for the network
/// * `epoch_length` - Expected number of slots per epoch
///
/// # Returns
///
/// The calculated eta value, capped at 1.0
///
/// # References
///
/// - [Cardano Ledger Implementation](https://github.com/input-output-hk/cardano-ledger/commit/c4f10d286faadcec9e4437411bce9c6c3b6e51c2)
/// - [Shelley Delegation Specification](https://hydra.iohk.io/build/2166577/download/1/delegation_design_spec.pdf)
///   section 5.4.3
/// - [CF Java Rewards Calculation](https://github.com/cardano-foundation/cf-java-rewards-calculation/blob/b05eddf495af6dc12d96c49718f27c34fa2042b1/calculation/src/main/java/org/cardanofoundation/rewards/calculation/TreasuryCalculation.java#L117)
pub fn calculate_eta(minted_blocks: u64, d: Ratio, f: f32, epoch_length: u64) -> Eta {
    let one = ratio!(1);

    let d_threshold = ratio!(8, 10); // 0.8

    if d >= d_threshold {
        return one;
    }

    let f =
        num_rational::Rational64::approximate_float(f).expect("invalid active slot coefficient");
    let f = ratio!(*f.numer(), *f.denom());

    let epoch_length = ratio!(epoch_length);
    let expected_blocks = f * epoch_length;

    let expected_non_obft_blocks = expected_blocks * (&one - d);

    // eta is the ratio between the number of blocks that have been produced during
    // the epoch, and the expectation value of blocks that should have been
    // produced during the epoch under ideal conditions.

    let minted_blocks = ratio!(minted_blocks);

    let eta = minted_blocks / expected_non_obft_blocks;

    // spec: η = min(1, minted/expected)
    std::cmp::min(one, eta)
}

pub fn epoch_incentives(
    reserves: u64, // current reserves at snapshot
    fee_ss: u64,   // fee snapshot ("feeSS") for the epoch being rewarded
    rho: Ratio,    // monetaryExpansion (ρ)
    tau: Ratio,    // treasuryCut (τ)
    eta: Ratio,    // from calculate_eta (already capped to ≤ 1)
) -> EpochIncentives {
    let reserves = ratio!(reserves);

    // Δr1 = floor( min(1,η) * ρ * reserves )
    let incentives_q = eta * rho * reserves;
    let delta_r1 = floor_int!(incentives_q, u64);

    // rewardPot = feeSS + Δr1
    let reward_pot = fee_ss + delta_r1;

    // Δt1 = floor( τ * rewardPot )
    let treasury_tax = floor_int!(tau * ratio!(reward_pot), u64);

    // R = rewardPot - Δt1
    let available_rewards = reward_pot - treasury_tax;

    EpochIncentives {
        total: delta_r1,   // this is Δr1 (minted from reserves)
        treasury_tax,      // Δt1 (to treasury)
        available_rewards, // R (to be distributed)
        used_fees: fee_ss,
    }
}

pub fn apply_delta(mut pots: Pots, incentives: &EpochIncentives, delta: &PotDelta) -> Pots {
    let used_rewards = delta.effective_rewards + delta.unspendable_rewards;

    let returned_rewards = incentives.available_rewards - used_rewards;

    // reserves pot
    pots.reserves -= incentives.total;
    pots.reserves += returned_rewards;

    // treasury pot
    pots.treasury += incentives.treasury_tax;
    pots.treasury += delta.unspendable_rewards;

    // fees pot
    pots.fees -= incentives.used_fees;
    pots.fees += delta.gathered_fees;

    // rewards pot
    pots.rewards += delta.effective_rewards;
    pots.rewards -= delta.withdrawals;
    pots.rewards += delta.removed_pools * pots.deposit_per_pool;

    // we don't need to return account deposit refunds to the rewards pot because
    // these refunds are returned directly as utxos in the deregistration
    // transaction.

    // utxos pot
    pots.utxos += delta.produced_utxos;
    pots.utxos -= delta.consumed_utxos;

    // pool count
    pots.pool_count += delta.new_pools;
    pots.pool_count -= delta.removed_pools;

    // account count
    pots.account_count += delta.new_accounts;
    pots.account_count -= delta.removed_accounts;

    pots
}

#[cfg(test)]
mod tests {

    use super::*;

    const MAX_SUPPLY: u64 = 45000000000000000;

    #[test]
    fn sub_performant_eta() {
        let result = calculate_eta(4298, ratio!(0), 0.05, 86400);
        let expected = ratio!(2149, 2160);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_known_transition_preview_epoch_1() {
        let pots = Pots {
            reserves: 14991000000000000,
            treasury: 9000000000000,
            fees: 437793,
            utxos: 29999998493562207,
            rewards: 0,
            pool_count: 3,
            account_count: 3,
            deposit_per_pool: 500_000_000,
            deposit_per_account: 2_000_000,
            nominal_deposits: 0,
        };

        pots.assert_consistency(MAX_SUPPLY);

        let fee_ss = 437793;
        let rho = ratio!(3, 1000);
        let tau = ratio!(20, 100);
        let eta = ratio!(1);

        let incentives = epoch_incentives(pots.reserves, fee_ss, rho, tau, eta);

        let delta = PotDelta::default();
        let pots = apply_delta(pots, &incentives, &delta);

        pots.assert_consistency(MAX_SUPPLY);

        assert_eq!(pots.reserves, 14982005400350235);
        assert_eq!(pots.treasury, 17994600087558);
        assert_eq!(pots.fees, 0);
        assert_eq!(pots.obligations(), 1506000000);
        assert_eq!(pots.utxos, 29999998493562207);
        assert_eq!(pots.rewards, 0);
    }

    #[test]
    fn test_expected_transition_preview_epoch_4_to_5() {
        let pots = Pots {
            reserves: 14964032387721723,
            treasury: 35967613128648,
            fees: 304233,
            pool_count: 3,
            account_count: 3,
            deposit_per_pool: 500_000_000,
            deposit_per_account: 2_000_000,
            utxos: 29999998492845396,
            rewards: 0,
            nominal_deposits: 0,
        };

        pots.assert_consistency(MAX_SUPPLY);

        let fee_ss = 304233;
        let rho = ratio!(3, 1000);
        let tau = ratio!(20, 100);

        let eta = calculate_eta(4298, ratio!(0), 0.05, 86400);

        let incentives = epoch_incentives(pots.reserves, fee_ss, rho, tau, eta);

        let delta = PotDelta {
            unspendable_rewards: 295063003292,
            ..Default::default()
        };

        let pots = apply_delta(pots, &incentives, &delta);

        pots.assert_consistency(MAX_SUPPLY);

        assert_eq!(pots.reserves, 14954804628961481);
        assert_eq!(pots.treasury, 45195372193123);
        assert_eq!(pots.fees, 0);
        assert_eq!(pots.obligations(), 1506000000);
        assert_eq!(pots.utxos, 29999998492845396);
        assert_eq!(pots.rewards, 0);
    }

    // TODO: add property based testing that ensures that the pots are
    // consistent
}
