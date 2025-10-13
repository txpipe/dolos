use pallas::codec::minicbor;
use pallas::codec::minicbor::{Decode, Encode};

use serde::{Deserialize, Serialize};

use crate::{floor_int, ratio};

pub type Ratio = num_rational::BigRational;
pub type PallasRatio = pallas::ledger::primitives::RationalNumber;

pub type Eta = Ratio;

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct Pots {
    #[n(0)]
    pub reserves: u64,

    #[n(1)]
    pub treasury: u64,

    #[n(2)]
    pub fees: u64,

    #[n(3)]
    pub deposits: u64,

    #[n(4)]
    pub utxos: u64,

    #[n(5)]
    pub rewards: u64,
}

impl Pots {
    pub fn max_supply(&self) -> u64 {
        self.reserves + self.treasury + self.fees + self.deposits + self.utxos + self.rewards
    }

    pub fn circulating(&self) -> u64 {
        self.max_supply() - self.reserves
    }

    pub fn check_consistency(&self, expected_max_supply: u64) {
        debug_assert_eq!(self.max_supply(), expected_max_supply);
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct PotDelta {
    #[n(0)]
    pub incentives: u64,

    #[n(2)]
    pub treasury_tax: u64,

    #[n(3)]
    pub available_rewards: u64,

    #[n(4)]
    pub used_fees: u64,

    #[n(5)]
    pub effective_rewards: Option<u64>,

    #[n(6)]
    pub unspendable_rewards: Option<u64>,
}

impl PotDelta {
    pub fn check_consistency(self) {
        let effective_rewards = self.effective_rewards.unwrap_or(0);
        let unspendable_rewards = self.unspendable_rewards.unwrap_or(0);
        let total_rewards = effective_rewards + unspendable_rewards;

        assert!(self.available_rewards >= total_rewards);
        assert_eq!(self.incentives, self.available_rewards + self.treasury_tax);
    }

    pub fn with_rewards(self, effective_rewards: u64, unspendable_rewards: u64) -> Self {
        Self {
            effective_rewards: Some(effective_rewards),
            unspendable_rewards: Some(unspendable_rewards),
            ..self
        }
    }
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

pub fn delta(
    reserves: u64, // current reserves at snapshot
    fee_ss: u64,   // fee snapshot ("feeSS") for the epoch being rewarded
    rho: Ratio,    // monetaryExpansion (ρ)
    tau: Ratio,    // treasuryCut (τ)
    eta: Ratio,    // from calculate_eta (already capped to ≤ 1)
) -> PotDelta {
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

    PotDelta {
        incentives: delta_r1, // this is Δr1 (minted from reserves)
        treasury_tax,         // Δt1 (to treasury)
        available_rewards,    // R (to be distributed)
        used_fees: fee_ss,
        effective_rewards: None,
        unspendable_rewards: None,
    }
}

pub fn apply_delta(mut pots: Pots, delta: &PotDelta) -> Pots {
    let effective_rewards = delta.effective_rewards.expect("reward data is missing");
    let unspendable_rewards = delta.unspendable_rewards.expect("reward data is missing");

    let used_rewards = effective_rewards + unspendable_rewards;

    let returned_rewards = delta.available_rewards - used_rewards;

    // reserves pot
    pots.reserves -= delta.incentives;
    pots.reserves += returned_rewards;

    // treasury pot
    pots.treasury += delta.treasury_tax;
    pots.treasury += unspendable_rewards;

    // fees pot
    pots.fees -= delta.used_fees;

    // rewards pot
    pots.rewards += effective_rewards;

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
            deposits: 1506000000,
            utxos: 29999998493562207,
            rewards: 0,
        };

        pots.check_consistency(MAX_SUPPLY);

        let fee_ss = 437793;
        let rho = ratio!(3, 1000);
        let tau = ratio!(20, 100);
        let eta = ratio!(1);

        let pot_delta = delta(pots.reserves, fee_ss, rho, tau, eta).with_rewards(0, 0);

        let pots = apply_delta(pots, &pot_delta);

        pots.check_consistency(MAX_SUPPLY);

        assert_eq!(pots.reserves, 14982005400350235);
        assert_eq!(pots.treasury, 17994600087558);
        assert_eq!(pots.fees, 0);
        assert_eq!(pots.deposits, 1506000000);
        assert_eq!(pots.utxos, 29999998493562207);
        assert_eq!(pots.rewards, 0);
    }

    #[test]
    fn test_expected_transition_preview_epoch_4_to_5() {
        let pots = Pots {
            reserves: 14964032387721723,
            treasury: 35967613128648,
            fees: 304233,
            deposits: 1506000000,
            utxos: 29999998492845396,
            rewards: 0,
        };

        pots.check_consistency(MAX_SUPPLY);

        let fee_ss = 304233;
        let rho = ratio!(3, 1000);
        let tau = ratio!(20, 100);

        let eta = calculate_eta(4298, ratio!(0), 0.05, 86400);

        let pot_delta = delta(pots.reserves, fee_ss, rho, tau, eta).with_rewards(0, 295063003292);

        let pots = apply_delta(pots, &pot_delta);

        pots.check_consistency(MAX_SUPPLY);

        dbg!(pots.reserves - 14954804628961481);

        assert_eq!(pots.reserves, 14954804628961481);
        assert_eq!(pots.treasury, 45195372193123);
        assert_eq!(pots.fees, 0);
        assert_eq!(pots.deposits, 1506000000);
        assert_eq!(pots.utxos, 29999998492845396);
        assert_eq!(pots.rewards, 0);
    }

    // TODO: add property based testing that ensures that the pots are
    // consistent
}
