use pallas::codec::minicbor;
use pallas::codec::minicbor::{Decode, Encode};

use serde::{Deserialize, Serialize};

use crate::{floor_int, ratio, sub, Lovelace};

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

    #[n(10)]
    pub drep_deposits: Lovelace,

    #[n(11)]
    pub proposal_deposits: Lovelace,
}

impl Pots {
    pub fn stake_deposits(&self) -> Lovelace {
        let pool_deposits = self.deposit_per_pool * self.pool_count;
        let account_deposits = self.deposit_per_account * self.account_count;

        pool_deposits + account_deposits
    }

    pub fn obligations(&self) -> Lovelace {
        self.nominal_deposits + self.stake_deposits() + self.drep_deposits + self.proposal_deposits
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

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct PotDelta {
    #[n(0)]
    pub produced_utxos: Lovelace,

    #[n(1)]
    pub consumed_utxos: Lovelace,

    #[n(2)]
    pub gathered_fees: Lovelace,

    #[n(3)]
    pub new_accounts: u64,

    #[n(4)]
    pub removed_accounts: u64,

    #[n(5)]
    pub pool_deposit_count: u64,

    #[n(6)]
    pub pool_refund_count: u64,

    #[n(7)]
    pub pool_invalid_refund_count: u64,

    #[n(8)]
    pub withdrawals: Lovelace,

    #[n(9)]
    pub effective_rewards: Lovelace,

    #[n(10)]
    pub unspendable_rewards: Lovelace,

    #[n(11)]
    pub drep_deposits: Lovelace,

    #[n(12)]
    pub proposal_deposits: Lovelace,

    #[n(13)]
    pub drep_refunds: Lovelace,

    #[n(14)]
    pub proposal_refunds: Lovelace,

    #[n(15)]
    #[cbor(default)]
    pub treasury_donations: Lovelace,

    #[n(16)]
    #[cbor(default)]
    pub proposal_invalid_refunds: Lovelace,

    #[n(17)]
    pub deposit_per_account: Option<Lovelace>,

    #[n(18)]
    pub deposit_per_pool: Option<Lovelace>,

    #[n(19)]
    pub protocol_version: u16,

    #[n(20)]
    #[cbor(default)]
    pub reserve_mirs: Lovelace,
}

impl PotDelta {
    pub fn neutral(protocol_version: u16) -> Self {
        Self {
            protocol_version,
            produced_utxos: 0,
            consumed_utxos: 0,
            gathered_fees: 0,
            new_accounts: 0,
            removed_accounts: 0,
            pool_deposit_count: 0,
            pool_refund_count: 0,
            pool_invalid_refund_count: 0,
            withdrawals: 0,
            effective_rewards: 0,
            unspendable_rewards: 0,
            drep_deposits: 0,
            proposal_deposits: 0,
            drep_refunds: 0,
            proposal_refunds: 0,
            treasury_donations: 0,
            reserve_mirs: 0,
            proposal_invalid_refunds: 0,
            deposit_per_account: None,
            deposit_per_pool: None,
        }
    }

    pub fn consumed_incentives(&self) -> Lovelace {
        if self.protocol_version < 7 {
            return self.effective_rewards;
        }

        self.effective_rewards + self.unspendable_rewards
    }

    pub fn incentives_back_to_treasury(&self) -> Lovelace {
        if self.protocol_version < 7 {
            return 0;
        }

        self.unspendable_rewards
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

pub fn apply_byron_delta(mut pots: Pots, _: &EpochIncentives, delta: &PotDelta) -> Pots {
    // force neutral values for concepts that doesn't apply to byron
    pots.deposit_per_pool = 0;
    pots.deposit_per_account = 0;
    pots.treasury = 0;
    pots.fees = 0;
    pots.rewards = 0;
    pots.pool_count = 0;
    pots.account_count = 0;
    pots.drep_deposits = 0;
    pots.proposal_deposits = 0;

    // utxos pot
    pots.utxos += delta.produced_utxos;
    pots.utxos -= delta.consumed_utxos;

    // we infer the reserves by looking at how the utxo pot changed
    pots.reserves -= delta.produced_utxos;
    pots.reserves += delta.consumed_utxos;

    pots
}

pub fn apply_shelley_delta(mut pots: Pots, incentives: &EpochIncentives, delta: &PotDelta) -> Pots {
    let used_rewards = delta.consumed_incentives();

    let returned_rewards = sub!(incentives.available_rewards, used_rewards);

    // update params
    if let Some(new) = delta.deposit_per_pool {
        pots.deposit_per_pool = new;
    }

    if let Some(new) = delta.deposit_per_account {
        pots.deposit_per_account = new;
    }

    // reserves pot
    pots.reserves -= incentives.total;
    pots.reserves -= delta.reserve_mirs;
    pots.reserves += returned_rewards;

    // treasury pot
    pots.treasury += incentives.treasury_tax;
    pots.treasury += delta.incentives_back_to_treasury();
    pots.treasury += delta.pool_invalid_refund_count * pots.deposit_per_pool;
    pots.treasury += delta.proposal_invalid_refunds;
    pots.treasury += delta.treasury_donations;

    // fees pot
    pots.fees -= incentives.used_fees;
    pots.fees += delta.gathered_fees;

    // rewards pot
    pots.rewards += delta.effective_rewards;
    pots.rewards -= delta.withdrawals;
    pots.rewards += delta.pool_refund_count * pots.deposit_per_pool;
    pots.rewards += delta.proposal_refunds;
    pots.rewards += delta.reserve_mirs;

    // we don't need to return account deposit refunds to the rewards pot because
    // these refunds are returned directly as utxos in the deregistration
    // transaction.

    // utxos pot
    pots.utxos += delta.produced_utxos;
    pots.utxos -= delta.consumed_utxos;

    // pool count
    pots.pool_count += delta.pool_deposit_count;
    pots.pool_count -= delta.pool_refund_count;
    pots.pool_count -= delta.pool_invalid_refund_count;

    // account count
    pots.account_count += delta.new_accounts;
    pots.account_count -= delta.removed_accounts;

    // for governance, since each cert contains the specific deposit amount, we deal directly with lovelace values.

    pots.drep_deposits += delta.drep_deposits;
    pots.drep_deposits -= delta.drep_refunds;

    pots.proposal_deposits += delta.proposal_deposits;
    pots.proposal_deposits -= delta.proposal_refunds;
    pots.proposal_deposits -= delta.proposal_invalid_refunds;

    pots
}

pub fn apply_delta(pots: Pots, incentives: &EpochIncentives, delta: &PotDelta) -> Pots {
    match delta.protocol_version {
        0 | 1 => apply_byron_delta(pots, incentives, delta),
        2.. => apply_shelley_delta(pots, incentives, delta),
    }
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
    fn test_mainnet_byron_delta() {
        let pots = Pots {
            reserves: 13887515255000000,
            treasury: 0,
            fees: 0,
            utxos: 31112484745000000,
            rewards: 0,
            pool_count: 0,
            account_count: 0,
            deposit_per_pool: 0,
            deposit_per_account: 0,
            nominal_deposits: 0,
            drep_deposits: 0,
            proposal_deposits: 0,
        };

        assert!(pots.is_consistent(MAX_SUPPLY));

        let delta = PotDelta {
            consumed_utxos: 3458053,
            gathered_fees: 5612092,
            ..PotDelta::neutral(0)
        };

        let incentives = EpochIncentives {
            total: delta.gathered_fees,
            treasury_tax: 0,
            available_rewards: 0,
            used_fees: 0,
        };

        let pots = apply_delta(pots, &incentives, &delta);

        dbg!(&pots);

        assert!(pots.is_consistent(MAX_SUPPLY));

        assert_eq!(pots.reserves, 13887515258458053);
        assert_eq!(pots.treasury, 0);
        assert_eq!(pots.fees, 0);
        assert_eq!(pots.utxos, 31112484741541947);
        assert_eq!(pots.rewards, 0);
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
            drep_deposits: 0,
            proposal_deposits: 0,
        };

        assert!(pots.is_consistent(MAX_SUPPLY));

        let fee_ss = 437793;
        let rho = ratio!(3, 1000);
        let tau = ratio!(20, 100);
        let eta = ratio!(1);

        let incentives = epoch_incentives(pots.reserves, fee_ss, rho, tau, eta);

        let delta = PotDelta {
            deposit_per_pool: Some(500_000_000),
            deposit_per_account: Some(2_000_000),
            ..PotDelta::neutral(6)
        };

        let pots = apply_delta(pots, &incentives, &delta);

        assert!(pots.is_consistent(MAX_SUPPLY));

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
            drep_deposits: 0,
            proposal_deposits: 0,
        };

        assert!(pots.is_consistent(MAX_SUPPLY));

        let fee_ss = 304233;
        let rho = ratio!(3, 1000);
        let tau = ratio!(20, 100);

        let eta = calculate_eta(4298, ratio!(0), 0.05, 86400);

        let incentives = epoch_incentives(pots.reserves, fee_ss, rho, tau, eta);

        let delta = PotDelta {
            unspendable_rewards: 295063003292,
            deposit_per_pool: Some(500_000_000),
            deposit_per_account: Some(2_000_000),
            ..PotDelta::neutral(7)
        };

        let pots = apply_delta(pots, &incentives, &delta);

        assert!(pots.is_consistent(MAX_SUPPLY));

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
