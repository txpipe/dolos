use dolos_core::{BrokenInvariant, ChainError};
use pallas::ledger::primitives::RationalNumber;

use crate::{
    sweep::{BoundaryWork, EraTransition, PoolData, PotDelta, Pots},
    utils::epoch_first_slot,
    AccountState, EpochState, Nonces, PParamsSet,
};

macro_rules! as_ratio {
    ($x:expr) => {{
        let numerator = $x.numerator as i64;
        let denominator = $x.denominator as i64;
        num_rational::Rational64::new(numerator, denominator).reduced()
    }};
}

macro_rules! into_ratio {
    ($x:expr) => {{
        let numerator = $x as i64;
        let denominator = 1i64;
        num_rational::Rational64::new(numerator, denominator)
    }};
}

macro_rules! into_int {
    ($x:expr) => {
        $x.floor().to_integer()
    };
}

fn compute_pot_delta(
    reserves: u64,
    gathered_fees: u64,
    decayed_deposits: u64,
    rho: &RationalNumber,
    tau: &RationalNumber,
) -> PotDelta {
    let rho = as_ratio!(rho);
    let reserves = into_ratio!(reserves);

    let incentives = rho * reserves;

    let reward_pot = incentives + into_ratio!(gathered_fees) + into_ratio!(decayed_deposits);

    let tau = as_ratio!(tau);
    let treasury_tax = (tau * reward_pot).floor();
    let available_rewards = reward_pot - treasury_tax.clone();

    let incentives = into_int!(incentives) as u64;
    let treasury_tax = into_int!(treasury_tax) as u64;
    let available_rewards = into_int!(available_rewards) as u64;

    PotDelta {
        incentives,
        treasury_tax,
        available_rewards,
    }
}

pub fn compute_genesis_pots(
    max_supply: u64,
    utxos: u64,
    pparams: &PParamsSet,
) -> Result<Pots, ChainError> {
    let reserves = max_supply.saturating_sub(utxos);

    let rho = pparams.rho().ok_or(ChainError::PParamsNotFound)?;
    let tau = pparams.tau().ok_or(ChainError::PParamsNotFound)?;

    let pot_delta = compute_pot_delta(reserves, 0, 0, &rho, &tau);

    let out = Pots {
        reserves: reserves - pot_delta.incentives + pot_delta.available_rewards,
        treasury: pot_delta.treasury_tax,
    };

    Ok(out)
}

pub type TotalPoolReward = u64;
pub type OperatorShare = u64;

fn compute_pool_reward(
    total_rewards: u64,
    total_active_stake: u64,
    pool: &PoolData,
    pool_stake: u64,
    k: u32,
    a0: &RationalNumber,
) -> (TotalPoolReward, OperatorShare) {
    let z0 = 1.0 / k as f64;
    let sigma = pool_stake as f64 / total_active_stake as f64;
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

fn compute_delegator_reward(remaining: u64, total_delegated: u64, delegator: &AccountState) -> u64 {
    let share = (delegator.active_stake as f64 / total_delegated as f64) * remaining as f64;
    share.round() as u64
}

impl BoundaryWork {
    pub fn initial_pots(&self) -> Pots {
        Pots {
            reserves: self.ending_state.reserves,
            treasury: self.ending_state.treasury,
        }
    }

    pub fn active_pparams(&self) -> Result<&PParamsSet, ChainError> {
        // on the first two epochs, we use the ending state pparams since there's no
        // active state yet
        if self.ending_state.number <= 2 {
            return Ok(&self.ending_state.pparams);
        }

        let p = &self
            .active_state
            .as_ref()
            .ok_or(ChainError::PParamsNotFound)?
            .pparams;

        Ok(p)
    }

    pub fn active_rho(&self) -> Result<RationalNumber, ChainError> {
        self.active_pparams()?
            .rho()
            .ok_or(ChainError::PParamsNotFound)
    }

    pub fn active_tau(&self) -> Result<RationalNumber, ChainError> {
        self.active_pparams()?
            .tau()
            .ok_or(ChainError::PParamsNotFound)
    }

    pub fn active_k(&self) -> Result<u32, ChainError> {
        self.active_pparams()?
            .k()
            .ok_or(ChainError::PParamsNotFound)
    }

    pub fn active_a0(&self) -> Result<RationalNumber, ChainError> {
        self.active_pparams()?
            .a0()
            .ok_or(ChainError::PParamsNotFound)
    }

    pub fn starting_pparams(&self) -> Result<&PParamsSet, ChainError> {
        let starting_state = self
            .starting_state
            .as_ref()
            .ok_or(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete))?;

        let p = &starting_state.pparams;

        Ok(p)
    }

    pub fn gathered_fees(&self) -> u64 {
        self.ending_state.gathered_fees
    }

    pub fn decayed_deposits(&self) -> u64 {
        self.ending_state.decayed_deposits
    }

    fn define_pot_delta(&mut self) -> Result<(), ChainError> {
        let delta = compute_pot_delta(
            self.initial_pots().reserves,
            self.gathered_fees(),
            self.decayed_deposits(),
            &self.active_rho()?,
            &self.active_tau()?,
        );

        self.pot_delta = Some(delta);

        Ok(())
    }

    fn define_pool_rewards(&mut self) -> Result<(), ChainError> {
        let pot_delta = self
            .pot_delta
            .as_ref()
            .ok_or(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete))?;

        let mut effective_rewards = 0;

        for (id, pool) in self.pools.iter() {
            let pool_stake = self.active_snapshot.get_pool_stake(id);

            let rewards = compute_pool_reward(
                pot_delta.available_rewards,
                self.active_snapshot.total_stake,
                &pool,
                pool_stake,
                self.active_k()?,
                &self.active_a0()?,
            );

            let pool_total = rewards.0 + rewards.1;

            effective_rewards += pool_total;
            self.pool_rewards.insert(id.clone(), pool_total);
        }

        self.effective_rewards = Some(effective_rewards);

        Ok(())
    }

    fn define_starting_state(&mut self) -> Result<(), ChainError> {
        let pot_delta = self
            .pot_delta
            .as_ref()
            .ok_or(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete))?;

        let effective_rewards = self
            .effective_rewards
            .ok_or(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete))?;

        let unused_rewards = pot_delta
            .available_rewards
            .saturating_sub(effective_rewards);

        let reserves = self.initial_pots().reserves - pot_delta.incentives + unused_rewards;

        let treasury = self.initial_pots().treasury + pot_delta.treasury_tax;

        let pparams = self.ending_state.pparams.clone();
        let deposits = self.ending_state.deposits;
        let utxos = self.ending_state.utxos;

        let nonces = if self
            .era_transition
            .as_ref()
            .map(|transition| transition.new_version == 2)
            .unwrap_or(false)
        {
            Some(Nonces::bootstrap(self.shelley_hash))
        } else {
            let previous_tail = match self.waiting_state.as_ref() {
                Some(state) => {
                    if let Some(nonces) = state.nonces.as_ref() {
                        nonces.tail
                    } else {
                        None
                    }
                }
                None => None,
            };

            self.ending_state
                .nonces
                .as_ref()
                .map(|nonces| nonces.sweep(previous_tail, None))
        };

        let state = EpochState {
            number: self.ending_state.number + 1,
            active_stake: self.active_snapshot.total_stake,
            deposits,
            utxos,
            reserves,
            treasury,
            pparams,
            largest_stable_slot: epoch_first_slot(self.ending_state.number + 2, &self.active_era)
                - self.mutable_slots,
            nonces,

            // computed throughout the epoch during _roll_
            gathered_fees: 0,
            gathered_deposits: 0,
            decayed_deposits: 0,

            // will be computed at the end of the epoch during _sweep_
            rewards_to_distribute: None,
            rewards_to_treasury: None,
        };

        self.starting_state = Some(state);

        Ok(())
    }

    fn define_era_transition(&mut self) -> Result<(), ChainError> {
        let (active_protocol, _) = self
            .active_pparams()?
            .protocol_version()
            .ok_or(ChainError::PParamsNotFound)?;

        let (starting_protocol, _) = self
            .starting_pparams()?
            .protocol_version()
            .ok_or(ChainError::PParamsNotFound)?;

        if starting_protocol != active_protocol as u64 {
            let epoch_length = self.ending_state.pparams.epoch_length_or_default();
            let slot_length = self.ending_state.pparams.slot_length_or_default();

            let era_transition = EraTransition {
                prev_version: active_protocol as u16,
                new_version: starting_protocol as u16,
                epoch_length,
                slot_length,
            };

            self.era_transition = Some(era_transition);
        }

        Ok(())
    }

    pub fn compute(&mut self) -> Result<(), ChainError> {
        self.define_pot_delta()?;
        self.define_pool_rewards()?;
        self.define_starting_state()?;
        self.define_era_transition()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{sweep::Snapshot, PParamValue};

    use super::*;

    #[test]
    fn test_genesis_pots() {
        let pparams = PParamsSet::new()
            .with(PParamValue::ExpansionRate(RationalNumber {
                numerator: 3,
                denominator: 1000,
            }))
            .with(PParamValue::TreasuryGrowthRate(RationalNumber {
                numerator: 20,
                denominator: 100,
            }))
            .with(PParamValue::DesiredNumberOfStakePools(150))
            .with(PParamValue::PoolPledgeInfluence(RationalNumber {
                numerator: 3,
                denominator: 10,
            }));

        let pots =
            compute_genesis_pots(45_000_000_000_000_000, 30_000_000_000_000_000, &pparams).unwrap();

        assert_eq!(pots.reserves, 14_991_000_000_000_000);
        assert_eq!(pots.treasury, 9_000_000_000_000);
    }

    #[test]
    fn test_simple_boundary() {
        // this is one of the initial boundaries when we still don't have any pools or
        // active stake. We're using data from preview network for the boundary going
        // from 0 to 1.

        let pparams = PParamsSet::new()
            .with(PParamValue::ExpansionRate(RationalNumber {
                numerator: 3,
                denominator: 1000,
            }))
            .with(PParamValue::TreasuryGrowthRate(RationalNumber {
                numerator: 20,
                denominator: 100,
            }))
            .with(PParamValue::DesiredNumberOfStakePools(150))
            .with(PParamValue::PoolPledgeInfluence(RationalNumber {
                numerator: 3,
                denominator: 10,
            }));

        let mut boundary = BoundaryWork {
            active_state: None,
            active_snapshot: Snapshot::empty(),
            waiting_state: None,
            ending_state: EpochState {
                number: 0,
                active_stake: 0,
                deposits: 0,
                reserves: 14_991_000_000_000_000,
                treasury: 9_000_000_000_000,
                pparams,
                utxos: 29_999_998_493_562_207,
                gathered_fees: 437_793,
                gathered_deposits: 0,
                decayed_deposits: 0,
                rewards_to_distribute: None,
                rewards_to_treasury: None,
                largest_stable_slot: 1,
                nonces: None,
            },
            ending_snapshot: Snapshot::empty(),
            mutable_slots: 10,
            is_first_shelley_epoch: false,
            shelley_hash: [0; 32].as_slice().into(),

            // empty until computed
            pool_rewards: Default::default(),
            pools: Default::default(),
            starting_state: None,
            pot_delta: None,
            effective_rewards: None,
            era_transition: None,
        };

        boundary.compute().unwrap();

        let starting_state = boundary.starting_state.unwrap();

        assert_eq!(starting_state.reserves, 14982005400350235);
    }
}
