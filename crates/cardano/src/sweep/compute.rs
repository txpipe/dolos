use dolos_core::{BrokenInvariant, ChainError, Domain, Genesis, StateStore as _};
use pallas::ledger::primitives::RationalNumber;
use tracing::{debug, instrument, trace};

use crate::{
    forks,
    sweep::{BoundaryVisitor as _, BoundaryWork, EraTransition, PotDelta, Pots},
    utils::nonce_stability_window,
    AccountState, DRepState, EpochState, EraProtocol, FixedNamespace as _, Nonces, PParamsSet,
    PoolState,
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
    let available_rewards = reward_pot - treasury_tax;

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

    let rho = pparams.ensure_rho()?;
    let tau = pparams.ensure_tau()?;

    let pot_delta = compute_pot_delta(reserves, 0, 0, &rho, &tau);

    let out = Pots {
        reserves: reserves - pot_delta.incentives + pot_delta.available_rewards,
        treasury: pot_delta.treasury_tax,
        utxos,
    };

    Ok(out)
}

impl BoundaryWork {
    pub fn initial_pots(&self) -> Pots {
        Pots {
            reserves: self.ending_state.reserves,
            treasury: self.ending_state.treasury,
            utxos: self.ending_state.utxos,
        }
    }

    pub fn valid_pparams(&self) -> Result<&PParamsSet, ChainError> {
        // on the initial epoch, we use the ending state pparams since there's no
        // active state yet
        if self.ending_state.number == 0 {
            return Ok(&self.ending_state.pparams);
        }

        let p = &self
            .waiting_state
            .as_ref()
            .ok_or(ChainError::NoActiveEpoch)?
            .pparams;

        Ok(p)
    }

    pub fn valid_rho(&self) -> Result<RationalNumber, ChainError> {
        self.valid_pparams()?.ensure_rho()
    }

    pub fn valid_tau(&self) -> Result<RationalNumber, ChainError> {
        self.valid_pparams()?.ensure_tau()
    }

    pub fn valid_k(&self) -> Result<u32, ChainError> {
        self.valid_pparams()?.ensure_k()
    }

    pub fn valid_a0(&self) -> Result<RationalNumber, ChainError> {
        self.valid_pparams()?.ensure_a0()
    }

    pub fn valid_drep_inactivity_period(&self) -> Result<u64, ChainError> {
        self.valid_pparams()?.ensure_drep_inactivity_period()
    }

    pub fn ending_pparams(&self) -> &PParamsSet {
        &self.ending_state.pparams
    }

    pub fn gathered_fees(&self) -> u64 {
        self.ending_state.gathered_fees
    }

    pub fn decayed_deposits(&self) -> u64 {
        self.ending_state.decayed_deposits
    }

    /// Check if this boundary is transitioning to shelley for the first time.
    fn is_transitioning_to_shelley(&self) -> bool {
        self.era_transition
            .as_ref()
            .map(|transition| transition.new_version == 2)
            .unwrap_or(false)
    }

    /// Check if the starting epoch is still within the byron era.
    pub fn still_byron(&self) -> bool {
        self.active_protocol < 2 && !self.is_transitioning_to_shelley()
    }

    fn set_neutral_pot_delta(&mut self) {
        self.pot_delta = Some(PotDelta {
            incentives: 0,
            treasury_tax: 0,
            available_rewards: 0,
        });
    }

    fn define_pot_delta(&mut self) -> Result<(), ChainError> {
        // if we're still in Byron, we just skip the pot delta computation by assigning
        // a neutral pot delta
        if self.still_byron() {
            debug!("skipping pot delta during byron era");
            self.set_neutral_pot_delta();
            return Ok(());
        }

        let delta = compute_pot_delta(
            self.initial_pots().reserves,
            self.gathered_fees(),
            self.decayed_deposits(),
            &self.valid_rho()?,
            &self.valid_tau()?,
        );

        debug!(
            %delta.incentives,
            %delta.treasury_tax,
            %delta.available_rewards,
            "defined pot delta"
        );

        self.pot_delta = Some(delta);

        Ok(())
    }

    fn define_starting_nonces(&mut self) -> Result<Option<Nonces>, ChainError> {
        if self.is_transitioning_to_shelley() {
            let initial = Nonces::bootstrap(self.shelley_hash);
            return Ok(Some(initial));
        }

        let tail = self
            .waiting_state
            .as_ref()
            .and_then(|state| state.nonces.as_ref())
            .and_then(|nonces| nonces.tail);

        let new_nonces = self
            .ending_state
            .nonces
            .as_ref()
            .map(|nonces| nonces.sweep(tail, None));

        Ok(new_nonces)
    }

    // TODO: since we don't have a nice way to update epochs via delta given the
    // race conditions in the fixed keys. We should probably move to sequential
    // keys for epochs. Ideally, the new epoch should be created via delta.
    fn define_starting_state(
        &mut self,
        genesis: &Genesis,
        effective_rewards: u64,
    ) -> Result<(), ChainError> {
        let pot_delta = self
            .pot_delta
            .as_ref()
            .ok_or(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete))?;

        let unused_rewards = pot_delta
            .available_rewards
            .saturating_sub(effective_rewards);

        let reserves = self.initial_pots().reserves - pot_delta.incentives + unused_rewards;

        let treasury = self.initial_pots().treasury + pot_delta.treasury_tax;

        let deposits = self.ending_state.deposits;
        let utxos = self.ending_state.utxos;

        let nonces = self.define_starting_nonces()?;

        let pparams = match &self.era_transition {
            Some(era_transition) => era_transition.new_pparams.clone(),
            None => self.ending_state.pparams.clone(),
        };

        let state = EpochState {
            number: self.ending_state.number + 1,
            active_stake: self.active_snapshot.total_stake,
            deposits,
            utxos,
            reserves,
            treasury,
            pparams,
            largest_stable_slot: self
                .active_era
                .epoch_start(self.ending_state.number as u64 + 2)
                - nonce_stability_window(self.active_protocol.into(), genesis),
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

    fn define_era_transition(&mut self, genesis: &Genesis) -> Result<(), ChainError> {
        let original = self.ending_pparams().version();
        let (effective, _) = self.ending_pparams().ensure_protocol_version()?;

        if effective != original as u64 {
            debug!(
                %original,
                %effective,
                "found protocol version change"
            );

            let new_pparams =
                forks::evolve_pparams(&self.ending_state.pparams, genesis, effective as u16)?;

            let era_transition = EraTransition {
                prev_version: EraProtocol::from(original),
                new_version: EraProtocol::from(effective as u16),
                new_pparams,
            };

            self.era_transition = Some(era_transition);
        }

        Ok(())
    }

    #[instrument(skip_all)]
    pub fn compute<D: Domain>(&mut self, domain: &D) -> Result<(), ChainError> {
        trace!("defining pot delta");
        self.define_pot_delta()?;

        let mut visitor_retires = super::retires::BoundaryVisitor::default();
        let mut visitor_rewards = super::rewards::BoundaryVisitor::default();
        let mut visitor_rotate = super::transition::BoundaryVisitor::default();

        let pools = domain
            .state()
            .iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for pool in pools {
            let (pool_id, pool) = pool?;

            visitor_retires.visit_pool(self, &pool_id, &pool)?;
            visitor_rewards.visit_pool(self, &pool_id, &pool)?;
            visitor_rotate.visit_pool(self, &pool_id, &pool)?;
        }

        let dreps = domain
            .state()
            .iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for drep in dreps {
            let (drep_id, drep) = drep?;

            visitor_retires.visit_drep(self, &drep_id, &drep)?;
            visitor_rewards.visit_drep(self, &drep_id, &drep)?;
            visitor_rotate.visit_drep(self, &drep_id, &drep)?;
        }

        let accounts = domain
            .state()
            .iter_entities_typed::<AccountState>(AccountState::NS, None)?;

        for account in accounts {
            let (account_id, account) = account?;

            visitor_retires.visit_account(self, &account_id, &account)?;
            visitor_rewards.visit_account(self, &account_id, &account)?;
            visitor_rotate.visit_account(self, &account_id, &account)?;
        }

        visitor_retires.flush(self)?;
        visitor_rewards.flush(self)?;
        visitor_rotate.flush(self)?;

        trace!("defining era transition");
        self.define_era_transition(domain.genesis())?;

        trace!("defining starting state");
        self.define_starting_state(domain.genesis(), visitor_rewards.effective_rewards)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{sweep::Snapshot, EraBoundary, EraSummary, PParamValue};

    use super::*;

    #[test]
    fn test_genesis_pots() {
        let pparams = PParamsSet::new(0)
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

        let pparams = PParamsSet::new(0)
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
            active_protocol: EraProtocol::from(6),
            active_era: EraSummary {
                start: EraBoundary {
                    epoch: 0,
                    slot: 0,
                    timestamp: 0,
                },
                end: None,
                epoch_length: 86400,
                slot_length: 1,
            },
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
            shelley_hash: [0; 32].as_slice().into(),

            // empty until computed
            deltas: Default::default(),
            logs: Default::default(),
            starting_state: None,
            pot_delta: None,
            era_transition: None,
        };

        let domain = todo!();

        boundary.compute(&domain).unwrap();

        let starting_state = boundary.starting_state.unwrap();

        assert_eq!(starting_state.reserves, 14982005400350235);
    }
}
