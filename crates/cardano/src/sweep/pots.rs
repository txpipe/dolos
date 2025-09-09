use dolos_core::{ChainError, Domain, EntityKey, State3Store as _};
use pallas::ledger::validate::utils::MultiEraProtocolParameters;

use crate::{EpochState, FixedNamespace as _, PParamsState, EPOCH_KEY_MARK, EPOCH_KEY_SET};

pub type PParams = MultiEraProtocolParameters;

pub type NewReserves = u64;
pub type ToTreasury = u64;
pub type ToDistribute = u64;

fn compute_new_pots(
    previous_reserves: u64,
    gathered_fees: u64,
    decayed_deposits: u64,
    pparams: &PParamsState,
) -> (NewReserves, ToTreasury, ToDistribute) {
    let rho = pparams.rho().numerator as f64 / pparams.rho().denominator as f64;
    let from_reserves = rho * (previous_reserves as f64);

    let reward_pot_f64 = (from_reserves.round() as u64 + gathered_fees + decayed_deposits) as f64;

    let tau = pparams.tau().numerator as f64 / pparams.tau().denominator as f64;
    let to_treasury_f64 = tau * reward_pot_f64;
    let to_distribute_f64 = (1.0 - tau) * reward_pot_f64;

    let to_treasury = to_treasury_f64.round() as u64;
    let to_distribute = to_distribute_f64.round() as u64;

    // Update reserves
    let new_reserves = previous_reserves.saturating_sub(from_reserves.round() as u64);

    (new_reserves, to_treasury, to_distribute)
}

pub fn sweep<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let prev_epoch = domain
        .state3()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_SET))?;

    let Some(prev_epoch) = prev_epoch else {
        return Ok(());
    };

    let live_epoch = domain
        .state3()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_MARK))?;

    let Some(mut live_epoch) = live_epoch else {
        return Ok(());
    };

    let pparams = domain
        .state3()
        .read_entity_typed::<PParamsState>(PParamsState::NS, &EntityKey::from(EPOCH_KEY_MARK))?;

    let Some(pparams) = pparams else {
        return Err(ChainError::PParamsNotFound);
    };

    let (new_reserves, to_treasury, to_distribute) = compute_new_pots(
        prev_epoch.reserves,
        live_epoch.gathered_fees,
        live_epoch.decayed_deposits,
        &pparams,
    );

    live_epoch.end_reserves = Some(new_reserves);
    live_epoch.to_treasury = Some(to_treasury);
    live_epoch.to_distribute = Some(to_distribute);

    domain
        .state3()
        .write_entity_typed::<EpochState>(&EntityKey::from(EPOCH_KEY_MARK), &live_epoch)?;

    Ok(())
}
