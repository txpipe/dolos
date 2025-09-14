use dolos_core::{BrokenInvariant, ChainError, Domain, EntityKey, StateStore as _};

use crate::{
    sweep::BoundaryWork, AccountState, EpochState, EraSummary, FixedNamespace as _, PoolState,
    EPOCH_KEY_GO, EPOCH_KEY_MARK, EPOCH_KEY_SET,
};

pub fn rotate_pool_stake_data<D: Domain>(
    domain: &D,
    boundary: &BoundaryWork,
) -> Result<(), ChainError> {
    let all = domain
        .state()
        .iter_entities_typed::<PoolState>(PoolState::NS, None)?;

    for record in all {
        let (key, mut state) = record?;

        let new_stake = boundary.ending_snapshot.get_pool_stake(&key);

        // order matters
        state.active_stake = state.wait_stake;
        state.wait_stake = new_stake;

        domain
            .state()
            .write_entity_typed::<PoolState>(&key, &state)?;
    }

    Ok(())
}

pub fn rotate_account_stake_data<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let all = domain
        .state()
        .iter_entities_typed::<AccountState>(AccountState::NS, None)?;

    for record in all {
        let (key, mut state) = record?;

        state.active_stake = state.wait_stake;
        state.wait_stake = state.live_stake();

        state.active_pool = state.latest_pool.clone();

        domain
            .state()
            .write_entity_typed::<AccountState>(&key, &state)?;
    }

    Ok(())
}

fn drop_active_epoch<D: Domain>(domain: &D) -> Result<(), ChainError> {
    domain
        .state()
        .delete_entity(EpochState::NS, &EntityKey::from(EPOCH_KEY_GO))?;

    Ok(())
}

fn start_new_epoch<D: Domain>(domain: &D, boundary: &BoundaryWork) -> Result<(), ChainError> {
    let epoch = boundary
        .starting_state
        .clone()
        .ok_or(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete))?;

    domain
        .state()
        .write_entity_typed(&EntityKey::from(EPOCH_KEY_MARK), &epoch)?;

    Ok(())
}

fn apply_era_transition<D: Domain>(domain: &D, boundary: &BoundaryWork) -> Result<(), ChainError> {
    let Some(transition) = &boundary.era_transition else {
        return Ok(());
    };

    let previous = domain.state().read_entity_typed::<EraSummary>(
        EraSummary::NS,
        &EntityKey::from(&transition.prev_version.to_be_bytes()),
    )?;

    let Some(mut previous) = previous else {
        return Err(BrokenInvariant::BadBootstrap.into());
    };

    previous.define_end(boundary.ending_state.number as u64);

    domain.state().write_entity_typed::<EraSummary>(
        &EntityKey::from(&transition.prev_version.to_be_bytes()),
        &previous,
    )?;

    let new = EraSummary {
        start: previous.end.clone().unwrap(),
        end: None,
        epoch_length: transition.epoch_length,
        slot_length: transition.slot_length,
    };

    domain.state().write_entity_typed(
        &EntityKey::from(&transition.new_version.to_be_bytes()),
        &new,
    )?;

    Ok(())
}

fn promote_waiting_epoch<D: Domain>(domain: &D, boundary: &BoundaryWork) -> Result<(), ChainError> {
    let Some(waiting) = &boundary.waiting_state else {
        // we don't have waiting state for early epochs, we just need to wait
        if boundary.ending_state.number <= 1 {
            return Ok(());
        }

        return Err(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete));
    };

    domain
        .state()
        .write_entity_typed(&EntityKey::from(EPOCH_KEY_GO), waiting)?;

    Ok(())
}

fn promote_ending_epoch<D: Domain>(domain: &D, boundary: &BoundaryWork) -> Result<(), ChainError> {
    let ending = &boundary.ending_state;

    domain
        .state()
        .write_entity_typed(&EntityKey::from(EPOCH_KEY_SET), ending)?;

    Ok(())
}

impl BoundaryWork {
    pub fn commit<D: Domain>(&self, domain: &D) -> Result<(), ChainError> {
        rotate_pool_stake_data(domain, self)?;
        rotate_account_stake_data(domain)?;
        drop_active_epoch(domain)?;
        promote_waiting_epoch(domain, self)?;
        promote_ending_epoch(domain, self)?;
        apply_era_transition(domain, self)?;
        start_new_epoch(domain, self)?;

        Ok(())
    }
}
