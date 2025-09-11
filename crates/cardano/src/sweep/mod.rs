use dolos_core::{BlockSlot, BrokenInvariant, ChainError, Domain, EntityKey, State3Store as _};

use crate::{
    EpochState, EraSummary, FixedNamespace as _, EPOCH_KEY_GO, EPOCH_KEY_MARK, EPOCH_KEY_SET,
};

mod pots;
mod rewards;
mod stake;

fn transition_era<D: Domain>(
    domain: &D,
    new_version: u16,
    epoch_length: u64,
    slot_length: u64,
) -> Result<(), ChainError> {
    let previous_version = new_version - 1;

    let previous = domain.state3().read_entity_typed::<EraSummary>(
        EraSummary::NS,
        &EntityKey::from(&previous_version.to_be_bytes()),
    )?;

    let Some(mut previous) = previous else {
        return Err(BrokenInvariant::BadBootstrap.into());
    };

    previous.define_end(new_version as u64);

    domain.state3().write_entity_typed::<EraSummary>(
        &EntityKey::from(&previous_version.to_be_bytes()),
        &previous,
    )?;

    let new = EraSummary {
        start: previous.start.clone(),
        end: None,
        epoch_length,
        slot_length,
    };

    domain
        .state3()
        .write_entity_typed(&EntityKey::from(&new_version.to_be_bytes()), &new)?;

    Ok(())
}

fn promote_set_epoch<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let set = domain
        .state3()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_SET))?;

    let Some(set) = set else {
        return Ok(());
    };

    let previous = domain
        .state3()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_GO))?;

    domain
        .state3()
        .write_entity_typed(&EntityKey::from(EPOCH_KEY_GO), &set)?;

    let current = set;

    if let Some(previous) = previous {
        let last_version = previous.pparams.protocol_major().unwrap_or_default();
        let new_version = current.pparams.protocol_major().unwrap_or_default();
        let epoch_length = current.pparams.epoch_length_or_default();
        let slot_length = current.pparams.slot_length_or_default();

        if last_version != new_version {
            transition_era(domain, new_version, epoch_length, slot_length)?;
        }
    }

    Ok(())
}

fn promote_mark_epoch<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let set = domain
        .state3()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_MARK))?;

    let Some(set) = set else {
        return Ok(());
    };

    domain
        .state3()
        .write_entity_typed(&EntityKey::from(EPOCH_KEY_SET), &set)?;

    Ok(())
}

fn start_new_epoch<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let previous = domain
        .state3()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_MARK))?;

    let Some(previous) = previous else {
        return Err(BrokenInvariant::BadBootstrap.into());
    };

    let epoch = EpochState {
        treasury: previous.treasury + previous.to_treasury.unwrap_or_default(),
        gathered_fees: 0,
        decayed_deposits: 0,
        rewards: 0,
        number: previous.number + 1,
        reserves: previous.end_reserves.unwrap_or_default(),
        end_reserves: None,
        to_treasury: None,
        to_distribute: None,
        pparams: previous.pparams.clone(),
        //TODO: supply_circulating: todo!(),
        //TODO: supply_locked: todo!(),
        //TODO: stake_live: todo!(),
        //TODO: stake_active: todo!(),
        ..Default::default()
    };

    domain
        .state3()
        .write_entity_typed(&EntityKey::from(EPOCH_KEY_MARK), &epoch)?;

    Ok(())
}

fn transition_epochs<D: Domain>(domain: &D) -> Result<(), ChainError> {
    // order matters
    promote_set_epoch(domain)?;
    promote_mark_epoch(domain)?;
    start_new_epoch(domain)?;

    Ok(())
}

pub fn sweep<D: Domain>(domain: &D, _: BlockSlot) -> Result<(), ChainError> {
    // TODO: this should all be one big atomic operation, but for that we need to
    // refactor stores to include start / commit semantics

    // order matters
    stake::sweep(domain)?;
    pots::sweep(domain)?;
    rewards::sweep(domain)?;

    transition_epochs(domain)?;

    Ok(())
}
