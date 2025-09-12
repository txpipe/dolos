use dolos_core::{BlockSlot, BrokenInvariant, ChainError, Domain, EntityKey, State3Store as _};
use pallas::ledger::primitives::ProtocolVersion;

use crate::{
    sweep::pots::Pots, EpochState, EraSummary, FixedNamespace as _, EPOCH_KEY_GO, EPOCH_KEY_MARK,
    EPOCH_KEY_SET,
};

mod accounts;
mod pools;
mod pots;
mod rewards;

fn apply_era_transition<D: Domain>(
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
        start: previous.end.clone().unwrap(),
        end: None,
        epoch_length,
        slot_length,
    };

    domain
        .state3()
        .write_entity_typed(&EntityKey::from(&new_version.to_be_bytes()), &new)?;

    Ok(())
}

fn check_era_transition<D: Domain>(
    domain: &D,
    current: &EpochState,
    prev_protocol: Option<ProtocolVersion>,
) -> Result<(), ChainError> {
    if let Some((last_version, _)) = prev_protocol {
        let new_version = current.pparams.protocol_major().unwrap_or_default();

        if last_version != new_version as u64 {
            let epoch_length = current.pparams.epoch_length_or_default();
            let slot_length = current.pparams.slot_length_or_default();
            apply_era_transition(domain, new_version, epoch_length, slot_length)?;
        }
    }

    Ok(())
}

fn drop_go_epoch<D: Domain>(
    domain: &D,
    go: Option<EpochState>,
) -> Result<Option<ProtocolVersion>, ChainError> {
    let Some(go) = go else {
        return Ok(None);
    };

    let protocol = go
        .pparams
        .protocol_version()
        .ok_or(ChainError::from(BrokenInvariant::InvalidEpochState))?;

    domain
        .state3()
        .delete_entity(EpochState::NS, &EntityKey::from(EPOCH_KEY_GO))?;

    Ok(Some(protocol))
}

fn promote_set_epoch<D: Domain>(
    domain: &D,
    set: Option<EpochState>,
    prev_protocol: Option<ProtocolVersion>,
) -> Result<Option<EpochState>, ChainError> {
    let Some(set) = set else {
        return Ok(None);
    };

    domain
        .state3()
        .write_entity_typed(&EntityKey::from(EPOCH_KEY_GO), &set)?;

    let new_go = set;

    check_era_transition(domain, &new_go, prev_protocol)?;

    Ok(Some(new_go))
}

fn promote_mark_epoch<D: Domain>(
    domain: &D,
    mark: EpochState,
    pots: &Pots,
) -> Result<EpochState, ChainError> {
    // variable name to avoid confusion
    let mut set = mark;

    set.rewards_to_distribute = Some(pots.to_distribute);
    set.rewards_to_treasury = Some(pots.to_treasury);

    domain
        .state3()
        .write_entity_typed(&EntityKey::from(EPOCH_KEY_SET), &set)?;

    Ok(set)
}

fn start_new_epoch<D: Domain>(domain: &D, prev: &EpochState) -> Result<(), ChainError> {
    let prev_rewards = prev
        .rewards()
        .ok_or(ChainError::from(BrokenInvariant::InvalidEpochState))?;

    let additional_treasury = prev
        .rewards_to_treasury
        .ok_or(ChainError::from(BrokenInvariant::InvalidEpochState))?;

    let new_reserves = prev.reserves.saturating_sub(prev_rewards);
    let new_treasury = prev.treasury + additional_treasury;
    let new_number = prev.number + 1;
    let new_pparams = prev.pparams.clone();

    let new_deposits = prev.deposits + prev.gathered_deposits - prev.decayed_deposits;

    let epoch = EpochState {
        treasury: new_treasury,
        number: new_number,
        reserves: new_reserves,
        pparams: new_pparams,
        deposits: new_deposits,
        stake: 0, // TODO: compute

        // computed throughout the epoch during _roll_
        gathered_fees: 0,
        gathered_deposits: 0,
        decayed_deposits: 0,

        // computed at the end of the epoch during _sweep_
        rewards_to_distribute: None,
        rewards_to_treasury: None,
    };

    domain
        .state3()
        .write_entity_typed(&EntityKey::from(EPOCH_KEY_MARK), &epoch)?;

    Ok(())
}

pub fn sweep<D: Domain>(domain: &D, _: BlockSlot) -> Result<(), ChainError> {
    // TODO: this should all be one big atomic operation, but for that we need to
    // refactor stores to include start / commit semantics

    let mark = crate::load_live_epoch(domain)?;
    let set = crate::load_previous_epoch(domain)?;
    let go = crate::load_active_epoch(domain)?;

    pools::aggregate_stake(domain)?;
    let pots = pots::compute_for_epoch(&mark)?;

    // order matters
    if let Some(go) = &go {
        rewards::distribute(domain, mark.number, pots.to_distribute, go.stake)?;
    }

    // HERE'S WHERE WE CONSIDER THE EPOCH TRANSITIONING CONCEPTUALLY

    // rotate individual delegation
    pools::rotate_delegation(domain)?;
    accounts::rotate_delegation(domain)?;

    // rotate epochs, order matters
    let prev_protocol = drop_go_epoch(domain, go)?;
    let _new_go = promote_set_epoch(domain, set, prev_protocol)?;
    let new_set = promote_mark_epoch(domain, mark, &pots)?;
    start_new_epoch(domain, &new_set)?;

    Ok(())
}
