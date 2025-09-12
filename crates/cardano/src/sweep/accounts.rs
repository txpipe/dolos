use dolos_core::{ChainError, Domain, StateStore as _};

use crate::{AccountState, FixedNamespace};

pub fn rotate_delegation<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let all = domain
        .state()
        .iter_entities_typed::<AccountState>(AccountState::NS, None)?;

    for record in all {
        let (key, mut state) = record?;

        state.active_stake = state.wait_stake;
        state.wait_stake = state.live_stake();

        domain
            .state()
            .write_entity_typed::<AccountState>(&key, &state)?;
    }

    Ok(())
}
