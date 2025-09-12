use dolos_core::{ChainError, Domain, State3Store as _};

use crate::{AccountState, FixedNamespace};

pub fn rotate_delegation<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let all = domain
        .state3()
        .iter_entities_typed::<AccountState>(AccountState::NS, None)?;

    for record in all {
        let (key, mut state) = record?;

        state.active_stake = state.wait_stake;
        state.wait_stake = state.live_stake;

        domain
            .state3()
            .write_entity_typed::<AccountState>(&key, &state)?;
    }

    Ok(())
}
