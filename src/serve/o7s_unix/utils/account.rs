use crate::prelude::*;
use dolos_cardano::load_epoch;
use pallas::codec::utils::{AnyCbor, AnyUInt};
use pallas::network::miniprotocols::localstate::queries_v16 as q16;
use tracing::debug;

/// Build response for GetAccountState query (treasury and reserves)
pub fn build_account_state_response<D: Domain>(domain: &D) -> Result<AnyCbor, Error> {
    let epoch_state = load_epoch::<D>(domain.state())
        .map_err(|e| Error::server(format!("failed to load epoch state: {}", e)))?;

    let treasury = epoch_state.initial_pots.treasury;
    let reserves = epoch_state.initial_pots.reserves;

    let account_state = q16::AccountState {
        treasury: AnyUInt::U64(treasury),
        reserves: AnyUInt::U64(reserves),
    };

    debug!(
        treasury = treasury,
        reserves = reserves,
        "returning account state"
    );

    Ok(AnyCbor::from_encode((account_state,)))
}
