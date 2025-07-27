use dolos_core::{State3Error, State3Store, StateDelta};
use pallas::ledger::{
    addresses::{Address, StakeAddress},
    traverse::MultiEraBlock,
};
use tracing::info;

use crate::model::AccountState;

pub fn compute_block_delta<'a>(
    state: &impl State3Store,
    block: &MultiEraBlock<'a>,
) -> Result<StateDelta, State3Error> {
    let mut delta = StateDelta::new(block.slot());

    for tx in block.txs() {
        for output in tx.outputs() {
            let full_address = output.address().unwrap();

            let stake = match full_address.clone() {
                Address::Shelley(x) => StakeAddress::try_from(x).ok(),
                Address::Stake(x) => Some(x),
                _ => None,
            };

            if let Some(stake) = stake {
                let stake_bytes = stake.clone().to_vec();
                let current = state.read_entity_typed::<AccountState>(&stake_bytes)?;

                if let Some(current) = current {
                    let mut new = current.clone();
                    new.seen_addresses.insert(full_address.to_vec());

                    info!(
                        "overriding account state for stake {}",
                        stake.to_bech32().ok().unwrap_or_default()
                    );

                    delta.override_entity(stake_bytes, new, Some(current));
                } else {
                    let mut new = AccountState::default();
                    new.seen_addresses.insert(full_address.to_vec());

                    info!(
                        "overriding account state for stake {}",
                        stake.to_bech32().ok().unwrap_or_default()
                    );

                    delta.override_entity(stake_bytes, new, None);
                }
            }
        }
    }

    Ok(delta)
}
