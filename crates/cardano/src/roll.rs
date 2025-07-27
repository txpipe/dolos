use dolos_core::{State3Error, State3Store, StateDelta};
use pallas::ledger::{
    addresses::{Address, StakeAddress},
    traverse::{MultiEraBlock, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx},
};
use tracing::info;

use crate::model::{AccountState, AssetState};

trait RollVisitor {
    fn visit_output(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        Ok(())
    }

    fn visit_mint(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        Ok(())
    }
}

fn crawl_block<'a, T: RollVisitor>(
    delta: &mut StateDelta,
    state: &impl State3Store,
    block: &MultiEraBlock<'a>,
    visitor: &mut T,
) -> Result<(), State3Error> {
    for tx in block.txs() {
        for output in tx.outputs() {
            visitor.visit_output(state, delta, &output)?;
        }

        for mint in tx.mints() {
            visitor.visit_mint(state, delta, &tx, &mint)?;
        }
    }

    Ok(())
}

struct SeenAddressesVisitor;

impl RollVisitor for SeenAddressesVisitor {
    fn visit_output(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        let full_address = output.address().unwrap();

        let stake = match full_address.clone() {
            Address::Shelley(x) => StakeAddress::try_from(x).ok(),
            Address::Stake(x) => Some(x),
            _ => None,
        };

        let Some(stake) = stake else {
            return Ok(());
        };

        let stake_bytes = stake.clone().to_vec();
        let current = state.read_entity_typed::<AccountState>(&stake_bytes)?;

        if let Some(current) = current {
            let mut new = current.clone();
            new.seen_addresses.insert(full_address.to_vec());
            delta.override_entity(stake_bytes, new, Some(current));
        } else {
            let mut new = AccountState::default();
            new.seen_addresses.insert(full_address.to_vec());
            delta.override_entity(stake_bytes, new, None);
        }

        Ok(())
    }
}

struct AssetStateVisitor;

impl RollVisitor for AssetStateVisitor {
    fn visit_mint(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        let policy = mint.policy();

        for asset in mint.assets() {
            let mut subject = vec![];
            subject.extend_from_slice(policy.as_slice());
            subject.extend_from_slice(asset.name());

            info!("tracking asset: {:?}", hex::encode(&subject));

            let current = state
                .read_entity_typed::<AssetState>(&subject)?
                .unwrap_or(AssetState {
                    quantity: 0,
                    initial_tx: tx.hash(),
                    mint_tx_count: 0,
                });

            let mut new = current.clone();
            new.quantity += asset.mint_coin().unwrap_or_default() as u64;
            new.mint_tx_count += 1;
            delta.override_entity(subject, new, Some(current));
        }

        Ok(())
    }
}

struct AllInOneVisitor {
    seen_addresses: SeenAddressesVisitor,
    asset_state: AssetStateVisitor,
}

impl RollVisitor for AllInOneVisitor {
    fn visit_output(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        self.seen_addresses.visit_output(state, delta, output)?;
        self.asset_state.visit_output(state, delta, output)?;
        Ok(())
    }

    fn visit_mint(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        self.seen_addresses.visit_mint(state, delta, tx, mint)?;
        self.asset_state.visit_mint(state, delta, tx, mint)?;
        Ok(())
    }
}

pub fn compute_block_delta<'a>(
    state: &impl State3Store,
    block: &MultiEraBlock<'a>,
) -> Result<StateDelta, State3Error> {
    let mut delta = StateDelta::new(block.slot());

    let mut visitor = AllInOneVisitor {
        seen_addresses: SeenAddressesVisitor,
        asset_state: AssetStateVisitor,
    };

    crawl_block(&mut delta, state, block, &mut visitor)?;

    Ok(delta)
}
