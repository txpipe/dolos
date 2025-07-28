use std::ops::Deref;

use dolos_core::{State3Error, State3Store, StateDelta};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::{Address, StakeAddress},
        traverse::{MultiEraBlock, MultiEraCert, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx},
    },
};

use pallas::ledger::primitives::alonzo::Certificate as AlonzoCert;
use pallas::ledger::primitives::conway::Certificate as ConwayCert;

use tracing::info;

use crate::model::{AccountState, AssetState, PoolDelegator, PoolState};

fn cert_to_pool_state(cert: &MultiEraCert) -> Option<(Hash<28>, PoolState)> {
    match cert {
        MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
            AlonzoCert::PoolRegistration {
                operator,
                vrf_keyhash,
                pledge,
                cost,
                margin,
                reward_account,
                pool_owners,
                relays,
                pool_metadata,
            } => {
                let state = PoolState {
                    active_stake: 0,
                    live_stake: 0,
                    blocks_minted: 0,
                    live_saturation: 0.0,
                    vrf_keyhash: *vrf_keyhash,
                    reward_account: reward_account.to_vec(),
                    pool_owners: pool_owners.clone(),
                    relays: relays.clone(),
                    declared_pledge: *pledge,
                    margin_cost: margin.clone(),
                    fixed_cost: *cost,
                    metadata: pool_metadata.clone(),
                };

                Some((*operator, state))
            }
            _ => None,
        },
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::PoolRegistration {
                operator,
                vrf_keyhash,
                pledge,
                cost,
                margin,
                reward_account,
                pool_owners,
                relays,
                pool_metadata,
            } => {
                let state = PoolState {
                    active_stake: 0,
                    live_stake: 0,
                    blocks_minted: 0,
                    live_saturation: 0.0,
                    vrf_keyhash: *vrf_keyhash,
                    reward_account: reward_account.to_vec(),
                    pool_owners: pool_owners.clone().to_vec(),
                    relays: relays.clone(),
                    declared_pledge: *pledge,
                    margin_cost: margin.clone(),
                    fixed_cost: *cost,
                    metadata: pool_metadata.clone(),
                };

                Some((*operator, state))
            }
            _ => None,
        },
        _ => None,
    }
}

fn cert_to_pool_delegator(cert: &MultiEraCert) -> Option<(Hash<28>, PoolDelegator)> {
    match cert {
        MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
            AlonzoCert::StakeDelegation(delegator, pool) => {
                let delegator = PoolDelegator(delegator.clone());

                Some((*pool, delegator))
            }
            _ => None,
        },
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::StakeDelegation(delegator, pool) => {
                let delegator = PoolDelegator(delegator.clone());

                Some((*pool, delegator))
            }
            _ => None,
        },
        _ => None,
    }
}

trait RollVisitor {
    #[allow(unused_variables)]
    fn visit_output(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        tx: &MultiEraTx,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_mint(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_cert(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        tx: &MultiEraTx,
        cert: &MultiEraCert,
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
        for (index, output) in tx.outputs().iter().enumerate() {
            visitor.visit_output(state, delta, &tx, index as u32, output)?;
        }

        for mint in tx.mints() {
            visitor.visit_mint(state, delta, &tx, &mint)?;
        }

        for cert in tx.certs() {
            visitor.visit_cert(state, delta, &tx, &cert)?;
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
        _: &MultiEraTx,
        _: u32,
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

struct PoolStateVisitor;

impl RollVisitor for PoolStateVisitor {
    fn visit_cert(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some((operator, new)) = cert_to_pool_state(cert) {
            let current = state.read_entity_typed::<PoolState>(&operator.to_vec())?;
            delta.override_entity(operator.to_vec(), new, current);
        }

        Ok(())
    }
}

struct PoolDelegatorVisitor;

impl RollVisitor for PoolDelegatorVisitor {
    fn visit_cert(
        &mut self,
        _: &impl State3Store,
        delta: &mut StateDelta,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some((operator, new)) = cert_to_pool_delegator(cert) {
            info!(%operator, "new pool delegator");
            delta.append_entity(operator.as_slice(), new);
        }

        Ok(())
    }
}

struct AllInOneVisitor {
    seen_addresses: SeenAddressesVisitor,
    asset_state: AssetStateVisitor,
    pool_state: PoolStateVisitor,
    pool_delegator: PoolDelegatorVisitor,
}

impl RollVisitor for AllInOneVisitor {
    fn visit_output(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        tx: &MultiEraTx,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        self.seen_addresses
            .visit_output(state, delta, tx, index, output)?;
        self.asset_state
            .visit_output(state, delta, tx, index, output)?;
        self.pool_state
            .visit_output(state, delta, tx, index, output)?;
        self.pool_delegator
            .visit_output(state, delta, tx, index, output)?;
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
        self.pool_state.visit_mint(state, delta, tx, mint)?;
        self.pool_delegator.visit_mint(state, delta, tx, mint)?;
        Ok(())
    }

    fn visit_cert(
        &mut self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        tx: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        self.seen_addresses.visit_cert(state, delta, tx, cert)?;
        self.asset_state.visit_cert(state, delta, tx, cert)?;
        self.pool_state.visit_cert(state, delta, tx, cert)?;
        self.pool_delegator.visit_cert(state, delta, tx, cert)?;
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
        pool_state: PoolStateVisitor,
        pool_delegator: PoolDelegatorVisitor,
    };

    crawl_block(&mut delta, state, block, &mut visitor)?;

    Ok(delta)
}
