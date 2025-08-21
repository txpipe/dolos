use std::ops::Deref;

use dolos_core::{State3Error, State3Store, StateDelta};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::{Address, StakeAddress},
        primitives::StakeCredential,
        traverse::{MultiEraBlock, MultiEraCert, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx},
    },
};

use pallas::ledger::primitives::alonzo::Certificate as AlonzoCert;
use pallas::ledger::primitives::conway::Certificate as ConwayCert;

use tracing::debug;

use crate::model::{
    AccountActivity, AccountState, AssetState, EpochState, PoolDelegator, PoolState,
};

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

fn cert_to_stake_credential(cert: &MultiEraCert) -> Option<StakeCredential> {
    match cert {
        MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
            AlonzoCert::StakeRegistration(credential) => Some(credential.clone()),
            AlonzoCert::StakeDeregistration(credential) => Some(credential.clone()),
            AlonzoCert::StakeDelegation(credential, _) => Some(credential.clone()),

            _ => None,
        },
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::StakeRegistration(credential) => Some(credential.clone()),
            ConwayCert::StakeDeregistration(credential) => Some(credential.clone()),
            ConwayCert::StakeDelegation(credential, _) => Some(credential.clone()),
            _ => None,
        },
        _ => None,
    }
}

pub trait RollVisitor {
    #[allow(unused_variables)]
    fn visit_block(
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        block: &MultiEraBlock,
    ) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_output(
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_mint(
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_cert(
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        Ok(())
    }
}

pub fn crawl_block<'a, T: RollVisitor>(
    delta: &mut StateDelta,
    state: &impl State3Store,
    block: &MultiEraBlock<'a>,
    visitor: &T,
) -> Result<(), State3Error> {
    visitor.visit_block(state, delta, block)?;

    for tx in block.txs() {
        for (index, output) in tx.outputs().iter().enumerate() {
            visitor.visit_output(state, delta, block, &tx, index as u32, output)?;
        }

        for mint in tx.mints() {
            visitor.visit_mint(state, delta, block, &tx, &mint)?;
        }

        for cert in tx.certs() {
            visitor.visit_cert(state, delta, block, &tx, &cert)?;
        }
    }

    Ok(())
}

struct SeenAddressesVisitor;

impl RollVisitor for SeenAddressesVisitor {
    fn visit_output(
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        _: &MultiEraBlock,
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
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        _: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        let policy = mint.policy();

        for asset in mint.assets() {
            let mut subject = vec![];
            subject.extend_from_slice(policy.as_slice());
            subject.extend_from_slice(asset.name());

            debug!(subject = %hex::encode(&subject), "tracking asset");

            let tx_hash = tx.hash();

            let current = state
                .read_entity_typed::<AssetState>(&subject)?
                .unwrap_or(AssetState {
                    quantity_bytes: 0_u128.to_be_bytes(),
                    initial_tx: tx_hash,
                    latest_tx: tx_hash,
                    mint_tx_count: 0,
                });

            let mut new = current.clone();
            new.add_quantity(asset.mint_coin().unwrap_or_default().into())?;
            new.mint_tx_count += 1;
            new.latest_tx = tx_hash;
            delta.override_entity(subject, new, Some(current));
        }

        Ok(())
    }
}

struct PoolStateVisitor;

impl RollVisitor for PoolStateVisitor {
    fn visit_cert(
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some((operator, new)) = cert_to_pool_state(cert) {
            let current = state.read_entity_typed::<PoolState>(operator)?;
            delta.override_entity(operator.to_vec(), new, current);
        }

        Ok(())
    }
}

struct PoolDelegatorVisitor;

impl RollVisitor for PoolDelegatorVisitor {
    fn visit_cert(
        &self,
        _: &impl State3Store,
        delta: &mut StateDelta,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some((operator, new)) = cert_to_pool_delegator(cert) {
            debug!(%operator, "new pool delegator");
            delta.append_entity(operator.as_slice(), new);
        }

        Ok(())
    }
}

struct EpochStateVisitor;

impl RollVisitor for EpochStateVisitor {
    fn visit_block(
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        block: &MultiEraBlock,
    ) -> Result<(), State3Error> {
        let current = state
            .read_entity_typed::<EpochState>(crate::model::CURRENT_EPOCH_KEY)?
            .unwrap_or_default();

        let block_fees = block.txs().iter().filter_map(|tx| tx.fee()).sum::<u64>();

        let new = EpochState {
            gathered_fees: Some(current.gathered_fees.unwrap_or_default() + block_fees),
            ..current
        };

        delta.override_entity(crate::model::CURRENT_EPOCH_KEY, new, None);

        Ok(())
    }
}

struct AccountActivityVisitor;

impl RollVisitor for AccountActivityVisitor {
    fn visit_cert(
        &self,
        _: &impl State3Store,
        delta: &mut StateDelta,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        let credential = cert_to_stake_credential(cert);

        if let Some(credential) = credential {
            let key = match credential {
                StakeCredential::ScriptHash(x) => x.to_vec(),
                StakeCredential::AddrKeyhash(x) => x.to_vec(),
            };

            let value = AccountActivity(block.slot());

            delta.append_entity(key, value);
        }

        Ok(())
    }
}

macro_rules! visit_block {
    ($self:ident, $visitor:ident, $state:ident, $delta:ident, $block:ident) => {{
        if $self.config.$visitor {
            $self.$visitor.visit_block($state, $delta, $block)?;
        }
    }};
}

macro_rules! visit_output {
    ($self:ident, $visitor:ident, $state:ident, $delta:ident, $block:ident, $tx:ident, $index:ident, $output:ident) => {{
        if $self.config.$visitor {
            $self
                .$visitor
                .visit_output($state, $delta, $block, $tx, $index, $output)?;
        }
    }};
}

macro_rules! visit_mint {
    ($self:ident, $visitor:ident, $state:ident, $delta:ident, $block:ident, $tx:ident, $mint:ident) => {{
        if $self.config.$visitor {
            $self
                .$visitor
                .visit_mint($state, $delta, $block, $tx, $mint)?;
        }
    }};
}

macro_rules! visit_cert {
    ($self:ident, $visitor:ident, $state:ident, $delta:ident, $block:ident, $tx:ident, $cert:ident) => {{
        if $self.config.$visitor {
            $self
                .$visitor
                .visit_cert($state, $delta, $block, $tx, $cert)?;
        }
    }};
}

pub struct DynamicVisitor {
    config: super::TrackConfig,
    seen_addresses: SeenAddressesVisitor,
    asset_state: AssetStateVisitor,
    pool_state: PoolStateVisitor,
    pool_delegator: PoolDelegatorVisitor,
    epoch_state: EpochStateVisitor,
    account_activity: AccountActivityVisitor,
}

impl DynamicVisitor {
    pub fn new(config: super::TrackConfig) -> Self {
        Self {
            config,
            seen_addresses: SeenAddressesVisitor,
            asset_state: AssetStateVisitor,
            pool_state: PoolStateVisitor,
            pool_delegator: PoolDelegatorVisitor,
            epoch_state: EpochStateVisitor,
            account_activity: AccountActivityVisitor,
        }
    }
}

impl RollVisitor for DynamicVisitor {
    fn visit_block(
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        block: &MultiEraBlock,
    ) -> Result<(), State3Error> {
        visit_block!(self, seen_addresses, state, delta, block);
        visit_block!(self, asset_state, state, delta, block);
        visit_block!(self, pool_state, state, delta, block);
        visit_block!(self, pool_delegator, state, delta, block);
        visit_block!(self, epoch_state, state, delta, block);
        visit_block!(self, account_activity, state, delta, block);

        Ok(())
    }

    fn visit_output(
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        visit_output!(self, seen_addresses, state, delta, block, tx, index, output);
        visit_output!(self, asset_state, state, delta, block, tx, index, output);
        visit_output!(self, pool_state, state, delta, block, tx, index, output);
        visit_output!(self, pool_delegator, state, delta, block, tx, index, output);
        visit_output!(self, epoch_state, state, delta, block, tx, index, output);
        visit_output!(
            self,
            account_activity,
            state,
            delta,
            block,
            tx,
            index,
            output
        );

        Ok(())
    }

    fn visit_mint(
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        visit_mint!(self, seen_addresses, state, delta, block, tx, mint);
        visit_mint!(self, asset_state, state, delta, block, tx, mint);
        visit_mint!(self, pool_state, state, delta, block, tx, mint);
        visit_mint!(self, pool_delegator, state, delta, block, tx, mint);
        visit_mint!(self, epoch_state, state, delta, block, tx, mint);
        visit_mint!(self, account_activity, state, delta, block, tx, mint);

        Ok(())
    }

    fn visit_cert(
        &self,
        state: &impl State3Store,
        delta: &mut StateDelta,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        visit_cert!(self, seen_addresses, state, delta, block, tx, cert);
        visit_cert!(self, asset_state, state, delta, block, tx, cert);
        visit_cert!(self, pool_state, state, delta, block, tx, cert);
        visit_cert!(self, pool_delegator, state, delta, block, tx, cert);
        visit_cert!(self, epoch_state, state, delta, block, tx, cert);
        visit_cert!(self, account_activity, state, delta, block, tx, cert);

        Ok(())
    }
}
