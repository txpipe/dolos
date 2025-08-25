use std::ops::Deref;

use dolos_core::{State3Error, State3Store, StateDelta, StateSlice, StateSliceView};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::{Address, StakeAddress},
        primitives::StakeCredential,
        traverse::{MultiEraBlock, MultiEraCert, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx},
    },
};

use tracing::debug;

use crate::{
    model::{AccountActivity, AccountState, AssetState, EpochState, PoolDelegator, PoolState},
    pallas_extras,
};

use super::TrackConfig;
pub trait BlockVisitor {
    #[allow(unused_variables)]
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_output(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_mint(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_cert(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        Ok(())
    }
}

pub fn crawl_block<'a, T: BlockVisitor>(
    block: &MultiEraBlock<'a>,
    visitor: &mut T,
) -> Result<(), State3Error> {
    visitor.visit_root(block)?;

    for tx in block.txs() {
        for (index, output) in tx.outputs().iter().enumerate() {
            visitor.visit_output(block, &tx, index as u32, output)?;
        }

        for mint in tx.mints() {
            visitor.visit_mint(block, &tx, &mint)?;
        }

        for cert in tx.certs() {
            visitor.visit_cert(block, &tx, &cert)?;
        }
    }

    Ok(())
}

macro_rules! maybe_visit {
    ($self:expr, $config:ident, $type:tt, $method:ident, $($args:tt)*) => {{
        if $self.config.$config {
            $type($self).$method($($args)*)?;
        }
    }};
}

macro_rules! visit_all {
    ($self:ident, $method:ident, $($args:tt)*) => {
        maybe_visit!($self, seen_addresses, SeenAddressesVisitor, $method, $($args)*);
        maybe_visit!($self, asset_state, AssetStateVisitor, $method, $($args)*);
        maybe_visit!($self, pool_state, PoolStateVisitor, $method, $($args)*);
        maybe_visit!($self, pool_delegator, PoolDelegatorVisitor, $method, $($args)*);
        maybe_visit!($self, epoch_state, EpochStateVisitor, $method, $($args)*);
        maybe_visit!($self, account_activity, AccountActivityVisitor, $method, $($args)*);
    };
}

pub struct DeltaBuilder<'a> {
    config: &'a TrackConfig,
    state: StateSliceView<'a>,
    delta: StateDelta,
}

impl<'a> DeltaBuilder<'a> {
    pub fn new(config: &'a TrackConfig, state: StateSliceView<'a>, delta: StateDelta) -> Self {
        Self {
            config,
            state,
            delta,
        }
    }

    pub fn delta_mut(&mut self) -> &mut StateDelta {
        &mut self.delta
    }

    pub fn slice(&self) -> &StateSliceView<'a> {
        &self.state
    }

    pub fn build(self) -> StateDelta {
        self.delta
    }
}

impl<'a> BlockVisitor for DeltaBuilder<'a> {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        visit_all!(self, visit_root, block);

        Ok(())
    }

    fn visit_output(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        visit_all!(self, visit_output, block, tx, index, output);

        Ok(())
    }

    fn visit_mint(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        visit_all!(self, visit_mint, block, tx, mint);

        Ok(())
    }

    fn visit_cert(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        visit_all!(self, visit_cert, block, tx, cert);

        Ok(())
    }
}

pub struct SliceBuilder<'a, S: State3Store> {
    config: &'a TrackConfig,
    store: &'a S,
    slice: StateSliceView<'a>,
}

impl<'a, S: State3Store> SliceBuilder<'a, S> {
    pub fn new(config: &'a TrackConfig, store: &'a S, unapplied_deltas: &'a [StateDelta]) -> Self {
        Self {
            config,
            store,
            slice: StateSliceView::new(StateSlice::default(), unapplied_deltas),
        }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn build(self) -> StateSlice {
        self.slice.unwrap()
    }
}

impl<'a, S: State3Store> BlockVisitor for SliceBuilder<'a, S> {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        visit_all!(self, visit_root, block);

        Ok(())
    }

    fn visit_output(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        visit_all!(self, visit_output, block, tx, index, output);

        Ok(())
    }

    fn visit_mint(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        visit_all!(self, visit_mint, block, tx, mint);

        Ok(())
    }

    fn visit_cert(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        visit_all!(self, visit_cert, block, tx, cert);

        Ok(())
    }
}

struct SeenAddressesVisitor<'a, T>(&'a mut T);

impl<T> SeenAddressesVisitor<'_, T> {
    fn extract_address(output: &MultiEraOutput) -> Option<(StakeAddress, Address)> {
        let full = output.address().ok()?;

        let stake = match &full {
            Address::Shelley(x) => StakeAddress::try_from(x.clone()).ok(),
            Address::Stake(x) => Some(x.clone()),
            _ => None,
        }?;

        Some((stake, full))
    }
}

impl<'a, S: State3Store> BlockVisitor for SeenAddressesVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_output(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        let Some((stake, _)) = Self::extract_address(output) else {
            return Ok(());
        };

        let stake_bytes = stake.to_vec();

        self.0
            .slice
            .ensure_loaded_typed::<AccountState>(&stake_bytes, self.0.store)?;

        Ok(())
    }
}

impl<'a> BlockVisitor for SeenAddressesVisitor<'a, DeltaBuilder<'_>> {
    fn visit_output(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        let Some((stake, full_address)) = Self::extract_address(output) else {
            return Ok(());
        };

        let stake_bytes = stake.to_vec();

        let current = self
            .0
            .state
            .get_entity_typed::<AccountState>(&stake_bytes)?;

        if let Some(current) = current {
            let mut new = current.clone();
            new.seen_addresses.insert(full_address.to_vec());
            self.0
                .delta_mut()
                .override_entity(stake_bytes, new, Some(current));
        } else {
            let mut new = AccountState::default();
            new.seen_addresses.insert(full_address.to_vec());
            self.0.delta_mut().override_entity(stake_bytes, new, None);
        }

        Ok(())
    }
}

struct AssetStateVisitor<'a, T>(&'a mut T);

impl<T> AssetStateVisitor<'_, T> {
    fn define_subject(policy: &Hash<28>, asset: &[u8]) -> Vec<u8> {
        let mut subject = vec![];
        subject.extend_from_slice(policy.as_slice());
        subject.extend_from_slice(asset);

        subject
    }
}

impl<'a, S: State3Store> BlockVisitor for AssetStateVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_mint(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        let policy = mint.policy();

        for asset in mint.assets() {
            let subject = Self::define_subject(policy, asset.name());

            self.0
                .slice
                .ensure_loaded_typed::<AssetState>(&subject, self.0.store)?;
        }

        Ok(())
    }
}

impl<'a> BlockVisitor for AssetStateVisitor<'a, DeltaBuilder<'_>> {
    fn visit_mint(
        &mut self,
        _: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        let policy = mint.policy();

        for asset in mint.assets() {
            let subject = Self::define_subject(policy, asset.name());

            debug!(subject = %hex::encode(&subject), "tracking asset");

            let current = self
                .0
                .slice()
                .get_entity_typed::<AssetState>(&subject)?
                .unwrap_or(AssetState {
                    quantity_bytes: 0_u128.to_be_bytes(),
                    initial_tx: tx.hash(),
                    mint_tx_count: 0,
                });

            let mut new = current.clone();
            new.add_quantity(asset.mint_coin().unwrap_or_default().into())?;
            new.mint_tx_count += 1;

            self.0
                .delta_mut()
                .override_entity(subject, new, Some(current));
        }

        Ok(())
    }
}

struct PoolStateVisitor<'a, T>(&'a mut T);

impl<'a, S: State3Store> BlockVisitor for PoolStateVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_cert(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(cert) = pallas_extras::cert_to_pool_state(cert) {
            self.0
                .slice
                .ensure_loaded_typed::<PoolState>(cert.operator, self.0.store)?;
        }

        Ok(())
    }
}

impl<'a> BlockVisitor for PoolStateVisitor<'a, DeltaBuilder<'_>> {
    fn visit_cert(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(cert) = pallas_extras::cert_to_pool_state(cert) {
            let current = self
                .0
                .slice()
                .get_entity_typed::<PoolState>(cert.operator)?;

            let entity = PoolState {
                active_stake: 0,
                live_stake: 0,
                blocks_minted: 0,
                live_saturation: 0.0,
                vrf_keyhash: cert.vrf_keyhash,
                reward_account: cert.reward_account.to_vec(),
                pool_owners: cert.pool_owners.clone(),
                relays: cert.relays.clone(),
                declared_pledge: cert.pledge,
                margin_cost: cert.margin.clone(),
                fixed_cost: cert.cost,
                metadata: cert.pool_metadata.clone(),
            };

            self.0
                .delta_mut()
                .override_entity(cert.operator.as_slice(), entity, current);
        }

        Ok(())
    }
}

struct PoolDelegatorVisitor<'a, T>(&'a mut T);

impl<'a, S: State3Store> BlockVisitor for PoolDelegatorVisitor<'a, SliceBuilder<'_, S>> {}

impl<'a> BlockVisitor for PoolDelegatorVisitor<'a, DeltaBuilder<'_>> {
    fn visit_cert(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(cert) = pallas_extras::cert_as_stake_delegation(cert) {
            debug!(%cert.pool, "new pool delegator");

            let entity = PoolDelegator(cert.delegator);

            self.0
                .delta_mut()
                .append_entity(cert.pool.as_slice(), entity);
        }

        Ok(())
    }
}

struct EpochStateVisitor<'a, T>(&'a mut T);

impl<'a, S: State3Store> BlockVisitor for EpochStateVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_root(&mut self, _: &MultiEraBlock) -> Result<(), State3Error> {
        self.0
            .slice
            .ensure_loaded_typed::<EpochState>(crate::model::CURRENT_EPOCH_KEY, self.0.store)?;

        Ok(())
    }
}

impl<'a> BlockVisitor for EpochStateVisitor<'a, DeltaBuilder<'_>> {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        let current = self
            .0
            .slice()
            .get_entity_typed::<EpochState>(crate::model::CURRENT_EPOCH_KEY)?
            .unwrap_or_default();

        let block_fees = block.txs().iter().filter_map(|tx| tx.fee()).sum::<u64>();

        let new = EpochState {
            gathered_fees: Some(current.gathered_fees.unwrap_or_default() + block_fees),
            ..current
        };

        self.0
            .delta_mut()
            .override_entity(crate::model::CURRENT_EPOCH_KEY, new, None);

        Ok(())
    }
}

struct AccountActivityVisitor<'a, T>(&'a mut T);

impl<'a, S: State3Store> BlockVisitor for AccountActivityVisitor<'a, SliceBuilder<'_, S>> {}

impl<'a> BlockVisitor for AccountActivityVisitor<'a, DeltaBuilder<'_>> {
    fn visit_cert(
        &mut self,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        let credential = pallas_extras::cert_as_stake_registration(cert);

        if let Some(credential) = credential {
            let key = match credential {
                StakeCredential::ScriptHash(x) => x.to_vec(),
                StakeCredential::AddrKeyhash(x) => x.to_vec(),
            };

            let value = AccountActivity(block.slot());

            self.0.delta_mut().append_entity(key, value);
        }

        Ok(())
    }
}
