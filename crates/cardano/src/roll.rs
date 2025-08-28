use std::collections::HashMap;
use std::ops::Deref;

use dolos_core::{
    EraCbor, InvariantViolation, LedgerSlice, State3Error, State3Store, StateDelta, StateSlice,
    StateSliceView, TxoRef,
};
use pallas::{
    crypto::hash::{Hash, Hasher},
    ledger::traverse::{
        Era, MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraPolicyAssets,
        MultiEraTx,
    },
    ledger::{
        addresses::{Address, Network, StakeAddress},
        primitives::{conway, StakeCredential},
    },
};

use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};
use tracing::debug;

use crate::{
    model::{
        AccountActivity, AccountState, AssetState, DRepState, EpochState, PoolDelegator, PoolState,
    },
    pallas_extras,
    pparams::ChainSummary,
};

use super::TrackConfig;

pub trait BlockVisitor {
    #[allow(unused_variables)]
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_input(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        input: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), State3Error> {
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

fn load_input<'a>(
    input: MultiEraInput<'a>,
    utxo_slice: &'a LedgerSlice,
) -> Result<(MultiEraInput<'a>, MultiEraOutput<'a>), State3Error> {
    let txoref = TxoRef::from(&input);

    let EraCbor(era, cbor) = utxo_slice
        .resolved_inputs
        .get(&txoref)
        .ok_or(InvariantViolation::InputNotFound(txoref))?;

    let era = Era::try_from(*era)?;

    let resolved = MultiEraOutput::decode(era, cbor)?;

    Ok((input, resolved))
}

pub fn crawl_block<'a, T: BlockVisitor>(
    block: &MultiEraBlock<'a>,
    utxo_slice: &LedgerSlice,
    visitor: &mut T,
) -> Result<(), State3Error> {
    visitor.visit_root(block)?;

    for tx in block.txs() {
        let consumed = tx
            .consumes()
            .into_par_iter()
            .map(|input| load_input(input, utxo_slice))
            .collect::<Result<Vec<_>, _>>()?;

        for (input, resolved) in consumed {
            visitor.visit_input(block, &tx, &input, &resolved)?;
        }

        for (index, output) in tx.produces() {
            visitor.visit_output(block, &tx, index as u32, &output)?;
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
        maybe_visit!($self, drep_state, DRepStateVisitor, $method, $($args)*);
    };
}

pub struct DeltaBuilder<'a> {
    config: &'a TrackConfig,
    state: StateSliceView<'a>,
    delta: StateDelta,
    network: Network,
}

impl<'a> DeltaBuilder<'a> {
    pub fn new(
        config: &'a TrackConfig,
        state: StateSliceView<'a>,
        delta: StateDelta,
        network: Network,
    ) -> Self {
        Self {
            config,
            state,
            delta,
            network,
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

    fn visit_input(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        input: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        visit_all!(self, visit_input, block, tx, input, resolved);

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
    network: Network,
    chain_summary: std::sync::Arc<ChainSummary>,
}

impl<'a, S: State3Store> SliceBuilder<'a, S> {
    pub fn new(
        config: &'a TrackConfig,
        store: &'a S,
        unapplied_deltas: &'a [StateDelta],
        network: Network,
        chain_summary: std::sync::Arc<ChainSummary>,
    ) -> Self {
        Self {
            config,
            store,
            slice: StateSliceView::new(StateSlice::default(), unapplied_deltas),
            network,
            chain_summary,
        }
    }

    pub fn store(&self) -> &S {
        self.store
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
    fn visit_input(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        let Some((stake, _)) = Self::extract_address(resolved) else {
            return Ok(());
        };

        let stake_bytes = stake.to_vec();

        self.0
            .slice
            .ensure_loaded_typed::<AccountState>(&stake_bytes, self.0.store)?;

        Ok(())
    }

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
    fn visit_input(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        let Some((stake, _)) = Self::extract_address(resolved) else {
            return Ok(());
        };

        let stake_bytes = stake.to_vec();

        let current = self
            .0
            .state
            .get_entity_typed::<AccountState>(&stake_bytes)?;

        let mut new = current.clone().unwrap_or_default();

        // TODO: refactor into CRDT
        // TODO: check same-crawl delta changes
        // TODO: saturating sub shouldn't be necesary on the long run, it should be
        // treated as a invariant violation
        new.controlled_amount = new
            .controlled_amount
            .saturating_sub(resolved.value().coin());

        self.0
            .delta_mut()
            .override_entity(stake_bytes, new, current);

        Ok(())
    }

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

        let mut new = current.clone().unwrap_or_default();

        // TODO: refactor into CRDT
        // TODO: check same-crawl delta changes
        new.controlled_amount += output.value().coin();
        new.seen_addresses.insert(full_address.to_vec());

        self.0
            .delta_mut()
            .override_entity(stake_bytes, new, current);

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
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        if let Some(key) = block.header().issuer_vkey() {
            let operator: Hash<28> = Hasher::<224>::hash(key);
            self.0
                .slice
                .ensure_loaded_typed::<PoolState>(operator, self.0.store)?;
        }

        Ok(())
    }

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
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        if let Some(key) = block.header().issuer_vkey() {
            let operator: Hash<28> = Hasher::<224>::hash(key);
            if let Some(mut entity) = self.0.slice().get_entity_typed::<PoolState>(operator)? {
                let prev = entity.clone();
                entity.blocks_minted += 1;
                self.0
                    .delta_mut()
                    .override_entity(operator.as_slice(), entity, Some(prev));
            }
        }

        Ok(())
    }

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

impl<'a, S: State3Store> BlockVisitor for PoolDelegatorVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_cert(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(cred) = pallas_extras::cert_as_stake_registration(cert) {
            let stake_address = pallas_extras::stake_credential_to_address(self.0.network, &cred);

            let stake_bytes = stake_address.to_vec();

            self.0
                .slice
                .ensure_loaded_typed::<AccountState>(&stake_bytes, self.0.store)?;
        }

        if let Some(cred) = pallas_extras::cert_as_stake_deregistration(cert) {
            let stake_address = pallas_extras::stake_credential_to_address(self.0.network, &cred);

            let stake_bytes = stake_address.to_vec();

            self.0
                .slice
                .ensure_loaded_typed::<AccountState>(&stake_bytes, self.0.store)?;
        }

        if let Some(cert) = pallas_extras::cert_as_stake_delegation(cert) {
            let stake_address =
                pallas_extras::stake_credential_to_address(self.0.network, &cert.delegator);

            let stake_bytes = stake_address.to_vec();

            self.0
                .slice
                .ensure_loaded_typed::<AccountState>(&stake_bytes, self.0.store)?;
        }
        Ok(())
    }
}

impl<'a> BlockVisitor for PoolDelegatorVisitor<'a, DeltaBuilder<'_>> {
    fn visit_cert(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(cert) = pallas_extras::cert_as_stake_delegation(cert) {
            debug!(%cert.pool, "new pool delegator");

            let stake_address =
                pallas_extras::stake_credential_to_address(self.0.network, &cert.delegator);

            let stake_bytes = stake_address.to_vec();

            let current = self
                .0
                .slice()
                .get_entity_typed::<AccountState>(&stake_bytes)?;

            let mut new = current.clone().unwrap_or_default();

            new.pool_id = Some(cert.pool.to_vec());

            self.0
                .delta_mut()
                .override_entity(stake_bytes, new, current);

            let entity = PoolDelegator(cert.delegator);

            self.0
                .delta_mut()
                .append_entity(cert.pool.as_slice(), entity);
        }

        if let Some(credential) = pallas_extras::cert_as_stake_registration(cert) {
            debug!("stake registration");

            let stake_address =
                pallas_extras::stake_credential_to_address(self.0.network, &credential);

            let stake_bytes = stake_address.to_vec();

            let current = self
                .0
                .slice()
                .get_entity_typed::<AccountState>(&stake_bytes)?;

            let mut new = current.clone().unwrap_or_default();

            new.active_epoch = Some(1);

            self.0
                .delta_mut()
                .override_entity(stake_bytes, new, current);
        }

        if let Some(credential) = pallas_extras::cert_as_stake_deregistration(cert) {
            debug!("stake deregistration");

            let stake_address =
                pallas_extras::stake_credential_to_address(self.0.network, &credential);

            let stake_bytes = stake_address.to_vec();

            let current = self
                .0
                .slice()
                .get_entity_typed::<AccountState>(&stake_bytes)?;

            let mut new = current.clone().unwrap_or_default();

            new.pool_id = None;
            new.active_epoch = None;

            self.0
                .delta_mut()
                .override_entity(stake_bytes, new, current);
        }

        Ok(())
    }
}

struct EpochStateVisitor<'a, T>(&'a mut T);

impl<'a, S: State3Store> BlockVisitor for EpochStateVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        self.0
            .slice
            .ensure_loaded_typed::<EpochState>(crate::model::CURRENT_EPOCH_KEY, self.0.store)?;

        let cursor = self.0.store().get_cursor()?;

        let should_compute =
            pallas_extras::is_epoch_boundary(&self.0.chain_summary, cursor, block.slot());

        if should_compute {
            let mut by_pool = HashMap::<[u8; 28], u128>::new();

            let all_accounts = self.0.store().iter_entities_typed::<AccountState>(
                &[0u8; 32].as_slice()..&[255u8; 32].as_slice(),
            )?;

            for record in all_accounts {
                let (_, value) = record?;

                if let Some(pool_id) = value.pool_id {
                    let key = pool_id.try_into().unwrap();
                    let entry = by_pool.entry(key).or_insert(0);
                    *entry += value.controlled_amount as u128;
                }
            }
        }

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

const DREP_KEY_PREFIX: u8 = 0b00100010;
const DREP_SCRIPT_PREFIX: u8 = 0b00100011;

struct DRepStateVisitor<'a, T>(&'a mut T);
impl<T> DRepStateVisitor<'_, T> {
    fn cred_to_id(cred: &StakeCredential) -> Vec<u8> {
        match cred {
            StakeCredential::AddrKeyhash(key) => [vec![DREP_KEY_PREFIX], key.to_vec()].concat(),
            StakeCredential::ScriptHash(key) => [vec![DREP_SCRIPT_PREFIX], key.to_vec()].concat(),
        }
    }

    fn drep_to_id(drep: &conway::DRep) -> Vec<u8> {
        match drep {
            conway::DRep::Key(key) => [vec![DREP_KEY_PREFIX], key.to_vec()].concat(),
            conway::DRep::Script(key) => [vec![DREP_SCRIPT_PREFIX], key.to_vec()].concat(),
            // Invented keys for convenience
            conway::DRep::Abstain => vec![0],
            conway::DRep::NoConfidence => vec![1],
        }
    }

    fn cert_to_id(cert: &MultiEraCert) -> Option<Vec<u8>> {
        match &cert {
            MultiEraCert::Conway(conway) => match conway.deref().deref() {
                conway::Certificate::RegDRepCert(cert, _, _) => Some(Self::cred_to_id(cert)),
                conway::Certificate::UnRegDRepCert(cert, _) => Some(Self::cred_to_id(cert)),
                conway::Certificate::UpdateDRepCert(cert, _) => Some(Self::cred_to_id(cert)),
                conway::Certificate::StakeVoteDeleg(_, _, drep) => Some(Self::drep_to_id(drep)),
                conway::Certificate::VoteRegDeleg(_, drep, _) => Some(Self::drep_to_id(drep)),
                conway::Certificate::VoteDeleg(_, drep) => Some(Self::drep_to_id(drep)),
                _ => None,
            },
            _ => None,
        }
    }
}

impl<'a, S: State3Store> BlockVisitor for DRepStateVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_cert(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(id) = Self::cert_to_id(cert) {
            self.0
                .slice
                .ensure_loaded_typed::<DRepState>(&id, self.0.store)?;
        }

        Ok(())
    }
}

impl<'a> BlockVisitor for DRepStateVisitor<'a, DeltaBuilder<'_>> {
    fn visit_cert(
        &mut self,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(drep_id) = Self::cert_to_id(cert) {
            let current = self
                .0
                .slice()
                .get_entity_typed::<DRepState>(&drep_id)?
                .unwrap_or(DRepState {
                    drep_id: drep_id.clone(),
                    initial_slot: Some(block.slot()),
                    voting_power: 0,
                    last_active_slot: None,
                    retired: false,
                });
            let mut new = current.clone();
            new.last_active_slot = Some(block.slot());

            if let MultiEraCert::Conway(conway) = &cert {
                match conway.deref().deref() {
                    conway::Certificate::RegDRepCert(_, coin, _) => {
                        new.voting_power += coin;
                        new.retired = false;
                    }
                    conway::Certificate::UnRegDRepCert(_, coin) => {
                        new.voting_power -= coin;
                        new.retired = true;
                    }
                    conway::Certificate::VoteRegDeleg(_, _, coin) => {
                        new.voting_power += coin;
                    }
                    _ => (),
                }
            };

            self.0
                .delta_mut()
                .override_entity(drep_id, new, Some(current));
        }

        Ok(())
    }
}
