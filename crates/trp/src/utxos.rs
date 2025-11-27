use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use tx3_lang::{
    backend::{UtxoPattern, UtxoStore},
    UtxoRef, UtxoSet,
};

use dolos_core::{Domain, StateStore as _, TxoRef};

use crate::{
    mapping::{from_tx3_utxoref, into_tx3_utxo, into_tx3_utxoref},
    Error,
};

#[derive(Default)]
pub struct UtxoLock {
    locks: RwLock<HashMap<TxoRef, u64>>,
}

const SLOTS_BETWEEN_BLOCKS: u64 = 20;
const LOCK_DURATION_BLOCKS: u64 = 3;
const LOCK_DURATION_SLOTS: u64 = SLOTS_BETWEEN_BLOCKS * LOCK_DURATION_BLOCKS;

impl UtxoLock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn lock(&self, refs: &[TxoRef], current_slot: u64) {
        let mut locks = self.locks.write().unwrap();

        locks.retain(|_, expiration| *expiration > current_slot);

        for r in refs {
            locks.insert(r.clone(), current_slot + LOCK_DURATION_SLOTS);
        }
    }

    pub fn is_locked(&self, txo: &TxoRef, current_slot: u64) -> bool {
        let locks = self.locks.read().unwrap();
        if let Some(expiration) = locks.get(txo) {
            return *expiration > current_slot;
        }
        false
    }
}

pub struct UtxoStoreAdapter<D: Domain> {
    state: D::State,
    locks: Arc<UtxoLock>,
}

impl<D: Domain> UtxoStoreAdapter<D> {
    pub fn new(state: D::State, locks: Arc<UtxoLock>) -> Self {
        Self { state, locks }
    }

    fn state(&self) -> &D::State {
        &self.state
    }

    fn is_locked(&self, txo: &TxoRef) -> bool {
        let current_slot = self
            .state()
            .cursor()
            .ok()
            .flatten()
            .map(|c| c.slot())
            .unwrap_or(0);

        self.locks.is_locked(txo, current_slot)
    }

    fn match_utxos_by_address(&self, address: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        let utxos = self.state().get_utxo_by_address(address)?;

        // Remove locked UTXOs
        let utxos = utxos.into_iter().filter(|u| !self.is_locked(u)).collect();

        Ok(utxos)
    }

    fn match_utxos_by_asset_policy(&self, policy: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        let utxos = self.state().get_utxo_by_policy(policy)?;

        // Remove locked UTXOs
        let utxos = utxos.into_iter().filter(|u| !self.is_locked(u)).collect();

        Ok(utxos)
    }

    fn match_utxos_by_asset(&self, policy: &[u8], name: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        let subject = [policy, name].concat();

        let utxos = self.state().get_utxo_by_asset(&subject)?;

        // Remove locked UTXOs
        let utxos = utxos.into_iter().filter(|u| !self.is_locked(u)).collect();

        Ok(utxos)
    }
}

impl<D: Domain> UtxoStore for UtxoStoreAdapter<D> {
    async fn narrow_refs(
        &self,
        pattern: UtxoPattern<'_>,
    ) -> Result<HashSet<UtxoRef>, tx3_lang::backend::Error> {
        let refs = match pattern {
            UtxoPattern::ByAddress(address) => self.match_utxos_by_address(address),
            UtxoPattern::ByAssetPolicy(policy) => self.match_utxos_by_asset_policy(policy),
            UtxoPattern::ByAsset(policy, name) => self.match_utxos_by_asset(policy, name),
        }?;

        let mapped = refs.into_iter().map(into_tx3_utxoref).collect();

        Ok(mapped)
    }

    async fn fetch_utxos(
        &self,
        refs: HashSet<UtxoRef>,
    ) -> Result<UtxoSet, tx3_lang::backend::Error> {
        let refs: Vec<_> = refs.into_iter().map(from_tx3_utxoref).collect();

        let utxos = self.state().get_utxos(refs).map_err(Error::from)?;

        let utxos = utxos
            .into_iter()
            .map(|(txoref, utxo)| into_tx3_utxo(txoref, utxo))
            .collect::<Result<_, _>>()?;

        Ok(utxos)
    }
}
