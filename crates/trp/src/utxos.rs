use std::collections::HashSet;

use tx3_lang::{
    backend::{UtxoPattern, UtxoStore},
    UtxoRef, UtxoSet,
};

use dolos_core::{Domain, StateStore as _, TxoRef};

use crate::{
    mapping::{from_tx3_utxoref, into_tx3_utxo, into_tx3_utxoref},
    Error,
};

pub struct UtxoStoreAdapter<D: Domain>(D::State);

impl<D: Domain> UtxoStoreAdapter<D> {
    pub fn new(state: D::State) -> Self {
        Self(state)
    }

    fn state(&self) -> &D::State {
        &self.0
    }

    fn match_utxos_by_address(&self, address: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        let utxos = self.state().get_utxo_by_address(address)?;

        Ok(utxos)
    }

    fn match_utxos_by_asset_policy(&self, policy: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        let utxos = self.state().get_utxo_by_policy(policy)?;

        Ok(utxos)
    }

    fn match_utxos_by_asset(&self, policy: &[u8], name: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        let subject = [policy, name].concat();

        let utxos = self.state().get_utxo_by_asset(&subject)?;

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

    async fn get_tip(&self) -> Result<u64, tx3_lang::backend::Error> {
        let cursor = self.state().cursor().map_err(Error::from)?;
        Ok(cursor.unwrap().slot())
    }
}
