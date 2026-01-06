use std::collections::HashSet;

use dolos_core::{Domain, MempoolAwareUtxoStore, StateError, TxoRef};
use tx3_resolver::{Error, UtxoPattern, UtxoStore};
use tx3_tir::model::v1beta0 as tir;

use crate::mapping::{from_tx3_utxoref, into_tx3_utxo, into_tx3_utxoref};

fn search_state_utxos<D: Domain>(
    pattern: &UtxoPattern<'_>,
    store: &MempoolAwareUtxoStore<D>,
) -> Result<HashSet<TxoRef>, StateError> {
    let refs = match pattern {
        UtxoPattern::ByAddress(address) => store.get_utxo_by_address(address)?,
        UtxoPattern::ByAssetPolicy(policy) => store.get_utxo_by_policy(policy)?,
        UtxoPattern::ByAsset(policy, name) => {
            let subject = [*policy, *name].concat();
            store.get_utxo_by_asset(&subject)?
        }
    };

    Ok(refs)
}

pub struct UtxoStoreAdapter<'a, D: Domain> {
    inner: MempoolAwareUtxoStore<'a, D>,
}

impl<'a, D: Domain> UtxoStoreAdapter<'a, D> {
    pub fn new(inner: MempoolAwareUtxoStore<'a, D>) -> Self {
        Self { inner }
    }
}

impl<'a, D: Domain> UtxoStore for UtxoStoreAdapter<'a, D> {
    async fn narrow_refs(&self, pattern: UtxoPattern<'_>) -> Result<HashSet<tir::UtxoRef>, Error> {
        let refs = search_state_utxos::<D>(&pattern, &self.inner)
            .map_err(|e| Error::StoreError(e.to_string()))?;

        let mapped = refs.into_iter().map(into_tx3_utxoref).collect();

        Ok(mapped)
    }

    async fn fetch_utxos(&self, refs: HashSet<tir::UtxoRef>) -> Result<tir::UtxoSet, Error> {
        let refs: HashSet<_> = refs.into_iter().map(from_tx3_utxoref).collect();

        let utxos = self
            .inner
            .get_utxos(refs)
            .map_err(|e| Error::StoreError(e.to_string()))?;

        let utxos = utxos
            .into_iter()
            .map(|(txoref, utxo)| into_tx3_utxo(txoref, utxo))
            .collect::<Result<_, _>>()?;

        Ok(utxos)
    }
}
