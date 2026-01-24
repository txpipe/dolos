use std::collections::HashSet;

use dolos_cardano::indexes::utxo_dimensions;
use dolos_core::{Domain, IndexError, MempoolAwareUtxoStore, TxoRef};
use pallas::ledger::traverse::MultiEraOutput;
use tx3_resolver::{Error as Tx3Error, UtxoPattern, UtxoRef, UtxoSet, UtxoStore};

use crate::{
    mapping::{from_tx3_utxoref, into_tx3_utxo, into_tx3_utxoref},
    Error,
};

fn search_state_utxos<D: Domain>(
    pattern: &UtxoPattern<'_>,
    store: &MempoolAwareUtxoStore<D>,
) -> Result<HashSet<TxoRef>, IndexError> {
    // Dummy filter that always returns true (we want all UTxOs matching the index)
    let no_filter = |_: &MultiEraOutput<'_>| true;

    let refs = match pattern {
        UtxoPattern::ByAddress(address) => {
            store.get_utxos_by_tag(utxo_dimensions::ADDRESS, address, no_filter)?
        }
        UtxoPattern::ByAssetPolicy(policy) => {
            store.get_utxos_by_tag(utxo_dimensions::POLICY, policy, no_filter)?
        }
        UtxoPattern::ByAsset(policy, name) => {
            let subject = [*policy, *name].concat();
            store.get_utxos_by_tag(utxo_dimensions::ASSET, &subject, no_filter)?
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

    async fn narrow_refs(&self, pattern: UtxoPattern<'_>) -> Result<HashSet<UtxoRef>, Error> {
        let refs = search_state_utxos::<D>(&pattern, &self.inner)?;

        let mapped = refs.into_iter().map(into_tx3_utxoref).collect();

        Ok(mapped)
    }

    async fn fetch_utxos(&self, refs: HashSet<UtxoRef>) -> Result<UtxoSet, Error> {
        let refs: HashSet<_> = refs.into_iter().map(from_tx3_utxoref).collect();

        let utxos = self.inner.get_utxos(refs)?;

        let utxos = utxos
            .into_iter()
            .map(|(txoref, utxo)| into_tx3_utxo(txoref, utxo))
            .collect::<Result<_, _>>()?;

        Ok(utxos)
    }
}

impl<'a, D: Domain> UtxoStore for UtxoStoreAdapter<'a, D> {
    async fn narrow_refs(&self, pattern: UtxoPattern<'_>) -> Result<HashSet<UtxoRef>, Tx3Error> {
        self.narrow_refs(pattern)
            .await
            .map_err(|e| Tx3Error::StoreError(e.to_string()))
    }

    async fn fetch_utxos(&self, refs: HashSet<UtxoRef>) -> Result<UtxoSet, Tx3Error> {
        self.fetch_utxos(refs)
            .await
            .map_err(|e| Tx3Error::StoreError(e.to_string()))
    }
}
