use std::collections::HashSet;

use tx3_lang::{
    backend::{UtxoPattern, UtxoStore},
    UtxoRef, UtxoSet,
};

use dolos_core::{Domain, MempoolAwareUtxoStore, TxoRef};

use crate::{
    mapping::{from_tx3_utxoref, into_tx3_utxo, into_tx3_utxoref},
    Error,
};

fn search_state_utxos<D: Domain>(
    pattern: &UtxoPattern<'_>,
    store: &MempoolAwareUtxoStore<D>,
) -> Result<HashSet<TxoRef>, Error> {
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
    async fn narrow_refs(
        &self,
        pattern: UtxoPattern<'_>,
    ) -> Result<HashSet<UtxoRef>, tx3_lang::backend::Error> {
        let refs = search_state_utxos::<D>(&pattern, &self.inner)?;

        let mapped = refs.into_iter().map(into_tx3_utxoref).collect();

        Ok(mapped)
    }

    async fn fetch_utxos(
        &self,
        refs: HashSet<UtxoRef>,
    ) -> Result<UtxoSet, tx3_lang::backend::Error> {
        let refs: HashSet<_> = refs.into_iter().map(from_tx3_utxoref).collect();

        let utxos = self.inner.get_utxos(refs).map_err(Error::from)?;

        let utxos = utxos
            .into_iter()
            .map(|(txoref, utxo)| into_tx3_utxo(txoref, &utxo))
            .collect::<Result<_, _>>()?;

        Ok(utxos)
    }
}
