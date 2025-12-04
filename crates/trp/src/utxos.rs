use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use tx3_lang::{
    backend::{UtxoPattern, UtxoStore},
    UtxoRef, UtxoSet,
};

use dolos_core::{Domain, EraCbor, StateStore as _, TxoRef};
use pallas::ledger::traverse::{Era, MultiEraOutput};

use crate::{
    mapping::{from_tx3_utxoref, into_tx3_utxo, into_tx3_utxoref},
    Error,
};

#[derive(Default)]
pub struct UtxoMempool {
    locks: RwLock<HashMap<TxoRef, u64>>,
    generated: RwLock<HashMap<TxoRef, Arc<EraCbor>>>,
}

#[derive(Default)]
pub struct SessionContext {
    pub owned_locks: RwLock<HashSet<TxoRef>>,
}

pub struct UtxoMempoolSession<'a> {
    mempool: &'a UtxoMempool,
    current_slot: u64,
    context: Arc<SessionContext>,
}

impl<'a> UtxoMempoolSession<'a> {
    pub fn new(
        mempool: &'a UtxoMempool,
        current_slot: u64,
        context: Arc<SessionContext>,
    ) -> Self {
        Self {
            mempool,
            current_slot,
            context,
        }
    }
}

impl<'a> tx3_resolver::UtxoMempool for UtxoMempoolSession<'a> {
    fn lock(&self, refs: &[UtxoRef]) -> bool {
        let refs: Vec<TxoRef> = refs.iter().map(|r| from_tx3_utxoref(r.clone())).collect();
        if self.mempool.lock(&refs, self.current_slot) {
            let mut owned = self.context.owned_locks.write().unwrap();
            for r in refs {
                owned.insert(r);
            }
            true
        } else {
            false
        }
    }

    fn unlock(&self, refs: &[UtxoRef]) {
        let refs: Vec<TxoRef> = refs.iter().map(|r| from_tx3_utxoref(r.clone())).collect();
        self.mempool.unlock(&refs);
        let mut owned = self.context.owned_locks.write().unwrap();
        for r in refs {
            owned.remove(&r);
        }
    }

    fn register_outputs(&self, tx_bytes: &[u8]) {
        if let Ok(tx) = pallas::ledger::traverse::MultiEraTx::decode(tx_bytes) {
            let tx_hash = tx.hash();
            let mut generated = vec![];
            let era = tx.era();

            for (idx, output) in tx.outputs().iter().enumerate() {
                let txoref = TxoRef(tx_hash, idx as u32);
                let cbor = output.encode();
                let era_cbor = EraCbor(era.into(), cbor);
                generated.push((txoref, Arc::new(era_cbor)));
            }

            self.mempool.add_generated(generated);
        }
    }
}

const SLOTS_BETWEEN_BLOCKS: u64 = 20;
const LOCK_DURATION_BLOCKS: u64 = 3;
const LOCK_DURATION_SLOTS: u64 = SLOTS_BETWEEN_BLOCKS * LOCK_DURATION_BLOCKS;

impl UtxoMempool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn lock(&self, refs: &[TxoRef], current_slot: u64) -> bool {
        let mut locks = self.locks.write().unwrap();

        locks.retain(|_, expiration| *expiration > current_slot);

        for r in refs {
            if locks.contains_key(r) {
                return false;
            }
        }

        for r in refs {
            locks.insert(r.clone(), current_slot + LOCK_DURATION_SLOTS);
        }

        true
    }

    pub fn unlock(&self, refs: &[TxoRef]) {
        let mut locks = self.locks.write().unwrap();
        for r in refs {
            locks.remove(r);
        }
    }

    pub fn add_generated(&self, utxos: Vec<(TxoRef, Arc<EraCbor>)>) {
        let mut generated = self.generated.write().unwrap();

        for (r#ref, utxo) in utxos {
            generated.insert(r#ref, utxo);
        }
    }

    pub fn is_locked(&self, txo: &TxoRef, current_slot: u64) -> bool {
        let locks = self.locks.read().unwrap();
        if let Some(expiration) = locks.get(txo) {
            return *expiration > current_slot;
        }
        false
    }

    pub fn get_generated(&self, r#ref: &TxoRef) -> Option<Arc<EraCbor>> {
        self.generated.read().unwrap().get(r#ref).cloned()
    }

    pub fn match_generated(&self, pattern: &UtxoPattern) -> HashSet<TxoRef> {
        let generated = self.generated.read().unwrap();

        generated
            .iter()
            .filter(|(_, u)| {
                let EraCbor(era, cbor) = u.as_ref();
                if let Ok(era) = Era::try_from(*era) {
                    if let Ok(output) = MultiEraOutput::decode(era, cbor) {
                        match pattern {
                            UtxoPattern::ByAddress(addr) => {
                                if let Ok(a) = output.address() {
                                    a.to_vec() == *addr
                                } else {
                                    false
                                }
                            }
                            UtxoPattern::ByAssetPolicy(policy) => output
                                .value()
                                .assets()
                                .iter()
                                .any(|pa| pa.policy().as_slice() == *policy),
                            UtxoPattern::ByAsset(policy, name) => output.value().assets().iter().any(|pa| {
                                pa.policy().as_slice() == *policy
                                    && pa.assets().iter().any(|a| a.name() == *name)
                            }),
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            })
            .map(|(r, _)| r.clone())
            .collect()
    }
}

pub struct UtxoStoreAdapter<D: Domain> {
    state: D::State,
    mempool: Arc<UtxoMempool>,
    context: Option<Arc<SessionContext>>,
}

impl<D: Domain> UtxoStoreAdapter<D> {
    pub fn new(
        state: D::State,
        mempool: Arc<UtxoMempool>,
        context: Option<Arc<SessionContext>>,
    ) -> Self {
        Self {
            state,
            mempool,
            context,
        }
    }

    fn state(&self) -> &D::State {
        &self.state
    }

    fn is_locked(&self, txo: &TxoRef) -> bool {
        if let Some(ctx) = &self.context {
            if ctx.owned_locks.read().unwrap().contains(txo) {
                return false;
            }
        }

        let current_slot = self
            .state()
            .read_cursor()
            .ok()
            .flatten()
            .map(|c| c.slot())
            .unwrap_or(0);

        self.mempool.is_locked(txo, current_slot)
    }

    fn match_utxos_by_address(&self, address: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        let mut utxos = self.state().get_utxo_by_address(address)?;

        let generated = self
            .mempool
            .match_generated(&UtxoPattern::ByAddress(address));

        utxos.extend(generated);

        // Remove locked UTXOs
        let utxos = utxos
            .into_iter()
            .filter(|u| !self.is_locked(u))
            .collect();

        Ok(utxos)
    }

    fn match_utxos_by_asset_policy(&self, policy: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        let mut utxos = self.state().get_utxo_by_policy(policy)?;

        let generated = self
            .mempool
            .match_generated(&UtxoPattern::ByAssetPolicy(policy));

        utxos.extend(generated);

        // Remove locked UTXOs
        let utxos = utxos
            .into_iter()
            .filter(|u| !self.is_locked(u))
            .collect();

        Ok(utxos)
    }

    fn match_utxos_by_asset(&self, policy: &[u8], name: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        let subject = [policy, name].concat();

        let mut utxos = self.state().get_utxo_by_asset(&subject)?;

        let generated = self
            .mempool
            .match_generated(&UtxoPattern::ByAsset(policy, name));

        utxos.extend(generated);

        // Remove locked UTXOs
        let utxos = utxos
            .into_iter()
            .filter(|u| !self.is_locked(u))
            .collect();

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

        let mut out = vec![];
        let mut missing = vec![];

        for r in refs {
            if let Some(u) = self.mempool.get_generated(&r) {
                let parsed = into_tx3_utxo(r, u)
                    .map_err(|e| tx3_lang::backend::Error::StoreError(e.to_string()))?;
                out.push(parsed);
            } else {
                missing.push(r);
            }
        }

        let utxos = self.state().get_utxos(missing).map_err(Error::from)?;

        let utxos = utxos
            .into_iter()
            .map(|(txoref, utxo)| into_tx3_utxo(txoref, utxo))
            .collect::<Result<Vec<_>, _>>()?;

        out.extend(utxos);

        Ok(out.into_iter().collect())
    }
}

