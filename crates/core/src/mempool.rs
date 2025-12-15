use super::*;

pub use pallas::ledger::validate::phase2::EvalReport;
use tracing::{debug, warn};

#[derive(Debug)]
pub struct MempoolTx {
    pub hash: TxHash,
    pub payload: EraCbor,
    // TODO: we'll improve this to track number of confirmations in further iterations.
    pub confirmed: bool,

    // this might be empty if the tx is cloned
    pub report: Option<EvalReport>,
}

impl PartialEq for MempoolTx {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for MempoolTx {}

impl Clone for MempoolTx {
    fn clone(&self) -> Self {
        Self {
            hash: self.hash.clone(),
            payload: self.payload.clone(),
            confirmed: self.confirmed,
            report: None,
        }
    }
}

#[derive(Clone)]
pub enum MempoolTxStage {
    Pending,
    Inflight,
    Acknowledged,
    Confirmed,
    Unknown,
}

#[derive(Clone)]
pub struct MempoolEvent {
    pub new_stage: MempoolTxStage,
    pub tx: MempoolTx,
}

pub struct MempoolAwareUtxoStore<'a, D: Domain> {
    inner: &'a D::State,
    mempool: &'a D::Mempool,
}

fn utxo_address_matches(utxo: &MultiEraOutput<'_>, expected: &[u8]) -> bool {
    let Ok(address) = utxo.address() else {
        return false;
    };

    address.to_vec() == expected
}

fn utxo_policy_matches(utxo: &MultiEraOutput<'_>, expected_policy: &[u8]) -> bool {
    for ma in utxo.value().assets() {
        if ma.policy() == expected_policy {
            return true;
        }
    }

    false
}

fn utxo_asset_matches(utxo: &MultiEraOutput<'_>, expected_subject: &[u8]) -> bool {
    for ma in utxo.value().assets() {
        for asset in ma.assets() {
            let subject = [ma.policy().as_slice(), asset.name()].concat();
            if subject == expected_subject {
                return true;
            }
        }
    }

    false
}

fn scan_mempool_utxos<D: Domain, F>(predicate: F, mempool: &D::Mempool) -> HashSet<TxoRef>
where
    F: Fn(&MultiEraOutput<'_>) -> bool,
{
    let mut refs = HashSet::new();

    for (_, era_cbor) in mempool.pending() {
        let Some(tx) = MultiEraTx::try_from(&era_cbor).ok() else {
            continue;
        };

        debug!(tx = %tx.hash(), "scanning mempool tx");

        for (idx, inflight) in tx.produces() {
            if predicate(&inflight) {
                let txoref = TxoRef::from((tx.hash(), idx as u32));
                debug!(txoref = %txoref, "mempool utxo matches predicate");
                refs.insert(txoref);
            }
        }
    }

    refs
}

fn exclude_inflight_stxis<D: Domain>(refs: &mut HashSet<TxoRef>, mempool: &D::Mempool) {
    debug!("excluding inflight stxis");

    for (_, era_cbor) in mempool.pending() {
        let Some(tx) = MultiEraTx::try_from(&era_cbor).ok() else {
            warn!("invalid inflight tx");
            continue;
        };

        debug!(tx = %tx.hash(), "checking inflight tx");

        for locked in tx.consumes() {
            let txoref = TxoRef::from(&locked);
            if refs.remove(&txoref) {
                info!(txoref = %txoref, "excluded stxi");
            }
        }
    }
}

fn select_mempool_utxos<D: Domain>(refs: &mut HashSet<TxoRef>, mempool: &D::Mempool) -> UtxoMap {
    let mut map = HashMap::new();

    for (_, era_cbor) in mempool.pending() {
        let Some(tx) = MultiEraTx::try_from(&era_cbor).ok() else {
            continue;
        };

        debug!(tx = %tx.hash(), "checking mempool tx");

        for (idx, inflight) in tx.produces() {
            let txoref = TxoRef::from((tx.hash(), idx as u32));
            debug!(txoref = %txoref, "checking mempool utxo");

            if refs.contains(&txoref) {
                let era_cbor = EraCbor::from(inflight);
                warn!(txoref = %txoref, "selected utxo available inmempool tx");
                refs.remove(&txoref);
                map.insert(txoref, era_cbor);
            }
        }
    }

    map
}

impl<'a, D: Domain> MempoolAwareUtxoStore<'a, D> {
    pub fn new(inner: &'a D::State, mempool: &'a D::Mempool) -> Self {
        Self { inner, mempool }
    }

    pub fn state(&self) -> &D::State {
        &self.inner
    }

    pub fn mempool(&self) -> &D::Mempool {
        &self.mempool
    }

    pub fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, StateError> {
        let predicate = |utxo: &MultiEraOutput<'_>| utxo_asset_matches(utxo, asset);

        let from_mempool = scan_mempool_utxos::<D, _>(predicate, &self.mempool);

        let mut utxos = self.inner.get_utxo_by_asset(asset)?;

        utxos.extend(from_mempool);

        exclude_inflight_stxis::<D>(&mut utxos, &self.mempool);

        Ok(utxos)
    }

    pub fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, StateError> {
        let predicate = |utxo: &MultiEraOutput<'_>| utxo_policy_matches(utxo, policy);

        let from_mempool = scan_mempool_utxos::<D, _>(predicate, &self.mempool);

        let mut utxos = self.inner.get_utxo_by_policy(policy)?;

        utxos.extend(from_mempool);

        exclude_inflight_stxis::<D>(&mut utxos, &self.mempool);

        Ok(utxos)
    }

    pub fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, StateError> {
        let predicate = |utxo: &MultiEraOutput<'_>| utxo_address_matches(utxo, address);

        let from_mempool = scan_mempool_utxos::<D, _>(predicate, &self.mempool);

        let mut utxos = self.inner.get_utxo_by_address(address)?;

        utxos.extend(from_mempool);

        exclude_inflight_stxis::<D>(&mut utxos, &self.mempool);

        Ok(utxos)
    }

    pub fn get_utxos(&self, mut refs: HashSet<TxoRef>) -> Result<UtxoMap, StateError> {
        exclude_inflight_stxis::<D>(&mut refs, &self.mempool);

        let from_mempool = select_mempool_utxos::<D>(&mut refs, &self.mempool);

        let mut utxos = self.inner.get_utxos(Vec::from_iter(refs))?;

        utxos.extend(from_mempool);

        Ok(utxos)
    }
}
