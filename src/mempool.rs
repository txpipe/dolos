use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use itertools::Itertools;
use pallas::crypto::hash::Hash;
use tracing::debug;

type TxHash = Hash<32>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Tx {
    pub hash: TxHash,
    pub era: u16,
    pub bytes: Vec<u8>,
    pub propagated: bool,
    pub confirmations: usize,
}

#[derive(Default)]
struct MempoolState {
    pending: Vec<Tx>,
    inflight: Vec<Tx>,
    acknowledged: HashMap<TxHash, Tx>,
}

/// A basic, single consumer mempool
#[derive(Clone)]
pub struct Mempool {
    mempool: Arc<RwLock<MempoolState>>,
}

impl Mempool {
    pub fn new() -> Self {
        Self {
            mempool: Arc::new(RwLock::new(MempoolState::default())),
        }
    }

    pub fn receive(&self, tx: Tx) {
        let mut state = self.mempool.write().unwrap();

        state.pending.push(tx);

        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );
    }

    pub fn request(&self, desired: usize) -> Vec<Tx> {
        let available = self.pending_total();
        self.request_exact(std::cmp::min(desired, available))
    }

    pub fn request_exact(&self, count: usize) -> Vec<Tx> {
        let mut state = self.mempool.write().unwrap();

        let selected = state.pending.drain(..count).collect_vec();

        for tx in selected.iter() {
            state.inflight.push(tx.clone());
        }

        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );

        selected
    }

    pub fn acknowledge(&self, count: usize) {
        debug!(n = count, "acknowledging txs");

        let mut state = self.mempool.write().unwrap();

        let selected = state.inflight.drain(..count).collect_vec();

        for tx in selected {
            state.acknowledged.insert(tx.hash.clone(), tx);
        }

        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );
    }

    pub fn find_inflight(&self, tx_hash: TxHash) -> Option<Tx> {
        let state = self.mempool.read().unwrap();
        state.inflight.iter().find(|x| x.hash == tx_hash).cloned()
    }

    pub fn pending_total(&self) -> usize {
        let state = self.mempool.read().unwrap();
        state.pending.len()
    }
}
