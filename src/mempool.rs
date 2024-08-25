use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use pallas::crypto::hash::Hash;

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
    pending: HashMap<TxHash, Tx>,
    acknowledged: HashMap<TxHash, Tx>,
}

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
        state.pending.insert(tx.hash, tx);
    }

    pub fn acknowledge(&self, txs: HashSet<TxHash>) {
        let mut state = self.mempool.write().unwrap();

        for tx in txs {
            let tx = state.pending.remove(&tx);

            if let Some(tx) = tx {
                state.acknowledged.insert(tx.hash.clone(), tx);
            }
        }
    }

    pub fn peek(&self, count: usize) -> Vec<Tx> {
        let state = self.mempool.read().unwrap();
        state.pending.values().take(count).cloned().collect()
    }

    pub fn pending_total(&self) -> usize {
        let state = self.mempool.read().unwrap();
        state.pending.len()
    }
}
