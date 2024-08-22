use std::collections::HashMap;

type TxHash = Vec<u8>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Tx {
    pub hash: TxHash,
    pub era: u16,
    pub bytes: Vec<u8>,
    pub propagated: bool,
    pub confirmations: usize,
}

pub struct Mempool {
    pending: Vec<Tx>,
    confirmed: HashMap<TxHash, Tx>,
}

impl Mempool {
    pub fn receive(&mut self, tx: Tx) {
        self.pending.push(tx);
    }

    pub fn acknowledge(&mut self, count: usize) {
        let txs = self.pending.drain(..count);

        for tx in txs {
            self.confirmed.insert(tx.hash.clone(), tx);
        }
    }

    pub fn peek(&self, count: usize) -> Vec<Tx> {
        self.pending.iter().take(count).cloned().collect()
    }

    pub fn pending_total(&self) -> usize {
        self.pending.len()
    }
}
