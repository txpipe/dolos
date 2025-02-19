use ::redb::{ReadTransaction, ReadableTable as _};
use ::redb::{TableDefinition, WriteTransaction};
use std::hash::{DefaultHasher, Hash as _, Hasher};

use crate::ledger::LedgerDelta;
use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct TxHashApproxIndexTable;
impl TxHashApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> = TableDefinition::new("txsapproxindex");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

    pub fn compute_key(tx_hash: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        tx_hash.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_tx_hash(rx: &ReadTransaction, tx_hash: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(tx_hash);
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        if let Some(point) = &delta.new_position {
            let slot = point.0;
            let tx_hashes = delta
                .new_txs
                .keys()
                .map(|hash| hash.to_vec())
                .collect::<Vec<Vec<u8>>>();

            for tx_hash in tx_hashes {
                let key = Self::compute_key(&tx_hash);

                let maybe_new = match table.get(key)? {
                    Some(value) => {
                        let mut previous = value.value().clone();
                        if !previous.contains(&slot) {
                            previous.push(slot);
                            Some(previous)
                        } else {
                            None
                        }
                    }
                    None => Some(vec![slot]),
                };
                if let Some(new) = maybe_new {
                    table.insert(key, new)?;
                }
            }
        }

        if let Some(point) = &delta.undone_position {
            let slot = point.0;
            let tx_hashes = delta
                .undone_txs
                .keys()
                .map(|hash| hash.to_vec())
                .collect::<Vec<Vec<u8>>>();

            for tx_hash in tx_hashes {
                let key = Self::compute_key(&tx_hash);

                let maybe_new = match table.get(key)? {
                    Some(value) => {
                        let mut previous = value.value().clone();
                        match previous.iter().position(|x| *x == slot) {
                            Some(index) => {
                                previous.remove(index);
                                Some(previous)
                            }
                            None => None,
                        }
                    }
                    None => None,
                };
                if let Some(new) = maybe_new {
                    table.insert(key, new)?;
                }
            }
        }

        Ok(())
    }

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        let source = rx.open_table(Self::DEF)?;
        let mut target = wx.open_table(Self::DEF)?;

        for entry in source.iter()? {
            let (k, v) = entry?;
            target.insert(k.value(), v.value())?;
        }

        Ok(())
    }
}
