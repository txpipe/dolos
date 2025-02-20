use ::redb::{ReadTransaction, TableDefinition};
use std::hash::{DefaultHasher, Hash as _, Hasher};

use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct TxHashApproxIndexTable;
impl TxHashApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> = TableDefinition::new("txsapproxindex");

    pub fn compute_key(tx_hash: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        tx_hash.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_tx_hash(rx: &ReadTransaction, tx_hash: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(&tx_hash.to_vec());
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }
}
