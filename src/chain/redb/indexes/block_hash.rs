use ::redb::{ReadTransaction, TableDefinition};
use std::hash::{DefaultHasher, Hash as _, Hasher};

use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct BlockHashApproxIndexTable;
impl BlockHashApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> =
        TableDefinition::new("blockhashapproxindex");

    pub fn compute_key(block_hash: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        block_hash.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_block_hash(
        rx: &ReadTransaction,
        block_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(&block_hash.to_vec());
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }
}
