use ::redb::{ReadTransaction, TableDefinition};

use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct BlockNumberApproxIndexTable;
impl BlockNumberApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> =
        TableDefinition::new("blocknumberapproxindex");

    pub fn compute_key(block_number: &u64) -> u64 {
        // Left for readability
        *block_number
    }

    pub fn get_by_block_number(
        rx: &ReadTransaction,
        block_number: &u64,
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(block_number);
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }
}
