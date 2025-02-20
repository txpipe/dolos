use ::redb::{MultimapTableDefinition, ReadTransaction};

use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct BlockNumberApproxIndexTable;
impl BlockNumberApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("blocknumberapproxindex");

    pub fn compute_key(block_number: &u64) -> u64 {
        // Left for readability
        *block_number
    }

    pub fn get_by_block_number(
        rx: &ReadTransaction,
        block_number: &u64,
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(block_number);
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }
}
