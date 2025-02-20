use ::redb::{ReadTransaction, TableDefinition};
use std::hash::{DefaultHasher, Hash as _, Hasher};

use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct DatumHashApproxIndexTable;
impl DatumHashApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> =
        TableDefinition::new("datumhashapproxindex");

    pub fn compute_key(datum_hash: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        datum_hash.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_datum_hash(
        rx: &ReadTransaction,
        datum_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(&datum_hash.to_vec());
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }
}
