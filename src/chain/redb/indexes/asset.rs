use ::redb::{ReadTransaction, TableDefinition};
use std::hash::{DefaultHasher, Hash as _, Hasher};

use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct AssetApproxIndexTable;
impl AssetApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> =
        TableDefinition::new("assetapproxindex");

    pub fn compute_key(asset: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        asset.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_asset(rx: &ReadTransaction, asset: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(&asset.to_vec());
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }
}
