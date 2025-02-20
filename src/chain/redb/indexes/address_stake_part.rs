use ::redb::{ReadTransaction, TableDefinition};
use std::hash::{DefaultHasher, Hash as _, Hasher};

use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct AddressStakePartApproxIndexTable;
impl AddressStakePartApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> =
        TableDefinition::new("addressstakepartapproxindextable");

    pub fn compute_key(address_stake_part: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        address_stake_part.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_address_stake_part(
        rx: &ReadTransaction,
        address_stake_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(&address_stake_part.to_vec());
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }
}
