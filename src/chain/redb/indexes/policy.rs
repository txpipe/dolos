use ::redb::{ReadTransaction, TableDefinition};
use std::hash::{DefaultHasher, Hash as _, Hasher};

use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct PolicyApproxIndexTable;
impl PolicyApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> =
        TableDefinition::new("policyapproxindex");

    pub fn compute_key(policy: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        policy.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_policy(rx: &ReadTransaction, policy: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(&policy.to_vec());
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }
}
