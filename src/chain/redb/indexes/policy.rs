use ::redb::{MultimapTableDefinition, ReadTransaction};
use std::hash::{DefaultHasher, Hash as _, Hasher};

use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct PolicyApproxIndexTable;
impl PolicyApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("policyapproxindex");

    pub fn compute_key(policy: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        policy.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_policy(rx: &ReadTransaction, policy: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&policy.to_vec());
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }
}
