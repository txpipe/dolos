use ::redb::{MultimapTableDefinition, ReadTransaction};
use std::hash::{DefaultHasher, Hash as _, Hasher};

use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct ScriptHashApproxIndexTable;
impl ScriptHashApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("scripthashapproxindex");

    pub fn compute_key(script_hash: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        script_hash.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_script_hash(
        rx: &ReadTransaction,
        script_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&script_hash.to_vec());
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }
}
