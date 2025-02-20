use ::redb::{MultimapTableDefinition, ReadTransaction};
use std::hash::{DefaultHasher, Hash as _, Hasher};

use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct AddressPaymentPartApproxIndexTable;
impl AddressPaymentPartApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("addresspaymentpartapproxindex");

    pub fn compute_key(address: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        address.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_address_payment_part(
        rx: &ReadTransaction,
        address_payment_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&address_payment_part.to_vec());
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }
}
