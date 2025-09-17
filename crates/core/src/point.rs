use std::fmt::Display;

use pallas::{crypto::hash::Hash, network::miniprotocols::Point as PallasPoint};
use serde::{Deserialize, Serialize};

use crate::{Block, BlockHash, BlockSlot};

#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
pub enum ChainPoint {
    Origin,
    Slot(BlockSlot),
    Specific(BlockSlot, BlockHash),
}

impl ChainPoint {
    pub fn slot(&self) -> BlockSlot {
        match self {
            Self::Origin => 0,
            Self::Slot(slot) => *slot,
            Self::Specific(slot, _) => *slot,
        }
    }

    pub fn hash(&self) -> Option<BlockHash> {
        match self {
            Self::Specific(_, hash) => Some(*hash),
            _ => None,
        }
    }
}

impl Display for ChainPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Origin => write!(f, "Origin"),
            Self::Slot(slot) => write!(f, "{slot}"),
            Self::Specific(slot, hash) => write!(f, "{slot}({hash})"),
        }
    }
}

impl PartialEq for ChainPoint {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Specific(l0, l1), Self::Specific(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Slot(l0), Self::Slot(r0)) => l0 == r0,
            (Self::Origin, Self::Origin) => true,
            // in the particular scenario where we are more specific than the other value, it's ok
            // to compare just slots. The inverse is not true (we're less specific than the other
            // value that requires also comparing hashes).
            (Self::Specific(l0, _), Self::Slot(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl Ord for ChainPoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let l_slot = self.slot();
        let r_slot = other.slot();

        // if slots are different, we can compare them directly
        if l_slot != r_slot {
            return l_slot.cmp(&r_slot);
        }

        // if the slots are the same, we need to compare hashes

        let l_hash = self.hash();
        let r_hash = other.hash();

        l_hash.cmp(&r_hash)
    }
}

impl PartialOrd for ChainPoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<PallasPoint> for ChainPoint {
    fn from(value: PallasPoint) -> Self {
        match value {
            PallasPoint::Origin => ChainPoint::Origin,
            PallasPoint::Specific(s, h) => ChainPoint::Specific(s, h.as_slice().into()),
        }
    }
}

impl TryFrom<ChainPoint> for PallasPoint {
    type Error = ();

    fn try_from(value: ChainPoint) -> Result<Self, Self::Error> {
        match value {
            ChainPoint::Origin => Ok(PallasPoint::Origin),
            ChainPoint::Specific(s, h) => Ok(PallasPoint::Specific(s, h.to_vec())),
            ChainPoint::Slot(_) => Err(()),
        }
    }
}

impl<T> From<&T> for ChainPoint
where
    T: Block,
{
    fn from(value: &T) -> Self {
        let slot = value.slot();
        let hash = value.hash();
        ChainPoint::Specific(slot, hash)
    }
}

impl ChainPoint {
    pub fn into_bytes(self) -> [u8; 40] {
        let slot = self.slot();

        let hash = match self.hash() {
            Some(hash) => *hash,
            None => [0u8; 32],
        };

        let mut out = [0u8; 40];
        out[0..8].copy_from_slice(&slot.to_be_bytes());
        out[8..40].copy_from_slice(hash.as_slice());
        out
    }

    const ORIGIN_BYTES: [u8; 40] = [0u8; 40];

    pub fn from_bytes(value: [u8; 40]) -> Self {
        if value == Self::ORIGIN_BYTES {
            return ChainPoint::Origin;
        }

        let slot_half: [u8; 8] = value[0..8].try_into().unwrap();
        let hash_half: [u8; 32] = value[8..40].try_into().unwrap();
        let slot = u64::from_be_bytes(slot_half);
        let hash = Hash::new(hash_half);
        ChainPoint::Specific(slot, hash)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest::proptest;

    use super::*;

    prop_compose! {
      fn any_hash() (bytes in any::<[u8; 32]>()) -> Hash<32> {
            Hash::new(bytes)
        }
    }

    prop_compose! {
      fn any_specific_point() (slot in any::<BlockSlot>(), hash in any_hash()) -> ChainPoint {
            ChainPoint::Specific(slot, hash)
        }
    }

    proptest! {
        #[test]
        fn test_binary_order_is_maintained(point1 in any_specific_point(), point2 in any_specific_point()) {
            let bytes1 = point1.clone().into_bytes();
            let bytes2 = point2.clone().into_bytes();

            let point_cmp = point1.cmp(&point2);
            let bytes_cmp = bytes1.cmp(&bytes2);

            assert_eq!(point_cmp, bytes_cmp);
        }
    }
}
