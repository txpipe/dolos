use pallas::{
    codec::minicbor::{self, bytes::ByteVec, Decode, Encode},
    crypto::hash::Hash,
    ledger::primitives::Epoch,
};
use serde::{Deserialize, Serialize};

use super::AssetKey;
use crate::periods::{ChunkRow, GroupKey, GroupRow, StageIfAbsent};

/// The first mint of an asset, held in state until its epoch seals into a
/// [`FirstMintChunk`], a [`PolicyMintChunk`] and the asset's entity index
/// entry.
///
/// Keyed `policy ‖ epoch ‖ asset key`, so one prefix scan returns every
/// unsealed epoch of a policy, and nothing else. The roll's witness stages a
/// row only for an asset that neither the entity index nor another staged row
/// knows, so every row is a first mint ever.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct FirstMintSet {
    #[n(0)]
    pub policy: Hash<28>,

    #[n(1)]
    #[cbor(with = "minicbor::bytes")]
    pub name: Vec<u8>,

    #[n(2)]
    pub epoch: Epoch,

    #[n(3)]
    pub slot: u64,

    #[n(4)]
    pub tx_index: u32,
}

hash_key!(
    /// A minting policy.
    PolicyKey,
    Hash<28>,
    28
);

display_as_stored!(PolicyKey);

entity_boilerplate!(
    FirstMintSet,
    "first-mints",
    state = GroupKey<PolicyKey, AssetKey>
);

impl FirstMintSet {
    pub fn subject(&self) -> Vec<u8> {
        [self.policy.as_slice(), self.name.as_slice()].concat()
    }

    /// Mint order: slot, then tx index, then subject bytes, the order db-sync
    /// writes `ma_tx_mint` rows in.
    pub fn mint_order(&self) -> (u64, u32, Vec<u8>) {
        (self.slot, self.tx_index, self.subject())
    }

    /// The key of the minted asset.
    pub fn asset_key(&self) -> AssetKey {
        AssetKey::of_parts(self.policy.as_slice(), &self.name)
    }
}

impl GroupRow for FirstMintSet {
    fn key(&self) -> GroupKey<PolicyKey, AssetKey> {
        GroupKey {
            group: PolicyKey(self.policy),
            epoch: self.epoch,
            member: self.asset_key(),
        }
    }
}

pub type StageFirstMint = StageIfAbsent<FirstMintSet>;

/// One chunk of the subjects first minted in an epoch, in mint order.
///
/// Sealed at `epoch_start(e) ‖ chunk (4 B)`. Every chunk carries the epoch's
/// total, so a reader learns how many subjects the epoch holds from its first
/// chunk.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Default)]
pub struct FirstMintChunk {
    #[n(0)]
    pub count: u64,

    #[n(1)]
    pub subjects: Vec<ByteVec>,
}

entity_boilerplate!(FirstMintChunk, "first-mint-chunks", log = u32);

impl ChunkRow for FirstMintChunk {
    type Item = Vec<u8>;

    fn new(count: u64, items: Vec<Vec<u8>>) -> Self {
        Self {
            count,
            subjects: items.into_iter().map(ByteVec::from).collect(),
        }
    }

    fn into_items(self) -> Vec<Vec<u8>> {
        self.subjects.into_iter().map(Vec::from).collect()
    }
}

/// One chunk of the asset names a policy first minted in an epoch, in mint
/// order.
///
/// Sealed at `epoch_start(e) ‖ policy ‖ chunk (4 B)`, with the policy's total
/// for the epoch on every chunk.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Default)]
pub struct PolicyMintChunk {
    #[n(0)]
    pub count: u64,

    #[n(1)]
    pub names: Vec<ByteVec>,
}

entity_boilerplate!(
    PolicyMintChunk,
    "policy-mint-chunks",
    log = (PolicyKey, u32)
);

impl ChunkRow for PolicyMintChunk {
    type Item = Vec<u8>;

    fn new(count: u64, items: Vec<Vec<u8>>) -> Self {
        Self {
            count,
            names: items.into_iter().map(ByteVec::from).collect(),
        }
    }

    fn into_items(self) -> Vec<Vec<u8>> {
        self.names.into_iter().map(Vec::from).collect()
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_first_mint_set()(
            policy in root::any_hash_28(),
            name in prop::collection::vec(any::<u8>(), 0..32),
            epoch in 0u64..1000,
            slot in root::any_slot(),
            tx_index in 0u32..500,
        ) -> FirstMintSet {
            FirstMintSet { policy, name, epoch, slot, tx_index }
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use super::testing::any_first_mint_set;
    use super::*;
    use crate::model::testing::{assert_delta_roundtrip, assert_delta_serde_roundtrip};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn stage_first_mint_roundtrip(
            entity in prop::option::of(any_first_mint_set()),
            row in any_first_mint_set(),
        ) {
            assert_delta_roundtrip(entity.clone(), StageFirstMint::new(row.clone()));
            assert_delta_serde_roundtrip(entity, StageFirstMint::new(row));
        }
    }

    #[test]
    fn staging_keeps_the_first_row() {
        let first = FirstMintSet {
            policy: [1u8; 28].into(),
            name: b"A".to_vec(),
            epoch: 7,
            slot: 100,
            tx_index: 0,
        };
        let later = FirstMintSet {
            slot: 200,
            ..first.clone()
        };

        let mut entity = None;
        let mut a = StageFirstMint::new(first.clone());
        let mut b = StageFirstMint::new(later);
        dolos_core::EntityDelta::apply(&mut a, &mut entity);
        dolos_core::EntityDelta::apply(&mut b, &mut entity);
        assert_eq!(entity, Some(first.clone()));

        dolos_core::EntityDelta::undo(&b, &mut entity);
        assert_eq!(entity, Some(first));

        dolos_core::EntityDelta::undo(&a, &mut entity);
        assert_eq!(entity, None);
    }
}

#[cfg(test)]
mod key_tests {
    use dolos_core::{keys::encode, KeyCodec};
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn layouts_are_pinned() {
        assert_eq!(
            encode(&PolicyKey(Hash::new([0x12; 28]))).to_vec(),
            [0x12; 28]
        );
        assert_eq!(encode(&7u32).to_vec(), [0, 0, 0, 7]);
        assert_eq!(
            encode(&(PolicyKey(Hash::new([0x17; 28])), 2u32)).to_vec(),
            [[0x17; 28].as_slice(), &[0, 0, 0, 2]].concat()
        );
    }

    proptest! {
        #[test]
        fn keys_round_trip(hash in any::<[u8; 28]>()) {
            let key = PolicyKey(hash.into());
            prop_assert_eq!(PolicyKey::decode_key(&encode(&key)), Ok(key));
        }
    }
}
