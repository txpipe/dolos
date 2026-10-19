use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
    ledger::primitives::Epoch,
};
use serde::{Deserialize, Serialize};

use super::AssetKey;
use crate::periods::{ChunkRow, GroupKey, StageIfAbsent};

/// A mint's position on chain: slot, then tx index.
pub type MintPosition = (u64, u32);

/// One mint or burn of an asset by a valid tx, held in state until its epoch
/// seals into [`AssetMintChunk`]s.
///
/// Keyed `AssetKey ‖ epoch ‖ (slot, tx index)`, so one prefix scan returns
/// every unsealed epoch of an asset, in chain order, and nothing else. The
/// value is `[tx hash, amount]`; the key holds the rest.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct AssetMintSet {
    #[n(0)]
    pub tx_hash: Hash<32>,

    /// The signed quantity: negative for a burn, never zero.
    #[n(1)]
    pub amount: i64,
}

entity_boilerplate!(
    AssetMintSet,
    "asset-mints",
    state = GroupKey<AssetKey, MintPosition>
);

pub type StageAssetMint = StageIfAbsent<AssetMintSet>;

impl StageAssetMint {
    /// Stage the mint of `asset` by the tx at `position`, in `epoch`.
    pub fn mint(
        asset: AssetKey,
        epoch: Epoch,
        position: MintPosition,
        tx_hash: Hash<32>,
        amount: i64,
    ) -> Self {
        let key = GroupKey {
            group: asset,
            epoch,
            member: position,
        };

        Self::at(key, AssetMintSet { tx_hash, amount })
    }
}

/// One chunk of an asset's mints and burns in an epoch, as `(tx hash,
/// amount)` in chain order.
///
/// Sealed at `epoch_start(e) ‖ AssetKey ‖ chunk (4 B)`. Every chunk carries
/// the asset's count for the epoch, so a reader learns it from the first
/// chunk.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Default)]
pub struct AssetMintChunk {
    #[n(0)]
    pub count: u64,

    #[n(1)]
    pub mints: Vec<(Hash<32>, i64)>,
}

entity_boilerplate!(AssetMintChunk, "asset-mint-chunks", log = (AssetKey, u32));

impl ChunkRow for AssetMintChunk {
    type Item = (Hash<32>, i64);

    fn new(count: u64, items: Vec<(Hash<32>, i64)>) -> Self {
        Self {
            count,
            mints: items,
        }
    }

    fn into_items(self) -> Vec<(Hash<32>, i64)> {
        self.mints
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_asset_mint_set()(
            tx_hash in root::any_hash_32(),
            amount in any::<i64>().prop_filter("never zero", |x| *x != 0),
        ) -> AssetMintSet {
            AssetMintSet { tx_hash, amount }
        }
    }

    prop_compose! {
        pub fn any_stage_asset_mint()(
            asset in root::any_hash_32(),
            epoch in 0u64..1000,
            slot in root::any_slot(),
            tx_index in 0u32..500,
            row in any_asset_mint_set(),
        ) -> StageAssetMint {
            StageAssetMint::mint(AssetKey(asset), epoch, (slot, tx_index), row.tx_hash, row.amount)
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use super::testing::*;
    use crate::model::testing::{assert_delta_roundtrip, assert_delta_serde_roundtrip};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn stage_asset_mint_roundtrip(
            entity in prop::option::of(any_asset_mint_set()),
            delta in any_stage_asset_mint(),
        ) {
            assert_delta_roundtrip(entity.clone(), delta.clone());
            assert_delta_serde_roundtrip(entity, delta);
        }
    }
}

#[cfg(test)]
mod key_tests {
    use dolos_core::{keys::encode, KeyCodec as _, RangeOf};
    use proptest::prelude::*;

    use super::*;

    fn key(asset: [u8; 32], epoch: Epoch, slot: u64, tx: u32) -> GroupKey<AssetKey, MintPosition> {
        GroupKey {
            group: AssetKey(asset.into()),
            epoch,
            member: (slot, tx),
        }
    }

    #[test]
    fn layouts_are_pinned() {
        assert_eq!(
            encode(&key([0x12; 32], 7, 300, 2)).to_vec(),
            [
                [0x12; 32].as_slice(),
                &7u64.to_be_bytes(),
                &300u64.to_be_bytes(),
                &2u32.to_be_bytes(),
            ]
            .concat()
        );
        assert_eq!(
            encode(&(AssetKey([0x17; 32].into()), 2u32)).to_vec(),
            [[0x17; 32].as_slice(), &[0, 0, 0, 2]].concat()
        );
    }

    #[test]
    fn an_asset_prefix_holds_its_rows_alone_in_chain_order() {
        let asset = AssetKey([0x12; 32].into());
        let range = RangeOf::prefix(&asset);

        assert!(range.contains(&key([0x12; 32], 0, 0, 0)));
        assert!(range.contains(&key([0x12; 32], u64::MAX, u64::MAX, u32::MAX)));
        assert!(!range.contains(&key([0x13; 32], 3, 3, 0)));

        // epoch, then slot, then tx index
        assert!(encode(&key([0x12; 32], 3, 900, 0)) < encode(&key([0x12; 32], 4, 1, 0)));
        assert!(encode(&key([0x12; 32], 3, 255, 9)) < encode(&key([0x12; 32], 3, 256, 0)));
        assert!(encode(&key([0x12; 32], 3, 256, 1)) < encode(&key([0x12; 32], 3, 256, 256)));
    }

    proptest! {
        #[test]
        fn keys_round_trip_exactly(
            asset in any::<[u8; 32]>(),
            epoch in any::<u64>(),
            slot in any::<u64>(),
            tx in any::<u32>(),
        ) {
            let key = key(asset, epoch, slot, tx);
            let raw = encode(&key);
            prop_assert_eq!(GroupKey::decode_key(&raw), Ok(key));
            prop_assert!(
                GroupKey::<AssetKey, MintPosition>::decode_key(&raw[..raw.len() - 1]).is_err()
            );
        }
    }
}
