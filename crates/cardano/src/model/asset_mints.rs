use pallas::{
    codec::minicbor::{self, data::Int, decode, encode, Decode, Decoder, Encode, Encoder},
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
/// value is `[tx hash, 2 × amount + names_asset]`; the key holds the rest.
/// The same pair is an entry of [`AssetMintChunk`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetMintSet {
    pub tx_hash: Hash<32>,

    /// The signed quantity: negative for a burn, never zero.
    pub amount: i64,

    /// The mint sets the asset's `metadata_tx`: its tx's CIP-25 metadata
    /// names the asset. Never set on a burn.
    pub names_asset: bool,
}

impl<C> Encode<C> for AssetMintSet {
    fn encode<W: encode::Write>(
        &self,
        e: &mut Encoder<W>,
        _: &mut C,
    ) -> Result<(), encode::Error<W::Error>> {
        // 2 × i64 + 1 spans exactly the CBOR integer range
        let packed = 2 * i128::from(self.amount) + i128::from(self.names_asset);
        let packed = Int::try_from(packed).expect("within the CBOR integer range");

        e.array(2)?.encode(self.tx_hash)?.int(packed)?;
        Ok(())
    }
}

impl<'b, C> Decode<'b, C> for AssetMintSet {
    fn decode(d: &mut Decoder<'b>, _: &mut C) -> Result<Self, decode::Error> {
        if d.array()? != Some(2) {
            return Err(decode::Error::message("expected a two-item mint"));
        }

        let tx_hash = d.decode()?;
        let packed = i128::from(d.int()?);
        let amount = i64::try_from(packed.div_euclid(2))
            .map_err(|_| decode::Error::message("mint amount out of range"))?;

        Ok(Self {
            tx_hash,
            amount,
            names_asset: packed.rem_euclid(2) == 1,
        })
    }
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
        names_asset: bool,
    ) -> Self {
        let key = GroupKey {
            group: asset,
            epoch,
            member: position,
        };

        Self::at(
            key,
            AssetMintSet {
                tx_hash,
                amount,
                names_asset,
            },
        )
    }
}

/// One chunk of an asset's mints and burns in an epoch, in chain order, each
/// as its staged [`AssetMintSet`] row.
///
/// Sealed at `epoch_start(e) ‖ AssetKey ‖ chunk (4 B)`. Every chunk carries
/// the asset's count for the epoch, so a reader learns it from the first
/// chunk.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Default)]
pub struct AssetMintChunk {
    #[n(0)]
    pub count: u64,

    #[n(1)]
    pub mints: Vec<AssetMintSet>,
}

entity_boilerplate!(AssetMintChunk, "asset-mint-chunks", log = (AssetKey, u32));

impl ChunkRow for AssetMintChunk {
    type Item = AssetMintSet;

    fn new(count: u64, items: Vec<AssetMintSet>) -> Self {
        Self {
            count,
            mints: items,
        }
    }

    fn into_items(self) -> Vec<AssetMintSet> {
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
            names_asset in any::<bool>(),
        ) -> AssetMintSet {
            AssetMintSet { tx_hash, amount, names_asset: names_asset && amount > 0 }
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
            StageAssetMint::mint(
                AssetKey(asset),
                epoch,
                (slot, tx_index),
                row.tx_hash,
                row.amount,
                row.names_asset,
            )
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

        #[test]
        fn a_mint_round_trips_through_cbor(
            tx_hash in crate::model::testing::any_hash_32(),
            amount in any::<i64>(),
            names_asset in any::<bool>(),
        ) {
            let row = super::AssetMintSet { tx_hash, amount, names_asset };
            let raw = pallas::codec::minicbor::to_vec(&row).unwrap();
            prop_assert_eq!(pallas::codec::minicbor::decode::<super::AssetMintSet>(&raw).unwrap(), row);
        }
    }
}

#[cfg(test)]
mod layout_tests {
    use pallas::codec::minicbor;

    use super::*;

    fn raw(amount: i64, names_asset: bool) -> Vec<u8> {
        let row = AssetMintSet {
            tx_hash: [0xab; 32].into(),
            amount,
            names_asset,
        };
        minicbor::to_vec(&row).unwrap()[35..].to_vec()
    }

    /// The flag rides in the amount's low bit, so the common amounts keep
    /// their one-byte head and the extremes still fit a CBOR integer.
    #[test]
    fn the_flag_packs_into_the_amount() {
        assert_eq!(raw(1, false), [0x02]);
        assert_eq!(raw(1, true), [0x03]);
        assert_eq!(raw(11, true), [0x17]);
        assert_eq!(raw(-1, false), [0x21]);
        assert_eq!(raw(-12, false), [0x37]);
        assert_eq!(raw(i64::MAX, true), [&[0x1b][..], &[0xff; 8]].concat());
        assert_eq!(raw(i64::MIN, false), [&[0x3b][..], &[0xff; 8]].concat());
    }

    #[test]
    fn a_mint_without_two_items_is_refused() {
        let mut good = minicbor::to_vec(AssetMintSet {
            tx_hash: [0xab; 32].into(),
            amount: 1,
            names_asset: false,
        })
        .unwrap();
        assert!(minicbor::decode::<AssetMintSet>(&good).is_ok());

        good[0] = 0x83;
        good.push(0x00);
        assert!(minicbor::decode::<AssetMintSet>(&good).is_err());
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
