use dolos_core::NsKey;
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
};
use serde::{Deserialize, Serialize};

use super::FixedNamespace as _;

#[derive(Debug, Encode, Decode, Clone, Default, PartialEq, Eq)]
pub struct AssetState {
    #[n(0)]
    pub quantity_bytes: [u8; 16],

    #[n(1)]
    pub initial_tx: Option<Hash<32>>,

    #[n(2)]
    pub initial_slot: Option<u64>,

    #[n(3)]
    pub mint_tx_count: u64,

    #[n(4)]
    #[cbor(default)]
    pub metadata_tx: Option<Hash<32>>,
}

entity_boilerplate!(AssetState, "assets");

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_asset_state()(
            quantity_bytes in any::<[u8; 16]>(),
            initial_tx in prop::option::of(root::any_hash_32()),
            initial_slot in prop::option::of(root::any_slot()),
            mint_tx_count in 0u64..1000u64,
            metadata_tx in prop::option::of(root::any_hash_32()),
        ) -> AssetState {
            AssetState {
                quantity_bytes,
                initial_tx,
                initial_slot,
                mint_tx_count,
                metadata_tx,
            }
        }
    }
}

impl AssetState {
    pub fn add_quantity(&mut self, value: i128) {
        let old = i128::from_be_bytes(self.quantity_bytes);
        let new = old.saturating_add(value).to_be_bytes();
        self.quantity_bytes = new;
    }

    pub fn quantity(&self) -> i128 {
        i128::from_be_bytes(self.quantity_bytes)
    }
}

// --- Deltas ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintStatsUpdate {
    pub(crate) policy: Hash<28>,
    pub(crate) asset: Vec<u8>,
    pub(crate) quantity: i128,
    pub(crate) seen_in_tx: Hash<32>,
    pub(crate) seen_in_slot: u64,

    // undo
    pub(crate) was_new: bool,
    /// `add_quantity` uses saturating math, so the pre-apply bytes can't be derived
    /// from (post_bytes - quantity). We record them to restore exactly.
    pub(crate) prev_quantity_bytes: [u8; 16],
    pub(crate) prev_initial_slot: Option<u64>,
    pub(crate) prev_initial_tx: Option<Hash<32>>,
}

impl MintStatsUpdate {
    pub fn new(
        policy: Hash<28>,
        asset: Vec<u8>,
        quantity: i128,
        seen_in_tx: Hash<32>,
        seen_in_slot: u64,
    ) -> Self {
        Self {
            policy,
            asset,
            quantity,
            seen_in_tx,
            seen_in_slot,
            was_new: false,
            prev_quantity_bytes: [0u8; 16],
            prev_initial_slot: None,
            prev_initial_tx: None,
        }
    }
}

impl dolos_core::EntityDelta for MintStatsUpdate {
    type Entity = AssetState;

    fn key(&self) -> NsKey {
        let mut hasher = pallas::crypto::hash::Hasher::<256>::new();
        hasher.input(self.policy.as_slice());
        hasher.input(self.asset.as_slice());
        let key = hasher.finalize();
        let key = key.as_slice();
        NsKey::from((AssetState::NS, key))
    }

    fn apply(&mut self, entity: &mut Option<AssetState>) {
        self.was_new = entity.is_none();

        let entity = entity.get_or_insert_default();

        // save undo info
        self.prev_quantity_bytes = entity.quantity_bytes;
        self.prev_initial_slot = entity.initial_slot;
        self.prev_initial_tx = entity.initial_tx;

        entity.add_quantity(self.quantity);
        entity.mint_tx_count += 1;

        if entity.initial_slot.unwrap_or(u64::MAX) > self.seen_in_slot {
            entity.initial_slot = Some(self.seen_in_slot);
            entity.initial_tx = Some(self.seen_in_tx);
        }
    }

    fn undo(&self, entity: &mut Option<AssetState>) {
        if self.was_new {
            *entity = None;
            return;
        }
        let entity = entity.as_mut().expect("existing asset");
        entity.quantity_bytes = self.prev_quantity_bytes;
        entity.mint_tx_count -= 1;
        entity.initial_slot = self.prev_initial_slot;
        entity.initial_tx = self.prev_initial_tx;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataTxUpdate {
    pub(crate) policy: Hash<28>,
    pub(crate) asset: Vec<u8>,
    pub(crate) metadata_tx: Hash<32>,

    // undo
    pub(crate) was_new: bool,
    pub(crate) prev_metadata_tx: Option<Hash<32>>,
}

impl MetadataTxUpdate {
    pub fn new(policy: Hash<28>, asset: Vec<u8>, metadata_tx: Hash<32>) -> Self {
        Self {
            policy,
            asset,
            metadata_tx,
            was_new: false,
            prev_metadata_tx: None,
        }
    }
}

impl dolos_core::EntityDelta for MetadataTxUpdate {
    type Entity = AssetState;

    fn key(&self) -> NsKey {
        let mut hasher = pallas::crypto::hash::Hasher::<256>::new();
        hasher.input(self.policy.as_slice());
        hasher.input(self.asset.as_slice());
        let key = hasher.finalize();
        let key = key.as_slice();
        NsKey::from((AssetState::NS, key))
    }

    fn apply(&mut self, entity: &mut Option<AssetState>) {
        self.was_new = entity.is_none();
        let entity = entity.get_or_insert_default();
        self.prev_metadata_tx = entity.metadata_tx;
        entity.metadata_tx = Some(self.metadata_tx);
    }

    fn undo(&self, entity: &mut Option<AssetState>) {
        if self.was_new {
            *entity = None;
            return;
        }
        let entity = entity.as_mut().expect("existing asset");
        entity.metadata_tx = self.prev_metadata_tx;
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use super::testing::any_asset_state;
    use crate::model::testing::{self as root, assert_delta_roundtrip};
    use proptest::prelude::*;

    prop_compose! {
        fn any_mint_stats_update()(
            policy in root::any_hash_28(),
            asset in prop::collection::vec(any::<u8>(), 0..16),
            quantity in -1_000_000_000i128..1_000_000_000i128,
            seen_in_tx in root::any_hash_32(),
            seen_in_slot in root::any_slot(),
        ) -> MintStatsUpdate {
            MintStatsUpdate::new(policy, asset, quantity, seen_in_tx, seen_in_slot)
        }
    }

    prop_compose! {
        fn any_metadata_tx_update()(
            policy in root::any_hash_28(),
            asset in prop::collection::vec(any::<u8>(), 0..16),
            metadata_tx in root::any_hash_32(),
        ) -> MetadataTxUpdate {
            MetadataTxUpdate::new(policy, asset, metadata_tx)
        }
    }

    proptest! {
        #[test]
        fn mint_stats_update_roundtrip(
            entity in prop::option::of(any_asset_state()),
            delta in any_mint_stats_update(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn metadata_tx_update_roundtrip(
            entity in prop::option::of(any_asset_state()),
            delta in any_metadata_tx_update(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }
    }
}
