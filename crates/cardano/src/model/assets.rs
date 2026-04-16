use dolos_core::NsKey;
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
};
use serde::{Deserialize, Serialize};

use super::FixedNamespace as _;

#[derive(Debug, Encode, Decode, Clone, Default)]
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
    pub(crate) is_first_mint: Option<bool>,
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
        let entity = entity.get_or_insert_default();

        entity.add_quantity(self.quantity);
        entity.mint_tx_count += 1;

        if entity.initial_slot.unwrap_or(u64::MAX) > self.seen_in_slot {
            entity.initial_slot = Some(self.seen_in_slot);
            entity.initial_tx = Some(self.seen_in_tx);
            self.is_first_mint = Some(true);
        }
    }

    fn undo(&self, _entity: &mut Option<AssetState>) {
        // no-op: undo not yet comprehensively implemented
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataTxUpdate {
    pub(crate) policy: Hash<28>,
    pub(crate) asset: Vec<u8>,
    pub(crate) metadata_tx: Hash<32>,

    // undo
    pub(crate) prev_metadata_tx: Option<Hash<32>>,
}

impl MetadataTxUpdate {
    pub fn new(policy: Hash<28>, asset: Vec<u8>, metadata_tx: Hash<32>) -> Self {
        Self {
            policy,
            asset,
            metadata_tx,
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
        let entity = entity.get_or_insert_default();
        self.prev_metadata_tx = entity.metadata_tx;
        entity.metadata_tx = Some(self.metadata_tx);
    }

    fn undo(&self, _entity: &mut Option<AssetState>) {
        // no-op: undo not yet comprehensively implemented
    }
}
