use dolos_core::batch::WorkDeltas;
use dolos_core::{ChainError, NsKey};
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraPolicyAssets, MultiEraTx};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::model::FixedNamespace as _;
use crate::CardanoLogic;
use crate::{model::AssetState, roll::BlockVisitor};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintStatsUpdate {
    policy: Hash<28>,
    asset: Vec<u8>,
    quantity: i128,
    seen_in_tx: Hash<32>,
    seen_in_slot: u64,

    // undo
    is_first_mint: Option<bool>,
}

impl dolos_core::EntityDelta for MintStatsUpdate {
    type Entity = AssetState;

    fn key(&self) -> NsKey {
        let mut key = vec![];
        key.extend_from_slice(self.policy.as_slice());
        key.extend_from_slice(self.asset.as_slice());
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

    fn undo(&self, entity: &mut Option<AssetState>) {
        let entity = entity.get_or_insert_default();

        entity.add_quantity(-self.quantity);
        entity.mint_tx_count -= 1;

        if self.is_first_mint.unwrap_or(false) {
            entity.initial_slot = None;
            entity.initial_tx = None;
        }
    }
}

#[derive(Default)]
pub struct AssetStateVisitor;

impl BlockVisitor for AssetStateVisitor {
    fn visit_mint(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), ChainError> {
        let policy = mint.policy();

        for asset in mint.assets() {
            debug!(%policy, asset = %hex::encode(asset.name()), "detected mint");

            deltas.add_for_entity(MintStatsUpdate {
                policy: *policy,
                asset: asset.name().to_vec(),
                quantity: asset.mint_coin().unwrap_or_default().into(),
                seen_in_tx: tx.hash(),
                seen_in_slot: block.slot(),
                is_first_mint: None,
            });
        }

        Ok(())
    }
}
