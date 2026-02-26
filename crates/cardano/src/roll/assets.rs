use dolos_core::{ChainError, NsKey};
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx};
use serde::{Deserialize, Serialize};
use tracing::trace;

use super::WorkDeltas;
use crate::cip25::{cip25_metadata_for_tx, cip25_metadata_has_asset};
use crate::cip68::{has_cip25_metadata, parse_cip67_label_from_asset_name};
use crate::model::FixedNamespace as _;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataTxUpdate {
    policy: Hash<28>,
    asset: Vec<u8>,
    metadata_tx: Hash<32>,

    // undo
    prev_metadata_tx: Option<Hash<32>>,
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

    fn undo(&self, entity: &mut Option<AssetState>) {
        let entity = entity.get_or_insert_default();
        entity.metadata_tx = self.prev_metadata_tx;
    }
}

#[derive(Default, Clone)]
pub struct AssetStateVisitor;

impl BlockVisitor for AssetStateVisitor {
    fn visit_mint(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), ChainError> {
        let policy = mint.policy();
        let has_metadata = has_cip25_metadata(tx);
        let cip25_metadata = if has_metadata {
            cip25_metadata_for_tx(tx)
        } else {
            None
        };

        for asset in mint.assets() {
            trace!(%policy, asset = %hex::encode(asset.name()), "detected mint");

            let quantity: i128 = asset.mint_coin().unwrap_or_default().into();

            deltas.add_for_entity(MintStatsUpdate {
                policy: *policy,
                asset: asset.name().to_vec(),
                quantity,
                seen_in_tx: tx.hash(),
                seen_in_slot: block.slot(),
                is_first_mint: None,
            });

            if quantity > 0 {
                if let Some(metadata) = cip25_metadata.as_ref() {
                    if cip25_metadata_has_asset(metadata, policy, asset.name()) {
                        deltas.add_for_entity(MetadataTxUpdate::new(
                            *policy,
                            asset.name().to_vec(),
                            tx.hash(),
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    fn visit_output(
        &mut self,
        deltas: &mut WorkDeltas,
        _block: &MultiEraBlock,
        tx: &MultiEraTx,
        _index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        let Some(_datum_option) = output.datum() else {
            return Ok(());
        };

        for multi_asset in output.value().assets() {
            let policy = multi_asset.policy();
            for asset in multi_asset.assets() {
                let Some(label) = parse_cip67_label_from_asset_name(asset.name()) else {
                    continue;
                };

                if label == 100 {
                    deltas.add_for_entity(MetadataTxUpdate::new(
                        *policy,
                        asset.name().to_vec(),
                        tx.hash(),
                    ));
                }
            }
        }

        Ok(())
    }
}
