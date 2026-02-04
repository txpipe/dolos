use crc::{Crc, CRC_8_SMBUS};
use dolos_core::{ChainError, NsKey};
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx};
use serde::{Deserialize, Serialize};
use tracing::trace;

use super::WorkDeltas;
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

const CRC8_ALGO: Crc<u8> = Crc::<u8>::new(&CRC_8_SMBUS);

fn parse_cip67_label(asset_name: &[u8]) -> Option<u32> {
    if asset_name.len() < 4 {
        return None;
    }

    let label_hex = hex::encode(&asset_name[..4]);
    if !label_hex.starts_with('0') || !label_hex.ends_with('0') {
        return None;
    }

    let number_hex = &label_hex[1..5];
    let checksum_hex = &label_hex[5..7];
    let bytes = hex::decode(number_hex).ok()?;
    let checksum = format!("{:02x}", CRC8_ALGO.checksum(&bytes));
    if !checksum_hex.eq_ignore_ascii_case(&checksum) {
        return None;
    }

    u32::from_str_radix(number_hex, 16).ok()
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

        for asset in mint.assets() {
            trace!(%policy, asset = %hex::encode(asset.name()), "detected mint");

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
                let Some(label) = parse_cip67_label(asset.name()) else {
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
