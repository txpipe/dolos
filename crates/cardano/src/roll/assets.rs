use dolos_core::{ChainError, NsKey};
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx};
use serde::{Deserialize, Serialize};
use tracing::trace;

use super::WorkDeltas;
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

            if has_metadata && quantity > 0 {
                deltas.add_for_entity(MetadataTxUpdate::new(
                    *policy,
                    asset.name().to_vec(),
                    tx.hash(),
                ));
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

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::EntityDelta;

    struct TxFixture {
        tx_id: u64,
        tx_hash: &'static str,
        cbor_hex: &'static str,
    }

    const CIP25_ASSET_HEX: &str =
        "d5e6bf0500378d4f0da4e8dde6becec7621cd8cbf5cbb9b87013d4cc537061636542756433343132";

    const CIP25_TX_FIXTURES: &[TxFixture] = &[
        TxFixture {
            tx_id: 5_370_870,
            tx_hash: "faa2833937966c130380d1dfa20014c9a899a4d1353f5867610a73fba6dbdb2d",
            cbor_hex: include_str!(
                "fixtures/faa2833937966c130380d1dfa20014c9a899a4d1353f5867610a73fba6dbdb2d"
            ),
        },
        TxFixture {
            tx_id: 5_398_081,
            tx_hash: "1fbd0a04ca35178e547f14017b1434d7f7617b3f19b3258b6a8c3950a3bf60cb",
            cbor_hex: include_str!(
                "fixtures/1fbd0a04ca35178e547f14017b1434d7f7617b3f19b3258b6a8c3950a3bf60cb"
            ),
        },
        TxFixture {
            tx_id: 5_398_393,
            tx_hash: "273eb66eed462971c9a05be3ab322e0753fbeda80b88eee9f2cb2c8fb3063a2b",
            cbor_hex: include_str!(
                "fixtures/273eb66eed462971c9a05be3ab322e0753fbeda80b88eee9f2cb2c8fb3063a2b"
            ),
        },
        TxFixture {
            tx_id: 5_499_889,
            tx_hash: "2bb13aa57d29e913d0d651696ee30aff2ae45eef01e2e017d8f02aa2ec36fbdb",
            cbor_hex: include_str!(
                "fixtures/2bb13aa57d29e913d0d651696ee30aff2ae45eef01e2e017d8f02aa2ec36fbdb"
            ),
        },
        TxFixture {
            tx_id: 5_499_941,
            tx_hash: "1812cb3d7b9f3baa1653f92e69eb74fc3ddaa1e651b9e2d6e81acf4e6a7a6e5d",
            cbor_hex: include_str!(
                "fixtures/1812cb3d7b9f3baa1653f92e69eb74fc3ddaa1e651b9e2d6e81acf4e6a7a6e5d"
            ),
        },
        TxFixture {
            tx_id: 5_500_216,
            tx_hash: "7b37bd31cf33c1e2e9005a97c3fe2d5544e92233b0754092187a9c36a4915c4e",
            cbor_hex: include_str!(
                "fixtures/7b37bd31cf33c1e2e9005a97c3fe2d5544e92233b0754092187a9c36a4915c4e"
            ),
        },
        TxFixture {
            tx_id: 5_500_233,
            tx_hash: "8d03f6e3ac5e55236f13ca744cbaf05e2f5893438a5fb296189b19c4e783c04f",
            cbor_hex: include_str!(
                "fixtures/8d03f6e3ac5e55236f13ca744cbaf05e2f5893438a5fb296189b19c4e783c04f"
            ),
        },
    ];

    const EXPECTED_METADATA_UPDATES: &[&str] = &[
        "faa2833937966c130380d1dfa20014c9a899a4d1353f5867610a73fba6dbdb2d",
        "273eb66eed462971c9a05be3ab322e0753fbeda80b88eee9f2cb2c8fb3063a2b",
        "2bb13aa57d29e913d0d651696ee30aff2ae45eef01e2e017d8f02aa2ec36fbdb",
        "7b37bd31cf33c1e2e9005a97c3fe2d5544e92233b0754092187a9c36a4915c4e",
    ];

    #[test]
    fn cip25_metadata_tx_tracks_latest_mint_metadata() {
        let asset_bytes = hex::decode(CIP25_ASSET_HEX).expect("valid asset hex");
        let policy = Hash::<28>::from(&asset_bytes[..28]);
        let asset_name = asset_bytes[28..].to_vec();

        let mut updates = Vec::new();
        let mut entity = Some(AssetState::default());

        for fixture in CIP25_TX_FIXTURES {
            let cbor = hex::decode(fixture.cbor_hex.trim()).expect("valid tx cbor hex fixture");
            let tx = MultiEraTx::decode(&cbor).expect("valid tx cbor");

            assert_eq!(hex::encode(tx.hash().as_slice()), fixture.tx_hash);

            let mut saw_asset = false;
            for mint in tx.mints() {
                if mint.policy().as_slice() != policy.as_slice() {
                    continue;
                }

                for asset in mint.assets() {
                    if asset.name() != asset_name.as_slice() {
                        continue;
                    }

                    saw_asset = true;
                    let quantity: i128 = asset.mint_coin().unwrap_or_default().into();
                    if quantity > 0 && has_cip25_metadata(&tx) {
                        updates.push(fixture.tx_hash);
                        let mut delta =
                            MetadataTxUpdate::new(policy, asset_name.clone(), tx.hash());
                        delta.apply(&mut entity);
                    }
                }
            }

            assert!(saw_asset, "asset not minted in tx_id {}", fixture.tx_id);
        }

        assert_eq!(updates, EXPECTED_METADATA_UPDATES);

        let state = entity.expect("asset state should exist");
        let metadata_tx = state.metadata_tx.expect("metadata tx should be set");
        assert_eq!(
            hex::encode(metadata_tx.as_slice()),
            EXPECTED_METADATA_UPDATES
                .last()
                .expect("expected metadata updates")
                .to_string()
        );
    }
}
