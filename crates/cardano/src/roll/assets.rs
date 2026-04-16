use dolos_core::ChainError;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx};
use tracing::trace;

use super::WorkDeltas;
use crate::cip25::{cip25_metadata_for_tx, cip25_metadata_has_asset};
use crate::cip68::{has_cip25_metadata, parse_cip67_label_from_asset_name};
use crate::roll::BlockVisitor;
use crate::{MetadataTxUpdate, MintStatsUpdate};

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
