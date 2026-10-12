use std::collections::HashSet;

use dolos_core::{ChainError, Genesis, TxOrder};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::Epoch,
        traverse::{MultiEraBlock, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx},
    },
};
use tracing::trace;

use super::WorkDeltas;
use crate::cip25::{cip25_metadata_for_tx, cip25_metadata_has_asset};
use crate::cip68::{has_cip25_metadata, parse_cip67_label_from_asset_name};
use crate::roll::BlockVisitor;
use crate::{MetadataTxUpdate, MintStatsUpdate, PParamsSet, StageFirstMint, StagedFirstMint};

#[derive(Default, Clone)]
pub struct AssetStateVisitor {
    epoch: Epoch,

    /// Subjects already staged by this block: a repeat changes no staged row
    /// but would still cost a WAL delta.
    staged: HashSet<(Hash<28>, Vec<u8>)>,
}

impl BlockVisitor for AssetStateVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &Genesis,
        _: &PParamsSet,
        epoch: Epoch,
        _: u64,
        _: u16,
    ) -> Result<(), ChainError> {
        self.epoch = epoch;
        Ok(())
    }

    fn visit_mint(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        order: &TxOrder,
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

            deltas.add_for_entity(MintStatsUpdate::new(
                *policy,
                asset.name().to_vec(),
                quantity,
                tx.hash(),
                block.slot(),
            ));

            if self.staged.insert((*policy, asset.name().to_vec())) {
                deltas.add_for_entity(StageFirstMint::new(StagedFirstMint {
                    policy: *policy,
                    name: asset.name().to_vec(),
                    epoch: self.epoch,
                    slot: block.slot(),
                    tx_index: *order as u32,
                }));
            }

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
