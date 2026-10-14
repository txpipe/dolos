//! First mints (decision 0049): the first block of each epoch that mints an
//! asset, staged until the epoch seals.

use std::collections::HashSet;

use dolos_core::{ChainError, Genesis, TxOrder};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::Epoch,
        traverse::{MultiEraBlock, MultiEraPolicyAssets, MultiEraTx},
    },
};

use super::{BlockVisitor, WorkDeltas};
use crate::{PParamsSet, StageFirstMint, StagedFirstMint};

#[derive(Default)]
pub struct FirstMintVisitor {
    epoch: Epoch,

    /// Subjects already staged by this block: a repeat changes no staged row
    /// but would still cost a WAL delta.
    staged: HashSet<(Hash<28>, Vec<u8>)>,
}

impl BlockVisitor for FirstMintVisitor {
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
        _: &MultiEraTx,
        order: &TxOrder,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), ChainError> {
        let policy = mint.policy();

        for asset in mint.assets() {
            if self.staged.insert((*policy, asset.name().to_vec())) {
                deltas.add_for_entity(StageFirstMint::new(StagedFirstMint {
                    policy: *policy,
                    name: asset.name().to_vec(),
                    epoch: self.epoch,
                    slot: block.slot(),
                    tx_index: *order as u32,
                }));
            }
        }

        Ok(())
    }
}
