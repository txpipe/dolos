use std::borrow::Cow;
use std::u64;

use dolos_core::{NsKey, StateDelta};
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraPolicyAssets, MultiEraTx};
use tracing::debug;

use crate::model::FixedNamespace as _;
use crate::roll::CardanoDelta;
use crate::{
    model::AssetState,
    roll::{BlockVisitor, State3Error},
};

#[derive(Debug, Clone)]
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

    fn key(&self) -> Cow<'_, NsKey> {
        let mut key = vec![];
        key.extend_from_slice(self.policy.as_slice());
        key.extend_from_slice(self.asset.as_slice());
        Cow::Owned(NsKey::from((AssetState::NS, key)))
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

    fn undo(&mut self, entity: &mut Option<AssetState>) {
        let entity = entity.get_or_insert_default();

        entity.add_quantity(-self.quantity);
        entity.mint_tx_count -= 1;

        if self.is_first_mint.unwrap_or(false) {
            entity.initial_slot = None;
            entity.initial_tx = None;
        }
    }
}

pub struct AssetStateVisitor<'a> {
    delta: &'a mut StateDelta<CardanoDelta>,
}

impl<'a> From<&'a mut StateDelta<CardanoDelta>> for AssetStateVisitor<'a> {
    fn from(delta: &'a mut StateDelta<CardanoDelta>) -> Self {
        Self { delta }
    }
}

impl AssetStateVisitor<'_> {
    fn define_subject(policy: &Hash<28>, asset: &[u8]) -> Vec<u8> {
        let mut subject = vec![];
        subject.extend_from_slice(policy.as_slice());
        subject.extend_from_slice(asset);

        subject
    }
}

impl<'a> BlockVisitor for AssetStateVisitor<'a> {
    fn visit_mint(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        let policy = mint.policy();

        for asset in mint.assets() {
            debug!(%policy, asset = %hex::encode(&asset.name()), "detected mint");

            self.delta.add_delta(MintStatsUpdate {
                policy: policy.clone(),
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
