use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraPolicyAssets, MultiEraTx};
use tracing::debug;

use crate::{
    model::AssetState,
    roll::{BlockVisitor, DeltaBuilder, SliceBuilder, State3Error, State3Store},
};

pub struct AssetStateVisitor<'a, T>(&'a mut T);

impl<'a, T> From<&'a mut T> for AssetStateVisitor<'a, T> {
    fn from(value: &'a mut T) -> Self {
        Self(value)
    }
}

impl<T> AssetStateVisitor<'_, T> {
    fn define_subject(policy: &Hash<28>, asset: &[u8]) -> Vec<u8> {
        let mut subject = vec![];
        subject.extend_from_slice(policy.as_slice());
        subject.extend_from_slice(asset);

        subject
    }
}

impl<'a, S: State3Store> BlockVisitor for AssetStateVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_mint(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        let policy = mint.policy();

        for asset in mint.assets() {
            let subject = Self::define_subject(policy, asset.name());

            self.0
                .slice
                .ensure_loaded_typed::<AssetState>(&subject, self.0.store)?;
        }

        Ok(())
    }
}

impl<'a> BlockVisitor for AssetStateVisitor<'a, DeltaBuilder<'_>> {
    fn visit_mint(
        &mut self,
        _: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        let policy = mint.policy();

        for asset in mint.assets() {
            let subject = Self::define_subject(policy, asset.name());

            debug!(subject = %hex::encode(&subject), "tracking asset");

            let current = self
                .0
                .slice()
                .get_entity_typed::<AssetState>(&subject)?
                .unwrap_or(AssetState {
                    quantity_bytes: 0_u128.to_be_bytes(),
                    initial_tx: tx.hash(),
                    mint_tx_count: 0,
                });

            let mut new = current.clone();
            new.add_quantity(asset.mint_coin().unwrap_or_default().into())?;
            new.mint_tx_count += 1;

            self.0
                .delta_mut()
                .override_entity(subject, new, Some(current));
        }

        Ok(())
    }
}
