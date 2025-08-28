use dolos_core::{State3Error, State3Store};
use pallas::crypto::hash::{Hash, Hasher};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraCert, MultiEraTx};

use crate::{
    model::PoolState,
    pallas_extras,
    roll::{BlockVisitor, DeltaBuilder, SliceBuilder},
};

pub struct PoolStateVisitor<'a, T>(&'a mut T);

impl<'a, T> From<&'a mut T> for PoolStateVisitor<'a, T> {
    fn from(value: &'a mut T) -> Self {
        Self(value)
    }
}

impl<'a, S: State3Store> BlockVisitor for PoolStateVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        if let Some(key) = block.header().issuer_vkey() {
            let operator: Hash<28> = Hasher::<224>::hash(key);
            self.0
                .slice
                .ensure_loaded_typed::<PoolState>(operator, self.0.store)?;
        }

        Ok(())
    }

    fn visit_cert(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(cert) = pallas_extras::cert_to_pool_state(cert) {
            self.0
                .slice
                .ensure_loaded_typed::<PoolState>(cert.operator, self.0.store)?;
        }

        Ok(())
    }
}

impl<'a> BlockVisitor for PoolStateVisitor<'a, DeltaBuilder<'_>> {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        if let Some(key) = block.header().issuer_vkey() {
            let operator: Hash<28> = Hasher::<224>::hash(key);
            if let Some(mut entity) = self.0.slice().get_entity_typed::<PoolState>(operator)? {
                let prev = entity.clone();
                entity.blocks_minted += 1;
                self.0
                    .delta_mut()
                    .override_entity(operator.as_slice(), entity, Some(prev));
            }
        }

        Ok(())
    }

    fn visit_cert(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(cert) = pallas_extras::cert_to_pool_state(cert) {
            let current = self
                .0
                .slice()
                .get_entity_typed::<PoolState>(cert.operator)?;

            let entity = PoolState {
                active_stake: 0,
                live_stake: 0,
                blocks_minted: 0,
                live_saturation: 0.0,
                vrf_keyhash: cert.vrf_keyhash,
                reward_account: cert.reward_account.to_vec(),
                pool_owners: cert.pool_owners.clone(),
                relays: cert.relays.clone(),
                declared_pledge: cert.pledge,
                margin_cost: cert.margin.clone(),
                fixed_cost: cert.cost,
                metadata: cert.pool_metadata.clone(),
            };

            self.0
                .delta_mut()
                .override_entity(cert.operator.as_slice(), entity, current);
        }

        Ok(())
    }
}
