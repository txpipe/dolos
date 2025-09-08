use std::borrow::Cow;

use dolos_core::batch::WorkDeltas;
use dolos_core::{ChainError, NsKey};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraCert, MultiEraTx};
use serde::{Deserialize, Serialize};

use crate::model::FixedNamespace as _;
use crate::pallas_extras::MultiEraPoolRegistration;
use crate::CardanoLogic;
use crate::{model::PoolState, pallas_extras, roll::BlockVisitor};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolRegistration {
    cert: MultiEraPoolRegistration,

    // undo
    prev_entity: Option<PoolState>,
}

impl PoolRegistration {
    pub fn new(cert: MultiEraPoolRegistration) -> Self {
        Self {
            cert,
            prev_entity: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolRegistration {
    type Entity = PoolState;

    fn key(&self) -> Cow<'_, NsKey> {
        let key = self.cert.operator.as_slice();
        Cow::Owned(NsKey::from((PoolState::NS, key)))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        self.prev_entity = entity.clone();

        let entity = entity.get_or_insert_with(|| PoolState::new(self.cert.vrf_keyhash));

        entity.vrf_keyhash = self.cert.vrf_keyhash;
        entity.reward_account = self.cert.reward_account.to_vec();
        entity.pool_owners = self.cert.pool_owners.clone();
        entity.relays = self.cert.relays.clone();
        entity.declared_pledge = self.cert.pledge;
        entity.margin_cost = self.cert.margin.clone();
        entity.fixed_cost = self.cert.cost;
        entity.metadata = self.cert.pool_metadata.clone();
    }

    fn undo(&mut self, entity: &mut Option<PoolState>) {
        *entity = self.prev_entity.clone();
    }
}

pub struct PoolStateVisitor;

impl BlockVisitor for PoolStateVisitor {
    fn visit_cert(
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        if let Some(cert) = pallas_extras::cert_to_pool_state(cert) {
            deltas.add_for_entity(PoolRegistration::new(cert));
        }

        Ok(())
    }
}
