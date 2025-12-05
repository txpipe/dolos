use std::ops::Deref as _;

use dolos_core::{batch::WorkDeltas, BlockSlot, ChainError, NsKey};
use pallas::ledger::{
    primitives::conway::{self, Anchor, DRep},
    traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
};
use serde::{Deserialize, Serialize};

use crate::{
    drep_to_entity_key, model::DRepState, pallas_extras::stake_cred_to_drep, roll::BlockVisitor,
    CardanoLogic, FixedNamespace as _,
};

fn cert_drep(cert: &MultiEraCert) -> Option<DRep> {
    match &cert {
        MultiEraCert::Conway(conway) => match conway.deref().deref() {
            conway::Certificate::RegDRepCert(cert, _, _) => Some(stake_cred_to_drep(cert)),
            conway::Certificate::UnRegDRepCert(cert, _) => Some(stake_cred_to_drep(cert)),
            conway::Certificate::UpdateDRepCert(cert, _) => Some(stake_cred_to_drep(cert)),
            conway::Certificate::StakeVoteDeleg(_, _, drep) => Some(drep.clone()),
            conway::Certificate::StakeVoteRegDeleg(_, _, drep, _) => Some(drep.clone()),
            conway::Certificate::VoteRegDeleg(_, drep, _) => Some(drep.clone()),
            conway::Certificate::VoteDeleg(_, drep) => Some(drep.clone()),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepRegistration {
    drep: DRep,
    slot: u64,
    deposit: u64,
    anchor: Option<Anchor>,

    // undo
    prev_deposit: Option<u64>,
}

impl DRepRegistration {
    pub fn new(drep: DRep, slot: u64, deposit: u64, anchor: Option<Anchor>) -> Self {
        Self {
            drep,
            slot,
            deposit,
            anchor,
            prev_deposit: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepRegistration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, drep_to_entity_key(&self.drep)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep.clone()));

        // apply changes
        entity.initial_slot = Some(self.slot);
        entity.voting_power = self.deposit;
        entity.deposit = self.deposit;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep.clone()));

        entity.initial_slot = None;
        entity.voting_power = 0;
        entity.deposit = self.prev_deposit.unwrap_or(0);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepUnRegistration {
    drep: DRep,
    unregistered_at: BlockSlot,

    // undo data
    prev_voting_power: Option<u64>,
    prev_deposit: Option<u64>,
    prev_unregistered_at: Option<BlockSlot>,
}

impl DRepUnRegistration {
    pub fn new(drep: DRep, unregistered_at: BlockSlot) -> Self {
        Self {
            drep,
            unregistered_at,
            prev_voting_power: None,
            prev_deposit: None,
            prev_unregistered_at: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepUnRegistration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, drep_to_entity_key(&self.drep)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.as_mut().expect("can't unregister missing drep");

        // save undo data
        self.prev_voting_power = Some(entity.voting_power);
        self.prev_unregistered_at = entity.unregistered_at;
        self.prev_deposit = Some(entity.deposit);

        // apply changes
        entity.voting_power = 0;
        entity.unregistered_at = Some(self.unregistered_at);
        entity.deposit = 0;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let entity = entity.as_mut().expect("can't undo missing drep");

        entity.voting_power = self.prev_voting_power.unwrap();
        entity.unregistered_at = self.prev_unregistered_at;
        entity.deposit = self.prev_deposit.unwrap_or(0);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepActivity {
    drep: DRep,
    slot: u64,
    previous_last_active_slot: Option<u64>,
}

impl DRepActivity {
    pub fn new(drep: DRep, slot: u64) -> Self {
        Self {
            drep,
            slot,
            previous_last_active_slot: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepActivity {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, drep_to_entity_key(&self.drep)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep.clone()));

        // save undo info
        self.previous_last_active_slot = entity.last_active_slot;

        // apply changes
        entity.last_active_slot = Some(self.slot);
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let entity = entity.as_mut().expect("can't undo missing drep");

        entity.last_active_slot = self.previous_last_active_slot;
    }
}

#[derive(Default, Clone)]
pub struct DRepStateVisitor;

impl BlockVisitor for DRepStateVisitor {
    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        let Some(drep) = cert_drep(cert) else {
            return Ok(());
        };

        if let MultiEraCert::Conway(conway) = &cert {
            match conway.deref().deref() {
                conway::Certificate::RegDRepCert(_, deposit, anchor) => {
                    deltas.add_for_entity(DRepRegistration::new(
                        drep.clone(),
                        block.slot(),
                        *deposit,
                        anchor.clone(),
                    ));
                }
                conway::Certificate::UnRegDRepCert(_, _) => {
                    deltas.add_for_entity(DRepUnRegistration::new(drep.clone(), block.slot()));
                }
                _ => (),
            }
        };

        deltas.add_for_entity(DRepActivity::new(drep.clone(), block.slot()));

        Ok(())
    }
}
