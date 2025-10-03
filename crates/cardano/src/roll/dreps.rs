use std::ops::Deref as _;

use dolos_core::{batch::WorkDeltas, ChainError, NsKey};
use pallas::ledger::{
    primitives::{conway::{self, Anchor, DRep}, Epoch},
    traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
};
use serde::{Deserialize, Serialize};

use crate::{
    drep_to_entity_key, model::DRepState, pallas_extras::stake_cred_to_drep, roll::BlockVisitor,
    CardanoLogic, FixedNamespace as _, PParamsSet,
};

fn cert_drep(cert: &MultiEraCert) -> Option<DRep> {
    match &cert {
        MultiEraCert::Conway(conway) => match conway.deref().deref() {
            conway::Certificate::RegDRepCert(cert, _, _) => Some(stake_cred_to_drep(cert)),
            conway::Certificate::UnRegDRepCert(cert, _) => Some(stake_cred_to_drep(cert)),
            conway::Certificate::UpdateDRepCert(cert, _) => Some(stake_cred_to_drep(cert)),
            conway::Certificate::StakeVoteDeleg(_, _, drep) => Some(drep.clone()),
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
    prev_retiring_epoch: Option<u64>,
    prev_deposit: Option<u64>,
}

impl DRepRegistration {
    pub fn new(drep: DRep, slot: u64, deposit: u64, anchor: Option<Anchor>) -> Self {
        Self {
            drep,
            slot,
            deposit,
            anchor,
            prev_retiring_epoch: None,
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
        let entity = entity.get_or_insert_default();

        // save undo info
        self.prev_retiring_epoch = entity.retiring_epoch;

        // apply changes
        entity.initial_slot = Some(self.slot);
        entity.voting_power = self.deposit;
        entity.retiring_epoch = None;
        entity.deposit = self.deposit;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_default();
        entity.initial_slot = None;
        entity.voting_power = 0;
        entity.retiring_epoch = self.prev_retiring_epoch;
        entity.deposit = self.prev_deposit.unwrap_or(0);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepUnRegistration {
    drep: DRep,
    retiring_epoch: Epoch,

    // undo data
    prev_voting_power: Option<u64>,
    prev_deposit: Option<u64>,
    prev_retiring_epoch: Option<u64>
}

impl DRepUnRegistration {
    pub fn new(drep: DRep, retiring_epoch: Epoch) -> Self {
        Self {
            drep,
            retiring_epoch,
            prev_voting_power: None,
            prev_deposit: None,
            prev_retiring_epoch: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepUnRegistration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, drep_to_entity_key(&self.drep)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_default();

        // save undo data
        self.prev_voting_power = Some(entity.voting_power);
        self.prev_retiring_epoch = entity.retiring_epoch;
        self.prev_deposit = Some(entity.deposit);

        // apply changes
        entity.voting_power = 0;
        entity.retiring_epoch = Some(self.retiring_epoch);
        entity.deposit = 0;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_default();
        entity.voting_power = self.prev_voting_power.unwrap();
        entity.retiring_epoch = self.prev_retiring_epoch;
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
        let entity = entity.get_or_insert_default();

        // save undo info
        self.previous_last_active_slot = entity.last_active_slot;

        // apply changes
        entity.last_active_slot = Some(self.slot);
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_default();
        entity.last_active_slot = self.previous_last_active_slot;
    }
}

#[derive(Default, Clone)]
pub struct DRepStateVisitor {
    epoch: Option<Epoch>,
}

impl BlockVisitor for DRepStateVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &PParamsSet,
        epoch: Epoch,
    ) -> Result<(), ChainError> {
        self.epoch = Some(epoch);
        Ok(())
    }

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

        deltas.add_for_entity(DRepActivity::new(drep.clone(), block.slot()));

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
                    deltas.add_for_entity(DRepUnRegistration::new(drep.clone(), self.epoch.expect("set in root")));
                }
                _ => (),
            }
        };

        Ok(())
    }
}
