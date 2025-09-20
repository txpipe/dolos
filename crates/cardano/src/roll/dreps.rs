use std::ops::Deref as _;

use dolos_core::{batch::WorkDeltas, ChainError, NsKey};
use pallas::ledger::{
    primitives::{
        conway::{self, Anchor},
        StakeCredential,
    },
    traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
};
use serde::{Deserialize, Serialize};

use crate::{model::DRepState, roll::BlockVisitor, CardanoLogic, FixedNamespace as _};

const DREP_KEY_PREFIX: u8 = 0b00100010;
const DREP_SCRIPT_PREFIX: u8 = 0b00100011;

fn cred_to_id(cred: &StakeCredential) -> Vec<u8> {
    match cred {
        StakeCredential::AddrKeyhash(key) => [vec![DREP_KEY_PREFIX], key.to_vec()].concat(),
        StakeCredential::ScriptHash(key) => [vec![DREP_SCRIPT_PREFIX], key.to_vec()].concat(),
    }
}

fn drep_to_id(drep: &conway::DRep) -> Vec<u8> {
    match drep {
        conway::DRep::Key(key) => [vec![DREP_KEY_PREFIX], key.to_vec()].concat(),
        conway::DRep::Script(key) => [vec![DREP_SCRIPT_PREFIX], key.to_vec()].concat(),
        // Invented keys for convenience
        conway::DRep::Abstain => vec![0],
        conway::DRep::NoConfidence => vec![1],
    }
}

fn cert_to_id(cert: &MultiEraCert) -> Option<Vec<u8>> {
    match &cert {
        MultiEraCert::Conway(conway) => match conway.deref().deref() {
            conway::Certificate::RegDRepCert(cert, _, _) => Some(cred_to_id(cert)),
            conway::Certificate::UnRegDRepCert(cert, _) => Some(cred_to_id(cert)),
            conway::Certificate::UpdateDRepCert(cert, _) => Some(cred_to_id(cert)),
            conway::Certificate::StakeVoteDeleg(_, _, drep) => Some(drep_to_id(drep)),
            conway::Certificate::VoteRegDeleg(_, drep, _) => Some(drep_to_id(drep)),
            conway::Certificate::VoteDeleg(_, drep) => Some(drep_to_id(drep)),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepRegistration {
    drep_id: Vec<u8>,
    slot: u64,
    deposit: u64,
    anchor: Option<Anchor>,

    // undo
    was_retired: bool,
}

impl DRepRegistration {
    pub fn new(drep_id: Vec<u8>, slot: u64, deposit: u64, anchor: Option<Anchor>) -> Self {
        Self {
            drep_id,
            slot,
            deposit,
            anchor,
            was_retired: false,
        }
    }
}

impl dolos_core::EntityDelta for DRepRegistration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, self.drep_id.clone()))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep_id.clone()));

        // save undo info
        self.was_retired = entity.retired;

        // apply changes
        entity.initial_slot = Some(self.slot);
        entity.voting_power = self.deposit;
        entity.retired = false;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep_id.clone()));
        entity.initial_slot = None;
        entity.voting_power = 0;
        entity.retired = self.was_retired;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepUnRegistration {
    drep_id: Vec<u8>,
    deposit: u64,

    // undo data
    prev_voting_power: Option<u64>,
}

impl DRepUnRegistration {
    pub fn new(drep_id: Vec<u8>, deposit: u64) -> Self {
        Self {
            drep_id,
            deposit,
            prev_voting_power: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepUnRegistration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, self.drep_id.clone()))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep_id.clone()));

        // save undo data
        self.prev_voting_power = Some(entity.voting_power);

        // apply changes
        entity.voting_power = 0;
        entity.retired = true;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep_id.clone()));
        entity.voting_power = self.prev_voting_power.unwrap();
        entity.retired = false;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepActivity {
    drep_id: Vec<u8>,
    slot: u64,
    previous_last_active_slot: Option<u64>,
}

impl DRepActivity {
    pub fn new(drep_id: Vec<u8>, slot: u64) -> Self {
        Self {
            drep_id,
            slot,
            previous_last_active_slot: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepActivity {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, self.drep_id.clone()))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep_id.clone()));

        // save undo info
        self.previous_last_active_slot = entity.last_active_slot;

        // apply changes
        entity.last_active_slot = Some(self.slot);
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep_id.clone()));
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
        if let Some(drep_id) = cert_to_id(cert) {
            deltas.add_for_entity(DRepActivity::new(drep_id.clone(), block.slot()));
            if let MultiEraCert::Conway(conway) = &cert {
                match conway.deref().deref() {
                    conway::Certificate::RegDRepCert(_, deposit, anchor) => {
                        deltas.add_for_entity(DRepRegistration::new(
                            drep_id.clone(),
                            block.slot(),
                            *deposit,
                            anchor.clone(),
                        ));
                    }
                    conway::Certificate::UnRegDRepCert(_, coin) => {
                        deltas.add_for_entity(DRepUnRegistration::new(drep_id.clone(), *coin));
                    }
                    _ => (),
                }
            };
        }

        Ok(())
    }
}
