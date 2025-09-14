use std::{borrow::Cow, ops::Deref as _};

use dolos_core::{batch::WorkDeltas, ChainError, NsKey};
use pallas::{
    codec::minicbor,
    ledger::{
        primitives::{
            conway::{self, Anchor},
            StakeCredential,
        },
        traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    model::DRepState,
    roll::{BlockVisitor, DeltaBuilder},
    CardanoDelta, CardanoLogic, FixedNamespace as _,
};

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
    cred: StakeCredential,
    slot: u64,
    deposit: u64,
    anchor: Option<Anchor>,

    // undo
    was_retired: bool,
}

impl DRepRegistration {
    pub fn new(cred: StakeCredential, slot: u64, deposit: u64, anchor: Option<Anchor>) -> Self {
        Self {
            cred,
            slot,
            deposit,
            anchor,
            was_retired: false,
        }
    }
}

impl dolos_core::EntityDelta for DRepRegistration {
    type Entity = DRepState;

    fn key(&self) -> Cow<'_, NsKey> {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        Cow::Owned(NsKey::from((DRepState::NS, enc)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_default();

        // save undo info
        self.was_retired = entity.retired;

        // apply changes
        entity.initial_slot = Some(self.slot);
        entity.voting_power = self.deposit;
        entity.retired = false;
    }

    fn undo(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_default();
        entity.initial_slot = None;
        entity.voting_power = 0;
        entity.retired = self.was_retired;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepUnRegistration {
    cred: StakeCredential,
    slot: u64,
    deposit: u64,

    // undo data
    prev_voting_power: Option<u64>,
}

impl DRepUnRegistration {
    pub fn new(cred: StakeCredential, slot: u64, deposit: u64) -> Self {
        Self {
            cred,
            slot,
            deposit,
            prev_voting_power: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepUnRegistration {
    type Entity = DRepState;

    fn key(&self) -> Cow<'_, NsKey> {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        Cow::Owned(NsKey::from((DRepState::NS, enc)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_default();

        // save undo data
        self.prev_voting_power = Some(entity.voting_power);

        // apply changes
        entity.voting_power = 0;
        entity.retired = true;
    }

    fn undo(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.get_or_insert_default();
        entity.voting_power = self.prev_voting_power.unwrap();
        entity.retired = false;
    }
}

#[derive(Default)]
pub struct DRepStateVisitor;

impl BlockVisitor for DRepStateVisitor {
    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        if let MultiEraCert::Conway(conway) = &cert {
            match conway.deref().deref() {
                conway::Certificate::RegDRepCert(cred, deposit, anchor) => {
                    deltas.add_for_entity(DRepRegistration::new(
                        cred.clone(),
                        block.slot(),
                        *deposit,
                        anchor.clone(),
                    ));
                }
                conway::Certificate::UnRegDRepCert(cred, coin) => {
                    deltas.add_for_entity(DRepUnRegistration::new(
                        cred.clone(),
                        block.slot(),
                        *coin,
                    ));
                }
                _ => (),
            }
        };

        Ok(())
    }
}
