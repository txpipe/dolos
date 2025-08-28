use std::ops::Deref as _;

use dolos_core::{State3Error, State3Store};
use pallas::ledger::{
    primitives::{conway, StakeCredential},
    traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
};

use crate::{
    model::DRepState,
    roll::{BlockVisitor, DeltaBuilder, SliceBuilder},
};

const DREP_KEY_PREFIX: u8 = 0b00100010;
const DREP_SCRIPT_PREFIX: u8 = 0b00100011;

pub struct DRepStateVisitor<'a, T>(&'a mut T);

impl<'a, T> From<&'a mut T> for DRepStateVisitor<'a, T> {
    fn from(value: &'a mut T) -> Self {
        Self(value)
    }
}

impl<T> DRepStateVisitor<'_, T> {
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
                conway::Certificate::RegDRepCert(cert, _, _) => Some(Self::cred_to_id(cert)),
                conway::Certificate::UnRegDRepCert(cert, _) => Some(Self::cred_to_id(cert)),
                conway::Certificate::UpdateDRepCert(cert, _) => Some(Self::cred_to_id(cert)),
                conway::Certificate::StakeVoteDeleg(_, _, drep) => Some(Self::drep_to_id(drep)),
                conway::Certificate::VoteRegDeleg(_, drep, _) => Some(Self::drep_to_id(drep)),
                conway::Certificate::VoteDeleg(_, drep) => Some(Self::drep_to_id(drep)),
                _ => None,
            },
            _ => None,
        }
    }
}

impl<'a, S: State3Store> BlockVisitor for DRepStateVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_cert(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(id) = Self::cert_to_id(cert) {
            self.0
                .slice
                .ensure_loaded_typed::<DRepState>(&id, self.0.store)?;
        }

        Ok(())
    }
}

impl<'a> BlockVisitor for DRepStateVisitor<'a, DeltaBuilder<'_>> {
    fn visit_cert(
        &mut self,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(drep_id) = Self::cert_to_id(cert) {
            let current = self
                .0
                .slice()
                .get_entity_typed::<DRepState>(&drep_id)?
                .unwrap_or(DRepState {
                    drep_id: drep_id.clone(),
                    initial_slot: Some(block.slot()),
                    voting_power: 0,
                    last_active_slot: None,
                    retired: false,
                });
            let mut new = current.clone();
            new.last_active_slot = Some(block.slot());

            if let MultiEraCert::Conway(conway) = &cert {
                match conway.deref().deref() {
                    conway::Certificate::RegDRepCert(_, coin, _) => {
                        new.voting_power += coin;
                        new.retired = false;
                    }
                    conway::Certificate::UnRegDRepCert(_, coin) => {
                        new.voting_power -= coin;
                        new.retired = true;
                    }
                    conway::Certificate::VoteRegDeleg(_, _, coin) => {
                        new.voting_power += coin;
                    }
                    _ => (),
                }
            };

            self.0
                .delta_mut()
                .override_entity(drep_id, new, Some(current));
        }

        Ok(())
    }
}
