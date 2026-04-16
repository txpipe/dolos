use std::{collections::HashMap, ops::Deref as _};

use dolos_core::{ChainError, TxOrder, TxoRef};
use pallas::ledger::{
    primitives::conway::{self, DRep, Voter},
    traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
};

use super::WorkDeltas;
use crate::{
    owned::OwnedMultiEraOutput, pallas_extras::stake_cred_to_drep, roll::BlockVisitor,
    DRepActivity, DRepRegistration, DRepUnRegistration,
};

fn cert_drep(cert: &MultiEraCert) -> Option<DRep> {
    match &cert {
        MultiEraCert::Conway(conway) => match conway.deref().deref() {
            conway::Certificate::RegDRepCert(cert, _, _) => Some(stake_cred_to_drep(cert)),
            conway::Certificate::UnRegDRepCert(cert, _) => Some(stake_cred_to_drep(cert)),
            conway::Certificate::UpdateDRepCert(cert, _) => Some(stake_cred_to_drep(cert)),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Default, Clone)]
pub struct DRepStateVisitor;

impl BlockVisitor for DRepStateVisitor {
    fn visit_tx(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        _: &HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Result<(), ChainError> {
        let MultiEraTx::Conway(conway_tx) = tx else {
            return Ok(());
        };

        let Some(voting_procedures) = &conway_tx.transaction_body.voting_procedures else {
            return Ok(());
        };

        for (voter, _) in voting_procedures.iter() {
            let drep = match voter {
                Voter::DRepKey(hash) => DRep::Key(*hash),
                Voter::DRepScript(hash) => DRep::Script(*hash),
                _ => continue,
            };

            deltas.add_for_entity(DRepActivity::new(drep, block.slot()));
        }

        Ok(())
    }

    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        order: &TxOrder,
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
                        *order,
                        *deposit,
                        anchor.clone(),
                    ));
                }
                conway::Certificate::UnRegDRepCert(_, _) => {
                    deltas.add_for_entity(DRepUnRegistration::new(
                        drep.clone(),
                        block.slot(),
                        *order,
                    ));
                }
                _ => (),
            }
        };

        deltas.add_for_entity(DRepActivity::new(drep.clone(), block.slot()));

        Ok(())
    }
}
