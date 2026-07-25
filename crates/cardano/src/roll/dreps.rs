use std::{collections::HashMap, ops::Deref as _, sync::Arc};

use dolos_core::{ChainError, EntityKey, Genesis, TxOrder, TxoRef};
use pallas::ledger::{
    primitives::{
        conway::{self, DRep, Voter},
        Epoch,
    },
    traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
};

use super::WorkDeltas;
use crate::{
    owned::OwnedMultiEraOutput,
    pallas_extras::{self, stake_cred_to_drep},
    roll::BlockVisitor,
    DRepActivity, DRepAnchorUpdate, DRepDormancyRelease, DRepExpiryUpdate, DRepRegistration,
    DRepUnRegistration, GovDormancyReset, PParamsSet,
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

/// Governance-bookkeeping context the roll visitors need from state:
/// the dormant-epoch counter (`GovState::num_dormant_epochs`) and — only
/// when the counter is non-zero, so the lookup stays free in the common
/// case — the key set of the dreps namespace for the dormancy-release
/// fan-out.
///
/// Built once per batch by `compute_delta`; the evolving counter is copied
/// back after each block so a release in block `n` is visible to block
/// `n + 1` of the same batch.
#[derive(Clone, Default)]
pub struct DormancyContext {
    pub dormant_epochs: u64,
    pub drep_keys: Arc<Vec<EntityKey>>,
}

/// Visitor for the GOVCERT-scoped state effects: DRep registration
/// lifecycle, activity + epoch-based expiry bookkeeping, dormancy release,
/// and the two committee certificates.
#[derive(Default, Clone)]
pub struct DRepStateVisitor {
    current_epoch: Epoch,
    protocol: u16,
    drep_activity: Option<u64>,
    dormancy: DormancyContext,
}

impl DRepStateVisitor {
    pub fn new(dormancy: DormancyContext) -> Self {
        Self {
            dormancy,
            ..Default::default()
        }
    }

    /// The dormant-epoch counter after the blocks visited so far.
    pub fn dormant_epochs(&self) -> u64 {
        self.dormancy.dormant_epochs
    }

    /// `currentEpoch + drepActivity − numDormantEpochs` — the refresh value
    /// for `UpdateDRepCert` and for DRep votes (research §3.3.2), and for
    /// post-bootstrap registrations.
    fn refresh_expiry(&self) -> Option<Epoch> {
        let activity = self.drep_activity?;

        Some((self.current_epoch + activity).saturating_sub(self.dormancy.dormant_epochs))
    }

    /// Expiry stamped by `RegDRepCert`. During the PV9 bootstrap phase the
    /// dormancy credit is (incorrectly, but consensus-relevantly) ignored —
    /// `computeDRepExpiryVersioned` in the Haskell ledger.
    fn registration_expiry(&self) -> Option<Epoch> {
        let activity = self.drep_activity?;

        if self.protocol == 9 {
            Some(self.current_epoch + activity)
        } else {
            self.refresh_expiry()
        }
    }
}

impl BlockVisitor for DRepStateVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &Genesis,
        pparams: &PParamsSet,
        epoch: Epoch,
        _: u64,
        protocol: u16,
    ) -> Result<(), ChainError> {
        self.current_epoch = epoch;
        self.protocol = protocol;
        self.drep_activity = pparams.drep_inactivity_period();

        Ok(())
    }

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

        // Dormancy release (research §3.3.1): the first proposal after a
        // dormant stretch folds the counter into every non-long-expired
        // DRep's stored expiry and resets it. In the Haskell CERTS rule this
        // runs in the terminal case *before* the certificates, so certs and
        // votes of the same tx already see the reset counter. Phase-2-invalid
        // transactions contribute nothing.
        if tx.is_valid() && self.dormancy.dormant_epochs > 0 && !tx.gov_proposals().is_empty() {
            for key in self.dormancy.drep_keys.iter() {
                deltas.add_for_entity(DRepDormancyRelease::new(
                    key.clone(),
                    self.dormancy.dormant_epochs,
                    self.current_epoch,
                ));
            }

            deltas.add_for_entity(GovDormancyReset::new());

            self.dormancy.dormant_epochs = 0;
        }

        let Some(voting_procedures) = &conway_tx.transaction_body.voting_procedures else {
            return Ok(());
        };

        for (voter, _) in voting_procedures.iter() {
            let drep = match voter {
                Voter::DRepKey(hash) => DRep::Key(*hash),
                Voter::DRepScript(hash) => DRep::Script(*hash),
                _ => continue,
            };

            deltas.add_for_entity(DRepActivity::new(drep.clone(), block.slot()));

            // Voting refresh (research §3.3.2): a registered DRep that votes
            // gets its expiry pushed out, exactly like an `UpdateDRepCert`
            // heartbeat. Valid txs only.
            if tx.is_valid() {
                if let Some(expiry) = self.refresh_expiry() {
                    deltas.add_for_entity(DRepExpiryUpdate::new(
                        drep,
                        expiry,
                        self.current_epoch,
                        true,
                    ));
                }
            }
        }

        Ok(())
    }

    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        order: &TxOrder,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        // Committee certificates target the governance singleton. Valid txs
        // only — CERT state effects never apply for phase-2-invalid txs.
        if tx.is_valid() {
            if let Some(auth) = pallas_extras::cert_as_committee_auth(cert) {
                deltas.add_for_entity(crate::CommitteeAuth::new(auth.cold, auth.hot, block.slot()));
            }

            if let Some(resign) = pallas_extras::cert_as_committee_resign(cert) {
                deltas.add_for_entity(crate::CommitteeResign::new(
                    resign.cold,
                    resign.anchor,
                    block.slot(),
                ));
            }
        }

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

                    deltas.add_for_entity(DRepAnchorUpdate::new(drep.clone(), anchor.clone()));

                    if tx.is_valid() {
                        if let Some(expiry) = self.registration_expiry() {
                            deltas.add_for_entity(DRepExpiryUpdate::new(
                                drep.clone(),
                                expiry,
                                self.current_epoch,
                                false,
                            ));
                        }
                    }
                }
                conway::Certificate::UnRegDRepCert(_, _) => {
                    deltas.add_for_entity(DRepUnRegistration::new(
                        drep.clone(),
                        block.slot(),
                        *order,
                    ));
                }
                conway::Certificate::UpdateDRepCert(_, anchor) => {
                    deltas.add_for_entity(DRepAnchorUpdate::new(drep.clone(), anchor.clone()));

                    if tx.is_valid() {
                        if let Some(expiry) = self.refresh_expiry() {
                            deltas.add_for_entity(DRepExpiryUpdate::new(
                                drep.clone(),
                                expiry,
                                self.current_epoch,
                                false,
                            ));
                        }
                    }
                }
                _ => (),
            }
        };

        deltas.add_for_entity(DRepActivity::new(drep.clone(), block.slot()));

        Ok(())
    }
}
