use std::collections::HashSet;

use dolos_core::{ChainError, EntityKey};
use pallas::ledger::primitives::{conway::GovActionId, StakeCredential};

use crate::{
    ewrap::{BoundaryWork, ProposalId},
    AccountState, CardanoDelta, CardanoEntity, CommitteeUpdate, Constitution, ConstitutionUpdate,
    GovRootsUpdate, PParamValue, PParamsSet, PParamsUpdate, ProposalAction, ProposalState,
    TreasuryWithdrawal,
};

#[derive(Default)]
pub struct BoundaryVisitor {
    deltas: Vec<CardanoDelta>,
    logs: Vec<(EntityKey, CardanoEntity)>,
}

/// The state effects an enacted proposal carries: the action's own effect
/// plus, for every Conway action belonging to a lineage tree, that tree's
/// new root.
///
/// Kept apart from the visitor so the mapping is exercisable on its own —
/// none of it depends on boundary context. `deliverable_withdrawal_targets`
/// is the boundary's ruling on which withdrawal credentials can receive
/// their credit (registered at the boundary): the ledger's
/// `applyEnactedWithdrawals` restricts the enacted withdrawal map to the
/// rewards UMap and discards the rest, so no delta is emitted for them.
pub(crate) fn enactment_deltas(
    id: &ProposalId,
    proposal: &ProposalState,
    deliverable_withdrawal_targets: &HashSet<StakeCredential>,
) -> Vec<CardanoDelta> {
    let mut deltas: Vec<CardanoDelta> = Vec::new();

    match &proposal.action {
        ProposalAction::HardFork(version) => {
            let value = PParamValue::ProtocolVersion(*version);
            let pparams = PParamsSet::default().with(value);
            deltas.push(PParamsUpdate::new(pparams).into());
        }
        ProposalAction::ParamChange(pparams) => {
            deltas.push(PParamsUpdate::new(pparams.clone()).into());
        }
        ProposalAction::TreasuryWithdrawal(withdrawals) => {
            for (credential, amount) in withdrawals {
                if deliverable_withdrawal_targets.contains(credential) {
                    deltas.push(TreasuryWithdrawal::new(credential.clone(), *amount).into());
                }
            }
        }
        ProposalAction::NoConfidence => {
            deltas.push(CommitteeUpdate::no_confidence().into());
        }
        ProposalAction::UpdateCommittee {
            to_remove,
            to_add,
            threshold,
        } => {
            deltas.push(
                CommitteeUpdate::update(to_remove.clone(), to_add.clone(), threshold.clone())
                    .into(),
            );
        }
        ProposalAction::NewConstitution {
            anchor,
            guardrail_script,
        } => {
            deltas.push(
                ConstitutionUpdate::new(Constitution {
                    anchor: anchor.clone(),
                    guardrail_script: *guardrail_script,
                })
                .into(),
            );
        }
        // Info actions ratify but carry no state effect.
        ProposalAction::Info => (),
        // Only reachable from rows written before the specific variants
        // existed, where the action's content was never recorded.
        ProposalAction::Other => {
            tracing::error!(proposal=%id, "can't enact legacy proposal with untracked action");
        }
    }

    // `ensPrevGovActionIds` is Conway enact-state: it names Conway
    // governance actions and nothing else. The tree membership to read is
    // the recorded `purpose`, written by `parse_gov_action` when the action
    // was decoded as a Conway action — not `action.purpose()`, which infers
    // a tree from the action's *shape* and so fires for any row carrying
    // parameters, a pre-Conway update proposal included.
    //
    // Such a proposal still enacts, which is how its parameter change lands,
    // but claiming a root leaves a phantom parent that no later action can
    // match: a first-of-purpose action declares no parent, so it fails
    // `prevActionAsExpected` forever. Observed on a preview replay, where
    // legacy `Params` proposals enacted by the pre-Conway update mechanism
    // held the pparam-update root and the chain's first ParameterChange
    // (epoch 735) could never ratify.
    if let Some(purpose) = proposal.purpose {
        let action = GovActionId {
            transaction_id: proposal.tx,
            action_index: proposal.idx,
        };

        deltas.push(GovRootsUpdate::new(purpose, action).into());
    }

    deltas
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_enacting_proposal(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &ProposalState,
        _: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        tracing::debug!(proposal=%id, "visiting enacted proposal");

        if let ProposalAction::TreasuryWithdrawal(withdrawals) = &proposal.action {
            for (credential, amount) in withdrawals {
                if ctx.deliverable_withdrawal_targets.contains(credential) {
                    ctx.effective_treasury_withdrawals += amount;
                } else {
                    ctx.invalid_treasury_withdrawals += amount;

                    tracing::warn!(
                        proposal=%id,
                        credential=?credential,
                        %amount,
                        "treasury withdrawal not applied (unregistered account) - stays in treasury"
                    );
                }
            }
        }

        self.deltas.extend(enactment_deltas(
            id,
            proposal,
            &ctx.deliverable_withdrawal_targets,
        ));

        Ok(())
    }

    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        for (key, log) in self.logs.drain(..) {
            ctx.logs.push((key, log));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dolos_core::EntityDelta as _;
    use pallas::{
        crypto::hash::Hash,
        ledger::primitives::{conway::Anchor, RationalNumber, StakeCredential},
    };

    use super::*;
    use crate::{
        gov_from_conway_genesis, Committee, Constitution, FixedNamespace as _, GovPurpose, GovState,
    };

    fn proposal(tx: Hash<32>, idx: u32, action: ProposalAction) -> ProposalState {
        ProposalState {
            slot: 0,
            tx,
            idx,
            action,
            max_epoch: None,
            ratified_epoch: None,
            canceled_epoch: None,
            deposit: None,
            reward_account: None,
            proposed_in: None,
            parent: None,
            purpose: None,
            anchor: None,
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        }
    }

    fn anchor(byte: u8) -> Anchor {
        Anchor {
            url: format!("ipfs://constitution-{byte}"),
            content_hash: [byte; 32].into(),
        }
    }

    fn two_thirds() -> RationalNumber {
        RationalNumber {
            numerator: 2,
            denominator: 3,
        }
    }

    /// Apply the governance-singleton share of a delta set to `state`. Deltas
    /// keyed at another namespace (pparams, treasury) are skipped, as the
    /// commit path skips them when it streams the `"gov"` singleton.
    fn apply_gov(state: GovState, deltas: Vec<CardanoDelta>) -> GovState {
        let mut entity: Option<CardanoEntity> = Some(state.into());

        for mut delta in deltas {
            if delta.key().0 == GovState::NS {
                delta.apply(&mut entity);
            }
        }

        Option::<GovState>::from(entity.unwrap()).unwrap()
    }

    /// A pre-Conway update proposal enacts its parameter change but claims
    /// no lineage root. Its row carries no recorded `purpose` — it was never
    /// decoded as a Conway action — while its `action` still looks like a
    /// parameter change, so gating on the action's shape would hand it the
    /// pparam-update root. The phantom parent that leaves can never be
    /// matched by a first-of-purpose action, which declares none: observed
    /// on a preview replay, where legacy `Params` proposals enacted by the
    /// pre-Conway update mechanism held the root and the chain's first
    /// ParameterChange (epoch 735) could never ratify.
    #[test]
    fn enactment_without_recorded_purpose_claims_no_lineage_root() {
        let action = ProposalAction::ParamChange(PParamsSet::default());
        let id = ProposalId::from(b"legacy".to_vec());

        let legacy = proposal([9u8; 32].into(), 0, action.clone());
        assert_eq!(legacy.purpose, None);
        assert_eq!(
            legacy.action.purpose(),
            Some(GovPurpose::PParamUpdate),
            "the action's shape alone would claim a tree"
        );

        let deltas = enactment_deltas(&id, &legacy, &HashSet::new());

        assert!(
            deltas
                .iter()
                .any(|delta| matches!(delta, CardanoDelta::PParamsUpdate(_))),
            "the parameter change itself must still land"
        );
        assert!(
            !deltas
                .iter()
                .any(|delta| matches!(delta, CardanoDelta::GovRootsUpdate(_))),
            "a row with no recorded purpose must not claim a lineage root"
        );

        // The same action decoded as a Conway proposal does claim it.
        let mut conway = proposal([9u8; 32].into(), 0, action);
        conway.purpose = Some(GovPurpose::PParamUpdate);

        let deltas = enactment_deltas(&id, &conway, &HashSet::new());

        assert!(
            deltas
                .iter()
                .any(|delta| matches!(delta, CardanoDelta::GovRootsUpdate(_))),
            "a Conway action must claim its lineage root"
        );
    }

    /// A withdrawal credential outside the deliverable set gets no
    /// per-account delta — the ledger's `applyEnactedWithdrawals` discards
    /// withdrawals to credentials absent from the rewards UMap, so neither
    /// the account nor the pots move for them.
    #[test]
    fn undeliverable_withdrawal_emits_no_account_delta() {
        let registered = StakeCredential::AddrKeyhash([1u8; 28].into());
        let unregistered = StakeCredential::AddrKeyhash([2u8; 28].into());

        let action = ProposalAction::TreasuryWithdrawal(vec![
            (registered.clone(), 3_000_000),
            (unregistered.clone(), 5_000_000),
        ]);

        let id = ProposalId::from(b"withdrawal".to_vec());
        let row = proposal([3u8; 32].into(), 0, action);

        let deliverable = HashSet::from([registered.clone()]);
        let deltas = enactment_deltas(&id, &row, &deliverable);

        let credited: Vec<_> = deltas
            .iter()
            .filter_map(|delta| match delta {
                CardanoDelta::TreasuryWithdrawal(w) => Some((w.account.clone(), w.amount)),
                _ => None,
            })
            .collect();

        assert_eq!(credited, vec![(registered, 3_000_000)]);
    }

    /// Every governance action a Conway chain can carry has an enactment
    /// effect — no variant falls through to an error arm. Only the legacy
    /// `Other` catch-all, which never records the action's content, does.
    #[test]
    fn every_real_action_enacts() {
        let cred = StakeCredential::AddrKeyhash([1u8; 28].into());

        let cases = [
            (
                ProposalAction::ParamChange(PParamsSet::default()),
                Some(GovPurpose::PParamUpdate),
            ),
            (
                ProposalAction::HardFork((10, 0)),
                Some(GovPurpose::HardFork),
            ),
            (
                ProposalAction::TreasuryWithdrawal(vec![(cred.clone(), 1_000_000)]),
                None,
            ),
            (ProposalAction::NoConfidence, Some(GovPurpose::Committee)),
            (
                ProposalAction::UpdateCommittee {
                    to_remove: vec![],
                    to_add: vec![(cred.clone(), 700)],
                    threshold: two_thirds(),
                },
                Some(GovPurpose::Committee),
            ),
            (
                ProposalAction::NewConstitution {
                    anchor: anchor(9),
                    guardrail_script: None,
                },
                Some(GovPurpose::Constitution),
            ),
            (ProposalAction::Info, None),
        ];

        let deliverable = HashSet::from([cred.clone()]);

        for (action, purpose) in cases {
            let is_info = matches!(action, ProposalAction::Info);
            let id = ProposalId::from(b"proposal".to_vec());
            let mut row = proposal([7u8; 32].into(), 0, action.clone());
            row.purpose = purpose;
            let deltas = enactment_deltas(&id, &row, &deliverable);

            let roots = deltas
                .iter()
                .filter(|delta| matches!(delta, CardanoDelta::GovRootsUpdate(_)))
                .count();

            assert_eq!(
                roots,
                usize::from(purpose.is_some()),
                "unexpected root updates for {action:?}"
            );

            let effects = deltas.len() - roots;

            if is_info {
                assert_eq!(effects, 0, "Info should carry no state effect");
            } else {
                assert!(effects > 0, "no state effect emitted for {action:?}");
            }
        }
    }

    /// The mainnet committee replacement (`47a0e7a4…#0`, ratified at epoch
    /// 580) and constitution replacement (`8c653ee5…#0`, ratified at epoch
    /// 541) move `GovState` off its Conway-genesis values and set their
    /// lineage roots.
    ///
    /// These two ids and epochs are what survives of the curated outcome
    /// table this suite replaced: the named mainnet enactments, kept as a
    /// fixture the engine's own output can be read against. The action ids
    /// and epochs are the real ones; the enacted content is invented, since
    /// the actions' payloads are not in the repository.
    #[test]
    fn mainnet_gov_enactments_replace_genesis_state() {
        let genesis = crate::load_test_genesis("mainnet");
        let (constitution, committee) = gov_from_conway_genesis(&genesis.conway).unwrap();

        let mut state = GovState::default();
        state.seed_genesis(constitution.clone(), committee.clone(), 507);

        let interim_member = committee
            .members
            .keys()
            .next()
            .expect("mainnet interim committee is not empty")
            .clone();

        let new_member = StakeCredential::ScriptHash([42u8; 28].into());

        let constitution_tx: Hash<32> =
            "8c653ee5c9800e6d31e79b5a7f7d4400c81d44717ad4db633dc18d4c07e4a4fd"
                .parse()
                .unwrap();

        let committee_tx: Hash<32> =
            "47a0e7a4f9383b1afc2192b23b41824d65ac978d7741aca61fc1fa16833d1111"
                .parse()
                .unwrap();

        let enacted_constitution = Constitution {
            anchor: anchor(1),
            guardrail_script: Some([2u8; 28].into()),
        };

        let id = ProposalId::from(constitution_tx.to_vec());
        let mut row = proposal(
            constitution_tx,
            0,
            ProposalAction::NewConstitution {
                anchor: enacted_constitution.anchor.clone(),
                guardrail_script: enacted_constitution.guardrail_script,
            },
        );
        row.purpose = Some(GovPurpose::Constitution);
        state = apply_gov(state, enactment_deltas(&id, &row, &HashSet::new()));

        let id = ProposalId::from(committee_tx.to_vec());
        let mut row = proposal(
            committee_tx,
            0,
            ProposalAction::UpdateCommittee {
                to_remove: committee.members.keys().cloned().collect(),
                to_add: vec![(new_member.clone(), 620)],
                threshold: two_thirds(),
            },
        );
        row.purpose = Some(GovPurpose::Committee);
        state = apply_gov(state, enactment_deltas(&id, &row, &HashSet::new()));

        assert_eq!(state.constitution, Some(enacted_constitution));
        assert_ne!(state.constitution, Some(constitution));

        let enacted_committee = state.committee.clone().unwrap();
        assert_eq!(
            enacted_committee,
            Committee {
                members: [(new_member, 620)].into_iter().collect(),
                threshold: two_thirds(),
            }
        );
        assert!(!enacted_committee.members.contains_key(&interim_member));

        assert_eq!(
            state.prev_gov_action_ids.constitution,
            Some(GovActionId {
                transaction_id: constitution_tx,
                action_index: 0,
            })
        );
        assert_eq!(
            state.prev_gov_action_ids.committee,
            Some(GovActionId {
                transaction_id: committee_tx,
                action_index: 0,
            })
        );

        assert_eq!(state.prev_gov_action_ids.hard_fork, None);
        assert_eq!(state.prev_gov_action_ids.pparam_update, None);
    }
}
