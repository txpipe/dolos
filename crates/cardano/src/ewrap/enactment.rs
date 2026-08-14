use dolos_core::{ChainError, EntityKey};
use pallas::ledger::primitives::conway::GovActionId;

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
/// plus, for every action belonging to a lineage tree, that tree's new root.
///
/// Kept apart from the visitor so the mapping is exercisable on its own —
/// none of it depends on boundary context.
pub(crate) fn enactment_deltas(id: &ProposalId, proposal: &ProposalState) -> Vec<CardanoDelta> {
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
                deltas.push(TreasuryWithdrawal::new(credential.clone(), *amount).into());
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

    if let Some(purpose) = proposal.action.purpose() {
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
        _: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &ProposalState,
        _: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        tracing::debug!(proposal=%id, "visiting enacted proposal");

        self.deltas.extend(enactment_deltas(id, proposal));

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

        for (action, purpose) in cases {
            let is_info = matches!(action, ProposalAction::Info);
            let id = ProposalId::from(b"proposal".to_vec());
            let deltas = enactment_deltas(&id, &proposal([7u8; 32].into(), 0, action.clone()));

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
    /// 541) — the two enactments the hack table stamps on mainnet — move
    /// `GovState` off its Conway-genesis values and set their lineage roots.
    ///
    /// The action ids are the real ones; the enacted content is a fixture,
    /// since the actions' payloads are not in the repository.
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
        state = apply_gov(
            state,
            enactment_deltas(
                &id,
                &proposal(
                    constitution_tx,
                    0,
                    ProposalAction::NewConstitution {
                        anchor: enacted_constitution.anchor.clone(),
                        guardrail_script: enacted_constitution.guardrail_script,
                    },
                ),
            ),
        );

        let id = ProposalId::from(committee_tx.to_vec());
        state = apply_gov(
            state,
            enactment_deltas(
                &id,
                &proposal(
                    committee_tx,
                    0,
                    ProposalAction::UpdateCommittee {
                        to_remove: committee.members.keys().cloned().collect(),
                        to_add: vec![(new_member.clone(), 620)],
                        threshold: two_thirds(),
                    },
                ),
            ),
        );

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
