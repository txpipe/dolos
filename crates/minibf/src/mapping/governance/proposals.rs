//! The Blockfrost models of a governance proposal: the proposal itself, its
//! lifecycle epochs and the parameter change it asks for.

use axum::http::StatusCode;
use blockfrost_openapi::models::{
    proposal::{self, Proposal},
    proposal_parameters::ProposalParameters,
    proposal_parameters_parameters::ProposalParametersParameters,
    proposals_inner::GovernanceType,
};
use dolos_cardano::{
    model::{ProposalAction, ProposalState},
    PParamsSet,
};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::Network,
        primitives::{conway::GovAction, Epoch},
    },
};

use crate::mapping::{
    bech32_gov_action, cost_models::map_cost_models_raw, governance::description::description_json,
    i32_or_500, protocol_params::protocol_params_model, rational_to_f64_unrounded,
    stake_cred_to_address, IntoModel, Unrounded,
};

pub fn governance_type(action: &ProposalAction) -> GovernanceType {
    match action {
        ProposalAction::ParamChange(_) => GovernanceType::ParameterChange,
        ProposalAction::HardFork(_) => GovernanceType::HardForkInitiation,
        ProposalAction::TreasuryWithdrawal(_) => GovernanceType::TreasuryWithdrawals,
        ProposalAction::NoConfidence => GovernanceType::NoConfidence,
        ProposalAction::UpdateCommittee { .. } => GovernanceType::NewCommittee,
        ProposalAction::NewConstitution { .. } => GovernanceType::NewConstitution,
        ProposalAction::Info => GovernanceType::InfoAction,
    }
}

/// The parameter change one proposal asks for, as the nullable delta
/// Blockfrost returns.
///
/// Every field is a change the proposal names, so an untouched parameter is
/// `null` rather than the value in force — the opposite of
/// `/epochs/{n}/parameters`, which reports the parameters actually effective
/// and shares its field mapping with this through `protocol_params_model!`.
/// Ratios come back [`Unrounded`]: Blockfrost serves this endpoint from
/// db-sync's `param_proposal` columns at full `double precision`, so a
/// proposal setting tau to 1/6 reads 0.16666666666666666, not the 0.167 the
/// epoch endpoint would say.
pub struct ProposalParametersBuilder {
    pub tx: Hash<32>,
    pub idx: u32,
    pub params: PParamsSet,
}

impl IntoModel<ProposalParameters> for ProposalParametersBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<ProposalParameters, StatusCode> {
        let Self { tx, idx, params } = self;

        let parameters = protocol_params_model!(
            params,
            Unrounded,
            ProposalParametersParameters {
                // A Conway proposal names no epoch: db-sync fills the column
                // only for the pre-Conway update proposals it keeps in the
                // same table.
                epoch: Some(None),
                // The model types fees, sizes and counts as `i32` while a
                // proposal can set them anywhere in the chain's range, so a
                // value past `i32::MAX` is a 500, not a negative parameter.
                min_fee_a: params.min_fee_a().map(i32_or_500).transpose()?,
                min_fee_b: params.min_fee_b().map(i32_or_500).transpose()?,
                max_block_size: params.max_block_body_size().map(i32_or_500).transpose()?,
                max_tx_size: params.max_transaction_size().map(i32_or_500).transpose()?,
                max_block_header_size: params
                    .max_block_header_size()
                    .map(i32_or_500)
                    .transpose()?,
                key_deposit: params.key_deposit().map(|x| x.to_string()),
                pool_deposit: params.pool_deposit().map(|x| x.to_string()),
                e_max: params.maximum_epoch().map(i32_or_500).transpose()?,
                n_opt: params
                    .desired_number_of_stake_pools()
                    .map(i32_or_500)
                    .transpose()?,
                a0: params.a0().map(|x| rational_to_f64_unrounded(&x)),
                rho: params.rho().map(|x| rational_to_f64_unrounded(&x)),
                tau: params.tau().map(|x| rational_to_f64_unrounded(&x)),
                // A pre-Conway knob that a Conway proposal cannot name.
                decentralisation_param: None,
                // A version bump is a hard fork, never a parameter change.
                protocol_major_ver: None,
                protocol_minor_ver: None,
                min_utxo: params.ada_per_utxo_byte().map(|x| x.to_string()),
                min_pool_cost: params.min_pool_cost().map(|x| x.to_string()),
                // Blockfrost tells "set to the empty map" (`{}`) apart from
                // "not named at all" (`null`) because db-sync keys a row per
                // cost model. `PParamsSet` records a language at a time, so a
                // change naming no language leaves nothing behind to tell the
                // two apart and both read as `null` here.
                cost_models: map_cost_models_raw(&params.cost_models_for_script_languages())
                    .flatten(),
            }
        );

        Ok(ProposalParameters {
            id: bech32_gov_action(&tx, idx)?,
            tx_hash: hex::encode(tx),
            cert_index: idx.try_into().map_err(|_| StatusCode::BAD_REQUEST)?,
            parameters: Box::new(parameters),
        })
    }
}

fn governance_type_from_action(action: &GovAction) -> proposal::GovernanceType {
    match action {
        GovAction::ParameterChange(..) => proposal::GovernanceType::ParameterChange,
        GovAction::HardForkInitiation(..) => proposal::GovernanceType::HardForkInitiation,
        GovAction::TreasuryWithdrawals(..) => proposal::GovernanceType::TreasuryWithdrawals,
        GovAction::NoConfidence(..) => proposal::GovernanceType::NoConfidence,
        GovAction::UpdateCommittee(..) => proposal::GovernanceType::NewCommittee,
        GovAction::NewConstitution(..) => proposal::GovernanceType::NewConstitution,
        GovAction::Information => proposal::GovernanceType::InfoAction,
    }
}

pub struct ProposalModelBuilder {
    pub state: ProposalState,
    pub gov_action: Option<GovAction>,
    pub network: Network,
    pub current_epoch: Epoch,
}

impl ProposalModelBuilder {
    fn governance_type(&self) -> proposal::GovernanceType {
        if let Some(action) = &self.gov_action {
            return governance_type_from_action(action);
        }

        match &self.state.action {
            ProposalAction::ParamChange(_) => proposal::GovernanceType::ParameterChange,
            ProposalAction::HardFork(_) => proposal::GovernanceType::HardForkInitiation,
            ProposalAction::TreasuryWithdrawal(_) => proposal::GovernanceType::TreasuryWithdrawals,
            ProposalAction::NoConfidence => proposal::GovernanceType::NoConfidence,
            ProposalAction::UpdateCommittee { .. } => proposal::GovernanceType::NewCommittee,
            ProposalAction::NewConstitution { .. } => proposal::GovernanceType::NewConstitution,
            ProposalAction::Info => proposal::GovernanceType::InfoAction,
        }
    }

    fn enactment_epoch(&self) -> Option<Epoch> {
        enactment_epoch(&self.state, self.current_epoch)
    }

    fn expired_epoch(&self) -> Option<Epoch> {
        expired_epoch(&self.state, self.current_epoch)
    }

    fn dropped_epoch(&self) -> Option<Epoch> {
        dropped_epoch(&self.state, self.current_epoch)
    }
}

/// Dolos stamps `ratified_epoch` with the epoch the ratifying boundary
/// closes. db-sync reports that same epoch as `ratified_epoch` and the
/// enactment lands one boundary later, so `enacted_epoch` is one more.
fn enactment_epoch(state: &ProposalState, current_epoch: Epoch) -> Option<Epoch> {
    let boundary = state.ratified_epoch? + 1;

    (current_epoch >= boundary).then_some(boundary)
}

/// An unratified proposal counts as expired from its `expires_at` epoch
/// on, before any boundary stamp. The expiry drop later stamps
/// `canceled_epoch`, one epoch past `expires_at`. A `canceled_epoch` at
/// or before `expires_at` is a sibling pruned by a competing enactment
/// instead: db-sync reports that as dropped, never as expired.
fn expired_epoch(state: &ProposalState, current_epoch: Epoch) -> Option<Epoch> {
    if state.ratified_epoch.is_some() {
        return None;
    }

    let expires = state.expires_at()?;

    if state.canceled_epoch.is_some_and(|x| x <= expires) {
        return None;
    }

    (current_epoch >= expires).then_some(expires)
}

/// db-sync marks a proposal as dropped when a competing action gets
/// enacted (canceled in dolos terms) or one epoch after it marks the
/// proposal as expired.
fn dropped_epoch(state: &ProposalState, current_epoch: Epoch) -> Option<Epoch> {
    if let Some(canceled) = state.canceled_epoch {
        return (current_epoch >= canceled).then_some(canceled);
    }

    let dropped = expired_epoch(state, current_epoch)? + 1;

    (current_epoch >= dropped).then_some(dropped)
}

/// The epoch the proposal left the active set and its tally froze — the
/// earliest boundary outcome Blockfrost reports. `None` while the proposal
/// is still live.
pub fn proposal_closed_epoch(state: &ProposalState, current_epoch: Epoch) -> Option<Epoch> {
    [
        state.ratified_epoch,
        enactment_epoch(state, current_epoch),
        dropped_epoch(state, current_epoch),
        expired_epoch(state, current_epoch),
    ]
    .into_iter()
    .flatten()
    .min()
}

impl IntoModel<Proposal> for ProposalModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<Proposal, StatusCode> {
        let return_address = self
            .state
            .reward_account
            .as_ref()
            .map(|cred| stake_cred_to_address(cred, self.network).to_bech32())
            .transpose()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .unwrap_or_default();

        let enactment = self.enactment_epoch();

        let governance_description = self.gov_action.as_ref().map(description_json).transpose()?;

        let out = Proposal {
            id: bech32_gov_action(&self.state.tx, self.state.idx)?,
            tx_hash: hex::encode(self.state.tx),
            cert_index: self.state.idx as i32,
            governance_type: self.governance_type(),
            governance_description,
            deposit: self.state.deposit.unwrap_or_default().to_string(),
            return_address,
            ratified_epoch: self.state.ratified_epoch.map(|x| x as i32),
            enacted_epoch: enactment.map(|x| x as i32),
            dropped_epoch: self.dropped_epoch().map(|x| x as i32),
            expired_epoch: self.expired_epoch().map(|x| x as i32),
            expiration: self.state.expires_at().unwrap_or_default() as i32,
        };

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas::ledger::primitives::StakeCredential;

    #[test]
    fn governance_type_names_every_action() {
        let cases = [
            (
                ProposalAction::ParamChange(PParamsSet::default()),
                GovernanceType::ParameterChange,
            ),
            (
                ProposalAction::HardFork((10, 0)),
                GovernanceType::HardForkInitiation,
            ),
            (
                ProposalAction::TreasuryWithdrawal(vec![]),
                GovernanceType::TreasuryWithdrawals,
            ),
            (ProposalAction::NoConfidence, GovernanceType::NoConfidence),
            (
                ProposalAction::UpdateCommittee {
                    to_remove: vec![],
                    to_add: vec![],
                    threshold: pallas::ledger::primitives::RationalNumber {
                        numerator: 1,
                        denominator: 2,
                    },
                },
                GovernanceType::NewCommittee,
            ),
            (
                ProposalAction::NewConstitution {
                    anchor: pallas::ledger::primitives::conway::Anchor {
                        url: "https://dolos.test".to_string(),
                        content_hash: Hash::from([1u8; 32]),
                    },
                    guardrail_script: None,
                },
                GovernanceType::NewConstitution,
            ),
            (ProposalAction::Info, GovernanceType::InfoAction),
        ];

        for (action, expected) in cases {
            assert_eq!(governance_type(&action), expected, "{action:?}");
        }
    }

    /// A proposal that expires after epoch 646 (`expires_at` = 647), with
    /// the boundary outcome stamps given by the caller.
    fn lifecycle_state(ratified: Option<Epoch>, canceled: Option<Epoch>) -> ProposalState {
        ProposalState {
            slot: 1,
            tx: [7u8; 32].into(),
            idx: 0,
            action: ProposalAction::Info,
            max_epoch: Some(646),
            ratified_epoch: ratified,
            canceled_epoch: canceled,
            deposit: Some(100_000_000),
            reward_account: Some(StakeCredential::AddrKeyhash([7u8; 28].into())),
            proposed_in: 640,
            parent: None,
            purpose: None,
            anchor: None,
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        }
    }

    fn lifecycle_model(state: ProposalState, current_epoch: Epoch) -> Proposal {
        ProposalModelBuilder {
            state,
            gov_action: None,
            network: Network::Testnet,
            current_epoch,
        }
        .into_model()
        .expect("failed to build proposal model")
    }

    /// The expected values mirror the Blockfrost mainnet responses for
    /// b2a591ac… (enacted), dfd81f8d… (expired), and 729daaf2… (pending
    /// through its expiration epoch).
    #[test]
    fn proposal_lifecycle_epochs_match_db_sync() {
        // ratified at the boundary closing 525: enacted one epoch later
        let enacted = lifecycle_model(lifecycle_state(Some(525), None), 653);
        assert_eq!(enacted.ratified_epoch, Some(525));
        assert_eq!(enacted.enacted_epoch, Some(526));
        assert_eq!(enacted.expired_epoch, None);
        assert_eq!(enacted.dropped_epoch, None);

        // expired at 647: the drop stamps canceled one epoch past expiry
        let expired = lifecycle_model(lifecycle_state(None, Some(648)), 653);
        assert_eq!(expired.ratified_epoch, None);
        assert_eq!(expired.enacted_epoch, None);
        assert_eq!(expired.expired_epoch, Some(647));
        assert_eq!(expired.dropped_epoch, Some(648));

        // canceled before expiry: a pruned sibling drops but never expires
        let pruned = lifecycle_model(lifecycle_state(None, Some(645)), 653);
        assert_eq!(pruned.expired_epoch, None);
        assert_eq!(pruned.dropped_epoch, Some(645));

        // in its expiration epoch without a boundary stamp: already expired,
        // the drop follows one epoch later (mirrors mainnet ab474223…)
        let expiring = lifecycle_model(lifecycle_state(None, None), 647);
        assert_eq!(expiring.expired_epoch, Some(647));
        assert_eq!(expiring.dropped_epoch, None);

        // still votable before its expiration epoch: no outcome yet
        let pending = lifecycle_model(lifecycle_state(None, None), 646);
        assert_eq!(pending.expired_epoch, None);
        assert_eq!(pending.dropped_epoch, None);
    }
}
