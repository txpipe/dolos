use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    proposal_parameters::ProposalParameters,
    proposal_parameters_parameters::ProposalParametersParameters,
    proposals_inner::{GovernanceType, ProposalsInner},
};
use dolos_cardano::{
    model::{DRepState, FixedNamespace as _, ProposalAction, ProposalState},
    pallas_extras, ChainSummary, PParamsSet,
};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain, StateStore as _};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::{Epoch, RationalNumber},
        traverse::MultiEraBlock,
    },
};
use std::collections::HashMap;

use crate::{
    error::Error,
    mapping::{bech32, bech32_gov_action, parse_gov_action_id, IntoModel},
    pagination::{Order, Pagination, PaginationParameters},
    routes::epochs::mapping::map_cost_models_raw,
    Facade,
};

fn parse_drep_id(drep_id: &str) -> Result<(String, Vec<u8>, bool, bool), StatusCode> {
    match drep_id {
        "drep_always_abstain" => Ok((drep_id.to_string(), vec![0], false, true)),
        "drep_always_no_confidence" => Ok((drep_id.to_string(), vec![1], false, true)),
        drep_id => {
            let (hrp, payload) = bech32::decode(drep_id).map_err(|_| StatusCode::BAD_REQUEST)?;

            match (hrp.as_str(), payload.len()) {
                ("drep", 29) => {
                    let header_byte = payload.first().ok_or(StatusCode::BAD_REQUEST)?;

                    // first 4 bits need to be equal to 0010
                    if header_byte & 0b11110000 != 0b00100000 {
                        return Err(StatusCode::BAD_REQUEST);
                    }

                    Ok((drep_id.to_string(), payload, false, false))
                }
                ("drep", 28) => Ok((
                    drep_id.to_string(),
                    [vec![pallas_extras::DREP_KEY_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                ("drep_vkh", 28) => Ok((
                    bech32(bech32::Hrp::parse("drep").unwrap(), &payload)
                        .map_err(|_| StatusCode::BAD_REQUEST)?,
                    [vec![pallas_extras::DREP_KEY_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                ("drep_script", 28) => Ok((
                    bech32(bech32::Hrp::parse("drep").unwrap(), &payload)
                        .map_err(|_| StatusCode::BAD_REQUEST)?,
                    [vec![pallas_extras::DREP_SCRIPT_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                _ => Err(StatusCode::BAD_REQUEST),
            }
        }
    }
}

pub struct DrepModelBuilder<'a> {
    drep_id: String,
    drep_id_encoded: Vec<u8>,
    is_legacy: bool,
    state: Option<DRepState>,
    pparams: PParamsSet,
    chain: &'a ChainSummary,
    tip: BlockSlot,
}

impl<'a> DrepModelBuilder<'a> {
    fn is_special_case(&self) -> bool {
        ["drep_always_abstain", "drep_always_no_confidence"].contains(&self.drep_id.as_str())
    }

    fn first_active_epoch(&self) -> Option<Epoch> {
        if self.is_special_case() {
            return None;
        }

        if self
            .state
            .as_ref()
            .map(|x| x.is_unregistered())
            .unwrap_or(true)
        {
            return None;
        }

        self.state
            .as_ref()?
            .registered_at
            .map(|x| self.chain.slot_epoch(x.0).0)
    }

    fn last_active_epoch(&self) -> Option<Epoch> {
        if self.is_special_case() {
            return None;
        }

        self.state
            .as_ref()?
            .last_active_slot
            .map(|x| self.chain.slot_epoch(x).0)
    }

    fn is_drep_expired(&self) -> bool {
        if self.is_special_case() {
            return false;
        }

        if self.is_drep_retired() {
            return false;
        }

        let last_active_epoch = self.last_active_epoch();

        let inactivity_period = self.pparams.drep_inactivity_period().unwrap_or_default();

        let expiring_epoch = last_active_epoch.map(|x| x + inactivity_period);

        let (current_epoch, _) = self.chain.slot_epoch(self.tip);

        expiring_epoch
            .map(|expiration| expiration <= current_epoch)
            .unwrap_or(false)
    }

    fn is_drep_retired(&self) -> bool {
        if self.is_special_case() {
            return false;
        }

        let Some(state) = self.state.as_ref() else {
            return false;
        };

        match (state.registered_at, state.unregistered_at) {
            (Some(registered), Some(unregistered)) => unregistered > registered,
            (Some(_), None) => false,
            _ => false,
        }
    }

    fn is_drep_active(&self) -> bool {
        !self.is_drep_retired()
    }
}

impl<'a> IntoModel<blockfrost_openapi::models::drep::Drep> for DrepModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<blockfrost_openapi::models::drep::Drep, StatusCode> {
        let expired = self.is_drep_expired();

        let out = blockfrost_openapi::models::drep::Drep {
            drep_id: self.drep_id.clone(),
            hex: if self.is_special_case() {
                "".to_string()
            } else if self.is_legacy {
                hex::encode(&self.drep_id_encoded[1..])
            } else {
                hex::encode(&self.drep_id_encoded)
            },
            amount: self
                .state
                .as_ref()
                .map(|x| x.voting_power.to_string())
                .unwrap_or_default(),
            active: self.is_drep_active(),
            active_epoch: self.first_active_epoch().map(|x| x as i32),
            has_script: pallas_extras::drep_id_is_script(&self.drep_id_encoded),
            retired: self.is_drep_retired(),
            expired,
            last_active_epoch: self.last_active_epoch().map(|x| x as i32),
        };

        Ok(out)
    }
}

pub async fn drep_by_id<D: Domain>(
    Path(drep): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<blockfrost_openapi::models::drep::Drep>, StatusCode>
where
    Option<DRepState>: From<D::Entity>,
{
    let (drep, drep_bytes, is_legacy, is_special_case) =
        parse_drep_id(&drep).map_err(|_| StatusCode::BAD_REQUEST)?;

    let drep_state = if is_special_case {
        None
    } else {
        Some(
            domain
                .read_cardano_entity::<DRepState>(drep_bytes.clone())?
                .ok_or(StatusCode::NOT_FOUND)?,
        )
    };

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (tip, _) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let pparams = domain.get_current_effective_pparams()?;

    let model = DrepModelBuilder {
        drep_id: drep,
        drep_id_encoded: drep_bytes,
        is_legacy,
        state: drep_state,
        pparams,
        chain: &chain,
        tip,
    };

    model.into_response()
}

struct ProposalRow {
    slot: BlockSlot,
    tx: Hash<32>,
    idx: u32,
    governance_type: GovernanceType,
}

fn governance_type(action: &ProposalAction) -> Option<GovernanceType> {
    match action {
        ProposalAction::ParamChange(_) => Some(GovernanceType::ParameterChange),
        ProposalAction::HardFork(_) => Some(GovernanceType::HardForkInitiation),
        ProposalAction::TreasuryWithdrawal(_) => Some(GovernanceType::TreasuryWithdrawals),
        ProposalAction::NoConfidence => Some(GovernanceType::NoConfidence),
        ProposalAction::UpdateCommittee { .. } => Some(GovernanceType::NewCommittee),
        ProposalAction::NewConstitution { .. } => Some(GovernanceType::NewConstitution),
        ProposalAction::Info => Some(GovernanceType::InfoAction),
        ProposalAction::Other => None,
    }
}

/// Whether the row is a Conway governance action rather than a pre-Conway
/// protocol update.
///
/// Dolos records both in the same namespace, but only the Conway ones are
/// governance actions: db-sync keeps the old update proposals in a table of
/// their own and Blockfrost never lists them. A proposal procedure always
/// carries a deposit and the reward account it goes back to, and an update
/// proposal, having no procedure behind it, carries neither.
fn is_gov_action(state: &ProposalState) -> bool {
    state.deposit.is_some() && state.reward_account.is_some()
}

/// Proposals of one block in the order the block itself puts them: by the
/// position of the proposing tx, then by the action index inside that tx.
///
/// The archive is the only place that order lives, since a state row knows the
/// slot its proposal landed on but not where in the block its tx sat. A block
/// the archive no longer holds leaves the group on its provisional tx hash
/// order, which is arbitrary but stable across requests.
fn order_within_block<D: Domain>(
    domain: &D,
    slot: BlockSlot,
    rows: &mut [ProposalRow],
) -> Result<(), Error> {
    if rows.len() < 2 {
        return Ok(());
    }

    let Some(body) = domain
        .archive()
        .get_block_by_slot(&slot)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Ok(());
    };

    let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let positions: HashMap<Hash<32>, usize> = block
        .txs()
        .iter()
        .enumerate()
        .map(|(position, tx)| (tx.hash(), position))
        .collect();

    rows.sort_by_key(|row| {
        (
            positions.get(&row.tx).copied().unwrap_or(usize::MAX),
            row.idx,
        )
    });

    Ok(())
}

/// Order the listing the way Blockfrost does — by the order the chain saw the
/// proposals — and cut it down to the requested page.
///
/// Proposals of one block form a group whose place in the listing the slot
/// already fixes, so the block behind a group is only read once the page
/// reaches it: a page costs at most as many block reads as it has rows, and
/// only for blocks that proposed more than once.
fn select_proposals<D: Domain>(
    domain: &D,
    mut proposals: Vec<ProposalRow>,
    pagination: &Pagination,
) -> Result<Vec<ProposalRow>, Error> {
    proposals.sort_unstable_by_key(|row| (row.slot, row.tx, row.idx));

    let mut groups: Vec<Vec<ProposalRow>> = Vec::new();

    for row in proposals {
        match groups.last_mut() {
            Some(group) if group[0].slot == row.slot => group.push(row),
            _ => groups.push(vec![row]),
        }
    }

    let descending = matches!(pagination.order, Order::Desc);

    if descending {
        groups.reverse();
    }

    let from = pagination.from();
    let to = from + pagination.count;

    let mut out = Vec::new();
    let mut seen = 0;

    for mut group in groups {
        let end = seen + group.len();

        if end <= from {
            seen = end;
            continue;
        }

        if seen >= to {
            break;
        }

        order_within_block(domain, group[0].slot, &mut group)?;

        // desc is the whole asc listing read backwards, group order included
        if descending {
            group.reverse();
        }

        for (offset, row) in group.into_iter().enumerate() {
            if (from..to).contains(&(seen + offset)) {
                out.push(row);
            }
        }

        seen = end;
    }

    Ok(out)
}

/// The page of `GET /governance/proposals`, read off the state and ordered
/// against the archive.
///
/// Every proposal is walked to build one page. The namespace holds one row per
/// governance action ever submitted and each of those costs a deposit, so it
/// grows in the hundreds — 155 rows on mainnet and 1536 on preview, the
/// cheapest of the networks to propose on — against the million-row namespaces
/// the other listings here already walk. What the page actually pays for is
/// the archive, and that is bounded by the page size.
fn read_page<D: Domain>(domain: &D, pagination: &Pagination) -> Result<Vec<ProposalsInner>, Error> {
    let mut rows = Vec::new();

    let entities = domain
        .state()
        .iter_entities_typed::<ProposalState>(ProposalState::NS, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for entry in entities {
        let (_, state) = entry.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if !is_gov_action(&state) {
            continue;
        }

        let Some(governance_type) = governance_type(&state.action) else {
            continue;
        };

        rows.push(ProposalRow {
            slot: state.slot,
            tx: state.tx,
            idx: state.idx,
            governance_type,
        });
    }

    select_proposals(domain, rows, pagination)?
        .into_iter()
        .map(|row| {
            Ok(ProposalsInner {
                id: bech32_gov_action(&row.tx, row.idx)?,
                tx_hash: hex::encode(row.tx),
                cert_index: row
                    .idx
                    .try_into()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
                governance_type: row.governance_type,
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()
        .map_err(Error::from)
}

/// `GET /governance/proposals`: every governance action, oldest first.
///
/// Blockfrost reads its listing straight off db-sync's proposal table with no
/// filter of its own, so the filtering here is only about telling governance
/// actions apart from the pre-Conway update proposals dolos keeps beside them.
pub async fn proposals<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ProposalsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let page = domain
        .query()
        .run_blocking(move |domain| Ok(read_page(&domain, &pagination)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(Json(page))
}

/// A ratio as the plain quotient, without the rounding
/// `/epochs/{n}/parameters` applies to the same parameters.
///
/// Blockfrost serves this endpoint from db-sync's `param_proposal` columns,
/// which hold the quotient at full `double precision`: a proposal setting tau
/// to 1/6 comes back as 0.16666666666666666, not 0.167.
fn ratio_to_f64(value: &RationalNumber) -> f64 {
    value.numerator as f64 / value.denominator as f64
}

/// The parameter change one proposal asks for, as the nullable delta
/// Blockfrost returns.
///
/// Every field is a change the proposal names, so an untouched parameter is
/// `null` rather than the value in force — the opposite of
/// `/epochs/{n}/parameters`, which reports the parameters actually effective.
/// The two Blockfrost renders from the same db-sync column each appear twice
/// here, once under each name: `coins_per_utxo_word` repeats
/// `coins_per_utxo_size`, and `pvt_p_p_security_group` repeats
/// `pvtpp_security_group`.
struct ProposalParametersBuilder {
    tx: Hash<32>,
    idx: u32,
    params: PParamsSet,
}

impl IntoModel<ProposalParameters> for ProposalParametersBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<ProposalParameters, StatusCode> {
        let Self { tx, idx, params } = self;

        let parameters = ProposalParametersParameters {
            // A Conway proposal names no epoch: db-sync fills the column only
            // for the pre-Conway update proposals it keeps in the same table.
            epoch: Some(None),
            min_fee_a: params.min_fee_a().map(|x| x as i32),
            min_fee_b: params.min_fee_b().map(|x| x as i32),
            max_block_size: params.max_block_body_size().map(|x| x as i32),
            max_tx_size: params.max_transaction_size().map(|x| x as i32),
            max_block_header_size: params.max_block_header_size().map(|x| x as i32),
            key_deposit: params.key_deposit().map(|x| x.to_string()),
            pool_deposit: params.pool_deposit().map(|x| x.to_string()),
            e_max: params.maximum_epoch().map(|x| x as i32),
            n_opt: params.desired_number_of_stake_pools().map(|x| x as i32),
            a0: params.a0().map(|x| ratio_to_f64(&x)),
            rho: params.rho().map(|x| ratio_to_f64(&x)),
            tau: params.tau().map(|x| ratio_to_f64(&x)),
            // Both are pre-Conway knobs that a Conway proposal cannot name.
            decentralisation_param: None,
            extra_entropy: None,
            // A version bump is a hard fork, never a parameter change.
            protocol_major_ver: None,
            protocol_minor_ver: None,
            min_utxo: params.ada_per_utxo_byte().map(|x| x.to_string()),
            min_pool_cost: params.min_pool_cost().map(|x| x.to_string()),
            // Blockfrost tells "set to the empty map" (`{}`) apart from "not
            // named at all" (`null`) because db-sync keys a row per cost
            // model. `PParamsSet` records a language at a time, so a change
            // naming no language leaves nothing behind to tell the two apart
            // and both read as `null` here.
            cost_models: map_cost_models_raw(&params.cost_models_for_script_languages()).flatten(),
            price_mem: params.execution_costs().map(|x| ratio_to_f64(&x.mem_price)),
            price_step: params
                .execution_costs()
                .map(|x| ratio_to_f64(&x.step_price)),
            max_tx_ex_mem: params.max_tx_ex_units().map(|x| x.mem.to_string()),
            max_tx_ex_steps: params.max_tx_ex_units().map(|x| x.steps.to_string()),
            max_block_ex_mem: params.max_block_ex_units().map(|x| x.mem.to_string()),
            max_block_ex_steps: params.max_block_ex_units().map(|x| x.steps.to_string()),
            max_val_size: params.max_value_size().map(|x| x.to_string()),
            collateral_percent: params.collateral_percentage().map(|x| x as i32),
            max_collateral_inputs: params.max_collateral_inputs().map(|x| x as i32),
            coins_per_utxo_size: params.ada_per_utxo_byte().map(|x| x.to_string()),
            coins_per_utxo_word: params.ada_per_utxo_byte().map(|x| x.to_string()),
            pvt_motion_no_confidence: params
                .pool_voting_thresholds()
                .map(|x| ratio_to_f64(&x.motion_no_confidence)),
            pvt_committee_normal: params
                .pool_voting_thresholds()
                .map(|x| ratio_to_f64(&x.committee_normal)),
            pvt_committee_no_confidence: params
                .pool_voting_thresholds()
                .map(|x| ratio_to_f64(&x.committee_no_confidence)),
            pvt_hard_fork_initiation: params
                .pool_voting_thresholds()
                .map(|x| ratio_to_f64(&x.hard_fork_initiation)),
            pvtpp_security_group: params
                .pool_voting_thresholds()
                .map(|x| ratio_to_f64(&x.security_voting_threshold)),
            pvt_p_p_security_group: params
                .pool_voting_thresholds()
                .map(|x| ratio_to_f64(&x.security_voting_threshold)),
            dvt_motion_no_confidence: params
                .drep_voting_thresholds()
                .map(|x| ratio_to_f64(&x.motion_no_confidence)),
            dvt_committee_normal: params
                .drep_voting_thresholds()
                .map(|x| ratio_to_f64(&x.committee_normal)),
            dvt_committee_no_confidence: params
                .drep_voting_thresholds()
                .map(|x| ratio_to_f64(&x.committee_no_confidence)),
            dvt_update_to_constitution: params
                .drep_voting_thresholds()
                .map(|x| ratio_to_f64(&x.update_constitution)),
            dvt_hard_fork_initiation: params
                .drep_voting_thresholds()
                .map(|x| ratio_to_f64(&x.hard_fork_initiation)),
            dvt_p_p_network_group: params
                .drep_voting_thresholds()
                .map(|x| ratio_to_f64(&x.pp_network_group)),
            dvt_p_p_economic_group: params
                .drep_voting_thresholds()
                .map(|x| ratio_to_f64(&x.pp_economic_group)),
            dvt_p_p_technical_group: params
                .drep_voting_thresholds()
                .map(|x| ratio_to_f64(&x.pp_technical_group)),
            dvt_p_p_gov_group: params
                .drep_voting_thresholds()
                .map(|x| ratio_to_f64(&x.pp_governance_group)),
            dvt_treasury_withdrawal: params
                .drep_voting_thresholds()
                .map(|x| ratio_to_f64(&x.treasury_withdrawal)),
            committee_min_size: params.min_committee_size().map(|x| x.to_string()),
            committee_max_term_length: params.committee_term_limit().map(|x| x.to_string()),
            gov_action_lifetime: params
                .governance_action_validity_period()
                .map(|x| x.to_string()),
            gov_action_deposit: params.governance_action_deposit().map(|x| x.to_string()),
            drep_deposit: params.drep_deposit().map(|x| x.to_string()),
            drep_activity: params.drep_inactivity_period().map(|x| x.to_string()),
            min_fee_ref_script_cost_per_byte: params
                .min_fee_ref_script_cost_per_byte()
                .map(|x| ratio_to_f64(&x)),
        };

        Ok(ProposalParameters {
            id: bech32_gov_action(&tx, idx)?,
            tx_hash: hex::encode(tx),
            cert_index: idx.try_into().map_err(|_| StatusCode::BAD_REQUEST)?,
            parameters: Box::new(parameters),
        })
    }
}

/// The parameter change proposed by `tx` at action index `idx`.
///
/// Blockfrost joins the proposal against db-sync's `param_proposal` table, so
/// a proposal of any other kind has no row to return and is a 404 — unlike the
/// withdrawal listing beside it, which answers with an empty array.
fn read_parameters<D: Domain>(
    domain: &Facade<D>,
    tx: Hash<32>,
    idx: u32,
) -> Result<ProposalParameters, Error> {
    let key = ProposalState::build_entity_key(tx, idx);

    let state = domain
        .state()
        .read_entity_typed::<ProposalState>(ProposalState::NS, &key)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    // A pre-Conway update proposal is a parameter change too, but db-sync
    // keeps those out of the governance table this endpoint reads.
    if !is_gov_action(&state) {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let ProposalAction::ParamChange(params) = state.action else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let model = ProposalParametersBuilder { tx, idx, params };

    Ok(model.into_model()?)
}

/// `GET /governance/proposals/{tx_hash}/{cert_index}/parameters`.
pub async fn proposal_parameters<D>(
    Path((tx_hash, cert_index)): Path<(String, String)>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ProposalParameters>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let cert_index = cert_index
        .parse::<u32>()
        .map_err(|_| Error::InvalidCertIndex)?;

    // Blockfrost matches the hash as text against db-sync, so a malformed one
    // finds nothing rather than failing the request.
    let tx = tx_hash
        .parse::<Hash<32>>()
        .map_err(|_| StatusCode::NOT_FOUND)?;

    Ok(Json(read_parameters(&domain, tx, cert_index)?))
}

/// `GET /governance/proposals/{gov_action_id}/parameters`: the same change,
/// addressed by CIP-129 id instead of by tx hash and action index.
pub async fn proposal_parameters_by_gov_action<D>(
    Path(gov_action_id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ProposalParameters>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let (tx, idx) = parse_gov_action_id(&gov_action_id).map_err(|_| Error::InvalidGovActionId)?;

    Ok(Json(read_parameters(&domain, tx, idx)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use bech32::{Bech32, Hrp};
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use itertools::Itertools;
    use pallas::ledger::primitives::{
        conway::{
            CostModels, DRepVotingThresholds, ExUnitPrices, GovAction, PoolVotingThresholds,
            ProtocolParamUpdate,
        },
        ExUnits, RationalNumber,
    };

    fn invalid_drep() -> &'static str {
        "not-a-drep"
    }

    fn missing_drep() -> String {
        let mut payload = Vec::with_capacity(29);
        payload.push(0b00100010);
        payload.extend_from_slice(&[8u8; 28]);
        let hrp = Hrp::parse_unchecked("drep");
        bech32::encode::<Bech32>(hrp, &payload).expect("failed to encode missing drep")
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, _body) = app.get_bytes(path).await;
        assert_eq!(status, expected);
    }

    #[tokio::test]
    async fn governance_drep_happy_path() {
        let app = TestApp::new();
        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        let _model: blockfrost_openapi::models::drep::Drep =
            serde_json::from_slice(&body).expect("failed to parse drep model");
    }

    #[tokio::test]
    async fn governance_drep_bad_request() {
        let app = TestApp::new();
        let path = format!("/governance/dreps/{}", invalid_drep());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_drep_not_found() {
        let app = TestApp::new();
        let missing = missing_drep();
        let path = format!("/governance/dreps/{missing}");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_drep_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    /// Three blocks: the first tx of block 1 proposes two actions, block 2
    /// proposes none and block 3 proposes one. Enough to pin the listing
    /// order, the cert index within a tx and a few action types.
    fn proposal_app() -> TestApp {
        TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 1,
            gov_actions_by_block: vec![
                vec![vec![
                    GovAction::Information,
                    GovAction::HardForkInitiation(None, (10, 0)),
                ]],
                vec![],
                vec![vec![GovAction::NoConfidence(None)]],
            ],
            ..Default::default()
        })
    }

    async fn get_proposals(app: &TestApp, query: &str) -> Vec<ProposalsInner> {
        let path = format!("/governance/proposals{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("failed to parse proposals")
    }

    fn tx_hash_of_block(app: &TestApp, block: usize) -> String {
        app.vectors().blocks[block].tx_hashes[0].clone()
    }

    fn as_rows(proposals: &[ProposalsInner]) -> Vec<(String, i32, GovernanceType)> {
        proposals
            .iter()
            .map(|x| (x.tx_hash.clone(), x.cert_index, x.governance_type))
            .collect_vec()
    }

    #[tokio::test]
    async fn governance_proposals_happy_path() {
        let app = proposal_app();
        let first_tx = tx_hash_of_block(&app, 0);
        let third_tx = tx_hash_of_block(&app, 2);

        let proposals = get_proposals(&app, "").await;

        assert_eq!(
            as_rows(&proposals),
            vec![
                (first_tx.clone(), 0, GovernanceType::InfoAction),
                (first_tx.clone(), 1, GovernanceType::HardForkInitiation),
                (third_tx, 0, GovernanceType::NoConfidence),
            ]
        );

        // the id names the same proposal as the tx hash and the cert index
        let tx: Hash<32> = first_tx.parse().expect("failed to parse tx hash");
        assert_eq!(proposals[1].id, bech32_gov_action(&tx, 1).unwrap());
    }

    #[tokio::test]
    async fn governance_proposals_orders_and_paginates() {
        let app = proposal_app();
        let first_tx = tx_hash_of_block(&app, 0);
        let third_tx = tx_hash_of_block(&app, 2);

        let desc = get_proposals(&app, "?order=desc").await;
        assert_eq!(
            as_rows(&desc),
            vec![
                (third_tx.clone(), 0, GovernanceType::NoConfidence),
                (first_tx.clone(), 1, GovernanceType::HardForkInitiation),
                (first_tx.clone(), 0, GovernanceType::InfoAction),
            ]
        );

        // asc: page 2 of size 1 is the second action of the first proposing tx
        let page = get_proposals(&app, "?page=2&count=1").await;
        assert_eq!(
            as_rows(&page),
            vec![(first_tx, 1, GovernanceType::HardForkInitiation)]
        );

        // desc: page 1 of size 1 is the newest proposal
        let page = get_proposals(&app, "?order=desc&page=1&count=1").await;
        assert_eq!(
            as_rows(&page),
            vec![(third_tx, 0, GovernanceType::NoConfidence)]
        );

        // a page past the end is empty, not an error
        let page = get_proposals(&app, "?page=4&count=1").await;
        assert!(page.is_empty());
    }

    /// Four txs of one block propose, so the whole listing sits in a single
    /// block and the order can only come from the position of each tx inside
    /// it — the hashes disagree, which is what Blockfrost's own ordering by
    /// db-sync row id exposes on preview.
    #[tokio::test]
    async fn governance_proposals_follow_tx_order_inside_a_block() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 4,
            gov_actions_by_block: vec![vec![
                vec![GovAction::Information],
                vec![GovAction::Information],
                vec![GovAction::Information],
                vec![GovAction::Information],
            ]],
            ..Default::default()
        });

        let txs = app.vectors().blocks[0].tx_hashes.clone();
        let by_hash = txs.iter().sorted().cloned().collect_vec();
        assert_ne!(
            txs, by_hash,
            "fixture stopped telling block order and hash order apart"
        );

        let proposals = get_proposals(&app, "").await;
        assert_eq!(
            proposals.iter().map(|x| x.tx_hash.clone()).collect_vec(),
            txs
        );

        // a page cutting the block's group in half keeps that order
        let page = get_proposals(&app, "?count=2&page=2").await;
        assert_eq!(
            page.iter().map(|x| x.tx_hash.clone()).collect_vec(),
            txs[2..].to_vec()
        );

        // desc is the same listing read backwards, inside the block too
        let desc = get_proposals(&app, "?order=desc").await;
        assert_eq!(
            desc.iter().map(|x| x.tx_hash.clone()).collect_vec(),
            txs.iter().rev().cloned().collect_vec()
        );
    }

    #[tokio::test]
    async fn governance_proposals_without_any_proposal() {
        // the default synthetic chain proposes nothing
        let app = TestApp::new();
        assert!(get_proposals(&app, "").await.is_empty());
    }

    #[tokio::test]
    async fn governance_proposals_bad_request() {
        let app = proposal_app();
        assert_status(
            &app,
            "/governance/proposals?count=0",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/proposals?page=x",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/proposals?order=sideways",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn governance_proposals_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(
            &app,
            "/governance/proposals",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }

    #[test]
    fn governance_type_names_every_action() {
        let cases = [
            (
                ProposalAction::ParamChange(PParamsSet::default()),
                Some(GovernanceType::ParameterChange),
            ),
            (
                ProposalAction::HardFork((10, 0)),
                Some(GovernanceType::HardForkInitiation),
            ),
            (
                ProposalAction::TreasuryWithdrawal(vec![]),
                Some(GovernanceType::TreasuryWithdrawals),
            ),
            (
                ProposalAction::NoConfidence,
                Some(GovernanceType::NoConfidence),
            ),
            (
                ProposalAction::UpdateCommittee {
                    to_remove: vec![],
                    to_add: vec![],
                    threshold: pallas::ledger::primitives::RationalNumber {
                        numerator: 1,
                        denominator: 2,
                    },
                },
                Some(GovernanceType::NewCommittee),
            ),
            (
                ProposalAction::NewConstitution {
                    anchor: pallas::ledger::primitives::conway::Anchor {
                        url: "https://dolos.test".to_string(),
                        content_hash: Hash::from([1u8; 32]),
                    },
                    guardrail_script: None,
                },
                Some(GovernanceType::NewConstitution),
            ),
            (ProposalAction::Info, Some(GovernanceType::InfoAction)),
            // the legacy catch-all carries no action to name
            (ProposalAction::Other, None),
        ];

        for (action, expected) in cases {
            assert_eq!(governance_type(&action), expected, "{action:?}");
        }
    }

    fn ratio(numerator: u64, denominator: u64) -> RationalNumber {
        RationalNumber {
            numerator,
            denominator,
        }
    }

    /// The scalar half of preview's `608037b7…e09b#0`, the widest parameter
    /// change on that network: every non-threshold field it sets, with the
    /// value Blockfrost returns for it.
    fn wide_update() -> ProtocolParamUpdate {
        ProtocolParamUpdate {
            minfee_a: Some(999),
            minfee_b: Some(9_999_999),
            max_block_body_size: Some(122_879),
            max_transaction_size: Some(32_768),
            max_block_header_size: Some(5_000),
            key_deposit: Some(5_000_000),
            pool_deposit: Some(250_000_000),
            maximum_epoch: Some(0),
            desired_number_of_stake_pools: Some(2_000),
            pool_pledge_influence: Some(ratio(1, 10)),
            expansion_rate: Some(ratio(5, 1_000)),
            treasury_growth_rate: Some(ratio(3, 10)),
            min_pool_cost: Some(500_000_000),
            ada_per_utxo_byte: Some(6_500),
            cost_models_for_script_languages: None,
            execution_costs: Some(ExUnitPrices {
                mem_price: ratio(2, 10),
                step_price: ratio(2, 10_000),
            }),
            max_tx_ex_units: Some(ExUnits {
                mem: 40_000_000,
                steps: 14_900_000_000,
            }),
            max_block_ex_units: Some(ExUnits {
                mem: 120_000_000,
                steps: 40_000_000_000,
            }),
            max_value_size: Some(12_287),
            collateral_percentage: Some(200),
            max_collateral_inputs: Some(999),
            pool_voting_thresholds: None,
            drep_voting_thresholds: None,
            min_committee_size: Some(10),
            committee_term_limit: Some(293),
            governance_action_validity_period: Some(1),
            governance_action_deposit: Some(1_000_000),
            drep_deposit: Some(99_999_000_000),
            drep_inactivity_period: Some(13),
            minfee_refscript_cost_per_byte: Some(ratio(999, 1)),
        }
    }

    fn empty_update() -> ProtocolParamUpdate {
        ProtocolParamUpdate {
            minfee_a: None,
            minfee_b: None,
            max_block_body_size: None,
            max_transaction_size: None,
            max_block_header_size: None,
            key_deposit: None,
            pool_deposit: None,
            maximum_epoch: None,
            desired_number_of_stake_pools: None,
            pool_pledge_influence: None,
            expansion_rate: None,
            treasury_growth_rate: None,
            min_pool_cost: None,
            ada_per_utxo_byte: None,
            cost_models_for_script_languages: None,
            execution_costs: None,
            max_tx_ex_units: None,
            max_block_ex_units: None,
            max_value_size: None,
            collateral_percentage: None,
            max_collateral_inputs: None,
            pool_voting_thresholds: None,
            drep_voting_thresholds: None,
            min_committee_size: None,
            committee_term_limit: None,
            governance_action_validity_period: None,
            governance_action_deposit: None,
            drep_deposit: None,
            drep_inactivity_period: None,
            minfee_refscript_cost_per_byte: None,
        }
    }

    /// The thresholds preview's `aa1fe93e…0f6b#0` (pool) and
    /// `1fb793f6…425b#0` (DRep) set, in one update so a single fixture pins
    /// both groups.
    fn thresholds_update() -> ProtocolParamUpdate {
        ProtocolParamUpdate {
            pool_voting_thresholds: Some(PoolVotingThresholds {
                motion_no_confidence: ratio(52, 100),
                committee_normal: ratio(52, 100),
                committee_no_confidence: ratio(52, 100),
                hard_fork_initiation: ratio(52, 100),
                security_voting_threshold: ratio(52, 100),
            }),
            drep_voting_thresholds: Some(DRepVotingThresholds {
                motion_no_confidence: ratio(68, 100),
                committee_normal: ratio(68, 100),
                committee_no_confidence: ratio(61, 100),
                update_constitution: ratio(76, 100),
                hard_fork_initiation: ratio(61, 100),
                pp_network_group: ratio(68, 100),
                pp_economic_group: ratio(68, 100),
                pp_technical_group: ratio(68, 100),
                pp_governance_group: ratio(76, 100),
                treasury_withdrawal: ratio(68, 100),
            }),
            ..empty_update()
        }
    }

    fn cost_models_update() -> ProtocolParamUpdate {
        ProtocolParamUpdate {
            cost_models_for_script_languages: Some(CostModels {
                plutus_v1: None,
                plutus_v2: None,
                plutus_v3: Some(vec![100_788, 420, 1, 1]),
                unknown: Default::default(),
            }),
            ..empty_update()
        }
    }

    /// One tx proposing four parameter changes and, at index 4, an info
    /// action — so the same fixture covers a proposal that names no
    /// parameters at all.
    fn parameters_app() -> TestApp {
        let change = |update| GovAction::ParameterChange(None, Box::new(update), None);

        TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![
                change(wide_update()),
                change(thresholds_update()),
                change(cost_models_update()),
                change(empty_update()),
                GovAction::Information,
            ]]],
            ..Default::default()
        })
    }

    async fn get_parameters(app: &TestApp, path: &str) -> ProposalParameters {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("failed to parse proposal parameters")
    }

    /// The fields the response actually sets, as `(name, json)` pairs. Keeps
    /// the assertions to what a proposal names instead of spelling out the
    /// fifty-odd nulls around it.
    fn set_fields(model: &ProposalParameters) -> Vec<(String, serde_json::Value)> {
        let value = serde_json::to_value(&*model.parameters).expect("failed to serialize");
        let object = value.as_object().expect("parameters is an object").clone();

        object
            .into_iter()
            .filter(|(_, v)| !v.is_null())
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .collect()
    }

    #[tokio::test]
    async fn governance_proposal_parameters_happy_path() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);
        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/0/parameters")).await;

        assert_eq!(model.tx_hash, tx);
        assert_eq!(model.cert_index, 0);
        let parsed: Hash<32> = tx.parse().expect("failed to parse tx hash");
        assert_eq!(model.id, bech32_gov_action(&parsed, 0).unwrap());

        // the value Blockfrost returns for every field this change names
        let expected = serde_json::json!({
            "min_fee_a": 999,
            "min_fee_b": 9_999_999,
            "max_block_size": 122_879,
            "max_tx_size": 32_768,
            "max_block_header_size": 5_000,
            "key_deposit": "5000000",
            "pool_deposit": "250000000",
            "e_max": 0,
            "n_opt": 2_000,
            "a0": 0.1,
            "rho": 0.005,
            "tau": 0.3,
            "min_utxo": "6500",
            "min_pool_cost": "500000000",
            "price_mem": 0.2,
            "price_step": 0.0002,
            "max_tx_ex_mem": "40000000",
            "max_tx_ex_steps": "14900000000",
            "max_block_ex_mem": "120000000",
            "max_block_ex_steps": "40000000000",
            "max_val_size": "12287",
            "collateral_percent": 200,
            "max_collateral_inputs": 999,
            "coins_per_utxo_size": "6500",
            "coins_per_utxo_word": "6500",
            "committee_min_size": "10",
            "committee_max_term_length": "293",
            "gov_action_lifetime": "1",
            "gov_action_deposit": "1000000",
            "drep_deposit": "99999000000",
            "drep_activity": "13",
            "min_fee_ref_script_cost_per_byte": 999.0,
        });

        let expected: Vec<(String, serde_json::Value)> = expected
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .collect();

        assert_eq!(set_fields(&model), expected);
    }

    /// The two names Blockfrost renders one db-sync column under have to move
    /// together, in both pairs.
    #[tokio::test]
    async fn governance_proposal_parameters_duplicate_names_agree() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);

        let wide = get_parameters(&app, &format!("/governance/proposals/{tx}/0/parameters")).await;
        assert_eq!(
            wide.parameters.coins_per_utxo_size,
            wide.parameters.coins_per_utxo_word
        );
        assert_eq!(wide.parameters.coins_per_utxo_size.as_deref(), Some("6500"));

        let thresholds =
            get_parameters(&app, &format!("/governance/proposals/{tx}/1/parameters")).await;
        assert_eq!(
            thresholds.parameters.pvtpp_security_group,
            thresholds.parameters.pvt_p_p_security_group
        );
        assert_eq!(thresholds.parameters.pvtpp_security_group, Some(0.52));
    }

    #[tokio::test]
    async fn governance_proposal_parameters_thresholds() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);
        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/1/parameters")).await;

        let expected = serde_json::json!({
            "pvt_motion_no_confidence": 0.52,
            "pvt_committee_normal": 0.52,
            "pvt_committee_no_confidence": 0.52,
            "pvt_hard_fork_initiation": 0.52,
            "pvtpp_security_group": 0.52,
            "pvt_p_p_security_group": 0.52,
            "dvt_motion_no_confidence": 0.68,
            "dvt_committee_normal": 0.68,
            "dvt_committee_no_confidence": 0.61,
            "dvt_update_to_constitution": 0.76,
            "dvt_hard_fork_initiation": 0.61,
            "dvt_p_p_network_group": 0.68,
            "dvt_p_p_economic_group": 0.68,
            "dvt_p_p_technical_group": 0.68,
            "dvt_p_p_gov_group": 0.76,
            "dvt_treasury_withdrawal": 0.68,
        });

        let expected: Vec<(String, serde_json::Value)> = expected
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .collect();

        assert_eq!(set_fields(&model), expected);
    }

    /// Cost models come back as the raw operation-cost vectors, not the named
    /// map `/epochs/{n}/parameters` builds from the same data.
    #[tokio::test]
    async fn governance_proposal_parameters_cost_models_stay_raw() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);
        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/2/parameters")).await;

        let cost_models = model.parameters.cost_models.expect("cost models are set");
        assert_eq!(cost_models.keys().collect_vec(), vec!["PlutusV3"]);
        assert_eq!(
            cost_models["PlutusV3"],
            serde_json::json!([100_788, 420, 1, 1])
        );
    }

    /// Ratios keep the precision the chain gave them.
    ///
    /// `/epochs/{n}/parameters` rounds the same parameters, and Blockfrost
    /// rounds there too — but this endpoint reads db-sync's `param_proposal`
    /// columns straight, so a third that cannot be written down exactly has
    /// to come back long. Preview's `4869ef5d…ff1a#0` sets tau to 1/6 and
    /// Blockfrost answers 0.16666666666666666.
    #[tokio::test]
    async fn governance_proposal_parameters_keep_full_precision() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![GovAction::ParameterChange(
                None,
                Box::new(ProtocolParamUpdate {
                    treasury_growth_rate: Some(ratio(1, 6)),
                    ..empty_update()
                }),
                None,
            )]]],
            ..Default::default()
        });

        let tx = tx_hash_of_block(&app, 0);
        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/0/parameters")).await;

        assert_eq!(model.parameters.tau, Some(1.0 / 6.0));

        // and it survives serialization as the long form, not as 0.167
        let value = serde_json::to_value(&*model.parameters).unwrap();
        assert_eq!(value["tau"].to_string(), "0.16666666666666666");
    }

    /// A change that names nothing still answers — with every field null.
    #[tokio::test]
    async fn governance_proposal_parameters_empty_change() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);
        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/3/parameters")).await;

        assert!(set_fields(&model).is_empty(), "{:?}", set_fields(&model));

        // `epoch` is serialized as an explicit null rather than dropped
        let value = serde_json::to_value(&*model.parameters).unwrap();
        assert_eq!(value["epoch"], serde_json::Value::Null);
        assert!(value.as_object().unwrap().contains_key("epoch"));
    }

    /// Unlike the withdrawal listing beside it, this one 404s rather than
    /// answering empty: Blockfrost inner-joins the proposal against
    /// `param_proposal`, so a proposal of another kind has no row.
    #[tokio::test]
    async fn governance_proposal_parameters_not_found() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);

        // index 4 of the same tx is the info action
        let path = format!("/governance/proposals/{tx}/4/parameters");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        // an index the tx never proposed at
        let path = format!("/governance/proposals/{tx}/9/parameters");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        let missing = hex::encode([0u8; 32]);
        let path = format!("/governance/proposals/{missing}/0/parameters");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        let path = "/governance/proposals/not-a-tx-hash/0/parameters";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;

        // a well-formed id for a proposal nobody made
        let id = bech32_gov_action(&Hash::from([0u8; 32]), 0).unwrap();
        let path = format!("/governance/proposals/{id}/parameters");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_proposal_parameters_by_gov_action_id() {
        let app = parameters_app();
        let tx: Hash<32> = tx_hash_of_block(&app, 0)
            .parse()
            .expect("failed to parse tx hash");

        let by_index =
            get_parameters(&app, &format!("/governance/proposals/{tx}/0/parameters")).await;

        let id = bech32_gov_action(&tx, 0).unwrap();
        let by_id = get_parameters(&app, &format!("/governance/proposals/{id}/parameters")).await;
        assert_eq!(by_id, by_index);

        // the bare-hash form explorers write for index 0 names the same
        // proposal as the one-byte form Blockfrost writes
        let minimal = bech32(bech32::Hrp::parse("gov_action").unwrap(), tx.as_slice()).unwrap();
        let by_minimal =
            get_parameters(&app, &format!("/governance/proposals/{minimal}/parameters")).await;
        assert_eq!(by_minimal, by_index);

        // an id past index 0 still resolves to its own action
        let id = bech32_gov_action(&tx, 2).unwrap();
        let by_id = get_parameters(&app, &format!("/governance/proposals/{id}/parameters")).await;
        assert_eq!(by_id.cert_index, 2);
        assert!(by_id.parameters.cost_models.is_some());
    }

    #[tokio::test]
    async fn governance_proposal_parameters_bad_request() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);

        // the cert index is the only path part that has to be a number
        let path = format!("/governance/proposals/{tx}/x/parameters");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;

        for id in ["not-bech32", &missing_drep()] {
            let path = format!("/governance/proposals/{id}/parameters");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn governance_proposal_parameters_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = format!(
            "/governance/proposals/{}/0/parameters",
            hex::encode([1u8; 32])
        );
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    /// CIP-129 ids read back into the proposal they name, including the
    /// bare-hash form that omits the index byte for index 0.
    #[test]
    fn gov_action_id_round_trips() {
        let tx: Hash<32> = "b2a591ac219ce6dcca5847e0248015209c7cb0436aa6bd6863d0c1f152a60bc5"
            .parse()
            .expect("failed to parse tx hash");

        for idx in [0, 1, 255, 256, u32::MAX] {
            let id = bech32_gov_action(&tx, idx).unwrap();
            assert_eq!(parse_gov_action_id(&id).unwrap(), (tx, idx), "{idx}");
        }

        let bare = bech32(bech32::Hrp::parse("gov_action").unwrap(), tx.as_slice()).unwrap();
        assert_eq!(parse_gov_action_id(&bare).unwrap(), (tx, 0));

        // not bech32, wrong hrp, and a payload too short to hold a tx hash
        assert!(parse_gov_action_id("not-bech32").is_err());
        assert!(parse_gov_action_id(&missing_drep()).is_err());
        let short = bech32(bech32::Hrp::parse("gov_action").unwrap(), [0u8; 31]).unwrap();
        assert!(parse_gov_action_id(&short).is_err());

        // the index is written in the shortest big-endian form, so a padded
        // one is a second spelling of an id that already has a canonical form
        let padded = bech32(
            bech32::Hrp::parse("gov_action").unwrap(),
            [tx.as_slice(), &[0x00, 0x01]].concat(),
        )
        .unwrap();
        assert!(parse_gov_action_id(&padded).is_err());
    }

    /// CIP-129: the id is the proposing tx hash with the action index
    /// trailing it. The first two vectors come from the Blockfrost spec kept
    /// in `crates/minibf/openapi.yaml`; the last one pins the minimal
    /// big-endian rule Blockfrost encodes the index with.
    #[test]
    fn gov_action_id_follows_cip129() {
        let tx = Hash::<32>::from([0x11u8; 32]);
        assert_eq!(
            bech32_gov_action(&tx, 0).unwrap(),
            "gov_action1zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zygsq6dmejn"
        );

        let tx: Hash<32> = "b2a591ac219ce6dcca5847e0248015209c7cb0436aa6bd6863d0c1f152a60bc5"
            .parse()
            .expect("failed to parse tx hash");
        assert_eq!(
            bech32_gov_action(&tx, 0).unwrap(),
            "gov_action1k2jertppnnndejjcglszfqq4yzw8evzrd2nt66rr6rqlz54xp0zsq05ecsn"
        );

        // one byte per index until it no longer fits, then two
        let payload = |idx| {
            let id = bech32_gov_action(&tx, idx).unwrap();
            bech32::decode(&id).unwrap().1[32..].to_vec()
        };
        assert_eq!(payload(0), vec![0x00]);
        assert_eq!(payload(1), vec![0x01]);
        assert_eq!(payload(255), vec![0xff]);
        assert_eq!(payload(256), vec![0x01, 0x00]);
    }
}
