mod dreps;
mod mapping;
mod metadata;

use std::collections::HashMap;

use self::mapping::description_json;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    proposal::{self, Proposal},
    proposal_withdrawals_inner::ProposalWithdrawalsInner,
    proposals_inner::{GovernanceType, ProposalsInner},
    DrepsInner,
};
use dolos_cardano::{
    model::{DRepState, FixedNamespace as _, ProposalAction, ProposalState},
    ChainSummary, PParamsSet,
};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain, StateStore as _};
use dreps::{drep_is_expired, drep_is_retired, drep_list_item, parse_drep_id, DrepModelBuilder};
use futures::future::join_all;
use metadata::fetch_drep_metadata;
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::Network,
        primitives::{conway::GovAction, Coin, Epoch, StakeCredential},
        traverse::{MultiEraBlock, MultiEraTx},
    },
};
use serde::Deserialize;

use crate::{
    error::Error,
    mapping::{bech32_gov_action, parse_gov_action_id, stake_cred_to_address, IntoModel},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

fn chain_context<D: Domain>(
    domain: &Facade<D>,
) -> Result<(ChainSummary, BlockSlot, PParamsSet), StatusCode> {
    let chain = domain.get_chain_summary()?;
    let tip = domain.get_tip_slot()?;
    let pparams = domain.get_current_effective_pparams()?;

    Ok((chain, tip, pparams))
}

/// Query parameters of `/governance/dreps`: the shared pagination set plus
/// the endpoint's own `order_by`, `retired` and `expired`. Blockfrost does
/// not define `from`/`to` here.
#[derive(Debug, Deserialize)]
pub struct DrepsListParameters {
    pub count: Option<String>,
    pub page: Option<String>,
    pub order: Option<String>,
    pub order_by: Option<String>,
    pub retired: Option<String>,
    pub expired: Option<String>,
}

impl DrepsListParameters {
    fn pagination(&self) -> PaginationParameters {
        PaginationParameters {
            count: self.count.clone(),
            page: self.page.clone(),
            order: self.order.clone(),
            from: None,
            to: None,
        }
    }

    /// `order_by` accepts only `amount`, mirroring the openapi enum.
    fn order_by_amount(&self) -> Result<bool, Error> {
        match self.order_by.as_deref() {
            None => Ok(false),
            Some("amount") => Ok(true),
            Some(_) => Err(StatusCode::BAD_REQUEST.into()),
        }
    }
}

/// Blockfrost validates these as booleans and rejects anything else.
fn parse_bool_filter(value: Option<&str>) -> Result<Option<bool>, Error> {
    match value {
        None => Ok(None),
        Some("true") => Ok(Some(true)),
        Some("false") => Ok(Some(false)),
        Some(_) => Err(StatusCode::BAD_REQUEST.into()),
    }
}

pub async fn all_dreps<D: Domain>(
    Query(params): Query<DrepsListParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<DrepsInner>>, Error>
where
    Option<DRepState>: From<D::Entity>,
{
    let order_by_amount = params.order_by_amount()?;
    let retired = parse_bool_filter(params.retired.as_deref())?;
    let expired = parse_bool_filter(params.expired.as_deref())?;

    let pagination = Pagination::try_from(params.pagination())?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let (chain, tip, pparams) = chain_context(&domain)?;

    let mut dreps = vec![];

    for item in domain.iter_cardano_entities::<DRepState>(None)? {
        let (key, state) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Blockfrost applies the filters before pagination, so every page
        // holds up to `count` matching rows.
        if retired.is_some_and(|wanted| drep_is_retired(&state) != wanted) {
            continue;
        }

        if expired.is_some_and(|wanted| drep_is_expired(&state, &chain, tip, &pparams) != wanted) {
            continue;
        }

        let appeared_at = state.first_seen_at.unwrap_or((u64::MAX, usize::MAX));

        dreps.push((appeared_at, key, state));
    }

    if order_by_amount {
        // `order` flips only the amount; the appearance order stays the
        // ascending tie-breaker, like Blockfrost's `ORDER BY amount, id ASC`.
        dreps.sort_by(|(a_order, a_key, a_state), (b_order, b_key, b_state)| {
            let amounts = match pagination.order {
                Order::Desc => b_state.voting_power.cmp(&a_state.voting_power),
                Order::Asc => a_state.voting_power.cmp(&b_state.voting_power),
            };

            amounts.then_with(|| (a_order, a_key).cmp(&(b_order, b_key)))
        });
    } else {
        dreps.sort_by(|(a_order, a_key, _), (b_order, b_key, _)| {
            (a_order, a_key).cmp(&(b_order, b_key))
        });

        if matches!(pagination.order, Order::Desc) {
            dreps.reverse();
        }
    }

    let items = dreps
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .map(|(_, _, state)| async {
            let metadata = fetch_drep_metadata(state.anchor.clone()).await;
            let mut model = drep_list_item(state, &pparams, &chain, tip)?;
            model.metadata = metadata.map(Box::new);
            Ok::<_, StatusCode>(model)
        });

    let page = join_all(items)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(page))
}

pub async fn drep_by_id<D: Domain>(
    Path(drep): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<blockfrost_openapi::models::drep::Drep>, StatusCode>
where
    Option<DRepState>: From<D::Entity>,
{
    let parsed = parse_drep_id(&drep)?;

    let drep_state = if parsed.is_special {
        domain.read_cardano_entity::<DRepState>(parsed.encoded.clone())?
    } else {
        Some(
            domain
                .read_cardano_entity::<DRepState>(parsed.encoded.clone())?
                .ok_or(StatusCode::NOT_FOUND)?,
        )
    };

    let (chain, tip, pparams) = chain_context(&domain)?;

    let model = DrepModelBuilder {
        drep_id: parsed.drep_id,
        drep_id_encoded: parsed.encoded,
        is_legacy: parsed.is_legacy,
        is_special: parsed.is_special,
        state: drep_state,
        pparams: &pparams,
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

/// One page of a proposal's treasury withdrawals, ordered the way the action
/// itself lists them.
///
/// A proposal procedure carries its withdrawals as a map keyed by reward
/// account, so their on-chain order is the raw reward-account bytes ascending.
/// That is the order dolos keeps in the action, and the one Blockfrost's own
/// listing — by the db-sync row id the rows were inserted with — lands in.
fn withdrawals_page(
    state: &ProposalState,
    network: Network,
    pagination: &Pagination,
) -> Result<Vec<ProposalWithdrawalsInner>, StatusCode> {
    let ProposalAction::TreasuryWithdrawal(withdrawals) = &state.action else {
        return Ok(vec![]);
    };

    // desc is the whole ascending listing read backwards
    let ordered: Box<dyn Iterator<Item = &(StakeCredential, Coin)>> = match pagination.order {
        Order::Asc => Box::new(withdrawals.iter()),
        Order::Desc => Box::new(withdrawals.iter().rev()),
    };

    ordered
        .skip(pagination.from())
        .take(pagination.count)
        .map(|(credential, amount)| {
            let stake_address = stake_cred_to_address(credential, network)
                .to_bech32()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            Ok(ProposalWithdrawalsInner {
                stake_address,
                amount: amount.to_string(),
            })
        })
        .collect()
}

/// The withdrawals of the proposal `tx` proposed at action index `idx`.
///
/// Blockfrost reads this off a join with db-sync's `treasury_withdrawal`
/// table and sends whatever rows come back, so a proposal it has never heard
/// of and a proposal that asks for no withdrawal are the same empty listing
/// rather than a 404.
fn read_withdrawals<D: Domain>(
    domain: &Facade<D>,
    tx: Hash<32>,
    idx: u32,
    pagination: &Pagination,
) -> Result<Vec<ProposalWithdrawalsInner>, Error> {
    let key = ProposalState::build_entity_key(tx, idx);

    let state = domain
        .state()
        .read_entity_typed::<ProposalState>(ProposalState::NS, &key)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let Some(state) = state else {
        return Ok(vec![]);
    };

    let network = domain.get_network_id()?;

    Ok(withdrawals_page(&state, network, pagination)?)
}

/// `GET /governance/proposals/{tx_hash}/{cert_index}/withdrawals`: the
/// treasury payouts a withdrawal proposal asks for, oldest first.
pub async fn proposal_withdrawals<D>(
    Path((tx_hash, cert_index)): Path<(String, String)>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ProposalWithdrawalsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let cert_index = cert_index
        .parse::<u32>()
        .map_err(|_| Error::InvalidCertIndex)?;

    // Blockfrost matches the hash as text against db-sync, so a malformed one
    // is a listing that matches nothing rather than a bad request.
    let Ok(tx) = tx_hash.parse::<Hash<32>>() else {
        return Ok(Json(vec![]));
    };

    let page = read_withdrawals(&domain, tx, cert_index, &pagination)?;

    Ok(Json(page))
}

/// `GET /governance/proposals/{gov_action_id}/withdrawals`: the same listing,
/// addressed by CIP-129 id instead of by tx hash and action index.
pub async fn proposal_withdrawals_by_gov_action<D>(
    Path(gov_action_id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ProposalWithdrawalsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let (tx, idx) = parse_gov_action_id(&gov_action_id).map_err(|_| Error::InvalidGovActionId)?;

    let page = read_withdrawals(&domain, tx, idx, &pagination)?;

    Ok(Json(page))
}

// The helpers below reconstruct the `governance_description` JSON that
// Blockfrost copies from db-sync. db-sync stores the cardano-ledger Aeson
// encoding of the submitted `GovAction`, so field names follow the ledger
// JSON instances.

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
    state: ProposalState,
    gov_action: Option<GovAction>,
    network: Network,
    current_epoch: Epoch,
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
            // read_proposal rejects a legacy row that the archive cannot
            // resolve, so this arm never renders.
            ProposalAction::Other => proposal::GovernanceType::InfoAction,
        }
    }

    /// Dolos stamps `ratified_epoch` with the epoch the ratifying boundary
    /// closes. db-sync reports that same epoch as `ratified_epoch` and the
    /// enactment lands one boundary later, so `enacted_epoch` is one more.
    fn enactment_epoch(&self) -> Option<Epoch> {
        let boundary = self.state.ratified_epoch? + 1;

        (self.current_epoch >= boundary).then_some(boundary)
    }

    /// An unratified proposal counts as expired from its `expires_at` epoch
    /// on, before any boundary stamp. The expiry drop later stamps
    /// `canceled_epoch`, one epoch past `expires_at`. A `canceled_epoch` at
    /// or before `expires_at` is a sibling pruned by a competing enactment
    /// instead: db-sync reports that as dropped, never as expired.
    fn expired_epoch(&self) -> Option<Epoch> {
        if self.state.ratified_epoch.is_some() {
            return None;
        }

        let expires = self.state.expires_at()?;

        if self.state.canceled_epoch.is_some_and(|x| x <= expires) {
            return None;
        }

        (self.current_epoch >= expires).then_some(expires)
    }

    /// db-sync marks a proposal as dropped when a competing action gets
    /// enacted (canceled in dolos terms) or one epoch after it marks the
    /// proposal as expired.
    fn dropped_epoch(&self) -> Option<Epoch> {
        if let Some(canceled) = self.state.canceled_epoch {
            return (self.current_epoch >= canceled).then_some(canceled);
        }

        let dropped = self.expired_epoch()? + 1;

        (self.current_epoch >= dropped).then_some(dropped)
    }
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

/// Recover the submitted `GovAction` from the archived proposal tx. Returns
/// `None` when the tx is absent (pruned archive), pre-Conway, or carries no
/// procedure at the index.
async fn load_gov_action<D>(
    domain: &Facade<D>,
    tx: Hash<32>,
    idx: u32,
) -> Result<Option<GovAction>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let Some(cbor) = domain.get_tx(tx).await? else {
        return Ok(None);
    };

    let Ok(era) = cbor.0.try_into() else {
        return Ok(None);
    };

    let Ok(decoded) = MultiEraTx::decode_for_era(era, &cbor.1) else {
        return Ok(None);
    };

    let MultiEraTx::Conway(conway_tx) = decoded else {
        return Ok(None);
    };

    let action = conway_tx
        .transaction_body
        .proposal_procedures
        .as_ref()
        .and_then(|procedures| procedures.get(idx as usize))
        .map(|procedure| procedure.gov_action.clone());

    Ok(action)
}

async fn read_proposal<D>(
    domain: &Facade<D>,
    tx: Hash<32>,
    idx: u32,
) -> Result<Json<Proposal>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<ProposalState>: From<D::Entity>,
{
    let key = ProposalState::build_entity_key(tx, idx);

    let state = domain
        .read_cardano_entity::<ProposalState>(key)?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Blockfrost never serves pre-Conway protocol updates here, and the
    // listing already hides them.
    if !is_gov_action(&state) {
        return Err(StatusCode::NOT_FOUND);
    }

    let gov_action = load_gov_action(domain, tx, idx).await?;

    // A legacy row keeps no action detail. When the archived tx cannot
    // resolve it either, any response would guess the type.
    if matches!(state.action, ProposalAction::Other) && gov_action.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    let chain = domain.get_chain_summary()?;

    let (tip, _) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let (current_epoch, _) = chain.slot_epoch(tip);

    let network = domain.get_network_id()?;

    let model = ProposalModelBuilder {
        state,
        gov_action,
        network,
        current_epoch,
    };

    model.into_response()
}

pub async fn proposal_by_tx_index<D>(
    Path((tx_hash, cert_index)): Path<(String, String)>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Proposal>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<ProposalState>: From<D::Entity>,
{
    let idx: u32 = cert_index.parse().map_err(|_| Error::InvalidCertIndex)?;

    // Blockfrost matches the hash as text against db-sync, so a malformed
    // one is a lookup that finds nothing rather than a bad request.
    let tx: Hash<32> = tx_hash.parse().map_err(|_| StatusCode::NOT_FOUND)?;

    Ok(read_proposal(&domain, tx, idx).await?)
}

pub async fn proposal_by_gov_action_id<D>(
    Path(gov_action_id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Proposal>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<ProposalState>: From<D::Entity>,
{
    let (tx, idx) = parse_gov_action_id(&gov_action_id).map_err(|_| Error::InvalidGovActionId)?;

    Ok(read_proposal(&domain, tx, idx).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::bech32;
    use crate::test_support::{TestApp, TestFault};
    use bech32::{Bech32, Hrp};
    use blockfrost_openapi::models::drep::Drep as DrepModel;
    use dolos_cardano::{model::GovPurpose, pallas_extras};
    use dolos_core::StateWriter as _;
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use itertools::Itertools;
    use pallas::{
        codec::utils::Bytes,
        ledger::primitives::conway::{GovAction, GovActionId},
    };
    use std::collections::BTreeMap;

    fn invalid_drep() -> &'static str {
        "not-a-drep"
    }

    fn encode_id(hrp: &str, payload: &[u8]) -> String {
        let hrp = Hrp::parse_unchecked(hrp);
        bech32::encode::<Bech32>(hrp, payload).expect("failed to encode bech32 id")
    }

    fn missing_drep() -> String {
        let payload = [vec![pallas_extras::DREP_KEY_PREFIX], vec![8u8; 28]].concat();
        encode_id("drep", &payload)
    }

    fn vector_drep_hash(app: &TestApp) -> Vec<u8> {
        let (_, payload) = bech32::decode(&app.vectors().drep_id).expect("invalid vector drep id");

        payload[1..].to_vec()
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, _body) = app.get_bytes(path).await;
        assert_eq!(status, expected);
    }

    async fn get_drep(app: &TestApp, drep_id: &str) -> DrepModel {
        let path = format!("/governance/dreps/{drep_id}");
        let (status, body) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&body)
        );

        serde_json::from_slice(&body).expect("failed to parse drep model")
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

    #[tokio::test]
    async fn governance_drep_happy_path() {
        let app = TestApp::builder()
            .with_cfg(SyntheticBlockConfig {
                drep_deposit: 7777,
                ..Default::default()
            })
            .with_protocol(9)
            .build();

        let drep_id = app.vectors().drep_id.clone();
        let model = get_drep(&app, &drep_id).await;

        let (_, payload) = bech32::decode(&drep_id).expect("invalid vector drep id");

        let expected = DrepModel {
            drep_id,
            hex: hex::encode(&payload),
            // the ledger's drep_distr counts the DRep's own deposit
            amount: "7777".to_string(),
            active: true,
            active_epoch: Some(2),
            has_script: false,
            retired: false,
            expired: false,
            last_active_epoch: Some(2),
        };

        assert_eq!(model, expected);
    }

    #[tokio::test]
    async fn governance_drep_special_ids() {
        let app = TestApp::new();

        for id in ["drep_always_abstain", "drep_always_no_confidence"] {
            let model = get_drep(&app, id).await;

            let expected = DrepModel {
                drep_id: id.to_string(),
                hex: "".to_string(),
                amount: "0".to_string(),
                active: true,
                active_epoch: None,
                has_script: false,
                retired: false,
                expired: false,
                last_active_epoch: None,
            };

            assert_eq!(model, expected);
        }
    }

    #[tokio::test]
    async fn governance_drep_by_id_accepts_legacy_encodings() {
        let app = TestApp::new();
        let hash = vector_drep_hash(&app);
        let cip105 = encode_id("drep", &hash);
        let cip129 = get_drep(&app, &app.vectors().drep_id.clone()).await;

        let expected = DrepModel {
            drep_id: cip105.clone(),
            hex: hex::encode(&hash),
            ..cip129
        };

        assert_eq!(get_drep(&app, &cip105).await, expected);

        // Blockfrost rejects the drep_vkh prefix
        let path = format!("/governance/dreps/{}", encode_id("drep_vkh", &hash));
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_drep_by_id_script_variant_not_found() {
        let app = TestApp::new();
        let hash = vector_drep_hash(&app);

        let path = format!("/governance/dreps/{}", encode_id("drep_script", &hash));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        let cip129_script = [vec![pallas_extras::DREP_SCRIPT_PREFIX], hash].concat();
        let path = format!("/governance/dreps/{}", encode_id("drep", &cip129_script));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    async fn get_dreps_list(app: &TestApp, path: &str) -> Vec<DrepsInner> {
        let (status, body) = app.get_bytes(path).await;
        assert_eq!(status, StatusCode::OK);

        serde_json::from_slice(&body).expect("failed to parse dreps list")
    }

    #[tokio::test]
    async fn governance_dreps_list_happy_path() {
        let app = TestApp::builder()
            .with_cfg(SyntheticBlockConfig {
                drep_deposit: 7777,
                ..Default::default()
            })
            .with_protocol(9)
            .build();

        let models = get_dreps_list(&app, "/governance/dreps").await;

        let drep_id = app.vectors().drep_id.clone();
        let (_, payload) = bech32::decode(&drep_id).expect("invalid vector drep id");

        assert_eq!(
            models,
            vec![DrepsInner {
                drep_id,
                hex: hex::encode(&payload),
                // the ledger's drep_distr counts the DRep's own deposit
                amount: "7777".to_string(),
                has_script: false,
                retired: false,
                expired: false,
                last_active_epoch: Some(2),
                metadata: None,
            }]
        );
    }

    #[tokio::test]
    async fn governance_dreps_list_pagination() {
        let app = TestApp::new();

        let models = get_dreps_list(&app, "/governance/dreps?page=2").await;
        assert!(models.is_empty());

        let models = get_dreps_list(&app, "/governance/dreps?order=desc&count=1").await;
        assert_eq!(models.len(), 1);
    }

    #[tokio::test]
    async fn governance_dreps_list_bad_request() {
        let app = TestApp::new();

        assert_status(&app, "/governance/dreps?count=0", StatusCode::BAD_REQUEST).await;
        assert_status(
            &app,
            "/governance/dreps?order=sideways",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/dreps?order_by=alphabet",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/dreps?retired=banana",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/dreps?expired=banana",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn governance_dreps_list_filters_apply_before_pagination() {
        let app = TestApp::new();

        // the synthetic drep is registered and active: it survives the
        // negative filters and disappears behind the positive ones
        let models = get_dreps_list(&app, "/governance/dreps?retired=false&expired=false").await;
        assert_eq!(models.len(), 1);

        let models = get_dreps_list(&app, "/governance/dreps?retired=true").await;
        assert!(models.is_empty());

        let models = get_dreps_list(&app, "/governance/dreps?expired=true").await;
        assert!(models.is_empty());
    }

    #[tokio::test]
    async fn governance_dreps_list_order_by_amount() {
        let app = TestApp::new();

        let models = get_dreps_list(&app, "/governance/dreps?order_by=amount").await;
        assert_eq!(models.len(), 1);

        let models = get_dreps_list(&app, "/governance/dreps?order_by=amount&order=desc").await;
        assert_eq!(models.len(), 1);
    }

    #[tokio::test]
    async fn governance_dreps_list_scan_limit() {
        let app = TestApp::new();

        // page * count above `max_scan_items` (default 3000)
        assert_status(
            &app,
            "/governance/dreps?page=1000&count=100",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn governance_dreps_list_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));

        assert_status(&app, "/governance/dreps", StatusCode::INTERNAL_SERVER_ERROR).await;
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

    /// The three withdrawals of preview's `0e58f693…6590#0`, each as the raw
    /// reward account the chain carries, the amount, and the bech32 address
    /// Blockfrost returns for it. Listed in the order the API returns them.
    fn withdrawal_vectors() -> Vec<(Vec<u8>, u64, &'static str)> {
        vec![
            (
                hex::decode("e0788cf0519348fefaf3c721c5f5bd60b195b444fa0d8fb4512dc259be").unwrap(),
                2000,
                "stake_test1upugeuz3jdy0a7hncusutadavzcetdzylgxcldz39hp9n0s0xy0n5",
            ),
            (
                hex::decode("e0ba149e2e2379097e65f0c03f2733d3103151e7f100d36dfdb01a0b22").unwrap(),
                1000,
                "stake_test1uzapf83wydusjln97rqr7fen6vgrz5087yqdxm0akqdqkgstjz8g4",
            ),
            (
                hex::decode("e0f631370cc87882bf5e14ab72534caf2655d0a2a50a9a8a3820bb6f4a").unwrap(),
                3000,
                "stake_test1urmrzdcvepug9067zj4hy56v4un9t59z559f4z3cyzak7js3z5t2t",
            ),
        ]
    }

    fn expected_withdrawals() -> Vec<(String, String)> {
        withdrawal_vectors()
            .into_iter()
            .map(|(_, amount, address)| (address.to_string(), amount.to_string()))
            .collect()
    }

    /// One tx proposing a treasury withdrawal at index 0 and an info action at
    /// index 1, so the same fixture covers a proposal that pays out and one
    /// that cannot.
    fn withdrawal_app() -> TestApp {
        let withdrawals: BTreeMap<Bytes, u64> = withdrawal_vectors()
            .into_iter()
            .map(|(account, amount, _)| (Bytes::from(account), amount))
            .collect();

        TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![
                GovAction::TreasuryWithdrawals(withdrawals, None),
                GovAction::Information,
            ]]],
            ..Default::default()
        })
    }

    async fn get_withdrawals(app: &TestApp, path: &str) -> Vec<(String, String)> {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let rows: Vec<ProposalWithdrawalsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse withdrawals");

        rows.into_iter()
            .map(|x| (x.stake_address, x.amount))
            .collect()
    }

    #[tokio::test]
    async fn governance_proposal_withdrawals_happy_path() {
        let app = withdrawal_app();
        let tx = tx_hash_of_block(&app, 0);
        let path = format!("/governance/proposals/{tx}/0/withdrawals");

        assert_eq!(get_withdrawals(&app, &path).await, expected_withdrawals());
    }

    #[tokio::test]
    async fn governance_proposal_withdrawals_orders_and_paginates() {
        let app = withdrawal_app();
        let tx = tx_hash_of_block(&app, 0);
        let base = format!("/governance/proposals/{tx}/0/withdrawals");
        let expected = expected_withdrawals();

        let desc = get_withdrawals(&app, &format!("{base}?order=desc")).await;
        assert_eq!(
            desc,
            expected.iter().rev().cloned().collect_vec(),
            "desc is the ascending listing read backwards"
        );

        let page = get_withdrawals(&app, &format!("{base}?count=2&page=2")).await;
        assert_eq!(page, expected[2..].to_vec());

        let page = get_withdrawals(&app, &format!("{base}?count=1&page=3&order=desc")).await;
        assert_eq!(page, expected[..1].to_vec());

        // a page past the end is empty, not an error
        let page = get_withdrawals(&app, &format!("{base}?page=5")).await;
        assert!(page.is_empty());
    }

    /// Blockfrost joins the proposal against db-sync's withdrawal table and
    /// sends whatever comes back, so everything that names no withdrawal —
    /// another action type, an unknown proposal, an unreadable hash — is the
    /// same empty listing rather than a 404.
    #[tokio::test]
    async fn governance_proposal_withdrawals_without_rows() {
        let app = withdrawal_app();
        let tx = tx_hash_of_block(&app, 0);

        // index 1 of the same tx is the info action
        let path = format!("/governance/proposals/{tx}/1/withdrawals");
        assert!(get_withdrawals(&app, &path).await.is_empty());

        // an index the tx never proposed at
        let path = format!("/governance/proposals/{tx}/9/withdrawals");
        assert!(get_withdrawals(&app, &path).await.is_empty());

        let missing = hex::encode([0u8; 32]);
        let path = format!("/governance/proposals/{missing}/0/withdrawals");
        assert!(get_withdrawals(&app, &path).await.is_empty());

        let path = "/governance/proposals/not-a-tx-hash/0/withdrawals";
        assert!(get_withdrawals(&app, path).await.is_empty());
    }

    #[tokio::test]
    async fn governance_proposal_withdrawals_by_gov_action_id() {
        let app = withdrawal_app();
        let tx: Hash<32> = tx_hash_of_block(&app, 0)
            .parse()
            .expect("failed to parse tx hash");

        let id = bech32_gov_action(&tx, 0).unwrap();
        let path = format!("/governance/proposals/{id}/withdrawals");
        assert_eq!(get_withdrawals(&app, &path).await, expected_withdrawals());

        // the same listing, paginated the same way
        let page = get_withdrawals(&app, &format!("{path}?order=desc&count=1")).await;
        assert_eq!(page, expected_withdrawals()[2..].to_vec());

        // the bare-hash form explorers write for index 0 names the same
        // proposal as the one-byte form Blockfrost writes
        let minimal = bech32(bech32::Hrp::parse("gov_action").unwrap(), tx.as_slice()).unwrap();
        let path = format!("/governance/proposals/{minimal}/withdrawals");
        assert_eq!(get_withdrawals(&app, &path).await, expected_withdrawals());

        // a well-formed id for a proposal nobody made
        let id = bech32_gov_action(&Hash::from([0u8; 32]), 0).unwrap();
        let path = format!("/governance/proposals/{id}/withdrawals");
        assert!(get_withdrawals(&app, &path).await.is_empty());
    }

    #[tokio::test]
    async fn governance_proposal_withdrawals_bad_request() {
        let app = withdrawal_app();
        let tx = tx_hash_of_block(&app, 0);
        let base = format!("/governance/proposals/{tx}/0/withdrawals");

        assert_status(&app, &format!("{base}?count=0"), StatusCode::BAD_REQUEST).await;
        assert_status(
            &app,
            &format!("{base}?order=sideways"),
            StatusCode::BAD_REQUEST,
        )
        .await;

        // the cert index is the only path part that has to be a number
        let path = format!("/governance/proposals/{tx}/x/withdrawals");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;

        for id in ["not-bech32", &missing_drep()] {
            let path = format!("/governance/proposals/{id}/withdrawals");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn governance_proposal_withdrawals_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = format!(
            "/governance/proposals/{}/0/withdrawals",
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

        // Blockfrost parses the whole suffix, so a zero-padded index is an
        // alias of the canonical spelling and resolves to the same proposal
        let padded = bech32(
            bech32::Hrp::parse("gov_action").unwrap(),
            [tx.as_slice(), &[0x00, 0x01]].concat(),
        )
        .unwrap();
        assert_eq!(parse_gov_action_id(&padded).unwrap(), (tx, 1));

        let long_zeros = bech32(
            bech32::Hrp::parse("gov_action").unwrap(),
            [tx.as_slice(), &[0x00; 8]].concat(),
        )
        .unwrap();
        assert_eq!(parse_gov_action_id(&long_zeros).unwrap(), (tx, 0));

        // an index past u32 can never name a proposal: it parses and misses
        let huge = bech32(
            bech32::Hrp::parse("gov_action").unwrap(),
            [tx.as_slice(), &[0xff; 8]].concat(),
        )
        .unwrap();
        assert_eq!(parse_gov_action_id(&huge).unwrap(), (tx, u32::MAX));
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

        // the parser accepts every suffix length the encoder produces,
        // like the Blockfrost one does
        for idx in [0, 1, 255, 256, u32::MAX] {
            let id = bech32_gov_action(&tx, idx).unwrap();
            assert_eq!(parse_gov_action_id(&id).unwrap(), (tx, idx));
        }
    }

    fn proposal_tx() -> Hash<32> {
        [7u8; 32].into()
    }

    /// A proposal that expires after epoch 646 (`expires_at` = 647), with
    /// the boundary outcome stamps given by the caller.
    fn lifecycle_state(ratified: Option<Epoch>, canceled: Option<Epoch>) -> ProposalState {
        ProposalState {
            slot: 1,
            tx: proposal_tx(),
            idx: 0,
            action: ProposalAction::Info,
            max_epoch: Some(646),
            ratified_epoch: ratified,
            canceled_epoch: canceled,
            deposit: Some(100_000_000),
            reward_account: Some(StakeCredential::AddrKeyhash([7u8; 28].into())),
            proposed_in: Some(640),
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

    fn seed_proposal(domain: &ToyDomain) {
        let state = ProposalState {
            slot: 1,
            tx: proposal_tx(),
            idx: 0,
            action: ProposalAction::HardFork((11, 0)),
            max_epoch: Some(1_000),
            ratified_epoch: None,
            canceled_epoch: None,
            deposit: Some(100_000_000),
            reward_account: Some(StakeCredential::AddrKeyhash([7u8; 28].into())),
            proposed_in: Some(2),
            parent: Some(GovActionId {
                transaction_id: [9u8; 32].into(),
                action_index: 0,
            }),
            purpose: Some(GovPurpose::HardFork),
            anchor: None,
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        };

        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");
        writer
            .write_entity_typed(&state.key(), &state)
            .expect("failed to write proposal");
        writer.commit().expect("failed to commit proposal");
    }

    fn proposal_lookup_app() -> TestApp {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        TestApp::new_with_cfg_and_setup(cfg, |domain, _| seed_proposal(domain))
    }

    fn assert_proposal_body(body: &[u8]) {
        let model: Proposal = serde_json::from_slice(body).expect("failed to parse proposal");

        assert_eq!(model.tx_hash, hex::encode(proposal_tx()));
        assert_eq!(model.cert_index, 0);
        assert_eq!(
            model.id,
            bech32_gov_action(&proposal_tx(), 0).expect("failed to encode gov action id")
        );
        assert_eq!(
            model.governance_type,
            proposal::GovernanceType::HardForkInitiation
        );
        // The seeded state has no archived tx, so no description derives.
        assert_eq!(model.governance_description, None);
        assert_eq!(model.deposit, "100000000");
        assert!(model.return_address.starts_with("stake_test"));
        assert_eq!(model.ratified_epoch, None);
        assert_eq!(model.enacted_epoch, None);
        assert_eq!(model.expiration, 1_001);
    }

    #[tokio::test]
    async fn governance_proposal_happy_path() {
        let app = proposal_lookup_app();
        let path = format!("/governance/proposals/{}/0", hex::encode(proposal_tx()));
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        assert_proposal_body(&body);
    }

    #[tokio::test]
    async fn governance_proposal_hides_legacy_rows() {
        // A pre-Conway protocol update row carries no deposit and no reward
        // account. Blockfrost never serves those from this endpoint.
        let seed_legacy = |domain: &ToyDomain| {
            let state = ProposalState {
                slot: 1,
                tx: proposal_tx(),
                idx: 0,
                action: ProposalAction::Other,
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
            };

            let writer = domain
                .state()
                .start_writer()
                .expect("failed to start writer");
            writer
                .write_entity_typed(&state.key(), &state)
                .expect("failed to write proposal");
            writer.commit().expect("failed to commit proposal");
        };

        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, _| seed_legacy(domain));

        let path = format!("/governance/proposals/{}/0", hex::encode(proposal_tx()));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        let id = bech32_gov_action(&proposal_tx(), 0).expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_proposal_bad_request() {
        let app = TestApp::new();

        // Blockfrost treats a malformed hash as a miss, not a bad request
        let path = "/governance/proposals/not-a-tx-hash/0";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;

        let path = format!(
            "/governance/proposals/{}/not-a-number",
            hex::encode(proposal_tx())
        );
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_proposal_not_found() {
        let app = TestApp::new();
        let path = format!("/governance/proposals/{}/0", hex::encode([0xffu8; 32]));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_proposal_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = format!("/governance/proposals/{}/0", hex::encode(proposal_tx()));
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn governance_proposal_by_gov_action_id_happy_path() {
        let app = proposal_lookup_app();
        let id = bech32_gov_action(&proposal_tx(), 0).expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        assert_proposal_body(&body);
    }

    #[tokio::test]
    async fn governance_proposal_by_gov_action_id_minimal_encoding() {
        let app = proposal_lookup_app();
        // CIP-0129 minimal encoding: cert index 0 omits the suffix byte.
        let hrp = Hrp::parse_unchecked("gov_action");
        let id = bech32::encode::<Bech32>(hrp, proposal_tx().as_slice())
            .expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        assert_proposal_body(&body);
    }

    #[test]
    fn gov_action_id_matches_cip0129_test_vectors() {
        // Official test vectors from CIP-0129.
        let vectors = [
            (
                "gov_action1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqpzklpgpf",
                [0u8; 32],
                17u32,
            ),
            (
                "gov_action1zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zygsq6dmejn",
                [0x11u8; 32],
                0u32,
            ),
        ];

        for (id, tx, idx) in vectors {
            let (parsed_tx, parsed_idx) = parse_gov_action_id(id).expect("failed to parse vector");
            assert_eq!(parsed_tx, Hash::from(tx));
            assert_eq!(parsed_idx, idx);

            let encoded = bech32_gov_action(&tx.into(), idx).expect("failed to encode vector");
            assert_eq!(encoded, id);
        }
    }

    #[tokio::test]
    async fn governance_proposal_by_gov_action_id_bad_request() {
        let app = TestApp::new();
        let path = "/governance/proposals/not-a-gov-action-id";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;

        // A valid bech32 string with the wrong prefix must fail too.
        let hrp = Hrp::parse_unchecked("drep");
        let payload = [8u8; 33];
        let wrong_hrp =
            bech32::encode::<Bech32>(hrp, &payload).expect("failed to encode bech32 id");
        let path = format!("/governance/proposals/{wrong_hrp}");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_proposal_by_gov_action_id_not_found() {
        let app = TestApp::new();
        let hrp = Hrp::parse_unchecked("gov_action");
        let mut payload = [0xffu8; 33];
        payload[32] = 0;
        let id = bech32::encode::<Bech32>(hrp, &payload).expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_proposal_by_gov_action_id_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let id = bech32_gov_action(&proposal_tx(), 0).expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
