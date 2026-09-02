use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    proposal_withdrawals_inner::ProposalWithdrawalsInner,
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
        addresses::Network,
        primitives::{Coin, Epoch, StakeCredential},
        traverse::MultiEraBlock,
    },
};
use std::collections::HashMap;

use crate::{
    error::Error,
    mapping::{bech32, bech32_gov_action, parse_gov_action_id, stake_cred_to_address, IntoModel},
    pagination::{Order, Pagination, PaginationParameters},
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use bech32::{Bech32, Hrp};
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use itertools::Itertools;
    use pallas::{codec::utils::Bytes, ledger::primitives::conway::GovAction};
    use std::collections::BTreeMap;

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
