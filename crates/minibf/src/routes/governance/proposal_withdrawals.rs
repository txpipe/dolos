use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::proposal_withdrawals_inner::ProposalWithdrawalsInner;
use dolos_cardano::model::{ProposalAction, ProposalKey, ProposalState};
use dolos_core::{Domain, StateStore as _};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::Network,
        primitives::{Coin, StakeCredential},
    },
};

use crate::{
    error::Error,
    mapping::{parse_gov_action_id, stake_cred_to_address},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

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
    let key = ProposalKey::new(tx, idx);

    let state = domain
        .state()
        .read_entity_typed::<ProposalState>(&key)
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
    use crate::{
        mapping::{bech32, bech32_gov_action},
        routes::governance::testing::*,
        test_support::{TestApp, TestFault},
    };
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use itertools::Itertools;
    use pallas::{codec::utils::Bytes, ledger::primitives::conway::GovAction};
    use std::collections::BTreeMap;

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
}
