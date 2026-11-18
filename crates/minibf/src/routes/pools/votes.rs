//! `/pools/{pool_id}/votes`: the governance votes a pool cast.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::pool_votes_inner::PoolVotesInner;
use dolos_cardano::model::PoolState;
use dolos_core::Domain;
use pallas::ledger::primitives::conway::Voter;

use crate::{
    error::Error,
    mapping::{i32_or_500, pools::pool_vote_model},
    pagination::{Pagination, PaginationParameters},
    routes::governance::voter_casts,
    Facade,
};

use super::{parse_pool_id_bounded, pool_exists};

/// `GET /pools/{pool_id}/votes` returns the votes of a stake pool. By default,
/// the list starts with the oldest vote.
///
/// `tx_hash` is the hash of the transaction that contains the vote.
/// `cert_index` is the index of the vote in the ballot of the pool in that
/// transaction. A repeated vote on the same proposal is a separate row.
///
/// If the pool never registered, the endpoint returns 404. The endpoint does
/// not return 404 for a retired pool. If the pool registered but has no votes,
/// the endpoint returns an empty list.
///
/// `max_scan_items` limits both the page depth and the number of blocks that
/// one request reads.
///
/// The endpoint does the checks in the same order as Blockfrost. The order is
/// the query string, then the pool ID, then the registration of the pool.
/// Blockfrost has no scan limit. Thus, the endpoint does the scan limit check
/// after the Blockfrost checks.
pub async fn by_id_votes<D>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolVotesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let Some(pool) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    if !pool_exists(&domain, pool).await? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let voter = Voter::StakePoolKey(pool);
    let tip = domain.get_tip_slot()?;
    let budget = domain.config.max_scan_items() as usize;

    let casts = domain
        .query()
        .run_blocking(move |domain| Ok(voter_casts(&domain, &voter, tip, &pagination, budget)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    let out = casts
        .into_iter()
        .map(|cast| {
            Ok(PoolVotesInner {
                tx_hash: hex::encode(cast.tx),
                cert_index: i32_or_500(cast.cert_index)?,
                vote: pool_vote_model(&cast.vote),
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mapping::bech32_pool,
        routes::pools::testing::{
            assert_error_message, assert_status, invalid_pool_id, missing_pool_id,
            wrong_length_pool_ids,
        },
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::pool_votes_inner;
    use dolos_testing::synthetic::{SyntheticBlockConfig, SyntheticProposalRef, SyntheticVote};
    use itertools::Itertools as _;
    use pallas::{
        crypto::hash::Hash,
        ledger::primitives::conway::{GovAction, Vote},
    };

    const VOTING_POOL: [u8; 28] = [9u8; 28];

    fn synthetic_vote(
        voter: &Voter,
        block: usize,
        tx: usize,
        action: u32,
        vote: Vote,
    ) -> SyntheticVote {
        SyntheticVote {
            voter: voter.clone(),
            proposal: SyntheticProposalRef { block, tx, action },
            vote,
            anchor: None,
        }
    }

    /// Transaction 0 of block 0 proposes three actions. In block 1,
    /// transaction 0 contains a ballot of the pool on actions 2 and 0. The
    /// same transaction also contains the ballots of a DRep and of a second
    /// pool. In block 1, transaction 1 contains a ballot of the pool on
    /// action 1. In block 2, transaction 0 contains a new vote of the pool on
    /// action 0.
    ///
    /// Each ballot is a map that sorts its votes by governance action ID. As a
    /// result, in transaction 0 of block 1, the vote of the pool on action 0
    /// has `cert_index` 0. The vote of the pool on action 2 has `cert_index` 1.
    /// In that transaction, the ballot of the DRep is before the ballot of the
    /// pool. The index starts at 0 for each voter. Thus, the ballot of the
    /// DRep does not change the index.
    ///
    /// Transaction 0 is before transaction 1. Thus, in chain order, the vote
    /// of the pool on action 2 is before the vote of the pool on action 1.
    fn pool_votes_config() -> SyntheticBlockConfig {
        let pool = Voter::StakePoolKey(Hash::from(VOTING_POOL));
        let drep = Voter::DRepKey(Hash::from([7u8; 28]));
        let other_pool = Voter::StakePoolKey(Hash::from([5u8; 28]));

        SyntheticBlockConfig {
            pool_id: bech32_pool(VOTING_POOL).expect("Cannot encode the pool ID."),
            block_count: 3,
            txs_per_block: 2,
            gov_actions_by_block: vec![
                vec![vec![
                    GovAction::Information,
                    GovAction::Information,
                    GovAction::Information,
                ]],
                vec![],
                vec![],
            ],
            votes_by_block: vec![
                vec![],
                vec![
                    vec![
                        synthetic_vote(&drep, 0, 0, 0, Vote::No),
                        synthetic_vote(&pool, 0, 0, 2, Vote::Abstain),
                        synthetic_vote(&pool, 0, 0, 0, Vote::Yes),
                        synthetic_vote(&other_pool, 0, 0, 1, Vote::No),
                    ],
                    vec![synthetic_vote(&pool, 0, 0, 1, Vote::No)],
                ],
                vec![vec![synthetic_vote(&pool, 0, 0, 0, Vote::No)]],
            ],
            ..Default::default()
        }
    }

    async fn get_pool_votes(app: &TestApp, pool_id: &str, query: &str) -> Vec<PoolVotesInner> {
        let path = format!("/pools/{pool_id}/votes{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "The request to {path} returned status {status}. The response body was {}.",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("The pool vote response did not contain valid JSON.")
    }

    fn pool_vote_row(
        tx_hash: &str,
        cert_index: i32,
        vote: pool_votes_inner::Vote,
    ) -> PoolVotesInner {
        PoolVotesInner {
            tx_hash: tx_hash.to_string(),
            cert_index,
            vote,
        }
    }

    fn expected_pool_votes(app: &TestApp) -> Vec<PoolVotesInner> {
        let blocks = &app.vectors().blocks;

        vec![
            pool_vote_row(&blocks[1].tx_hashes[0], 0, pool_votes_inner::Vote::Yes),
            pool_vote_row(&blocks[1].tx_hashes[0], 1, pool_votes_inner::Vote::Abstain),
            pool_vote_row(&blocks[1].tx_hashes[1], 0, pool_votes_inner::Vote::No),
            pool_vote_row(&blocks[2].tx_hashes[0], 0, pool_votes_inner::Vote::No),
        ]
    }

    #[tokio::test]
    async fn pools_votes_happy_path() {
        let app = TestApp::new_with_cfg(pool_votes_config());
        let expected = expected_pool_votes(&app);

        let rows = get_pool_votes(&app, &app.vectors().pool_id, "").await;
        assert_eq!(rows, expected);

        let rows = get_pool_votes(&app, &hex::encode(VOTING_POOL), "").await;
        assert_eq!(rows, expected);
    }

    #[tokio::test]
    async fn pools_votes_orders_and_paginates() {
        let app = TestApp::new_with_cfg(pool_votes_config());
        let pool_id = app.vectors().pool_id.clone();
        let expected = expected_pool_votes(&app);
        let reversed = expected.iter().rev().cloned().collect_vec();

        assert_eq!(
            get_pool_votes(&app, &pool_id, "?order=desc").await,
            reversed
        );
        assert_eq!(
            get_pool_votes(&app, &pool_id, "?order=desc&count=3").await,
            reversed[..3]
        );
        assert_eq!(
            get_pool_votes(&app, &pool_id, "?count=2").await,
            expected[..2]
        );

        // If `count` is 2, page 1 contains two of the three votes of the pool
        // in block 1. The first vote on page 2 is the third of these votes.
        assert_eq!(
            get_pool_votes(&app, &pool_id, "?count=2&page=2").await,
            expected[2..]
        );
        assert_eq!(
            get_pool_votes(&app, &pool_id, "?count=1&page=2").await,
            expected[1..2]
        );
        assert!(get_pool_votes(&app, &pool_id, "?page=2").await.is_empty());
    }

    #[tokio::test]
    async fn pools_votes_without_votes() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.clone();

        assert!(get_pool_votes(&app, &pool_id, "").await.is_empty());
        assert!(get_pool_votes(&app, &pool_id, "?order=desc")
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn pools_votes_bad_request() {
        let app = TestApp::new_with_cfg(pool_votes_config());
        let path = format!("/pools/{}/votes", invalid_pool_id());
        assert_error_message(&app, &path, "Invalid or malformed pool id format.").await;

        // The endpoint does the query string check before the pool ID check.
        let path = format!("/pools/{}/votes?count=0", invalid_pool_id());
        assert_error_message(&app, &path, "querystring/count must be >= 1").await;

        let path = format!("/pools/{}/votes?order=sideways", missing_pool_id());
        assert_error_message(
            &app,
            &path,
            "querystring/order must be equal to one of the allowed values",
        )
        .await;
    }

    #[tokio::test]
    async fn pools_votes_not_found() {
        let app = TestApp::new_with_cfg(pool_votes_config());
        let path = format!("/pools/{}/votes", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        // Block 1 contains a vote of the second pool, but the second pool
        // never registered.
        let path = format!("/pools/{}/votes", hex::encode([5u8; 28]));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        for pool_id in wrong_length_pool_ids(&app) {
            let path = format!("/pools/{pool_id}/votes");
            assert_status(&app, &path, StatusCode::NOT_FOUND).await;
        }
    }

    #[tokio::test]
    async fn pools_votes_rejects_deep_page() {
        let app = TestApp::new_with_scan_limit(pool_votes_config(), 3);
        let path = format!("/pools/{}/votes?count=2&page=2", app.vectors().pool_id);
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn pools_votes_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/votes");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
