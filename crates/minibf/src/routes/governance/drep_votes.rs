use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::drep_votes_inner::{self, DrepVotesInner};
use dolos_cardano::{indexes::CardanoArchiveIndexExt as _, pallas_extras};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::conway::{DRep, Vote, Voter},
        traverse::{MultiEraBlock, MultiEraTx},
    },
};

use crate::{
    error::Error,
    mapping::{bech32_gov_action, governance::dreps::parse_drep_id, i32_or_500},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

use super::{collect_block_rows, drep_key};

/// A vote and the transaction that contains the vote.
pub(crate) struct CastVote {
    /// The hash of the transaction that contains the vote.
    pub tx: Hash<32>,

    /// The index of the vote in the ballot of the voter in `tx`.
    pub cert_index: u32,

    pub proposal_tx: Hash<32>,
    pub proposal_idx: u32,
    pub vote: Vote,
}

/// This function converts a DRep ID to the voter key of its ballots.
///
/// `parse_drep_id` builds the bytes of a `DRepKey`, which decodes them. The
/// CIP-129 header rule stays in `dolos-cardano`, next to the writer.
fn drep_voter(drep_bytes: &[u8]) -> Result<Voter, StatusCode> {
    match drep_key(drep_bytes)?.0 {
        DRep::Key(hash) => Ok(Voter::DRepKey(hash)),
        DRep::Script(hash) => Ok(Voter::DRepScript(hash)),
        DRep::Abstain | DRep::NoConfidence => Err(StatusCode::BAD_REQUEST),
    }
}

/// This function converts a ledger vote to a Blockfrost value.
fn vote_model(vote: &Vote) -> drep_votes_inner::Vote {
    match vote {
        Vote::Yes => drep_votes_inner::Vote::Yes,
        Vote::No => drep_votes_inner::Vote::No,
        Vote::Abstain => drep_votes_inner::Vote::Abstain,
    }
}

/// This function returns the votes of one voter in one block, in Blockfrost
/// order: the position of the transaction first, then the index of the vote in
/// the ballot of the voter. A repeated vote on the same proposal is a separate
/// row.
///
/// `cert_index` is the index in one voter ballot. db-sync restarts this value
/// for each voter. The ledger stores each ballot in a map that uses governance
/// action IDs as keys. Thus, the order uses the proposal transaction hash
/// first and the action index second.
///
/// The ledger ignores the votes of a phase-2-invalid transaction, so the
/// function skips those transactions.
fn voter_casts_in_block(block: &MultiEraBlock, voter: &Voter) -> Vec<CastVote> {
    let mut out = Vec::new();

    for tx in block.txs() {
        if !tx.is_valid() {
            continue;
        }

        let MultiEraTx::Conway(conway) = &tx else {
            continue;
        };

        let Some(ballot) = conway
            .transaction_body
            .voting_procedures
            .as_ref()
            .and_then(|procedures| procedures.get(voter))
        else {
            continue;
        };

        for (cert_index, (action, procedure)) in ballot.iter().enumerate() {
            out.push(CastVote {
                tx: tx.hash(),
                cert_index: cert_index as u32,
                proposal_tx: action.transaction_id,
                proposal_idx: action.action_index,
                vote: procedure.vote.clone(),
            });
        }
    }

    out
}

/// This function reads the vote blocks of one voter from the archive, in the
/// requested order, and returns the votes on the requested page. The votes
/// are in chain order, or in the opposite order for `Order::Desc`. Chain order
/// is the order of the blocks, then the position of the transaction in the
/// block, then the ballot index.
///
/// The `voter_votes` archive dimension gives the blocks that hold votes of the
/// voter. The function reads blocks only until the page is full, and at most
/// `budget` of them. Votes are listed up to `tip`, the state cursor.
///
/// A block that the archive no longer holds leaves the list, and the votes
/// after it move up. The other history endpoints do the same under
/// rolling history.
pub(crate) fn voter_casts<D: Domain>(
    domain: &D,
    voter: &Voter,
    tip: BlockSlot,
    pagination: &Pagination,
    mut budget: usize,
) -> Result<Vec<CastVote>, Error> {
    let archive = domain.archive();

    let slots = archive
        .slots_by_voter_votes(&pallas_extras::voter_id_bytes(voter), 0, tip)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let descending = matches!(pagination.order, Order::Desc);

    let slots: Box<dyn Iterator<Item = _>> = if descending {
        Box::new(slots.rev())
    } else {
        Box::new(slots)
    };

    let rows = collect_block_rows(
        slots,
        |slot| archive.get_block_by_slot(&slot),
        descending,
        pagination.to(),
        &mut budget,
        |block| Ok(voter_casts_in_block(block, voter)),
    )?;

    Ok(rows
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .collect())
}

fn vote_page<D: Domain>(
    domain: &D,
    voter: &Voter,
    tip: BlockSlot,
    pagination: &Pagination,
    budget: usize,
) -> Result<Vec<DrepVotesInner>, Error> {
    voter_casts(domain, voter, tip, pagination, budget)?
        .into_iter()
        .map(|cast| {
            Ok(DrepVotesInner {
                tx_hash: hex::encode(cast.tx),
                cert_index: i32_or_500(cast.cert_index)?,
                proposal_id: bech32_gov_action(&cast.proposal_tx, cast.proposal_idx)?,
                proposal_tx_hash: hex::encode(cast.proposal_tx),
                proposal_cert_index: i32_or_500(cast.proposal_idx)?,
                vote: vote_model(&cast.vote),
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()
        .map_err(Error::from)
}

/// `GET /governance/dreps/{drep_id}/votes` lists all votes from a DRep in
/// oldest-first order.
///
/// If a DRep does not exist or has no votes, the endpoint returns an empty
/// list. It does not return 404.
///
/// `max_scan_items` limits both the page depth and the number of blocks that
/// one request reads.
pub async fn drep_votes<D>(
    Path(drep_id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<DrepVotesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let parsed = parse_drep_id(&drep_id)?;

    // The two special DReps are delegation targets, not voters. They cannot
    // cast votes.
    if parsed.is_special {
        return Ok(Json(Vec::new()));
    }

    let voter = drep_voter(&parsed.encoded)?;
    let tip = domain.get_tip_slot()?;
    let budget = domain.config.max_scan_items() as usize;

    let page = domain
        .query()
        .run_blocking(move |domain| Ok(vote_page(&domain, &voter, tip, &pagination, budget)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(Json(page))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mapping::bech32,
        routes::governance::testing::*,
        test_support::{TestApp, TestFault},
    };
    use bech32::Hrp;
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use itertools::Itertools;
    use pallas::ledger::primitives::conway::GovAction;

    /// Transaction 0 in block 0 proposes two actions. Transaction 1 in block 0
    /// proposes one action. Two transactions in block 1 cast votes on all
    /// three actions. Block 2 changes the first vote. A script DRep also votes
    /// in block 1. Thus, one chain covers both CIP-129 credential variants.
    fn drep_votes_config() -> SyntheticBlockConfig {
        let key_voter = Voter::DRepKey(Hash::from([7u8; 28]));
        let script_voter = Voter::DRepScript(Hash::from([8u8; 28]));

        SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 2,
            gov_actions_by_block: vec![
                vec![
                    vec![GovAction::Information, GovAction::Information],
                    vec![GovAction::Information],
                ],
                vec![],
                vec![],
            ],
            votes_by_block: vec![
                vec![],
                vec![
                    vec![
                        // The transaction map sorts these votes by action.
                        synthetic_vote(key_voter.clone(), 0, 0, 1, Vote::No),
                        synthetic_vote(key_voter.clone(), 0, 0, 0, Vote::Yes),
                        synthetic_vote(script_voter, 0, 0, 1, Vote::Abstain),
                    ],
                    vec![synthetic_vote(key_voter.clone(), 0, 1, 0, Vote::Abstain)],
                ],
                vec![vec![synthetic_vote(key_voter, 0, 0, 0, Vote::No)]],
            ],
            ..Default::default()
        }
    }

    fn drep_votes_app() -> TestApp {
        TestApp::new_with_cfg(drep_votes_config())
    }

    async fn get_drep_votes(app: &TestApp, drep: &str, query: &str) -> Vec<DrepVotesInner> {
        let path = format!("/governance/dreps/{drep}/votes{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "The request to {path} returned status {status}. The response body was {}.",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("The DRep vote response did not contain valid JSON.")
    }

    #[tokio::test]
    async fn governance_drep_votes_happy_path() {
        let app = drep_votes_app();
        let rows = get_drep_votes(&app, &app.vectors().drep_id, "").await;
        let blocks = &app.vectors().blocks;

        assert_eq!(rows.len(), 4);

        assert_eq!(rows[0].tx_hash, blocks[1].tx_hashes[0]);
        assert_eq!(rows[0].cert_index, 0);
        assert_eq!(rows[0].proposal_tx_hash, blocks[0].tx_hashes[0]);
        assert_eq!(rows[0].proposal_cert_index, 0);
        assert_eq!(rows[0].vote, drep_votes_inner::Vote::Yes);

        assert_eq!(rows[1].tx_hash, blocks[1].tx_hashes[0]);
        assert_eq!(rows[1].cert_index, 1);
        assert_eq!(rows[1].proposal_tx_hash, blocks[0].tx_hashes[0]);
        assert_eq!(rows[1].proposal_cert_index, 1);
        assert_eq!(rows[1].vote, drep_votes_inner::Vote::No);

        assert_eq!(rows[2].tx_hash, blocks[1].tx_hashes[1]);
        assert_eq!(rows[2].cert_index, 0);
        assert_eq!(rows[2].proposal_tx_hash, blocks[0].tx_hashes[1]);
        assert_eq!(rows[2].proposal_cert_index, 0);
        assert_eq!(rows[2].vote, drep_votes_inner::Vote::Abstain);

        assert_eq!(rows[3].tx_hash, blocks[2].tx_hashes[0]);
        assert_eq!(rows[3].cert_index, 0);
        assert_eq!(rows[3].proposal_tx_hash, blocks[0].tx_hashes[0]);
        assert_eq!(rows[3].proposal_cert_index, 0);
        assert_eq!(rows[3].vote, drep_votes_inner::Vote::No);

        for row in rows {
            let proposal_tx: Hash<32> = row.proposal_tx_hash.parse().unwrap();
            assert_eq!(
                row.proposal_id,
                bech32_gov_action(&proposal_tx, row.proposal_cert_index as u32).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn governance_drep_votes_for_same_block_proposal() {
        let voter = Voter::DRepKey(Hash::from([7u8; 28]));
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 2,
            gov_actions_by_block: vec![vec![vec![GovAction::Information], vec![]]],
            votes_by_block: vec![vec![
                vec![],
                vec![synthetic_vote(voter, 0, 0, 0, Vote::Yes)],
            ]],
            ..Default::default()
        });

        let rows = get_drep_votes(&app, &app.vectors().drep_id, "").await;
        let block = &app.vectors().blocks[0];

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_hash, block.tx_hashes[1]);
        assert_eq!(rows[0].cert_index, 0);
        assert_eq!(rows[0].proposal_tx_hash, block.tx_hashes[0]);
        assert_eq!(rows[0].proposal_cert_index, 0);
        assert_eq!(rows[0].vote, drep_votes_inner::Vote::Yes);
    }

    #[tokio::test]
    async fn governance_drep_votes_orders_and_paginates() {
        let app = drep_votes_app();
        let drep = &app.vectors().drep_id;
        let ascending = get_drep_votes(&app, drep, "").await;
        let descending = get_drep_votes(&app, drep, "?order=desc").await;

        assert_eq!(
            descending
                .iter()
                .map(|row| (&row.tx_hash, row.cert_index, &row.proposal_id))
                .collect_vec(),
            ascending
                .iter()
                .rev()
                .map(|row| (&row.tx_hash, row.cert_index, &row.proposal_id))
                .collect_vec()
        );

        let page = get_drep_votes(&app, drep, "?count=2&page=2").await;
        assert_eq!(
            page.iter().map(|row| &row.proposal_id).collect_vec(),
            ascending[2..]
                .iter()
                .map(|row| &row.proposal_id)
                .collect_vec()
        );

        assert!(get_drep_votes(&app, drep, "?page=9").await.is_empty());
    }

    /// A pruned block takes its votes out of the listing, and the retained
    /// votes move up to fill the pages. The other history endpoints return
    /// the same under rolling history.
    ///
    /// The test prunes the archive to one slot. Only the last block remains.
    /// The three votes of block 1 are gone. The one vote of block 2 is the
    /// first row.
    #[tokio::test]
    async fn governance_drep_votes_paginates_retained_rows() {
        let app = TestApp::new_with_cfg_and_setup(drep_votes_config(), |domain, _| {
            domain
                .archive()
                .prune_history(0, None, None)
                .expect("The archive did not prune its history.");
        });
        let drep = &app.vectors().drep_id;

        let rows = get_drep_votes(&app, drep, "?count=1").await;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_hash, app.vectors().blocks[2].tx_hashes[0]);
        assert_eq!(rows[0].vote, drep_votes_inner::Vote::No);

        assert!(get_drep_votes(&app, drep, "?count=1&page=2")
            .await
            .is_empty());
        assert!(get_drep_votes(&app, drep, "?count=1&page=4")
            .await
            .is_empty());
    }

    /// The ledger ignores the votes of a phase-2-invalid tx, so they are not
    /// listed, although their block is tagged for the voter.
    #[tokio::test]
    async fn governance_drep_votes_skip_invalid_transactions() {
        let voter = Voter::DRepKey(Hash::from([7u8; 28]));
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 2,
            txs_per_block: 2,
            gov_actions_by_block: vec![vec![vec![GovAction::Information], vec![]], vec![]],
            votes_by_block: vec![
                vec![],
                vec![
                    vec![synthetic_vote(voter.clone(), 0, 0, 0, Vote::Yes)],
                    vec![synthetic_vote(voter, 0, 0, 0, Vote::No)],
                ],
            ],
            invalid_txs_by_block: vec![vec![], vec![1]],
            ..Default::default()
        });

        let rows = get_drep_votes(&app, &app.vectors().drep_id, "").await;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_hash, app.vectors().blocks[1].tx_hashes[0]);
        assert_eq!(rows[0].vote, drep_votes_inner::Vote::Yes);
    }

    /// One DRep votes two times on one proposal in one block. The cast queue
    /// for each proposal exists for this case. Both rows have the same slot
    /// and the same proposal. Only the position of the vote transaction
    /// separates them.
    #[tokio::test]
    async fn governance_drep_votes_pairs_repeated_votes_within_one_block() {
        let voter = Voter::DRepKey(Hash::from([7u8; 28]));
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 2,
            txs_per_block: 2,
            gov_actions_by_block: vec![vec![vec![GovAction::Information], vec![]], vec![]],
            votes_by_block: vec![
                vec![],
                vec![
                    vec![synthetic_vote(voter.clone(), 0, 0, 0, Vote::Yes)],
                    vec![synthetic_vote(voter, 0, 0, 0, Vote::No)],
                ],
            ],
            ..Default::default()
        });

        let rows = get_drep_votes(&app, &app.vectors().drep_id, "").await;
        let votes = &app.vectors().blocks[1];

        assert_eq!(rows.len(), 2);

        // The first transaction of the block gives the first row.
        assert_eq!(rows[0].tx_hash, votes.tx_hashes[0]);
        assert_eq!(rows[0].cert_index, 0);
        assert_eq!(rows[0].vote, drep_votes_inner::Vote::Yes);

        assert_eq!(rows[1].tx_hash, votes.tx_hashes[1]);
        assert_eq!(rows[1].cert_index, 0);
        assert_eq!(rows[1].vote, drep_votes_inner::Vote::No);

        // Both rows name the same proposal. Only the transaction hash orders
        // them.
        assert_eq!(rows[0].proposal_id, rows[1].proposal_id);
    }

    #[tokio::test]
    async fn governance_drep_votes_rejects_deep_page() {
        let app = TestApp::new_with_scan_limit(drep_votes_config(), 3);
        let path = format!(
            "/governance/dreps/{}/votes?count=2&page=2",
            app.vectors().drep_id
        );

        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_script_drep_votes() {
        let app = drep_votes_app();
        let drep = bech32(
            Hrp::parse("drep").unwrap(),
            [vec![pallas_extras::DREP_SCRIPT_PREFIX], vec![8u8; 28]].concat(),
        )
        .unwrap();
        let rows = get_drep_votes(&app, &drep, "").await;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cert_index, 0);
        assert_eq!(rows[0].proposal_cert_index, 1);
        assert_eq!(rows[0].vote, drep_votes_inner::Vote::Abstain);
    }

    #[tokio::test]
    async fn governance_drep_votes_without_rows() {
        let app = drep_votes_app();

        assert!(get_drep_votes(&app, &missing_drep(), "").await.is_empty());
        assert!(get_drep_votes(&app, "drep_always_abstain", "")
            .await
            .is_empty());
        assert!(get_drep_votes(&app, "drep_always_no_confidence", "")
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn governance_drep_votes_bad_request() {
        let app = drep_votes_app();
        let base = format!("/governance/dreps/{}/votes", app.vectors().drep_id);

        assert_status(&app, &format!("{base}?count=0"), StatusCode::BAD_REQUEST).await;
        assert_status(
            &app,
            &format!("{base}?order=sideways"),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/dreps/not-a-drep/votes",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn governance_drep_votes_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = format!("/governance/dreps/{}/votes", app.vectors().drep_id);
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
