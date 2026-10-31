use std::collections::{BTreeSet, HashMap};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::committee_votes_inner::{self, CommitteeVotesInner};
use dolos_cardano::{
    indexes::CardanoArchiveIndexExt as _,
    model::{ProposalAction, ProposalKey},
    pallas_extras,
};
use dolos_core::{ArchiveError, ArchiveStore as _, BlockSlot, Domain};
use itertools::Itertools;
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::{
            conway::{Anchor, Vote, Voter},
            StakeCredential,
        },
        traverse::{MultiEraBlock, MultiEraTx},
    },
};

use crate::{
    error::Error,
    log_and_500,
    mapping::{
        bech32_committee_hot, bech32_gov_action, i32_or_500, parse_committee_id,
        CommitteeCredentialRole,
    },
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

use super::{collect_block_rows, read_proposal_row};

/// One vote of a committee member, as the archived block records it.
struct CommitteeCast {
    /// The hash of the transaction that contains the vote.
    tx: Hash<32>,

    /// The hot credential that cast the vote.
    voter: StakeCredential,

    proposal_tx: Hash<32>,
    proposal_idx: u32,
    governance_type: committee_votes_inner::GovernanceType,
    vote: Vote,
    anchor: Option<Anchor>,
    block_height: u64,
    slot: BlockSlot,
}

fn committee_governance_type(action: &ProposalAction) -> committee_votes_inner::GovernanceType {
    use committee_votes_inner::GovernanceType;

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

fn committee_vote_model(vote: &Vote) -> committee_votes_inner::Vote {
    match vote {
        Vote::Yes => committee_votes_inner::Vote::Yes,
        Vote::No => committee_votes_inner::Vote::No,
        Vote::Abstain => committee_votes_inner::Vote::Abstain,
    }
}

fn committee_voter(cred: &StakeCredential) -> Voter {
    match cred {
        StakeCredential::AddrKeyhash(hash) => Voter::ConstitutionalCommitteeKey(*hash),
        StakeCredential::ScriptHash(hash) => Voter::ConstitutionalCommitteeScript(*hash),
    }
}

/// The governance action type of each proposal that a request has looked
/// up. A request reads each `ProposalState` row once, however many votes it
/// lists on the same action.
///
/// A proposal row lives in state until the boundary after its resolution, and
/// in the final-row log from then on, so a vote on an action always finds its
/// row. The legacy `Other` variant has no Blockfrost type, so a vote on such a
/// row has no type (`None`).
struct ProposalTypes<'a, D: Domain> {
    domain: &'a D,
    types: HashMap<(Hash<32>, u32), Option<committee_votes_inner::GovernanceType>>,
}

impl<'a, D: Domain> ProposalTypes<'a, D> {
    fn new(domain: &'a D) -> Self {
        Self {
            domain,
            types: HashMap::new(),
        }
    }

    fn get(
        &mut self,
        tx: Hash<32>,
        idx: u32,
    ) -> Result<Option<committee_votes_inner::GovernanceType>, StatusCode> {
        if let Some(governance_type) = self.types.get(&(tx, idx)) {
            return Ok(*governance_type);
        }

        let key = ProposalKey::new(tx, idx);

        let row = read_proposal_row(self.domain, &key)?;

        let governance_type = match row {
            Some(row) => Some(committee_governance_type(&row.state().action)),
            None => {
                // The ledger rejects a vote on an unknown action, so a block
                // that holds the vote also holds, or follows, the proposal.
                // If neither the state nor the log holds the row, the stores
                // do not agree.
                tracing::warn!(
                    proposal_tx = %tx,
                    proposal_idx = idx,
                    "no proposal row for a committee vote, so the vote leaves the listing"
                );
                None
            }
        };

        self.types.insert((tx, idx), governance_type);

        Ok(governance_type)
    }
}

/// This function returns the committee votes of `voters` in one block, in
/// Blockfrost order: the position of the transaction first, then the ballot
/// map of the transaction. The map sorts the ballots by voter and the votes
/// in a ballot by governance action ID. A committee script sorts before a
/// committee key.
///
/// The ledger ignores the votes of a phase-2-invalid transaction, so the
/// function skips those transactions. A vote without a governance action type
/// also leaves the listing. Both filters run before pagination, so the
/// offsets are the same on every page.
fn committee_casts_in_block<D: Domain>(
    block: &MultiEraBlock,
    voters: Option<&BTreeSet<Voter>>,
    types: &mut ProposalTypes<'_, D>,
) -> Result<Vec<CommitteeCast>, StatusCode> {
    let mut out = Vec::new();

    for tx in block.txs() {
        if !tx.is_valid() {
            continue;
        }

        let MultiEraTx::Conway(conway) = &tx else {
            continue;
        };

        let Some(procedures) = conway.transaction_body.voting_procedures.as_ref() else {
            continue;
        };

        for (voter, ballot) in procedures {
            if voters.is_some_and(|voters| !voters.contains(voter)) {
                continue;
            }

            let cred = match voter {
                Voter::ConstitutionalCommitteeKey(hash) => StakeCredential::AddrKeyhash(*hash),
                Voter::ConstitutionalCommitteeScript(hash) => StakeCredential::ScriptHash(*hash),
                _ => continue,
            };

            for (action, procedure) in ballot {
                let Some(governance_type) =
                    types.get(action.transaction_id, action.action_index)?
                else {
                    continue;
                };

                out.push(CommitteeCast {
                    tx: tx.hash(),
                    voter: cred.clone(),
                    proposal_tx: action.transaction_id,
                    proposal_idx: action.action_index,
                    governance_type,
                    vote: procedure.vote.clone(),
                    anchor: procedure.anchor.clone(),
                    block_height: block.number(),
                    slot: block.slot(),
                });
            }
        }
    }

    Ok(out)
}

/// This function merges sorted slot lanes into one sorted lane without
/// repeated slots. Every lane is ascending, or every lane is descending for
/// `descending`. A block that holds the votes of two voters is in two lanes,
/// and the merged lane lists it once.
///
/// An error stops the merge at its position. The caller reads the lane until
/// the page is full, so an error after the page has no effect.
fn merge_slots(
    lanes: Vec<Box<dyn Iterator<Item = Result<BlockSlot, ArchiveError>>>>,
    descending: bool,
) -> impl Iterator<Item = Result<BlockSlot, ArchiveError>> {
    let mut lanes = lanes.into_iter().map(|lane| lane.peekable()).collect_vec();
    let mut previous = None;

    std::iter::from_fn(move || loop {
        let mut selected = None;
        let mut selected_slot = None;
        for (index, lane) in lanes.iter_mut().enumerate() {
            match lane.peek() {
                Some(Err(_)) => return lane.next(),
                Some(Ok(slot))
                    if selected_slot.is_none_or(|best| {
                        if descending {
                            *slot > best
                        } else {
                            *slot < best
                        }
                    }) =>
                {
                    selected = Some(index);
                    selected_slot = Some(*slot);
                }
                _ => {}
            }
        }
        let slot = lanes[selected?].next()?;
        if selected_slot != previous {
            previous = selected_slot;
            return Some(slot);
        }
    })
}

enum CommitteeVoteQuery {
    All,
    Hot(StakeCredential),
    Cold(StakeCredential),
}

fn committee_hot_credentials<D: Domain>(
    domain: &D,
    cold: &StakeCredential,
    tip: BlockSlot,
    budget: &mut usize,
) -> Result<BTreeSet<StakeCredential>, Error> {
    let archive = domain.archive();
    let slots = archive
        .slots_by_committee_certs(cold, 0, tip)
        .map_err(log_and_500(
            "The read of the index for committee certificates failed.",
        ))?;

    let mut voters = BTreeSet::new();
    collect_block_rows::<()>(
        slots,
        |slot| archive.get_block_by_slot(&slot),
        false,
        usize::MAX,
        budget,
        |block| {
            for tx in block.txs().into_iter().filter(|tx| tx.is_valid()) {
                for cert in tx.certs() {
                    if let Some(auth) = pallas_extras::cert_as_committee_auth(&cert) {
                        if auth.cold == *cold {
                            voters.insert(auth.hot);
                        }
                    }
                }
            }
            Ok(Vec::new())
        },
    )?;
    Ok(voters)
}

/// This function reads committee votes from retained blocks in chain order.
/// For `Order::Desc`, it reads the votes in the opposite order.
/// For a cold credential, it reads retained certificates to find distinct hot
/// credentials. It merges their voter indexes into one sequence of slots.
/// The query for all committee votes uses one index.
///
/// The certificate scan and the vote scan share one limit for reads of block
/// bodies. Both scans use the same state tip.
/// The function omits votes from missing block bodies under
/// rolling history.
fn committee_vote_rows<D: Domain>(
    domain: &D,
    query: &CommitteeVoteQuery,
    tip: BlockSlot,
    pagination: &Pagination,
    mut budget: usize,
) -> Result<Vec<CommitteeCast>, Error> {
    if pagination.count == 0 {
        return Ok(Vec::new());
    }

    let archive = domain.archive();
    let descending = matches!(pagination.order, Order::Desc);
    let voters = match query {
        CommitteeVoteQuery::All => None,
        CommitteeVoteQuery::Hot(hot) => Some(BTreeSet::from([committee_voter(hot)])),
        CommitteeVoteQuery::Cold(cold) => Some(
            committee_hot_credentials(domain, cold, tip, &mut budget)?
                .iter()
                .map(committee_voter)
                .collect(),
        ),
    };

    let mut lanes: Vec<Box<dyn Iterator<Item = Result<BlockSlot, ArchiveError>>>> = Vec::new();

    if let Some(voters) = &voters {
        for voter in voters {
            let slots = archive
                .slots_by_voter_votes(&pallas_extras::voter_id_bytes(voter), 0, tip)
                .map_err(log_and_500(
                    "The read of the index for votes by voter failed.",
                ))?;

            if descending {
                lanes.push(Box::new(slots.rev()));
            } else {
                lanes.push(Box::new(slots));
            }
        }
    } else {
        let slots = archive
            .slots_by_committee_votes(0, tip)
            .map_err(log_and_500(
                "The read of the index for committee votes failed.",
            ))?;
        if descending {
            lanes.push(Box::new(slots.rev()));
        } else {
            lanes.push(Box::new(slots));
        }
    }

    let mut types = ProposalTypes::new(domain);
    let rows = collect_block_rows(
        merge_slots(lanes, descending),
        |slot| archive.get_block_by_slot(&slot),
        descending,
        pagination.to(),
        &mut budget,
        |block| committee_casts_in_block(block, voters.as_ref(), &mut types),
    )?;

    Ok(rows
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .collect())
}

/// This function lists committee votes on the requested page.
///
/// `max_scan_items` limits both the page depth and the number of blocks that
/// one request reads.
async fn query_committee_votes<D>(
    domain: Facade<D>,
    query: CommitteeVoteQuery,
    pagination: Pagination,
) -> Result<Json<Vec<CommitteeVotesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    if pagination.count == 0 {
        return Ok(Json(Vec::new()));
    }

    let tip = domain.get_tip_slot()?;
    let chain = domain.get_chain_summary()?;
    let budget = domain.config.max_scan_items() as usize;

    let rows = domain
        .query()
        .run_blocking(move |domain| {
            Ok(committee_vote_rows(
                &domain,
                &query,
                tip,
                &pagination,
                budget,
            ))
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    let page = rows
        .into_iter()
        .map(|cast| {
            let (metadata_url, metadata_hash) = match cast.anchor {
                Some(anchor) => (Some(anchor.url), Some(hex::encode(anchor.content_hash))),
                None => (None, None),
            };

            Ok(CommitteeVotesInner {
                tx_hash: hex::encode(cast.tx),
                voter_hot_id: bech32_committee_hot(&cast.voter)?,
                proposal_id: bech32_gov_action(&cast.proposal_tx, cast.proposal_idx)?,
                proposal_tx_hash: hex::encode(cast.proposal_tx),
                proposal_index: i32_or_500(cast.proposal_idx)?,
                governance_type: cast.governance_type,
                vote: committee_vote_model(&cast.vote),
                metadata_url,
                metadata_hash,
                block_height: i32_or_500(cast.block_height)?,
                block_time: i32_or_500(chain.slot_time(cast.slot))?,
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(page))
}

/// `GET /governance/committee/votes` lists all committee votes from retained
/// blocks. It reads the index for committee votes and does not use
/// authorization state.
pub async fn committee_votes<D>(
    Query(mut params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<CommitteeVotesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    params.from = None;
    params.to = None;

    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    query_committee_votes(domain, CommitteeVoteQuery::All, pagination).await
}

/// `GET /governance/committee/{cc_id}/votes` accepts a hot or cold CIP-129
/// credential. A cold credential selects every hot credential that its
/// retained authorization certificates name.
pub async fn committee_votes_by_id<D>(
    Path(cc_id): Path<String>,
    Query(mut params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<CommitteeVotesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    params.from = None;
    params.to = None;

    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;
    let (role, credential) = parse_committee_id(&cc_id).map_err(|_| Error::InvalidCommitteeId)?;

    let query = match role {
        CommitteeCredentialRole::Hot => CommitteeVoteQuery::Hot(credential),
        CommitteeCredentialRole::Cold => CommitteeVoteQuery::Cold(credential),
    };

    query_committee_votes(domain, query, pagination).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mapping::{bech32_committee_cold, bech32_committee_hot},
        routes::governance::testing::*,
        test_support::{TestApp, TestDomainBuilder, TestFault},
    };
    use bech32::{Bech32, Hrp};
    use dolos_cardano::model::gov::{CommitteeAuthorization, GovState};
    use dolos_core::{ChainPoint, StateStore as _, StateWriter as _};
    use dolos_testing::{
        synthetic::{SyntheticBlockConfig, SyntheticVote},
        toy_domain::ToyDomain,
    };
    use itertools::Itertools;
    use pallas::ledger::primitives::conway::{Anchor, Certificate, GovAction};
    use std::collections::BTreeMap;

    /// A vote with an anchor. The committee endpoints show this anchor as
    /// `metadata_url` and `metadata_hash`.
    fn synthetic_vote_with_anchor(
        voter: Voter,
        block: usize,
        tx: usize,
        action: u32,
        vote: Vote,
        url: &str,
    ) -> SyntheticVote {
        SyntheticVote {
            anchor: Some(Anchor {
                url: url.to_string(),
                content_hash: Hash::from([9u8; 32]),
            }),
            ..synthetic_vote(voter, block, tx, action, vote)
        }
    }

    fn cc_hot_key() -> StakeCredential {
        StakeCredential::AddrKeyhash(Hash::from([21u8; 28]))
    }

    fn cc_hot_script() -> StakeCredential {
        StakeCredential::ScriptHash(Hash::from([22u8; 28]))
    }

    fn seed_committee_votes_gov(domain: &ToyDomain) {
        seed_gov(
            domain,
            GovState {
                committee_auths: BTreeMap::from([(
                    cc_cold_key(31),
                    vec![
                        (15, CommitteeAuthorization::HotCredential(cc_hot_key())),
                        (25, CommitteeAuthorization::Resigned(None)),
                    ],
                )]),
                active_since: Some(0),
                ..Default::default()
            },
        );
    }

    fn committee_votes_app(mut cfg: SyntheticBlockConfig) -> TestApp {
        cfg.extra_certs_by_block = vec![vec![]; cfg.block_count];
        cfg.extra_certs_by_block[0] = vec![vec![
            Certificate::AuthCommitteeHot(cc_cold_key(31), cc_hot_script()),
            Certificate::AuthCommitteeHot(cc_cold_key(31), cc_hot_key()),
            Certificate::ResignCommitteeCold(cc_cold_key(31), None),
        ]];
        TestApp::new_with_cfg(cfg)
    }

    /// Transaction 0 in block 0 proposes two actions. Transaction 1 proposes
    /// one more action. Block 1 casts four committee votes, and three of them
    /// are in one transaction. The listing must therefore resolve the order
    /// inside one block. Block 2 changes an earlier vote. As a result, one hot
    /// credential has two rows for one proposal.
    fn committee_votes_config() -> SyntheticBlockConfig {
        let key = Voter::ConstitutionalCommitteeKey(Hash::from([21u8; 28]));
        let script = Voter::ConstitutionalCommitteeScript(Hash::from([22u8; 28]));

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
                        synthetic_vote_with_anchor(
                            key.clone(),
                            0,
                            0,
                            1,
                            Vote::No,
                            "https://example.com/no",
                        ),
                        synthetic_vote_with_anchor(
                            key.clone(),
                            0,
                            0,
                            0,
                            Vote::Yes,
                            "https://example.com/yes",
                        ),
                        synthetic_vote(script, 0, 0, 1, Vote::Abstain),
                    ],
                    vec![synthetic_vote(key.clone(), 0, 1, 0, Vote::Abstain)],
                ],
                vec![vec![synthetic_vote(key, 0, 0, 0, Vote::No)]],
            ],
            ..Default::default()
        }
    }

    /// Transaction 0 in block 0 proposes one action. Each subsequent block
    /// casts one committee vote on that action. Thus, the four votes are in
    /// four different blocks.
    fn committee_votes_across_blocks_config() -> SyntheticBlockConfig {
        committee_votes_in_blocks_config(&[1, 1, 1, 1])
    }

    /// Transaction 0 in block 0 proposes one action for each vote in the
    /// largest block. Each subsequent block casts the given number of
    /// committee votes in one transaction. The votes go to action 0, action
    /// 1, and so on. The blocks therefore hold different numbers of rows. The
    /// votes cycle through yes, no, abstain, and no in cast order. Thus, two
    /// consecutive rows are different, and the fourth row is no.
    fn committee_votes_in_blocks_config(votes_per_block: &[usize]) -> SyntheticBlockConfig {
        let key = Voter::ConstitutionalCommitteeKey(Hash::from([21u8; 28]));
        let votes = [Vote::Yes, Vote::No, Vote::Abstain, Vote::No];
        let actions = votes_per_block.iter().copied().max().unwrap_or(1);

        let mut gov_actions_by_block =
            vec![vec![(0..actions).map(|_| GovAction::Information).collect()]];
        let mut votes_by_block = vec![vec![]];
        let mut cast = 0;

        for &count in votes_per_block {
            gov_actions_by_block.push(vec![]);
            votes_by_block.push(vec![(0..count)
                .map(|action| {
                    let vote = votes[cast % votes.len()].clone();
                    cast += 1;
                    synthetic_vote(key.clone(), 0, 0, action as u32, vote)
                })
                .collect()]);
        }

        SyntheticBlockConfig {
            block_count: votes_per_block.len() + 1,
            txs_per_block: 1,
            gov_actions_by_block,
            votes_by_block,
            ..Default::default()
        }
    }

    /// Transaction 0 in block 0 proposes one action. The hot key votes in
    /// blocks 1 and 3, and the hot script votes in block 2. Each voter lane
    /// therefore skips a block that the other lane holds.
    fn committee_votes_interleaved_config() -> SyntheticBlockConfig {
        let key = Voter::ConstitutionalCommitteeKey(Hash::from([21u8; 28]));
        let script = Voter::ConstitutionalCommitteeScript(Hash::from([22u8; 28]));

        SyntheticBlockConfig {
            block_count: 4,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![GovAction::Information]], vec![], vec![], vec![]],
            votes_by_block: vec![
                vec![],
                vec![vec![synthetic_vote(key.clone(), 0, 0, 0, Vote::Yes)]],
                vec![vec![synthetic_vote(script, 0, 0, 0, Vote::No)]],
                vec![vec![synthetic_vote(key, 0, 0, 0, Vote::Abstain)]],
            ],
            ..Default::default()
        }
    }

    async fn get_committee_votes(app: &TestApp, path: &str) -> Vec<CommitteeVotesInner> {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "The request to {path} returned status {status}. The response body was {}.",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes)
            .expect("The committee vote response did not contain valid JSON.")
    }

    /// The identity of a row. The tests use this identity when the assertion
    /// is about the order and not about the content.
    fn committee_row_id(row: &CommitteeVotesInner) -> (&String, &String, &String) {
        (&row.tx_hash, &row.voter_hot_id, &row.proposal_id)
    }

    #[tokio::test]
    async fn governance_committee_votes_happy_path() {
        let app = committee_votes_app(committee_votes_config());
        let rows = get_committee_votes(&app, "/governance/committee/votes").await;
        let blocks = &app.vectors().blocks;
        let hot_key = bech32_committee_hot(&cc_hot_key()).unwrap();
        let hot_script = bech32_committee_hot(&cc_hot_script()).unwrap();
        let (domain, _) = TestDomainBuilder::new_with_synthetic(committee_votes_config()).finish();
        let chain = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();

        assert_eq!(rows.len(), 5);

        // A committee script sorts before a committee key. The script row is
        // therefore first, but the chain configuration lists this vote last.
        assert_eq!(rows[0].tx_hash, blocks[1].tx_hashes[0]);
        assert_eq!(rows[0].voter_hot_id, hot_script);
        assert_eq!(rows[0].proposal_tx_hash, blocks[0].tx_hashes[0]);
        assert_eq!(rows[0].proposal_index, 1);
        assert_eq!(rows[0].vote, committee_votes_inner::Vote::Abstain);
        assert_eq!(rows[0].metadata_url, None);
        assert_eq!(rows[0].metadata_hash, None);

        assert_eq!(rows[1].tx_hash, blocks[1].tx_hashes[0]);
        assert_eq!(rows[1].voter_hot_id, hot_key);
        assert_eq!(rows[1].proposal_index, 0);
        assert_eq!(rows[1].vote, committee_votes_inner::Vote::Yes);
        assert_eq!(
            rows[1].metadata_url.as_deref(),
            Some("https://example.com/yes")
        );
        assert_eq!(
            rows[1].metadata_hash.as_deref(),
            Some(&hex::encode([9u8; 32])[..])
        );

        assert_eq!(rows[2].tx_hash, blocks[1].tx_hashes[0]);
        assert_eq!(rows[2].voter_hot_id, hot_key);
        assert_eq!(rows[2].proposal_index, 1);
        assert_eq!(rows[2].vote, committee_votes_inner::Vote::No);
        assert_eq!(
            rows[2].metadata_url.as_deref(),
            Some("https://example.com/no")
        );

        assert_eq!(rows[3].tx_hash, blocks[1].tx_hashes[1]);
        assert_eq!(rows[3].proposal_tx_hash, blocks[0].tx_hashes[1]);
        assert_eq!(rows[3].vote, committee_votes_inner::Vote::Abstain);

        // The second vote adds a row. It does not replace the first row.
        assert_eq!(rows[4].tx_hash, blocks[2].tx_hashes[0]);
        assert_eq!(rows[4].proposal_index, 0);
        assert_eq!(rows[4].vote, committee_votes_inner::Vote::No);

        for (row, block) in rows.iter().zip([1usize, 1, 1, 1, 2]) {
            let proposal_tx: Hash<32> = row.proposal_tx_hash.parse().unwrap();
            assert_eq!(
                row.proposal_id,
                bech32_gov_action(&proposal_tx, row.proposal_index as u32).unwrap()
            );
            assert_eq!(
                row.governance_type,
                committee_votes_inner::GovernanceType::InfoAction
            );
            assert_eq!(row.block_height, blocks[block].block_number as i32);
            assert_eq!(row.block_time, chain.slot_time(blocks[block].slot) as i32);
        }
    }

    #[tokio::test]
    async fn governance_committee_votes_list_is_independent_of_authorization_state() {
        let app = TestApp::new_with_cfg_and_setup(committee_votes_config(), |domain, _| {
            seed_gov(
                domain,
                GovState {
                    committee_auths: BTreeMap::from([(
                        cc_cold_key(31),
                        vec![(15, CommitteeAuthorization::HotCredential(cc_hot_script()))],
                    )]),
                    active_since: Some(0),
                    ..Default::default()
                },
            );
        });
        let rows = get_committee_votes(&app, "/governance/committee/votes").await;
        assert_eq!(rows.len(), 5);

        let app = TestApp::new_with_cfg_and_setup(committee_votes_config(), |domain, _| {
            let writer = domain.state().start_writer().unwrap();
            writer.save_entity_typed::<GovState>(&(), None).unwrap();
            writer.commit().unwrap();
        });
        assert_eq!(
            get_committee_votes(&app, "/governance/committee/votes").await,
            rows
        );
    }

    #[tokio::test]
    async fn governance_committee_votes_orders_and_paginates() {
        let app = committee_votes_app(committee_votes_config());
        let ascending = get_committee_votes(&app, "/governance/committee/votes").await;
        let descending = get_committee_votes(&app, "/governance/committee/votes?order=desc").await;

        assert_eq!(
            descending.iter().map(committee_row_id).collect_vec(),
            ascending.iter().rev().map(committee_row_id).collect_vec()
        );

        let page = get_committee_votes(&app, "/governance/committee/votes?count=2&page=2").await;
        assert_eq!(
            page.iter().map(committee_row_id).collect_vec(),
            ascending[2..4].iter().map(committee_row_id).collect_vec()
        );

        assert!(
            get_committee_votes(&app, "/governance/committee/votes?page=9")
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn governance_committee_votes_ignores_from_and_to() {
        let app = committee_votes_app(committee_votes_config());
        let hot = bech32_committee_hot(&cc_hot_key()).unwrap();
        let cold = bech32_committee_cold(&cc_cold_key(31)).unwrap();

        for path in [
            "/governance/committee/votes".to_string(),
            format!("/governance/committee/{hot}/votes"),
            format!("/governance/committee/{cold}/votes"),
        ] {
            for pagination in [
                "order=asc",
                "count=1&page=2&order=asc",
                "count=1&page=2&order=desc",
            ] {
                let base = format!("{path}?{pagination}");
                let expected = get_committee_votes(&app, &base).await;
                assert!(!expected.is_empty());

                for query in [
                    "from=bad",
                    "to=bad",
                    "from=200&to=100",
                    "from=100:2&to=100:1",
                    "from=0&to=0",
                ] {
                    let url = format!("{base}&{query}");
                    assert_eq!(
                        get_committee_votes(&app, &url).await,
                        expected,
                        "The response for {url} must ignore from and to."
                    );
                }
            }

            for pagination in ["count=0", "page=0", "order=sideways"] {
                assert_status(
                    &app,
                    &format!("{path}?{pagination}&from=bad&to=bad"),
                    StatusCode::BAD_REQUEST,
                )
                .await;
            }
        }
    }

    #[tokio::test]
    async fn governance_committee_votes_by_hot_id_selects_one_voter() {
        let app = committee_votes_app(committee_votes_config());
        let hot_key = bech32_committee_hot(&cc_hot_key()).unwrap();
        let hot_script = bech32_committee_hot(&cc_hot_script()).unwrap();

        let rows =
            get_committee_votes(&app, &format!("/governance/committee/{hot_key}/votes")).await;
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|row| row.voter_hot_id == hot_key));

        let rows =
            get_committee_votes(&app, &format!("/governance/committee/{hot_script}/votes")).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].voter_hot_id, hot_script);
        assert_eq!(rows[0].vote, committee_votes_inner::Vote::Abstain);

        // Bech32 permits an ID in upper case. Blockfrost accepts it.
        let upper = get_committee_votes(
            &app,
            &format!(
                "/governance/committee/{}/votes",
                hot_script.to_ascii_uppercase()
            ),
        )
        .await;
        assert_eq!(
            upper.iter().map(committee_row_id).collect_vec(),
            rows.iter().map(committee_row_id).collect_vec()
        );
    }

    /// A cold credential selects every hot credential that it authorized. A
    /// rotation therefore returns the votes of both hot keys. A resignation
    /// adds no hot credential.
    #[tokio::test]
    async fn governance_committee_votes_by_cold_id_follows_authorizations() {
        let app = committee_votes_app(committee_votes_config());

        let cold_id = bech32_committee_cold(&cc_cold_key(31)).unwrap();
        let rows =
            get_committee_votes(&app, &format!("/governance/committee/{cold_id}/votes")).await;
        let all = get_committee_votes(&app, "/governance/committee/votes").await;

        // Both authorized hot credentials voted. The cold listing is therefore
        // the full listing.
        assert_eq!(rows.len(), 5);
        assert_eq!(
            rows.iter().map(committee_row_id).collect_vec(),
            all.iter().map(committee_row_id).collect_vec()
        );
    }

    #[tokio::test]
    async fn governance_committee_votes_by_unauthorized_cold_id_is_empty() {
        let app = committee_votes_app(committee_votes_config());
        let cold_id = bech32_committee_cold(&cc_cold_key(32)).unwrap();

        assert!(
            get_committee_votes(&app, &format!("/governance/committee/{cold_id}/votes"))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn governance_committee_cold_votes_ignore_stale_state() {
        let app = TestApp::new_with_cfg_and_setup(committee_votes_config(), |domain, _| {
            seed_committee_votes_gov(domain);
        });
        let cold = bech32_committee_cold(&cc_cold_key(31)).unwrap();
        assert!(
            get_committee_votes(&app, &format!("/governance/committee/{cold}/votes"))
                .await
                .is_empty()
        );
        assert_eq!(
            get_committee_votes(&app, "/governance/committee/votes")
                .await
                .len(),
            5
        );
    }

    #[tokio::test]
    async fn governance_committee_cold_votes_retained_auth_overrides_stale_state() {
        let mut cfg = committee_votes_config();
        cfg.extra_certs_by_block = vec![vec![]; cfg.block_count];
        cfg.extra_certs_by_block[0] = vec![vec![Certificate::AuthCommitteeHot(
            cc_cold_key(31),
            cc_hot_script(),
        )]];
        let app =
            TestApp::new_with_cfg_and_setup(cfg, |domain, _| seed_committee_votes_gov(domain));
        let cold = bech32_committee_cold(&cc_cold_key(31)).unwrap();
        let rows = get_committee_votes(&app, &format!("/governance/committee/{cold}/votes")).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].voter_hot_id,
            bech32_committee_hot(&cc_hot_script()).unwrap()
        );
    }

    #[tokio::test]
    async fn governance_committee_cold_votes_match_typed_credentials_and_shared_hots() {
        let cold_key = cc_cold_key(31);
        let cold_script = StakeCredential::ScriptHash(Hash::from([31u8; 28]));
        let hot_key = cc_hot_key();
        let hot_script = StakeCredential::ScriptHash(Hash::from([21u8; 28]));
        let cfg = SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 2,
            gov_actions_by_block: vec![vec![vec![GovAction::Information]], vec![], vec![]],
            extra_certs_by_block: vec![
                vec![vec![
                    Certificate::AuthCommitteeHot(cold_key.clone(), hot_key.clone()),
                    Certificate::AuthCommitteeHot(cold_key.clone(), hot_key.clone()),
                    Certificate::AuthCommitteeHot(cold_script.clone(), hot_script.clone()),
                    Certificate::AuthCommitteeHot(cc_cold_key(32), hot_key.clone()),
                    Certificate::ResignCommitteeCold(cc_cold_key(33), None),
                ]],
                vec![
                    vec![],
                    vec![Certificate::AuthCommitteeHot(
                        cc_cold_key(34),
                        hot_key.clone(),
                    )],
                ],
                vec![],
            ],
            votes_by_block: vec![
                vec![],
                vec![],
                vec![vec![
                    synthetic_vote(committee_voter(&hot_key), 0, 0, 0, Vote::Yes),
                    synthetic_vote(committee_voter(&hot_script), 0, 0, 0, Vote::No),
                    synthetic_vote(Voter::DRepKey(Hash::from([21u8; 28])), 0, 0, 0, Vote::Yes),
                    synthetic_vote(
                        Voter::StakePoolKey(Hash::from([21u8; 28])),
                        0,
                        0,
                        0,
                        Vote::Yes,
                    ),
                ]],
            ],
            invalid_txs_by_block: vec![vec![], vec![1], vec![]],
            ..Default::default()
        };
        let app = TestApp::new_with_cfg(cfg);
        let all = get_committee_votes(&app, "/governance/committee/votes").await;
        assert_eq!(all.len(), 2);
        for (cold, hot) in [
            (cold_key, hot_key.clone()),
            (cold_script, hot_script),
            (cc_cold_key(32), hot_key),
        ] {
            let id = bech32_committee_cold(&cold).unwrap();
            let rows =
                get_committee_votes(&app, &format!("/governance/committee/{id}/votes")).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].voter_hot_id, bech32_committee_hot(&hot).unwrap());
        }
        for cold in [cc_cold_key(33), cc_cold_key(34)] {
            let id = bech32_committee_cold(&cold).unwrap();
            assert!(
                get_committee_votes(&app, &format!("/governance/committee/{id}/votes"))
                    .await
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn governance_committee_cold_votes_include_before_auth_and_after_resign() {
        let mut cfg = committee_votes_in_blocks_config(&[1, 1, 1, 1]);
        cfg.extra_certs_by_block = vec![vec![]; 5];
        cfg.extra_certs_by_block[2] = vec![vec![Certificate::AuthCommitteeHot(
            cc_cold_key(31),
            cc_hot_key(),
        )]];
        cfg.extra_certs_by_block[3] = vec![vec![Certificate::ResignCommitteeCold(
            cc_cold_key(31),
            None,
        )]];
        let app = TestApp::new_with_cfg(cfg);
        let cold = bech32_committee_cold(&cc_cold_key(31)).unwrap();
        let path = format!("/governance/committee/{cold}/votes");
        let all = get_committee_votes(&app, "/governance/committee/votes").await;
        assert_eq!(all.len(), 4);
        assert_eq!(get_committee_votes(&app, &path).await, all);
        assert_eq!(
            get_committee_votes(&app, &format!("{path}?order=desc")).await,
            all.iter().rev().cloned().collect_vec()
        );
    }

    #[tokio::test]
    async fn governance_committee_cold_votes_share_certificate_and_vote_budget() {
        let mut cfg = committee_votes_across_blocks_config();
        cfg.extra_certs_by_block = vec![vec![]; cfg.block_count];
        cfg.extra_certs_by_block[0] = vec![vec![Certificate::AuthCommitteeHot(
            cc_cold_key(31),
            cc_hot_key(),
        )]];
        let cold = bech32_committee_cold(&cc_cold_key(31)).unwrap();
        let path = format!("/governance/committee/{cold}/votes?count=1&page=2");
        let app = TestApp::new_with_scan_limit(cfg.clone(), 2);
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        let app = TestApp::new_with_scan_limit(cfg, 3);
        let rows = get_committee_votes(&app, &path).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_hash, app.vectors().blocks[2].tx_hashes[0]);
    }

    #[test]
    fn governance_committee_cold_votes_exact_shared_budget_and_deduplicated_blocks() {
        let mut cfg = committee_votes_config();
        cfg.extra_certs_by_block = vec![vec![]; cfg.block_count];
        cfg.extra_certs_by_block[0] = vec![vec![
            Certificate::AuthCommitteeHot(cc_cold_key(31), cc_hot_script()),
            Certificate::AuthCommitteeHot(cc_cold_key(31), cc_hot_key()),
        ]];
        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
        let query = CommitteeVoteQuery::Cold(cc_cold_key(31));
        let tip = vectors.blocks[2].slot;
        let rows = committee_vote_rows(&domain, &query, tip, &Pagination::default(), 3).unwrap();
        assert_eq!(rows.len(), 5);
        let pagination = Pagination {
            count: 1,
            page: 2,
            ..Default::default()
        };
        let rows = committee_vote_rows(&domain, &query, tip, &pagination, 2).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].slot, vectors.blocks[1].slot);
        let result = committee_vote_rows(&domain, &query, tip, &Pagination::default(), 2);
        assert!(matches!(result, Err(Error::ScanBudgetExceeded)));
        let query = CommitteeVoteQuery::Cold(cc_cold_key(32));
        assert!(
            committee_vote_rows(&domain, &query, tip, &Pagination::default(), 0)
                .unwrap()
                .is_empty()
        );
        let pagination = Pagination {
            count: 0,
            ..Default::default()
        };
        for query in [
            CommitteeVoteQuery::All,
            CommitteeVoteQuery::Hot(cc_hot_key()),
            CommitteeVoteQuery::Cold(cc_cold_key(31)),
        ] {
            assert!(committee_vote_rows(&domain, &query, tip, &pagination, 0)
                .unwrap()
                .is_empty());
        }
    }

    #[test]
    fn governance_committee_vote_queries_fail_independently_on_archive_index_errors() {
        let (domain, vectors) =
            TestDomainBuilder::new_with_synthetic(committee_votes_config()).finish();
        let domain =
            dolos_testing::faults::FaultyToyDomain::new(domain, TestFault::ArchiveStoreError);
        let tip = vectors.blocks[2].slot;
        let result = committee_hot_credentials(&domain, &cc_cold_key(31), tip, &mut 1);
        assert!(matches!(
            result,
            Err(Error::Code(StatusCode::INTERNAL_SERVER_ERROR))
        ));
        for query in [
            CommitteeVoteQuery::All,
            CommitteeVoteQuery::Hot(cc_hot_key()),
        ] {
            let result = committee_vote_rows(&domain, &query, tip, &Pagination::default(), 1);
            assert!(matches!(
                result,
                Err(Error::Code(StatusCode::INTERNAL_SERVER_ERROR))
            ));
        }
    }

    #[tokio::test]
    async fn governance_committee_cold_votes_exclude_future_auth_at_captured_tip() {
        let mut cfg = committee_votes_interleaved_config();
        cfg.extra_certs_by_block = vec![vec![]; cfg.block_count];
        cfg.extra_certs_by_block[0] = vec![vec![Certificate::AuthCommitteeHot(
            cc_cold_key(31),
            cc_hot_key(),
        )]];
        cfg.extra_certs_by_block[3] = vec![vec![Certificate::AuthCommitteeHot(
            cc_cold_key(31),
            cc_hot_script(),
        )]];
        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
        let tip = vectors.blocks[2].slot;
        let query = CommitteeVoteQuery::Cold(cc_cold_key(31));
        let rows = committee_vote_rows(&domain, &query, tip, &Pagination::default(), 100).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].slot, vectors.blocks[1].slot);
        assert_eq!(rows[0].voter, cc_hot_key());
        let rows = committee_vote_rows(
            &domain,
            &query,
            vectors.blocks[3].slot,
            &Pagination::default(),
            100,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[tokio::test]
    async fn governance_committee_votes_missing_and_invalid_bodies() {
        use dolos_cardano::indexes::archive_dimensions::{
            COMMITTEE_CERTS, COMMITTEE_VOTES, VOTER_VOTES,
        };
        use dolos_core::{ArchiveIndexDelta, ArchiveWriter as _, Tag};

        for dimension in [COMMITTEE_CERTS, COMMITTEE_VOTES, VOTER_VOTES] {
            for body in [None, Some(vec![0xFF; 8])] {
                let invalid = body.is_some();
                let app =
                    TestApp::new_with_cfg_and_setup(committee_votes_config(), |domain, vectors| {
                        seed_committee_votes_gov(domain);
                        let slot = vectors.blocks[2].slot + 1;
                        let hash = Hash::<32>::from([0xEE; 32]);
                        let writer = domain.archive().start_writer().unwrap();
                        if let Some(body) = body {
                            writer
                                .apply(
                                    &ChainPoint::Specific(slot, hash),
                                    &std::sync::Arc::new(body),
                                )
                                .unwrap();
                        }
                        let key = match dimension {
                            COMMITTEE_CERTS => {
                                pallas_extras::committee_cold_id_bytes(&cc_cold_key(31))
                            }
                            COMMITTEE_VOTES => b"committee".to_vec(),
                            _ => pallas_extras::voter_id_bytes(&committee_voter(&cc_hot_key())),
                        };
                        writer
                            .apply_index(&[ArchiveIndexDelta {
                                slot,
                                block_hash: hash.to_vec(),
                                block_number: None,
                                tx_hashes: vec![],
                                tags: vec![Tag::new(dimension, key)],
                            }])
                            .unwrap();
                        writer.commit().unwrap();
                        let writer = domain.state().start_writer().unwrap();
                        writer.set_cursor(ChainPoint::Specific(slot, hash)).unwrap();
                        writer.commit().unwrap();
                    });
                let id = if dimension == COMMITTEE_CERTS {
                    Some(bech32_committee_cold(&cc_cold_key(31)).unwrap())
                } else if dimension == VOTER_VOTES {
                    Some(bech32_committee_hot(&cc_hot_key()).unwrap())
                } else {
                    None
                };
                let path = match id {
                    Some(id) => format!("/governance/committee/{id}/votes"),
                    None => "/governance/committee/votes".to_string(),
                };
                if invalid {
                    assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
                } else {
                    let rows = get_committee_votes(&app, &path).await;
                    assert_eq!(
                        rows.len(),
                        if dimension == COMMITTEE_CERTS {
                            0
                        } else if dimension == COMMITTEE_VOTES {
                            5
                        } else {
                            4
                        }
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn governance_committee_votes_archive_index_errors() {
        let app = TestApp::new_with_cfg_and_fault(
            committee_votes_config(),
            Some(TestFault::ArchiveStoreError),
        );
        for id in [
            None,
            Some(bech32_committee_hot(&cc_hot_key()).unwrap()),
            Some(bech32_committee_cold(&cc_cold_key(31)).unwrap()),
        ] {
            let path = match id {
                None => "/governance/committee/votes".to_string(),
                Some(id) => format!("/governance/committee/{id}/votes"),
            };
            assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
        }
    }

    #[tokio::test]
    async fn governance_committee_votes_bad_request() {
        let app = committee_votes_app(committee_votes_config());
        let hot = bech32_committee_hot(&cc_hot_key()).unwrap();

        assert_status(
            &app,
            "/governance/committee/votes?count=0",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/committee/votes?order=sideways",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            &format!("/governance/committee/{hot}/votes?page=0"),
            StatusCode::BAD_REQUEST,
        )
        .await;
        let (status, body) = app
            .get_bytes("/governance/committee/not-a-credential/votes")
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["message"], "Invalid or malformed cc credential id.");

        // A DRep credential is correct Bech32 text with the wrong prefix.
        assert_status(
            &app,
            &format!("/governance/committee/{}/votes", app.vectors().drep_id),
            StatusCode::BAD_REQUEST,
        )
        .await;

        // The prefix says hot, but the CIP-129 header says cold.
        let mismatched = bech32::encode::<Bech32>(
            Hrp::parse_unchecked("cc_hot"),
            &[&[0x12u8][..], &[21u8; 28][..]].concat(),
        )
        .unwrap();
        assert_status(
            &app,
            &format!("/governance/committee/{mismatched}/votes"),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn governance_committee_votes_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let hot = bech32_committee_hot(&cc_hot_key()).unwrap();

        assert_status(
            &app,
            "/governance/committee/votes",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        assert_status(
            &app,
            &format!("/governance/committee/{hot}/votes"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }

    /// The page depth is limited like on the other endpoints that read a
    /// block for each row.
    #[tokio::test]
    async fn governance_committee_votes_rejects_a_deep_page() {
        let app = TestApp::new_with_scan_limit(committee_votes_across_blocks_config(), 3);
        let hot = bech32_committee_hot(&cc_hot_key()).unwrap();

        let rows = get_committee_votes(
            &app,
            &format!("/governance/committee/{hot}/votes?count=1&page=3"),
        )
        .await;
        assert_eq!(rows.len(), 1);

        let (status, bytes) = app
            .get_bytes(&format!("/governance/committee/{hot}/votes?count=2&page=2"))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("scan limit"), "The response body was {body}.");

        assert_status(
            &app,
            "/governance/committee/votes?count=2&page=2",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    /// A page can start and end in the middle of a block, and it can span
    /// blocks. Each page must be the same slice of the full listing, in both
    /// orders.
    #[tokio::test]
    async fn governance_committee_votes_pages_across_blocks() {
        let app = committee_votes_app(committee_votes_across_blocks_config());
        let blocks = app.vectors().blocks.clone();

        let page = get_committee_votes(&app, "/governance/committee/votes?count=1&page=2").await;
        assert_eq!(
            page.iter().map(|row| &row.tx_hash).collect_vec(),
            vec![&blocks[2].tx_hashes[0]]
        );

        let descending = get_committee_votes(
            &app,
            "/governance/committee/votes?count=1&page=2&order=desc",
        )
        .await;
        assert_eq!(
            descending.iter().map(|row| &row.tx_hash).collect_vec(),
            vec![&blocks[3].tx_hashes[0]]
        );

        let all = get_committee_votes(&app, "/governance/committee/votes").await;
        assert_eq!(
            all.iter().map(|row| &row.tx_hash).collect_vec(),
            blocks[1..5].iter().map(|b| &b.tx_hashes[0]).collect_vec()
        );
    }

    /// The blocks hold different numbers of rows. A block that is larger
    /// than the page does not move the rows around it. Each page must be
    /// equal to the same slice of the full listing.
    #[tokio::test]
    async fn governance_committee_votes_page_uneven_blocks() {
        let app = committee_votes_app(committee_votes_in_blocks_config(&[1, 5, 1, 1]));
        let blocks = app.vectors().blocks.clone();

        let all = get_committee_votes(&app, "/governance/committee/votes").await;
        assert_eq!(all.len(), 8);
        assert_eq!(
            all.iter().map(|row| &row.tx_hash).collect_vec(),
            [1usize, 2, 2, 2, 2, 2, 3, 4]
                .iter()
                .map(|&block| &blocks[block].tx_hashes[0])
                .collect_vec()
        );
        assert_eq!(
            all.iter().map(|row| row.vote).collect_vec(),
            [
                committee_votes_inner::Vote::Yes,
                committee_votes_inner::Vote::No,
                committee_votes_inner::Vote::Abstain,
                committee_votes_inner::Vote::No,
                committee_votes_inner::Vote::Yes,
                committee_votes_inner::Vote::No,
                committee_votes_inner::Vote::Abstain,
                committee_votes_inner::Vote::No,
            ]
        );

        let reversed = all.iter().rev().cloned().collect_vec();

        for count in 1..=8 {
            for page in 1..=8 {
                let from = (page - 1) * count;
                let to = (from + count).min(all.len());
                let slice = |listing: &[CommitteeVotesInner]| {
                    listing.get(from..to).unwrap_or_default().to_vec()
                };

                let ascending = get_committee_votes(
                    &app,
                    &format!("/governance/committee/votes?count={count}&page={page}"),
                )
                .await;
                assert_eq!(
                    ascending,
                    slice(&all),
                    "The ascending page {page} with count {count} is wrong."
                );

                let descending = get_committee_votes(
                    &app,
                    &format!("/governance/committee/votes?count={count}&page={page}&order=desc"),
                )
                .await;
                assert_eq!(
                    descending,
                    slice(&reversed),
                    "The descending page {page} with count {count} is wrong."
                );
            }
        }
    }

    /// Two hot credentials vote in different blocks. The listing merges
    /// their block lanes in chain order, in both directions, and a page can
    /// cross from one lane to the other.
    #[tokio::test]
    async fn governance_committee_votes_merge_voter_lanes() {
        let app = committee_votes_app(committee_votes_interleaved_config());
        let blocks = app.vectors().blocks.clone();
        let hot_key = bech32_committee_hot(&cc_hot_key()).unwrap();
        let hot_script = bech32_committee_hot(&cc_hot_script()).unwrap();

        let all = get_committee_votes(&app, "/governance/committee/votes").await;
        assert_eq!(
            all.iter()
                .map(|row| (&row.tx_hash, &row.voter_hot_id))
                .collect_vec(),
            vec![
                (&blocks[1].tx_hashes[0], &hot_key),
                (&blocks[2].tx_hashes[0], &hot_script),
                (&blocks[3].tx_hashes[0], &hot_key),
            ]
        );

        let descending = get_committee_votes(&app, "/governance/committee/votes?order=desc").await;
        assert_eq!(
            descending.iter().map(committee_row_id).collect_vec(),
            all.iter().rev().map(committee_row_id).collect_vec()
        );

        let page = get_committee_votes(&app, "/governance/committee/votes?count=1&page=2").await;
        assert_eq!(
            page.iter().map(committee_row_id).collect_vec(),
            vec![committee_row_id(&all[1])]
        );

        let page = get_committee_votes(
            &app,
            "/governance/committee/votes?count=2&page=1&order=desc",
        )
        .await;
        assert_eq!(
            page.iter().map(committee_row_id).collect_vec(),
            vec![committee_row_id(&all[2]), committee_row_id(&all[1])]
        );

        // The cold listing is the same merge.
        let cold_id = bech32_committee_cold(&cc_cold_key(31)).unwrap();
        let path = format!("/governance/committee/{cold_id}/votes");
        let rows = get_committee_votes(&app, &path).await;
        assert_eq!(rows, all);
        assert_eq!(
            get_committee_votes(&app, &format!("{path}?order=desc")).await,
            all.iter().rev().cloned().collect_vec()
        );
        for order in ["asc", "desc"] {
            let expected = if order == "asc" {
                all.clone()
            } else {
                all.iter().rev().cloned().collect_vec()
            };
            for page in 1..=4 {
                let rows =
                    get_committee_votes(&app, &format!("{path}?count=1&page={page}&order={order}"))
                        .await;
                assert_eq!(rows, expected.get(page - 1..page).unwrap_or_default());
            }
        }
    }

    /// A block that holds the votes of two hot credentials is in two lanes.
    /// The merge lists it once, so the listing has no repeated rows.
    #[test]
    fn merge_slots_lists_a_shared_slot_once() {
        type Lane = Box<dyn Iterator<Item = Result<BlockSlot, ArchiveError>>>;

        let lanes = |slots: Vec<Vec<BlockSlot>>| -> Vec<Lane> {
            slots
                .into_iter()
                .map(|lane| Box::new(lane.into_iter().map(Ok)) as Lane)
                .collect()
        };

        let ascending = merge_slots(lanes(vec![vec![10, 20, 40], vec![20, 30], vec![]]), false)
            .map(Result::unwrap)
            .collect_vec();
        assert_eq!(ascending, vec![10, 20, 30, 40]);

        let descending = merge_slots(lanes(vec![vec![40, 20, 10], vec![30, 20], vec![]]), true)
            .map(Result::unwrap)
            .collect_vec();
        assert_eq!(descending, vec![40, 30, 20, 10]);

        assert!(merge_slots(lanes(vec![]), false).next().is_none());

        // An error comes out at its position in the lane.
        let faulty: Vec<Lane> = vec![
            Box::new(vec![Ok(10), Ok(30)].into_iter()),
            Box::new(
                vec![
                    Ok(20),
                    Err(ArchiveError::InternalError("fault".to_string())),
                ]
                .into_iter(),
            ),
        ];
        let merged = merge_slots(faulty, false).collect_vec();
        assert_eq!(merged.len(), 4);
        assert_eq!(*merged[0].as_ref().unwrap(), 10);
        assert_eq!(*merged[1].as_ref().unwrap(), 20);
        assert!(merged[2].is_err());
        assert_eq!(*merged[3].as_ref().unwrap(), 30);
    }

    /// A pruned block takes its votes out of the listing, and the retained
    /// votes move up to fill the pages. The other history endpoints return
    /// the same under rolling history.
    ///
    /// The test prunes the archive to one slot. Only the last block remains.
    /// The three votes of the first three vote blocks are gone. The one vote
    /// of the last block is the first row.
    #[tokio::test]
    async fn governance_committee_votes_paginate_retained_rows() {
        let mut cfg = committee_votes_across_blocks_config();
        cfg.extra_certs_by_block = vec![vec![]; cfg.block_count];
        cfg.extra_certs_by_block[0] = vec![vec![Certificate::AuthCommitteeHot(
            cc_cold_key(31),
            cc_hot_key(),
        )]];
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, _| {
            seed_committee_votes_gov(domain);
            domain
                .archive()
                .prune_history(0, None, None)
                .expect("The archive did not prune its history.");
        });
        let last = app.vectors().blocks[4].tx_hashes[0].clone();

        let rows = get_committee_votes(&app, "/governance/committee/votes?count=1").await;
        assert_eq!(
            rows.iter().map(|row| &row.tx_hash).collect_vec(),
            vec![&last]
        );
        assert_eq!(rows[0].vote, committee_votes_inner::Vote::No);
        let hot = bech32_committee_hot(&cc_hot_key()).unwrap();
        assert_eq!(
            get_committee_votes(&app, &format!("/governance/committee/{hot}/votes?count=1")).await,
            rows
        );
        let cold = bech32_committee_cold(&cc_cold_key(31)).unwrap();
        assert!(
            get_committee_votes(&app, &format!("/governance/committee/{cold}/votes?count=1"))
                .await
                .is_empty()
        );

        assert!(
            get_committee_votes(&app, "/governance/committee/votes?count=1&page=2")
                .await
                .is_empty()
        );
        assert!(
            get_committee_votes(&app, "/governance/committee/votes?count=1&page=4")
                .await
                .is_empty()
        );

        let rows =
            get_committee_votes(&app, "/governance/committee/votes?count=1&order=desc").await;
        assert_eq!(
            rows.iter().map(|row| &row.tx_hash).collect_vec(),
            vec![&last]
        );
    }

    /// The ledger ignores the votes of a phase-2-invalid tx, so they are not
    /// listed, although their block is tagged for the voter.
    #[tokio::test]
    async fn governance_committee_votes_skip_invalid_transactions() {
        let key = Voter::ConstitutionalCommitteeKey(Hash::from([21u8; 28]));
        let app = committee_votes_app(SyntheticBlockConfig {
            block_count: 2,
            txs_per_block: 2,
            gov_actions_by_block: vec![vec![vec![GovAction::Information], vec![]], vec![]],
            votes_by_block: vec![
                vec![],
                vec![
                    vec![
                        synthetic_vote(key.clone(), 0, 0, 0, Vote::Yes),
                        synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::Yes),
                        synthetic_vote(spo_voter(), 0, 0, 0, Vote::Yes),
                    ],
                    vec![
                        synthetic_vote(key, 0, 0, 0, Vote::No),
                        synthetic_vote(committee_voter(&cc_hot_script()), 0, 0, 0, Vote::No),
                    ],
                ],
            ],
            invalid_txs_by_block: vec![vec![], vec![1]],
            ..Default::default()
        });

        let rows = get_committee_votes(&app, "/governance/committee/votes").await;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_hash, app.vectors().blocks[1].tx_hashes[0]);
        assert_eq!(rows[0].vote, committee_votes_inner::Vote::Yes);
    }

    /// A tagged block whose committee votes sit in phase-2-invalid txs adds
    /// no row, but the scan still counts it. Enough of those blocks stop the
    /// request instead of letting it read without end.
    #[tokio::test]
    async fn governance_committee_votes_scan_stops_at_the_budget() {
        let key = Voter::ConstitutionalCommitteeKey(Hash::from([21u8; 28]));
        let cfg = SyntheticBlockConfig {
            block_count: 4,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![GovAction::Information]], vec![], vec![], vec![]],
            votes_by_block: vec![
                vec![],
                vec![vec![synthetic_vote(key.clone(), 0, 0, 0, Vote::Yes)]],
                vec![vec![synthetic_vote(key.clone(), 0, 0, 0, Vote::No)]],
                vec![vec![synthetic_vote(key, 0, 0, 0, Vote::Abstain)]],
            ],
            invalid_txs_by_block: vec![vec![], vec![0], vec![0], vec![]],
            ..Default::default()
        };
        let hot = bech32_committee_hot(&cc_hot_key()).unwrap();
        let path = format!("/governance/committee/{hot}/votes?count=1");

        // Two empty blocks spend a budget of two before the row appears.
        let app = TestApp::new_with_scan_limit(cfg.clone(), 2);
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("scan limit"), "The response body was {body}.");

        // A budget of three reaches the block that holds the row.
        let app = TestApp::new_with_scan_limit(cfg, 3);
        let rows = get_committee_votes(&app, &path).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_hash, app.vectors().blocks[3].tx_hashes[0]);
        assert_eq!(rows[0].vote, committee_votes_inner::Vote::Abstain);
    }

    #[test]
    fn merge_slots_overlapping_lanes_stop_before_failing_tails() {
        for descending in [false, true] {
            let (first, last) = if descending { (3, 1) } else { (1, 3) };
            let pulled = std::rc::Rc::new(std::cell::Cell::new(0));
            let lanes = [last, 2]
                .into_iter()
                .map(|next| {
                    let count = pulled.clone();
                    let lane = [
                        Ok(first),
                        Ok(next),
                        Err(ArchiveError::InternalError(
                            "The scan must stop before this archive error.".into(),
                        )),
                    ]
                    .into_iter()
                    .inspect(move |_| count.set(count.get() + 1));
                    Box::new(lane) as Box<dyn Iterator<Item = Result<BlockSlot, ArchiveError>>>
                })
                .collect();
            let mut fetched = Vec::new();
            let rows = collect_block_rows(
                merge_slots(lanes, descending),
                |slot| {
                    fetched.push(slot);
                    Ok(Some(drep_registration_block(true)))
                },
                descending,
                2,
                &mut 2,
                |_| Ok(vec![()]),
            )
            .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(fetched, [first, 2]);
            assert_eq!(pulled.get(), 4);
        }
    }

    #[test]
    fn merge_slots_does_not_prefetch_after_single_lane_page() {
        let pulled = std::rc::Rc::new(std::cell::Cell::new(0));
        let count = pulled.clone();
        let lane = (0..3).map(Ok).inspect(move |_| count.set(count.get() + 1));
        let rows = collect_block_rows(
            merge_slots(vec![Box::new(lane)], false),
            |_| Ok(Some(drep_registration_block(true))),
            false,
            1,
            &mut 1,
            |_| Ok(vec![()]),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(pulled.get(), 1);
    }
}
