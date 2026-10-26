use std::collections::{BTreeMap, HashSet};

use axum::http::StatusCode;
use dolos_cardano::{
    final_rows::newest_final_row,
    model::{gov::GovState, DRepKey, ProposalKey, ProposalState},
    ChainSummary, PParamsSet,
};
use dolos_core::{
    ArchiveError, ArchiveStore as _, BlockBody, BlockSlot, Domain, KeyCodec as _, StateStore as _,
};
use pallas::ledger::{primitives::conway::DRep, traverse::MultiEraBlock};

use crate::{error::Error, log_and_500, Facade};

mod committee;
mod committee_votes;
mod drep_by_id;
mod drep_delegators;
mod drep_metadata;
mod drep_updates;
mod drep_votes;
mod dreps;
mod proposal;
mod proposal_metadata;
mod proposal_parameters;
mod proposal_votes;
mod proposal_withdrawals;
mod proposals;

pub use committee::committee;
pub use committee_votes::{committee_votes, committee_votes_by_id};
pub use drep_by_id::drep_by_id;
pub use drep_delegators::drep_delegators;
pub use drep_metadata::drep_metadata;
pub use drep_updates::drep_updates;
pub use drep_votes::drep_votes;
pub(crate) use drep_votes::voter_casts;
pub use dreps::all_dreps;
pub use proposal::{proposal_by_gov_action_id, proposal_by_tx_index};
pub use proposal_metadata::{proposal_metadata, proposal_metadata_by_gov_action};
pub use proposal_parameters::{proposal_parameters, proposal_parameters_by_gov_action};
pub use proposal_votes::{proposal_votes, proposal_votes_by_gov_action};
pub use proposal_withdrawals::{proposal_withdrawals, proposal_withdrawals_by_gov_action};
pub use proposals::proposals;

fn chain_context<D: Domain>(
    domain: &Facade<D>,
) -> Result<(ChainSummary, BlockSlot, PParamsSet), StatusCode> {
    let chain = domain.get_chain_summary()?;
    let tip = domain.get_tip_slot()?;
    let pparams = domain.get_current_effective_pparams()?;

    Ok((chain, tip, pparams))
}

/// The last completed DRep distribution, empty before the first one. See
/// [`crate::mapping::governance::dreps::drep_amount`].
fn drep_distribution<D: Domain>(domain: &Facade<D>) -> Result<BTreeMap<DRep, u64>, StatusCode> {
    let distribution = domain
        .read_cardano_entity::<GovState>(&())?
        .and_then(|gov| gov.prev_distr)
        .map(|distr| distr.drep_distr)
        .unwrap_or_default();

    Ok(distribution)
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

/// A proposal's row, and where it was read.
pub(crate) enum ProposalRow {
    /// A live proposal's state row, vote maps included.
    Live(ProposalState),

    /// The final row of a proposal that has left state
    /// (`dolos_cardano::final_rows`), without vote maps.
    Final(ProposalState),
}

impl ProposalRow {
    pub(crate) fn state(&self) -> &ProposalState {
        match self {
            Self::Live(state) | Self::Final(state) => state,
        }
    }

    pub(crate) fn into_state(self) -> ProposalState {
        match self {
            Self::Live(state) | Self::Final(state) => state,
        }
    }
}

/// A proposal's row: the state row, then the final-row log.
///
/// ESTART commits a final row before it deletes the state row, so a state
/// miss read first finds a moved proposal in the log.
pub(crate) fn read_proposal_row<D: Domain>(
    domain: &D,
    key: &ProposalKey,
) -> Result<Option<ProposalRow>, StatusCode> {
    let live = domain
        .state()
        .read_entity_typed::<ProposalState>(key)
        .map_err(log_and_500("failed to read proposal state"))?;

    if let Some(state) = live {
        return Ok(Some(ProposalRow::Live(state)));
    }

    let logged = newest_final_row::<ProposalState>(domain.archive(), key)
        .map_err(log_and_500("failed to read the proposals log"))?;

    Ok(logged.map(|(_, state)| ProposalRow::Final(state)))
}

/// Visit every proposal: the live rows in state, then the final rows of those
/// that left it.
///
/// The state is read first, as in [`read_proposal_row`]. A proposal that moves
/// between the two reads is in both, and is visited once, from state.
pub(crate) fn visit_proposals<D: Domain>(
    domain: &D,
    mut visit: impl FnMut(&ProposalState),
) -> Result<(), StatusCode> {
    let mut live = HashSet::new();

    let rows = domain
        .state()
        .iter_entities_typed::<ProposalState>(None)
        .map_err(log_and_500("failed to iterate proposal state"))?;

    for row in rows {
        let (key, state) = row.map_err(log_and_500("failed to read proposal state"))?;
        live.insert(key);
        visit(&state);
    }

    let rows = domain
        .archive()
        .iter_logs_typed::<ProposalState>(None)
        .map_err(log_and_500("failed to iterate the proposals log"))?;

    for row in rows {
        let (_, key, state) = row.map_err(log_and_500("failed to read the proposals log"))?;

        if !live.contains(&key) {
            visit(&state);
        }
    }

    Ok(())
}

/// The `dreps` key of the DRep whose id bytes `parse_drep_id` returned.
fn drep_key(drep_bytes: &[u8]) -> Result<DRepKey, StatusCode> {
    DRepKey::decode_key(drep_bytes).map_err(|_| StatusCode::BAD_REQUEST)
}

/// This function reads tagged blocks in order and returns the rows that
/// `rows_in_block` finds in them until it has `needed` rows.
///
/// A tagged block can hold no row, for example when its certificate or vote
/// sits in a phase-2-invalid transaction. Such a block costs a read but adds
/// no row. So the function stops with `ScanBudgetExceeded` when it has read
/// `budget` blocks and still needs another one.
fn collect_block_rows<T>(
    mut slots: impl Iterator<Item = Result<BlockSlot, ArchiveError>>,
    mut fetch: impl FnMut(BlockSlot) -> Result<Option<BlockBody>, ArchiveError>,
    descending: bool,
    needed: usize,
    budget: &mut usize,
    mut rows_in_block: impl FnMut(&MultiEraBlock) -> Result<Vec<T>, StatusCode>,
) -> Result<Vec<T>, Error> {
    let mut rows = Vec::new();

    while rows.len() < needed {
        let Some(slot) = slots.next() else {
            break;
        };

        if *budget == 0 {
            return Err(Error::ScanBudgetExceeded);
        }

        *budget -= 1;
        let slot = slot.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let body = fetch(slot);

        // A block that the archive no longer has leaves the list. The other
        // history endpoints do the same under rolling history.
        let Some(body) = body.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? else {
            continue;
        };

        let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let mut block_rows = rows_in_block(&block)?;

        // The blocks come from the tip backward in descending order. The rows
        // of each block are reversed too, so that the list is the exact
        // reverse of the ascending list.
        if descending {
            block_rows.reverse();
        }

        rows.append(&mut block_rows);
    }

    Ok(rows)
}

#[cfg(test)]
pub(super) mod testing {
    use super::*;
    use crate::test_support::TestApp;
    use bech32::{Bech32, Hrp};
    use blockfrost_openapi::models::drep::Drep as DrepModel;
    use dolos_cardano::{
        final_rows::FinalRows,
        model::{
            gov::{GovDistr, GovState},
            DRepSightingState,
        },
        pallas_extras,
    };
    use dolos_core::{ArchiveWriter as _, StateWriter as _};
    use dolos_testing::{
        synthetic::{SyntheticBlockConfig, SyntheticProposalRef, SyntheticVote},
        toy_domain::ToyDomain,
    };
    use pallas::{
        codec::utils::Bytes,
        crypto::hash::Hash,
        ledger::primitives::{
            conway::{Anchor, DRep, GovAction, ProtocolParamUpdate, Vote, Voter},
            StakeCredential,
        },
    };

    pub fn invalid_drep() -> &'static str {
        "not-a-drep"
    }

    pub fn encode_id(hrp: &str, payload: &[u8]) -> String {
        let hrp = Hrp::parse_unchecked(hrp);
        bech32::encode::<Bech32>(hrp, payload).expect("failed to encode bech32 id")
    }

    pub fn missing_drep() -> String {
        let payload = [vec![pallas_extras::DREP_KEY_PREFIX], vec![8u8; 28]].concat();
        encode_id("drep", &payload)
    }

    pub async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, _body) = app.get_bytes(path).await;
        assert_eq!(status, expected);
    }

    pub async fn get_drep(app: &TestApp, drep_id: &str) -> DrepModel {
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

    /// This function gives the synthetic DRep an anchor that Dolos can never
    /// fetch. `example.invalid` does not resolve (RFC 6761), so the fetch
    /// always stops with a connection error. The endpoint still returns the
    /// anchor fields. The synthetic chain imported a registration with no
    /// anchor, and this function replaces it.
    pub fn seed_drep_anchor(domain: &ToyDomain, drep_bytes: Vec<u8>) {
        let anchor = Anchor {
            url: "https://example.invalid/drep".to_string(),
            content_hash: Hash::from([9u8; 32]),
        };

        seed_drep_with_anchor(domain, drep_bytes, anchor);
    }

    /// The anchor the DRep's sighting carries, which both the list and
    /// `/metadata` serve.
    pub fn seed_drep_with_anchor(domain: &ToyDomain, drep_bytes: Vec<u8>, anchor: Anchor) {
        let key = drep_key(&drep_bytes).unwrap();

        let mut sighting = domain
            .state()
            .read_entity_typed::<DRepSightingState>(&key)
            .expect("failed to read drep sighting")
            .expect("the synthetic drep has a sighting");
        sighting.anchor = Some(anchor);

        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");
        writer
            .write_entity_typed(&key, &sighting)
            .expect("failed to write drep sighting");
        writer.commit().expect("failed to commit drep sighting");
    }

    pub fn synthetic_drep() -> DRep {
        DRep::Key(SyntheticBlockConfig::default().drep_keyhash.into())
    }

    pub fn synthetic_vote(
        voter: Voter,
        block: usize,
        tx: usize,
        action: u32,
        vote: Vote,
    ) -> SyntheticVote {
        SyntheticVote {
            voter,
            proposal: SyntheticProposalRef { block, tx, action },
            vote,
            anchor: None,
        }
    }

    /// Three blocks: the first tx of block 1 proposes two actions, block 2
    /// proposes none and block 3 proposes one. Enough to pin the listing
    /// order, the cert index within a tx and a few action types.
    pub fn proposal_app() -> TestApp {
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

    pub fn tx_hash_of_block(app: &TestApp, block: usize) -> String {
        app.vectors().blocks[block].tx_hashes[0].clone()
    }

    pub fn proposal_tx() -> Hash<32> {
        [7u8; 32].into()
    }

    pub fn cc_cold_key(byte: u8) -> StakeCredential {
        StakeCredential::AddrKeyhash(Hash::from([byte; 28]))
    }

    /// Makes `distribution` the last completed DRep distribution, the one the
    /// DRep routes serve `amount` from.
    pub fn seed_distribution(domain: &ToyDomain, distribution: BTreeMap<DRep, u64>) {
        let mut gov = domain
            .state()
            .read_entity_typed::<GovState>(&())
            .expect("The state store cannot read the governance state.")
            .unwrap_or_default();

        let mut distr = GovDistr::new(0, 1);
        distr.committed_shards = 1;
        distr.drep_distr = distribution;
        gov.prev_distr = Some(distr);

        seed_gov(domain, gov);
    }

    /// Makes `distribution` the partial DRep distribution of a boundary whose
    /// shards are still running, beside the last completed one.
    pub fn seed_partial_distribution(domain: &ToyDomain, distribution: BTreeMap<DRep, u64>) {
        let mut gov = domain
            .state()
            .read_entity_typed::<GovState>(&())
            .expect("The state store cannot read the governance state.")
            .unwrap_or_default();

        let mut distr = GovDistr::new(1, 2);
        distr.committed_shards = 1;
        distr.drep_distr = distribution;
        gov.distr = Some(distr);

        seed_gov(domain, gov);
    }

    /// This function overwrites the governance singleton with the given state.
    pub fn seed_gov(domain: &ToyDomain, state: GovState) {
        let writer = domain
            .state()
            .start_writer()
            .expect("The state store cannot start a writer.");
        writer
            .write_entity_typed(&(), &state)
            .expect("The state writer cannot write the governance state.");
        writer
            .commit()
            .expect("The state writer cannot commit the governance state.");
    }

    /// A block with one tx, which registers the DRep with key hash `[7; 28]`.
    pub fn drep_registration_block(valid: bool) -> BlockBody {
        use pallas::{
            codec::utils::{NonEmptySet, Set},
            ledger::primitives::conway::{Certificate, TransactionBody},
        };

        let cred = StakeCredential::AddrKeyhash(Hash::from([7u8; 28]));

        let body = TransactionBody {
            inputs: Set::from(vec![]),
            outputs: vec![],
            fee: 0,
            ttl: None,
            certificates: Some(
                NonEmptySet::try_from(vec![Certificate::RegDRepCert(cred, 500, None)]).unwrap(),
            ),
            withdrawals: None,
            auxiliary_data_hash: None,
            validity_interval_start: None,
            mint: None,
            script_data_hash: None,
            collateral: None,
            required_signers: None,
            network_id: None,
            collateral_return: None,
            total_collateral: None,
            reference_inputs: None,
            voting_procedures: None,
            proposal_procedures: None,
            treasury_value: None,
            donation: None,
        };

        let (_, raw) = dolos_testing::blocks::make_conway_block_with_tx(100, body, None, valid);
        (*raw).clone()
    }

    pub fn drep_key_voter() -> Voter {
        Voter::DRepKey([0x66u8; 28].into())
    }

    pub fn spo_voter() -> Voter {
        Voter::StakePoolKey([0x99u8; 28].into())
    }

    pub fn cc_script_voter() -> Voter {
        Voter::ConstitutionalCommitteeScript([0xAAu8; 28].into())
    }

    pub fn cc_key_voter() -> Voter {
        Voter::ConstitutionalCommitteeKey([0xBBu8; 28].into())
    }

    /// A pool that votes once, in the last block of [`resolved_votes_cfg`].
    pub fn late_spo_voter() -> Voter {
        Voter::StakePoolKey([0x98u8; 28].into())
    }

    pub fn empty_update() -> ProtocolParamUpdate {
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

    /// Block 0 proposes two info actions, a treasury withdrawal and a
    /// parameter change. Every later block votes on action 0, and the DRep
    /// and the pool vote on it again two blocks later. Blocks 2 and 5 carry
    /// the DRep's votes on action 1 beside another voter's vote on action 0,
    /// so an oldest-first page that looks past itself reads blocks that the
    /// DRep's tag and action 0's tag both name, without the DRep's vote on
    /// action 0. Block 5 comes after the DRep's newest vote on action 0.
    pub fn resolved_votes_cfg() -> SyntheticBlockConfig {
        let withdrawal = GovAction::TreasuryWithdrawals(
            BTreeMap::from([(
                Bytes::from(
                    hex::decode("e0788cf0519348fefaf3c721c5f5bd60b195b444fa0d8fb4512dc259be")
                        .unwrap(),
                ),
                2000,
            )]),
            None,
        );

        let change = GovAction::ParameterChange(
            None,
            Box::new(ProtocolParamUpdate {
                minfee_a: Some(999),
                key_deposit: Some(5_000_000),
                ..empty_update()
            }),
            None,
        );

        SyntheticBlockConfig {
            block_count: 6,
            txs_per_block: 1,
            gov_actions_by_block: vec![
                vec![vec![
                    GovAction::Information,
                    GovAction::Information,
                    withdrawal,
                    change,
                ]],
                vec![],
                vec![],
                vec![],
                vec![],
                vec![],
            ],
            votes_by_block: vec![
                vec![],
                vec![vec![
                    synthetic_vote(cc_key_voter(), 0, 0, 0, Vote::Yes),
                    synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::Yes),
                    synthetic_vote(spo_voter(), 0, 0, 0, Vote::Abstain),
                ]],
                vec![vec![
                    synthetic_vote(cc_script_voter(), 0, 0, 0, Vote::No),
                    synthetic_vote(drep_key_voter(), 0, 0, 1, Vote::No),
                ]],
                vec![vec![synthetic_vote(
                    drep_key_voter(),
                    0,
                    0,
                    0,
                    Vote::Abstain,
                )]],
                vec![vec![synthetic_vote(spo_voter(), 0, 0, 0, Vote::Yes)]],
                vec![vec![
                    synthetic_vote(late_spo_voter(), 0, 0, 0, Vote::Yes),
                    synthetic_vote(drep_key_voter(), 0, 0, 1, Vote::Yes),
                ]],
            ],
            ..Default::default()
        }
    }

    /// Enacts every proposal in state at the boundary closing epoch 0, as
    /// EWRAP stamps it, and when `moved` moves each to the final-row log as
    /// the ESTART opening epoch 2 does.
    pub fn enact_proposals(domain: &ToyDomain, moved: bool) {
        let rows: Vec<(ProposalKey, ProposalState)> = domain
            .state()
            .iter_entities_typed::<ProposalState>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect();

        let writer = domain.state().start_writer().unwrap();

        for (key, row) in &rows {
            let mut row = row.clone();
            row.ratified_epoch = Some(0);
            writer.write_entity_typed(key, &row).unwrap();
        }

        writer.commit().unwrap();

        if moved {
            move_proposals_to_log(domain, true);
        }
    }

    /// Moves every proposal in state to the final-row log, keyed at the start
    /// of epoch 1, as the ESTART finalize commit does: the archive commits
    /// first, then the state deletes the rows. Without `commit_state` the
    /// state commit never runs, as after a crash between the two.
    pub fn move_proposals_to_log(domain: &ToyDomain, commit_state: bool) {
        let slot = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
            .unwrap()
            .epoch_start(1);

        let mut finals = FinalRows::default();

        for row in domain
            .state()
            .iter_entities_typed::<ProposalState>(None)
            .unwrap()
        {
            let (key, row) = row.unwrap();
            finals.push(slot, &key, &row);
        }

        assert!(!finals.is_empty(), "no proposal to move");

        let archive = domain.archive().start_writer().unwrap();
        let state = domain.state().start_writer().unwrap();
        finals.write(&state, &archive).unwrap();
        archive.commit().unwrap();

        if commit_state {
            state.commit().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{testing::*, *};
    use crate::mapping::{bech32, bech32_gov_action, parse_gov_action_id};
    use bech32::{Bech32, Hrp};
    use pallas::crypto::hash::Hash;

    #[test]
    fn governance_action_id_rejects_bech32m() {
        let tx = Hash::<32>::from([0x11u8; 32]);
        let payload = [tx.as_slice(), &[0]].concat();
        let hrp = Hrp::parse_unchecked("gov_action");

        let bech32m = bech32::encode::<bech32::Bech32m>(hrp, payload.as_slice())
            .expect("Cannot encode the Bech32m governance action ID.");
        assert_eq!(parse_gov_action_id(&bech32m), Err(StatusCode::BAD_REQUEST));

        // The same payload with the Bech32 checksum still parses.
        let canonical = bech32::encode::<Bech32>(hrp, payload.as_slice())
            .expect("Cannot encode the Bech32 governance action ID.");
        assert_eq!(parse_gov_action_id(&canonical).unwrap(), (tx, 0));
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
    /// trailing it. The first two vectors come from the upstream Blockfrost
    /// OpenAPI spec (see the crate README for the pinned link); the last one
    /// pins the minimal big-endian rule Blockfrost encodes the index with.
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

    #[test]
    fn collect_block_rows_exact_budget_and_extra_slot_without_fetch() {
        let mut fetched = Vec::new();
        let rows = collect_block_rows::<()>(
            (0..2).map(Ok),
            |slot| {
                fetched.push(slot);
                Ok(None)
            },
            false,
            1,
            &mut 2,
            |_| Ok(Vec::new()),
        )
        .unwrap();
        assert!(rows.is_empty());
        assert_eq!(fetched, [0, 1]);

        fetched.clear();
        let result = collect_block_rows::<()>(
            (0..3).map(Ok),
            |slot| {
                fetched.push(slot);
                Ok(None)
            },
            false,
            1,
            &mut 2,
            |_| Ok(Vec::new()),
        );
        assert!(matches!(result, Err(Error::ScanBudgetExceeded)));
        assert_eq!(fetched, [0, 1]);
    }

    #[test]
    fn collect_block_rows_full_page_and_zero_rows_do_not_pull_extra_slots() {
        let pulled = std::cell::Cell::new(0);
        let fetched = std::cell::Cell::new(0);
        let slots = (0..3).map(Ok).inspect(|_| pulled.set(pulled.get() + 1));
        let rows = collect_block_rows(
            slots,
            |_| {
                fetched.set(fetched.get() + 1);
                Ok(Some(drep_registration_block(true)))
            },
            false,
            1,
            &mut 1,
            |_| Ok(vec![()]),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(pulled.get(), 1);
        assert_eq!(fetched.get(), 1);

        let rows = collect_block_rows::<()>(
            std::iter::from_fn(|| -> Option<Result<BlockSlot, ArchiveError>> {
                panic!("The scan read a slot.")
            }),
            |_| panic!("The scan read a block body."),
            false,
            0,
            &mut 0,
            |_| panic!("The scan read a row."),
        )
        .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn collect_block_rows_errors_and_budget_precedence() {
        let fault = || ArchiveError::InternalError("The test caused an archive error.".into());
        let result = collect_block_rows::<()>(
            std::iter::once(Err(fault())),
            |_| panic!("The scan read a block body."),
            false,
            1,
            &mut 0,
            |_| Ok(Vec::new()),
        );
        assert!(matches!(result, Err(Error::ScanBudgetExceeded)));
        let result = collect_block_rows::<()>(
            std::iter::once(Err(fault())),
            |_| panic!("The scan read a block body."),
            false,
            1,
            &mut 1,
            |_| Ok(Vec::new()),
        );
        assert!(matches!(
            result,
            Err(Error::Code(StatusCode::INTERNAL_SERVER_ERROR))
        ));
        let result = collect_block_rows::<()>(
            std::iter::once(Ok(0)),
            |_| Err(fault()),
            false,
            1,
            &mut 1,
            |_| Ok(Vec::new()),
        );
        assert!(matches!(
            result,
            Err(Error::Code(StatusCode::INTERNAL_SERVER_ERROR))
        ));
        let result = collect_block_rows::<()>(
            std::iter::once(Ok(0)),
            |_| Ok(Some(vec![0xFF; 8])),
            false,
            1,
            &mut 1,
            |_| Ok(Vec::new()),
        );
        assert!(matches!(
            result,
            Err(Error::Code(StatusCode::INTERNAL_SERVER_ERROR))
        ));
    }
}

/// A proposal that is submitted, voted on, enacted and moved to the final-row
/// log answers every proposal route as it did from its state row. The vote
/// routes now read the votes from the archive alone.
#[cfg(test)]
mod final_row_tests {
    use super::{testing::*, visit_proposals};
    use crate::{
        mapping::bech32_gov_action,
        test_support::{TestApp, TestDomainBuilder},
    };
    use blockfrost_openapi::models::{
        proposal_metadata_v2::ProposalMetadataV2, proposal_votes_inner::ProposalVotesInner,
    };
    use dolos_cardano::model::ProposalState;
    use dolos_core::{ArchiveStore as _, Domain as _, StateStore as _};
    use pallas::crypto::hash::Hash;
    use serde_json::Value;

    fn app(moved: bool) -> TestApp {
        TestApp::new_with_cfg_and_setup(resolved_votes_cfg(), |domain, _| {
            enact_proposals(domain, moved)
        })
    }

    /// The status and the JSON value of an answer, an empty body as `null`.
    /// Values rather than bytes, because `governance_description` orders its
    /// keys differently from one request to the next, on `main` too.
    async fn body(app: &TestApp, path: &str) -> (axum::http::StatusCode, Value) {
        let (status, bytes) = app.get_bytes(path).await;

        let value = match bytes.is_empty() {
            true => Value::Null,
            false => serde_json::from_slice(&bytes).unwrap(),
        };

        (status, value)
    }

    #[tokio::test]
    async fn a_moved_proposal_answers_as_its_state_row_did() {
        let live = app(false);
        let moved = app(true);

        let tx = tx_hash_of_block(&live, 0);
        assert_eq!(tx, tx_hash_of_block(&moved, 0));

        let id = |idx| bech32_gov_action(&tx.parse::<Hash<32>>().unwrap(), idx).unwrap();
        let (info, withdrawal, change) = (id(0), id(2), id(3));

        let mut paths = vec![
            "/governance/proposals".to_string(),
            "/governance/proposals?order=desc".to_string(),
            "/governance/proposals?count=1&page=2".to_string(),
            "/governance/committee/votes".to_string(),
            format!("/governance/proposals/{tx}/0"),
            format!("/governance/proposals/{tx}/1"),
            format!("/governance/proposals/{tx}/2"),
            format!("/governance/proposals/{tx}/3"),
            format!("/governance/proposals/{info}"),
            format!("/governance/proposals/{tx}/0/withdrawals"),
            format!("/governance/proposals/{tx}/0/parameters"),
            format!("/governance/proposals/{tx}/2/withdrawals"),
            format!("/governance/proposals/{withdrawal}/withdrawals"),
            format!("/governance/proposals/{tx}/3/parameters"),
            format!("/governance/proposals/{change}/parameters"),
            format!("/governance/proposals/{tx}/0/metadata"),
            format!("/governance/proposals/{info}/metadata"),
            format!("/governance/proposals/{info}/votes"),
            format!("/governance/proposals/{tx}/1/votes"),
        ];

        // every page of one and two rows, both ways: an oldest-first page
        // ends before the re-votes that supersede its rows
        for order in ["asc", "desc"] {
            for count in [1, 2] {
                for page in 1..=7 {
                    paths.push(format!(
                        "/governance/proposals/{tx}/0/votes?order={order}&count={count}&page={page}"
                    ));
                }
            }
        }

        for path in &paths {
            let expected = body(&live, path).await;
            assert!(
                expected.0.is_success() || expected.0 == 404,
                "{path}: {expected:?}"
            );
            assert_eq!(body(&moved, path).await, expected, "{path}");
        }

        // the payout, the parameter change and the anchor come from the
        // final row
        let (status, withdrawals) =
            body(&moved, &format!("/governance/proposals/{tx}/2/withdrawals")).await;
        assert_eq!(status, 200);
        assert!(withdrawals.as_array().is_some_and(|rows| !rows.is_empty()));

        let (status, _) = body(&moved, &format!("/governance/proposals/{tx}/3/parameters")).await;
        assert_eq!(status, 200);

        let (status, metadata) =
            body(&moved, &format!("/governance/proposals/{info}/metadata")).await;
        assert_eq!(status, 200);
        let metadata: ProposalMetadataV2 = serde_json::from_value(metadata).unwrap();
        assert_eq!(metadata.url, "https://example.invalid/proposal");

        // the votes the archive alone tells apart: the DRep's and the pool's
        // first votes were superseded two blocks later
        let (_, votes) = body(&moved, &format!("/governance/proposals/{tx}/0/votes")).await;
        let votes: Vec<ProposalVotesInner> = serde_json::from_value(votes).unwrap();
        let counted: Vec<bool> = votes.iter().map(|row| row.counted).collect();
        assert_eq!(counted, [true, false, false, true, true, true, true]);

        // the DRep's newest vote, on a page that ends before block 5, which
        // both tags name for the DRep's vote on action 1
        let path = format!("/governance/proposals/{tx}/0/votes?count=1&page=5");
        let (_, page) = body(&moved, &path).await;
        let page: Vec<ProposalVotesInner> = serde_json::from_value(page).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].voter, votes[1].voter);
        assert!(page[0].counted);
    }

    /// ESTART commits a final row before it deletes the state row, so a crash
    /// between the two leaves the proposal in both stores. The listing names
    /// it once.
    #[tokio::test]
    async fn a_proposal_in_both_stores_is_listed_once() {
        let live = app(false);
        let both = TestApp::new_with_cfg_and_setup(resolved_votes_cfg(), |domain, _| {
            enact_proposals(domain, false);
            move_proposals_to_log(domain, false);
        });

        for path in [
            "/governance/proposals",
            "/governance/proposals?order=desc",
            "/governance/proposals?count=1&page=2",
        ] {
            assert_eq!(body(&both, path).await, body(&live, path).await, "{path}");
        }
    }

    /// The proposal in both stores is visited from its state row, vote maps
    /// included.
    #[test]
    fn a_proposal_in_both_stores_is_visited_from_state() {
        let (domain, _) = TestDomainBuilder::new_with_synthetic(resolved_votes_cfg()).finish();
        enact_proposals(&domain, false);
        move_proposals_to_log(&domain, false);

        let in_state: Vec<ProposalState> = domain
            .state()
            .iter_entities_typed::<ProposalState>(None)
            .unwrap()
            .map(|row| row.unwrap().1)
            .collect();

        let logged = domain
            .archive()
            .iter_logs_typed::<ProposalState>(None)
            .unwrap()
            .count();

        assert_eq!(logged, in_state.len());
        assert!(in_state.iter().any(|row| !row.drep_votes.is_empty()));

        let mut visited = vec![];
        visit_proposals(&domain, |row| visited.push(row.clone())).unwrap();

        assert_eq!(visited, in_state);
    }
}
