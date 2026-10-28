use std::collections::{BTreeMap, BTreeSet, HashMap};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::proposal_votes_inner::{self, ProposalVotesInner};
use dolos_cardano::{
    indexes::CardanoArchiveIndexExt as _,
    model::{DRepKey, ProposalKey},
    pallas_extras,
};
use dolos_core::{
    ArchiveError, ArchiveStore as _, BlockSlot, ChainPoint, Domain, StateStore as _, TxOrder,
};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::{
            conway::{DRep, GovActionId, Vote, Voter},
            StakeCredential,
        },
        traverse::{MultiEraBlock, MultiEraTx},
    },
};

use crate::{
    error::Error,
    mapping::{
        bech32_committee_hot, bech32_drep, bech32_pool,
        governance::proposals::proposal_closed_epoch, i32_or_500, parse_gov_action_id, IntoModel,
    },
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

use super::{collect_block_rows, read_drep_row, read_proposal_row, DRepReads, ProposalRow};

struct ProposalVoteRow {
    tx: Hash<32>,
    tx_order: TxOrder,
    slot: BlockSlot,
    cert_index: usize,
    voter: Voter,
    vote: Vote,
    counted: bool,
}

/// The CIP-129 spelling Blockfrost gives each voter: a hot-credential id for
/// a committee member, a drep id for a DRep, the bare pool id for an SPO.
fn voter_model(voter: &Voter) -> Result<(proposal_votes_inner::VoterRole, String), StatusCode> {
    use proposal_votes_inner::VoterRole;

    let out = match voter {
        Voter::ConstitutionalCommitteeKey(hash) => (
            VoterRole::ConstitutionalCommittee,
            bech32_committee_hot(&StakeCredential::AddrKeyhash(*hash))?,
        ),
        Voter::ConstitutionalCommitteeScript(hash) => (
            VoterRole::ConstitutionalCommittee,
            bech32_committee_hot(&StakeCredential::ScriptHash(*hash))?,
        ),
        Voter::DRepKey(hash) => (VoterRole::Drep, bech32_drep(&DRep::Key(*hash))?),
        Voter::DRepScript(hash) => (VoterRole::Drep, bech32_drep(&DRep::Script(*hash))?),
        Voter::StakePoolKey(hash) => (VoterRole::Spo, bech32_pool(hash)?),
    };

    Ok(out)
}

fn proposal_vote_model(vote: &Vote) -> proposal_votes_inner::Vote {
    match vote {
        Vote::Yes => proposal_votes_inner::Vote::Yes,
        Vote::No => proposal_votes_inner::Vote::No,
        Vote::Abstain => proposal_votes_inner::Vote::Abstain,
    }
}

impl IntoModel<ProposalVotesInner> for ProposalVoteRow {
    type SortKey = ();

    fn into_model(self) -> Result<ProposalVotesInner, StatusCode> {
        let (voter_role, voter) = voter_model(&self.voter)?;

        Ok(ProposalVotesInner {
            tx_hash: hex::encode(self.tx),
            cert_index: i32_or_500(self.cert_index)?,
            voter_role,
            voter,
            vote: proposal_vote_model(&self.vote),
            counted: self.counted,
        })
    }
}

/// Scans the certificate blocks of the DRep for a deregistration between the
/// vote and `end`, through the `drep_certs` archive dimension.
///
/// A deregistration is always newer than the vote it drops, so its block is
/// in the archive whenever the block of the vote is. A tagged block that the
/// archive does not hold is a broken archive: the check fails loudly instead
/// of reporting the vote as counted.
fn scan_drep_deregistration<D: Domain>(
    domain: &D,
    drep_id: &[u8],
    vote_at: (BlockSlot, TxOrder),
    end: BlockSlot,
    budget: &mut usize,
) -> Result<bool, Error> {
    let slots = domain
        .archive()
        .slots_by_drep_certs(drep_id, vote_at.0, end)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for slot in slots {
        let slot = slot.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // The same budget the listing draws on: a page pays for every block
        // it reads, the certificate blocks of its DReps included.
        if *budget == 0 {
            return Err(Error::ScanBudgetExceeded);
        }

        *budget -= 1;

        let Some(body) = domain
            .archive()
            .get_block_by_slot(&slot)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        else {
            return Err(StatusCode::INTERNAL_SERVER_ERROR.into());
        };

        let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        for (tx_order, tx) in block.txs().iter().enumerate() {
            // a deregistration before the voting tx cannot drop the vote
            if !tx.is_valid() || (slot, tx_order as TxOrder) < vote_at {
                continue;
            }

            let deregisters = tx.certs().iter().any(|cert| {
                pallas_extras::cert_as_drep_unregistration(cert)
                    .is_some_and(|unreg| pallas_extras::drep_id_bytes(&unreg.cred) == drep_id)
            });

            if deregisters {
                return Ok(false);
            }
        }
    }

    Ok(true)
}

/// Whether a DRep's newest vote still counts toward the tally.
///
/// The vote stops counting when the DRep deregisters at or after casting it
/// while the proposal is still live; re-registering does not restore it. Once
/// the proposal closes the tally freezes, so only a deregistration at or
/// before `end` — the last slot before the close epoch, capped at the visible
/// tip — matters.
///
/// A deregistration in the voting tx also drops the vote. The ledger's GOV
/// rule adds the votes of a tx first. Then it removes every vote of each DRep
/// that the tx deregisters. Blockfrost checks only later txs and reports such
/// a vote as counted. Dolos follows the ledger here.
///
/// `DRepState.unregistered_at` holds the newest deregistration, so most rows
/// need no block read. Only when that deregistration sits past `end` can an
/// older one hide between the vote and `end`: that case scans the certificate
/// blocks of the DRep. A DRep that left state is read from its final row. A
/// registration after that makes a fresh row, without the deregistration the
/// DRep left under, so a vote cast before the row's registration scans the
/// certificate blocks too.
fn drep_vote_counts<D: Domain>(
    domain: &D,
    reads: &DRepReads,
    cred: &StakeCredential,
    vote_at: (BlockSlot, TxOrder),
    end: BlockSlot,
    budget: &mut usize,
) -> Result<bool, Error> {
    if end < vote_at.0 {
        return Ok(true);
    }

    let drep = DRepKey(pallas_extras::stake_cred_to_drep(cred));

    let Some(row) = read_drep_row(domain, reads, &drep)? else {
        return Ok(true);
    };

    let fresh_since_vote = row
        .registered_at
        .is_some_and(|registered| registered > vote_at);

    match row.unregistered_at {
        None if !fresh_since_vote => Ok(true),
        Some(dereg) if dereg < vote_at => Ok(true),
        Some((slot, _)) if slot <= end => Ok(false),
        _ => scan_drep_deregistration(
            domain,
            &pallas_extras::drep_id_bytes(cred),
            vote_at,
            end,
            budget,
        ),
    }
}

/// Returns the votes on `action` in one block, ordered the way Blockfrost
/// lists them.
///
/// Blockfrost returns rows in db-sync insertion order. Inside one block that
/// order is: first the position of the voting tx, then each tx's voters map.
/// The ledger sorts that map by voter kind — committee, then DRep, then
/// pool — with script credentials before key credentials. Pallas declares
/// the `Voter` enum variants in that same order, so iterating the `BTreeMap`
/// here visits voters exactly as db-sync inserts them.
///
/// `cert_index` is db-sync's `voting_procedure.index`: the position of the
/// action inside that voter's vote map, counted per voter rather than per tx.
///
/// The ledger ignores the votes of a phase-2-invalid transaction, so the
/// function skips those transactions.
fn proposal_votes_in_block(block: &MultiEraBlock, action: &GovActionId) -> Vec<ProposalVoteRow> {
    let slot = block.slot();
    let mut out = Vec::new();

    for (tx_order, tx) in block.txs().into_iter().enumerate() {
        if !tx.is_valid() {
            continue;
        }

        let tx_hash = tx.hash();

        let MultiEraTx::Conway(conway_tx) = tx else {
            continue;
        };

        let Some(procedures) = &conway_tx.transaction_body.voting_procedures else {
            continue;
        };

        for (voter, votes) in procedures.iter() {
            for (cert_index, (gov_action_id, procedure)) in votes.iter().enumerate() {
                if gov_action_id != action {
                    continue;
                }

                out.push(ProposalVoteRow {
                    tx: tx_hash,
                    tx_order: tx_order as TxOrder,
                    slot,
                    cert_index,
                    voter: voter.clone(),
                    vote: procedure.vote.clone(),
                    counted: false,
                });
            }
        }
    }

    out
}

/// How many of a voter's tagged slots in a row, none of them an action slot,
/// the walk reads before it seeks the voter's tag at the next action slot.
/// A seek opens a store iterator and costs about as much as reading a run
/// of entries from an open one.
const MISSES_BEFORE_SEEK: usize = 16;

/// The first slot at or after `from` that both `action_slots` and a voter's
/// `voter_votes` tag name, where `voter_slots(start, end)` reads that tag.
///
/// The voter's tag is read from the next action slot up to the last one, so
/// nothing outside the action's own votes ahead is read. A run of
/// [`MISSES_BEFORE_SEEK`] voter slots that the action does not name ends in a
/// seek at the next action slot. Each seek passes at least one action slot,
/// so a voter costs at most that many reads per vote block of the action
/// ahead, however often it votes on other actions.
fn next_shared_slot<I>(
    action_slots: &BTreeSet<BlockSlot>,
    mut from: BlockSlot,
    mut voter_slots: impl FnMut(BlockSlot, BlockSlot) -> Result<I, ArchiveError>,
) -> Result<Option<BlockSlot>, Error>
where
    I: Iterator<Item = Result<BlockSlot, ArchiveError>>,
{
    let Some(&last) = action_slots.last() else {
        return Ok(None);
    };

    'seek: while let Some(&next) = action_slots.range(from..).next() {
        let voter = voter_slots(next, last).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        for (read, slot) in voter.enumerate() {
            let slot = slot.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            if action_slots.contains(&slot) {
                return Ok(Some(slot));
            }

            if read + 1 == MISSES_BEFORE_SEEK {
                from = slot;
                continue 'seek;
            }
        }

        return Ok(None);
    }

    Ok(None)
}

/// The newest vote of each voter on a resolved proposal, found by the
/// `action_votes` walk instead of the vote maps its final row dropped.
///
/// A page reads a prefix of the listing, in whole blocks. Read newest first,
/// that prefix holds every vote newer than a row on the page. Read oldest
/// first, a newer vote can sit past it, in a block that both the action's
/// `action_votes` tag and the voter's `voter_votes` tag name
/// ([`next_shared_slot`]), so the check reads only those blocks. No vote
/// lands on a proposal once it is resolved, and the archive holds its votes
/// by the boundary after, so nothing newer hides past the visible tip.
///
/// The blocks the check reads draw on the request's budget, as the listing's
/// do. So any oldest-first page of a resolved proposal, the first one
/// included, can exceed a budget that the same page of its live row fits in.
/// The check reads no more blocks than the action has vote blocks up to the
/// visible tip.
struct WalkedNewest<'a, D: Domain> {
    domain: &'a D,
    key: ProposalKey,
    action: GovActionId,

    /// The newest position of each voter among the rows the page read.
    read: BTreeMap<Voter, (BlockSlot, TxOrder)>,

    /// The slots past the rows read, inclusive, where an oldest-first page
    /// looks for newer votes; `None` newest first.
    ahead: Option<(BlockSlot, BlockSlot)>,

    /// The action's tagged slots in [`Self::ahead`], read on first use.
    action_slots: Option<BTreeSet<BlockSlot>>,

    /// The blocks past the rows read, by slot, read on first use.
    blocks: HashMap<BlockSlot, Vec<ProposalVoteRow>>,
}

impl<'a, D: Domain> WalkedNewest<'a, D> {
    fn new(
        domain: &'a D,
        key: &ProposalKey,
        action: &GovActionId,
        rows: &[ProposalVoteRow],
        descending: bool,
        visible: BlockSlot,
    ) -> Self {
        let mut read: BTreeMap<Voter, (BlockSlot, TxOrder)> = BTreeMap::new();

        for row in rows {
            let position = (row.slot, row.tx_order);
            let newest = read.entry(row.voter.clone()).or_insert(position);
            *newest = (*newest).max(position);
        }

        let last = rows.iter().map(|row| row.slot).max();

        let ahead = match last {
            Some(last) if !descending && last < visible => Some((last + 1, visible)),
            _ => None,
        };

        Self {
            domain,
            key: *key,
            action: action.clone(),
            read,
            ahead,
            action_slots: None,
            blocks: HashMap::new(),
        }
    }

    /// Whether `row` holds its voter's newest vote on the action.
    fn is_newest(&mut self, row: &ProposalVoteRow, budget: &mut usize) -> Result<bool, Error> {
        let position = (row.slot, row.tx_order);

        if self
            .read
            .get(&row.voter)
            .is_some_and(|newest| *newest > position)
        {
            return Ok(false);
        }

        let Some((from, to)) = self.ahead else {
            return Ok(true);
        };

        let archive = self.domain.archive();

        if self.action_slots.is_none() {
            let slots = archive
                .slots_by_action_votes(&self.key, from, to)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .collect::<Result<BTreeSet<_>, _>>()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            self.action_slots = Some(slots);
        }

        let action_slots = self.action_slots.as_ref().expect("read above");
        let voter = pallas_extras::voter_id_bytes(&row.voter);
        let mut from = from;

        while let Some(slot) = next_shared_slot(action_slots, from, |start, end| {
            archive.slots_by_voter_votes(&voter, start, end)
        })? {
            from = slot + 1;

            if !self.blocks.contains_key(&slot) {
                // The same budget the listing draws on.
                if *budget == 0 {
                    return Err(Error::ScanBudgetExceeded);
                }

                *budget -= 1;

                // A tagged block the archive no longer holds leaves the
                // walk, as it leaves the listing.
                let rows = match archive
                    .get_block_by_slot(&slot)
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                {
                    Some(body) => {
                        let block = MultiEraBlock::decode(&body)
                            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                        proposal_votes_in_block(&block, &self.action)
                    }
                    None => vec![],
                };

                self.blocks.insert(slot, rows);
            }

            if self.blocks[&slot]
                .iter()
                .any(|later| later.voter == row.voter)
            {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

const STABLE_CURSOR_ATTEMPTS: usize = 3;

/// Whether two cursors are the same commit: same variant, slot and hash.
///
/// `ChainPoint`'s own equality lets `Specific(slot, hash)` match `Slot(slot)`,
/// and the epoch-start commit writes a `Slot` cursor at the slot of the block
/// before it, so that equality would miss a state change at the same slot.
fn same_cursor(a: &ChainPoint, b: &ChainPoint) -> bool {
    match (a, b) {
        (ChainPoint::Origin, ChainPoint::Origin) => true,
        (ChainPoint::Slot(a), ChainPoint::Slot(b)) => a == b,
        (ChainPoint::Specific(a, hash_a), ChainPoint::Specific(b, hash_b)) => {
            a == b && hash_a == hash_b
        }
        _ => false,
    }
}

/// Runs `read` between two reads of the state cursor, and repeats it while
/// the cursor moves. `read` gets the cursor it runs at; the result is that
/// cursor and what `read` returned.
///
/// The state commits the cursor and its entities in one batch, so an
/// unchanged cursor shows that `read` saw the state at that cursor.
fn read_at_stable_cursor<T, E: From<StatusCode>>(
    mut read_cursor: impl FnMut() -> Result<ChainPoint, StatusCode>,
    mut read: impl FnMut(&ChainPoint) -> Result<T, E>,
) -> Result<(ChainPoint, T), E> {
    for _ in 0..STABLE_CURSOR_ATTEMPTS {
        let before = read_cursor()?;
        let value = read(&before)?;

        if same_cursor(&read_cursor()?, &before) {
            return Ok((before, value));
        }
    }

    Err(StatusCode::INTERNAL_SERVER_ERROR.into())
}

/// One page of a proposal's votes.
///
/// The listing is a vote history, not a tally: when a voter votes again,
/// the new vote appears as an extra row and the old row stays. Blockfrost
/// works this way because db-sync keeps every `voting_procedure` it sees,
/// even ones the ledger no longer counts.
///
/// The `action_votes` archive dimension gives the blocks that hold votes on
/// the proposal. The function reads blocks only until the page is full, and
/// at most `budget` of them. A block that the archive no longer holds leaves
/// the list, and the votes after it move up, like the other history endpoints
/// under rolling history.
///
/// Only the newest vote of a voter can count. A live proposal's state row
/// holds that vote's position (`VoteEntry`); a resolved proposal's final row
/// has no vote maps, and the walk finds it instead ([`WalkedNewest`]). Either
/// way only the rows on the page pay for the `counted` check.
///
/// The `counted` flags compare the archive rows with the proposal row and
/// with the DRep rows, so the whole page is read between two reads of the
/// state cursor and read again if the cursor moved: one response describes
/// one chain state, never the rows of one and the flags of another. The
/// budget spans the attempts, so a request reads at most `budget` blocks
/// however often it repeats.
fn read_votes<D: Domain>(
    domain: &D,
    tx: Hash<32>,
    idx: u32,
    pagination: &Pagination,
    mut budget: usize,
) -> Result<Vec<ProposalVotesInner>, Error> {
    let (_, page) = read_at_stable_cursor(
        || {
            domain
                .state()
                .read_cursor()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
        },
        |cursor| read_votes_at(domain, tx, idx, pagination, &mut budget, cursor.slot()),
    )?;

    Ok(page)
}

/// The page of [`read_votes`] as the state at `cursor` describes it, drawing
/// on what is left of `budget`.
fn read_votes_at<D: Domain>(
    domain: &D,
    tx: Hash<32>,
    idx: u32,
    pagination: &Pagination,
    budget: &mut usize,
    cursor: BlockSlot,
) -> Result<Vec<ProposalVotesInner>, Error> {
    let key = ProposalKey::new(tx, idx);

    // Blockfrost joins against db-sync and sends whatever rows come back, so
    // an unknown proposal is an empty listing rather than a 404.
    let Some(proposal) = read_proposal_row(domain, &key)? else {
        return Ok(vec![]);
    };

    // The sync commits the state before the archive, on a roll-forward and on
    // a rollback. So for a short time the two stores can disagree: the state
    // can be ahead of the archive, and the archive can hold blocks that a
    // rollback already took out of the state. The listing and the
    // deregistration reads stay at or below the lower tip, so one response
    // reads one consistent chain.
    let Some((archive_tip, _)) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Ok(vec![]);
    };

    let visible = archive_tip.min(cursor);

    let archive = domain.archive();

    let slots = archive
        .slots_by_action_votes(&key, 0, visible)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let descending = matches!(pagination.order, Order::Desc);

    let slots: Box<dyn Iterator<Item = _>> = if descending {
        Box::new(slots.rev())
    } else {
        Box::new(slots)
    };

    let action = GovActionId {
        transaction_id: tx,
        action_index: idx,
    };

    let rows = collect_block_rows(
        slots,
        |slot| archive.get_block_by_slot(&slot),
        descending,
        pagination.to(),
        budget,
        |block| Ok(proposal_votes_in_block(block, &action)),
    )?;

    let chain = dolos_cardano::eras::load_era_summary::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (current_epoch, _) = chain.slot_epoch(cursor);

    // The last slot whose deregistration can still drop a vote: the one
    // before the epoch that the proposal closes in, capped at the visible
    // tip.
    let end = match proposal_closed_epoch(proposal.state(), current_epoch) {
        Some(closed) => chain.epoch_start(closed).saturating_sub(1),
        None => BlockSlot::MAX,
    }
    .min(visible);

    let mut walked = matches!(proposal, ProposalRow::Final(_))
        .then(|| WalkedNewest::new(domain, &key, &action, &rows, descending, visible));

    let page = rows
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count);

    let reads = DRepReads::new(&chain);
    let mut out = Vec::new();

    for mut row in page {
        // Only the newest vote of a voter can count. The vote map of a live
        // proposal's row pins its position; a final row's comes from the walk.
        row.counted = match walked.as_mut() {
            Some(walked) => walked.is_newest(&row, budget)?,
            None => proposal
                .state()
                .vote_entry(&row.voter)
                .is_some_and(|entry| {
                    entry.newest.0 == row.slot && entry.newest_order == row.tx_order
                }),
        };

        let drep = match &row.voter {
            Voter::DRepKey(hash) => Some(StakeCredential::AddrKeyhash(*hash)),
            Voter::DRepScript(hash) => Some(StakeCredential::ScriptHash(*hash)),
            _ => None,
        };

        // Only the DRep rows on the page pay for the deregistration check.
        if row.counted {
            if let Some(cred) = drep {
                row.counted =
                    drep_vote_counts(domain, &reads, &cred, (row.slot, row.tx_order), end, budget)?;
            }
        }

        out.push(row.into_model()?);
    }

    Ok(out)
}

/// `GET /governance/proposals/{tx_hash}/{cert_index}/votes`: every vote cast
/// on the proposal, oldest first.
///
/// `max_scan_items` limits both the page depth and the number of blocks that
/// one request reads.
pub async fn proposal_votes<D>(
    Path((tx_hash, cert_index)): Path<(String, String)>,
    Query(mut params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ProposalVotesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    // Drop `from`/`to` before validation: Blockfrost never reads them here,
    // so a malformed or reversed window is ignored rather than rejected.
    params.from = None;
    params.to = None;

    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let cert_index = cert_index
        .parse::<u32>()
        .map_err(|_| Error::InvalidCertIndex)?;

    // Blockfrost matches the hash as text against db-sync, so a malformed one
    // is a listing that matches nothing rather than a bad request.
    let Ok(tx) = tx_hash.parse::<Hash<32>>() else {
        return Ok(Json(vec![]));
    };

    let budget = domain.config.max_scan_items() as usize;

    let page = domain
        .query()
        .run_blocking(move |domain| Ok(read_votes(&domain, tx, cert_index, &pagination, budget)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(Json(page))
}

/// `GET /governance/proposals/{gov_action_id}/votes`: the same listing,
/// addressed by CIP-129 id instead of by tx hash and action index.
pub async fn proposal_votes_by_gov_action<D>(
    Path(gov_action_id): Path<String>,
    Query(mut params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ProposalVotesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    // Drop `from`/`to` before validation: Blockfrost never reads them here,
    // so a malformed or reversed window is ignored rather than rejected.
    params.from = None;
    params.to = None;

    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let (tx, idx) = parse_gov_action_id(&gov_action_id).map_err(|_| Error::InvalidGovActionId)?;

    let budget = domain.config.max_scan_items() as usize;

    let page = domain
        .query()
        .run_blocking(move |domain| Ok(read_votes(&domain, tx, idx, &pagination, budget)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(Json(page))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDomainBuilder;
    use crate::{
        mapping::{bech32, bech32_gov_action},
        routes::governance::testing::*,
        test_support::{TestApp, TestFault},
    };
    use dolos_cardano::model::{DRepState, ProposalState};
    use dolos_core::StateWriter as _;
    use dolos_testing::{
        measured::MeasuredStores,
        synthetic::{SyntheticBlockConfig, SyntheticVectors},
        toy_domain::{MemoryStores, ToyDomain},
    };
    use itertools::Itertools;
    use pallas::ledger::primitives::{conway::GovAction, Epoch};

    /// Block 0 proposes three actions in one tx. Block 1 votes on action 0
    /// with all three voter roles — its drep votes on action 1 too, so the
    /// per-voter cert index shows. Block 2 carries the same drep re-voting on
    /// action 0. Action 2 collects no votes.
    fn vote_cfg() -> SyntheticBlockConfig {
        SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 1,
            gov_actions_by_block: vec![
                vec![vec![
                    GovAction::Information,
                    GovAction::NoConfidence(None),
                    GovAction::Information,
                ]],
                vec![],
                vec![],
            ],
            votes_by_block: vec![
                vec![],
                vec![vec![
                    synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::Yes),
                    synthetic_vote(drep_key_voter(), 0, 0, 1, Vote::No),
                    synthetic_vote(cc_key_voter(), 0, 0, 0, Vote::Yes),
                    synthetic_vote(cc_script_voter(), 0, 0, 0, Vote::No),
                    synthetic_vote(spo_voter(), 0, 0, 0, Vote::Abstain),
                ]],
                vec![vec![synthetic_vote(
                    drep_key_voter(),
                    0,
                    0,
                    0,
                    Vote::Abstain,
                )]],
            ],
            ..Default::default()
        }
    }

    fn vote_app() -> TestApp {
        TestApp::new_with_cfg(vote_cfg())
    }

    /// Reads proposal 0 of a vote fixture, changes it with `change`, and
    /// writes it back.
    fn edit_vote_proposal(
        domain: &ToyDomain,
        vectors: &dolos_testing::synthetic::SyntheticVectors,
        change: impl FnOnce(&mut ProposalState),
    ) {
        let tx: Hash<32> = vectors.blocks[0].tx_hashes[0]
            .parse()
            .expect("failed to parse the proposal tx hash");
        let key = ProposalKey::new(tx, 0);

        let mut state = domain
            .state()
            .read_entity_typed::<ProposalState>(&key)
            .expect("failed to read the proposal")
            .expect("the proposal is missing");

        change(&mut state);

        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");
        writer
            .write_entity_typed(&key, &state)
            .expect("failed to write the proposal");
        writer.commit().expect("failed to commit the proposal");
    }

    /// Reads the DRep state of `cred`, changes it with `change`, and writes
    /// it back.
    fn edit_drep_state(
        domain: &ToyDomain,
        cred: &StakeCredential,
        change: impl FnOnce(&mut DRepState),
    ) {
        let key = DRepKey(pallas_extras::stake_cred_to_drep(cred));

        let mut state = domain
            .state()
            .read_entity_typed::<DRepState>(&key)
            .expect("failed to read the drep")
            .expect("the drep is missing");

        change(&mut state);

        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");
        writer
            .write_entity_typed(&key, &state)
            .expect("failed to write the drep");
        writer.commit().expect("failed to commit the drep");
    }

    async fn get_votes(app: &TestApp, path: &str) -> Vec<ProposalVotesInner> {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("failed to parse votes")
    }

    fn voter_id(voter: &Voter) -> String {
        voter_model(voter).expect("failed to encode voter").1
    }

    /// The action-0 listing in ascending order: block 1's rows sit in ledger
    /// voter order — committee script, committee key, drep, pool — and block
    /// 2's re-vote trails them. Only the drep's block-1 vote is superseded,
    /// so it alone reads as not counted.
    fn expected_votes(app: &TestApp) -> Vec<ProposalVotesInner> {
        let first_vote_tx = tx_hash_of_block(app, 1);
        let second_vote_tx = tx_hash_of_block(app, 2);

        let row = |tx: &str, voter: &Voter, vote: &Vote, counted: bool| ProposalVotesInner {
            tx_hash: tx.to_string(),
            cert_index: 0,
            voter_role: voter_model(voter).expect("failed to encode voter").0,
            voter: voter_id(voter),
            vote: proposal_vote_model(vote),
            counted,
        };

        vec![
            row(&first_vote_tx, &cc_script_voter(), &Vote::No, true),
            row(&first_vote_tx, &cc_key_voter(), &Vote::Yes, true),
            row(&first_vote_tx, &drep_key_voter(), &Vote::Yes, false),
            row(&first_vote_tx, &spo_voter(), &Vote::Abstain, true),
            row(&second_vote_tx, &drep_key_voter(), &Vote::Abstain, true),
        ]
    }

    #[tokio::test]
    async fn governance_proposal_votes_happy_path() {
        let app = vote_app();
        let proposal_tx = tx_hash_of_block(&app, 0);

        let path = format!("/governance/proposals/{proposal_tx}/0/votes");
        assert_eq!(get_votes(&app, &path).await, expected_votes(&app));

        // the drep's vote on action 1 is its second procedure in the voting
        // tx: the cert index counts inside the voter's vote map
        let path = format!("/governance/proposals/{proposal_tx}/1/votes");
        let rows = get_votes(&app, &path).await;
        assert_eq!(
            rows,
            vec![ProposalVotesInner {
                tx_hash: tx_hash_of_block(&app, 1),
                cert_index: 1,
                voter_role: proposal_votes_inner::VoterRole::Drep,
                voter: voter_id(&drep_key_voter()),
                vote: proposal_votes_inner::Vote::No,
                counted: true,
            }]
        );
    }

    #[tokio::test]
    async fn governance_proposal_votes_orders_and_paginates() {
        let app = vote_app();
        let proposal_tx = tx_hash_of_block(&app, 0);
        let base = format!("/governance/proposals/{proposal_tx}/0/votes");
        let expected = expected_votes(&app);

        let desc = get_votes(&app, &format!("{base}?order=desc")).await;
        assert_eq!(
            desc,
            expected.iter().rev().cloned().collect_vec(),
            "desc is the ascending listing read backwards"
        );

        let page = get_votes(&app, &format!("{base}?count=2&page=2")).await;
        assert_eq!(page, expected[2..4].to_vec());

        // a page crossing from block 1's rows into block 2's
        let page = get_votes(&app, &format!("{base}?count=3&page=2")).await;
        assert_eq!(page, expected[3..].to_vec());

        let page = get_votes(&app, &format!("{base}?count=1&page=1&order=desc")).await;
        assert_eq!(page, expected[4..].to_vec());

        // a page past the end is empty, not an error
        let page = get_votes(&app, &format!("{base}?page=7")).await;
        assert!(page.is_empty());
    }

    #[tokio::test]
    async fn governance_proposal_votes_rejects_deep_page() {
        let app = TestApp::new_with_scan_limit(vote_cfg(), 3);
        let proposal_tx = tx_hash_of_block(&app, 0);
        let path = format!("/governance/proposals/{proposal_tx}/0/votes?count=2&page=2");

        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    /// Block 0 proposes an action, and the two txs of block 1 each carry a
    /// vote of the same DRep on it.
    fn revote_in_one_block_cfg() -> SyntheticBlockConfig {
        SyntheticBlockConfig {
            block_count: 2,
            txs_per_block: 2,
            gov_actions_by_block: vec![vec![vec![GovAction::Information], vec![]], vec![]],
            votes_by_block: vec![
                vec![],
                vec![
                    vec![synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::Yes)],
                    vec![synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::No)],
                ],
            ],
            ..Default::default()
        }
    }

    /// Two txs of one block carry a vote of the same DRep on the same
    /// proposal. Both rows are listed, and only the later one counts.
    #[tokio::test]
    async fn governance_proposal_votes_revote_in_one_block() {
        let app = TestApp::new_with_cfg(revote_in_one_block_cfg());

        let proposal_tx = tx_hash_of_block(&app, 0);
        let path = format!("/governance/proposals/{proposal_tx}/0/votes");
        let rows = get_votes(&app, &path).await;

        let txs = &app.vectors().blocks[1].tx_hashes;
        let summary = rows
            .iter()
            .map(|row| (row.tx_hash.clone(), row.vote, row.counted))
            .collect_vec();

        assert_eq!(
            summary,
            [
                (txs[0].clone(), proposal_votes_inner::Vote::Yes, false),
                (txs[1].clone(), proposal_votes_inner::Vote::No, true),
            ]
        );
    }

    /// The re-vote of the test above on a resolved proposal: the walk orders
    /// the DRep's two rows by tx within the block, as the vote map did.
    #[tokio::test]
    async fn governance_proposal_votes_revote_in_one_block_of_a_resolved_proposal() {
        let app = |moved| {
            TestApp::new_with_cfg_and_setup(revote_in_one_block_cfg(), move |domain, _| {
                enact_proposals(domain, moved)
            })
        };
        let (live, moved) = (app(false), app(true));

        let proposal_tx = tx_hash_of_block(&live, 0);
        let base = format!("/governance/proposals/{proposal_tx}/0/votes");

        for path in [base.clone(), format!("{base}?order=desc")] {
            let rows = get_votes(&moved, &path).await;
            assert_eq!(rows, get_votes(&live, &path).await, "{path}");
        }

        let rows = get_votes(&moved, &base).await;
        assert_eq!(
            rows.iter().map(|row| row.counted).collect_vec(),
            [false, true]
        );
    }

    /// The ledger ignores the votes of a phase-2-invalid tx, so they are not
    /// listed, although their block is tagged for the action.
    #[tokio::test]
    async fn governance_proposal_votes_skip_invalid_transactions() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 2,
            txs_per_block: 2,
            gov_actions_by_block: vec![vec![vec![GovAction::Information], vec![]], vec![]],
            votes_by_block: vec![
                vec![],
                vec![
                    vec![synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::Yes)],
                    vec![synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::No)],
                ],
            ],
            invalid_txs_by_block: vec![vec![], vec![1]],
            ..Default::default()
        });

        let proposal_tx = tx_hash_of_block(&app, 0);
        let path = format!("/governance/proposals/{proposal_tx}/0/votes");
        let rows = get_votes(&app, &path).await;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_hash, app.vectors().blocks[1].tx_hashes[0]);
        assert!(rows[0].counted);
    }

    /// Blockfrost joins the proposal against db-sync's voting table and sends
    /// whatever comes back, so everything that names no vote — an unvoted
    /// proposal, an unknown one, an unreadable hash — is the same empty
    /// listing rather than a 404.
    #[tokio::test]
    async fn governance_proposal_votes_without_rows() {
        let app = vote_app();
        let proposal_tx = tx_hash_of_block(&app, 0);

        // action 2 exists and nobody voted on it
        let path = format!("/governance/proposals/{proposal_tx}/2/votes");
        assert!(get_votes(&app, &path).await.is_empty());

        // an index the tx never proposed at
        let path = format!("/governance/proposals/{proposal_tx}/9/votes");
        assert!(get_votes(&app, &path).await.is_empty());

        let missing = hex::encode([0u8; 32]);
        let path = format!("/governance/proposals/{missing}/0/votes");
        assert!(get_votes(&app, &path).await.is_empty());

        let path = "/governance/proposals/not-a-tx-hash/0/votes";
        assert!(get_votes(&app, path).await.is_empty());
    }

    /// Blockfrost declares no `from`/`to` for these endpoints, so a malformed
    /// or reversed window is ignored, not rejected.
    #[tokio::test]
    async fn governance_proposal_votes_ignores_from_and_to() {
        let app = vote_app();
        let tx: Hash<32> = tx_hash_of_block(&app, 0)
            .parse()
            .expect("failed to parse tx hash");
        let expected = expected_votes(&app);

        let by_tx = format!("/governance/proposals/{tx}/0/votes");
        let by_id = format!(
            "/governance/proposals/{}/votes",
            bech32_gov_action(&tx, 0).unwrap()
        );

        for base in [by_tx, by_id] {
            for query in ["?from=bad", "?to=bad", "?from=200&to=100"] {
                assert_eq!(
                    get_votes(&app, &format!("{base}{query}")).await,
                    expected,
                    "{base}{query} must ignore the window"
                );
            }
        }
    }

    #[tokio::test]
    async fn governance_proposal_votes_by_gov_action_id() {
        let app = vote_app();
        let tx: Hash<32> = tx_hash_of_block(&app, 0)
            .parse()
            .expect("failed to parse tx hash");
        let expected = expected_votes(&app);

        let id = bech32_gov_action(&tx, 0).unwrap();
        let path = format!("/governance/proposals/{id}/votes");
        assert_eq!(get_votes(&app, &path).await, expected);

        // the same listing, paginated the same way
        let page = get_votes(&app, &format!("{path}?order=desc&count=2")).await;
        assert_eq!(page, expected[3..].iter().rev().cloned().collect_vec());

        // the bare-hash form explorers write for index 0 names the same
        // proposal as the one-byte form Blockfrost writes
        let minimal = bech32(bech32::Hrp::parse("gov_action").unwrap(), tx.as_slice()).unwrap();
        let path = format!("/governance/proposals/{minimal}/votes");
        assert_eq!(get_votes(&app, &path).await, expected);

        // a well-formed id for a proposal nobody made
        let id = bech32_gov_action(&Hash::from([0u8; 32]), 0).unwrap();
        let path = format!("/governance/proposals/{id}/votes");
        assert!(get_votes(&app, &path).await.is_empty());
    }

    #[tokio::test]
    async fn governance_proposal_votes_bad_request() {
        let app = vote_app();
        let tx = tx_hash_of_block(&app, 0);
        let base = format!("/governance/proposals/{tx}/0/votes");

        assert_status(&app, &format!("{base}?count=0"), StatusCode::BAD_REQUEST).await;
        assert_status(
            &app,
            &format!("{base}?order=sideways"),
            StatusCode::BAD_REQUEST,
        )
        .await;

        // the cert index is the only path part that has to be a number
        let path = format!("/governance/proposals/{tx}/x/votes");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;

        for id in ["not-bech32", &missing_drep()] {
            let path = format!("/governance/proposals/{id}/votes");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn governance_proposal_votes_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = format!("/governance/proposals/{}/0/votes", hex::encode([1u8; 32]));
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;

        let id = bech32_gov_action(&Hash::from([1u8; 32]), 0).unwrap();
        let path = format!("/governance/proposals/{id}/votes");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    /// Pruning takes a vote's block out of the archive. The listing
    /// paginates the retained votes only, like the DRep and pool vote
    /// listings.
    #[tokio::test]
    async fn governance_proposal_votes_paginates_retained_rows() {
        let app = TestApp::new_with_cfg_and_setup(vote_cfg(), |domain, _| {
            domain
                .archive()
                .prune_history(0, None, None)
                .expect("The archive did not prune its history.");
        });

        let proposal_tx = tx_hash_of_block(&app, 0);
        let base = format!("/governance/proposals/{proposal_tx}/0/votes");
        let expected = expected_votes(&app);

        // block 1's four rows are gone; block 2's re-vote is the listing
        assert_eq!(get_votes(&app, &base).await, expected[4..].to_vec());

        // the surviving row is on page 1, not at its original offset
        let rows = get_votes(&app, &format!("{base}?count=1")).await;
        assert_eq!(rows, expected[4..].to_vec());

        assert!(get_votes(&app, &format!("{base}?count=1&page=2"))
            .await
            .is_empty());
    }

    /// The sync commits the state before the archive. So for a short time
    /// the state holds a newest vote whose block the archive does not have
    /// yet. The listing leaves out that vote, and the rows it supersedes
    /// read as not counted.
    #[tokio::test]
    async fn governance_proposal_votes_stop_at_the_archive_tip() {
        use dolos_core::ChainPoint;

        let app = TestApp::new_with_cfg_and_setup(vote_cfg(), |domain, vectors| {
            let next_slot = vectors.blocks[2].slot + 1;

            edit_vote_proposal(domain, vectors, |state| {
                let entry = state
                    .drep_votes
                    .get_mut(&StakeCredential::AddrKeyhash([0x66u8; 28].into()))
                    .expect("the drep has no votes");
                entry.newest = (next_slot, Vote::No);
                entry.newest_order = 0;
            });

            // the state has already committed the block of that vote
            let writer = domain
                .state()
                .start_writer()
                .expect("failed to start writer");
            writer
                .set_cursor(ChainPoint::Slot(next_slot))
                .expect("failed to move the cursor");
            writer.commit().expect("failed to commit the cursor");
        });

        let proposal_tx = tx_hash_of_block(&app, 0);
        let path = format!("/governance/proposals/{proposal_tx}/0/votes");

        let mut expected = expected_votes(&app);
        expected[4].counted = false;

        assert_eq!(get_votes(&app, &path).await, expected);
    }

    /// Block 0 registers the DRep and proposes. In block 1, a committee
    /// member and the DRep vote. The setup then tags the next slot as a
    /// certificate block of the DRep and moves the DRep's newest
    /// deregistration past the visible tip, so the `counted` check must scan
    /// the tagged blocks. With `body`, the archive holds that body at the
    /// tagged slot. Without it, the slot has no block, and an untagged filler
    /// block after it keeps the slot below the archive tip. Both make the
    /// check of the DRep vote fail with 500.
    fn vote_app_with_extra_drep_tag(body: Option<Vec<u8>>) -> TestApp {
        use dolos_cardano::indexes::archive_dimensions::DREP_CERTS;
        use dolos_core::{ArchiveIndexDelta, ArchiveWriter as _, ChainPoint, Tag};
        use pallas::ledger::primitives::conway::Certificate;

        let cred = StakeCredential::AddrKeyhash([0x66u8; 28].into());
        let mut extra_certs_by_block = vec![vec![vec![]]; 2];
        extra_certs_by_block[0][0].push(Certificate::RegDRepCert(cred.clone(), 500, None));

        let cfg = SyntheticBlockConfig {
            block_count: 2,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![GovAction::Information]], vec![]],
            votes_by_block: vec![
                vec![],
                vec![vec![
                    synthetic_vote(cc_key_voter(), 0, 0, 0, Vote::Yes),
                    synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::Yes),
                ]],
            ],
            extra_certs_by_block,
            ..Default::default()
        };

        TestApp::new_with_cfg_and_setup(cfg, move |domain, vectors| {
            let slot = vectors.blocks[1].slot + 1;
            let hash = Hash::<32>::from([0xEEu8; 32]);

            let top = match body {
                Some(body) => (slot, hash, body),
                None => (slot + 1, Hash::<32>::from([0xEFu8; 32]), vec![0xFF; 8]),
            };
            let (top_slot, top_hash, top_body) = top;

            let writer = domain
                .archive()
                .start_writer()
                .expect("failed to start writer");
            writer
                .apply(
                    &ChainPoint::Specific(top_slot, top_hash),
                    &std::sync::Arc::new(top_body),
                )
                .expect("failed to write the block");
            writer
                .apply_index(&[ArchiveIndexDelta {
                    slot,
                    block_hash: hash.to_vec(),
                    block_number: None,
                    tx_hashes: vec![],
                    tags: vec![Tag::new(DREP_CERTS, pallas_extras::drep_id_bytes(&cred))],
                }])
                .expect("failed to tag the block");
            writer.commit().expect("failed to commit the block");

            // The state cursor moves up to the archive tip, so the tagged slot
            // is inside the chain that the response reads.
            let writer = domain
                .state()
                .start_writer()
                .expect("failed to start writer");
            writer
                .set_cursor(ChainPoint::Specific(top_slot, top_hash))
                .expect("failed to move the cursor");
            writer.commit().expect("failed to commit the cursor");

            // The newest deregistration sits past the visible tip. An older
            // one can hide below it, so the check scans the tagged blocks.
            edit_drep_state(domain, &cred, |state| {
                state.unregistered_at = Some((top_slot + 100, 0));
            });
        })
    }

    /// Only the rows on the page check `counted`. So a page without the DRep
    /// row never reads the certificate blocks of the DRep.
    #[tokio::test]
    async fn governance_proposal_votes_check_only_the_page() {
        // a block that cannot be decoded
        let app = vote_app_with_extra_drep_tag(Some(vec![0xFF; 8]));
        let proposal_tx = tx_hash_of_block(&app, 0);
        let base = format!("/governance/proposals/{proposal_tx}/0/votes");

        // the committee row alone; the DRep vote in the same block is off
        // the page
        let rows = get_votes(&app, &format!("{base}?count=1")).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].voter, voter_id(&cc_key_voter()));
        assert!(rows[0].counted);

        // the DRep row reads the corrupt block
        assert_status(
            &app,
            &format!("{base}?count=2"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }

    /// A tagged certificate block that the archive does not hold could hide a
    /// deregistration. So the DRep row fails loudly instead of reporting the
    /// vote as counted.
    #[tokio::test]
    async fn governance_proposal_votes_missing_drep_block() {
        let app = vote_app_with_extra_drep_tag(None);
        let proposal_tx = tx_hash_of_block(&app, 0);
        let base = format!("/governance/proposals/{proposal_tx}/0/votes");

        assert_status(
            &app,
            &format!("{base}?count=2"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }

    /// A rollback commits the state before the archive. So for a short time
    /// the archive still holds a block that the state already took back. A
    /// deregistration in that block must not drop the vote.
    #[tokio::test]
    async fn governance_proposal_votes_stop_at_the_state_cursor() {
        use dolos_core::ChainPoint;
        use pallas::ledger::primitives::conway::Certificate;

        let cred = StakeCredential::AddrKeyhash([0x66u8; 28].into());
        let mut extra_certs_by_block = vec![vec![vec![]]; 3];
        extra_certs_by_block[0][0].push(Certificate::RegDRepCert(cred.clone(), 500, None));
        extra_certs_by_block[2][0].push(Certificate::UnRegDRepCert(cred, 500));

        let cfg = SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![GovAction::Information]], vec![], vec![]],
            votes_by_block: vec![
                vec![],
                vec![vec![synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::Yes)]],
                vec![],
            ],
            extra_certs_by_block,
            ..Default::default()
        };

        // the state steps back to block 1, as a rollback of block 2 leaves it
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let block = &vectors.blocks[1];
            let hash: Hash<32> = block
                .block_hash
                .parse()
                .expect("failed to parse the block hash");

            let writer = domain
                .state()
                .start_writer()
                .expect("failed to start writer");
            writer
                .set_cursor(ChainPoint::Specific(block.slot, hash))
                .expect("failed to move the cursor");
            writer.commit().expect("failed to commit the cursor");
        });

        let proposal_tx = tx_hash_of_block(&app, 0);
        let path = format!("/governance/proposals/{proposal_tx}/0/votes");
        let rows = get_votes(&app, &path).await;
        assert_eq!(rows.iter().map(|row| row.counted).collect_vec(), [true]);
    }

    /// The vote fixture with a fourth, empty block. The tx of block 0
    /// registers the voting DRep, and the tx of block `dereg_block`
    /// deregisters it. When `closed_epoch` is set, the setup marks proposal 0
    /// as ratified in that epoch, which closes it.
    fn vote_app_with_drep_dereg(dereg_block: usize, closed_epoch: Option<Epoch>) -> TestApp {
        use pallas::ledger::primitives::conway::Certificate;

        let cred = StakeCredential::AddrKeyhash([0x66u8; 28].into());
        let mut extra_certs_by_block = vec![vec![vec![]]; 4];
        extra_certs_by_block[0][0].push(Certificate::RegDRepCert(cred.clone(), 500, None));
        extra_certs_by_block[dereg_block][0].push(Certificate::UnRegDRepCert(cred, 500));

        let cfg = SyntheticBlockConfig {
            block_count: 4,
            txs_per_block: 1,
            gov_actions_by_block: vec![
                vec![vec![
                    GovAction::Information,
                    GovAction::NoConfidence(None),
                    GovAction::Information,
                ]],
                vec![],
                vec![],
                vec![],
            ],
            votes_by_block: vec![
                vec![],
                vec![vec![
                    synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::Yes),
                    synthetic_vote(drep_key_voter(), 0, 0, 1, Vote::No),
                    synthetic_vote(cc_key_voter(), 0, 0, 0, Vote::Yes),
                    synthetic_vote(cc_script_voter(), 0, 0, 0, Vote::No),
                    synthetic_vote(spo_voter(), 0, 0, 0, Vote::Abstain),
                ]],
                vec![vec![synthetic_vote(
                    drep_key_voter(),
                    0,
                    0,
                    0,
                    Vote::Abstain,
                )]],
                vec![],
            ],
            extra_certs_by_block,
            ..Default::default()
        };

        TestApp::new_with_cfg_and_setup(cfg, move |domain, vectors| {
            if let Some(closed_epoch) = closed_epoch {
                edit_vote_proposal(domain, vectors, |state| {
                    state.ratified_epoch = Some(closed_epoch);
                });
            }
        })
    }

    async fn counted_on(app: &TestApp, action: u32) -> Vec<bool> {
        let proposal_tx = tx_hash_of_block(app, 0);
        let path = format!("/governance/proposals/{proposal_tx}/{action}/votes");
        get_votes(app, &path)
            .await
            .iter()
            .map(|row| row.counted)
            .collect()
    }

    /// A deregistration after the newest vote, or in the same tx, drops it
    /// from the tally while the proposal is live; one before the vote changes
    /// nothing.
    #[tokio::test]
    async fn governance_proposal_votes_drep_deregistration() {
        // deregistered after every vote: the drep's newest votes stop
        // counting on both proposals; the superseded one already did not
        let app = vote_app_with_drep_dereg(3, None);
        assert_eq!(counted_on(&app, 0).await, [true, true, false, true, false]);
        assert_eq!(counted_on(&app, 1).await, [false]);

        // deregistered before the votes: the newest votes still count
        let app = vote_app_with_drep_dereg(0, None);
        assert_eq!(counted_on(&app, 0).await, [true, true, false, true, true]);

        // deregistered in the tx of the newest vote: the ledger drops that
        // vote, while Blockfrost still counts it
        let app = vote_app_with_drep_dereg(2, None);
        assert_eq!(counted_on(&app, 0).await, [true, true, false, true, false]);
    }

    /// Once a proposal closes, its tally is frozen: a deregistration in the
    /// close epoch or later does not drop the vote.
    #[tokio::test]
    async fn governance_proposal_votes_freeze_at_close() {
        let app = vote_app_with_drep_dereg(3, None);
        let dereg_epoch = app.vectors().blocks[3].slot / 86_400;

        let app = vote_app_with_drep_dereg(3, Some(dereg_epoch));
        assert_eq!(counted_on(&app, 0).await, [true, true, false, true, true]);
    }

    /// Every voter role in its CIP-129 spelling: one header byte — key type,
    /// then key or script — before the credential hash, except the pool id,
    /// which Blockfrost sends bare.
    #[test]
    fn voter_ids_follow_cip129() {
        let cases = [
            (
                cc_script_voter(),
                proposal_votes_inner::VoterRole::ConstitutionalCommittee,
                "cc_hot1qw42424242424242424242424242424242424242424242slw89ck",
            ),
            (
                cc_key_voter(),
                proposal_votes_inner::VoterRole::ConstitutionalCommittee,
                "cc_hot1q2amhwamhwamhwamhwamhwamhwamhwamhwamhwamhwamhwc56yc3q",
            ),
            (
                drep_key_voter(),
                proposal_votes_inner::VoterRole::Drep,
                "drep1yfnxvenxvenxvenxvenxvenxvenxvenxvenxvenxvenxvesyutktf",
            ),
            (
                spo_voter(),
                proposal_votes_inner::VoterRole::Spo,
                "pool1nxvenxvenxvenxvenxvenxvenxvenxvenxvenxvenxvejwc07h9",
            ),
        ];

        let actual = cases
            .iter()
            .map(|(voter, _, _)| voter_model(voter).expect("failed to encode voter"))
            .collect_vec();

        let expected = cases
            .iter()
            .map(|(_, role, id)| (*role, id.to_string()))
            .collect_vec();

        assert_eq!(actual, expected);
    }

    /// Plays `cursors` as the successive cursor reads and counts the reads of
    /// the value between them.
    fn stable_cursor_read(cursors: &[ChainPoint]) -> (Result<ChainPoint, StatusCode>, usize) {
        use std::cell::Cell;

        let next_cursor = Cell::new(0);
        let reads = Cell::new(0);

        let result = read_at_stable_cursor(
            || {
                let i = next_cursor.get();
                next_cursor.set(i + 1);
                Ok(cursors[i].clone())
            },
            |cursor| {
                reads.set(reads.get() + 1);
                assert_eq!(
                    cursor,
                    &cursors[next_cursor.get() - 1],
                    "read sees its cursor"
                );
                Ok::<_, StatusCode>(reads.get())
            },
        );

        let result = result.map(|(cursor, read)| {
            assert_eq!(read, reads.get(), "the result comes from the last read");
            cursor
        });

        (result, reads.get())
    }

    /// The sync can commit a block between the read of a proposal row and the
    /// read of the cursor. The row then belongs to another chain than the
    /// listing, so the read runs again until the cursor holds still.
    #[test]
    fn stable_cursor_read_repeats_while_the_cursor_moves() {
        let point = |slot: u64, hash: u8| ChainPoint::Specific(slot, Hash::from([hash; 32]));

        // a still cursor needs one read
        let (cursor, reads) = stable_cursor_read(&[point(10, 1), point(10, 1)]);
        assert_eq!((cursor, reads), (Ok(point(10, 1)), 1));

        // a roll-forward during the first read
        let (cursor, reads) =
            stable_cursor_read(&[point(10, 1), point(11, 2), point(11, 2), point(11, 2)]);
        assert_eq!((cursor, reads), (Ok(point(11, 2)), 2));

        // a rollback to another block at the same slot
        let (cursor, reads) =
            stable_cursor_read(&[point(11, 2), point(11, 3), point(11, 3), point(11, 3)]);
        assert_eq!((cursor, reads), (Ok(point(11, 3)), 2));

        // an epoch-start commit at the slot of the block before it: the
        // `Slot` cursor it writes is a new state, though `ChainPoint` calls
        // it equal to the block's `Specific` cursor
        let boundary = ChainPoint::Slot(11);
        let (cursor, reads) = stable_cursor_read(&[
            point(11, 2),
            boundary.clone(),
            boundary.clone(),
            boundary.clone(),
        ]);
        assert_eq!((cursor, reads), (Ok(boundary), 2));

        // a cursor that never holds still fails the request
        let moving = (0..6).map(|slot| point(slot, slot as u8)).collect_vec();
        let (cursor, reads) = stable_cursor_read(&moving);
        assert_eq!(
            (cursor, reads),
            (
                Err(StatusCode::INTERNAL_SERVER_ERROR),
                STABLE_CURSOR_ATTEMPTS
            )
        );
    }

    /// A read repeated by a moving cursor keeps drawing on the same budget.
    #[test]
    fn stable_cursor_read_shares_the_budget_across_attempts() {
        use std::cell::Cell;

        let point = |slot: u64, hash: u8| ChainPoint::Specific(slot, Hash::from([hash; 32]));
        let cursors = [point(10, 1), point(11, 2), point(11, 2), point(11, 2)];
        let next_cursor = Cell::new(0);
        let mut budget = 3usize;

        let result = read_at_stable_cursor(
            || {
                let i = next_cursor.get();
                next_cursor.set(i + 1);
                Ok(cursors[i].clone())
            },
            |_| {
                // a page that reads two blocks
                for _ in 0..2 {
                    if budget == 0 {
                        return Err(Error::ScanBudgetExceeded);
                    }
                    budget -= 1;
                }
                Ok(())
            },
        );

        // the second attempt finds one block of budget left, not three
        assert!(matches!(result, Err(Error::ScanBudgetExceeded)));
        assert_eq!(budget, 0);
    }

    /// A chain where the DRep's newest deregistration sits past the visible
    /// tip while `tagged` of its certificate blocks sit between its vote and
    /// the tip, so the `counted` check of its row scans every one of them.
    fn drep_scan_domain(tagged: usize) -> (ToyDomain, SyntheticVectors) {
        use dolos_cardano::indexes::archive_dimensions::DREP_CERTS;
        use dolos_core::{ArchiveIndexDelta, ArchiveStore as _, ArchiveWriter as _, Tag};
        use pallas::ledger::primitives::conway::Certificate;

        let cred = StakeCredential::AddrKeyhash([0x66u8; 28].into());
        let mut extra_certs_by_block = vec![vec![vec![]]; 2];
        extra_certs_by_block[0][0].push(Certificate::RegDRepCert(cred.clone(), 500, None));

        let cfg = SyntheticBlockConfig {
            block_count: 2,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![GovAction::Information]], vec![]],
            votes_by_block: vec![
                vec![],
                vec![vec![
                    synthetic_vote(cc_key_voter(), 0, 0, 0, Vote::Yes),
                    synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::Yes),
                ]],
            ],
            extra_certs_by_block,
            ..Default::default()
        };

        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
        let vote_slot = vectors.blocks[1].slot;

        // the voting block carries no certificate, so a copy of it is a
        // certificate block the scan reads through without a verdict
        let body = domain
            .archive()
            .get_block_by_slot(&vote_slot)
            .expect("failed to read the voting block")
            .expect("the voting block is missing");

        let mut top = None;
        let writer = domain
            .archive()
            .start_writer()
            .expect("failed to start writer");
        for i in 1..=tagged {
            let slot = vote_slot + i as u64;
            let hash = Hash::<32>::from([0xE0 + i as u8; 32]);
            writer
                .apply(
                    &ChainPoint::Specific(slot, hash),
                    &std::sync::Arc::new(body.clone()),
                )
                .expect("failed to write the block");
            writer
                .apply_index(&[ArchiveIndexDelta {
                    slot,
                    block_hash: hash.to_vec(),
                    block_number: None,
                    tx_hashes: vec![],
                    tags: vec![Tag::new(DREP_CERTS, pallas_extras::drep_id_bytes(&cred))],
                }])
                .expect("failed to tag the block");
            top = Some(ChainPoint::Specific(slot, hash));
        }
        writer.commit().expect("failed to commit the blocks");

        let top = top.expect("at least one tagged block");
        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");
        writer
            .set_cursor(top.clone())
            .expect("failed to move the cursor");
        writer.commit().expect("failed to commit the cursor");

        edit_drep_state(&domain, &cred, |state| {
            state.unregistered_at = Some((top.slot() + 100, 0));
        });

        (domain, vectors)
    }

    #[test]
    fn governance_proposal_votes_deregistration_scan_draws_on_the_budget() {
        let (domain, vectors) = drep_scan_domain(2);
        let tx: Hash<32> = vectors.blocks[0].tx_hashes[0]
            .parse()
            .expect("failed to parse the proposal tx");
        let page = |count| Pagination {
            count,
            ..Default::default()
        };

        // one block for the listing, two for the DRep's certificate blocks
        let rows = read_votes(&domain, tx, 0, &page(2), 3).expect("the page fits the budget");
        assert_eq!(
            rows.iter().map(|row| row.counted).collect_vec(),
            [true, true]
        );

        // the listing fits, the certificate scan does not
        assert!(matches!(
            read_votes(&domain, tx, 0, &page(2), 2),
            Err(Error::ScanBudgetExceeded)
        ));

        // the committee row alone reads no certificate block
        let rows = read_votes(&domain, tx, 0, &page(1), 1).expect("the page fits the budget");
        assert_eq!(rows.len(), 1);
    }

    /// Runs [`next_shared_slot`] from slot 0 over a voter tag held in memory,
    /// and returns every shared slot, the seeks of the tag and the entries
    /// pulled from it.
    fn shared_slots(action: &[BlockSlot], voter: &[BlockSlot]) -> (Vec<BlockSlot>, usize, usize) {
        use std::cell::Cell;

        let action: BTreeSet<BlockSlot> = action.iter().copied().collect();
        let seeks = Cell::new(0);
        let pulled = Cell::new(0);

        let mut shared = vec![];
        let mut from = 0;

        while let Some(slot) = next_shared_slot(&action, from, |start, end| {
            seeks.set(seeks.get() + 1);

            Ok(voter
                .iter()
                .copied()
                .filter(move |slot| (start..=end).contains(slot))
                .inspect(|_| pulled.set(pulled.get() + 1))
                .map(Ok))
        })
        .expect("the walk failed")
        {
            shared.push(slot);
            from = slot + 1;
        }

        (shared, seeks.get(), pulled.get())
    }

    /// A voter's walk reads its tag only over the action's own vote blocks
    /// ahead, at most [`MISSES_BEFORE_SEEK`] entries per block, never its
    /// votes on other actions before or after them.
    #[test]
    fn next_shared_slot_walks_the_action_slots() {
        let others = (2..=11).collect_vec();

        // nothing on the action ahead: no seek at all
        assert_eq!(shared_slots(&[], &others), (vec![], 0, 0));

        // one action slot past ten votes on other actions: one seek, which
        // finds nothing from that slot on
        assert_eq!(shared_slots(&[12], &others), (vec![], 1, 0));

        // the voter's slots between the action's are read in one pass
        assert_eq!(shared_slots(&[2, 5, 9], &[3, 4, 9]), (vec![9], 1, 3));

        // a dense action and a sparse voter: the voter's slots bound the walk
        let dense = (1..=100).collect_vec();
        assert_eq!(shared_slots(&dense, &[50, 150]), (vec![50], 2, 1));

        // every shared slot, in order
        assert_eq!(
            shared_slots(&[1, 2, 3, 7], &[2, 3, 4, 7]),
            (vec![2, 3, 7], 3, 3)
        );

        // a long run of votes on other actions between two action slots ends
        // in a seek at the next one, whatever the run's length
        let run = (11..=999).collect_vec();
        let misses = MISSES_BEFORE_SEEK;
        assert_eq!(shared_slots(&[10, 1000], &run), (vec![], 2, misses));

        let run_then_vote = (11..=1000).collect_vec();
        assert_eq!(
            shared_slots(&[10, 1000], &run_then_vote),
            (vec![1000], 2, misses + 1)
        );

        // the seek after a run starts past the run's last slot, not at the
        // action slot the run passed without the voter
        let run_past_15 = (11..=14).chain(16..=999).collect_vec();
        assert_eq!(
            shared_slots(&[10, 15, 1000], &run_past_15),
            (vec![], 2, misses)
        );
    }

    /// Block 0 proposes two actions, block 1 holds the DRep's vote on action
    /// 0, and blocks 2 to 11 hold its votes on action 1.
    fn other_action_votes_cfg() -> SyntheticBlockConfig {
        let mut gov_actions_by_block = vec![vec![]; 12];
        gov_actions_by_block[0] = vec![vec![GovAction::Information, GovAction::Information]];

        let mut votes_by_block = vec![vec![]; 12];
        votes_by_block[1] = vec![vec![synthetic_vote(drep_key_voter(), 0, 0, 0, Vote::Yes)]];

        for block in &mut votes_by_block[2..] {
            *block = vec![vec![synthetic_vote(drep_key_voter(), 0, 0, 1, Vote::No)]];
        }

        SyntheticBlockConfig {
            block_count: 12,
            txs_per_block: 1,
            gov_actions_by_block,
            votes_by_block,
            ..Default::default()
        }
    }

    /// The DRep's later votes on another action leave nothing for the walk of
    /// a resolved proposal to read ([`next_shared_slot_walks_the_action_slots`]
    /// counts the seeks), and the listing answers as its live row did.
    #[tokio::test]
    async fn governance_proposal_votes_walk_past_votes_on_other_actions() {
        let app = |moved| {
            TestApp::new_with_cfg_and_setup(other_action_votes_cfg(), move |domain, _| {
                enact_proposals(domain, moved)
            })
        };
        let (live, moved) = (app(false), app(true));

        let proposal_tx = tx_hash_of_block(&live, 0);
        let path = format!("/governance/proposals/{proposal_tx}/0/votes");
        let rows = get_votes(&moved, &path).await;

        assert_eq!(rows, get_votes(&live, &path).await);
        assert_eq!(rows.iter().map(|row| row.counted).collect_vec(), [true]);
    }

    /// The page of the test above reads as many tag entries from the resolved
    /// proposal as from its live row: the walk reads none of the DRep's ten
    /// later votes on action 1.
    #[test]
    fn governance_proposal_votes_walk_reads_no_tag_entry_past_the_action() {
        let tag_entries = |moved| {
            let (domain, vectors) =
                TestDomainBuilder::<MeasuredStores<MemoryStores>>::new_with_synthetic_in(
                    other_action_votes_cfg(),
                    |_| (),
                )
                .finish();
            enact_proposals(&domain, moved);

            let tx: Hash<32> = vectors.blocks[0].tx_hashes[0]
                .parse()
                .expect("failed to parse the proposal tx");
            let counters = &domain.archive().counters;

            let before = counters.snapshot().tag_candidates;
            let rows = read_votes(&domain, tx, 0, &Pagination::default(), 100)
                .expect("the page fits the budget");
            assert_eq!(rows.iter().map(|row| row.counted).collect_vec(), [true]);

            counters.snapshot().tag_candidates - before
        };

        assert_eq!(tag_entries(true), tag_entries(false));
    }

    /// Block 0 proposes two actions, block 1 holds the DRep's vote on action
    /// 0 and block 6 the pool's. Blocks 2 to 5 and 7 to 11 hold the DRep's
    /// votes on action 1.
    fn vote_ahead_cfg() -> SyntheticBlockConfig {
        let mut cfg = other_action_votes_cfg();
        cfg.votes_by_block[6] = vec![vec![synthetic_vote(spo_voter(), 0, 0, 0, Vote::Yes)]];
        cfg
    }

    /// An oldest-first page of one row, over block 1, has one vote block of
    /// the action ahead. The walk of the resolved proposal reads that slot
    /// from the action's tag, and none of the DRep's votes on action 1 before
    /// or after it.
    #[test]
    fn governance_proposal_votes_walk_reads_the_voter_tag_only_at_action_slots_ahead() {
        let tag_entries = |moved| {
            let (domain, vectors) =
                TestDomainBuilder::<MeasuredStores<MemoryStores>>::new_with_synthetic_in(
                    vote_ahead_cfg(),
                    |_| (),
                )
                .finish();
            enact_proposals(&domain, moved);

            let tx: Hash<32> = vectors.blocks[0].tx_hashes[0]
                .parse()
                .expect("failed to parse the proposal tx");
            let page = Pagination {
                count: 1,
                ..Default::default()
            };
            let counters = &domain.archive().counters;

            let before = counters.snapshot().tag_candidates;
            let rows = read_votes(&domain, tx, 0, &page, 100).expect("the page fits the budget");
            assert_eq!(rows.iter().map(|row| row.counted).collect_vec(), [true]);

            counters.snapshot().tag_candidates - before
        };

        assert_eq!(tag_entries(true), tag_entries(false) + 1);
    }

    /// The walk of a resolved proposal reads its blocks from the request's
    /// budget. The DRep row of an oldest-first page over block 1 walks
    /// block 2, which both tags name, and block 3, which holds its re-vote:
    /// three blocks with the listing's one. The live row's vote map needs
    /// none of them.
    #[test]
    fn governance_proposal_votes_walk_draws_on_the_budget() {
        let domain = |moved| {
            let (domain, vectors) =
                TestDomainBuilder::new_with_synthetic(resolved_votes_cfg()).finish();
            enact_proposals(&domain, moved);
            (domain, vectors)
        };

        let (moved, vectors) = domain(true);
        let tx: Hash<32> = vectors.blocks[0].tx_hashes[0]
            .parse()
            .expect("failed to parse the proposal tx");
        let page = Pagination {
            count: 2,
            ..Default::default()
        };

        let rows = read_votes(&moved, tx, 0, &page, 3).expect("the page fits the budget");
        assert_eq!(
            rows.iter().map(|row| row.counted).collect_vec(),
            [true, false]
        );

        assert!(matches!(
            read_votes(&moved, tx, 0, &page, 2),
            Err(Error::ScanBudgetExceeded)
        ));

        let (live, _) = domain(false);
        let rows = read_votes(&live, tx, 0, &page, 1).expect("the page fits the budget");
        assert_eq!(
            rows.iter().map(|row| row.counted).collect_vec(),
            [true, false]
        );
    }

    /// Block 0 proposes an action. One tx of block 1 holds the DRep's and the
    /// pool's votes on it, and one tx of block 2 holds both their re-votes.
    fn shared_revote_cfg() -> SyntheticBlockConfig {
        let both = |vote: Vote| {
            vec![vec![
                synthetic_vote(drep_key_voter(), 0, 0, 0, vote.clone()),
                synthetic_vote(spo_voter(), 0, 0, 0, vote),
            ]]
        };

        SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![GovAction::Information]], vec![], vec![]],
            votes_by_block: vec![vec![], both(Vote::Yes), both(Vote::No)],
            ..Default::default()
        }
    }

    /// The rows of one page share the blocks their walks read. An
    /// oldest-first page over block 1 holds the DRep's and the pool's votes,
    /// and both walks find a re-vote in block 2. The page reads block 2 once
    /// more than its live row does, and charges it once: two blocks with the
    /// listing's one.
    #[test]
    fn governance_proposal_votes_walks_of_one_page_read_a_block_once() {
        let read = |moved, budget| {
            let (domain, vectors) =
                TestDomainBuilder::<MeasuredStores<MemoryStores>>::new_with_synthetic_in(
                    shared_revote_cfg(),
                    |_| (),
                )
                .finish();
            enact_proposals(&domain, moved);

            let tx: Hash<32> = vectors.blocks[0].tx_hashes[0]
                .parse()
                .expect("failed to parse the proposal tx");
            let page = Pagination {
                count: 2,
                ..Default::default()
            };
            let counters = &domain.archive().counters;

            let before = counters.snapshot().block_reads;
            let counted = read_votes(&domain, tx, 0, &page, budget)
                .map(|rows| rows.iter().map(|row| row.counted).collect_vec());

            (counted, counters.snapshot().block_reads - before)
        };

        let (live, live_reads) = read(false, 1);
        assert_eq!(live.expect("the page fits the budget"), [false, false]);

        let (moved, moved_reads) = read(true, 2);
        assert_eq!(moved.expect("the page fits the budget"), [false, false]);
        assert_eq!(moved_reads, live_reads + 1);

        assert!(matches!(read(true, 1).0, Err(Error::ScanBudgetExceeded)));
    }
}
