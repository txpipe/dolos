mod mapping;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use self::mapping::description_json;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    committee::Committee,
    committee_members_inner::{CommitteeMembersInner, Status},
    committee_quorum::CommitteeQuorum,
    committee_votes_inner::{self, CommitteeVotesInner},
    drep_delegators_inner::DrepDelegatorsInner,
    drep_metadata::DrepMetadata,
    drep_votes_inner::{self, DrepVotesInner},
    proposal::{self, Proposal},
    proposal_metadata::ProposalMetadata,
    proposal_metadata_v2::ProposalMetadataV2,
    proposal_parameters::ProposalParameters,
    proposal_parameters_parameters::ProposalParametersParameters,
    proposal_withdrawals_inner::ProposalWithdrawalsInner,
    proposals_inner::{GovernanceType, ProposalsInner},
    DrepsInnerMetadataError,
};
use dolos_cardano::{
    model::{
        drep_from_entity_key,
        gov::{CommitteeAuthorization, GovState},
        AccountState, DRepState, FixedNamespace as _, ProposalAction, ProposalState,
        SingletonEntity as _,
    },
    pallas_extras, ChainSummary, PParamsSet,
};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain, EntityKey, StateStore as _, TxOrder};
use itertools::Itertools;
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::Network,
        primitives::{
            conway::{Anchor, DRep, GovAction, Vote, Voter},
            Coin, Epoch, StakeCredential,
        },
        traverse::{MultiEraBlock, MultiEraTx},
    },
};

use crate::{
    error::Error,
    log_and_500,
    mapping::{
        anchor_offchain_metadata, bech32, bech32_committee_cold, bech32_committee_hot,
        bech32_gov_action, i32_or_500, parse_committee_id, parse_gov_action_id,
        rational_to_f64_unrounded, stake_cred_to_address, AnchorMetadata, CommitteeCredentialRole,
        IntoModel, Unrounded,
    },
    pagination::{Order, Pagination, PaginationParameters},
    routes::epochs::mapping::{map_cost_models_raw, protocol_params_model},
    Facade,
};

fn parse_drep_id(drep_id: &str) -> Result<(String, Vec<u8>, bool, bool), StatusCode> {
    match drep_id {
        "drep_always_abstain" => Ok((drep_id.to_string(), vec![0], false, true)),
        "drep_always_no_confidence" => Ok((drep_id.to_string(), vec![1], false, true)),
        drep_id => {
            // Blockfrost decodes a DRep id as bech32 and does no other check.
            // As a result, a hex id is a 400, not a lookup. This rule keeps
            // the same behavior as Blockfrost.
            let (hrp, payload) = bech32::decode(drep_id).map_err(|_| StatusCode::BAD_REQUEST)?;

            match (hrp.as_str(), payload.len()) {
                ("drep", 29) => {
                    let header_byte = *payload.first().ok_or(StatusCode::BAD_REQUEST)?;

                    // A CIP-129 DRep header is the key prefix or the script prefix.
                    if header_byte != pallas_extras::DREP_KEY_PREFIX
                        && header_byte != pallas_extras::DREP_SCRIPT_PREFIX
                    {
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

/// `GET /governance/dreps/{drep_id}/metadata`: the registered anchor of the
/// DRep, and the off-chain metadata for that anchor.
///
/// db-sync returns the latest registration or update that still has an
/// anchor. Its query uses an inner join to `voting_anchor`. Then it takes
/// the newest row. Dolos obeys the ledger rules and clears the anchor on a
/// registration or update that has no anchor. As a result, Blockfrost keeps
/// the older anchor, but Dolos answers 404.
///
/// A DRep with no current anchor is a 404, not an empty body. The two
/// special DReps have no registration, so they are also a 404.
///
/// If the fetch fails, Dolos keeps the anchor and puts the cause in `error`,
/// like the proposal metadata endpoints. The hex and the DRep id use the
/// same CIP-129 or legacy form as `drep_by_id`.
pub async fn drep_metadata<D: Domain>(
    Path(drep): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<DrepMetadata>, StatusCode>
where
    Option<DRepState>: From<D::Entity>,
{
    let (drep_id, drep_bytes, is_legacy, is_special_case) = parse_drep_id(&drep)?;

    // The special DReps have no registration, so they have no anchor.
    if is_special_case {
        return Err(StatusCode::NOT_FOUND);
    }

    let state = domain
        .read_cardano_entity::<DRepState>(drep_bytes.clone())?
        .ok_or(StatusCode::NOT_FOUND)?;

    let anchor = state.anchor.as_ref().ok_or(StatusCode::NOT_FOUND)?;

    let hex = if is_legacy {
        hex::encode(&drep_bytes[1..])
    } else {
        hex::encode(&drep_bytes)
    };

    let hash = hex::encode(anchor.content_hash);

    let gateways = domain.config.ipfs_gateways();
    let (metadata, error) =
        anchor_offchain_metadata(&anchor.url, anchor.content_hash.as_ref(), &gateways).await;

    let (json_metadata, bytes) = match metadata {
        Some(AnchorMetadata { json, bytes }) => (Some(json), Some(bytes)),
        None => (None, None),
    };

    Ok(Json(DrepMetadata {
        drep_id,
        hex,
        url: anchor.url.clone(),
        hash,
        json_metadata,
        bytes,
        error: error.map(Box::new),
    }))
}

struct DrepDelegatorModelBuilder {
    delegator: StakeCredential,
    live_stake: u64,
    network: Network,
}

impl IntoModel<DrepDelegatorsInner> for DrepDelegatorModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<DrepDelegatorsInner, StatusCode> {
        let address = stake_cred_to_address(&self.delegator, self.network)
            .to_bech32()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(DrepDelegatorsInner {
            address,
            amount: self.live_stake.to_string(),
        })
    }
}

/// One account that delegates its vote to the requested DRep.
struct DrepDelegatorRow {
    delegated_at: Option<(BlockSlot, TxOrder)>,
    key: EntityKey,
    delegator: StakeCredential,
    live_stake: u64,
}

impl DrepDelegatorRow {
    fn new(key: EntityKey, account: AccountState) -> Self {
        Self {
            delegated_at: account.vote_delegated_at,
            key,
            live_stake: account.live_stake(),
            delegator: account.credential,
        }
    }

    /// Blockfrost orders delegators by their latest vote delegation.
    fn sort_key(&self) -> (Option<(BlockSlot, TxOrder)>, &EntityKey) {
        (self.delegated_at, &self.key)
    }
}

/// Whether Blockfrost counts `account` as a delegator of `drep`.
///
/// A delegation made before the DRep's latest registration does not count.
fn delegates_to(
    account: &AccountState,
    drep: &DRep,
    registered_at: Option<(BlockSlot, TxOrder)>,
) -> bool {
    if account.delegated_drep_live() != Some(drep) {
        return false;
    }

    match registered_at {
        Some(cutoff) => account.vote_delegated_at.is_some_and(|at| at >= cutoff),
        None => true,
    }
}

pub async fn drep_delegators<D>(
    Path(drep_id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<DrepDelegatorsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AccountState>: From<D::Entity>,
    Option<DRepState>: From<D::Entity>,
{
    let (_, drep_key, _, is_special_case) = parse_drep_id(&drep_id)?;
    let pagination = Pagination::try_from(params)?;

    let drep_key = EntityKey::from(drep_key);
    let drep = drep_from_entity_key(&drep_key).ok_or(StatusCode::BAD_REQUEST)?;

    let registered_at = if is_special_case {
        None
    } else {
        match domain.read_cardano_entity::<DRepState>(drep_key)? {
            Some(state) if !state.is_unregistered() => state.registered_at,
            // Blockfrost returns an empty list for an unknown or retired DRep.
            _ => return Ok(Json(vec![])),
        }
    };

    let network = domain.get_network_id()?;

    let scan = domain.clone();
    let mut rows =
        tokio::task::spawn_blocking(move || -> Result<Vec<DrepDelegatorRow>, StatusCode> {
            scan.iter_cardano_entities::<AccountState>(None)?
                .filter_ok(|(_, account)| delegates_to(account, &drep, registered_at))
                .map_ok(|(key, account)| DrepDelegatorRow::new(key, account))
                .collect::<Result<_, _>>()
                .map_err(log_and_500("failed to scan drep delegators"))
        })
        .await
        .map_err(log_and_500("drep delegators scan task failed"))??;

    rows.sort_unstable_by(|a, b| a.sort_key().cmp(&b.sort_key()));

    if matches!(pagination.order, Order::Desc) {
        rows.reverse();
    }

    let page = rows
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|row| {
            DrepDelegatorModelBuilder {
                delegator: row.delegator,
                live_stake: row.live_stake,
                network,
            }
            .into_model()
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(page))
}

/// This function returns the 28-byte credential hash as hex and a script flag.
/// Blockfrost serves this pair (`cc_*_hex` and `cc_*_has_script`) beside each
/// CIP-129 committee ID.
fn committee_credential_parts(cred: &StakeCredential) -> (String, bool) {
    match cred {
        StakeCredential::AddrKeyhash(key) => (hex::encode(key), false),
        StakeCredential::ScriptHash(key) => (hex::encode(key), true),
    }
}

/// This function resolves one committee member against its hot-key
/// authorization history.
///
/// The code uses the last event in the authorization history to select the
/// status. There are three possible events:
///
/// - No event: the member never authorized a hot key (`not_authorized`).
/// - A resignation: the hot key is gone (`resigned`).
/// - A hot credential: the member can vote now (`authorized`).
///
/// The `cc_hot_*` fields carry the hot credential only for the `authorized`
/// status. For the other two statuses, these fields are null. This behavior is
/// the same as Blockfrost.
fn committee_member(
    gov: &GovState,
    cold: &StakeCredential,
    expiry: Epoch,
) -> Result<CommitteeMembersInner, StatusCode> {
    let (cc_cold_hex, cc_cold_has_script) = committee_credential_parts(cold);

    let (status, hot) = match gov.committee_auth(cold) {
        None => (Status::NotAuthorized, None),
        Some(CommitteeAuthorization::Resigned(_)) => (Status::Resigned, None),
        Some(CommitteeAuthorization::HotCredential(hot)) => (Status::Authorized, Some(hot)),
    };

    let (cc_hot_id, cc_hot_hex, cc_hot_has_script) = match hot {
        Some(hot) => {
            let (hex, is_script) = committee_credential_parts(hot);
            (Some(bech32_committee_hot(hot)?), Some(hex), Some(is_script))
        }
        None => (None, None, None),
    };

    Ok(CommitteeMembersInner {
        cc_cold_id: bech32_committee_cold(cold)?,
        cc_cold_hex,
        cc_cold_has_script,
        cc_hot_id,
        cc_hot_hex,
        cc_hot_has_script,
        status,
        expiration_epoch: i32_or_500(expiry)?,
    })
}

/// This function resolves all committee members against their authorization
/// history. It orders the members by the raw cold-hash hex, as Blockfrost does.
fn committee_members(
    gov: &GovState,
    members: &BTreeMap<StakeCredential, Epoch>,
) -> Result<Vec<CommitteeMembersInner>, StatusCode> {
    let mut rows = members
        .iter()
        .map(|(cold, expiry)| committee_member(gov, cold, *expiry))
        .collect::<Result<Vec<_>, _>>()?;

    rows.sort_by(|a, b| a.cc_cold_hex.cmp(&b.cc_cold_hex));

    Ok(rows)
}

/// The `GET /governance/committee` endpoint returns the constitutional
/// committee in force. The response gives the members, the hot-key
/// authorization status of each member, the vote threshold, and the
/// `NewCommittee` action that seated the committee.
///
/// Dolos keeps three items in the governance singleton: the enacted
/// committee, the per-member authorization history, and the root of the
/// previous committee action. This is the same data that Blockfrost
/// reconstructs from the db-sync tables `committee`, `committee_member`, and
/// `committee_registration` or `_de_registration`. The Conway-genesis
/// committee has no seating action. Its `gov_action_id`, `proposal_tx_hash`,
/// and `proposal_index` are null.
///
/// A `NoConfidence` enactment dissolves the committee. Dolos does not keep the
/// last-seated members. It sets the committee value to null. As a result, a
/// dissolved committee has `is_dissolved` true, an empty member list, and a
/// zero quorum. Blockfrost is different. It still returns the historical
/// members.
///
/// The `is_dissolved` flag depends on evidence of a committee lineage action,
/// not on governance activation. A previous committee action on record makes a
/// null committee a dissolution. A null committee with no such action is not a
/// dissolution.
///
/// A store migrated across the in-place upgrade gap has an unknown enact-state:
/// a null committee with no lineage root. The endpoint does not report this
/// state as a dissolution. A fresh sync recovers the true state. If governance
/// is not active (`active_since` unset), no committee exists. The endpoint then
/// returns 404 instead of an empty committee.
pub async fn committee<D>(State(domain): State<Facade<D>>) -> Result<Json<Committee>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<GovState>: From<D::Entity>,
{
    let gov = domain
        .read_cardano_entity::<GovState>(GovState::singleton_key())?
        .filter(|gov| gov.active_since.is_some())
        .ok_or(StatusCode::NOT_FOUND)?;

    let (gov_action_id, proposal_tx_hash, proposal_index) =
        match gov.prev_gov_action_ids.committee.as_ref() {
            Some(id) => (
                Some(bech32_gov_action(&id.transaction_id, id.action_index)?),
                Some(hex::encode(id.transaction_id)),
                Some(i32_or_500(id.action_index)?),
            ),
            None => (None, None, None),
        };

    let (quorum, members) = match gov.committee.as_ref() {
        Some(committee) => (
            CommitteeQuorum {
                numerator: i32_or_500(committee.threshold.numerator)?,
                denominator: i32_or_500(committee.threshold.denominator)?,
            },
            committee_members(&gov, &committee.members)?,
        ),
        None => (CommitteeQuorum::default(), Vec::new()),
    };

    Ok(Json(Committee {
        gov_action_id,
        proposal_tx_hash,
        proposal_index,
        is_dissolved: gov.committee.is_none() && gov.prev_gov_action_ids.committee.is_some(),
        quorum: Box::new(quorum),
        members,
    }))
}

/// A constitutional-committee vote from the proposal namespace.
struct CommitteeVoteRow {
    slot: BlockSlot,
    voter: StakeCredential,
    proposal_tx: Hash<32>,
    proposal_idx: u32,
    governance_type: committee_votes_inner::GovernanceType,
    vote: Vote,
    cast: Option<CommitteeVoteCast>,
}

/// The transaction data that one vote gets from its archived block.
struct CommitteeVoteCast {
    order: (usize, usize),
    tx: Hash<32>,
    anchor: Option<Anchor>,
    block_height: u64,
}

/// A bounded set of complete slot groups at one end of the listing.
///
/// The set keeps the highest slots in descending order and the lowest slots in
/// ascending order. A group stays in the set until the nearer groups hold
/// `limit` rows. The count of nearer rows only increases during a scan. Thus,
/// a group that leaves the set does not return, and the boundary slot moves
/// in one direction only. Each group in the set is therefore complete.
///
/// The set covers the first `limit` positions of the listing, or it holds the
/// whole listing. It holds fewer than `limit` rows plus the farthest group.
/// Thus, its size does not depend on the number of votes in one block.
struct CommitteeVoteFrontier {
    groups: BTreeMap<BlockSlot, Vec<CommitteeVoteRow>>,
    rows: usize,
    limit: usize,
    descending: bool,
}

impl CommitteeVoteFrontier {
    fn new(limit: usize, descending: bool) -> Self {
        Self {
            groups: BTreeMap::new(),
            rows: 0,
            limit,
            descending,
        }
    }

    /// The slot of the group farthest from the requested end.
    fn far_slot(&self) -> Option<BlockSlot> {
        let far = if self.descending {
            self.groups.first_key_value()
        } else {
            self.groups.last_key_value()
        };

        far.map(|(slot, _)| *slot)
    }

    /// This function is true if a row at `slot` has a place in the set.
    fn accepts(&self, slot: BlockSlot) -> bool {
        if self.rows < self.limit || self.groups.contains_key(&slot) {
            return true;
        }

        self.far_slot().is_some_and(|far| {
            if self.descending {
                slot > far
            } else {
                slot < far
            }
        })
    }

    fn push(&mut self, row: CommitteeVoteRow) {
        if !self.accepts(row.slot) {
            return;
        }

        self.groups.entry(row.slot).or_default().push(row);
        self.rows += 1;

        // If the nearer rows alone cover the `limit` positions, the farthest
        // group leaves the set. One push adds one row, so at most one group
        // leaves.
        let far = if self.descending {
            self.groups.first_entry()
        } else {
            self.groups.last_entry()
        };

        if let Some(far) = far {
            if self.rows - far.get().len() >= self.limit {
                self.rows -= far.remove().len();
            }
        }
    }
}

fn committee_governance_type(
    action: &ProposalAction,
) -> Option<committee_votes_inner::GovernanceType> {
    use committee_votes_inner::GovernanceType;

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

/// This function finds the transaction, the anchor, and the block height for
/// each vote in one block. It then sorts the rows by transaction position and
/// ballot index.
fn settle_committee_casts<D: Domain>(
    domain: &D,
    slot: BlockSlot,
    rows: &mut Vec<CommitteeVoteRow>,
) -> Result<(), Error> {
    let Some(body) = domain
        .archive()
        .get_block_by_slot(&slot)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Ok(());
    };

    let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let block_height = block.number();
    type CastsByTarget = BTreeMap<(Voter, Hash<32>, u32), VecDeque<CommitteeVoteCast>>;
    let mut casts = CastsByTarget::new();

    for (tx_position, tx) in block.txs().iter().enumerate() {
        if !tx.is_valid() {
            continue;
        }

        let MultiEraTx::Conway(conway) = tx else {
            continue;
        };
        let Some(procedures) = &conway.transaction_body.voting_procedures else {
            continue;
        };

        let mut vote_position = 0;
        for (voter, ballot) in procedures {
            if !matches!(
                voter,
                Voter::ConstitutionalCommitteeKey(_) | Voter::ConstitutionalCommitteeScript(_)
            ) {
                continue;
            }

            for (target, procedure) in ballot {
                casts
                    .entry((voter.clone(), target.transaction_id, target.action_index))
                    .or_default()
                    .push_back(CommitteeVoteCast {
                        order: (tx_position, vote_position),
                        tx: tx.hash(),
                        anchor: procedure.anchor.clone(),
                        block_height,
                    });
                vote_position += 1;
            }
        }
    }

    let mut settled: Vec<((usize, usize), CommitteeVoteRow)> = rows
        .drain(..)
        .map(|mut row| {
            let key = (
                committee_voter(&row.voter),
                row.proposal_tx,
                row.proposal_idx,
            );
            let cast = casts.get_mut(&key).and_then(VecDeque::pop_front);

            if cast.is_none() {
                // The archive contains this block, so the block must
                // contain the ballot. If the cast is missing, the state and
                // the archive do not agree.
                tracing::warn!(
                    slot,
                    voter = ?row.voter,
                    proposal_tx = %row.proposal_tx,
                    proposal_idx = row.proposal_idx,
                    "the archived block does not contain this committee vote, so the row leaves the page"
                );
            }

            let order = cast
                .as_ref()
                .map(|cast| cast.order)
                .unwrap_or((usize::MAX, usize::MAX));
            row.cast = cast;
            (order, row)
        })
        .collect();

    settled.sort_by_key(|(order, _)| *order);
    rows.extend(settled.into_iter().map(|(_, row)| row));

    Ok(())
}

/// This function scans the committee vote histories of every proposal. It
/// keeps only the slot groups that cover the first `limit` positions at the
/// requested end of the listing.
///
/// The namespace contains one row for each submitted governance action, and
/// every action requires a deposit. `/governance/proposals` and the DRep vote
/// listing scan the same namespace. The archive reads are the cost of a
/// request, and `enforce_max_scan_limit` limits that cost.
fn collect_committee_vote_frontier<D: Domain>(
    domain: &D,
    voters: Option<&BTreeSet<StakeCredential>>,
    limit: usize,
    descending: bool,
) -> Result<CommitteeVoteFrontier, Error> {
    let mut frontier = CommitteeVoteFrontier::new(limit, descending);
    let entities = domain
        .state()
        .iter_entities_typed::<ProposalState>(ProposalState::NS, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for entry in entities {
        let (_, state) = entry.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let Some(governance_type) = committee_governance_type(&state.action) else {
            continue;
        };

        for (voter, history) in &state.cc_votes {
            if voters.is_some_and(|voters| !voters.contains(voter)) {
                continue;
            }

            for (slot, vote) in history {
                if !frontier.accepts(*slot) {
                    continue;
                }

                frontier.push(CommitteeVoteRow {
                    slot: *slot,
                    voter: voter.clone(),
                    proposal_tx: state.tx,
                    proposal_idx: state.idx,
                    governance_type,
                    vote: vote.clone(),
                    cast: None,
                });
            }
        }
    }

    Ok(frontier)
}

/// This function reads committee votes from state and adds archive data to
/// the page.
///
/// The frontier holds the slot groups that cover the first `to()` positions
/// at the requested end of the listing. Thus, it covers every position of the
/// page. `page_slot_groups` then counts positions before it reads a block. It
/// reads blocks only for the groups that overlap the page.
///
/// If the archive does not contain the block of a vote, the vote leaves its
/// page. The rows after it do not move. `/governance/dreps/{drep_id}/votes`
/// and the other history endpoints return the same short page under
/// `sync.max_history`. The offsets stay the same as in Blockfrost.
fn committee_vote_page<D: Domain>(
    domain: &D,
    voters: Option<&BTreeSet<StakeCredential>>,
    chain: &ChainSummary,
    pagination: &Pagination,
) -> Result<Vec<CommitteeVotesInner>, Error> {
    let descending = matches!(pagination.order, Order::Desc);

    let frontier = collect_committee_vote_frontier(domain, voters, pagination.to(), descending)?;
    let rows: Vec<_> = frontier.groups.into_values().flatten().collect();

    let page = page_slot_groups(
        rows,
        |row| row.slot,
        pagination,
        |slot, group| settle_committee_casts(domain, slot, group),
    )?;

    page.into_iter()
        // A row without archive data has no transaction to report. The row
        // leaves its page. The rows after it do not move.
        .filter(|row| row.cast.is_some())
        .map(|row| {
            let cast = row.cast.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
            let (metadata_url, metadata_hash) = match cast.anchor {
                Some(anchor) => (Some(anchor.url), Some(hex::encode(anchor.content_hash))),
                None => (None, None),
            };

            Ok(CommitteeVotesInner {
                tx_hash: hex::encode(cast.tx),
                voter_hot_id: bech32_committee_hot(&row.voter)?,
                proposal_id: bech32_gov_action(&row.proposal_tx, row.proposal_idx)?,
                proposal_tx_hash: hex::encode(row.proposal_tx),
                proposal_index: i32_or_500(row.proposal_idx)?,
                governance_type: row.governance_type,
                vote: committee_vote_model(&row.vote),
                metadata_url,
                metadata_hash,
                block_height: i32_or_500(cast.block_height)?,
                block_time: i32_or_500(chain.slot_time(row.slot))?,
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()
        .map_err(Error::from)
}

async fn query_committee_votes<D>(
    domain: Facade<D>,
    pagination: Pagination,
    voters: Option<BTreeSet<StakeCredential>>,
) -> Result<Json<Vec<CommitteeVotesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let chain = domain.get_chain_summary()?;
    let page = domain
        .query()
        .run_blocking(move |domain| {
            Ok(committee_vote_page(
                &domain,
                voters.as_ref(),
                &chain,
                &pagination,
            ))
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(Json(page))
}

/// `GET /governance/committee/votes` lists votes from every past and current
/// constitutional committee member.
pub async fn committee_votes<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<CommitteeVotesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    query_committee_votes(domain, pagination, None).await
}

/// `GET /governance/committee/{cc_id}/votes` accepts a hot or cold CIP-129
/// credential. A cold credential selects every hot credential that it
/// authorized.
pub async fn committee_votes_by_id<D>(
    Path(cc_id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<CommitteeVotesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<GovState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;
    let (role, credential) = parse_committee_id(&cc_id).map_err(|_| Error::InvalidCommitteeId)?;

    let voters = match role {
        CommitteeCredentialRole::Hot => BTreeSet::from([credential]),
        CommitteeCredentialRole::Cold => domain
            .read_cardano_entity::<GovState>(GovState::singleton_key())?
            .into_iter()
            .flat_map(|gov| {
                gov.committee_hot_credentials(&credential)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect(),
    };

    if voters.is_empty() {
        return Ok(Json(Vec::new()));
    }

    query_committee_votes(domain, pagination, Some(voters)).await
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

/// This function selects a page from a listing that uses slots as its primary
/// order.
///
/// Rows with the same slot form a group. The slot determines the position of
/// the group. `settle` reads blocks only for groups that overlap the requested
/// page. Thus, the page reads no more blocks than it contains rows.
///
/// The caller supplies `rows` in ascending slot order. It also supplies a
/// deterministic order for rows with the same slot. If the archive does not
/// contain a block, the group keeps this order.
fn page_slot_groups<T>(
    rows: Vec<T>,
    slot_of: impl Fn(&T) -> BlockSlot,
    pagination: &Pagination,
    mut settle: impl FnMut(BlockSlot, &mut Vec<T>) -> Result<(), Error>,
) -> Result<Vec<T>, Error> {
    let mut groups: Vec<Vec<T>> = Vec::new();

    for row in rows {
        match groups.last_mut() {
            Some(group) if slot_of(&group[0]) == slot_of(&row) => group.push(row),
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

        settle(slot_of(&group[0]), &mut group)?;

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

/// This function orders proposals by chain order and selects the requested
/// page.
fn select_proposals<D: Domain>(
    domain: &D,
    mut proposals: Vec<ProposalRow>,
    pagination: &Pagination,
) -> Result<Vec<ProposalRow>, Error> {
    proposals.sort_unstable_by_key(|row| (row.slot, row.tx, row.idx));

    page_slot_groups(
        proposals,
        |row| row.slot,
        pagination,
        |slot, group| order_within_block(domain, slot, group),
    )
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

/// A DRep vote from the proposal namespace.
struct VoteRow {
    slot: BlockSlot,
    proposal_tx: Hash<32>,
    proposal_idx: u32,
    vote: Vote,

    /// The transaction that contains the vote and its index in that
    /// transaction. These values stay empty until `settle_casts` processes the
    /// row group.
    cast: Option<(Hash<32>, u32)>,
}

/// This function converts a DRep ID to the voter key of its ballots.
///
/// `parse_drep_id` builds the same bytes that `drep_to_entity_key` writes.
/// Thus, `drep_from_entity_key` reads them back. The CIP-129 header rule
/// stays in `dolos-cardano`, next to the writer.
fn drep_voter(drep_key: &EntityKey) -> Result<Voter, StatusCode> {
    match drep_from_entity_key(drep_key) {
        Some(DRep::Key(hash)) => Ok(Voter::DRepKey(hash)),
        Some(DRep::Script(hash)) => Ok(Voter::DRepScript(hash)),
        Some(DRep::Abstain | DRep::NoConfidence) | None => Err(StatusCode::BAD_REQUEST),
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

/// This function returns proposal IDs in Blockfrost index order.
///
/// `cert_index` is the index in one voter ballot. db-sync restarts this value
/// for each voter. The ledger stores each ballot in a map that uses governance
/// action IDs as keys. Thus, the order uses the proposal transaction hash
/// first and the action index second.
fn ballot(tx: &MultiEraTx, voter: &Voter) -> Vec<(Hash<32>, u32)> {
    let MultiEraTx::Conway(tx) = tx else {
        return Vec::new();
    };

    let Some(procedures) = &tx.transaction_body.voting_procedures else {
        return Vec::new();
    };

    let Some(ballot) = procedures.get(voter) else {
        return Vec::new();
    };

    ballot
        .keys()
        .map(|id| (id.transaction_id, id.action_index))
        .collect()
}

/// This function finds the transaction and index for each vote in one block.
/// It also sorts vote rows by transaction position and ballot index.
///
/// The proposal state stores the vote slot, but it does not store transaction
/// data. The archived block supplies the transaction hash and ballot index.
///
/// If the archive does not contain the block, rows keep their provisional
/// order and have no cast data. `vote_page` removes these rows.
///
/// If the archive contains the block but the block has no matching cast, the
/// state and the archive disagree. This function logs a warning for that row.
fn settle_casts<D: Domain>(
    domain: &D,
    voter: &Voter,
    slot: BlockSlot,
    rows: &mut Vec<VoteRow>,
) -> Result<(), Error> {
    let Some(body) = domain
        .archive()
        .get_block_by_slot(&slot)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Ok(());
    };

    let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // The map stores ballot casts for this voter. The keys are target
    // proposals. The values preserve block order. A later transaction can
    // cast another vote for the same proposal.
    type CastsByProposal = HashMap<(Hash<32>, u32), VecDeque<(usize, Hash<32>, u32)>>;
    let mut casts = CastsByProposal::new();

    for (position, tx) in block.txs().iter().enumerate() {
        // The ledger ignores governance procedures in phase-2-invalid txs.
        if !tx.is_valid() {
            continue;
        }

        for (cert_index, target) in ballot(tx, voter).into_iter().enumerate() {
            casts
                .entry(target)
                .or_default()
                .push_back((position, tx.hash(), cert_index as u32));
        }
    }

    // Proposal history stores rows in cast order. Each removal takes the first
    // matching cast in block order.
    let mut settled: Vec<((usize, u32), VoteRow)> = rows
        .drain(..)
        .map(|row| {
            match casts
                .get_mut(&(row.proposal_tx, row.proposal_idx))
                .and_then(VecDeque::pop_front)
            {
                Some((position, tx, cert_index)) => (
                    (position, cert_index),
                    VoteRow {
                        cast: Some((tx, cert_index)),
                        ..row
                    },
                ),
                None => {
                    // The archive contains this block, so the block must
                    // contain the ballot. A missing cast means that the state
                    // and the archive disagree.
                    tracing::warn!(
                        slot,
                        voter = ?voter,
                        proposal_tx = %row.proposal_tx,
                        proposal_idx = row.proposal_idx,
                        "archived block has no matching DRep vote, row removed"
                    );

                    ((usize::MAX, u32::MAX), row)
                }
            }
        })
        .collect();

    settled.sort_by_key(|(order, _)| *order);
    rows.extend(settled.into_iter().map(|(_, row)| row));

    Ok(())
}

/// This function reads DRep votes from state and adds archive data to the
/// page.
///
/// The proposal that receives a vote stores that vote. Thus, this function
/// scans the proposal namespace for the voter. The namespace contains one row
/// for each submitted governance action. Every action requires a deposit.
/// `/governance/proposals` scans the same namespace without a budget. The
/// archive reads are the cost of a request. `enforce_max_scan_limit` limits
/// that cost. `page_slot_groups` reads a block only for a group that overlaps
/// the requested page.
///
/// If the archive does not contain the block of a vote, the vote leaves its
/// page. The other history endpoints return the same short page under
/// `sync.max_history`. The offsets stay the same as in Blockfrost.
fn vote_page<D: Domain>(
    domain: &D,
    voter: &Voter,
    pagination: &Pagination,
) -> Result<Vec<DrepVotesInner>, Error> {
    let mut rows = Vec::new();

    let entities = domain
        .state()
        .iter_entities_typed::<ProposalState>(ProposalState::NS, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for entry in entities {
        let (_, state) = entry.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let Some(history) = state.vote_history(voter) else {
            continue;
        };

        for (slot, vote) in history {
            rows.push(VoteRow {
                slot: *slot,
                proposal_tx: state.tx,
                proposal_idx: state.idx,
                vote: vote.clone(),
                cast: None,
            });
        }
    }

    // This sort orders rows by slot, then by governance action ID. The stable
    // sort keeps the cast order for repeated votes on the same proposal.
    rows.sort_by_key(|row| (row.slot, row.proposal_tx, row.proposal_idx));

    let page = page_slot_groups(
        rows,
        |row| row.slot,
        pagination,
        |slot, group| settle_casts(domain, voter, slot, group),
    )?;

    page.into_iter()
        // A row without archive data has no transaction to report. The row
        // leaves its page. The rows after it do not move.
        .filter(|row| row.cast.is_some())
        .map(|row| {
            let (tx, cert_index) = row.cast.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

            Ok(DrepVotesInner {
                tx_hash: hex::encode(tx),
                cert_index: i32_or_500(cert_index)?,
                proposal_id: bech32_gov_action(&row.proposal_tx, row.proposal_idx)?,
                proposal_tx_hash: hex::encode(row.proposal_tx),
                proposal_cert_index: i32_or_500(row.proposal_idx)?,
                vote: vote_model(&row.vote),
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
/// `max_scan_items` limits the page depth. The other endpoints that read a
/// block for each row use the same limit.
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

    let (_, drep_bytes, _, is_special_case) = parse_drep_id(&drep_id)?;

    // The two special DReps are delegation targets, not voters. They cannot
    // cast votes.
    if is_special_case {
        return Ok(Json(Vec::new()));
    }

    let voter = drep_voter(&EntityKey::from(drep_bytes))?;

    let page = domain
        .query()
        .run_blocking(move |domain| Ok(vote_page(&domain, &voter, &pagination)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(Json(page))
}

/// This function builds the common fields for a proposal metadata response.
///
/// The anchor contains the metadata URL and hash from the proposal procedure.
/// The function returns metadata or an error, but not both.
async fn proposal_metadata_parts(
    tx: &Hash<32>,
    idx: u32,
    anchor: &pallas::ledger::primitives::conway::Anchor,
    ipfs_gateways: &[String],
) -> Result<
    (
        String,
        String,
        Option<AnchorMetadata>,
        Option<DrepsInnerMetadataError>,
    ),
    StatusCode,
> {
    let id = bech32_gov_action(tx, idx)?;
    let hash = hex::encode(anchor.content_hash);

    let (metadata, error) =
        anchor_offchain_metadata(&anchor.url, anchor.content_hash.as_ref(), ipfs_gateways).await;

    Ok((id, hash, metadata, error))
}

/// This endpoint returns proposal metadata by transaction hash and certificate
/// index.
///
/// An absent anchor causes a 404 response.
/// A failed metadata fetch causes a 404 response.
/// The `/{gov_action_id}/metadata` endpoint returns the anchor and an error
/// object.
pub async fn proposal_metadata<D: Domain>(
    Path((tx_hash, cert_index)): Path<(String, u32)>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ProposalMetadata>, Error>
where
    Option<ProposalState>: From<D::Entity>,
{
    let tx: Hash<32> = tx_hash.parse().map_err(|_| StatusCode::BAD_REQUEST)?;

    let state = domain
        .read_cardano_entity::<ProposalState>(ProposalState::build_entity_key(tx, cert_index))?
        .ok_or(StatusCode::NOT_FOUND)?;

    let anchor = state.anchor.as_ref().ok_or(StatusCode::NOT_FOUND)?;

    let gateways = domain.config.ipfs_gateways();
    let (id, hash, metadata, _error) =
        proposal_metadata_parts(&tx, cert_index, anchor, &gateways).await?;

    let metadata = metadata.ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ProposalMetadata {
        id,
        tx_hash: hex::encode(tx),
        cert_index: cert_index.try_into().map_err(|_| StatusCode::BAD_REQUEST)?,
        url: anchor.url.clone(),
        hash,
        json_metadata: Some(metadata.json),
        bytes: metadata.bytes,
    }))
}

/// This endpoint returns proposal metadata by CIP-129 governance-action ID.
///
/// The endpoint returns the anchor for a failed metadata fetch.
/// It returns null metadata fields and an error object.
pub async fn proposal_metadata_by_gov_action<D: Domain>(
    Path(gov_action_id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ProposalMetadataV2>, Error>
where
    Option<ProposalState>: From<D::Entity>,
{
    let (tx, idx) = parse_gov_action_id(&gov_action_id).map_err(|_| Error::InvalidGovActionId)?;

    let state = domain
        .read_cardano_entity::<ProposalState>(ProposalState::build_entity_key(tx, idx))?
        .ok_or(StatusCode::NOT_FOUND)?;

    let anchor = state.anchor.as_ref().ok_or(StatusCode::NOT_FOUND)?;

    let gateways = domain.config.ipfs_gateways();
    let (id, hash, metadata, error) = proposal_metadata_parts(&tx, idx, anchor, &gateways).await?;

    let (json_metadata, bytes) = match metadata {
        Some(AnchorMetadata { json, bytes }) => (Some(json), Some(bytes)),
        None => (None, None),
    };

    Ok(Json(ProposalMetadataV2 {
        id,
        tx_hash: hex::encode(tx),
        cert_index: idx.try_into().map_err(|_| StatusCode::BAD_REQUEST)?,
        url: anchor.url.clone(),
        hash,
        json_metadata,
        bytes,
        error: error.map(Box::new),
    }))
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
struct ProposalParametersBuilder {
    tx: Hash<32>,
    idx: u32,
    params: PParamsSet,
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
    use crate::mapping::bech32_drep;
    use crate::test_support::{TestApp, TestFault};
    use bech32::{Bech32, Hrp};
    use dolos_cardano::model::{drep_to_entity_key, DRepDelegation, EpochValue, GovPurpose, Stake};
    use dolos_core::StateWriter as _;
    use dolos_testing::{
        synthetic::{SyntheticBlockConfig, SyntheticProposalRef, SyntheticVote},
        toy_domain::ToyDomain,
    };
    use itertools::Itertools;
    use pallas::{
        codec::{minicbor, utils::Bytes},
        ledger::primitives::{
            conway::{
                CostModels, DRepVotingThresholds, ExUnitPrices, GovAction, GovActionId,
                PoolVotingThresholds, ProtocolParamUpdate,
            },
            ExUnits, RationalNumber,
        },
    };
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

    /// This function gives the synthetic DRep an anchor that Dolos can never
    /// fetch. `example.invalid` does not resolve (RFC 6761), so the fetch
    /// always stops with a connection error. The endpoint still returns the
    /// anchor fields. This function uses the same entity key that the
    /// endpoint reads. The synthetic chain imported a registration with no
    /// anchor, and this function replaces it.
    fn seed_drep_anchor(domain: &ToyDomain, drep_bytes: Vec<u8>) {
        use pallas::ledger::primitives::conway::Anchor;

        // This code builds the identifier from the same bytes as the entity
        // key. If the keyhash of the synthetic vector changes, the identifier
        // and the key still agree.
        let keyhash: [u8; 28] = drep_bytes[1..]
            .try_into()
            .expect("drep bytes carry a 28-byte hash");

        let mut state = DRepState::new(DRep::Key(Hash::from(keyhash)));
        state.anchor = Some(Anchor {
            url: "https://example.invalid/drep".to_string(),
            content_hash: Hash::from([9u8; 32]),
        });

        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");
        writer
            .write_entity_typed(&dolos_core::EntityKey::from(drep_bytes), &state)
            .expect("failed to write drep state");
        writer.commit().expect("failed to commit drep state");
    }

    #[tokio::test]
    async fn governance_drep_metadata_returns_anchor_error() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let (_, drep_bytes, _, _) =
                parse_drep_id(&vectors.drep_id).expect("failed to parse drep id");
            seed_drep_anchor(domain, drep_bytes);
        });

        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}/metadata");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let model: DrepMetadata =
            serde_json::from_slice(&body).expect("failed to parse drep metadata");

        assert_eq!(&model.drep_id, drep);
        // the drep id in the vectors is the 29-byte CIP-129 form, so the hex keeps the
        // header
        assert_eq!(model.hex, hex::encode([vec![0x22], vec![7u8; 28]].concat()));
        assert_eq!(model.url, "https://example.invalid/drep");
        assert_eq!(model.hash, hex::encode([9u8; 32]));
        assert!(model.json_metadata.is_none());
        assert!(model.bytes.is_none());
        assert_eq!(
            model.error.expect("the fetch error is absent").code,
            blockfrost_openapi::models::dreps_inner_metadata_error::Code::ConnectionError
        );
    }

    #[test]
    fn parse_drep_id_rejects_hex_id() {
        // Blockfrost decodes a DRep id as bech32 and does no other check.
        // As a result, both the CIP-129 and the legacy hex forms are a 400,
        // never a lookup.
        let cip129 = [vec![pallas_extras::DREP_KEY_PREFIX], vec![7u8; 28]].concat();
        assert_eq!(
            parse_drep_id(&hex::encode(&cip129)),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(
            parse_drep_id(&hex::encode([7u8; 28])),
            Err(StatusCode::BAD_REQUEST)
        );
    }

    #[test]
    fn parse_drep_id_rejects_invalid_cip129_header() {
        // 0x20 has the DRep high nibble. 0x20 is not a DRep key or script header.
        let mut payload = vec![0x20u8];
        payload.extend_from_slice(&[7u8; 28]);

        let bech32_id = bech32(bech32::Hrp::parse("drep").unwrap(), &payload)
            .expect("failed to encode drep id");

        assert_eq!(parse_drep_id(&bech32_id), Err(StatusCode::BAD_REQUEST));
    }

    #[tokio::test]
    async fn governance_drep_metadata_without_anchor_returns_404() {
        // the default synthetic drep registers with no anchor
        let app = TestApp::new();
        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}/metadata");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_drep_metadata_bad_request() {
        let app = TestApp::new();
        let path = format!("/governance/dreps/{}/metadata", invalid_drep());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_drep_metadata_not_found() {
        let app = TestApp::new();
        let missing = missing_drep();
        let path = format!("/governance/dreps/{missing}/metadata");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_drep_metadata_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}/metadata");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    fn drep_delegators_path(drep: &str) -> String {
        format!("/governance/dreps/{drep}/delegators")
    }

    async fn get_drep_delegators(app: &TestApp, path: &str) -> Vec<DrepDelegatorsInner> {
        let (status, body) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&body)
        );
        serde_json::from_slice(&body).expect("failed to parse drep delegators")
    }

    fn synthetic_drep() -> DRep {
        DRep::Key(SyntheticBlockConfig::default().drep_keyhash.into())
    }

    /// Overwrites the synthetic DRep state with the given registration bounds.
    fn seed_drep(
        domain: &ToyDomain,
        registered_at: Option<(BlockSlot, TxOrder)>,
        unregistered_at: Option<(BlockSlot, TxOrder)>,
    ) {
        let drep = synthetic_drep();
        let mut state = DRepState::new(drep.clone());
        state.registered_at = registered_at;
        state.unregistered_at = unregistered_at;

        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");
        writer
            .write_entity_typed(&drep_to_entity_key(&drep), &state)
            .expect("failed to write drep");
        writer.commit().expect("failed to commit drep");
    }

    /// Slot and tx order of the synthetic account's latest vote delegation.
    /// Every synthetic tx carries the delegation, so it is the last tx of the
    /// last block.
    fn last_vote_delegation(
        vectors: &dolos_testing::synthetic::SyntheticVectors,
    ) -> (BlockSlot, TxOrder) {
        let last = vectors.blocks.last().expect("synthetic chain has blocks");
        (last.slot, last.tx_hashes.len() - 1)
    }

    fn tip_epoch(domain: &ToyDomain) -> Epoch {
        let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
            .expect("era summary");
        let tip = domain
            .state()
            .read_cursor()
            .expect("cursor read failed")
            .expect("missing tip")
            .slot();
        summary.slot_epoch(tip).0
    }

    fn seeded_credential(seed: u8) -> StakeCredential {
        StakeCredential::AddrKeyhash([seed; 28].into())
    }

    fn seeded_stake_address(seed: u8) -> String {
        stake_cred_to_address(&seeded_credential(seed), Network::Testnet)
            .to_bech32()
            .expect("failed to encode stake address")
    }

    /// Writes an account that holds `utxo_sum` lovelace and delegates its
    /// vote to `drep` at `delegated_at`.
    fn seed_delegator(
        domain: &ToyDomain,
        seed: u8,
        drep: DRep,
        delegated_at: (BlockSlot, TxOrder),
        utxo_sum: u64,
    ) {
        let epoch = tip_epoch(domain);
        let credential = seeded_credential(seed);

        let mut account = AccountState::new(epoch, credential.clone());
        account.registered_at = Some(delegated_at.0);
        account.stake = EpochValue::with_live(
            epoch,
            Stake {
                utxo_sum,
                ..Default::default()
            },
        );
        account.drep = EpochValue::with_live(epoch, DRepDelegation::Delegated(drep));
        account.vote_delegated_at = Some(delegated_at);

        let key = EntityKey::from(minicbor::to_vec(&credential).expect("encode credential"));
        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");
        writer
            .write_entity_typed(&key, &account)
            .expect("failed to write account");
        writer.commit().expect("failed to commit account");
    }

    fn delegator(seed: u8, amount: u64) -> DrepDelegatorsInner {
        DrepDelegatorsInner {
            address: seeded_stake_address(seed),
            amount: amount.to_string(),
        }
    }

    #[tokio::test]
    async fn governance_drep_delegators_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();

        let (status, body) = app.get_bytes(&format!("/accounts/{stake_address}")).await;
        assert_eq!(status, StatusCode::OK);
        let account: blockfrost_openapi::models::account_content::AccountContent =
            serde_json::from_slice(&body).expect("failed to parse account");

        let legacy = get_drep_delegators(&app, &drep_delegators_path(&app.vectors().drep_id)).await;
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].address, stake_address);
        assert_eq!(legacy[0].amount, account.controlled_amount);

        let cip129_id = bech32_drep(&synthetic_drep()).expect("failed to encode drep id");
        let cip129 = get_drep_delegators(&app, &drep_delegators_path(&cip129_id)).await;
        assert_eq!(cip129, legacy);
    }

    /// Three seeded delegators join the synthetic one, each at a later
    /// position. A fourth one delegated before the DRep registration and
    /// must not appear.
    #[tokio::test]
    async fn governance_drep_delegators_paginated() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let (slot, _) = last_vote_delegation(vectors);
            let drep = synthetic_drep();
            seed_delegator(domain, 0x41, drep.clone(), (slot - 1, 0), 1_000);
            seed_delegator(domain, 0x42, drep.clone(), (slot + 1, 0), 2_000);
            seed_delegator(domain, 0x43, drep.clone(), (slot + 1, 1), 3_000);
            seed_delegator(domain, 0x44, drep, (slot + 2, 0), 4_000);
        });
        let base = drep_delegators_path(&app.vectors().drep_id);

        let asc = get_drep_delegators(&app, &base).await;
        assert_eq!(asc.len(), 4);
        assert_eq!(asc[0].address, app.vectors().stake_address);
        assert_eq!(
            asc[1..],
            [
                delegator(0x42, 2_000),
                delegator(0x43, 3_000),
                delegator(0x44, 4_000)
            ]
        );

        let page_1 = get_drep_delegators(&app, &format!("{base}?count=3&page=1")).await;
        let page_2 = get_drep_delegators(&app, &format!("{base}?count=3&page=2")).await;
        let page_3 = get_drep_delegators(&app, &format!("{base}?count=3&page=3")).await;
        assert_eq!(page_1, asc[..3]);
        assert_eq!(page_2, asc[3..]);
        assert!(page_3.is_empty());

        let desc = get_drep_delegators(&app, &format!("{base}?order=desc")).await;
        let reversed: Vec<_> = asc.iter().rev().cloned().collect();
        assert_eq!(desc, reversed);

        let desc_page_2 =
            get_drep_delegators(&app, &format!("{base}?order=desc&count=3&page=2")).await;
        assert_eq!(desc_page_2, reversed[3..]);
    }

    #[tokio::test]
    async fn governance_drep_delegators_bad_request() {
        let app = TestApp::new();
        let path = drep_delegators_path(invalid_drep());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;

        let path = format!("{}?count=0", drep_delegators_path(&app.vectors().drep_id));
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    /// Blockfrost answers `200 []` for a well-formed DRep id it has never seen.
    #[tokio::test]
    async fn governance_drep_delegators_unknown_drep_is_empty() {
        let app = TestApp::new();
        let rows = get_drep_delegators(&app, &drep_delegators_path(&missing_drep())).await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn governance_drep_delegators_special_dreps() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let (slot, _) = last_vote_delegation(vectors);
            seed_delegator(domain, 0x51, DRep::Abstain, (slot, 0), 5_000);
            seed_delegator(domain, 0x52, DRep::NoConfidence, (slot, 1), 6_000);
        });

        let abstain = get_drep_delegators(&app, &drep_delegators_path("drep_always_abstain")).await;
        assert_eq!(abstain, [delegator(0x51, 5_000)]);

        let no_confidence =
            get_drep_delegators(&app, &drep_delegators_path("drep_always_no_confidence")).await;
        assert_eq!(no_confidence, [delegator(0x52, 6_000)]);

        // The seeded accounts do not leak into a regular DRep's list.
        let regular =
            get_drep_delegators(&app, &drep_delegators_path(&app.vectors().drep_id)).await;
        assert_eq!(regular.len(), 1);
        assert_eq!(regular[0].address, app.vectors().stake_address);
    }

    #[tokio::test]
    async fn governance_drep_delegators_retired_drep_is_empty() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, _| {
            seed_drep(domain, Some((1, 0)), Some((2, 0)))
        });

        let rows = get_drep_delegators(&app, &drep_delegators_path(&app.vectors().drep_id)).await;
        assert!(rows.is_empty());
    }

    /// A delegation in the same tx as the DRep registration counts. One made
    /// before the DRep's latest registration does not.
    #[tokio::test]
    async fn governance_drep_delegators_honor_registration_cutoff() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        let same_tx = TestApp::new_with_cfg_and_setup(cfg.clone(), |domain, vectors| {
            seed_drep(domain, Some(last_vote_delegation(vectors)), None)
        });
        let rows =
            get_drep_delegators(&same_tx, &drep_delegators_path(&same_tx.vectors().drep_id)).await;
        assert_eq!(rows.len(), 1);

        let reregistered = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let (slot, order) = last_vote_delegation(vectors);
            seed_drep(domain, Some((slot, order + 1)), None)
        });
        let rows = get_drep_delegators(
            &reregistered,
            &drep_delegators_path(&reregistered.vectors().drep_id),
        )
        .await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn governance_drep_delegators_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = drep_delegators_path(&app.vectors().drep_id);
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    fn synthetic_vote(
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

    /// A pruned block removes its votes from their page. The pages after it
    /// do not move. The other history endpoints return the same short page
    /// under `sync.max_history`. Thus, a client that reads all pages gets the
    /// same offsets as from Blockfrost.
    ///
    /// The test prunes the archive to one slot. Only the last block remains.
    /// The three votes of block 1 are gone. The one vote of block 2 keeps
    /// offset 3.
    #[tokio::test]
    async fn governance_drep_votes_skips_pruned_rows_without_shifting_offsets() {
        let app = TestApp::new_with_cfg_and_setup(drep_votes_config(), |domain, _| {
            domain
                .archive()
                .prune_history(0, None)
                .expect("The archive did not prune its history.");
        });
        let drep = &app.vectors().drep_id;

        assert!(get_drep_votes(&app, drep, "?count=1").await.is_empty());
        assert!(get_drep_votes(&app, drep, "?count=1&page=3")
            .await
            .is_empty());

        let rows = get_drep_votes(&app, drep, "?count=1&page=4").await;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_hash, app.vectors().blocks[2].tx_hashes[0]);
        assert_eq!(rows[0].vote, drep_votes_inner::Vote::No);
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

    fn cc_hot_key() -> StakeCredential {
        StakeCredential::AddrKeyhash(Hash::from([21u8; 28]))
    }

    fn cc_hot_script() -> StakeCredential {
        StakeCredential::ScriptHash(Hash::from([22u8; 28]))
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
    /// four different slots. A small frontier limit cannot hold all four.
    fn committee_votes_across_slots_config() -> SyntheticBlockConfig {
        committee_votes_in_blocks_config(&[1, 1, 1, 1])
    }

    /// Transaction 0 in block 0 proposes one action for each vote in the
    /// largest block. Each subsequent block casts the given number of
    /// committee votes in one transaction. The votes go to action 0, action
    /// 1, and so on. The listing therefore has one slot group per block, and
    /// the groups differ in size. The votes cycle through yes, no, abstain,
    /// and no in cast order. Thus, two consecutive rows are different, and
    /// the fourth row is no.
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
        let app = TestApp::new_with_cfg(committee_votes_config());
        let rows = get_committee_votes(&app, "/governance/committee/votes").await;
        let blocks = &app.vectors().blocks;
        let hot_key = bech32_committee_hot(&cc_hot_key()).unwrap();
        let hot_script = bech32_committee_hot(&cc_hot_script()).unwrap();

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
        }
    }

    #[tokio::test]
    async fn governance_committee_votes_orders_and_paginates() {
        let app = TestApp::new_with_cfg(committee_votes_config());
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
    async fn governance_committee_votes_by_hot_id_selects_one_voter() {
        let app = TestApp::new_with_cfg(committee_votes_config());
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
        let cold = cc_cold_key(31);
        let app = TestApp::new_with_cfg_and_setup(committee_votes_config(), move |domain, _| {
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
                    committee_auth_archive: BTreeMap::from([(
                        cc_cold_key(31),
                        vec![(5, CommitteeAuthorization::HotCredential(cc_hot_script()))],
                    )]),
                    active_since: Some(0),
                    ..Default::default()
                },
            );
        });

        let cold_id = bech32_committee_cold(&cold).unwrap();
        let rows =
            get_committee_votes(&app, &format!("/governance/committee/{cold_id}/votes")).await;
        let all = get_committee_votes(&app, "/governance/committee/votes").await;

        // Both authorized hot credentials voted. The cold listing is therefore
        // the full listing.
        assert_eq!(
            rows.iter().map(committee_row_id).collect_vec(),
            all.iter().map(committee_row_id).collect_vec()
        );
    }

    #[tokio::test]
    async fn governance_committee_votes_by_unauthorized_cold_id_is_empty() {
        let app = TestApp::new_with_cfg(committee_votes_config());
        let cold_id = bech32_committee_cold(&cc_cold_key(31)).unwrap();

        assert!(
            get_committee_votes(&app, &format!("/governance/committee/{cold_id}/votes"))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn governance_committee_votes_bad_request() {
        let app = TestApp::new_with_cfg(committee_votes_config());
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
        assert_status(
            &app,
            "/governance/committee/not-a-credential/votes",
            StatusCode::BAD_REQUEST,
        )
        .await;

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
        assert_status(
            &app,
            "/governance/committee/votes",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }

    /// The page depth is limited like on the other endpoints that read a
    /// block for each row. The vote history itself has no limit.
    #[tokio::test]
    async fn governance_committee_votes_rejects_a_deep_page() {
        let app = TestApp::new_with_scan_limit(committee_votes_across_slots_config(), 3);

        let rows = get_committee_votes(&app, "/governance/committee/votes?count=1&page=3").await;
        assert_eq!(rows.len(), 1);

        let (status, bytes) = app
            .get_bytes("/governance/committee/votes?count=2&page=2")
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("scan limit"), "The response body was {body}.");
    }

    /// The frontier covers `count * page` positions. The chain has more votes
    /// than that, so the scan removes groups at the far end. The page must
    /// still be the same as on an unbounded listing.
    #[tokio::test]
    async fn governance_committee_votes_pages_a_bounded_frontier() {
        let app = TestApp::new_with_cfg(committee_votes_across_slots_config());
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

    /// The frontier counts rows, not groups. A group that is larger than the
    /// page does not remove the groups before it. A group is complete or
    /// absent. Each page must be equal to the same slice of the unbounded
    /// listing.
    #[tokio::test]
    async fn governance_committee_votes_page_lopsided_groups() {
        let app = TestApp::new_with_cfg(committee_votes_in_blocks_config(&[1, 5, 1, 1]));
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

    fn frontier_row(slot: BlockSlot) -> CommitteeVoteRow {
        CommitteeVoteRow {
            slot,
            voter: cc_hot_key(),
            proposal_tx: Hash::from([0u8; 32]),
            proposal_idx: 0,
            governance_type: committee_votes_inner::GovernanceType::InfoAction,
            vote: Vote::Yes,
            cast: None,
        }
    }

    fn frontier_slots(frontier: &CommitteeVoteFrontier) -> Vec<(BlockSlot, usize)> {
        frontier
            .groups
            .iter()
            .map(|(slot, group)| (*slot, group.len()))
            .collect()
    }

    /// The frontier keeps the groups that cover the first `limit` positions
    /// and no more. If the nearest group is larger than `limit`, it stays
    /// complete. The arrival order of the rows has no effect.
    #[test]
    fn committee_vote_frontier_is_bounded_by_rows() {
        let mut ascending = CommitteeVoteFrontier::new(3, false);
        for slot in [50, 50, 50, 50, 50, 10, 30, 30, 20, 60, 10] {
            ascending.push(frontier_row(slot));
        }
        assert_eq!(frontier_slots(&ascending), vec![(10, 2), (20, 1)]);
        assert_eq!(ascending.rows, 3);
        // A group in the set stays complete, so the set accepts its slot.
        assert!(ascending.accepts(10));
        assert!(ascending.accepts(20));
        assert!(ascending.accepts(5));
        assert!(ascending.accepts(15));
        assert!(!ascending.accepts(30));
        assert!(!ascending.accepts(50));

        let mut descending = CommitteeVoteFrontier::new(3, true);
        for slot in [10, 10, 10, 10, 10, 50, 30, 30, 40, 5, 50] {
            descending.push(frontier_row(slot));
        }
        assert_eq!(frontier_slots(&descending), vec![(40, 1), (50, 2)]);
        assert_eq!(descending.rows, 3);
        assert!(descending.accepts(50));
        assert!(descending.accepts(40));
        assert!(descending.accepts(60));
        assert!(descending.accepts(45));
        assert!(!descending.accepts(30));
        assert!(!descending.accepts(10));

        // The nearest group is larger than the limit. It stays complete.
        let mut wide = CommitteeVoteFrontier::new(2, false);
        for slot in [10, 10, 10, 10, 20, 5, 5, 5] {
            wide.push(frontier_row(slot));
        }
        assert_eq!(frontier_slots(&wide), vec![(5, 3)]);
        assert_eq!(wide.rows, 3);
        assert!(!wide.accepts(10));

        // The listing is smaller than the limit. Nothing leaves.
        let mut small = CommitteeVoteFrontier::new(10, true);
        for slot in [10, 20, 20, 30] {
            small.push(frontier_row(slot));
        }
        assert_eq!(frontier_slots(&small), vec![(10, 1), (20, 2), (30, 1)]);
        assert_eq!(small.rows, 4);
    }

    /// The listing has groups of different sizes. For each arrival order of
    /// its rows, the frontier holds exactly the groups that cover the first
    /// `limit` positions. Its size is less than `limit` plus the size of its
    /// farthest group.
    #[test]
    fn committee_vote_frontier_covers_the_limit_in_any_order() {
        // Each pair is a slot and the number of rows in its group.
        let listing = [(10u64, 3usize), (20, 1), (30, 4), (40, 2), (50, 1), (60, 5)];
        let mut rows = Vec::new();
        for (slot, size) in listing {
            rows.extend(std::iter::repeat_n(slot, size));
        }

        // Each stride visits the rows in a different order.
        for stride in [1usize, 5, 7, 11, 13] {
            let order: Vec<u64> = (0..rows.len())
                .map(|i| rows[(i * stride) % rows.len()])
                .collect();

            for limit in 1..=rows.len() + 1 {
                for descending in [false, true] {
                    let mut frontier = CommitteeVoteFrontier::new(limit, descending);
                    for &slot in &order {
                        frontier.push(frontier_row(slot));
                    }

                    let mut expected = Vec::new();
                    let mut covered = 0;
                    let mut groups = listing.to_vec();
                    if descending {
                        groups.reverse();
                    }
                    for (slot, size) in groups {
                        if covered >= limit {
                            break;
                        }
                        expected.push((slot, size));
                        covered += size;
                    }
                    expected.sort_unstable();

                    assert_eq!(
                        frontier_slots(&frontier),
                        expected,
                        "The frontier is wrong for stride {stride}, limit {limit}, descending {descending}."
                    );
                    assert_eq!(frontier.rows, covered);
                    let far = frontier
                        .far_slot()
                        .map_or(0, |slot| frontier.groups[&slot].len());
                    assert!(frontier.rows < limit + far);
                }
            }
        }
    }

    /// If the archive does not contain the block of a vote, the vote leaves
    /// its page. The rows after it keep their offsets, as on the DRep vote
    /// listing.
    #[tokio::test]
    async fn governance_committee_votes_skip_pruned_blocks_without_shifting_offsets() {
        let app =
            TestApp::new_with_cfg_and_setup(committee_votes_across_slots_config(), |domain, _| {
                // This call keeps only the last slot of the chain.
                domain
                    .archive()
                    .prune_history(0, None)
                    .expect("The archive did not prune its history.");
            });
        let blocks = app.vectors().blocks.clone();
        let last = &blocks[4].tx_hashes[0];

        // The archive does not contain the blocks of the first three votes.
        // Their pages are empty.
        assert!(
            get_committee_votes(&app, "/governance/committee/votes?count=1")
                .await
                .is_empty()
        );

        // The fourth vote keeps its offset.
        let page = get_committee_votes(&app, "/governance/committee/votes?count=1&page=4").await;
        assert_eq!(
            page.iter().map(|row| &row.tx_hash).collect_vec(),
            vec![last]
        );
        assert_eq!(page[0].vote, committee_votes_inner::Vote::No);

        // A page that contains a missing block and a kept block is short.
        let page = get_committee_votes(&app, "/governance/committee/votes?count=2&page=2").await;
        assert_eq!(
            page.iter().map(|row| &row.tx_hash).collect_vec(),
            vec![last]
        );

        // In descending order the kept vote is the first row.
        let page =
            get_committee_votes(&app, "/governance/committee/votes?count=1&order=desc").await;
        assert_eq!(
            page.iter().map(|row| &row.tx_hash).collect_vec(),
            vec![last]
        );
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
    async fn governance_proposal_metadata_returns_404_when_fetch_fails() {
        let app = proposal_app();
        let tx = tx_hash_of_block(&app, 0);

        assert_status(
            &app,
            &format!("/governance/proposals/{tx}/0/metadata"),
            StatusCode::NOT_FOUND,
        )
        .await;
    }

    #[tokio::test]
    async fn governance_proposal_metadata_by_gov_action_returns_anchor_error() {
        let app = proposal_app();
        let tx = tx_hash_of_block(&app, 0);
        let tx_hash: Hash<32> = tx.parse().expect("Cannot parse the transaction hash.");
        let id = bech32_gov_action(&tx_hash, 0).expect("Cannot encode the governance action ID.");

        let (status, body) = app
            .get_bytes(&format!("/governance/proposals/{id}/metadata"))
            .await;

        assert_eq!(status, StatusCode::OK);

        let metadata: ProposalMetadataV2 =
            serde_json::from_slice(&body).expect("Cannot parse the proposal metadata.");

        assert_eq!(metadata.id, id);
        assert_eq!(metadata.tx_hash, tx);
        assert_eq!(metadata.cert_index, 0);
        assert_eq!(metadata.url, "https://example.invalid/proposal");
        assert_eq!(metadata.hash, hex::encode([6u8; 32]));
        assert!(metadata.json_metadata.is_none());
        assert!(metadata.bytes.is_none());
        assert_eq!(
            metadata.error.expect("The fetch error is absent.").code,
            blockfrost_openapi::models::dreps_inner_metadata_error::Code::ConnectionError
        );
    }

    #[tokio::test]
    async fn governance_proposal_metadata_by_gov_action_bad_request() {
        let app = proposal_app();

        // a malformed CIP-129 id is a 400, not a lookup that misses
        for id in ["not-bech32", &missing_drep()] {
            let path = format!("/governance/proposals/{id}/metadata");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

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

    /// The model types fees, sizes and counts as `i32`, while a proposal can
    /// set them anywhere in the chain's range: a value past `i32::MAX` is a
    /// 500, not a parameter wrapped negative. Index 0 names a field the
    /// proposal model maps itself, index 1 one the shared mapping maps, and
    /// index 2 sits exactly on the limit to show it is the limit that fails.
    #[tokio::test]
    async fn governance_proposal_parameters_past_i32_are_500() {
        let change = |update| GovAction::ParameterChange(None, Box::new(update), None);

        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![
                change(ProtocolParamUpdate {
                    minfee_b: Some(3_000_000_000),
                    ..empty_update()
                }),
                change(ProtocolParamUpdate {
                    collateral_percentage: Some(3_000_000_000),
                    ..empty_update()
                }),
                change(ProtocolParamUpdate {
                    minfee_b: Some(i32::MAX as u64),
                    ..empty_update()
                }),
            ]]],
            ..Default::default()
        });

        let tx = tx_hash_of_block(&app, 0);

        for idx in 0..2 {
            let path = format!("/governance/proposals/{tx}/{idx}/parameters");
            assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
        }

        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/2/parameters")).await;
        assert_eq!(model.parameters.min_fee_b, Some(i32::MAX));
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

    fn cc_cold_key(byte: u8) -> StakeCredential {
        StakeCredential::AddrKeyhash(Hash::from([byte; 28]))
    }

    fn cc_cold_script(byte: u8) -> StakeCredential {
        StakeCredential::ScriptHash(Hash::from([byte; 28]))
    }

    /// This function overwrites the governance singleton with the given state.
    fn seed_gov(domain: &ToyDomain, state: GovState) {
        let writer = domain
            .state()
            .start_writer()
            .expect("The state store cannot start a writer.");
        writer
            .write_entity_typed(&GovState::singleton_key(), &state)
            .expect("The state writer cannot write the governance state.");
        writer
            .commit()
            .expect("The state writer cannot commit the governance state.");
    }

    /// This function writes an active-governance committee, its per-member
    /// authorization history, and its action root to the governance singleton.
    /// The endpoint reads this data. The `seed_drep_anchor` helper writes
    /// equivalent data for a DRep registration.
    fn seed_committee(
        domain: &ToyDomain,
        committee: Option<dolos_cardano::model::gov::Committee>,
        auths: BTreeMap<StakeCredential, Vec<(BlockSlot, CommitteeAuthorization)>>,
        seating_action: Option<GovActionId>,
    ) {
        seed_gov(
            domain,
            GovState {
                committee,
                committee_auths: auths,
                prev_gov_action_ids: dolos_cardano::model::gov::GovRoots {
                    committee: seating_action,
                    ..Default::default()
                },
                active_since: Some(0),
                ..Default::default()
            },
        );
    }

    #[test]
    fn committee_credential_ids_follow_cip0129() {
        // For a cold key, the high nibble is 0x1 (cold), and the low nibble is 0x2
        // (key).
        let cold =
            bech32_committee_cold(&cc_cold_key(1)).expect("The encoder cannot encode the cold ID.");
        let (hrp, payload) = bech32::decode(&cold).expect("The decoder cannot decode the cold ID.");
        assert_eq!(hrp.as_str(), "cc_cold");
        assert_eq!(payload[0], 0x12);
        assert_eq!(&payload[1..], &[1u8; 28]);

        // For a cold script, the low nibble is 0x3 (script).
        let cold_script = bech32_committee_cold(&cc_cold_script(3))
            .expect("The encoder cannot encode the cold-script ID.");
        let (_, payload) =
            bech32::decode(&cold_script).expect("The decoder cannot decode the cold-script ID.");
        assert_eq!(payload[0], 0x13);

        // For a hot key, the high nibble is 0x0 (hot), and the low nibble is 0x2 (key).
        let hot =
            bech32_committee_hot(&cc_cold_key(11)).expect("The encoder cannot encode the hot ID.");
        let (hrp, payload) = bech32::decode(&hot).expect("The decoder cannot decode the hot ID.");
        assert_eq!(hrp.as_str(), "cc_hot");
        assert_eq!(payload[0], 0x02);

        // For a hot script, the low nibble is 0x3 (script).
        let hot_script = bech32_committee_hot(&cc_cold_script(9))
            .expect("The encoder cannot encode the hot-script ID.");
        let (_, payload) =
            bech32::decode(&hot_script).expect("The decoder cannot decode the hot-script ID.");
        assert_eq!(payload[0], 0x03);
    }

    #[tokio::test]
    async fn governance_committee_happy_path() {
        let seating = GovActionId {
            transaction_id: Hash::from([7u8; 32]),
            action_index: 3,
        };
        let seating_for_setup = seating.clone();

        let app = TestApp::new_with_cfg_and_setup(
            SyntheticBlockConfig::default(),
            move |domain, _vectors| {
                let committee = dolos_cardano::model::gov::Committee {
                    members: BTreeMap::from([
                        (cc_cold_key(1), 100),
                        (cc_cold_key(2), 200),
                        (cc_cold_script(3), 300),
                    ]),
                    threshold: RationalNumber {
                        numerator: 2,
                        denominator: 3,
                    },
                };

                let auths = BTreeMap::from([
                    (
                        cc_cold_key(1),
                        vec![(10, CommitteeAuthorization::HotCredential(cc_cold_key(11)))],
                    ),
                    (
                        cc_cold_script(3),
                        vec![(20, CommitteeAuthorization::Resigned(None))],
                    ),
                ]);

                seed_committee(domain, Some(committee), auths, Some(seating_for_setup));
            },
        );

        let (status, body) = app.get_bytes("/governance/committee").await;
        assert_eq!(status, StatusCode::OK);

        let model: Committee = serde_json::from_slice(&body)
            .expect("The JSON parser cannot parse the committee response.");

        assert_eq!(
            model.gov_action_id,
            Some(bech32_gov_action(&seating.transaction_id, seating.action_index).unwrap())
        );
        assert_eq!(model.proposal_tx_hash, Some(hex::encode([7u8; 32])));
        assert_eq!(model.proposal_index, Some(3));
        assert!(!model.is_dissolved);
        assert_eq!(model.quorum.numerator, 2);
        assert_eq!(model.quorum.denominator, 3);
        assert_eq!(model.members.len(), 3);

        // The endpoint orders the members by the raw cold-hash hex.
        let hexes: Vec<_> = model
            .members
            .iter()
            .map(|m| m.cc_cold_hex.clone())
            .collect();
        let mut sorted = hexes.clone();
        sorted.sort();
        assert_eq!(hexes, sorted);

        let authorized = &model.members[0];
        assert_eq!(authorized.cc_cold_hex, hex::encode([1u8; 28]));
        assert!(!authorized.cc_cold_has_script);
        assert!(authorized.cc_cold_id.starts_with("cc_cold1"));
        assert_eq!(authorized.status, Status::Authorized);
        assert_eq!(authorized.cc_hot_hex, Some(hex::encode([11u8; 28])));
        assert_eq!(authorized.cc_hot_has_script, Some(false));
        assert!(authorized
            .cc_hot_id
            .as_ref()
            .expect("The authorized member has no hot ID.")
            .starts_with("cc_hot1"));
        assert_eq!(authorized.expiration_epoch, 100);

        let unauthorized = &model.members[1];
        assert_eq!(unauthorized.status, Status::NotAuthorized);
        assert!(unauthorized.cc_hot_id.is_none());
        assert!(unauthorized.cc_hot_hex.is_none());
        assert!(unauthorized.cc_hot_has_script.is_none());
        assert_eq!(unauthorized.expiration_epoch, 200);

        let resigned = &model.members[2];
        assert_eq!(resigned.status, Status::Resigned);
        assert!(resigned.cc_cold_has_script);
        assert!(resigned.cc_hot_id.is_none());
        assert!(resigned.cc_hot_hex.is_none());
        assert!(resigned.cc_hot_has_script.is_none());
        assert_eq!(resigned.expiration_epoch, 300);
    }

    #[tokio::test]
    async fn governance_committee_genesis_has_no_seating_action() {
        let app =
            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _vectors| {
                let committee = dolos_cardano::model::gov::Committee {
                    members: BTreeMap::from([(cc_cold_key(1), 100)]),
                    threshold: RationalNumber {
                        numerator: 1,
                        denominator: 2,
                    },
                };

                seed_committee(domain, Some(committee), BTreeMap::new(), None);
            });

        let (status, body) = app.get_bytes("/governance/committee").await;
        assert_eq!(status, StatusCode::OK);

        let model: Committee = serde_json::from_slice(&body)
            .expect("The JSON parser cannot parse the committee response.");

        // The Conway-genesis committee has no seating action.
        assert!(model.gov_action_id.is_none());
        assert!(model.proposal_tx_hash.is_none());
        assert!(model.proposal_index.is_none());
        assert!(!model.is_dissolved);
        assert_eq!(model.members.len(), 1);
        assert_eq!(model.members[0].status, Status::NotAuthorized);
    }

    #[tokio::test]
    async fn governance_committee_migration_gap_is_not_dissolved() {
        // A store migrated across the in-place upgrade gap has an unknown
        // enact-state: a null committee with no lineage root. This state is not
        // a dissolution. The endpoint reports `is_dissolved` false and does not
        // read the null committee as no-confidence.
        let app =
            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _vectors| {
                seed_committee(domain, None, BTreeMap::new(), None);
            });

        let (status, body) = app.get_bytes("/governance/committee").await;
        assert_eq!(status, StatusCode::OK);

        let model: Committee = serde_json::from_slice(&body)
            .expect("The JSON parser cannot parse the committee response.");

        assert!(!model.is_dissolved);
        assert!(model.members.is_empty());
        assert_eq!(model.quorum.numerator, 0);
        assert_eq!(model.quorum.denominator, 0);
    }

    #[tokio::test]
    async fn governance_committee_no_confidence_is_dissolved() {
        // A NoConfidence enactment clears the committee and writes the committee
        // lineage root. These two changes are the evidence of a dissolution.
        let app =
            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _vectors| {
                let action = GovActionId {
                    transaction_id: Hash::from([9u8; 32]),
                    action_index: 1,
                };
                seed_committee(domain, None, BTreeMap::new(), Some(action));
            });

        let (status, body) = app.get_bytes("/governance/committee").await;
        assert_eq!(status, StatusCode::OK);

        let model: Committee = serde_json::from_slice(&body)
            .expect("The JSON parser cannot parse the committee response.");

        assert!(model.is_dissolved);
        assert!(model.members.is_empty());
    }

    #[tokio::test]
    async fn governance_committee_pre_conway_returns_not_found() {
        // Before governance activates, the singleton exists but `active_since`
        // is unset. No committee exists, so the endpoint returns 404 instead of
        // an empty committee.
        let app =
            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _vectors| {
                seed_gov(domain, GovState::default());
            });

        assert_status(&app, "/governance/committee", StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_committee_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(
            &app,
            "/governance/committee",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }
}
