use std::collections::BTreeMap;

use dolos_core::{BlockSlot, EntityKey, NsKey};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
    ledger::primitives::{
        conway::{Anchor, GovActionId, Vote, Voter},
        Coin, Epoch, ProtocolVersion, RationalNumber, ScriptHash, StakeCredential,
    },
};
use serde::{Deserialize, Serialize};

use super::{epochs::Lovelace, pools::PoolHash, pparams::PParamsSet, FixedNamespace as _};

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProposalAction {
    #[n(0)]
    ParamChange(#[n(0)] PParamsSet),

    #[n(1)]
    HardFork(#[n(0)] ProtocolVersion),

    #[n(2)]
    TreasuryWithdrawal(#[n(0)] Vec<(StakeCredential, Coin)>),

    /// Legacy catch-all for governance actions that weren't tracked in full.
    /// Kept so pre-existing rows decode; new rows use the specific variants
    /// below.
    #[n(3)]
    Other,

    // Variants below are backward-compatible additions: old rows only carry
    // indexes 0..=3, so they keep decoding. Variant order is also part of the
    // WAL format (bincode positional encoding) — append only, never reorder.
    #[n(4)]
    NoConfidence,

    #[n(5)]
    UpdateCommittee {
        #[n(0)]
        to_remove: Vec<StakeCredential>,

        /// Members to add, each with its term-expiry epoch (inclusive).
        #[n(1)]
        to_add: Vec<(StakeCredential, Epoch)>,

        #[n(2)]
        threshold: RationalNumber,
    },

    #[n(6)]
    NewConstitution {
        #[n(0)]
        anchor: Anchor,

        #[n(1)]
        guardrail_script: Option<ScriptHash>,
    },

    #[n(7)]
    Info,
}

impl ProposalAction {
    /// The lineage tree this action belongs to. Derived from the action
    /// itself rather than read off `ProposalState.purpose`, so rows written
    /// before the field existed still resolve their purpose at enactment.
    /// `None` for the actions that have no lineage — `TreasuryWithdrawal`,
    /// `Info`, and the legacy `Other` catch-all.
    pub fn purpose(&self) -> Option<GovPurpose> {
        match self {
            Self::ParamChange(_) => Some(GovPurpose::PParamUpdate),
            Self::HardFork(_) => Some(GovPurpose::HardFork),
            Self::NoConfidence | Self::UpdateCommittee { .. } => Some(GovPurpose::Committee),
            Self::NewConstitution { .. } => Some(GovPurpose::Constitution),
            Self::TreasuryWithdrawal(_) | Self::Info | Self::Other => None,
        }
    }
}

/// The four independent lineage trees a governance action can belong to.
///
/// `TreasuryWithdrawal` and `Info` actions have no lineage (no parent field,
/// never become a root), so their proposals carry no purpose.
#[derive(
    Debug, Encode, Decode, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
#[cbor(index_only)]
pub enum GovPurpose {
    #[n(0)]
    PParamUpdate,

    #[n(1)]
    HardFork,

    #[n(2)]
    Committee,

    #[n(3)]
    Constitution,
}

/// One voter's votes on one proposal, bounded to the two a tally can need,
/// each with the slot it was cast at.
///
/// The ledger keeps one vote per voter (newest wins) and freezes the vote
/// maps when an epoch starts, so the tally closing epoch `n` reads the vote
/// standing when `n` began. Ewrap tallies after every block of `n` and before
/// any block of `n + 1`, so the newest vote is never later than `n`, and the
/// answer is either the newest vote or the one standing when its epoch began.
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VoteEntry {
    #[n(0)]
    pub newest: (BlockSlot, Vote),

    /// The vote standing when `newest`'s epoch began; `None` when the voter
    /// had not voted on the proposal before that epoch.
    #[n(1)]
    pub standing: Option<(BlockSlot, Vote)>,
}

impl VoteEntry {
    /// The entry after `vote` is cast at `slot`, in the epoch starting at
    /// `epoch_start`.
    fn cast(prev: Option<&Self>, slot: BlockSlot, vote: Vote, epoch_start: BlockSlot) -> Self {
        let standing = match prev {
            None => None,
            Some(prev) if prev.newest.0 < epoch_start => Some(prev.newest.clone()),
            Some(prev) => prev.standing.clone(),
        };

        Self {
            newest: (slot, vote),
            standing,
        }
    }

    /// The vote standing at `cutoff`: the newest vote if cast by then,
    /// otherwise the standing vote if cast by then. Exact when `cutoff` is
    /// the slot before some epoch starts and the newest vote is no later
    /// than that epoch.
    fn as_of(&self, cutoff: BlockSlot) -> Option<&Vote> {
        if self.newest.0 <= cutoff {
            return Some(&self.newest.1);
        }

        self.standing
            .as_ref()
            .filter(|(slot, _)| *slot <= cutoff)
            .map(|(_, vote)| vote)
    }
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProposalState {
    #[n(0)]
    pub slot: BlockSlot,

    #[n(1)]
    pub tx: Hash<32>,

    #[n(2)]
    pub idx: u32,

    #[n(3)]
    pub action: ProposalAction,

    /// Set at the initialization of the proposal representing the last valid
    /// epoch. The existence of a value doesn't mean the proposal has expired.
    #[n(4)]
    pub max_epoch: Option<Epoch>,

    #[n(5)]
    pub ratified_epoch: Option<Epoch>,

    #[n(6)]
    pub canceled_epoch: Option<Epoch>,

    #[n(7)]
    pub deposit: Option<Lovelace>,

    #[n(8)]
    pub reward_account: Option<StakeCredential>,

    // Fields below are backward-compatible additions: absent in pre-existing
    // rows, they decode as `None` / empty. Existing indexes are frozen.
    // Indexes 13..=15 held unbounded vote histories; they are retired and
    // never reused.
    /// Epoch in which the proposal was submitted. Drives the snapshot filter
    /// at epoch boundaries (`max_epoch` stays as the expiry bound).
    #[n(9)]
    pub proposed_in: Option<Epoch>,

    /// Lineage parent declared by the action: the previous governance action
    /// id of the same purpose (`None` for the root of the tree, and always
    /// `None` for TreasuryWithdrawal/Info, which have no lineage).
    #[n(10)]
    pub parent: Option<GovActionId>,

    /// Which lineage tree the action belongs to (`None` for
    /// TreasuryWithdrawal/Info).
    #[n(11)]
    pub purpose: Option<GovPurpose>,

    /// Metadata anchor as submitted in the proposal procedure.
    #[n(12)]
    pub anchor: Option<Anchor>,

    /// Constitutional-committee votes, keyed by the member's hot credential.
    #[n(16)]
    #[cbor(default)]
    pub cc_votes: BTreeMap<StakeCredential, VoteEntry>,

    /// DRep votes, keyed by the DRep credential.
    #[n(17)]
    #[cbor(default)]
    pub drep_votes: BTreeMap<StakeCredential, VoteEntry>,

    /// Stake-pool operator votes, keyed by the pool operator key hash.
    #[n(18)]
    #[cbor(default)]
    pub spo_votes: BTreeMap<PoolHash, VoteEntry>,
}

entity_boilerplate!(ProposalState, "proposals");

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    pub fn any_gov_purpose() -> impl Strategy<Value = GovPurpose> {
        prop_oneof![
            Just(GovPurpose::PParamUpdate),
            Just(GovPurpose::HardFork),
            Just(GovPurpose::Committee),
            Just(GovPurpose::Constitution),
        ]
    }

    pub fn any_proposal_action() -> impl Strategy<Value = ProposalAction> {
        prop_oneof![
            Just(ProposalAction::ParamChange(PParamsSet::default())),
            Just(ProposalAction::HardFork((9, 0))),
            root::any_stake_credential()
                .prop_map(|cred| { ProposalAction::TreasuryWithdrawal(vec![(cred, 1_000_000)]) }),
            Just(ProposalAction::Other),
            Just(ProposalAction::NoConfidence),
            (
                root::any_stake_credential(),
                root::any_stake_credential(),
                root::any_epoch(),
                root::any_rational(),
            )
                .prop_map(|(rm, add, epoch, threshold)| {
                    ProposalAction::UpdateCommittee {
                        to_remove: vec![rm],
                        to_add: vec![(add, epoch)],
                        threshold,
                    }
                }),
            (root::any_anchor(), prop::option::of(root::any_hash_28())).prop_map(
                |(anchor, guardrail_script)| ProposalAction::NewConstitution {
                    anchor,
                    guardrail_script,
                }
            ),
            Just(ProposalAction::Info),
        ]
    }

    pub fn any_vote_entry() -> impl Strategy<Value = VoteEntry> {
        (
            (root::any_slot(), root::any_vote()),
            prop::option::of((root::any_slot(), root::any_vote())),
        )
            .prop_map(|(newest, standing)| VoteEntry { newest, standing })
    }

    prop_compose! {
        pub fn any_proposal_state()(
            slot in root::any_slot(),
            tx in root::any_hash_32(),
            idx in 0u32..16u32,
            action in any_proposal_action(),
            deposit in prop::option::of(root::any_lovelace()),
            reward_account in prop::option::of(root::any_stake_credential()),
            max_epoch in prop::option::of(root::any_epoch()),
            ratified_epoch in prop::option::of(root::any_epoch()),
            canceled_epoch in prop::option::of(root::any_epoch()),
            proposed_in in prop::option::of(root::any_epoch()),
            parent in prop::option::of(root::any_gov_action_id()),
            purpose in prop::option::of(any_gov_purpose()),
            anchor in prop::option::of(root::any_anchor()),
            cc_votes in prop::collection::btree_map(root::any_stake_credential(), any_vote_entry(), 0..3),
            drep_votes in prop::collection::btree_map(root::any_stake_credential(), any_vote_entry(), 0..3),
            spo_votes in prop::collection::btree_map(root::any_hash_28(), any_vote_entry(), 0..3),
        ) -> ProposalState {
            ProposalState {
                slot,
                tx,
                idx,
                action,
                max_epoch,
                ratified_epoch,
                canceled_epoch,
                deposit,
                reward_account,
                proposed_in,
                parent,
                purpose,
                anchor,
                cc_votes,
                drep_votes,
                spo_votes,
            }
        }
    }
}

fn votes_as_of<K: Ord + Clone>(
    votes: &BTreeMap<K, VoteEntry>,
    cutoff: BlockSlot,
) -> BTreeMap<K, Vote> {
    votes
        .iter()
        .filter_map(|(voter, entry)| {
            entry
                .as_of(cutoff)
                .map(|vote| (voter.clone(), vote.clone()))
        })
        .collect()
}

/// Sets `key`'s entry in `map` (`None` removes it), returning the previous
/// one.
fn replace_entry<K: Ord>(
    map: &mut BTreeMap<K, VoteEntry>,
    key: K,
    entry: Option<VoteEntry>,
) -> Option<VoteEntry> {
    match entry {
        Some(entry) => map.insert(key, entry),
        None => map.remove(&key),
    }
}

impl ProposalState {
    pub fn key(&self) -> EntityKey {
        Self::build_entity_key(self.tx, self.idx)
    }

    pub fn gov_action_id(&self) -> GovActionId {
        GovActionId {
            transaction_id: self.tx,
            action_index: self.idx,
        }
    }

    /// The entry of `voter` in the vote map of its class, or `None` if the
    /// voter never voted on this proposal.
    pub fn vote_entry(&self, voter: &Voter) -> Option<&VoteEntry> {
        match voter {
            Voter::ConstitutionalCommitteeKey(hash) => {
                self.cc_votes.get(&StakeCredential::AddrKeyhash(*hash))
            }
            Voter::ConstitutionalCommitteeScript(hash) => {
                self.cc_votes.get(&StakeCredential::ScriptHash(*hash))
            }
            Voter::DRepKey(hash) => self.drep_votes.get(&StakeCredential::AddrKeyhash(*hash)),
            Voter::DRepScript(hash) => self.drep_votes.get(&StakeCredential::ScriptHash(*hash)),
            Voter::StakePoolKey(hash) => self.spo_votes.get(hash),
        }
    }

    /// Sets the entry of `voter` in the vote map of its class (`None`
    /// removes it), returning the previous one.
    fn replace_vote_entry(&mut self, voter: &Voter, entry: Option<VoteEntry>) -> Option<VoteEntry> {
        match voter {
            Voter::ConstitutionalCommitteeKey(hash) => replace_entry(
                &mut self.cc_votes,
                StakeCredential::AddrKeyhash(*hash),
                entry,
            ),
            Voter::ConstitutionalCommitteeScript(hash) => replace_entry(
                &mut self.cc_votes,
                StakeCredential::ScriptHash(*hash),
                entry,
            ),
            Voter::DRepKey(hash) => replace_entry(
                &mut self.drep_votes,
                StakeCredential::AddrKeyhash(*hash),
                entry,
            ),
            Voter::DRepScript(hash) => replace_entry(
                &mut self.drep_votes,
                StakeCredential::ScriptHash(*hash),
                entry,
            ),
            Voter::StakePoolKey(hash) => replace_entry(&mut self.spo_votes, *hash, entry),
        }
    }

    /// Committee votes standing at `cutoff`, keyed by hot credential.
    ///
    /// Precondition: `cutoff` is the slot before some epoch starts, and no
    /// vote is later than that epoch — the tally's cutoff (see
    /// [`VoteEntry`]).
    pub fn cc_votes_as_of(&self, cutoff: BlockSlot) -> BTreeMap<StakeCredential, Vote> {
        votes_as_of(&self.cc_votes, cutoff)
    }

    /// DRep votes standing at `cutoff`, keyed by DRep credential.
    ///
    /// Precondition: `cutoff` is the slot before some epoch starts, and no
    /// vote is later than that epoch — the tally's cutoff (see
    /// [`VoteEntry`]).
    pub fn drep_votes_as_of(&self, cutoff: BlockSlot) -> BTreeMap<StakeCredential, Vote> {
        votes_as_of(&self.drep_votes, cutoff)
    }

    /// SPO votes standing at `cutoff`, keyed by pool operator hash.
    ///
    /// Precondition: `cutoff` is the slot before some epoch starts, and no
    /// vote is later than that epoch — the tally's cutoff (see
    /// [`VoteEntry`]).
    pub fn spo_votes_as_of(&self, cutoff: BlockSlot) -> BTreeMap<PoolHash, Vote> {
        votes_as_of(&self.spo_votes, cutoff)
    }

    /// Whether the proposal is still in the live governance forest when the
    /// boundary closing `epoch` begins — no earlier boundary enacted or
    /// dropped it. Distinct from `is_active`, which keeps resolved
    /// proposals visible one extra epoch for the drop pass: a proposal
    /// enacted at the previous boundary is `is_active` here but already
    /// out of the forest.
    pub fn is_unresolved_at_close(&self, epoch: Epoch) -> bool {
        // enacted at the boundary closing `ratified_epoch`
        if self.ratified_epoch.is_some_and(|ratified| ratified < epoch) {
            return false;
        }

        // dropped at the boundary closing `canceled_epoch - 1`
        if self
            .canceled_epoch
            .is_some_and(|canceled| canceled <= epoch)
        {
            return false;
        }

        // expiry-dropped at the boundary closing `max_epoch + 1`
        if self.max_epoch.is_some_and(|max| max + 1 < epoch) {
            return false;
        }

        true
    }

    /// Build the ID of the proposal in its string form, as found on explorers.
    pub fn id(tx: Hash<32>, idx: u32) -> String {
        format!("{}#{}", hex::encode(tx), idx)
    }

    /// Get ID of the proposal in its string form, as found on explorers.
    pub fn id_as_string(&self) -> String {
        Self::id(self.tx, self.idx)
    }

    pub fn build_entity_key(tx: Hash<32>, idx: u32) -> EntityKey {
        EntityKey::from([idx.to_be_bytes().as_slice(), tx.as_slice()].concat())
    }

    pub fn expires_at(&self) -> Option<Epoch> {
        self.max_epoch.map(|x| x + 1)
    }

    pub fn has_expired(&self, current_epoch: Epoch) -> bool {
        let expires_at = self.expires_at();
        expires_at.is_some_and(|x| x <= current_epoch)
    }

    pub fn was_enacted(&self, current_epoch: Epoch) -> bool {
        if let Some(ratified_epoch) = self.ratified_epoch {
            if current_epoch > ratified_epoch + 1 {
                return true;
            }
        }

        false
    }

    pub fn was_canceled(&self, current_epoch: Epoch) -> bool {
        if let Some(canceled_epoch) = self.canceled_epoch {
            if current_epoch > canceled_epoch {
                return true;
            }
        }

        false
    }

    /// Returns true if the proposal is still beign evaluated. Not to confuse
    /// with `is_enacted`.
    pub fn is_active(&self, current_epoch: Epoch) -> bool {
        if self.was_enacted(current_epoch) {
            return false;
        }

        if self.was_canceled(current_epoch) {
            return false;
        }

        if let Some(expires_at) = self.expires_at() {
            // +1 after the expiration to allow for the drop epoch
            return current_epoch <= expires_at + 1;
        }

        true
    }
}

// --- Deltas ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewProposal {
    pub(crate) slot: BlockSlot,
    pub(crate) tx: Hash<32>,
    pub(crate) idx: u32,
    pub(crate) action: ProposalAction,
    pub(crate) deposit: Option<Lovelace>,
    pub(crate) reward_account: Option<StakeCredential>,
    pub(crate) validity_period: Option<u64>,
    pub(crate) current_epoch: Epoch,
    pub(crate) network_magic: u32,
    pub(crate) protocol: u16,

    pub(crate) prev: Option<ProposalState>,
}

impl NewProposal {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        slot: BlockSlot,
        tx: Hash<32>,
        idx: u32,
        action: ProposalAction,
        deposit: Option<Lovelace>,
        reward_account: Option<StakeCredential>,
        validity_period: Option<u64>,
        current_epoch: Epoch,
        network_magic: u32,
        protocol: u16,
    ) -> Self {
        Self {
            slot,
            tx,
            idx,
            action,
            deposit,
            reward_account,
            validity_period,
            current_epoch,
            network_magic,
            protocol,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for NewProposal {
    type Entity = ProposalState;

    fn key(&self) -> NsKey {
        NsKey::from((
            ProposalState::NS,
            ProposalState::build_entity_key(self.tx, self.idx),
        ))
    }

    fn apply(&mut self, entity: &mut Option<ProposalState>) {
        self.prev = entity.clone();

        let max_epoch = self.validity_period.map(|x| self.current_epoch + x);

        let state = ProposalState {
            slot: self.slot,
            tx: self.tx,
            idx: self.idx,
            action: self.action.clone(),
            reward_account: self.reward_account.clone(),
            deposit: self.deposit,
            max_epoch,
            // A proposal is born unresolved: the epoch boundary that
            // ratifies, expires, or prunes it stamps these through
            // [`ProposalResolved`].
            ratified_epoch: None,
            canceled_epoch: None,
            // `proposed_in` derives from data this legacy delta already
            // carries, so WAL replay of old rows populates it too. The
            // remaining phase-2 fields weren't captured at the time.
            proposed_in: Some(self.current_epoch),
            parent: None,
            purpose: None,
            anchor: None,
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        };

        let _ = entity.insert(state);
    }

    fn undo(&self, entity: &mut Option<ProposalState>) {
        *entity = self.prev.clone();
    }
}

/// Creation of a proposal with full governance capture.
///
/// Supersedes [`NewProposal`] as the emitted delta: it additionally records
/// the lineage parent, the purpose, and the metadata anchor of the submitted
/// action (the full seven-variant action mapping flows through the shared
/// `ProposalAction`). [`NewProposal`] remains only so pre-existing WAL rows
/// keep decoding and replaying — its field layout is frozen (bincode) and
/// can't grow.
///
/// `network_magic` and `protocol` are vestigial on both deltas: they fed the
/// per-network table that stamped ratification at creation. The epoch
/// boundary computes outcomes now and stamps them through
/// [`ProposalResolved`], but the fields stay — the layout is
/// bincode-positional and can't shrink any more than it can grow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewProposalV2 {
    pub(crate) slot: BlockSlot,
    pub(crate) tx: Hash<32>,
    pub(crate) idx: u32,
    pub(crate) action: ProposalAction,
    pub(crate) deposit: Option<Lovelace>,
    pub(crate) reward_account: Option<StakeCredential>,
    pub(crate) validity_period: Option<u64>,
    pub(crate) current_epoch: Epoch,
    pub(crate) network_magic: u32,
    pub(crate) protocol: u16,
    pub(crate) parent: Option<GovActionId>,
    pub(crate) purpose: Option<GovPurpose>,
    pub(crate) anchor: Option<Anchor>,

    pub(crate) prev: Option<ProposalState>,
}

impl NewProposalV2 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        slot: BlockSlot,
        tx: Hash<32>,
        idx: u32,
        action: ProposalAction,
        deposit: Option<Lovelace>,
        reward_account: Option<StakeCredential>,
        validity_period: Option<u64>,
        current_epoch: Epoch,
        network_magic: u32,
        protocol: u16,
        parent: Option<GovActionId>,
        purpose: Option<GovPurpose>,
        anchor: Option<Anchor>,
    ) -> Self {
        Self {
            slot,
            tx,
            idx,
            action,
            deposit,
            reward_account,
            validity_period,
            current_epoch,
            network_magic,
            protocol,
            parent,
            purpose,
            anchor,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for NewProposalV2 {
    type Entity = ProposalState;

    fn key(&self) -> NsKey {
        NsKey::from((
            ProposalState::NS,
            ProposalState::build_entity_key(self.tx, self.idx),
        ))
    }

    fn apply(&mut self, entity: &mut Option<ProposalState>) {
        self.prev = entity.clone();

        let max_epoch = self.validity_period.map(|x| self.current_epoch + x);

        let state = ProposalState {
            slot: self.slot,
            tx: self.tx,
            idx: self.idx,
            action: self.action.clone(),
            reward_account: self.reward_account.clone(),
            deposit: self.deposit,
            max_epoch,
            // Unresolved until a boundary rules on it ([`ProposalResolved`]).
            ratified_epoch: None,
            canceled_epoch: None,
            proposed_in: Some(self.current_epoch),
            parent: self.parent.clone(),
            purpose: self.purpose,
            anchor: self.anchor.clone(),
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        };

        let _ = entity.insert(state);
    }

    fn undo(&self, entity: &mut Option<ProposalState>) {
        *entity = self.prev.clone();
    }
}

/// A vote cast on a live proposal by any of the three voter classes
/// (constitutional committee, DRep, or stake-pool operator).
///
/// Makes `(slot, vote)` the newest vote of the voter's [`VoteEntry`] on the
/// target [`ProposalState`]. When the replaced newest vote predates
/// `epoch_start`, it becomes the standing vote. Undo restores the entry's
/// pre-image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteCast {
    pub(crate) proposal_tx: Hash<32>,
    pub(crate) proposal_idx: u32,
    pub(crate) voter: Voter,
    pub(crate) vote: Vote,
    pub(crate) slot: BlockSlot,

    /// First slot of the epoch `slot` belongs to.
    pub(crate) epoch_start: BlockSlot,

    // undo
    pub(crate) applied: bool,
    pub(crate) prev: Option<VoteEntry>,
}

impl VoteCast {
    pub fn new(
        proposal_tx: Hash<32>,
        proposal_idx: u32,
        voter: Voter,
        vote: Vote,
        slot: BlockSlot,
        epoch_start: BlockSlot,
    ) -> Self {
        Self {
            proposal_tx,
            proposal_idx,
            voter,
            vote,
            slot,
            epoch_start,
            applied: false,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for VoteCast {
    type Entity = ProposalState;

    fn key(&self) -> NsKey {
        NsKey::from((
            ProposalState::NS,
            ProposalState::build_entity_key(self.proposal_tx, self.proposal_idx),
        ))
    }

    fn apply(&mut self, entity: &mut Option<ProposalState>) {
        let Some(state) = entity.as_mut() else {
            // Valid chains only carry votes for existing proposals; an
            // in-place-upgraded store tracked proposals before this delta
            // existed, so the target should always be present. Tolerate the
            // gap instead of corrupting state.
            tracing::warn!(
                tx = %self.proposal_tx,
                idx = self.proposal_idx,
                "vote cast on unknown proposal; skipping"
            );

            self.applied = false;
            self.prev = None;
            return;
        };

        let next = VoteEntry::cast(
            state.vote_entry(&self.voter),
            self.slot,
            self.vote.clone(),
            self.epoch_start,
        );

        self.prev = state.replace_vote_entry(&self.voter, Some(next));
        self.applied = true;
    }

    fn undo(&self, entity: &mut Option<ProposalState>) {
        if !self.applied {
            return;
        }

        let Some(state) = entity.as_mut() else {
            return;
        };

        state.replace_vote_entry(&self.voter, self.prev.clone());
    }
}

/// How the epoch boundary removed a proposal from the live governance
/// forest — the three removal classes of the Conway `EPOCH` rule
/// (research §5.5 step 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalOutcome {
    /// Accepted by every body and every structural check: enacts at this
    /// boundary (`enacted`).
    Enacted,

    /// Not accepted and past its voting lifetime (`expired`).
    Expired,

    /// Not accepted and still votable, but removed as a sibling subtree of
    /// an action enacted at the same boundary (`removedDueToEnactment`).
    PrunedSibling,
}

/// The boundary's ruling on one proposal.
///
/// Stamps `ratified_epoch` / `canceled_epoch` on the target
/// [`ProposalState`] with the epoch conventions the rest of the model
/// reads: `ratified_epoch` is the epoch the enacting boundary *closes*
/// (`is_unresolved_at_close`, `was_enacted`), while `canceled_epoch` is
/// the epoch it *opens*, one later.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalResolved {
    pub(crate) tx: Hash<32>,
    pub(crate) idx: u32,
    pub(crate) outcome: ProposalOutcome,

    /// Epoch being closed by the boundary that ruled.
    pub(crate) closing_epoch: Epoch,

    // undo
    pub(crate) prev: Option<(Option<Epoch>, Option<Epoch>)>,
}

impl ProposalResolved {
    pub fn new(tx: Hash<32>, idx: u32, outcome: ProposalOutcome, closing_epoch: Epoch) -> Self {
        Self {
            tx,
            idx,
            outcome,
            closing_epoch,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for ProposalResolved {
    type Entity = ProposalState;

    fn key(&self) -> NsKey {
        NsKey::from((
            ProposalState::NS,
            ProposalState::build_entity_key(self.tx, self.idx),
        ))
    }

    fn apply(&mut self, entity: &mut Option<ProposalState>) {
        let Some(state) = entity.as_mut() else {
            // The boundary rules on rows it just read from this same
            // store, so the target is always present. Tolerate a gap
            // instead of corrupting state.
            tracing::warn!(
                tx = %self.tx,
                idx = self.idx,
                "resolution for unknown proposal; skipping"
            );
            return;
        };

        self.prev = Some((state.ratified_epoch, state.canceled_epoch));

        match self.outcome {
            ProposalOutcome::Enacted => state.ratified_epoch = Some(self.closing_epoch),
            ProposalOutcome::Expired | ProposalOutcome::PrunedSibling => {
                state.canceled_epoch = Some(self.closing_epoch + 1)
            }
        }
    }

    fn undo(&self, entity: &mut Option<ProposalState>) {
        let (Some(state), Some((ratified, canceled))) = (entity.as_mut(), self.prev) else {
            return;
        };

        state.ratified_epoch = ratified;
        state.canceled_epoch = canceled;
    }
}

#[cfg(test)]
mod compat_tests {
    use super::*;

    /// Replica of the on-disk `ProposalState` shape before the phase-2
    /// governance additions (indexes 0..=8, four-variant action). Encoding
    /// this and decoding it as the current `ProposalState` proves that
    /// pre-existing rows keep decoding, with the new fields empty.
    #[derive(Debug, Encode, Decode, Clone, PartialEq, Eq)]
    enum LegacyProposalAction {
        #[n(0)]
        ParamChange(#[n(0)] PParamsSet),

        #[n(1)]
        HardFork(#[n(0)] ProtocolVersion),

        #[n(2)]
        TreasuryWithdrawal(#[n(0)] Vec<(StakeCredential, Coin)>),

        #[n(3)]
        Other,
    }

    #[derive(Debug, Encode, Decode, Clone, PartialEq, Eq)]
    struct LegacyProposalState {
        #[n(0)]
        slot: BlockSlot,

        #[n(1)]
        tx: Hash<32>,

        #[n(2)]
        idx: u32,

        #[n(3)]
        action: LegacyProposalAction,

        #[n(4)]
        max_epoch: Option<Epoch>,

        #[n(5)]
        ratified_epoch: Option<Epoch>,

        #[n(6)]
        canceled_epoch: Option<Epoch>,

        #[n(7)]
        deposit: Option<Lovelace>,

        #[n(8)]
        reward_account: Option<StakeCredential>,
    }

    #[test]
    fn legacy_rows_decode_with_new_fields_empty() {
        let actions = [
            LegacyProposalAction::ParamChange(PParamsSet::default()),
            LegacyProposalAction::HardFork((9, 0)),
            LegacyProposalAction::TreasuryWithdrawal(vec![(
                StakeCredential::AddrKeyhash([7u8; 28].into()),
                42,
            )]),
            LegacyProposalAction::Other,
        ];

        for action in actions {
            let legacy = LegacyProposalState {
                slot: 1234,
                tx: [1u8; 32].into(),
                idx: 3,
                action,
                max_epoch: Some(500),
                ratified_epoch: None,
                canceled_epoch: Some(400),
                deposit: Some(100_000_000),
                reward_account: Some(StakeCredential::AddrKeyhash([2u8; 28].into())),
            };

            let bytes = minicbor::to_vec(&legacy).unwrap();
            let decoded: ProposalState = minicbor::decode(&bytes).unwrap();

            assert_eq!(decoded.slot, legacy.slot);
            assert_eq!(decoded.tx, legacy.tx);
            assert_eq!(decoded.idx, legacy.idx);
            assert_eq!(decoded.max_epoch, legacy.max_epoch);
            assert_eq!(decoded.ratified_epoch, legacy.ratified_epoch);
            assert_eq!(decoded.canceled_epoch, legacy.canceled_epoch);
            assert_eq!(decoded.deposit, legacy.deposit);
            assert_eq!(decoded.reward_account, legacy.reward_account);

            assert_eq!(decoded.proposed_in, None);
            assert_eq!(decoded.parent, None);
            assert_eq!(decoded.purpose, None);
            assert_eq!(decoded.anchor, None);
            assert!(decoded.cc_votes.is_empty());
            assert!(decoded.drep_votes.is_empty());
            assert!(decoded.spo_votes.is_empty());
        }
    }

    #[test]
    fn new_rows_roundtrip() {
        let mut state = ProposalState {
            slot: 1234,
            tx: [1u8; 32].into(),
            idx: 3,
            action: ProposalAction::NewConstitution {
                anchor: Anchor {
                    url: "https://example.com".to_string(),
                    content_hash: [9u8; 32].into(),
                },
                guardrail_script: Some([8u8; 28].into()),
            },
            max_epoch: Some(500),
            ratified_epoch: None,
            canceled_epoch: None,
            deposit: Some(100_000_000),
            reward_account: Some(StakeCredential::AddrKeyhash([2u8; 28].into())),
            proposed_in: Some(490),
            parent: Some(GovActionId {
                transaction_id: [3u8; 32].into(),
                action_index: 0,
            }),
            purpose: Some(GovPurpose::Constitution),
            anchor: Some(Anchor {
                url: "ipfs://meta".to_string(),
                content_hash: [4u8; 32].into(),
            }),
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        };

        state.cc_votes.insert(
            StakeCredential::AddrKeyhash([5u8; 28].into()),
            VoteEntry {
                newest: (200, Vote::No),
                standing: Some((100, Vote::Yes)),
            },
        );
        state.drep_votes.insert(
            StakeCredential::ScriptHash([6u8; 28].into()),
            VoteEntry {
                newest: (150, Vote::Abstain),
                standing: None,
            },
        );
        state.spo_votes.insert(
            [7u8; 28].into(),
            VoteEntry {
                newest: (160, Vote::Yes),
                standing: None,
            },
        );

        let bytes = minicbor::to_vec(&state).unwrap();
        let decoded: ProposalState = minicbor::decode(&bytes).unwrap();

        assert_eq!(decoded, state);
    }
}

#[cfg(test)]
mod prop_tests {
    use super::testing::{any_proposal_action, any_proposal_state};
    use super::*;
    use crate::model::testing::{self as root, assert_delta_roundtrip};
    use proptest::prelude::*;

    prop_compose! {
        fn any_new_proposal()(
            slot in root::any_slot(),
            tx in root::any_hash_32(),
            idx in 0u32..16u32,
            deposit in prop::option::of(root::any_lovelace()),
            reward_account in prop::option::of(root::any_stake_credential()),
            validity_period in prop::option::of(1u64..10u64),
            current_epoch in root::any_epoch(),
            network_magic in any::<u32>(),
            protocol in 0u16..10u16,
        ) -> NewProposal {
            NewProposal::new(
                slot, tx, idx,
                ProposalAction::Other,
                deposit, reward_account, validity_period,
                current_epoch, network_magic, protocol,
            )
        }
    }

    prop_compose! {
        fn any_new_proposal_v2()(
            slot in root::any_slot(),
            tx in root::any_hash_32(),
            idx in 0u32..16u32,
            action in any_proposal_action(),
            deposit in prop::option::of(root::any_lovelace()),
            reward_account in prop::option::of(root::any_stake_credential()),
            validity_period in prop::option::of(1u64..10u64),
            current_epoch in root::any_epoch(),
            network_magic in any::<u32>(),
            protocol in 0u16..10u16,
            parent in prop::option::of(root::any_gov_action_id()),
            purpose in prop::option::of(super::testing::any_gov_purpose()),
            anchor in prop::option::of(root::any_anchor()),
        ) -> NewProposalV2 {
            NewProposalV2::new(
                slot, tx, idx, action,
                deposit, reward_account, validity_period,
                current_epoch, network_magic, protocol,
                parent, purpose, anchor,
            )
        }
    }

    /// A slot and the start of its epoch.
    fn any_slot_in_epoch() -> impl Strategy<Value = (BlockSlot, BlockSlot)> {
        (root::any_slot(), 0u64..432_000u64)
            .prop_map(|(slot, offset)| (slot, slot.saturating_sub(offset)))
    }

    prop_compose! {
        fn any_vote_cast()(
            proposal_tx in root::any_hash_32(),
            proposal_idx in 0u32..16u32,
            voter in root::any_voter(),
            vote in root::any_vote(),
            (slot, epoch_start) in any_slot_in_epoch(),
        ) -> VoteCast {
            VoteCast::new(proposal_tx, proposal_idx, voter, vote, slot, epoch_start)
        }
    }

    /// A vote cast targeting a proposal that exists, aimed at the entity the
    /// strategy pairs it with (`any_proposal_state` fixes tx/idx per sample).
    fn any_matching_vote_cast(tx: Hash<32>, idx: u32) -> impl Strategy<Value = VoteCast> {
        (root::any_voter(), root::any_vote(), any_slot_in_epoch()).prop_map(
            move |(voter, vote, (slot, epoch_start))| {
                VoteCast::new(tx, idx, voter, vote, slot, epoch_start)
            },
        )
    }

    proptest! {
        #[test]
        fn new_proposal_roundtrip(
            entity in prop::option::of(any_proposal_state()),
            delta in any_new_proposal(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn new_proposal_v2_roundtrip(
            entity in prop::option::of(any_proposal_state()),
            delta in any_new_proposal_v2(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn new_proposal_v2_serde_roundtrip(
            entity in prop::option::of(any_proposal_state()),
            delta in any_new_proposal_v2(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn vote_cast_roundtrip(
            entity in prop::option::of(any_proposal_state()),
            delta in any_vote_cast(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn vote_cast_serde_roundtrip(
            entity in prop::option::of(any_proposal_state()),
            delta in any_vote_cast(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn vote_cast_on_existing_proposal_roundtrip(
            (entity, delta) in any_proposal_state()
                .prop_flat_map(|state| {
                    let deltas = any_matching_vote_cast(state.tx, state.idx);
                    (Just(state), deltas)
                }),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }
    }

    const TX: [u8; 32] = [1u8; 32];

    fn proposal() -> ProposalState {
        ProposalState {
            slot: 10,
            tx: TX.into(),
            idx: 0,
            action: ProposalAction::Info,
            max_epoch: None,
            ratified_epoch: None,
            canceled_epoch: None,
            deposit: None,
            reward_account: None,
            proposed_in: Some(1),
            parent: None,
            purpose: None,
            anchor: None,
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        }
    }

    /// Short epochs, so random sequences re-vote within and across epochs
    /// and share slots.
    const EPOCH_LENGTH: BlockSlot = 10;

    const EPOCHS: u64 = 6;

    /// Two voters of every class. Classes share hashes, so a write routed to
    /// the wrong map shows up in the wrong read.
    fn voters() -> Vec<Voter> {
        [[1u8; 28], [2u8; 28]]
            .into_iter()
            .flat_map(|hash| {
                let hash = Hash::from(hash);

                [
                    Voter::ConstitutionalCommitteeKey(hash),
                    Voter::ConstitutionalCommitteeScript(hash),
                    Voter::DRepKey(hash),
                    Voter::DRepScript(hash),
                    Voter::StakePoolKey(hash),
                ]
            })
            .collect()
    }

    /// Votes over epochs `1..=EPOCHS` in slot order, as the roll pipeline
    /// emits them.
    fn any_vote_sequence() -> impl Strategy<Value = Vec<(Voter, BlockSlot, Vote)>> {
        prop::collection::vec(
            (
                prop::sample::select(voters()),
                EPOCH_LENGTH..(EPOCHS + 1) * EPOCH_LENGTH,
                root::any_vote(),
            ),
            0..48,
        )
        .prop_map(|mut votes| {
            votes.sort_by_key(|(_, slot, _)| *slot);
            votes
        })
    }

    /// The reference: every vote, per voter, in the map its class reads.
    #[derive(Default)]
    struct History {
        cc: BTreeMap<StakeCredential, Vec<(BlockSlot, Vote)>>,
        drep: BTreeMap<StakeCredential, Vec<(BlockSlot, Vote)>>,
        spo: BTreeMap<PoolHash, Vec<(BlockSlot, Vote)>>,
    }

    impl History {
        fn push(&mut self, voter: &Voter, slot: BlockSlot, vote: Vote) {
            let cast = (slot, vote);

            match voter {
                Voter::ConstitutionalCommitteeKey(hash) => self
                    .cc
                    .entry(StakeCredential::AddrKeyhash(*hash))
                    .or_default()
                    .push(cast),
                Voter::ConstitutionalCommitteeScript(hash) => self
                    .cc
                    .entry(StakeCredential::ScriptHash(*hash))
                    .or_default()
                    .push(cast),
                Voter::DRepKey(hash) => self
                    .drep
                    .entry(StakeCredential::AddrKeyhash(*hash))
                    .or_default()
                    .push(cast),
                Voter::DRepScript(hash) => self
                    .drep
                    .entry(StakeCredential::ScriptHash(*hash))
                    .or_default()
                    .push(cast),
                Voter::StakePoolKey(hash) => self.spo.entry(*hash).or_default().push(cast),
            }
        }
    }

    /// The last vote at or before `cutoff`, per voter.
    fn history_as_of<K: Ord + Clone>(
        votes: &BTreeMap<K, Vec<(BlockSlot, Vote)>>,
        cutoff: BlockSlot,
    ) -> BTreeMap<K, Vote> {
        votes
            .iter()
            .filter_map(|(voter, history)| {
                history
                    .iter()
                    .rfind(|(slot, _)| *slot <= cutoff)
                    .map(|(_, vote)| (voter.clone(), vote.clone()))
            })
            .collect()
    }

    proptest! {
        /// Done criterion: at every epoch's tally cutoff, the bounded read
        /// equals the read over the full history, for every voter class.
        #[test]
        fn bounded_votes_match_full_history_at_every_tally_cutoff(
            votes in any_vote_sequence(),
        ) {
            use dolos_core::EntityDelta as _;

            let mut entity = Some(proposal());
            let mut history = History::default();
            let mut votes = votes.into_iter().peekable();

            for epoch in 1..=EPOCHS {
                let epoch_start = epoch * EPOCH_LENGTH;
                let next_start = epoch_start + EPOCH_LENGTH;

                while let Some((voter, slot, vote)) =
                    votes.next_if(|(_, slot, _)| *slot < next_start)
                {
                    let mut delta =
                        VoteCast::new(TX.into(), 0, voter.clone(), vote.clone(), slot, epoch_start);
                    delta.apply(&mut entity);
                    history.push(&voter, slot, vote);
                }

                // the boundary closing `epoch`
                let cutoff = epoch_start - 1;
                let state = entity.as_ref().unwrap();

                prop_assert_eq!(state.cc_votes_as_of(cutoff), history_as_of(&history.cc, cutoff));
                prop_assert_eq!(state.drep_votes_as_of(cutoff), history_as_of(&history.drep, cutoff));
                prop_assert_eq!(state.spo_votes_as_of(cutoff), history_as_of(&history.spo, cutoff));
            }
        }
    }

    #[test]
    fn vote_cast_keeps_newest_and_standing_votes() {
        use dolos_core::EntityDelta as _;

        let mut entity = Some(proposal());
        let voter = Voter::DRepKey([5u8; 28].into());
        let key = StakeCredential::AddrKeyhash([5u8; 28].into());

        // epochs of 100 slots: two votes in epoch 1, two in epoch 2
        let mut casts = [
            (110, Vote::No, 100),
            (150, Vote::Yes, 100),
            (220, Vote::Abstain, 200),
            (250, Vote::No, 200),
        ]
        .map(|(slot, vote, epoch_start)| {
            VoteCast::new(TX.into(), 0, voter.clone(), vote, slot, epoch_start)
        });

        let expected = [
            ((110, Vote::No), None),
            ((150, Vote::Yes), None),
            ((220, Vote::Abstain), Some((150, Vote::Yes))),
            ((250, Vote::No), Some((150, Vote::Yes))),
        ];

        for (cast, (newest, standing)) in casts.iter_mut().zip(expected) {
            cast.apply(&mut entity);

            let state = entity.as_ref().unwrap();
            assert_eq!(
                state.drep_votes.get(&key),
                Some(&VoteEntry { newest, standing })
            );
        }

        for cast in casts.iter().rev() {
            cast.undo(&mut entity);
        }

        assert_eq!(entity, Some(proposal()));
    }

    proptest! {
        /// For every voter class, the read side finds the map and the key
        /// that the write side used.
        #[test]
        fn vote_entry_reads_back_what_vote_cast_wrote(
            state in any_proposal_state(),
            voter in root::any_voter(),
            vote in root::any_vote(),
            (slot, epoch_start) in any_slot_in_epoch(),
        ) {
            use dolos_core::EntityDelta as _;

            let mut delta = VoteCast::new(
                state.tx,
                state.idx,
                voter.clone(),
                vote.clone(),
                slot,
                epoch_start,
            );
            let mut entity = Some(state);
            delta.apply(&mut entity);

            let entry = entity.as_ref().unwrap().vote_entry(&voter);
            prop_assert_eq!(entry.map(|entry| &entry.newest), Some(&(slot, vote)));
        }
    }
}

#[cfg(test)]
mod resolution_tests {
    use dolos_core::EntityDelta as _;

    use super::*;

    fn proposal() -> ProposalState {
        ProposalState {
            slot: 0,
            tx: [3u8; 32].into(),
            idx: 1,
            action: ProposalAction::Info,
            max_epoch: Some(510),
            ratified_epoch: None,
            canceled_epoch: None,
            deposit: Some(100),
            reward_account: None,
            proposed_in: Some(500),
            parent: None,
            purpose: None,
            anchor: None,
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        }
    }

    /// The two epoch conventions the rest of the model reads back, and
    /// which the outcome table encoded by hand: an enactment names the
    /// epoch its boundary *closed*, a removal the epoch that boundary
    /// *opened*. Both make the proposal resolved from the next boundary
    /// on, and neither before it.
    #[test]
    fn resolution_stamps_the_epoch_each_consumer_expects() {
        let cases = [
            (ProposalOutcome::Enacted, Some(505), None),
            (ProposalOutcome::Expired, None, Some(506)),
            (ProposalOutcome::PrunedSibling, None, Some(506)),
        ];

        for (outcome, ratified, canceled) in cases {
            let mut entity = Some(proposal());

            let mut delta = ProposalResolved::new([3u8; 32].into(), 1, outcome, 505);
            delta.apply(&mut entity);

            let state = entity.as_ref().unwrap();
            assert_eq!(state.ratified_epoch, ratified, "{outcome:?}");
            assert_eq!(state.canceled_epoch, canceled, "{outcome:?}");

            // still in the forest for the boundary that ruled, out of it
            // for the next one
            assert!(state.is_unresolved_at_close(505), "{outcome:?}");
            assert!(!state.is_unresolved_at_close(506), "{outcome:?}");

            delta.undo(&mut entity);
            assert_eq!(entity, Some(proposal()), "{outcome:?}");
        }
    }

    /// A proposal is born unresolved. Outcome stamping at creation is what
    /// the per-network table did, and the only thing that could: the
    /// boundary that rules hadn't run yet.
    #[test]
    fn creation_leaves_the_outcome_open() {
        let mut entity = None;

        let mut delta = NewProposalV2::new(
            10,
            [3u8; 32].into(),
            1,
            ProposalAction::Info,
            Some(100),
            None,
            Some(10),
            500,
            764824073,
            10,
            None,
            None,
            None,
        );

        delta.apply(&mut entity);

        let state = entity.as_ref().unwrap();
        assert_eq!(state.ratified_epoch, None);
        assert_eq!(state.canceled_epoch, None);
        assert_eq!(state.max_epoch, Some(510));
        assert!(state.is_unresolved_at_close(505));
    }
}
