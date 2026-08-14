//! Governance singleton state (`"gov"` namespace).
//!
//! Mirrors the Conway governance residue that has no per-entity home:
//! the enacted constitution, the constitutional committee, the committee
//! hot-key authorization map (`vsCommitteeState` in the Haskell ledger),
//! the four per-purpose previous-governance-action roots, and the
//! dormant-epoch counter (`vsNumDormantEpochs`).
//!
//! A single entity lives in the namespace under [`GOV_STATE_KEY`]. Its
//! existence is an invariant: every store carries the row regardless of
//! era — created inactive at genesis bootstrap (or by the CARDANO-006
//! startup migration on stores bootstrapped before the invariant) and
//! activated with the Conway genesis enact-state at the era boundary
//! that enters protocol 9 (or directly at bootstrap for networks that
//! force-start in Conway). `active_since` distinguishes the phases.
//!
//! Stores upgraded in place past the Chang boundary get the row from the
//! migration, but their enact-state fields stay unset — the committee
//! certs and enactments since the boundary were never recorded; complete
//! governance content comes from a fresh sync.

use std::collections::BTreeMap;

use dolos_core::{BlockSlot, NsKey};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    ledger::primitives::{
        conway::{Anchor, DRep, GovActionId, RationalNumber},
        Epoch, ScriptHash, StakeCredential,
    },
};
use serde::{Deserialize, Serialize};

use super::{GovPurpose, PoolHash, SingletonEntity};

/// Key of the single `GovState` entity inside the `"gov"` namespace.
pub const GOV_STATE_KEY: &[u8] = b"0";

/// Expect message for delta apply/undo on the governance singleton — its
/// existence is an invariant, so a miss means a corrupt store.
const GOV_MUST_EXIST: &str =
    "gov singleton must exist: seeded at bootstrap or by the CARDANO-006 migration";

/// The enacted constitution: metadata anchor plus the optional guardrails
/// script hash.
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Constitution {
    #[n(0)]
    pub anchor: Anchor,

    #[n(1)]
    pub guardrail_script: Option<ScriptHash>,
}

/// The enacted constitutional committee: members (cold credential → term
/// expiry epoch, inclusive) and the vote threshold.
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Committee {
    #[n(0)]
    pub members: BTreeMap<StakeCredential, Epoch>,

    #[n(1)]
    pub threshold: RationalNumber,
}

/// Authorization state of one committee cold credential — the Haskell
/// `CommitteeAuthorization`.
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommitteeAuthorization {
    #[n(0)]
    HotCredential(#[n(0)] StakeCredential),

    #[n(1)]
    Resigned(#[n(0)] Option<Anchor>),
}

/// Slot-stamped, append-only authorization history for a single committee
/// cold credential. The last entry is the effective authorization; keeping
/// the full history is what makes as-of reads possible at epoch boundaries,
/// where the committee tally must use the authorization as of the previous
/// boundary even if the member re-authorized since. Committee events are
/// rare, so histories stay tiny.
pub type AuthHistory = Vec<(BlockSlot, CommitteeAuthorization)>;

/// Crash-safe accumulator for the epoch-boundary stake distributions: the
/// DRep voting-power distribution (`drepDistr` in the Haskell ledger) and the
/// per-pool stake distribution (SPO tally denominator), both computed by the
/// EWRAP sharded account scan from the `mark`-position account state — the
/// pulser-snapshot-equivalent read at the boundary.
///
/// Shards merge their contribution through [`GovDistrAccumulate`], which is
/// idempotent per `(closing_epoch, shard)` so a resumed mid-scan EWRAP cannot
/// double-count. The accumulator is keyed by the epoch being closed and is
/// replaced wholesale when the next boundary's shard 0 arrives — which is
/// also what prunes the previous epoch's maps.
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GovDistr {
    /// The epoch the accumulating EWRAP pass is closing.
    #[n(0)]
    pub closing_epoch: Epoch,

    /// Shards `0..committed_shards` have merged their contribution;
    /// `committed_shards == total_shards` means the distributions are
    /// complete.
    #[n(1)]
    pub committed_shards: u32,

    /// Shard plan of the accumulating boundary.
    #[n(2)]
    pub total_shards: u32,

    /// Delegated stake per DRep. `AlwaysAbstain` / `AlwaysNoConfidence`
    /// accumulate under their own keys — the ratification tallies need both.
    #[n(3)]
    pub drep_distr: BTreeMap<DRep, u64>,

    /// Delegated stake per pool.
    #[n(4)]
    pub pool_distr: BTreeMap<PoolHash, u64>,

    /// Total pool-delegated stake (SPO tally denominator input).
    #[n(5)]
    pub pool_total: u64,
}

impl GovDistr {
    pub fn new(closing_epoch: Epoch, total_shards: u32) -> Self {
        Self {
            closing_epoch,
            committed_shards: 0,
            total_shards,
            drep_distr: BTreeMap::new(),
            pool_distr: BTreeMap::new(),
            pool_total: 0,
        }
    }

    /// Whether every shard of the boundary closing `epoch` has merged its
    /// contribution.
    pub fn is_complete_for(&self, epoch: Epoch) -> bool {
        self.closing_epoch == epoch && self.committed_shards == self.total_shards
    }
}

/// The four per-purpose previous-governance-action roots (`prevGovActionIds`
/// in the Haskell ledger). Updated on enactment; consumed by the
/// `prevActionAsExpected` check during ratification.
#[derive(Debug, Encode, Decode, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GovRoots {
    #[n(0)]
    pub pparam_update: Option<GovActionId>,

    #[n(1)]
    pub hard_fork: Option<GovActionId>,

    #[n(2)]
    pub committee: Option<GovActionId>,

    #[n(3)]
    pub constitution: Option<GovActionId>,
}

impl GovRoots {
    /// The root slot of a single lineage tree, for read-modify-write by the
    /// enactment deltas.
    pub fn root_mut(&mut self, purpose: GovPurpose) -> &mut Option<GovActionId> {
        match purpose {
            GovPurpose::PParamUpdate => &mut self.pparam_update,
            GovPurpose::HardFork => &mut self.hard_fork,
            GovPurpose::Committee => &mut self.committee,
            GovPurpose::Constitution => &mut self.constitution,
        }
    }
}

#[derive(Debug, Encode, Decode, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GovState {
    /// The enacted constitution. `None` before governance activates
    /// (`active_since` unset) — or on a migrated store whose enact-state
    /// is unknown (in-place upgrade gap; fresh sync recovers it).
    #[n(0)]
    pub constitution: Option<Constitution>,

    /// The enacted committee. `None` means the no-confidence state — or,
    /// as with `constitution`, pre-activation / the migration gap.
    #[n(1)]
    pub committee: Option<Committee>,

    /// Per cold credential, the slot-stamped history of hot-key
    /// authorizations and resignations.
    #[n(2)]
    #[cbor(default)]
    pub committee_auths: BTreeMap<StakeCredential, AuthHistory>,

    /// The four per-purpose previous-enacted-action roots.
    #[n(3)]
    #[cbor(default)]
    pub prev_gov_action_ids: GovRoots,

    /// Count of consecutive epochs with zero live proposals
    /// (`vsNumDormantEpochs`). Folded into every DRep's stored expiry when
    /// a proposal finally shows up, then reset to zero.
    #[n(4)]
    #[cbor(default)]
    pub num_dormant_epochs: u64,

    /// Epoch since which governance is active — the chain entered Conway.
    /// `None` means governance hasn't activated yet (pre-Conway). Set to 0
    /// for networks that force-start in Conway, to the Chang-boundary
    /// epoch otherwise, and derived from the era summary by the startup
    /// migration (which leaves the enact-state fields unset).
    #[n(5)]
    #[cbor(default)]
    pub active_since: Option<Epoch>,

    /// The boundary stake-distribution accumulator. `None` until the first
    /// governance-active boundary accumulates; afterwards holds the most
    /// recent boundary's distributions (in progress while its shards run,
    /// complete after the last one) until the next boundary's shard 0
    /// replaces it. Index 6 must not be reused for anything else.
    #[n(6)]
    #[cbor(default)]
    pub distr: Option<GovDistr>,
}

entity_boilerplate!(GovState, "gov");

impl SingletonEntity for GovState {
    const KEY: &'static [u8] = GOV_STATE_KEY;
}

impl GovState {
    /// Activate governance with the Conway genesis enact-state (the
    /// initial constitution and committee). Shared by the two activation
    /// paths — `bootstrap_gov` for networks that force-start in Conway
    /// and `GovGenesisInit` at the Chang boundary — so they can't drift.
    pub fn seed_genesis(
        &mut self,
        constitution: Constitution,
        committee: Committee,
        active_since: Epoch,
    ) {
        self.constitution = Some(constitution);
        self.committee = Some(committee);
        self.active_since = Some(active_since);
    }

    /// Effective authorization of `cold` as of the latest event.
    pub fn committee_auth(&self, cold: &StakeCredential) -> Option<&CommitteeAuthorization> {
        self.committee_auths
            .get(cold)
            .and_then(|history| history.last())
            .map(|(_, auth)| auth)
    }

    /// Effective authorization of `cold` considering only events at or
    /// before `slot` — the as-of read used by boundary tallies.
    pub fn committee_auth_as_of(
        &self,
        cold: &StakeCredential,
        slot: BlockSlot,
    ) -> Option<&CommitteeAuthorization> {
        self.committee_auths
            .get(cold)
            .and_then(|history| history.iter().rev().find(|(at, _)| *at <= slot))
            .map(|(_, auth)| auth)
    }
}

/// Parse the committee-member key format used by Conway genesis files:
/// `"scriptHash-<hex>"` or `"keyHash-<hex>"`.
fn parse_genesis_committee_member(raw: &str) -> Option<StakeCredential> {
    let (kind, hex_part) = raw.split_once('-')?;
    let bytes = hex::decode(hex_part).ok()?;
    let hash: [u8; 28] = bytes.try_into().ok()?;

    match kind {
        "scriptHash" => Some(StakeCredential::ScriptHash(hash.into())),
        "keyHash" => Some(StakeCredential::AddrKeyhash(hash.into())),
        _ => None,
    }
}

/// Map the Conway genesis file's constitution + committee into their model
/// representation. This is the initial enact-state a network starts Conway
/// with; every later value comes from enacted governance actions.
pub fn gov_from_conway_genesis(
    genesis: &pallas::interop::hardano::configs::conway::GenesisFile,
) -> Result<(Constitution, Committee), dolos_core::ChainError> {
    let malformed =
        |what: &str| dolos_core::ChainError::GenesisFieldMissing(format!("conway {what}"));

    let content_hash = hex::decode(&genesis.constitution.anchor.data_hash)
        .ok()
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        .ok_or_else(|| malformed("constitution.anchor.dataHash"))?;

    let guardrail_script = genesis
        .constitution
        .script
        .as_ref()
        .map(|script| {
            hex::decode(script)
                .ok()
                .and_then(|bytes| <[u8; 28]>::try_from(bytes).ok())
                .map(ScriptHash::from)
                .ok_or_else(|| malformed("constitution.script"))
        })
        .transpose()?;

    let constitution = Constitution {
        anchor: Anchor {
            url: genesis.constitution.anchor.url.clone(),
            content_hash: content_hash.into(),
        },
        guardrail_script,
    };

    let mut members = BTreeMap::new();

    for (raw, term) in genesis.committee.members.iter() {
        let cred =
            parse_genesis_committee_member(raw).ok_or_else(|| malformed("committee.members"))?;
        members.insert(cred, *term);
    }

    let committee = Committee {
        members,
        threshold: genesis.committee.threshold.clone().into(),
    };

    Ok((constitution, committee))
}

// --- Deltas ---

/// Activate the governance singleton with the Conway genesis constitution
/// and committee, effective from `epoch`. Emitted once, at the era boundary
/// that enters protocol 9 (Chang); networks that force-start in Conway
/// activate the entity directly at bootstrap instead. Undo restores the
/// inactive row — never removes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovGenesisInit {
    pub(crate) constitution: Constitution,
    pub(crate) committee: Committee,
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) prev_constitution: Option<Constitution>,
    pub(crate) prev_committee: Option<Committee>,
    pub(crate) prev_active_since: Option<Epoch>,
}

impl GovGenesisInit {
    pub fn new(constitution: Constitution, committee: Committee, epoch: Epoch) -> Self {
        Self {
            constitution,
            committee,
            epoch,
            prev_constitution: None,
            prev_committee: None,
            prev_active_since: None,
        }
    }
}

impl dolos_core::EntityDelta for GovGenesisInit {
    type Entity = GovState;

    fn key(&self) -> NsKey {
        GovState::ns_key()
    }

    fn apply(&mut self, entity: &mut Option<GovState>) {
        let state = entity.as_mut().expect(GOV_MUST_EXIST);

        self.prev_constitution = state.constitution.clone();
        self.prev_committee = state.committee.clone();
        self.prev_active_since = state.active_since;

        state.seed_genesis(
            self.constitution.clone(),
            self.committee.clone(),
            self.epoch,
        );
    }

    fn undo(&self, entity: &mut Option<GovState>) {
        let state = entity.as_mut().expect(GOV_MUST_EXIST);

        state.constitution = self.prev_constitution.clone();
        state.committee = self.prev_committee.clone();
        state.active_since = self.prev_active_since;
    }
}

/// A committee member authorized a hot credential (`AuthCommitteeHot`
/// certificate). Appends `(slot, HotCredential)` to the cold credential's
/// history; undo pops exactly the appended entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitteeAuth {
    pub(crate) cold: StakeCredential,
    pub(crate) hot: StakeCredential,
    pub(crate) slot: BlockSlot,

    // undo
    pub(crate) created_entry: bool,
}

impl CommitteeAuth {
    pub fn new(cold: StakeCredential, hot: StakeCredential, slot: BlockSlot) -> Self {
        Self {
            cold,
            hot,
            slot,
            created_entry: false,
        }
    }
}

/// A committee member resigned (`ResignCommitteeCold` certificate).
/// Appends `(slot, Resigned)` to the cold credential's history; undo pops
/// exactly the appended entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitteeResign {
    pub(crate) cold: StakeCredential,
    pub(crate) anchor: Option<Anchor>,
    pub(crate) slot: BlockSlot,

    // undo
    pub(crate) created_entry: bool,
}

impl CommitteeResign {
    pub fn new(cold: StakeCredential, anchor: Option<Anchor>, slot: BlockSlot) -> Self {
        Self {
            cold,
            anchor,
            slot,
            created_entry: false,
        }
    }
}

/// Append an authorization event to `cold`'s history. Returns whether the
/// history entry itself was created (undo state for `pop_auth`).
fn push_auth(
    entity: &mut Option<GovState>,
    cold: &StakeCredential,
    slot: BlockSlot,
    auth: CommitteeAuthorization,
) -> bool {
    let state = entity.as_mut().expect(GOV_MUST_EXIST);

    let created_entry = !state.committee_auths.contains_key(cold);

    state
        .committee_auths
        .entry(cold.clone())
        .or_default()
        .push((slot, auth));

    created_entry
}

fn pop_auth(entity: &mut Option<GovState>, cold: &StakeCredential, created: bool) {
    let state = entity.as_mut().expect(GOV_MUST_EXIST);

    if let Some(history) = state.committee_auths.get_mut(cold) {
        history.pop();
    }

    if created {
        state.committee_auths.remove(cold);
    }
}

impl dolos_core::EntityDelta for CommitteeAuth {
    type Entity = GovState;

    fn key(&self) -> NsKey {
        GovState::ns_key()
    }

    fn apply(&mut self, entity: &mut Option<GovState>) {
        self.created_entry = push_auth(
            entity,
            &self.cold,
            self.slot,
            CommitteeAuthorization::HotCredential(self.hot.clone()),
        );
    }

    fn undo(&self, entity: &mut Option<GovState>) {
        pop_auth(entity, &self.cold, self.created_entry);
    }
}

impl dolos_core::EntityDelta for CommitteeResign {
    type Entity = GovState;

    fn key(&self) -> NsKey {
        GovState::ns_key()
    }

    fn apply(&mut self, entity: &mut Option<GovState>) {
        self.created_entry = push_auth(
            entity,
            &self.cold,
            self.slot,
            CommitteeAuthorization::Resigned(self.anchor.clone()),
        );
    }

    fn undo(&self, entity: &mut Option<GovState>) {
        pop_auth(entity, &self.cold, self.created_entry);
    }
}

/// The committee effect an enacted governance action carries.
///
/// Variant order is part of the WAL format (bincode positional encoding) —
/// append only, never reorder.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommitteeChange {
    /// `NoConfidence` — the committee is dissolved outright.
    NoConfidence,

    /// `UpdateCommittee` — the surviving members are the current ones minus
    /// `to_remove` plus `to_add`, under the new `threshold`.
    Update {
        to_remove: Vec<StakeCredential>,
        to_add: Vec<(StakeCredential, Epoch)>,
        threshold: RationalNumber,
    },
}

/// A ratified committee action reached enactment. Rewrites
/// `GovState.committee`; undo restores the pre-image wholesale, which also
/// covers the `None` (no-confidence) pre-state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitteeUpdate {
    pub(crate) change: CommitteeChange,

    // undo
    pub(crate) prev: Option<Committee>,
}

impl CommitteeUpdate {
    pub fn no_confidence() -> Self {
        Self {
            change: CommitteeChange::NoConfidence,
            prev: None,
        }
    }

    pub fn update(
        to_remove: Vec<StakeCredential>,
        to_add: Vec<(StakeCredential, Epoch)>,
        threshold: RationalNumber,
    ) -> Self {
        Self {
            change: CommitteeChange::Update {
                to_remove,
                to_add,
                threshold,
            },
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for CommitteeUpdate {
    type Entity = GovState;

    fn key(&self) -> NsKey {
        GovState::ns_key()
    }

    fn apply(&mut self, entity: &mut Option<GovState>) {
        let state = entity.as_mut().expect(GOV_MUST_EXIST);

        self.prev = state.committee.clone();

        state.committee = match &self.change {
            CommitteeChange::NoConfidence => None,
            CommitteeChange::Update {
                to_remove,
                to_add,
                threshold,
            } => {
                // An update enacted out of the no-confidence state starts from
                // an empty member set, as in the Haskell ledger.
                let mut members = state
                    .committee
                    .as_ref()
                    .map(|committee| committee.members.clone())
                    .unwrap_or_default();

                for cold in to_remove {
                    members.remove(cold);
                }

                for (cold, term) in to_add {
                    members.insert(cold.clone(), *term);
                }

                Some(Committee {
                    members,
                    threshold: threshold.clone(),
                })
            }
        };
    }

    fn undo(&self, entity: &mut Option<GovState>) {
        let state = entity.as_mut().expect(GOV_MUST_EXIST);

        state.committee = self.prev.clone();
    }
}

/// A ratified `NewConstitution` action reached enactment. Replaces
/// `GovState.constitution`; undo restores the pre-image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstitutionUpdate {
    pub(crate) constitution: Constitution,

    // undo
    pub(crate) prev: Option<Constitution>,
}

impl ConstitutionUpdate {
    pub fn new(constitution: Constitution) -> Self {
        Self {
            constitution,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for ConstitutionUpdate {
    type Entity = GovState;

    fn key(&self) -> NsKey {
        GovState::ns_key()
    }

    fn apply(&mut self, entity: &mut Option<GovState>) {
        let state = entity.as_mut().expect(GOV_MUST_EXIST);

        self.prev = state.constitution.clone();
        state.constitution = Some(self.constitution.clone());
    }

    fn undo(&self, entity: &mut Option<GovState>) {
        let state = entity.as_mut().expect(GOV_MUST_EXIST);

        state.constitution = self.prev.clone();
    }
}

/// An enacted action becomes the new root of its purpose's lineage tree
/// (`prevGovActionIds` in the Haskell ledger). Emitted alongside whatever
/// state effect the action carries, for every action that has a purpose —
/// `TreasuryWithdrawal` and `Info` have none and emit nothing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovRootsUpdate {
    pub(crate) purpose: GovPurpose,
    pub(crate) action: GovActionId,

    // undo
    pub(crate) prev: Option<GovActionId>,
}

impl GovRootsUpdate {
    pub fn new(purpose: GovPurpose, action: GovActionId) -> Self {
        Self {
            purpose,
            action,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for GovRootsUpdate {
    type Entity = GovState;

    fn key(&self) -> NsKey {
        GovState::ns_key()
    }

    fn apply(&mut self, entity: &mut Option<GovState>) {
        let state = entity.as_mut().expect(GOV_MUST_EXIST);
        let root = state.prev_gov_action_ids.root_mut(self.purpose);

        self.prev = root.replace(self.action.clone());
    }

    fn undo(&self, entity: &mut Option<GovState>) {
        let state = entity.as_mut().expect(GOV_MUST_EXIST);

        *state.prev_gov_action_ids.root_mut(self.purpose) = self.prev.clone();
    }
}

/// Reset the dormant-epoch counter to zero. Emitted together with the
/// per-DRep [`crate::DRepDormancyRelease`] fan-out when the first proposal
/// after a dormant stretch shows up (research §3.3.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovDormancyReset {
    // undo
    pub(crate) prev: u64,
}

impl GovDormancyReset {
    pub fn new() -> Self {
        Self { prev: 0 }
    }
}

impl Default for GovDormancyReset {
    fn default() -> Self {
        Self::new()
    }
}

impl dolos_core::EntityDelta for GovDormancyReset {
    type Entity = GovState;

    fn key(&self) -> NsKey {
        GovState::ns_key()
    }

    fn apply(&mut self, entity: &mut Option<GovState>) {
        let state = entity.as_mut().expect(GOV_MUST_EXIST);

        self.prev = state.num_dormant_epochs;
        state.num_dormant_epochs = 0;
    }

    fn undo(&self, entity: &mut Option<GovState>) {
        let state = entity.as_mut().expect(GOV_MUST_EXIST);

        state.num_dormant_epochs = self.prev;
    }
}

/// Merge one EWRAP shard's stake-distribution contribution into
/// `GovState.distr`. Emitted once per shard by the boundary account scan.
///
/// Idempotent per `(closing_epoch, shard)` and ordered, mirroring the
/// `EWrapProgress` guard discipline: a shard whose merge already landed is
/// skipped (crash-resume replay), an out-of-order shard is skipped as a
/// broken invariant, and a `total_shards` mismatch against the in-flight
/// accumulator is skipped as corruption. Shard 0 of a new closing epoch
/// replaces the previous epoch's accumulator wholesale (the pruning step);
/// undo restores the full pre-image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovDistrAccumulate {
    pub(crate) closing_epoch: Epoch,
    pub(crate) shard: u32,
    pub(crate) total_shards: u32,
    pub(crate) drep_distr: BTreeMap<DRep, u64>,
    pub(crate) pool_distr: BTreeMap<PoolHash, u64>,
    pub(crate) pool_total: u64,

    // undo — captured by `apply` only when state was actually mutated
    // (i.e. the idempotency / ordering / consistency guards all passed).
    // When `applied = false`, `undo` is a no-op so a rolled-back skip
    // can't clobber the accumulator.
    pub(crate) applied: bool,
    pub(crate) prev: Option<GovDistr>,
}

impl GovDistrAccumulate {
    pub fn new(
        closing_epoch: Epoch,
        shard: u32,
        total_shards: u32,
        drep_distr: BTreeMap<DRep, u64>,
        pool_distr: BTreeMap<PoolHash, u64>,
        pool_total: u64,
    ) -> Self {
        Self {
            closing_epoch,
            shard,
            total_shards,
            drep_distr,
            pool_distr,
            pool_total,
            applied: false,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for GovDistrAccumulate {
    type Entity = GovState;

    fn key(&self) -> NsKey {
        GovState::ns_key()
    }

    fn apply(&mut self, entity: &mut Option<GovState>) {
        let state = entity.as_mut().expect(GOV_MUST_EXIST);

        // Idempotency + ordering guards, per accumulating epoch.
        match state.distr.as_ref() {
            Some(distr) if distr.closing_epoch == self.closing_epoch => {
                if distr.committed_shards > self.shard {
                    // Already merged (crash-recovery replay of a shard whose
                    // state commit landed). Skip to preserve idempotency.
                    tracing::debug!(
                        closing_epoch = self.closing_epoch,
                        shard = self.shard,
                        committed = distr.committed_shards,
                        "GovDistrAccumulate already applied — skipping (idempotent)"
                    );
                    return;
                }
                if distr.committed_shards < self.shard {
                    tracing::error!(
                        closing_epoch = self.closing_epoch,
                        shard = self.shard,
                        committed = distr.committed_shards,
                        "GovDistrAccumulate applied out of order — skipping to avoid corruption"
                    );
                    return;
                }
                if distr.total_shards != self.total_shards {
                    tracing::error!(
                        closing_epoch = self.closing_epoch,
                        shard = self.shard,
                        stored_total = distr.total_shards,
                        delta_total = self.total_shards,
                        "GovDistrAccumulate total_shards disagrees with in-flight \
                         accumulator — skipping to avoid corruption"
                    );
                    return;
                }
            }
            Some(distr) if distr.closing_epoch > self.closing_epoch => {
                // Replay of a boundary that a newer one already replaced.
                tracing::debug!(
                    closing_epoch = self.closing_epoch,
                    stored_epoch = distr.closing_epoch,
                    "GovDistrAccumulate for a superseded boundary — skipping (idempotent)"
                );
                return;
            }
            _ => {
                // Fresh boundary (no accumulator yet, or a previous epoch's
                // residue about to be pruned). Only shard 0 may open it.
                if self.shard != 0 {
                    tracing::error!(
                        closing_epoch = self.closing_epoch,
                        shard = self.shard,
                        "GovDistrAccumulate opening a boundary at shard != 0 — \
                         skipping to avoid corruption"
                    );
                    return;
                }
            }
        }

        self.prev = state.distr.clone();

        let distr = match state.distr.as_mut() {
            Some(distr) if distr.closing_epoch == self.closing_epoch => distr,
            _ => state
                .distr
                .insert(GovDistr::new(self.closing_epoch, self.total_shards)),
        };

        for (drep, weight) in &self.drep_distr {
            *distr.drep_distr.entry(drep.clone()).or_default() += weight;
        }

        for (pool, weight) in &self.pool_distr {
            *distr.pool_distr.entry(*pool).or_default() += weight;
        }

        distr.pool_total += self.pool_total;
        distr.committed_shards = self.shard + 1;

        self.applied = true;
    }

    fn undo(&self, entity: &mut Option<GovState>) {
        if !self.applied {
            return;
        }

        let state = entity.as_mut().expect(GOV_MUST_EXIST);

        state.distr = self.prev.clone();
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    pub fn any_committee_authorization() -> impl Strategy<Value = CommitteeAuthorization> {
        prop_oneof![
            root::any_stake_credential().prop_map(CommitteeAuthorization::HotCredential),
            prop::option::of(root::any_anchor()).prop_map(CommitteeAuthorization::Resigned),
        ]
    }

    pub fn any_auth_history() -> impl Strategy<Value = AuthHistory> {
        prop::collection::vec((root::any_slot(), any_committee_authorization()), 0..3)
    }

    prop_compose! {
        pub fn any_constitution()(
            anchor in root::any_anchor(),
            guardrail_script in prop::option::of(root::any_hash_28()),
        ) -> Constitution {
            Constitution { anchor, guardrail_script }
        }
    }

    prop_compose! {
        pub fn any_committee()(
            members in prop::collection::btree_map(
                root::any_stake_credential(),
                root::any_epoch(),
                0..5,
            ),
            threshold in root::any_rational(),
        ) -> Committee {
            Committee { members, threshold }
        }
    }

    prop_compose! {
        pub fn any_gov_roots()(
            pparam_update in prop::option::of(root::any_gov_action_id()),
            hard_fork in prop::option::of(root::any_gov_action_id()),
            committee in prop::option::of(root::any_gov_action_id()),
            constitution in prop::option::of(root::any_gov_action_id()),
        ) -> GovRoots {
            GovRoots { pparam_update, hard_fork, committee, constitution }
        }
    }

    prop_compose! {
        pub fn any_gov_distr()(
            closing_epoch in root::any_epoch(),
            committed_shards in 0u32..8u32,
            total_shards in 8u32..16u32,
            drep_distr in prop::collection::btree_map(
                root::any_drep(),
                root::any_lovelace(),
                0..4,
            ),
            pool_distr in prop::collection::btree_map(
                root::any_pool_hash(),
                root::any_lovelace(),
                0..4,
            ),
            pool_total in root::any_lovelace(),
        ) -> GovDistr {
            GovDistr {
                closing_epoch,
                committed_shards,
                total_shards,
                drep_distr,
                pool_distr,
                pool_total,
            }
        }
    }

    prop_compose! {
        pub fn any_gov_state()(
            constitution in prop::option::of(any_constitution()),
            committee in prop::option::of(any_committee()),
            committee_auths in prop::collection::btree_map(
                root::any_stake_credential(),
                any_auth_history(),
                0..3,
            ),
            prev_gov_action_ids in any_gov_roots(),
            num_dormant_epochs in 0u64..32u64,
            active_since in prop::option::of(root::any_epoch()),
            distr in prop::option::of(any_gov_distr()),
        ) -> GovState {
            GovState {
                constitution,
                committee,
                committee_auths,
                prev_gov_action_ids,
                num_dormant_epochs,
                active_since,
                distr,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_roundtrip() {
        let mut state = GovState {
            constitution: Some(Constitution {
                anchor: Anchor {
                    url: "ipfs://constitution".to_string(),
                    content_hash: [1u8; 32].into(),
                },
                guardrail_script: Some([2u8; 28].into()),
            }),
            committee: Some(Committee {
                members: BTreeMap::from([
                    (StakeCredential::ScriptHash([3u8; 28].into()), 580),
                    (StakeCredential::AddrKeyhash([4u8; 28].into()), 600),
                ]),
                threshold: RationalNumber {
                    numerator: 2,
                    denominator: 3,
                },
            }),
            committee_auths: BTreeMap::from([(
                StakeCredential::ScriptHash([3u8; 28].into()),
                vec![
                    (
                        100,
                        CommitteeAuthorization::HotCredential(StakeCredential::AddrKeyhash(
                            [5u8; 28].into(),
                        )),
                    ),
                    (200, CommitteeAuthorization::Resigned(None)),
                ],
            )]),
            prev_gov_action_ids: GovRoots {
                committee: Some(GovActionId {
                    transaction_id: [6u8; 32].into(),
                    action_index: 0,
                }),
                ..Default::default()
            },
            num_dormant_epochs: 3,
            active_since: Some(507),
            distr: Some(GovDistr {
                closing_epoch: 509,
                committed_shards: 2,
                total_shards: 32,
                drep_distr: BTreeMap::from([
                    (DRep::Key([7u8; 28].into()), 5_000_000),
                    (DRep::Abstain, 1_000_000),
                ]),
                pool_distr: BTreeMap::from([([8u8; 28].into(), 6_000_000)]),
                pool_total: 6_000_000,
            }),
        };

        let bytes = minicbor::to_vec(&state).unwrap();
        let decoded: GovState = minicbor::decode(&bytes).unwrap();
        assert_eq!(decoded, state);

        // as-of reads walk the slot-stamped history
        let cold = StakeCredential::ScriptHash([3u8; 28].into());

        assert_eq!(
            state.committee_auth(&cold),
            Some(&CommitteeAuthorization::Resigned(None))
        );
        assert_eq!(
            state.committee_auth_as_of(&cold, 150),
            Some(&CommitteeAuthorization::HotCredential(
                StakeCredential::AddrKeyhash([5u8; 28].into())
            ))
        );
        assert_eq!(state.committee_auth_as_of(&cold, 50), None);

        state.committee_auths.clear();
        assert_eq!(state.committee_auth(&cold), None);
    }

    #[test]
    fn mainnet_conway_genesis_maps() {
        let genesis = crate::load_test_genesis("mainnet");

        let (constitution, committee) = gov_from_conway_genesis(&genesis.conway).unwrap();

        // interim constitution: guardrails script present, anchor on ipfs
        assert!(constitution.anchor.url.starts_with("ipfs://"));
        assert_eq!(
            hex::encode(constitution.guardrail_script.unwrap()),
            "fa24fb305126805cf2164c161d852a0e7330cf988f1fe558cf7d4a64"
        );

        // interim committee: 7 script members, threshold 2/3
        assert_eq!(committee.members.len(), 7);
        assert!(committee
            .members
            .keys()
            .all(|cred| matches!(cred, StakeCredential::ScriptHash(_))));
        assert!(committee.members.values().all(|term| *term == 580));
        assert_eq!(
            committee.threshold,
            RationalNumber {
                numerator: 2,
                denominator: 3,
            }
        );
    }

    #[test]
    fn genesis_member_key_parsing() {
        let script = parse_genesis_committee_member(
            "scriptHash-df0e83bde65416dade5b1f97e7f115cc1ff999550ad968850783fe50",
        );
        assert!(matches!(script, Some(StakeCredential::ScriptHash(_))));

        let key = parse_genesis_committee_member(
            "keyHash-df0e83bde65416dade5b1f97e7f115cc1ff999550ad968850783fe50",
        );
        assert!(matches!(key, Some(StakeCredential::AddrKeyhash(_))));

        assert_eq!(parse_genesis_committee_member("bogus"), None);
        assert_eq!(parse_genesis_committee_member("keyHash-zz"), None);
    }
}

#[cfg(test)]
mod prop_tests {
    use super::testing::{any_committee, any_constitution, any_gov_state};
    use super::*;
    use crate::model::testing::{self as root, assert_delta_roundtrip};
    use proptest::prelude::*;

    prop_compose! {
        fn any_genesis_init()(
            constitution in any_constitution(),
            committee in any_committee(),
            epoch in root::any_epoch(),
        ) -> GovGenesisInit {
            GovGenesisInit::new(constitution, committee, epoch)
        }
    }

    prop_compose! {
        fn any_committee_auth()(
            cold in root::any_stake_credential(),
            hot in root::any_stake_credential(),
            slot in root::any_slot(),
        ) -> CommitteeAuth {
            CommitteeAuth::new(cold, hot, slot)
        }
    }

    prop_compose! {
        fn any_committee_resign()(
            cold in root::any_stake_credential(),
            anchor in prop::option::of(root::any_anchor()),
            slot in root::any_slot(),
        ) -> CommitteeResign {
            CommitteeResign::new(cold, anchor, slot)
        }
    }

    prop_compose! {
        fn any_committee_update()(
            change in prop_oneof![
                Just(CommitteeChange::NoConfidence),
                (
                    prop::collection::vec(root::any_stake_credential(), 0..3),
                    prop::collection::vec(
                        (root::any_stake_credential(), root::any_epoch()),
                        0..3,
                    ),
                    root::any_rational(),
                ).prop_map(|(to_remove, to_add, threshold)| CommitteeChange::Update {
                    to_remove,
                    to_add,
                    threshold,
                }),
            ],
        ) -> CommitteeUpdate {
            CommitteeUpdate { change, prev: None }
        }
    }

    prop_compose! {
        fn any_constitution_update()(
            constitution in any_constitution(),
        ) -> ConstitutionUpdate {
            ConstitutionUpdate::new(constitution)
        }
    }

    prop_compose! {
        fn any_gov_roots_update()(
            purpose in crate::model::proposals::testing::any_gov_purpose(),
            action in root::any_gov_action_id(),
        ) -> GovRootsUpdate {
            GovRootsUpdate::new(purpose, action)
        }
    }

    prop_compose! {
        fn any_gov_distr_accumulate()(
            closing_epoch in root::any_epoch(),
            shard in 0u32..4u32,
            total_shards in 4u32..8u32,
            drep_distr in prop::collection::btree_map(
                root::any_drep(),
                root::any_lovelace(),
                0..4,
            ),
            pool_distr in prop::collection::btree_map(
                root::any_pool_hash(),
                root::any_lovelace(),
                0..4,
            ),
            pool_total in root::any_lovelace(),
        ) -> GovDistrAccumulate {
            GovDistrAccumulate::new(
                closing_epoch,
                shard,
                total_shards,
                drep_distr,
                pool_distr,
                pool_total,
            )
        }
    }

    proptest! {
        #[test]
        fn entity_cbor_roundtrip(entity in any_gov_state()) {
            let bytes = minicbor::to_vec(&entity).unwrap();
            let decoded: GovState = minicbor::decode(&bytes).unwrap();
            prop_assert_eq!(decoded, entity);
        }

        #[test]
        fn genesis_init_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_genesis_init(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn committee_auth_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_committee_auth(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn committee_auth_serde_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_committee_auth(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn committee_resign_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_committee_resign(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn committee_resign_serde_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_committee_resign(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn dormancy_reset_roundtrip(
            entity in any_gov_state().prop_map(Some),
        ) {
            assert_delta_roundtrip(entity, GovDormancyReset::new());
        }

        #[test]
        fn distr_accumulate_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_gov_distr_accumulate(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn distr_accumulate_serde_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_gov_distr_accumulate(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn dormancy_reset_serde_roundtrip(
            entity in any_gov_state().prop_map(Some),
        ) {
            root::assert_delta_serde_roundtrip(entity, GovDormancyReset::new());
        }

        #[test]
        fn genesis_init_serde_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_genesis_init(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn committee_update_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_committee_update(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn committee_update_serde_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_committee_update(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn constitution_update_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_constitution_update(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn constitution_update_serde_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_constitution_update(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn gov_roots_update_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_gov_roots_update(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn gov_roots_update_serde_roundtrip(
            entity in any_gov_state().prop_map(Some),
            delta in any_gov_roots_update(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }
    }

    /// The member set an `UpdateCommittee` enacts is the current one minus
    /// `to_remove` plus `to_add`, under the action's threshold — and undo puts
    /// the previous committee back exactly.
    #[test]
    fn committee_update_edits_the_member_set() {
        use dolos_core::EntityDelta as _;

        let staying = StakeCredential::ScriptHash([1u8; 28].into());
        let leaving = StakeCredential::ScriptHash([2u8; 28].into());
        let joining = StakeCredential::AddrKeyhash([3u8; 28].into());

        let before = Committee {
            members: BTreeMap::from([(staying.clone(), 500), (leaving.clone(), 500)]),
            threshold: RationalNumber {
                numerator: 2,
                denominator: 3,
            },
        };

        let mut entity = Some(GovState {
            committee: Some(before.clone()),
            ..Default::default()
        });

        let mut delta = CommitteeUpdate::update(
            vec![leaving.clone()],
            vec![(joining.clone(), 620)],
            RationalNumber {
                numerator: 3,
                denominator: 5,
            },
        );

        delta.apply(&mut entity);

        let committee = entity.as_ref().unwrap().committee.as_ref().unwrap();
        assert_eq!(
            committee.members,
            BTreeMap::from([(staying, 500), (joining, 620)])
        );
        assert_eq!(
            committee.threshold,
            RationalNumber {
                numerator: 3,
                denominator: 5,
            }
        );

        delta.undo(&mut entity);
        assert_eq!(entity.unwrap().committee, Some(before));
    }

    /// `NoConfidence` dissolves the committee; a later `UpdateCommittee`
    /// rebuilds it from an empty member set.
    #[test]
    fn no_confidence_dissolves_then_rebuilds() {
        use dolos_core::EntityDelta as _;

        let sitting = StakeCredential::ScriptHash([1u8; 28].into());
        let fresh = StakeCredential::AddrKeyhash([2u8; 28].into());

        let threshold = RationalNumber {
            numerator: 2,
            denominator: 3,
        };

        let mut entity = Some(GovState {
            committee: Some(Committee {
                members: BTreeMap::from([(sitting.clone(), 500)]),
                threshold: threshold.clone(),
            }),
            ..Default::default()
        });

        let mut dissolve = CommitteeUpdate::no_confidence();
        dissolve.apply(&mut entity);
        assert_eq!(entity.as_ref().unwrap().committee, None);

        let mut rebuild =
            CommitteeUpdate::update(vec![], vec![(fresh.clone(), 700)], threshold.clone());
        rebuild.apply(&mut entity);

        assert_eq!(
            entity.as_ref().unwrap().committee.as_ref().unwrap().members,
            BTreeMap::from([(fresh, 700)])
        );

        rebuild.undo(&mut entity);
        assert_eq!(entity.as_ref().unwrap().committee, None);

        dissolve.undo(&mut entity);
        assert_eq!(
            entity.unwrap().committee.unwrap().members,
            BTreeMap::from([(sitting, 500)])
        );
    }

    /// Each purpose writes its own root slot and leaves the others alone.
    #[test]
    fn gov_roots_update_targets_one_purpose() {
        use dolos_core::EntityDelta as _;

        let action = GovActionId {
            transaction_id: [7u8; 32].into(),
            action_index: 3,
        };

        let mut entity = Some(GovState::default());

        let mut delta = GovRootsUpdate::new(GovPurpose::Constitution, action.clone());
        delta.apply(&mut entity);

        let roots = &entity.as_ref().unwrap().prev_gov_action_ids;
        assert_eq!(roots.constitution, Some(action));
        assert_eq!(roots.committee, None);
        assert_eq!(roots.hard_fork, None);
        assert_eq!(roots.pparam_update, None);

        delta.undo(&mut entity);
        assert_eq!(entity.unwrap().prev_gov_action_ids, GovRoots::default());
    }

    fn shard_delta(
        closing_epoch: Epoch,
        shard: u32,
        total: u32,
        weight: u64,
    ) -> GovDistrAccumulate {
        let drep = DRep::Key([1u8; 28].into());
        let pool: crate::PoolHash = [2u8; 28].into();

        GovDistrAccumulate::new(
            closing_epoch,
            shard,
            total,
            BTreeMap::from([(drep, weight)]),
            BTreeMap::from([(pool, weight)]),
            weight,
        )
    }

    fn apply_to(state: &mut Option<GovState>, delta: &GovDistrAccumulate) {
        use dolos_core::EntityDelta as _;

        // fresh instance so replays behave like WAL replays (no undo state)
        let mut delta = GovDistrAccumulate::new(
            delta.closing_epoch,
            delta.shard,
            delta.total_shards,
            delta.drep_distr.clone(),
            delta.pool_distr.clone(),
            delta.pool_total,
        );

        delta.apply(state);
    }

    /// Shards merge in order and the accumulator reports completeness once
    /// the last one lands.
    #[test]
    fn distr_shards_merge_to_completion() {
        let mut entity = Some(GovState::default());

        for shard in 0..3 {
            apply_to(&mut entity, &shard_delta(500, shard, 3, 10));
        }

        let distr = entity.unwrap().distr.unwrap();
        let pool: crate::PoolHash = [2u8; 28].into();
        assert!(distr.is_complete_for(500));
        assert_eq!(distr.drep_distr[&DRep::Key([1u8; 28].into())], 30);
        assert_eq!(distr.pool_distr[&pool], 30);
        assert_eq!(distr.pool_total, 30);
    }

    /// Done criterion: `GovDistrAccumulate` re-applied for an
    /// already-committed `(epoch, shard)` is a no-op, and a resumed mid-scan
    /// accumulation (with replayed shards) produces the same distributions
    /// as an uninterrupted one.
    #[test]
    fn distr_resumed_scan_equals_uninterrupted() {
        let mut uninterrupted = Some(GovState::default());
        for shard in 0..3 {
            apply_to(&mut uninterrupted, &shard_delta(500, shard, 3, 10));
        }

        // crash after shard 1 committed; the restart replays shards 0 and 1
        // before resuming at shard 2
        let mut resumed = Some(GovState::default());
        apply_to(&mut resumed, &shard_delta(500, 0, 3, 10));
        apply_to(&mut resumed, &shard_delta(500, 1, 3, 10));
        apply_to(&mut resumed, &shard_delta(500, 0, 3, 10));
        apply_to(&mut resumed, &shard_delta(500, 1, 3, 10));
        apply_to(&mut resumed, &shard_delta(500, 2, 3, 10));

        assert_eq!(resumed, uninterrupted);
    }

    /// A shard arriving ahead of its predecessor is skipped instead of
    /// corrupting the accumulator, and so is a shard opening a fresh
    /// boundary at an index other than 0.
    #[test]
    fn distr_out_of_order_shards_are_skipped() {
        let mut entity = Some(GovState::default());

        apply_to(&mut entity, &shard_delta(500, 1, 3, 10));
        assert_eq!(entity.as_ref().unwrap().distr, None);

        apply_to(&mut entity, &shard_delta(500, 0, 3, 10));
        apply_to(&mut entity, &shard_delta(500, 2, 3, 10));

        let distr = entity.unwrap().distr.unwrap();
        assert_eq!(distr.committed_shards, 1);
        assert_eq!(distr.pool_total, 10);
    }

    /// Shard 0 of the next boundary replaces the previous epoch's
    /// accumulator wholesale — the pruning step — and undo restores it.
    #[test]
    fn distr_new_boundary_prunes_previous_epoch() {
        use dolos_core::EntityDelta as _;

        let mut entity = Some(GovState::default());

        for shard in 0..2 {
            apply_to(&mut entity, &shard_delta(500, shard, 2, 10));
        }

        let previous = entity.as_ref().unwrap().distr.clone();
        assert!(previous.as_ref().unwrap().is_complete_for(500));

        let mut opener = shard_delta(501, 0, 2, 7);
        opener.apply(&mut entity);

        let distr = entity.as_ref().unwrap().distr.as_ref().unwrap();
        assert_eq!(distr.closing_epoch, 501);
        assert_eq!(distr.committed_shards, 1);
        assert_eq!(distr.pool_total, 7);

        opener.undo(&mut entity);
        assert_eq!(entity.unwrap().distr, previous);
    }

    /// A replayed shard for a boundary that a newer accumulator already
    /// replaced is a no-op — it must not resurrect the pruned epoch.
    #[test]
    fn distr_superseded_boundary_replay_is_noop() {
        let mut entity = Some(GovState::default());

        apply_to(&mut entity, &shard_delta(500, 0, 1, 10));
        apply_to(&mut entity, &shard_delta(501, 0, 1, 7));

        apply_to(&mut entity, &shard_delta(500, 0, 1, 10));

        let distr = entity.unwrap().distr.unwrap();
        assert_eq!(distr.closing_epoch, 501);
        assert_eq!(distr.pool_total, 7);
    }

    #[test]
    fn auth_history_appends_in_order() {
        use dolos_core::EntityDelta as _;

        let cold = StakeCredential::ScriptHash([1u8; 28].into());
        let hot = StakeCredential::AddrKeyhash([2u8; 28].into());

        let mut entity: Option<GovState> = Some(GovState::default());

        let mut auth = CommitteeAuth::new(cold.clone(), hot.clone(), 100);
        let mut resign = CommitteeResign::new(cold.clone(), None, 200);

        auth.apply(&mut entity);
        resign.apply(&mut entity);

        let state = entity.as_ref().unwrap();
        assert_eq!(
            state.committee_auths.get(&cold).unwrap(),
            &vec![
                (100, CommitteeAuthorization::HotCredential(hot)),
                (200, CommitteeAuthorization::Resigned(None)),
            ]
        );

        resign.undo(&mut entity);
        auth.undo(&mut entity);

        // undo restores the pristine row — never removes it
        assert_eq!(entity, Some(GovState::default()));
    }
}
