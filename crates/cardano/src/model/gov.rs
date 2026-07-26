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
        conway::{Anchor, GovActionId, RationalNumber},
        Epoch, ScriptHash, StakeCredential,
    },
};
use serde::{Deserialize, Serialize};

use super::SingletonEntity;

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
        ) -> GovState {
            GovState {
                constitution,
                committee,
                committee_auths,
                prev_gov_action_ids,
                num_dormant_epochs,
                active_since,
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
