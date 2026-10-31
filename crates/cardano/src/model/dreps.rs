use dolos_core::{keys::exact, BlockSlot, KeyBytes, KeyCodec, KeyError, NsKey, TxOrder};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    ledger::primitives::{conway::DRep, Epoch},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::pallas_extras::{DREP_KEY_PREFIX, DREP_SCRIPT_PREFIX};

/// Epoch-based DRep expiry, stored exactly as the Haskell ledger stores
/// `drepExpiry`: **without** dormant-epoch credit. The actual expiry is
/// `current + GovState::num_dormant_epochs`; the counter is folded into the
/// stored value whenever a proposal ends a dormant stretch
/// ([`DRepDormancyRelease`]).
///
/// The `(updated_in, prev)` pair makes the value snapshot-safe: epoch
/// boundary tallies read the value as of the end of the previous epoch even
/// if activity during the closing epoch refreshed it. `prev` holds the value
/// that was in effect before the first write of epoch `updated_in`, which is
/// sufficient because reads never look further back than one epoch.
#[derive(Debug, Encode, Decode, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct DRepExpiry {
    /// Expiry epoch (inclusive), without dormancy credit.
    #[n(0)]
    pub current: Epoch,

    /// The epoch during which `current` was last written.
    #[n(1)]
    pub updated_in: Epoch,

    /// The value in effect at the end of every epoch before `updated_in`.
    #[n(2)]
    pub prev: Option<Epoch>,
}

impl DRepExpiry {
    pub fn new(value: Epoch, epoch: Epoch) -> Self {
        Self {
            current: value,
            updated_in: epoch,
            prev: None,
        }
    }

    /// Write `value` during `epoch`, rotating the previous value out when
    /// this is the first write of the epoch.
    pub fn set(&mut self, value: Epoch, epoch: Epoch) {
        if self.updated_in != epoch {
            self.prev = Some(self.current);
            self.updated_in = epoch;
        }

        self.current = value;
    }

    /// The value as of the end of `epoch`. `None` means the value did not
    /// exist yet (reads older than the retained window also land here, but
    /// boundary tallies only ever look one epoch back).
    pub fn as_of(&self, epoch: Epoch) -> Option<Epoch> {
        if self.updated_in <= epoch {
            Some(self.current)
        } else {
            self.prev
        }
    }
}

/// A registered DRep. Only a registration creates a row. ESTART moves an
/// unregistered row no ledger rule reads again to the final-row log
/// ([`DRepState::leaves_state_at`]), and a later registration makes a fresh
/// row. Indexes 1 and 7 held fields that left the record; they must not be
/// reused.
#[derive(Debug, Encode, Decode, Clone, PartialEq, Eq, Serialize)]
pub struct DRepState {
    #[n(0)]
    pub registered_at: Option<(BlockSlot, TxOrder)>,

    #[n(2)]
    pub last_active_slot: Option<u64>,

    #[n(3)]
    pub unregistered_at: Option<(BlockSlot, TxOrder)>,

    #[n(4)]
    pub expired: bool,

    #[n(5)]
    pub deposit: u64,

    #[n(6)]
    pub identifier: DRep,

    #[n(8)]
    pub expiry: DRepExpiry,
}

impl DRepState {
    pub fn new(identifier: DRep, expiry: DRepExpiry) -> Self {
        Self {
            registered_at: None,
            last_active_slot: None,
            unregistered_at: None,
            expired: false,
            deposit: 0,
            identifier,
            expiry,
        }
    }

    pub fn is_unregistered(&self) -> bool {
        match (self.registered_at, self.unregistered_at) {
            (Some(registered_at), Some(unregistered_at)) => registered_at < unregistered_at,
            (_, None) => false,
            (None, Some(unregistered_at)) => {
                warn!(
                    drep = ?self.identifier,
                    unregistered_at = ?unregistered_at,
                    "unexpected drep unregistration without registration"
                );
                false
            }
        }
    }

    /// Whether the boundary closing the epoch that starts at `closing_start`
    /// moves the DRep to the final-row log (`crate::final_rows`): it
    /// unregistered, its deposit refunded, before that epoch and before
    /// `oldest_live`, the earliest submission among the proposals the next
    /// boundary can still ratify (`None` when there are none).
    ///
    /// - **Live proposals.** The stale-vote filter is the one ledger rule that
    ///   reads an unregistered DRep: ratify drops the votes a DRep cast before
    ///   its newest deregistration, which a fresh row no longer holds. A vote
    ///   cast before the deregistration is on a proposal submitted before it,
    ///   which the rule requires to be resolved.
    /// - **The closing epoch.** A rollback can reach that epoch's last blocks,
    ///   and undoing an unregistration needs the row.
    /// - **One transaction.** A DRep registered and unregistered in the same
    ///   transaction carries equal stamps, as one unregistered and registered
    ///   again there does, so [`Self::is_unregistered`] is false and the row
    ///   stays.
    pub fn leaves_state_at(
        &self,
        closing_start: BlockSlot,
        oldest_live: Option<BlockSlot>,
    ) -> bool {
        let Some((unregistered, _)) = self.unregistered_at else {
            return false;
        };

        self.is_unregistered()
            && self.deposit == 0
            && unregistered < closing_start
            && oldest_live.is_none_or(|submitted| unregistered < submitted)
    }
}

/// A DRep: the CIP-129 credential header (key or script) and its hash, 29 B,
/// or a single byte for the predefined abstain (`0`) and no-confidence (`1`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DRepKey(pub DRep);

/// Hashes the stored bytes, which tell DReps apart exactly as `Eq` does.
impl std::hash::Hash for DRepKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        dolos_core::keys::encode(self).hash(state);
    }
}

/// Orders by the stored bytes, the order the store keeps DReps in.
impl Ord for DRepKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        dolos_core::keys::encode(self).cmp(&dolos_core::keys::encode(other))
    }
}

impl PartialOrd for DRepKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<DRep> for DRepKey {
    fn from(value: DRep) -> Self {
        Self(value)
    }
}

impl From<&DRep> for DRepKey {
    fn from(value: &DRep) -> Self {
        Self(value.clone())
    }
}

impl From<DRepKey> for DRep {
    fn from(value: DRepKey) -> Self {
        value.0
    }
}

impl KeyCodec for DRepKey {
    fn encode_key(&self, out: &mut KeyBytes) {
        match &self.0 {
            DRep::Key(hash) => {
                out.push(DREP_KEY_PREFIX);
                out.extend_from_slice(hash.as_slice());
            }
            DRep::Script(hash) => {
                out.push(DREP_SCRIPT_PREFIX);
                out.extend_from_slice(hash.as_slice());
            }
            DRep::Abstain => out.push(0),
            DRep::NoConfidence => out.push(1),
        }
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        let Some((header, hash)) = bytes.split_first() else {
            return Err(KeyError::Short {
                expected: 1,
                found: 0,
            });
        };

        let drep = match *header {
            0 => exact::<0>(hash).map(|_| DRep::Abstain)?,
            1 => exact::<0>(hash).map(|_| DRep::NoConfidence)?,
            DREP_KEY_PREFIX => DRep::Key(exact::<28>(hash)?.into()),
            DREP_SCRIPT_PREFIX => DRep::Script(exact::<28>(hash)?.into()),
            other => {
                return Err(KeyError::Malformed(format!("drep header {other:#04x}")));
            }
        };

        Ok(Self(drep))
    }
}

display_as_stored!(DRepKey);

entity_boilerplate!(DRepState, "dreps", state = DRepKey, log = DRepKey);

/// A moved DRep's final row is its whole state row: the DRep routes read
/// every field of it.
impl crate::final_rows::FinalRow for DRepState {
    fn final_row(&self) -> Self {
        self.clone()
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_drep_expiry()(
            current in root::any_epoch(),
            updated_in in root::any_epoch(),
            prev in prop::option::of(root::any_epoch()),
        ) -> DRepExpiry {
            DRepExpiry { current, updated_in, prev }
        }
    }

    prop_compose! {
        pub fn any_drep_state()(
            identifier in root::any_drep(),
            registered_at in prop::option::of((root::any_slot(), root::any_tx_order())),
            last_active_slot in prop::option::of(root::any_slot()),
            unregistered_at in prop::option::of((root::any_slot(), root::any_tx_order())),
            expired in any::<bool>(),
            deposit in root::any_lovelace(),
            expiry in any_drep_expiry(),
        ) -> DRepState {
            DRepState {
                identifier,
                registered_at,
                last_active_slot,
                unregistered_at,
                expired,
                deposit,
                expiry,
            }
        }
    }
}

// --- Deltas ---

/// Registers a DRep, creating its row on the first registration with the
/// expiry the registration stamps. A re-registration leaves the expiry to the
/// `DRepExpiryUpdate` queued after it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepRegistration {
    pub(crate) drep: DRep,
    pub(crate) slot: BlockSlot,
    pub(crate) txorder: TxOrder,
    pub(crate) deposit: u64,
    pub(crate) expiry: DRepExpiry,

    // undo
    pub(crate) was_new: bool,
    pub(crate) prev_registered_at: Option<(BlockSlot, TxOrder)>,
    pub(crate) prev_deposit: u64,
}

impl DRepRegistration {
    pub fn new(
        drep: DRep,
        slot: BlockSlot,
        txorder: TxOrder,
        deposit: u64,
        expiry: DRepExpiry,
    ) -> Self {
        Self {
            drep,
            slot,
            txorder,
            deposit,
            expiry,
            was_new: false,
            prev_registered_at: None,
            prev_deposit: 0,
        }
    }
}

impl dolos_core::EntityDelta for DRepRegistration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::of::<DRepState>(&DRepKey::from(&self.drep))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        self.was_new = entity.is_none();

        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep.clone(), self.expiry));

        // save undo info
        self.prev_registered_at = entity.registered_at;
        self.prev_deposit = entity.deposit;

        // apply changes
        entity.registered_at = Some((self.slot, self.txorder));
        entity.deposit = self.deposit;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        if self.was_new {
            *entity = None;
            return;
        }
        let entity = entity.as_mut().expect("existing drep");
        entity.registered_at = self.prev_registered_at;
        entity.deposit = self.prev_deposit;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepUnRegistration {
    pub(crate) drep: DRep,
    pub(crate) slot: BlockSlot,
    pub(crate) txorder: TxOrder,

    // undo data
    pub(crate) prev_deposit: Option<u64>,
    pub(crate) prev_unregistered_at: Option<(BlockSlot, TxOrder)>,
}

impl DRepUnRegistration {
    pub fn new(drep: DRep, slot: BlockSlot, txorder: TxOrder) -> Self {
        Self {
            drep,
            slot,
            txorder,
            prev_deposit: None,
            prev_unregistered_at: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepUnRegistration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::of::<DRepState>(&DRepKey::from(&self.drep))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.as_mut().expect("can't unregister missing drep");

        // save undo data
        self.prev_unregistered_at = entity.unregistered_at;
        self.prev_deposit = Some(entity.deposit);

        // apply changes
        entity.unregistered_at = Some((self.slot, self.txorder));
        entity.deposit = 0;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let state = entity
            .as_mut()
            .expect("can't undo unregister on missing drep");
        state.unregistered_at = self.prev_unregistered_at;
        state.deposit = self.prev_deposit.unwrap_or(0);
    }
}

/// Stamps a DRep's newest certificate or vote. A row it does not find is left
/// absent: only a registration creates one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepActivity {
    pub(crate) drep: DRep,
    pub(crate) slot: u64,
    pub(crate) previous_last_active_slot: Option<u64>,
}

impl DRepActivity {
    pub fn new(drep: DRep, slot: u64) -> Self {
        Self {
            drep,
            slot,
            previous_last_active_slot: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepActivity {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::of::<DRepState>(&DRepKey::from(&self.drep))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let Some(entity) = entity else {
            return;
        };

        // save undo info
        self.previous_last_active_slot = entity.last_active_slot;

        // apply changes
        entity.last_active_slot = Some(self.slot);
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        if let Some(state) = entity {
            state.last_active_slot = self.previous_last_active_slot;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepExpiration {
    pub(crate) drep_id: DRepKey,
    pub(crate) prev_expired: bool,
}

impl DRepExpiration {
    pub fn new(drep_id: DRepKey) -> Self {
        Self {
            drep_id,
            prev_expired: false,
        }
    }
}

impl dolos_core::EntityDelta for DRepExpiration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::of::<DRepState>(&self.drep_id)
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing drep");

        debug!(drep=%self.drep_id, "expiring drep");

        self.prev_expired = entity.expired;
        entity.expired = true;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        if let Some(state) = entity {
            state.expired = self.prev_expired;
        }
    }
}

/// Refresh a DRep's stored expiry epoch.
///
/// Emitted for `RegDRepCert` and `UpdateDRepCert` (`only_if_registered =
/// false`) and for every DRep vote (`only_if_registered = true`, mirroring
/// the Haskell `Map.adjust`, which only touches registered DReps). The
/// visitor computes the value — `current_epoch + drepActivity −
/// numDormantEpochs`, with the PV9 bootstrap exception on registration — so
/// the delta carries concrete data and replays exactly. A row it does not
/// find is left absent: only a registration creates one.
///
/// Also clears the `expired` flag: expiry in the ledger is implicit, so any
/// refresh makes the DRep active again.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepExpiryUpdate {
    pub(crate) drep: DRep,
    pub(crate) expiry: Epoch,
    pub(crate) epoch: Epoch,
    pub(crate) only_if_registered: bool,

    // undo
    pub(crate) applied: bool,
    pub(crate) prev_expiry: Option<DRepExpiry>,
    pub(crate) prev_expired: bool,
}

impl DRepExpiryUpdate {
    pub fn new(drep: DRep, expiry: Epoch, epoch: Epoch, only_if_registered: bool) -> Self {
        Self {
            drep,
            expiry,
            epoch,
            only_if_registered,
            applied: false,
            prev_expiry: None,
            prev_expired: false,
        }
    }
}

impl dolos_core::EntityDelta for DRepExpiryUpdate {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::of::<DRepState>(&DRepKey::from(&self.drep))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        self.applied = false;

        let Some(entity) = entity.as_mut() else {
            return;
        };

        if self.only_if_registered && (entity.registered_at.is_none() || entity.is_unregistered()) {
            return;
        }

        // save undo info
        self.prev_expiry = Some(entity.expiry);
        self.prev_expired = entity.expired;

        // apply changes
        entity.expiry.set(self.expiry, self.epoch);
        entity.expired = false;
        self.applied = true;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        if !self.applied {
            return;
        }

        if let (Some(state), Some(expiry)) = (entity, self.prev_expiry) {
            state.expiry = expiry;
            state.expired = self.prev_expired;
        }
    }
}

/// Fold the dormant-epoch counter into one DRep's stored expiry
/// (research §3.3.1). Emitted as a per-DRep fan-out — together with a
/// single [`crate::GovDormancyReset`] — when a transaction carries the
/// first proposal after a dormant stretch.
///
/// Long-expired DReps (`current + dormant < epoch`) are not resurrected:
/// their stored value stays untouched, exactly as in the Haskell
/// `updateDormantDRepExpiry`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepDormancyRelease {
    pub(crate) drep_id: DRepKey,
    pub(crate) dormant_epochs: u64,
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) applied: bool,
    pub(crate) prev_expiry: Option<DRepExpiry>,
}

impl DRepDormancyRelease {
    pub fn new(drep_id: DRepKey, dormant_epochs: u64, epoch: Epoch) -> Self {
        Self {
            drep_id,
            dormant_epochs,
            epoch,
            applied: false,
            prev_expiry: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepDormancyRelease {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::of::<DRepState>(&self.drep_id)
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        self.applied = false;

        let Some(state) = entity.as_mut() else {
            return;
        };

        if state.is_unregistered() {
            return;
        }

        let expiry = &mut state.expiry;

        let actual = expiry.current + self.dormant_epochs;

        if actual < self.epoch {
            // long-expired: don't resurrect
            return;
        }

        self.prev_expiry = Some(*expiry);
        expiry.set(actual, self.epoch);
        self.applied = true;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        if !self.applied {
            return;
        }

        if let (Some(state), Some(expiry)) = (entity, self.prev_expiry) {
            state.expiry = expiry;
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use super::testing::{self, any_drep_state};
    use super::*;
    use crate::model::testing::{self as root, assert_delta_roundtrip};
    use proptest::prelude::*;

    prop_compose! {
        fn any_drep_registration()(
            drep in root::any_drep(),
            slot in root::any_slot(),
            txorder in root::any_tx_order(),
            deposit in root::any_lovelace(),
            expiry in testing::any_drep_expiry(),
        ) -> DRepRegistration {
            DRepRegistration::new(drep, slot, txorder, deposit, expiry)
        }
    }

    prop_compose! {
        fn any_drep_unregistration()(
            drep in root::any_drep(),
            slot in root::any_slot(),
            txorder in root::any_tx_order(),
        ) -> DRepUnRegistration {
            DRepUnRegistration::new(drep, slot, txorder)
        }
    }

    prop_compose! {
        fn any_drep_activity()(
            drep in root::any_drep(),
            slot in root::any_slot(),
        ) -> DRepActivity {
            DRepActivity::new(drep, slot)
        }
    }

    prop_compose! {
        fn any_drep_expiration()(
            drep in root::any_drep(),
        ) -> DRepExpiration {
            DRepExpiration::new(DRepKey(drep))
        }
    }

    prop_compose! {
        fn any_drep_expiry_update()(
            drep in root::any_drep(),
            expiry in root::any_epoch(),
            epoch in root::any_epoch(),
            only_if_registered in any::<bool>(),
        ) -> DRepExpiryUpdate {
            DRepExpiryUpdate::new(drep, expiry, epoch, only_if_registered)
        }
    }

    prop_compose! {
        fn any_drep_dormancy_release()(
            drep in root::any_drep(),
            dormant_epochs in 1u64..32u64,
            epoch in root::any_epoch(),
        ) -> DRepDormancyRelease {
            DRepDormancyRelease::new(DRepKey(drep), dormant_epochs, epoch)
        }
    }

    proptest! {
        #[test]
        fn drep_registration_roundtrip(
            entity in prop::option::of(any_drep_state()),
            delta in any_drep_registration(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn drep_unregistration_roundtrip(
            entity in any_drep_state(),
            delta in any_drep_unregistration(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn drep_activity_roundtrip(
            entity in prop::option::of(any_drep_state()),
            delta in any_drep_activity(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn drep_expiration_roundtrip(
            entity in any_drep_state(),
            delta in any_drep_expiration(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn drep_expiry_update_roundtrip(
            entity in prop::option::of(any_drep_state()),
            delta in any_drep_expiry_update(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn drep_expiry_update_serde_roundtrip(
            entity in prop::option::of(any_drep_state()),
            delta in any_drep_expiry_update(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn drep_dormancy_release_roundtrip(
            entity in prop::option::of(any_drep_state()),
            delta in any_drep_dormancy_release(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn drep_dormancy_release_serde_roundtrip(
            entity in prop::option::of(any_drep_state()),
            delta in any_drep_dormancy_release(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }
    }
}

#[cfg(test)]
mod expiry_tests {
    use super::*;
    use dolos_core::EntityDelta as _;

    #[test]
    fn expiry_set_rotates_once_per_epoch() {
        let mut expiry = DRepExpiry::new(120, 100);

        // second write in the same epoch keeps the pre-epoch value out of prev
        expiry.set(121, 100);
        assert_eq!(expiry.as_of(100), Some(121));
        assert_eq!(expiry.as_of(99), None);

        // first write of a later epoch rotates
        expiry.set(125, 105);
        assert_eq!(expiry.as_of(105), Some(125));
        assert_eq!(expiry.as_of(104), Some(121));

        // another write in the same epoch overwrites without rotating
        expiry.set(126, 105);
        assert_eq!(expiry.as_of(105), Some(126));
        assert_eq!(expiry.as_of(104), Some(121));
    }

    #[test]
    fn vote_refresh_skips_unregistered_dreps() {
        let drep = DRep::Key([1u8; 28].into());

        // absent entity: a vote refresh must not create one
        let mut entity: Option<DRepState> = None;
        let mut delta = DRepExpiryUpdate::new(drep.clone(), 130, 110, true);
        delta.apply(&mut entity);
        assert!(entity.is_none());
        delta.undo(&mut entity);
        assert!(entity.is_none());

        // unregistered entity: same
        let mut state = DRepState::new(drep.clone(), DRepExpiry::new(120, 100));
        state.registered_at = Some((100, 0));
        state.unregistered_at = Some((200, 0));
        let mut entity = Some(state.clone());
        let mut delta = DRepExpiryUpdate::new(drep.clone(), 130, 110, true);
        delta.apply(&mut entity);
        assert_eq!(entity.as_ref().unwrap().expiry, DRepExpiry::new(120, 100));

        // cert-driven refresh applies regardless
        let mut delta = DRepExpiryUpdate::new(drep.clone(), 130, 110, false);
        delta.apply(&mut entity);
        assert_eq!(entity.as_ref().unwrap().expiry.as_of(110), Some(130));
        assert_eq!(entity.as_ref().unwrap().expiry.as_of(109), Some(120));
        delta.undo(&mut entity);
        assert_eq!(entity.unwrap().expiry, DRepExpiry::new(120, 100));

        // absent entity: a cert-driven refresh does not create one either
        let mut entity: Option<DRepState> = None;
        let mut delta = DRepExpiryUpdate::new(drep.clone(), 130, 110, false);
        delta.apply(&mut entity);
        assert!(entity.is_none());
    }

    #[test]
    fn only_a_registration_creates_a_row() {
        let drep = DRep::Key([1u8; 28].into());

        let mut entity: Option<DRepState> = None;
        let mut activity = DRepActivity::new(drep.clone(), 500);
        activity.apply(&mut entity);
        assert!(entity.is_none());

        let mut registration =
            DRepRegistration::new(drep.clone(), 500, 1, 1_000, DRepExpiry::new(130, 110));
        registration.apply(&mut entity);
        let row = entity.as_ref().unwrap();
        assert_eq!(row.registered_at, Some((500, 1)));
        assert_eq!(row.expiry, DRepExpiry::new(130, 110));

        registration.undo(&mut entity);
        assert!(entity.is_none());
    }

    #[test]
    fn expiry_refresh_clears_expired_flag() {
        let drep = DRep::Key([1u8; 28].into());

        let mut state = DRepState::new(drep.clone(), DRepExpiry::new(105, 85));
        state.registered_at = Some((100, 0));
        state.expired = true;

        let mut entity = Some(state);

        let mut delta = DRepExpiryUpdate::new(drep, 130, 110, true);
        delta.apply(&mut entity);

        let applied = entity.as_ref().unwrap();
        assert!(!applied.expired);
        assert_eq!(applied.expiry.current, 130);
        assert_eq!(applied.expiry.as_of(109), Some(105));

        delta.undo(&mut entity);
        let restored = entity.unwrap();
        assert!(restored.expired);
        assert_eq!(restored.expiry, DRepExpiry::new(105, 85));
    }

    #[test]
    fn dormancy_release_folds_but_never_resurrects() {
        let drep = DRep::Key([1u8; 28].into());
        let key = DRepKey(drep.clone());

        // active drep: counter folds into the stored value
        let mut state = DRepState::new(drep.clone(), DRepExpiry::new(118, 98));
        state.registered_at = Some((100, 0));
        let mut entity = Some(state);

        let mut delta = DRepDormancyRelease::new(key.clone(), 5, 120);
        delta.apply(&mut entity);
        assert_eq!(entity.as_ref().unwrap().expiry.current, 123);
        assert_eq!(entity.as_ref().unwrap().expiry.as_of(119), Some(118));

        delta.undo(&mut entity);
        assert_eq!(entity.as_ref().unwrap().expiry, DRepExpiry::new(118, 98));

        // long-expired drep: untouched
        let mut state = DRepState::new(drep, DRepExpiry::new(110, 98));
        state.registered_at = Some((100, 0));
        let mut entity = Some(state.clone());

        let mut delta = DRepDormancyRelease::new(key, 5, 120);
        delta.apply(&mut entity);
        assert_eq!(entity.as_ref().unwrap().expiry, state.expiry);
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;

    #[test]
    fn rows_roundtrip() {
        let mut state = DRepState::new(
            DRep::Script([3u8; 28].into()),
            DRepExpiry {
                current: 520,
                updated_in: 500,
                prev: Some(510),
            },
        );
        state.registered_at = Some((100, 1));
        state.last_active_slot = Some(200);

        let bytes = minicbor::to_vec(&state).unwrap();
        let decoded: DRepState = minicbor::decode(&bytes).unwrap();
        assert_eq!(decoded, state);
    }
}

#[cfg(test)]
mod key_tests {
    use dolos_core::keys::encode;
    use pallas::crypto::hash::Hash;
    use proptest::prelude::*;

    use super::*;
    use crate::model::testing as root;

    #[test]
    fn layouts_are_pinned() {
        assert_eq!(
            hex::encode(encode(&DRepKey(DRep::Key(Hash::new([0x14; 28]))))),
            format!("22{}", "14".repeat(28))
        );
        assert_eq!(
            hex::encode(encode(&DRepKey(DRep::Script(Hash::new([0x15; 28]))))),
            format!("23{}", "15".repeat(28))
        );
        assert_eq!(encode(&DRepKey(DRep::Abstain)).to_vec(), [0]);
        assert_eq!(encode(&DRepKey(DRep::NoConfidence)).to_vec(), [1]);
    }

    #[test]
    fn malformed_keys_are_refused() {
        assert!(DRepKey::decode_key(&[]).is_err());
        assert!(DRepKey::decode_key(&[0, 0]).is_err());
        assert!(DRepKey::decode_key(&[DREP_KEY_PREFIX; 28]).is_err());
        assert!(DRepKey::decode_key(&[DREP_KEY_PREFIX; 30]).is_err());
        assert!(DRepKey::decode_key(&[0x42; 29]).is_err());
    }

    proptest! {
        #[test]
        fn keys_round_trip(key in root::any_drep().prop_map(DRepKey)) {
            prop_assert_eq!(DRepKey::decode_key(&encode(&key)), Ok(key));
        }
    }
}
