use dolos_core::{BlockSlot, EntityKey, NsKey, TxOrder};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    ledger::primitives::{
        conway::{Anchor, DRep},
        Epoch,
    },
};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::FixedNamespace as _;
use crate::pallas_extras;

pub fn drep_to_entity_key(value: &DRep) -> EntityKey {
    let bytes = match value {
        DRep::Key(key) => [vec![pallas_extras::DREP_KEY_PREFIX], key.to_vec()].concat(),
        DRep::Script(key) => [vec![pallas_extras::DREP_SCRIPT_PREFIX], key.to_vec()].concat(),
        // Invented keys for convenience
        DRep::Abstain => vec![0],
        DRep::NoConfidence => vec![1],
    };

    EntityKey::from(bytes)
}

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

#[derive(Debug, Encode, Decode, Clone, PartialEq, Eq)]
pub struct DRepState {
    #[n(0)]
    pub registered_at: Option<(BlockSlot, TxOrder)>,

    #[n(1)]
    pub voting_power: u64,

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

    // Backward-compatible addition: absent in pre-existing rows, decodes as
    // `None`. Index 7 must not be reused for anything else.
    #[n(7)]
    pub anchor: Option<Anchor>,

    // Backward-compatible addition: absent in pre-existing rows, decodes as
    // `None` (those rows keep the legacy slot-arithmetic expiry heuristic
    // until activity repopulates the field). Index 8 must not be reused for
    // anything else.
    #[n(8)]
    pub expiry: Option<DRepExpiry>,

    // Backward-compatible addition: absent in pre-existing rows, decodes as
    // `None`. First on-chain reference by any certificate, vote delegations
    // included; mirrors db-sync's `drep_hash` insertion order. Index 9 must
    // not be reused for anything else.
    #[n(9)]
    pub first_seen_at: Option<(BlockSlot, TxOrder)>,
}

impl DRepState {
    pub fn new(identifier: DRep) -> Self {
        Self {
            registered_at: None,
            voting_power: 0,
            last_active_slot: None,
            unregistered_at: None,
            expired: false,
            deposit: 0,
            identifier,
            anchor: None,
            expiry: None,
            first_seen_at: None,
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
}

entity_boilerplate!(DRepState, "dreps");

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
            voting_power in root::any_lovelace(),
            last_active_slot in prop::option::of(root::any_slot()),
            unregistered_at in prop::option::of((root::any_slot(), root::any_tx_order())),
            expired in any::<bool>(),
            deposit in root::any_lovelace(),
            anchor in prop::option::of(root::any_anchor()),
            expiry in prop::option::of(any_drep_expiry()),
            first_seen_at in prop::option::of((root::any_slot(), root::any_tx_order())),
        ) -> DRepState {
            DRepState {
                identifier,
                registered_at,
                voting_power,
                last_active_slot,
                unregistered_at,
                expired,
                deposit,
                anchor,
                expiry,
                first_seen_at,
            }
        }
    }
}

// --- Deltas ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepRegistration {
    pub(crate) drep: DRep,
    pub(crate) slot: BlockSlot,
    pub(crate) txorder: TxOrder,
    pub(crate) deposit: u64,
    pub(crate) anchor: Option<Anchor>,

    // undo
    pub(crate) was_new: bool,
    pub(crate) prev_registered_at: Option<(BlockSlot, TxOrder)>,
    pub(crate) prev_voting_power: u64,
    pub(crate) prev_deposit: u64,
}

impl DRepRegistration {
    pub fn new(
        drep: DRep,
        slot: BlockSlot,
        txorder: TxOrder,
        deposit: u64,
        anchor: Option<Anchor>,
    ) -> Self {
        Self {
            drep,
            slot,
            txorder,
            deposit,
            anchor,
            was_new: false,
            prev_registered_at: None,
            prev_voting_power: 0,
            prev_deposit: 0,
        }
    }
}

impl dolos_core::EntityDelta for DRepRegistration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, drep_to_entity_key(&self.drep)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        self.was_new = entity.is_none();

        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep.clone()));

        // save undo info
        self.prev_registered_at = entity.registered_at;
        self.prev_voting_power = entity.voting_power;
        self.prev_deposit = entity.deposit;

        // apply changes
        entity.registered_at = Some((self.slot, self.txorder));
        entity.voting_power = self.deposit;
        entity.deposit = self.deposit;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        if self.was_new {
            *entity = None;
            return;
        }
        let entity = entity.as_mut().expect("existing drep");
        entity.registered_at = self.prev_registered_at;
        entity.voting_power = self.prev_voting_power;
        entity.deposit = self.prev_deposit;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepUnRegistration {
    pub(crate) drep: DRep,
    pub(crate) slot: BlockSlot,
    pub(crate) txorder: TxOrder,

    // undo data
    pub(crate) prev_voting_power: Option<u64>,
    pub(crate) prev_deposit: Option<u64>,
    pub(crate) prev_unregistered_at: Option<(BlockSlot, TxOrder)>,
}

impl DRepUnRegistration {
    pub fn new(drep: DRep, slot: BlockSlot, txorder: TxOrder) -> Self {
        Self {
            drep,
            slot,
            txorder,
            prev_voting_power: None,
            prev_deposit: None,
            prev_unregistered_at: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepUnRegistration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, drep_to_entity_key(&self.drep)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.as_mut().expect("can't unregister missing drep");

        // save undo data
        self.prev_voting_power = Some(entity.voting_power);
        self.prev_unregistered_at = entity.unregistered_at;
        self.prev_deposit = Some(entity.deposit);

        // apply changes
        entity.voting_power = 0;
        entity.unregistered_at = Some((self.slot, self.txorder));
        entity.deposit = 0;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let state = entity
            .as_mut()
            .expect("can't undo unregister on missing drep");
        state.voting_power = self.prev_voting_power.unwrap_or(0);
        state.unregistered_at = self.prev_unregistered_at;
        state.deposit = self.prev_deposit.unwrap_or(0);
    }
}

/// Records the first on-chain appearance of a DRep, creating the entity if it
/// doesn't exist yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepSeen {
    pub(crate) drep: DRep,
    pub(crate) slot: BlockSlot,
    pub(crate) txorder: TxOrder,

    // undo
    pub(crate) prev_first_seen_at: Option<(BlockSlot, TxOrder)>,
    pub(crate) was_new: bool,
}

impl DRepSeen {
    pub fn new(drep: DRep, slot: BlockSlot, txorder: TxOrder) -> Self {
        Self {
            drep,
            slot,
            txorder,
            prev_first_seen_at: None,
            was_new: false,
        }
    }
}

impl dolos_core::EntityDelta for DRepSeen {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, drep_to_entity_key(&self.drep)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        self.was_new = entity.is_none();

        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep.clone()));

        // save undo info
        self.prev_first_seen_at = entity.first_seen_at;

        // only the earliest sighting counts
        if entity.first_seen_at.is_none() {
            entity.first_seen_at = Some((self.slot, self.txorder));
        }
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        if self.was_new {
            *entity = None;
        } else if let Some(state) = entity {
            state.first_seen_at = self.prev_first_seen_at;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepActivity {
    pub(crate) drep: DRep,
    pub(crate) slot: u64,
    pub(crate) previous_last_active_slot: Option<u64>,
    pub(crate) was_new: bool,
}

impl DRepActivity {
    pub fn new(drep: DRep, slot: u64) -> Self {
        Self {
            drep,
            slot,
            previous_last_active_slot: None,
            was_new: false,
        }
    }
}

impl dolos_core::EntityDelta for DRepActivity {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, drep_to_entity_key(&self.drep)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        self.was_new = entity.is_none();

        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep.clone()));

        // save undo info
        self.previous_last_active_slot = entity.last_active_slot;

        // apply changes
        entity.last_active_slot = Some(self.slot);
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        if self.was_new {
            *entity = None;
        } else if let Some(state) = entity {
            state.last_active_slot = self.previous_last_active_slot;
        }
    }
}

/// Sets the anchor advertised by a DRep.
///
/// Emitted for `RegDRepCert` (alongside `DRepRegistration`, whose on-disk
/// shape is frozen and can't grow the undo field) and for `UpdateDRepCert`,
/// whose anchor was previously discarded. Carries the cert's anchor verbatim:
/// a cert without an anchor clears the stored one, matching the ledger rules
/// where both certs replace `drepAnchor` wholesale.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepAnchorUpdate {
    pub(crate) drep: DRep,
    pub(crate) anchor: Option<Anchor>,

    // undo
    pub(crate) was_new: bool,
    pub(crate) prev_anchor: Option<Anchor>,
}

impl DRepAnchorUpdate {
    pub fn new(drep: DRep, anchor: Option<Anchor>) -> Self {
        Self {
            drep,
            anchor,
            was_new: false,
            prev_anchor: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepAnchorUpdate {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, drep_to_entity_key(&self.drep)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        self.was_new = entity.is_none();

        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep.clone()));

        // save undo info
        self.prev_anchor = entity.anchor.clone();

        // apply changes
        entity.anchor = self.anchor.clone();
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        if self.was_new {
            *entity = None;
        } else if let Some(state) = entity {
            state.anchor = self.prev_anchor.clone();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepExpiration {
    pub(crate) drep_id: EntityKey,
    pub(crate) prev_expired: bool,
}

impl DRepExpiration {
    pub fn new(drep_id: EntityKey) -> Self {
        Self {
            drep_id,
            prev_expired: false,
        }
    }
}

impl dolos_core::EntityDelta for DRepExpiration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, self.drep_id.clone()))
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
/// the delta carries concrete data and replays exactly.
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
    pub(crate) was_new: bool,
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
            was_new: false,
            prev_expiry: None,
            prev_expired: false,
        }
    }
}

impl dolos_core::EntityDelta for DRepExpiryUpdate {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, drep_to_entity_key(&self.drep)))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        if self.only_if_registered {
            let registered = entity
                .as_ref()
                .is_some_and(|state| state.registered_at.is_some() && !state.is_unregistered());

            if !registered {
                self.applied = false;
                self.was_new = false;
                return;
            }
        }

        self.was_new = entity.is_none();

        // Cert-driven updates (`only_if_registered = false`) are always
        // queued after the same tx's `DRepRegistration`, which creates the
        // entity; creation here is defensive only — a row born this way
        // would carry no `registered_at`.
        let entity = entity.get_or_insert_with(|| DRepState::new(self.drep.clone()));

        // save undo info
        self.prev_expiry = entity.expiry;
        self.prev_expired = entity.expired;

        // apply changes
        match entity.expiry.as_mut() {
            Some(expiry) => expiry.set(self.expiry, self.epoch),
            None => entity.expiry = Some(DRepExpiry::new(self.expiry, self.epoch)),
        }

        entity.expired = false;
        self.applied = true;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        if !self.applied {
            return;
        }

        if self.was_new {
            *entity = None;
        } else if let Some(state) = entity {
            state.expiry = self.prev_expiry;
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
/// `updateDormantDRepExpiry`. Rows without the epoch-based expiry field
/// (pre-upgrade) are skipped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepDormancyRelease {
    pub(crate) drep_id: EntityKey,
    pub(crate) dormant_epochs: u64,
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) applied: bool,
    pub(crate) prev_expiry: Option<DRepExpiry>,
}

impl DRepDormancyRelease {
    pub fn new(drep_id: EntityKey, dormant_epochs: u64, epoch: Epoch) -> Self {
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
        NsKey::from((DRepState::NS, self.drep_id.clone()))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        self.applied = false;

        let Some(state) = entity.as_mut() else {
            return;
        };

        if state.is_unregistered() {
            return;
        }

        let Some(expiry) = state.expiry.as_mut() else {
            return;
        };

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

        if let Some(state) = entity {
            state.expiry = self.prev_expiry;
        }
    }
}

/// Write a DRep's computed voting power — the delegated stake accumulated by
/// the EWRAP boundary scan (`GovState.distr.drep_distr`) — into
/// `voting_power`. Emitted at EWRAP finalize for every registered DRep whose
/// stored value differs from the accumulated one.
///
/// The field stays `u64` at its existing CBOR index — its type is frozen; a
/// per-epoch history, if APIs ever want one, is a new field at a higher
/// index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepPowerUpdate {
    pub(crate) drep_id: EntityKey,
    pub(crate) power: u64,

    // undo
    pub(crate) prev_power: u64,
}

impl DRepPowerUpdate {
    pub fn new(drep_id: EntityKey, power: u64) -> Self {
        Self {
            drep_id,
            power,
            prev_power: 0,
        }
    }
}

impl dolos_core::EntityDelta for DRepPowerUpdate {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, self.drep_id.clone()))
    }

    fn apply(&mut self, entity: &mut Option<DRepState>) {
        let entity = entity.as_mut().expect("existing drep");

        self.prev_power = entity.voting_power;
        entity.voting_power = self.power;
    }

    fn undo(&self, entity: &mut Option<DRepState>) {
        let entity = entity.as_mut().expect("existing drep");

        entity.voting_power = self.prev_power;
    }
}

#[cfg(test)]
mod prop_tests {
    use super::testing::any_drep_state;
    use super::*;
    use crate::model::testing::{self as root, assert_delta_roundtrip};
    use proptest::prelude::*;

    prop_compose! {
        fn any_drep_registration()(
            drep in root::any_drep(),
            slot in root::any_slot(),
            txorder in root::any_tx_order(),
            deposit in root::any_lovelace(),
            anchor in prop::option::of(root::any_anchor()),
        ) -> DRepRegistration {
            DRepRegistration::new(drep, slot, txorder, deposit, anchor)
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
        fn any_drep_anchor_update()(
            drep in root::any_drep(),
            anchor in prop::option::of(root::any_anchor()),
        ) -> DRepAnchorUpdate {
            DRepAnchorUpdate::new(drep, anchor)
        }
    }

    prop_compose! {
        fn any_drep_expiration()(
            drep in root::any_drep(),
        ) -> DRepExpiration {
            DRepExpiration::new(drep_to_entity_key(&drep))
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
        fn any_drep_power_update()(
            drep in root::any_drep(),
            power in root::any_lovelace(),
        ) -> DRepPowerUpdate {
            DRepPowerUpdate::new(drep_to_entity_key(&drep), power)
        }
    }

    prop_compose! {
        fn any_drep_dormancy_release()(
            drep in root::any_drep(),
            dormant_epochs in 1u64..32u64,
            epoch in root::any_epoch(),
        ) -> DRepDormancyRelease {
            DRepDormancyRelease::new(drep_to_entity_key(&drep), dormant_epochs, epoch)
        }
    }

    prop_compose! {
        fn any_drep_seen()(
            drep in root::any_drep(),
            slot in root::any_slot(),
            txorder in root::any_tx_order(),
        ) -> DRepSeen {
            DRepSeen::new(drep, slot, txorder)
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
        fn drep_anchor_update_roundtrip(
            entity in prop::option::of(any_drep_state()),
            delta in any_drep_anchor_update(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn drep_anchor_update_serde_roundtrip(
            entity in prop::option::of(any_drep_state()),
            delta in any_drep_anchor_update(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn drep_expiration_roundtrip(
            entity in any_drep_state(),
            delta in any_drep_expiration(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn drep_power_update_roundtrip(
            entity in any_drep_state(),
            delta in any_drep_power_update(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn drep_power_update_serde_roundtrip(
            entity in any_drep_state(),
            delta in any_drep_power_update(),
        ) {
            root::assert_delta_serde_roundtrip(Some(entity), delta);
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

        #[test]
        fn drep_seen_roundtrip(
            entity in prop::option::of(any_drep_state()),
            delta in any_drep_seen(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn drep_seen_serde_roundtrip(
            entity in prop::option::of(any_drep_state()),
            delta in any_drep_seen(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }
    }

    #[test]
    fn drep_seen_keeps_earliest_sighting() {
        use dolos_core::EntityDelta as _;

        let drep = DRep::Key([1u8; 28].into());
        let mut entity = None;

        DRepSeen::new(drep.clone(), 100, 3).apply(&mut entity);
        assert_eq!(entity.as_ref().unwrap().first_seen_at, Some((100, 3)));

        // a later sighting must not move the first appearance
        DRepSeen::new(drep, 200, 1).apply(&mut entity);
        assert_eq!(entity.unwrap().first_seen_at, Some((100, 3)));
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
        let mut state = DRepState::new(drep.clone());
        state.registered_at = Some((100, 0));
        state.unregistered_at = Some((200, 0));
        let mut entity = Some(state.clone());
        let mut delta = DRepExpiryUpdate::new(drep.clone(), 130, 110, true);
        delta.apply(&mut entity);
        assert_eq!(entity.as_ref().unwrap().expiry, None);

        // cert-driven refresh applies regardless
        let mut delta = DRepExpiryUpdate::new(drep.clone(), 130, 110, false);
        delta.apply(&mut entity);
        assert_eq!(
            entity.as_ref().unwrap().expiry,
            Some(DRepExpiry::new(130, 110))
        );
        delta.undo(&mut entity);
        assert_eq!(entity.unwrap().expiry, None);
    }

    #[test]
    fn expiry_refresh_clears_expired_flag() {
        let drep = DRep::Key([1u8; 28].into());

        let mut state = DRepState::new(drep.clone());
        state.registered_at = Some((100, 0));
        state.expired = true;
        state.expiry = Some(DRepExpiry::new(105, 85));

        let mut entity = Some(state);

        let mut delta = DRepExpiryUpdate::new(drep, 130, 110, true);
        delta.apply(&mut entity);

        let applied = entity.as_ref().unwrap();
        assert!(!applied.expired);
        assert_eq!(applied.expiry.unwrap().current, 130);
        assert_eq!(applied.expiry.unwrap().as_of(109), Some(105));

        delta.undo(&mut entity);
        let restored = entity.unwrap();
        assert!(restored.expired);
        assert_eq!(restored.expiry, Some(DRepExpiry::new(105, 85)));
    }

    #[test]
    fn dormancy_release_folds_but_never_resurrects() {
        let drep = DRep::Key([1u8; 28].into());
        let key = drep_to_entity_key(&drep);

        // active drep: counter folds into the stored value
        let mut state = DRepState::new(drep.clone());
        state.registered_at = Some((100, 0));
        state.expiry = Some(DRepExpiry::new(118, 98));
        let mut entity = Some(state);

        let mut delta = DRepDormancyRelease::new(key.clone(), 5, 120);
        delta.apply(&mut entity);
        assert_eq!(entity.as_ref().unwrap().expiry.unwrap().current, 123);
        assert_eq!(
            entity.as_ref().unwrap().expiry.unwrap().as_of(119),
            Some(118)
        );

        delta.undo(&mut entity);
        assert_eq!(
            entity.as_ref().unwrap().expiry,
            Some(DRepExpiry::new(118, 98))
        );

        // long-expired drep: untouched
        let mut state = DRepState::new(drep.clone());
        state.registered_at = Some((100, 0));
        state.expiry = Some(DRepExpiry::new(110, 98));
        let mut entity = Some(state.clone());

        let mut delta = DRepDormancyRelease::new(key.clone(), 5, 120);
        delta.apply(&mut entity);
        assert_eq!(entity.as_ref().unwrap().expiry, state.expiry);

        // legacy row without the field: skipped
        let mut state = DRepState::new(drep);
        state.registered_at = Some((100, 0));
        let mut entity = Some(state);
        let mut delta = DRepDormancyRelease::new(key, 5, 120);
        delta.apply(&mut entity);
        assert_eq!(entity.as_ref().unwrap().expiry, None);
    }
}

#[cfg(test)]
mod compat_tests {
    use super::*;

    /// Replica of the on-disk `DRepState` shape before the phase-3 expiry
    /// and first-seen additions (indexes 0..=7). Encoding this and decoding
    /// it as the current `DRepState` proves that pre-existing rows keep
    /// decoding, with the new fields empty.
    #[derive(Debug, Encode, Decode, Clone, PartialEq, Eq)]
    struct LegacyDRepState {
        #[n(0)]
        registered_at: Option<(BlockSlot, TxOrder)>,

        #[n(1)]
        voting_power: u64,

        #[n(2)]
        last_active_slot: Option<u64>,

        #[n(3)]
        unregistered_at: Option<(BlockSlot, TxOrder)>,

        #[n(4)]
        expired: bool,

        #[n(5)]
        deposit: u64,

        #[n(6)]
        identifier: DRep,

        #[n(7)]
        anchor: Option<Anchor>,
    }

    #[test]
    fn legacy_rows_decode_with_new_fields_empty() {
        let legacy = LegacyDRepState {
            registered_at: Some((1234, 2)),
            voting_power: 500_000_000,
            last_active_slot: Some(5678),
            unregistered_at: None,
            expired: false,
            deposit: 500_000_000,
            identifier: DRep::Key([7u8; 28].into()),
            anchor: Some(Anchor {
                url: "https://example.com".to_string(),
                content_hash: [9u8; 32].into(),
            }),
        };

        let bytes = minicbor::to_vec(&legacy).unwrap();
        let decoded: DRepState = minicbor::decode(&bytes).unwrap();

        assert_eq!(decoded.registered_at, legacy.registered_at);
        assert_eq!(decoded.voting_power, legacy.voting_power);
        assert_eq!(decoded.last_active_slot, legacy.last_active_slot);
        assert_eq!(decoded.unregistered_at, legacy.unregistered_at);
        assert_eq!(decoded.expired, legacy.expired);
        assert_eq!(decoded.deposit, legacy.deposit);
        assert_eq!(decoded.identifier, legacy.identifier);
        assert_eq!(decoded.anchor, legacy.anchor);
        assert_eq!(decoded.expiry, None);
        assert_eq!(decoded.first_seen_at, None);
    }

    #[test]
    fn new_rows_roundtrip() {
        let mut state = DRepState::new(DRep::Script([3u8; 28].into()));
        state.registered_at = Some((100, 1));
        state.expiry = Some(DRepExpiry {
            current: 520,
            updated_in: 500,
            prev: Some(510),
        });
        state.first_seen_at = Some((100, 1));

        let bytes = minicbor::to_vec(&state).unwrap();
        let decoded: DRepState = minicbor::decode(&bytes).unwrap();
        assert_eq!(decoded, state);
    }
}
