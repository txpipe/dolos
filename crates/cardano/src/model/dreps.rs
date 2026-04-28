use dolos_core::{BlockSlot, EntityKey, NsKey, TxOrder};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    ledger::primitives::conway::{Anchor, DRep},
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
        pub fn any_drep_state()(
            identifier in root::any_drep(),
            registered_at in prop::option::of((root::any_slot(), root::any_tx_order())),
            voting_power in root::any_lovelace(),
            last_active_slot in prop::option::of(root::any_slot()),
            unregistered_at in prop::option::of((root::any_slot(), root::any_tx_order())),
            expired in any::<bool>(),
            deposit in root::any_lovelace(),
        ) -> DRepState {
            DRepState {
                identifier,
                registered_at,
                voting_power,
                last_active_slot,
                unregistered_at,
                expired,
                deposit,
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
        let state = entity.as_mut().expect("can't undo unregister on missing drep");
        state.voting_power = self.prev_voting_power.unwrap_or(0);
        state.unregistered_at = self.prev_unregistered_at;
        state.deposit = self.prev_deposit.unwrap_or(0);
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

#[cfg(test)]
mod prop_tests {
    use super::*;
    use super::testing::any_drep_state;
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
        fn any_drep_expiration()(
            drep in root::any_drep(),
        ) -> DRepExpiration {
            DRepExpiration::new(drep_to_entity_key(&drep))
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
    }
}
