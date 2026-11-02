use dolos_core::{BlockSlot, NsKey, TxOrder};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    ledger::primitives::conway::{Anchor, DRep},
};
use serde::{Deserialize, Serialize};

use super::DRepKey;

/// Where a certificate sits on chain: the slot of its block, the position of
/// its transaction in the block, and its own position among that
/// transaction's certificates. Sorting by it is the chain's certificate order.
pub type CertPosition = (BlockSlot, TxOrder, usize);

/// What certificates have shown about a DRep: the first certificate
/// position that named it, and the latest anchor a registration or update
/// carried.
///
/// db-sync writes a `drep_hash` row for every DRep a certificate of a valid
/// tx names, vote delegations and the special DReps included, and Blockfrost
/// lists DReps in that order. Its metadata is the latest registration or
/// update that carried an anchor, where the ledger's `DRepState::anchor` is
/// cleared by one that carries none. A DRep only a vote delegation names has
/// a row here and none in `dreps`, which keeps that namespace to the DReps
/// the ledger knows. Rows are never pruned.
#[derive(Debug, Encode, Decode, Clone, PartialEq, Eq)]
pub struct DRepObservationState {
    /// The position of the first certificate that named the DRep.
    #[n(0)]
    pub first_seen: CertPosition,

    /// The anchor of the latest registration or update that carried one;
    /// absent when none has.
    #[n(1)]
    pub anchor: Option<Anchor>,
}

entity_boilerplate!(DRepObservationState, "drep-observations", state = DRepKey);

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_cert_position()(
            slot in root::any_slot(),
            order in root::any_tx_order(),
            cert in any::<u16>().prop_map(usize::from),
        ) -> CertPosition {
            (slot, order, cert)
        }
    }

    prop_compose! {
        pub fn any_drep_observation_state()(
            first_seen in any_cert_position(),
            anchor in prop::option::of(root::any_anchor()),
        ) -> DRepObservationState {
            DRepObservationState { first_seen, anchor }
        }
    }
}

// --- Deltas ---

/// A certificate naming a DRep. The first one sets `first_seen`; one that
/// carries an anchor, a registration or an update, replaces the anchor, and
/// any other keeps it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepObservation {
    pub(crate) drep: DRep,
    pub(crate) at: CertPosition,
    pub(crate) anchor: Option<Anchor>,

    // undo
    pub(crate) prev: Option<(CertPosition, Option<Anchor>)>,
}

impl DRepObservation {
    pub fn new(drep: DRep, at: CertPosition, anchor: Option<Anchor>) -> Self {
        Self {
            drep,
            at,
            anchor,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepObservation {
    type Entity = DRepObservationState;

    fn key(&self) -> NsKey {
        NsKey::of::<DRepObservationState>(&DRepKey::from(&self.drep))
    }

    fn apply(&mut self, entity: &mut Option<DRepObservationState>) {
        self.prev = entity
            .as_ref()
            .map(|state| (state.first_seen, state.anchor.clone()));

        let state = entity.get_or_insert_with(|| DRepObservationState {
            first_seen: self.at,
            anchor: None,
        });

        if let Some(anchor) = &self.anchor {
            state.anchor = Some(anchor.clone());
        }
    }

    fn undo(&self, entity: &mut Option<DRepObservationState>) {
        *entity = self
            .prev
            .clone()
            .map(|(first_seen, anchor)| DRepObservationState { first_seen, anchor });
    }
}

#[cfg(test)]
mod tests {
    use dolos_core::EntityDelta as _;
    use proptest::prelude::*;

    use super::{testing::*, *};
    use crate::model::testing::{self as root, assert_delta_roundtrip};

    prop_compose! {
        fn any_drep_observation()(
            drep in root::any_drep(),
            at in any_cert_position(),
            anchor in prop::option::of(root::any_anchor()),
        ) -> DRepObservation {
            DRepObservation::new(drep, at, anchor)
        }
    }

    proptest! {
        #[test]
        fn drep_observation_roundtrip(
            entity in prop::option::of(any_drep_observation_state()),
            delta in any_drep_observation(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn drep_observation_serde_roundtrip(
            entity in prop::option::of(any_drep_observation_state()),
            delta in any_drep_observation(),
        ) {
            root::assert_delta_serde_roundtrip(entity, delta);
        }
    }

    fn anchor(byte: u8) -> Anchor {
        Anchor {
            url: format!("https://drep.invalid/{byte}"),
            content_hash: [byte; 32].into(),
        }
    }

    #[test]
    fn first_observation_holds_and_anchor_follows_the_latest_carrier() {
        let drep = DRep::Key([1u8; 28].into());
        let mut entity = None;

        // a vote delegation names the DRep first
        DRepObservation::new(drep.clone(), (100, 3, 1), None).apply(&mut entity);
        assert_eq!(
            entity,
            Some(DRepObservationState {
                first_seen: (100, 3, 1),
                anchor: None,
            })
        );

        // its registration carries an anchor
        DRepObservation::new(drep.clone(), (200, 0, 0), Some(anchor(1))).apply(&mut entity);

        // an update without one, then a retirement, keep it
        DRepObservation::new(drep.clone(), (300, 0, 0), None).apply(&mut entity);
        DRepObservation::new(drep.clone(), (400, 0, 0), None).apply(&mut entity);

        assert_eq!(
            entity,
            Some(DRepObservationState {
                first_seen: (100, 3, 1),
                anchor: Some(anchor(1)),
            })
        );

        // an update with a new one replaces it
        DRepObservation::new(drep, (500, 0, 0), Some(anchor(2))).apply(&mut entity);
        assert_eq!(entity.unwrap().anchor, Some(anchor(2)));
    }
}
