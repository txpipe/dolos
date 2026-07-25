use std::cmp::Ordering;

use dolos_core::EntityKey;
use pallas::codec::minicbor::{self, Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct EraProtocol(#[n(0)] u16);

impl EraProtocol {
    pub fn is_shelley_or_later(&self) -> bool {
        self.0 >= 2
    }
}

impl std::fmt::Display for EraProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq<u16> for EraProtocol {
    fn eq(&self, other: &u16) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u16> for EraProtocol {
    fn partial_cmp(&self, other: &u16) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

impl From<u16> for EraProtocol {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<EraProtocol> for u16 {
    fn from(value: EraProtocol) -> Self {
        value.0
    }
}

impl From<EraProtocol> for EntityKey {
    fn from(value: EraProtocol) -> Self {
        EntityKey::from(&value.0.to_be_bytes())
    }
}

impl From<EntityKey> for EraProtocol {
    fn from(value: EntityKey) -> Self {
        let bytes: [u8; 2] = value.as_ref()[..2].try_into().unwrap();
        Self(u16::from_be_bytes(bytes))
    }
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EraTransition {
    #[n(0)]
    pub prev_version: EraProtocol,

    #[n(1)]
    pub new_version: EraProtocol,
}

impl EraTransition {
    /// Check if this boundary is transitioning to shelley for the first time.
    pub fn entering_shelley(&self) -> bool {
        self.prev_version < 2 && self.new_version == 2
    }

    /// Check if this boundary is transitioning from Shelley to Allegra.
    /// At this boundary, unredeemed AVVM UTxOs are reclaimed to reserves.
    pub fn entering_allegra(&self) -> bool {
        self.prev_version == 2 && self.new_version == 3
    }

    /// Check if this boundary is transitioning into Conway (the Chang hard
    /// fork). At this boundary, the governance singleton is seeded from the
    /// Conway genesis (initial constitution + committee).
    pub fn entering_conway(&self) -> bool {
        self.prev_version < 9 && self.new_version >= 9
    }
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
pub struct EraBoundary {
    #[n(0)]
    pub epoch: u64,

    #[n(1)]
    pub slot: u64,

    #[n(2)]
    pub timestamp: u64,
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
pub struct EraSummary {
    #[n(0)]
    pub start: EraBoundary,

    #[n(1)]
    pub end: Option<EraBoundary>,

    #[n(2)]
    pub epoch_length: u64,

    #[n(3)]
    pub slot_length: u64,

    #[n(4)]
    #[cbor(default)]
    pub protocol: u16,
}

entity_boilerplate!(EraSummary, "eras");

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_era_transition()(
            prev in 0u16..10u16,
            new in 0u16..10u16,
        ) -> EraTransition {
            EraTransition {
                prev_version: EraProtocol::from(prev),
                new_version: EraProtocol::from(new),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transition(prev: u16, new: u16) -> EraTransition {
        EraTransition {
            prev_version: EraProtocol::from(prev),
            new_version: EraProtocol::from(new),
        }
    }

    #[test]
    fn entering_conway_detects_chang_boundary() {
        // the Chang hard fork proper
        assert!(transition(8, 9).entering_conway());

        // a jump over protocol 9 still enters Conway
        assert!(transition(8, 10).entering_conway());

        // intra-Conway bumps and earlier boundaries don't re-seed
        assert!(!transition(9, 10).entering_conway());
        assert!(!transition(7, 8).entering_conway());
        assert!(!transition(2, 3).entering_conway());
    }
}
