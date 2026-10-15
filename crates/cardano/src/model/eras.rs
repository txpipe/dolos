use std::cmp::Ordering;

use dolos_core::{FixedKey, KeyBytes, KeyCodec, KeyError};
use pallas::codec::minicbor::{self, Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct EraKey(#[n(0)] u16);

impl EraKey {
    pub fn is_shelley_or_later(&self) -> bool {
        self.0 >= 2
    }
}

impl std::fmt::Display for EraKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq<u16> for EraKey {
    fn eq(&self, other: &u16) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u16> for EraKey {
    fn partial_cmp(&self, other: &u16) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

impl From<u16> for EraKey {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<EraKey> for u16 {
    fn from(value: EraKey) -> Self {
        value.0
    }
}

impl KeyCodec for EraKey {
    fn encode_key(&self, out: &mut KeyBytes) {
        u16::from(*self).encode_key(out);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        u16::decode_key(bytes).map(Self::from)
    }
}

impl FixedKey for EraKey {
    const LEN: usize = 2;
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EraTransition {
    #[n(0)]
    pub prev_version: EraKey,

    #[n(1)]
    pub new_version: EraKey,
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
pub struct EraState {
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

entity_boilerplate!(EraState, "eras", state = EraKey);

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
                prev_version: EraKey::from(prev),
                new_version: EraKey::from(new),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transition(prev: u16, new: u16) -> EraTransition {
        EraTransition {
            prev_version: EraKey::from(prev),
            new_version: EraKey::from(new),
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

#[cfg(test)]
mod key_tests {
    use dolos_core::keys::encode;
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn layout_is_pinned() {
        assert_eq!(encode(&EraKey::from(9)).to_vec(), [0, 9]);
    }

    #[test]
    fn malformed_keys_are_refused() {
        assert!(EraKey::decode_key(&[0; 32]).is_err());
    }

    proptest! {
        #[test]
        fn keys_round_trip(era in any::<u16>()) {
            let key = EraKey::from(era);
            prop_assert_eq!(EraKey::decode_key(&encode(&key)), Ok(key));
        }
    }
}
