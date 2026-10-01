use dolos_core::{EntityKey, NsKey};
use pallas::codec::minicbor::{self, Decode, Encode};
use serde::{Deserialize, Serialize};

use super::FixedNamespace as _;

/// Gives the entity key for a transaction-metadata label. The first eight
/// bytes of the key contain the label in big-endian byte order. As a result,
/// an iteration of the namespace gives the labels in numeric order.
pub fn metadata_label_to_entity_key(label: u64) -> EntityKey {
    EntityKey::from(&label.to_be_bytes())
}

/// Decodes the label that [`metadata_label_to_entity_key`] encoded into `key`.
pub fn metadata_label_from_entity_key(key: &EntityKey) -> u64 {
    let bytes: [u8; 8] = key.as_ref()[..8].try_into().unwrap();
    u64::from_be_bytes(bytes)
}

/// The number of transactions that have a transaction-metadata label.
///
/// The entity key is the label (see [`metadata_label_to_entity_key`]). The
/// value does not contain the label again. Each valid transaction that has
/// the label in its auxiliary data adds 1 to the count. This count is equal to
/// the number of `tx_metadata` rows for the label in db-sync.
#[derive(Debug, Clone, Default, Encode, Decode, PartialEq, Eq)]
pub struct MetadataLabelState {
    #[n(0)]
    pub tx_count: u64,
}

entity_boilerplate!(MetadataLabelState, "metadata-labels");

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_metadata_label_state()(tx_count in 0u64..1000u64) -> MetadataLabelState {
            MetadataLabelState { tx_count }
        }
    }
}

// --- Deltas ---

/// Adds `count` transactions to the count of a label.
///
/// The roll visitor makes one delta for each label in a block. The `count` is
/// the number of valid transactions in the block that have the label.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataLabelTxInc {
    pub(crate) label: u64,
    pub(crate) count: u64,

    /// If this delta created the entity of the label, the flag is `true`. If
    /// the entity existed before this delta, the flag is `false`. The `undo`
    /// function uses this flag.
    pub(crate) was_new: bool,
}

impl MetadataLabelTxInc {
    pub fn new(label: u64, count: u64) -> Self {
        Self {
            label,
            count,
            was_new: false,
        }
    }
}

impl dolos_core::EntityDelta for MetadataLabelTxInc {
    type Entity = MetadataLabelState;

    fn key(&self) -> NsKey {
        NsKey::from((
            MetadataLabelState::NS,
            metadata_label_to_entity_key(self.label),
        ))
    }

    fn apply(&mut self, entity: &mut Option<MetadataLabelState>) {
        match entity {
            Some(state) => {
                self.was_new = false;
                state.tx_count += self.count;
            }
            None => {
                self.was_new = true;
                *entity = Some(MetadataLabelState {
                    tx_count: self.count,
                });
            }
        }
    }

    fn undo(&self, entity: &mut Option<MetadataLabelState>) {
        if self.was_new {
            *entity = None;
        } else if let Some(state) = entity {
            state.tx_count -= self.count;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::any_metadata_label_state;
    use super::*;
    use crate::model::testing::{assert_delta_roundtrip, assert_delta_serde_roundtrip};
    use dolos_core::EntityDelta;
    use proptest::prelude::*;

    #[test]
    fn entity_key_round_trips_the_label() {
        for label in [0, 1, 674, 61284, u64::MAX] {
            let key = metadata_label_to_entity_key(label);
            assert_eq!(metadata_label_from_entity_key(&key), label);
        }
    }

    #[test]
    fn entity_keys_sort_in_label_order() {
        let keys: Vec<_> = [0u64, 1, 255, 256, 674, 61284, u64::MAX]
            .into_iter()
            .map(metadata_label_to_entity_key)
            .collect();

        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn first_increment_creates_the_row_and_undo_removes_it() {
        let mut delta = MetadataLabelTxInc::new(674, 3);
        let mut entity = None;

        delta.apply(&mut entity);
        assert!(delta.was_new);
        assert_eq!(entity, Some(MetadataLabelState { tx_count: 3 }));

        delta.undo(&mut entity);
        assert_eq!(entity, None);
    }

    #[test]
    fn increment_adds_to_an_existing_row() {
        let mut delta = MetadataLabelTxInc::new(674, 2);
        let mut entity = Some(MetadataLabelState { tx_count: 5 });

        delta.apply(&mut entity);
        assert!(!delta.was_new);
        assert_eq!(entity, Some(MetadataLabelState { tx_count: 7 }));

        delta.undo(&mut entity);
        assert_eq!(entity, Some(MetadataLabelState { tx_count: 5 }));
    }

    prop_compose! {
        fn any_metadata_label_tx_inc()(
            label in any::<u64>(),
            count in 1u64..1000u64,
        ) -> MetadataLabelTxInc {
            MetadataLabelTxInc::new(label, count)
        }
    }

    proptest! {
        #[test]
        fn metadata_label_tx_inc_roundtrip(
            entity in prop::option::of(any_metadata_label_state()),
            delta in any_metadata_label_tx_inc(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn metadata_label_tx_inc_serde_roundtrip(
            entity in prop::option::of(any_metadata_label_state()),
            delta in any_metadata_label_tx_inc(),
        ) {
            assert_delta_serde_roundtrip(entity, delta);
        }
    }
}
