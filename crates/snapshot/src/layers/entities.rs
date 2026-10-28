//! The `entity-{kind}` layers: the archive's entity index, one layer per entity
//! kind and epoch (decision 0064).
//!
//! `[id: bytes, slot]`, ordered by `id`, for the entities first seen in the
//! window.
//!
//! The kind is not in the record: it is the layer's kind
//! (`crate::ENTITY_KINDS`), as the namespace is a log layer's. The kind still
//! decides the id width, so [`encode`] and [`decode`] take it as an argument,
//! and the width rule is [`EntityRecord::new`]'s: a record whose id is not its
//! kind's width is refused at the record.
//!
//! Ids are stored verbatim, so this codec, like the `indexes` one, copies
//! stored bytes through and derives nothing.

use dolos_core::{EntityKind, EntityRecord};
use stelae::frame::{self, CanonicalCbor};

use crate::Error;
use stelae::codec::{blob, close, open, uint};

/// The name this codec refuses under: one record shape serves every
/// `entity-{kind}` kind.
const ENTITY: &str = "entity";

/// Encode one record of its kind's layer.
pub fn encode(record: &EntityRecord) -> Result<CanonicalCbor, Error> {
    Ok(frame::encode(|e| {
        e.array(2)?.bytes(record.key())?.u64(record.slot)?;
        Ok(())
    })?)
}

/// Decode one record of `kind`'s layer, holding the id to the kind's width.
pub fn decode(kind: EntityKind, bytes: &[u8]) -> Result<EntityRecord, Error> {
    let mut decoder = minicbor::Decoder::new(bytes);

    open(ENTITY, &mut decoder, 2)?;

    let id = blob(ENTITY, "id", &mut decoder)?;
    let slot = uint(ENTITY, "slot", &mut decoder)?;

    close(ENTITY, &decoder, bytes)?;

    Ok(EntityRecord::new(kind, id, slot)?)
}

/// Strictly ascending id.
///
/// One kind per layer, so the id alone orders it, and an id names one entity:
/// two records sharing one are a contradiction rather than a pair.
#[derive(Debug, Default, Clone)]
pub struct OrderCheck {
    last: Option<EntityRecord>,
}

impl OrderCheck {
    pub fn check(&mut self, record: &EntityRecord) -> Result<(), Error> {
        if let Some(previous) = &self.last {
            if record.key() <= previous.key() {
                return Err(Error::out_of_order(
                    ENTITY,
                    format!(
                        "{} follows {}",
                        hex::encode(record.key()),
                        hex::encode(previous.key()),
                    ),
                ));
            }
        }

        self.last = Some(*record);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOY: EntityKind = EntityKind::new("toy", 4);

    fn record(id: [u8; 4], slot: u64) -> EntityRecord {
        EntityRecord::new(TOY, &id, slot).unwrap()
    }

    fn wire(id: &[u8], slot: u64) -> CanonicalCbor {
        frame::encode(|e| {
            e.array(2)?.bytes(id)?.u64(slot)?;
            Ok(())
        })
        .unwrap()
    }

    #[test]
    fn round_trips() {
        let original = record([1, 2, 3, 4], 432_000);
        let encoded = encode(&original).unwrap();

        assert_eq!(encoded.as_bytes(), wire(&[1, 2, 3, 4], 432_000).as_bytes());
        assert_eq!(decode(TOY, encoded.as_bytes()).unwrap(), original);
    }

    /// The width is the kind's, refused by the record type this codec defers
    /// to rather than checked a second time here.
    #[test]
    fn an_id_of_another_width_is_refused() {
        for width in [0, 3, 5, 32] {
            let err = decode(TOY, wire(&vec![0; width], 1).as_bytes()).unwrap_err();
            assert!(matches!(err, Error::Index(_)), "{width}: {err:?}");
        }
    }

    #[test]
    fn a_record_of_another_shape_is_refused() {
        let three = frame::encode(|e| {
            e.array(3)?.str("toy")?.bytes(&[0; 4])?.u64(1)?;
            Ok(())
        })
        .unwrap();

        let swapped = frame::encode(|e| {
            e.array(2)?.u64(1)?.bytes(&[0; 4])?;
            Ok(())
        })
        .unwrap();

        for bytes in [three, swapped] {
            let err = decode(TOY, bytes.as_bytes()).unwrap_err();
            assert!(matches!(err, Error::MalformedRecord { .. }), "{err:?}");
        }
    }

    #[test]
    fn ordering_is_by_id_alone() {
        let mut order = OrderCheck::default();

        for record in [record([0, 0, 0, 1], 9), record([0, 0, 0, 2], 0)] {
            order.check(&record).unwrap();
        }

        for backwards in [record([0, 0, 0, 2], 5), record([0, 0, 0, 0], 99)] {
            let err = order.check(&backwards).unwrap_err();
            assert!(matches!(err, Error::OutOfOrder { .. }), "{err:?}");
        }
    }
}
