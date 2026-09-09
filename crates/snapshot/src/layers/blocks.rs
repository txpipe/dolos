//! The `blocks` layer: raw block data, one record per block.
//!
//! `[slot, hash: bytes(32), body: bytes]`, ascending slot, stream order
//! preserved within a slot.
//!
//! ## The hash is an input, not a derivation
//!
//! [`dolos_core::ArchiveStore::get_range`] yields `(slot, body)` — the block
//! hash is not stored alongside the block, it is computed by decoding the
//! header. This codec therefore takes the hash from its caller and neither
//! derives nor checks it: deriving it means Cardano block decoding, which
//! belongs to the export driver that already holds a decoded block, not to a
//! format description. The consequence is explicit rather than implicit — a
//! publisher that supplies a wrong hash produces a layer that restores cleanly
//! and answers block-hash lookups wrongly, so the derivation site is the one
//! place that has to be right.
//!
//! ## Why the order rule is not a sort key
//!
//! Byron end-of-epoch boundary blocks share a slot with the block that follows
//! them, and the two are distinguishable only by the order they arrived in.
//! "Ascending slot, stream order within a slot" is therefore a property of a
//! *sequence*, not a function of a record, and no comparator expresses it. The
//! validator enforces the half that is checkable — slots never go backwards —
//! and the export driver is responsible for feeding blocks in chain order.

use dolos_core::{BlockHash, BlockSlot};
use stelae::frame::{self, CanonicalCbor};

use crate::{Error, BLOCKS};
use stelae::codec::{blob, close, fixed, open, uint};

/// One block, as the layer carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRecord {
    pub slot: BlockSlot,
    pub hash: BlockHash,
    /// Raw wire CBOR, verbatim. Never re-encoded — a block's bytes are its
    /// identity on the chain, and this layer is not the place to reinterpret
    /// them.
    pub body: Vec<u8>,
}

impl BlockRecord {
    pub fn new(slot: BlockSlot, hash: BlockHash, body: impl Into<Vec<u8>>) -> Self {
        Self {
            slot,
            hash,
            body: body.into(),
        }
    }
}

pub fn encode(record: &BlockRecord) -> Result<CanonicalCbor, Error> {
    Ok(frame::encode(|e| {
        e.array(3)?
            .u64(record.slot)?
            .bytes(record.hash.as_ref())?
            .bytes(&record.body)?;
        Ok(())
    })?)
}

pub fn decode(bytes: &[u8]) -> Result<BlockRecord, Error> {
    let mut decoder = minicbor::Decoder::new(bytes);

    open(BLOCKS, &mut decoder, 3)?;

    let slot = uint(BLOCKS, "slot", &mut decoder)?;
    let hash = fixed::<32>(BLOCKS, "hash", &mut decoder)?;
    let body = blob(BLOCKS, "body", &mut decoder)?.to_vec();

    close(BLOCKS, &decoder, bytes)?;

    Ok(BlockRecord {
        slot,
        hash: BlockHash::new(hash),
        body,
    })
}

/// Ascending slot, with same-slot runs left alone.
#[derive(Debug, Default, Clone, Copy)]
pub struct OrderCheck {
    last: Option<BlockSlot>,
}

impl OrderCheck {
    pub fn check(&mut self, record: &BlockRecord) -> Result<(), Error> {
        if let Some(last) = self.last {
            if record.slot < last {
                return Err(Error::out_of_order(
                    BLOCKS,
                    format!("slot {} follows slot {last}", record.slot),
                ));
            }
        }

        self.last = Some(record.slot);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(slot: BlockSlot, byte: u8) -> BlockRecord {
        BlockRecord::new(slot, BlockHash::new([byte; 32]), vec![byte; 4])
    }

    #[test]
    fn round_trips() {
        for original in [record(0, 0), record(u32::MAX as u64 + 1, 0xff)] {
            let encoded = encode(&original).unwrap();
            assert_eq!(decode(encoded.as_bytes()).unwrap(), original);
        }
    }

    #[test]
    fn an_empty_body_is_a_shape_error_nowhere() {
        // A zero-length body is structurally fine; refusing it would be this
        // crate inventing a chain rule it has no business having.
        let original = BlockRecord::new(7, BlockHash::new([0; 32]), Vec::new());
        let encoded = encode(&original).unwrap();
        assert_eq!(decode(encoded.as_bytes()).unwrap(), original);
    }

    #[test]
    fn a_wrong_width_hash_is_refused() {
        let bad = frame::encode(|e| {
            e.array(3)?.u64(1)?.bytes(&[0u8; 28])?.bytes(&[])?;
            Ok(())
        })
        .unwrap();

        let err = decode(bad.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::MalformedRecord { .. }), "{err:?}");
    }

    #[test]
    fn a_trailing_byte_is_refused() {
        let mut bytes = encode(&record(1, 1)).unwrap().into_bytes();
        bytes.push(0x00);

        let err = decode(&bytes).unwrap_err();
        assert!(matches!(err, Error::MalformedRecord { .. }), "{err:?}");
    }

    /// The Byron case: two blocks at one slot, in the order they arrived.
    #[test]
    fn same_slot_blocks_are_accepted_in_stream_order() {
        let mut order = OrderCheck::default();

        for record in [record(10, 1), record(10, 2), record(11, 3)] {
            order.check(&record).unwrap();
        }
    }

    #[test]
    fn a_slot_going_backwards_is_refused() {
        let mut order = OrderCheck::default();

        order.check(&record(11, 1)).unwrap();

        let err = order.check(&record(10, 2)).unwrap_err();
        assert!(matches!(err, Error::OutOfOrder { .. }), "{err:?}");
    }
}
