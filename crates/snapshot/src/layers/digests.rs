//! The `digests` layer: sha256 of every Cardano immutable-DB file the stele
//! covers.
//!
//! `[immutable_number, chunk: bytes(32), primary: bytes(32), secondary:
//! bytes(32)]`, ascending `immutable_number`.
//!
//! ## Carries no restorable data
//!
//! Nothing here is written to a store. The layer exists so that block data
//! obtained from *somewhere else* — a Mithril aggregator, a mirror, a relay
//! replay — can be checked against the stele's signed inscription before it is
//! imported. It is also the enabler for a future Mithril-sourced restore mode,
//! which is why the digests are exactly Mithril Cardano DB v2's merkle leaves:
//! sha256 over the raw `.chunk`/`.primary`/`.secondary` file bytes, so a
//! certificate whose beacon covers `lastImmutable` verifies them by merkle
//! proof.
//!
//! The certificate itself is deliberately *not* part of a stele. Certificates
//! are produced on the aggregator's cadence, so two publishers at the same
//! boundary would reference different ones and stop reproducing each other's
//! inscription; the digest values, by contrast, are byte-stable properties of
//! the chain.
//!
//! The layer is optional — a network without a Mithril aggregator simply has no
//! `digests` layer, and the inscription says so.

use stelae::{
    frame::{self, CanonicalCbor},
    Digest,
};

use super::{close, fixed, open, uint};
use crate::{Error, DIGESTS};

/// The three files of one immutable chunk, by content.
///
/// Uses [`stelae::Digest`] rather than a fourth thirty-two-byte newtype: these
/// are sha256 over bytes, which is what that type is, and it already prints and
/// parses in the `sha256:…` form the rest of a stele uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImmutableDigests {
    pub immutable_number: u64,
    pub chunk: Digest,
    pub primary: Digest,
    pub secondary: Digest,
}

pub fn encode(record: &ImmutableDigests) -> Result<CanonicalCbor, Error> {
    Ok(frame::encode(|e| {
        e.array(4)?
            .u64(record.immutable_number)?
            .bytes(record.chunk.as_bytes())?
            .bytes(record.primary.as_bytes())?
            .bytes(record.secondary.as_bytes())?;
        Ok(())
    })?)
}

pub fn decode(bytes: &[u8]) -> Result<ImmutableDigests, Error> {
    let mut decoder = minicbor::Decoder::new(bytes);

    open(DIGESTS, &mut decoder, 4)?;

    let immutable_number = uint(DIGESTS, "immutable_number", &mut decoder)?;
    let chunk = fixed::<32>(DIGESTS, "chunk", &mut decoder)?;
    let primary = fixed::<32>(DIGESTS, "primary", &mut decoder)?;
    let secondary = fixed::<32>(DIGESTS, "secondary", &mut decoder)?;

    close(DIGESTS, &decoder, bytes)?;

    Ok(ImmutableDigests {
        immutable_number,
        chunk: Digest::from_bytes(chunk),
        primary: Digest::from_bytes(primary),
        secondary: Digest::from_bytes(secondary),
    })
}

/// Strictly ascending `immutable_number` — one record per immutable file set.
#[derive(Debug, Default, Clone, Copy)]
pub struct OrderCheck {
    last: Option<u64>,
}

impl OrderCheck {
    pub fn check(&mut self, record: &ImmutableDigests) -> Result<(), Error> {
        if let Some(last) = self.last {
            if record.immutable_number <= last {
                return Err(Error::out_of_order(
                    DIGESTS,
                    format!(
                        "immutable {} follows immutable {last}",
                        record.immutable_number
                    ),
                ));
            }
        }

        self.last = Some(record.immutable_number);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(number: u64) -> ImmutableDigests {
        ImmutableDigests {
            immutable_number: number,
            chunk: Digest::from_bytes([0x11; 32]),
            primary: Digest::from_bytes([0x22; 32]),
            secondary: Digest::from_bytes([0x33; 32]),
        }
    }

    #[test]
    fn round_trips() {
        for number in [0u64, 6187, u32::MAX as u64 + 1] {
            let original = record(number);
            let encoded = encode(&original).unwrap();

            assert_eq!(decode(encoded.as_bytes()).unwrap(), original);
        }
    }

    #[test]
    fn a_wrong_width_digest_is_refused() {
        let wire = frame::encode(|e| {
            e.array(4)?
                .u64(1)?
                .bytes(&[0u8; 32])?
                .bytes(&[0u8; 20])?
                .bytes(&[0u8; 32])?;
            Ok(())
        })
        .unwrap();

        let err = decode(wire.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::MalformedRecord { .. }), "{err:?}");
    }

    #[test]
    fn ordering_is_strictly_ascending() {
        let mut order = OrderCheck::default();

        order.check(&record(0)).unwrap();
        order.check(&record(1)).unwrap();

        for backwards in [record(1), record(0)] {
            let err = OrderCheck { last: Some(1) }.check(&backwards).unwrap_err();
            assert!(matches!(err, Error::OutOfOrder { .. }), "{err:?}");
        }
    }
}
