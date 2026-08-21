//! The ground rules: minicbor's own tolerance behaviour, pinned.
//!
//! Everything the compatibility contract of decision 0026 promises rests on
//! three properties of the codec, none of which this repository controls:
//!
//! 1. a decoder **skips** a trailing field it does not know;
//! 2. a **gap** in the index sequence is null-padded, so appending after a gap
//!    does not move the fields before it;
//! 3. a **missing** trailing field is tolerated where the field is `Option` or
//!    carries `#[cbor(default)]`.
//!
//! The types below are synthetic — they model nothing in the ledger — and
//! exist so that a minicbor upgrade which changed any of the three breaks
//! *here*, in a test with a name that says what happened, rather than in a
//! stele in production. They also carry the pinned array shape, so a change
//! from array to map encoding, or a change in how a gap is padded, is a
//! failure and not a silent re-encoding of every record the profile carries.
//!
//! The same types double as the exercise for the registry's retained-history
//! machinery ([`synthetic_entry`]): today every real namespace sits at
//! revision 1 with no history behind it, so without this the append-only path
//! would ship untested.

use dolos_cardano::pallas::codec::minicbor::{self, Decode, Encode};

use super::{Entry, Pinned};

/// A record before a field was appended.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct BeforeAppend {
    #[n(0)]
    pub a: u64,

    #[n(1)]
    pub b: u64,
}

/// The same record after appending an optional field at the next free index.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct AfterAppend {
    #[n(0)]
    pub a: u64,

    #[n(1)]
    pub b: u64,

    #[n(2)]
    pub c: Option<u64>,
}

/// The same append, but of a non-`Option` field made tolerant by
/// `#[cbor(default)]` — the other half of rule 3, and the shape most of the
/// real appends in `model/*.rs` actually use.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct AfterDefaultedAppend {
    #[n(0)]
    pub a: u64,

    #[n(1)]
    pub b: u64,

    #[n(2)]
    #[cbor(default)]
    pub c: u64,
}

/// A record whose indexes leave a gap — `PoolState` and `RollingStats` both do
/// this for real. The gap is null-padded, which is what lets a later append
/// land at the next index without moving anything before it.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Gapped {
    #[n(0)]
    pub a: u64,

    #[n(3)]
    pub d: u64,
}

/// [`BeforeAppend`] with its two indexes swapped: same fields, same types,
/// same names, different contract. Pinned so the suite demonstrates on itself
/// that renumbering moves bytes — the failure mode the whole registry exists
/// to catch.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Renumbered {
    #[n(1)]
    pub a: u64,

    #[n(0)]
    pub b: u64,
}

pub fn before_append() -> BeforeAppend {
    BeforeAppend { a: 1, b: 2 }
}

pub fn after_append() -> AfterAppend {
    AfterAppend {
        a: 1,
        b: 2,
        c: Some(3),
    }
}

pub fn gapped() -> Gapped {
    Gapped { a: 7, d: 9 }
}

pub fn renumbered() -> Renumbered {
    Renumbered { a: 1, b: 2 }
}

/// `[1, 2]` — two definite-length array elements, shortest-form.
pub const BEFORE_APPEND_HEX: &str = "820102";

/// `[1, 2, 3]` — the appended field lands after the existing two, which keep
/// their positions.
pub const AFTER_APPEND_HEX: &str = "83010203";

/// `[7, null, null, 9]` — the gap is padded, not closed.
pub const GAPPED_HEX: &str = "8407f6f609";

/// `[2, 1]` — the same two values, swapped, because the indexes were.
pub const RENUMBERED_HEX: &str = "820201";

fn encode_synthetic() -> (&'static str, Vec<u8>) {
    (
        SYNTHETIC_NS,
        minicbor::to_vec(after_append()).expect("the synthetic canary encodes"),
    )
}

fn decode_synthetic(bytes: &[u8]) -> Result<(), String> {
    minicbor::decode::<AfterAppend>(bytes)
        .map(|_| ())
        .map_err(|err| err.to_string())
}

/// Not a namespace this profile defines — the synthetic entry is deliberately
/// outside `NAMESPACES` so the coverage assertion cannot accept it as one.
pub const SYNTHETIC_NS: &str = "$synthetic";

/// The revision the synthetic entry stands at, playing the part `SCHEMA_REVS`
/// plays for a real namespace.
pub const SYNTHETIC_REV: u64 = 2;

/// A registry entry whose history has actually moved: revision 1 is the
/// retained encoding of [`BeforeAppend`], revision 2 the current
/// [`AfterAppend`]. Running the real assertions over it is what proves the
/// append-only path works before a real namespace needs it.
pub fn synthetic_entry() -> Entry {
    Entry {
        ns: SYNTHETIC_NS,
        history: &[
            Pinned {
                rev: 1,
                hex: BEFORE_APPEND_HEX,
            },
            Pinned {
                rev: 2,
                hex: AFTER_APPEND_HEX,
            },
        ],
        encode: encode_synthetic,
        decode: decode_synthetic,
    }
}
