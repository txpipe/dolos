//! One codec per layer kind.
//!
//! Every module here exposes the same three things, so a caller meets one shape
//! five times:
//!
//! - `encode(&Record) -> Result<CanonicalCbor, Error>` — through
//!   [`stelae::frame::encode`], so the deterministic-encoding profile is
//!   enforced by construction rather than by review.
//! - `decode(&[u8]) -> Result<Record, Error>` — fail-closed, and total: a
//!   record that decodes is a record the restore path can write.
//! - `OrderCheck` — the kind's ordering contract as a streaming validator,
//!   because ordering *is* content here. A restore ingests these records
//!   append-only into sorted stores; records arriving out of order do not fail
//!   loudly, they land in a store whose own queries then miss them.
//!
//! The ordering rules are ADR-004's, and three of the five are exactly the
//! iteration order the source stores already promise
//! ([`dolos_core::IndexStore::iter_archive_tags`],
//! [`dolos_core::IndexStore::iter_exact_records`]). The validator is not a
//! substitute for that promise; it is what catches a driver that merges,
//! chunks or parallelizes those iterators and loses it.
//!
//! ## Why decoding does not re-validate canonical form
//!
//! Records reach `decode` from [`stelae::dir::Layer::records`] or
//! [`stelae::LayerReader::next_record`], both of which have already validated
//! every byte against the deterministic profile — that is the framing layer's
//! job and it is not repeated here. What `decode` does check is *shape*: the
//! field count, each field's type and width, and that nothing trails the
//! record.

pub mod blocks;
pub mod digests;
pub mod indexes;
pub mod logs;
pub mod state;

use minicbor::Decoder;

use crate::Error;

/// Open a record's outer array, insisting on a definite length of `expected`.
pub(crate) fn open(
    kind: &'static str,
    decoder: &mut Decoder<'_>,
    expected: u64,
) -> Result<(), Error> {
    let fields = decoder
        .array()
        .map_err(|e| Error::malformed(kind, format!("expected an array: {e}")))?
        .ok_or_else(|| Error::malformed(kind, "indefinite-length array"))?;

    if fields != expected {
        return Err(Error::malformed(
            kind,
            format!("expected {expected} fields, found {fields}"),
        ));
    }

    Ok(())
}

/// Insist the record ended where the array did.
///
/// A CBOR sequence has no frame markers, so a record with a tail would be read
/// as one item by the framing layer and as a different, shorter item here — two
/// readers disagreeing about the same bytes, which is how a diffId stops
/// meaning anything.
pub(crate) fn close(kind: &'static str, decoder: &Decoder<'_>, bytes: &[u8]) -> Result<(), Error> {
    let read = decoder.position();

    if read != bytes.len() {
        return Err(Error::malformed(
            kind,
            format!("{} trailing byte(s) after the record", bytes.len() - read),
        ));
    }

    Ok(())
}

pub(crate) fn uint(
    kind: &'static str,
    field: &str,
    decoder: &mut Decoder<'_>,
) -> Result<u64, Error> {
    decoder
        .u64()
        .map_err(|e| Error::malformed(kind, format!("{field}: {e}")))
}

pub(crate) fn text<'b>(
    kind: &'static str,
    field: &str,
    decoder: &mut Decoder<'b>,
) -> Result<&'b str, Error> {
    decoder
        .str()
        .map_err(|e| Error::malformed(kind, format!("{field}: {e}")))
}

pub(crate) fn blob<'b>(
    kind: &'static str,
    field: &str,
    decoder: &mut Decoder<'b>,
) -> Result<&'b [u8], Error> {
    decoder
        .bytes()
        .map_err(|e| Error::malformed(kind, format!("{field}: {e}")))
}

/// A byte string of exactly `N` bytes.
///
/// Width is checked here rather than by a lossy conversion downstream: every
/// fixed-width key type in `dolos-core` (`EntityKey`, `LogKey`, `KeyHash`)
/// converts from a slice by zero-padding or truncating, so a wrong-width field
/// would become a valid-looking key that no lookup can ever reach.
pub(crate) fn fixed<const N: usize>(
    kind: &'static str,
    field: &str,
    decoder: &mut Decoder<'_>,
) -> Result<[u8; N], Error> {
    let raw = blob(kind, field, decoder)?;

    raw.try_into().map_err(|_| {
        Error::malformed(
            kind,
            format!("{field}: expected {N} bytes, found {}", raw.len()),
        )
    })
}
