//! Fixed-arity decode helpers for a layer's content records.
//!
//! A profile's records are CBOR arrays of a known width, read out of bytes
//! [`crate::frame`] has already validated against the deterministic profile.
//! What is left to check is *shape* — the field count, each field's type and
//! width, and that nothing trails the record — and every profile checks it the
//! same way, so the helpers live here rather than once per profile.
//!
//! ## Why decoding does not re-validate canonical form
//!
//! Records reach a profile's `decode` from [`crate::dir::Layer::records`] or
//! [`crate::LayerReader::next_record`], both of which have already validated
//! every byte against the deterministic profile — that is the framing layer's
//! job and it is not repeated here.

use minicbor::Decoder;

use crate::Error;

/// Open a record's outer array, insisting on a definite length of `expected`.
pub fn open(kind: &'static str, decoder: &mut Decoder<'_>, expected: u64) -> Result<(), Error> {
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
pub fn close(kind: &'static str, decoder: &Decoder<'_>, bytes: &[u8]) -> Result<(), Error> {
    let read = decoder.position();

    if read != bytes.len() {
        return Err(Error::malformed(
            kind,
            format!("{} trailing byte(s) after the record", bytes.len() - read),
        ));
    }

    Ok(())
}

pub fn uint(kind: &'static str, field: &str, decoder: &mut Decoder<'_>) -> Result<u64, Error> {
    decoder
        .u64()
        .map_err(|e| Error::malformed(kind, format!("{field}: {e}")))
}

pub fn text<'b>(
    kind: &'static str,
    field: &str,
    decoder: &mut Decoder<'b>,
) -> Result<&'b str, Error> {
    decoder
        .str()
        .map_err(|e| Error::malformed(kind, format!("{field}: {e}")))
}

pub fn blob<'b>(
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
/// Width is checked here rather than by a lossy conversion downstream: a
/// fixed-width key type typically converts from a slice by zero-padding or
/// truncating, so a wrong-width field would become a valid-looking key that no
/// lookup can ever reach.
pub fn fixed<const N: usize>(
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
