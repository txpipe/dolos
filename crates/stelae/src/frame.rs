//! Deterministic CBOR sequence framing.
//!
//! Every layer blob is, uncompressed, a CBOR sequence (RFC 8742): concatenated
//! CBOR data items with no outer container. The first item is the
//! protocol-owned [`LayerHeader`]; the rest are the profile's content records,
//! which this crate never interprets.
//!
//! The encoding profile is RFC 8949 §4.2.1 ("core deterministic encoding"),
//! narrowed by the spec to the closed set the format needs:
//!
//! - integers in shortest form,
//! - definite lengths only,
//! - map keys sorted bytewise by their encoded form, no duplicates,
//! - no floats, no tags, no `undefined`, no simple values beyond
//!   `false`/`true`/`null`,
//! - text strings valid UTF-8.
//!
//! Both directions enforce it. [`CanonicalCbor`] cannot be constructed from
//! bytes that violate the profile, so a record is validated before it is ever
//! written; [`SeqReader`] validates every record it yields. The read-side check
//! is not belt-and-braces: a layer's identity is the sha256 of these bytes, so
//! a producer that emits a non-canonical encoding would publish a diffId that
//! no independent re-encoding can reproduce. Rejecting it at the door is what
//! keeps "reproduce the digest" a decidable claim.

use std::io::Write;

use crate::{Error, LAYER_FORMAT_VERSION};

/// Maximum nesting depth accepted by the canonical-form scanner.
///
/// Layer records are flat by construction; the bound exists so that a hostile
/// blob cannot drive the scanner into a stack overflow.
pub const MAX_NESTING_DEPTH: usize = 64;

/// A CBOR data item known to be in the protocol's deterministic encoding.
///
/// The only way to obtain one is [`CanonicalCbor::new`] or [`encode`], both of
/// which validate. Everything downstream — records, layer headers, a profile's
/// opaque `scope` — carries the invariant in its type rather than by
/// convention.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CanonicalCbor(Vec<u8>);

impl CanonicalCbor {
    /// Validate `bytes` as exactly one canonical CBOR data item.
    ///
    /// Trailing bytes are an error: a record is one item, and silently ignoring
    /// a tail would let two different blobs claim the same logical content.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, Error> {
        let bytes = bytes.into();
        let len = scan_item(&bytes)?;

        if len != bytes.len() {
            return Err(Error::TrailingCbor {
                trailing: bytes.len() - len,
            });
        }

        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// A decoder positioned at the start of the item.
    ///
    /// Profiles use this to read their own records back. The protocol only ever
    /// uses it for the fields of the layer header.
    pub fn decoder(&self) -> minicbor::Decoder<'_> {
        minicbor::Decoder::new(&self.0)
    }
}

impl std::fmt::Debug for CanonicalCbor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CanonicalCbor(0x{})", hex::encode(&self.0))
    }
}

impl AsRef<[u8]> for CanonicalCbor {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Encode one canonical CBOR item with `minicbor`, validating the result.
///
/// `minicbor` already emits shortest-form integers and definite lengths, so the
/// validation is a guard rather than a fixer: it catches the two things the
/// encoder cannot know are wrong — an indefinite-length container opened
/// explicitly, and map keys written out of order.
///
/// ```
/// let record = stelae::frame::encode(|e| {
///     e.array(2)?.u64(42)?.str("hello")?;
///     Ok(())
/// })
/// .unwrap();
/// assert_eq!(record.as_bytes(), &[0x82, 0x18, 0x2a, 0x65, b'h', b'e', b'l', b'l', b'o']);
/// ```
pub fn encode<F>(f: F) -> Result<CanonicalCbor, Error>
where
    F: FnOnce(
        &mut minicbor::Encoder<Vec<u8>>,
    ) -> Result<(), minicbor::encode::Error<std::convert::Infallible>>,
{
    let mut encoder = minicbor::Encoder::new(Vec::new());
    f(&mut encoder).map_err(|e| Error::CborEncode(e.to_string()))?;
    CanonicalCbor::new(encoder.into_writer())
}

/// Validate the canonical CBOR data item at the start of `bytes` and return its
/// length in bytes. Trailing bytes are left for the caller — this is what makes
/// a CBOR *sequence* walkable.
pub fn scan_item(bytes: &[u8]) -> Result<usize, Error> {
    let mut scanner = Scanner { bytes, pos: 0 };
    scanner.item(0)?;
    Ok(scanner.pos)
}

struct Scanner<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn non_canonical(offset: usize, reason: impl Into<String>) -> Error {
        Error::NonCanonicalCbor {
            offset,
            reason: reason.into(),
        }
    }

    fn byte(&mut self) -> Result<u8, Error> {
        let b = *self.bytes.get(self.pos).ok_or(Error::TruncatedCbor {
            offset: self.pos,
            expected: 1,
        })?;

        self.pos += 1;

        Ok(b)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self.pos.checked_add(n).ok_or(Error::TruncatedCbor {
            offset: self.pos,
            expected: n,
        })?;

        let slice = self.bytes.get(self.pos..end).ok_or(Error::TruncatedCbor {
            offset: self.pos,
            expected: n,
        })?;

        self.pos = end;

        Ok(slice)
    }

    /// Read the argument of a head byte, rejecting every non-shortest encoding.
    fn argument(&mut self, offset: usize, ai: u8) -> Result<u64, Error> {
        match ai {
            0..=23 => Ok(u64::from(ai)),
            24 => {
                let v = u64::from(self.byte()?);
                if v < 24 {
                    return Err(Self::non_canonical(
                        offset,
                        format!("value {v} must be encoded in the head byte, not as uint8"),
                    ));
                }
                Ok(v)
            }
            25 => {
                let v = u64::from(u16::from_be_bytes(self.take(2)?.try_into().unwrap()));
                if v <= u64::from(u8::MAX) {
                    return Err(Self::non_canonical(
                        offset,
                        format!("value {v} must be encoded as uint8, not uint16"),
                    ));
                }
                Ok(v)
            }
            26 => {
                let v = u64::from(u32::from_be_bytes(self.take(4)?.try_into().unwrap()));
                if v <= u64::from(u16::MAX) {
                    return Err(Self::non_canonical(
                        offset,
                        format!("value {v} must be encoded as uint16, not uint32"),
                    ));
                }
                Ok(v)
            }
            27 => {
                let v = u64::from_be_bytes(self.take(8)?.try_into().unwrap());
                if v <= u64::from(u32::MAX) {
                    return Err(Self::non_canonical(
                        offset,
                        format!("value {v} must be encoded as uint32, not uint64"),
                    ));
                }
                Ok(v)
            }
            28..=30 => Err(Self::non_canonical(
                offset,
                format!("reserved additional information {ai}"),
            )),
            31 => Err(Self::non_canonical(
                offset,
                "indefinite lengths are excluded by the deterministic profile",
            )),
            _ => unreachable!("additional information is 5 bits"),
        }
    }

    fn item(&mut self, depth: usize) -> Result<(), Error> {
        if depth >= MAX_NESTING_DEPTH {
            return Err(Error::CborTooDeep {
                limit: MAX_NESTING_DEPTH,
            });
        }

        let offset = self.pos;
        let head = self.byte()?;
        let major = head >> 5;
        let ai = head & 0x1f;

        // Major type 7 carries simple values and floats; its additional
        // information is not a length, so it never goes through `argument`.
        if major == 7 {
            return match ai {
                20..=22 => Ok(()), // false, true, null
                23 => Err(Self::non_canonical(
                    offset,
                    "`undefined` is excluded by the deterministic profile",
                )),
                24 => Err(Self::non_canonical(
                    offset,
                    "simple values other than false/true/null are excluded",
                )),
                25..=27 => Err(Self::non_canonical(
                    offset,
                    "floating-point values are excluded by the deterministic profile",
                )),
                31 => Err(Self::non_canonical(
                    offset,
                    "`break` outside an indefinite-length item",
                )),
                _ => Err(Self::non_canonical(
                    offset,
                    format!("reserved simple value {ai}"),
                )),
            };
        }

        if major == 6 {
            return Err(Self::non_canonical(
                offset,
                "tags are excluded by the deterministic profile",
            ));
        }

        let arg = self.argument(offset, ai)?;

        match major {
            0 | 1 => Ok(()),
            2 => {
                let len = usize::try_from(arg).map_err(|_| Error::TruncatedCbor {
                    offset,
                    expected: usize::MAX,
                })?;
                self.take(len)?;
                Ok(())
            }
            3 => {
                let len = usize::try_from(arg).map_err(|_| Error::TruncatedCbor {
                    offset,
                    expected: usize::MAX,
                })?;
                let raw = self.take(len)?;
                std::str::from_utf8(raw)
                    .map_err(|e| Self::non_canonical(offset, format!("invalid utf-8: {e}")))?;
                Ok(())
            }
            4 => {
                for _ in 0..arg {
                    self.item(depth + 1)?;
                }
                Ok(())
            }
            5 => self.map(arg, depth),
            _ => unreachable!("major types 6 and 7 handled above"),
        }
    }

    fn map(&mut self, entries: u64, depth: usize) -> Result<(), Error> {
        let all = self.bytes;
        let mut previous: Option<&'a [u8]> = None;

        for _ in 0..entries {
            let key_start = self.pos;
            self.item(depth + 1)?;
            let key = &all[key_start..self.pos];

            if let Some(previous) = previous {
                match key.cmp(previous) {
                    std::cmp::Ordering::Less => {
                        return Err(Self::non_canonical(
                            key_start,
                            "map keys must be sorted bytewise by their encoded form",
                        ))
                    }
                    std::cmp::Ordering::Equal => {
                        return Err(Self::non_canonical(key_start, "duplicate map key"))
                    }
                    std::cmp::Ordering::Greater => {}
                }
            }

            previous = Some(key);
            self.item(depth + 1)?;
        }

        Ok(())
    }
}

/// Writes a CBOR sequence, counting the records it emits.
pub struct SeqWriter<W> {
    inner: W,
    count: u64,
}

impl<W: Write> SeqWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }

    /// Append one record. The [`CanonicalCbor`] type is the proof that it is in
    /// deterministic form, so nothing is re-checked here.
    pub fn write_record(&mut self, record: &CanonicalCbor) -> Result<(), Error> {
        self.inner.write_all(record.as_bytes())?;
        self.count += 1;
        Ok(())
    }

    /// Number of records written so far, header included.
    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

/// Walks a CBOR sequence, validating each item's canonical form.
///
/// Yields borrowed record bytes. Once an item fails validation the iterator is
/// exhausted — a sequence is not resynchronizable, and pretending otherwise
/// would hand the caller records from an arbitrary offset.
pub struct SeqReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    failed: bool,
}

impl<'a> SeqReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            offset: 0,
            failed: false,
        }
    }

    /// Byte offset of the next record.
    pub fn offset(&self) -> usize {
        self.offset
    }
}

impl<'a> Iterator for SeqReader<'a> {
    type Item = Result<&'a [u8], Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.offset >= self.bytes.len() {
            return None;
        }

        let rest = &self.bytes[self.offset..];

        match scan_item(rest) {
            Ok(len) => {
                let record = &rest[..len];
                self.offset += len;
                Some(Ok(record))
            }
            Err(e) => {
                self.failed = true;
                // Report the offset within the whole sequence, not the record.
                Some(Err(match e {
                    Error::NonCanonicalCbor { offset, reason } => Error::NonCanonicalCbor {
                        offset: self.offset + offset,
                        reason,
                    },
                    Error::TruncatedCbor { offset, expected } => Error::TruncatedCbor {
                        offset: self.offset + offset,
                        expected,
                    },
                    other => other,
                }))
            }
        }
    }
}

impl std::iter::FusedIterator for SeqReader<'_> {}

/// The first record of every layer, defined by the protocol so a blob stays
/// interpretable when detached from the registry that served it.
///
/// `[format_version, profile: tstr, kind: tstr, scope: any]`
///
/// `scope` is the profile's, and stays opaque: the protocol validates that it
/// is canonical CBOR and copies it, never reading inside.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerHeader {
    pub format_version: u64,
    pub profile: String,
    pub kind: String,
    pub scope: CanonicalCbor,
}

impl LayerHeader {
    /// A header at the format version this implementation writes.
    pub fn new(profile: impl Into<String>, kind: impl Into<String>, scope: CanonicalCbor) -> Self {
        Self {
            format_version: LAYER_FORMAT_VERSION,
            profile: profile.into(),
            kind: kind.into(),
            scope,
        }
    }

    pub fn encode(&self) -> Result<CanonicalCbor, Error> {
        let mut out = Vec::new();

        {
            let mut encoder = minicbor::Encoder::new(&mut out);
            encoder
                .array(4)
                .and_then(|e| e.u64(self.format_version))
                .and_then(|e| e.str(&self.profile))
                .and_then(|e| e.str(&self.kind))
                .map_err(|e| Error::CborEncode(e.to_string()))?;
        }

        out.extend_from_slice(self.scope.as_bytes());

        CanonicalCbor::new(out)
    }

    /// Parse a header record, failing closed on a format version this
    /// implementation does not implement.
    pub fn decode(record: &[u8]) -> Result<Self, Error> {
        let record = CanonicalCbor::new(record.to_vec())?;
        let bytes = record.as_bytes();
        let mut decoder = record.decoder();

        let fields = decoder
            .array()
            .map_err(|e| Error::MalformedHeader(e.to_string()))?
            .ok_or_else(|| Error::MalformedHeader("indefinite-length array".into()))?;

        if fields != 4 {
            return Err(Error::MalformedHeader(format!(
                "expected 4 fields, found {fields}"
            )));
        }

        let format_version = decoder
            .u64()
            .map_err(|e| Error::MalformedHeader(format!("format_version: {e}")))?;

        if format_version != LAYER_FORMAT_VERSION {
            return Err(Error::MalformedHeader(format!(
                "unsupported layer format version {format_version}; \
                 this implementation implements {LAYER_FORMAT_VERSION}"
            )));
        }

        let profile = decoder
            .str()
            .map_err(|e| Error::MalformedHeader(format!("profile: {e}")))?
            .to_owned();

        let kind = decoder
            .str()
            .map_err(|e| Error::MalformedHeader(format!("kind: {e}")))?
            .to_owned();

        let scope = CanonicalCbor::new(bytes[decoder.position()..].to_vec())?;

        Ok(Self {
            format_version,
            profile,
            kind,
            scope,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `minicbor` is the encoder the protocol hands to profiles. Unknown #1 of
    /// the implementation plan asks whether it emits RFC 8949 §4.2.1 canonical
    /// output by default; these are the boundary values where a non-canonical
    /// encoder would differ, checked against bytes taken from RFC 8949 §3 and
    /// Appendix A.
    #[test]
    fn minicbor_emits_shortest_form_integers() {
        let cases: &[(u64, &[u8])] = &[
            (0, &[0x00]),
            (1, &[0x01]),
            (10, &[0x0a]),
            (23, &[0x17]),
            (24, &[0x18, 0x18]),
            (25, &[0x18, 0x19]),
            (100, &[0x18, 0x64]),
            (255, &[0x18, 0xff]),
            (256, &[0x19, 0x01, 0x00]),
            (1000, &[0x19, 0x03, 0xe8]),
            (65535, &[0x19, 0xff, 0xff]),
            (65536, &[0x1a, 0x00, 0x01, 0x00, 0x00]),
            (1_000_000, &[0x1a, 0x00, 0x0f, 0x42, 0x40]),
            (4_294_967_295, &[0x1a, 0xff, 0xff, 0xff, 0xff]),
            (
                4_294_967_296,
                &[0x1b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                u64::MAX,
                &[0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            ),
        ];

        for (value, expected) in cases {
            let encoded = encode(|e| {
                e.u64(*value)?;
                Ok(())
            })
            .unwrap();
            assert_eq!(encoded.as_bytes(), *expected, "u64 {value}");
        }

        let negatives: &[(i64, &[u8])] = &[
            (-1, &[0x20]),
            (-10, &[0x29]),
            (-24, &[0x37]),
            (-25, &[0x38, 0x18]),
            (-100, &[0x38, 0x63]),
            (-1000, &[0x39, 0x03, 0xe7]),
        ];

        for (value, expected) in negatives {
            let encoded = encode(|e| {
                e.i64(*value)?;
                Ok(())
            })
            .unwrap();
            assert_eq!(encoded.as_bytes(), *expected, "i64 {value}");
        }
    }

    #[test]
    fn minicbor_emits_definite_lengths() {
        let encoded = encode(|e| {
            e.array(3)?.u64(1)?.u64(2)?.u64(3)?;
            Ok(())
        })
        .unwrap();
        assert_eq!(encoded.as_bytes(), &[0x83, 0x01, 0x02, 0x03]);

        let encoded = encode(|e| {
            e.bytes(&[0x01, 0x02, 0x03, 0x04])?;
            Ok(())
        })
        .unwrap();
        assert_eq!(encoded.as_bytes(), &[0x44, 0x01, 0x02, 0x03, 0x04]);

        let encoded = encode(|e| {
            e.map(2)?.str("a")?.u64(1)?.str("b")?.u64(2)?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            encoded.as_bytes(),
            &[0xa2, 0x61, b'a', 0x01, 0x61, b'b', 0x02]
        );
    }

    /// The encoder cannot know that an explicitly opened indefinite-length
    /// container is wrong; the validation in [`encode`] is what catches it.
    #[test]
    fn encode_rejects_indefinite_lengths_from_minicbor() {
        let err = encode(|e| {
            e.begin_array()?.u64(1)?.end()?;
            Ok(())
        })
        .unwrap_err();

        assert!(
            matches!(err, Error::NonCanonicalCbor { .. }),
            "expected non-canonical, got {err:?}"
        );
    }

    /// Likewise for map keys written out of order.
    #[test]
    fn encode_rejects_unsorted_map_keys() {
        let err = encode(|e| {
            e.map(2)?.str("b")?.u64(1)?.str("a")?.u64(2)?;
            Ok(())
        })
        .unwrap_err();

        assert!(
            matches!(&err, Error::NonCanonicalCbor { reason, .. } if reason.contains("sorted")),
            "expected sort violation, got {err:?}"
        );
    }

    #[test]
    fn rejects_non_shortest_integers() {
        // 0x18 0x00: value 0 written as uint8.
        let err = CanonicalCbor::new(vec![0x18, 0x00]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // 0x19 0x00 0x18: value 24 written as uint16.
        let err = CanonicalCbor::new(vec![0x19, 0x00, 0x18]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // 0x1a 0x00 0x00 0x01 0x00: value 256 written as uint32.
        let err = CanonicalCbor::new(vec![0x1a, 0x00, 0x00, 0x01, 0x00]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // 0x1b ... : value 65536 written as uint64.
        let err = CanonicalCbor::new(vec![0x1b, 0, 0, 0, 0, 0, 1, 0, 0]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // Non-shortest length prefix on a byte string.
        let err = CanonicalCbor::new(vec![0x58, 0x02, 0xaa, 0xbb]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");
    }

    #[test]
    fn rejects_indefinite_lengths() {
        // Indefinite array.
        let err = CanonicalCbor::new(vec![0x9f, 0x01, 0xff]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // Indefinite map.
        let err = CanonicalCbor::new(vec![0xbf, 0x61, b'a', 0x01, 0xff]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // Indefinite byte string.
        let err = CanonicalCbor::new(vec![0x5f, 0x41, 0xaa, 0xff]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // Indefinite text string.
        let err = CanonicalCbor::new(vec![0x7f, 0x61, b'a', 0xff]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");
    }

    #[test]
    fn rejects_floats_tags_and_undefined() {
        // f16 1.0
        let err = CanonicalCbor::new(vec![0xf9, 0x3c, 0x00]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // f32
        let err = CanonicalCbor::new(vec![0xfa, 0x47, 0xc3, 0x50, 0x00]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // f64
        let err = CanonicalCbor::new(vec![0xfb, 0x3f, 0xf1, 0, 0, 0, 0, 0, 0]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // tag 0 over a text string
        let err = CanonicalCbor::new(vec![0xc0, 0x61, b'a']).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // undefined
        let err = CanonicalCbor::new(vec![0xf7]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");
    }

    #[test]
    fn rejects_unsorted_and_duplicate_map_keys() {
        // {"b": 1, "a": 2} — out of order.
        let err = CanonicalCbor::new(vec![0xa2, 0x61, b'b', 0x01, 0x61, b'a', 0x02]).unwrap_err();
        assert!(
            matches!(&err, Error::NonCanonicalCbor { reason, .. } if reason.contains("sorted")),
            "{err:?}"
        );

        // {"a": 1, "a": 2} — duplicate.
        let err = CanonicalCbor::new(vec![0xa2, 0x61, b'a', 0x01, 0x61, b'a', 0x02]).unwrap_err();
        assert!(
            matches!(&err, Error::NonCanonicalCbor { reason, .. } if reason.contains("duplicate")),
            "{err:?}"
        );

        // Shorter keys sort first bytewise: {"a": 1, "aa": 2} is canonical,
        // {"aa": 1, "a": 2} is not.
        CanonicalCbor::new(vec![0xa2, 0x61, b'a', 0x01, 0x62, b'a', b'a', 0x02]).unwrap();
        let err =
            CanonicalCbor::new(vec![0xa2, 0x62, b'a', b'a', 0x01, 0x61, b'a', 0x02]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");
    }

    #[test]
    fn rejects_invalid_utf8_and_truncation() {
        // Text string claiming 1 byte of invalid UTF-8.
        let err = CanonicalCbor::new(vec![0x61, 0xff]).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");

        // Byte string claiming 4 bytes but carrying 2.
        let err = CanonicalCbor::new(vec![0x44, 0x01, 0x02]).unwrap_err();
        assert!(matches!(err, Error::TruncatedCbor { .. }), "{err:?}");

        // Array claiming 3 items but carrying 2.
        let err = CanonicalCbor::new(vec![0x83, 0x01, 0x02]).unwrap_err();
        assert!(matches!(err, Error::TruncatedCbor { .. }), "{err:?}");
    }

    #[test]
    fn rejects_trailing_bytes_in_a_single_item() {
        let err = CanonicalCbor::new(vec![0x01, 0x02]).unwrap_err();
        assert!(
            matches!(err, Error::TrailingCbor { trailing: 1 }),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_excessive_nesting() {
        // MAX_NESTING_DEPTH + 1 nested single-element arrays.
        let mut bytes = vec![0x81; MAX_NESTING_DEPTH + 1];
        bytes.push(0x00);

        let err = CanonicalCbor::new(bytes).unwrap_err();
        assert!(matches!(err, Error::CborTooDeep { .. }), "{err:?}");
    }

    #[test]
    fn sequence_roundtrip_is_byte_identical() {
        let records: Vec<CanonicalCbor> = (0..8u64)
            .map(|i| {
                encode(|e| {
                    e.array(3)?.u64(i)?.bytes(&[i as u8; 4])?.str("record")?;
                    Ok(())
                })
                .unwrap()
            })
            .collect();

        let mut writer = SeqWriter::new(Vec::new());
        for record in &records {
            writer.write_record(record).unwrap();
        }
        assert_eq!(writer.count(), 8);
        let written = writer.into_inner();

        let read: Vec<&[u8]> = SeqReader::new(&written)
            .collect::<Result<_, _>>()
            .expect("every record is canonical");
        assert_eq!(read.len(), 8);

        // write -> read -> write is byte-identical: nothing about a record's
        // encoding is lost or normalized on the way through.
        let mut rewriter = SeqWriter::new(Vec::new());
        for record in &read {
            rewriter
                .write_record(&CanonicalCbor::new(record.to_vec()).unwrap())
                .unwrap();
        }
        assert_eq!(rewriter.into_inner(), written);
    }

    #[test]
    fn sequence_reader_stops_at_the_first_bad_record() {
        let good = encode(|e| {
            e.u64(1)?;
            Ok(())
        })
        .unwrap();

        let mut bytes = good.as_bytes().to_vec();
        bytes.extend_from_slice(&[0x18, 0x00]); // non-shortest uint
        bytes.extend_from_slice(good.as_bytes());

        let mut reader = SeqReader::new(&bytes);
        assert_eq!(reader.next().unwrap().unwrap(), good.as_bytes());
        assert!(matches!(
            reader.next().unwrap(),
            Err(Error::NonCanonicalCbor { offset: 1, .. })
        ));
        assert!(reader.next().is_none(), "iterator must not resynchronize");
    }

    #[test]
    fn layer_header_roundtrip() {
        let scope = encode(|e| {
            e.array(2)?.u64(7)?.u64(42)?;
            Ok(())
        })
        .unwrap();

        let header = LayerHeader::new("dev.example.toy", "notes", scope.clone());
        let encoded = header.encode().unwrap();
        let decoded = LayerHeader::decode(encoded.as_bytes()).unwrap();

        assert_eq!(decoded, header);
        assert_eq!(decoded.scope, scope);
        assert_eq!(decoded.encode().unwrap(), encoded);
    }

    /// The header's `scope` slot takes any canonical CBOR item, and the
    /// protocol carries it through untouched. A profile using a map, an
    /// array or a bare integer must all survive.
    #[test]
    fn layer_header_scope_stays_opaque() {
        let scopes = [
            encode(|e| {
                e.u64(3)?;
                Ok(())
            })
            .unwrap(),
            encode(|e| {
                e.map(2)?.str("epoch")?.u64(550)?.str("shard")?.u64(0)?;
                Ok(())
            })
            .unwrap(),
            encode(|e| {
                e.array(0)?;
                Ok(())
            })
            .unwrap(),
            encode(|e| {
                e.null()?;
                Ok(())
            })
            .unwrap(),
        ];

        for scope in scopes {
            let header = LayerHeader::new("dev.example.toy", "notes", scope.clone());
            let decoded = LayerHeader::decode(header.encode().unwrap().as_bytes()).unwrap();
            assert_eq!(decoded.scope, scope);
        }
    }

    #[test]
    fn layer_header_rejects_a_future_format_version() {
        let scope = encode(|e| {
            e.u64(0)?;
            Ok(())
        })
        .unwrap();

        let record = encode(|e| {
            e.array(4)?
                .u64(2)?
                .str("dev.example.toy")?
                .str("notes")?
                .u64(0)?;
            Ok(())
        })
        .unwrap();

        let err = LayerHeader::decode(record.as_bytes()).unwrap_err();
        assert!(
            matches!(&err, Error::MalformedHeader(m) if m.contains("format version")),
            "{err:?}"
        );

        // Sanity: the same shape at the implemented version parses.
        let ok = LayerHeader::new("dev.example.toy", "notes", scope)
            .encode()
            .unwrap();
        LayerHeader::decode(ok.as_bytes()).unwrap();
    }

    #[test]
    fn layer_header_rejects_a_non_canonical_record() {
        // A header whose `scope` is an indefinite-length array.
        let mut bytes = vec![0x84, 0x01];
        bytes.extend_from_slice(&[0x6f]); // tstr(15)
        bytes.extend_from_slice(b"dev.example.toy");
        bytes.extend_from_slice(&[0x65]); // tstr(5)
        bytes.extend_from_slice(b"notes");
        bytes.extend_from_slice(&[0x9f, 0x01, 0xff]); // indefinite array

        let err = LayerHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, Error::NonCanonicalCbor { .. }), "{err:?}");
    }
}
