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
//!
//! ## Two readers, one validator
//!
//! A sequence is read either from bytes already in hand ([`SeqReader`], which
//! borrows out of the slice it was given) or from a stream ([`RecordReader`],
//! which refills a bounded window). They are two entry points, not two
//! implementations: both walk items with the same scanner, so a rule can
//! never hold on one path and lapse on the other. [`scan_item`] and
//! [`measure_item`] are that scanner's two exits — the first for callers that
//! hold the whole item, the second for callers that must decide whether an item
//! is worth holding at all.

use std::{
    io::{Read, Write},
    ops::Range,
};

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
    let mut scanner = Scanner::new(bytes);
    scanner.item(0)?;
    Ok(scanner.pos)
}

/// What scanning the start of `bytes` established about the item there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Measure {
    /// One complete, canonical item occupying the first `len` bytes.
    Complete { len: usize },
    /// A canonical *prefix* of an item: nothing seen so far violates the
    /// profile, but the item does not end within `bytes`.
    Incomplete {
        /// Lower bound, in bytes, on the whole item.
        ///
        /// Not an estimate and not a guess about what follows: it is what the
        /// length prefixes already read oblige the encoder to deliver — the
        /// bytes a string header claimed, plus one byte for each element of an
        /// enclosing array or map that has not been reached. It is therefore
        /// always greater than `bytes.len()`, which is what lets a refill loop
        /// make progress.
        required: u64,
    },
}

/// Validate the start of `bytes` as a canonical CBOR item, tolerating an item
/// that runs off the end.
///
/// The reason this exists rather than [`scan_item`] plus a derivation at the
/// call site: a streaming reader has to decide *whether to hold* a record
/// before it holds it, and [`Error::TruncatedCbor`] reports a local need — one
/// byte for a missing head, that chunk's `n` for a string body, a `usize::MAX`
/// sentinel for a length prefix beyond the platform's reach. Reconstructing the
/// record's total size from those would put length arithmetic in a second
/// place, which is exactly how two readers start disagreeing about what is
/// canonical.
///
/// A violation of the deterministic profile is still an error, not an
/// `Incomplete`: no quantity of further bytes rehabilitates a float, a tag or a
/// non-shortest integer.
///
/// ```
/// use stelae::frame::{measure_item, Measure};
///
/// // A byte string of four bytes, of which two arrived.
/// assert_eq!(
///     measure_item(&[0x44, 0x01, 0x02]).unwrap(),
///     Measure::Incomplete { required: 5 },
/// );
/// // The same item, complete, with the next record's first byte behind it.
/// assert_eq!(
///     measure_item(&[0x44, 0x01, 0x02, 0x03, 0x04, 0x00]).unwrap(),
///     Measure::Complete { len: 5 },
/// );
/// ```
pub fn measure_item(bytes: &[u8]) -> Result<Measure, Error> {
    let mut scanner = Scanner::new(bytes);

    match scanner.item(0) {
        Ok(()) => Ok(Measure::Complete { len: scanner.pos }),
        // Every `TruncatedCbor` this scanner raises means "the item needs bytes
        // that are not here", and `required` is what the raising site recorded.
        Err(Error::TruncatedCbor { .. }) => Ok(Measure::Incomplete {
            required: scanner.required,
        }),
        Err(e) => Err(e),
    }
}

struct Scanner<'a> {
    bytes: &'a [u8],
    pos: usize,
    /// Lower bound on the size of the item under scan. Only meaningful once a
    /// [`Error::TruncatedCbor`] has been raised; see [`Measure::Incomplete`].
    required: u64,
}

impl<'a> Scanner<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            pos: 0,
            required: 0,
        }
    }

    fn non_canonical(offset: usize, reason: impl Into<String>) -> Error {
        Error::NonCanonicalCbor {
            offset,
            reason: reason.into(),
        }
    }

    /// Report an item that runs off the end, recording what it still needs.
    ///
    /// The need is absolute (`offset` is measured from the start of the item),
    /// so the recorded bound survives the unwind through enclosing containers,
    /// which only add to it.
    fn truncated(&mut self, offset: usize, expected: usize) -> Error {
        self.required = (offset as u64).saturating_add(expected as u64);

        Error::TruncatedCbor { offset, expected }
    }

    /// Add what an enclosing container still owes to the bound, on the way out
    /// of a truncation. Every unread element is at least one byte.
    fn note_pending(&mut self, e: &Error, items: u64) {
        if matches!(e, Error::TruncatedCbor { .. }) {
            self.required = self.required.saturating_add(items);
        }
    }

    fn byte(&mut self) -> Result<u8, Error> {
        let Some(b) = self.bytes.get(self.pos).copied() else {
            return Err(self.truncated(self.pos, 1));
        };

        self.pos += 1;

        Ok(b)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let Some(end) = self.pos.checked_add(n) else {
            return Err(self.truncated(self.pos, n));
        };

        let Some(slice) = self.bytes.get(self.pos..end) else {
            return Err(self.truncated(self.pos, n));
        };

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
                let len = self.string_len(offset, arg)?;
                self.take(len)?;
                Ok(())
            }
            3 => {
                let len = self.string_len(offset, arg)?;
                let raw = self.take(len)?;
                std::str::from_utf8(raw)
                    .map_err(|e| Self::non_canonical(offset, format!("invalid utf-8: {e}")))?;
                Ok(())
            }
            4 => {
                // `remaining` counts the elements *after* the one being scanned,
                // so a truncation deep inside can be charged for the ones that
                // never got their turn.
                for remaining in (0..arg).rev() {
                    if let Err(e) = self.item(depth + 1) {
                        self.note_pending(&e, remaining);
                        return Err(e);
                    }
                }
                Ok(())
            }
            5 => self.map(arg, depth),
            _ => unreachable!("major types 6 and 7 handled above"),
        }
    }

    /// Length of a byte or text string, as a `usize` this platform can address.
    ///
    /// A prefix beyond `usize` is reported as truncation with a sentinel
    /// `expected`: the item is unreachable on this platform whatever follows.
    /// The recorded bound stays exact, so a caller enforcing a ceiling refuses
    /// it for its real size rather than for the sentinel.
    fn string_len(&mut self, offset: usize, arg: u64) -> Result<usize, Error> {
        usize::try_from(arg).map_err(|_| {
            self.required = (self.pos as u64).saturating_add(arg);

            Error::TruncatedCbor {
                offset,
                expected: usize::MAX,
            }
        })
    }

    fn map(&mut self, entries: u64, depth: usize) -> Result<(), Error> {
        let all = self.bytes;
        let mut previous: Option<&'a [u8]> = None;

        for remaining in (0..entries).rev() {
            let key_start = self.pos;

            if let Err(e) = self.item(depth + 1) {
                // Every entry left owes a key and a value; this one still owes
                // its value.
                self.note_pending(&e, remaining.saturating_mul(2).saturating_add(1));
                return Err(e);
            }

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

            if let Err(e) = self.item(depth + 1) {
                self.note_pending(&e, remaining.saturating_mul(2));
                return Err(e);
            }
        }

        Ok(())
    }
}

/// Writes a CBOR sequence, counting the records it emits and holding each to
/// the ceiling its reader will apply.
pub struct SeqWriter<W> {
    inner: W,
    count: u64,
    written: u64,
    max_record: usize,
}

impl<W: Write> SeqWriter<W> {
    /// A writer holding records to [`DEFAULT_MAX_RECORD`].
    pub fn new(inner: W) -> Self {
        Self::with_max_record(inner, DEFAULT_MAX_RECORD)
    }

    /// A writer holding records to `max_record`.
    ///
    /// `max_record` must be the ceiling the eventual reader is given, which is
    /// why the profile owns the number rather than each end picking its own —
    /// see [`crate::profile::Profile::max_record`].
    ///
    /// Floored at one byte to agree with [`Limits::normalized`], which floors a
    /// reader's ceiling the same way. Zero is not a meaningful ceiling for
    /// either end — the smallest CBOR item is one byte — so what matters about
    /// it is not which behaviour it selects but that both ends select the same
    /// one. A writer keeping a literal zero would refuse every record a reader
    /// at that ceiling goes on to accept, which is the disagreement this type
    /// exists to prevent.
    pub fn with_max_record(inner: W, max_record: usize) -> Self {
        Self {
            inner,
            count: 0,
            written: 0,
            max_record: max_record.max(1),
        }
    }

    /// Append one record. The [`CanonicalCbor`] type is the proof that it is in
    /// deterministic form, so nothing is re-checked here.
    ///
    /// Its *size* is checked, and that check is the writer's whole reason for
    /// knowing a ceiling. A record past it is refused here rather than written
    /// and discovered by whoever tries to read the layer back: a stele whose
    /// records no reader will accept is not a stele, and the publisher is the
    /// only party positioned to say so while the fix is still cheap. Reported
    /// in the record's offset within the layer, the same coordinate
    /// [`RecordReader`] fails in, so the two ends name the same byte.
    pub fn write_record(&mut self, record: &CanonicalCbor) -> Result<(), Error> {
        let bytes = record.as_bytes();

        if bytes.len() > self.max_record {
            return Err(Error::RecordTooLarge {
                offset: self.written as usize,
                required: bytes.len() as u64,
                limit: self.max_record,
            });
        }

        self.inner.write_all(bytes)?;
        self.count += 1;
        self.written += bytes.len() as u64;

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
                Some(Err(at_sequence_offset(e, self.offset)))
            }
        }
    }
}

impl std::iter::FusedIterator for SeqReader<'_> {}

/// Restate a record-local error in the coordinates of the whole sequence.
///
/// Both readers report positions the same way — an offset a reader hands back
/// is an offset into the layer, which is what a `diffId` covers and therefore
/// the only frame of reference two implementations can agree on.
fn at_sequence_offset(e: Error, base: usize) -> Error {
    match e {
        Error::NonCanonicalCbor { offset, reason } => Error::NonCanonicalCbor {
            offset: base.saturating_add(offset),
            reason,
        },
        Error::TruncatedCbor { offset, expected } => Error::TruncatedCbor {
            offset: base.saturating_add(offset),
            expected,
        },
        other => other,
    }
}

/// Largest single record [`RecordReader`] accepts by default: 16 MiB.
///
/// Two orders of magnitude above the largest record any profile plans to write
/// (a Cardano block is order 100 KB), and small enough that refusing a hostile
/// length prefix costs a bounded allocation. A profile whose records genuinely
/// approach this is telling its publisher something, not this crate: raise the
/// limit deliberately through [`Limits`], with the profile's own reason.
pub const DEFAULT_MAX_RECORD: usize = 16 * 1024 * 1024;

/// Refill window [`RecordReader`] starts with: 64 KiB, matching the buffers the
/// digest pipeline reads through.
pub const DEFAULT_WINDOW: usize = 64 * 1024;

/// What a streaming read is allowed to hold.
///
/// The bound the format actually needs is *one record fits in memory; a layer
/// does not*. Both fields exist to keep that promise checkable rather than
/// hoped for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest single record accepted. Checked against what a record's length
    /// prefixes claim *before* a buffer grows to hold it, so a corrupt or
    /// hostile prefix costs a comparison rather than an allocation.
    pub max_record: usize,
    /// Size the refill window starts at. It grows — never past `max_record` —
    /// only for a record that does not fit, and never shrinks back, so peak
    /// memory is `max(window, the largest record actually read)`.
    pub window: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_record: DEFAULT_MAX_RECORD,
            window: DEFAULT_WINDOW,
        }
    }
}

impl Limits {
    /// A window larger than the record ceiling would buy nothing — nothing that
    /// large is ever yielded — so it is clamped rather than refused.
    fn normalized(self) -> Self {
        Self {
            max_record: self.max_record.max(1),
            window: self.window.clamp(1, self.max_record.max(1)),
        }
    }
}

/// Walks a CBOR sequence arriving over a stream, validating each record's
/// canonical form and holding no more than one record at a time.
///
/// This is [`SeqReader`]'s guarantee list on a source too large to hold: every
/// record is validated against the deterministic profile before it is yielded,
/// and a bad record ends the walk rather than being skipped — a CBOR sequence
/// has no frame markers, so continuing past one would hand the caller records
/// from an arbitrary offset.
///
/// It adds the guarantee a stream needs and a slice does not: a record's size
/// is checked against [`Limits::max_record`] as soon as its length prefixes
/// claim it, and before the window grows. Nothing here trusts a length prefix
/// far enough to allocate for it.
///
/// Records are borrowed out of the window, so this is not an [`Iterator`]: the
/// borrow has to end before the window can be refilled. Callers loop on
/// [`RecordReader::next_record`].
///
/// ```
/// use stelae::frame::{encode, RecordReader, SeqWriter};
///
/// let mut writer = SeqWriter::new(Vec::new());
/// for i in 0..3u64 {
///     writer.write_record(&encode(|e| { e.u64(i)?; Ok(()) }).unwrap()).unwrap();
/// }
/// let sequence = writer.into_inner();
///
/// let mut reader = RecordReader::new(std::io::Cursor::new(&sequence));
/// let mut seen = 0;
/// while let Some(record) = reader.next_record() {
///     record.unwrap();
///     seen += 1;
/// }
/// assert_eq!(seen, 3);
/// ```
pub struct RecordReader<R> {
    source: R,
    /// The refill window. Its length *is* its capacity: everything from
    /// `filled` on is scratch space for the next read.
    window: Vec<u8>,
    /// Bytes of `window` that hold data read from the source.
    filled: usize,
    /// Where the next record starts within `window`.
    cursor: usize,
    limits: Limits,
    /// Offset of the next record within the sequence, for error reporting.
    offset: usize,
    count: u64,
    eof: bool,
    failed: bool,
}

impl<R: Read> RecordReader<R> {
    /// A reader with [`Limits::default`].
    pub fn new(source: R) -> Self {
        Self::with_limits(source, Limits::default())
    }

    pub fn with_limits(source: R, limits: Limits) -> Self {
        let limits = limits.normalized();

        Self {
            source,
            window: vec![0u8; limits.window],
            filled: 0,
            cursor: 0,
            limits,
            offset: 0,
            count: 0,
            eof: false,
            failed: false,
        }
    }

    /// The next record, or `None` at the end of the sequence.
    ///
    /// Once a record fails validation the reader is done: every later call
    /// returns `None`, the same way [`SeqReader`] stops.
    pub fn next_record(&mut self) -> Option<Result<&[u8], Error>> {
        if self.failed {
            return None;
        }

        match self.advance() {
            Ok(Some(record)) => {
                self.count += 1;
                Some(Ok(&self.window[record]))
            }
            Ok(None) => None,
            Err(e) => {
                self.failed = true;
                Some(Err(e))
            }
        }
    }

    /// Number of records yielded so far.
    ///
    /// A record that failed validation is not counted — it is not a record.
    /// Counting the failure instead is how a corrupt layer gets waved through
    /// by a descriptor written to match the inflated number.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Byte offset of the next record within the sequence.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Whether the walk ended because a record failed validation.
    pub fn failed(&self) -> bool {
        self.failed
    }

    /// The source, once the walk is over. [`crate::layer`] uses it to reach the
    /// digests the pipeline underneath accumulated.
    pub fn into_inner(self) -> R {
        self.source
    }

    /// Locate the next record in the window, refilling until it is whole.
    fn advance(&mut self) -> Result<Option<Range<usize>>, Error> {
        loop {
            let pending = &self.window[self.cursor..self.filled];

            if pending.is_empty() {
                if self.eof {
                    return Ok(None);
                }

                self.fill(0)?;
                continue;
            }

            match measure_item(pending) {
                Ok(Measure::Complete { len }) => {
                    // The window can hold more than one record, so a complete
                    // record is still checked: the ceiling is a promise about
                    // what a caller is handed, not only about what was
                    // allocated to get there.
                    self.check_ceiling(len as u64)?;

                    let record = self.cursor..self.cursor + len;
                    self.cursor += len;
                    self.offset += len;

                    return Ok(Some(record));
                }
                Ok(Measure::Incomplete { required }) => {
                    // Before the window grows, not after: this is the whole
                    // point of `measure_item` reporting a size.
                    self.check_ceiling(required)?;

                    if self.eof {
                        // Out of bytes with an unfinished record. Re-scan
                        // strictly so the error is the one the slice reader
                        // would have raised on the same bytes.
                        let e = scan_item(pending).expect_err("the item is incomplete");
                        return Err(at_sequence_offset(e, self.offset));
                    }

                    let required = usize::try_from(required)
                        .expect("checked against the ceiling, which is a usize");

                    self.fill(required)?;
                }
                Err(e) => return Err(at_sequence_offset(e, self.offset)),
            }
        }
    }

    fn check_ceiling(&self, required: u64) -> Result<(), Error> {
        if required > self.limits.max_record as u64 {
            return Err(Error::RecordTooLarge {
                offset: self.offset,
                required,
                limit: self.limits.max_record,
            });
        }

        Ok(())
    }

    /// Make room for a record of at least `required` bytes and read into it.
    ///
    /// Compacting first is what keeps the promise in [`Limits::window`]: the
    /// bytes of the record under construction move to the front, so the window
    /// holds one record plus whatever of the next one came along for the ride,
    /// never a growing tail of records already handed out.
    fn fill(&mut self, required: usize) -> Result<(), Error> {
        if self.cursor > 0 {
            self.window.copy_within(self.cursor..self.filled, 0);
            self.filled -= self.cursor;
            self.cursor = 0;
        }

        if self.filled == self.window.len() {
            // A full window and still no record: the record is at least one
            // byte larger than everything held, which is the bound to check.
            self.check_ceiling(self.filled as u64 + 1)?;

            let grown = self
                .window
                .len()
                .saturating_mul(2)
                .clamp(required.max(self.filled + 1), self.limits.max_record);

            self.window.resize(grown, 0);
        } else if required > self.window.len() {
            self.window.resize(required.min(self.limits.max_record), 0);
        }

        let read = self.source.read(&mut self.window[self.filled..])?;

        if read == 0 {
            self.eof = true;
        } else {
            self.filled += read;
        }

        Ok(())
    }
}

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

    /// The writer refuses what the reader would refuse, at the same ceiling.
    ///
    /// Regression for a stele that published cleanly and restored nowhere: the
    /// reader held records to 16 MiB, nothing held the writer to anything, and
    /// a profile with a 24 MiB record produced 928 layers, a valid manifest and
    /// an artifact whose first oversized record ended every restore of it.
    #[test]
    fn writer_refuses_a_record_past_its_ceiling() {
        let big = encode(|e| {
            e.bytes(&[0u8; 4096])?;
            Ok(())
        })
        .unwrap();

        let mut writer = SeqWriter::with_max_record(Vec::new(), 1024);
        let err = writer.write_record(&big).unwrap_err();

        assert!(
            matches!(err, Error::RecordTooLarge { limit: 1024, .. }),
            "{err:?}"
        );

        // Refused before the write, so nothing partial reached the sink.
        assert_eq!(writer.count(), 0);
        assert!(writer.into_inner().is_empty());
    }

    /// The offset a refusal names is the record's offset in the layer, so a
    /// publisher and a reader failing on the same record report the same byte.
    #[test]
    fn writer_refusal_names_the_offset_the_reader_would() {
        let small = encode(|e| {
            e.bytes(&[0u8; 8])?;
            Ok(())
        })
        .unwrap();
        let big = encode(|e| {
            e.bytes(&[0u8; 4096])?;
            Ok(())
        })
        .unwrap();

        let mut writer = SeqWriter::with_max_record(Vec::new(), 1024);
        writer.write_record(&small).unwrap();
        writer.write_record(&small).unwrap();

        let err = writer.write_record(&big).unwrap_err();
        let Error::RecordTooLarge { offset, .. } = err else {
            panic!("{err:?}");
        };

        assert_eq!(offset, small.as_bytes().len() * 2);
    }

    /// At any ceiling, the writer accepts exactly what a reader at that same
    /// ceiling will.
    ///
    /// The guarantee the profile's number buys is that one value binds both
    /// ends; a ceiling where the two disagree is that guarantee with a hole in
    /// it, and the hole does not have to be a reachable value to be worth
    /// closing. Zero is the only such value, because [`Limits::normalized`]
    /// floors a reader at one byte: a writer keeping a literal zero refuses the
    /// one-byte records that reader accepts.
    #[test]
    fn the_writer_accepts_exactly_what_the_reader_will() {
        let one_byte = encode(|e| {
            e.u64(0)?;
            Ok(())
        })
        .unwrap();
        assert_eq!(one_byte.as_bytes().len(), 1, "smallest possible record");

        let larger = encode(|e| {
            e.bytes(&[0xab; 1000])?;
            Ok(())
        })
        .unwrap();

        let ceilings = [0, 1, 2, larger.as_bytes().len(), DEFAULT_MAX_RECORD];

        for ceiling in ceilings {
            for record in [&one_byte, &larger] {
                let mut writer = SeqWriter::with_max_record(Vec::new(), ceiling);
                let writer_took = writer.write_record(record).is_ok();

                // The reader needs a sequence to walk, so the record is laid
                // down by a writer with a ceiling that refuses nothing.
                let mut permissive = SeqWriter::with_max_record(Vec::new(), usize::MAX);
                permissive.write_record(record).unwrap();
                let sequence = permissive.into_inner();

                let mut reader = RecordReader::with_limits(
                    std::io::Cursor::new(&sequence),
                    Limits {
                        max_record: ceiling,
                        window: 64,
                    },
                );
                let reader_took = matches!(reader.next_record(), Some(Ok(_)));

                assert_eq!(
                    writer_took,
                    reader_took,
                    "ceiling {ceiling} disagrees on a {}-byte record",
                    record.as_bytes().len()
                );
            }
        }
    }

    /// A profile that raises its ceiling can write what the default refuses,
    /// which is the whole point of the number being the profile's.
    #[test]
    fn a_raised_ceiling_admits_a_record_the_default_would_refuse() {
        let record = encode(|e| {
            e.bytes(&[0u8; DEFAULT_MAX_RECORD + 1])?;
            Ok(())
        })
        .unwrap();

        assert!(SeqWriter::new(Vec::new()).write_record(&record).is_err());

        let mut writer = SeqWriter::with_max_record(Vec::new(), DEFAULT_MAX_RECORD * 4);
        writer
            .write_record(&record)
            .expect("within the raised ceiling");

        assert_eq!(writer.count(), 1);
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

    /// Every input the deterministic profile refuses, in the encodings a
    /// hostile or buggy producer would actually emit.
    ///
    /// The list is the crate's rejection corpus: both readers are run over it
    /// below, and the point is not that each one refuses — it is that they
    /// refuse *identically*. Two readers that disagree about what is canonical
    /// are a determinism bug wearing a compatibility costume: the same bytes
    /// would restore on one path and fail on the other, and whichever produced
    /// the layer would have published a `diffId` nobody else reproduces.
    const NON_CANONICAL: &[(&str, &[u8])] = &[
        ("value 0 as uint8", &[0x18, 0x00]),
        ("value 24 as uint16", &[0x19, 0x00, 0x18]),
        ("value 256 as uint32", &[0x1a, 0x00, 0x00, 0x01, 0x00]),
        ("value 65536 as uint64", &[0x1b, 0, 0, 0, 0, 0, 1, 0, 0]),
        ("non-shortest byte-string length", &[0x58, 0x02, 0xaa, 0xbb]),
        ("indefinite array", &[0x9f, 0x01, 0xff]),
        ("indefinite map", &[0xbf, 0x61, b'a', 0x01, 0xff]),
        ("indefinite byte string", &[0x5f, 0x41, 0xaa, 0xff]),
        ("indefinite text string", &[0x7f, 0x61, b'a', 0xff]),
        ("half-precision float", &[0xf9, 0x3c, 0x00]),
        ("single-precision float", &[0xfa, 0x47, 0xc3, 0x50, 0x00]),
        (
            "double-precision float",
            &[0xfb, 0x3f, 0xf1, 0, 0, 0, 0, 0, 0],
        ),
        ("tag 0", &[0xc0, 0x61, b'a']),
        ("undefined", &[0xf7]),
        ("simple value 255", &[0xf8, 0xff]),
        ("break outside an indefinite item", &[0xff]),
        ("reserved additional information", &[0x1c]),
        (
            "unsorted map keys",
            &[0xa2, 0x61, b'b', 0x01, 0x61, b'a', 0x02],
        ),
        (
            "duplicate map key",
            &[0xa2, 0x61, b'a', 0x01, 0x61, b'a', 0x02],
        ),
        (
            "map keys unsorted by length",
            &[0xa2, 0x62, b'a', b'a', 0x01, 0x61, b'a', 0x02],
        ),
        ("invalid utf-8", &[0x61, 0xff]),
        ("byte string cut short", &[0x44, 0x01, 0x02]),
        ("array cut short", &[0x83, 0x01, 0x02]),
    ];

    fn good_record() -> CanonicalCbor {
        encode(|e| {
            e.array(2)?.u64(7)?.str("good")?;
            Ok(())
        })
        .unwrap()
    }

    /// Drain a [`RecordReader`], copying records out so the result can be
    /// compared against the slice reader's borrowed ones.
    fn drain<R: Read>(reader: &mut RecordReader<R>) -> (Vec<Vec<u8>>, Option<Error>) {
        let mut records = Vec::new();

        while let Some(next) = reader.next_record() {
            match next {
                Ok(record) => records.push(record.to_vec()),
                Err(e) => return (records, Some(e)),
            }
        }

        (records, None)
    }

    /// The bound reported for an unfinished item is a real lower bound on the
    /// whole item, derived from what the encoder has already committed to —
    /// never an estimate, and never less than what is already in hand, or a
    /// refill loop would spin.
    #[test]
    fn measure_reports_what_an_unfinished_item_still_needs() {
        let cases: &[(&[u8], u64)] = &[
            // Nothing at all: a head byte, at least.
            (&[], 1),
            // bytes(4) with two of them: head + 4.
            (&[0x44, 0x01, 0x02], 5),
            // Three-element array, nothing inside: head + one byte each.
            (&[0x83], 4),
            // ... and with two elements present, the third is still owed.
            (&[0x83, 0x01, 0x02], 4),
            // A map with one entry present owes both halves of the next.
            (&[0xa2, 0x61, b'a', 0x01], 6),
            // Nested: the outer array owes its second element, the inner byte
            // string owes its body.
            (&[0x82, 0x43, 0xaa], 6),
            // A text string whose length prefix is larger than any layer:
            // reported at full size, which is what a ceiling check needs.
            (
                &[0x7b, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00],
                1_099_511_627_785,
            ),
        ];

        for (bytes, required) in cases {
            assert_eq!(
                measure_item(bytes).unwrap(),
                Measure::Incomplete {
                    required: *required
                },
                "0x{}",
                hex::encode(bytes)
            );

            assert!(
                *required > bytes.len() as u64,
                "the bound must exceed what is in hand, or a refill loop stalls"
            );
        }
    }

    /// A violation is a verdict, not a request for more bytes: no quantity of
    /// further input rehabilitates a float or a tag, so `measure_item` reports
    /// those as errors rather than as an unfinished item.
    #[test]
    fn measure_refuses_rather_than_waits_on_non_canonical_input() {
        for (name, bytes) in NON_CANONICAL {
            let measured = measure_item(bytes);

            match measured {
                // Truncation is the one honest "incomplete" in the corpus.
                Ok(Measure::Incomplete { .. }) => assert!(
                    name.contains("cut short"),
                    "{name} must not be reported as merely unfinished"
                ),
                Ok(Measure::Complete { .. }) => panic!("{name} must not measure as complete"),
                Err(_) => {}
            }
        }
    }

    /// The guarantee the whole rejection corpus exists for: one scanner, two
    /// entry points, one verdict.
    #[test]
    fn both_readers_refuse_the_same_bytes_the_same_way() {
        let good = good_record();

        for (name, bad) in NON_CANONICAL {
            let mut bytes = good.as_bytes().to_vec();
            bytes.extend_from_slice(bad);

            let mut slice = SeqReader::new(&bytes);
            assert_eq!(slice.next().unwrap().unwrap(), good.as_bytes(), "{name}");
            let slice_error = slice.next().unwrap().unwrap_err();
            assert!(slice.next().is_none(), "{name}: must not resynchronize");

            // Window sizes across the interesting boundaries: one byte at a
            // time, mid-record, and comfortably larger than the whole input.
            for window in [1, 3, 4096] {
                let mut reader = RecordReader::with_limits(
                    std::io::Cursor::new(&bytes),
                    Limits {
                        window,
                        ..Limits::default()
                    },
                );

                let (records, error) = drain(&mut reader);

                assert_eq!(records, vec![good.as_bytes().to_vec()], "{name} @ {window}");
                assert_eq!(
                    reader.count(),
                    1,
                    "{name} @ {window}: a failure is not a record"
                );

                let error = error.unwrap_or_else(|| panic!("{name} @ {window}: expected an error"));

                // Same variant, same offset, same reason — compared through the
                // rendered message so a divergence in any of the three shows up
                // as a diff rather than as a silently weaker assertion.
                assert_eq!(
                    error.to_string(),
                    slice_error.to_string(),
                    "{name} @ {window}"
                );

                assert!(
                    reader.next_record().is_none(),
                    "{name} @ {window}: must not resynchronize"
                );
            }
        }
    }

    /// Nesting is bounded for both readers by the same constant — built here
    /// rather than in the corpus because the input is generated.
    #[test]
    fn both_readers_bound_nesting() {
        let mut bytes = vec![0x81; MAX_NESTING_DEPTH + 1];
        bytes.push(0x00);

        let slice_error = SeqReader::new(&bytes).next().unwrap().unwrap_err();
        assert!(
            matches!(slice_error, Error::CborTooDeep { .. }),
            "{slice_error:?}"
        );

        let mut reader = RecordReader::new(std::io::Cursor::new(&bytes));
        let error = reader.next_record().unwrap().unwrap_err();
        assert_eq!(error.to_string(), slice_error.to_string());
    }

    /// Records that straddle every refill boundary still come back whole and in
    /// order, whatever the window size — including a window far smaller than a
    /// single record, which is the case the ceiling has to distinguish from a
    /// record that is genuinely too big.
    #[test]
    fn records_survive_every_refill_boundary() {
        let records: Vec<CanonicalCbor> = (0..64u64)
            .map(|i| {
                encode(|e| {
                    // Sizes crossing the 24/256 encoding boundaries, so records
                    // of several byte lengths land at every window offset.
                    e.array(2)?.u64(i)?.bytes(&vec![i as u8; i as usize * 7])?;
                    Ok(())
                })
                .unwrap()
            })
            .collect();

        let mut writer = SeqWriter::new(Vec::new());
        for record in &records {
            writer.write_record(record).unwrap();
        }
        let sequence = writer.into_inner();

        let expected: Vec<Vec<u8>> = records.iter().map(|r| r.as_bytes().to_vec()).collect();

        for window in [1, 2, 3, 17, 64, 129, 4096, 1 << 20] {
            let mut reader = RecordReader::with_limits(
                std::io::Cursor::new(&sequence),
                Limits {
                    window,
                    ..Limits::default()
                },
            );

            let (read, error) = drain(&mut reader);

            assert!(error.is_none(), "window {window}: {error:?}");
            assert_eq!(read, expected, "window {window}");
            assert_eq!(reader.count(), 64);
            assert_eq!(reader.offset(), sequence.len());
        }
    }

    /// A length prefix is a claim, not an instruction. The reader checks it
    /// against the ceiling before the window grows, so a record claiming a
    /// terabyte costs a comparison — the attack surface the buffered path never
    /// had, because there the bytes had to exist before they could be claimed.
    #[test]
    fn an_oversized_length_prefix_is_refused_before_it_is_allocated_for() {
        // bytes(2^40): a header that arrives in nine bytes and asks for a
        // terabyte.
        let bytes: &[u8] = &[0x5b, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00];

        let mut reader = RecordReader::new(std::io::Cursor::new(bytes));
        let error = reader.next_record().unwrap().unwrap_err();

        assert!(
            matches!(
                error,
                Error::RecordTooLarge {
                    offset: 0,
                    required: 1_099_511_627_785,
                    limit: DEFAULT_MAX_RECORD,
                }
            ),
            "{error:?}"
        );

        // The window is still the window: nothing was allocated on the strength
        // of the prefix.
        assert!(reader.next_record().is_none());
    }

    /// The ceiling is checked against the record, not against the window, and
    /// it is exact at the boundary — one byte either side decides it.
    #[test]
    fn the_record_ceiling_is_exact() {
        let record = encode(|e| {
            e.bytes(&[0xab; 1000])?;
            Ok(())
        })
        .unwrap();

        let len = record.len();
        assert_eq!(len, 1003, "3-byte head over a 1000-byte body");

        let mut writer = SeqWriter::new(Vec::new());
        writer.write_record(&record).unwrap();
        let sequence = writer.into_inner();

        // Exactly at the ceiling, from a window a fraction of the size.
        let mut reader = RecordReader::with_limits(
            std::io::Cursor::new(&sequence),
            Limits {
                max_record: len,
                window: 16,
            },
        );
        assert_eq!(reader.next_record().unwrap().unwrap(), record.as_bytes());
        assert!(reader.next_record().is_none());

        // One byte under it, and the same record is refused — from the length
        // prefix, before the window ever grew to 1000 bytes.
        let mut reader = RecordReader::with_limits(
            std::io::Cursor::new(&sequence),
            Limits {
                max_record: len - 1,
                window: 16,
            },
        );
        let error = reader.next_record().unwrap().unwrap_err();
        assert!(
            matches!(
                error,
                Error::RecordTooLarge {
                    offset: 0,
                    required: 1003,
                    limit: 1002
                }
            ),
            "{error:?}"
        );

        // And a record that fits is not refused for the company it keeps: a
        // window large enough to hold two of them still yields them one at a
        // time.
        let mut writer = SeqWriter::new(Vec::new());
        writer.write_record(&record).unwrap();
        writer.write_record(&record).unwrap();
        let pair = writer.into_inner();

        let mut reader = RecordReader::with_limits(
            std::io::Cursor::new(&pair),
            Limits {
                max_record: len,
                window: 4096,
            },
        );
        let (read, error) = drain(&mut reader);
        assert!(error.is_none(), "{error:?}");
        assert_eq!(read.len(), 2);
    }

    /// A sequence that ends mid-record is truncation, not an end: the reader
    /// says so rather than quietly reporting one record fewer.
    #[test]
    fn a_sequence_that_ends_mid_record_is_reported() {
        let record = good_record();
        let mut bytes = record.as_bytes().to_vec();
        bytes.extend_from_slice(&record.as_bytes()[..2]);

        let mut reader = RecordReader::new(std::io::Cursor::new(&bytes));

        assert_eq!(reader.next_record().unwrap().unwrap(), record.as_bytes());

        let error = reader.next_record().unwrap().unwrap_err();
        assert!(matches!(error, Error::TruncatedCbor { .. }), "{error:?}");

        // Reported in the coordinates of the sequence, as the slice reader
        // would have.
        assert_eq!(
            error.to_string(),
            SeqReader::new(&bytes)
                .nth(1)
                .unwrap()
                .unwrap_err()
                .to_string()
        );
    }

    /// An empty sequence is an empty walk, not an error.
    #[test]
    fn an_empty_sequence_yields_nothing() {
        let mut reader = RecordReader::new(std::io::Cursor::new(Vec::new()));

        assert!(reader.next_record().is_none());
        assert_eq!(reader.count(), 0);
        assert_eq!(reader.offset(), 0);
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
