//! Reading a layer without holding it.
//!
//! [`crate::digest::read_blob`] decompresses a whole layer into a `Vec` and
//! hands it over. That is the right shape for a fixture and the wrong one for
//! the sizes a profile publishes: ADR-004's worked example gives a state shard
//! of 402,653,184 uncompressed bytes, and one mainnet epoch of blocks runs to
//! 0.5–1.5 GB. The write path never had this problem —
//! [`crate::digest::LayerWriter`] hashes, compresses and hashes again in one
//! pass with nothing buffered — so the asymmetry was one-sided and easy to
//! miss.
//!
//! [`LayerReader`] closes it: one pass over the compressed blob, records
//! yielded out of a bounded window, both digests established on the way past.
//!
//! ## Two bounds, and why one does not subsume the other
//!
//! - **Per record** ([`crate::frame::Limits`]) — a record is held whole, so its
//!   size is checked against a ceiling before the window grows for it.
//! - **Per layer** (the descriptor's `uncompressedSize`) — the record bound
//!   says nothing about how *many* records arrive. A blob whose descriptor
//!   claims 400 MB can stream 400 GB one small record at a time and never trip
//!   a per-record ceiling. `read_blob` refuses that blob with
//!   [`Error::DecompressedTooLarge`]; so does this, at the same threshold, for
//!   the same reason.
//!
//! ## Records arrive before the layer is proven
//!
//! A layer's `diffId` covers its whole byte string, so it cannot be confirmed
//! until the last record has gone past. Buying earlier verification would mean
//! a second pass over those 400 MB. This crate keeps the single pass and states
//! the consequence instead: **records are consumable before the layer is
//! proven, and only [`LayerReader::finish`] proves it.** A consumer must not
//! commit a checkpoint over records it has read until `finish` returns `Ok` —
//! which is what the Dolos restore pipeline already does, recording *completed*
//! layer diffIds in its progress file.

use std::io::{self, Read};

use sha2::{Digest as _, Sha256};

use crate::{
    digest::{Digest, LayerDigests, Tap},
    frame::{LayerHeader, Limits, RecordReader},
    inscription::LayerDescriptor,
    profile::Profile,
    Error,
};

/// The compressed-blob pipeline, read end to end:
/// file → [`Tap`] (blob digest) → zstd → [`Meter`] (diffId, ceiling) → records.
type Pipeline<R> = Meter<zstd::stream::read::Decoder<'static, io::BufReader<Tap<R>>>>;

/// A layer being read from a compressed blob, one record at a time.
///
/// Every claim its descriptor makes is checked, and each one as early as the
/// bytes allow: the header record's profile and kind on construction, the
/// decompression ceiling as the stream advances, the identity digest, the
/// uncompressed size and the record count in [`LayerReader::finish`].
pub struct LayerReader<R: Read> {
    records: RecordReader<Pipeline<R>>,
    header: LayerHeader,
    descriptor: LayerDescriptor,
}

impl<R: Read> LayerReader<R> {
    /// Open `source` — the compressed bytes of the layer `descriptor`
    /// describes — and read its header record.
    ///
    /// Fails immediately if the blob's own header names a different profile or
    /// kind than the document pointing at it: a layer that disagrees with its
    /// descriptor is refused before any of its content is handed out.
    pub fn new(
        source: R,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
        limits: Limits,
    ) -> Result<Self, Error> {
        let tap = Tap::new(source);
        let decoder = zstd::stream::read::Decoder::new(tap)?;
        let meter = Meter::new(decoder, descriptor.uncompressed_size);

        let mut records = RecordReader::with_limits(meter, limits);

        let header = match records.next_record() {
            Some(Ok(record)) => LayerHeader::decode(record)?,
            Some(Err(e)) => return Err(lift(e)),
            None => {
                return Err(Error::LayerMismatch {
                    kind: descriptor.kind.clone(),
                    reason: "layer is empty; every layer starts with a header record".to_owned(),
                })
            }
        };

        check_header(&header, profile, descriptor)?;

        Ok(Self {
            records,
            header,
            descriptor: descriptor.clone(),
        })
    }

    /// The layer's header record, parsed and checked against the descriptor.
    pub fn header(&self) -> &LayerHeader {
        &self.header
    }

    /// The next of the profile's content records, header excluded.
    ///
    /// Borrowed out of the reader's window, which is why this is not an
    /// [`Iterator`]: the borrow ends before the window is refilled. The record
    /// is canonical — that much is proven — but the *layer* is not proven until
    /// [`LayerReader::finish`] says so.
    pub fn next_record(&mut self) -> Option<Result<&[u8], Error>> {
        match self.records.next_record() {
            Some(Err(e)) => Some(Err(lift(e))),
            other => other,
        }
    }

    /// Finish the read: drain whatever is left, then confirm the layer.
    ///
    /// This is the point at which the layer becomes trustworthy. It reads to
    /// the end of the blob — the identity digest covers every byte, so there is
    /// no confirming it early — and checks the record count, the uncompressed
    /// size and the `diffId` against the descriptor. Anything a caller did with
    /// records before this returned `Ok` was done on faith.
    pub fn finish(mut self) -> Result<LayerDigests, Error> {
        loop {
            match self.records.next_record() {
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(lift(e)),
                None => break,
            }
        }

        // A caller that read a bad record and carried on anyway must not be
        // handed a confirmation. Most such layers fail the digest check below
        // — the bytes after the bad record were never read, so they never
        // reached the hasher — but a layer whose *last* record is malformed
        // hashes and sizes exactly as its descriptor claims. Refusing here is
        // what makes "`finish` confirms the layer" true rather than usually
        // true.
        if self.records.failed() {
            return Err(Error::LayerMismatch {
                kind: self.descriptor.kind.clone(),
                reason: "a record failed validation; the layer cannot be confirmed".to_owned(),
            });
        }

        let count = self.records.count();
        let meter = self.records.into_inner();

        let (decoder, diff_id, uncompressed_size) = meter.finish();

        // The decoder ran to EOF, so the tap has already seen the whole blob;
        // the drain is a safety net for a decoder that stops at a frame
        // boundary instead. `BufReader` may hold bytes it read ahead, and those
        // passed through the tap on the way in, so nothing is missed by
        // dropping it.
        let mut tap = decoder.finish().into_inner();
        io::copy(&mut tap, &mut io::sink())?;

        let (_, blob_digest, compressed_size) = tap.finish();

        let digests = LayerDigests {
            diff_id,
            blob_digest,
            uncompressed_size,
            compressed_size,
        };

        check_identity(&digests, &self.descriptor)?;
        check_record_count(count, &self.descriptor)?;

        Ok(digests)
    }
}

/// The layer header a blob carries has to agree with the document that points
/// at it, and with the profile the caller implements.
///
/// Shared by both read paths so a header cannot be acceptable to one and not
/// the other.
pub(crate) fn check_header(
    header: &LayerHeader,
    profile: &dyn Profile,
    descriptor: &LayerDescriptor,
) -> Result<(), Error> {
    if header.profile != profile.name() {
        return Err(Error::UnknownProfile {
            found: header.profile.clone(),
            expected: profile.name().to_owned(),
        });
    }

    if header.kind != descriptor.kind {
        return Err(Error::LayerMismatch {
            kind: descriptor.kind.clone(),
            reason: format!("header record names kind {:?}", header.kind),
        });
    }

    Ok(())
}

/// The identity digest and the size the descriptor claims, against what the
/// bytes turned out to be.
pub(crate) fn check_identity(
    digests: &LayerDigests,
    descriptor: &LayerDescriptor,
) -> Result<(), Error> {
    if digests.diff_id != descriptor.diff_id {
        return Err(Error::DigestMismatch {
            subject: format!("layer {:?}", descriptor.kind),
            expected: descriptor.diff_id.to_string(),
            actual: digests.diff_id.to_string(),
        });
    }

    if digests.uncompressed_size != descriptor.uncompressed_size {
        return Err(Error::LayerMismatch {
            kind: descriptor.kind.clone(),
            reason: format!(
                "descriptor claims {} uncompressed bytes, blob holds {}",
                descriptor.uncompressed_size, digests.uncompressed_size
            ),
        });
    }

    Ok(())
}

/// The record count, header record included.
pub(crate) fn check_record_count(records: u64, descriptor: &LayerDescriptor) -> Result<(), Error> {
    if records != descriptor.records {
        return Err(Error::LayerMismatch {
            kind: descriptor.kind.clone(),
            reason: format!(
                "descriptor claims {} records, blob holds {records}",
                descriptor.records
            ),
        });
    }

    Ok(())
}

/// Recover a protocol error that a pipeline stage had to smuggle through
/// [`io::Error`].
///
/// [`Read`] can only fail with an `io::Error`, but a decompression ceiling is a
/// protocol verdict, not an I/O fault. [`Meter`] boxes the real error inside
/// one; this takes it back out, so a caller matches on
/// [`Error::DecompressedTooLarge`] whichever path produced it.
fn lift(e: Error) -> Error {
    match e {
        Error::Io(io) if io.get_ref().is_some_and(|inner| inner.is::<Error>()) => *io
            .into_inner()
            .expect("checked by the guard")
            .downcast::<Error>()
            .expect("checked by the guard"),
        other => other,
    }
}

/// Hashes, counts and bounds the uncompressed side of a layer stream.
///
/// The ceiling is checked before the bytes are handed on, so it bounds what the
/// reader above is ever asked to hold — the same discipline
/// [`crate::digest::read_blob`] applies to its buffer.
struct Meter<R> {
    inner: R,
    hasher: Sha256,
    total: u64,
    limit: u64,
}

impl<R> Meter<R> {
    fn new(inner: R, limit: u64) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            total: 0,
            limit,
        }
    }

    fn finish(self) -> (R, Digest, u64) {
        (
            self.inner,
            Digest::from_bytes(self.hasher.finalize().into()),
            self.total,
        )
    }
}

impl<R: Read> Read for Meter<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;

        self.total += read as u64;

        if self.total > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                Error::DecompressedTooLarge { limit: self.limit },
            ));
        }

        self.hasher.update(&buf[..read]);

        Ok(read)
    }
}
