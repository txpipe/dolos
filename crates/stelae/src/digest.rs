//! Digests and the layer compression pipeline.
//!
//! A layer has two digests and they answer different questions:
//!
//! - **`diffId`** — sha256 over the *uncompressed* CBOR sequence. This is
//!   identity. It is what the inscription lists, what independent publishers
//!   reproduce, and what a signature ultimately covers.
//! - **blob digest** — sha256 over the *zstd-compressed* bytes. This is
//!   transport. It is what an OCI registry addresses the blob by, and it is not
//!   stable across zstd versions or levels, which is precisely why it cannot be
//!   the identity anchor (ADR-004, "Determinism is anchored on uncompressed
//!   bytes").
//!
//! [`LayerWriter`] produces both in a single pass over the data: bytes are
//! hashed on the way in, compressed, and hashed again on the way out. Nothing
//! is buffered, so a layer of any size costs one sequential scan.

use std::io::{Read, Write};

use sha2::{Digest as _, Sha256};

use crate::Error;

/// A sha256 digest, rendered as `sha256:<64 hex chars>` wherever it is written
/// down — the OCI spelling, so inscription values paste straight into registry
/// tooling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    pub const ALGORITHM: &'static str = "sha256";

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// sha256 of a byte slice.
    pub fn compute(data: impl AsRef<[u8]>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data.as_ref());
        Self(hasher.finalize().into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The bare hex encoding, without the `sha256:` prefix. This is the file
    /// name an OCI image layout uses under `blobs/sha256/`.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl std::fmt::Display for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", Self::ALGORITHM, hex::encode(self.0))
    }
}

impl std::str::FromStr for Digest {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = |reason: &str| Error::InvalidDigest {
            value: s.to_owned(),
            reason: reason.to_owned(),
        };

        let (algorithm, hex_digits) = s
            .split_once(':')
            .ok_or_else(|| invalid("expected `<algorithm>:<hex>`"))?;

        if algorithm != Self::ALGORITHM {
            return Err(invalid("only sha256 is defined by this protocol version"));
        }

        if hex_digits.len() != 64 {
            return Err(invalid("expected 64 hex digits"));
        }

        if hex_digits
            .bytes()
            .any(|b| !b.is_ascii_digit() && !(b'a'..=b'f').contains(&b))
        {
            return Err(invalid("expected lowercase hex digits"));
        }

        let mut bytes = [0u8; 32];
        hex::decode_to_slice(hex_digits, &mut bytes).map_err(|e| invalid(&e.to_string()))?;

        Ok(Self(bytes))
    }
}

impl serde::Serialize for Digest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for Digest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

/// What one pass over a layer blob establishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayerDigests {
    /// sha256 over the uncompressed CBOR sequence — the layer's identity.
    pub diff_id: Digest,
    /// sha256 over the compressed bytes — how a registry addresses the blob.
    pub blob_digest: Digest,
    pub uncompressed_size: u64,
    pub compressed_size: u64,
}

/// A writer that hashes what it is given, compresses it, and hashes the result.
///
/// Write the uncompressed CBOR sequence into it; [`LayerWriter::finish`]
/// returns the sink together with both digests and both sizes.
pub struct LayerWriter<W: Write> {
    encoder: zstd::stream::write::Encoder<'static, Tap<W>>,
    hasher: Sha256,
    uncompressed_size: u64,
}

impl<W: Write> LayerWriter<W> {
    /// Wrap `sink`, compressing at `level`.
    ///
    /// The level is transport policy: it changes the blob digest and the bytes
    /// on the wire, never the `diffId`. It is recorded in the inscription's
    /// `compression` so publishers converge on identical blobs in practice, but
    /// verification never depends on that.
    pub fn new(sink: W, level: i32) -> Result<Self, Error> {
        Ok(Self {
            encoder: zstd::stream::write::Encoder::new(Tap::new(sink), level)?,
            hasher: Sha256::new(),
            uncompressed_size: 0,
        })
    }

    pub fn finish(self) -> Result<(W, LayerDigests), Error> {
        let diff_id = Digest(self.hasher.finalize().into());
        let tap = self.encoder.finish()?;
        let (sink, blob_digest, compressed_size) = tap.finish();

        Ok((
            sink,
            LayerDigests {
                diff_id,
                blob_digest,
                uncompressed_size: self.uncompressed_size,
                compressed_size,
            },
        ))
    }
}

impl<W: Write> Write for LayerWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.encoder.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.uncompressed_size += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.encoder.flush()
    }
}

/// sha256 and byte count of a stream, with no interpretation of its content.
///
/// Used to check a stored blob against the digest it is named by, which must
/// hold whether or not the blob turns out to be a readable layer.
pub fn digest_reader<R: Read>(mut source: R) -> Result<(Digest, u64), Error> {
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut total = 0u64;

    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total += read as u64;
    }

    Ok((Digest(hasher.finalize().into()), total))
}

/// Read a compressed layer blob, computing both digests without holding the
/// uncompressed content.
pub fn scan_blob<R: Read>(source: R) -> Result<LayerDigests, Error> {
    digest_blob(source, &mut std::io::sink())
}

/// Read a compressed layer blob, returning its uncompressed bytes and both
/// digests.
///
/// The uncompressed content is buffered, so this is for callers that intend to
/// walk the records anyway. Verification-only callers want [`scan_blob`].
pub fn read_blob<R: Read>(source: R) -> Result<(Vec<u8>, LayerDigests), Error> {
    let mut content = Vec::new();
    let digests = digest_blob(source, &mut content)?;
    Ok((content, digests))
}

fn digest_blob<R: Read, W: Write>(source: R, sink: &mut W) -> Result<LayerDigests, Error> {
    let mut tap = Tap::new(source);
    let mut uncompressed_size = 0u64;
    let mut hasher = Sha256::new();

    {
        let mut decoder = zstd::stream::read::Decoder::new(&mut tap)?;
        let mut buffer = [0u8; 64 * 1024];

        loop {
            let read = decoder.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            sink.write_all(&buffer[..read])?;
            uncompressed_size += read as u64;
        }
    }

    // Decoding runs to EOF, so the tap has already seen the whole blob; the
    // drain is a safety net in case a future decoder stops at a frame boundary
    // instead. Either way the blob digest must cover every byte of the file, not
    // only the part decompression happened to need.
    std::io::copy(&mut tap, &mut std::io::sink())?;

    let (_, blob_digest, compressed_size) = tap.finish();

    Ok(LayerDigests {
        diff_id: Digest(hasher.finalize().into()),
        blob_digest,
        uncompressed_size,
        compressed_size,
    })
}

/// Hashes and counts every byte that passes through, in either direction.
struct Tap<T> {
    inner: T,
    hasher: Sha256,
    bytes: u64,
}

impl<T> Tap<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(self) -> (T, Digest, u64) {
        (
            self.inner,
            Digest(self.hasher.finalize().into()),
            self.bytes,
        )
    }
}

impl<W: Write> Write for Tap<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<R: Read> Read for Tap<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        self.bytes += read as u64;
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{encode, SeqWriter};

    fn sample_sequence() -> Vec<u8> {
        let mut writer = SeqWriter::new(Vec::new());

        for i in 0..256u64 {
            let record = encode(|e| {
                e.array(2)?
                    .u64(i)?
                    .str("a repeated payload that compresses")?;
                Ok(())
            })
            .unwrap();
            writer.write_record(&record).unwrap();
        }

        writer.into_inner()
    }

    fn write_layer(content: &[u8], level: i32) -> (Vec<u8>, LayerDigests) {
        let mut writer = LayerWriter::new(Vec::new(), level).unwrap();
        writer.write_all(content).unwrap();
        writer.finish().unwrap()
    }

    /// Known-answer check against the sha256 of the empty string, so the digest
    /// type is anchored to something outside this crate.
    #[test]
    fn digest_display_and_parse() {
        let digest = Digest::compute([]);
        assert_eq!(
            digest.to_string(),
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        let parsed: Digest = digest.to_string().parse().unwrap();
        assert_eq!(parsed, digest);

        for bad in [
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "sha512:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "sha256:e3b0",
            "sha256:E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
        ] {
            assert!(bad.parse::<Digest>().is_err(), "{bad} should not parse");
        }
    }

    #[test]
    fn one_pass_yields_both_digests() {
        let content = sample_sequence();
        let (blob, digests) = write_layer(&content, 9);

        assert_eq!(digests.diff_id, Digest::compute(&content));
        assert_eq!(digests.blob_digest, Digest::compute(&blob));
        assert_eq!(digests.uncompressed_size, content.len() as u64);
        assert_eq!(digests.compressed_size, blob.len() as u64);
        assert!(digests.compressed_size < digests.uncompressed_size);
    }

    #[test]
    fn read_back_reproduces_the_digests() {
        let content = sample_sequence();
        let (blob, written) = write_layer(&content, 9);

        let (read_content, read_digests) = read_blob(blob.as_slice()).unwrap();
        assert_eq!(read_content, content);
        assert_eq!(read_digests, written);

        let scanned = scan_blob(blob.as_slice()).unwrap();
        assert_eq!(scanned, written);
    }

    /// Unknown #3 of the implementation plan: identity must survive compression
    /// variance. The same records at three zstd levels produce one `diffId` and
    /// three different blob digests — so a publisher who compresses differently
    /// still reproduces the inscription, while the registry still addresses
    /// each blob by its own bytes.
    #[test]
    fn identity_survives_compression_variance() {
        let content = sample_sequence();

        let results: Vec<LayerDigests> = [1, 9, 19]
            .into_iter()
            .map(|level| write_layer(&content, level).1)
            .collect();

        let diff_ids: std::collections::BTreeSet<_> = results.iter().map(|d| d.diff_id).collect();
        assert_eq!(diff_ids.len(), 1, "diffId must not depend on zstd level");
        assert_eq!(
            diff_ids.into_iter().next().unwrap(),
            Digest::compute(&content)
        );

        let blob_digests: std::collections::BTreeSet<_> =
            results.iter().map(|d| d.blob_digest).collect();
        assert_eq!(
            blob_digests.len(),
            3,
            "the three levels should produce three distinct blobs; \
             if they ever collide the test has stopped proving anything"
        );

        for digests in &results {
            assert_eq!(digests.uncompressed_size, content.len() as u64);
        }

        // And every one of them decompresses back to the same bytes.
        for level in [1, 9, 19] {
            let (blob, _) = write_layer(&content, level);
            let (round_tripped, _) = read_blob(blob.as_slice()).unwrap();
            assert_eq!(round_tripped, content);
        }
    }

    #[test]
    fn empty_layer_content_is_well_defined() {
        let (blob, digests) = write_layer(&[], 9);
        assert_eq!(digests.diff_id, Digest::compute([]));
        assert_eq!(digests.uncompressed_size, 0);

        let (content, read) = read_blob(blob.as_slice()).unwrap();
        assert!(content.is_empty());
        assert_eq!(read, digests);
    }

    /// A blob that has been altered must never read back as the layer it claims
    /// to be. Padding is refused by the decoder (it looks for a further frame),
    /// truncation leaves the frame incomplete, and a flipped byte either fails
    /// to decode or yields a different `diffId` — never a silent match.
    #[test]
    fn a_tampered_blob_never_reads_back_clean() {
        let content = sample_sequence();
        let (blob, digests) = write_layer(&content, 9);

        let mut padded = blob.clone();
        padded.extend_from_slice(b"appended");
        assert!(scan_blob(padded.as_slice()).is_err(), "padded blob");

        assert!(
            scan_blob(&blob[..blob.len() - 4]).is_err(),
            "truncated blob"
        );

        let mut flipped = blob.clone();
        let middle = flipped.len() / 2;
        flipped[middle] ^= 0xff;
        if let Ok(read) = scan_blob(flipped.as_slice()) {
            assert_ne!(read.diff_id, digests.diff_id, "corrupted blob");
        }
    }

    /// The blob digest and the compressed size cover the whole file, which is
    /// what lets a stele directory detect tampering by comparing a blob's name
    /// with its content.
    #[test]
    fn blob_digest_covers_every_byte() {
        let content = sample_sequence();
        let (blob, _) = write_layer(&content, 9);

        let read = scan_blob(blob.as_slice()).unwrap();
        assert_eq!(read.compressed_size, blob.len() as u64);
        assert_eq!(read.blob_digest, Digest::compute(&blob));
    }
}
