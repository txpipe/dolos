//! One zstd frame per block body, compressed with the bundled dictionary.
//!
//! The dictionary and the frame parameters are part of the storage format:
//! every frame carries a checksum, its content size and the dictionary id,
//! so a reader can bound its allocation from the header and refuse a frame
//! that was not written for this dictionary.

use std::io;
use std::sync::OnceLock;

use zstd::bulk::{Compressor, Decompressor};
use zstd::dict::{DecoderDictionary, EncoderDictionary};

/// The zstd dictionary every frame is compressed with, fixed as part of the
/// storage format. `dictionary/README.md` records its provenance.
pub const BUNDLED_DICTIONARY: &[u8] = include_bytes!("../dictionary/cardano.dict");

/// zstd level every frame is compressed at.
pub const COMPRESSION_LEVEL: i32 = 3;

/// Largest body one frame may hold. An append of a larger body is refused
/// before anything is written; a frame header declaring more is refused
/// before any output is allocated for it.
pub const MAX_BODY_BYTES: usize = 16 << 20;

/// Buffers a pooled decoder keeps across reads; larger ones are shrunk
/// back before the decoder is pooled.
const POOLED_BUFFER_BYTES: usize = 1 << 20;

/// The most bytes the frame for a `len`-byte body can take: what an
/// encoder reserves before compressing it, and what an import window
/// budgets for it before it is encoded.
pub fn frame_bound(len: usize) -> usize {
    zstd::zstd_safe::compress_bound(len)
}

pub(crate) fn oversized(len: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("a {len}-byte body exceeds the {MAX_BODY_BYTES}-byte frame limit"),
    )
}

fn encoder_dictionary() -> &'static EncoderDictionary<'static> {
    static DICTIONARY: OnceLock<EncoderDictionary<'static>> = OnceLock::new();
    DICTIONARY.get_or_init(|| EncoderDictionary::copy(BUNDLED_DICTIONARY, COMPRESSION_LEVEL))
}

fn decoder_dictionary() -> &'static DecoderDictionary<'static> {
    static DICTIONARY: OnceLock<DecoderDictionary<'static>> = OnceLock::new();
    DICTIONARY.get_or_init(|| DecoderDictionary::copy(BUNDLED_DICTIONARY))
}

/// A reusable compression context and output buffer.
pub struct Encoder {
    compressor: Compressor<'static>,
    frame: Vec<u8>,
    bounded: bool,
}

impl Encoder {
    pub fn new() -> io::Result<Self> {
        let mut compressor = Compressor::with_prepared_dictionary(encoder_dictionary())?;
        compressor.include_checksum(true)?;
        compressor.include_contentsize(true)?;
        compressor.include_dictid(true)?;
        Ok(Self {
            compressor,
            frame: Vec::new(),
            bounded: false,
        })
    }

    pub(crate) fn for_parallel() -> io::Result<Self> {
        Ok(Self {
            bounded: true,
            ..Self::new()?
        })
    }

    /// Compress `body` into one frame, borrowed from the encoder's buffer
    /// until the next call.
    pub fn encode(&mut self, body: &[u8]) -> io::Result<&[u8]> {
        if body.len() > MAX_BODY_BYTES {
            return Err(oversized(body.len()));
        }
        self.frame.clear();
        let bound = frame_bound(body.len());
        if self.frame.capacity() < bound {
            if self.bounded {
                self.frame.reserve_exact(bound);
            } else {
                self.frame.reserve(bound);
            }
        }
        let n = self.compressor.compress_to_buffer(body, &mut self.frame)?;
        Ok(&self.frame[..n])
    }

    pub(crate) fn capacity(&self) -> usize {
        self.frame.capacity()
    }
}

/// A reusable decompression context with its frame and body buffers.
pub struct Decoder {
    decompressor: Decompressor<'static>,
    frame: Vec<u8>,
    body: Vec<u8>,
}

impl Decoder {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            decompressor: Decompressor::with_prepared_dictionary(decoder_dictionary())?,
            frame: Vec::new(),
            body: Vec::new(),
        })
    }

    /// The buffer to read a `len`-byte frame into before
    /// [`decode`](Self::decode).
    pub fn frame_buffer(&mut self, len: usize) -> &mut [u8] {
        self.frame.clear();
        self.frame.resize(len, 0);
        &mut self.frame
    }

    /// Decode the frame last read into the frame buffer.
    ///
    /// The declared content size is checked against [`MAX_BODY_BYTES`]
    /// before the body is allocated; the checksum is verified by zstd on the
    /// way, and the decoded length must match what the header declared.
    pub fn decode(&mut self) -> io::Result<&[u8]> {
        let declared = zstd::zstd_safe::get_frame_content_size(&self.frame)
            .map_err(|_| invalid("unreadable frame header"))?
            .ok_or_else(|| invalid("frame header carries no content size"))?;
        if declared > MAX_BODY_BYTES as u64 {
            return Err(invalid(format!(
                "frame declares {declared} bytes, over the {MAX_BODY_BYTES}-byte limit"
            )));
        }
        let declared = declared as usize;
        self.body.clear();
        if self.body.capacity() < declared {
            self.body.reserve(declared);
        }
        let n = self
            .decompressor
            .decompress_to_buffer(&self.frame, &mut self.body)
            .map_err(|e| invalid(e.to_string()))?;
        if n != declared {
            return Err(invalid(format!(
                "frame decoded to {n} bytes, header declares {declared}"
            )));
        }
        Ok(&self.body[..n])
    }

    /// Release the memory a large read left behind, before pooling.
    pub fn shrink(&mut self) {
        self.frame.clear();
        self.frame.shrink_to(POOLED_BUFFER_BYTES);
        self.body.clear();
        self.body.shrink_to(POOLED_BUFFER_BYTES);
    }
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}
