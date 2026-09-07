//! Byte-level constants and codecs for the compressed segment layout.
//!
//! The layout is the Zstandard Seekable Format (v0.1.0) with one Dolos
//! skippable frame in front: `[metadata][zstd frame]*[seek table]`. See
//! `crates/flatfiles/COMPRESSED.md` for the normative description.

use std::io;

use super::dictionary::DictionaryId;

/// Magic number of a standard zstd frame, little-endian on disk.
pub const ZSTD_FRAME_MAGIC: u32 = 0xFD2F_B528;

/// Skippable-frame magic the Dolos metadata frame uses (`0x184D2A50`).
pub const METADATA_FRAME_MAGIC: u32 = 0x184D_2A50;

/// Skippable-frame magic the seekable format reserves for its seek table.
pub const SEEK_TABLE_FRAME_MAGIC: u32 = 0x184D_2A5E;

/// Trailing magic of the seek table footer.
pub const SEEKABLE_MAGIC: u32 = 0x8F92_EAB1;

/// Size of a skippable frame header: magic plus payload size.
pub const SKIPPABLE_HEADER_SIZE: usize = 8;

/// Size of the seek table footer: frame count, descriptor, magic.
pub const SEEK_TABLE_FOOTER_SIZE: usize = 9;

/// Seek table entry without the optional checksum.
pub const SEEK_ENTRY_SIZE: usize = 8;

/// Seek table entry with the optional checksum (never written, accepted).
pub const SEEK_ENTRY_SIZE_WITH_CHECKSUM: usize = 12;

const DESCRIPTOR_CHECKSUM_FLAG: u8 = 0b1000_0000;
const DESCRIPTOR_RESERVED_MASK: u8 = 0b0111_1100;

/// Magic at the start of the Dolos metadata payload.
pub const METADATA_MAGIC: [u8; 4] = *b"DOLZ";

/// The only metadata payload version this crate reads or writes.
pub const METADATA_VERSION: u16 = 1;

/// Size of the version-1 metadata payload.
pub const METADATA_PAYLOAD_SIZE: usize = 64;

/// Size of the whole metadata frame, header included.
pub const METADATA_FRAME_SIZE: usize = SKIPPABLE_HEADER_SIZE + METADATA_PAYLOAD_SIZE;

const METADATA_FLAG_DICTIONARY: u8 = 0b0000_0001;

/// How block boundaries pick frame boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameMode {
    /// One zstd frame per block.
    PerBlock,
    /// Adjacent blocks share a frame until adding the next block would push
    /// the frame past `target` bytes. A block larger than `target` still gets
    /// a frame of its own, so `target` bounds nothing but the grouping.
    Chunked { target: u32 },
}

impl FrameMode {
    fn code(self) -> u8 {
        match self {
            FrameMode::PerBlock => 1,
            FrameMode::Chunked { .. } => 2,
        }
    }

    fn target(self) -> u32 {
        match self {
            FrameMode::PerBlock => 0,
            FrameMode::Chunked { target } => target,
        }
    }

    fn from_parts(code: u8, target: u32) -> io::Result<Self> {
        match (code, target) {
            (1, 0) => Ok(FrameMode::PerBlock),
            (1, _) => Err(invalid("per-block metadata carries a frame target")),
            (2, 0) => Err(invalid("chunked metadata carries a zero frame target")),
            (2, target) => Ok(FrameMode::Chunked { target }),
            (other, _) => Err(invalid(format!("unknown frame mode {other}"))),
        }
    }
}

/// What the Dolos metadata frame records about a segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub mode: FrameMode,
    pub level: i32,
    /// Identity of the dictionary every data frame was compressed with, or
    /// `None` when the frames are self-contained.
    pub dictionary: Option<DictionaryId>,
    /// The id zstd stamped into the frames, `0` for raw-content dictionaries
    /// and dictionary-free segments.
    pub zstd_dictionary_id: u32,
    /// `ZSTD_versionNumber()` of the library that wrote the segment.
    pub zstd_version: u32,
}

impl Metadata {
    /// Encode the whole metadata frame: skippable header plus payload.
    pub fn encode(&self) -> [u8; METADATA_FRAME_SIZE] {
        let mut buf = [0u8; METADATA_FRAME_SIZE];
        buf[0..4].copy_from_slice(&METADATA_FRAME_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&(METADATA_PAYLOAD_SIZE as u32).to_le_bytes());

        let payload = &mut buf[SKIPPABLE_HEADER_SIZE..];
        payload[0..4].copy_from_slice(&METADATA_MAGIC);
        payload[4..6].copy_from_slice(&METADATA_VERSION.to_le_bytes());
        payload[6] = self.mode.code();
        payload[7] = if self.dictionary.is_some() {
            METADATA_FLAG_DICTIONARY
        } else {
            0
        };
        payload[8..12].copy_from_slice(&self.level.to_le_bytes());
        payload[12..16].copy_from_slice(&self.mode.target().to_le_bytes());
        payload[16..20].copy_from_slice(&self.zstd_dictionary_id.to_le_bytes());
        if let Some(id) = &self.dictionary {
            payload[20..52].copy_from_slice(id.as_bytes());
        }
        payload[52..56].copy_from_slice(&self.zstd_version.to_le_bytes());
        buf
    }

    /// Decode a metadata frame from the exact bytes the seek table attributes
    /// to it, header included.
    pub fn decode(frame: &[u8]) -> io::Result<Self> {
        if frame.len() < SKIPPABLE_HEADER_SIZE {
            return Err(invalid("metadata frame shorter than a skippable header"));
        }

        let magic = u32::from_le_bytes(frame[0..4].try_into().unwrap());
        if magic != METADATA_FRAME_MAGIC {
            return Err(invalid(format!(
                "first frame is not a Dolos metadata frame (magic {magic:#010x})"
            )));
        }

        let declared = u32::from_le_bytes(frame[4..8].try_into().unwrap()) as usize;
        let payload = &frame[SKIPPABLE_HEADER_SIZE..];
        if declared != payload.len() {
            return Err(invalid(format!(
                "metadata frame declares {declared} payload bytes but the seek table gives it {}",
                payload.len()
            )));
        }
        if payload.len() < 6 || payload[0..4] != METADATA_MAGIC {
            return Err(invalid("truncated or foreign metadata payload"));
        }

        let version = u16::from_le_bytes(payload[4..6].try_into().unwrap());
        if version != METADATA_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "compressed segment metadata version {version} is not supported (this build reads version {METADATA_VERSION})"
                ),
            ));
        }
        if payload.len() != METADATA_PAYLOAD_SIZE {
            return Err(invalid(format!(
                "metadata version {version} payload is {} bytes, expected {METADATA_PAYLOAD_SIZE}",
                payload.len()
            )));
        }

        let flags = payload[7];
        let target = u32::from_le_bytes(payload[12..16].try_into().unwrap());
        let mode = FrameMode::from_parts(payload[6], target)?;
        let level = i32::from_le_bytes(payload[8..12].try_into().unwrap());
        let zstd_dictionary_id = u32::from_le_bytes(payload[16..20].try_into().unwrap());
        let dictionary = if flags & METADATA_FLAG_DICTIONARY != 0 {
            Some(DictionaryId::from_bytes(
                payload[20..52].try_into().unwrap(),
            ))
        } else {
            if payload[20..52].iter().any(|b| *b != 0) {
                return Err(invalid(
                    "dictionary identity set on a dictionary-free segment",
                ));
            }
            None
        };
        if payload[56..64].iter().any(|b| *b != 0) {
            return Err(invalid("reserved metadata bytes are not zero"));
        }

        Ok(Self {
            mode,
            level,
            dictionary,
            zstd_dictionary_id,
            zstd_version: u32::from_le_bytes(payload[52..56].try_into().unwrap()),
        })
    }
}

/// One seek table entry: the sizes of a frame on disk and once decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeekEntry {
    pub compressed_size: u32,
    pub decompressed_size: u32,
}

/// Encode the seek table frame for `entries`, in file order.
pub fn encode_seek_table(entries: &[SeekEntry]) -> io::Result<Vec<u8>> {
    let count = u32::try_from(entries.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "too many frames for a seek table",
        )
    })?;
    let payload_len = entries.len() * SEEK_ENTRY_SIZE + SEEK_TABLE_FOOTER_SIZE;
    let payload_size = u32::try_from(payload_len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "seek table exceeds 4 GiB"))?;

    let mut buf = Vec::with_capacity(SKIPPABLE_HEADER_SIZE + payload_len);
    buf.extend_from_slice(&SEEK_TABLE_FRAME_MAGIC.to_le_bytes());
    buf.extend_from_slice(&payload_size.to_le_bytes());
    for entry in entries {
        buf.extend_from_slice(&entry.compressed_size.to_le_bytes());
        buf.extend_from_slice(&entry.decompressed_size.to_le_bytes());
    }
    buf.extend_from_slice(&count.to_le_bytes());
    buf.push(0);
    buf.extend_from_slice(&SEEKABLE_MAGIC.to_le_bytes());
    Ok(buf)
}

/// Where a seek table sits inside a file, derived from its footer.
#[derive(Debug, Clone, Copy)]
pub struct SeekTableShape {
    /// Frame count the footer declares.
    pub count: usize,
    /// Bytes per entry, depending on the checksum flag.
    pub entry_size: usize,
    /// Whole frame size, header included.
    pub frame_size: u64,
}

/// Parse the nine-byte footer that ends every seekable file.
pub fn parse_seek_table_footer(
    footer: &[u8; SEEK_TABLE_FOOTER_SIZE],
) -> io::Result<SeekTableShape> {
    let magic = u32::from_le_bytes(footer[5..9].try_into().unwrap());
    if magic != SEEKABLE_MAGIC {
        return Err(invalid(format!(
            "file does not end in a zstd seek table (magic {magic:#010x})"
        )));
    }
    let descriptor = footer[4];
    if descriptor & DESCRIPTOR_RESERVED_MASK != 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("seek table descriptor {descriptor:#04x} sets reserved bits"),
        ));
    }
    let entry_size = if descriptor & DESCRIPTOR_CHECKSUM_FLAG != 0 {
        SEEK_ENTRY_SIZE_WITH_CHECKSUM
    } else {
        SEEK_ENTRY_SIZE
    };
    let count = u32::from_le_bytes(footer[0..4].try_into().unwrap()) as usize;
    let payload = count as u64 * entry_size as u64 + SEEK_TABLE_FOOTER_SIZE as u64;
    if payload > u32::MAX as u64 {
        return Err(invalid(format!(
            "seek table with {count} frames exceeds 4 GiB"
        )));
    }
    Ok(SeekTableShape {
        count,
        entry_size,
        frame_size: payload + SKIPPABLE_HEADER_SIZE as u64,
    })
}

/// Parse the seek table frame given its exact bytes, header included, and the
/// shape its footer announced.
pub fn parse_seek_table(frame: &[u8], shape: SeekTableShape) -> io::Result<Vec<SeekEntry>> {
    if frame.len() as u64 != shape.frame_size {
        return Err(invalid("seek table frame length does not match its footer"));
    }
    let magic = u32::from_le_bytes(frame[0..4].try_into().unwrap());
    if magic != SEEK_TABLE_FRAME_MAGIC {
        return Err(invalid(format!(
            "seek table frame has magic {magic:#010x}, expected {SEEK_TABLE_FRAME_MAGIC:#010x}"
        )));
    }
    let declared = u32::from_le_bytes(frame[4..8].try_into().unwrap()) as u64;
    if declared != shape.frame_size - SKIPPABLE_HEADER_SIZE as u64 {
        return Err(invalid("seek table header size disagrees with its footer"));
    }

    let body = &frame[SKIPPABLE_HEADER_SIZE..frame.len() - SEEK_TABLE_FOOTER_SIZE];
    let entries = body
        .chunks_exact(shape.entry_size)
        .map(|entry| SeekEntry {
            compressed_size: u32::from_le_bytes(entry[0..4].try_into().unwrap()),
            decompressed_size: u32::from_le_bytes(entry[4..8].try_into().unwrap()),
        })
        .collect::<Vec<_>>();
    debug_assert_eq!(entries.len(), shape.count);
    Ok(entries)
}

pub(crate) fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}
