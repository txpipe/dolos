//! Flat segment files for archived block bodies.
//!
//! Bodies are appended to numbered segment files (one Cardano epoch per
//! segment) and addressed by packed [`BlockLocation`]s stored in whichever
//! archive index backend is in use. The layout is backend-independent: the
//! redb and fjall archive stores share these files byte for byte, which is
//! why this crate knows nothing about Cardano and depends on little beyond
//! the standard library: `tempfile` for throwaway stores, and `zstd` plus
//! `sha2` for the [`compressed`] segment codec.
//!
//! A segment is stored raw or compressed; the store reads either through the
//! same [`BlockLocation`]s and moves a segment between the two with a
//! recoverable transition (`LIFECYCLE.md`).

pub mod compressed;
mod layout;
mod store;

pub use layout::{Representation, Transition, DICTIONARIES_DIR};
pub use store::{FlatFileOptions, FlatFileStore, SegmentInfo};

/// Number of slots per segment file (one Cardano epoch).
pub const SLOTS_PER_SEGMENT: u64 = 432_000;

/// Location of a block within the flat file store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockLocation {
    pub segment_id: u32,
    pub offset: u64,
    pub length: u32,
}

/// Packed size of a BlockLocation in bytes.
pub const BLOCK_LOCATION_SIZE: usize = 16;

impl BlockLocation {
    pub fn to_bytes(self) -> [u8; BLOCK_LOCATION_SIZE] {
        let mut buf = [0u8; BLOCK_LOCATION_SIZE];
        buf[0..4].copy_from_slice(&self.segment_id.to_be_bytes());
        buf[4..12].copy_from_slice(&self.offset.to_be_bytes());
        buf[12..16].copy_from_slice(&self.length.to_be_bytes());
        buf
    }

    /// Unpack the first location in `bytes`.
    ///
    /// An index value holds one location per block recorded at its slot, and
    /// the first is the canonical one, so this stays the point read it always
    /// was — including for a binary that predates multi-block slots.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= BLOCK_LOCATION_SIZE);
        let segment_id = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let offset = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
        let length = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        Self {
            segment_id,
            offset,
            length,
        }
    }

    /// Compute the segment ID for a given slot.
    pub fn segment_for_slot(slot: u64) -> u32 {
        (slot / SLOTS_PER_SEGMENT) as u32
    }
}

/// Read the blocks recorded at one slot out of a packed index value.
///
/// The list is newest first: position 0 is the slot's canonical block and the
/// entries after it are the ones it displaced, so reversing this yields chain
/// order. A value written before the archive could hold more than one block
/// per slot is one entry long and reads the same either way.
pub fn decode_locations(bytes: &[u8]) -> impl DoubleEndedIterator<Item = BlockLocation> + '_ {
    bytes
        .chunks_exact(BLOCK_LOCATION_SIZE)
        .map(BlockLocation::from_bytes)
}

/// Pack a slot's blocks back into an index value, newest first.
pub fn encode_locations(locations: &[BlockLocation]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(locations.len() * BLOCK_LOCATION_SIZE);

    for loc in locations {
        buf.extend_from_slice(&loc.to_bytes());
    }

    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_path(store: &FlatFileStore, segment_id: u32) -> std::path::PathBuf {
        store
            .segments_dir()
            .join(format!("{segment_id:06}.segment"))
    }

    #[test]
    fn test_block_location_roundtrip() {
        let loc = BlockLocation {
            segment_id: 42,
            offset: 123456789,
            length: 65535,
        };
        let bytes = loc.to_bytes();
        let loc2 = BlockLocation::from_bytes(&bytes);
        assert_eq!(loc, loc2);
    }

    #[test]
    fn test_segment_for_slot() {
        assert_eq!(BlockLocation::segment_for_slot(0), 0);
        assert_eq!(BlockLocation::segment_for_slot(431_999), 0);
        assert_eq!(BlockLocation::segment_for_slot(432_000), 1);
        assert_eq!(BlockLocation::segment_for_slot(864_000), 2);
    }

    #[test]
    fn test_append_and_read() {
        let (dir, store) = FlatFileStore::for_tempdir().unwrap();
        let data1 = b"block_one";
        let data2 = b"block_two";

        let locs = store
            .append_batch(&[(0, data1.as_slice()), (0, data2.as_slice())])
            .unwrap();

        assert_eq!(locs.len(), 2);
        assert_eq!(locs[0].segment_id, 0);
        assert_eq!(locs[0].offset, 0);
        assert_eq!(locs[0].length, data1.len() as u32);
        assert_eq!(locs[1].offset, data1.len() as u64);

        let read1 = store.read(&locs[0]).unwrap();
        assert_eq!(read1, data1);
        let read2 = store.read(&locs[1]).unwrap();
        assert_eq!(read2, data2);

        drop(dir); // cleanup
    }

    #[test]
    fn test_cross_segment_batch() {
        let (dir, store) = FlatFileStore::for_tempdir().unwrap();
        let data_a = b"segment_zero";
        let data_b = b"segment_one";

        let locs = store
            .append_batch(&[(0, data_a.as_slice()), (1, data_b.as_slice())])
            .unwrap();

        assert_eq!(locs[0].segment_id, 0);
        assert_eq!(locs[1].segment_id, 1);

        assert_eq!(store.read(&locs[0]).unwrap(), data_a);
        assert_eq!(store.read(&locs[1]).unwrap(), data_b);

        drop(dir);
    }

    #[test]
    fn test_truncate() {
        let (dir, store) = FlatFileStore::for_tempdir().unwrap();
        let data1 = b"first";
        let data2 = b"second";

        let locs = store
            .append_batch(&[(0, data1.as_slice()), (0, data2.as_slice())])
            .unwrap();

        // Truncate after first block.
        store.truncate(0, locs[1].offset).unwrap();

        // First block still readable.
        assert_eq!(store.read(&locs[0]).unwrap(), data1);

        // Second block should fail (truncated).
        assert!(store.read(&locs[1]).is_err());

        // Can append again after truncation.
        let data3 = b"third";
        let locs2 = store.append_batch(&[(0, data3.as_slice())]).unwrap();
        assert_eq!(locs2[0].offset, locs[1].offset); // reuses truncated space
        assert_eq!(store.read(&locs2[0]).unwrap(), data3);

        drop(dir);
    }

    #[test]
    fn test_truncate_to_zero_removes_file() {
        let (dir, store) = FlatFileStore::for_tempdir().unwrap();
        store.append_batch(&[(0, b"data".as_slice())]).unwrap();

        store.truncate(0, 0).unwrap();
        assert!(!raw_path(&store, 0).exists());

        drop(dir);
    }

    #[test]
    fn test_delete_segments_before() {
        let (dir, store) = FlatFileStore::for_tempdir().unwrap();
        store
            .append_batch(&[
                (0, b"a".as_slice()),
                (1, b"b".as_slice()),
                (2, b"c".as_slice()),
            ])
            .unwrap();

        store.delete_segments_before(2).unwrap();

        assert!(!raw_path(&store, 0).exists());
        assert!(!raw_path(&store, 1).exists());
        assert!(raw_path(&store, 2).exists());

        drop(dir);
    }
}
