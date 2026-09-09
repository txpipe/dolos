//! Flat segment files for archived block bodies.
//!
//! Bodies are appended to numbered segment files (one Cardano epoch per
//! segment) as independent zstd frames, one per body, compressed with the
//! dictionary bundled in this crate. A packed [`BlockLocation`] names a frame
//! by its physical offset and length inside its segment; the archive index
//! holds those locations and this crate decodes the frame they name. The
//! crate knows nothing about Cardano and depends on little beyond the
//! standard library: `zstd` for the frames, `rayon` for the offline import's
//! parallel encoding and `tempfile` for throwaway stores.

mod codec;
mod store;

pub use codec::{frame_bound, BUNDLED_DICTIONARY, COMPRESSION_LEVEL, MAX_BODY_BYTES};
pub use store::{
    parse_segment_filename, AppendStats, FlatFileStore, ResourceStats, IMPORT_WINDOW_BYTES,
};

/// Number of slots per segment file (one Cardano epoch).
pub const SLOTS_PER_SEGMENT: u64 = 432_000;

/// Location of a block's frame within the flat file store: the segment, the
/// frame's byte offset in the segment file, and the frame's length.
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
        assert_eq!(locs[1].offset, locs[0].length as u64);
        assert_eq!(
            locs[1].offset + locs[1].length as u64,
            std::fs::metadata(store.segment_path(0)).unwrap().len()
        );

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
        assert!(!store.segment_path(0).exists());

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

        assert!(!store.segment_path(0).exists());
        assert!(!store.segment_path(1).exists());
        assert!(store.segment_path(2).exists());

        drop(dir);
    }
}
