use std::collections::{hash_map::Entry, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Mutex;

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

/// Segment file name for a given segment ID.
fn segment_filename(segment_id: u32) -> String {
    format!("{:06}.segment", segment_id)
}

/// Manages append-only segment files for block storage.
pub struct FlatFileStore {
    segments_dir: PathBuf,
    writers: Mutex<HashMap<u32, File>>,
}

impl FlatFileStore {
    /// Create a new FlatFileStore at the given directory.
    /// Creates the directory if it does not exist.
    pub fn new(segments_dir: impl Into<PathBuf>) -> io::Result<Self> {
        let segments_dir = segments_dir.into();
        fs::create_dir_all(&segments_dir)?;
        Ok(Self {
            segments_dir,
            writers: Mutex::new(HashMap::new()),
        })
    }

    fn segment_path(&self, segment_id: u32) -> PathBuf {
        self.segments_dir.join(segment_filename(segment_id))
    }

    /// Get or create an append-mode file handle for a segment.
    fn get_writer(&self, segment_id: u32) -> io::Result<()> {
        let mut writers = self.writers.lock().unwrap();
        if let Entry::Vacant(entry) = writers.entry(segment_id) {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.segment_path(segment_id))?;
            entry.insert(file);
        }
        Ok(())
    }

    /// Append a batch of blocks to their respective segment files.
    ///
    /// Each item is `(segment_id, block_data)`. Blocks are appended in order.
    /// A single fsync is performed per segment file after all blocks for that
    /// segment have been written.
    ///
    /// Returns a `BlockLocation` for each input item, in the same order.
    pub fn append_batch(&self, items: &[(u32, &[u8])]) -> io::Result<Vec<BlockLocation>> {
        let mut locations = Vec::with_capacity(items.len());
        let mut touched_segments: HashMap<u32, ()> = HashMap::new();

        // Ensure all writers exist.
        for &(segment_id, _) in items {
            self.get_writer(segment_id)?;
            touched_segments.insert(segment_id, ());
        }

        let mut writers = self.writers.lock().unwrap();

        for &(segment_id, data) in items {
            let file = writers.get_mut(&segment_id).unwrap();
            // Current position is the offset (file is in append mode).
            let offset = file.seek(SeekFrom::End(0))?;
            file.write_all(data)?;
            locations.push(BlockLocation {
                segment_id,
                offset,
                length: data.len() as u32,
            });
        }

        // Fsync all touched segments.
        for &segment_id in touched_segments.keys() {
            if let Some(file) = writers.get(&segment_id) {
                file.sync_data()?;
            }
        }

        Ok(locations)
    }

    /// Read block data at the given location.
    pub fn read(&self, loc: &BlockLocation) -> io::Result<Vec<u8>> {
        let mut file = File::open(self.segment_path(loc.segment_id))?;
        file.seek(SeekFrom::Start(loc.offset))?;
        let mut buf = vec![0u8; loc.length as usize];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Truncate a segment file at the given offset.
    /// Used for undo: removes everything from `offset` onwards.
    pub fn truncate(&self, segment_id: u32, offset: u64) -> io::Result<()> {
        // Drop the cached writer so we reopen fresh next time.
        {
            let mut writers = self.writers.lock().unwrap();
            writers.remove(&segment_id);
        }

        let path = self.segment_path(segment_id);
        if !path.exists() {
            return Ok(());
        }

        if offset == 0 {
            // Remove the file entirely if truncating to zero.
            fs::remove_file(&path)?;
        } else {
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(offset)?;
            file.sync_data()?;
        }

        Ok(())
    }

    /// Delete all segment files with IDs strictly less than `segment_id`.
    pub fn delete_segments_before(&self, segment_id: u32) -> io::Result<()> {
        let mut writers = self.writers.lock().unwrap();

        // Remove cached writers for deleted segments.
        writers.retain(|&id, _| id >= segment_id);

        // Scan directory for segment files to delete.
        for entry in fs::read_dir(&self.segments_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            if !name_str.ends_with(".segment") {
                continue;
            }
            let prefix = &name_str[..6];
            if let Ok(id) = prefix.parse::<u32>() {
                if id < segment_id {
                    fs::remove_file(entry.path())?;
                }
            }
        }

        Ok(())
    }

    /// Create a FlatFileStore backed by a temporary directory.
    /// Returns the TempDir (caller must keep it alive) and the store.
    pub fn for_tempdir() -> io::Result<(tempfile::TempDir, Self)> {
        let dir = tempfile::tempdir()?;
        let store = Self::new(dir.path())?;
        Ok((dir, store))
    }
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
