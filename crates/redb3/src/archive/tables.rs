use redb::{ReadTransaction, ReadableTable as _, TableDefinition, WriteTransaction};
use tracing::trace;

use dolos_core::{BlockBody, BlockSlot, ChainPoint, RawBlock};

use super::flatfiles::{BlockLocation, FlatFileStore};

type Error = super::RedbArchiveError;

pub struct BlocksTable;

impl BlocksTable {
    /// Index table: slot -> BlockLocation (16 bytes packed).
    pub const DEF: TableDefinition<'static, BlockSlot, &'static [u8]> =
        TableDefinition::new("blocks");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;
        Ok(())
    }

    /// Read a BlockLocation from the index for a given slot.
    fn get_location(rx: &ReadTransaction, slot: BlockSlot) -> Result<Option<BlockLocation>, Error> {
        let table = rx.open_table(Self::DEF)?;
        match table.get(slot)? {
            Some(value) => Ok(Some(BlockLocation::from_bytes(value.value()))),
            None => Ok(None),
        }
    }

    pub fn get_tip(
        rx: &ReadTransaction,
        flatfiles: &FlatFileStore,
    ) -> Result<Option<(BlockSlot, BlockBody)>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let entry = table.last()?;
        let result = entry
            .map(|(slot, loc_bytes)| (slot.value(), BlockLocation::from_bytes(loc_bytes.value())));
        drop(table);

        match result {
            Some((slot, loc)) => {
                let data = flatfiles
                    .read(&loc)
                    .map_err(super::RedbArchiveError::from_io)?;
                Ok(Some((slot, data)))
            }
            None => Ok(None),
        }
    }

    pub fn get_by_slot(
        rx: &ReadTransaction,
        flatfiles: &FlatFileStore,
        slot: BlockSlot,
    ) -> Result<Option<BlockBody>, Error> {
        match Self::get_location(rx, slot)? {
            Some(loc) => {
                let data = flatfiles
                    .read(&loc)
                    .map_err(super::RedbArchiveError::from_io)?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    /// Apply a batch of blocks: append to flat files (with fsync), then insert
    /// all index entries into redb.
    pub fn apply_batch(
        wx: &WriteTransaction,
        flatfiles: &FlatFileStore,
        blocks: &[(ChainPoint, RawBlock)],
    ) -> Result<(), Error> {
        if blocks.is_empty() {
            return Ok(());
        }

        // Prepare flat file batch items.
        let items: Vec<(u32, &[u8])> = blocks
            .iter()
            .map(|(point, block)| {
                let segment_id = BlockLocation::segment_for_slot(point.slot());
                (segment_id, block.as_slice())
            })
            .collect();

        // Append to flat files (fsyncs internally).
        let locations = flatfiles
            .append_batch(&items)
            .map_err(super::RedbArchiveError::from_io)?;

        // Insert all index entries.
        let mut table = wx.open_table(Self::DEF)?;
        for (i, (point, _)) in blocks.iter().enumerate() {
            let bytes = locations[i].to_bytes();
            table.insert(point.slot(), bytes.as_slice())?;
        }

        Ok(())
    }

    pub fn undo(
        wx: &WriteTransaction,
        flatfiles: &FlatFileStore,
        point: &ChainPoint,
    ) -> Result<(), Error> {
        let slot = point.slot();

        // Read location before removing.
        let table = wx.open_table(Self::DEF)?;
        let loc = match table.get(slot)? {
            Some(value) => BlockLocation::from_bytes(value.value()),
            None => return Ok(()),
        };
        drop(table);

        // Remove from index.
        let mut table = wx.open_table(Self::DEF)?;
        table.remove(slot)?;
        drop(table);

        // Truncate the segment file at this block's offset.
        flatfiles
            .truncate(loc.segment_id, loc.offset)
            .map_err(super::RedbArchiveError::from_io)?;

        Ok(())
    }

    pub fn first(rx: &ReadTransaction) -> Result<Option<(BlockSlot, BlockLocation)>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let entry = table.first()?;
        Ok(entry
            .map(|(slot, loc_bytes)| (slot.value(), BlockLocation::from_bytes(loc_bytes.value()))))
    }

    pub fn last(rx: &ReadTransaction) -> Result<Option<(BlockSlot, BlockLocation)>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let entry = table.last()?;
        Ok(entry
            .map(|(slot, loc_bytes)| (slot.value(), BlockLocation::from_bytes(loc_bytes.value()))))
    }

    pub fn remove_before(
        wx: &WriteTransaction,
        flatfiles: &FlatFileStore,
        slot: BlockSlot,
    ) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;
        let mut to_remove = table.extract_from_if(..slot, |_, _| true)?;

        while let Some(Ok((slot, _))) = to_remove.next() {
            trace!(slot = slot.value(), "removing block index entry");
        }
        drop(to_remove);
        drop(table);

        // Delete segment files that are fully before this slot.
        let threshold_segment = BlockLocation::segment_for_slot(slot);
        flatfiles
            .delete_segments_before(threshold_segment)
            .map_err(super::RedbArchiveError::from_io)?;

        Ok(())
    }

    pub fn remove_after(
        wx: &WriteTransaction,
        flatfiles: &FlatFileStore,
        slot: BlockSlot,
    ) -> Result<(), Error> {
        // Find the last entry after `slot` to know where to truncate.
        let table = wx.open_table(Self::DEF)?;

        // Find the earliest entry after `slot` to determine truncation offset.
        let mut earliest_after: Option<BlockLocation> = None;
        {
            let range = table.range((slot + 1)..)?;
            for entry in range {
                let (_, loc_bytes) = entry?;
                let loc = BlockLocation::from_bytes(loc_bytes.value());
                match &earliest_after {
                    None => earliest_after = Some(loc),
                    Some(prev) => {
                        if loc.segment_id < prev.segment_id
                            || (loc.segment_id == prev.segment_id && loc.offset < prev.offset)
                        {
                            earliest_after = Some(loc);
                        }
                    }
                }
            }
        }
        drop(table);

        // Remove index entries.
        let mut table = wx.open_table(Self::DEF)?;
        let mut to_remove = table.extract_from_if(slot.., |x, _| x > slot)?;
        while let Some(Ok((slot, _))) = to_remove.next() {
            trace!(slot = slot.value(), "removing block index entry");
        }
        drop(to_remove);
        drop(table);

        // Truncate the segment file if we found entries to remove.
        if let Some(loc) = earliest_after {
            flatfiles
                .truncate(loc.segment_id, loc.offset)
                .map_err(super::RedbArchiveError::from_io)?;
        }

        Ok(())
    }

    /// Get a range of (slot, BlockLocation) from the index.
    /// Block data is NOT fetched here; callers use FlatFileStore to read lazily.
    pub fn get_range(
        rx: &ReadTransaction,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<redb::Range<'static, u64, &'static [u8]>, Error> {
        let table = rx.open_table(Self::DEF)?;
        match (from, to) {
            (Some(from), Some(to)) => Ok(table.range(from..to)?),
            (Some(from), None) => Ok(table.range(from..)?),
            (None, Some(to)) => Ok(table.range(..to)?),
            (None, None) => Ok(table.range(0..)?),
        }
    }
}
