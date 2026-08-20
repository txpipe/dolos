use redb::{ReadTransaction, ReadableTable as _, TableDefinition, WriteTransaction};
use tracing::trace;

use dolos_core::{BlockBody, BlockSlot, ChainPoint, RawBlock};

use super::flatfiles::{decode_locations, encode_locations, BlockLocation, FlatFileStore};

type Error = super::RedbArchiveError;

pub struct BlocksTable;

impl BlocksTable {
    /// Index table: slot -> the blocks recorded at that slot, one packed
    /// 16-byte BlockLocation each.
    ///
    /// A slot is not a unique chain position. Byron's epoch-boundary blocks
    /// carry the same absolute slot as the first main block of the epoch they
    /// open, and a value holding a single location loses whichever of the two
    /// is written second — silently, since the body is already in the segment
    /// file. The value is therefore a list, newest first: position 0 is the
    /// block the slot resolves to and the entries behind it are the ones it
    /// displaced. A value written before the list existed is one entry long
    /// and reads identically, which is what keeps existing archives readable
    /// and readers of position 0 unchanged.
    ///
    /// Nothing here knows why a slot would hold two blocks. It only refuses to
    /// drop one.
    pub const DEF: TableDefinition<'static, BlockSlot, &'static [u8]> =
        TableDefinition::new("blocks");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;
        Ok(())
    }

    /// Read the canonical BlockLocation from the index for a given slot.
    fn get_location(rx: &ReadTransaction, slot: BlockSlot) -> Result<Option<BlockLocation>, Error> {
        let table = rx.open_table(Self::DEF)?;
        match table.get(slot)? {
            Some(value) => Ok(Some(BlockLocation::from_bytes(value.value()))),
            None => Ok(None),
        }
    }

    /// Read every BlockLocation recorded at a slot, in chain order.
    fn get_locations(rx: &ReadTransaction, slot: BlockSlot) -> Result<Vec<BlockLocation>, Error> {
        let table = rx.open_table(Self::DEF)?;
        match table.get(slot)? {
            Some(value) => Ok(decode_locations(value.value()).rev().collect()),
            None => Ok(vec![]),
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

    /// Every block recorded at `slot`, in chain order.
    ///
    /// All but the last are what [`Self::get_by_slot`] cannot return: at a
    /// Byron boundary that is the epoch-boundary block, which the epoch's
    /// first main block shares a slot with and displaces.
    pub fn get_all_by_slot(
        rx: &ReadTransaction,
        flatfiles: &FlatFileStore,
        slot: BlockSlot,
    ) -> Result<Vec<BlockBody>, Error> {
        Self::get_locations(rx, slot)?
            .into_iter()
            .map(|loc| {
                flatfiles
                    .read(&loc)
                    .map_err(super::RedbArchiveError::from_io)
            })
            .collect()
    }

    /// Apply a batch of blocks: append to flat files (with fsync), then insert
    /// all index entries into redb.
    ///
    /// The bodies are already durable when the index loop runs, so a slot that
    /// turns out to be occupied can be resolved by reading what was stored
    /// there: the same block again is a rewrite, a different one is a second
    /// block at that slot and both are kept. See [`Self::merge_location`].
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
        for (i, (point, body)) in blocks.iter().enumerate() {
            let slot = point.slot();
            let bytes = encode_locations(&[locations[i]]);

            // The insert answers whether the slot was already taken, so a slot
            // no other block claims — every slot on a post-Byron chain — costs
            // one index write and no lookup. Only a collision pays for the
            // merge below.
            let occupied = table
                .insert(slot, bytes.as_slice())?
                .map(|previous| previous.value().to_vec());

            if let Some(occupied) = occupied {
                let merged = Self::merge_location(&occupied, locations[i], body, flatfiles)?;
                table.insert(slot, merged.as_slice())?;
            }
        }

        Ok(())
    }

    /// Fold a newly written block into the value already held at its slot.
    ///
    /// The incoming block is compared against what is stored — length first,
    /// so a body is read only where a match is possible at all.
    ///
    /// An identical body means this block is being written again, as a resumed
    /// restore rewrites the layer it was in the middle of. The entry that
    /// points at it is then left exactly where it is, and the copy just
    /// appended is the one nothing points at — which is the dead space restore
    /// already documents. Repointing at the new copy would move an index entry
    /// forward in the segment past a block it precedes in the chain, and
    /// [`Self::undo`] truncates at the offset it removes, so that block's bytes
    /// would go with the cut.
    ///
    /// Anything else is a second block at the same slot, and it takes position
    /// 0: blocks arrive in chain order, so the newcomer is the one the slot
    /// should resolve to.
    fn merge_location(
        existing: &[u8],
        incoming: BlockLocation,
        body: &[u8],
        flatfiles: &FlatFileStore,
    ) -> Result<Vec<u8>, Error> {
        let mut locations: Vec<BlockLocation> = decode_locations(existing).collect();

        for loc in locations.iter() {
            if loc.length as usize != body.len() {
                continue;
            }

            let stored = flatfiles
                .read(loc)
                .map_err(super::RedbArchiveError::from_io)?;

            if stored == body {
                return Ok(existing.to_vec());
            }
        }

        locations.insert(0, incoming);

        Ok(encode_locations(&locations))
    }

    /// Undo the block at `point`.
    ///
    /// A rollback walks the chain backwards, so at a slot holding more than
    /// one block the one to remove is the newest — position 0 — and the slot
    /// survives until its last block is gone.
    pub fn undo(
        wx: &WriteTransaction,
        flatfiles: &FlatFileStore,
        point: &ChainPoint,
    ) -> Result<(), Error> {
        let slot = point.slot();

        // Read locations before removing.
        let mut table = wx.open_table(Self::DEF)?;
        let stored = table.get(slot)?.map(|value| value.value().to_vec());

        let Some(stored) = stored else {
            return Ok(());
        };

        let mut locations: Vec<BlockLocation> = decode_locations(&stored).collect();

        if locations.is_empty() {
            return Ok(());
        }

        let loc = locations.remove(0);

        if locations.is_empty() {
            table.remove(slot)?;
        } else {
            table.insert(slot, encode_locations(&locations).as_slice())?;
        }

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
        // A slot holding more than one block contributes all of them, so the
        // cut lands before the earliest body it has to drop and none survives.
        let mut earliest_after: Option<BlockLocation> = None;
        {
            let range = table.range((slot + 1)..)?;
            for entry in range {
                let (_, loc_bytes) = entry?;

                for loc in decode_locations(loc_bytes.value()) {
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

    /// Get a range of (slot, index value) from the index.
    /// Block data is NOT fetched here; callers use FlatFileStore to read
    /// lazily.
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
