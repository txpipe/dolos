use pallas::ledger::traverse::probe;
use redb::{ReadTransaction, ReadableTable as _, TableDefinition, WriteTransaction};
use tracing::trace;

use dolos_core::{BlockBody, BlockSlot, ChainPoint, RawBlock};

use super::flatfiles::{BlockLocation, FlatFileStore};

type Error = super::RedbArchiveError;

/// Shape shared by the block index and its epoch-boundary sidecar:
/// `slot -> BlockLocation`, the location packed into 16 bytes.
type IndexDef = TableDefinition<'static, BlockSlot, &'static [u8]>;

/// A slot-ordered walk over one of those tables.
pub type IndexRange = redb::Range<'static, BlockSlot, &'static [u8]>;

/// True when `block` is a Byron epoch-boundary block.
///
/// The probe reads the first two CBOR tokens of the block wrapper, so this
/// costs nothing next to the segment append it gates. Anything that is not a
/// recognizable era wrapper — the opaque payloads the tests write, say — is
/// not an EBB and belongs in the main index.
pub fn is_ebb(block: &[u8]) -> bool {
    matches!(probe::block_era(block), probe::Outcome::EpochBoundary)
}

/// Read a `BlockLocation` for `slot` out of one index table.
fn get_location(
    rx: &ReadTransaction,
    def: IndexDef,
    slot: BlockSlot,
) -> Result<Option<BlockLocation>, Error> {
    let table = rx.open_table(def)?;
    match table.get(slot)? {
        Some(value) => Ok(Some(BlockLocation::from_bytes(value.value()))),
        None => Ok(None),
    }
}

/// Same as [`get_location`], against a write transaction.
fn get_location_mut(
    wx: &WriteTransaction,
    def: IndexDef,
    slot: BlockSlot,
) -> Result<Option<BlockLocation>, Error> {
    let table = wx.open_table(def)?;
    let found = table
        .get(slot)?
        .map(|value| BlockLocation::from_bytes(value.value()));

    Ok(found)
}

fn first_of(
    rx: &ReadTransaction,
    def: IndexDef,
) -> Result<Option<(BlockSlot, BlockLocation)>, Error> {
    let table = rx.open_table(def)?;
    let entry = table.first()?;
    Ok(entry.map(|(slot, loc)| (slot.value(), BlockLocation::from_bytes(loc.value()))))
}

fn last_of(
    rx: &ReadTransaction,
    def: IndexDef,
) -> Result<Option<(BlockSlot, BlockLocation)>, Error> {
    let table = rx.open_table(def)?;
    let entry = table.last()?;
    Ok(entry.map(|(slot, loc)| (slot.value(), BlockLocation::from_bytes(loc.value()))))
}

fn range_of(
    rx: &ReadTransaction,
    def: IndexDef,
    from: Option<BlockSlot>,
    to: Option<BlockSlot>,
) -> Result<IndexRange, Error> {
    let table = rx.open_table(def)?;
    match (from, to) {
        (Some(from), Some(to)) => Ok(table.range(from..to)?),
        (Some(from), None) => Ok(table.range(from..)?),
        (None, Some(to)) => Ok(table.range(..to)?),
        (None, None) => Ok(table.range(0..)?),
    }
}

/// Order two segment positions. Within one segment the file is append-only, so
/// the larger offset was written later; segments themselves are written in
/// ascending order.
fn written_later(a: &BlockLocation, b: &BlockLocation) -> bool {
    (a.segment_id, a.offset) > (b.segment_id, b.offset)
}

/// The Byron epoch-boundary sidecar.
///
/// A Byron EBB carries the same absolute slot as the first main block of the
/// epoch it opens, so a single `slot -> BlockLocation` index cannot hold both:
/// whichever arrives second overwrites the first, and the archive silently
/// loses one block per Byron epoch. This table has the same shape as
/// [`BlocksTable`] and holds exactly the epoch-boundary blocks, which keeps
/// `blocks` — and every point read that goes through it — untouched.
///
/// Ouroboros forbids two blocks at one slot in every post-Byron era, so the
/// EBB is definitionally the only collision and this table stays small: 208
/// rows on mainnet, 4 on preprod, none on preview.
pub struct EbbsTable;

impl EbbsTable {
    pub const DEF: IndexDef = TableDefinition::new("ebbs");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;
        Ok(())
    }

    /// Read the epoch-boundary block at `slot`, if the archive holds one.
    pub fn get_by_slot(
        rx: &ReadTransaction,
        flatfiles: &FlatFileStore,
        slot: BlockSlot,
    ) -> Result<Option<BlockBody>, Error> {
        match get_location(rx, Self::DEF, slot)? {
            Some(loc) => {
                let data = flatfiles
                    .read(&loc)
                    .map_err(super::RedbArchiveError::from_io)?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }
}

pub struct BlocksTable;

impl BlocksTable {
    pub const DEF: IndexDef = TableDefinition::new("blocks");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;
        EbbsTable::initialize(wx)?;
        Ok(())
    }

    /// The tip of the archive.
    ///
    /// Both indexes are consulted so an archive whose last written block is an
    /// epoch-boundary block reports that block rather than the previous
    /// epoch's last main block. Where the two share a slot the main block
    /// wins, which is the boundary-slot semantics every point read already
    /// has.
    pub fn get_tip(
        rx: &ReadTransaction,
        flatfiles: &FlatFileStore,
    ) -> Result<Option<(BlockSlot, BlockBody)>, Error> {
        let main = last_of(rx, Self::DEF)?;
        let ebb = last_of(rx, EbbsTable::DEF)?;

        let result = match (main, ebb) {
            (Some(main), Some(ebb)) if ebb.0 > main.0 => Some(ebb),
            (Some(main), _) => Some(main),
            (None, ebb) => ebb,
        };

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

    /// Read the main block at `slot`.
    ///
    /// At a Byron boundary slot this is the epoch's first main block, never
    /// the EBB that shares the slot — the semantics callers already had.
    /// [`EbbsTable::get_by_slot`] reaches the other one.
    pub fn get_by_slot(
        rx: &ReadTransaction,
        flatfiles: &FlatFileStore,
        slot: BlockSlot,
    ) -> Result<Option<BlockBody>, Error> {
        match get_location(rx, Self::DEF, slot)? {
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
    ///
    /// Every block lands in the segment file; only the index row is routed,
    /// epoch-boundary blocks to `ebbs` and everything else to `blocks`. The
    /// caller hands blocks over in chain order and the batch sort upstream is
    /// stable, so an EBB's bytes precede those of the main block it shares a
    /// slot with — which is what lets the merged range iterator put them back
    /// in that order.
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
        let mut main = wx.open_table(Self::DEF)?;
        let mut ebbs = wx.open_table(EbbsTable::DEF)?;

        for (i, (point, body)) in blocks.iter().enumerate() {
            let bytes = locations[i].to_bytes();

            if is_ebb(body) {
                ebbs.insert(point.slot(), bytes.as_slice())?;
            } else {
                main.insert(point.slot(), bytes.as_slice())?;
            }
        }

        Ok(())
    }

    /// Undo the block at `point`.
    ///
    /// At a Byron boundary slot the two indexes both hold a row, and the one
    /// written later — the larger segment offset — is removed first. Rollback
    /// walks the chain backwards, so that is the entry it means, and reading
    /// the offset answers it without decoding either block.
    pub fn undo(
        wx: &WriteTransaction,
        flatfiles: &FlatFileStore,
        point: &ChainPoint,
    ) -> Result<(), Error> {
        let slot = point.slot();

        let main = get_location_mut(wx, Self::DEF, slot)?;
        let ebb = get_location_mut(wx, EbbsTable::DEF, slot)?;

        let (def, loc) = match (main, ebb) {
            (Some(main), Some(ebb)) => {
                if written_later(&ebb, &main) {
                    (EbbsTable::DEF, ebb)
                } else {
                    (Self::DEF, main)
                }
            }
            (Some(main), None) => (Self::DEF, main),
            (None, Some(ebb)) => (EbbsTable::DEF, ebb),
            (None, None) => return Ok(()),
        };

        // Remove from index.
        let mut table = wx.open_table(def)?;
        table.remove(slot)?;
        drop(table);

        // Truncate the segment file at this block's offset.
        flatfiles
            .truncate(loc.segment_id, loc.offset)
            .map_err(super::RedbArchiveError::from_io)?;

        Ok(())
    }

    /// The earliest block in the archive. At a shared slot the epoch-boundary
    /// block is the earlier of the two.
    pub fn first(rx: &ReadTransaction) -> Result<Option<(BlockSlot, BlockLocation)>, Error> {
        let main = first_of(rx, Self::DEF)?;
        let ebb = first_of(rx, EbbsTable::DEF)?;

        Ok(match (main, ebb) {
            (Some(main), Some(ebb)) if ebb.0 <= main.0 => Some(ebb),
            (Some(main), _) => Some(main),
            (None, ebb) => ebb,
        })
    }

    /// The latest block in the archive. At a shared slot the main block is the
    /// later of the two.
    pub fn last(rx: &ReadTransaction) -> Result<Option<(BlockSlot, BlockLocation)>, Error> {
        let main = last_of(rx, Self::DEF)?;
        let ebb = last_of(rx, EbbsTable::DEF)?;

        Ok(match (main, ebb) {
            (Some(main), Some(ebb)) if ebb.0 > main.0 => Some(ebb),
            (Some(main), _) => Some(main),
            (None, ebb) => ebb,
        })
    }

    pub fn remove_before(
        wx: &WriteTransaction,
        flatfiles: &FlatFileStore,
        slot: BlockSlot,
    ) -> Result<(), Error> {
        for def in [Self::DEF, EbbsTable::DEF] {
            let mut table = wx.open_table(def)?;
            let mut to_remove = table.extract_from_if(..slot, |_, _| true)?;

            while let Some(Ok((slot, _))) = to_remove.next() {
                trace!(slot = slot.value(), "removing block index entry");
            }
            drop(to_remove);
            drop(table);
        }

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
        // An EBB opening the first epoch after the cut sits before that
        // epoch's first main block in the segment, so taking the minimum
        // across both indexes is what keeps its bytes from surviving the
        // truncation.
        let mut earliest_after: Option<BlockLocation> = None;

        for def in [Self::DEF, EbbsTable::DEF] {
            let table = wx.open_table(def)?;
            let range = table.range((slot + 1)..)?;

            for entry in range {
                let (_, loc_bytes) = entry?;
                let loc = BlockLocation::from_bytes(loc_bytes.value());

                match &earliest_after {
                    None => earliest_after = Some(loc),
                    Some(prev) => {
                        if written_later(prev, &loc) {
                            earliest_after = Some(loc);
                        }
                    }
                }
            }
        }

        // Remove index entries.
        for def in [Self::DEF, EbbsTable::DEF] {
            let mut table = wx.open_table(def)?;
            let mut to_remove = table.extract_from_if(slot.., |x, _| x > slot)?;

            while let Some(Ok((slot, _))) = to_remove.next() {
                trace!(slot = slot.value(), "removing block index entry");
            }
            drop(to_remove);
            drop(table);
        }

        // Truncate the segment file if we found entries to remove.
        if let Some(loc) = earliest_after {
            flatfiles
                .truncate(loc.segment_id, loc.offset)
                .map_err(super::RedbArchiveError::from_io)?;
        }

        Ok(())
    }

    /// Get a range of (slot, BlockLocation) from each index.
    ///
    /// The two are returned separately and merged by the iterator that reads
    /// them; block data is NOT fetched here, so callers page through index
    /// keys and only pay for the bodies they keep.
    pub fn get_range(
        rx: &ReadTransaction,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<(IndexRange, IndexRange), Error> {
        let main = range_of(rx, Self::DEF, from, to)?;
        let ebbs = range_of(rx, EbbsTable::DEF, from, to)?;

        Ok((main, ebbs))
    }
}
