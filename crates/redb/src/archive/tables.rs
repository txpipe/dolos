use ::redb::{ReadTransaction, ReadableTable as _};
use ::redb::{TableDefinition, WriteTransaction};
use redb::Range;
use tracing::trace;

use dolos_core::{BlockBody, BlockSlot, ChainPoint, RawBlock};

type Error = super::RedbArchiveError;

pub struct BlocksTable;

impl BlocksTable {
    pub const DEF: TableDefinition<'static, BlockSlot, BlockBody> = TableDefinition::new("blocks");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

    pub fn get_tip(rx: &ReadTransaction) -> Result<Option<(BlockSlot, BlockBody)>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let result = table
            .last()?
            .map(|(slot, raw)| (slot.value(), raw.value().clone()));
        Ok(result)
    }

    pub fn get_by_slot(rx: &ReadTransaction, slot: BlockSlot) -> Result<Option<BlockBody>, Error> {
        let table = rx.open_table(Self::DEF)?;
        match table.get(slot)? {
            Some(value) => Ok(Some(value.value().clone())),
            None => Ok(None),
        }
    }

    pub fn apply(wx: &WriteTransaction, point: &ChainPoint, block: &RawBlock) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        let slot = point.slot();
        table.insert(slot, block.clone())?;

        Ok(())
    }

    pub fn undo(wx: &WriteTransaction, point: &ChainPoint) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        let slot = point.slot();
        table.remove(slot)?;

        Ok(())
    }

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        let source = rx.open_table(Self::DEF)?;
        let mut target = wx.open_table(Self::DEF)?;

        for entry in source.iter()? {
            let (k, v) = entry?;
            target.insert(k.value(), v.value())?;
        }

        Ok(())
    }

    pub fn first(rx: &ReadTransaction) -> Result<Option<(BlockSlot, BlockBody)>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let result = table
            .first()?
            .map(|(slot, raw)| (slot.value(), raw.value().clone()));
        Ok(result)
    }

    pub fn last(rx: &ReadTransaction) -> Result<Option<(BlockSlot, BlockBody)>, Error> {
        Self::get_tip(rx)
    }

    pub fn remove_before(wx: &WriteTransaction, slot: BlockSlot) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;
        let mut to_remove = table.extract_from_if(..slot, |_, _| true)?;

        while let Some(Ok((slot, _))) = to_remove.next() {
            trace!(slot = slot.value(), "removing log entry");
        }

        Ok(())
    }

    pub fn remove_after(wx: &WriteTransaction, slot: BlockSlot) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;
        let mut to_remove = table.extract_from_if(slot.., |x, _| x > slot)?;

        while let Some(Ok((slot, _))) = to_remove.next() {
            trace!(slot = slot.value(), "removing log entry");
        }

        Ok(())
    }

    pub fn get_range<'a>(
        rx: &ReadTransaction,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Range<'a, u64, Vec<u8>>, Error> {
        let table = rx.open_table(Self::DEF)?;
        match (from, to) {
            (Some(from), Some(to)) => Ok(table.range(from..to)?),
            (Some(from), None) => Ok(table.range(from..)?),
            (None, Some(to)) => Ok(table.range(..to)?),
            (None, None) => Ok(table.range(0..)?),
        }
    }
}
