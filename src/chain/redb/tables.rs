use ::redb::{ReadTransaction, ReadableTable as _};
use ::redb::{TableDefinition, WriteTransaction};

use crate::ledger::LedgerDelta;
use crate::model::{BlockBody, BlockSlot};

type Error = crate::chain::ChainError;

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

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;
        if let Some(point) = &delta.new_position {
            let slot = point.0;
            table.insert(slot, delta.new_block.clone())?;
        }

        if let Some(point) = &delta.undone_position {
            let slot = point.0;
            table.remove(slot)?;
        }

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
}
