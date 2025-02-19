use ::redb::{ReadTransaction, ReadableTable as _};
use ::redb::{TableDefinition, WriteTransaction};
use pallas::ledger::traverse::MultiEraBlock;

use crate::ledger::LedgerDelta;
use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct BlockNumberApproxIndexTable;
impl BlockNumberApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> =
        TableDefinition::new("blocknumberapproxindex");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

    pub fn compute_key(block_number: u64) -> u64 {
        // Left for readability
        block_number
    }

    pub fn get_by_block_number(
        rx: &ReadTransaction,
        block_number: u64,
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(block_number);
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        if let Some(point) = &delta.new_position {
            let block =
                MultiEraBlock::decode(&delta.new_block).map_err(Error::BlockDecodingError)?;

            let key = Self::compute_key(block.number());
            let slot = point.0;

            let maybe_new = match table.get(key)? {
                Some(value) => {
                    let mut previous = value.value().clone();
                    if !previous.contains(&slot) {
                        previous.push(slot);
                        Some(previous)
                    } else {
                        None
                    }
                }
                None => Some(vec![slot]),
            };
            if let Some(new) = maybe_new {
                table.insert(key, new)?;
            }
        }

        if let Some(point) = &delta.undone_position {
            let block =
                MultiEraBlock::decode(&delta.undone_block).map_err(Error::BlockDecodingError)?;

            let key = Self::compute_key(block.number());
            let slot = point.0;

            let maybe_new = match table.get(key)? {
                Some(value) => {
                    let mut previous = value.value().clone();
                    match previous.iter().position(|x| *x == slot) {
                        Some(index) => {
                            previous.remove(index);
                            Some(previous)
                        }
                        None => None,
                    }
                }
                None => None,
            };
            if let Some(new) = maybe_new {
                table.insert(key, new)?;
            }
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
