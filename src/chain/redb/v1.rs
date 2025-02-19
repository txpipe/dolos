use ::redb::{Database, Durability};
use pallas::ledger::traverse::MultiEraBlock;
use std::sync::Arc;

type Error = crate::chain::ChainError;

use super::{indexes, tables, LedgerDelta};
use crate::model::{BlockBody, BlockSlot};

#[derive(Clone)]
pub struct ChainStore(pub Arc<Database>);

impl ChainStore {
    pub fn initialize(db: Database) -> Result<Self, Error> {
        let mut wx = db.begin_write()?;
        wx.set_durability(Durability::Immediate);

        indexes::AddressApproxIndexTable::initialize(&wx)?;
        indexes::AddressPaymentPartApproxIndexTable::initialize(&wx)?;
        indexes::AddressStakePartApproxIndexTable::initialize(&wx)?;
        indexes::BlockHashApproxIndexTable::initialize(&wx)?;
        indexes::TxsApproxIndexTable::initialize(&wx)?;
        tables::BlocksTable::initialize(&wx)?;

        wx.commit()?;

        Ok(db.into())
    }

    pub(crate) fn db(&self) -> &Database {
        &self.0
    }

    pub(crate) fn db_mut(&mut self) -> Option<&mut Database> {
        Arc::get_mut(&mut self.0)
    }

    pub fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), Error> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Eventual);

        for delta in deltas {
            indexes::AddressApproxIndexTable::apply(&wx, delta)?;
            indexes::AddressPaymentPartApproxIndexTable::apply(&wx, delta)?;
            indexes::AddressStakePartApproxIndexTable::apply(&wx, delta)?;
            indexes::BlockHashApproxIndexTable::apply(&wx, delta)?;
            indexes::TxsApproxIndexTable::apply(&wx, delta)?;
            tables::BlocksTable::apply(&wx, delta)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn copy(&self, target: &Self) -> Result<(), Error> {
        let rx = self.db().begin_read()?;
        let wx = target.db().begin_write()?;

        indexes::AddressApproxIndexTable::copy(&rx, &wx)?;
        indexes::AddressPaymentPartApproxIndexTable::copy(&rx, &wx)?;
        indexes::AddressStakePartApproxIndexTable::copy(&rx, &wx)?;
        indexes::BlockHashApproxIndexTable::copy(&rx, &wx)?;
        indexes::TxsApproxIndexTable::copy(&rx, &wx)?;
        tables::BlocksTable::copy(&rx, &wx)?;

        wx.commit()?;

        Ok(())
    }

    pub fn finalize(&self, _: BlockSlot) -> Result<(), Error> {
        Ok(())
    }

    pub fn get_possible_block_slots_by_address(
        &self,
        address: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::AddressApproxIndexTable::get_by_address(&rx, address)
    }

    pub fn get_possible_block_slots_by_address_payment_part(
        &self,
        address_payment_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::AddressPaymentPartApproxIndexTable::get_by_address_payment_part(
            &rx,
            address_payment_part,
        )
    }

    pub fn get_possible_block_slots_by_address_stake_part(
        &self,
        address_stake_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::AddressStakePartApproxIndexTable::get_by_address_stake_part(
            &rx,
            address_stake_part,
        )
    }

    pub fn get_possible_block_slots_by_tx_hash(
        &self,
        tx_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::TxsApproxIndexTable::get_by_tx_hash(&rx, tx_hash)
    }

    pub fn get_possible_block_slots_by_block_hash(
        &self,
        block_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::BlockHashApproxIndexTable::get_by_block_hash(&rx, block_hash)
    }

    pub fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, Error> {
        let rx = self.db().begin_read()?;
        tables::BlocksTable::get_by_slot(&rx, *slot)
    }

    pub fn get_possible_blocks_by_address(&self, address: &[u8]) -> Result<Vec<BlockBody>, Error> {
        self.get_possible_block_slots_by_address(address)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_tx_hash(&self, tx_hash: &[u8]) -> Result<Vec<BlockBody>, Error> {
        self.get_possible_block_slots_by_tx_hash(tx_hash)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_block_hash(
        &self,
        block_hash: &[u8],
    ) -> Result<Vec<BlockBody>, Error> {
        self.get_possible_block_slots_by_block_hash(block_hash)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_address_payment_part(
        &self,
        address_payment_part: &[u8],
    ) -> Result<Vec<BlockBody>, Error> {
        self.get_possible_block_slots_by_address_payment_part(address_payment_part)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_address_stake_part(
        &self,
        address_stake_part: &[u8],
    ) -> Result<Vec<BlockBody>, Error> {
        self.get_possible_block_slots_by_address_stake_part(address_stake_part)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, Error> {
        let possible = self.get_possible_blocks_by_block_hash(block_hash)?;
        for raw in possible {
            let block = MultiEraBlock::decode(&raw).map_err(Error::BlockDecodingError)?;
            if *block.hash() == *block_hash {
                return Ok(Some(raw));
            }
        }
        Ok(None)
    }

    pub fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, Error> {
        let rx = self.db().begin_read()?;
        tables::BlocksTable::get_tip(&rx)
    }
}

impl From<Database> for ChainStore {
    fn from(value: Database) -> Self {
        Self(Arc::new(value))
    }
}
