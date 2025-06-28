use ::redb::{Database, Durability};
use itertools::Itertools;
use pallas::ledger::traverse::MultiEraBlock;
use std::sync::Arc;
use tracing::{debug, info};

type Error = super::RedbArchiveError;

use dolos_core::{ArchiveError, BlockBody, BlockSlot, EraCbor, LedgerDelta, TxOrder};

use super::{indexes, tables, ChainIter};

#[derive(Clone)]
pub struct ChainStore {
    db: Arc<Database>,
}

impl ChainStore {
    pub fn initialize(db: Database) -> Result<Self, Error> {
        let mut wx = db.begin_write()?;
        wx.set_durability(Durability::Immediate);

        indexes::Indexes::initialize(&wx)?;
        tables::BlocksTable::initialize(&wx)?;

        wx.commit()?;

        Ok(Self { db: Arc::new(db) })
    }

    pub(crate) fn db(&self) -> &Database {
        &self.db
    }

    pub(crate) fn db_mut(&mut self) -> Option<&mut Database> {
        Arc::get_mut(&mut self.db)
    }

    pub fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), Error> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Eventual);
        wx.set_quick_repair(true);

        for delta in deltas {
            indexes::Indexes::apply(&wx, delta)?;
            tables::BlocksTable::apply(&wx, delta)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn copy(&self, target: &Self) -> Result<(), Error> {
        let rx = self.db().begin_read()?;
        let wx = target.db().begin_write()?;

        indexes::Indexes::copy(&rx, &wx)?;
        tables::BlocksTable::copy(&rx, &wx)?;

        wx.commit()?;

        Ok(())
    }

    pub fn rebuild_indexes(&self) -> Result<(), Error> {
        let rx = self.db().begin_read()?;
        let wx = self.db().begin_write()?;

        indexes::Indexes::nuke(&wx)?;

        for block in tables::BlocksTable::get_range(&rx, None, None)? {
            let (slot, block) = block?;
            indexes::Indexes::index_block(&wx, &block)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<ChainIter<'a>, Error> {
        let rx = self.db().begin_read()?;
        let range = tables::BlocksTable::get_range(&rx, from, to)?;
        Ok(ChainIter(range))
    }

    pub fn get_possible_block_slots_by_address(
        &self,
        address: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_address(&rx, address)
    }

    pub fn get_possible_block_slots_by_address_payment_part(
        &self,
        address_payment_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_address_payment_part(&rx, address_payment_part)
    }

    pub fn get_possible_block_slots_by_address_stake_part(
        &self,
        address_stake_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_address_stake_part(&rx, address_stake_part)
    }

    pub fn get_possible_block_slots_by_asset(&self, asset: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_asset(&rx, asset)
    }

    pub fn get_possible_block_slots_by_block_hash(
        &self,
        block_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_block_hash(&rx, block_hash)
    }

    pub fn get_possible_block_slots_by_block_number(
        &self,
        block_number: &u64,
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_block_number(&rx, block_number)
    }

    pub fn get_possible_block_slots_by_datum_hash(
        &self,
        datum_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_datum_hash(&rx, datum_hash)
    }

    pub fn get_possible_block_slots_by_policy(
        &self,
        policy: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_policy(&rx, policy)
    }

    pub fn get_possible_block_slots_by_script_hash(
        &self,
        script_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_script_hash(&rx, script_hash)
    }

    pub fn get_possible_block_slots_by_tx_hash(
        &self,
        tx_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_tx_hash(&rx, tx_hash)
    }

    pub fn get_possible_blocks_by_address(&self, address: &[u8]) -> Result<Vec<BlockBody>, Error> {
        self.get_possible_block_slots_by_address(address)?
            .iter()
            .sorted()
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

    pub fn get_possible_blocks_by_asset(&self, asset: &[u8]) -> Result<Vec<BlockBody>, Error> {
        self.get_possible_block_slots_by_asset(asset)?
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

    pub fn get_possible_blocks_by_block_number(
        &self,
        block_number: &u64,
    ) -> Result<Vec<BlockBody>, Error> {
        self.get_possible_block_slots_by_block_number(block_number)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_datum_hash(
        &self,
        datum_hash: &[u8],
    ) -> Result<Vec<BlockBody>, Error> {
        self.get_possible_block_slots_by_datum_hash(datum_hash)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_policy(&self, policy: &[u8]) -> Result<Vec<BlockBody>, Error> {
        self.get_possible_block_slots_by_policy(policy)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_script_hash(
        &self,
        script_hash: &[u8],
    ) -> Result<Vec<BlockBody>, Error> {
        self.get_possible_block_slots_by_script_hash(script_hash)?
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

    pub fn get_block_with_tx(&self, tx_hash: &[u8]) -> Result<Option<(BlockBody, TxOrder)>, Error> {
        let possible = self.get_possible_blocks_by_tx_hash(tx_hash)?;
        for raw in possible {
            let block = MultiEraBlock::decode(&raw).map_err(ArchiveError::BlockDecodingError)?;
            for (idx, tx) in block.txs().iter().enumerate() {
                if tx.hash().to_vec() == tx_hash {
                    return Ok(Some((raw, idx)));
                }
            }
        }
        Ok(None)
    }

    pub fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, Error> {
        let rx = self.db().begin_read()?;
        tables::BlocksTable::get_by_slot(&rx, *slot)
    }

    pub fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, Error> {
        let possible = self.get_possible_blocks_by_block_hash(block_hash)?;
        for raw in possible {
            let block = MultiEraBlock::decode(&raw).map_err(ArchiveError::BlockDecodingError)?;
            if *block.hash() == *block_hash {
                return Ok(Some(raw));
            }
        }
        Ok(None)
    }

    pub fn get_block_by_number(&self, block_number: &u64) -> Result<Option<BlockBody>, Error> {
        let possible = self.get_possible_blocks_by_block_number(block_number)?;
        for raw in possible {
            let block = MultiEraBlock::decode(&raw).map_err(ArchiveError::BlockDecodingError)?;
            if block.number() == *block_number {
                return Ok(Some(raw));
            }
        }
        Ok(None)
    }

    pub fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, Error> {
        let mut possible = self.get_possible_block_slots_by_tx_hash(tx_hash)?;
        if possible.len() == 1 {
            Ok(possible.pop())
        } else {
            for slot in possible {
                if let Some(raw) = self.get_block_by_slot(&slot)? {
                    let block =
                        MultiEraBlock::decode(&raw).map_err(ArchiveError::BlockDecodingError)?;
                    if block.txs().iter().any(|x| x.hash().to_vec() == tx_hash) {
                        return Ok(Some(slot));
                    }
                }
            }
            Ok(None)
        }
    }

    pub fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, Error> {
        let possible = self.get_possible_blocks_by_tx_hash(tx_hash)?;
        for raw in possible {
            let block = MultiEraBlock::decode(&raw).map_err(ArchiveError::BlockDecodingError)?;
            if let Some(tx) = block.txs().iter().find(|x| x.hash().to_vec() == tx_hash) {
                return Ok(Some(EraCbor(block.era().into(), tx.encode())));
            }
        }
        Ok(None)
    }

    pub fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, Error> {
        let rx = self.db().begin_read()?;
        tables::BlocksTable::get_tip(&rx)
    }

    pub fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, Error> {
        let rx = self.db().begin_read()?;
        let start = match tables::BlocksTable::first(&rx)? {
            Some((slot, _)) => slot,
            None => {
                debug!("no start point found on chain, skipping housekeeping");
                return Ok(true);
            }
        };

        let last = match tables::BlocksTable::last(&rx)? {
            Some((slot, _)) => slot,
            None => {
                debug!("no tip found on chain, skipping housekeeping");
                return Ok(true);
            }
        };

        let delta = last.saturating_sub(start);
        let excess = delta.saturating_sub(max_slots);

        debug!(delta, excess, last, start, "chain history delta computed");

        if excess == 0 {
            debug!(delta, max_slots, excess, "no pruning necessary on chain");
            return Ok(true);
        }

        let (done, max_prune) = match max_prune {
            Some(max) => (excess <= max, core::cmp::min(excess, max)),
            None => (true, excess),
        };

        let prune_before = start + max_prune;

        info!(
            cutoff_slot = prune_before,
            start, excess, "pruning archive for excess history"
        );

        let mut wx = self.db().begin_write()?;
        wx.set_quick_repair(true);

        tables::BlocksTable::remove_before(&wx, prune_before)?;

        wx.commit()?;

        Ok(done)
    }
}

impl From<Database> for ChainStore {
    fn from(value: Database) -> Self {
        Self {
            db: Arc::new(value),
        }
    }
}
