use dolos_core::{ArchiveError, ArchiveStore, BlockBody, BlockSlot, LedgerDelta};

pub mod redb;

/// A persistent store for ledger state
#[derive(Clone)]
#[non_exhaustive]
pub enum ChainStore {
    Redb(redb::ChainStore),
}

impl ArchiveStore for ChainStore {
    type BlockIter<'a> = ChainIter<'a>;

    fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, ArchiveError> {
        let out = match self {
            ChainStore::Redb(x) => x.get_block_by_hash(block_hash)?,
        };

        Ok(out)
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        let out = match self {
            ChainStore::Redb(x) => x.get_block_by_slot(slot)?,
        };

        Ok(out)
    }

    fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, ArchiveError> {
        let out = match self {
            ChainStore::Redb(x) => x.get_block_by_number(number)?,
        };

        Ok(out)
    }

    fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<Vec<u8>>, ArchiveError> {
        let out = match self {
            ChainStore::Redb(x) => x.get_tx(tx_hash)?,
        };

        Ok(out)
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        let out = match self {
            ChainStore::Redb(x) => x.get_range(from, to)?.into(),
        };

        Ok(out)
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        let out = match self {
            ChainStore::Redb(x) => x.get_tip()?,
        };

        Ok(out)
    }

    fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), ArchiveError> {
        let out = match self {
            ChainStore::Redb(x) => x.apply(deltas)?,
        };

        Ok(out)
    }

    fn housekeeping(&mut self) -> Result<(), ArchiveError> {
        let out = match self {
            ChainStore::Redb(x) => x.housekeeping()?,
        };

        Ok(out)
    }

    fn finalize(&self, until: BlockSlot) -> Result<(), ArchiveError> {
        let out = match self {
            ChainStore::Redb(x) => x.finalize(until)?,
        };

        Ok(out)
    }
}

impl From<redb::ChainStore> for ChainStore {
    fn from(value: redb::ChainStore) -> Self {
        Self::Redb(value)
    }
}

pub enum ChainIter<'a> {
    Redb(redb::ChainIter<'a>),
}

impl Iterator for ChainIter<'_> {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ChainIter::Redb(chainiter) => chainiter.next(),
        }
    }
}

impl DoubleEndedIterator for ChainIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            ChainIter::Redb(chainiter) => chainiter.next_back(),
        }
    }
}

impl<'a> From<redb::ChainIter<'a>> for ChainIter<'a> {
    fn from(value: redb::ChainIter<'a>) -> Self {
        Self::Redb(value)
    }
}
