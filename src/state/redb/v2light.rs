use ::redb::{Database, Durability, Error};
use itertools::Itertools;
use std::sync::Arc;

use crate::ledger::*;

use super::tables;

#[derive(Clone)]
pub struct LedgerStore(Arc<Database>);

impl LedgerStore {
    pub fn new(db: Database) -> Self {
        LedgerStore(db.into())
    }

    pub(crate) fn db(&self) -> &Database {
        &self.0
    }

    pub fn initialize(db: Database) -> Result<Self, Error> {
        let mut wx = db.begin_write()?;
        wx.set_durability(Durability::Immediate);

        tables::CursorTable::initialize(&wx)?;
        tables::UtxosTable::initialize(&wx)?;
        tables::PParamsTable::initialize(&wx)?;

        wx.commit()?;

        Ok(Self(db.into()))
    }

    pub fn is_empty(&self) -> Result<bool, Error> {
        self.cursor().map(|x| x.is_none())
    }

    pub fn cursor(&self) -> Result<Option<ChainPoint>, Error> {
        let rx = self.db().begin_read()?;

        let last = tables::CursorTable::last(&rx)?.map(|(k, v)| ChainPoint(k, v.hash));

        Ok(last)
    }

    pub fn apply(&mut self, deltas: &[LedgerDelta]) -> Result<(), Error> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Eventual);

        for delta in deltas {
            tables::CursorTable::apply(&wx, delta)?;
            tables::UtxosTable::apply(&wx, delta)?;
            tables::PParamsTable::apply(&wx, delta)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn finalize(&mut self, until: BlockSlot) -> Result<(), Error> {
        let rx = self.db().begin_read()?;
        let cursors = tables::CursorTable::get_range(&rx, until)?;

        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Eventual);

        for (slot, value) in cursors {
            tables::CursorTable::compact(&wx, slot)?;
            tables::UtxosTable::compact(&wx, slot, &value.tombstones)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, Error> {
        // exit early before opening a read tx in case there's nothing to fetch
        if refs.is_empty() {
            return Ok(Default::default());
        }

        let rx = self.db().begin_read()?;
        tables::UtxosTable::get_sparse(&rx, refs)
    }

    pub fn get_pparams(&self, until: BlockSlot) -> Result<Vec<PParamsBody>, Error> {
        let rx = self.db().begin_read()?;
        tables::PParamsTable::get_range(&rx, until)
    }

    /// Upgrades a v2-light store to v2 by adding indexes
    ///
    /// This method will fail if the store has been cloned and those instances
    /// are still active.
    pub fn upgrade(self) -> Result<Database, Error> {
        let db = Arc::try_unwrap(self.0).unwrap();

        let mut wx = db.begin_write()?;
        wx.set_durability(Durability::Eventual);

        tables::FilterIndexes::initialize(&wx)?;

        let rx = db.begin_read()?;

        let utxo_chunks = tables::UtxosTable::iter(&rx)?.chunks(1000);

        for chunk in utxo_chunks.into_iter() {
            let chunk: Vec<_> = chunk.try_collect()?;

            let delta = LedgerDelta {
                produced_utxo: chunk.into_iter().collect(),
                new_position: Default::default(),
                undone_position: Default::default(),
                consumed_utxo: Default::default(),
                recovered_stxi: Default::default(),
                undone_utxo: Default::default(),
                new_pparams: Default::default(),
            };

            tables::FilterIndexes::apply(&wx, &delta)?;
        }

        wx.commit()?;

        Ok(db)
    }
}
