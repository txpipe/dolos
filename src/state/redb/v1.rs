use ::redb::{Database, Durability};
use std::sync::Arc;

use super::tables;
use crate::state::*;

type Error = super::RedbStateError;

#[derive(Clone)]
pub struct LedgerStore(pub Arc<Database>);

impl LedgerStore {
    pub fn initialize(db: Database) -> Result<Self, Error> {
        let mut wx = db.begin_write()?;
        wx.set_durability(Durability::Immediate);

        tables::UtxosTable::initialize(&wx)?;
        tables::PParamsTable::initialize(&wx)?;
        tables::TombstonesTable::initialize(&wx)?;
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

    pub fn is_empty(&self) -> Result<bool, Error> {
        Ok(self.cursor()?.is_none())
    }

    pub fn cursor(&self) -> Result<Option<ChainPoint>, Error> {
        let rx = self.db().begin_read()?;
        tables::BlocksTable::last(&rx)
    }

    pub fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), Error> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Eventual);

        for delta in deltas {
            tables::UtxosTable::apply(&wx, delta)?;
            tables::PParamsTable::apply(&wx, delta)?;
            tables::TombstonesTable::apply(&wx, delta)?;
            tables::BlocksTable::apply(&wx, delta)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn finalize(&self, until: BlockSlot) -> Result<(), Error> {
        let rx = self.db().begin_read()?;
        let tss = tables::TombstonesTable::get_range(&rx, until)?;

        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Eventual);

        for ts in tss {
            let (slot, txos) = ts;
            tables::UtxosTable::compact(&wx, slot, &txos)?;
            tables::TombstonesTable::compact(&wx, slot, &txos)?;
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

    pub fn get_pparams(&self, until: BlockSlot) -> Result<Vec<EraCbor>, Error> {
        let rx = self.db().begin_read()?;
        tables::PParamsTable::get_range(&rx, until)
    }
}

impl From<Database> for LedgerStore {
    fn from(value: Database) -> Self {
        Self(Arc::new(value))
    }
}
