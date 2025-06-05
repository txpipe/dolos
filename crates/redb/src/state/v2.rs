use ::redb::{Database, Durability};
use std::sync::Arc;

use super::tables;
use crate::state::*;

type Error = super::RedbStateError;

#[derive(Clone)]
pub struct LedgerStore(Arc<Database>);

impl LedgerStore {
    pub fn new(db: Database) -> Self {
        LedgerStore(db.into())
    }

    pub(crate) fn db(&self) -> &Database {
        &self.0
    }

    pub(crate) fn db_mut(&mut self) -> Option<&mut Database> {
        Arc::get_mut(&mut self.0)
    }

    pub fn initialize(db: Database) -> Result<Self, Error> {
        let mut wx = db.begin_write()?;
        wx.set_durability(Durability::Immediate);

        tables::CursorTable::initialize(&wx)?;
        tables::UtxosTable::initialize(&wx)?;
        tables::PParamsTable::initialize(&wx)?;
        tables::FilterIndexes::initialize(&wx)?;

        wx.commit()?;

        Ok(Self(db.into()))
    }

    pub fn is_empty(&self) -> Result<bool, Error> {
        self.cursor().map(|x| x.is_none())
    }

    pub fn cursor(&self) -> Result<Option<ChainPoint>, Error> {
        let rx = self.db().begin_read()?;

        let last = tables::CursorTable::last(&rx)?.map(|(k, v)| ChainPoint::Specific(k, v.hash));

        Ok(last)
    }

    pub fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), Error> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Eventual);

        for delta in deltas {
            tables::CursorTable::apply(&wx, delta)?;
            tables::UtxosTable::apply(&wx, delta)?;
            tables::PParamsTable::apply(&wx, delta)?;
            tables::FilterIndexes::apply(&wx, delta)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn finalize(&self, until: BlockSlot) -> Result<(), Error> {
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

    pub fn copy(&self, target: &Self) -> Result<(), Error> {
        let rx = self.db().begin_read()?;
        let wx = target.db().begin_write()?;

        tables::CursorTable::copy(&rx, &wx)?;
        tables::UtxosTable::copy(&rx, &wx)?;
        tables::PParamsTable::copy(&rx, &wx)?;
        tables::FilterIndexes::copy(&rx, &wx)?;

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

    pub fn get_utxos_by_address(&self, address: &[u8]) -> Result<UtxoSet, Error> {
        let rx = self.db().begin_read()?;
        tables::FilterIndexes::get_by_address(&rx, address)
    }

    pub fn get_utxos_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, Error> {
        let rx = self.db().begin_read()?;
        tables::FilterIndexes::get_by_payment(&rx, payment)
    }

    pub fn get_utxos_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, Error> {
        let rx = self.db().begin_read()?;
        tables::FilterIndexes::get_by_stake(&rx, stake)
    }

    pub fn get_utxos_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, Error> {
        let rx = self.db().begin_read()?;
        tables::FilterIndexes::get_by_policy(&rx, policy)
    }

    pub fn get_utxos_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, Error> {
        let rx = self.db().begin_read()?;
        tables::FilterIndexes::get_by_asset(&rx, asset)
    }
}
