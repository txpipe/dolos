use ::redb::{Database, Durability, Error};
use std::sync::Arc;

use crate::ledger::*;

use super::tables;

#[derive(Clone)]
pub struct LedgerStore(pub Arc<Database>);

impl LedgerStore {
    pub fn is_empty(&self) -> bool {
        match self.cursor() {
            Ok(x) => x.is_none(),
            Err(_) => false,
        }
    }

    pub fn cursor(&self) -> Result<Option<ChainPoint>, Error> {
        let rx = self.0.begin_read()?;
        tables::TombstonesV2Table::last(&rx)
    }

    pub fn apply(&mut self, deltas: &[LedgerDelta]) -> Result<(), Error> {
        let mut wx = self.0.begin_write()?;
        wx.set_durability(Durability::Eventual);

        for delta in deltas {
            tables::UtxosTable::apply(&wx, delta)?;
            tables::PParamsTable::apply(&wx, delta)?;
            tables::TombstonesV2Table::apply(&wx, delta)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn finalize(&mut self, until: BlockSlot) -> Result<(), Error> {
        let rx = self.0.begin_read()?;
        let tss = tables::TombstonesV2Table::get_range(&rx, until)?;

        let mut wx = self.0.begin_write()?;
        wx.set_durability(Durability::Eventual);

        for ts in tss {
            let (slot, txos) = ts;
            tables::UtxosTable::compact(&wx, slot, &txos)?;
            tables::TombstonesV2Table::compact(&wx, slot)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, Error> {
        // exit early before opening a read tx in case there's nothing to fetch
        if refs.is_empty() {
            return Ok(Default::default());
        }

        let rx = self.0.begin_read()?;
        tables::UtxosTable::get_sparse(&rx, refs)
    }

    pub fn get_pparams(&self, until: BlockSlot) -> Result<Vec<PParamsBody>, Error> {
        let rx = self.0.begin_read()?;
        tables::PParamsTable::get_range(&rx, until)
    }
}

impl From<Database> for LedgerStore {
    fn from(value: Database) -> Self {
        Self(Arc::new(value))
    }
}
