use ::redb::{Database, Durability};
use itertools::Itertools;
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
        wx.set_quick_repair(true);

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

        let last = tables::CursorTable::last(&rx)?.map(|(k, v)| ChainPoint::Specific(k, v.hash));

        Ok(last)
    }

    pub fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), Error> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Eventual);
        wx.set_quick_repair(true);

        for delta in deltas {
            tables::CursorTable::apply(&wx, delta)?;
            tables::UtxosTable::apply(&wx, delta)?;
            tables::PParamsTable::apply(&wx, delta)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, Error> {
        let rx = self.db().begin_read()?;
        let start = match tables::CursorTable::first(&rx)? {
            Some((slot, _)) => slot,
            None => {
                debug!("no start point found on ledger, skipping housekeeping");
                return Ok(true);
            }
        };

        let last = match tables::CursorTable::last(&rx)? {
            Some((slot, _)) => slot,
            None => {
                debug!("no tip found on chain, skipping housekeeping");
                return Ok(true);
            }
        };

        let delta = last.saturating_sub(start);
        let excess = delta.saturating_sub(max_slots);

        debug!(delta, excess, last, start, "ledger history delta computed");

        if excess == 0 {
            debug!(delta, max_slots, excess, "no pruning necessary on ledger");
            return Ok(true);
        }

        let (done, max_prune) = match max_prune {
            Some(max) => (excess <= max, core::cmp::min(excess, max)),
            None => (true, excess),
        };

        let prune_before = start + max_prune;

        info!(
            cutoff_slot = prune_before,
            start, excess, "pruning ledger for excess history"
        );

        let cursors = tables::CursorTable::get_range(&rx, prune_before)?;

        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Eventual);
        wx.set_quick_repair(true);

        for (slot, value) in cursors {
            tables::CursorTable::compact(&wx, slot)?;
            tables::UtxosTable::compact(&wx, slot, &value.tombstones)?;
        }

        wx.commit()?;

        Ok(done)
    }

    pub fn copy(&self, target: &Self) -> Result<(), Error> {
        let rx = self.db().begin_read()?;
        let mut wx = target.db().begin_write()?;
        wx.set_quick_repair(true);

        tables::CursorTable::copy(&rx, &wx)?;
        tables::UtxosTable::copy(&rx, &wx)?;
        tables::PParamsTable::copy(&rx, &wx)?;

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

    /// Upgrades a v2-light store to v2 by adding indexes
    ///
    /// This method will fail if the store has been cloned and those instances
    /// are still active.
    pub fn upgrade(self) -> Result<Database, Error> {
        let db = Arc::try_unwrap(self.0).unwrap();

        let mut wx = db.begin_write()?;
        wx.set_durability(Durability::Eventual);
        wx.set_quick_repair(true);

        tables::FilterIndexes::initialize(&wx)?;

        let rx = db.begin_read()?;

        let utxo_chunks = tables::UtxosTable::iter(&rx)?.chunks(1000);

        for chunk in utxo_chunks.into_iter() {
            let chunk: Vec<_> = chunk.try_collect()?;

            let delta = LedgerDelta {
                produced_utxo: chunk.into_iter().collect(),
                ..Default::default()
            };

            tables::FilterIndexes::apply(&wx, &delta)?;
        }

        wx.commit()?;

        Ok(db)
    }
}
