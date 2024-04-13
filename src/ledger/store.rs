use super::*;
use redb::{MultimapTableDefinition, TableDefinition, WriteTransaction};

trait LedgerTable {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), redb::Error>;
}

const UTXOS: TableDefinition<(&[u8; 32], u32), (Era, &[u8])> = TableDefinition::new("utxos");
struct UtxosTable;

impl LedgerTable for UtxosTable {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), redb::Error> {
        let mut table = wx.open_table(UTXOS)?;

        for (k, v) in delta.produced_utxo.iter() {
            let k: (&[u8; 32], u32) = (&k.0, k.1);
            let v: (u16, &[u8]) = (v.0, &v.1);
            table.insert(k, v)?;
        }

        for k in delta.undone_utxo.iter() {
            let k: (&[u8; 32], u32) = (&k.0, k.1);
            table.remove(k)?;
        }

        Ok(())
    }
}

pub const TOMBSTONES: MultimapTableDefinition<BlockSlot, (&[u8; 32], TxoIdx)> =
    MultimapTableDefinition::new("tombstones");
struct TombstonesTable;

impl LedgerTable for TombstonesTable {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), redb::Error> {
        let mut table = wx.open_multimap_table(TOMBSTONES)?;

        if let Some(ChainPoint(slot, _)) = delta.new_position.as_ref() {
            for stxi in delta.consumed_utxo.iter() {
                let stxi: (&[u8; 32], u32) = (&stxi.0, stxi.1);
                table.insert(slot, stxi)?;
            }
        }

        if let Some(ChainPoint(slot, _)) = delta.undone_position.as_ref() {
            table.remove_all(slot)?;
        }

        Ok(())
    }
}

pub struct LedgerStore(redb::Database);

impl LedgerStore {
    pub fn apply(&mut self, delta: LedgerDelta) -> Result<(), redb::Error> {
        let wx = self.0.begin_write()?;

        UtxosTable::apply(&wx, &delta)?;
        TombstonesTable::apply(&wx, &delta)?;

        wx.commit()?;

        Ok(())
    }
}
