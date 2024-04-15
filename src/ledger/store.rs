use redb::{MultimapTableDefinition, ReadableTable, TableDefinition, WriteTransaction};
use std::path::Path;

use super::*;

trait LedgerTable {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), redb::Error>;
}

const BLOCKS: TableDefinition<u64, &[u8; 32]> = TableDefinition::new("blocks");
struct BlocksTable;

impl LedgerTable for BlocksTable {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), redb::Error> {
        let mut table = wx.open_table(BLOCKS)?;

        if let Some(ChainPoint(slot, hash)) = delta.new_position.as_ref() {
            let v: &[u8; 32] = &hash;
            table.insert(slot, v)?;
        }

        if let Some(ChainPoint(slot, _)) = delta.undone_position.as_ref() {
            table.remove(slot)?;
        }

        Ok(())
    }
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

const PPARAMS: TableDefinition<u64, (Era, &[u8])> = TableDefinition::new("pparams");
struct PParamsTable;

impl LedgerTable for PParamsTable {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), redb::Error> {
        let mut table = wx.open_table(PPARAMS)?;

        if let Some(ChainPoint(slot, _)) = delta.new_position {
            for PParamsBody(era, body) in delta.new_pparams.iter() {
                let v: (u16, &[u8]) = (*era, &body);
                table.insert(slot, v)?;
            }
        }

        if let Some(ChainPoint(slot, _)) = delta.undone_position {
            table.remove(slot)?;
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
    pub fn open(path: impl AsRef<Path>) -> Result<Self, redb::Error> {
        let inner = redb::Database::create(path)?;

        Ok(Self(inner))
    }

    pub fn is_empty(&self) -> bool {
        match self.cursor() {
            Ok(x) => x.is_some(),
            Err(_) => false,
        }
    }

    pub fn cursor(&self) -> Result<Option<ChainPoint>, redb::Error> {
        let rx = self.0.begin_read()?;
        let table = rx.open_table(BLOCKS)?;
        let last = table.last()?;
        let last = last.map(|(k, v)| ChainPoint(k.value(), Hash::new(*v.value())));

        Ok(last)
    }

    pub fn apply(&mut self, deltas: &[LedgerDelta]) -> Result<(), redb::Error> {
        let mut wx = self.0.begin_write()?;
        wx.set_durability(redb::Durability::Eventual);

        for delta in deltas {
            UtxosTable::apply(&wx, &delta)?;
            PParamsTable::apply(&wx, &delta)?;
            TombstonesTable::apply(&wx, &delta)?;

            // indexes?
            BlocksTable::apply(&wx, &delta)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn get_utxos(
        &self,
        refs: impl IntoIterator<Item = TxoRef>,
    ) -> Result<HashMap<TxoRef, UtxoBody>, redb::Error> {
        let rx = self.0.begin_read()?;
        let table = rx.open_table(UTXOS)?;
        let mut out = HashMap::new();

        for key in refs {
            let body = table.get(&(&key.0 as &[u8; 32], key.1))?;
            let body = body.unwrap();
            // TODO: return invariant broken error
            let (era, cbor) = body.value();
            out.insert(key, UtxoBody(era, Vec::from(cbor)));
        }

        Ok(out)
    }

    pub fn get_pparams(&self, until: BlockSlot) -> Result<Vec<PParamsBody>, redb::Error> {
        let rx = self.0.begin_read()?;
        let table = rx.open_table(PPARAMS)?;

        let mut out = vec![];

        for item in table.range(..until)? {
            let (_, body) = item?;
            let (era, cbor) = body.value();
            out.push(PParamsBody(era, Vec::from(cbor)));
        }

        Ok(out)
    }
}
