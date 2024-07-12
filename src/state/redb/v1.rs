use ::redb::{
    Database, Durability, Error, MultimapTableDefinition, ReadableMultimapTable, ReadableTable,
    TableDefinition, TableError, WriteTransaction,
};
use itertools::Itertools as _;
use pallas::{
    crypto::hash::Hash,
    ledger::traverse::{Era, MultiEraOutput},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::ledger::*;

trait LedgerTable {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error>;
    fn compact(wx: &WriteTransaction, slot: BlockSlot, tombstone: &[TxoRef]) -> Result<(), Error>;
}

const BLOCKS: TableDefinition<u64, &[u8; 32]> = TableDefinition::new("blocks");
struct BlocksTable;

impl LedgerTable for BlocksTable {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(BLOCKS)?;

        if let Some(ChainPoint(slot, hash)) = delta.new_position.as_ref() {
            let v: &[u8; 32] = hash;
            table.insert(slot, v)?;
        }

        if let Some(ChainPoint(slot, _)) = delta.undone_position.as_ref() {
            table.remove(slot)?;
        }

        Ok(())
    }

    fn compact(
        _wx: &WriteTransaction,
        _slot: BlockSlot,
        _tombstone: &[TxoRef],
    ) -> Result<(), Error> {
        // do nothing
        Ok(())
    }
}

type UtxosKey<'a> = (&'a [u8; 32], u32);
type UtxosValue<'a> = (u16, &'a [u8]);

const UTXOS: TableDefinition<UtxosKey, UtxosValue> = TableDefinition::new("utxos");
struct UtxosTable;

impl LedgerTable for UtxosTable {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(UTXOS)?;

        for (k, v) in delta.produced_utxo.iter() {
            let k: (&[u8; 32], u32) = (&k.0, k.1);
            let v: (u16, &[u8]) = (v.0.into(), &v.1);
            table.insert(k, v)?;
        }

        for (k, _) in delta.undone_utxo.iter() {
            let k: (&[u8; 32], u32) = (&k.0, k.1);
            table.remove(k)?;
        }

        Ok(())
    }

    fn compact(wx: &WriteTransaction, _slot: BlockSlot, tombstone: &[TxoRef]) -> Result<(), Error> {
        let mut table = wx.open_table(UTXOS)?;

        for txo in tombstone {
            let k: (&[u8; 32], u32) = (&txo.0, txo.1);
            table.remove(k)?;
        }

        Ok(())
    }
}

const PPARAMS: TableDefinition<u64, (u16, &[u8])> = TableDefinition::new("pparams");
struct PParamsTable;

impl LedgerTable for PParamsTable {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(PPARAMS)?;

        if let Some(ChainPoint(slot, _)) = delta.new_position {
            for PParamsBody(era, body) in delta.new_pparams.iter() {
                let v: (u16, &[u8]) = (u16::from(*era), body);
                table.insert(slot, v)?;
            }
        }

        if let Some(ChainPoint(slot, _)) = delta.undone_position {
            table.remove(slot)?;
        }

        Ok(())
    }

    fn compact(
        _wx: &WriteTransaction,
        _slot: BlockSlot,
        _tombstone: &[TxoRef],
    ) -> Result<(), Error> {
        // do nothing
        Ok(())
    }
}

pub const TOMBSTONES: MultimapTableDefinition<BlockSlot, (&[u8; 32], TxoIdx)> =
    MultimapTableDefinition::new("tombstones");
struct TombstonesTable;

impl LedgerTable for TombstonesTable {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_multimap_table(TOMBSTONES)?;

        if let Some(ChainPoint(slot, _)) = delta.new_position.as_ref() {
            for (stxi, _) in delta.consumed_utxo.iter() {
                let stxi: (&[u8; 32], u32) = (&stxi.0, stxi.1);
                table.insert(slot, stxi)?;
            }
        }

        if let Some(ChainPoint(slot, _)) = delta.undone_position.as_ref() {
            table.remove_all(slot)?;
        }

        Ok(())
    }

    fn compact(wx: &WriteTransaction, slot: BlockSlot, _tombstone: &[TxoRef]) -> Result<(), Error> {
        let mut table = wx.open_multimap_table(TOMBSTONES)?;

        table.remove_all(slot)?;

        Ok(())
    }
}

pub const BY_ADDRESS_INDEX: MultimapTableDefinition<&[u8], UtxosKey> =
    MultimapTableDefinition::new("byaddress");
struct ByAddressIndex;

impl LedgerTable for ByAddressIndex {
    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_multimap_table(BY_ADDRESS_INDEX)?;

        for (utxo, body) in delta.produced_utxo.iter() {
            // TODO: decoding here is very inefficient
            let body = MultiEraOutput::try_from(body).unwrap();

            if let Ok(address) = body.address() {
                let k = address.to_vec();
                let v: (&[u8; 32], u32) = (&utxo.0, utxo.1);
                table.insert(k.as_slice(), v)?;
            }
        }

        for (stxi, body) in delta.consumed_utxo.iter() {
            // TODO: decoding here is very inefficient
            let body = MultiEraOutput::try_from(body).unwrap();

            if let Ok(address) = body.address() {
                let k = address.to_vec();
                let v: (&[u8; 32], u32) = (&stxi.0, stxi.1);
                table.remove(k.as_slice(), v)?;
            }
        }

        for (stxi, body) in delta.undone_utxo.iter() {
            // TODO: decoding here is very inefficient
            let body = MultiEraOutput::try_from(body).unwrap();

            if let Ok(address) = body.address() {
                let k = address.to_vec();
                let v: (&[u8; 32], u32) = (&stxi.0, stxi.1);
                table.remove(k.as_slice(), v)?;
            }
        }

        Ok(())
    }

    fn compact(
        _wx: &WriteTransaction,
        _slot: BlockSlot,
        _tombstone: &[TxoRef],
    ) -> Result<(), Error> {
        // do nothing
        Ok(())
    }
}

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

        let table = match rx.open_table(BLOCKS) {
            Ok(x) => x,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(x) => return Err(x.into()),
        };

        let last = table.last()?;
        let last = last.map(|(k, v)| ChainPoint(k.value(), Hash::new(*v.value())));

        Ok(last)
    }

    pub fn apply(&mut self, deltas: &[LedgerDelta]) -> Result<(), Error> {
        let mut wx = self.0.begin_write()?;
        wx.set_durability(Durability::Eventual);

        for delta in deltas {
            UtxosTable::apply(&wx, delta)?;
            PParamsTable::apply(&wx, delta)?;
            TombstonesTable::apply(&wx, delta)?;
            BlocksTable::apply(&wx, delta)?;

            // indexes?
            //ByAddressIndex::apply(&wx, delta)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn finalize(&mut self, until: BlockSlot) -> Result<(), Error> {
        let mut wx = self.0.begin_write()?;
        wx.set_durability(Durability::Eventual);

        let tss: Vec<_> = {
            wx.open_multimap_table(TOMBSTONES)?
                .range(..until)?
                .map_ok(|(k, v)| {
                    let values: Vec<_> = v
                        .into_iter()
                        .map_ok(|x| (*x.value().0, x.value().1))
                        .map_ok(|(hash, idx)| TxoRef(hash.into(), idx))
                        .try_collect()?;

                    Result::<_, Error>::Ok((k.value(), values))
                })
                .try_collect()?
        };

        for ts in tss {
            let (slot, txos) = ts?;
            UtxosTable::compact(&wx, slot, &txos)?;
            PParamsTable::compact(&wx, slot, &txos)?;
            BlocksTable::compact(&wx, slot, &txos)?;
            TombstonesTable::compact(&wx, slot, &txos)?;
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

        let table = rx.open_table(UTXOS)?;
        let mut out = HashMap::new();

        for key in refs {
            if let Some(body) = table.get(&(&key.0 as &[u8; 32], key.1))? {
                let (era, cbor) = body.value();
                let era = Era::try_from(era).unwrap();
                let cbor = cbor.to_owned();
                let value = EraCbor(era, cbor);

                out.insert(key, value);
            }
        }

        Ok(out)
    }

    pub fn get_pparams(&self, until: BlockSlot) -> Result<Vec<PParamsBody>, Error> {
        let rx = self.0.begin_read()?;
        let table = rx.open_table(PPARAMS)?;

        let mut out = vec![];

        for item in table.range(..until)? {
            let (_, body) = item?;
            let (era, cbor) = body.value();
            let era = Era::try_from(era).unwrap();
            out.push(PParamsBody(era, Vec::from(cbor)));
        }

        Ok(out)
    }

    pub fn get_utxo_by_address_set(&self, address: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        let rx = self.0.begin_read()?;
        let table = rx.open_multimap_table(BY_ADDRESS_INDEX)?;

        let mut out = HashSet::new();

        for item in table.get(address)? {
            let item = item?;
            let (hash, idx) = item.value();
            out.insert(TxoRef((*hash).into(), idx));
        }

        Ok(out)
    }
}
