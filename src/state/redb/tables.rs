use std::collections::{HashMap, HashSet};

use ::redb::{Error, MultimapTableDefinition, TableDefinition, WriteTransaction};
use itertools::Itertools as _;
use pallas::{crypto::hash::Hash, ledger::traverse::MultiEraOutput};
use redb::{ReadTransaction, ReadableTable as _, TableError};

use crate::ledger::*;

pub struct BlocksTable;

impl BlocksTable {
    pub const DEF: TableDefinition<'static, u64, &'static [u8; 32]> =
        TableDefinition::new("blocks");

    pub fn last(rx: &ReadTransaction) -> Result<Option<ChainPoint>, Error> {
        let table = match rx.open_table(Self::DEF) {
            Ok(x) => x,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(x) => return Err(x.into()),
        };

        let last = table.last()?;
        let last = last.map(|(k, v)| ChainPoint(k.value(), Hash::new(*v.value())));

        Ok(last)
    }

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        if let Some(ChainPoint(slot, hash)) = delta.new_position.as_ref() {
            let v: &[u8; 32] = hash;
            table.insert(slot, v)?;
        }

        if let Some(ChainPoint(slot, _)) = delta.undone_position.as_ref() {
            table.remove(slot)?;
        }

        Ok(())
    }
}

type UtxosKey = (&'static [u8; 32], u32);
type UtxosValue = (u16, &'static [u8]);

pub struct UtxosTable;

impl UtxosTable {
    pub const DEF: TableDefinition<'static, UtxosKey, UtxosValue> = TableDefinition::new("utxos");

    pub fn get_sparse(
        rx: &ReadTransaction,
        refs: Vec<TxoRef>,
    ) -> Result<HashMap<TxoRef, EraCbor>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let mut out = HashMap::new();

        for key in refs {
            if let Some(body) = table.get(&(&key.0 as &[u8; 32], key.1))? {
                let (era, cbor) = body.value();
                let era = pallas::ledger::traverse::Era::try_from(era).unwrap();
                let cbor = cbor.to_owned();
                let value = EraCbor(era, cbor);

                out.insert(key, value);
            }
        }

        Ok(out)
    }

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

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

    pub fn compact(
        wx: &WriteTransaction,
        _slot: BlockSlot,
        tombstone: &[TxoRef],
    ) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        for txo in tombstone {
            let k: (&[u8; 32], u32) = (&txo.0, txo.1);
            table.remove(k)?;
        }

        Ok(())
    }
}

pub struct PParamsTable;

impl PParamsTable {
    pub const DEF: TableDefinition<'static, u64, (u16, &'static [u8])> =
        TableDefinition::new("pparams");

    pub fn get_range(rx: &ReadTransaction, until: BlockSlot) -> Result<Vec<PParamsBody>, Error> {
        let table = rx.open_table(Self::DEF)?;

        let mut out = vec![];

        for item in table.range(..until)? {
            let (_, body) = item?;
            let (era, cbor) = body.value();
            let era = pallas::ledger::traverse::Era::try_from(era).unwrap();
            out.push(PParamsBody(era, Vec::from(cbor)));
        }

        Ok(out)
    }

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(PParamsTable::DEF)?;

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
}

pub struct TombstonesTable;

impl TombstonesTable {
    pub const DEF: MultimapTableDefinition<'static, BlockSlot, (&'static [u8; 32], TxoIdx)> =
        MultimapTableDefinition::new("tombstones");

    pub fn get_range(
        rx: &ReadTransaction,
        until: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Vec<TxoRef>)>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;

        let mut out = vec![];

        for entry in table.range(..until)? {
            let (slot, tss) = entry?;

            let tss: Vec<_> = tss
                .into_iter()
                .map_ok(|x| (*x.value().0, x.value().1))
                .map_ok(|(hash, idx)| TxoRef(hash.into(), idx))
                .try_collect()?;

            out.push((slot.value(), tss));
        }

        Ok(out)
    }

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_multimap_table(Self::DEF)?;

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

    pub fn compact(
        wx: &WriteTransaction,
        slot: BlockSlot,
        _tombstone: &[TxoRef],
    ) -> Result<(), Error> {
        let mut table = wx.open_multimap_table(Self::DEF)?;

        table.remove_all(slot)?;

        Ok(())
    }
}

pub struct ByAddressIndex;

impl ByAddressIndex {
    pub const DEF: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("byaddress");

    pub fn get_utxo_by_address_set(
        rx: &ReadTransaction,
        address: &[u8],
    ) -> Result<HashSet<TxoRef>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;

        let mut out = HashSet::new();

        for item in table.get(address)? {
            let item = item?;
            let (hash, idx) = item.value();
            out.insert(TxoRef((*hash).into(), idx));
        }

        Ok(out)
    }

    fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_multimap_table(Self::DEF)?;

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
}
