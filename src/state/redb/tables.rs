use ::redb::{Error, MultimapTableDefinition, TableDefinition, WriteTransaction};
use itertools::Itertools as _;
use pallas::{crypto::hash::Hash, ledger::traverse::MultiEraOutput};
use redb::{ReadTransaction, ReadableTable as _, TableError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::ledger::*;

pub struct BlocksTable;

impl BlocksTable {
    pub const DEF: TableDefinition<'static, u64, &'static [u8; 32]> =
        TableDefinition::new("blocks");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

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

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

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

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

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

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_multimap_table(Self::DEF)?;

        Ok(())
    }

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

pub struct CursorTable;

#[derive(Serialize, Deserialize)]
pub struct CursorValue {
    pub hash: Hash<32>,
    pub tombstones: Vec<TxoRef>,
}

impl CursorTable {
    pub const DEF: TableDefinition<'static, BlockSlot, &'static [u8]> =
        TableDefinition::new("cursor");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

    /// Checks if the table exists in the DB
    pub fn exists(rx: &ReadTransaction) -> Result<bool, Error> {
        match rx.open_table(Self::DEF) {
            Ok(_) => Ok(true),
            Err(TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(x) => return Err(x.into()),
        }
    }

    pub fn get_range(
        rx: &ReadTransaction,
        until: BlockSlot,
    ) -> Result<Vec<(BlockSlot, CursorValue)>, Error> {
        let table = rx.open_table(Self::DEF)?;

        let mut out = vec![];

        for entry in table.range(..until)? {
            let (slot, value) = entry?;
            let value = bincode::deserialize(value.value()).unwrap();

            out.push((slot.value(), value));
        }

        Ok(out)
    }

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        if let Some(ChainPoint(slot, hash)) = delta.new_position.as_ref() {
            let value = CursorValue {
                hash: *hash,
                tombstones: delta
                    .consumed_utxo
                    .iter()
                    .map(|(txo, _)| txo.clone())
                    .collect_vec(),
            };

            let value = bincode::serialize(&value).unwrap();

            table.insert(slot, value.as_slice())?;
        }

        if let Some(ChainPoint(slot, _)) = delta.undone_position.as_ref() {
            table.remove(slot)?;
        }

        Ok(())
    }

    pub fn compact(wx: &WriteTransaction, slot: BlockSlot) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        table.remove(slot)?;

        Ok(())
    }

    pub fn last(rx: &ReadTransaction) -> Result<Option<(BlockSlot, CursorValue)>, Error> {
        let table = rx.open_table(Self::DEF)?;

        let last = table.last()?;

        if let Some((slot, value)) = last {
            let slot = slot.value();
            let value = bincode::deserialize(value.value()).unwrap();

            Ok(Some((slot, value)))
        } else {
            Ok(None)
        }
    }
}

pub struct FilterIndexes;

impl FilterIndexes {
    pub const BY_ADDRESS: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("byaddress");

    pub const BY_PAYMENT: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("bypayment");

    pub const BY_STAKE: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("bystake");

    pub const BY_POLICY: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("bypolicy");

    pub const BY_ASSET: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("byasset");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_multimap_table(Self::BY_ADDRESS)?;
        wx.open_multimap_table(Self::BY_PAYMENT)?;
        wx.open_multimap_table(Self::BY_STAKE)?;
        wx.open_multimap_table(Self::BY_POLICY)?;
        wx.open_multimap_table(Self::BY_ASSET)?;

        Ok(())
    }

    fn get_by_key(
        rx: &ReadTransaction,
        table_def: MultimapTableDefinition<&[u8], UtxosKey>,
        key: &[u8],
    ) -> Result<HashSet<TxoRef>, Error> {
        let table = rx.open_multimap_table(table_def)?;

        let mut out = HashSet::new();

        for item in table.get(key)? {
            let item = item?;
            let (hash, idx) = item.value();
            out.insert(TxoRef((*hash).into(), idx));
        }

        Ok(out)
    }

    pub fn get_by_address(
        rx: &ReadTransaction,
        exact_address: &[u8],
    ) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_ADDRESS, exact_address)
    }

    pub fn get_by_payment(
        rx: &ReadTransaction,
        payment_part: &[u8],
    ) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_PAYMENT, payment_part)
    }

    pub fn get_by_stake(rx: &ReadTransaction, stake_part: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_STAKE, stake_part)
    }

    pub fn get_by_policy(rx: &ReadTransaction, policy: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_POLICY, policy)
    }

    pub fn get_by_asset(rx: &ReadTransaction, asset: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_ASSET, asset)
    }

    fn split_address(utxo: &MultiEraOutput) -> (Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>) {
        use pallas::ledger::addresses::Address;

        match utxo.address() {
            Ok(address) => match &address {
                Address::Shelley(x) => {
                    let a = x.to_vec();
                    let b = x.payment().to_vec();
                    let c = x.delegation().to_vec();
                    (Some(a), Some(b), Some(c))
                }
                Address::Stake(x) => {
                    let a = x.to_vec();
                    let c = x.to_vec();
                    (Some(a), None, Some(c))
                }
                Address::Byron(x) => {
                    let a = x.to_vec();
                    (Some(a), None, None)
                }
            },
            Err(_) => todo!(),
        }
    }

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut address_table = wx.open_multimap_table(Self::BY_ADDRESS)?;
        let mut payment_table = wx.open_multimap_table(Self::BY_PAYMENT)?;
        let mut stake_table = wx.open_multimap_table(Self::BY_STAKE)?;
        let mut policy_table = wx.open_multimap_table(Self::BY_POLICY)?;
        let mut asset_table = wx.open_multimap_table(Self::BY_ASSET)?;

        for (utxo, body) in delta.produced_utxo.iter() {
            let v: (&[u8; 32], u32) = (&utxo.0, utxo.1);

            // TODO: decoding here is very inefficient
            let body = MultiEraOutput::try_from(body).unwrap();
            let (addr, pay, stake) = Self::split_address(&body);

            if let Some(k) = addr {
                address_table.insert(k.as_slice(), v)?;
            }

            if let Some(k) = pay {
                payment_table.insert(k.as_slice(), v)?;
            }

            if let Some(k) = stake {
                stake_table.insert(k.as_slice(), v)?;
            }

            let assets = body.non_ada_assets();

            for batch in assets {
                policy_table.insert(batch.policy().as_slice(), v)?;

                for asset in batch.assets() {
                    let mut subject = asset.policy().to_vec();
                    subject.extend(asset.name());

                    asset_table.insert(subject.as_slice(), v)?;
                }
            }
        }

        let forgettable = delta.consumed_utxo.iter().chain(delta.undone_utxo.iter());

        for (stxi, body) in forgettable {
            let v: (&[u8; 32], u32) = (&stxi.0, stxi.1);

            // TODO: decoding here is very inefficient
            let body = MultiEraOutput::try_from(body).unwrap();

            let (addr, pay, stake) = Self::split_address(&body);

            if let Some(k) = addr {
                address_table.remove(k.as_slice(), v)?;
            }

            if let Some(k) = pay {
                payment_table.remove(k.as_slice(), v)?;
            }

            if let Some(k) = stake {
                stake_table.remove(k.as_slice(), v)?;
            }

            let assets = body.non_ada_assets();

            for batch in assets {
                policy_table.remove(batch.policy().as_slice(), v)?;

                for asset in batch.assets() {
                    let mut subject = asset.policy().to_vec();
                    subject.extend(asset.name());

                    asset_table.remove(subject.as_slice(), v)?;
                }
            }
        }

        Ok(())
    }
}
