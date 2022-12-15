use serde::{Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};

use rocksdb::{ColumnFamilyRef, IteratorMode, WriteBatch, DB};

#[derive(Debug)]
pub struct Key(u64);

impl TryFrom<Box<[u8]>> for Key {
    type Error = super::Error;

    fn try_from(value: Box<[u8]>) -> Result<Self, super::Error> {
        let inner: [u8; 8] = value[0..8].try_into().map_err(|_| super::Error::Serde)?;
        let inner = u64::from_be_bytes(inner);
        Ok(Self(inner))
    }
}

impl From<Key> for Box<[u8]> {
    fn from(v: Key) -> Self {
        v.0.to_be_bytes().into()
    }
}

impl From<Key> for u64 {
    fn from(v: Key) -> Self {
        v.0
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum WalAction {
    Apply,
    Undo,
    Mark,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Value(WalAction, super::BlockSlot, super::BlockHash);

impl Value {
    pub fn slot(&self) -> super::BlockSlot {
        self.1
    }

    pub fn into_undo(self) -> Option<Self> {
        match self.0 {
            WalAction::Apply => Some(Self(WalAction::Undo, self.1, self.2)),
            WalAction::Undo => None,
            WalAction::Mark => None,
        }
    }

    pub fn into_mark(self) -> Option<Self> {
        match self.0 {
            WalAction::Apply => Some(Self(WalAction::Mark, self.1, self.2)),
            WalAction::Undo => None,
            WalAction::Mark => None,
        }
    }
}

impl TryFrom<Value> for Box<[u8]> {
    type Error = super::Error;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        bincode::serialize(&v)
            .map(|x| x.into_boxed_slice())
            .map_err(|_| super::Error::Serde)
    }
}

impl TryFrom<Box<[u8]>> for Value {
    type Error = super::Error;

    fn try_from(value: Box<[u8]>) -> Result<Self, Self::Error> {
        bincode::deserialize(&value).map_err(|_| super::Error::Serde)
    }
}

#[derive(Debug)]
pub struct Entry(Key, Value);

impl TryFrom<super::RawKV> for Entry {
    type Error = super::Error;

    fn try_from((k, v): super::RawKV) -> Result<Self, super::Error> {
        let k = k.try_into()?;
        let v = v.try_into()?;

        Ok(Entry(k, v))
    }
}

pub const CF_NAME: &str = "wal";

pub fn wal_cf(db: &DB) -> ColumnFamilyRef {
    db.cf_handle(CF_NAME).unwrap()
}

type RocksIterator<'a> = rocksdb::DBIteratorWithThreadMode<'a, rocksdb::DB>;

pub struct CrawlIterator<'a>(RocksIterator<'a>);

impl<'a> Iterator for CrawlIterator<'a> {
    type Item = Result<Value, super::Error>;

    fn next(&mut self) -> Option<Result<Value, super::Error>> {
        match self.0.next() {
            Some(Ok((key, value))) => Some(Value::try_from(value)),
            Some(Err(err)) => Some(Err(super::Error::IO)),
            None => None,
        }
    }
}

pub fn crawl_forward(db: &DB) -> CrawlIterator {
    let cf = wal_cf(db);
    let inner = db.iterator_cf(cf, IteratorMode::Start);
    CrawlIterator(inner)
}

pub fn crawl_backwards(db: &DB) -> CrawlIterator {
    let cf = wal_cf(db);
    let inner = db.iterator_cf(cf, IteratorMode::End);
    CrawlIterator(inner)
}

pub fn find_lastest_seq(db: &DB) -> Result<Key, super::Error> {
    let cf = wal_cf(db);
    let mut iter = db.iterator_cf(cf, IteratorMode::End);

    match iter.next() {
        Some(Ok((key, _))) => Ok(Key::try_from(key)?),
        Some(Err(err)) => Err(super::Error::IO),
        None => Ok(Key(0)),
    }
}

fn stage_append(
    cf: ColumnFamilyRef,
    last_seq: u64,
    value: Value,
    batch: &mut WriteBatch,
) -> Result<u64, super::Error> {
    let new_seq = last_seq + 1;
    let key = Box::<[u8]>::from(Key(new_seq));
    let value = Box::<[u8]>::try_from(value)?;

    batch.put_cf(cf, key, value);

    Ok(new_seq)
}

pub fn stage_roll_back(
    db: &DB,
    mut last_seq: u64,
    until: super::BlockSlot,
    batch: &mut WriteBatch,
) -> Result<u64, super::Error> {
    let iter = crawl_backwards(db);
    let cf = wal_cf(db);

    for step in iter {
        let value = step.map_err(|_| super::Error::IO)?;

        if value.slot() <= until {
            last_seq = stage_append(cf, last_seq, value.into_mark().unwrap(), batch)?;
            break;
        }

        match value.into_undo() {
            Some(undo) => {
                last_seq = stage_append(cf, last_seq, undo, batch)?;
            }
            None => continue,
        };
    }

    Ok(last_seq)
}

pub fn stage_roll_forward(
    db: &DB,
    last_seq: u64,
    slot: super::BlockSlot,
    hash: super::BlockHash,
    batch: &mut WriteBatch,
) -> Result<u64, super::Error> {
    let cf = wal_cf(db);

    let last_seq = stage_append(cf, last_seq, Value(WalAction::Apply, slot, hash), batch)?;

    Ok(last_seq)
}

pub fn find_tip(db: &DB) -> Result<Option<(super::BlockSlot, super::BlockHash)>, super::Error> {
    let iter = crawl_backwards(db);

    for value in iter {
        match value {
            Ok(Value(WalAction::Apply | WalAction::Mark, slot, hash)) => {
                return Ok(Some((slot, hash)))
            }
            Ok(_) => (),
            Err(err) => return Err(err),
        }
    }

    Ok(None)
}
