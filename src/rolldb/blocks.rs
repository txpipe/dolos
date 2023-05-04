use pallas::crypto::hash::Hash;
use rocksdb::{ColumnFamilyRef, WriteBatch, DB};

#[derive(Debug)]
struct Key(Hash<32>);

impl TryFrom<Box<[u8]>> for Key {
    type Error = super::Error;

    fn try_from(value: Box<[u8]>) -> Result<Self, Self::Error> {
        let inner: [u8; 32] = value[0..32].try_into().map_err(|_| super::Error::Serde)?;
        let inner = Hash::<32>::from(inner);
        Ok(Self(inner))
    }
}

impl From<Key> for Box<[u8]> {
    fn from(v: Key) -> Self {
        v.0.as_slice().into()
    }
}

type Value = Box<[u8]>;

#[derive(Debug)]
pub struct Entry(Key, Value);

impl TryFrom<super::RawKV> for Entry {
    type Error = super::Error;

    fn try_from((k, v): super::RawKV) -> Result<Self, super::Error> {
        let k = k.try_into()?;

        Ok(Entry(k, v))
    }
}

pub fn stage_upsert(
    db: &DB,
    hash: super::BlockHash,
    body: super::BlockBody,
    batch: &mut WriteBatch,
) -> Result<(), super::Error> {
    let cf = blocks_cf(db);

    batch.put_cf(cf, hash, body);

    Ok(())
}

pub const CF_NAME: &str = "blocks";

pub fn blocks_cf(db: &DB) -> ColumnFamilyRef {
    db.cf_handle(CF_NAME).unwrap()
}

pub fn get_body(db: &DB, hash: Hash<32>) -> Result<Option<super::BlockBody>, super::Error> {
    let cf = blocks_cf(db);
    let key = Box::<[u8]>::from(Key(hash));
    db.get_cf(cf, key).map_err(|_| super::Error::IO)
}
