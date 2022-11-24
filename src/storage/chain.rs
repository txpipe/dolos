use pallas::{crypto::hash::Hash, network::miniprotocols::Point};

use std::{
    convert::{TryFrom, TryInto},
    path::Path,
};

use rocksdb::{Direction, IteratorMode, Options, DB};

#[derive(Debug)]
struct Key(u64);

impl TryFrom<Box<[u8]>> for Key {
    type Error = super::Error;

    fn try_from(value: Box<[u8]>) -> Result<Self, Self::Error> {
        let inner: [u8; 8] = value[0..8].try_into().map_err(Self::Error::serde)?;
        let inner = u64::from_be_bytes(inner);
        Ok(Self(inner))
    }
}

impl Into<Box<[u8]>> for Key {
    fn into(self) -> Box<[u8]> {
        self.0.to_be_bytes().into()
    }
}

#[derive(Debug)]
struct Value(Hash<32>);

impl TryFrom<Box<[u8]>> for Value {
    type Error = super::Error;

    fn try_from(value: Box<[u8]>) -> Result<Self, Self::Error> {
        let inner: [u8; 32] = value[0..32].try_into().map_err(Self::Error::serde)?;
        let inner = Hash::<32>::from(inner);
        Ok(Self(inner))
    }
}

impl Into<Box<[u8]>> for Value {
    fn into(self) -> Box<[u8]> {
        self.0.as_slice().into()
    }
}

type RawKV = (Box<[u8]>, Box<[u8]>);
type RocksIterator<'a> = rocksdb::DBIteratorWithThreadMode<'a, rocksdb::DB>;

pub struct Iterator<'a>(RocksIterator<'a>);

impl<'a> Iterator<'a> {
    pub fn next(&mut self) -> Option<Result<Entry, super::Error>> {
        match self.0.next() {
            Some(Ok(kv)) => Some(Entry::try_from(kv)),
            Some(Err(err)) => Some(Err(super::Error::storage(err))),
            None => None,
        }
    }
}

#[derive(Debug)]
pub struct Entry(Key, Value);

impl TryFrom<RawKV> for Entry {
    type Error = super::Error;

    fn try_from((k, v): RawKV) -> Result<Self, super::Error> {
        let k = k.try_into()?;
        let v = v.try_into()?;

        Ok(Entry(k, v))
    }
}

pub struct ChainDB {
    db: DB,
}

impl ChainDB {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, super::Error> {
        let db = DB::open_default(path).map_err(super::Error::storage)?;
        Ok(Self { db })
    }

    /// Extends the state with a newly received block
    pub fn extend(&mut self, slot: u64, hash: Hash<32>) -> Result<(), super::Error> {
        let key: Box<[u8]> = Key(slot).into();
        let value: Box<[u8]> = Value(hash).into();

        self.db.put(key, value).map_err(super::Error::storage)
    }

    pub fn read_tip(&mut self) -> Result<Point, super::Error> {
        let mut iter = self.db.iterator(IteratorMode::End);

        match iter.next() {
            Some(x) => {
                let raw = x.map_err(super::Error::storage)?;
                let Entry(key, value) = raw.try_into().map_err(super::Error::serde)?;
                Ok(Point::Specific(key.0, Vec::from(value.0.as_slice())))
            }
            None => Ok(Point::Origin),
        }
    }

    /// Clears entries since a certain slot
    pub fn rollback(&mut self, slot: u64) -> Result<(), super::Error> {
        let tip = self.read_tip()?;

        let cf = self.db.cf_handle("default").unwrap();
        let from: Box<[u8]> = Key(slot).into();
        let to: Box<[u8]> = Key(tip.slot_or_default()).into();

        self.db
            .delete_range_cf(&cf, from, to)
            .map_err(super::Error::storage)?;

        Ok(())
    }

    /// Returns an entry interator from a certain slot
    pub fn read_since(&self, slot: u64) -> Iterator {
        let k: Box<[u8]> = Key(slot).into();
        let inner = self.db.iterator(IteratorMode::From(&k, Direction::Forward));
        Iterator(inner)
    }
}

#[cfg(test)]
mod tests {
    use super::ChainDB;
    use pallas::crypto::hash::Hash;
    use std::str::FromStr;

    #[test]
    fn test_rocks() {
        let mut db = ChainDB::open("./tmp1").unwrap();

        db.extend(
            0,
            Hash::from_str("c5e51fb496cb215246a6c2b7354ca1078620cab8ae6f961e39a90b1291abd705")
                .unwrap(),
        )
        .unwrap();

        db.extend(
            1,
            Hash::from_str("c5e51fb496cb215246a6c2b7354ca1078620cab8ae6f961e39a90b1291abd705")
                .unwrap(),
        )
        .unwrap();

        db.extend(
            2,
            Hash::from_str("c5e51fb496cb215246a6c2b7354ca1078620cab8ae6f961e39a90b1291abd705")
                .unwrap(),
        )
        .unwrap();

        {
            let mut iter = db.read_since(0);

            while let Some(point) = iter.next() {
                dbg!(point);
            }
        }

        db.extend(
            3,
            Hash::from_str("c5e51fb496cb215246a6c2b7354ca1078620cab8ae6f961e39a90b1291abd705")
                .unwrap(),
        )
        .unwrap();

        db.extend(
            4,
            Hash::from_str("c5e51fb496cb215246a6c2b7354ca1078620cab8ae6f961e39a90b1291abd705")
                .unwrap(),
        )
        .unwrap();

        db.extend(
            5,
            Hash::from_str("c5e51fb496cb215246a6c2b7354ca1078620cab8ae6f961e39a90b1291abd705")
                .unwrap(),
        )
        .unwrap();

        {
            let mut iter = db.read_since(3);

            while let Some(point) = iter.next() {
                dbg!(point);
            }
        }

        //ChainDB::destroy(&Options::default(), "./tmp2");
    }
}
