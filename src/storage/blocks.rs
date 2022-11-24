use pallas::{crypto::hash::Hash, network::miniprotocols::Point};

use std::{
    convert::{TryFrom, TryInto},
    path::Path,
};

use rocksdb::{Direction, IteratorMode, Options, DB};

#[derive(Debug)]
struct Key(Hash<32>);

impl TryFrom<Box<[u8]>> for Key {
    type Error = super::Error;

    fn try_from(value: Box<[u8]>) -> Result<Self, Self::Error> {
        let inner: [u8; 32] = value[0..32].try_into().map_err(Self::Error::serde)?;
        let inner = Hash::<32>::from(inner);
        Ok(Self(inner))
    }
}

impl Into<Box<[u8]>> for Key {
    fn into(self) -> Box<[u8]> {
        self.0.as_slice().into()
    }
}

type Value = Box<[u8]>;

type RawKV = (Box<[u8]>, Box<[u8]>);

#[derive(Debug)]
pub struct Entry(Key, Value);

impl TryFrom<RawKV> for Entry {
    type Error = super::Error;

    fn try_from((k, v): RawKV) -> Result<Self, super::Error> {
        let k = k.try_into()?;

        Ok(Entry(k, v))
    }
}

pub struct BlocksDB {
    db: DB,
}

impl BlocksDB {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, super::Error> {
        let db = DB::open_default(path).map_err(super::Error::storage)?;
        Ok(Self { db })
    }

    /// Sets the content of a block for a specific hash
    pub fn set(&mut self, hash: Hash<32>, body: Vec<u8>) -> Result<(), super::Error> {
        let key: Box<[u8]> = Key(hash).into();
        self.db.put(key, body).map_err(super::Error::storage)
    }

    pub fn get(&mut self, hash: Hash<32>) -> Result<Option<Vec<u8>>, super::Error> {
        let key: Box<[u8]> = Key(hash).into();
        self.db.get(key).map_err(super::Error::storage)
    }
}

#[cfg(test)]
mod tests {
    use super::BlocksDB;
    use pallas::crypto::hash::Hash;
    use std::str::FromStr;

    #[test]
    fn test_rocks() {
        let mut db = BlocksDB::open("./tmp2").unwrap();

        db.set(
            Hash::from_str("c5e51fb496cb215246a6c2b7354ca1078620cab8ae6f961e39a90b1291abd705")
                .unwrap(),
            vec![0u8, 1u8],
        )
        .unwrap();

        db.get(
            Hash::from_str("c5e51fb496cb215246a6c2b7354ca1078620cab8ae6f961e39a90b1291abd705")
                .unwrap(),
        )
        .unwrap();

        //BlocksDB::destroy(&Options::default(), "./tmp2");
    }
}
