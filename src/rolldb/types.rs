use pallas::crypto::hash::Hash;
use serde::{de::DeserializeOwned, Serialize};

pub struct DBHash(pub Hash<32>);

impl From<Box<[u8]>> for DBHash {
    fn from(value: Box<[u8]>) -> Self {
        let inner: [u8; 32] = value[0..32].try_into().unwrap();
        let inner = Hash::<32>::from(inner);
        Self(inner)
    }
}

impl From<DBHash> for Box<[u8]> {
    fn from(value: DBHash) -> Self {
        let b = value.0.to_vec();
        b.into()
    }
}

pub struct DBInt(pub u64);

impl From<DBInt> for Box<[u8]> {
    fn from(value: DBInt) -> Self {
        let b = value.0.to_be_bytes();
        Box::new(b)
    }
}

impl From<Box<[u8]>> for DBInt {
    fn from(value: Box<[u8]>) -> Self {
        let inner: [u8; 8] = value[0..8].try_into().unwrap();
        let inner = u64::from_be_bytes(inner);
        Self(inner)
    }
}

pub struct DBBytes(pub Vec<u8>);

impl From<DBBytes> for Box<[u8]> {
    fn from(value: DBBytes) -> Self {
        value.0.into()
    }
}

impl From<Box<[u8]>> for DBBytes {
    fn from(value: Box<[u8]>) -> Self {
        Self(value.into())
    }
}

pub struct DBSerde<V>(pub V);

impl<V> std::ops::Deref for DBSerde<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<V> From<DBSerde<V>> for Box<[u8]>
where
    V: Serialize,
{
    fn from(v: DBSerde<V>) -> Self {
        bincode::serialize(&v.0)
            .map(|x| x.into_boxed_slice())
            .unwrap()
    }
}

impl<V> From<Box<[u8]>> for DBSerde<V>
where
    V: DeserializeOwned,
{
    fn from(value: Box<[u8]>) -> Self {
        let inner = bincode::deserialize(&value).unwrap();
        DBSerde(inner)
    }
}
