use pallas::crypto::hash::Hash;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub struct DBHash(pub Hash<32>);

impl TryFrom<Box<[u8]>> for DBHash {
    type Error = super::Error;

    fn try_from(value: Box<[u8]>) -> Result<Self, super::Error> {
        let inner: [u8; 32] = value[0..32].try_into().map_err(|_| super::Error::Serde)?;
        let inner = Hash::<32>::from(inner);
        Ok(Self(inner))
    }
}

impl TryFrom<DBHash> for Box<[u8]> {
    type Error = super::Error;

    fn try_from(value: DBHash) -> Result<Self, Self::Error> {
        let b = value.0.to_vec();
        Ok(b.into())
    }
}

pub struct DBInt(pub u64);

impl TryFrom<DBInt> for Box<[u8]> {
    type Error = super::Error;

    fn try_from(value: DBInt) -> Result<Self, Self::Error> {
        let b = value.0.to_be_bytes();
        Ok(Box::new(b))
    }
}

impl TryFrom<Box<[u8]>> for DBInt {
    type Error = super::Error;

    fn try_from(value: Box<[u8]>) -> Result<Self, super::Error> {
        let inner: [u8; 8] = value[0..8].try_into().map_err(|_| super::Error::Serde)?;
        let inner = u64::from_be_bytes(inner);
        Ok(Self(inner))
    }
}

pub struct DBBytes(pub Vec<u8>);

impl TryFrom<DBBytes> for Box<[u8]> {
    type Error = super::Error;

    fn try_from(value: DBBytes) -> Result<Self, Self::Error> {
        Ok(value.0.into())
    }
}

impl TryFrom<Box<[u8]>> for DBBytes {
    type Error = super::Error;

    fn try_from(value: Box<[u8]>) -> Result<Self, super::Error> {
        Ok(Self(value.into()))
    }
}

pub struct DBSerde<V>(pub V);

impl<V> std::ops::Deref for DBSerde<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<V> TryFrom<DBSerde<V>> for Box<[u8]>
where
    V: Serialize,
{
    type Error = super::Error;

    fn try_from(v: DBSerde<V>) -> Result<Self, Self::Error> {
        bincode::serialize(&v.0)
            .map(|x| x.into_boxed_slice())
            .map_err(|_| super::Error::Serde)
    }
}

impl<V> TryFrom<Box<[u8]>> for DBSerde<V>
where
    V: DeserializeOwned,
{
    type Error = super::Error;

    fn try_from(value: Box<[u8]>) -> Result<Self, Self::Error> {
        let inner = bincode::deserialize(&value).map_err(|_| super::Error::Serde)?;
        Ok(DBSerde(inner))
    }
}
