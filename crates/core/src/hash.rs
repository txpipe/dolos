use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Hash<const N: usize>([u8; N]);

impl<const N: usize> Hash<N> {
    pub fn new(bytes: [u8; N]) -> Hash<N> {
        Hash(bytes)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl<const N: usize> fmt::Display for Hash<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl<const N: usize> FromStr for Hash<N> {
    type Err = hex::FromHexError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s)?;
        if bytes.len() != N {
            return Err(hex::FromHexError::InvalidStringLength);
        }
        let mut arr = [0u8; N];

        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }
}

impl<const N: usize> Serialize for Hash<N> {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        s.serialize_str(&hex::encode(self.0))
    }
}

impl<'de, const N: usize> Deserialize<'de> for Hash<N> {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl<const N: usize> From<[u8; N]> for Hash<N> {
    fn from(bytes: [u8; N]) -> Self {
        Self(bytes)
    }
}

impl<const N: usize> AsRef<[u8]> for Hash<N> {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
