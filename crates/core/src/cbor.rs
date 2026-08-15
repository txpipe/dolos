//! CBOR utilities shared by every dolos crate.
//!
//! Types here exist to cover gaps in the minicbor line that Pallas pins, or to
//! give a single definition to an encoding that would otherwise be repeated as
//! a private `#[cbor(with = "...")]` module in each crate that needs it.
//!
//! Unlike the rest of core, this module is deliberately not re-exported at the
//! crate root: the names inside it are short and only make sense next to the
//! module that qualifies them, so a field reads `cbor::U128`, not `U128`.

use std::{
    fmt::{Display, Formatter},
    iter::Sum,
    ops::{Add, AddAssign, Sub, SubAssign},
    str::FromStr,
};

use pallas::codec::minicbor::{
    decode::{self, Decoder},
    encode::{self, Encoder, Write},
    Decode, Encode,
};
use serde::{Deserialize, Serialize};

/// An unsigned 128-bit integer that can be stored in an entity.
///
/// The minicbor line that Pallas pins (0.26) models CBOR's native integer
/// range, which tops out at `u64::MAX`, so a plain `u128` field has no
/// `Encode`/`Decode` impl. This newtype supplies one: the value is written as a
/// fixed 16-byte big-endian byte string, which keeps the encoded length
/// constant and the decode path allocation-free.
///
/// Use it for quantities whose per-transaction or per-block magnitude fits in
/// `u64` but whose aggregate over an epoch (or over the whole chain) does not.
/// For anything that genuinely fits in 64 bits, keep `u64`: this type costs 16
/// bytes on disk regardless of magnitude.
///
/// ```ignore
/// use dolos_core::cbor;
///
/// #[derive(Encode, Decode)]
/// struct Stats {
///     #[n(0)]
///     #[cbor(default)]
///     output: cbor::U128,
/// }
/// ```
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct U128(pub u128);

impl U128 {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u128::MAX);

    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u128 {
        self.0
    }

    pub const fn checked_add(self, rhs: Self) -> Option<Self> {
        match self.0.checked_add(rhs.0) {
            Some(x) => Some(Self(x)),
            None => None,
        }
    }

    pub const fn checked_sub(self, rhs: Self) -> Option<Self> {
        match self.0.checked_sub(rhs.0) {
            Some(x) => Some(Self(x)),
            None => None,
        }
    }
}

impl<C> Encode<C> for U128 {
    fn encode<W: Write>(
        &self,
        e: &mut Encoder<W>,
        _ctx: &mut C,
    ) -> Result<(), encode::Error<W::Error>> {
        e.bytes(&self.0.to_be_bytes())?;

        Ok(())
    }
}

impl<'b, C> Decode<'b, C> for U128 {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, decode::Error> {
        let bytes: [u8; 16] = d
            .bytes()?
            .try_into()
            .map_err(|_| decode::Error::message("expected 16-byte big-endian u128"))?;

        Ok(Self(u128::from_be_bytes(bytes)))
    }
}

impl From<u128> for U128 {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

impl From<u64> for U128 {
    fn from(value: u64) -> Self {
        Self(value as u128)
    }
}

impl From<u32> for U128 {
    fn from(value: u32) -> Self {
        Self(value as u128)
    }
}

impl From<U128> for u128 {
    fn from(value: U128) -> Self {
        value.0
    }
}

impl TryFrom<U128> for u64 {
    type Error = std::num::TryFromIntError;

    fn try_from(value: U128) -> Result<Self, Self::Error> {
        u64::try_from(value.0)
    }
}

impl Add for U128 {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }
}

impl Sub for U128 {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self(self.0 - rhs.0)
    }
}

impl AddAssign for U128 {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

impl SubAssign for U128 {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
    }
}

/// Accumulating a `u64` quantity is the common case at the call site (one
/// output's coin, one tx's fee), so it gets its own impl to spare the caller a
/// widening cast on every add.
impl AddAssign<u64> for U128 {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs as u128;
    }
}

impl Sum for U128 {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::ZERO, Add::add)
    }
}

impl Display for U128 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl FromStr for U128 {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        u128::from_str(s).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use pallas::codec::minicbor;
    use proptest::prelude::*;
    use proptest::proptest;

    use super::*;

    /// A struct field of this type is the intended use, so the roundtrip is
    /// exercised through the derive rather than through the impls directly.
    #[derive(Debug, Encode, Decode, PartialEq, Eq)]
    struct Holder {
        #[n(0)]
        value: U128,

        #[n(1)]
        #[cbor(default)]
        absent: U128,
    }

    proptest! {
        #[test]
        fn test_roundtrip(value in any::<u128>()) {
            let encoded = minicbor::to_vec(U128(value)).unwrap();
            let decoded: U128 = minicbor::decode(&encoded).unwrap();

            assert_eq!(decoded, U128(value));
        }

        #[test]
        fn test_encoded_length_is_constant(value in any::<u128>()) {
            let encoded = minicbor::to_vec(U128(value)).unwrap();

            // 1-byte byte string header plus the 16-byte payload.
            assert_eq!(encoded.len(), 17);
        }

        #[test]
        fn test_binary_order_is_maintained(a in any::<u128>(), b in any::<u128>()) {
            let bytes_a = minicbor::to_vec(U128(a)).unwrap();
            let bytes_b = minicbor::to_vec(U128(b)).unwrap();

            assert_eq!(a.cmp(&b), bytes_a.cmp(&bytes_b));
        }
    }

    #[test]
    fn test_roundtrip_boundaries() {
        for value in [0, 1, u64::MAX as u128, u64::MAX as u128 + 1, u128::MAX] {
            let encoded = minicbor::to_vec(U128(value)).unwrap();
            let decoded: U128 = minicbor::decode(&encoded).unwrap();

            assert_eq!(decoded, U128(value));
        }
    }

    #[test]
    fn test_derive_roundtrip() {
        let holder = Holder {
            value: U128(u128::from(u64::MAX) + 1),
            absent: U128::ZERO,
        };

        let encoded = minicbor::to_vec(&holder).unwrap();
        let decoded: Holder = minicbor::decode(&encoded).unwrap();

        assert_eq!(decoded, holder);
    }

    #[test]
    fn test_missing_field_defaults_to_zero() {
        let mut buf = Vec::new();
        let mut encoder = minicbor::Encoder::new(&mut buf);
        encoder.array(1).unwrap();
        encoder.encode(U128(7)).unwrap();

        let decoded: Holder = minicbor::decode(&buf).unwrap();

        assert_eq!(decoded.value, U128(7));
        assert_eq!(decoded.absent, U128::ZERO);
    }

    #[test]
    fn test_rejects_wrong_width() {
        let mut buf = Vec::new();
        let mut encoder = minicbor::Encoder::new(&mut buf);
        encoder.bytes(&[0u8; 8]).unwrap();

        assert!(minicbor::decode::<U128>(&buf).is_err());
    }

    #[test]
    fn test_serde_is_transparent() {
        let value = U128(u128::from(u64::MAX) + 1);

        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            serde_json::to_string(&value.get()).unwrap()
        );
    }
}
