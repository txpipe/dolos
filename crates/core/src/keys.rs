//! Typed keys for state entities and archive logs.
//!
//! A stored key is a byte string of any length, ordered by its bytes. Each key
//! kind has one typed struct implementing [`KeyCodec`], and both the writers
//! and the readers of a namespace go through it, so they share one convention
//! for the key's parts, its length and its order. [`EntityKey`] and [`LogKey`]
//! carry the encoded bytes; they are built from a typed key with `of`, and
//! from bytes only through `from_raw`, which is for backends, the snapshot
//! crate, CLI tools and tests.
//!
//! A composite key concatenates its parts. Only a [`FixedKey`] may precede
//! another part: with a fixed width in front, the bytes of a leading part are
//! a byte prefix of every key it starts, which is what keeps a prefix range
//! ([`KeyPrefix`]) sound.

use std::{fmt, marker::PhantomData, ops::Range};

use smallvec::SmallVec;

use crate::BlockSlot;

/// Key bytes held inline before a key spills to the heap: every fixed entity
/// key, and a log key over one of up to 32 bytes.
const INLINE_KEY: usize = 40;

/// The encoded bytes of a key.
pub type KeyBytes = SmallVec<[u8; INLINE_KEY]>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("key needs {expected} bytes, found {found}")]
    Short { expected: usize, found: usize },

    #[error("key has {0} trailing bytes")]
    Trailing(usize),

    #[error("malformed key: {0}")]
    Malformed(String),
}

/// One kind of key: how it encodes to bytes and decodes back.
pub trait KeyCodec: Sized {
    /// Append the key's bytes to `out`.
    fn encode_key(&self, out: &mut KeyBytes);

    /// Decode a key from exactly `bytes`, refusing short input and trailing
    /// bytes.
    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError>;
}

/// A key whose encoding always has [`FixedKey::LEN`] bytes.
pub trait FixedKey: KeyCodec {
    const LEN: usize;
}

/// The encoding of `Self` is a byte prefix of the encoding of every `K` that
/// starts with it.
pub trait KeyPrefix<K: KeyCodec>: KeyCodec {}

pub fn encode<K: KeyCodec>(key: &K) -> KeyBytes {
    let mut out = KeyBytes::new();
    key.encode_key(&mut out);
    out
}

/// Exactly `N` bytes.
pub fn exact<const N: usize>(bytes: &[u8]) -> Result<[u8; N], KeyError> {
    match bytes.len() {
        len if len < N => Err(KeyError::Short {
            expected: N,
            found: len,
        }),
        len if len > N => Err(KeyError::Trailing(len - N)),
        _ => Ok(bytes.try_into().expect("length checked")),
    }
}

/// The least byte string above every string that starts with `prefix`, or
/// `None` when no such string exists (`prefix` empty or all `0xff`).
pub fn successor(prefix: &[u8]) -> Option<KeyBytes> {
    let mut out = KeyBytes::from_slice(prefix);

    while let Some(last) = out.pop() {
        if last < 0xff {
            out.push(last + 1);
            return Some(out);
        }
    }

    None
}

impl KeyCodec for () {
    fn encode_key(&self, _: &mut KeyBytes) {}

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        exact::<0>(bytes).map(|_| ())
    }
}

macro_rules! int_key {
    ($($int:ty),*) => {
        $(
            impl KeyCodec for $int {
                fn encode_key(&self, out: &mut KeyBytes) {
                    out.extend_from_slice(&self.to_be_bytes());
                }

                fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
                    exact(bytes).map(<$int>::from_be_bytes)
                }
            }

            impl FixedKey for $int {
                const LEN: usize = std::mem::size_of::<$int>();
            }
        )*
    };
}

int_key!(u8, u16, u32, u64);

impl<const N: usize> KeyCodec for [u8; N] {
    fn encode_key(&self, out: &mut KeyBytes) {
        out.extend_from_slice(self);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        exact(bytes)
    }
}

impl<const N: usize> FixedKey for [u8; N] {
    const LEN: usize = N;
}

impl<A: FixedKey, B: KeyCodec> KeyCodec for (A, B) {
    fn encode_key(&self, out: &mut KeyBytes) {
        self.0.encode_key(out);
        self.1.encode_key(out);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        if bytes.len() < A::LEN {
            return Err(KeyError::Short {
                expected: A::LEN,
                found: bytes.len(),
            });
        }

        let (a, b) = bytes.split_at(A::LEN);

        Ok((A::decode_key(a)?, B::decode_key(b)?))
    }
}

impl<A: FixedKey, B: FixedKey> FixedKey for (A, B) {
    const LEN: usize = A::LEN + B::LEN;
}

impl<A: FixedKey, B: KeyCodec> KeyPrefix<(A, B)> for A {}

impl<A: FixedKey, B: KeyCodec, P: KeyPrefix<B>> KeyPrefix<(A, B)> for (A, P) {}

/// Raw keys from `start`, inclusive, up to `end`, exclusive, or to the end of
/// the namespace when `end` is `None`.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct KeyRange {
    pub start: KeyBytes,
    pub end: Option<KeyBytes>,
}

impl KeyRange {
    /// Every key of a namespace.
    pub fn all() -> Self {
        Self::default()
    }

    /// Every key that starts with `prefix`.
    pub fn prefix(prefix: &[u8]) -> Self {
        Self {
            start: KeyBytes::from_slice(prefix),
            end: successor(prefix),
        }
    }

    /// Every log key at a slot in `slots`, which is half-open, whatever the
    /// kind of key after the slot.
    pub fn slots(slots: Range<BlockSlot>) -> Self {
        Self {
            start: KeyBytes::from_slice(&slots.start.to_be_bytes()),
            end: Some(KeyBytes::from_slice(&slots.end.to_be_bytes())),
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && self.end.as_ref().is_none_or(|end| key < end.as_slice())
    }

    /// Whether no key can fall in the range.
    pub fn is_empty(&self) -> bool {
        self.end.as_ref().is_some_and(|end| *end <= self.start)
    }
}

impl fmt::Debug for KeyRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.end {
            Some(end) => write!(f, "{}..{}", hex::encode(&self.start), hex::encode(end)),
            None => write!(f, "{}..", hex::encode(&self.start)),
        }
    }
}

/// Marks a [`Ranged`] over state entity keys.
pub enum StateKeys {}

/// Marks a [`Ranged`] over archive log keys, by slot and then the part after
/// it.
pub enum LogKeys {}

/// A typed range over the keys of kind `K` in the key space `S`.
pub struct Ranged<K, S> {
    raw: KeyRange,
    _key: PhantomData<fn() -> (K, S)>,
}

/// A range of state keys of kind `K`.
pub type RangeOf<K> = Ranged<K, StateKeys>;

/// A range of log keys whose part after the slot is of kind `K`.
pub type LogRangeOf<K> = Ranged<K, LogKeys>;

impl<K: KeyCodec, S> Ranged<K, S> {
    fn new(raw: KeyRange) -> Self {
        Self {
            raw,
            _key: PhantomData,
        }
    }

    /// Every key of the namespace.
    pub fn all() -> Self {
        Self::new(KeyRange::all())
    }

    pub fn raw(&self) -> &KeyRange {
        &self.raw
    }

    pub fn into_raw(self) -> KeyRange {
        self.raw
    }
}

impl<K: KeyCodec> Ranged<K, StateKeys> {
    /// Every key that starts with `prefix`.
    pub fn prefix<P: KeyPrefix<K>>(prefix: &P) -> Self {
        Self::new(KeyRange::prefix(&encode(prefix)))
    }

    /// From `start`, inclusive, to `end`, exclusive.
    pub fn between(start: &K, end: &K) -> Self {
        Self::new(KeyRange {
            start: encode(start),
            end: Some(encode(end)),
        })
    }

    /// The range, starting at `start` instead.
    pub fn starting_at(mut self, start: &K) -> Self {
        self.raw.start = encode(start);
        self
    }

    /// The range, ending before `end` instead.
    pub fn ending_before(mut self, end: &K) -> Self {
        self.raw.end = Some(encode(end));
        self
    }

    pub fn contains(&self, key: &K) -> bool {
        self.raw.contains(&encode(key))
    }
}

impl<K: KeyCodec> Ranged<K, LogKeys> {
    /// Every row at `slot`.
    pub fn slot(slot: BlockSlot) -> Self {
        Self::new(KeyRange::prefix(&slot.to_be_bytes()))
    }

    /// Every row at a slot in `slots`, which is half-open.
    pub fn slots(slots: Range<BlockSlot>) -> Self {
        Self::new(KeyRange::slots(slots))
    }

    /// Every row at `slot` whose key starts with `prefix`:
    /// `[slot ‖ prefix, successor(slot ‖ prefix))`.
    pub fn prefix<P: KeyPrefix<K>>(slot: BlockSlot, prefix: &P) -> Self {
        let mut bytes = KeyBytes::from_slice(&slot.to_be_bytes());
        prefix.encode_key(&mut bytes);
        Self::new(KeyRange::prefix(&bytes))
    }
}

impl<K, S> Clone for Ranged<K, S> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            _key: PhantomData,
        }
    }
}

impl<K, S> fmt::Debug for Ranged<K, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.raw.fmt(f)
    }
}

/// The stored key of a state entity, within its namespace.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntityKey(KeyBytes);

impl EntityKey {
    pub fn of<K: KeyCodec>(key: &K) -> Self {
        Self(encode(key))
    }

    /// A key from its stored bytes, for backends, the snapshot crate, CLI
    /// tools and tests.
    pub fn from_raw(bytes: &[u8]) -> Self {
        Self(KeyBytes::from_slice(bytes))
    }

    pub fn decode<K: KeyCodec>(&self) -> Result<K, KeyError> {
        K::decode_key(&self.0)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<[u8]> for EntityKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for EntityKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(&self.0))
    }
}

impl fmt::Debug for EntityKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EntityKey({self})")
    }
}

/// The stored key of an archive log row: `slot (8 B, big-endian) ‖ key`.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogKey(KeyBytes);

impl LogKey {
    pub const SLOT_LEN: usize = 8;

    pub fn of<K: KeyCodec>(slot: BlockSlot, key: &K) -> Self {
        let mut bytes = KeyBytes::from_slice(&slot.to_be_bytes());
        key.encode_key(&mut bytes);
        Self(bytes)
    }

    /// A key from its stored bytes, for backends, the snapshot crate, CLI
    /// tools and tests. Refuses bytes too short to hold the slot.
    pub fn from_raw(bytes: &[u8]) -> Result<Self, KeyError> {
        if bytes.len() < Self::SLOT_LEN {
            return Err(KeyError::Short {
                expected: Self::SLOT_LEN,
                found: bytes.len(),
            });
        }

        Ok(Self(KeyBytes::from_slice(bytes)))
    }

    pub fn slot(&self) -> BlockSlot {
        let slot = self.0[..Self::SLOT_LEN]
            .try_into()
            .expect("checked on build");
        BlockSlot::from_be_bytes(slot)
    }

    /// The bytes after the slot.
    pub fn entity(&self) -> &[u8] {
        &self.0[Self::SLOT_LEN..]
    }

    pub fn decode<K: KeyCodec>(&self) -> Result<(BlockSlot, K), KeyError> {
        Ok((self.slot(), K::decode_key(self.entity())?))
    }
}

impl AsRef<[u8]> for LogKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for LogKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.slot(), hex::encode(self.entity()))
    }
}

impl fmt::Debug for LogKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogKey({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn bytes<K: KeyCodec>(key: &K) -> Vec<u8> {
        encode(key).to_vec()
    }

    #[test]
    fn integers_are_big_endian_and_exact() {
        assert_eq!(bytes(&0x0102u16), [1, 2]);
        assert_eq!(bytes(&7u64), [0, 0, 0, 0, 0, 0, 0, 7]);
        assert_eq!(
            u32::decode_key(&[1, 2, 3]),
            Err(KeyError::Short {
                expected: 4,
                found: 3
            })
        );
        assert_eq!(
            u32::decode_key(&[1, 2, 3, 4, 5]),
            Err(KeyError::Trailing(1))
        );
    }

    #[test]
    fn unit_is_empty_and_exact() {
        assert!(bytes(&()).is_empty());
        assert_eq!(<()>::decode_key(&[]), Ok(()));
        assert_eq!(<()>::decode_key(&[0]), Err(KeyError::Trailing(1)));
    }

    #[test]
    fn composites_split_after_the_fixed_part() {
        let key: ([u8; 2], u32) = ([9, 8], 0x01020304);
        assert_eq!(bytes(&key), [9, 8, 1, 2, 3, 4]);
        assert_eq!(<([u8; 2], u32)>::decode_key(&bytes(&key)), Ok(key));
        assert_eq!(
            <([u8; 2], u32)>::decode_key(&[9]),
            Err(KeyError::Short {
                expected: 2,
                found: 1
            })
        );
        assert_eq!(<([u8; 2], (u8, u32)) as FixedKey>::LEN, 7);
    }

    #[test]
    fn successor_carries_past_trailing_ff() {
        assert_eq!(successor(&[1, 2]).unwrap().as_slice(), [1, 3]);
        assert_eq!(successor(&[1, 0xff, 0xff]).unwrap().as_slice(), [2]);
        assert_eq!(successor(&[0xff, 0xff]), None);
        assert_eq!(successor(&[]), None);
    }

    #[test]
    fn prefix_ranges_hold_exactly_their_keys() {
        type K = (u8, (u16, [u8; 3]));

        let range = RangeOf::<K>::prefix(&(7u8, 0x00ffu16));
        assert!(range.contains(&(7, (0x00ff, [0; 3]))));
        assert!(range.contains(&(7, (0x00ff, [0xff; 3]))));
        assert!(!range.contains(&(7, (0x0100, [0; 3]))));
        assert!(!range.contains(&(6, (0x00ff, [0xff; 3]))));

        let all = RangeOf::<K>::prefix(&0xffu8);
        assert_eq!(all.raw().end, None);
        assert!(all.contains(&(0xff, (0xffff, [0xff; 3]))));
    }

    #[test]
    fn log_ranges_bound_by_slot() {
        let key = |slot, entity: u32| LogKey::of(slot, &entity);

        let at = LogRangeOf::<u32>::slot(5);
        assert!(at.raw().contains(key(5, 0).as_ref()));
        assert!(at.raw().contains(key(5, u32::MAX).as_ref()));
        assert!(!at.raw().contains(key(4, u32::MAX).as_ref()));
        assert!(!at.raw().contains(key(6, 0).as_ref()));

        let slots = LogRangeOf::<u32>::slots(5..7);
        assert!(slots.raw().contains(key(6, u32::MAX).as_ref()));
        assert!(!slots.raw().contains(key(7, 0).as_ref()));

        let prefixed = LogRangeOf::<(u8, u32)>::prefix(5, &3u8);
        assert!(prefixed
            .raw()
            .contains(LogKey::of(5, &(3u8, 9u32)).as_ref()));
        assert!(!prefixed
            .raw()
            .contains(LogKey::of(5, &(4u8, 0u32)).as_ref()));
        assert!(!prefixed
            .raw()
            .contains(LogKey::of(6, &(3u8, 0u32)).as_ref()));
    }

    #[test]
    fn log_keys_carry_their_slot() {
        let key = LogKey::of(42, &(1u8, 2u16));
        assert_eq!(key.slot(), 42);
        assert_eq!(key.entity(), [1, 0, 2]);
        assert_eq!(key.decode::<(u8, u16)>(), Ok((42, (1, 2))));
        assert_eq!(LogKey::of(42, &()).as_ref().len(), 8);
        assert!(LogKey::from_raw(&[0; 7]).is_err());
    }

    proptest! {
        #[test]
        fn composite_keys_round_trip_and_order_by_parts(
            a in any::<(u16, (u32, [u8; 4]))>(),
            b in any::<(u16, (u32, [u8; 4]))>(),
        ) {
            prop_assert_eq!(<(u16, (u32, [u8; 4]))>::decode_key(&bytes(&a)), Ok(a));
            prop_assert_eq!(bytes(&a).cmp(&bytes(&b)), a.cmp(&b));
        }

        #[test]
        fn variable_tails_round_trip(head in any::<u32>(), tail in any::<[u8; 5]>()) {
            let key = (head, (tail[0], tail));
            let raw = bytes(&key);
            prop_assert_eq!(<(u32, (u8, [u8; 5]))>::decode_key(&raw), Ok(key));
            prop_assert!(<(u32, (u8, [u8; 5]))>::decode_key(&raw[..raw.len() - 1]).is_err());
            let mut long = raw.clone();
            long.push(0);
            prop_assert!(<(u32, (u8, [u8; 5]))>::decode_key(&long).is_err());
        }

        #[test]
        fn prefix_range_matches_starts_with(prefix in any::<[u8; 2]>(), key in any::<([u8; 2], u16)>()) {
            let range = RangeOf::<([u8; 2], u16)>::prefix(&prefix);
            prop_assert_eq!(range.contains(&key), key.0 == prefix);
        }
    }
}
