//! The keys of the Cardano state entities and archive logs (decision 0051).
//!
//! Each key kind has one struct here, and every reader and writer of a
//! namespace goes through it, so they share one convention for its bytes:
//!
//! | Struct | Used by | Bytes |
//! |---|---|---|
//! | [`AccountKey`] | accounts, pending rewards and MIRs; `account-epochs` | CBOR stake credential, 32 B |
//! | [`PoolKey`] | pools; `stakes` | 28 B |
//! | [`DRepKey`] | dreps | header ‖ hash, 29 B, or 1 B for abstain and no-confidence |
//! | [`ProposalKey`] | proposals; `action_votes` tag | `idx (4 B) ‖ tx (32 B)`, 36 B |
//! | [`AssetKey`] | assets | `blake2b256(policy ‖ name)`, 32 B |
//! | [`DatumKey`] | datums | 32 B |
//! | [`EraProtocol`] | eras | `u16`, 2 B |
//! | `()` | epochs and gov singletons; `epochs` log | empty |
//! | [`PolicyKey`] | the staged first-mint group; `policy-first-mints` with a `u32` chunk | 28 B |
//! | `u32` | `first-mints` | chunk index, 4 B |
//! | [`crate::periods::StagedKey`] | period-log staged rows | `group ‖ epoch (8 B) ‖ member` |
//!
//! Log keys are the part after the slot. Every integer is big-endian.

use dolos_core::{keys::exact, FixedKey, KeyBytes, KeyCodec, KeyError, KeyPrefix, RangeOf};
use pallas::{
    crypto::hash::{Hash, Hasher},
    ledger::primitives::{conway::DRep, StakeCredential},
};
use serde::{Deserialize, Serialize};

use super::{EraProtocol, PoolHash};
use crate::{
    pallas_extras::{DREP_KEY_PREFIX, DREP_SCRIPT_PREFIX},
    shard::{validate_total_shards, PREFIX_SPACE},
};

/// The variant of a stake credential, keyed by the CBOR header its
/// [`AccountKey`]s start with: `0x82 ‖ variant ‖ 0x58 0x1c`.
///
/// The header carries no entropy, so it is the prefix the account shards
/// split under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CredentialKind {
    KeyHash,
    Script,
}

impl CredentialKind {
    const ARRAY: u8 = 0x82;
    const BYTES_28: [u8; 2] = [0x58, 0x1c];

    fn variant(self) -> u8 {
        match self {
            Self::KeyHash => 0,
            Self::Script => 1,
        }
    }

    pub fn of(credential: &StakeCredential) -> Self {
        match credential {
            StakeCredential::AddrKeyhash(_) => Self::KeyHash,
            StakeCredential::ScriptHash(_) => Self::Script,
        }
    }

    fn credential(self, hash: Hash<28>) -> StakeCredential {
        match self {
            Self::KeyHash => StakeCredential::AddrKeyhash(hash),
            Self::Script => StakeCredential::ScriptHash(hash),
        }
    }
}

impl KeyCodec for CredentialKind {
    fn encode_key(&self, out: &mut KeyBytes) {
        out.extend_from_slice(&[
            Self::ARRAY,
            self.variant(),
            Self::BYTES_28[0],
            Self::BYTES_28[1],
        ]);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        match exact::<4>(bytes)? {
            [Self::ARRAY, 0, 0x58, 0x1c] => Ok(Self::KeyHash),
            [Self::ARRAY, 1, 0x58, 0x1c] => Ok(Self::Script),
            header => Err(KeyError::Malformed(format!(
                "stake credential header {}",
                hex::encode(header)
            ))),
        }
    }
}

impl FixedKey for CredentialKind {
    const LEN: usize = 4;
}

impl KeyPrefix<AccountKey> for CredentialKind {}

/// A stake credential, keyed by its CBOR encoding: the
/// [`CredentialKind`] header and the 28-byte hash.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AccountKey(pub StakeCredential);

impl AccountKey {
    pub fn credential(&self) -> &StakeCredential {
        &self.0
    }

    /// The two ranges, one per credential variant, of shard `index` of
    /// `total`. Shards split on the first byte of the hash, the first byte
    /// after the header, so `total` must divide 256.
    ///
    /// Panics on an invalid split in every build profile: `total` can come
    /// from persisted progress, and a broken partition would skip or repeat
    /// accounts silently.
    pub fn shard_ranges(index: u32, total: u32) -> Vec<RangeOf<AccountKey>> {
        if let Err(e) = validate_total_shards(total) {
            panic!("shard_ranges: {e}");
        }

        assert!(
            index < total,
            "shard_ranges: shard index ({index}) must be < total shards ({total})",
        );

        let step = PREFIX_SPACE / total;
        let bucket = |kind: CredentialKind, first: u32| {
            let mut hash = [0u8; 28];
            hash[0] = first as u8;
            AccountKey(kind.credential(hash.into()))
        };

        [CredentialKind::KeyHash, CredentialKind::Script]
            .into_iter()
            .map(|kind| {
                let range = RangeOf::prefix(&kind).starting_at(&bucket(kind, index * step));

                if index + 1 == total {
                    range
                } else {
                    range.ending_before(&bucket(kind, (index + 1) * step))
                }
            })
            .collect()
    }
}

impl From<StakeCredential> for AccountKey {
    fn from(value: StakeCredential) -> Self {
        Self(value)
    }
}

impl From<&StakeCredential> for AccountKey {
    fn from(value: &StakeCredential) -> Self {
        Self(value.clone())
    }
}

impl From<AccountKey> for StakeCredential {
    fn from(value: AccountKey) -> Self {
        value.0
    }
}

impl KeyCodec for AccountKey {
    fn encode_key(&self, out: &mut KeyBytes) {
        CredentialKind::of(&self.0).encode_key(out);

        match &self.0 {
            StakeCredential::AddrKeyhash(hash) | StakeCredential::ScriptHash(hash) => {
                out.extend_from_slice(hash.as_slice())
            }
        }
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        let (kind, hash) = <(CredentialKind, [u8; 28])>::decode_key(bytes)?;
        Ok(Self(kind.credential(hash.into())))
    }
}

impl FixedKey for AccountKey {
    const LEN: usize = 32;
}

macro_rules! hash_key {
    ($(#[$doc:meta])* $name:ident, $hash:ty, $len:literal) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(pub $hash);

        impl From<$hash> for $name {
            fn from(value: $hash) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $hash {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl KeyCodec for $name {
            fn encode_key(&self, out: &mut KeyBytes) {
                out.extend_from_slice(self.0.as_slice());
            }

            fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
                exact::<$len>(bytes).map(|hash| Self(hash.into()))
            }
        }

        impl FixedKey for $name {
            const LEN: usize = $len;
        }
    };
}

hash_key!(
    /// A pool, keyed by its operator hash.
    PoolKey,
    PoolHash,
    28
);

hash_key!(
    /// A minting policy.
    PolicyKey,
    Hash<28>,
    28
);

hash_key!(
    /// An asset, keyed by `blake2b256(policy ‖ name)`; [`AssetKey::of`] and
    /// [`AssetKey::of_parts`] are the only places that hash is taken.
    AssetKey,
    Hash<32>,
    32
);

hash_key!(
    /// A datum, keyed by its hash.
    DatumKey,
    Hash<32>,
    32
);

impl AssetKey {
    /// The key of the asset whose subject is `policy ‖ name`.
    pub fn of(subject: &[u8]) -> Self {
        Self(Hasher::<256>::hash(subject))
    }

    /// The key of the asset `name` under `policy`, without building the
    /// subject.
    pub fn of_parts(policy: &[u8], name: &[u8]) -> Self {
        let mut hasher = Hasher::<256>::new();
        hasher.input(policy);
        hasher.input(name);
        Self(hasher.finalize())
    }
}

/// A DRep: the CIP-129 credential header (key or script) and its hash, 29 B,
/// or a single byte for the predefined abstain (`0`) and no-confidence (`1`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DRepKey(pub DRep);

/// Hashes the stored bytes, which tell DReps apart exactly as `Eq` does.
impl std::hash::Hash for DRepKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        dolos_core::keys::encode(self).hash(state);
    }
}

/// Orders by the stored bytes, the order the store keeps DReps in.
impl Ord for DRepKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        dolos_core::keys::encode(self).cmp(&dolos_core::keys::encode(other))
    }
}

impl PartialOrd for DRepKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<DRep> for DRepKey {
    fn from(value: DRep) -> Self {
        Self(value)
    }
}

impl From<&DRep> for DRepKey {
    fn from(value: &DRep) -> Self {
        Self(value.clone())
    }
}

impl From<DRepKey> for DRep {
    fn from(value: DRepKey) -> Self {
        value.0
    }
}

impl KeyCodec for DRepKey {
    fn encode_key(&self, out: &mut KeyBytes) {
        match &self.0 {
            DRep::Key(hash) => {
                out.push(DREP_KEY_PREFIX);
                out.extend_from_slice(hash.as_slice());
            }
            DRep::Script(hash) => {
                out.push(DREP_SCRIPT_PREFIX);
                out.extend_from_slice(hash.as_slice());
            }
            DRep::Abstain => out.push(0),
            DRep::NoConfidence => out.push(1),
        }
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        let Some((header, hash)) = bytes.split_first() else {
            return Err(KeyError::Short {
                expected: 1,
                found: 0,
            });
        };

        let drep = match *header {
            0 => exact::<0>(hash).map(|_| DRep::Abstain)?,
            1 => exact::<0>(hash).map(|_| DRep::NoConfidence)?,
            DREP_KEY_PREFIX => DRep::Key(exact::<28>(hash)?.into()),
            DREP_SCRIPT_PREFIX => DRep::Script(exact::<28>(hash)?.into()),
            other => {
                return Err(KeyError::Malformed(format!("drep header {other:#04x}")));
            }
        };

        Ok(Self(drep))
    }
}

/// A governance proposal: its index, then its transaction, `idx ‖ tx`, 36 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProposalKey {
    pub tx: Hash<32>,
    pub idx: u32,
}

/// Orders as the stored bytes do: index first, then transaction.
impl Ord for ProposalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.idx, self.tx).cmp(&(other.idx, other.tx))
    }
}

impl PartialOrd for ProposalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl ProposalKey {
    pub fn new(tx: Hash<32>, idx: u32) -> Self {
        Self { tx, idx }
    }
}

impl KeyCodec for ProposalKey {
    fn encode_key(&self, out: &mut KeyBytes) {
        (self.idx, *self.tx).encode_key(out);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        let (idx, tx) = <(u32, [u8; 32])>::decode_key(bytes)?;
        Ok(Self::new(tx.into(), idx))
    }
}

impl FixedKey for ProposalKey {
    const LEN: usize = 36;
}

/// Keys display as the hex of their stored bytes.
macro_rules! display_as_stored {
    ($($name:ty),*) => {
        $(
            impl std::fmt::Display for $name {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str(&hex::encode(dolos_core::keys::encode(self)))
                }
            }
        )*
    };
}

display_as_stored!(
    AccountKey,
    PoolKey,
    PolicyKey,
    AssetKey,
    DatumKey,
    DRepKey,
    ProposalKey
);

impl KeyCodec for EraProtocol {
    fn encode_key(&self, out: &mut KeyBytes) {
        u16::from(*self).encode_key(out);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        u16::decode_key(bytes).map(Self::from)
    }
}

impl FixedKey for EraProtocol {
    const LEN: usize = 2;
}

#[cfg(test)]
mod tests {
    use dolos_core::keys::encode;
    use pallas::codec::minicbor;
    use proptest::prelude::*;

    use super::*;
    use crate::model::testing as root;

    fn bytes<K: KeyCodec>(key: &K) -> Vec<u8> {
        encode(key).to_vec()
    }

    fn hash<const N: usize>(fill: u8) -> Hash<N> {
        Hash::new([fill; N])
    }

    /// Every layout of the key table, pinned byte for byte.
    #[test]
    fn layouts_are_pinned() {
        let account = AccountKey(StakeCredential::AddrKeyhash(hash(0xaa)));
        assert_eq!(
            hex::encode(bytes(&account)),
            format!("8200581c{}", "aa".repeat(28))
        );

        let script = AccountKey(StakeCredential::ScriptHash(hash(0xbb)));
        assert_eq!(
            hex::encode(bytes(&script)),
            format!("8201581c{}", "bb".repeat(28))
        );

        assert_eq!(bytes(&PoolKey(hash(0x11))), [0x11; 28]);
        assert_eq!(bytes(&PolicyKey(hash(0x12))), [0x12; 28]);
        assert_eq!(bytes(&DatumKey(hash(0x13))), [0x13; 32]);

        assert_eq!(
            hex::encode(bytes(&DRepKey(DRep::Key(hash(0x14))))),
            format!("22{}", "14".repeat(28))
        );
        assert_eq!(
            hex::encode(bytes(&DRepKey(DRep::Script(hash(0x15))))),
            format!("23{}", "15".repeat(28))
        );
        assert_eq!(bytes(&DRepKey(DRep::Abstain)), [0]);
        assert_eq!(bytes(&DRepKey(DRep::NoConfidence)), [1]);

        assert_eq!(
            hex::encode(bytes(&ProposalKey::new(hash(0x16), 0x0102_0304))),
            format!("01020304{}", "16".repeat(32))
        );

        assert_eq!(
            hex::encode(bytes(&AssetKey::of(b"policy-and-name"))),
            hex::encode(Hasher::<256>::hash(b"policy-and-name"))
        );
        assert_eq!(
            AssetKey::of_parts(b"policy-and-", b"name"),
            AssetKey::of(b"policy-and-name")
        );

        assert_eq!(bytes(&EraProtocol::from(9)), [0, 9]);
        assert!(bytes(&()).is_empty());
        assert_eq!(bytes(&7u32), [0, 0, 0, 7]);
        assert_eq!(
            bytes(&(PolicyKey(hash(0x17)), 2u32)),
            [[0x17; 28].as_slice(), &[0, 0, 0, 2]].concat()
        );
    }

    /// The account key is the credential's own CBOR, unchanged.
    #[test]
    fn account_keys_are_the_credential_cbor() {
        for credential in [
            StakeCredential::AddrKeyhash(hash(0x42)),
            StakeCredential::ScriptHash(hash(0x43)),
        ] {
            assert_eq!(
                bytes(&AccountKey(credential.clone())),
                minicbor::to_vec(&credential).unwrap()
            );
        }
    }

    #[test]
    fn malformed_keys_are_refused() {
        let account = bytes(&AccountKey(StakeCredential::AddrKeyhash(hash(1))));
        assert!(AccountKey::decode_key(&account[..31]).is_err());
        assert!(AccountKey::decode_key(&[account.as_slice(), &[0]].concat()).is_err());

        let mut bad_header = account.clone();
        bad_header[1] = 2;
        assert!(AccountKey::decode_key(&bad_header).is_err());

        assert!(DRepKey::decode_key(&[]).is_err());
        assert!(DRepKey::decode_key(&[0, 0]).is_err());
        assert!(DRepKey::decode_key(&[DREP_KEY_PREFIX; 28]).is_err());
        assert!(DRepKey::decode_key(&[DREP_KEY_PREFIX; 30]).is_err());
        assert!(DRepKey::decode_key(&[0x42; 29]).is_err());

        assert!(PoolKey::decode_key(&[0; 32]).is_err());
        assert!(ProposalKey::decode_key(&[0; 32]).is_err());
        assert!(EraProtocol::decode_key(&[0; 32]).is_err());
    }

    fn any_drep_key() -> impl Strategy<Value = DRepKey> {
        root::any_drep().prop_map(DRepKey)
    }

    proptest! {
        #[test]
        fn account_keys_round_trip(credential in root::any_stake_credential()) {
            let key = AccountKey(credential);
            prop_assert_eq!(AccountKey::decode_key(&bytes(&key)), Ok(key));
        }

        #[test]
        fn drep_keys_round_trip(key in any_drep_key()) {
            prop_assert_eq!(DRepKey::decode_key(&bytes(&key)), Ok(key));
        }

        #[test]
        fn hash_keys_round_trip(pool in any::<[u8; 28]>(), datum in any::<[u8; 32]>()) {
            prop_assert_eq!(PoolKey::decode_key(&bytes(&PoolKey(pool.into()))), Ok(PoolKey(pool.into())));
            prop_assert_eq!(PolicyKey::decode_key(&bytes(&PolicyKey(pool.into()))), Ok(PolicyKey(pool.into())));
            prop_assert_eq!(DatumKey::decode_key(&bytes(&DatumKey(datum.into()))), Ok(DatumKey(datum.into())));
            prop_assert_eq!(AssetKey::decode_key(&bytes(&AssetKey(datum.into()))), Ok(AssetKey(datum.into())));
        }

        #[test]
        fn proposal_keys_round_trip_in_idx_then_tx_order(
            a in (any::<[u8; 32]>(), any::<u32>()),
            b in (any::<[u8; 32]>(), any::<u32>()),
        ) {
            let a = ProposalKey::new(a.0.into(), a.1);
            let b = ProposalKey::new(b.0.into(), b.1);
            prop_assert_eq!(ProposalKey::decode_key(&bytes(&a)), Ok(a));
            prop_assert_eq!(bytes(&a).cmp(&bytes(&b)), (a.idx, a.tx).cmp(&(b.idx, b.tx)));
        }

        #[test]
        fn era_keys_round_trip(era in any::<u16>()) {
            let key = EraProtocol::from(era);
            prop_assert_eq!(EraProtocol::decode_key(&bytes(&key)), Ok(key));
        }

        /// Exactly one shard range holds any account key.
        #[test]
        fn shards_partition_the_accounts(credential in root::any_stake_credential(), total in prop::sample::select(vec![1u32, 2, 16, 32, 256])) {
            let key = AccountKey(credential);

            let hits = (0..total)
                .flat_map(|index| AccountKey::shard_ranges(index, total))
                .filter(|range| range.contains(&key))
                .count();

            prop_assert_eq!(hits, 1);
        }
    }

    #[test]
    fn shard_ranges_split_on_the_first_hash_byte() {
        let key = |kind: CredentialKind, first: u8| {
            let mut hash = [0xffu8; 28];
            hash[0] = first;
            AccountKey(kind.credential(hash.into()))
        };

        for kind in [CredentialKind::KeyHash, CredentialKind::Script] {
            let index = usize::from(kind == CredentialKind::Script);

            let third = &AccountKey::shard_ranges(3, 16)[index];
            assert!(third.contains(&key(kind, 0x30)));
            assert!(third.contains(&key(kind, 0x3f)));
            assert!(!third.contains(&key(kind, 0x40)));
            assert!(!third.contains(&key(kind, 0x2f)));

            let last = &AccountKey::shard_ranges(15, 16)[index];
            assert!(last.contains(&key(kind, 0xff)));
            assert!(!last.contains(&key(kind, 0xef)));
        }
    }

    #[test]
    #[should_panic(expected = "must divide")]
    fn shard_ranges_refuse_a_non_divisor() {
        AccountKey::shard_ranges(0, 3);
    }
}
