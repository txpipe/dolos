//! The asset key. An asset's supply, mint count and first and metadata txs
//! are not state: `periods::assets` folds them from the asset-mint period logs.

use pallas::crypto::hash::{Hash, Hasher};

hash_key!(
    /// An asset, keyed by `blake2b256(policy ‖ name)`; [`AssetKey::of`] and
    /// [`AssetKey::of_parts`] are the only places that hash is taken.
    AssetKey,
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

display_as_stored!(AssetKey);

#[cfg(test)]
mod key_tests {
    use dolos_core::{keys::encode, KeyCodec};
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn layout_is_pinned() {
        assert_eq!(
            hex::encode(encode(&AssetKey::of(b"policy-and-name"))),
            hex::encode(Hasher::<256>::hash(b"policy-and-name"))
        );
        assert_eq!(
            AssetKey::of_parts(b"policy-and-", b"name"),
            AssetKey::of(b"policy-and-name")
        );
    }

    proptest! {
        #[test]
        fn keys_round_trip(hash in any::<[u8; 32]>()) {
            let key = AssetKey(hash.into());
            prop_assert_eq!(AssetKey::decode_key(&encode(&key)), Ok(key));
        }
    }
}
