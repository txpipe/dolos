use dolos_core::{KeyBytes, KeyCodec, KeyError};
use pallas::{
    codec::minicbor::{self, bytes::ByteVec, Decode, Encode},
    ledger::primitives::{Epoch, StakeCredential},
};
use serde::{Deserialize, Serialize};

use super::AccountKey;
use crate::periods::{StageIfAbsent, StagedKey, StagedRow};

/// An address, keyed by its bytes, whole: the last part of a key, so its
/// length needs no prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AddressKey(pub Vec<u8>);

impl KeyCodec for AddressKey {
    fn encode_key(&self, out: &mut KeyBytes) {
        out.extend_from_slice(&self.0);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        if bytes.is_empty() {
            return Err(KeyError::Malformed("empty address".into()));
        }

        Ok(Self(bytes.to_vec()))
    }
}

display_as_stored!(AddressKey);

/// The first appearance of an address under a stake credential within one
/// epoch, staged until the epoch seals.
///
/// Keyed `account ‖ epoch ‖ address`, so one prefix scan returns every staged
/// epoch of an account, and nothing else.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct StagedStakeAddress {
    #[n(0)]
    pub account: StakeCredential,

    #[n(1)]
    #[cbor(with = "minicbor::bytes")]
    pub address: Vec<u8>,

    #[n(2)]
    pub epoch: Epoch,

    #[n(3)]
    pub slot: u64,

    #[n(4)]
    pub tx_index: u32,

    #[n(5)]
    pub output_index: u32,
}

entity_boilerplate!(
    StagedStakeAddress,
    "staged-stake-addresses",
    state = StagedKey<AccountKey, AddressKey>
);

impl StagedStakeAddress {
    /// Chain order: slot, then tx index, then output index.
    pub fn chain_order(&self) -> (u64, u32, u32) {
        (self.slot, self.tx_index, self.output_index)
    }
}

impl StagedRow for StagedStakeAddress {
    fn key(&self) -> StagedKey<AccountKey, AddressKey> {
        StagedKey {
            group: AccountKey(self.account.clone()),
            epoch: self.epoch,
            member: AddressKey(self.address.clone()),
        }
    }

    fn epoch(&self) -> Epoch {
        self.epoch
    }
}

pub type StageStakeAddress = StageIfAbsent<StagedStakeAddress>;

/// The addresses seen under a stake credential in one epoch, in order of
/// first appearance within it.
///
/// Sealed at `epoch_start(e) ‖ account`. An address active in several epochs
/// is listed in each; readers drop the repeats.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Default)]
pub struct StakeAddressesRow {
    #[n(0)]
    pub addresses: Vec<ByteVec>,
}

entity_boilerplate!(StakeAddressesRow, "stake-addresses", log = AccountKey);

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_staged_stake_address()(
            account in root::any_stake_credential(),
            address in prop::collection::vec(any::<u8>(), 1..64),
            epoch in 0u64..1000,
            slot in root::any_slot(),
            tx_index in 0u32..500,
            output_index in 0u32..500,
        ) -> StagedStakeAddress {
            StagedStakeAddress { account, address, epoch, slot, tx_index, output_index }
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use super::testing::any_staged_stake_address;
    use super::*;
    use crate::model::testing::{assert_delta_roundtrip, assert_delta_serde_roundtrip};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn stage_stake_address_roundtrip(
            entity in prop::option::of(any_staged_stake_address()),
            row in any_staged_stake_address(),
        ) {
            assert_delta_roundtrip(entity.clone(), StageStakeAddress::new(row.clone()));
            assert_delta_serde_roundtrip(entity, StageStakeAddress::new(row));
        }
    }
}

#[cfg(test)]
mod key_tests {
    use dolos_core::{keys::encode, RangeOf};
    use pallas::crypto::hash::Hash;
    use proptest::prelude::*;

    use super::*;

    fn key(
        account: StakeCredential,
        epoch: Epoch,
        address: &[u8],
    ) -> StagedKey<AccountKey, AddressKey> {
        StagedKey {
            group: AccountKey(account),
            epoch,
            member: AddressKey(address.to_vec()),
        }
    }

    #[test]
    fn layouts_are_pinned() {
        let account = StakeCredential::ScriptHash(Hash::new([0x12; 28]));
        let address = [0x31; 57];

        assert_eq!(encode(&AddressKey(address.to_vec())).to_vec(), address);
        assert_eq!(
            encode(&key(account, 7, &address)).to_vec(),
            [
                [0x82, 0x01, 0x58, 0x1c].as_slice(),
                &[0x12; 28],
                &7u64.to_be_bytes(),
                &address
            ]
            .concat()
        );
    }

    #[test]
    fn an_account_prefix_holds_its_rows_alone() {
        let a = StakeCredential::AddrKeyhash(Hash::new([0x12; 28]));
        let b = StakeCredential::AddrKeyhash(Hash::new([0x13; 28]));
        let script = StakeCredential::ScriptHash(Hash::new([0x12; 28]));

        let range = RangeOf::prefix(&AccountKey(a.clone()));

        assert!(range.contains(&key(a.clone(), 0, &[0])));
        assert!(range.contains(&key(a, u64::MAX, &[0xff; 120])));
        assert!(!range.contains(&key(b, 3, &[0])));
        assert!(!range.contains(&key(script, 3, &[0])));
    }

    proptest! {
        #[test]
        fn staged_keys_round_trip(
            hash in any::<[u8; 28]>(),
            epoch in any::<u64>(),
            address in prop::collection::vec(any::<u8>(), 1..120),
        ) {
            let key = key(StakeCredential::AddrKeyhash(hash.into()), epoch, &address);
            let raw = encode(&key);
            prop_assert_eq!(StagedKey::decode_key(&raw), Ok(key));
        }
    }

    #[test]
    fn an_empty_address_is_refused() {
        assert!(AddressKey::decode_key(&[]).is_err());
        assert!(
            StagedKey::<AccountKey, AddressKey>::decode_key(&encode(&key(
                StakeCredential::AddrKeyhash(Hash::new([0; 28])),
                1,
                &[]
            )))
            .is_err()
        );
    }
}
