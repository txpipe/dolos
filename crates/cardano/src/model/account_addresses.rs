use pallas::{
    codec::minicbor::{self, bytes::ByteVec, Decode, Encode},
    ledger::primitives::{Epoch, StakeCredential},
};
use serde::{Deserialize, Serialize};

use super::{AccountKey, AddressKey};
use crate::periods::{GroupKey, GroupRow, StageIfAbsent};

/// The first appearance of an address under an account within one epoch,
/// held in state until the epoch seals into an [`AccountAddressChunk`].
///
/// Keyed `account ‖ epoch ‖ AddressKey`, so one prefix scan returns every
/// unsealed epoch of an account, and nothing else.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct AccountAddressSet {
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
    AccountAddressSet,
    "account-addresses",
    state = GroupKey<AccountKey, AddressKey>
);

impl AccountAddressSet {
    /// Chain order: slot, then tx index, then output index.
    pub fn chain_order(&self) -> (u64, u32, u32) {
        (self.slot, self.tx_index, self.output_index)
    }
}

impl GroupRow for AccountAddressSet {
    fn key(&self) -> GroupKey<AccountKey, AddressKey> {
        GroupKey {
            group: AccountKey(self.account.clone()),
            epoch: self.epoch,
            member: AddressKey::split(Some(&self.account), self.address.clone()),
        }
    }
}

pub type StageAccountAddress = StageIfAbsent<AccountAddressSet>;

/// The addresses seen under an account in one epoch, in order of first
/// appearance within it, built by the seal of [`AccountAddressSet`].
///
/// Sealed at `epoch_start(e) ‖ account`. An address active in several epochs
/// is listed in each; readers drop the repeats.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Default)]
pub struct AccountAddressChunk {
    #[n(0)]
    pub addresses: Vec<ByteVec>,
}

entity_boilerplate!(
    AccountAddressChunk,
    "account-address-chunks",
    log = AccountKey
);

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_account_address_set()(
            account in root::any_stake_credential(),
            address in prop::collection::vec(any::<u8>(), 1..64),
            epoch in 0u64..1000,
            slot in root::any_slot(),
            tx_index in 0u32..500,
            output_index in 0u32..500,
        ) -> AccountAddressSet {
            AccountAddressSet { account, address, epoch, slot, tx_index, output_index }
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use super::testing::any_account_address_set;
    use super::*;
    use crate::model::testing::{assert_delta_roundtrip, assert_delta_serde_roundtrip};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn stage_account_address_roundtrip(
            entity in prop::option::of(any_account_address_set()),
            row in any_account_address_set(),
        ) {
            assert_delta_roundtrip(entity.clone(), StageAccountAddress::new(row.clone()));
            assert_delta_serde_roundtrip(entity, StageAccountAddress::new(row));
        }
    }
}

#[cfg(test)]
mod key_tests {
    use dolos_core::{keys::encode, KeyCodec as _, RangeOf};
    use pallas::crypto::hash::Hash;
    use proptest::prelude::*;

    use super::*;

    fn key(
        account: StakeCredential,
        epoch: Epoch,
        address: &[u8],
    ) -> GroupKey<AccountKey, AddressKey> {
        GroupKey {
            group: AccountKey(account.clone()),
            epoch,
            member: AddressKey::split(Some(&account), address.to_vec()),
        }
    }

    /// A base address with a script payment part, staked under `account`.
    fn base(payment: u8, account: &StakeCredential) -> Vec<u8> {
        let (StakeCredential::AddrKeyhash(hash) | StakeCredential::ScriptHash(hash)) = account;
        [[0x31].as_slice(), &[payment; 28], hash.as_slice()].concat()
    }

    #[test]
    fn layouts_are_pinned() {
        let account = StakeCredential::ScriptHash(Hash::new([0x12; 28]));
        let address = base(0x31, &account);
        let header = [0x82, 0x01, 0x58, 0x1c];

        assert_eq!(
            encode(&key(account, 7, &address)).to_vec(),
            [
                header.as_slice(),
                &[0x12; 28],
                &7u64.to_be_bytes(),
                &header,
                &[0x12; 28],
                &[0x31],
                &[0x31; 28],
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

        assert!(range.contains(&key(a.clone(), 0, &base(0x00, &a))));
        assert!(range.contains(&key(a.clone(), u64::MAX, &base(0xff, &a))));
        assert!(!range.contains(&key(b.clone(), 3, &base(0x00, &b))));
        assert!(!range.contains(&key(script.clone(), 3, &base(0x00, &script))));
    }

    proptest! {
        #[test]
        fn group_keys_round_trip(
            hash in any::<[u8; 28]>(),
            epoch in any::<u64>(),
            payment in any::<u8>(),
        ) {
            let account = StakeCredential::AddrKeyhash(hash.into());
            let key = key(account.clone(), epoch, &base(payment, &account));
            let raw = encode(&key);
            prop_assert_eq!(GroupKey::decode_key(&raw), Ok(key));
        }
    }

    #[test]
    fn an_empty_address_is_refused() {
        let account = StakeCredential::AddrKeyhash(Hash::new([0; 28]));
        let empty = key(account, 1, &[]);

        assert_eq!(empty.member, AddressKey::Unstaked(vec![]));
        assert!(GroupKey::<AccountKey, AddressKey>::decode_key(&encode(&empty)).is_err());
    }
}
