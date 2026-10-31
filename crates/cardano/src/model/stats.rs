//! Stats (decision 0050): per-epoch received and sent sums and tx counts by
//! address, payment credential and account.

use std::collections::BTreeMap;

use dolos_core::{FixedKey as _, KeyBytes, KeyCodec, KeyError, KeyPrefix};
use pallas::{
    codec::{
        minicbor::{
            self,
            bytes::ByteVec,
            data::{IanaTag, Tag, Type},
            Decode, Decoder, Encode, Encoder,
        },
        utils::Bytes,
    },
    crypto::hash::Hash,
    ledger::{
        addresses::Address,
        primitives::{Epoch, StakeCredential},
    },
};
use serde::{Deserialize, Serialize};

use super::AccountKey;
use crate::periods::{GroupKey, GroupRow, StageIfAbsent, StageSum, SumRow};

hash_key!(
    /// A payment credential, by its hash alone: what `/addresses/{address}/total`
    /// matches a payment credential on.
    PaymentKey,
    Hash<28>,
    28
);

display_as_stored!(PaymentKey);

const UNSTAKED: u8 = 0x00;

/// A stake hash closes every address that has a stake credential.
const STAKE_HASH: usize = 28;

/// An address, keyed stake-first.
///
/// `AccountKey ‖ rest` for an address with a stake credential, where `rest`
/// is the address without its stake hash (a base address's header and
/// payment hash, a stake address's header), and `0x00 ‖ address` for any
/// other. An `AccountKey` opens with its CBOR header `0x82`, so the first
/// byte tells the forms apart, the header fixes the length of `rest`, and an
/// account's addresses are its prefix range.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AddressKey {
    Staked { account: AccountKey, rest: Vec<u8> },
    Unstaked(Vec<u8>),
}

impl AddressKey {
    pub fn of(address: &Address) -> Self {
        Self::split(
            crate::pallas_extras::address_account(address).as_ref(),
            address.to_vec(),
        )
    }

    /// The key of an address's bytes, staked under `account` when they close
    /// with its hash.
    pub fn split(account: Option<&StakeCredential>, address: Vec<u8>) -> Self {
        let staked = |account: &StakeCredential| {
            address
                .first()
                .and_then(|header| rest_len(*header))
                .is_some_and(|rest| address.len() == rest + STAKE_HASH)
                && address.ends_with(account_hash(account).as_slice())
        };

        match account {
            Some(account) if staked(account) => {
                let mut rest = address;
                rest.truncate(rest.len() - STAKE_HASH);

                Self::Staked {
                    account: AccountKey(account.clone()),
                    rest,
                }
            }
            _ => Self::Unstaked(address),
        }
    }

    /// The address the key was built from, whole.
    pub fn address(&self) -> Vec<u8> {
        match self {
            Self::Staked { account, rest } => {
                [rest.as_slice(), account_hash(&account.0).as_slice()].concat()
            }
            Self::Unstaked(address) => address.clone(),
        }
    }
}

/// The length of a staked address's rest, by its header: a base address
/// keeps its header and payment hash, a stake address its header alone.
fn rest_len(header: u8) -> Option<usize> {
    match header >> 4 {
        0x0..=0x3 => Some(1 + 28),
        0xe | 0xf => Some(1),
        _ => None,
    }
}

fn account_hash(account: &StakeCredential) -> Hash<28> {
    match account {
        StakeCredential::AddrKeyhash(hash) | StakeCredential::ScriptHash(hash) => *hash,
    }
}

fn non_empty(bytes: &[u8], what: &str) -> Result<Vec<u8>, KeyError> {
    if bytes.is_empty() {
        return Err(KeyError::Malformed(format!("empty {what}")));
    }

    Ok(bytes.to_vec())
}

impl KeyCodec for AddressKey {
    fn encode_key(&self, out: &mut KeyBytes) {
        match self {
            Self::Staked { account, rest } => {
                account.encode_key(out);
                out.extend_from_slice(rest);
            }
            Self::Unstaked(address) => {
                out.push(UNSTAKED);
                out.extend_from_slice(address);
            }
        }
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        match bytes.split_first() {
            None => Err(KeyError::Short {
                expected: 1,
                found: 0,
            }),
            Some((&UNSTAKED, address)) => Ok(Self::Unstaked(non_empty(address, "address")?)),
            Some(_) if bytes.len() < AccountKey::LEN => Err(KeyError::Short {
                expected: AccountKey::LEN,
                found: bytes.len(),
            }),
            Some(_) => {
                let (account, rest) = bytes.split_at(AccountKey::LEN);
                let account = AccountKey::decode_key(account)?;

                let Some(&header) = rest.first() else {
                    return Err(KeyError::Short {
                        expected: AccountKey::LEN + 1,
                        found: bytes.len(),
                    });
                };

                let Some(expected) = rest_len(header) else {
                    return Err(KeyError::Malformed(format!(
                        "staked address header {header:#04x}"
                    )));
                };

                match rest.len() {
                    len if len < expected => Err(KeyError::Short {
                        expected: AccountKey::LEN + expected,
                        found: bytes.len(),
                    }),
                    len if len > expected => Err(KeyError::Trailing(len - expected)),
                    _ => Ok(Self::Staked {
                        account,
                        rest: rest.to_vec(),
                    }),
                }
            }
        }
    }
}

display_as_stored!(AddressKey);

impl KeyPrefix<AddressKey> for AccountKey {}

/// An address without the payment hash of its group: the header, then the
/// delegation part: the tail [`split_payment`] returns and [`join_payment`]
/// takes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PaymentAddressKey(pub Vec<u8>);

impl KeyCodec for PaymentAddressKey {
    fn encode_key(&self, out: &mut KeyBytes) {
        out.extend_from_slice(&self.0);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        non_empty(bytes, "payment address key").map(Self)
    }
}

/// A Shelley address's payment hash, and the address without it.
pub fn split_payment(address: &Address) -> Option<(Hash<28>, Vec<u8>)> {
    let Address::Shelley(shelley) = address else {
        return None;
    };

    let bytes = shelley.to_vec();
    let tail = [&bytes[..1], &bytes[1 + 28..]].concat();

    Some((*shelley.payment().as_hash(), tail))
}

/// The address `split_payment` took apart.
pub fn join_payment(payment: &Hash<28>, tail: &[u8]) -> Vec<u8> {
    let (header, delegation) = tail.split_at(1.min(tail.len()));
    [header, payment.as_slice(), delegation].concat()
}

/// A sum of unit quantities, wider than u64: one epoch's sums overflow it.
///
/// A CBOR unsigned integer, or a positive bignum past u64. Decode refuses a
/// bignum that fits in u64 or has leading zeros, so each sum has one
/// encoding.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Sum(pub u128);

impl<C> Encode<C> for Sum {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut Encoder<W>,
        _: &mut C,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        match u64::try_from(self.0) {
            Ok(small) => {
                e.u64(small)?;
            }
            Err(_) => {
                let bytes = self.0.to_be_bytes();
                let zeros = bytes.iter().take_while(|byte| **byte == 0).count();
                e.tag(IanaTag::PosBignum)?.bytes(&bytes[zeros..])?;
            }
        }

        Ok(())
    }
}

impl<'b, C> Decode<'b, C> for Sum {
    fn decode(d: &mut Decoder<'b>, _: &mut C) -> Result<Self, minicbor::decode::Error> {
        if d.datatype()? != Type::Tag {
            return d.u64().map(|small| Self(small.into()));
        }

        if d.tag()? != Tag::from(IanaTag::PosBignum) {
            return Err(minicbor::decode::Error::message(
                "sum: not a positive bignum",
            ));
        }

        let bytes = d.bytes()?;

        if bytes.len() > 16 {
            return Err(minicbor::decode::Error::message("sum: wider than u128"));
        }

        if bytes.first() == Some(&0) {
            return Err(minicbor::decode::Error::message(
                "sum: bignum with leading zeros",
            ));
        }

        if bytes.len() <= 8 {
            return Err(minicbor::decode::Error::message(
                "sum: bignum that fits in u64",
            ));
        }

        let mut wide = [0u8; 16];
        wide[16 - bytes.len()..].copy_from_slice(bytes);

        Ok(Self(u128::from_be_bytes(wide)))
    }
}

/// What an address received and sent of one unit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct UnitSums {
    #[n(0)]
    pub received: Sum,

    #[n(1)]
    pub sent: Sum,
}

impl UnitSums {
    pub fn is_zero(&self) -> bool {
        self.received.0 == 0 && self.sent.0 == 0
    }
}

/// An address's received and sent sums per unit in one epoch, and the txs
/// that moved them.
///
/// The unit is empty for lovelace and `policy ‖ name` for an asset. Only a
/// unit an output carried a quantity of is listed. Held in state at
/// `epoch ‖ AddressKey` until the epoch seals, then in the archive at
/// `epoch_start(e) ‖ AddressKey`. Epoch first: an address has no fixed-width
/// group to lead its key, so its rows are found by epoch in both stores.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct AddressStats {
    #[n(0)]
    #[cbor(with = "super::codec::canonical_map")]
    pub units: BTreeMap<Bytes, UnitSums>,

    #[n(1)]
    pub tx_count: u64,
}

entity_boilerplate!(
    AddressStats,
    "address-stats",
    state = (Epoch, AddressKey),
    log = AddressKey
);

impl AddressStats {
    pub fn add(&mut self, other: &Self) {
        for (unit, sums) in &other.units {
            match self.units.get_mut(unit) {
                Some(entry) => {
                    entry.received.0 = entry.received.0.saturating_add(sums.received.0);
                    entry.sent.0 = entry.sent.0.saturating_add(sums.sent.0);
                }
                None => {
                    self.units.insert(unit.clone(), *sums);
                }
            }
        }

        self.tx_count = self.tx_count.saturating_add(other.tx_count);
    }

    pub fn sub(&mut self, other: &Self) {
        for (unit, sums) in &other.units {
            if let Some(entry) = self.units.get_mut(unit) {
                entry.received.0 = entry.received.0.saturating_sub(sums.received.0);
                entry.sent.0 = entry.sent.0.saturating_sub(sums.sent.0);

                if entry.is_zero() {
                    self.units.remove(unit);
                }
            }
        }

        self.tx_count = self.tx_count.saturating_sub(other.tx_count);
    }

    pub fn is_zero(&self) -> bool {
        self.tx_count == 0 && self.units.is_empty()
    }
}

impl SumRow for AddressStats {
    fn add(&mut self, other: &Self) {
        AddressStats::add(self, other);
    }

    fn sub(&mut self, other: &Self) {
        AddressStats::sub(self, other);
    }

    fn is_zero(&self) -> bool {
        AddressStats::is_zero(self)
    }
}

pub type StageAddressStats = StageSum<AddressStats>;

/// A payment credential's addresses that moved value in one epoch, as
/// tails, and the txs that moved any of them: the seal of a
/// [`PaymentAddressSet`] and its [`PaymentStats`] together.
///
/// Sealed at `epoch_start(e) ‖ PaymentKey`, the tails in byte order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub struct PaymentAddressChunk {
    #[n(0)]
    pub tails: Vec<ByteVec>,

    #[n(1)]
    pub tx_count: u64,
}

entity_boilerplate!(
    PaymentAddressChunk,
    "payment-address-chunks",
    log = PaymentKey
);

/// One address of a payment credential that moved value in an epoch, held in
/// state until the epoch seals into a [`PaymentAddressChunk`].
///
/// Keyed `payment ‖ epoch ‖ tail`, so one prefix scan returns every unsealed
/// epoch of a credential.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct PaymentAddressSet {
    #[n(0)]
    pub payment: Hash<28>,

    #[n(1)]
    pub epoch: Epoch,

    #[n(2)]
    #[cbor(with = "minicbor::bytes")]
    pub tail: Vec<u8>,
}

entity_boilerplate!(
    PaymentAddressSet,
    "payment-addresses",
    state = GroupKey<PaymentKey, PaymentAddressKey>
);

impl GroupRow for PaymentAddressSet {
    fn key(&self) -> GroupKey<PaymentKey, PaymentAddressKey> {
        GroupKey {
            group: PaymentKey(self.payment),
            epoch: self.epoch,
            member: PaymentAddressKey(self.tail.clone()),
        }
    }
}

pub type StagePaymentAddress = StageIfAbsent<PaymentAddressSet>;

/// The txs that moved value through a payment credential's addresses in an
/// epoch that has not sealed, keyed `payment ‖ epoch`.
///
/// In state only: its count seals into the [`PaymentAddressChunk`] beside the
/// addresses.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct PaymentStats {
    #[n(0)]
    pub tx_count: u64,
}

entity_boilerplate!(
    PaymentStats,
    "payment-stats",
    state = GroupKey<PaymentKey, ()>
);

impl SumRow for PaymentStats {
    fn add(&mut self, other: &Self) {
        self.tx_count = self.tx_count.saturating_add(other.tx_count);
    }

    fn sub(&mut self, other: &Self) {
        self.tx_count = self.tx_count.saturating_sub(other.tx_count);
    }

    fn is_zero(&self) -> bool {
        self.tx_count == 0
    }
}

pub type StagePaymentStats = StageSum<PaymentStats>;

/// The distinct txs that moved value through any of an account's addresses
/// in one epoch.
///
/// One row per (epoch, account) whose addresses moved value, so the rows name
/// the epochs an account was active in. Held in state at `epoch ‖ AccountKey`
/// until the epoch seals, then in the archive at `epoch_start(e) ‖
/// AccountKey`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct AccountStats {
    #[n(0)]
    pub tx_count: u64,
}

entity_boilerplate!(
    AccountStats,
    "account-stats",
    state = (Epoch, AccountKey),
    log = AccountKey
);

impl SumRow for AccountStats {
    fn add(&mut self, other: &Self) {
        self.tx_count = self.tx_count.saturating_add(other.tx_count);
    }

    fn sub(&mut self, other: &Self) {
        self.tx_count = self.tx_count.saturating_sub(other.tx_count);
    }

    fn is_zero(&self) -> bool {
        self.tx_count == 0
    }
}

pub type StageAccountStats = StageSum<AccountStats>;

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    /// Sums as the roll stages them: every unit carries a quantity.
    pub fn any_address_stats() -> impl Strategy<Value = AddressStats> {
        (
            prop::collection::btree_map(
                prop::collection::vec(any::<u8>(), 0..33),
                (0u128..u128::MAX / 4, 0u128..u128::MAX / 4),
                0..6,
            ),
            1u64..1_000,
        )
            .prop_map(|(units, tx_count)| AddressStats {
                units: units
                    .into_iter()
                    .filter(|(_, (received, sent))| *received > 0 || *sent > 0)
                    .map(|(unit, (received, sent))| {
                        (
                            Bytes::from(unit),
                            UnitSums {
                                received: Sum(received),
                                sent: Sum(sent),
                            },
                        )
                    })
                    .collect(),
                tx_count,
            })
    }

    pub fn any_address_key() -> impl Strategy<Value = AddressKey> {
        prop_oneof![
            (
                root::any_stake_credential(),
                prop_oneof![
                    (0x00u8..0x40, any::<[u8; 28]>()).prop_map(|(header, payment)| [
                        [header].as_slice(),
                        &payment
                    ]
                    .concat()),
                    (0xe0u8..=0xff).prop_map(|header| vec![header]),
                ]
            )
                .prop_map(|(account, rest)| AddressKey::Staked {
                    account: AccountKey(account),
                    rest,
                }),
            prop::collection::vec(any::<u8>(), 1..90).prop_map(AddressKey::Unstaked),
        ]
    }

    prop_compose! {
        pub fn any_payment_address_set()(
            payment in root::any_hash_28(),
            epoch in 0u64..1000,
            tail in prop::collection::vec(any::<u8>(), 1..64),
        ) -> PaymentAddressSet {
            PaymentAddressSet { payment, epoch, tail }
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use super::testing::*;
    use super::*;
    use crate::model::testing::{assert_delta_roundtrip, assert_delta_serde_roundtrip};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn stage_address_stats_roundtrip(
            entity in prop::option::of(any_address_stats()),
            key in any_address_key(),
            delta in any_address_stats(),
        ) {
            let delta = StageAddressStats::new((7, key), delta);

            assert_delta_roundtrip(entity.clone(), delta.clone());
            assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn stage_counts_roundtrip(
            entity in prop::option::of(1u64..1_000_000),
            delta in 1u64..1_000_000,
        ) {
            let key = GroupKey { group: PaymentKey([1; 28].into()), epoch: 3, member: () };
            let stage = StagePaymentStats::new(key, PaymentStats { tx_count: delta });
            let rows = entity.map(|tx_count| PaymentStats { tx_count });
            assert_delta_roundtrip(rows.clone(), stage.clone());
            assert_delta_serde_roundtrip(rows, stage);

            let key = (3, AccountKey(StakeCredential::AddrKeyhash([2; 28].into())));
            let stage = StageAccountStats::new(key, AccountStats { tx_count: delta });
            let entity = entity.map(|tx_count| AccountStats { tx_count });
            assert_delta_roundtrip(entity.clone(), stage.clone());
            assert_delta_serde_roundtrip(entity, stage);
        }

        #[test]
        fn stage_payment_address_roundtrip(
            entity in prop::option::of(any_payment_address_set()),
            row in any_payment_address_set(),
        ) {
            assert_delta_roundtrip(entity.clone(), StagePaymentAddress::new(row.clone()));
            assert_delta_serde_roundtrip(entity, StagePaymentAddress::new(row));
        }

        #[test]
        fn address_stats_encode_and_decode_back(stats in any_address_stats()) {
            let raw = minicbor::to_vec(&stats).unwrap();
            prop_assert_eq!(minicbor::decode::<AddressStats>(&raw).unwrap(), stats);
        }

        #[test]
        fn address_stats_keys_round_trip(key in any_address_key()) {
            let raw = dolos_core::keys::encode(&key);
            prop_assert_eq!(AddressKey::decode_key(&raw), Ok(key.clone()));

            let staged = dolos_core::keys::encode(&(9u64, key.clone()));
            prop_assert_eq!(<(Epoch, AddressKey)>::decode_key(&staged), Ok((9, key)));
        }
    }

    /// A row back at zero after undo is removed, whatever units it touched.
    #[test]
    fn undoing_the_first_contribution_removes_the_row() {
        let unit = |byte: u8, received: u128| {
            (
                Bytes::from(vec![byte]),
                UnitSums {
                    received: Sum(received),
                    sent: Sum(0),
                },
            )
        };

        let first = AddressStats {
            units: [unit(1, 5)].into_iter().collect(),
            tx_count: 1,
        };
        let second = AddressStats {
            units: [unit(1, 2), unit(2, 9)].into_iter().collect(),
            tx_count: 1,
        };

        let key = (4, AddressKey::Unstaked(vec![0x61; 29]));
        let mut a = StageAddressStats::new(key.clone(), first.clone());
        let mut b = StageAddressStats::new(key, second);

        let mut row = None;
        dolos_core::EntityDelta::apply(&mut a, &mut row);
        dolos_core::EntityDelta::apply(&mut b, &mut row);
        assert_eq!(row.as_ref().unwrap().tx_count, 2);

        // the unit only the second brought leaves with it
        dolos_core::EntityDelta::undo(&b, &mut row);
        assert_eq!(row, Some(first));

        dolos_core::EntityDelta::undo(&a, &mut row);
        assert_eq!(row, None);
    }
}

#[cfg(test)]
mod key_tests {
    use dolos_core::{keys::encode, KeyError, RangeOf};
    use pallas::ledger::addresses::{
        ByronAddress, Network, Pointer, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
        StakeAddress, StakePayload,
    };
    use proptest::prelude::*;

    use super::*;

    fn base(payment: u8, stake: StakeCredential) -> Address {
        let delegation = match stake {
            StakeCredential::AddrKeyhash(hash) => ShelleyDelegationPart::Key(hash),
            StakeCredential::ScriptHash(hash) => ShelleyDelegationPart::Script(hash),
        };

        Address::Shelley(ShelleyAddress::new(
            Network::Mainnet,
            ShelleyPaymentPart::Script([payment; 28].into()),
            delegation,
        ))
    }

    fn enterprise(payment: u8) -> Address {
        Address::Shelley(ShelleyAddress::new(
            Network::Mainnet,
            ShelleyPaymentPart::Key([payment; 28].into()),
            ShelleyDelegationPart::Null,
        ))
    }

    fn pointer(payment: u8) -> Address {
        Address::Shelley(ShelleyAddress::new(
            Network::Mainnet,
            ShelleyPaymentPart::Key([payment; 28].into()),
            ShelleyDelegationPart::Pointer(Pointer::new(13200, 526450, 149104513)),
        ))
    }

    fn byron() -> Address {
        let byron = ByronAddress::from_base58(
            "Ae2tdPwUPEZ4YjgvykNpoFeYUxoyhNj2kg8KfKWN2FizsSpLUPv68MpTVDo",
        )
        .unwrap();

        Address::Byron(byron)
    }

    fn stake(stake: StakeCredential) -> Address {
        let payload = match stake {
            StakeCredential::AddrKeyhash(hash) => StakePayload::Stake(hash),
            StakeCredential::ScriptHash(hash) => StakePayload::Script(hash),
        };

        Address::Stake(StakeAddress::new(Network::Testnet, payload))
    }

    fn every_kind() -> Vec<Address> {
        vec![
            byron(),
            enterprise(0x33),
            pointer(0x44),
            base(0x11, StakeCredential::AddrKeyhash([0x22; 28].into())),
            base(0x11, StakeCredential::ScriptHash([0x22; 28].into())),
            stake(StakeCredential::AddrKeyhash([0x22; 28].into())),
            stake(StakeCredential::ScriptHash([0x22; 28].into())),
        ]
    }

    #[test]
    fn layouts_are_pinned() {
        let stake = StakeCredential::AddrKeyhash([0x22; 28].into());
        let address = base(0x11, stake.clone());

        // stake-first: the credential's CBOR, then header and payment hash
        assert_eq!(
            encode(&AddressKey::of(&address)).to_vec(),
            [
                [0x82, 0x00, 0x58, 0x1c].as_slice(),
                &[0x22; 28],
                &[0x11],
                &[0x11; 28],
            ]
            .concat()
        );

        let address = enterprise(0x33);
        assert_eq!(
            encode(&AddressKey::of(&address)).to_vec(),
            [[0x00].as_slice(), &address.to_vec()].concat()
        );

        assert_eq!(
            encode(&GroupKey {
                group: PaymentKey([0x44; 28].into()),
                epoch: 5,
                member: PaymentAddressKey(vec![0x60, 0x01]),
            })
            .to_vec(),
            [[0x44; 28].as_slice(), &5u64.to_be_bytes(), &[0x60, 0x01]].concat()
        );
    }

    #[test]
    fn every_kind_of_address_round_trips() {
        for address in every_kind() {
            let key = AddressKey::of(&address);
            let raw = encode(&key);

            assert_eq!(AddressKey::decode_key(&raw), Ok(key.clone()));
            assert_eq!(key.address(), address.to_vec());
            assert_eq!(
                AddressKey::decode_key(&raw).unwrap().address(),
                address.to_vec()
            );

            let staked = matches!(key, AddressKey::Staked { .. });
            let has_account = crate::pallas_extras::address_account(&address).is_some();
            assert_eq!(staked, has_account, "{address}");
        }
    }

    #[test]
    fn bytes_split_under_their_account_alone() {
        let a = StakeCredential::AddrKeyhash([0x22; 28].into());
        let b = StakeCredential::AddrKeyhash([0x23; 28].into());
        let address = base(0x11, a.clone()).to_vec();

        assert_eq!(
            AddressKey::split(Some(&a), address.clone()),
            AddressKey::of(&base(0x11, a))
        );
        assert_eq!(
            AddressKey::split(Some(&b), address.clone()),
            AddressKey::Unstaked(address.clone())
        );
        assert_eq!(
            AddressKey::split(None, address.clone()),
            AddressKey::Unstaked(address)
        );
    }

    #[test]
    fn payment_tails_rebuild_their_address() {
        let stake = StakeCredential::AddrKeyhash([0x22; 28].into());

        for address in [base(0x11, stake), enterprise(0x33)] {
            let (payment, tail) = split_payment(&address).unwrap();
            assert_eq!(join_payment(&payment, &tail), address.to_vec());
        }
    }

    #[test]
    fn an_account_prefix_holds_its_addresses_alone() {
        let a = AccountKey(StakeCredential::AddrKeyhash([0x22; 28].into()));
        let b = AccountKey(StakeCredential::AddrKeyhash([0x23; 28].into()));
        let script = AccountKey(StakeCredential::ScriptHash([0x22; 28].into()));

        let range = RangeOf::prefix(&a);
        let unstaked = [byron(), enterprise(0x22), pointer(0x22)];

        assert!(range.contains(&AddressKey::of(&base(0x00, a.0.clone()))));
        assert!(range.contains(&AddressKey::of(&base(0xff, a.0.clone()))));
        assert!(range.contains(&AddressKey::of(&stake(a.0.clone()))));
        assert!(!range.contains(&AddressKey::of(&base(0x11, b.0.clone()))));
        assert!(!range.contains(&AddressKey::of(&base(0x11, script.0.clone()))));
        assert!(!range.contains(&AddressKey::of(&stake(script.0.clone()))));

        for address in &unstaked {
            assert!(!range.contains(&AddressKey::of(address)));
        }

        // within an epoch, as the staged rows are keyed
        let key = |account: &AccountKey, epoch, payment| {
            (epoch, AddressKey::of(&base(payment, account.0.clone())))
        };

        let range = RangeOf::prefix(&(7u64, a.clone()));

        assert!(range.contains(&key(&a, 7, 0x00)));
        assert!(range.contains(&key(&a, 7, 0xff)));
        assert!(!range.contains(&key(&a, 6, 0x11)));
        assert!(!range.contains(&key(&a, 8, 0x11)));
        assert!(!range.contains(&key(&b, 7, 0x11)));
        assert!(!range.contains(&key(&script, 7, 0x11)));

        for address in &unstaked {
            assert!(!range.contains(&(7u64, AddressKey::of(address))));
        }
    }

    #[test]
    fn malformed_keys_are_refused() {
        assert!(AddressKey::decode_key(&[]).is_err());
        assert!(AddressKey::decode_key(&[0x00]).is_err());
        assert!(AddressKey::decode_key(&[0x02, 1, 2]).is_err());

        let account = encode(&AccountKey(StakeCredential::AddrKeyhash([0; 28].into())));
        assert!(AddressKey::decode_key(&account).is_err());
        assert!(AddressKey::decode_key(&account[..20]).is_err());

        // a rest neither a base nor a stake address header opens
        let odd = [account.as_slice(), &[0x60], &[0x11; 28]].concat();
        assert!(AddressKey::decode_key(&odd).is_err());

        let staked = encode(&AddressKey::of(&base(
            0x11,
            StakeCredential::AddrKeyhash([0x22; 28].into()),
        )));

        assert!(matches!(
            AddressKey::decode_key(&staked[..staked.len() - 1]),
            Err(KeyError::Short { .. })
        ));
        assert_eq!(
            AddressKey::decode_key(&[staked.as_slice(), &[0]].concat()),
            Err(KeyError::Trailing(1))
        );

        let stake = encode(&AddressKey::of(&stake(StakeCredential::ScriptHash(
            [0x22; 28].into(),
        ))));
        assert_eq!(
            AddressKey::decode_key(&[stake.as_slice(), &[0; 28]].concat()),
            Err(KeyError::Trailing(28))
        );

        assert!(PaymentAddressKey::decode_key(&[]).is_err());
    }

    #[test]
    fn sums_past_u64_encode_as_bignums() {
        let small = Sum(u64::MAX.into());
        assert_eq!(
            minicbor::to_vec(small).unwrap(),
            [[0x1b].as_slice(), &[0xff; 8]].concat()
        );

        let wide = Sum(u128::from(u64::MAX) + 1);
        assert_eq!(
            minicbor::to_vec(wide).unwrap(),
            [0xc2, 0x49, 0x01, 0, 0, 0, 0, 0, 0, 0, 0]
        );

        for sum in [Sum(0), small, wide, Sum(u128::MAX)] {
            assert_eq!(
                minicbor::decode::<Sum>(&minicbor::to_vec(sum).unwrap()).unwrap(),
                sum
            );
        }

        // a bignum wider than u128 is refused
        let mut too_wide = vec![0xc2, 0x51];
        too_wide.extend([1u8; 17]);
        assert!(minicbor::decode::<Sum>(&too_wide).is_err());
    }

    proptest! {
        #[test]
        fn sums_round_trip(sum in any::<u128>()) {
            let raw = minicbor::to_vec(Sum(sum)).unwrap();
            prop_assert_eq!(minicbor::decode::<Sum>(&raw).unwrap(), Sum(sum));
        }
    }
}
