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
use crate::periods::{StageIfAbsent, StageSum, StagedKey, StagedRow, SumRow};

hash_key!(
    /// A payment credential, by its hash alone: what `/addresses/{address}/total`
    /// matches a payment credential on.
    PaymentKey,
    Hash<28>,
    28
);

display_as_stored!(PaymentKey);

const UNSTAKED: u8 = 0x00;
const STAKED: u8 = 0x01;

/// A stake hash closes every address that has a stake credential.
const STAKE_HASH: usize = 28;

/// The key of one address's stats.
///
/// `0x01 ‖ stake credential ‖ rest` for an address with a stake credential,
/// where `rest` is the address without its stake hash (a base address's
/// header and payment hash), and `0x00 ‖ address` for any other. Keyed
/// stake-first, so an account's addresses of one epoch are one prefix range;
/// the tag keeps the arms apart, and the variable part comes last.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AddressStatsKey {
    Staked { account: AccountKey, rest: Vec<u8> },
    Unstaked(Vec<u8>),
}

impl AddressStatsKey {
    pub fn of(address: &Address) -> Self {
        let bytes = address.to_vec();

        match crate::pallas_extras::address_account(address) {
            Some(account) if bytes.len() > STAKE_HASH => {
                let (rest, stake) = bytes.split_at(bytes.len() - STAKE_HASH);
                debug_assert_eq!(stake, account_hash(&account).as_slice());

                Self::Staked {
                    account: AccountKey(account),
                    rest: rest.to_vec(),
                }
            }
            _ => Self::Unstaked(bytes),
        }
    }

    /// The address the key was built from, whole.
    pub fn address(&self) -> Vec<u8> {
        match self {
            Self::Staked { account, rest } => [rest.as_slice(), account_hash(&account.0).as_slice()]
                .concat(),
            Self::Unstaked(address) => address.clone(),
        }
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

impl KeyCodec for AddressStatsKey {
    fn encode_key(&self, out: &mut KeyBytes) {
        match self {
            Self::Staked { account, rest } => {
                out.push(STAKED);
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
        let Some((tag, bytes)) = bytes.split_first() else {
            return Err(KeyError::Short {
                expected: 1,
                found: 0,
            });
        };

        match *tag {
            STAKED => {
                if bytes.len() < AccountKey::LEN {
                    return Err(KeyError::Short {
                        expected: 1 + AccountKey::LEN,
                        found: 1 + bytes.len(),
                    });
                }

                let (account, rest) = bytes.split_at(AccountKey::LEN);

                Ok(Self::Staked {
                    account: AccountKey::decode_key(account)?,
                    rest: non_empty(rest, "address rest")?,
                })
            }
            UNSTAKED => Ok(Self::Unstaked(non_empty(bytes, "address")?)),
            other => Err(KeyError::Malformed(format!(
                "address stats tag {other:#04x}"
            ))),
        }
    }
}

display_as_stored!(AddressStatsKey);

/// The prefix every address-stats key of one account starts with:
/// `0x01 ‖ stake credential`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountAddresses(pub AccountKey);

impl KeyCodec for AccountAddresses {
    fn encode_key(&self, out: &mut KeyBytes) {
        out.push(STAKED);
        self.0.encode_key(out);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        match bytes.split_first() {
            Some((&STAKED, account)) => Ok(Self(AccountKey::decode_key(account)?)),
            _ => Err(KeyError::Malformed("not an account's address stats".into())),
        }
    }
}

impl KeyPrefix<AddressStatsKey> for AccountAddresses {}

/// An address without its payment hash: the header, then the delegation part.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PaymentTail(pub Vec<u8>);

impl KeyCodec for PaymentTail {
    fn encode_key(&self, out: &mut KeyBytes) {
        out.extend_from_slice(&self.0);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        non_empty(bytes, "payment tail").map(Self)
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
/// A CBOR unsigned integer, or a positive bignum past u64.
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
            return Err(minicbor::decode::Error::message("sum: not a positive bignum"));
        }

        let bytes = d.bytes()?;

        if bytes.len() > 16 {
            return Err(minicbor::decode::Error::message("sum: wider than u128"));
        }

        let mut wide = [0u8; 16];
        wide[16 - bytes.len()..].copy_from_slice(bytes);

        Ok(Self(u128::from_be_bytes(wide)))
    }
}

/// What an address received and sent of one unit.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize,
)]
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
/// unit an output carried a quantity of is listed. Sealed at
/// `epoch_start(e) ‖ AddressStatsKey`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct AddressStats {
    #[n(0)]
    pub units: BTreeMap<Bytes, UnitSums>,

    #[n(1)]
    pub tx_count: u64,
}

entity_boilerplate!(AddressStats, "address-stats", log = AddressStatsKey);

impl AddressStats {
    pub fn add(&mut self, other: &Self) {
        for (unit, sums) in &other.units {
            let entry = self.units.entry(unit.clone()).or_default();
            entry.received.0 = entry.received.0.saturating_add(sums.received.0);
            entry.sent.0 = entry.sent.0.saturating_add(sums.sent.0);
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

/// An address's stats in an epoch that has not sealed.
///
/// Keyed `epoch ‖ AddressStatsKey`, epoch first: an address has no
/// fixed-width group to lead a staged key, so its rows are found by epoch, as
/// the sealed ones are. The value is the sealed row's.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
#[cbor(transparent)]
#[serde(transparent)]
pub struct StagedAddressStats(#[n(0)] pub AddressStats);

entity_boilerplate!(
    StagedAddressStats,
    "staged-address-stats",
    state = (Epoch, AddressStatsKey)
);

impl SumRow for StagedAddressStats {
    fn add(&mut self, other: &Self) {
        self.0.add(&other.0);
    }

    fn sub(&mut self, other: &Self) {
        self.0.sub(&other.0);
    }

    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

pub type StageAddressStats = StageSum<StagedAddressStats>;

/// A payment credential's addresses that moved value in one epoch, as
/// tails, and the txs that moved any of them.
///
/// Sealed at `epoch_start(e) ‖ PaymentKey`, the tails in byte order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub struct PaymentStats {
    #[n(0)]
    pub tails: Vec<ByteVec>,

    #[n(1)]
    pub tx_count: u64,
}

entity_boilerplate!(PaymentStats, "payment-stats", log = PaymentKey);

/// One address of a payment credential that moved value in an epoch, staged
/// until the epoch seals.
///
/// Keyed `payment ‖ epoch ‖ tail`, so one prefix scan returns every staged
/// epoch of a credential.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct StagedPaymentAddress {
    #[n(0)]
    pub payment: Hash<28>,

    #[n(1)]
    pub epoch: Epoch,

    #[n(2)]
    #[cbor(with = "minicbor::bytes")]
    pub tail: Vec<u8>,
}

entity_boilerplate!(
    StagedPaymentAddress,
    "staged-payment-addresses",
    state = StagedKey<PaymentKey, PaymentTail>
);

impl StagedRow for StagedPaymentAddress {
    fn key(&self) -> StagedKey<PaymentKey, PaymentTail> {
        StagedKey {
            group: PaymentKey(self.payment),
            epoch: self.epoch,
            member: PaymentTail(self.tail.clone()),
        }
    }

    fn epoch(&self) -> Epoch {
        self.epoch
    }
}

pub type StagePaymentAddress = StageIfAbsent<StagedPaymentAddress>;

/// The txs that moved value through a payment credential's addresses in an
/// epoch that has not sealed, keyed `payment ‖ epoch`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct StagedPaymentStats {
    #[n(0)]
    pub tx_count: u64,
}

entity_boilerplate!(
    StagedPaymentStats,
    "staged-payment-stats",
    state = StagedKey<PaymentKey, ()>
);

impl SumRow for StagedPaymentStats {
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

pub type StagePaymentStats = StageSum<StagedPaymentStats>;

/// The txs an account's address rows count more than once in one epoch: a
/// tx moving value through `n` of its addresses adds `n - 1`.
///
/// Sealed at `epoch_start(e) ‖ AccountKey`, only when nonzero.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub struct AccountStats {
    #[n(0)]
    pub tx_overlap: u64,
}

entity_boilerplate!(AccountStats, "account-stats", log = AccountKey);

/// An account's tx overlap in an epoch that has not sealed, keyed
/// `account ‖ epoch`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct StagedAccountStats {
    #[n(0)]
    pub tx_overlap: u64,
}

entity_boilerplate!(
    StagedAccountStats,
    "staged-account-stats",
    state = StagedKey<AccountKey, ()>
);

impl SumRow for StagedAccountStats {
    fn add(&mut self, other: &Self) {
        self.tx_overlap = self.tx_overlap.saturating_add(other.tx_overlap);
    }

    fn sub(&mut self, other: &Self) {
        self.tx_overlap = self.tx_overlap.saturating_sub(other.tx_overlap);
    }

    fn is_zero(&self) -> bool {
        self.tx_overlap == 0
    }
}

pub type StageAccountStats = StageSum<StagedAccountStats>;

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

    pub fn any_address_stats_key() -> impl Strategy<Value = AddressStatsKey> {
        prop_oneof![
            (
                root::any_stake_credential(),
                prop::collection::vec(any::<u8>(), 1..40)
            )
                .prop_map(|(account, rest)| AddressStatsKey::Staked {
                    account: AccountKey(account),
                    rest,
                }),
            prop::collection::vec(any::<u8>(), 1..90).prop_map(AddressStatsKey::Unstaked),
        ]
    }

    prop_compose! {
        pub fn any_staged_payment_address()(
            payment in root::any_hash_28(),
            epoch in 0u64..1000,
            tail in prop::collection::vec(any::<u8>(), 1..64),
        ) -> StagedPaymentAddress {
            StagedPaymentAddress { payment, epoch, tail }
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
            key in any_address_stats_key(),
            delta in any_address_stats(),
        ) {
            let entity = entity.map(StagedAddressStats);
            let delta = StageAddressStats::new((7, key), StagedAddressStats(delta));

            assert_delta_roundtrip(entity.clone(), delta.clone());
            assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn stage_counts_roundtrip(
            entity in prop::option::of(1u64..1_000_000),
            delta in 1u64..1_000_000,
        ) {
            let key = StagedKey { group: PaymentKey([1; 28].into()), epoch: 3, member: () };
            let stage = StagePaymentStats::new(key, StagedPaymentStats { tx_count: delta });
            let rows = entity.map(|tx_count| StagedPaymentStats { tx_count });
            assert_delta_roundtrip(rows.clone(), stage.clone());
            assert_delta_serde_roundtrip(rows, stage);

            let key = StagedKey {
                group: AccountKey(StakeCredential::AddrKeyhash([2; 28].into())),
                epoch: 3,
                member: (),
            };
            let stage = StageAccountStats::new(key, StagedAccountStats { tx_overlap: delta });
            let entity = entity.map(|tx_overlap| StagedAccountStats { tx_overlap });
            assert_delta_roundtrip(entity.clone(), stage.clone());
            assert_delta_serde_roundtrip(entity, stage);
        }

        #[test]
        fn stage_payment_address_roundtrip(
            entity in prop::option::of(any_staged_payment_address()),
            row in any_staged_payment_address(),
        ) {
            assert_delta_roundtrip(entity.clone(), StagePaymentAddress::new(row.clone()));
            assert_delta_serde_roundtrip(entity, StagePaymentAddress::new(row));
        }

        #[test]
        fn address_stats_encode_and_decode_back(stats in any_address_stats()) {
            let raw = minicbor::to_vec(&stats).unwrap();
            prop_assert_eq!(minicbor::decode::<AddressStats>(&raw).unwrap(), stats.clone());

            // the staged row encodes as the sealed one
            prop_assert_eq!(minicbor::to_vec(StagedAddressStats(stats)).unwrap(), raw);
        }

        #[test]
        fn address_stats_keys_round_trip(key in any_address_stats_key()) {
            let raw = dolos_core::keys::encode(&key);
            prop_assert_eq!(AddressStatsKey::decode_key(&raw), Ok(key.clone()));

            let staged = dolos_core::keys::encode(&(9u64, key.clone()));
            prop_assert_eq!(<(Epoch, AddressStatsKey)>::decode_key(&staged), Ok((9, key)));
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

        let key = (4, AddressStatsKey::Unstaked(vec![0x61; 29]));
        let mut a = StageAddressStats::new(key.clone(), StagedAddressStats(first.clone()));
        let mut b = StageAddressStats::new(key, StagedAddressStats(second));

        let mut row = None;
        dolos_core::EntityDelta::apply(&mut a, &mut row);
        dolos_core::EntityDelta::apply(&mut b, &mut row);
        assert_eq!(row.as_ref().unwrap().0.tx_count, 2);

        // the unit only the second brought leaves with it
        dolos_core::EntityDelta::undo(&b, &mut row);
        assert_eq!(row, Some(StagedAddressStats(first)));

        dolos_core::EntityDelta::undo(&a, &mut row);
        assert_eq!(row, None);
    }
}

#[cfg(test)]
mod key_tests {
    use dolos_core::{keys::encode, RangeOf};
    use pallas::ledger::addresses::{
        Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart, StakeAddress,
        StakePayload,
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

    #[test]
    fn layouts_are_pinned() {
        let stake = StakeCredential::AddrKeyhash([0x22; 28].into());
        let address = base(0x11, stake.clone());

        // stake-first: tag, the credential's CBOR, then header and payment hash
        assert_eq!(
            encode(&AddressStatsKey::of(&address)).to_vec(),
            [
                [0x01, 0x82, 0x00, 0x58, 0x1c].as_slice(),
                &[0x22; 28],
                &[0x11],
                &[0x11; 28],
            ]
            .concat()
        );

        let address = enterprise(0x33);
        assert_eq!(
            encode(&AddressStatsKey::of(&address)).to_vec(),
            [[0x00].as_slice(), &address.to_vec()].concat()
        );

        assert_eq!(
            encode(&AccountAddresses(AccountKey(stake))).to_vec(),
            [[0x01, 0x82, 0x00, 0x58, 0x1c].as_slice(), &[0x22; 28]].concat()
        );

        assert_eq!(
            encode(&StagedKey {
                group: PaymentKey([0x44; 28].into()),
                epoch: 5,
                member: PaymentTail(vec![0x60, 0x01]),
            })
            .to_vec(),
            [[0x44; 28].as_slice(), &5u64.to_be_bytes(), &[0x60, 0x01]].concat()
        );
    }

    #[test]
    fn keys_rebuild_their_address() {
        let stake = StakeCredential::ScriptHash([0x22; 28].into());

        for address in [
            base(0x11, stake.clone()),
            enterprise(0x33),
            Address::Stake(StakeAddress::new(
                Network::Testnet,
                StakePayload::Script([0x22; 28].into()),
            )),
        ] {
            assert_eq!(AddressStatsKey::of(&address).address(), address.to_vec());
        }

        assert!(matches!(
            AddressStatsKey::of(&base(0x11, stake)),
            AddressStatsKey::Staked { .. }
        ));
        assert!(matches!(
            AddressStatsKey::of(&enterprise(0x11)),
            AddressStatsKey::Unstaked(_)
        ));
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

        let key = |account: &AccountKey, epoch, payment| {
            (
                epoch,
                AddressStatsKey::of(&base(payment, account.0.clone())),
            )
        };

        let range = RangeOf::prefix(&(7u64, AccountAddresses(a.clone())));

        assert!(range.contains(&key(&a, 7, 0x00)));
        assert!(range.contains(&key(&a, 7, 0xff)));
        assert!(!range.contains(&key(&a, 6, 0x11)));
        assert!(!range.contains(&key(&a, 8, 0x11)));
        assert!(!range.contains(&key(&b, 7, 0x11)));
        assert!(!range.contains(&key(&script, 7, 0x11)));

        let unstaked = (7u64, AddressStatsKey::Unstaked(vec![0x01; 60]));
        assert!(!range.contains(&unstaked));
    }

    #[test]
    fn malformed_keys_are_refused() {
        assert!(AddressStatsKey::decode_key(&[]).is_err());
        assert!(AddressStatsKey::decode_key(&[0x00]).is_err());
        assert!(AddressStatsKey::decode_key(&[0x02, 1, 2]).is_err());

        let account = encode(&AccountKey(StakeCredential::AddrKeyhash([0; 28].into())));
        let staked = [[0x01].as_slice(), &account].concat();
        assert!(AddressStatsKey::decode_key(&staked).is_err());
        assert!(AddressStatsKey::decode_key(&staked[..20]).is_err());
        assert!(PaymentTail::decode_key(&[]).is_err());
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
