use std::collections::BTreeMap;

use dolos_core::NsKey;
use pallas::codec::{
    minicbor::{self, Decode, Encode},
    utils::Bytes,
};
use pallas::ledger::primitives::StakeCredential;
use serde::{Deserialize, Serialize};

use super::FixedNamespace as _;

/// Cumulative lovelace activity for all addresses associated with an account.
#[derive(Debug, Encode, Decode, Clone, Default, PartialEq, Eq)]
pub struct AccountActivity {
    #[n(0)]
    pub received_lovelace_bytes: [u8; 16],

    #[n(1)]
    pub sent_lovelace_bytes: [u8; 16],

    #[n(2)]
    pub tx_count: u64,
}

entity_boilerplate!(AccountActivity, "account_activity");

impl AccountActivity {
    pub fn received_lovelace(&self) -> u128 {
        u128::from_be_bytes(self.received_lovelace_bytes)
    }

    pub fn sent_lovelace(&self) -> u128 {
        u128::from_be_bytes(self.sent_lovelace_bytes)
    }
}

/// Cumulative native-asset activity for all addresses associated with an
/// account, keyed by the same stake credential as `AccountActivity`.
#[derive(Debug, Encode, Decode, Clone, Default, PartialEq, Eq)]
pub struct AccountAssetActivity {
    /// asset unit (policy id ++ asset name) → u128 big-endian bytes
    #[n(0)]
    pub received_assets: BTreeMap<Bytes, [u8; 16]>,

    #[n(1)]
    pub sent_assets: BTreeMap<Bytes, [u8; 16]>,
}

entity_boilerplate!(AccountAssetActivity, "account_asset_activity");

fn cred_key(cred: &StakeCredential, ns: &'static str) -> NsKey {
    let enc = minicbor::to_vec(cred).unwrap();
    NsKey::from((ns, enc))
}

fn add_to_quantity(bytes: &mut [u8; 16], amount: u128) {
    *bytes = u128::from_be_bytes(*bytes)
        .saturating_add(amount)
        .to_be_bytes();
}

fn add_to_assets(map: &mut BTreeMap<Bytes, [u8; 16]>, unit: &Bytes, amount: u128) {
    let entry = map.entry(unit.clone()).or_insert([0u8; 16]);
    add_to_quantity(entry, amount);
}

// --- Deltas ---

/// Lovelace moved by a single tx for a single account; bumps tx_count by one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountActivityRecord {
    pub(crate) cred: StakeCredential,
    pub(crate) received_lovelace: u128,
    pub(crate) sent_lovelace: u128,

    // undo
    pub(crate) was_new: bool,
    pub(crate) prev_received_lovelace: [u8; 16],
    pub(crate) prev_sent_lovelace: [u8; 16],
}

impl AccountActivityRecord {
    pub fn new(cred: StakeCredential, received_lovelace: u128, sent_lovelace: u128) -> Self {
        Self {
            cred,
            received_lovelace,
            sent_lovelace,
            was_new: false,
            prev_received_lovelace: [0u8; 16],
            prev_sent_lovelace: [0u8; 16],
        }
    }
}

impl dolos_core::EntityDelta for AccountActivityRecord {
    type Entity = AccountActivity;

    fn key(&self) -> NsKey {
        cred_key(&self.cred, AccountActivity::NS)
    }

    fn apply(&mut self, entity: &mut Option<AccountActivity>) {
        self.was_new = entity.is_none();

        let entity = entity.get_or_insert_default();

        // save undo info
        self.prev_received_lovelace = entity.received_lovelace_bytes;
        self.prev_sent_lovelace = entity.sent_lovelace_bytes;

        add_to_quantity(&mut entity.received_lovelace_bytes, self.received_lovelace);
        add_to_quantity(&mut entity.sent_lovelace_bytes, self.sent_lovelace);

        entity.tx_count += 1;
    }

    fn undo(&self, entity: &mut Option<AccountActivity>) {
        if self.was_new {
            *entity = None;
            return;
        }

        let entity = entity.as_mut().expect("existing account activity");

        entity.received_lovelace_bytes = self.prev_received_lovelace;
        entity.sent_lovelace_bytes = self.prev_sent_lovelace;

        entity.tx_count -= 1;
    }
}

/// Assets moved by a single tx for a single account. Skipped for
/// lovelace-only txs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountAssetActivityRecord {
    pub(crate) cred: StakeCredential,
    pub(crate) received_assets: Vec<(Bytes, u128)>,
    pub(crate) sent_assets: Vec<(Bytes, u128)>,

    // undo
    pub(crate) was_new: bool,
    /// pre-apply value for each touched key (None = key was absent)
    pub(crate) prev_received_assets: Vec<(Bytes, Option<[u8; 16]>)>,
    pub(crate) prev_sent_assets: Vec<(Bytes, Option<[u8; 16]>)>,
}

impl AccountAssetActivityRecord {
    pub fn new(
        cred: StakeCredential,
        received_assets: Vec<(Bytes, u128)>,
        sent_assets: Vec<(Bytes, u128)>,
    ) -> Self {
        Self {
            cred,
            received_assets,
            sent_assets,
            was_new: false,
            prev_received_assets: vec![],
            prev_sent_assets: vec![],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.received_assets.is_empty() && self.sent_assets.is_empty()
    }
}

impl dolos_core::EntityDelta for AccountAssetActivityRecord {
    type Entity = AccountAssetActivity;

    fn key(&self) -> NsKey {
        cred_key(&self.cred, AccountAssetActivity::NS)
    }

    fn apply(&mut self, entity: &mut Option<AccountAssetActivity>) {
        self.was_new = entity.is_none();

        let entity = entity.get_or_insert_default();

        // save undo info
        self.prev_received_assets = self
            .received_assets
            .iter()
            .map(|(unit, _)| (unit.clone(), entity.received_assets.get(unit).copied()))
            .collect();
        self.prev_sent_assets = self
            .sent_assets
            .iter()
            .map(|(unit, _)| (unit.clone(), entity.sent_assets.get(unit).copied()))
            .collect();

        for (unit, amount) in &self.received_assets {
            add_to_assets(&mut entity.received_assets, unit, *amount);
        }

        for (unit, amount) in &self.sent_assets {
            add_to_assets(&mut entity.sent_assets, unit, *amount);
        }
    }

    fn undo(&self, entity: &mut Option<AccountAssetActivity>) {
        if self.was_new {
            *entity = None;
            return;
        }

        let entity = entity.as_mut().expect("existing account asset activity");

        for (unit, prev) in &self.prev_received_assets {
            match prev {
                Some(value) => entity.received_assets.insert(unit.clone(), *value),
                None => entity.received_assets.remove(unit),
            };
        }

        for (unit, prev) in &self.prev_sent_assets {
            match prev {
                Some(value) => entity.sent_assets.insert(unit.clone(), *value),
                None => entity.sent_assets.remove(unit),
            };
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    fn any_quantity_bytes() -> impl Strategy<Value = [u8; 16]> {
        // bounded so apply → undo stays an exact roundtrip (no saturation)
        (0u128..u64::MAX as u128).prop_map(|x| x.to_be_bytes())
    }

    fn any_asset_map() -> impl Strategy<Value = BTreeMap<Bytes, [u8; 16]>> {
        prop::collection::btree_map(
            prop::collection::vec(any::<u8>(), 28..60).prop_map(Bytes::from),
            any_quantity_bytes(),
            0..4,
        )
    }

    fn any_asset_moves() -> impl Strategy<Value = Vec<(Bytes, u128)>> {
        prop::collection::vec(
            (
                prop::collection::vec(any::<u8>(), 28..60).prop_map(Bytes::from),
                0u128..u64::MAX as u128,
            ),
            0..4,
        )
    }

    prop_compose! {
        pub fn any_account_activity()(
            received_lovelace_bytes in any_quantity_bytes(),
            sent_lovelace_bytes in any_quantity_bytes(),
            tx_count in 1u64..1_000_000u64,
        ) -> AccountActivity {
            AccountActivity {
                received_lovelace_bytes,
                sent_lovelace_bytes,
                tx_count,
            }
        }
    }

    prop_compose! {
        pub fn any_account_activity_record()(
            cred in root::any_stake_credential(),
            received_lovelace in 0u128..u64::MAX as u128,
            sent_lovelace in 0u128..u64::MAX as u128,
        ) -> AccountActivityRecord {
            AccountActivityRecord::new(cred, received_lovelace, sent_lovelace)
        }
    }

    prop_compose! {
        pub fn any_account_asset_activity()(
            received_assets in any_asset_map(),
            sent_assets in any_asset_map(),
        ) -> AccountAssetActivity {
            AccountAssetActivity {
                received_assets,
                sent_assets,
            }
        }
    }

    prop_compose! {
        pub fn any_account_asset_activity_record()(
            cred in root::any_stake_credential(),
            received_assets in any_asset_moves(),
            sent_assets in any_asset_moves(),
        ) -> AccountAssetActivityRecord {
            AccountAssetActivityRecord::new(cred, received_assets, sent_assets)
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use super::testing::*;
    use crate::model::testing::{assert_delta_roundtrip, assert_delta_serde_roundtrip};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn account_activity_record_roundtrip(
            entity in prop::option::of(any_account_activity()),
            delta in any_account_activity_record(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn account_activity_record_serde_roundtrip(
            entity in prop::option::of(any_account_activity()),
            delta in any_account_activity_record(),
        ) {
            assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn account_asset_activity_record_roundtrip(
            entity in prop::option::of(any_account_asset_activity()),
            delta in any_account_asset_activity_record(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn account_asset_activity_record_serde_roundtrip(
            entity in prop::option::of(any_account_asset_activity()),
            delta in any_account_asset_activity_record(),
        ) {
            assert_delta_serde_roundtrip(entity, delta);
        }
    }
}
