use std::{borrow::Cow, collections::HashMap};

use dolos_core::{BlockHash, ChainPoint, Entity, EntityDelta, LogValue, NsKey};
use serde::{Deserialize, Serialize};

use crate::wal::RedbWalStore;

const DUMMY_BLOCK_BYTES: &str = "820183851a2d964a09582089d9b5a5b8ddc8d7e5a6795e9774d97faf1efea59b2caf7eaf9f8c5b32059df484830058200e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a85820afc0da64183bf2664f3d4eec7238d524ba607faeeab24fc100eb861dba69971b8300582025777aca9e4a73d48fc73b4f961d345b06d4a6f349cb7916570d35537d53479f5820d36a2619a672494604e11bb447cbcf5231e9f2ba25c2169177edc941bd50ad6c5820afc0da64183bf2664f3d4eec7238d524ba607faeeab24fc100eb861dba69971b58204e66280cd94d591072349bec0a3090a53aa945562efb6d08d56e53654b0e40988482000058401bc97a2fe02c297880ce8ecfd997fe4c1ec09ee10feeee9f686760166b05281d6283468ffd93becb0c956ccddd642df9b1244c915911185fa49355f6f22bfab98101820282840058401bc97a2fe02c297880ce8ecfd997fe4c1ec09ee10feeee9f686760166b05281d6283468ffd93becb0c956ccddd642df9b1244c915911185fa49355f6f22bfab9584061261a95b7613ee6bf2067dad77b70349729b0c50d57bc1cf30de0db4a1e73a885d0054af7c23fc6c37919dba41c602a57e2d0f9329a7954b867338d6fb2c9455840e03e62f083df5576360e60a32e22bbb07b3c8df4fcab8079f1d6f61af3954d242ba8a06516c395939f24096f3df14e103a7d9c2b80a68a9363cf1f27c7a4e307584044f18ef23db7d2813415cb1b62e8f3ead497f238edf46bb7a97fd8e9105ed9775e8421d18d47e05a2f602b700d932c181e8007bbfb231d6f1a050da4ebeeba048483000000826a63617264616e6f2d736c00a058204ba92aa320c60acc9ad7b9a64f2eda55c4d2ec28e604faf186708b4f0c4e8edf849fff8300d9010280d90102809fff82809fff81a0";

pub struct DummyEntity;

impl Entity for DummyEntity {
    fn decode_entity(
        _ns: dolos_core::Namespace,
        _value: &dolos_core::EntityValue,
    ) -> Result<Self, dolos_core::state::StateError> {
        todo!()
    }

    fn encode_entity(_value: &Self) -> (dolos_core::Namespace, dolos_core::EntityValue) {
        todo!()
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DummyDelta;

impl EntityDelta for DummyDelta {
    type Entity = DummyEntity;

    fn key(&self) -> NsKey {
        todo!()
    }

    fn apply(&mut self, _entity: &mut Option<Self::Entity>) {
        todo!()
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        todo!()
    }
}

pub fn slot_to_hash(slot: u64) -> BlockHash {
    let mut hasher = pallas::crypto::hash::Hasher::<256>::new();
    hasher.input(&(slot as i32).to_le_bytes());
    hasher.finalize()
}

pub fn dummy_entry_from_slot(slot: u64) -> (ChainPoint, LogValue<DummyDelta>) {
    let bytes = hex::decode(DUMMY_BLOCK_BYTES).unwrap();

    (
        ChainPoint::Slot(slot),
        LogValue {
            block: bytes,
            delta: vec![DummyDelta],
            inputs: HashMap::new(),
        },
    )
}

pub fn empty_wal_db() -> RedbWalStore<DummyDelta> {
    RedbWalStore::memory().unwrap()
}

pub fn wal_with_dummy_blocks(quantity: usize) -> RedbWalStore<DummyDelta> {
    let wal = empty_wal_db();

    let logs: Vec<_> = (0..quantity)
        .map(|x| dummy_entry_from_slot(x as u64))
        .collect();

    wal.append_entries(&logs).unwrap();

    wal
}
