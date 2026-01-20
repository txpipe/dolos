use std::{marker::PhantomData, ops::Range};

use pallas::{
    crypto::hash::Hash,
    ledger::{primitives::PlutusData, traverse::MultiEraTx},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    state::KEY_SIZE, BlockBody, BlockSlot, BrokenInvariant, ChainPoint, Entity, EntityKey,
    EntityValue, EraCbor, Namespace, RawBlock, TxHash, TxOrder, TxoRef,
};

const TEMPORAL_KEY_SIZE: usize = 8;
const LOG_KEY_SIZE: usize = TEMPORAL_KEY_SIZE + KEY_SIZE;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct TemporalKey([u8; TEMPORAL_KEY_SIZE]);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct LogKey([u8; LOG_KEY_SIZE]);

impl AsRef<[u8]> for TemporalKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for LogKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for LogKey {
    fn from(value: &[u8]) -> Self {
        let mut key = [0u8; LOG_KEY_SIZE];
        let len = value.len().min(LOG_KEY_SIZE);
        key[..len].copy_from_slice(&value[..len]);
        LogKey(key)
    }
}

impl From<Vec<u8>> for LogKey {
    fn from(value: Vec<u8>) -> Self {
        value.as_slice().into()
    }
}

impl From<LogKey> for TemporalKey {
    fn from(value: LogKey) -> Self {
        // Safe to unwrap, we know the length matches.
        let bytes: [u8; TEMPORAL_KEY_SIZE] =
            value.as_ref()[..TEMPORAL_KEY_SIZE].try_into().unwrap();
        TemporalKey(bytes)
    }
}

impl From<LogKey> for EntityKey {
    fn from(value: LogKey) -> Self {
        EntityKey::from(&value.as_ref()[TEMPORAL_KEY_SIZE..])
    }
}

impl From<&ChainPoint> for TemporalKey {
    fn from(value: &ChainPoint) -> Self {
        value.slot().into()
    }
}

impl From<u64> for TemporalKey {
    fn from(value: u64) -> Self {
        TemporalKey(value.to_be_bytes())
    }
}

impl From<(TemporalKey, EntityKey)> for LogKey {
    fn from((temporal, entity): (TemporalKey, EntityKey)) -> Self {
        // Safe to unwrap, we know the length matches.
        let bytes: [u8; LOG_KEY_SIZE] = [temporal.as_ref(), entity.as_ref()]
            .concat()
            .try_into()
            .unwrap();
        Self(bytes)
    }
}

impl From<TemporalKey> for LogKey {
    fn from(value: TemporalKey) -> Self {
        // Safe to unwrap, we know the length matches. We extend the key with 0 to match
        // length.
        let bytes: [u8; LOG_KEY_SIZE] = [value.as_ref(), &[0; KEY_SIZE]]
            .concat()
            .try_into()
            .unwrap();
        Self(bytes)
    }
}

impl From<&ChainPoint> for LogKey {
    fn from(value: &ChainPoint) -> Self {
        let temporal: TemporalKey = value.into();
        temporal.into()
    }
}

impl LogKey {
    pub fn full_range() -> Range<LogKey> {
        Range {
            start: LogKey([0u8; LOG_KEY_SIZE]),
            end: LogKey([255u8; LOG_KEY_SIZE]),
        }
    }
}

pub struct LogIterTyped<A: ArchiveStore, E: Entity> {
    inner: A::LogIter,
    ns: Namespace,
    _marker: PhantomData<E>,
}

impl<A: ArchiveStore, E: Entity> LogIterTyped<A, E> {
    pub fn new(inner: A::LogIter, ns: Namespace) -> Self {
        Self {
            inner,
            ns,
            _marker: PhantomData,
        }
    }
}

impl<A: ArchiveStore, E: Entity> Iterator for LogIterTyped<A, E> {
    type Item = Result<(LogKey, E), ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next()?;

        let mapped = next.and_then(|(key, value)| {
            E::decode_entity(self.ns, &value)
                .map(|v| (key, v))
                .map_err(|x| ArchiveError::EntityDecodingError(x.to_string()))
        });

        Some(mapped)
    }
}

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("broken invariant")]
    BrokenInvariant(#[from] BrokenInvariant),

    #[error("storage error")]
    InternalError(String),

    #[error("address decoding error")]
    AddressDecoding(#[from] pallas::ledger::addresses::Error),

    #[error("query not supported")]
    QueryNotSupported,

    #[error("invalid store version")]
    InvalidStoreVersion,

    #[error("decoding error")]
    DecodingError(#[from] pallas::codec::minicbor::decode::Error),

    #[error("block decoding error")]
    BlockDecodingError(#[from] pallas::ledger::traverse::Error),

    #[error("entity decoding error")]
    EntityDecodingError(String),

    #[error("namespace {0} not found")]
    NamespaceNotFound(Namespace),
}

pub type OpaqueTag = Vec<u8>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SlotTags {
    pub number: Option<u64>,
    pub tx_hashes: Vec<OpaqueTag>,
    pub scripts: Vec<OpaqueTag>,
    pub datums: Vec<OpaqueTag>,
    pub policies: Vec<OpaqueTag>,
    pub assets: Vec<OpaqueTag>,
    pub full_addresses: Vec<OpaqueTag>,
    pub payment_addresses: Vec<OpaqueTag>,
    pub stake_addresses: Vec<OpaqueTag>,
    pub spent_txo: Vec<OpaqueTag>,
    pub account_certs: Vec<OpaqueTag>,
    pub metadata: Vec<u64>,
}

pub trait ArchiveWriter: Send + Sync + 'static {
    fn apply(
        &self,
        point: &ChainPoint,
        block: &RawBlock,
        tags: &SlotTags,
    ) -> Result<(), ArchiveError>;

    fn write_log(
        &self,
        ns: Namespace,
        key: &LogKey,
        value: &EntityValue,
    ) -> Result<(), ArchiveError>;

    fn write_log_typed<E: Entity>(&self, key: &LogKey, entity: &E) -> Result<(), ArchiveError> {
        let (ns, raw) = E::encode_entity(entity);

        self.write_log(ns, key, &raw)
    }

    fn undo(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), ArchiveError>;

    fn commit(self) -> Result<(), ArchiveError>;
}

pub trait ArchiveStore: Clone + Send + Sync + 'static {
    type BlockIter<'a>: Iterator<Item = (BlockSlot, BlockBody)> + DoubleEndedIterator + 'a;
    type SparseBlockIter: Iterator<Item = Result<(BlockSlot, Option<BlockBody>), ArchiveError>>
        + DoubleEndedIterator;
    type Writer: ArchiveWriter;
    type LogIter: Iterator<Item = Result<(LogKey, EntityValue), ArchiveError>>;
    type EntityValueIter: Iterator<Item = Result<EntityValue, ArchiveError>>;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError>;

    fn read_logs(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<EntityValue>>, ArchiveError>;

    fn iter_logs(&self, ns: Namespace, range: Range<LogKey>)
        -> Result<Self::LogIter, ArchiveError>;

    fn read_logs_typed<E: Entity>(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<E>>, ArchiveError> {
        let raw = self.read_logs(ns, keys)?;

        let decoded = raw
            .into_iter()
            .map(|x| {
                x.map(|v| {
                    E::decode_entity(ns, &v)
                        .map_err(|x| ArchiveError::EntityDecodingError(x.to_string()))
                })
            })
            .map(|x| x.transpose())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(decoded)
    }

    fn read_log_typed<E: Entity>(
        &self,
        ns: Namespace,
        key: &LogKey,
    ) -> Result<Option<E>, ArchiveError> {
        let raw = self.read_logs_typed(ns, &[key])?;

        let first = raw.into_iter().next().unwrap();

        Ok(first)
    }

    fn iter_logs_typed<E: Entity>(
        &self,
        ns: Namespace,
        range: Option<Range<LogKey>>,
    ) -> Result<LogIterTyped<Self, E>, ArchiveError> {
        let range = range.unwrap_or_else(LogKey::full_range);

        let inner = self.iter_logs(ns, range)?;

        Ok(LogIterTyped::<Self, E>::new(inner, ns))
    }

    // TODO: Generalize blocks in archive using log mechanism
    fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_block_with_tx(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, TxOrder)>, ArchiveError>;

    fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, ArchiveError>;

    fn get_utxo(&self, txo_ref: &TxoRef) -> Result<Option<EraCbor>, ArchiveError> {
        let Some(tx) = self.get_tx(txo_ref.0.as_ref())? else {
            return Ok(None);
        };
        let era = tx.era();
        let decoded = MultiEraTx::decode_for_era(tx.era().try_into().unwrap(), tx.cbor())?;
        let Some(output) = decoded.output_at(txo_ref.1 as usize) else {
            return Ok(None);
        };
        Ok(Some(EraCbor(era, output.encode())))
    }

    fn get_plutus_data(&self, datum_hash: &Hash<32>) -> Result<Option<PlutusData>, ArchiveError>;

    fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError>;

    fn get_tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, ArchiveError>;

    fn iter_blocks_with_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn iter_blocks_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn iter_blocks_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn iter_blocks_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn iter_blocks_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn iter_blocks_with_metadata(
        &self,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError>;

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError>;

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError>;

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError>;

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError>;
}
