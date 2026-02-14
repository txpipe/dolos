use std::sync::Arc;

use pallas::codec::minicbor::{self, Decode, Encode};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, info, warn};

use dolos_core::{
    config::RedbMempoolConfig, ChainPoint, EraCbor, FinalizedTx, MempoolError, MempoolEvent,
    MempoolStore, MempoolTx, MempoolTxStage, TxHash, TxStatus,
};

// ── Error newtype (mirrors wal/mod.rs pattern) ──────────────────────────

#[derive(Debug, Error)]
#[error(transparent)]
struct RedbMempoolError(#[from] MempoolError);

impl From<redb::Error> for RedbMempoolError {
    fn from(value: redb::Error) -> Self {
        Self(MempoolError::Internal(Box::new(value)))
    }
}

impl From<RedbMempoolError> for MempoolError {
    fn from(value: RedbMempoolError) -> Self {
        value.0
    }
}

impl From<redb::DatabaseError> for RedbMempoolError {
    fn from(value: redb::DatabaseError) -> Self {
        Self(MempoolError::Internal(Box::new(redb::Error::from(value))))
    }
}

impl From<redb::TableError> for RedbMempoolError {
    fn from(value: redb::TableError) -> Self {
        Self(MempoolError::Internal(Box::new(redb::Error::from(value))))
    }
}

impl From<redb::CommitError> for RedbMempoolError {
    fn from(value: redb::CommitError) -> Self {
        Self(MempoolError::Internal(Box::new(redb::Error::from(value))))
    }
}

impl From<redb::StorageError> for RedbMempoolError {
    fn from(value: redb::StorageError) -> Self {
        Self(MempoolError::Internal(Box::new(redb::Error::from(value))))
    }
}

impl From<redb::TransactionError> for RedbMempoolError {
    fn from(value: redb::TransactionError) -> Self {
        Self(MempoolError::Internal(Box::new(redb::Error::from(value))))
    }
}

// ── Table definitions ───────────────────────────────────────────────────

const DEFAULT_CACHE_SIZE_MB: usize = 32;

// PENDING: key = [8-byte seq BE ++ 32-byte tx_hash], value = era(u16 LE) ++ cbor bytes
const PENDING_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("pending");
// TRACKING: key = 32-byte tx_hash, value = bincode(TrackingRecord)
const TRACKING_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("tracking");
// FINALIZED_LOG: key = u64 sequence number, value = bincode(FinalizedLogEntry)
const FINALIZED_LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("finalized_log");

// ── Tracking record ─────────────────────────────────────────────────────

#[derive(Encode, Decode, PartialEq)]
enum TrackingStage {
    #[n(0)]
    Inflight,
    #[n(1)]
    Acknowledged,
    #[n(2)]
    Confirmed,
}

#[derive(Encode, Decode)]
struct TrackingRecord {
    #[n(0)]
    stage: TrackingStage,
    #[n(1)]
    confirmations: u32,
    #[n(2)]
    payload: EraCbor,
    #[cbor(n(3), with = "minicbor::bytes")]
    confirmed_at: Option<Vec<u8>>,
}

/// Entry stored in FINALIZED_LOG_TABLE for pagination.
#[derive(Encode, Decode)]
struct FinalizedLogEntry {
    #[cbor(n(0), with = "minicbor::bytes")]
    hash: Vec<u8>,
    #[n(1)]
    confirmations: u32,
    #[cbor(n(2), with = "minicbor::bytes")]
    confirmed_at: Option<Vec<u8>>,
}

impl FinalizedLogEntry {
    fn serialize(&self) -> Vec<u8> {
        minicbor::to_vec(self).unwrap()
    }

    fn deserialize(bytes: &[u8]) -> Self {
        minicbor::decode(bytes).unwrap()
    }

    fn to_finalized_tx(self) -> FinalizedTx {
        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(&self.hash);
        FinalizedTx {
            hash: TxHash::from(hash_bytes),
            confirmations: self.confirmations,
            confirmed_at: self.confirmed_at.map(|b| {
                ChainPoint::from_bytes(b[..].try_into().unwrap())
            }),
        }
    }
}

impl TrackingRecord {
    fn new(payload: EraCbor) -> Self {
        Self {
            stage: TrackingStage::Inflight,
            confirmations: 0,
            payload,
            confirmed_at: None,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        minicbor::to_vec(self).unwrap()
    }

    fn deserialize(bytes: &[u8]) -> Self {
        minicbor::decode(bytes).unwrap()
    }

    fn acknowledge(&mut self) -> bool {
        if self.stage == TrackingStage::Inflight {
            self.stage = TrackingStage::Acknowledged;
            true
        } else {
            false
        }
    }

    fn confirm(&mut self, point: &ChainPoint) {
        self.stage = TrackingStage::Confirmed;
        self.confirmations += 1;
        if self.confirmed_at.is_none() {
            self.confirmed_at = Some(point.clone().into_bytes().to_vec());
        }
    }

    fn is_finalizable(&self, threshold: u32) -> bool {
        self.stage == TrackingStage::Confirmed && self.confirmations >= threshold
    }

    fn to_tx_status(&self) -> TxStatus {
        let stage = match self.stage {
            TrackingStage::Inflight => MempoolTxStage::Inflight,
            TrackingStage::Acknowledged => MempoolTxStage::Acknowledged,
            TrackingStage::Confirmed => MempoolTxStage::Confirmed,
        };
        TxStatus {
            stage,
            confirmations: self.confirmations,
            confirmed_at: self.confirmed_at.as_ref().map(|b| {
                ChainPoint::from_bytes(b[..].try_into().unwrap())
            }),
        }
    }

    fn to_finalized_log_entry(self, hash: TxHash) -> FinalizedLogEntry {
        FinalizedLogEntry {
            hash: hash.to_vec(),
            confirmations: self.confirmations,
            confirmed_at: self.confirmed_at,
        }
    }

    fn to_mempool_tx(&self, hash: TxHash) -> MempoolTx {
        MempoolTx {
            hash,
            payload: self.payload.clone(),
            confirmed: self.stage == TrackingStage::Confirmed,
            report: None,
        }
    }

    fn read(
        table: &redb::Table<'_, &[u8], &[u8]>,
        hash: &TxHash,
    ) -> Result<Option<Self>, RedbMempoolError> {
        let entry = table.get(hash.as_ref())?;
        Ok(entry.map(|e| Self::deserialize(e.value())))
    }

    fn write(
        &self,
        table: &mut redb::Table<'_, &[u8], &[u8]>,
        hash: &TxHash,
    ) -> Result<(), RedbMempoolError> {
        table.insert(hash.as_ref(), self.serialize().as_slice())?;
        Ok(())
    }
}

// ── Key types ────────────────────────────────────────────────────────────

/// Composite key for PENDING_TABLE: `[8-byte seq BE ++ 32-byte tx_hash]`.
struct PendingKey([u8; 40]);

impl PendingKey {
    fn new(seq: u64, hash: &TxHash) -> Self {
        let mut key = [0u8; 40];
        key[..8].copy_from_slice(&seq.to_be_bytes());
        key[8..].copy_from_slice(hash.as_ref());
        Self(key)
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let mut key = [0u8; 40];
        key.copy_from_slice(bytes);
        Self(key)
    }

    fn hash(&self) -> TxHash {
        let mut h = [0u8; 32];
        h.copy_from_slice(&self.0[8..40]);
        TxHash::from(h)
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    fn next_seq(table: &redb::Table<'_, &[u8], &[u8]>) -> Result<u64, RedbMempoolError> {
        match table.last()? {
            Some(entry) => {
                let key = Self::from_bytes(entry.0.value());
                Ok(u64::from_be_bytes(key.0[..8].try_into().unwrap()) + 1)
            }
            None => Ok(0),
        }
    }
}

fn next_finalized_seq(table: &redb::Table<'_, u64, &[u8]>) -> Result<u64, RedbMempoolError> {
    match table.last()? {
        Some(entry) => Ok(entry.0.value() + 1),
        None => Ok(0),
    }
}

// ── RedbMempool ─────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct RedbMempool {
    db: Arc<redb::Database>,
    updates: broadcast::Sender<MempoolEvent>,
}

pub struct RedbMempoolStream {
    inner: BroadcastStream<MempoolEvent>,
}

impl futures_core::Stream for RedbMempoolStream {
    type Item = Result<MempoolEvent, MempoolError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures_util::StreamExt;

        match self.inner.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(x)) => match x {
                Ok(x) => std::task::Poll::Ready(Some(Ok(x))),
                Err(err) => {
                    std::task::Poll::Ready(Some(Err(MempoolError::Internal(Box::new(err)))))
                }
            },
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl RedbMempool {
    pub fn open(
        path: impl AsRef<std::path::Path>,
        config: &RedbMempoolConfig,
    ) -> Result<Self, MempoolError> {
        let db = redb::Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "mempool db is repairing")
            })
            .set_cache_size(1024 * 1024 * config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        Self::from_db(db)
    }

    pub fn in_memory() -> Result<Self, MempoolError> {
        let db = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        Self::from_db(db)
    }

    fn from_db(db: redb::Database) -> Result<Self, MempoolError> {
        let (updates, _) = broadcast::channel(16);

        let out = Self {
            db: Arc::new(db),
            updates,
        };

        out.ensure_initialized()?;
        Ok(out)
    }

    fn ensure_initialized(&self) -> Result<(), RedbMempoolError> {
        let wx = self.db.begin_write()?;
        wx.open_table(PENDING_TABLE)?;
        wx.open_table(TRACKING_TABLE)?;
        wx.open_table(FINALIZED_LOG_TABLE)?;

        wx.commit()?;
        Ok(())
    }

    fn notify(&self, new_stage: MempoolTxStage, tx: MempoolTx) {
        if self.updates.send(MempoolEvent { new_stage, tx }).is_err() {
            debug!("no mempool update receivers");
        }
    }

    fn with_write_tx<F>(&self, op_name: &str, f: F)
    where
        F: FnOnce(
            &redb::WriteTransaction,
        ) -> Result<Vec<(MempoolTxStage, MempoolTx)>, RedbMempoolError>,
    {
        let wx = match self.db.begin_write() {
            Ok(wx) => wx,
            Err(e) => {
                warn!(error = %e, "failed to begin write for {}", op_name);
                return;
            }
        };

        let events = match f(&wx) {
            Ok(events) => events,
            Err(e) => {
                let e: MempoolError = e.into();
                warn!(error = %e, "failed to execute {}", op_name);
                return;
            }
        };

        if let Err(e) = wx.commit() {
            warn!(error = %e, "failed to commit {}", op_name);
            return;
        }

        for (stage, tx) in events {
            self.notify(stage, tx);
        }
    }

    fn receive_inner(&self, tx: MempoolTx) -> Result<(), RedbMempoolError> {
        let wx = self.db.begin_write()?;

        {
            let mut table = wx.open_table(PENDING_TABLE)?;
            let seq = PendingKey::next_seq(&table)?;
            let key = PendingKey::new(seq, &tx.hash);
            let value = minicbor::to_vec(&tx.payload).unwrap();
            table.insert(key.as_bytes(), value.as_slice())?;
        }

        wx.commit()?;
        self.notify(MempoolTxStage::Pending, tx);
        Ok(())
    }
}

impl MempoolStore for RedbMempool {
    type Stream = RedbMempoolStream;

    fn receive(&self, tx: MempoolTx) -> Result<(), MempoolError> {
        info!(tx.hash = %tx.hash, "tx received (redb)");
        self.receive_inner(tx)?;
        Ok(())
    }

    fn has_pending(&self) -> bool {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return false,
        };
        let table = match rx.open_table(PENDING_TABLE) {
            Ok(t) => t,
            Err(_) => return false,
        };
        table.len().unwrap_or(0) > 0
    }

    fn peek_pending(&self, limit: usize) -> Vec<MempoolTx> {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return vec![],
        };
        let table = match rx.open_table(PENDING_TABLE) {
            Ok(t) => t,
            Err(_) => return vec![],
        };

        let iter = match table.iter() {
            Ok(it) => it,
            Err(_) => return vec![],
        };

        let mut result = Vec::with_capacity(limit);
        for entry in iter {
            if result.len() >= limit {
                break;
            }
            let Ok(entry) = entry else { break };
            let key = PendingKey::from_bytes(entry.0.value());
            let payload: EraCbor = minicbor::decode(entry.1.value()).unwrap();
            result.push(MempoolTx {
                hash: key.hash(),
                payload,
                confirmed: false,
                report: None,
            });
        }

        result
    }

    fn pending(&self) -> Vec<(TxHash, EraCbor)> {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return vec![],
        };
        let table = match rx.open_table(PENDING_TABLE) {
            Ok(t) => t,
            Err(_) => return vec![],
        };

        let iter = match table.iter() {
            Ok(it) => it,
            Err(_) => return vec![],
        };

        let mut result = Vec::new();
        for entry in iter {
            let Ok(entry) = entry else { break };
            let key = PendingKey::from_bytes(entry.0.value());
            let payload: EraCbor = minicbor::decode(entry.1.value()).unwrap();
            result.push((key.hash(), payload));
        }

        result
    }

    fn mark_inflight(&self, hashes: &[TxHash]) {
        let hash_set: std::collections::HashSet<TxHash> = hashes.iter().copied().collect();

        self.with_write_tx("mark_inflight", |wx| {
            let mut pending = wx.open_table(PENDING_TABLE)?;
            let mut tracking = wx.open_table(TRACKING_TABLE)?;

            // Collect keys to remove first (can't mutate while iterating)
            let mut keys_to_remove: Vec<(PendingKey, Vec<u8>)> = Vec::new();
            {
                let iter = pending.iter()?;
                for entry in iter {
                    let entry = entry?;
                    let key = PendingKey::from_bytes(entry.0.value());
                    if hash_set.contains(&key.hash()) {
                        keys_to_remove.push((key, entry.1.value().to_vec()));
                    }
                }
            }

            let mut events = Vec::new();
            for (pkey, era_cbor_bytes) in keys_to_remove {
                let hash = pkey.hash();
                pending.remove(pkey.as_bytes())?;
                let payload = minicbor::decode(&era_cbor_bytes).unwrap();
                let record = TrackingRecord::new(payload);
                let tx = record.to_mempool_tx(hash);
                record.write(&mut tracking, &hash)?;
                info!(tx.hash = %tx.hash, "tx inflight (redb)");
                events.push((MempoolTxStage::Inflight, tx));
            }

            Ok(events)
        });
    }

    fn mark_acknowledged(&self, hashes: &[TxHash]) {
        self.with_write_tx("mark_acknowledged", |wx| {
            let mut tracking = wx.open_table(TRACKING_TABLE)?;
            let mut events = Vec::new();

            for hash in hashes {
                if let Some(mut record) = TrackingRecord::read(&tracking, hash)? {
                    if record.acknowledge() {
                        let tx = record.to_mempool_tx(*hash);
                        record.write(&mut tracking, hash)?;
                        info!(tx.hash = %tx.hash, "tx acknowledged (redb)");
                        events.push((MempoolTxStage::Acknowledged, tx));
                    }
                }
            }

            Ok(events)
        });
    }

    fn get_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
        let rx = self.db.begin_read().ok()?;
        let table = rx.open_table(TRACKING_TABLE).ok()?;
        let entry = table.get(tx_hash.as_ref()).ok()??;
        let record = TrackingRecord::deserialize(entry.value());
        if record.stage != TrackingStage::Inflight {
            return None;
        }
        Some(record.to_mempool_tx(*tx_hash))
    }

    fn apply(&self, point: &ChainPoint, seen_txs: &[TxHash], unseen_txs: &[TxHash]) {
        let point = point.clone();
        self.with_write_tx("apply", |wx| {
            let mut tracking = wx.open_table(TRACKING_TABLE)?;
            let mut events = Vec::new();

            for tx_hash in seen_txs {
                if let Some(mut record) = TrackingRecord::read(&tracking, tx_hash)? {
                    record.confirm(&point);
                    let tx = record.to_mempool_tx(*tx_hash);
                    record.write(&mut tracking, tx_hash)?;
                    info!(tx.hash = %tx.hash, "tx confirmed (redb)");
                    events.push((MempoolTxStage::Confirmed, tx));
                }
            }

            let mut pending = wx.open_table(PENDING_TABLE)?;

            for tx_hash in unseen_txs {
                if let Some(mut record) = TrackingRecord::read(&tracking, tx_hash)? {
                    record.confirmed_at = None;
                    tracking.remove(tx_hash.as_ref())?;
                    let seq = PendingKey::next_seq(&pending)?;
                    let key = PendingKey::new(seq, tx_hash);
                    let value = minicbor::to_vec(&record.payload).unwrap();
                    pending.insert(key.as_bytes(), value.as_slice())?;
                    let tx = record.to_mempool_tx(*tx_hash);
                    info!(tx.hash = %tx.hash, "tx rollback to pending (redb)");
                    events.push((MempoolTxStage::Pending, tx));
                }
            }

            Ok(events)
        });
    }

    fn finalize(&self, threshold: u32) {
        self.with_write_tx("finalize", |wx| {
            let mut tracking = wx.open_table(TRACKING_TABLE)?;
            let mut fin_log = wx.open_table(FINALIZED_LOG_TABLE)?;

            // Collect entries to finalize first (can't mutate while iterating)
            let mut to_finalize: Vec<(Vec<u8>, TrackingRecord)> = Vec::new();
            {
                let iter = tracking.iter()?;
                for entry in iter {
                    let entry = entry?;
                    let key = entry.0.value().to_vec();
                    let record = TrackingRecord::deserialize(entry.1.value());
                    if record.is_finalizable(threshold) {
                        to_finalize.push((key, record));
                    }
                }
            }

            let mut events = Vec::new();
            for (key, record) in to_finalize {
                let mut hash_bytes = [0u8; 32];
                hash_bytes.copy_from_slice(&key);
                let hash = TxHash::from(hash_bytes);
                let tx = record.to_mempool_tx(hash);
                let log_entry = record.to_finalized_log_entry(hash);

                tracking.remove(key.as_slice())?;

                let fin_seq = next_finalized_seq(&fin_log)?;
                fin_log.insert(fin_seq, log_entry.serialize().as_slice())?;

                info!(tx.hash = %tx.hash, "tx finalized (redb)");
                events.push((MempoolTxStage::Finalized, tx));
            }

            Ok(events)
        });
    }

    fn check_stage(&self, tx_hash: &TxHash) -> MempoolTxStage {
        self.get_tx_status(tx_hash).stage
    }

    fn get_tx_status(&self, tx_hash: &TxHash) -> TxStatus {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => {
                return TxStatus {
                    stage: MempoolTxStage::Unknown,
                    confirmations: 0,
                    confirmed_at: None,
                }
            }
        };

        // Check tracking table first
        if let Ok(table) = rx.open_table(TRACKING_TABLE) {
            if let Ok(Some(entry)) = table.get(tx_hash.as_ref()) {
                let record = TrackingRecord::deserialize(entry.value());
                return record.to_tx_status();
            }
        }

        // Check pending
        if let Ok(table) = rx.open_table(PENDING_TABLE) {
            if let Ok(iter) = table.iter() {
                for entry in iter {
                    let Ok(entry) = entry else { break };
                    let hash = PendingKey::from_bytes(entry.0.value()).hash();
                    if hash == *tx_hash {
                        return TxStatus {
                            stage: MempoolTxStage::Pending,
                            confirmations: 0,
                            confirmed_at: None,
                        };
                    }
                }
            }
        }

        TxStatus {
            stage: MempoolTxStage::Unknown,
            confirmations: 0,
            confirmed_at: None,
        }
    }

    fn read_finalized_log(&self, cursor: u64, limit: usize) -> (Vec<FinalizedTx>, Option<u64>) {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return (vec![], None),
        };

        let table = match rx.open_table(FINALIZED_LOG_TABLE) {
            Ok(t) => t,
            Err(_) => return (vec![], None),
        };

        let iter = match table.range(cursor..) {
            Ok(it) => it,
            Err(_) => return (vec![], None),
        };

        let mut entries = Vec::with_capacity(limit);
        let mut last_seq = None;

        for entry in iter {
            if entries.len() >= limit {
                break;
            }
            let Ok(entry) = entry else { break };
            let seq = entry.0.value();
            let log_entry = FinalizedLogEntry::deserialize(entry.1.value());
            entries.push(log_entry.to_finalized_tx());
            last_seq = Some(seq);
        }

        let next_cursor = if entries.len() >= limit {
            last_seq.map(|s| s + 1)
        } else {
            None
        };

        (entries, next_cursor)
    }

    fn subscribe(&self) -> Self::Stream {
        RedbMempoolStream {
            inner: BroadcastStream::new(self.updates.subscribe()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> RedbMempool {
        RedbMempool::in_memory().unwrap()
    }

    fn test_hash(n: u8) -> TxHash {
        dolos_testing::tx_sequence_to_hash(n as u64)
    }

    fn test_tx(n: u8) -> MempoolTx {
        dolos_testing::mempool::make_test_mempool_tx(test_hash(n))
    }

    fn test_point() -> ChainPoint {
        ChainPoint::Specific(12345, pallas::crypto::hash::Hash::new([0xAB; 32]))
    }

    fn test_point_2() -> ChainPoint {
        ChainPoint::Specific(12346, pallas::crypto::hash::Hash::new([0xCD; 32]))
    }

    #[test]
    fn test_empty_store() {
        let store = test_store();
        assert!(!store.has_pending());
        assert!(store.peek_pending(10).is_empty());
        assert!(store.pending().is_empty());
    }

    #[test]
    fn test_receive_and_has_pending() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;
        let payload = tx.payload.clone();

        store.receive(tx).unwrap();

        assert!(store.has_pending());

        let peeked = store.peek_pending(10);
        assert_eq!(peeked.len(), 1);
        assert_eq!(peeked[0].hash, hash);

        let pending = store.pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, hash);
        assert_eq!(pending[0].1, payload);
    }

    #[test]
    fn test_peek_pending_respects_limit() {
        let store = test_store();
        for n in 0..3 {
            store.receive(test_tx(n)).unwrap();
        }

        let peeked = store.peek_pending(2);
        assert_eq!(peeked.len(), 2);
    }

    #[test]
    fn test_receive_duplicate_hash() {
        let store = test_store();
        let tx = test_tx(1);
        store.receive(tx.clone()).unwrap();
        store.receive(tx).unwrap();

        assert_eq!(store.peek_pending(10).len(), 2);
    }

    #[test]
    fn test_mark_inflight() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]);

        assert!(!store.has_pending());
        assert!(matches!(store.check_stage(&hash), MempoolTxStage::Inflight));
        assert!(store.get_inflight(&hash).is_some());
    }

    #[test]
    fn test_mark_acknowledged() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]);
        store.mark_acknowledged(&[hash]);

        assert!(matches!(store.check_stage(&hash), MempoolTxStage::Acknowledged));
        assert!(store.get_inflight(&hash).is_none());
    }

    #[test]
    fn test_apply_seen() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]);
        store.mark_acknowledged(&[hash]);
        store.apply(&test_point(), &[hash], &[]);

        assert!(matches!(store.check_stage(&hash), MempoolTxStage::Confirmed));
    }

    #[test]
    fn test_apply_unseen_rollback() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]);
        store.mark_acknowledged(&[hash]);
        store.apply(&test_point(), &[hash], &[]);
        store.apply(&test_point_2(), &[], &[hash]);

        assert!(matches!(store.check_stage(&hash), MempoolTxStage::Pending));
        assert!(store.has_pending());
        let peeked = store.peek_pending(10);
        assert_eq!(peeked.len(), 1);
        assert_eq!(peeked[0].hash, hash);
    }

    #[test]
    fn test_finalize() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]);
        store.mark_acknowledged(&[hash]);
        store.apply(&test_point(), &[hash], &[]);
        store.apply(&test_point_2(), &[hash], &[]);
        store.finalize(2);

        assert!(matches!(store.check_stage(&hash), MempoolTxStage::Unknown));
    }

    #[test]
    fn test_finalize_below_threshold() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]);
        store.mark_acknowledged(&[hash]);
        store.apply(&test_point(), &[hash], &[]);
        store.finalize(2);

        assert!(matches!(store.check_stage(&hash), MempoolTxStage::Confirmed));
    }

    #[test]
    fn test_check_stage_unknown() {
        let store = test_store();
        let hash = test_hash(99);

        assert!(matches!(store.check_stage(&hash), MempoolTxStage::Unknown));
    }

    #[test]
    fn test_get_tx_status_lifecycle() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;
        let point = test_point();

        // Unknown
        let status = store.get_tx_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Unknown));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());

        // Pending
        store.receive(tx).unwrap();
        let status = store.get_tx_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Pending));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());

        // Inflight
        store.mark_inflight(&[hash]);
        let status = store.get_tx_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Inflight));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());

        // Acknowledged
        store.mark_acknowledged(&[hash]);
        let status = store.get_tx_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Acknowledged));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());

        // Confirmed (1st confirmation)
        store.apply(&point, &[hash], &[]);
        let status = store.get_tx_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Confirmed));
        assert_eq!(status.confirmations, 1);
        assert!(status.confirmed_at.is_some());
        assert_eq!(status.confirmed_at.as_ref().unwrap().slot(), point.slot());

        // Confirmed (2nd confirmation — confirmed_at stays the same)
        store.apply(&test_point_2(), &[hash], &[]);
        let status = store.get_tx_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Confirmed));
        assert_eq!(status.confirmations, 2);
        assert_eq!(status.confirmed_at.as_ref().unwrap().slot(), point.slot());

        // Finalized — no longer tracked by get_tx_status, returns Unknown
        store.finalize(2);
        let status = store.get_tx_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Unknown));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());
    }

    #[test]
    fn test_read_finalized_log_empty() {
        let store = test_store();
        let (entries, next) = store.read_finalized_log(0, 50);
        assert!(entries.is_empty());
        assert!(next.is_none());
    }

    #[test]
    fn test_read_finalized_log_pagination() {
        let store = test_store();
        let point = test_point();

        // Finalize 3 transactions
        for n in 0..3u8 {
            let tx = test_tx(n);
            let hash = tx.hash;
            store.receive(tx).unwrap();
            store.mark_inflight(&[hash]);
            store.mark_acknowledged(&[hash]);
            store.apply(&point, &[hash], &[]);
            store.apply(&test_point_2(), &[hash], &[]);
        }
        store.finalize(2);

        // Read all
        let (entries, next) = store.read_finalized_log(0, 50);
        assert_eq!(entries.len(), 3);
        assert!(next.is_none());
        for entry in &entries {
            assert_eq!(entry.confirmations, 2);
            assert!(entry.confirmed_at.is_some());
        }

        // Read with limit
        let (entries, next) = store.read_finalized_log(0, 2);
        assert_eq!(entries.len(), 2);
        assert!(next.is_some());

        // Read remainder
        let (entries2, next2) = store.read_finalized_log(next.unwrap(), 50);
        assert_eq!(entries2.len(), 1);
        assert!(next2.is_none());
    }
}
