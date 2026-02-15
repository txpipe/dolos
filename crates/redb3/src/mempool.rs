use std::sync::Arc;

use pallas::codec::minicbor::{self, Decode, Encode};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, info, warn};

use dolos_core::{
    config::RedbMempoolConfig, ChainPoint, EraCbor, MempoolError, MempoolEvent, MempoolPage,
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
// INFLIGHT: key = 32-byte tx_hash, value = cbor(InflightRecord)  (table name stays "tracking" to avoid migration)
const INFLIGHT_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("tracking");
// FINALIZED_LOG: key = u64 sequence number, value = bincode(FinalizedLogEntry)
const FINALIZED_LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("finalized_log");

// ── Inflight record ─────────────────────────────────────────────────────

#[derive(Encode, Decode, PartialEq)]
enum InflightStage {
    #[n(0)]
    Propagated,
    #[n(1)]
    Acknowledged,
    #[n(2)]
    Confirmed,
}

#[derive(Encode, Decode)]
struct InflightRecord {
    #[n(0)]
    stage: InflightStage,
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
    #[n(3)]
    payload: Option<EraCbor>,
}

impl FinalizedLogEntry {
    fn serialize(&self) -> Vec<u8> {
        minicbor::to_vec(self).unwrap()
    }

    fn deserialize(bytes: &[u8]) -> Self {
        minicbor::decode(bytes).unwrap()
    }

    fn into_mempool_tx(self) -> MempoolTx {
        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(&self.hash);
        MempoolTx {
            hash: TxHash::from(hash_bytes),
            payload: self.payload.unwrap_or(EraCbor(0, vec![])),
            stage: MempoolTxStage::Finalized,
            confirmations: self.confirmations,
            confirmed_at: self.confirmed_at.map(|b| {
                ChainPoint::from_bytes(b[..].try_into().unwrap())
            }),
            report: None,
        }
    }
}

impl InflightRecord {
    fn new(payload: EraCbor) -> Self {
        Self {
            stage: InflightStage::Propagated,
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
        if self.stage == InflightStage::Propagated {
            self.stage = InflightStage::Acknowledged;
            true
        } else {
            false
        }
    }

    fn confirm(&mut self, point: &ChainPoint) {
        self.stage = InflightStage::Confirmed;
        self.confirmations += 1;
        if self.confirmed_at.is_none() {
            self.confirmed_at = Some(point.clone().into_bytes().to_vec());
        }
    }

    fn is_finalizable(&self, threshold: u32) -> bool {
        self.stage == InflightStage::Confirmed && self.confirmations >= threshold
    }

    fn to_tx_status(&self) -> TxStatus {
        let stage = match self.stage {
            InflightStage::Propagated => MempoolTxStage::Propagated,
            InflightStage::Acknowledged => MempoolTxStage::Acknowledged,
            InflightStage::Confirmed => MempoolTxStage::Confirmed,
        };
        TxStatus {
            stage,
            confirmations: self.confirmations,
            confirmed_at: self.confirmed_at.as_ref().map(|b| {
                ChainPoint::from_bytes(b[..].try_into().unwrap())
            }),
        }
    }

    fn into_finalized_log_entry(self, hash: TxHash) -> FinalizedLogEntry {
        FinalizedLogEntry {
            hash: hash.to_vec(),
            confirmations: self.confirmations,
            confirmed_at: self.confirmed_at,
            payload: Some(self.payload),
        }
    }

    fn to_mempool_tx(&self, hash: TxHash) -> MempoolTx {
        let stage = match self.stage {
            InflightStage::Propagated => MempoolTxStage::Propagated,
            InflightStage::Acknowledged => MempoolTxStage::Acknowledged,
            InflightStage::Confirmed => MempoolTxStage::Confirmed,
        };
        MempoolTx {
            hash,
            payload: self.payload.clone(),
            stage,
            confirmations: self.confirmations,
            confirmed_at: self.confirmed_at.as_ref().map(|b| {
                ChainPoint::from_bytes(b[..].try_into().unwrap())
            }),
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
        wx.open_table(INFLIGHT_TABLE)?;
        wx.open_table(FINALIZED_LOG_TABLE)?;

        wx.commit()?;
        Ok(())
    }

    fn notify(&self, tx: MempoolTx) {
        if self.updates.send(MempoolEvent { tx }).is_err() {
            debug!("no mempool update receivers");
        }
    }

    fn with_write_tx<F>(&self, op_name: &str, f: F)
    where
        F: FnOnce(
            &redb::WriteTransaction,
        ) -> Result<Vec<MempoolTx>, RedbMempoolError>,
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

        for tx in events {
            self.notify(tx);
        }
    }

    fn receive_inner(&self, tx: MempoolTx) -> Result<(), RedbMempoolError> {
        let wx = self.db.begin_write()?;

        {
            let mut table = wx.open_table(PENDING_TABLE)?;

            // Check for duplicate hash in pending queue
            for entry in table.iter()? {
                let entry = entry?;
                let key = PendingKey::from_bytes(entry.0.value());
                if key.hash() == tx.hash {
                    return Err(MempoolError::DuplicateTx.into());
                }
            }

            let seq = PendingKey::next_seq(&table)?;
            let key = PendingKey::new(seq, &tx.hash);
            let value = minicbor::to_vec(&tx.payload).unwrap();
            table.insert(key.as_bytes(), value.as_slice())?;
        }

        wx.commit()?;
        self.notify(tx);
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
                stage: MempoolTxStage::Pending,
                confirmations: 0,
                confirmed_at: None,
                report: None,
            });
        }

        result
    }

    fn mark_inflight(&self, hashes: &[TxHash]) {
        let hash_set: std::collections::HashSet<TxHash> = hashes.iter().copied().collect();

        self.with_write_tx("mark_inflight", |wx| {
            let mut pending = wx.open_table(PENDING_TABLE)?;
            let mut tracking = wx.open_table(INFLIGHT_TABLE)?;

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
                let record = InflightRecord::new(payload);
                let tx = record.to_mempool_tx(hash);
                record.write(&mut tracking, &hash)?;
                info!(tx.hash = %tx.hash, "tx inflight (redb)");
                events.push(tx);
            }

            Ok(events)
        });
    }

    fn mark_acknowledged(&self, hashes: &[TxHash]) {
        self.with_write_tx("mark_acknowledged", |wx| {
            let mut tracking = wx.open_table(INFLIGHT_TABLE)?;
            let mut events = Vec::new();

            for hash in hashes {
                if let Some(mut record) = InflightRecord::read(&tracking, hash)? {
                    if record.acknowledge() {
                        let tx = record.to_mempool_tx(*hash);
                        record.write(&mut tracking, hash)?;
                        info!(tx.hash = %tx.hash, "tx acknowledged (redb)");
                        events.push(tx);
                    }
                }
            }

            Ok(events)
        });
    }

    fn find_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
        let rx = self.db.begin_read().ok()?;
        let table = rx.open_table(INFLIGHT_TABLE).ok()?;
        let entry = table.get(tx_hash.as_ref()).ok()??;
        let record = InflightRecord::deserialize(entry.value());
        Some(record.to_mempool_tx(*tx_hash))
    }

    fn peek_inflight(&self, limit: usize) -> Vec<MempoolTx> {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return vec![],
        };
        let table = match rx.open_table(INFLIGHT_TABLE) {
            Ok(t) => t,
            Err(_) => return vec![],
        };
        let iter = match table.iter() {
            Ok(it) => it,
            Err(_) => return vec![],
        };

        let mut result = Vec::new();
        for entry in iter {
            if result.len() >= limit {
                break;
            }
            let Ok(entry) = entry else { break };
            let key_bytes = entry.0.value();
            let mut hash_bytes = [0u8; 32];
            hash_bytes.copy_from_slice(key_bytes);
            let hash = TxHash::from(hash_bytes);
            let record = InflightRecord::deserialize(entry.1.value());
            result.push(record.to_mempool_tx(hash));
        }

        result
    }

    fn confirm(&self, point: &ChainPoint, seen_txs: &[TxHash], unseen_txs: &[TxHash]) {
        let point = point.clone();
        self.with_write_tx("confirm", |wx| {
            let mut tracking = wx.open_table(INFLIGHT_TABLE)?;
            let mut events = Vec::new();

            for tx_hash in seen_txs {
                if let Some(mut record) = InflightRecord::read(&tracking, tx_hash)? {
                    record.confirm(&point);
                    let tx = record.to_mempool_tx(*tx_hash);
                    record.write(&mut tracking, tx_hash)?;
                    info!(tx.hash = %tx.hash, "tx confirmed (redb)");
                    events.push(tx);
                }
            }

            let mut pending = wx.open_table(PENDING_TABLE)?;

            for tx_hash in unseen_txs {
                if let Some(mut record) = InflightRecord::read(&tracking, tx_hash)? {
                    record.confirmed_at = None;
                    tracking.remove(tx_hash.as_ref())?;
                    let seq = PendingKey::next_seq(&pending)?;
                    let key = PendingKey::new(seq, tx_hash);
                    let value = minicbor::to_vec(&record.payload).unwrap();
                    pending.insert(key.as_bytes(), value.as_slice())?;
                    let mut tx = record.to_mempool_tx(*tx_hash);
                    tx.stage = MempoolTxStage::Pending;
                    tx.confirmations = 0;
                    tx.confirmed_at = None;
                    info!(tx.hash = %tx.hash, "tx rollback to pending (redb)");
                    events.push(tx);
                }
            }

            Ok(events)
        });
    }

    fn finalize(&self, threshold: u32) {
        self.with_write_tx("finalize", |wx| {
            let mut tracking = wx.open_table(INFLIGHT_TABLE)?;
            let mut fin_log = wx.open_table(FINALIZED_LOG_TABLE)?;

            // Collect entries to finalize first (can't mutate while iterating)
            let mut to_finalize: Vec<(Vec<u8>, InflightRecord)> = Vec::new();
            {
                let iter = tracking.iter()?;
                for entry in iter {
                    let entry = entry?;
                    let key = entry.0.value().to_vec();
                    let record = InflightRecord::deserialize(entry.1.value());
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
                let mut tx = record.to_mempool_tx(hash);
                let log_entry = record.into_finalized_log_entry(hash);

                tracking.remove(key.as_slice())?;

                let fin_seq = next_finalized_seq(&fin_log)?;
                fin_log.insert(fin_seq, log_entry.serialize().as_slice())?;

                tx.stage = MempoolTxStage::Finalized;
                info!(tx.hash = %tx.hash, "tx finalized (redb)");
                events.push(tx);
            }

            Ok(events)
        });
    }

    fn check_status(&self, tx_hash: &TxHash) -> TxStatus {
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
        if let Ok(table) = rx.open_table(INFLIGHT_TABLE) {
            if let Ok(Some(entry)) = table.get(tx_hash.as_ref()) {
                let record = InflightRecord::deserialize(entry.value());
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

    fn dump_finalized(&self, cursor: u64, limit: usize) -> MempoolPage {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return MempoolPage { items: vec![], next_cursor: None },
        };

        let table = match rx.open_table(FINALIZED_LOG_TABLE) {
            Ok(t) => t,
            Err(_) => return MempoolPage { items: vec![], next_cursor: None },
        };

        let iter = match table.range(cursor..) {
            Ok(it) => it,
            Err(_) => return MempoolPage { items: vec![], next_cursor: None },
        };

        let mut items = Vec::with_capacity(limit);
        let mut last_seq = None;

        for entry in iter {
            if items.len() >= limit {
                break;
            }
            let Ok(entry) = entry else { break };
            let seq = entry.0.value();
            let log_entry = FinalizedLogEntry::deserialize(entry.1.value());
            items.push(log_entry.into_mempool_tx());
            last_seq = Some(seq);
        }

        let next_cursor = if items.len() >= limit {
            last_seq.map(|s| s + 1)
        } else {
            None
        };

        MempoolPage { items, next_cursor }
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
        assert_eq!(peeked[0].payload, payload);
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

        let err = store.receive(tx).unwrap_err();
        assert!(matches!(err, MempoolError::DuplicateTx));
        assert_eq!(store.peek_pending(10).len(), 1);
    }

    #[test]
    fn test_mark_inflight() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]);

        assert!(!store.has_pending());
        assert!(matches!(store.check_status(&hash).stage, MempoolTxStage::Propagated));
        assert!(store.find_inflight(&hash).is_some());
    }

    #[test]
    fn test_mark_acknowledged() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]);
        store.mark_acknowledged(&[hash]);

        assert!(matches!(store.check_status(&hash).stage, MempoolTxStage::Acknowledged));
        // find_inflight now returns any inflight sub-stage (Propagated, Acknowledged, Confirmed)
        assert!(store.find_inflight(&hash).is_some());
    }

    #[test]
    fn test_apply_seen() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]);
        store.mark_acknowledged(&[hash]);
        store.confirm(&test_point(), &[hash], &[]);

        assert!(matches!(store.check_status(&hash).stage, MempoolTxStage::Confirmed));
    }

    #[test]
    fn test_apply_unseen_rollback() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]);
        store.mark_acknowledged(&[hash]);
        store.confirm(&test_point(), &[hash], &[]);
        store.confirm(&test_point_2(), &[], &[hash]);

        assert!(matches!(store.check_status(&hash).stage, MempoolTxStage::Pending));
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
        store.confirm(&test_point(), &[hash], &[]);
        store.confirm(&test_point_2(), &[hash], &[]);
        store.finalize(2);

        assert!(matches!(store.check_status(&hash).stage, MempoolTxStage::Unknown));
    }

    #[test]
    fn test_finalize_below_threshold() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]);
        store.mark_acknowledged(&[hash]);
        store.confirm(&test_point(), &[hash], &[]);
        store.finalize(2);

        assert!(matches!(store.check_status(&hash).stage, MempoolTxStage::Confirmed));
    }

    #[test]
    fn test_stage_unknown() {
        let store = test_store();
        let hash = test_hash(99);

        assert!(matches!(store.check_status(&hash).stage, MempoolTxStage::Unknown));
    }

    #[test]
    fn test_get_tx_status_lifecycle() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;
        let point = test_point();

        // Unknown
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Unknown));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());

        // Pending
        store.receive(tx).unwrap();
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Pending));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());

        // Propagated
        store.mark_inflight(&[hash]);
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Propagated));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());

        // Acknowledged
        store.mark_acknowledged(&[hash]);
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Acknowledged));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());

        // Confirmed (1st confirmation)
        store.confirm(&point, &[hash], &[]);
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Confirmed));
        assert_eq!(status.confirmations, 1);
        assert!(status.confirmed_at.is_some());
        assert_eq!(status.confirmed_at.as_ref().unwrap().slot(), point.slot());

        // Confirmed (2nd confirmation — confirmed_at stays the same)
        store.confirm(&test_point_2(), &[hash], &[]);
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Confirmed));
        assert_eq!(status.confirmations, 2);
        assert_eq!(status.confirmed_at.as_ref().unwrap().slot(), point.slot());

        // Finalized — no longer tracked by check_status, returns Unknown
        store.finalize(2);
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Unknown));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());
    }

    #[test]
    fn test_read_finalized_log_empty() {
        let store = test_store();
        let page = store.dump_finalized(0, 50);
        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
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
            store.confirm(&point, &[hash], &[]);
            store.confirm(&test_point_2(), &[hash], &[]);
        }
        store.finalize(2);

        // Read all
        let page = store.dump_finalized(0, 50);
        assert_eq!(page.items.len(), 3);
        assert!(page.next_cursor.is_none());
        for entry in &page.items {
            assert_eq!(entry.confirmations, 2);
            assert!(entry.confirmed_at.is_some());
            assert!(!entry.payload.1.is_empty(), "finalized entry should include payload");
        }

        // Read with limit
        let page = store.dump_finalized(0, 2);
        assert_eq!(page.items.len(), 2);
        assert!(page.next_cursor.is_some());

        // Read remainder
        let page2 = store.dump_finalized(page.next_cursor.unwrap(), 50);
        assert_eq!(page2.items.len(), 1);
        assert!(page2.next_cursor.is_none());
    }

    #[test]
    fn test_inflight_listing() {
        let store = test_store();
        let h1 = test_hash(1);
        let h2 = test_hash(2);
        let h3 = test_hash(3);

        store.receive(test_tx(1)).unwrap();
        store.receive(test_tx(2)).unwrap();
        store.receive(test_tx(3)).unwrap();

        store.mark_inflight(&[h1, h2, h3]);

        // All three start as Propagated
        let listing = store.peek_inflight(usize::MAX);
        assert_eq!(listing.len(), 3);
        assert!(listing.iter().all(|tx| tx.stage == MempoolTxStage::Propagated));

        // Acknowledge h2
        store.mark_acknowledged(&[h2]);
        let listing = store.peek_inflight(usize::MAX);
        assert_eq!(listing.len(), 3);
        let h2_stage = listing.iter().find(|tx| tx.hash == h2).unwrap().stage.clone();
        assert_eq!(h2_stage, MempoolTxStage::Acknowledged);

        // Confirm h2
        store.confirm(&test_point(), &[h2], &[]);
        let listing = store.peek_inflight(usize::MAX);
        assert_eq!(listing.len(), 3);
        let h2_stage = listing.iter().find(|tx| tx.hash == h2).unwrap().stage.clone();
        assert_eq!(h2_stage, MempoolTxStage::Confirmed);

        // Finalize h2 — should drop from listing
        store.confirm(&test_point_2(), &[h2], &[]);
        store.finalize(2);
        let listing = store.peek_inflight(usize::MAX);
        assert_eq!(listing.len(), 2);
        assert!(!listing.iter().any(|tx| tx.hash == h2));
    }
}
