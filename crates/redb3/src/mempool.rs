use std::sync::Arc;

use pallas::codec::minicbor::{self, Decode, Encode};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, info, warn};

use dolos_core::{
    config::RedbMempoolConfig, EraCbor, MempoolError, MempoolEvent, MempoolStore, MempoolTx,
    MempoolTxStage, TxHash,
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
// SEQ: singleton counter for the next pending sequence number
const SEQ_TABLE: TableDefinition<(), u64> = TableDefinition::new("seq");
// FINALIZED: key = 32-byte tx_hash, value = () (lightweight record of finalized txs)
const FINALIZED_TABLE: TableDefinition<&[u8], ()> = TableDefinition::new("finalized");

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
}

impl TrackingRecord {
    fn new(payload: EraCbor) -> Self {
        Self {
            stage: TrackingStage::Inflight,
            confirmations: 0,
            payload,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        minicbor::to_vec(self).unwrap()
    }

    fn deserialize(bytes: &[u8]) -> Self {
        minicbor::decode(bytes).unwrap()
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

// ── Helpers ─────────────────────────────────────────────────────────────

fn encode_era_cbor(ec: &EraCbor) -> Vec<u8> {
    let EraCbor(era, cbor) = ec;
    let mut buf = Vec::with_capacity(2 + cbor.len());
    buf.extend_from_slice(&era.to_le_bytes());
    buf.extend_from_slice(cbor);
    buf
}

fn decode_era_cbor(bytes: &[u8]) -> EraCbor {
    let era = u16::from_le_bytes([bytes[0], bytes[1]]);
    let cbor = bytes[2..].to_vec();
    EraCbor(era, cbor)
}

fn pending_key(seq: u64, hash: &TxHash) -> [u8; 40] {
    let mut key = [0u8; 40];
    key[..8].copy_from_slice(&seq.to_be_bytes());
    key[8..].copy_from_slice(hash.as_ref());
    key
}

fn hash_from_pending_key(key: &[u8]) -> TxHash {
    let mut h = [0u8; 32];
    h.copy_from_slice(&key[8..40]);
    TxHash::from(h)
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
        wx.open_table(FINALIZED_TABLE)?;

        {
            let mut seq_table = wx.open_table(SEQ_TABLE)?;
            if seq_table.get(())?.is_none() {
                seq_table.insert((), 0u64)?;
            }
        }

        wx.commit()?;
        Ok(())
    }

    fn next_seq(wx: &redb::WriteTransaction) -> Result<u64, RedbMempoolError> {
        let mut seq_table = wx.open_table(SEQ_TABLE)?;
        let current = seq_table.get(())?.map(|v| v.value()).unwrap_or(0);
        seq_table.insert((), current + 1)?;
        Ok(current)
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
            let seq = Self::next_seq(&wx)?;
            let key = pending_key(seq, &tx.hash);
            let value = encode_era_cbor(&tx.payload);
            let mut table = wx.open_table(PENDING_TABLE)?;
            table.insert(key.as_slice(), value.as_slice())?;
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
            let key = entry.0.value();
            let value = entry.1.value();
            let hash = hash_from_pending_key(key);
            let payload = decode_era_cbor(value);
            result.push(MempoolTx {
                hash,
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
            let key = entry.0.value();
            let value = entry.1.value();
            let hash = hash_from_pending_key(key);
            let payload = decode_era_cbor(value);
            result.push((hash, payload));
        }

        result
    }

    fn mark_inflight(&self, hashes: &[TxHash]) {
        let hash_set: std::collections::HashSet<TxHash> = hashes.iter().copied().collect();

        self.with_write_tx("mark_inflight", |wx| {
            let mut pending = wx.open_table(PENDING_TABLE)?;
            let mut tracking = wx.open_table(TRACKING_TABLE)?;

            // Collect keys to remove first (can't mutate while iterating)
            let mut keys_to_remove: Vec<([u8; 40], Vec<u8>)> = Vec::new();
            {
                let iter = pending.iter()?;
                for entry in iter {
                    let entry = entry?;
                    let key_bytes = entry.0.value();
                    let value_bytes = entry.1.value();
                    let hash = hash_from_pending_key(key_bytes);
                    if hash_set.contains(&hash) {
                        let mut k = [0u8; 40];
                        k.copy_from_slice(key_bytes);
                        keys_to_remove.push((k, value_bytes.to_vec()));
                    }
                }
            }

            let mut events = Vec::new();
            for (pkey, era_cbor_bytes) in keys_to_remove {
                let hash = hash_from_pending_key(&pkey);
                pending.remove(pkey.as_slice())?;
                let payload = decode_era_cbor(&era_cbor_bytes);
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
                    if record.stage == TrackingStage::Inflight {
                        record.stage = TrackingStage::Acknowledged;
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

    fn apply(&self, seen_txs: &[TxHash], unseen_txs: &[TxHash]) {
        self.with_write_tx("apply", |wx| {
            let mut tracking = wx.open_table(TRACKING_TABLE)?;
            let mut events = Vec::new();

            for tx_hash in seen_txs {
                if let Some(mut record) = TrackingRecord::read(&tracking, tx_hash)? {
                    record.stage = TrackingStage::Confirmed;
                    record.confirmations += 1;
                    let tx = record.to_mempool_tx(*tx_hash);
                    record.write(&mut tracking, tx_hash)?;
                    info!(tx.hash = %tx.hash, "tx confirmed (redb)");
                    events.push((MempoolTxStage::Confirmed, tx));
                }
            }

            for tx_hash in unseen_txs {
                if let Some(mut record) = TrackingRecord::read(&tracking, tx_hash)? {
                    record.stage = TrackingStage::Acknowledged;
                    record.confirmations = 0;
                    let tx = record.to_mempool_tx(*tx_hash);
                    record.write(&mut tracking, tx_hash)?;
                    info!(tx.hash = %tx.hash, "tx rollback (redb)");
                    events.push((MempoolTxStage::RolledBack, tx));
                }
            }

            Ok(events)
        });
    }

    fn finalize(&self, threshold: u32) {
        self.with_write_tx("finalize", |wx| {
            let mut tracking = wx.open_table(TRACKING_TABLE)?;
            let mut fin_table = wx.open_table(FINALIZED_TABLE)?;

            // Collect entries to finalize first (can't mutate while iterating)
            let mut to_finalize: Vec<(Vec<u8>, MempoolTx)> = Vec::new();
            {
                let iter = tracking.iter()?;
                for entry in iter {
                    let entry = entry?;
                    let key = entry.0.value().to_vec();
                    let record = TrackingRecord::deserialize(entry.1.value());
                    if record.stage == TrackingStage::Confirmed && record.confirmations >= threshold
                    {
                        let mut hash_bytes = [0u8; 32];
                        hash_bytes.copy_from_slice(&key);
                        let hash = TxHash::from(hash_bytes);
                        let tx = record.to_mempool_tx(hash);
                        to_finalize.push((key, tx));
                    }
                }
            }

            let mut events = Vec::new();
            for (key, tx) in to_finalize {
                tracking.remove(key.as_slice())?;
                fin_table.insert(key.as_slice(), ())?;
                info!(tx.hash = %tx.hash, "tx finalized (redb)");
                events.push((MempoolTxStage::Finalized, tx));
            }

            Ok(events)
        });
    }

    fn check_stage(&self, tx_hash: &TxHash) -> MempoolTxStage {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return MempoolTxStage::Unknown,
        };

        // Check tracking table first
        if let Ok(table) = rx.open_table(TRACKING_TABLE) {
            if let Ok(Some(entry)) = table.get(tx_hash.as_ref()) {
                let record = TrackingRecord::deserialize(entry.value());
                return match record.stage {
                    TrackingStage::Inflight => MempoolTxStage::Inflight,
                    TrackingStage::Acknowledged => MempoolTxStage::Acknowledged,
                    TrackingStage::Confirmed => MempoolTxStage::Confirmed,
                };
            }
        }

        // Check finalized table
        if let Ok(table) = rx.open_table(FINALIZED_TABLE) {
            if let Ok(Some(_)) = table.get(tx_hash.as_ref()) {
                return MempoolTxStage::Finalized;
            }
        }

        // Check pending
        if let Ok(table) = rx.open_table(PENDING_TABLE) {
            if let Ok(iter) = table.iter() {
                for entry in iter {
                    let Ok(entry) = entry else { break };
                    let key = entry.0.value();
                    let hash = hash_from_pending_key(key);
                    if hash == *tx_hash {
                        return MempoolTxStage::Pending;
                    }
                }
            }
        }

        MempoolTxStage::Unknown
    }

    fn subscribe(&self) -> Self::Stream {
        RedbMempoolStream {
            inner: BroadcastStream::new(self.updates.subscribe()),
        }
    }
}
