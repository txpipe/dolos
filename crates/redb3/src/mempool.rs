use std::sync::Arc;

use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, info, warn};

use dolos_core::{
    config::RedbMempoolConfig, EraCbor, MempoolError, MempoolEvent, MempoolStore, MempoolTx,
    MempoolTxStage, TxHash,
};

const DEFAULT_CACHE_SIZE_MB: usize = 32;

// Table definitions:
// PENDING: key = [8-byte seq BE ++ 32-byte tx_hash], value = era(u16 LE) ++ cbor bytes
const PENDING_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("pending");
// INFLIGHT: key = 32-byte tx_hash, value = era(u16 LE) ++ cbor bytes
const INFLIGHT_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("inflight");
// ACKNOWLEDGED: key = 32-byte tx_hash, value = [1-byte confirmed flag] ++ era(u16 LE) ++ cbor bytes
const ACKNOWLEDGED_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("acknowledged");
// SEQ: singleton counter for the next pending sequence number
const SEQ_TABLE: TableDefinition<(), u64> = TableDefinition::new("seq");

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
    pub fn open(path: impl AsRef<std::path::Path>, config: &RedbMempoolConfig) -> Result<Self, MempoolError> {
        let db = redb::Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "mempool db is repairing")
            })
            .set_cache_size(1024 * 1024 * config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        let out = Self::from_db(db)?;
        Ok(out)
    }

    pub fn in_memory() -> Result<Self, MempoolError> {
        let db = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        let out = Self::from_db(db)?;
        Ok(out)
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

    fn ensure_initialized(&self) -> Result<(), MempoolError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        wx.open_table(PENDING_TABLE)
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;
        wx.open_table(INFLIGHT_TABLE)
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;
        wx.open_table(ACKNOWLEDGED_TABLE)
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        {
            let mut seq_table = wx
                .open_table(SEQ_TABLE)
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;
            if seq_table
                .get(())
                .map_err(|e| MempoolError::Internal(Box::new(e)))?
                .is_none()
            {
                seq_table
                    .insert((), 0u64)
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;
            }
        }

        wx.commit()
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;
        Ok(())
    }

    fn next_seq(wx: &redb::WriteTransaction) -> Result<u64, MempoolError> {
        let mut seq_table = wx
            .open_table(SEQ_TABLE)
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;
        let current = seq_table
            .get(())
            .map_err(|e| MempoolError::Internal(Box::new(e)))?
            .map(|v| v.value())
            .unwrap_or(0);
        seq_table
            .insert((), current + 1)
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;
        Ok(current)
    }

    fn notify(&self, new_stage: MempoolTxStage, tx: MempoolTx) {
        if self.updates.send(MempoolEvent { new_stage, tx }).is_err() {
            debug!("no mempool update receivers");
        }
    }
}

impl MempoolStore for RedbMempool {
    type Stream = RedbMempoolStream;

    fn receive(&self, tx: MempoolTx) -> Result<(), MempoolError> {
        info!(tx.hash = %tx.hash, "tx received (redb)");

        let wx = self
            .db
            .begin_write()
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        {
            let seq = Self::next_seq(&wx)?;
            let key = pending_key(seq, &tx.hash);
            let value = encode_era_cbor(&tx.payload);
            let mut table = wx
                .open_table(PENDING_TABLE)
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;
        }

        wx.commit()
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        self.notify(MempoolTxStage::Pending, tx);
        Ok(())
    }

    fn has_pending(&self) -> bool {
        let rx = match self
            .db
            .begin_read()
        {
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

        let wx = match self
            .db
            .begin_write()
        {
            Ok(wx) => wx,
            Err(e) => {
                warn!(error = %e, "failed to begin write for mark_inflight");
                return;
            }
        };

        let mut moved = Vec::new();

        {
            let mut pending = match wx.open_table(PENDING_TABLE) {
                Ok(t) => t,
                Err(_) => return,
            };
            let mut inflight = match wx.open_table(INFLIGHT_TABLE) {
                Ok(t) => t,
                Err(_) => return,
            };

            // Collect keys to remove first (can't mutate while iterating)
            let mut keys_to_remove: Vec<([u8; 40], Vec<u8>)> = Vec::new();
            {
                let iter = match pending.iter() {
                    Ok(it) => it,
                    Err(_) => return,
                };
                for entry in iter {
                    let Ok(entry) = entry else { break };
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

            for (pkey, value) in keys_to_remove {
                let hash = hash_from_pending_key(&pkey);
                let _ = pending.remove(pkey.as_slice());
                let _ = inflight.insert(hash.as_ref(), value.as_slice());
                let payload = decode_era_cbor(&value);
                moved.push(MempoolTx {
                    hash,
                    payload,
                    confirmed: false,
                    report: None,
                });
            }
        }

        if let Err(e) = wx.commit() {
            warn!(error = %e, "failed to commit mark_inflight");
            return;
        }

        for tx in moved {
            info!(tx.hash = %tx.hash, "tx inflight (redb)");
            self.notify(MempoolTxStage::Inflight, tx);
        }
    }

    fn mark_acknowledged(&self, hashes: &[TxHash]) {
        let hash_set: std::collections::HashSet<TxHash> = hashes.iter().copied().collect();

        let wx = match self
            .db
            .begin_write()
        {
            Ok(wx) => wx,
            Err(e) => {
                warn!(error = %e, "failed to begin write for mark_acknowledged");
                return;
            }
        };

        let mut moved = Vec::new();

        {
            let mut inflight = match wx.open_table(INFLIGHT_TABLE) {
                Ok(t) => t,
                Err(_) => return,
            };
            let mut acknowledged = match wx.open_table(ACKNOWLEDGED_TABLE) {
                Ok(t) => t,
                Err(_) => return,
            };

            for hash in &hash_set {
                if let Ok(Some(entry)) = inflight.remove(hash.as_ref()) {
                    let era_cbor_bytes = entry.value().to_vec();
                    // Acknowledged value: [confirmed=0] ++ era_cbor
                    let mut ack_value = Vec::with_capacity(1 + era_cbor_bytes.len());
                    ack_value.push(0u8); // not confirmed
                    ack_value.extend_from_slice(&era_cbor_bytes);
                    let _ = acknowledged.insert(hash.as_ref(), ack_value.as_slice());
                    let payload = decode_era_cbor(&era_cbor_bytes);
                    moved.push(MempoolTx {
                        hash: *hash,
                        payload,
                        confirmed: false,
                        report: None,
                    });
                }
            }
        }

        if let Err(e) = wx.commit() {
            warn!(error = %e, "failed to commit mark_acknowledged");
            return;
        }

        for tx in moved {
            info!(tx.hash = %tx.hash, "tx acknowledged (redb)");
            self.notify(MempoolTxStage::Acknowledged, tx);
        }
    }

    fn get_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
        let rx = self.db.begin_read().ok()?;
        let table = rx.open_table(INFLIGHT_TABLE).ok()?;
        let entry = table.get(tx_hash.as_ref()).ok()??;
        let value = entry.value();
        let payload = decode_era_cbor(value);
        Some(MempoolTx {
            hash: *tx_hash,
            payload,
            confirmed: false,
            report: None,
        })
    }

    fn apply(&self, seen_txs: &[TxHash], unseen_txs: &[TxHash]) {
        let wx = match self
            .db
            .begin_write()
        {
            Ok(wx) => wx,
            Err(e) => {
                warn!(error = %e, "failed to begin write for apply");
                return;
            }
        };

        let mut events = Vec::new();

        {
            let mut acknowledged = match wx.open_table(ACKNOWLEDGED_TABLE) {
                Ok(t) => t,
                Err(_) => return,
            };

            for tx_hash in seen_txs {
                let existing = acknowledged
                    .get(tx_hash.as_ref())
                    .ok()
                    .flatten()
                    .map(|entry| entry.value().to_vec());

                if let Some(mut value) = existing {
                    if !value.is_empty() {
                        value[0] = 1; // confirmed = true
                        let _ = acknowledged.insert(tx_hash.as_ref(), value.as_slice());
                        let payload = decode_era_cbor(&value[1..]);
                        let tx = MempoolTx {
                            hash: *tx_hash,
                            payload,
                            confirmed: true,
                            report: None,
                        };
                        info!(tx.hash = %tx.hash, "tx confirmed (redb)");
                        events.push((MempoolTxStage::Confirmed, tx));
                    }
                }
            }

            for tx_hash in unseen_txs {
                let existing = acknowledged
                    .get(tx_hash.as_ref())
                    .ok()
                    .flatten()
                    .map(|entry| entry.value().to_vec());

                if let Some(mut value) = existing {
                    if !value.is_empty() {
                        value[0] = 0; // confirmed = false
                        let _ = acknowledged.insert(tx_hash.as_ref(), value.as_slice());
                        let payload = decode_era_cbor(&value[1..]);
                        let tx = MempoolTx {
                            hash: *tx_hash,
                            payload,
                            confirmed: false,
                            report: None,
                        };
                        info!(tx.hash = %tx.hash, "tx rollback (redb)");
                        events.push((MempoolTxStage::RolledBack, tx));
                    }
                }
            }
        }

        if let Err(e) = wx.commit() {
            warn!(error = %e, "failed to commit apply");
            return;
        }

        for (stage, tx) in events {
            self.notify(stage, tx);
        }
    }

    fn check_stage(&self, tx_hash: &TxHash) -> MempoolTxStage {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return MempoolTxStage::Unknown,
        };

        // Check acknowledged first
        if let Ok(table) = rx.open_table(ACKNOWLEDGED_TABLE) {
            if let Ok(Some(entry)) = table.get(tx_hash.as_ref()) {
                let value = entry.value();
                if !value.is_empty() && value[0] == 1 {
                    return MempoolTxStage::Confirmed;
                }
                return MempoolTxStage::Acknowledged;
            }
        }

        // Check inflight
        if let Ok(table) = rx.open_table(INFLIGHT_TABLE) {
            if let Ok(Some(_)) = table.get(tx_hash.as_ref()) {
                return MempoolTxStage::Inflight;
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
