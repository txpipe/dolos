use minicbor::{Decode, Encode};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, info};

use dolos_core::{
    config::RedisMempoolConfig, ChainPoint, EraCbor, MempoolError, MempoolEvent, MempoolPage,
    MempoolStore, MempoolTx, MempoolTxStage, TxHash, TxStatus,
};

// Same as RedbMempool for consistency
#[derive(Clone, Debug, Encode, Decode, PartialEq)]
enum InflightStage {
    #[n(0)]
    Propagated,
    #[n(1)]
    Acknowledged,
    #[n(2)]
    Confirmed,
}

#[derive(Clone, Debug, Encode, Decode)]
struct InflightRecord {
    #[n(0)]
    stage: InflightStage,
    #[n(1)]
    confirmations: u32,
    #[n(2)]
    payload: EraCbor,
    #[cbor(n(3), with = "minicbor::bytes")]
    confirmed_at: Option<Vec<u8>>,
    #[n(4)]
    non_confirmations: u32,
}

impl InflightRecord {
    fn new(payload: EraCbor) -> Self {
        Self {
            stage: InflightStage::Propagated,
            confirmations: 0,
            payload,
            confirmed_at: None,
            non_confirmations: 0,
        }
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
        self.non_confirmations = 0;
        if self.confirmed_at.is_none() {
            self.confirmed_at = Some(point.clone().into_bytes().to_vec());
        }
    }

    fn retry(&mut self) {
        self.stage = InflightStage::Propagated;
        self.confirmations = 0;
        self.non_confirmations = 0;
        self.confirmed_at = None;
    }

    fn mark_stale(&mut self) {
        self.non_confirmations += 1;
    }

    fn is_finalizable(&self, threshold: u32) -> bool {
        self.stage == InflightStage::Confirmed && self.confirmations >= threshold
    }

    fn is_droppable(&self, threshold: u32) -> bool {
        self.stage != InflightStage::Confirmed && self.non_confirmations >= threshold
    }

    fn to_mempool_tx(&self, hash: TxHash) -> MempoolTx {
        MempoolTx {
            hash,
            payload: self.payload.clone(),
            stage: match self.stage {
                InflightStage::Propagated => MempoolTxStage::Propagated,
                InflightStage::Acknowledged => MempoolTxStage::Acknowledged,
                InflightStage::Confirmed => MempoolTxStage::Confirmed,
            },
            confirmations: self.confirmations,
            non_confirmations: self.non_confirmations,
            confirmed_at: self.confirmed_at.as_ref().map(|b| {
                let bytes: [u8; 40] = b.as_slice().try_into().expect("valid chainpoint bytes");
                ChainPoint::from_bytes(bytes)
            }),
            report: None,
        }
    }

    fn to_tx_status(&self) -> TxStatus {
        TxStatus {
            stage: match self.stage {
                InflightStage::Propagated => MempoolTxStage::Propagated,
                InflightStage::Acknowledged => MempoolTxStage::Acknowledged,
                InflightStage::Confirmed => MempoolTxStage::Confirmed,
            },
            confirmations: self.confirmations,
            non_confirmations: self.non_confirmations,
            confirmed_at: self.confirmed_at.as_ref().map(|b| {
                let bytes: [u8; 40] = b.as_slice().try_into().expect("valid chainpoint bytes");
                ChainPoint::from_bytes(bytes)
            }),
        }
    }
}

/// Watcher state for leader election
#[derive(Clone)]
struct WatcherState {
    node_id: String,
    lock_ttl: u64,
}

/// Finalized entry structure for sorted set storage
#[derive(Clone, Debug, Encode, Decode)]
struct FinalizedEntry {
    #[cbor(n(0), with = "minicbor::bytes")]
    hash: [u8; 32],
    #[n(1)]
    stage: FinalizedStage,
    #[n(2)]
    confirmations: u32,
    #[n(3)]
    non_confirmations: u32,
    #[cbor(n(4), with = "minicbor::bytes")]
    confirmed_at: Option<Vec<u8>>,
    #[n(5)]
    payload: EraCbor,
}

#[derive(Clone, Debug, Encode, Decode)]
enum FinalizedStage {
    #[n(0)]
    Finalized,
    #[n(1)]
    Dropped,
}

impl From<MempoolTxStage> for FinalizedStage {
    fn from(stage: MempoolTxStage) -> Self {
        match stage {
            MempoolTxStage::Finalized => FinalizedStage::Finalized,
            _ => FinalizedStage::Dropped,
        }
    }
}

impl From<FinalizedStage> for MempoolTxStage {
    fn from(stage: FinalizedStage) -> Self {
        match stage {
            FinalizedStage::Finalized => MempoolTxStage::Finalized,
            FinalizedStage::Dropped => MempoolTxStage::Dropped,
        }
    }
}

#[derive(Clone)]
pub struct RedisMempool {
    pool: deadpool_redis::Pool,
    key_prefix: String,
    max_finalized: usize,
    updates: broadcast::Sender<MempoolEvent>,
    watcher: Arc<RwLock<WatcherState>>,
}

pub struct RedisMempoolStream {
    inner: BroadcastStream<MempoolEvent>,
}

impl futures_core::Stream for RedisMempoolStream {
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

impl RedisMempool {
    pub fn open(config: &RedisMempoolConfig) -> Result<Self, MempoolError> {
        let cfg = deadpool_redis::Config::from_url(&config.url);
        let pool = cfg
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        let (updates, _) = broadcast::channel(16);
        let node_id = uuid::Uuid::new_v4().to_string();

        Ok(Self {
            pool,
            key_prefix: config.key_prefix.clone(),
            max_finalized: config.max_finalized,
            updates,
            watcher: Arc::new(RwLock::new(WatcherState {
                node_id,
                lock_ttl: config.watcher_lock_ttl,
            })),
        })
    }

    fn pending_key(&self) -> String {
        format!("{}:pending", self.key_prefix)
    }

    fn inflight_key(&self) -> String {
        format!("{}:inflight", self.key_prefix)
    }

    fn finalized_key(&self) -> String {
        format!("{}:finalized", self.key_prefix)
    }

    fn seq_key(&self) -> String {
        format!("{}:seq", self.key_prefix)
    }

    fn watcher_lock_key(&self) -> String {
        format!("{}:watcher:lock", self.key_prefix)
    }

    fn payload_key(&self, hash: &TxHash) -> String {
        format!("{}:payload:{}", self.key_prefix, hex::encode(hash))
    }

    fn notify(&self, tx: MempoolTx) {
        if self.updates.send(MempoolEvent { tx }).is_err() {
            debug!("no mempool update receivers");
        }
    }

    /// Try to acquire watcher lock. Returns true if acquired.
    async fn try_acquire_watcher_lock(&self) -> Result<bool, MempoolError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        let watcher = self.watcher.read().await;
        let node_id = watcher.node_id.clone();
        let lock_ttl = watcher.lock_ttl;
        drop(watcher);

        // SET key value NX EX ttl - only set if not exists, with expiry
        let result: Option<String> = redis::cmd("SET")
            .arg(&self.watcher_lock_key())
            .arg(&node_id)
            .arg("NX")
            .arg("EX")
            .arg(lock_ttl)
            .query_async(&mut conn)
            .await
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        Ok(result.is_some())
    }

    /// Renew watcher lock if we still hold it.
    async fn renew_watcher_lock(&self) -> Result<bool, MempoolError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        let watcher = self.watcher.read().await;
        let node_id = watcher.node_id.clone();
        let lock_ttl = watcher.lock_ttl;
        drop(watcher);

        // Check if we still hold the lock
        let current: Option<String> = redis::cmd("GET")
            .arg(&self.watcher_lock_key())
            .query_async(&mut conn)
            .await
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        if current.as_ref() == Some(&node_id) {
            // We still hold it, renew the expiry
            let _: () = redis::cmd("EXPIRE")
                .arg(&self.watcher_lock_key())
                .arg(lock_ttl)
                .query_async(&mut conn)
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Check if we are the current watcher (without renewing).
    async fn is_watcher(&self) -> Result<bool, MempoolError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        let watcher = self.watcher.read().await;
        let node_id = watcher.node_id.clone();
        drop(watcher);

        let current: Option<String> = redis::cmd("GET")
            .arg(&self.watcher_lock_key())
            .query_async(&mut conn)
            .await
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        Ok(current.as_ref() == Some(&node_id))
    }
}

impl MempoolStore for RedisMempool {
    type Stream = RedisMempoolStream;

    fn receive(&self, tx: MempoolTx) -> Result<(), MempoolError> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        rt.block_on(async {
            let mut conn = self
                .pool
                .get()
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            let exists_in_pending: Option<usize> = redis::cmd("LPOS")
                .arg(&self.pending_key())
                .arg(tx.hash.as_ref())
                .query_async(&mut conn)
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            if exists_in_pending.is_some() {
                return Err(MempoolError::DuplicateTx);
            }

            let exists_in_inflight: Option<Vec<u8>> = redis::cmd("HGET")
                .arg(&self.inflight_key())
                .arg(tx.hash.as_ref())
                .query_async(&mut conn)
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            if exists_in_inflight.is_some() {
                return Err(MempoolError::DuplicateTx);
            }

            let _: () = redis::cmd("RPUSH")
                .arg(&self.pending_key())
                .arg(tx.hash.as_ref())
                .query_async(&mut conn)
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            let payload_key = self.payload_key(&tx.hash);
            let payload_bytes =
                minicbor::to_vec(&tx.payload).map_err(|e| MempoolError::Internal(Box::new(e)))?;

            let _: () = redis::cmd("SET")
                .arg(&payload_key)
                .arg(payload_bytes)
                .arg("EX")
                .arg(86400)
                .query_async(&mut conn)
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            info!(tx.hash = %tx.hash, "tx received (redis)");
            self.notify(tx);

            Ok(())
        })
    }

    fn has_pending(&self) -> bool {
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(rt) => rt,
            Err(_) => return false,
        };

        rt.block_on(async {
            let mut conn = match self.pool.get().await {
                Ok(c) => c,
                Err(_) => return false,
            };

            let len: usize = match redis::cmd("LLEN")
                .arg(&self.pending_key())
                .query_async(&mut conn)
                .await
            {
                Ok(l) => l,
                Err(_) => return false,
            };

            len > 0
        })
    }

    fn peek_pending(&self, limit: usize) -> Vec<MempoolTx> {
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(rt) => rt,
            Err(_) => return vec![],
        };

        rt.block_on(async {
            let mut conn = match self.pool.get().await {
                Ok(c) => c,
                Err(_) => return vec![],
            };

            let hashes: Vec<Vec<u8>> = match redis::cmd("LRANGE")
                .arg(&self.pending_key())
                .arg(0)
                .arg(limit - 1)
                .query_async(&mut conn)
                .await
            {
                Ok(h) => h,
                Err(_) => return vec![],
            };

            let mut result = Vec::new();
            for hash_bytes in hashes {
                if hash_bytes.len() != 32 {
                    continue;
                }
                let hash = TxHash::from(<[u8; 32]>::try_from(&hash_bytes[..]).unwrap());

                let payload_key = self.payload_key(&hash);
                let payload_bytes: Option<Vec<u8>> = match redis::cmd("GET")
                    .arg(&payload_key)
                    .query_async(&mut conn)
                    .await
                {
                    Ok(Some(b)) => Some(b),
                    _ => None,
                };

                if let Some(bytes) = payload_bytes {
                    if let Ok(payload) = minicbor::decode(&bytes) {
                        result.push(MempoolTx {
                            hash,
                            payload,
                            stage: MempoolTxStage::Pending,
                            confirmations: 0,
                            non_confirmations: 0,
                            confirmed_at: None,
                            report: None,
                        });
                    }
                }
            }

            result
        })
    }

    fn mark_inflight(&self, hashes: &[TxHash]) -> Result<(), MempoolError> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        rt.block_on(async {
            let mut conn = self
                .pool
                .get()
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            let hash_set: HashSet<TxHash> = hashes.iter().copied().collect();
            let mut events = Vec::new();

            let all_hashes: Vec<Vec<u8>> = redis::cmd("LRANGE")
                .arg(&self.pending_key())
                .arg(0)
                .arg(-1)
                .query_async(&mut conn)
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            let to_keep: Vec<Vec<u8>> = all_hashes
                .into_iter()
                .filter(|h| {
                    if h.len() != 32 {
                        return true;
                    }
                    let h_arr = <[u8; 32]>::try_from(&h[..]).unwrap();
                    !hash_set.contains(&TxHash::from(h_arr))
                })
                .collect();

            let _: () = redis::cmd("DEL")
                .arg(&self.pending_key())
                .query_async(&mut conn)
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            if !to_keep.is_empty() {
                let mut cmd = redis::cmd("RPUSH");
                cmd.arg(&self.pending_key());
                for h in &to_keep {
                    cmd.arg(h);
                }
                let _: () = cmd
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;
            }

            for hash in &hash_set {
                let payload_key = self.payload_key(hash);
                let payload_bytes: Option<Vec<u8>> = redis::cmd("GET")
                    .arg(&payload_key)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;

                if let Some(bytes) = payload_bytes {
                    if let Ok(payload) = minicbor::decode(&bytes) {
                        let record = InflightRecord::new(payload);
                        let record_bytes = minicbor::to_vec(&record)
                            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

                        let _: () = redis::cmd("HSET")
                            .arg(&self.inflight_key())
                            .arg(hash.as_ref())
                            .arg(record_bytes)
                            .query_async(&mut conn)
                            .await
                            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

                        let tx = record.to_mempool_tx(*hash);
                        info!(tx.hash = %tx.hash, "tx inflight (redis)");
                        events.push(tx);
                    }
                }
            }

            for tx in events {
                self.notify(tx);
            }

            Ok(())
        })
    }

    fn mark_acknowledged(&self, hashes: &[TxHash]) -> Result<(), MempoolError> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        rt.block_on(async {
            let mut conn = self
                .pool
                .get()
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            let mut events = Vec::new();

            for hash in hashes {
                let record_bytes: Option<Vec<u8>> = redis::cmd("HGET")
                    .arg(&self.inflight_key())
                    .arg(hash.as_ref())
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;

                if let Some(bytes) = record_bytes {
                    if let Ok(mut record) = minicbor::decode::<InflightRecord>(&bytes) {
                        if record.acknowledge() {
                            let new_bytes = minicbor::to_vec(&record)
                                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

                            let _: () = redis::cmd("HSET")
                                .arg(&self.inflight_key())
                                .arg(hash.as_ref())
                                .arg(new_bytes)
                                .query_async(&mut conn)
                                .await
                                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

                            let tx = record.to_mempool_tx(*hash);
                            info!(tx.hash = %tx.hash, "tx acknowledged (redis)");
                            events.push(tx);
                        }
                    }
                }
            }

            for tx in events {
                self.notify(tx);
            }

            Ok(())
        })
    }

    fn find_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
        let rt = tokio::runtime::Handle::try_current().ok()?;

        rt.block_on(async {
            let mut conn = self.pool.get().await.ok()?;

            let record_bytes: Option<Vec<u8>> = redis::cmd("HGET")
                .arg(&self.inflight_key())
                .arg(tx_hash.as_ref())
                .query_async(&mut conn)
                .await
                .ok()?;

            record_bytes.and_then(|bytes| {
                minicbor::decode::<InflightRecord>(&bytes)
                    .ok()
                    .map(|r| r.to_mempool_tx(*tx_hash))
            })
        })
    }

    fn peek_inflight(&self, limit: usize) -> Vec<MempoolTx> {
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(rt) => rt,
            Err(_) => return vec![],
        };

        rt.block_on(async {
            let mut conn = match self.pool.get().await {
                Ok(c) => c,
                Err(_) => return vec![],
            };

            let all_records: Vec<(Vec<u8>, Vec<u8>)> = match redis::cmd("HGETALL")
                .arg(&self.inflight_key())
                .query_async(&mut conn)
                .await
            {
                Ok(r) => r,
                Err(_) => return vec![],
            };

            let mut result = Vec::new();
            for (hash_bytes, record_bytes) in all_records.into_iter().take(limit) {
                if hash_bytes.len() != 32 {
                    continue;
                }
                let hash = TxHash::from(<[u8; 32]>::try_from(&hash_bytes[..]).unwrap());

                if let Ok(record) = minicbor::decode::<InflightRecord>(&record_bytes) {
                    result.push(record.to_mempool_tx(hash));
                }
            }

            result
        })
    }

    fn confirm(
        &self,
        point: &ChainPoint,
        seen_txs: &[TxHash],
        unseen_txs: &[TxHash],
        finalize_threshold: u32,
        drop_threshold: u32,
    ) -> Result<(), MempoolError> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        rt.block_on(async {
            let is_watcher = self.is_watcher().await?;

            if !is_watcher {
                let acquired = self.try_acquire_watcher_lock().await?;
                if !acquired {
                    debug!("not watcher, skipping confirm");
                    return Ok(());
                }
                info!("acquired watcher lock");
            } else {
                let renewed = self.renew_watcher_lock().await?;
                if !renewed {
                    debug!("lost watcher lock, skipping confirm");
                    return Ok(());
                }
            }

            let mut conn = self
                .pool
                .get()
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            let seen_set: HashSet<TxHash> = seen_txs.iter().copied().collect();
            let unseen_set: HashSet<TxHash> = unseen_txs.iter().copied().collect();

            let all_inflight: Vec<(Vec<u8>, Vec<u8>)> = redis::cmd("HGETALL")
                .arg(&self.inflight_key())
                .query_async(&mut conn)
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            let mut events = Vec::new();
            let mut to_remove = Vec::new();
            let mut to_update = Vec::new();
            let mut to_finalize = Vec::new();

            for (hash_bytes, record_bytes) in all_inflight {
                if hash_bytes.len() != 32 {
                    continue;
                }
                let hash = TxHash::from(<[u8; 32]>::try_from(&hash_bytes[..]).unwrap());

                let mut record: InflightRecord = match minicbor::decode(&record_bytes) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                if seen_set.contains(&hash) {
                    record.confirm(point);

                    if record.is_finalizable(finalize_threshold) {
                        let mut tx = record.to_mempool_tx(hash);
                        to_finalize.push((hash, record.clone(), MempoolTxStage::Finalized));
                        tx.stage = MempoolTxStage::Finalized;
                        info!(tx.hash = %tx.hash, "tx finalized (redis)");
                        events.push(tx);
                        to_remove.push(hash);
                    } else {
                        to_update.push((hash, record.clone()));
                        let tx = record.to_mempool_tx(hash);
                        info!(tx.hash = %tx.hash, "tx confirmed (redis)");
                        events.push(tx);
                    }
                } else if unseen_set.contains(&hash) {
                    record.retry();

                    let _: () = redis::cmd("RPUSH")
                        .arg(&self.pending_key())
                        .arg(hash.as_ref())
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| MempoolError::Internal(Box::new(e)))?;

                    let mut tx = record.to_mempool_tx(hash);
                    tx.retry();
                    info!(tx.hash = %tx.hash, "retry tx (redis)");
                    events.push(tx);
                    to_remove.push(hash);
                } else if record.stage == InflightStage::Confirmed {
                    record.confirm(point);

                    if record.is_finalizable(finalize_threshold) {
                        let mut tx = record.to_mempool_tx(hash);
                        to_finalize.push((hash, record.clone(), MempoolTxStage::Finalized));
                        tx.stage = MempoolTxStage::Finalized;
                        info!(tx.hash = %tx.hash, "tx finalized (redis)");
                        events.push(tx);
                        to_remove.push(hash);
                    } else {
                        to_update.push((hash, record.clone()));
                        let tx = record.to_mempool_tx(hash);
                        events.push(tx);
                    }
                } else {
                    record.mark_stale();

                    if record.is_droppable(drop_threshold) {
                        let mut tx = record.to_mempool_tx(hash);
                        to_finalize.push((hash, record.clone(), MempoolTxStage::Dropped));
                        tx.stage = MempoolTxStage::Dropped;
                        info!(tx.hash = %tx.hash, "tx dropped (redis)");
                        events.push(tx);
                        to_remove.push(hash);
                    } else {
                        to_update.push((hash, record.clone()));
                    }
                }
            }

            for (hash, record) in to_update {
                let bytes =
                    minicbor::to_vec(&record).map_err(|e| MempoolError::Internal(Box::new(e)))?;
                let _: () = redis::cmd("HSET")
                    .arg(&self.inflight_key())
                    .arg(hash.as_ref())
                    .arg(bytes)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;
            }

            for hash in to_remove {
                let _: () = redis::cmd("HDEL")
                    .arg(&self.inflight_key())
                    .arg(hash.as_ref())
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;
            }

            for (hash, record, stage) in to_finalize {
                let seq: u64 = redis::cmd("INCR")
                    .arg(&self.seq_key())
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;

                let finalized_record = FinalizedEntry {
                    hash: {
                        let h: [u8; 32] = hash.as_ref().try_into().unwrap();
                        h
                    },
                    stage: stage.into(),
                    confirmations: record.confirmations,
                    non_confirmations: record.non_confirmations,
                    confirmed_at: record.confirmed_at,
                    payload: record.payload,
                };

                let bytes = minicbor::to_vec(&finalized_record)
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;

                let _: () = redis::cmd("ZADD")
                    .arg(&self.finalized_key())
                    .arg(seq as f64)
                    .arg(bytes)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;

                let payload_key = self.payload_key(&hash);
                let _: () = redis::cmd("DEL")
                    .arg(&payload_key)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;
            }

            let finalized_count: usize = redis::cmd("ZCARD")
                .arg(&self.finalized_key())
                .query_async(&mut conn)
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;

            if finalized_count > self.max_finalized {
                let to_remove_count = finalized_count - self.max_finalized;
                let _: () = redis::cmd("ZREMRANGEBYRANK")
                    .arg(&self.finalized_key())
                    .arg(0)
                    .arg(to_remove_count - 1)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;
            }

            for tx in events {
                self.notify(tx);
            }

            Ok(())
        })
    }

    fn check_status(&self, tx_hash: &TxHash) -> TxStatus {
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(rt) => rt,
            Err(_) => {
                return TxStatus {
                    stage: MempoolTxStage::Unknown,
                    confirmations: 0,
                    non_confirmations: 0,
                    confirmed_at: None,
                }
            }
        };

        rt.block_on(async {
            let mut conn = match self.pool.get().await {
                Ok(c) => c,
                Err(_) => {
                    return TxStatus {
                        stage: MempoolTxStage::Unknown,
                        confirmations: 0,
                        non_confirmations: 0,
                        confirmed_at: None,
                    }
                }
            };

            let record_bytes: Option<Vec<u8>> = redis::cmd("HGET")
                .arg(&self.inflight_key())
                .arg(tx_hash.as_ref())
                .query_async(&mut conn)
                .await
                .ok()
                .flatten();

            if let Some(bytes) = record_bytes {
                if let Ok(record) = minicbor::decode::<InflightRecord>(&bytes) {
                    return record.to_tx_status();
                }
            }

            let in_pending: Option<usize> = redis::cmd("LPOS")
                .arg(&self.pending_key())
                .arg(tx_hash.as_ref())
                .query_async(&mut conn)
                .await
                .ok()
                .flatten();

            if in_pending.is_some() {
                return TxStatus {
                    stage: MempoolTxStage::Pending,
                    confirmations: 0,
                    non_confirmations: 0,
                    confirmed_at: None,
                };
            }

            let finalized: Vec<(f64, Vec<u8>)> = match redis::cmd("ZRANGE")
                .arg(&self.finalized_key())
                .arg(0)
                .arg(-1)
                .arg("WITHSCORES")
                .query_async(&mut conn)
                .await
            {
                Ok(f) => f,
                Err(_) => {
                    return TxStatus {
                        stage: MempoolTxStage::Unknown,
                        confirmations: 0,
                        non_confirmations: 0,
                        confirmed_at: None,
                    }
                }
            };

            for (_, bytes) in finalized {
                if let Ok(entry) = minicbor::decode::<FinalizedEntry>(&bytes) {
                    if entry.hash == tx_hash.as_ref() {
                        return TxStatus {
                            stage: entry.stage.into(),
                            confirmations: entry.confirmations,
                            non_confirmations: entry.non_confirmations,
                            confirmed_at: entry.confirmed_at.as_ref().map(|b| {
                                let bytes: [u8; 40] =
                                    b.as_slice().try_into().expect("valid chainpoint");
                                ChainPoint::from_bytes(bytes)
                            }),
                        };
                    }
                }
            }

            TxStatus {
                stage: MempoolTxStage::Unknown,
                confirmations: 0,
                non_confirmations: 0,
                confirmed_at: None,
            }
        })
    }

    fn dump_finalized(&self, cursor: u64, limit: usize) -> MempoolPage {
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(rt) => rt,
            Err(_) => {
                return MempoolPage {
                    items: vec![],
                    next_cursor: None,
                }
            }
        };

        rt.block_on(async {
            let mut conn = match self.pool.get().await {
                Ok(c) => c,
                Err(_) => {
                    return MempoolPage {
                        items: vec![],
                        next_cursor: None,
                    }
                }
            };

            let entries: Vec<(f64, Vec<u8>)> = match redis::cmd("ZRANGEBYSCORE")
                .arg(&self.finalized_key())
                .arg(cursor as f64)
                .arg("+inf")
                .arg("WITHSCORES")
                .arg("LIMIT")
                .arg(0)
                .arg(limit + 1)
                .query_async(&mut conn)
                .await
            {
                Ok(e) => e,
                Err(_) => {
                    return MempoolPage {
                        items: vec![],
                        next_cursor: None,
                    }
                }
            };

            let mut items = Vec::new();
            let mut next_cursor = None;

            for (idx, (score, bytes)) in entries.into_iter().enumerate() {
                if idx >= limit {
                    next_cursor = Some(score as u64);
                    break;
                }

                if let Ok(entry) = minicbor::decode::<FinalizedEntry>(&bytes) {
                    items.push(MempoolTx {
                        hash: TxHash::from(entry.hash),
                        payload: entry.payload,
                        stage: entry.stage.into(),
                        confirmations: entry.confirmations,
                        non_confirmations: entry.non_confirmations,
                        confirmed_at: entry.confirmed_at.as_ref().map(|b| {
                            let bytes: [u8; 40] =
                                b.as_slice().try_into().expect("valid chainpoint");
                            ChainPoint::from_bytes(bytes)
                        }),
                        report: None,
                    });
                }
            }

            MempoolPage { items, next_cursor }
        })
    }

    fn subscribe(&self) -> Self::Stream {
        RedisMempoolStream {
            inner: BroadcastStream::new(self.updates.subscribe()),
        }
    }
}
