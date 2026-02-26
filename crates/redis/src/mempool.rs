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
        let mut cfg = deadpool_redis::Config::from_url(&config.url);
        cfg.pool = Some(deadpool_redis::PoolConfig::new(config.pool_size));
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

    /// Atomically acquire or renew the watcher lock using a Lua script.
    ///
    /// Returns `true` if this node now holds the lock (either freshly acquired
    /// or renewed), `false` if another node holds it.
    ///
    /// This replaces the previous three-step `is_watcher` → `try_acquire` /
    /// `renew` flow with a single atomic operation, eliminating the race
    /// window where the lock could expire between the read and the
    /// write.
    async fn acquire_or_renew_watcher_lock(&self) -> Result<bool, MempoolError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        let watcher = self.watcher.read().await;
        let node_id = watcher.node_id.clone();
        let lock_ttl = watcher.lock_ttl;
        drop(watcher);

        // Lua script: atomically acquire-or-renew.
        //   KEYS[1] = lock key
        //   ARGV[1] = our node_id
        //   ARGV[2] = TTL in seconds
        //
        // Returns 1 if we hold the lock (acquired or renewed), 0 otherwise.
        let script = redis::Script::new(
            r#"
            local current = redis.call('GET', KEYS[1])
            if current == false then
                redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
                return 1
            elseif current == ARGV[1] then
                redis.call('EXPIRE', KEYS[1], ARGV[2])
                return 1
            else
                return 0
            end
            "#,
        );

        let result: i64 = script
            .key(&self.watcher_lock_key())
            .arg(&node_id)
            .arg(lock_ttl)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        Ok(result == 1)
    }
}

impl MempoolStore for RedisMempool {
    type Stream = RedisMempoolStream;

    async fn receive(&self, tx: MempoolTx) -> Result<(), MempoolError> {
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
    }

    async fn has_pending(&self) -> bool {
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
    }

    async fn peek_pending(&self, limit: usize) -> Vec<MempoolTx> {
        if limit == 0 {
            return vec![];
        }

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
    }

    async fn mark_inflight(&self, hashes: &[TxHash]) -> Result<(), MempoolError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        let hash_set: HashSet<TxHash> = hashes.iter().copied().collect();
        let mut events = Vec::new();

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

                    // Write inflight BEFORE removing from pending.
                    // If we crash after HSET but before LREM, the tx is in
                    // both pending and inflight — safe because receive()
                    // dedup catches it, and the next mark_inflight() will
                    // LREM it.
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

            let _: () = redis::cmd("LREM")
                .arg(&self.pending_key())
                .arg(1)
                .arg(hash.as_ref())
                .query_async(&mut conn)
                .await
                .map_err(|e| MempoolError::Internal(Box::new(e)))?;
        }

        for tx in events {
            self.notify(tx);
        }

        Ok(())
    }

    async fn mark_acknowledged(&self, hashes: &[TxHash]) -> Result<(), MempoolError> {
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
    }

    async fn find_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
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
    }

    async fn peek_inflight(&self, limit: usize) -> Vec<MempoolTx> {
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
    }

    async fn confirm(
        &self,
        point: &ChainPoint,
        seen_txs: &[TxHash],
        unseen_txs: &[TxHash],
        finalize_threshold: u32,
        drop_threshold: u32,
    ) -> Result<(), MempoolError> {
        let is_watcher = self.acquire_or_renew_watcher_lock().await?;

        if !is_watcher {
            debug!("not watcher, skipping confirm");
            return Ok(());
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

                // Remove from inflight FIRST to prevent a concurrent
                // mark_inflight() from having its fresh record deleted
                // by the deferred HDEL pass.
                let _: () = redis::cmd("HDEL")
                    .arg(&self.inflight_key())
                    .arg(hash.as_ref())
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| MempoolError::Internal(Box::new(e)))?;

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
                // Do NOT add to to_remove — already deleted above
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
    }

    async fn check_status(&self, tx_hash: &TxHash) -> TxStatus {
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
    }

    async fn dump_finalized(&self, cursor: u64, limit: usize) -> MempoolPage {
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
                        let bytes: [u8; 40] = b.as_slice().try_into().expect("valid chainpoint");
                        ChainPoint::from_bytes(bytes)
                    }),
                    report: None,
                });
            }
        }

        MempoolPage { items, next_cursor }
    }

    fn subscribe(&self) -> Self::Stream {
        RedisMempoolStream {
            inner: BroadcastStream::new(self.updates.subscribe()),
        }
    }
}
