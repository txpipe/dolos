use std::collections::HashSet;
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

const DEFAULT_CACHE_SIZE_MB: usize = 32;

// ── Layer 1: redb key/value types ───────────────────────────────────────

/// Composite key for pending table: `[8-byte seq BE ++ 32-byte tx_hash]`.
#[derive(Debug)]
struct DbPendingKey([u8; 40]);

impl DbPendingKey {
    fn new(seq: u64, hash: &TxHash) -> Self {
        let mut key = [0u8; 40];
        key[..8].copy_from_slice(&seq.to_be_bytes());
        key[8..].copy_from_slice(hash.as_ref());
        Self(key)
    }

    fn seq(&self) -> u64 {
        u64::from_be_bytes(self.0[..8].try_into().unwrap())
    }

    fn hash(&self) -> TxHash {
        let mut h = [0u8; 32];
        h.copy_from_slice(&self.0[8..40]);
        TxHash::from(h)
    }
}

impl redb::Value for DbPendingKey {
    type SelfType<'a>
        = Self
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8; 40]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(40)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let inner = <[u8; 40]>::try_from(data).unwrap();
        Self(inner)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        &value.0
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("mempool_pending_key")
    }
}

impl redb::Key for DbPendingKey {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

/// 32-byte tx hash key for the inflight table.
#[derive(Debug)]
struct DbTxHash([u8; 32]);

impl DbTxHash {
    fn from_tx_hash(hash: &TxHash) -> Self {
        let mut inner = [0u8; 32];
        inner.copy_from_slice(hash.as_ref());
        Self(inner)
    }

    fn to_tx_hash(&self) -> TxHash {
        TxHash::from(self.0)
    }
}

impl redb::Value for DbTxHash {
    type SelfType<'a>
        = Self
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8; 32]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(32)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let inner = <[u8; 32]>::try_from(data).unwrap();
        Self(inner)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        &value.0
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("mempool_tx_hash")
    }
}

impl redb::Key for DbTxHash {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

/// Newtype wrapping `EraCbor` for the pending table value (foreign type).
#[derive(Debug)]
struct DbEraCbor(EraCbor);

impl redb::Value for DbEraCbor {
    type SelfType<'a>
        = Self
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self(minicbor::decode(data).unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        minicbor::to_vec(&value.0).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("mempool_era_cbor")
    }
}

// ── Inflight record ─────────────────────────────────────────────────────

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

impl redb::Value for InflightRecord {
    type SelfType<'a>
        = Self
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        minicbor::decode(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        minicbor::to_vec(value).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("mempool_inflight_record")
    }
}

/// Entry stored in finalized log table for pagination.
#[derive(Debug, Encode, Decode)]
struct FinalizedEntry {
    #[cbor(n(0), with = "minicbor::bytes")]
    hash: Vec<u8>,
    #[n(1)]
    confirmations: u32,
    #[cbor(n(2), with = "minicbor::bytes")]
    confirmed_at: Option<Vec<u8>>,
    #[n(3)]
    payload: Option<EraCbor>,
}

impl redb::Value for FinalizedEntry {
    type SelfType<'a>
        = Self
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        minicbor::decode(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        minicbor::to_vec(value).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("mempool_finalized_log_entry")
    }
}

impl FinalizedEntry {
    fn into_mempool_tx(self) -> MempoolTx {
        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(&self.hash);
        MempoolTx {
            hash: TxHash::from(hash_bytes),
            payload: self.payload.unwrap_or(EraCbor(0, vec![])),
            stage: MempoolTxStage::Finalized,
            confirmations: self.confirmations,
            non_confirmations: 0,
            confirmed_at: self
                .confirmed_at
                .map(|b| ChainPoint::from_bytes(b[..].try_into().unwrap())),
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
        self.confirmed_at = None;
    }

    fn mark_stale(&mut self) {
        self.non_confirmations += 1;
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
            non_confirmations: self.non_confirmations,
            confirmed_at: self
                .confirmed_at
                .as_ref()
                .map(|b| ChainPoint::from_bytes(b[..].try_into().unwrap())),
        }
    }

    fn into_finalized_entry(self, hash: TxHash) -> FinalizedEntry {
        FinalizedEntry {
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
            non_confirmations: self.non_confirmations,
            confirmed_at: self
                .confirmed_at
                .as_ref()
                .map(|b| ChainPoint::from_bytes(b[..].try_into().unwrap())),
            report: None,
        }
    }
}

// ── Layer 2: table wrapper structs ──────────────────────────────────────

struct PendingTable;

impl PendingTable {
    const DEF: TableDefinition<'static, DbPendingKey, DbEraCbor> = TableDefinition::new("pending");

    fn initialize(wx: &redb::WriteTransaction) -> Result<(), RedbMempoolError> {
        wx.open_table(Self::DEF)?;
        Ok(())
    }

    fn has_any(rx: &redb::ReadTransaction) -> Result<bool, RedbMempoolError> {
        let table = rx.open_table(Self::DEF)?;
        Ok(table.len()? > 0)
    }

    fn peek(rx: &redb::ReadTransaction, limit: usize) -> Result<Vec<MempoolTx>, RedbMempoolError> {
        let table = rx.open_table(Self::DEF)?;
        let iter = table.iter()?;
        let mut result = Vec::with_capacity(limit);
        for entry in iter {
            if result.len() >= limit {
                break;
            }
            let entry = entry?;
            let key = entry.0.value();
            let payload = entry.1.value().0;
            result.push(MempoolTx {
                hash: key.hash(),
                payload,
                stage: MempoolTxStage::Pending,
                confirmations: 0,
                non_confirmations: 0,
                confirmed_at: None,
                report: None,
            });
        }
        Ok(result)
    }

    fn contains(wx: &redb::WriteTransaction, tx_hash: &TxHash) -> Result<bool, RedbMempoolError> {
        let table = wx.open_table(Self::DEF)?;
        for entry in table.iter()? {
            let entry = entry?;
            if entry.0.value().hash() == *tx_hash {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn contains_hash(
        rx: &redb::ReadTransaction,
        tx_hash: &TxHash,
    ) -> Result<bool, RedbMempoolError> {
        let table = rx.open_table(Self::DEF)?;
        for entry in table.iter()? {
            let entry = entry?;
            if entry.0.value().hash() == *tx_hash {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn insert(
        wx: &redb::WriteTransaction,
        hash: &TxHash,
        payload: &EraCbor,
    ) -> Result<(), RedbMempoolError> {
        let mut table = wx.open_table(Self::DEF)?;
        let seq = match table.last()? {
            Some(entry) => entry.0.value().seq() + 1,
            None => 0,
        };
        let key = DbPendingKey::new(seq, hash);
        table.insert(key, DbEraCbor(payload.clone()))?;
        Ok(())
    }

    fn drain_by_hashes(
        wx: &redb::WriteTransaction,
        hashes: &HashSet<TxHash>,
    ) -> Result<Vec<(TxHash, EraCbor)>, RedbMempoolError> {
        let mut table = wx.open_table(Self::DEF)?;
        let extracted = table.extract_if(|key, _value| hashes.contains(&key.hash()))?;
        extracted
            .map(|entry| {
                let (key, value) = entry?;
                Ok((key.value().hash(), value.value().0))
            })
            .collect()
    }
}

struct InflightTable;

impl InflightTable {
    const DEF: TableDefinition<'static, DbTxHash, InflightRecord> =
        TableDefinition::new("inflight");

    fn initialize(wx: &redb::WriteTransaction) -> Result<(), RedbMempoolError> {
        wx.open_table(Self::DEF)?;
        Ok(())
    }

    fn get(
        rx: &redb::ReadTransaction,
        hash: &TxHash,
    ) -> Result<Option<InflightRecord>, RedbMempoolError> {
        let table = rx.open_table(Self::DEF)?;
        let key = DbTxHash::from_tx_hash(hash);
        let result = table.get(key)?.map(|e| e.value());
        Ok(result)
    }

    fn read(
        wx: &redb::WriteTransaction,
        hash: &TxHash,
    ) -> Result<Option<InflightRecord>, RedbMempoolError> {
        let table = wx.open_table(Self::DEF)?;
        let key = DbTxHash::from_tx_hash(hash);
        let result = table.get(key)?.map(|e| e.value());
        Ok(result)
    }

    fn write(
        wx: &redb::WriteTransaction,
        hash: &TxHash,
        record: &InflightRecord,
    ) -> Result<(), RedbMempoolError> {
        let mut table = wx.open_table(Self::DEF)?;
        table.insert(DbTxHash::from_tx_hash(hash), record.clone())?;
        Ok(())
    }

    fn remove(wx: &redb::WriteTransaction, hash: &TxHash) -> Result<(), RedbMempoolError> {
        let mut table = wx.open_table(Self::DEF)?;
        table.remove(DbTxHash::from_tx_hash(hash))?;
        Ok(())
    }

    fn collect_all(
        wx: &redb::WriteTransaction,
    ) -> Result<Vec<(TxHash, InflightRecord)>, RedbMempoolError> {
        let table = wx.open_table(Self::DEF)?;
        let mut entries = Vec::new();
        for entry in table.iter()? {
            let entry = entry?;
            entries.push((entry.0.value().to_tx_hash(), entry.1.value()));
        }
        Ok(entries)
    }

    fn peek(rx: &redb::ReadTransaction, limit: usize) -> Result<Vec<MempoolTx>, RedbMempoolError> {
        let table = rx.open_table(Self::DEF)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
            if result.len() >= limit {
                break;
            }
            let entry = entry?;
            let hash = entry.0.value().to_tx_hash();
            let record = entry.1.value();
            result.push(record.to_mempool_tx(hash));
        }
        Ok(result)
    }
}

struct FinalizedTable;

impl FinalizedTable {
    const DEF: TableDefinition<'static, u64, FinalizedEntry> = TableDefinition::new("finalized");

    fn initialize(wx: &redb::WriteTransaction) -> Result<(), RedbMempoolError> {
        wx.open_table(Self::DEF)?;
        Ok(())
    }

    fn append(wx: &redb::WriteTransaction, entry: FinalizedEntry) -> Result<(), RedbMempoolError> {
        let mut table = wx.open_table(Self::DEF)?;
        let seq = match table.last()? {
            Some(e) => e.0.value() + 1,
            None => 0,
        };
        table.insert(seq, entry)?;
        Ok(())
    }

    fn paginate(
        rx: &redb::ReadTransaction,
        cursor: u64,
        limit: usize,
    ) -> Result<MempoolPage, RedbMempoolError> {
        let table = rx.open_table(Self::DEF)?;
        let iter = table.range(cursor..)?;
        let mut items = Vec::with_capacity(limit);
        let mut last_seq = None;

        for entry in iter {
            if items.len() >= limit {
                break;
            }
            let entry = entry?;
            let seq = entry.0.value();
            let log_entry = entry.1.value();
            items.push(log_entry.into_mempool_tx());
            last_seq = Some(seq);
        }

        let next_cursor = if items.len() >= limit {
            last_seq.map(|s| s + 1)
        } else {
            None
        };

        Ok(MempoolPage { items, next_cursor })
    }
}

// ── Layer 3: RedbMempool orchestration ──────────────────────────────────

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
        PendingTable::initialize(&wx)?;
        InflightTable::initialize(&wx)?;
        FinalizedTable::initialize(&wx)?;
        wx.commit()?;
        Ok(())
    }

    fn notify(&self, tx: MempoolTx) {
        if self.updates.send(MempoolEvent { tx }).is_err() {
            debug!("no mempool update receivers");
        }
    }

    fn with_write_tx<F>(&self, f: F) -> Result<(), RedbMempoolError>
    where
        F: FnOnce(&redb::WriteTransaction) -> Result<Vec<MempoolTx>, RedbMempoolError>,
    {
        let wx = self.db.begin_write()?;
        let events = f(&wx)?;
        wx.commit()?;

        for tx in events {
            self.notify(tx);
        }

        Ok(())
    }

    fn receive_inner(&self, tx: MempoolTx) -> Result<(), RedbMempoolError> {
        let wx = self.db.begin_write()?;
        if PendingTable::contains(&wx, &tx.hash)? {
            return Err(MempoolError::DuplicateTx.into());
        }
        if InflightTable::read(&wx, &tx.hash)?.is_some() {
            return Err(MempoolError::DuplicateTx.into());
        }
        PendingTable::insert(&wx, &tx.hash, &tx.payload)?;
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
        PendingTable::has_any(&rx).unwrap_or(false)
    }

    fn peek_pending(&self, limit: usize) -> Vec<MempoolTx> {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return vec![],
        };
        PendingTable::peek(&rx, limit).unwrap_or_default()
    }

    fn mark_inflight(&self, hashes: &[TxHash]) -> Result<(), MempoolError> {
        let hash_set: HashSet<TxHash> = hashes.iter().copied().collect();

        self.with_write_tx(|wx| {
            let drained = PendingTable::drain_by_hashes(wx, &hash_set)?;
            let mut events = Vec::new();
            for (hash, payload) in drained {
                let record = InflightRecord::new(payload);
                let tx = record.to_mempool_tx(hash);
                InflightTable::write(wx, &hash, &record)?;
                info!(tx.hash = %tx.hash, "tx inflight (redb)");
                events.push(tx);
            }
            Ok(events)
        })?;

        Ok(())
    }

    fn mark_acknowledged(&self, hashes: &[TxHash]) -> Result<(), MempoolError> {
        self.with_write_tx(|wx| {
            let mut events = Vec::new();
            for hash in hashes {
                if let Some(mut record) = InflightTable::read(wx, hash)? {
                    if record.acknowledge() {
                        let tx = record.to_mempool_tx(*hash);
                        InflightTable::write(wx, hash, &record)?;
                        info!(tx.hash = %tx.hash, "tx acknowledged (redb)");
                        events.push(tx);
                    }
                }
            }
            Ok(events)
        })?;

        Ok(())
    }

    fn find_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
        let rx = self.db.begin_read().ok()?;
        let record = InflightTable::get(&rx, tx_hash).ok()??;
        Some(record.to_mempool_tx(*tx_hash))
    }

    fn peek_inflight(&self, limit: usize) -> Vec<MempoolTx> {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return vec![],
        };
        InflightTable::peek(&rx, limit).unwrap_or_default()
    }

    fn confirm(
        &self,
        point: &ChainPoint,
        seen_txs: &[TxHash],
        unseen_txs: &[TxHash],
    ) -> Result<(), MempoolError> {
        let point = point.clone();
        self.with_write_tx(|wx| {
            let seen_set: HashSet<TxHash> = seen_txs.iter().copied().collect();
            let unseen_set: HashSet<TxHash> = unseen_txs.iter().copied().collect();
            let entries = InflightTable::collect_all(wx)?;
            let mut events = Vec::new();

            for (tx_hash, mut record) in entries {
                if seen_set.contains(&tx_hash) {
                    record.confirm(&point);
                    InflightTable::write(wx, &tx_hash, &record)?;
                    let tx = record.to_mempool_tx(tx_hash);
                    info!(tx.hash = %tx.hash, "tx confirmed (redb)");
                    events.push(tx);
                } else if unseen_set.contains(&tx_hash) {
                    record.retry();
                    InflightTable::remove(wx, &tx_hash)?;
                    PendingTable::insert(wx, &tx_hash, &record.payload)?;
                    let mut tx = record.to_mempool_tx(tx_hash);
                    tx.retry();
                    info!(tx.hash = %tx.hash, "retry tx (redb)");
                    events.push(tx);
                } else {
                    record.mark_stale();
                    InflightTable::write(wx, &tx_hash, &record)?;
                }
            }

            Ok(events)
        })?;

        Ok(())
    }

    fn finalize(&self, threshold: u32) -> Result<(), MempoolError> {
        self.with_write_tx(|wx| {
            let entries = InflightTable::collect_all(wx)?;
            let mut events = Vec::new();

            for (hash, record) in entries {
                if record.is_finalizable(threshold) {
                    let mut tx = record.to_mempool_tx(hash);
                    let log_entry = record.into_finalized_entry(hash);
                    InflightTable::remove(wx, &hash)?;
                    FinalizedTable::append(wx, log_entry)?;
                    tx.stage = MempoolTxStage::Finalized;
                    info!(tx.hash = %tx.hash, "tx finalized (redb)");
                    events.push(tx);
                }
            }

            Ok(events)
        })?;

        Ok(())
    }

    fn check_status(&self, tx_hash: &TxHash) -> TxStatus {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => {
                return TxStatus {
                    stage: MempoolTxStage::Unknown,
                    confirmations: 0,
                    non_confirmations: 0,
                    confirmed_at: None,
                }
            }
        };

        if let Ok(Some(record)) = InflightTable::get(&rx, tx_hash) {
            return record.to_tx_status();
        }

        if let Ok(true) = PendingTable::contains_hash(&rx, tx_hash) {
            return TxStatus {
                stage: MempoolTxStage::Pending,
                confirmations: 0,
                non_confirmations: 0,
                confirmed_at: None,
            };
        }

        TxStatus {
            stage: MempoolTxStage::Unknown,
            confirmations: 0,
            non_confirmations: 0,
            confirmed_at: None,
        }
    }

    fn dump_finalized(&self, cursor: u64, limit: usize) -> MempoolPage {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => {
                return MempoolPage {
                    items: vec![],
                    next_cursor: None,
                }
            }
        };
        FinalizedTable::paginate(&rx, cursor, limit).unwrap_or(MempoolPage {
            items: vec![],
            next_cursor: None,
        })
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
        store.mark_inflight(&[hash]).unwrap();

        assert!(!store.has_pending());
        assert!(matches!(
            store.check_status(&hash).stage,
            MempoolTxStage::Propagated
        ));
        assert!(store.find_inflight(&hash).is_some());
    }

    #[test]
    fn test_mark_acknowledged() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]).unwrap();
        store.mark_acknowledged(&[hash]).unwrap();

        assert!(matches!(
            store.check_status(&hash).stage,
            MempoolTxStage::Acknowledged
        ));
        // find_inflight now returns any inflight sub-stage (Propagated, Acknowledged, Confirmed)
        assert!(store.find_inflight(&hash).is_some());
    }

    #[test]
    fn test_apply_seen() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]).unwrap();
        store.mark_acknowledged(&[hash]).unwrap();
        store.confirm(&test_point(), &[hash], &[]).unwrap();

        assert!(matches!(
            store.check_status(&hash).stage,
            MempoolTxStage::Confirmed
        ));
    }

    #[test]
    fn test_apply_unseen_rollback() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]).unwrap();
        store.mark_acknowledged(&[hash]).unwrap();
        store.confirm(&test_point(), &[hash], &[]).unwrap();
        store.confirm(&test_point_2(), &[], &[hash]).unwrap();

        assert!(matches!(
            store.check_status(&hash).stage,
            MempoolTxStage::Pending
        ));
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
        store.mark_inflight(&[hash]).unwrap();
        store.mark_acknowledged(&[hash]).unwrap();
        store.confirm(&test_point(), &[hash], &[]).unwrap();
        store.confirm(&test_point_2(), &[hash], &[]).unwrap();
        store.finalize(2).unwrap();

        assert!(matches!(
            store.check_status(&hash).stage,
            MempoolTxStage::Unknown
        ));
    }

    #[test]
    fn test_finalize_below_threshold() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;

        store.receive(tx).unwrap();
        store.mark_inflight(&[hash]).unwrap();
        store.mark_acknowledged(&[hash]).unwrap();
        store.confirm(&test_point(), &[hash], &[]).unwrap();
        store.finalize(2).unwrap();

        assert!(matches!(
            store.check_status(&hash).stage,
            MempoolTxStage::Confirmed
        ));
    }

    #[test]
    fn test_stage_unknown() {
        let store = test_store();
        let hash = test_hash(99);

        assert!(matches!(
            store.check_status(&hash).stage,
            MempoolTxStage::Unknown
        ));
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
        store.mark_inflight(&[hash]).unwrap();
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Propagated));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());

        // Acknowledged
        store.mark_acknowledged(&[hash]).unwrap();
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Acknowledged));
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());

        // Confirmed (1st confirmation)
        store.confirm(&point, &[hash], &[]).unwrap();
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Confirmed));
        assert_eq!(status.confirmations, 1);
        assert!(status.confirmed_at.is_some());
        assert_eq!(status.confirmed_at.as_ref().unwrap().slot(), point.slot());

        // Confirmed (2nd confirmation — confirmed_at stays the same)
        store.confirm(&test_point_2(), &[hash], &[]).unwrap();
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Confirmed));
        assert_eq!(status.confirmations, 2);
        assert_eq!(status.confirmed_at.as_ref().unwrap().slot(), point.slot());

        // Finalized — no longer tracked by check_status, returns Unknown
        store.finalize(2).unwrap();
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
            store.mark_inflight(&[hash]).unwrap();
            store.mark_acknowledged(&[hash]).unwrap();
            store.confirm(&point, &[hash], &[]).unwrap();
            store.confirm(&test_point_2(), &[hash], &[]).unwrap();
        }
        store.finalize(2).unwrap();

        // Read all
        let page = store.dump_finalized(0, 50);
        assert_eq!(page.items.len(), 3);
        assert!(page.next_cursor.is_none());
        for entry in &page.items {
            assert_eq!(entry.confirmations, 2);
            assert!(entry.confirmed_at.is_some());
            assert!(
                !entry.payload.1.is_empty(),
                "finalized entry should include payload"
            );
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

        store.mark_inflight(&[h1, h2, h3]).unwrap();

        // All three start as Propagated
        let listing = store.peek_inflight(usize::MAX);
        assert_eq!(listing.len(), 3);
        assert!(listing
            .iter()
            .all(|tx| tx.stage == MempoolTxStage::Propagated));

        // Acknowledge h2
        store.mark_acknowledged(&[h2]).unwrap();
        let listing = store.peek_inflight(usize::MAX);
        assert_eq!(listing.len(), 3);
        let h2_stage = listing
            .iter()
            .find(|tx| tx.hash == h2)
            .unwrap()
            .stage
            .clone();
        assert_eq!(h2_stage, MempoolTxStage::Acknowledged);

        // Confirm h2
        store.confirm(&test_point(), &[h2], &[]).unwrap();
        let listing = store.peek_inflight(usize::MAX);
        assert_eq!(listing.len(), 3);
        let h2_stage = listing
            .iter()
            .find(|tx| tx.hash == h2)
            .unwrap()
            .stage
            .clone();
        assert_eq!(h2_stage, MempoolTxStage::Confirmed);

        // Finalize h2 — should drop from listing
        store.confirm(&test_point_2(), &[h2], &[]).unwrap();
        store.finalize(2).unwrap();
        let listing = store.peek_inflight(usize::MAX);
        assert_eq!(listing.len(), 2);
        assert!(!listing.iter().any(|tx| tx.hash == h2));
    }
}
