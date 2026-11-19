use std::collections::HashSet;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pallas::codec::minicbor::{self, Decode, Encode};
use redb::{ReadableTable, ReadableTableMetadata, TableDefinition};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, warn};

use crate::handle::{is_uninitialised, needs_repair, Handle};
use crate::layout::{check_layout, is_corrupt};

use dolos_core::{
    config::RedbMempoolConfig, ChainPoint, EraCbor, MempoolCount, MempoolError, MempoolEvent,
    MempoolPage, MempoolStore, MempoolTx, MempoolTxStage, TxHash, TxStatus,
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

/// The layout of the mempool file. A file recording another, or none over
/// transactions, is recreated empty by a writable open: the mempool is
/// transient.
pub const MEMPOOL_VERSION: u32 = 1;

/// The file's metadata: its [`MEMPOOL_VERSION`] under [`VERSION_KEY`].
const META: TableDefinition<&str, u32> = TableDefinition::new("meta");

const VERSION_KEY: &str = "version";

/// What a mempool file records in place of this release's
/// [`MEMPOOL_VERSION`].
#[derive(Debug)]
pub enum FoundVersion {
    Other(u32),
    /// No version, over transactions a release before the marker kept.
    Unmarked,
    /// Contents that fail to read or decode: damage, a cut-short file, or a
    /// layout of another release.
    Unreadable(String),
}

impl std::fmt::Display for FoundVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Other(version) => write!(f, "version {version}"),
            Self::Unmarked => f.write_str("no version over transactions"),
            Self::Unreadable(error) => write!(f, "unreadable ({error})"),
        }
    }
}

/// A mempool file this release does not read, refused by a read-only open,
/// which leaves it as it is.
#[derive(Debug, Error)]
#[error("the mempool file {} records {found}, this release writes version {MEMPOOL_VERSION}", .path.display())]
pub struct IncompatibleMempool {
    pub path: PathBuf,
    pub found: FoundVersion,
}

/// The [`IncompatibleMempool`] `error` carries, if it is one.
pub fn incompatible(error: &MempoolError) -> Option<&IncompatibleMempool> {
    match error {
        MempoolError::Internal(error) => error.downcast_ref(),
        _ => None,
    }
}

/// Whether the file at `path` is empty, as a crash during its creation
/// leaves it: a writable open initialises it, a read-only one cannot.
fn is_empty(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|metadata| metadata.len() == 0)
}

fn internal(error: impl std::error::Error + Send + Sync + 'static) -> MempoolError {
    MempoolError::Internal(Box::new(error))
}

/// `error` as what the file records, when it is about the file's contents;
/// otherwise, a lock, permission or I/O error, unchanged.
fn unreadable(error: impl Into<redb::Error>) -> Result<FoundVersion, redb::Error> {
    let error = error.into();

    if is_corrupt(&error) {
        Ok(FoundVersion::Unreadable(error.to_string()))
    } else {
        Err(error)
    }
}

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

/// The bytes of a `T`, read without decoding them, so that a value of
/// another layout is found rather than panicking. redb admits it for a table
/// of `T`: it records the same type name and width.
struct Raw<T>(PhantomData<T>);

impl<T> std::fmt::Debug for Raw<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Raw")
    }
}

impl<T: redb::Value + 'static> redb::Value for Raw<T> {
    type SelfType<'a>
        = &'a [u8]
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        T::fixed_width()
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        data
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value
    }

    fn type_name() -> redb::TypeName {
        T::type_name()
    }
}

impl<T: redb::Key + 'static> redb::Key for Raw<T> {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        T::compare(data1, data2)
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
    #[n(4)]
    dropped: Option<bool>,
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
        let stage = if self.dropped.unwrap_or(false) {
            MempoolTxStage::Dropped
        } else {
            MempoolTxStage::Finalized
        };
        MempoolTx {
            hash: TxHash::from(hash_bytes),
            payload: self.payload.unwrap_or(EraCbor(0, vec![])),
            stage,
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

    fn is_droppable(&self, threshold: u32) -> bool {
        self.non_confirmations >= threshold
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
            dropped: Some(false),
        }
    }

    fn into_dropped_entry(self, hash: TxHash) -> FinalizedEntry {
        FinalizedEntry {
            hash: hash.to_vec(),
            confirmations: self.confirmations,
            confirmed_at: self.confirmed_at,
            payload: Some(self.payload),
            dropped: Some(true),
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
    const NAME: &str = "pending";
    const DEF: TableDefinition<'static, DbPendingKey, DbEraCbor> = TableDefinition::new(Self::NAME);

    fn initialize(wx: &redb::WriteTransaction) -> Result<(), RedbMempoolError> {
        wx.open_table(Self::DEF)?;
        Ok(())
    }

    fn has_any(rx: &redb::ReadTransaction) -> Result<bool, RedbMempoolError> {
        let table = rx.open_table(Self::DEF)?;
        Ok(table.len()? > 0)
    }

    fn len(rx: &redb::ReadTransaction) -> Result<u64, RedbMempoolError> {
        Ok(rx.open_table(Self::DEF)?.len()?)
    }

    fn peek(rx: &redb::ReadTransaction) -> Result<Vec<MempoolTx>, RedbMempoolError> {
        let table = rx.open_table(Self::DEF)?;
        let iter = table.iter()?;
        let mut result = Vec::new();
        for entry in iter {
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
    const NAME: &str = "inflight";
    const DEF: TableDefinition<'static, DbTxHash, InflightRecord> =
        TableDefinition::new(Self::NAME);

    fn initialize(wx: &redb::WriteTransaction) -> Result<(), RedbMempoolError> {
        wx.open_table(Self::DEF)?;
        Ok(())
    }

    fn len(rx: &redb::ReadTransaction) -> Result<u64, RedbMempoolError> {
        Ok(rx.open_table(Self::DEF)?.len()?)
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

    fn peek(rx: &redb::ReadTransaction) -> Result<Vec<MempoolTx>, RedbMempoolError> {
        let table = rx.open_table(Self::DEF)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
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
    const NAME: &str = "finalized";
    const DEF: TableDefinition<'static, u64, FinalizedEntry> = TableDefinition::new(Self::NAME);

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
    db: Arc<Handle>,
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
    /// Opens the mempool file at `path`, creating it if it is not there. A
    /// file this release does not read (see [`FoundVersion`]) is deleted and
    /// recreated empty, with one warning, dropping its transactions. A lock,
    /// permission or I/O error is returned unchanged.
    pub fn open(path: impl AsRef<Path>, config: &RedbMempoolConfig) -> Result<Self, MempoolError> {
        let path = path.as_ref();

        let found = match Self::create(path, config)? {
            Ok(store) => return Ok(store),
            Err(found) => found,
        };

        warn!(
            path = %path.display(),
            %found,
            expected = MEMPOOL_VERSION,
            "mempool file incompatible, recreating it empty"
        );

        std::fs::remove_file(path).map_err(internal)?;

        match Self::create(path, config)? {
            Ok(store) => Ok(store),
            Err(found) => Err(internal(IncompatibleMempool {
                path: path.to_path_buf(),
                found,
            })),
        }
    }

    /// The mempool over the file at `path`, or what it records when that is
    /// not this release's layout.
    fn create(
        path: &Path,
        config: &RedbMempoolConfig,
    ) -> Result<Result<Self, FoundVersion>, MempoolError> {
        if let Err(error) = check_layout(path) {
            return unreadable(error).map(Err).map_err(internal);
        }

        let db = redb::Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "mempool db is repairing")
            })
            .set_cache_size(1024 * 1024 * config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path);

        let db = match db {
            Ok(db) => Handle::Writable(db),
            Err(error) => return unreadable(error).map(Err).map_err(internal),
        };

        if let Err(found) = check_version(&db).map_err(internal)? {
            return Ok(Err(found));
        }

        Self::from_db(db).map(Ok)
    }

    /// Open an existing mempool read-only (see [`Handle`]), without creating
    /// its tables. A file this release does not read is refused as
    /// [`IncompatibleMempool`] and left as it is; one a crash left
    /// unrepaired, or empty, keeps redb's error.
    pub fn open_read_only(
        path: impl AsRef<Path>,
        config: &RedbMempoolConfig,
    ) -> Result<Self, MempoolError> {
        let path = path.as_ref();

        let refused = |found| {
            internal(IncompatibleMempool {
                path: path.to_path_buf(),
                found,
            })
        };

        if let Err(error) = check_layout(path) {
            return Err(refused(unreadable(error).map_err(internal)?));
        }

        let db = match Handle::open_read_only(
            path,
            1024 * 1024 * config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB),
        ) {
            Ok(db) => db,
            Err(error) if needs_repair(&error) || is_uninitialised(&error) && is_empty(path) => {
                return Err(internal(error));
            }
            Err(error) => return Err(refused(unreadable(error).map_err(internal)?)),
        };

        check_version(&db).map_err(internal)?.map_err(refused)?;

        let (updates, _) = broadcast::channel(16);

        Ok(Self {
            db: Arc::new(db),
            updates,
        })
    }

    pub fn in_memory() -> Result<Self, MempoolError> {
        let db = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| MempoolError::Internal(Box::new(e)))?;

        Self::from_db(Handle::Writable(db))
    }

    fn from_db(db: Handle) -> Result<Self, MempoolError> {
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
        wx.open_table(META)?.insert(VERSION_KEY, MEMPOOL_VERSION)?;
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

/// Whether the file records this release's [`MEMPOOL_VERSION`], or nothing
/// over no transactions, with every live record and the ends of the finalized
/// log decoding. The log is not read whole: it grows with every finalized
/// transaction, and a layout change shows in any of its records.
fn check_version(db: &Handle) -> Result<Result<(), FoundVersion>, redb::Error> {
    let found = match check_tables(db) {
        Ok(found) => found,
        Err(error) => Err(unreadable(error)?),
    };

    Ok(found)
}

fn check_tables(db: &Handle) -> Result<Result<(), FoundVersion>, redb::Error> {
    let rx = db.begin_read()?;

    let recorded = match open_existing(&rx, META)? {
        Some(meta) => meta.get(VERSION_KEY)?.map(|version| version.value()),
        None => None,
    };

    if let Some(other) = recorded.filter(|version| *version != MEMPOOL_VERSION) {
        return Ok(Err(FoundVersion::Other(other)));
    }

    let mut held = 0;

    if let Some(pending) = open_existing(&rx, raw::<DbPendingKey, DbEraCbor>(PendingTable::NAME))? {
        held += pending.len()?;

        for entry in pending.iter()? {
            let (_, payload) = entry?;
            decodes::<EraCbor>("pending", payload.value())?;
        }
    }

    if let Some(inflight) =
        open_existing(&rx, raw::<DbTxHash, InflightRecord>(InflightTable::NAME))?
    {
        held += inflight.len()?;

        for entry in inflight.iter()? {
            let (_, record) = entry?;
            let record = decodes::<InflightRecord>("inflight", record.value())?;
            point_decodes("inflight", record.confirmed_at.as_deref())?;
        }
    }

    if let Some(finalized) = open_existing(&rx, raw::<u64, FinalizedEntry>(FinalizedTable::NAME))? {
        held += finalized.len()?;

        for entry in [finalized.first()?, finalized.last()?]
            .into_iter()
            .flatten()
        {
            let entry = decodes::<FinalizedEntry>("finalized", entry.1.value())?;

            if entry.hash.len() != 32 {
                return Err(redb::Error::Corrupted(format!(
                    "a finalized hash is {} bytes",
                    entry.hash.len()
                )));
            }

            point_decodes("finalized", entry.confirmed_at.as_deref())?;
        }
    }

    match recorded {
        None if held > 0 => Ok(Err(FoundVersion::Unmarked)),
        _ => Ok(Ok(())),
    }
}

/// The table `name` of `K` to `V`, over the raw bytes of its keys and values.
fn raw<K: redb::Key + 'static, V: redb::Value + 'static>(
    name: &'static str,
) -> TableDefinition<'static, Raw<K>, Raw<V>> {
    TableDefinition::new(name)
}

/// A table for reading, or none if no write ever created it.
fn open_existing<K: redb::Key + 'static, V: redb::Value + 'static>(
    rx: &redb::ReadTransaction,
    def: TableDefinition<'_, K, V>,
) -> Result<Option<redb::ReadOnlyTable<K, V>>, redb::Error> {
    match rx.open_table(def) {
        Ok(table) => Ok(Some(table)),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn decodes<T: for<'b> Decode<'b, ()>>(table: &str, bytes: &[u8]) -> Result<T, redb::Error> {
    minicbor::decode(bytes).map_err(|error| {
        redb::Error::Corrupted(format!("a {table} record fails to decode: {error}"))
    })
}

fn point_decodes(table: &str, point: Option<&[u8]>) -> Result<(), redb::Error> {
    match point {
        Some(point) if point.len() != 40 => Err(redb::Error::Corrupted(format!(
            "a {table} confirmation point is {} bytes",
            point.len()
        ))),
        _ => Ok(()),
    }
}

impl MempoolStore for RedbMempool {
    type Stream = RedbMempoolStream;

    fn receive(&self, tx: MempoolTx) -> Result<(), MempoolError> {
        debug!(tx.hash = %tx.hash, "tx received");
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

    fn peek_pending(&self) -> Vec<MempoolTx> {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return vec![],
        };
        PendingTable::peek(&rx).unwrap_or_default()
    }

    fn count(&self) -> MempoolCount {
        let Ok(rx) = self.db.begin_read() else {
            return MempoolCount::default();
        };

        let len = |len: Result<u64, RedbMempoolError>| len.unwrap_or(0) as usize;

        MempoolCount {
            pending: len(PendingTable::len(&rx)),
            inflight: len(InflightTable::len(&rx)),
        }
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
                debug!(tx.hash = %tx.hash, "tx inflight");
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
                        debug!(tx.hash = %tx.hash, "tx acknowledged");
                        events.push(tx);
                    }
                }
            }
            Ok(events)
        })?;

        Ok(())
    }

    fn requeue_propagated(&self, except: &[TxHash]) -> Result<(), MempoolError> {
        self.with_write_tx(|wx| {
            let mut events = Vec::new();
            for (hash, record) in InflightTable::collect_all(wx)? {
                if record.stage == InflightStage::Propagated && !except.contains(&hash) {
                    InflightTable::remove(wx, &hash)?;
                    PendingTable::insert(wx, &hash, &record.payload)?;
                    let mut tx = record.to_mempool_tx(hash);
                    tx.retry();
                    debug!(tx.hash = %tx.hash, "tx requeued");
                    events.push(tx);
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

    fn peek_inflight(&self) -> Vec<MempoolTx> {
        let rx = match self.db.begin_read() {
            Ok(rx) => rx,
            Err(_) => return vec![],
        };
        InflightTable::peek(&rx).unwrap_or_default()
    }

    fn confirm(
        &self,
        point: &ChainPoint,
        seen_txs: &[TxHash],
        unseen_txs: &[TxHash],
        finalize_threshold: u32,
        drop_threshold: u32,
    ) -> Result<(), MempoolError> {
        let point = point.clone();
        self.with_write_tx(|wx| {
            let seen_set: HashSet<TxHash> = seen_txs.iter().copied().collect();
            let unseen_set: HashSet<TxHash> = unseen_txs.iter().copied().collect();

            // A re-queued tx can land on chain before it is offered again.
            for (hash, payload) in PendingTable::drain_by_hashes(wx, &seen_set)? {
                InflightTable::write(wx, &hash, &InflightRecord::new(payload))?;
            }

            let entries = InflightTable::collect_all(wx)?;
            let mut events = Vec::new();

            for (tx_hash, mut record) in entries {
                if seen_set.contains(&tx_hash) {
                    record.confirm(&point);
                    // Check if this tx is now finalizable
                    if record.is_finalizable(finalize_threshold) {
                        let mut tx = record.to_mempool_tx(tx_hash);
                        let log_entry = record.into_finalized_entry(tx_hash);
                        InflightTable::remove(wx, &tx_hash)?;
                        FinalizedTable::append(wx, log_entry)?;
                        tx.stage = MempoolTxStage::Finalized;
                        debug!(tx.hash = %tx.hash, "tx finalized");
                        events.push(tx);
                    } else {
                        InflightTable::write(wx, &tx_hash, &record)?;
                        let tx = record.to_mempool_tx(tx_hash);
                        debug!(tx.hash = %tx.hash, "tx confirmed");
                        events.push(tx);
                    }
                } else if unseen_set.contains(&tx_hash) {
                    record.retry();
                    InflightTable::remove(wx, &tx_hash)?;
                    PendingTable::insert(wx, &tx_hash, &record.payload)?;
                    let mut tx = record.to_mempool_tx(tx_hash);
                    tx.retry();
                    debug!(tx.hash = %tx.hash, "retry tx");
                    events.push(tx);
                } else if record.stage == InflightStage::Confirmed {
                    // Already confirmed on-chain; treat subsequent blocks as
                    // additional confirmations (increasing depth).
                    record.confirm(&point);
                    if record.is_finalizable(finalize_threshold) {
                        let mut tx = record.to_mempool_tx(tx_hash);
                        let log_entry = record.into_finalized_entry(tx_hash);
                        InflightTable::remove(wx, &tx_hash)?;
                        FinalizedTable::append(wx, log_entry)?;
                        tx.stage = MempoolTxStage::Finalized;
                        debug!(tx.hash = %tx.hash, "tx finalized");
                        events.push(tx);
                    } else {
                        InflightTable::write(wx, &tx_hash, &record)?;
                        let tx = record.to_mempool_tx(tx_hash);
                        events.push(tx);
                    }
                } else {
                    record.mark_stale();
                    // Check if this tx should be dropped
                    if record.is_droppable(drop_threshold) {
                        let mut tx = record.to_mempool_tx(tx_hash);
                        let log_entry = record.into_dropped_entry(tx_hash);
                        InflightTable::remove(wx, &tx_hash)?;
                        FinalizedTable::append(wx, log_entry)?;
                        tx.stage = MempoolTxStage::Dropped;
                        debug!(tx.hash = %tx.hash, "tx dropped");
                        events.push(tx);
                    } else {
                        InflightTable::write(wx, &tx_hash, &record)?;
                    }
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
        assert!(store.peek_pending().is_empty());
    }

    #[test]
    fn test_receive_and_has_pending() {
        let store = test_store();
        let tx = test_tx(1);
        let hash = tx.hash;
        let payload = tx.payload.clone();

        store.receive(tx).unwrap();

        assert!(store.has_pending());

        let peeked = store.peek_pending();
        assert_eq!(peeked.len(), 1);
        assert_eq!(peeked[0].hash, hash);
        assert_eq!(peeked[0].payload, payload);
    }

    #[test]
    fn test_receive_duplicate_hash() {
        let store = test_store();
        let tx = test_tx(1);
        store.receive(tx.clone()).unwrap();

        let err = store.receive(tx).unwrap_err();
        assert!(matches!(err, MempoolError::DuplicateTx));
        assert_eq!(store.peek_pending().len(), 1);
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
        // find_inflight now returns any inflight sub-stage (Propagated, Acknowledged,
        // Confirmed)
        assert!(store.find_inflight(&hash).is_some());
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
        store
            .confirm(&point, &[hash], &[], u32::MAX, u32::MAX)
            .unwrap();
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Confirmed));
        assert_eq!(status.confirmations, 1);
        assert!(status.confirmed_at.is_some());
        assert_eq!(status.confirmed_at.as_ref().unwrap().slot(), point.slot());

        // Confirmed (2nd confirmation — confirmed_at stays the same)
        store
            .confirm(&test_point_2(), &[hash], &[], u32::MAX, u32::MAX)
            .unwrap();
        let status = store.check_status(&hash);
        assert!(matches!(status.stage, MempoolTxStage::Confirmed));
        assert_eq!(status.confirmations, 2);
        assert_eq!(status.confirmed_at.as_ref().unwrap().slot(), point.slot());

        // Finalized — trigger via confirm with finalize_threshold=2
        store
            .confirm(&test_point(), &[hash], &[], 2, u32::MAX)
            .unwrap();
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
            store
                .confirm(&point, &[hash], &[], u32::MAX, u32::MAX)
                .unwrap();
            // Second confirm with finalize_threshold=2 triggers finalization
            store
                .confirm(&test_point_2(), &[hash], &[], 2, u32::MAX)
                .unwrap();
        }

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
        let listing = store.peek_inflight();
        assert_eq!(listing.len(), 3);
        assert!(listing
            .iter()
            .all(|tx| tx.stage == MempoolTxStage::Propagated));

        // Acknowledge h2
        store.mark_acknowledged(&[h2]).unwrap();
        let listing = store.peek_inflight();
        assert_eq!(listing.len(), 3);
        let h2_stage = listing
            .iter()
            .find(|tx| tx.hash == h2)
            .unwrap()
            .stage
            .clone();
        assert_eq!(h2_stage, MempoolTxStage::Acknowledged);

        // Confirm h2
        store
            .confirm(&test_point(), &[h2], &[], u32::MAX, u32::MAX)
            .unwrap();
        let listing = store.peek_inflight();
        assert_eq!(listing.len(), 3);
        let h2_stage = listing
            .iter()
            .find(|tx| tx.hash == h2)
            .unwrap()
            .stage
            .clone();
        assert_eq!(h2_stage, MempoolTxStage::Confirmed);

        // Finalize h2 — second confirm with finalize_threshold=2
        store
            .confirm(&test_point_2(), &[h2], &[], 2, u32::MAX)
            .unwrap();
        let listing = store.peek_inflight();
        assert_eq!(listing.len(), 2);
        assert!(!listing.iter().any(|tx| tx.hash == h2));
    }

    mod versions {
        use super::*;
        use redb::ReadableDatabase as _;

        const CONFIG: RedbMempoolConfig = RedbMempoolConfig { cache: None };

        fn file() -> (tempfile::TempDir, PathBuf) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("mempool.redb");
            (dir, path)
        }

        /// A file holding one pending transaction, as this release writes it.
        fn holding_a_tx(path: &Path) {
            let store = RedbMempool::open(path, &CONFIG).unwrap();
            store.receive(test_tx(1)).unwrap();
        }

        /// Rewrites the file's marker, removing it on `None`.
        fn mark(path: &Path, version: Option<u32>) {
            let db = redb::Database::create(path).unwrap();
            let wx = db.begin_write().unwrap();

            match version {
                Some(version) => {
                    wx.open_table(META)
                        .unwrap()
                        .insert(VERSION_KEY, version)
                        .unwrap();
                }
                None => {
                    wx.delete_table(META).unwrap();
                }
            }

            wx.commit().unwrap();
        }

        fn recorded(path: &Path) -> Option<u32> {
            let db = redb::Database::create(path).unwrap();
            let rx = db.begin_read().unwrap();
            let meta = rx.open_table(META).unwrap();
            let version = meta.get(VERSION_KEY).unwrap().map(|v| v.value());
            version
        }

        fn pending(path: &Path) -> usize {
            RedbMempool::open(path, &CONFIG)
                .unwrap()
                .peek_pending()
                .len()
        }

        #[test]
        fn a_new_file_is_stamped() {
            let (_dir, path) = file();
            drop(RedbMempool::open(&path, &CONFIG).unwrap());

            assert_eq!(recorded(&path), Some(MEMPOOL_VERSION));
        }

        #[test]
        fn a_file_of_this_version_keeps_its_transactions() {
            let (_dir, path) = file();
            holding_a_tx(&path);

            assert_eq!(pending(&path), 1);
            assert_eq!(recorded(&path), Some(MEMPOOL_VERSION));
        }

        #[test]
        fn a_file_of_another_version_is_recreated_empty() {
            for version in [0, MEMPOOL_VERSION + 1] {
                let (_dir, path) = file();
                holding_a_tx(&path);
                mark(&path, Some(version));

                assert_eq!(pending(&path), 0, "version {version}");
                assert_eq!(recorded(&path), Some(MEMPOOL_VERSION));
            }
        }

        #[test]
        fn a_file_without_a_marker_over_transactions_is_recreated_empty() {
            let (_dir, path) = file();
            holding_a_tx(&path);
            mark(&path, None);

            assert_eq!(pending(&path), 0);
            assert_eq!(recorded(&path), Some(MEMPOOL_VERSION));
        }

        /// As a release before the marker left an empty mempool.
        #[test]
        fn a_file_without_a_marker_or_transactions_is_stamped() {
            let (_dir, path) = file();
            drop(RedbMempool::open(&path, &CONFIG).unwrap());
            mark(&path, None);

            let store = RedbMempool::open(&path, &CONFIG).unwrap();
            store.receive(test_tx(2)).unwrap();
            drop(store);

            assert_eq!(pending(&path), 1);
            assert_eq!(recorded(&path), Some(MEMPOOL_VERSION));
        }

        #[test]
        fn a_record_that_fails_to_decode_is_recreated_empty() {
            let (_dir, path) = file();
            holding_a_tx(&path);

            let db = redb::Database::create(&path).unwrap();
            let wx = db.begin_write().unwrap();
            wx.open_table(raw::<DbTxHash, InflightRecord>(InflightTable::NAME))
                .unwrap()
                .insert([7; 32].as_slice(), [0xff, 0x00].as_slice())
                .unwrap();
            wx.commit().unwrap();
            drop(db);

            let store = RedbMempool::open(&path, &CONFIG).unwrap();
            assert!(store.peek_pending().is_empty());
            assert!(store.peek_inflight().is_empty());
        }

        #[test]
        fn a_pending_record_that_fails_to_decode_is_recreated_empty() {
            let (_dir, path) = file();
            holding_a_tx(&path);

            let db = redb::Database::create(&path).unwrap();
            let wx = db.begin_write().unwrap();
            wx.open_table(raw::<DbPendingKey, DbEraCbor>(PendingTable::NAME))
                .unwrap()
                .insert([7; 40].as_slice(), [0xff, 0x00].as_slice())
                .unwrap();
            wx.commit().unwrap();
            drop(db);

            let Err(error) = RedbMempool::open_read_only(&path, &CONFIG) else {
                panic!("a mempool with an undecodable pending record opened read-only");
            };
            assert!(incompatible(&error).is_some(), "{error}");

            assert_eq!(pending(&path), 0);
            assert_eq!(recorded(&path), Some(MEMPOOL_VERSION));
        }

        /// redb panics on a file shorter than the layout its header records,
        /// or not a whole number of pages, before it reports any error.
        #[test]
        fn a_file_cut_short_is_recreated_empty() {
            let (_dir, path) = file();
            holding_a_tx(&path);
            let len = std::fs::metadata(&path).unwrap().len();

            for cut in [320, 4096, 8192, len - 1] {
                let (_dir, path) = file();
                holding_a_tx(&path);
                let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
                file.set_len(cut).unwrap();
                drop(file);

                let Err(error) = RedbMempool::open_read_only(&path, &CONFIG) else {
                    panic!("a mempool cut to {cut} bytes opened read-only");
                };
                assert!(
                    incompatible(&error).is_some(),
                    "cut to {cut} bytes: {error}"
                );
                assert_eq!(std::fs::metadata(&path).unwrap().len(), cut);

                assert_eq!(pending(&path), 0, "cut to {cut} bytes");
                assert_eq!(recorded(&path), Some(MEMPOOL_VERSION), "cut to {cut} bytes");
            }
        }

        #[test]
        fn a_table_of_another_type_is_recreated_empty() {
            let (_dir, path) = file();
            holding_a_tx(&path);

            let db = redb::Database::create(&path).unwrap();
            let wx = db.begin_write().unwrap();
            wx.delete_table(FinalizedTable::DEF).unwrap();
            wx.open_table(TableDefinition::<u64, u64>::new(FinalizedTable::NAME))
                .unwrap()
                .insert(0, 1)
                .unwrap();
            wx.commit().unwrap();
            drop(db);

            assert_eq!(pending(&path), 0);
            assert_eq!(recorded(&path), Some(MEMPOOL_VERSION));
        }

        #[test]
        fn a_file_that_is_not_a_database_is_recreated_empty() {
            let (_dir, path) = file();
            std::fs::write(&path, vec![0xa5; 64 * 1024]).unwrap();

            assert_eq!(pending(&path), 0);
            assert_eq!(recorded(&path), Some(MEMPOOL_VERSION));
        }

        #[test]
        fn a_file_held_by_another_handle_is_a_lock_error_and_kept() {
            let (_dir, path) = file();
            holding_a_tx(&path);
            mark(&path, Some(MEMPOOL_VERSION + 1));
            // the holder marks the file in use as it opens it
            let _held = redb::Database::create(&path).unwrap();
            let before = std::fs::read(&path).unwrap();

            for opened in [
                RedbMempool::open(&path, &CONFIG),
                RedbMempool::open_read_only(&path, &CONFIG),
            ] {
                let Err(error) = opened else {
                    panic!("a held mempool file opened");
                };
                assert!(crate::handle::is_already_open(&error), "{error}");
            }

            assert_eq!(std::fs::read(&path).unwrap(), before);
        }

        #[cfg(unix)]
        #[test]
        fn a_file_it_may_not_write_is_an_error_and_kept() {
            use std::os::unix::fs::PermissionsExt as _;

            let (_dir, path) = file();
            holding_a_tx(&path);
            mark(&path, Some(MEMPOOL_VERSION + 1));
            let before = std::fs::read(&path).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();

            // root writes it regardless
            if std::fs::OpenOptions::new().write(true).open(&path).is_ok() {
                return;
            }

            let Err(error) = RedbMempool::open(&path, &CONFIG) else {
                panic!("a mempool file it may not write opened");
            };
            assert!(incompatible(&error).is_none(), "{error}");
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }

        #[test]
        fn a_read_only_open_refuses_another_version_and_keeps_it() {
            let (_dir, path) = file();
            holding_a_tx(&path);
            mark(&path, Some(MEMPOOL_VERSION + 1));
            let before = std::fs::read(&path).unwrap();

            let Err(error) = RedbMempool::open_read_only(&path, &CONFIG) else {
                panic!("a mempool file of another version opened read-only");
            };
            let refused = incompatible(&error).expect("not an incompatible mempool");
            assert!(
                matches!(refused.found, FoundVersion::Other(v) if v == MEMPOOL_VERSION + 1),
                "{refused}"
            );
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }

        #[test]
        fn a_read_only_open_refuses_a_file_that_is_not_a_database() {
            let (_dir, path) = file();
            std::fs::write(&path, vec![0xa5; 64 * 1024]).unwrap();

            let Err(error) = RedbMempool::open_read_only(&path, &CONFIG) else {
                panic!("a file that is not a database opened read-only");
            };
            assert!(incompatible(&error).is_some(), "{error}");
        }

        #[test]
        fn a_read_only_open_reads_a_file_of_this_version() {
            let (_dir, path) = file();
            holding_a_tx(&path);

            let store = RedbMempool::open_read_only(&path, &CONFIG).unwrap();
            assert_eq!(store.peek_pending().len(), 1);
        }
    }
}
