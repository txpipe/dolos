use bincode;
use log::info;
use redb::{Range, ReadableTable, TableDefinition};
use std::{path::Path, sync::Arc};
use tracing::warn;

use super::{ChainPoint, LogEntry, LogSeq, LogValue, RawBlock, WalError, WalReader, WalWriter};

impl redb::Value for LogValue {
    type SelfType<'a> = Self;
    type AsBytes<'a> = Vec<u8> where Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        bincode::deserialize(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        bincode::serialize(value).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("logvalue")
    }
}

pub type AugmentedBlockSlot = i128;

const WAL: TableDefinition<LogSeq, LogValue> = TableDefinition::new("wal");
const POS: TableDefinition<AugmentedBlockSlot, LogSeq> = TableDefinition::new("pos");

fn point_to_augmented_slot(point: &ChainPoint) -> AugmentedBlockSlot {
    match point {
        ChainPoint::Origin => -1i128,
        ChainPoint::Specific(x, _) => *x as i128,
    }
}

pub struct WalIter<'a>(Range<'a, LogSeq, LogValue>);

impl<'a> Iterator for WalIter<'a> {
    type Item = LogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
    }
}

impl<'a> DoubleEndedIterator for WalIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
    }
}

impl<T> From<T> for WalError
where
    T: Into<redb::Error>,
{
    fn from(value: T) -> Self {
        WalError::IO(value.into().into())
    }
}

/// Concrete implementation of WalStore using Redb
#[derive(Clone)]
pub struct WalStore {
    db: Arc<redb::Database>,
    tip_change: Arc<tokio::sync::Notify>,
}

impl WalStore {
    pub fn is_empty(&self) -> Result<bool, WalError> {
        let wr = self.db.begin_read()?;

        if wr.list_tables()?.count() == 0 {
            return Ok(true);
        }

        if self.crawl_from(None)?.next().is_none() {
            return Ok(true);
        }

        Ok(false)
    }

    fn initialize(&mut self) -> Result<(), WalError> {
        if self.is_empty()? {
            info!("initializing wal");
            self.append_entries(std::iter::once(LogValue::Mark(ChainPoint::Origin)))?;
        }

        Ok(())
    }

    pub fn memory() -> Result<Self, WalError> {
        let db =
            redb::Database::builder().create_with_backend(redb::backends::InMemoryBackend::new())?;

        let mut out = Self {
            db: Arc::new(db),
            tip_change: Arc::new(tokio::sync::Notify::new()),
        };

        out.initialize()?;

        Ok(out)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let inner = redb::Database::builder()
            .set_repair_callback(|x| warn!(progress = x.progress() * 100f64, "wal db is repairing"))
            .create(path)?;

        let mut out = Self {
            db: Arc::new(inner),
            tip_change: Arc::new(tokio::sync::Notify::new()),
        };

        out.initialize()?;

        Ok(out)
    }
}

impl super::WalReader for WalStore {
    type LogIterator<'a> = WalIter<'a>;

    async fn tip_change(&self) -> Result<(), WalError> {
        self.tip_change.notified().await;

        Ok(())
    }

    fn crawl_range<'a>(
        &self,
        start: LogSeq,
        end: LogSeq,
    ) -> Result<Self::LogIterator<'a>, WalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let range = table.range(start..=end)?;

        Ok(WalIter(range))
    }

    fn crawl_from<'a>(&self, start: Option<LogSeq>) -> Result<Self::LogIterator<'a>, WalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let range = match start {
            Some(start) => table.range(start..)?,
            None => table.range(0..)?,
        };

        Ok(WalIter(range))
    }

    fn locate_point(&self, point: &super::ChainPoint) -> Result<Option<LogSeq>, WalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(POS)?;

        let pos_key = point_to_augmented_slot(point);
        let pos = table.get(pos_key)?.map(|x| x.value());

        Ok(pos)
    }
}

impl super::WalWriter for WalStore {
    fn append_entries(
        &mut self,
        logs: impl Iterator<Item = super::LogValue>,
    ) -> Result<(), super::WalError> {
        let wx = self.db.begin_write()?;

        {
            let mut wal = wx.open_table(WAL)?;
            let mut pos = wx.open_table(POS)?;

            let mut next_seq = wal.last()?.map(|(x, _)| x.value() + 1).unwrap_or_default();

            for log in logs {
                // Since we need to track Origin as part of the wal, we turn slots into signed
                // integers and treat -1 as the reference for Origin. This is not ideal from
                // disk space perspective, but good enough for this stage.
                let pos_key = match &log {
                    LogValue::Apply(RawBlock { slot, .. }) => *slot as i128,
                    LogValue::Undo(RawBlock { slot, .. }) => *slot as i128,
                    LogValue::Mark(x) => point_to_augmented_slot(x),
                };

                pos.insert(pos_key, next_seq)?;
                wal.insert(next_seq, log)?;

                next_seq += 1;
            }
        }

        wx.commit()?;

        self.tip_change.notify_waiters();

        Ok(())
    }
}
