use std::{path::Path, sync::Arc};

use itertools::Itertools;
use redb::{ReadableTable as _, TableDefinition};
use tracing::warn;

pub type WorkerId = String;
pub type LogSeq = u64;

const CURSORS: TableDefinition<WorkerId, LogSeq> = TableDefinition::new("cursors");

const DEFAULT_CACHE_SIZE_MB: usize = 50;

#[derive(Clone)]
pub struct Store {
    db: Arc<redb::Database>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Self, super::Error> {
        let inner = redb::Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "balius db is repairing")
            })
            .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)?;

        let out = Self {
            db: Arc::new(inner),
        };

        Ok(out)
    }

    pub fn lowest_cursor(&self) -> Result<Option<LogSeq>, super::Error> {
        let rx = self.db.begin_read()?;

        let table = rx.open_table(CURSORS)?;

        let cursors: Vec<_> = table
            .iter()?
            .map_ok(|(_, value)| value.value())
            .try_collect()?;

        let lowest = cursors.iter().fold(None, |all, item| all.min(Some(*item)));

        Ok(lowest)
    }
}
