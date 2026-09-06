//! In-memory index store: a cursor behind a lock.
//!
//! The archive tags and the exact lookups this store used to hold moved to
//! the memory archive beside the blocks they project (see `memory/archive.rs`);
//! the live-UTxO tags had already moved to the memory state store. What is
//! left is the cursor the bootstrap catch-up still reads.

use std::sync::{Arc, Mutex, RwLock};

use crate::indexes::{IndexDelta, IndexError, IndexStore, IndexWriter};
use crate::ChainPoint;

fn poisoned() -> IndexError {
    IndexError::DbError("index store lock poisoned".into())
}

/// Index store held entirely in memory.
///
/// Cloning shares the underlying cursor — a clone is another handle on the
/// same store, not a copy of it.
#[derive(Clone, Default)]
pub struct MemoryIndexStore {
    cursor: Arc<RwLock<Option<ChainPoint>>>,
}

impl MemoryIndexStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Present for symmetry with the disk backends. Nothing to drain.
    pub fn shutdown(&self) -> Result<(), IndexError> {
        Ok(())
    }
}

/// Writer that holds the cursor it was asked to place until
/// [`IndexWriter::commit`].
pub struct MemoryIndexWriter {
    store: MemoryIndexStore,
    cursor: Mutex<Option<ChainPoint>>,
}

impl IndexWriter for MemoryIndexWriter {
    fn apply(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        *self.cursor.lock().map_err(|_| poisoned())? = Some(delta.cursor.clone());

        Ok(())
    }

    fn commit(self) -> Result<(), IndexError> {
        let cursor = self.cursor.into_inner().map_err(|_| poisoned())?;

        if let Some(cursor) = cursor {
            *self.store.cursor.write().map_err(|_| poisoned())? = Some(cursor);
        }

        Ok(())
    }
}

impl IndexStore for MemoryIndexStore {
    type Writer = MemoryIndexWriter;

    fn start_writer(&self) -> Result<Self::Writer, IndexError> {
        Ok(MemoryIndexWriter {
            store: self.clone(),
            cursor: Mutex::new(None),
        })
    }

    fn initialize_schema(&self) -> Result<(), IndexError> {
        Ok(())
    }

    /// Merges this store's contents into `target`, the same way the disk
    /// backends do: a copy adds, it does not replace.
    fn copy(&self, target: &Self) -> Result<(), IndexError> {
        // Cloned and released before touching the target, so copying a store
        // onto itself is a no-op rather than a deadlock.
        let source = self.cursor.read().map_err(|_| poisoned())?.clone();

        if let Some(cursor) = source {
            *target.cursor.write().map_err(|_| poisoned())? = Some(cursor);
        }

        Ok(())
    }

    fn cursor(&self) -> Result<Option<ChainPoint>, IndexError> {
        Ok(self.cursor.read().map_err(|_| poisoned())?.clone())
    }
}
