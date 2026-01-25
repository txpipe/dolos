use std::{collections::HashMap, path::Path, sync::Arc};

use dolos_core::{
    ChainPoint, EntityKey, EntityValue, Namespace, StateError, StateSchema, TxoRef, UtxoMap,
};

use redb::{
    Database, Durability, ReadTransaction, ReadableDatabase, TableDefinition, WriteTransaction,
};

use tracing::warn;

pub(crate) mod utxoset;

use crate::{build_tables, Error, Table};

impl From<Error> for StateError {
    fn from(error: Error) -> Self {
        StateError::InternalStoreError(error.to_string())
    }
}

pub struct EntityIter(pub(crate) ::redb::Range<'static, &'static [u8], &'static [u8]>);

impl Iterator for EntityIter {
    type Item = Result<(EntityKey, EntityValue), StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0.next()?;

        let entry = next
            .map(|(k, v)| (k.value().to_vec(), v.value().to_vec()))
            .map(|(k, v)| (EntityKey::from(k), v))
            .map_err(Error::from)
            .map_err(StateError::from);

        Some(entry)
    }
}

pub struct EntityValueIter(pub(crate) ::redb::MultimapValue<'static, &'static [u8]>);

impl Iterator for EntityValueIter {
    type Item = Result<EntityValue, StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0.next()?;

        let entry = next
            .map(|v| v.value().to_vec())
            .map_err(Error::from)
            .map_err(StateError::from);

        Some(entry)
    }
}

pub const CURRENT_CURSOR_KEY: u16 = 0;

pub const CURSOR_TABLE: TableDefinition<'static, u16, Vec<u8>> = TableDefinition::new("cursor");

const DEFAULT_CACHE_SIZE_MB: usize = 500;

#[derive(Clone)]
pub struct StateStore {
    db: Arc<Database>,
    tables: HashMap<Namespace, Table>,
}

impl StateStore {
    /// Gracefully shutdown the state store.
    ///
    /// For Redb, this is a no-op since Redb handles cleanup automatically
    /// during drop without blocking issues.
    pub fn shutdown(&self) -> Result<(), Error> {
        Ok(())
    }

    pub fn open(
        schema: StateSchema,
        path: impl AsRef<Path>,
        cache_size: Option<usize>,
    ) -> Result<Self, Error> {
        let db = ::redb::Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "state3 db is repairing")
            })
            .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)?;

        let tables = build_tables(schema);

        let store = Self {
            db: db.into(),
            tables: HashMap::from_iter(tables),
        };

        store.initialize_schema()?;

        Ok(store)
    }

    pub fn in_memory(schema: StateSchema) -> Result<Self, Error> {
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())?;

        let tables = build_tables(schema);

        let store = Self {
            db: db.into(),
            tables: HashMap::from_iter(tables),
        };

        store.initialize_schema()?;

        Ok(store)
    }

    pub(crate) fn db(&self) -> &Database {
        &self.db
    }

    #[allow(dead_code)]
    pub(crate) fn db_mut(&mut self) -> Option<&mut Database> {
        Arc::get_mut(&mut self.db)
    }

    pub fn initialize_schema(&self) -> Result<(), Error> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Immediate)?;

        let _ = wx.open_table(CURSOR_TABLE)?;

        for (_, table) in self.tables.iter() {
            table.initialize(&mut wx)?;
        }

        // TODO: refactor into entities model
        utxoset::UtxosTable::initialize(&wx)?;

        wx.commit()?;

        Ok(())
    }

    fn read_cursor(rx: &ReadTransaction) -> Result<Option<ChainPoint>, Error> {
        let cursor = rx.open_table(CURSOR_TABLE)?;
        let value = cursor.get(CURRENT_CURSOR_KEY)?.map(|x| x.value());

        let Some(value) = value else {
            return Ok(None);
        };

        let point = bincode::deserialize(&value).map_err(|_| Error::InvalidCursor)?;

        Ok(Some(point))
    }

    pub fn copy(&self, target: &Self) -> Result<(), Error> {
        let rx = self.db().begin_read()?;
        let wx = target.db().begin_write()?;

        utxoset::UtxosTable::copy(&rx, &wx)?;

        wx.commit()?;

        Ok(())
    }
}

pub struct StateWriter {
    tables: HashMap<Namespace, Table>,
    wx: WriteTransaction,
}

impl StateWriter {
    fn new(db: &Database, tables: HashMap<Namespace, Table>) -> Self {
        let wx = db.begin_write().unwrap();
        Self { tables, wx }
    }
}

impl dolos_core::StateWriter for StateWriter {
    fn set_cursor(&self, cursor: ChainPoint) -> Result<(), StateError> {
        let mut table = self.wx.open_table(CURSOR_TABLE).map_err(Error::from)?;

        let point = bincode::serialize(&cursor).unwrap();

        table
            .insert(CURRENT_CURSOR_KEY, &point)
            .map_err(Error::from)?;

        Ok(())
    }

    fn write_entity(
        &self,
        ns: Namespace,
        key: &EntityKey,
        value: &EntityValue,
    ) -> Result<(), StateError> {
        let table = self
            .tables
            .get(&ns)
            .ok_or(StateError::NamespaceNotFound(ns))?;

        table.write(&self.wx, key, value)?;

        Ok(())
    }

    fn delete_entity(&self, ns: Namespace, key: &EntityKey) -> Result<(), StateError> {
        let table = self
            .tables
            .get(&ns)
            .ok_or(StateError::NamespaceNotFound(ns))?;

        table.delete(&self.wx, key)?;

        Ok(())
    }

    fn apply_utxoset(&self, delta: &dolos_core::UtxoSetDelta) -> Result<(), StateError> {
        utxoset::UtxosTable::apply(&self.wx, delta)?;

        Ok(())
    }

    fn commit(self) -> Result<(), StateError> {
        self.wx.commit().map_err(Error::from)?;

        Ok(())
    }
}

impl dolos_core::StateStore for StateStore {
    type EntityIter = EntityIter;
    type EntityValueIter = EntityValueIter;
    type Writer = StateWriter;

    fn read_cursor(&self) -> Result<Option<ChainPoint>, StateError> {
        let rx = self.db().begin_read().map_err(Error::from)?;

        let cursor = Self::read_cursor(&rx)?;

        Ok(cursor)
    }

    fn start_writer(&self) -> Result<Self::Writer, StateError> {
        Ok(StateWriter::new(&self.db, self.tables.clone()))
    }

    fn iter_entities(
        &self,
        ns: Namespace,
        range: std::ops::Range<EntityKey>,
    ) -> Result<Self::EntityIter, StateError> {
        let mut rx = self.db().begin_read().map_err(Error::from)?;

        let range = std::ops::Range {
            start: range.start.as_ref(),
            end: range.end.as_ref(),
        };

        let table = self
            .tables
            .get(&ns)
            .ok_or(StateError::NamespaceNotFound(ns))?;

        let values = table.range(&mut rx, range)?;

        Ok(EntityIter(values))
    }

    fn iter_entity_values(
        &self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Self::EntityValueIter, StateError> {
        let mut rx = self.db().begin_read().map_err(Error::from)?;

        let table = self
            .tables
            .get(&ns)
            .ok_or(StateError::NamespaceNotFound(ns))?;

        let values = table.multivalues(&mut rx, key.as_ref())?;

        Ok(EntityValueIter(values))
    }

    fn read_entities(
        &self,
        ns: Namespace,
        keys: &[&EntityKey],
    ) -> Result<Vec<Option<EntityValue>>, StateError> {
        let mut rx = self.db().begin_read().map_err(Error::from)?;

        let table = self
            .tables
            .get(&ns)
            .ok_or(StateError::NamespaceNotFound(ns))?;

        let mut out = vec![];

        for key in keys {
            let value = table.read_value(&mut rx, key.as_ref())?;
            out.push(value);
        }

        Ok(out)
    }

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError> {
        // exit early before opening a read tx in case there's nothing to fetch
        if refs.is_empty() {
            return Ok(Default::default());
        }

        let rx = self.db().begin_read().map_err(Error::from)?;

        let out = utxoset::UtxosTable::get_sparse(&rx, refs)?;

        Ok(out)
    }
}
