use std::{collections::HashMap, ops::Range, path::Path, sync::Arc};

use dolos_core::{
    BlockSlot, EntityKey, EntityValue, Namespace, NamespaceType, State3Error as StateError,
    StateSchema,
};

use redb::{
    Database, Durability, MultimapTableDefinition, ReadTransaction, TableDefinition,
    WriteTransaction,
};

use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    DatabaseError(#[from] ::redb::DatabaseError),

    #[error(transparent)]
    TransactionError(Box<::redb::TransactionError>),

    #[error(transparent)]
    CommitError(#[from] ::redb::CommitError),

    #[error(transparent)]
    TableError(#[from] ::redb::TableError),

    #[error(transparent)]
    StorageError(#[from] ::redb::StorageError),

    #[error(transparent)]
    StateError(#[from] StateError),
}

impl From<::redb::TransactionError> for Error {
    fn from(error: ::redb::TransactionError) -> Self {
        Error::TransactionError(Box::new(error))
    }
}

impl From<Error> for StateError {
    fn from(error: Error) -> Self {
        match error {
            Error::StateError(e) => e,
            e => StateError::InternalStoreError(e.to_string()),
        }
    }
}

pub type ValueTable = TableDefinition<'static, &'static [u8], &'static [u8]>;
pub type MultiValueTable = MultimapTableDefinition<'static, &'static [u8], &'static [u8]>;

#[derive(Clone)]
pub enum Table {
    Value(ValueTable),
    MultiValue(MultiValueTable),
}

impl Table {
    pub fn new_value(name: &'static str) -> Self {
        Table::Value(TableDefinition::new(name))
    }

    pub fn new_multimap(name: &'static str) -> Self {
        Table::MultiValue(MultimapTableDefinition::new(name))
    }

    pub fn as_value(&self) -> Option<&ValueTable> {
        match self {
            Table::Value(def) => Some(def),
            _ => None,
        }
    }

    pub fn as_multivalue(&self) -> Option<&MultiValueTable> {
        match self {
            Table::MultiValue(def) => Some(def),
            _ => None,
        }
    }

    pub fn initialize(&self, wx: &mut WriteTransaction) -> Result<(), Error> {
        match self {
            Table::Value(def) => {
                let _ = wx.open_table(*def)?;
            }
            Table::MultiValue(def) => {
                let _ = wx.open_multimap_table(*def)?;
            }
        }

        Ok(())
    }

    pub fn read_value(
        &self,
        rx: &mut ReadTransaction,
        key: &[u8],
    ) -> Result<Option<EntityValue>, Error> {
        match self {
            Table::Value(def) => {
                let table = rx.open_table(*def)?;
                let value = table.get(key)?;
                Ok(value.map(|v| v.value().to_vec()))
            }
            _ => Err(Error::from(StateError::InvalidOpForTable)),
        }
    }

    pub fn iter_values(
        &self,
        rx: &mut ReadTransaction,
        range: Range<EntityKey>,
    ) -> Result<EntityIter, Error> {
        let Some(table) = self.as_value() else {
            return Err(Error::from(StateError::InvalidOpForTable));
        };

        let table = rx.open_table(*table)?;

        let start = range.start.as_ref();
        let end = range.end.as_ref();

        let values = table.range(start..end)?;

        Ok(EntityIter(values))
    }

    pub fn iter_multivalues(
        &self,
        rx: &mut ReadTransaction,
        key: &[u8],
    ) -> Result<EntityValueIter, Error> {
        let Some(table) = self.as_multivalue() else {
            return Err(Error::from(StateError::InvalidOpForTable));
        };

        let table = rx.open_multimap_table(*table)?;

        let all_values = table.get(key.as_ref())?;

        Ok(EntityValueIter(all_values))
    }

    fn write_entity(
        &self,
        wx: &mut WriteTransaction,
        key: &EntityKey,
        value: &EntityValue,
    ) -> Result<(), Error> {
        match self {
            Table::Value(def) => {
                let mut open_table = wx.open_table(*def)?;
                open_table.insert(key.as_ref(), value.as_slice())?;
            }
            Table::MultiValue(def) => {
                let mut open_table = wx.open_multimap_table(*def)?;
                open_table.insert(key.as_ref(), value.as_slice())?;
            }
        }

        Ok(())
    }

    fn delete_entity(&self, wx: &mut WriteTransaction, key: &EntityKey) -> Result<(), Error> {
        match self {
            Table::Value(def) => {
                let mut open_table = wx.open_table(*def)?;
                open_table.remove(key.as_ref())?;
            }
            Table::MultiValue(def) => {
                let mut open_table = wx.open_multimap_table(*def)?;
                open_table.remove_all(key.as_ref())?;
            }
        }

        Ok(())
    }

    fn delete_entity_value(
        &self,
        wx: &mut WriteTransaction,
        key: &EntityKey,
        value: &EntityValue,
    ) -> Result<(), Error> {
        match self {
            Table::MultiValue(def) => {
                let mut open_table = wx.open_multimap_table(*def)?;
                open_table.remove(key.as_ref(), value.as_slice())?;
            }
            _ => return Err(Error::from(StateError::InvalidOpForTable)),
        }

        Ok(())
    }
}

pub struct EntityIter(::redb::Range<'static, &'static [u8], &'static [u8]>);

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

pub struct EntityValueIter(::redb::MultimapValue<'static, &'static [u8]>);

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

pub const CURSOR_TABLE: TableDefinition<'static, u16, u64> = TableDefinition::new("cursor");

fn build_tables(schema: StateSchema) -> HashMap<Namespace, Table> {
    let tables = schema.iter().map(|(ns, ty)| {
        (
            *ns,
            match ty {
                NamespaceType::KeyValue => Table::new_value(ns),
                NamespaceType::KeyMultiValue => Table::new_multimap(ns),
            },
        )
    });

    HashMap::from_iter(tables)
}

const DEFAULT_CACHE_SIZE_MB: usize = 500;

#[derive(Clone)]
pub struct StateStore {
    db: Arc<Database>,
    tables: HashMap<Namespace, Table>,
}

impl StateStore {
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
        wx.set_durability(Durability::Immediate);

        let _ = wx.open_table(CURSOR_TABLE)?;

        for (_, table) in self.tables.iter() {
            table.initialize(&mut wx)?;
        }

        wx.commit()?;

        Ok(())
    }

    fn set_cursor(wx: &mut WriteTransaction, slot: BlockSlot) -> Result<(), Error> {
        let mut cursor = wx.open_table(CURSOR_TABLE)?;
        cursor.insert(CURRENT_CURSOR_KEY, &slot)?;

        Ok(())
    }

    fn read_cursor(rx: &ReadTransaction) -> Result<Option<BlockSlot>, Error> {
        let cursor = rx.open_table(CURSOR_TABLE)?;
        let value = cursor.get(CURRENT_CURSOR_KEY)?.map(|x| x.value());

        Ok(value)
    }
}

impl dolos_core::State3Store for StateStore {
    type EntityIter = EntityIter;
    type EntityValueIter = EntityValueIter;

    fn read_cursor(&self) -> Result<Option<BlockSlot>, StateError> {
        let rx = self.db().begin_read().map_err(Error::from)?;

        let cursor = Self::read_cursor(&rx)?;

        Ok(cursor)
    }

    fn set_cursor(&self, cursor: BlockSlot) -> Result<(), StateError> {
        let mut wx = self.db().begin_write().map_err(Error::from)?;

        Self::set_cursor(&mut wx, cursor)?;

        wx.commit().map_err(Error::from)?;

        Ok(())
    }

    fn iter_entities(
        &self,
        ns: Namespace,
        range: std::ops::Range<EntityKey>,
    ) -> Result<Self::EntityIter, StateError> {
        let mut rx = self.db().begin_read().map_err(Error::from)?;

        let table = self
            .tables
            .get(&ns)
            .ok_or(StateError::NamespaceNotFound(ns))?;

        let values = table.iter_values(&mut rx, range)?;

        Ok(values)
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

        let values = table.iter_multivalues(&mut rx, key.as_ref())?;

        Ok(values)
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

    fn write_entity(
        &self,
        ns: Namespace,
        key: &EntityKey,
        value: &EntityValue,
    ) -> Result<(), StateError> {
        let mut wx = self.db().begin_write().map_err(Error::from)?;

        let table = self
            .tables
            .get(&ns)
            .ok_or(StateError::NamespaceNotFound(ns))?;

        table.write_entity(&mut wx, key, value)?;

        wx.commit().map_err(Error::from)?;

        Ok(())
    }

    fn write_entity_batch(
        &self,
        ns: Namespace,
        batch: HashMap<EntityKey, EntityValue>,
    ) -> Result<(), StateError> {
        let mut wx = self.db().begin_write().map_err(Error::from)?;

        let table = self
            .tables
            .get(&ns)
            .ok_or(StateError::NamespaceNotFound(ns))?;

        for (k, v) in batch.iter() {
            table.write_entity(&mut wx, k, v)?;
        }

        wx.commit().map_err(Error::from)?;

        Ok(())
    }

    fn delete_entity(&self, ns: Namespace, key: &EntityKey) -> Result<(), StateError> {
        let mut wx = self.db().begin_write().map_err(Error::from)?;

        let table = self
            .tables
            .get(&ns)
            .ok_or(StateError::NamespaceNotFound(ns))?;

        table.delete_entity(&mut wx, key)?;

        wx.commit().map_err(Error::from)?;

        Ok(())
    }
}
