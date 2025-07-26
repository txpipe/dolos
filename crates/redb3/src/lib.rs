use std::{collections::HashMap, ops::Range, path::Path, sync::Arc};

use dolos_core::{
    BlockSlot, EntityDelta, EntityKey, EntityValue, Namespace, NamespaceType, StateDelta,
    StateError3 as StateError, StateSchema,
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
    TransactionError(#[from] ::redb::TransactionError),

    #[error(transparent)]
    CommitError(#[from] ::redb::CommitError),

    #[error(transparent)]
    TableError(#[from] ::redb::TableError),

    #[error(transparent)]
    StorageError(#[from] ::redb::StorageError),

    #[error(transparent)]
    StateError(#[from] StateError),
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
            _ => return Err(Error::from(StateError::InvalidOpForTable)),
        }
    }

    pub fn iter_values(
        &self,
        rx: &mut ReadTransaction,
        range: Range<&[u8]>,
    ) -> Result<EntityIter, Error> {
        let Some(table) = self.as_value() else {
            return Err(Error::from(StateError::InvalidOpForTable));
        };

        let table = rx.open_table(*table)?;

        let values = table.range(range)?;

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

    pub fn apply(&self, wx: &mut WriteTransaction, crdt: EntityDelta) -> Result<(), Error> {
        match self {
            Table::Value(def) => {
                let mut open_table = wx.open_table(*def)?;

                match crdt {
                    EntityDelta::OverrideKey(key, value, _) => {
                        open_table.insert(key.as_slice(), value.as_slice())?;
                    }
                    EntityDelta::DeleteKey(key, _) => {
                        open_table.remove(key.as_slice())?;
                    }
                    _ => return Err(Error::from(StateError::InvalidOpForTable)),
                }
            }

            Table::MultiValue(def) => {
                let mut open_table = wx.open_multimap_table(*def)?;

                match crdt {
                    EntityDelta::AppendValue(key, value) => {
                        open_table.insert(key.as_slice(), value.as_slice())?;
                    }
                    EntityDelta::RemoveValue(key, value) => {
                        open_table.remove(key.as_slice(), value.as_slice())?;
                    }
                    _ => return Err(Error::from(StateError::InvalidOpForTable)),
                }
            }
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

pub const CURSOR_TABLE: TableDefinition<'static, u64, ()> = TableDefinition::new("cursor");

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

    pub(crate) fn db_mut(&mut self) -> Option<&mut Database> {
        Arc::get_mut(&mut self.db)
    }

    pub fn initialize_schema(&self) -> Result<(), Error> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Immediate);

        for (_, table) in self.tables.iter() {
            table.initialize(&mut wx)?;
        }

        wx.commit()?;

        Ok(())
    }

    fn append_cursor(&self, wx: &mut WriteTransaction, slot: BlockSlot) -> Result<(), Error> {
        let mut cursor = wx.open_table(CURSOR_TABLE)?;
        cursor.insert(slot, ())?;
        Ok(())
    }

    fn undo_cursor(&self, wx: &mut WriteTransaction, slot: BlockSlot) -> Result<(), Error> {
        let mut cursor = wx.open_table(CURSOR_TABLE)?;
        cursor.remove(slot)?;
        Ok(())
    }

    fn get_cursor(&self) -> Result<Option<BlockSlot>, Error> {
        let rx = self.db().begin_read().map_err(Error::from)?;
        let cursor = rx.open_table(CURSOR_TABLE)?;

        let mut range = cursor.range(0..)?.rev();

        let last = range.next().transpose()?.map(|(k, _)| k.value());

        Ok(last)
    }
}

impl dolos_core::StateStore3 for StateStore {
    type EntityIter = EntityIter;
    type EntityValueIter = EntityValueIter;

    fn get_cursor(&self) -> Result<Option<BlockSlot>, StateError> {
        let cursor = self.get_cursor()?;

        Ok(cursor)
    }

    fn read_entity(
        &self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<EntityValue>, StateError> {
        let mut rx = self.db().begin_read().map_err(Error::from)?;

        let table = self
            .tables
            .get(&ns)
            .ok_or(StateError::NamespaceNotFound(ns))?;

        let value = table.read_value(&mut rx, key.as_ref())?;

        Ok(value)
    }

    fn iter_entities(
        &self,
        ns: Namespace,
        range: Range<&[u8]>,
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

    fn apply_delta(&self, delta: StateDelta) -> Result<(), StateError> {
        let mut wx = self.db().begin_write().map_err(Error::from)?;

        wx.set_durability(Durability::Eventual);
        wx.set_quick_repair(true);

        self.append_cursor(&mut wx, delta.slot())?;

        for (ns, crdts) in delta.iter_deltas() {
            let table = self
                .tables
                .get(&ns)
                .ok_or(StateError::NamespaceNotFound(ns))?;

            for crdt in crdts {
                table.apply(&mut wx, crdt.clone())?;
            }
        }

        wx.commit().map_err(Error::from)?;

        Ok(())
    }

    fn undo_delta(&self, delta: StateDelta) -> Result<(), StateError> {
        let mut wx = self.db().begin_write().map_err(Error::from)?;

        wx.set_durability(Durability::Eventual);
        wx.set_quick_repair(true);

        self.undo_cursor(&mut wx, delta.slot())?;

        for (ns, crdts) in delta.iter_deltas() {
            let table = self
                .tables
                .get(&ns)
                .ok_or(StateError::NamespaceNotFound(ns))?;

            for crdt in crdts {
                let undo = crdt.into_undo();
                table.apply(&mut wx, undo)?;
            }
        }

        wx.commit().map_err(Error::from)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::StateStore3 as _;

    #[test]
    fn test_apply_value_table() {
        let mut schema = StateSchema::default();
        schema.insert("x", NamespaceType::KeyValue);

        let store = StateStore::in_memory(schema).unwrap();

        let mut delta = StateDelta::new(1);
        delta.override_key("x", b"a", b"123", None);
        delta.override_key("x", b"b", b"123", None);
        delta.override_key("x", b"c", b"123", None);
        delta.override_key("x", b"d", b"123", None);

        store.apply_delta(delta).unwrap();

        let cursor = store.get_cursor().unwrap();
        assert_eq!(cursor, Some(1));

        let value = store.read_entity("x", b"a").unwrap();
        assert_eq!(value, Some(b"123".to_vec()));

        let value = store.read_entity("x", b"b").unwrap();
        assert_eq!(value, Some(b"123".to_vec()));

        let mut iter = store.iter_entities("x", b"b"..b"d").unwrap();

        let (k, v) = iter.next().unwrap().unwrap();
        assert_eq!(k, b"b");
        assert_eq!(v, b"123");

        let (k, v) = iter.next().unwrap().unwrap();
        assert_eq!(k, b"c");
        assert_eq!(v, b"123");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_apply_multivalue_table() {
        let mut schema = StateSchema::default();
        schema.insert("y", NamespaceType::KeyMultiValue);

        let store = StateStore::in_memory(schema).unwrap();

        let mut delta = StateDelta::new(1);
        delta.append_value("y", b"a", b"123");
        delta.append_value("y", b"a", b"456");
        delta.append_value("y", b"b", b"123");
        delta.append_value("y", b"b", b"456");

        store.apply_delta(delta).unwrap();

        let cursor = store.get_cursor().unwrap();
        assert_eq!(cursor, Some(1));

        let mut iter = store.iter_entity_values("y", b"a").unwrap();

        let v = iter.next().unwrap().unwrap();
        assert_eq!(v, b"123");

        let v = iter.next().unwrap().unwrap();
        assert_eq!(v, b"456");

        assert!(iter.next().is_none());
    }
}
