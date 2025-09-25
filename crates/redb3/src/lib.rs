use dolos_core::{EntityValue, Namespace, NamespaceType, StateSchema, TemporalKey};
use redb::{MultimapTableDefinition, ReadTransaction, TableDefinition, WriteTransaction};
use std::{collections::HashMap, ops::Range};
use tracing::trace;

pub mod archive;
pub mod state;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid cursor")]
    InvalidCursor,

    #[error(transparent)]
    DatabaseError(#[from] ::redb::DatabaseError),

    #[error(transparent)]
    TransactionError(Box<::redb::TransactionError>),

    #[error("internal error")]
    SetDurabilityError(Box<::redb::SetDurabilityError>),

    #[error(transparent)]
    CommitError(#[from] ::redb::CommitError),

    #[error(transparent)]
    TableError(#[from] ::redb::TableError),

    #[error(transparent)]
    StorageError(#[from] ::redb::StorageError),

    #[error("invalid operation")]
    InvalidOperation,

    // TODO: remove this once we generalize opaque filters
    #[error(transparent)]
    AddressError(#[from] pallas::ledger::addresses::Error),
}

impl From<::redb::SetDurabilityError> for Error {
    fn from(error: ::redb::SetDurabilityError) -> Self {
        Error::SetDurabilityError(Box::new(error))
    }
}

impl From<::redb::TransactionError> for Error {
    fn from(error: ::redb::TransactionError) -> Self {
        Error::TransactionError(Box::new(error))
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
            _ => Err(Error::InvalidOperation),
        }
    }

    pub fn range(
        &self,
        rx: &mut ReadTransaction,
        range: Range<&[u8]>,
    ) -> Result<redb::Range<'static, &'static [u8], &'static [u8]>, Error> {
        let Some(table) = self.as_value() else {
            return Err(Error::InvalidOperation);
        };

        let table = rx.open_table(*table)?;
        let values = table.range(range)?;

        Ok(values)
    }

    pub fn multivalues(
        &self,
        rx: &mut ReadTransaction,
        key: &[u8],
    ) -> Result<redb::MultimapValue<'static, &'static [u8]>, Error> {
        let Some(table) = self.as_multivalue() else {
            return Err(Error::InvalidOperation);
        };

        let table = rx.open_multimap_table(*table)?;

        let all_values = table.get(key.as_ref())?;

        Ok(all_values)
    }

    fn write(
        &self,
        wx: &WriteTransaction,
        key: impl AsRef<[u8]>,
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

    fn delete(&self, wx: &WriteTransaction, key: impl AsRef<[u8]>) -> Result<(), Error> {
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

    pub fn remove_before(
        &self,
        wx: &WriteTransaction,
        temporal: &TemporalKey,
    ) -> Result<(), Error> {
        match self {
            Table::Value(def) => {
                let mut open_table = wx.open_table(*def)?;
                let mut to_remove = open_table.extract_from_if(..temporal.as_ref(), |_, _| true)?;
                while let Some(Ok(_)) = to_remove.next() {
                    trace!("removing table entry");
                }

                Ok(())
            }
            _ => Err(Error::InvalidOperation),
        }
    }

    pub fn remove_after(&self, wx: &WriteTransaction, temporal: &TemporalKey) -> Result<(), Error> {
        match self {
            Table::Value(def) => {
                let mut open_table = wx.open_table(*def)?;
                let mut to_remove = open_table.extract_from_if(temporal.as_ref().., |_, _| true)?;
                while let Some(Ok(_)) = to_remove.next() {
                    trace!("removing table entry");
                }

                Ok(())
            }
            _ => Err(Error::InvalidOperation),
        }
    }
}

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
