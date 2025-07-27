use std::{collections::HashMap, ops::Range};

use crate::BlockSlot;

pub type Namespace = &'static str;
pub type EntityKey = Vec<u8>;
pub type EntityValue = Vec<u8>;
pub type EntityPrevValue = EntityValue;
pub type EntityNewValue = EntityValue;

#[derive(Debug, Clone)]
pub enum NamespaceType {
    KeyValue,
    KeyMultiValue,
}

#[derive(Debug, Clone, Default)]
pub struct StateSchema(HashMap<Namespace, NamespaceType>);

impl std::ops::DerefMut for StateSchema {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::ops::Deref for StateSchema {
    type Target = HashMap<Namespace, NamespaceType>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub enum EntityDelta {
    OverrideKey(EntityKey, EntityNewValue, Option<EntityPrevValue>),
    DeleteKey(EntityKey, EntityPrevValue),
    AppendValue(EntityKey, EntityNewValue),
    RemoveValue(EntityKey, EntityPrevValue),
}

impl EntityDelta {
    pub fn into_undo(self) -> Self {
        match self {
            Self::OverrideKey(key, new, Some(prev)) => Self::OverrideKey(key, prev, Some(new)),
            Self::OverrideKey(key, new, None) => Self::DeleteKey(key, new),
            Self::DeleteKey(key, prev) => Self::OverrideKey(key, prev, None),
            Self::AppendValue(key, new) => Self::RemoveValue(key, new),
            Self::RemoveValue(key, prev) => Self::AppendValue(key, prev),
        }
    }
}

pub struct StateDelta {
    slot: BlockSlot,
    entries: HashMap<Namespace, Vec<EntityDelta>>,
}

impl StateDelta {
    pub fn new(slot: BlockSlot) -> Self {
        Self {
            slot,
            entries: HashMap::new(),
        }
    }

    pub fn slot(&self) -> BlockSlot {
        self.slot
    }

    pub fn iter_deltas(self) -> impl Iterator<Item = (Namespace, Vec<EntityDelta>)> {
        self.entries.into_iter()
    }

    pub fn override_key(
        &mut self,
        ns: Namespace,
        key: impl Into<EntityKey>,
        value: impl Into<EntityValue>,
        prev: Option<EntityPrevValue>,
    ) {
        self.entries
            .entry(ns)
            .or_default()
            .push(EntityDelta::OverrideKey(key.into(), value.into(), prev));
    }

    pub fn delete_key(
        &mut self,
        ns: Namespace,
        key: impl Into<EntityKey>,
        prev: impl Into<EntityPrevValue>,
    ) {
        self.entries
            .entry(ns)
            .or_default()
            .push(EntityDelta::DeleteKey(key.into(), prev.into()));
    }

    pub fn append_value(
        &mut self,
        ns: Namespace,
        key: impl Into<EntityKey>,
        value: impl Into<EntityValue>,
    ) {
        self.entries
            .entry(ns)
            .or_default()
            .push(EntityDelta::AppendValue(key.into(), value.into()));
    }

    pub fn remove_value(
        &mut self,
        ns: Namespace,
        key: impl Into<EntityKey>,
        value: impl Into<EntityValue>,
    ) {
        self.entries
            .entry(ns)
            .or_default()
            .push(EntityDelta::RemoveValue(key.into(), value.into()));
    }

    pub fn override_entity<T: Entity>(
        &mut self,
        key: impl Into<EntityKey>,
        entity: T,
        prev: Option<T>,
    ) {
        let entity = entity.encode_value();
        let prev = prev.map(T::encode_value);

        self.override_key(T::NS, key, entity, prev);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("internal store error: {0}")]
    InternalStoreError(String),

    #[error("invalid operation for table")]
    InvalidOpForTable,

    #[error("namespace {0} not found")]
    NamespaceNotFound(Namespace),

    #[error("encoding error: {0}")]
    EncodingError(String),

    #[error(transparent)]
    DecodingError(#[from] pallas::codec::minicbor::decode::Error),
}

// temporary alias to avoid collision with existing StateError
pub type State3Error = StateError;

pub trait State3Store {
    type EntityIter: Iterator<Item = Result<(EntityKey, EntityValue), StateError>>;
    type EntityValueIter: Iterator<Item = Result<EntityValue, StateError>>;

    fn get_cursor(&self) -> Result<Option<BlockSlot>, StateError>;

    fn apply_delta(&self, delta: StateDelta) -> Result<(), StateError>;

    fn undo_delta(&self, delta: StateDelta) -> Result<(), StateError>;

    fn read_entity(
        &self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<EntityValue>, StateError>;

    fn iter_entities(
        &self,
        ns: Namespace,
        range: Range<&[u8]>,
    ) -> Result<Self::EntityIter, StateError>;

    fn iter_entity_values(
        &self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Self::EntityValueIter, StateError>;

    fn read_entity_typed<T: Entity>(&self, key: impl AsRef<[u8]>) -> Result<Option<T>, StateError> {
        let value = self.read_entity(T::NS, key)?;
        let decoded = value.map(T::decode_value).transpose()?;

        Ok(decoded)
    }
}

pub trait Entity: Sized {
    const NS: Namespace;
    const NS_TYPE: NamespaceType;

    fn decode_value(value: EntityValue) -> Result<Self, StateError>;
    fn encode_value(self) -> EntityValue;
}
