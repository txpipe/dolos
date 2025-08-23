use std::{collections::HashMap, marker::PhantomData, ops::Range};

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
    entries: HashMap<Namespace, Vec<EntityDelta>>,
    overriden: HashMap<Namespace, HashMap<EntityKey, EntityValue>>,
    slot: Option<BlockSlot>,
}

impl Default for StateDelta {
    fn default() -> Self {
        Self::new()
    }
}

impl StateDelta {
    pub fn new() -> Self {
        Self {
            slot: None,
            entries: HashMap::new(),
            overriden: HashMap::new(),
        }
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
        let key = key.into();
        let value = value.into();
        self.entries
            .entry(ns)
            .or_default()
            .push(EntityDelta::OverrideKey(key.clone(), value.clone(), prev));
        self.overriden.entry(ns).or_default().insert(key, value);
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

    pub fn get_from_overriden<T: Entity>(&mut self, key: impl Into<EntityKey>) -> Option<T> {
        self.overriden
            .entry(T::NS)
            .or_default()
            .get(&key.into())
            .map(|value| T::decode_value(value.clone()).unwrap())
    }

    pub fn append_entity<T: Entity>(&mut self, key: impl Into<EntityKey>, entity: T) {
        self.append_value(T::NS, key, entity.encode_value());
    }

    pub fn set_slot(&mut self, slot: BlockSlot) {
        self.slot = Some(slot)
    }

    pub fn slot(&self) -> BlockSlot {
        self.slot.unwrap()
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

    #[error("invalid delta: {0}")]
    InvalidDelta(String),
}

// temporary alias to avoid collision with existing StateError
pub type State3Error = StateError;

pub struct EntityIterTyped<S: State3Store, T: Entity> {
    inner: S::EntityIter,
    _marker: PhantomData<T>,
}

impl<S: State3Store, T: Entity> EntityIterTyped<S, T> {
    pub fn new(inner: S::EntityIter) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<S: State3Store, T: Entity> Iterator for EntityIterTyped<S, T> {
    type Item = Result<(EntityKey, T), StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next()?;

        let mapped = next.and_then(|(key, value)| T::decode_value(value).map(|v| (key, v)));

        Some(mapped)
    }
}

pub struct EntityValueIterTyped<S: State3Store, T: Entity> {
    inner: S::EntityValueIter,
    _marker: PhantomData<T>,
}

impl<S: State3Store, T: Entity> EntityValueIterTyped<S, T> {
    pub fn new(inner: S::EntityValueIter) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<S: State3Store, T: Entity> Iterator for EntityValueIterTyped<S, T> {
    type Item = Result<T, StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next()?;

        let mapped = next.and_then(|value| T::decode_value(value));

        Some(mapped)
    }
}

pub trait State3Store: Sized {
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

    fn iter_entities_typed<T: Entity>(
        &self,
        range: Range<&[u8]>,
    ) -> Result<EntityIterTyped<Self, T>, StateError> {
        let inner = self.iter_entities(T::NS, range)?;
        Ok(EntityIterTyped::<_, T>::new(inner))
    }

    fn iter_entity_values_typed<T: Entity>(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<EntityValueIterTyped<Self, T>, StateError> {
        let inner = self.iter_entity_values(T::NS, key)?;
        Ok(EntityValueIterTyped::<_, T>::new(inner))
    }
}

pub trait Entity: Sized {
    const NS: Namespace;
    const NS_TYPE: NamespaceType;

    fn decode_value(value: EntityValue) -> Result<Self, StateError>;
    fn encode_value(self) -> EntityValue;
}
